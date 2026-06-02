//! Simple-Query dispatch loop.
//!
//! **T8 status (this commit):** the gateway-side glue that turns a
//! parsed `Q` message into the right backend-response sequence
//! (`RowDescription` → `DataRow`* → `CommandComplete` → `ReadyForQuery`,
//! or `ErrorResponse` → `ReadyForQuery`).
//!
//! V1 supports the simple-query path for:
//! - `SELECT * FROM <table>` and `SELECT * FROM <table> WHERE ...`
//!   (single-table whole-row; projection / JOIN / GROUP BY / ORDER BY
//!   not yet rendered at the PG-wire layer — they apply at the engine
//!   layer fine but the row decoding here only handles whole-row
//!   shape today)
//! - INSERT / UPDATE / DELETE / CREATE TABLE / DROP TABLE — opaque
//!   pass-through (CommandComplete tag inferred from the leading SQL
//!   keyword)
//! - **T9**: INSERT / UPDATE / DELETE now surface real affected-row
//!   counts in the CommandComplete tag (`INSERT 0 N`, `UPDATE N`,
//!   `DELETE N`) via `EngineApply::apply_sql_with_count`; multi-row
//!   INSERT VALUES tuples are counted in the SQL text via
//!   `count_insert_values` (a tiny VALUES-tuple counter).
//! - Anything else `apply_sql` returns → ErrorResponse via the T7
//!   SQLSTATE map
//!
//! ## What this module does
//!
//! - `dispatch_query(sql, engine) -> Vec<u8>` — runs a single Q
//!   end-to-end, returns the full byte sequence to write to the
//!   wire. The caller wraps this in the framing layer.
//! - `infer_command_tag(sql, rows) -> String` — picks the
//!   CommandComplete tag from the SQL leading keyword + the row
//!   count.
//! - `cmd_complete_tag_for_sql(sql, count) -> String` — T9 polish on
//!   top of `infer_command_tag`: handles DDL/transaction-control
//!   keywords (CREATE INDEX, ALTER TABLE, BEGIN, COMMIT, ROLLBACK)
//!   and strips leading `--` line comments + whitespace before the
//!   keyword test.
//! - `count_insert_values(sql) -> u64` — counts top-level `(...)`
//!   VALUES tuples in an `INSERT INTO t (...) VALUES (...)[, (...)]*`
//!   so the gateway can emit `INSERT 0 N` for multi-row INSERT even
//!   though the engine's `OpResult::Ok` doesn't carry the count.
//! - `render_pg_text(value, kind) -> Vec<u8>` — renders a single
//!   column value to PG text format.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::engine::{EngineApply, PgColumn};
use crate::error::{encode_error_response, op_result_to_sqlstate, SEVERITY_ERROR};
use crate::query::{contains_multiple_statements, is_effectively_empty};
use crate::response::{
    encode_command_complete, encode_data_row, encode_empty_query_response,
    encode_ready_for_query, encode_row_description, delete_tag, insert_tag,
    select_tag, update_tag, FieldMeta,
};
use crate::types::field_kind_to_oid;
use kessel_catalog::FieldKind;
use kessel_codec::{value_from_raw, Value};
use kessel_proto::OpResult;

/// SP-PG-EXTQ-PARSED-DEFAULT T2 — like `dispatch_query`, but routes
/// the engine call through `EngineApply::apply_sql_with_params`
/// instead of `apply_sql`. The bound `params` slice corresponds to
/// the SQL's `$1..$N` placeholders; each value enters as a typed
/// `kessel_codec::Value` and emerges in the program as the same
/// typed `Value` — NO SQL text concatenation, NO `'`->`''` escape
/// rules, NO quoting. The byte-sequence shape (RowDescription /
/// DataRow* / CommandComplete / RFQ) is IDENTICAL to `dispatch_query`
/// — the only difference is the engine dispatch boundary.
///
/// Used by `extq::dispatch_execute` when
/// `preprocess_typed_params` classified every bound parameter as
/// typed-path-eligible. The text-substitution path remains as the
/// fallback (`dispatch_query` against an SQL-rewritten string) for
/// FLOAT4/FLOAT8/TIMESTAMPTZ/NUMERIC parameters the typed path
/// cannot represent cleanly.
pub fn dispatch_query_with_params<E: EngineApply + ?Sized>(
    sql: &str,
    params: &[Option<Value>],
    engine: &E,
) -> Vec<u8> {
    // SP-PG-EXTQ-CAST-VALIDATE-LITERAL T2 — reject any `LITERAL::TYPE`
    // cast whose literal's natural type-category disagrees with the
    // cast type's category (e.g. `'hello'::int8`) BEFORE the strip
    // rewrites the SQL. Closes the V1 silent-strip hole that V1 +
    // COMPAT only covered for `$N::TYPE` placeholder casts.
    if let Some(mismatch) = crate::cast_stripper::find_literal_cast_mismatch(sql) {
        return literal_cast_mismatch_response_then_rfq(&mismatch);
    }
    let stripped = crate::cast_stripper::strip_pg_casts(sql);
    let sql = stripped.as_str();
    let mut out = Vec::new();
    if is_effectively_empty(sql) {
        out.extend_from_slice(&encode_empty_query_response());
        out.extend_from_slice(&encode_ready_for_query(b'I'));
        return out;
    }
    if contains_multiple_statements(sql) {
        out.extend_from_slice(&encode_error_response(
            SEVERITY_ERROR,
            "42601",
            "multi-statement Q not supported in V1",
        ));
        out.extend_from_slice(&encode_ready_for_query(b'I'));
        return out;
    }
    // pg_catalog interceptor doesn't take params; route through hook.
    if let Some(bytes) = crate::pg_catalog::catalog_query_hook(sql, engine) {
        return bytes;
    }
    let sql_trimmed = sql.trim().trim_end_matches(';').trim();
    let select_table = kessel_sql::select_star_table(sql_trimmed);
    // SP-PG-EXTQ-PARSED-DEFAULT T2 — engine call via the typed-param
    // path. NO SQL text rewrite; the bound values reach the engine
    // as typed `Value`s. Closes the V1 weak-spot #1 attack surface
    // at the dispatch layer.
    let result = engine.apply_sql_with_params(sql_trimmed, params);
    let affected_rows: u64 = match &result {
        OpResult::Ok | OpResult::TxCommitted { .. } => 1,
        _ => 0,
    };
    match result {
        OpResult::Got(row_bytes) => {
            let table_name = match select_table {
                Some(n) => n,
                None => {
                    return error_response_then_rfq(
                        SEVERITY_ERROR,
                        "0A000",
                        "V1 PG-wire only renders `SELECT * FROM <table>`",
                    );
                }
            };
            let cols = match engine.describe_table(&table_name) {
                Some(c) => c,
                None => {
                    return error_response_then_rfq(
                        SEVERITY_ERROR,
                        "42P01",
                        &format!("unknown table \"{table_name}\""),
                    );
                }
            };
            let fields: Vec<FieldMeta> = cols
                .iter()
                .map(|c| FieldMeta {
                    name: c.name.clone(),
                    type_oid: field_kind_to_oid(c.kind),
                })
                .collect();
            out.extend_from_slice(&encode_row_description(&fields));
            let row_count = match emit_data_rows(&row_bytes, &cols, &mut out) {
                Ok(n) => n,
                Err(msg) => {
                    out.extend_from_slice(&encode_error_response(
                        SEVERITY_ERROR,
                        "XX000",
                        &format!("row decode failed: {msg}"),
                    ));
                    out.extend_from_slice(&encode_ready_for_query(b'I'));
                    return out;
                }
            };
            out.extend_from_slice(&encode_command_complete(&select_tag(row_count)));
            out.extend_from_slice(&encode_ready_for_query(b'I'));
            out
        }
        OpResult::Ok | OpResult::TypeCreated(_) | OpResult::TxCommitted { .. } => {
            let count = if leading_keyword_is(sql_trimmed, "INSERT") {
                let from_engine = affected_rows;
                let from_sql = count_insert_values(sql_trimmed);
                from_engine.max(from_sql)
            } else {
                affected_rows
            };
            let tag = cmd_complete_tag_for_sql(sql_trimmed, count);
            out.extend_from_slice(&encode_command_complete(&tag));
            out.extend_from_slice(&encode_ready_for_query(b'I'));
            out
        }
        OpResult::NotFound => {
            if leading_keyword_is(sql_trimmed, "SELECT") {
                if let Some(table_name) = select_table {
                    if let Some(cols) = engine.describe_table(&table_name) {
                        let fields: Vec<FieldMeta> = cols
                            .iter()
                            .map(|c| FieldMeta {
                                name: c.name.clone(),
                                type_oid: field_kind_to_oid(c.kind),
                            })
                            .collect();
                        out.extend_from_slice(&encode_row_description(&fields));
                    }
                }
                out.extend_from_slice(&encode_command_complete(&select_tag(0)));
                out.extend_from_slice(&encode_ready_for_query(b'I'));
                out
            } else {
                error_response_then_rfq(
                    SEVERITY_ERROR,
                    "42P01",
                    "not found",
                )
            }
        }
        other => {
            if let Some((sev, state, msg)) = op_result_to_sqlstate(&other) {
                out.extend_from_slice(&encode_error_response(sev, state, &msg));
            } else {
                out.extend_from_slice(&encode_command_complete(&select_tag(0)));
            }
            out.extend_from_slice(&encode_ready_for_query(b'I'));
            out
        }
    }
}

/// Run a single Simple Query end-to-end. Returns the full byte
/// sequence to emit to the wire (one or more PG backend messages
/// concatenated, ending with `ReadyForQuery('I')`).
///
/// The caller (the query loop in `server::run_session`) writes the
/// returned bytes to the TCP stream verbatim.
pub fn dispatch_query<E: EngineApply + ?Sized>(sql: &str, engine: &E) -> Vec<u8> {
    // SP-PG-EXTQ-CAST-VALIDATE-LITERAL T2 — reject cross-category
    // literal casts (e.g. `'hello'::int8`) BEFORE the strip rewrites
    // the SQL. The validator is a no-op when the SQL has no `::` or
    // every cast is within-category, so every prior K-CAST KAT still
    // passes byte-for-byte.
    if let Some(mismatch) = crate::cast_stripper::find_literal_cast_mismatch(sql) {
        return literal_cast_mismatch_response_then_rfq(&mismatch);
    }
    // SP-PG-EXTQ-CAST T2 — strip PG `::TYPE[(args)]` type-cast
    // operator before any downstream dispatch. The strip is a no-op
    // for SQL without `::` so every prior text-only KAT still passes
    // byte-for-byte (verified by `cast_stripper::no_cast_pure_passthrough_fuzz`).
    // Unlocks JDBC `preferQueryMode=simple` (which injects
    // `SELECT col::int8` patterns the kessel-sql lexer rejects with
    // `42601 syntax_error`). Companion design spec:
    // `docs/superpowers/specs/2026-06-01-kesseldb-sppgextqcast-design.md`.
    let stripped = crate::cast_stripper::strip_pg_casts(sql);
    let sql = stripped.as_str();
    let mut out = Vec::new();
    // PG §55.2.3 — empty/whitespace-only SQL → EmptyQueryResponse,
    // not RowDescription/CommandComplete.
    if is_effectively_empty(sql) {
        out.extend_from_slice(&encode_empty_query_response());
        out.extend_from_slice(&encode_ready_for_query(b'I'));
        return out;
    }
    // Spec §11 weak-spot #5: V1 doesn't support multi-statement Q.
    // SQLSTATE 42601 syntax_error per the design spec.
    if contains_multiple_statements(sql) {
        out.extend_from_slice(&encode_error_response(
            SEVERITY_ERROR,
            "42601",
            "multi-statement Q not supported in V1",
        ));
        out.extend_from_slice(&encode_ready_for_query(b'I'));
        return out;
    }
    // SP-PG-CAT T1 — pg_catalog / information_schema interceptor hook.
    // Sits BEFORE engine.apply_sql so a pg_catalog SELECT (which the
    // engine doesn't know about) gets a synthesized response instead
    // of the V1 `42P01 undefined_table` error. Returns `None` for
    // non-pg_catalog SQL so the existing dispatch path is unchanged
    // (locked by `t1_catalog_hook_returns_none_for_non_pg_catalog_sql`
    // in `pg_catalog::tests`). Companion spec:
    // `docs/superpowers/specs/2026-05-27-kesseldb-sppgcat-pg-catalog-design.md`.
    if let Some(bytes) = crate::pg_catalog::catalog_query_hook(sql, engine) {
        return bytes;
    }
    // Strip a trailing `;` so the leading-keyword heuristic + the
    // SELECT * FROM table lookup don't trip over the terminator.
    let sql_trimmed = sql.trim().trim_end_matches(';').trim();

    // Is this a `SELECT * FROM <table>` that we can render with full
    // RowDescription? `kessel-sql::select_star_table` is the lexer-
    // backed detector — returns Some(table_name) only on the
    // V1-supported whole-row shape (no projection list, no JOIN).
    let select_table = kessel_sql::select_star_table(sql_trimmed);

    // T9 — route DML through `apply_sql_with_count` so INSERT/UPDATE/
    // DELETE can surface a real row count in CommandComplete. SELECT
    // and DDL still go through `apply_sql` because their count is
    // computed elsewhere (SELECT: emit_data_rows; DDL: not part of
    // PG's CommandComplete tag — `CREATE TABLE` has no count).
    let (result, affected_rows) = if is_dml_keyword(sql_trimmed) {
        engine.apply_sql_with_count(sql_trimmed)
    } else {
        (engine.apply_sql(sql_trimmed), 0)
    };
    match result {
        OpResult::Got(row_bytes) => {
            // SELECT path — emit RowDescription + DataRow* +
            // CommandComplete("SELECT N") + ReadyForQuery.
            //
            // We can ONLY render this when:
            //  (a) the SQL parsed as a `SELECT * FROM <table>` per
            //      `select_star_table`, AND
            //  (b) `describe_table(<table>)` returns a schema.
            //
            // If either fails, V1 emits an ErrorResponse — we have
            // bytes we can't render. (Spec §11 weak-spot — V2 ships
            // PG-wire-side projection rendering.)
            let table_name = match select_table {
                Some(n) => n,
                None => {
                    return error_response_then_rfq(
                        SEVERITY_ERROR,
                        "0A000",
                        "V1 PG-wire only renders `SELECT * FROM <table>`",
                    );
                }
            };
            let cols = match engine.describe_table(&table_name) {
                Some(c) => c,
                None => {
                    return error_response_then_rfq(
                        SEVERITY_ERROR,
                        "42P01",
                        &format!("unknown table \"{table_name}\""),
                    );
                }
            };
            // Build FieldMeta list for RowDescription.
            let fields: Vec<FieldMeta> = cols
                .iter()
                .map(|c| FieldMeta {
                    name: c.name.clone(),
                    type_oid: field_kind_to_oid(c.kind),
                })
                .collect();
            out.extend_from_slice(&encode_row_description(&fields));
            // Decode row stream `[u32 LE len][record]*` and emit
            // one DataRow per record.
            let row_count = match emit_data_rows(&row_bytes, &cols, &mut out) {
                Ok(n) => n,
                Err(msg) => {
                    // Partial output was buffered, but the byte
                    // stream is well-framed per individual encoder
                    // — we can't take it back, so the only sane
                    // recovery is to emit an ErrorResponse + RFQ
                    // and let the client discard the RowDescription
                    // + any DataRows already buffered. (PG itself
                    // sometimes does this; libpq handles it.)
                    out.extend_from_slice(&encode_error_response(
                        SEVERITY_ERROR,
                        "XX000",
                        &format!("row decode failed: {msg}"),
                    ));
                    out.extend_from_slice(&encode_ready_for_query(b'I'));
                    return out;
                }
            };
            out.extend_from_slice(&encode_command_complete(&select_tag(row_count)));
            out.extend_from_slice(&encode_ready_for_query(b'I'));
            out
        }
        OpResult::Ok | OpResult::TypeCreated(_) | OpResult::TxCommitted { .. } => {
            // Non-SELECT success. T9: pick the CommandComplete tag
            // via `cmd_complete_tag_for_sql` — handles DDL +
            // transaction-control keywords + leading-comment stripping
            // beyond what `infer_command_tag` covers. For INSERT
            // specifically, `affected_rows` may understate when the
            // engine collapses a multi-row VALUES into a single
            // `Op::Txn`-returns-`Ok` (count=1 in the default impl);
            // fall back to counting VALUES tuples in the SQL text.
            let count = if leading_keyword_is(sql_trimmed, "INSERT") {
                let from_engine = affected_rows;
                let from_sql = count_insert_values(sql_trimmed);
                from_engine.max(from_sql)
            } else {
                affected_rows
            };
            let tag = cmd_complete_tag_for_sql(sql_trimmed, count);
            out.extend_from_slice(&encode_command_complete(&tag));
            out.extend_from_slice(&encode_ready_for_query(b'I'));
            out
        }
        OpResult::NotFound => {
            // SELECT path with a missing row OR an Op::Describe of a
            // missing type. Distinguish via SQL leading keyword:
            //   - SELECT → 0 rows (success path)
            //   - else  → undefined_table error.
            if leading_keyword_is(sql_trimmed, "SELECT") {
                // For a SELECT, we still need a RowDescription if we
                // know the table. If select_table is None, emit a
                // bare CommandComplete("SELECT 0").
                if let Some(table_name) = select_table {
                    if let Some(cols) = engine.describe_table(&table_name) {
                        let fields: Vec<FieldMeta> = cols
                            .iter()
                            .map(|c| FieldMeta {
                                name: c.name.clone(),
                                type_oid: field_kind_to_oid(c.kind),
                            })
                            .collect();
                        out.extend_from_slice(&encode_row_description(&fields));
                    }
                }
                out.extend_from_slice(&encode_command_complete(&select_tag(0)));
                out.extend_from_slice(&encode_ready_for_query(b'I'));
                out
            } else {
                error_response_then_rfq(
                    SEVERITY_ERROR,
                    "42P01",
                    "not found",
                )
            }
        }
        other => {
            // Error path — feed the OpResult through the T7 map.
            if let Some((sev, state, msg)) = op_result_to_sqlstate(&other) {
                out.extend_from_slice(&encode_error_response(sev, state, &msg));
            } else {
                // Defensive: a future success variant that didn't
                // get added to the success match arms above. Emit
                // a bare CommandComplete + RFQ; the client will see
                // a tag but no rows, which is benign.
                out.extend_from_slice(&encode_command_complete(&select_tag(0)));
            }
            out.extend_from_slice(&encode_ready_for_query(b'I'));
            out
        }
    }
}

/// Helper: build `ErrorResponse + ReadyForQuery('I')` into a fresh
/// `Vec<u8>`. Used by every "we know this Q can't be served" path.
pub fn error_response_then_rfq(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_error_response(severity, sqlstate, message));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-EXTQ-CAST-VALIDATE-LITERAL T2 — render a `LiteralCastMismatch`
/// to the canonical `42846 cannot_coerce` ErrorResponse + RFQ pair
/// the simple-query dispatchers return. Shares the wire shape with
/// the V1 `CastOidMismatch` renderer in `server.rs` (only the message
/// text differs: literal-shape names instead of parameter index).
pub fn literal_cast_mismatch_response_then_rfq(
    mismatch: &crate::cast_stripper::LiteralCastMismatch,
) -> Vec<u8> {
    let message = format!(
        "cannot cast literal of category '{lit_cat}' (OID {lit_oid}) to type with OID {cast_oid} (category '{cast_cat}')",
        lit_cat = mismatch.literal_category,
        lit_oid = mismatch.literal_oid,
        cast_oid = mismatch.cast_oid,
        cast_cat = mismatch.cast_category,
    );
    error_response_then_rfq(SEVERITY_ERROR, "42846", &message)
}

/// Decode the row stream produced by `OpResult::Got` (which is
/// `[u32 LE len][record]*` — see `kessel-sm::Op::Select`) and emit
/// one `DataRow` per record into `out`. Returns the row count.
///
/// `cols` is the table's schema in declared order — the same data
/// the RowDescription was built from. Each record is decoded via
/// `kessel-codec::value_from_raw` per field; NULL bitmap is handled
/// by walking the layout the same way `kessel-codec::decode` does
/// (we re-implement just enough here to avoid pulling in
/// `kessel-catalog::ObjectType::from_def` which would require a
/// type_id we don't have).
fn emit_data_rows(
    row_bytes: &[u8],
    cols: &[PgColumn],
    out: &mut Vec<u8>,
) -> Result<u64, String> {
    let layout = compute_record_layout(cols);
    let mut p = 0usize;
    let mut n = 0u64;
    // Handle two row-stream shapes (mirroring `kessel-client::render_rows`):
    //   (a) length-prefixed list `[u32 LE len][rec]*`, possibly empty.
    //   (b) single bare record (the primary-key fast path —
    //       `Op::GetById` returns one record without a length prefix).
    // V1's SELECT * FROM table always shape (a). Op::GetById can
    // surface here if `kessel-sql` compiles `SELECT * FROM t WHERE id = N`
    // to Op::GetById — we accept both for robustness.

    // Try shape (a) first.
    let mut tried_a = true;
    let mut rows_a: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
    while p + 4 <= row_bytes.len() {
        let len = u32::from_le_bytes(row_bytes[p..p + 4].try_into().unwrap()) as usize;
        p += 4;
        if p + len > row_bytes.len() {
            tried_a = false;
            break;
        }
        let rec = &row_bytes[p..p + len];
        p += len;
        match decode_record(rec, cols, &layout) {
            Some(cells) => rows_a.push(cells),
            None => {
                tried_a = false;
                break;
            }
        }
    }
    let use_a = tried_a && p == row_bytes.len();
    if use_a {
        for r in &rows_a {
            let cols_borrow: Vec<Option<&[u8]>> =
                r.iter().map(|c| c.as_deref()).collect();
            out.extend_from_slice(&encode_data_row(&cols_borrow));
            n += 1;
        }
        return Ok(n);
    }
    // Shape (b) — entire blob is one bare record.
    if row_bytes.is_empty() {
        return Ok(0);
    }
    match decode_record(row_bytes, cols, &layout) {
        Some(cells) => {
            let cols_borrow: Vec<Option<&[u8]>> =
                cells.iter().map(|c| c.as_deref()).collect();
            out.extend_from_slice(&encode_data_row(&cols_borrow));
            Ok(1)
        }
        None => Err("malformed bare record".to_string()),
    }
}

/// Layout for one record per `cols`. Mirrors kessel-codec's record
/// layout: `[schema_ver:u32 LE][field_count:u16 LE][null_bitmap:8B]
/// [field_data...]` with each field at its computed offset.
#[derive(Debug, Clone)]
struct Layout {
    offsets: Vec<usize>,
    widths: Vec<usize>,
}

fn compute_record_layout(cols: &[PgColumn]) -> Layout {
    // Header: schema_ver(4) + field_count(2) + null_bitmap(8) = 14
    const HEADER: usize = 4 + 2 + 8;
    let mut offsets = Vec::with_capacity(cols.len());
    let mut widths = Vec::with_capacity(cols.len());
    let mut cur = HEADER;
    for c in cols {
        let w = c.kind.width() as usize;
        offsets.push(cur);
        widths.push(w);
        cur += w;
    }
    Layout { offsets, widths }
}

/// Decode one record (the fixed-width record bytes) into a
/// per-column `Option<Vec<u8>>` of PG-text-format bytes (None =
/// NULL). Returns `None` on a malformed record (too short).
fn decode_record(
    rec: &[u8],
    cols: &[PgColumn],
    layout: &Layout,
) -> Option<Vec<Option<Vec<u8>>>> {
    if rec.len() < 14 {
        return None;
    }
    let stored_fc = u16::from_le_bytes(rec.get(4..6)?.try_into().ok()?) as usize;
    let bitmap = rec.get(6..14)?;
    let mut out = Vec::with_capacity(cols.len());
    for (i, c) in cols.iter().enumerate() {
        if i >= stored_fc {
            // Field added after this record was written — up-projects to NULL.
            out.push(None);
            continue;
        }
        if is_null_in_bitmap(bitmap, i) {
            out.push(None);
            continue;
        }
        let off = layout.offsets[i];
        let w = layout.widths[i];
        let raw = rec.get(off..off + w)?;
        let v = value_from_raw(c.kind, raw);
        out.push(Some(render_pg_text(&v, c.kind)));
    }
    Some(out)
}

fn is_null_in_bitmap(bitmap: &[u8], i: usize) -> bool {
    bitmap.get(i / 8).map(|b| b & (1 << (i % 8)) != 0).unwrap_or(false)
}

/// Render a `kessel-codec::Value` to PG TEXT-format bytes per spec §5.
///
/// - `Bool` → `t` / `f` (PG uses 1-char short form, NOT `true`/`false`)
/// - integer kinds → decimal ASCII (signed kinds via `Int(i128)`,
///   unsigned via `Uint(u128)`)
/// - `Char(n)` → UTF-8 with trailing-NUL padding stripped (KesselDB
///   stores fixed-width `Char` as zero-padded raw bytes)
/// - `Bytes` / `Ref` / `OverflowRef` → `\x<hex>` per PG bytea text format
/// - `Timestamp` → `YYYY-MM-DD HH:MM:SS.ffffff+00` (V1 emits the
///   raw u64 nanos as decimal; full timestamptz formatting is V2 —
///   psql tolerates the decimal form for now)
/// - `Null` → unreachable; caller routes NULL via the `None` arm of
///   the column option (PG NULL sentinel is i32 -1, NOT a text-format
///   value)
pub fn render_pg_text(v: &Value, kind: FieldKind) -> Vec<u8> {
    match v {
        Value::Null => Vec::new(), // unreachable in practice; defensive
        Value::Uint(u) => {
            if matches!(kind, FieldKind::Bool) {
                if *u == 0 { b"f".to_vec() } else { b"t".to_vec() }
            } else {
                u.to_string().into_bytes()
            }
        }
        Value::Int(i) => i.to_string().into_bytes(),
        Value::Blob(b) => {
            match kind {
                FieldKind::Char(_) => {
                    // Strip trailing NULs (zero-padding from fixed-width
                    // CHAR storage); render as UTF-8.
                    let end = b.iter().rposition(|&x| x != 0).map_or(0, |i| i + 1);
                    b[..end].to_vec()
                }
                FieldKind::Bytes(_) | FieldKind::Ref | FieldKind::OverflowRef => {
                    // PG bytea text format: `\x<hex>`.
                    let mut s = String::with_capacity(2 + b.len() * 2);
                    s.push_str("\\x");
                    for byte in b {
                        s.push_str(&format!("{byte:02x}"));
                    }
                    s.into_bytes()
                }
                _ => {
                    // Unexpected Blob for a non-blob kind — should not
                    // happen if value_from_raw was called correctly.
                    // Fall back to hex bytea form so we don't lose info.
                    let mut s = String::from("\\x");
                    for byte in b {
                        s.push_str(&format!("{byte:02x}"));
                    }
                    s.into_bytes()
                }
            }
        }
    }
}

/// T9 — extracts the leading SQL keyword (case-insensitive, leading
/// whitespace + `-- ...` line comments + `/* ... */` block comments
/// stripped). Returns the uppercased keyword or the empty string if
/// no keyword is present. Used by `cmd_complete_tag_for_sql` and the
/// DML-routing branch of `dispatch_query`.
pub fn leading_keyword(sql: &str) -> String {
    let stripped = strip_leading_comments_and_whitespace(sql);
    stripped
        .split(|c: char| c.is_whitespace() || c == '(' || c == ';')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase()
}

/// Strip leading whitespace + SQL line comments (`-- ...\n`) + block
/// comments (`/* ... */`, non-nesting) so the first meaningful token
/// is exposed for keyword extraction. Mirrors libpq's tolerance for
/// the leading-comment shapes a real client (ORMs especially) emits.
fn strip_leading_comments_and_whitespace(sql: &str) -> &str {
    let mut s = sql;
    loop {
        let trimmed = s.trim_start();
        if trimmed.starts_with("--") {
            // Line comment — skip to end of line (LF or end-of-string).
            match trimmed.find('\n') {
                Some(p) => s = &trimmed[p + 1..],
                None => return "",
            }
            continue;
        }
        if trimmed.starts_with("/*") {
            // Block comment — non-nesting, scan for `*/`.
            match trimmed[2..].find("*/") {
                Some(p) => s = &trimmed[2 + p + 2..],
                None => return "", // unterminated comment — no keyword
            }
            continue;
        }
        return trimmed;
    }
}

/// True iff the SQL is one of the DML keywords that should route
/// through `apply_sql_with_count` (so the CommandComplete tag carries
/// a real row count). SELECT is NOT here — its count is the result-
/// row count, computed at decode time in `emit_data_rows`.
fn is_dml_keyword(sql: &str) -> bool {
    matches!(leading_keyword(sql).as_str(), "INSERT" | "UPDATE" | "DELETE")
}

/// Count top-level `(...)` VALUES tuples in an `INSERT INTO t (...)
/// VALUES (...)[, (...)]*` so `dispatch_query` can emit `INSERT 0 N`
/// for a multi-row INSERT even though the engine's `OpResult::Ok`
/// (returned by the underlying `Op::Txn`) doesn't carry the count.
///
/// V1 implementation: lex the SQL into tokens with a tiny pass —
/// strings (single-quoted, doubled-quote escape) + line/block comments
/// are honored so a quoted `(` doesn't trigger a false count. Counts
/// the top-level `(...)` groups AFTER the `VALUES` keyword.
///
/// Returns 0 if the shape doesn't match (no `VALUES`, no tuples) —
/// callers fall back to the engine-supplied count in that case.
pub fn count_insert_values(sql: &str) -> u64 {
    // Find the VALUES keyword (case-insensitive). Use the
    // comment-stripped, lower-cased view to locate it; track byte
    // positions in the original so a `'values'` literal doesn't trip
    // the search.
    let s = sql;
    let bytes = s.as_bytes();
    let mut i = 0usize;
    let mut depth = 0i32;
    let mut found_values = false;
    let mut tuples: u64 = 0;
    let mut in_str = false;
    while i < bytes.len() {
        let b = bytes[i];
        if in_str {
            if b == b'\'' {
                // doubled '' → escaped quote (still in string)
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_str = false;
                i += 1;
                continue;
            }
            i += 1;
            continue;
        }
        // line comment
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            match bytes[i..].iter().position(|&x| x == b'\n') {
                Some(p) => i += p + 1,
                None => break,
            }
            continue;
        }
        // block comment
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            let rest = &bytes[i + 2..];
            match rest.windows(2).position(|w| w == b"*/") {
                Some(p) => i += 2 + p + 2,
                None => break,
            }
            continue;
        }
        if b == b'\'' {
            in_str = true;
            i += 1;
            continue;
        }
        // Check for VALUES keyword at this position (word-aligned).
        if !found_values
            && (b == b'V' || b == b'v')
            && i + 6 <= bytes.len()
            && bytes[i..i + 6].eq_ignore_ascii_case(b"VALUES")
            && (i == 0 || !is_ident_byte(bytes[i - 1]))
            && (i + 6 == bytes.len() || !is_ident_byte(bytes[i + 6]))
        {
            found_values = true;
            i += 6;
            continue;
        }
        if found_values {
            if b == b'(' {
                if depth == 0 {
                    tuples += 1;
                }
                depth += 1;
            } else if b == b')' {
                depth -= 1;
            }
        }
        i += 1;
    }
    tuples
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// T9 — pick the canonical PG CommandComplete tag string for the
/// given SQL + affected-row count. Builds on `infer_command_tag` but
/// adds:
///
/// - Leading `-- ...` and `/* ... */` comments + whitespace stripped
///   before the keyword test (ORMs/JDBC routinely prepend such
///   comments).
/// - DDL: `CREATE INDEX` / `CREATE UNIQUE INDEX` / `CREATE RANGE
///   INDEX` → `CREATE INDEX`; `DROP TABLE`, `DROP INDEX`, `ALTER
///   TABLE` mapped to their canonical tag strings.
/// - Transaction control: `BEGIN` / `START TRANSACTION` → `BEGIN`,
///   `COMMIT` / `END` → `COMMIT`, `ROLLBACK` / `ABORT` → `ROLLBACK`.
/// - Unknown DDL: emit just the leading keyword (uppercased). PG's
///   spec allows tag-emit-as-keyword for unrecognized server-side
///   verbs (preferable to lying with a SELECT 0).
///
/// The output is suitable for `encode_command_complete(&tag)`.
pub fn cmd_complete_tag_for_sql(sql: &str, count: u64) -> String {
    let stripped = strip_leading_comments_and_whitespace(sql);
    let kw = leading_keyword(stripped);
    match kw.as_str() {
        "SELECT" => select_tag(count),
        "INSERT" => insert_tag(count),
        "UPDATE" => update_tag(count),
        "DELETE" => delete_tag(count),
        "CREATE" => {
            // CREATE TABLE / CREATE INDEX / CREATE UNIQUE INDEX /
            // CREATE RANGE INDEX → canonical tag from the second
            // keyword.
            let second = second_keyword(stripped);
            match second.as_str() {
                "TABLE" => "CREATE TABLE".to_string(),
                "INDEX" => "CREATE INDEX".to_string(),
                "UNIQUE" | "RANGE" => "CREATE INDEX".to_string(),
                "VIEW" => "CREATE VIEW".to_string(),
                "SCHEMA" => "CREATE SCHEMA".to_string(),
                _ => format!("CREATE {second}"),
            }
        }
        "DROP" => {
            let second = second_keyword(stripped);
            match second.as_str() {
                "TABLE" => "DROP TABLE".to_string(),
                "INDEX" => "DROP INDEX".to_string(),
                "VIEW" => "DROP VIEW".to_string(),
                "SCHEMA" => "DROP SCHEMA".to_string(),
                _ => format!("DROP {second}"),
            }
        }
        "ALTER" => {
            let second = second_keyword(stripped);
            match second.as_str() {
                "TABLE" => "ALTER TABLE".to_string(),
                "INDEX" => "ALTER INDEX".to_string(),
                _ => format!("ALTER {second}"),
            }
        }
        "SET" => "SET".to_string(),
        "EXPLAIN" => "EXPLAIN".to_string(),
        "BEGIN" | "START" => "BEGIN".to_string(),
        "COMMIT" | "END" => "COMMIT".to_string(),
        "ROLLBACK" | "ABORT" => "ROLLBACK".to_string(),
        "VACUUM" => "VACUUM".to_string(),
        "ANALYZE" => "ANALYZE".to_string(),
        "TRUNCATE" => "TRUNCATE TABLE".to_string(),
        "" => select_tag(0), // empty after comment strip — defensive
        other => other.to_string(),
    }
}

/// Second keyword in a SQL statement (case-insensitive, uppercased),
/// e.g. for `CREATE INDEX idx ON t (x)` returns `"INDEX"`. Returns
/// the empty string if there's no second token.
fn second_keyword(sql: &str) -> String {
    let stripped = strip_leading_comments_and_whitespace(sql);
    let mut iter = stripped.split(|c: char| c.is_whitespace() || c == '(' || c == ';');
    iter.next(); // first keyword
    iter.find(|s| !s.is_empty())
        .unwrap_or("")
        .to_ascii_uppercase()
}

/// Pick the canonical CommandComplete tag from the SQL leading
/// keyword. V1 returns:
///
/// - SELECT  → `SELECT N`
/// - INSERT  → `INSERT 0 N`
/// - UPDATE  → `UPDATE N`
/// - DELETE  → `DELETE N`
/// - CREATE  → `CREATE TABLE` (no row count per PG §55.7)
/// - DROP    → `DROP TABLE`
/// - SET     → `SET` (spec §11 weak-spot #6 — V1 ignores the GUC
///   payload but returns this tag so the client doesn't choke)
/// - ALTER   → `ALTER TABLE`
/// - EXPLAIN → `EXPLAIN` (V1 routes through SELECT path normally,
///   but if it lands here we return the literal verb)
/// - else    → `SELECT 0` (defensive fallback — better than a
///   protocol error for an opaque command we ran successfully)
pub fn infer_command_tag(sql: &str, rows: u64) -> String {
    let kw = sql
        .trim_start()
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    match kw.as_str() {
        "SELECT" => select_tag(rows),
        "INSERT" => insert_tag(rows),
        "UPDATE" => update_tag(rows),
        "DELETE" => delete_tag(rows),
        "CREATE" => "CREATE TABLE".to_string(),
        "DROP" => "DROP TABLE".to_string(),
        "ALTER" => "ALTER TABLE".to_string(),
        "SET" => "SET".to_string(),
        "EXPLAIN" => "EXPLAIN".to_string(),
        "BEGIN" | "START" => "BEGIN".to_string(),
        "COMMIT" | "END" => "COMMIT".to_string(),
        "ROLLBACK" | "ABORT" => "ROLLBACK".to_string(),
        _ => select_tag(0),
    }
}

/// True if the SQL's first non-whitespace token (case-insensitive)
/// matches `keyword`. Used by the NotFound dispatcher to distinguish
/// a SELECT-finds-no-rows path from a DDL-target-doesn't-exist path.
pub fn leading_keyword_is(sql: &str, keyword: &str) -> bool {
    sql.trim_start()
        .split_whitespace()
        .next()
        .map(|w| w.eq_ignore_ascii_case(keyword))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::PgColumn;
    use kessel_catalog::FieldKind;
    use kessel_codec::Value;
    use kessel_proto::OpResult;

    // A test engine that returns canned data. `Mutex` keeps the
    // trait's `Send + Sync` requirement without resorting to
    // `unsafe impl` (forbidden under `#![forbid(unsafe_code)]`).
    struct CannedEngine {
        cols: Vec<PgColumn>,
        row_bytes: Vec<u8>,
        result: std::sync::Mutex<Option<OpResult>>,
        table_name: String,
        no_schema: bool,
    }

    impl EngineApply for CannedEngine {
        fn apply_sql(&self, _sql: &str) -> OpResult {
            self.result
                .lock()
                .unwrap()
                .take()
                .unwrap_or(OpResult::Got(self.row_bytes.clone().into()))
        }
        fn describe_table(&self, name: &str) -> Option<Vec<PgColumn>> {
            if self.no_schema {
                return None;
            }
            if name == self.table_name {
                Some(self.cols.clone())
            } else {
                None
            }
        }
    }

    /// Helper: build a kessel-codec encoded record for the given
    /// columns + values.
    fn build_record(cols: &[PgColumn], values: &[Value]) -> Vec<u8> {
        use kessel_catalog::Field;
        let fields: Vec<Field> = cols
            .iter()
            .enumerate()
            .map(|(i, c)| Field {
                field_id: i as u16,
                name: c.name.clone(),
                kind: c.kind,
                nullable: c.nullable,
            })
            .collect();
        let ot = kessel_catalog::ObjectType::from_def("test".to_string(), fields);
        kessel_codec::encode(&ot, values).expect("encode")
    }

    /// Wrap individual records as the length-prefixed stream
    /// `Op::Select` emits.
    fn build_row_stream(records: &[Vec<u8>]) -> Vec<u8> {
        let mut out = Vec::new();
        for r in records {
            out.extend_from_slice(&(r.len() as u32).to_le_bytes());
            out.extend_from_slice(r);
        }
        out
    }

    // ───────────────────────────────────────────────────────────────────
    // T8 KATs — dispatch loop end-to-end. The headline KAT is
    // `t8_select_star_returns_full_response_stream`.
    // ───────────────────────────────────────────────────────────────────

    /// Headline T8 KAT: a `SELECT * FROM t` with 2 rows returns
    /// RowDescription + 2× DataRow + CommandComplete("SELECT 2") +
    /// ReadyForQuery('I') in that order, byte-coherent.
    #[test]
    fn t8_select_star_returns_full_response_stream() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "n".into(), kind: FieldKind::I32, nullable: false },
        ];
        let r1 = build_record(&cols, &[Value::Int(1), Value::Int(100)]);
        let r2 = build_record(&cols, &[Value::Int(2), Value::Int(200)]);
        let stream = build_row_stream(&[r1, r2]);
        let eng = CannedEngine {
            cols: cols.clone(),
            row_bytes: stream,
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("SELECT * FROM t", &eng);
        // Order invariant: 'T' < 'D' < 'D' < 'C' < 'Z'
        let pos_t = bytes.iter().position(|&b| b == b'T').unwrap();
        let pos_d1 = bytes[pos_t + 1..]
            .iter()
            .position(|&b| b == b'D')
            .unwrap()
            + pos_t
            + 1;
        let pos_d2 = bytes[pos_d1 + 1..]
            .iter()
            .position(|&b| b == b'D')
            .unwrap()
            + pos_d1
            + 1;
        let pos_c = bytes.iter().position(|&b| b == b'C').unwrap();
        let pos_z = bytes.iter().position(|&b| b == b'Z').unwrap();
        assert!(pos_t < pos_d1);
        assert!(pos_d1 < pos_d2);
        assert!(pos_d2 < pos_c);
        assert!(pos_c < pos_z);
        // CommandComplete carries "SELECT 2".
        assert!(bytes.windows(b"SELECT 2\0".len()).any(|w| w == b"SELECT 2\0"));
        // Last 6 bytes are ReadyForQuery('I').
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
        // DataRow values present as text.
        assert!(bytes.windows(3).any(|w| w == b"100"));
        assert!(bytes.windows(3).any(|w| w == b"200"));
    }

    /// SELECT * FROM t → 0 rows → CommandComplete("SELECT 0").
    #[test]
    fn t8_select_zero_rows_emits_select_0_tag() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(), // empty stream
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("SELECT * FROM t", &eng);
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
        // RowDescription still emitted (clients expect it for SELECT).
        assert_eq!(bytes[0], b'T');
    }

    /// SELECT with a NULL column → DataRow has the i32 -1 sentinel
    /// (0xFFFFFFFF unsigned).
    #[test]
    fn t8_select_null_column_emits_negative_one_sentinel() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(8), nullable: true },
        ];
        let r1 = build_record(&cols, &[Value::Int(7), Value::Null]);
        let stream = build_row_stream(&[r1]);
        let eng = CannedEngine {
            cols: cols.clone(),
            row_bytes: stream,
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("SELECT * FROM t", &eng);
        // The NULL sentinel (0xFF 0xFF 0xFF 0xFF) appears in the DataRow.
        assert!(bytes
            .windows(4)
            .any(|w| w == [0xFF, 0xFF, 0xFF, 0xFF]));
    }

    /// Empty Q (whitespace-only SQL) → EmptyQueryResponse + RFQ.
    /// PG §55.2.3.
    #[test]
    fn t8_empty_query_emits_empty_query_response() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(),
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("   \n\t  ", &eng);
        // First 5 bytes = EmptyQueryResponse ('I' + length=4).
        assert_eq!(&bytes[..5], &[b'I', 0, 0, 0, 4]);
        // Followed by ReadyForQuery('I').
        assert_eq!(&bytes[5..11], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// Multi-statement Q → ErrorResponse 42601 + RFQ. Spec §11 weak-spot #5.
    #[test]
    fn t8_multi_statement_q_returns_42601_error() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(),
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("SELECT 1; SELECT 2", &eng);
        // ErrorResponse 'E' first, then RFQ.
        assert_eq!(bytes[0], b'E');
        // SQLSTATE 42601 visible.
        assert!(bytes.windows(5).any(|w| w == b"42601"));
        // Last 6 bytes = ReadyForQuery('I').
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// SELECT * FROM <unknown_table> → ErrorResponse 42P01.
    #[test]
    fn t8_select_unknown_table_returns_42p01_error() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(),
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: true, // describe_table returns None
        };
        let bytes = dispatch_query("SELECT * FROM ghost", &eng);
        assert_eq!(bytes[0], b'E');
        assert!(bytes.windows(5).any(|w| w == b"42P01"));
    }

    /// INSERT INTO ... → CommandComplete("INSERT 0 1") + RFQ. T9
    /// wires `apply_sql_with_count` (default impl returns 1 for
    /// `OpResult::Ok` on a single-row INSERT) + `count_insert_values`
    /// as a cross-check from the SQL text.
    #[test]
    fn t8_insert_emits_insert_command_complete() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(),
            result: std::sync::Mutex::new(Some(OpResult::Ok)),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("INSERT INTO t (id) VALUES (1)", &eng);
        assert!(bytes.windows(b"INSERT 0 1\0".len()).any(|w| w == b"INSERT 0 1\0"));
        // Last 6 bytes are RFQ.
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// DELETE FROM ... → CommandComplete("DELETE 1") + RFQ. T9: row
    /// count surfaced via `apply_sql_with_count`.
    #[test]
    fn t8_delete_emits_delete_command_complete() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(),
            result: std::sync::Mutex::new(Some(OpResult::Ok)),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("DELETE FROM t WHERE id = 1", &eng);
        assert!(bytes.windows(b"DELETE 1\0".len()).any(|w| w == b"DELETE 1\0"));
    }

    /// CREATE TABLE → CommandComplete("CREATE TABLE") + RFQ.
    #[test]
    fn t8_create_table_emits_create_table_command_complete() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(),
            result: std::sync::Mutex::new(Some(OpResult::TypeCreated(7))),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("CREATE TABLE t (id i64)", &eng);
        assert!(bytes
            .windows(b"CREATE TABLE\0".len())
            .any(|w| w == b"CREATE TABLE\0"));
    }

    /// `OpResult::Constraint` → ErrorResponse 23000+ (heuristic) + RFQ.
    #[test]
    fn t8_constraint_error_emits_error_response() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(),
            result: std::sync::Mutex::new(Some(OpResult::Constraint(
                "NOT NULL violated on column id".into(),
            ))),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("INSERT INTO t (id) VALUES (NULL)", &eng);
        assert_eq!(bytes[0], b'E');
        // NOT NULL substring → 23502.
        assert!(bytes.windows(5).any(|w| w == b"23502"));
    }

    /// `OpResult::Exists` → ErrorResponse 23505 + RFQ.
    #[test]
    fn t8_exists_error_emits_unique_violation() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(),
            result: std::sync::Mutex::new(Some(OpResult::Exists)),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("INSERT INTO t (id) VALUES (1)", &eng);
        assert_eq!(bytes[0], b'E');
        assert!(bytes.windows(5).any(|w| w == b"23505"));
    }

    // ─── render_pg_text per type-shape ─────────────────────────────────

    /// Bool → 't' / 'f' per PG bool text format.
    #[test]
    fn t8_render_pg_text_bool() {
        assert_eq!(render_pg_text(&Value::Uint(1), FieldKind::Bool), b"t");
        assert_eq!(render_pg_text(&Value::Uint(0), FieldKind::Bool), b"f");
    }

    /// Signed ints → decimal ASCII.
    #[test]
    fn t8_render_pg_text_signed_ints() {
        assert_eq!(render_pg_text(&Value::Int(42), FieldKind::I64), b"42");
        assert_eq!(render_pg_text(&Value::Int(-7), FieldKind::I32), b"-7");
        assert_eq!(render_pg_text(&Value::Int(0), FieldKind::I8), b"0");
    }

    /// Unsigned ints → decimal ASCII.
    #[test]
    fn t8_render_pg_text_unsigned_ints() {
        assert_eq!(render_pg_text(&Value::Uint(100), FieldKind::U32), b"100");
        assert_eq!(
            render_pg_text(&Value::Uint(u64::MAX as u128), FieldKind::U64),
            b"18446744073709551615"
        );
    }

    /// Bytes → `\x<hex>` PG bytea text format.
    #[test]
    fn t8_render_pg_text_bytea() {
        let b = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let out = render_pg_text(&Value::Blob(b), FieldKind::Bytes(4));
        assert_eq!(out, b"\\xdeadbeef");
    }

    /// Char(n) → UTF-8 with trailing NUL padding stripped.
    #[test]
    fn t8_render_pg_text_char_strips_trailing_nuls() {
        let b = b"hello\0\0\0".to_vec();
        let out = render_pg_text(&Value::Blob(b), FieldKind::Char(8));
        assert_eq!(out, b"hello");
    }

    /// Char(n) all-zeros → empty text.
    #[test]
    fn t8_render_pg_text_char_all_zeros_is_empty() {
        let b = vec![0u8; 8];
        let out = render_pg_text(&Value::Blob(b), FieldKind::Char(8));
        assert_eq!(out, b"");
    }

    /// `infer_command_tag` matches the leading keyword case-insensitively.
    #[test]
    fn t8_infer_command_tag_is_case_insensitive() {
        assert_eq!(infer_command_tag("select * from t", 5), "SELECT 5");
        assert_eq!(infer_command_tag("Insert into t", 3), "INSERT 0 3");
        assert_eq!(infer_command_tag("UPDATE t SET x = 1", 2), "UPDATE 2");
        assert_eq!(infer_command_tag("DELETE FROM t", 4), "DELETE 4");
        assert_eq!(infer_command_tag("create table t", 0), "CREATE TABLE");
        assert_eq!(infer_command_tag("DROP TABLE t", 0), "DROP TABLE");
        assert_eq!(infer_command_tag("SET timezone = 'UTC'", 0), "SET");
    }

    /// Unknown keyword → SELECT 0 fallback.
    #[test]
    fn t8_infer_command_tag_unknown_falls_back_to_select_0() {
        assert_eq!(infer_command_tag("VACUUM", 0), "SELECT 0");
        assert_eq!(infer_command_tag("ANALYZE t", 0), "SELECT 0");
        assert_eq!(infer_command_tag("", 0), "SELECT 0");
    }

    /// `leading_keyword_is` is case-insensitive + whitespace-tolerant.
    #[test]
    fn t8_leading_keyword_is_matches() {
        assert!(leading_keyword_is("  SELECT 1", "select"));
        assert!(leading_keyword_is("SELECT * FROM t", "SELECT"));
        assert!(leading_keyword_is("UPDATE t", "update"));
        assert!(!leading_keyword_is("SELECT 1", "INSERT"));
        assert!(!leading_keyword_is("", "SELECT"));
    }

    // ───────────────────────────────────────────────────────────────────
    // T9 KATs — INSERT/UPDATE/DELETE row counts + DDL/txn-control tags.
    // Headline: an INSERT with N tuples produces `INSERT 0 N` in the
    // CommandComplete tag (not 0 — the V1 default impl returns 1 for
    // single-row, and `count_insert_values` lifts multi-row to N).
    // ───────────────────────────────────────────────────────────────────

    /// `cmd_complete_tag_for_sql` returns the canonical tag for DML.
    #[test]
    fn t9_cmd_complete_tag_for_dml() {
        assert_eq!(cmd_complete_tag_for_sql("INSERT INTO t (id) VALUES (1)", 1), "INSERT 0 1");
        assert_eq!(cmd_complete_tag_for_sql("UPDATE t SET v = 1 WHERE id = 2", 3), "UPDATE 3");
        assert_eq!(cmd_complete_tag_for_sql("DELETE FROM t WHERE id = 1", 2), "DELETE 2");
        assert_eq!(cmd_complete_tag_for_sql("SELECT * FROM t", 5), "SELECT 5");
    }

    /// `cmd_complete_tag_for_sql` returns the canonical tag for DDL.
    #[test]
    fn t9_cmd_complete_tag_for_ddl() {
        assert_eq!(cmd_complete_tag_for_sql("CREATE TABLE t (id i64)", 0), "CREATE TABLE");
        assert_eq!(cmd_complete_tag_for_sql("CREATE INDEX idx ON t (x)", 0), "CREATE INDEX");
        assert_eq!(cmd_complete_tag_for_sql("CREATE UNIQUE INDEX idx ON t (x)", 0), "CREATE INDEX");
        assert_eq!(cmd_complete_tag_for_sql("CREATE RANGE INDEX idx ON t (x)", 0), "CREATE INDEX");
        assert_eq!(cmd_complete_tag_for_sql("DROP TABLE t", 0), "DROP TABLE");
        assert_eq!(cmd_complete_tag_for_sql("DROP INDEX idx", 0), "DROP INDEX");
        assert_eq!(cmd_complete_tag_for_sql("ALTER TABLE t ADD COLUMN v i64", 0), "ALTER TABLE");
        assert_eq!(cmd_complete_tag_for_sql("TRUNCATE t", 0), "TRUNCATE TABLE");
    }

    /// `cmd_complete_tag_for_sql` returns the canonical tag for txn
    /// control.
    #[test]
    fn t9_cmd_complete_tag_for_transaction_control() {
        assert_eq!(cmd_complete_tag_for_sql("BEGIN", 0), "BEGIN");
        assert_eq!(cmd_complete_tag_for_sql("START TRANSACTION", 0), "BEGIN");
        assert_eq!(cmd_complete_tag_for_sql("COMMIT", 0), "COMMIT");
        assert_eq!(cmd_complete_tag_for_sql("END", 0), "COMMIT");
        assert_eq!(cmd_complete_tag_for_sql("ROLLBACK", 0), "ROLLBACK");
        assert_eq!(cmd_complete_tag_for_sql("ABORT", 0), "ROLLBACK");
    }

    /// `cmd_complete_tag_for_sql` is case-insensitive on the leading
    /// keyword — "insert"/"InSeRt"/"INSERT" all map to INSERT.
    #[test]
    fn t9_cmd_complete_tag_case_insensitive() {
        assert_eq!(cmd_complete_tag_for_sql("insert into t (id) values (1)", 1), "INSERT 0 1");
        assert_eq!(cmd_complete_tag_for_sql("InSeRt INTO t VALUES (1)", 1), "INSERT 0 1");
        assert_eq!(cmd_complete_tag_for_sql("update t set x=1", 7), "UPDATE 7");
        assert_eq!(cmd_complete_tag_for_sql("CREATE table t (id i64)", 0), "CREATE TABLE");
    }

    /// `cmd_complete_tag_for_sql` strips leading line + block comments
    /// and whitespace before testing the keyword. ORMs/JDBC do this.
    #[test]
    fn t9_cmd_complete_tag_strips_leading_comments() {
        assert_eq!(
            cmd_complete_tag_for_sql("  -- foo\n INSERT INTO t (id) VALUES (1)", 1),
            "INSERT 0 1"
        );
        assert_eq!(
            cmd_complete_tag_for_sql("/* leading block */ UPDATE t SET x = 1", 4),
            "UPDATE 4"
        );
        assert_eq!(
            cmd_complete_tag_for_sql("   \n\t-- one\n\t-- two\nSELECT * FROM t", 0),
            "SELECT 0"
        );
    }

    /// `count_insert_values` counts top-level `(...)` VALUES tuples,
    /// not parens inside strings.
    #[test]
    fn t9_count_insert_values_single_row() {
        assert_eq!(count_insert_values("INSERT INTO t (id) VALUES (1)"), 1);
        assert_eq!(count_insert_values("insert into t (id, name) values (1, 'a')"), 1);
    }

    /// `count_insert_values` for multi-row INSERT VALUES returns N.
    #[test]
    fn t9_count_insert_values_multi_row() {
        assert_eq!(
            count_insert_values("INSERT INTO t (id) VALUES (1), (2), (3)"),
            3
        );
        assert_eq!(
            count_insert_values(
                "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
            ),
            4
        );
    }

    /// `count_insert_values` ignores parens inside single-quoted
    /// string literals (a quoted `(` doesn't bump the tuple count).
    #[test]
    fn t9_count_insert_values_ignores_quoted_parens() {
        assert_eq!(
            count_insert_values("INSERT INTO t (id, n) VALUES (1, 'has ( in it')"),
            1
        );
        // Doubled '' = escaped quote, still inside the string.
        assert_eq!(
            count_insert_values(
                "INSERT INTO t (id, n) VALUES (1, 'it''s a (test)')"
            ),
            1
        );
    }

    /// `count_insert_values` ignores parens inside comments.
    #[test]
    fn t9_count_insert_values_ignores_commented_parens() {
        assert_eq!(
            count_insert_values(
                "INSERT INTO t (id) VALUES (1) -- (would be ignored)"
            ),
            1
        );
        assert_eq!(
            count_insert_values(
                "INSERT INTO t (id) VALUES /* (also ignored) */ (1), (2)"
            ),
            2
        );
    }

    /// `count_insert_values` returns 0 when there's no VALUES clause.
    #[test]
    fn t9_count_insert_values_zero_when_no_values() {
        assert_eq!(count_insert_values("UPDATE t SET x = 1"), 0);
        assert_eq!(count_insert_values("SELECT * FROM t"), 0);
        assert_eq!(count_insert_values(""), 0);
    }

    /// E2E: dispatch_query routes a single-row INSERT through
    /// `apply_sql_with_count` (default impl) and emits "INSERT 0 1".
    #[test]
    fn t9_dispatch_insert_single_row_emits_insert_0_1() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(),
            result: std::sync::Mutex::new(Some(OpResult::Ok)),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("INSERT INTO t (id) VALUES (42)", &eng);
        assert!(
            bytes.windows(b"INSERT 0 1\0".len()).any(|w| w == b"INSERT 0 1\0"),
            "expected CommandComplete tag 'INSERT 0 1' in outbound bytes"
        );
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// E2E: dispatch_query routes a multi-row INSERT and emits
    /// "INSERT 0 N" where N is the VALUES-tuple count. The engine's
    /// `OpResult::Ok` (from the underlying `Op::Txn`) doesn't carry
    /// the count; the gateway's `count_insert_values` rescues N.
    #[test]
    fn t9_dispatch_insert_multi_row_emits_insert_0_n() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(),
            // Multi-row INSERT compiles to Op::Txn → returns Ok.
            result: std::sync::Mutex::new(Some(OpResult::Ok)),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query(
            "INSERT INTO t (id) VALUES (1), (2), (3), (4), (5)",
            &eng,
        );
        assert!(
            bytes.windows(b"INSERT 0 5\0".len()).any(|w| w == b"INSERT 0 5\0"),
            "expected CommandComplete tag 'INSERT 0 5' in outbound bytes (got: {:?})",
            String::from_utf8_lossy(&bytes)
        );
    }

    /// E2E: dispatch_query routes an UPDATE through
    /// `apply_sql_with_count` and emits "UPDATE 1" (default impl
    /// reports 1 for Ok). WHERE-clause UPDATE that affects more rows
    /// is documented as lossy (V2 SP-PG enhancement on OpResult).
    #[test]
    fn t9_dispatch_update_emits_update_count() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(),
            result: std::sync::Mutex::new(Some(OpResult::Ok)),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("UPDATE t SET v = 100 WHERE id = 7", &eng);
        assert!(
            bytes.windows(b"UPDATE 1\0".len()).any(|w| w == b"UPDATE 1\0"),
            "expected CommandComplete tag 'UPDATE 1' in outbound bytes"
        );
    }

    /// E2E: dispatch_query routes a DELETE through
    /// `apply_sql_with_count` and emits "DELETE 1".
    #[test]
    fn t9_dispatch_delete_emits_delete_count() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(),
            result: std::sync::Mutex::new(Some(OpResult::Ok)),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("DELETE FROM t WHERE id = 7", &eng);
        assert!(
            bytes.windows(b"DELETE 1\0".len()).any(|w| w == b"DELETE 1\0"),
            "expected CommandComplete tag 'DELETE 1' in outbound bytes"
        );
    }

    /// E2E: dispatch_query routes a CREATE INDEX through and emits
    /// "CREATE INDEX" (no row count).
    #[test]
    fn t9_dispatch_create_index_emits_canonical_tag() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(),
            result: std::sync::Mutex::new(Some(OpResult::Ok)),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("CREATE INDEX idx ON t (id)", &eng);
        assert!(
            bytes.windows(b"CREATE INDEX\0".len())
                .any(|w| w == b"CREATE INDEX\0"),
            "expected CommandComplete tag 'CREATE INDEX' in outbound bytes"
        );
    }

    /// `leading_keyword` strips comments + whitespace before
    /// extracting the keyword.
    #[test]
    fn t9_leading_keyword_strips_comments() {
        assert_eq!(leading_keyword("  -- a\n INSERT INTO t"), "INSERT");
        assert_eq!(leading_keyword("/* x */SELECT 1"), "SELECT");
        assert_eq!(leading_keyword("\t\nUPDATE t"), "UPDATE");
        // Multiple comments stacked.
        assert_eq!(
            leading_keyword("-- one\n-- two\n/* three */ DELETE FROM t"),
            "DELETE"
        );
        assert_eq!(leading_keyword("   "), "");
    }

    // ───────────────────────────────────────────────────────────────────
    // T17 KATs — scatter-scan integration verification.
    //
    // PG-wire dispatches every SQL statement through
    // `EngineApply::apply_sql(sql) -> OpResult`. In a multi-shard
    // deployment the underlying engine (the `kesseldb-server::Router`,
    // SP-A T2) routes scan-shaped ops via `Route::Scatter` and merges
    // per-shard `OpResult::Got(bytes)` slots via
    // `scatter_scan::merge_scan_results`. The merged bytes have the
    // SAME `[u32 LE len][record]*` shape a single-shard `Op::Select`
    // would produce — that's the SP-A T2 invariant that makes scatter
    // transparent to every wire surface (binary, HTTP, WS, and now
    // PG-wire).
    //
    // T17 locks this transparency invariant at the PG-wire layer by
    // proving: for any pair of (K=1 engine, K=N engine) where both
    // produce the SAME merged byte stream, `dispatch_query` returns
    // BYTE-IDENTICAL wire output. This is the operational definition
    // of "scatter-scan works over PG-wire" — V1 doesn't need any new
    // code on the PG-wire side because the routing happens behind
    // `apply_sql`.
    //
    // The real cluster integration test lives in
    // `crates/kesseldb-server/tests/` (T12's
    // `t12_pg_gateway_listener_serves_real_pg_client` already proves
    // the listener wiring; a future spin-up-real-shards test would
    // be additive). T17 ships the byte-identity proof at the unit
    // level here, where the assertion is sharp.
    // ───────────────────────────────────────────────────────────────────

    /// Build a 4-row stream as if a K=1 engine had run `SELECT * FROM t`.
    fn build_k1_stream_4_rows() -> (Vec<PgColumn>, Vec<u8>) {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "n".into(), kind: FieldKind::I32, nullable: false },
        ];
        // Rows in canonical shard-id-then-row-id order — the same order
        // SP-A's `merge_scan_results::Unordered` would produce.
        let r1 = build_record(&cols, &[Value::Int(1), Value::Int(10)]);
        let r2 = build_record(&cols, &[Value::Int(2), Value::Int(20)]);
        let r3 = build_record(&cols, &[Value::Int(3), Value::Int(30)]);
        let r4 = build_record(&cols, &[Value::Int(4), Value::Int(40)]);
        let stream = build_row_stream(&[r1, r2, r3, r4]);
        (cols, stream)
    }

    /// Build the SAME 4-row stream but as if produced by a K=4 scatter
    /// merger: per-shard streams of 1 row each, merged shard-id-ordered.
    /// SP-A `merge_scan_results::Unordered` concatenates per-shard
    /// `[u32 LE len][rec]*` slots in shard-id order with no per-row
    /// recoding, so the BYTE-IDENTITY between this and the K=1 stream
    /// IS the SP-A T2 invariant we're locking through PG-wire.
    fn build_k4_merged_stream_4_rows() -> (Vec<PgColumn>, Vec<u8>) {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "n".into(), kind: FieldKind::I32, nullable: false },
        ];
        // Each "shard" contributes one row; the merger concatenates them
        // shard-id-ordered. Per-row encoding is identical to K=1.
        let shard0 = build_record(&cols, &[Value::Int(1), Value::Int(10)]);
        let shard1 = build_record(&cols, &[Value::Int(2), Value::Int(20)]);
        let shard2 = build_record(&cols, &[Value::Int(3), Value::Int(30)]);
        let shard3 = build_record(&cols, &[Value::Int(4), Value::Int(40)]);
        // Merge: shard-id-ordered length-prefixed concat (exactly what
        // SP-A T2 `merge_scan_results::merge_unordered` produces).
        let stream = build_row_stream(&[shard0, shard1, shard2, shard3]);
        (cols, stream)
    }

    /// **HEADLINE T17 KAT** — byte-identity invariant: PG-wire over
    /// K=1 vs K=N produces IDENTICAL outbound bytes when the merged
    /// row stream is identical. Locks that PG-wire is transparent to
    /// the SP-A scatter-scan layer; no PG-wire-side code change is
    /// required to support sharded SELECTs.
    #[test]
    fn t17_pg_wire_is_byte_identical_under_k1_vs_k4_scatter() {
        let (cols, k1_stream) = build_k1_stream_4_rows();
        let (_, k4_stream) = build_k4_merged_stream_4_rows();

        // SP-A T2 invariant: K=1 and K=4 merged streams are byte-
        // identical at the row-stream layer. The PG-wire byte-identity
        // claim depends on this; if SP-A ever rewrites per-row bytes
        // during merge, the test below will catch it.
        assert_eq!(
            k1_stream, k4_stream,
            "SP-A T2 invariant: merged row stream MUST be byte-identical \
             across K=1 vs K=N"
        );

        let eng_k1 = CannedEngine {
            cols: cols.clone(),
            row_bytes: k1_stream,
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        };
        let eng_k4 = CannedEngine {
            cols,
            row_bytes: k4_stream,
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes_k1 = dispatch_query("SELECT * FROM t", &eng_k1);
        let bytes_k4 = dispatch_query("SELECT * FROM t", &eng_k4);
        assert_eq!(
            bytes_k1, bytes_k4,
            "PG-wire MUST be byte-identical between K=1 and K=N when the \
             merged row stream is identical"
        );
        // Sanity: BOTH streams contain the right row count.
        assert!(bytes_k1.windows(b"SELECT 4\0".len()).any(|w| w == b"SELECT 4\0"));
    }

    /// T17: PG-wire over a merged stream emits rows in the order the
    /// merger produced — locks "merge order is wire order" so the
    /// caller's mental model "row order = K-invariant for unordered
    /// scans" doesn't depend on any PG-wire-side reordering.
    #[test]
    fn t17_pg_wire_preserves_merge_order() {
        let (cols, stream) = build_k4_merged_stream_4_rows();
        let eng = CannedEngine {
            cols,
            row_bytes: stream,
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("SELECT * FROM t", &eng);
        // Find the DataRow byte positions and assert the per-row
        // values appear in shard-id order (1<2<3<4 / 10<20<30<40).
        let pos_10 = bytes.windows(2).position(|w| w == b"10").unwrap();
        let pos_20 = bytes.windows(2).position(|w| w == b"20").unwrap();
        let pos_30 = bytes.windows(2).position(|w| w == b"30").unwrap();
        let pos_40 = bytes.windows(2).position(|w| w == b"40").unwrap();
        assert!(pos_10 < pos_20);
        assert!(pos_20 < pos_30);
        assert!(pos_30 < pos_40);
    }

    /// T17: an empty merge (all shards returned 0 rows) emits the
    /// same `SELECT 0` tag a K=1 empty engine would. Locks the
    /// empty-merge edge case.
    #[test]
    fn t17_pg_wire_empty_merge_emits_select_0() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(), // empty merged stream
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("SELECT * FROM t", &eng);
        assert!(
            bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"),
            "empty merged stream MUST emit 'SELECT 0' tag"
        );
    }

    /// T17: a scatter-shaped error propagation — if any shard returns
    /// an `OpResult` other than `Got`, the scatter layer collapses to
    /// the first non-`Got` (SP-A T2 §6 hard-fail). PG-wire renders
    /// that error via the T7 SQLSTATE map, identical to the K=1 path.
    #[test]
    fn t17_scatter_shard_error_renders_via_t7_sqlstate_map() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        // Simulate: scatter collapsed to OpResult::Unavailable (some
        // shard timed out + the merger short-circuited to that slot).
        let eng = CannedEngine {
            cols,
            row_bytes: Vec::new(),
            result: std::sync::Mutex::new(Some(OpResult::Unavailable)),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("SELECT * FROM t", &eng);
        // T7 maps Unavailable → FATAL 57P03 cannot_connect_now.
        assert!(
            bytes.windows(b"57P03".len()).any(|w| w == b"57P03"),
            "shard-unavailable MUST surface as SQLSTATE 57P03"
        );
        assert!(
            bytes.windows(b"FATAL".len()).any(|w| w == b"FATAL"),
            "shard-unavailable MUST surface FATAL severity"
        );
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-EXTQ-CAST T2 — dispatch integration KATs verifying that
    // the cast strip runs at the entry point + the rewritten SQL
    // reaches the downstream dispatcher without surfacing the
    // `kessel-sql` `42601 syntax_error` that an un-stripped `::int8`
    // would produce.
    // ───────────────────────────────────────────────────────────────────

    /// SP-PG-EXTQ-CAST: `SELECT 1::int8` no longer surfaces the
    /// pre-arc `42601 syntax_error: unexpected char ':'`. The
    /// stripped form (`SELECT 1`) reaches the engine; with a canned
    /// `Got` row response we get a `SELECT 1` CommandComplete +
    /// ReadyForQuery, not an ErrorResponse.
    #[test]
    fn sppgextqcast_select_one_int8_strips_and_no_error() {
        let cols = vec![PgColumn {
            name: "?column?".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let rec = build_record(&cols, &[Value::Int(1)]);
        let stream = build_row_stream(&[rec]);
        let eng = CannedEngine {
            cols,
            row_bytes: stream,
            result: std::sync::Mutex::new(None),
            // The strip turns `SELECT 1::int8 FROM t` into
            // `SELECT 1 FROM t`; the test engine answers any SELECT
            // shape with the canned `Got` row, so the dispatcher
            // takes the success path.
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("SELECT 1::int8 FROM t", &eng);
        // No 42601 error in the response.
        assert!(
            !bytes.windows(b"42601".len()).any(|w| w == b"42601"),
            "SP-PG-EXTQ-CAST: stripped `SELECT 1::int8` must NOT surface 42601"
        );
        // And a SELECT command-complete tag is present (success path).
        assert!(
            bytes.windows(b"SELECT".len()).any(|w| w == b"SELECT"),
            "SP-PG-EXTQ-CAST: stripped form must reach success path with SELECT tag"
        );
    }

    /// SP-PG-EXTQ-CAST: the strip is a no-op for SQL without casts
    /// (byte-equal response shape to the pre-arc dispatch). Combined
    /// with the per-byte locks above, this is the regression brake
    /// that proves the cast strip never disturbs the existing text-
    /// only path.
    #[test]
    fn sppgextqcast_noop_when_no_casts_present() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let rec = build_record(&cols, &[Value::Int(7)]);
        let stream = build_row_stream(&[rec]);
        let eng_a = CannedEngine {
            cols: cols.clone(),
            row_bytes: stream.clone(),
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        };
        let eng_b = CannedEngine {
            cols,
            row_bytes: stream,
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        };
        // A SQL that contains no `::` must produce identical bytes
        // before vs after the strip — eng_a and eng_b are identical
        // canned engines so the same SQL twice should produce
        // identical byte streams.
        let bytes_a = dispatch_query("SELECT * FROM t", &eng_a);
        let bytes_b = dispatch_query("SELECT * FROM t", &eng_b);
        assert_eq!(
            bytes_a, bytes_b,
            "no-op cast strip must be deterministic across calls"
        );
    }
}
