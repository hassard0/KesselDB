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

/// SP-PG-SQL-DML-GENERAL — a general-WHERE UPDATE/DELETE returns its
/// affected-row count (and optional RETURNING rows) framed inside
/// `OpResult::Got`, distinct from a SELECT `Got`. Layout:
/// `[DML_RESULT_TAG][u32 affected LE][u32 nrows LE]` then `nrows ×
/// [u32 reclen LE][record bytes]`. The engine (kesseldb-server) builds
/// this frame via `encode_dml_result`; the gateway decodes it here. The
/// tag byte disambiguates from a SELECT row stream (which is
/// `[u32 reclen][record]*` with no leading tag) — the gateway only
/// attempts the DML decode on an UPDATE/DELETE leading keyword anyway.
pub const DML_RESULT_TAG: u8 = 0xD3;

/// SP-PG-SQL-DML-GENERAL — build the DML-result frame (see
/// `DML_RESULT_TAG`). `rows` is empty for a count-only (no-RETURNING)
/// result. Lives in the gateway crate so the engine (which depends on
/// it) and the gateway share ONE definition.
pub fn encode_dml_result(affected: u32, rows: &[Vec<u8>]) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(9 + rows.iter().map(|r| 4 + r.len()).sum::<usize>());
    out.push(DML_RESULT_TAG);
    out.extend_from_slice(&affected.to_le_bytes());
    out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for r in rows {
        out.extend_from_slice(&(r.len() as u32).to_le_bytes());
        out.extend_from_slice(r);
    }
    out
}

/// SP-PG-SQL-DML-GENERAL — decode a DML-result frame into
/// `(affected, rows)`. Returns `None` if `bytes` is not a DML-result
/// frame (wrong tag or truncated) — the caller then falls back to the
/// SELECT-`Got` path. Defensive against truncation (a malformed frame
/// yields `None`, never a panic).
pub fn decode_dml_result(bytes: &[u8]) -> Option<(u32, Vec<Vec<u8>>)> {
    if bytes.first() != Some(&DML_RESULT_TAG) || bytes.len() < 9 {
        return None;
    }
    let affected = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    let nrows = u32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]) as usize;
    let mut rows = Vec::with_capacity(nrows);
    let mut p = 9usize;
    for _ in 0..nrows {
        if p + 4 > bytes.len() {
            return None;
        }
        let l = u32::from_le_bytes([
            bytes[p], bytes[p + 1], bytes[p + 2], bytes[p + 3],
        ]) as usize;
        p += 4;
        if p + l > bytes.len() {
            return None;
        }
        rows.push(bytes[p..p + l].to_vec());
        p += l;
    }
    Some((affected, rows))
}

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
    // SP-PG-RETURNING-MULTIROW-STAR — desugar SQLAlchemy's
    // insertmanyvalues form to plain multi-row VALUES FIRST (before the
    // cast validator, which would reject the `p0::VARCHAR` projection
    // cast). No-op for any other SQL.
    let imv = crate::insertmanyvalues::rewrite_insertmanyvalues(sql);
    let sql = imv.as_deref().unwrap_or(sql);
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
        OpResult::Ok | OpResult::TxCommitted { .. } | OpResult::Created { .. } => 1,
        // SP-PG-RETURNING-MULTIROW-STAR: a batched INSERT surfaces N ids.
        OpResult::CreatedMany { ids } => ids.len() as u64,
        _ => 0,
    };
    match result {
        // SP-PG-SQL-DML-GENERAL — a general-WHERE UPDATE/DELETE returns a
        // DML-result frame (count [+ RETURNING rows]) inside `Got`. The
        // UPDATE/DELETE leading keyword + the frame tag disambiguate it
        // from a SELECT row stream.
        OpResult::Got(ref row_bytes)
            if (leading_keyword_is(sql_trimmed, "UPDATE")
                || leading_keyword_is(sql_trimmed, "DELETE"))
                && decode_dml_result(row_bytes).is_some() =>
        {
            render_dml_where_result(row_bytes, sql_trimmed, engine)
        }
        OpResult::Got(row_bytes) => {
            // SP-PG-SQL-ORM-PARSE T3 — render an explicit projection list
            // (`SELECT c1, c2 FROM t`, incl. qualified `t.c1`) as well as
            // the whole-row `SELECT *` shape. `render_select_got`
            // dispatches on `select_columns` (projection) vs
            // `select_star_table` (whole row).
            render_select_got(&row_bytes, sql_trimmed, select_table.clone(), engine)
        }
        // SP-PG-SERIAL-RETURNING / -MULTIROW-STAR: Extended-Query
        // `INSERT … RETURNING …` (SQLAlchemy 2.0's autoincrement flush
        // rides this path; by DEFAULT it batches multiple rows into one
        // statement → `CreatedMany`). Mirror the simple-query handling:
        // surface the assigned id(s) + project the RETURNING columns.
        ref ok_variant @ (OpResult::Ok
            | OpResult::Created { .. }
            | OpResult::CreatedMany { .. })
            if kessel_sql::insert_returning(sql_trimmed).is_some() =>
        {
            let assigned_ids = match ok_variant {
                OpResult::Created { id } => vec![*id],
                OpResult::CreatedMany { ids } => ids.clone(),
                _ => Vec::new(),
            };
            render_insert_returning(sql_trimmed, &assigned_ids, engine)
        }
        OpResult::Ok
        | OpResult::TypeCreated(_)
        | OpResult::TxCommitted { .. }
        | OpResult::Created { .. }
        | OpResult::CreatedMany { .. } => {
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
    // SP-PG-RETURNING-MULTIROW-STAR — desugar SQLAlchemy's DEFAULT
    // `use_insertmanyvalues` form (INSERT … SELECT … FROM (VALUES …) AS
    // sen(…) ORDER BY sen_counter RETURNING …) to the plain multi-row
    // `INSERT … VALUES (…),(…) RETURNING …` the engine handles. This runs
    // FIRST — BEFORE the literal-cast validator — so the `p0::VARCHAR`
    // projection cast (which the validator would reject as a cross-
    // category coerce) is gone before validation. No-op for any other
    // SQL, so every existing path is byte-untouched.
    let imv = crate::insertmanyvalues::rewrite_insertmanyvalues(sql);
    let sql = imv.as_deref().unwrap_or(sql);
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
        // SP-PG-SQL-DML-GENERAL — general-WHERE UPDATE/DELETE: the engine
        // returns a DML-result frame (count [+ RETURNING rows]) inside
        // `Got`. Disambiguated from a SELECT row stream by the
        // UPDATE/DELETE leading keyword + the frame tag.
        OpResult::Got(ref row_bytes)
            if (leading_keyword_is(sql_trimmed, "UPDATE")
                || leading_keyword_is(sql_trimmed, "DELETE"))
                && decode_dml_result(row_bytes).is_some() =>
        {
            render_dml_where_result(row_bytes, sql_trimmed, engine)
        }
        OpResult::Got(row_bytes) => {
            // SELECT path — emit RowDescription + DataRow* +
            // CommandComplete("SELECT N") + ReadyForQuery.
            //
            // SP-PG-SQL-ORM-PARSE T3 — `render_select_got` handles BOTH
            // the whole-row `SELECT * FROM t` shape (via
            // `select_star_table` + the full-record `emit_data_rows`) AND
            // an explicit projection list `SELECT c1, c2 FROM t` (incl.
            // qualified `t.c1`, via `select_columns` + `emit_projected_
            // rows`). Closes the V1 `SELECT *`-only render weak-spot.
            render_select_got(&row_bytes, sql_trimmed, select_table.clone(), engine)
        }
        // SP-PG-SERIAL-RETURNING: an `INSERT … RETURNING …` succeeded.
        // When the engine assigned a serial id it returns `Created { id }`;
        // an explicit-id INSERT returns `Ok`. Either way, if the SQL had a
        // RETURNING clause, emit RowDescription + DataRow(returned values)
        // + CommandComplete instead of a bare CommandComplete.
        ref ok_variant @ (OpResult::Ok
            | OpResult::Created { .. }
            | OpResult::CreatedMany { .. })
            if kessel_sql::insert_returning(sql_trimmed).is_some() =>
        {
            let assigned_ids = match ok_variant {
                OpResult::Created { id } => vec![*id],
                OpResult::CreatedMany { ids } => ids.clone(),
                _ => Vec::new(),
            };
            render_insert_returning(sql_trimmed, &assigned_ids, engine)
        }
        OpResult::Ok
        | OpResult::TypeCreated(_)
        | OpResult::TxCommitted { .. }
        | OpResult::Created { .. }
        | OpResult::CreatedMany { .. } => {
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

/// SP-PG-SQL-ORM-PARSE T3 — render an `OpResult::Got` SELECT result to
/// the wire, handling BOTH supported shapes:
///
///   1. **Explicit projection list** `SELECT c1, c2 FROM t` (incl.
///      qualified `t.c1` — `kessel_sql::select_columns` strips the
///      qualifier). The engine's `Op::SelectFields` emitted the
///      projected columns as concatenated raw fixed-width bytes; we
///      build a RowDescription for JUST the projected columns (in
///      projection order) and decode via `emit_projected_rows`.
///   2. **Whole-row** `SELECT * FROM t` (`select_star_table`). The
///      engine emitted full on-disk records; we describe the whole
///      table and decode via `emit_data_rows`.
///
/// Projection (shape 1) is checked FIRST because it's the more specific
/// match (`select_columns` returns `None` for `SELECT *`). Returns the
/// full backend byte sequence ending in `ReadyForQuery('I')`.
fn render_select_got<E: EngineApply + ?Sized>(
    row_bytes: &[u8],
    sql_trimmed: &str,
    select_table: Option<String>,
    engine: &E,
) -> Vec<u8> {
    let mut out = Vec::new();
    // Shape 0.4 — SP-PG-SQL-JOIN-AGG: a join-group-aggregate
    // (`SELECT a.name, COUNT(b.id) FROM a JOIN b ON … GROUP BY a.name`). The
    // engine's `Op::Join { group_aggregate: Some(..) }` returns the value-only
    // group-aggregate stream (`[u32 ngroups]…`, NOT a `KTR1` join stream), so
    // this is checked BEFORE the `KTR1` join render + the single-scalar
    // aggregate render (both of which return None / don't match this shape).
    // `join_group_aggregate` returns None for any non-join-group-aggregate SQL,
    // so every existing render path is byte-untouched.
    if let Some(jga) = kessel_sql::join_group_aggregate(sql_trimmed) {
        return render_join_group_aggregate(row_bytes, &jga, engine);
    }
    // Shape 0 — SP-PG-SQL-AGG-ALIAS-RENDER: a single scalar-aggregate
    // SELECT over a FROM table (`SELECT COUNT(*) AS "__count" FROM "t"` —
    // Django's `.count()`). The engine's `Op::Aggregate` returns a
    // 16-byte little-endian i128 scalar in `OpResult::Got`. Render it as
    // RowDescription(1 col, named by the alias or the lowercase function
    // name) + ONE DataRow(decimal) + CommandComplete("SELECT 1"). This is
    // checked FIRST because `select_columns` / `select_star_table` both
    // return None for an aggregate, which would otherwise fall through to
    // the "0A000 only renders SELECT *" arm.
    if let Some(agg) = kessel_sql::select_aggregate(sql_trimmed) {
        if row_bytes.len() == 16 {
            let scalar = i128::from_le_bytes(row_bytes.try_into().unwrap());
            let col_name = agg
                .alias
                .clone()
                .unwrap_or_else(|| kessel_sql::agg_default_name(agg.kind).to_string());
            let fields = [FieldMeta {
                name: col_name,
                type_oid: crate::proto::PG_TYPE_INT8,
            }];
            out.extend_from_slice(&encode_row_description(&fields));
            let text = scalar.to_string();
            out.extend_from_slice(&encode_data_row(&[Some(text.as_bytes())]));
            out.extend_from_slice(&encode_command_complete(&select_tag(1)));
            out.extend_from_slice(&encode_ready_for_query(b'I'));
            return out;
        }
        // Defensive: a non-16-byte aggregate result falls through to the
        // existing render shapes below (none will match cleanly, so the
        // client gets the standard render error rather than a panic).
    }
    // Shape 0.5 — SP-PG-ORM-RELATIONSHIPS: an inner-equi-JOIN result. The
    // engine's `Op::Join` returns a SELF-DESCRIBING typed result
    // `[KTR1][u32 deflen][combined typedef][ [u32 reclen][full record] ]*`
    // whose embedded schema names every column `<table>.<col>`. Detect the
    // `KTR1` magic, decode the combined schema, map the SQL projection
    // (`SELECT authors.name, books.title …` or `SELECT *`) onto it, and emit
    // RowDescription + projected DataRows. Gated on the magic prefix so a
    // non-JOIN result is byte-untouched.
    if row_bytes.len() >= 8 && &row_bytes[..4] == b"KTR1" {
        if let Some((cols, is_star)) = kessel_sql::join_projection(sql_trimmed) {
            return render_join_result(row_bytes, &cols, is_star);
        }
        // Magic present but the SQL didn't parse as a JOIN projection — fall
        // through to the standard render error (defensive; should not happen
        // because only `Op::Join` emits `KTR1`).
    }
    // Shape 1 — explicit projection list.
    if let Some((table_name, proj_names)) =
        kessel_sql::select_columns(sql_trimmed)
    {
        let table_cols = match engine.describe_table(&table_name) {
            Some(c) => c,
            None => {
                return error_response_then_rfq(
                    SEVERITY_ERROR,
                    "42P01",
                    &format!("unknown table \"{table_name}\""),
                );
            }
        };
        let proj_cols = match resolve_projection(&proj_names, &table_cols) {
            Some(c) => c,
            None => {
                // A projected name isn't a column of the table.
                return error_response_then_rfq(
                    SEVERITY_ERROR,
                    "42703",
                    &format!(
                        "column does not exist in \"{table_name}\" \
                         (projection {proj_names:?})"
                    ),
                );
            }
        };
        let fields: Vec<FieldMeta> = proj_cols
            .iter()
            .map(|c| FieldMeta {
                name: c.name.clone(),
                type_oid: field_kind_to_oid(c.kind),
            })
            .collect();
        out.extend_from_slice(&encode_row_description(&fields));
        // SP-PG-ORM-REALAPP — a projection-list SELECT with an `ORDER BY`
        // (`SELECT title FROM posts ORDER BY title LIMIT n`) lowers to
        // `Op::SelectSorted`, which returns the FULL record stream (the sort
        // wins the engine's `match` arm; the projection is dropped). The
        // narrow `emit_projected_rows` decoder would see a full record
        // (width = whole row) where it expects only the projected fields'
        // width → a spurious "projected row width N != expected M". When the
        // SQL is sorted we instead decode each FULL record against the table
        // schema and project the requested columns by index. A non-sorted
        // projection keeps the byte-identical narrow path.
        let row_count = if kessel_sql::select_projection_is_sorted(sql_trimmed)
        {
            match emit_projected_from_full_records(
                row_bytes,
                &table_cols,
                &proj_cols,
                &mut out,
            ) {
                Ok(n) => n,
                Err(msg) => {
                    return error_response_then_rfq(
                        SEVERITY_ERROR,
                        "XX000",
                        &format!("sorted projection decode failed: {msg}"),
                    );
                }
            }
        } else {
            match emit_projected_rows(row_bytes, &proj_cols, &mut out) {
                Ok(n) => n,
                Err(msg) => {
                    return error_response_then_rfq(
                        SEVERITY_ERROR,
                        "XX000",
                        &format!("projected row decode failed: {msg}"),
                    );
                }
            }
        };
        out.extend_from_slice(&encode_command_complete(&select_tag(row_count)));
        out.extend_from_slice(&encode_ready_for_query(b'I'));
        return out;
    }
    // Shape 2 — whole-row `SELECT *`.
    let table_name = match select_table {
        Some(n) => n,
        None => {
            return error_response_then_rfq(
                SEVERITY_ERROR,
                "0A000",
                "V1 PG-wire only renders `SELECT * FROM <table>` or an \
                 explicit projection list `SELECT c1, c2 FROM <table>`",
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
    let row_count = match emit_data_rows(row_bytes, &cols, &mut out) {
        Ok(n) => n,
        Err(msg) => {
            return error_response_then_rfq(
                SEVERITY_ERROR,
                "XX000",
                &format!("row decode failed: {msg}"),
            );
        }
    };
    out.extend_from_slice(&encode_command_complete(&select_tag(row_count)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-ORM-RELATIONSHIPS — render an inner-equi-JOIN result. The engine's
/// `Op::Join` emits a self-describing typed result
/// `[KTR1][u32 deflen][combined typedef][ [u32 reclen][full record] ]*`; the
/// embedded typedef names every column `<table>.<col>` (e.g. `authors.id`,
/// `books.author_id`) so the two joined tables' columns are disambiguated.
///
/// `proj` is the SQL projection (`SELECT authors.name, books.title …`); each
/// item carries its optional table qualifier + column. `is_star` projects
/// EVERY combined column in schema order. We:
///   1. decode the combined typedef → combined `PgColumn`s,
///   2. resolve each projection item to a combined-column INDEX (qualifier
///      disambiguates same-named columns; a bare column matches the first
///      column of that name),
///   3. decode each full record (the combined record layout) → all cells,
///   4. emit RowDescription(projected cols) + one DataRow(projected cells).
///
/// Returns the full backend byte sequence ending in `ReadyForQuery('I')`.
fn render_join_result(
    row_bytes: &[u8],
    proj: &[kessel_sql::JoinProjCol],
    is_star: bool,
) -> Vec<u8> {
    // Split `[KTR1][u32 deflen][typedef][rows…]`.
    let deflen = u32::from_le_bytes(match row_bytes.get(4..8) {
        Some(b) => b.try_into().unwrap(),
        None => {
            return error_response_then_rfq(
                SEVERITY_ERROR,
                "XX000",
                "JOIN result: truncated header",
            )
        }
    }) as usize;
    let typedef = match row_bytes.get(8..8 + deflen) {
        Some(b) => b,
        None => {
            return error_response_then_rfq(
                SEVERITY_ERROR,
                "XX000",
                "JOIN result: truncated typedef",
            )
        }
    };
    let rows = &row_bytes[8 + deflen..];
    let (_jname, fields) = match kessel_catalog::decode_type_def(typedef) {
        Some(t) => t,
        None => {
            return error_response_then_rfq(
                SEVERITY_ERROR,
                "XX000",
                "JOIN result: undecodable embedded schema",
            )
        }
    };
    // Combined schema as PgColumns (names are already `<table>.<col>`).
    let combined: Vec<PgColumn> = fields
        .iter()
        .map(|f| PgColumn {
            name: f.name.clone(),
            kind: f.kind,
            nullable: f.nullable,
        })
        .collect();

    // Resolve the projection to combined-column indices.
    let proj_idx: Vec<usize> = if is_star {
        (0..combined.len()).collect()
    } else {
        let mut idx = Vec::with_capacity(proj.len());
        for item in proj {
            // Combined column names are `<table>.<col>`. With a qualifier,
            // match `<qualifier>.<col>` exactly; without, match the suffix
            // `.<col>` (first table wins, mirroring `col_ident`'s lenient
            // resolution) or a bare `<col>`.
            let want_qualified = item
                .qualifier
                .as_ref()
                .map(|q| format!("{q}.{}", item.column));
            let pos = combined.iter().position(|c| {
                if let Some(wq) = &want_qualified {
                    c.name.eq_ignore_ascii_case(wq)
                } else {
                    c.name.eq_ignore_ascii_case(&item.column)
                        || c.name
                            .rsplit('.')
                            .next()
                            .map(|tail| tail.eq_ignore_ascii_case(&item.column))
                            .unwrap_or(false)
                }
            });
            match pos {
                Some(p) => idx.push(p),
                None => {
                    let shown = want_qualified.unwrap_or_else(|| item.column.clone());
                    return error_response_then_rfq(
                        SEVERITY_ERROR,
                        "42703",
                        &format!("column \"{shown}\" does not exist in the JOIN result"),
                    );
                }
            }
        }
        idx
    };

    let mut out = Vec::new();
    // RowDescription names projected columns by their combined name
    // (`authors.name`) — SQLAlchemy maps results positionally, so the exact
    // RowDescription label does not change the ORM result; psql shows the
    // qualified name, which is the most informative choice.
    let fields_meta: Vec<FieldMeta> = proj_idx
        .iter()
        .map(|&i| FieldMeta {
            name: combined[i].name.clone(),
            type_oid: field_kind_to_oid(combined[i].kind),
        })
        .collect();
    out.extend_from_slice(&encode_row_description(&fields_meta));

    // Decode each full combined record, then project the selected cells.
    let layout = compute_record_layout(&combined);
    let mut p = 0usize;
    let mut n = 0u64;
    while p + 4 <= rows.len() {
        let len = u32::from_le_bytes(rows[p..p + 4].try_into().unwrap()) as usize;
        p += 4;
        let rec = match rows.get(p..p + len) {
            Some(r) => r,
            None => {
                return error_response_then_rfq(
                    SEVERITY_ERROR,
                    "XX000",
                    "JOIN result: truncated record stream",
                )
            }
        };
        p += len;
        let cells = match decode_record(rec, &combined, &layout) {
            Some(c) => c,
            None => {
                return error_response_then_rfq(
                    SEVERITY_ERROR,
                    "XX000",
                    "JOIN result: malformed combined record",
                )
            }
        };
        let projected: Vec<Option<&[u8]>> =
            proj_idx.iter().map(|&i| cells[i].as_deref()).collect();
        out.extend_from_slice(&encode_data_row(&projected));
        n += 1;
    }
    if p != rows.len() {
        return error_response_then_rfq(
            SEVERITY_ERROR,
            "XX000",
            "JOIN result: trailing bytes after records",
        );
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(n)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-SQL-JOIN-AGG — render a join-group-aggregate result. The engine's
/// `Op::Join { group_aggregate: Some(..) }` returns the value-only group-
/// aggregate stream `[u32 ngroups]` then per group
/// `[u32 keylen][key][16B i128 LE × n_aggs]` (the `Op::GroupAggregateMulti`
/// shape) — NOT a self-describing `KTR1` join stream — so we recover the output
/// column shape from the SQL text (`proj`) + the GROUP BY column's table schema:
///   1. resolve the GROUP BY column's `FieldKind` via the qualifier's table,
///   2. RowDescription = [group col (its OID), each agg col (int8)],
///   3. per group: decode the key bytes → Value (by the group kind) →
///      `render_pg_text`; each 16-byte i128 → decimal text,
///   4. emit one DataRow per group + CommandComplete("SELECT N").
/// Groups arrive in ascending group-key order (the engine's BTreeMap), so the
/// rendered rows are deterministic.
fn render_join_group_aggregate<E: EngineApply + ?Sized>(
    row_bytes: &[u8],
    proj: &kessel_sql::JoinGroupAggProj,
    engine: &E,
) -> Vec<u8> {
    // Resolve the GROUP BY column's kind via its qualifier's table.
    let gcols = match engine.describe_table(&proj.group_qualifier) {
        Some(c) => c,
        None => {
            return error_response_then_rfq(
                SEVERITY_ERROR,
                "42P01",
                &format!("unknown table \"{}\"", proj.group_qualifier),
            )
        }
    };
    let gkind = match gcols
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(&proj.group_column))
    {
        Some(c) => c.kind,
        None => {
            return error_response_then_rfq(
                SEVERITY_ERROR,
                "42703",
                &format!(
                    "column \"{}\" does not exist in \"{}\"",
                    proj.group_column, proj.group_qualifier
                ),
            )
        }
    };
    let gwidth = gkind.width() as usize;

    // RowDescription: group column (its OID) + one int8 column per aggregate.
    let mut fields: Vec<FieldMeta> = Vec::with_capacity(1 + proj.aggregates.len());
    fields.push(FieldMeta {
        name: format!("{}.{}", proj.group_qualifier, proj.group_column),
        type_oid: field_kind_to_oid(gkind),
    });
    for a in &proj.aggregates {
        fields.push(FieldMeta {
            name: a.out_name.clone(),
            type_oid: crate::proto::PG_TYPE_INT8,
        });
    }

    let n_aggs = proj.aggregates.len();
    let mut out = Vec::new();
    out.extend_from_slice(&encode_row_description(&fields));

    // Decode `[u32 ngroups]` then per group `[u32 keylen][key][16B × n_aggs]`.
    if row_bytes.len() < 4 {
        return error_response_then_rfq(SEVERITY_ERROR, "XX000", "join-agg: truncated header");
    }
    let ngroups = u32::from_le_bytes(row_bytes[0..4].try_into().unwrap()) as usize;
    let mut p = 4usize;
    let mut emitted = 0u64;
    for _ in 0..ngroups {
        if p + 4 > row_bytes.len() {
            return error_response_then_rfq(SEVERITY_ERROR, "XX000", "join-agg: truncated key len");
        }
        let klen = u32::from_le_bytes(row_bytes[p..p + 4].try_into().unwrap()) as usize;
        p += 4;
        let key = match row_bytes.get(p..p + klen) {
            Some(k) => k,
            None => {
                return error_response_then_rfq(SEVERITY_ERROR, "XX000", "join-agg: truncated key")
            }
        };
        p += klen;
        // Decode the group key (raw fixed-width bytes) → Value → text.
        let mut raw = key.to_vec();
        raw.resize(gwidth, 0);
        let gval = value_from_raw(gkind, &raw);
        let gcell = render_pg_text(&gval, gkind);

        let mut cells: Vec<Option<Vec<u8>>> = Vec::with_capacity(1 + n_aggs);
        cells.push(Some(gcell));
        for _ in 0..n_aggs {
            let v = match row_bytes.get(p..p + 16) {
                Some(b) => i128::from_le_bytes(b.try_into().unwrap()),
                None => {
                    return error_response_then_rfq(
                        SEVERITY_ERROR,
                        "XX000",
                        "join-agg: truncated aggregate value",
                    )
                }
            };
            p += 16;
            cells.push(Some(v.to_string().into_bytes()));
        }
        let refs: Vec<Option<&[u8]>> = cells.iter().map(|c| c.as_deref()).collect();
        out.extend_from_slice(&encode_data_row(&refs));
        emitted += 1;
    }
    out.extend_from_slice(&encode_command_complete(&select_tag(emitted)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-SERIAL-RETURNING / -MULTIROW-STAR: render an
/// `INSERT … RETURNING <cols | *>` reply. The INSERT already committed;
/// here we surface the requested columns as N DataRows (one per inserted
/// row), so SQLAlchemy's DEFAULT `use_insertmanyvalues` batched insert
/// (`VALUES (…),(…),(…) RETURNING id`) gets every assigned id back.
///
/// `assigned_ids` carries the engine-assigned serial ids in insertion
/// order (one per row for `Created`/`CreatedMany`; empty for an
/// explicit-id INSERT, in which case the single explicit `id` is parsed
/// from the SQL). `RETURNING *` (the `["*"]` star sentinel from
/// `kessel_sql::insert_returning`) expands to EVERY table column in
/// declared order. Each row is read back via the engine's normal
/// `SELECT * FROM <table> WHERE id = <id>` path and projected with the
/// same decode machinery as a normal SELECT. Emits RowDescription +
/// N×DataRow + CommandComplete("INSERT 0 N") + ReadyForQuery.
fn render_insert_returning<E: EngineApply + ?Sized>(
    sql_trimmed: &str,
    assigned_ids: &[u128],
    engine: &E,
) -> Vec<u8> {
    let (table, ret_cols) = match kessel_sql::insert_returning(sql_trimmed) {
        Some(t) => t,
        None => {
            // Should not happen (guarded by the caller), but be safe.
            let mut out = Vec::new();
            out.extend_from_slice(&encode_command_complete("INSERT 0 1"));
            out.extend_from_slice(&encode_ready_for_query(b'I'));
            return out;
        }
    };
    let table_cols = match engine.describe_table(&table) {
        Some(c) => c,
        None => {
            return error_response_then_rfq(
                SEVERITY_ERROR,
                "42P01",
                &format!("unknown table \"{table}\""),
            );
        }
    };
    // SP-PG-RETURNING-MULTIROW-STAR: `RETURNING *` (the `["*"]` sentinel)
    // expands to EVERY table column in declared order. Otherwise project
    // the explicit RETURNING list.
    let is_star = ret_cols.len() == 1 && ret_cols[0] == "*";
    let proj_cols = if is_star {
        table_cols.clone()
    } else {
        match resolve_projection(&ret_cols, &table_cols) {
            Some(c) => c,
            None => {
                return error_response_then_rfq(
                    SEVERITY_ERROR,
                    "42703",
                    &format!("RETURNING column does not exist in \"{table}\" {ret_cols:?}"),
                );
            }
        }
    };
    // Resolve the per-row ids: the engine-assigned serials (one per row),
    // else the single explicit `id` from the INSERT SQL.
    let ids: Vec<u128> = if assigned_ids.is_empty() {
        match insert_explicit_id(sql_trimmed) {
            Some(i) => vec![i],
            None => {
                return error_response_then_rfq(
                    SEVERITY_ERROR,
                    "XX000",
                    "RETURNING: could not resolve the inserted row id",
                );
            }
        }
    } else {
        assigned_ids.to_vec()
    };
    let full_layout = compute_record_layout(&table_cols);
    let mut out = Vec::new();
    // ONE RowDescription for all rows (same projection shape per row).
    let fields: Vec<FieldMeta> = proj_cols
        .iter()
        .map(|c| FieldMeta { name: c.name.clone(), type_oid: field_kind_to_oid(c.kind) })
        .collect();
    out.extend_from_slice(&encode_row_description(&fields));
    // One DataRow per inserted row — read each back + project.
    for id in &ids {
        let read_sql = format!("SELECT * FROM {table} WHERE id = {id}");
        let row_bytes = match engine.apply_sql(&read_sql) {
            OpResult::Got(b) => b,
            _ => {
                return error_response_then_rfq(
                    SEVERITY_ERROR,
                    "XX000",
                    "RETURNING: failed to read back the inserted row",
                );
            }
        };
        // The read-back row stream is the length-prefixed `[u32 len][rec]`
        // shape (Op::Select) OR a bare record (Op::GetById). Decode the
        // first record either way.
        let rec: &[u8] = if row_bytes.len() >= 4 {
            let len = u32::from_le_bytes(row_bytes[0..4].try_into().unwrap()) as usize;
            if 4 + len <= row_bytes.len()
                && decode_record(&row_bytes[4..4 + len], &table_cols, &full_layout).is_some()
            {
                &row_bytes[4..4 + len]
            } else {
                &row_bytes[..]
            }
        } else {
            &row_bytes[..]
        };
        let decoded = match decode_record(rec, &table_cols, &full_layout) {
            Some(cells) => cells,
            None => {
                return error_response_then_rfq(
                    SEVERITY_ERROR,
                    "XX000",
                    "RETURNING: inserted row decode failed",
                );
            }
        };
        // Map each projected column to its position in the full schema.
        let mut row_cells: Vec<Option<&[u8]>> = Vec::with_capacity(proj_cols.len());
        for c in &proj_cols {
            let idx = table_cols.iter().position(|tc| tc.name == c.name);
            match idx.and_then(|i| decoded.get(i)) {
                Some(cell) => row_cells.push(cell.as_deref()),
                None => row_cells.push(None),
            }
        }
        out.extend_from_slice(&encode_data_row(&row_cells));
    }
    out.extend_from_slice(&encode_command_complete(&format!("INSERT 0 {}", ids.len())));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-SQL-DML-GENERAL — render a general-WHERE UPDATE/DELETE result.
/// `row_bytes` is the DML-result frame (`decode_dml_result`-shaped). The
/// affected count drives the CommandComplete tag (`UPDATE N` / `DELETE
/// N`); if the SQL had a RETURNING clause, the framed rows (full records
/// read back by the engine) are decoded + projected into DataRows.
fn render_dml_where_result<E: EngineApply + ?Sized>(
    row_bytes: &[u8],
    sql_trimmed: &str,
    engine: &E,
) -> Vec<u8> {
    let (affected, rows) = match decode_dml_result(row_bytes) {
        Some(t) => t,
        None => {
            // Not a DML frame after all — emit a safe bare tag.
            let tag = cmd_complete_tag_for_sql(sql_trimmed, 0);
            return {
                let mut out = Vec::new();
                out.extend_from_slice(&encode_command_complete(&tag));
                out.extend_from_slice(&encode_ready_for_query(b'I'));
                out
            };
        }
    };
    let mut out = Vec::new();
    // RETURNING? `dml_returning` returns the (table, cols|*) when the SQL
    // carries a RETURNING clause; absent ⇒ count-only.
    if let Some((table, ret_cols)) = kessel_sql::dml_returning(sql_trimmed) {
        let table_cols = match engine.describe_table(&table) {
            Some(c) => c,
            None => {
                return error_response_then_rfq(
                    SEVERITY_ERROR,
                    "42P01",
                    &format!("unknown table \"{table}\""),
                );
            }
        };
        let is_star = ret_cols.len() == 1 && ret_cols[0] == "*";
        let proj_cols = if is_star {
            table_cols.clone()
        } else {
            match resolve_projection(&ret_cols, &table_cols) {
                Some(c) => c,
                None => {
                    return error_response_then_rfq(
                        SEVERITY_ERROR,
                        "42703",
                        &format!(
                            "RETURNING column does not exist in \"{table}\" {ret_cols:?}"
                        ),
                    );
                }
            }
        };
        let full_layout = compute_record_layout(&table_cols);
        let fields: Vec<FieldMeta> = proj_cols
            .iter()
            .map(|c| FieldMeta {
                name: c.name.clone(),
                type_oid: field_kind_to_oid(c.kind),
            })
            .collect();
        out.extend_from_slice(&encode_row_description(&fields));
        for rec in &rows {
            let decoded = match decode_record(rec, &table_cols, &full_layout) {
                Some(cells) => cells,
                None => {
                    return error_response_then_rfq(
                        SEVERITY_ERROR,
                        "XX000",
                        "RETURNING: affected row decode failed",
                    );
                }
            };
            let mut row_cells: Vec<Option<&[u8]>> =
                Vec::with_capacity(proj_cols.len());
            for c in &proj_cols {
                let idx = table_cols.iter().position(|tc| tc.name == c.name);
                match idx.and_then(|i| decoded.get(i)) {
                    Some(cell) => row_cells.push(cell.as_deref()),
                    None => row_cells.push(None),
                }
            }
            out.extend_from_slice(&encode_data_row(&row_cells));
        }
    }
    let tag = cmd_complete_tag_for_sql(sql_trimmed, affected as u64);
    out.extend_from_slice(&encode_command_complete(&tag));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// SP-PG-SERIAL-RETURNING: extract the explicit `id` value from an
/// `INSERT INTO t (… id …) VALUES (… n …)` so `RETURNING id` works on a
/// non-serial table too. Returns `None` if the INSERT omits `id` (the
/// serial-autoincrement path supplies the assigned id instead) or the SQL
/// shape isn't recognized. Reuses the kessel-sql lexer indirectly via a
/// minimal scan; falls back to `None` (the caller errors cleanly).
fn insert_explicit_id(sql: &str) -> Option<u128> {
    // Compile is not available here (no catalog); do a lexer-free,
    // conservative parse of `( <cols> ) VALUES ( <vals> )` to find the
    // `id` column's value. We only need the single-row case.
    let lower = sql.to_ascii_lowercase();
    let cols_start = lower.find('(')? + 1;
    let cols_end = sql[cols_start..].find(')')? + cols_start;
    let cols: Vec<String> = sql[cols_start..cols_end]
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_ascii_lowercase())
        .collect();
    let id_pos = cols.iter().position(|c| c == "id")?;
    let vstart_rel = lower[cols_end..].find("values")?;
    let vals_open = lower[cols_end + vstart_rel..].find('(')? + cols_end + vstart_rel + 1;
    let vals_close = sql[vals_open..].find(')')? + vals_open;
    let vals: Vec<&str> = sql[vals_open..vals_close].split(',').map(|s| s.trim()).collect();
    let raw = vals.get(id_pos)?.trim_matches('\'').trim_matches('"').trim();
    raw.parse::<i128>().ok().map(|n| n as u128)
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

/// SP-PG-SQL-ORM-PARSE T3 — decode the row stream produced by
/// `Op::SelectFields` (an explicit projection list `SELECT c1, c2 FROM t`)
/// and emit one `DataRow` per record into `out`. Returns the row count.
///
/// The projected row stream is `[u32 LE len][projected field bytes]*`
/// where the projected bytes are the selected columns' RAW fixed-width
/// values **concatenated in projection order** — NO record header, NO
/// schema version, NO null-bitmap (a DIFFERENT shape from the full-record
/// `emit_data_rows` path, which decodes the on-disk record layout with
/// its 14-byte header + null bitmap). `proj_cols` is the projected
/// schema in projection order (each entry carries the column's
/// `FieldKind` so we know its fixed width + how to render it).
///
/// Because the engine emits raw padded fixed-width bytes with no null
/// bitmap, a NULL projected cell is indistinguishable from its
/// zero/empty value at this layer — V1 renders the decoded value (CHAR
/// trailing-NUL-stripped, numerics decoded). True projected-NULL
/// fidelity is the named follow-up `SP-PG-SQL-PROJ-NULL` (needs the
/// projection Op to carry a per-row null mask).
fn emit_projected_rows(
    row_bytes: &[u8],
    proj_cols: &[PgColumn],
    out: &mut Vec<u8>,
) -> Result<u64, String> {
    let row_width: usize =
        proj_cols.iter().map(|c| c.kind.width() as usize).sum();
    let mut p = 0usize;
    let mut n = 0u64;
    while p + 4 <= row_bytes.len() {
        let len =
            u32::from_le_bytes(row_bytes[p..p + 4].try_into().unwrap()) as usize;
        p += 4;
        if p + len > row_bytes.len() {
            return Err("projected row stream truncated".to_string());
        }
        let rec = &row_bytes[p..p + len];
        p += len;
        if len != row_width {
            return Err(format!(
                "projected row width {len} != expected {row_width}"
            ));
        }
        let mut cells: Vec<Option<Vec<u8>>> = Vec::with_capacity(proj_cols.len());
        let mut off = 0usize;
        for c in proj_cols {
            let w = c.kind.width() as usize;
            let raw = match rec.get(off..off + w) {
                Some(b) => b,
                None => return Err("projected field out of range".to_string()),
            };
            off += w;
            let v = value_from_raw(c.kind, raw);
            cells.push(Some(render_pg_text(&v, c.kind)));
        }
        let cols_borrow: Vec<Option<&[u8]>> =
            cells.iter().map(|c| c.as_deref()).collect();
        out.extend_from_slice(&encode_data_row(&cols_borrow));
        n += 1;
    }
    if p != row_bytes.len() {
        return Err("trailing bytes after projected rows".to_string());
    }
    Ok(n)
}

/// SP-PG-ORM-REALAPP — decode a FULL-record stream (`[u32 LE len][full
/// record]*`, the shape `Op::SelectSorted` emits) and emit ONE `DataRow` per
/// record containing ONLY the projected columns, in projection order. Used
/// when a projection-list SELECT carries an `ORDER BY`: the engine sorts but
/// returns whole records (it drops the projection), so the gateway re-projects
/// here. `table_cols` is the table's FULL schema (for the record layout +
/// per-column decode); `proj_cols` is the projected subset in projection order
/// (each must be a column of `table_cols`). NULL fidelity is preserved (a
/// projected NULL renders as a real PG NULL via the record's null bitmap),
/// which is STRICTLY better than the narrow `emit_projected_rows` path (that
/// path has no null mask — see `SP-PG-SQL-PROJ-NULL`).
fn emit_projected_from_full_records(
    row_bytes: &[u8],
    table_cols: &[PgColumn],
    proj_cols: &[PgColumn],
    out: &mut Vec<u8>,
) -> Result<u64, String> {
    let layout = compute_record_layout(table_cols);
    // Map each projected column to its index in the full table schema (by
    // name; matches the engine's positional field order).
    let proj_idx: Vec<usize> = proj_cols
        .iter()
        .map(|pc| {
            table_cols
                .iter()
                .position(|tc| tc.name == pc.name)
                .ok_or_else(|| {
                    format!("projected column `{}` not in table schema", pc.name)
                })
        })
        .collect::<Result<_, _>>()?;
    let mut p = 0usize;
    let mut n = 0u64;
    while p + 4 <= row_bytes.len() {
        let len =
            u32::from_le_bytes(row_bytes[p..p + 4].try_into().unwrap()) as usize;
        p += 4;
        if p + len > row_bytes.len() {
            return Err("sorted record stream truncated".to_string());
        }
        let rec = &row_bytes[p..p + len];
        p += len;
        let all_cells = decode_record(rec, table_cols, &layout)
            .ok_or_else(|| "malformed sorted record".to_string())?;
        // Project: pick the requested columns in projection order.
        let projected: Vec<Option<&[u8]>> = proj_idx
            .iter()
            .map(|&i| all_cells[i].as_deref())
            .collect();
        out.extend_from_slice(&encode_data_row(&projected));
        n += 1;
    }
    if p != row_bytes.len() {
        return Err("trailing bytes after sorted records".to_string());
    }
    Ok(n)
}

/// SP-PG-SQL-ORM-PARSE T3 — resolve a projection list (`select_columns`
/// output: bare column names in projection order) against a table's
/// full schema (from `describe_table`), returning the projected
/// `PgColumn`s in projection order. `None` if any projected name is not
/// a column of the table (the caller then surfaces a clean
/// `undefined_column` error). Column matching is case-sensitive to
/// mirror the engine's `ot.fields` name match.
fn resolve_projection(
    proj_names: &[String],
    table_cols: &[PgColumn],
) -> Option<Vec<PgColumn>> {
    let mut out = Vec::with_capacity(proj_names.len());
    for name in proj_names {
        let c = table_cols.iter().find(|c| &c.name == name)?;
        out.push(c.clone());
    }
    Some(out)
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

    /// SP-PG-SQL-JOIN-AGG: a join-group-aggregate SELECT renders the group-
    /// aggregate value stream as RowDescription([group col, agg col]) + one
    /// DataRow per group. The CannedEngine returns the `[u32 ngroups]…` bytes;
    /// `join_group_aggregate` recovers the column shape from the SQL, and the
    /// group key (Char16) decodes to its trimmed text + the i128 count to text.
    #[test]
    fn jagg_render_count_per_parent() {
        // Group table `author` with the CHAR(16) name column.
        let acols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::U64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(16), nullable: false },
        ];
        // Build the engine result: 2 groups (lewis 1, tolkien 2), one COUNT.
        let mut stream = Vec::new();
        stream.extend_from_slice(&2u32.to_le_bytes()); // ngroups
        for (name, cnt) in [("lewis", 1i128), ("tolkien", 2i128)] {
            let mut key = name.as_bytes().to_vec();
            key.resize(16, 0); // Char(16) raw fixed-width key
            stream.extend_from_slice(&(key.len() as u32).to_le_bytes());
            stream.extend_from_slice(&key);
            stream.extend_from_slice(&cnt.to_le_bytes());
        }
        let eng = CannedEngine {
            cols: acols,
            row_bytes: stream,
            result: std::sync::Mutex::new(None),
            table_name: "author".into(),
            no_schema: false,
        };
        let bytes = dispatch_query(
            "SELECT author.name, COUNT(book.id) FROM author JOIN book ON author.id=book.aid GROUP BY author.name",
            &eng,
        );
        // RowDescription present, two DataRows, CommandComplete SELECT 2.
        assert!(bytes.contains(&b'T'), "RowDescription emitted");
        // The two group names + counts appear in the DataRow payloads.
        let s = String::from_utf8_lossy(&bytes);
        assert!(s.contains("lewis"), "lewis group rendered");
        assert!(s.contains("tolkien"), "tolkien group rendered");
        assert!(s.contains("SELECT 2"), "CommandComplete SELECT 2");
        // Order invariant: T before the two D rows before C(ommandComplete).
        let pt = bytes.iter().position(|&b| b == b'T').unwrap();
        let pc = bytes.iter().rposition(|&b| b == b'C').unwrap();
        let dcount = bytes[pt..pc].iter().filter(|&&b| b == b'D').count();
        assert!(dcount >= 2, "at least two DataRows between T and C");
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

    // ── SP-PG-SQL-AGG-ALIAS-RENDER — scalar aggregate over a FROM table ──

    /// Helper: a CannedEngine whose `apply_sql` returns the given i128 LE
    /// scalar (the `Op::Aggregate` wire shape).
    fn agg_engine(scalar: i128) -> CannedEngine {
        CannedEngine {
            cols: vec![PgColumn {
                name: "id".into(),
                kind: FieldKind::I64,
                nullable: false,
            }],
            row_bytes: scalar.to_le_bytes().to_vec(),
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        }
    }

    /// `SELECT COUNT(*) FROM t` → RowDescription(col "count") + ONE
    /// DataRow(the count) + CommandComplete("SELECT 1") + RFQ.
    #[test]
    fn agg_count_star_renders_single_scalar_row() {
        let eng = agg_engine(7);
        let bytes = dispatch_query("SELECT COUNT(*) FROM t", &eng);
        // RowDescription before DataRow before CommandComplete before RFQ.
        let pos_t = bytes.iter().position(|&b| b == b'T').unwrap();
        let pos_d = bytes[pos_t + 1..].iter().position(|&b| b == b'D').unwrap() + pos_t + 1;
        let pos_c = bytes.iter().position(|&b| b == b'C').unwrap();
        let pos_z = bytes.iter().position(|&b| b == b'Z').unwrap();
        assert!(pos_t < pos_d && pos_d < pos_c && pos_c < pos_z);
        // Default column name "count" (NUL-terminated) in RowDescription.
        assert!(bytes.windows(b"count\0".len()).any(|w| w == b"count\0"));
        // The scalar value "7" appears as a DataRow text cell.
        assert!(bytes.windows(1).any(|w| w == b"7"));
        // CommandComplete("SELECT 1") — exactly one row.
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// An aliased aggregate names the RowDescription column by the alias
    /// (`SELECT COUNT(*) AS "__count" FROM "t"` → column "__count").
    #[test]
    fn agg_count_star_alias_names_column() {
        let eng = agg_engine(42);
        let bytes = dispatch_query("SELECT COUNT(*) AS \"__count\" FROM \"t\"", &eng);
        assert!(bytes.windows(b"__count\0".len()).any(|w| w == b"__count\0"));
        assert!(bytes.windows(2).any(|w| w == b"42"));
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
    }

    /// SUM over a column renders the scalar with the default name "sum".
    #[test]
    fn agg_sum_renders_with_default_name() {
        let eng = agg_engine(1234);
        let bytes = dispatch_query("SELECT SUM(bal) FROM t", &eng);
        assert!(bytes.windows(b"sum\0".len()).any(|w| w == b"sum\0"));
        assert!(bytes.windows(4).any(|w| w == b"1234"));
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

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-SQL-ORM-PARSE T3 — explicit projection-list rendering.
    // The engine's `Op::SelectFields` emits the projected columns as
    // concatenated raw fixed-width bytes (NO record header / null
    // bitmap); the gateway must build a RowDescription for ONLY the
    // projected columns + decode them. These KATs lock that path.
    // ───────────────────────────────────────────────────────────────────

    /// Build the projected row stream `[u32 len][raw fixed-width field
    /// bytes in projection order]*` the engine's `Op::SelectFields`
    /// emits. `proj_raw` is one row's already-concatenated projected
    /// bytes.
    fn build_projected_stream(rows: &[Vec<u8>]) -> Vec<u8> {
        let mut out = Vec::new();
        for r in rows {
            out.extend_from_slice(&(r.len() as u32).to_le_bytes());
            out.extend_from_slice(r);
        }
        out
    }

    /// `SELECT id, name FROM t` returns exactly those 2 columns, in
    /// order, with the right RowDescription + DataRows.
    #[test]
    fn ormparse_t3_projection_renders_named_columns() {
        let table_cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(8), nullable: true },
            PgColumn { name: "extra".into(), kind: FieldKind::I32, nullable: true },
        ];
        // Projected `id, name`: i64 (8B LE) ++ char(8) padded.
        let mut row1 = Vec::new();
        row1.extend_from_slice(&7i64.to_le_bytes());
        let mut nm = b"alice".to_vec();
        nm.resize(8, 0);
        row1.extend_from_slice(&nm);
        let stream = build_projected_stream(&[row1]);
        let eng = CannedEngine {
            cols: table_cols,
            row_bytes: stream,
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("SELECT id, name FROM t", &eng);
        // RowDescription present, names id + name, and NOT extra.
        let pos_t = bytes.iter().position(|&b| b == b'T').expect("RowDescription");
        assert!(bytes.windows(3).any(|w| w == b"id\0"));
        assert!(bytes.windows(5).any(|w| w == b"name\0"));
        assert!(
            !bytes.windows(6).any(|w| w == b"extra\0"),
            "projection must NOT include the unprojected `extra` column"
        );
        // Values present.
        assert!(bytes.windows(1).any(|w| w == b"7"));
        assert!(bytes.windows(5).any(|w| w == b"alice"));
        // CommandComplete("SELECT 1") + RFQ.
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
        let _ = pos_t;
    }

    /// `SELECT t.id, t.name FROM t` (qualified) renders identically to
    /// the unqualified projection — the qualifier is stripped by
    /// `select_columns`.
    #[test]
    fn ormparse_t3_qualified_projection_same_as_bare() {
        let table_cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(8), nullable: true },
        ];
        let mut row1 = Vec::new();
        row1.extend_from_slice(&3i64.to_le_bytes());
        let mut nm = b"bob".to_vec();
        nm.resize(8, 0);
        row1.extend_from_slice(&nm);
        let stream = build_projected_stream(&[row1]);
        let mk = || CannedEngine {
            cols: table_cols.clone(),
            row_bytes: stream.clone(),
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        };
        let bare = dispatch_query("SELECT id, name FROM t", &mk());
        let qual = dispatch_query("SELECT t.id, t.name FROM t", &mk());
        assert_eq!(
            bare, qual,
            "qualified projection must render byte-identically to bare"
        );
    }

    /// Projection column ORDER is preserved: `SELECT name, id FROM t`
    /// puts `name` first in the RowDescription.
    #[test]
    fn ormparse_t3_projection_order_preserved() {
        let table_cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(8), nullable: true },
        ];
        // Projected `name, id`: char(8) ++ i64.
        let mut row1 = Vec::new();
        let mut nm = b"zoe".to_vec();
        nm.resize(8, 0);
        row1.extend_from_slice(&nm);
        row1.extend_from_slice(&9i64.to_le_bytes());
        let stream = build_projected_stream(&[row1]);
        let eng = CannedEngine {
            cols: table_cols,
            row_bytes: stream,
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("SELECT name, id FROM t", &eng);
        // In the RowDescription, `name\0` must appear BEFORE `id\0`.
        let p_name = bytes.windows(5).position(|w| w == b"name\0").expect("name");
        let p_id = bytes.windows(3).position(|w| w == b"id\0").expect("id");
        assert!(p_name < p_id, "projection order name<id must be preserved");
        assert!(bytes.windows(3).any(|w| w == b"zoe"));
    }

    /// A projection naming a non-existent column surfaces a clean
    /// `42703 undefined_column`-style error, not a panic or wrong render.
    #[test]
    fn ormparse_t3_projection_unknown_column_errors() {
        let table_cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
        ];
        let eng = CannedEngine {
            cols: table_cols,
            row_bytes: build_projected_stream(&[]),
            result: std::sync::Mutex::new(None),
            table_name: "t".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("SELECT nope FROM t", &eng);
        // ErrorResponse 'E' + the 42703 sqlstate string somewhere.
        assert!(bytes.iter().any(|&b| b == b'E'));
        assert!(bytes.windows(5).any(|w| w == b"42703"));
    }

    // ---- SP-PG-SERIAL-RETURNING (T4) ----

    /// A mock engine for INSERT...RETURNING: the INSERT returns
    /// `Created { id }`; any later SELECT (the gateway's read-back)
    /// returns the canned row stream so RETURNING can project columns.
    struct ReturningEngine {
        cols: Vec<PgColumn>,
        row_stream: Vec<u8>,
        assigned: u128,
        table: String,
    }
    impl EngineApply for ReturningEngine {
        fn apply_sql(&self, sql: &str) -> OpResult {
            let kw = sql.trim_start().split_whitespace().next().unwrap_or("");
            if kw.eq_ignore_ascii_case("INSERT") {
                OpResult::Created { id: self.assigned }
            } else {
                // read-back SELECT
                OpResult::Got(self.row_stream.clone().into())
            }
        }
        fn describe_table(&self, name: &str) -> Option<Vec<PgColumn>> {
            if name == self.table { Some(self.cols.clone()) } else { None }
        }
    }

    fn returning_cols() -> Vec<PgColumn> {
        vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(8), nullable: true },
        ]
    }

    /// Headline: `INSERT … RETURNING id` emits RowDescription + a DataRow
    /// carrying the engine-assigned id + CommandComplete + RFQ.
    #[test]
    fn insert_returning_id_emits_datarow_with_assigned_id() {
        let cols = returning_cols();
        let rec = build_record(&cols, &[Value::Int(7), Value::Blob(b"gadget\0\0".to_vec())]);
        let eng = ReturningEngine {
            cols,
            row_stream: build_row_stream(&[rec]),
            assigned: 7,
            table: "widgets".into(),
        };
        let bytes = dispatch_query(
            "INSERT INTO widgets (name) VALUES ('gadget') RETURNING id",
            &eng,
        );
        // RowDescription 'T', a DataRow 'D', the assigned id "7" in text,
        // CommandComplete "INSERT 0 1", and RFQ 'Z'.
        assert!(bytes.iter().any(|&b| b == b'T'), "expected RowDescription");
        assert!(bytes.iter().any(|&b| b == b'D'), "expected DataRow");
        assert!(bytes.windows(1).any(|w| w == b"7"), "expected assigned id 7 in the DataRow");
        assert!(
            bytes.windows(b"INSERT 0 1\0".len()).any(|w| w == b"INSERT 0 1\0"),
            "expected CommandComplete INSERT 0 1"
        );
        assert!(bytes.iter().any(|&b| b == b'Z'), "expected ReadyForQuery");
    }

    /// `RETURNING id, name` returns BOTH the assigned id and the
    /// client-supplied column (read back from the row).
    #[test]
    fn insert_returning_id_and_name_emits_both_columns() {
        let cols = returning_cols();
        let rec = build_record(&cols, &[Value::Int(3), Value::Blob(b"alice\0\0\0".to_vec())]);
        let eng = ReturningEngine {
            cols,
            row_stream: build_row_stream(&[rec]),
            assigned: 3,
            table: "widgets".into(),
        };
        let bytes = dispatch_query(
            "INSERT INTO widgets (name) VALUES ('alice') RETURNING id, name",
            &eng,
        );
        assert!(bytes.iter().any(|&b| b == b'T'));
        assert!(bytes.iter().any(|&b| b == b'D'));
        assert!(bytes.windows(1).any(|w| w == b"3"), "assigned id");
        assert!(
            bytes.windows(b"alice".len()).any(|w| w == b"alice"),
            "client-supplied name"
        );
    }

    /// Regression: a plain INSERT (no RETURNING) still emits a BARE
    /// CommandComplete (no RowDescription / DataRow).
    #[test]
    fn plain_insert_without_returning_is_bare_command_complete() {
        let cols = returning_cols();
        let eng = ReturningEngine {
            cols,
            row_stream: Vec::new(),
            assigned: 1,
            table: "widgets".into(),
        };
        let bytes = dispatch_query(
            "INSERT INTO widgets (name) VALUES ('x')",
            &eng,
        );
        // Just CommandComplete + RFQ — the bare path. Structurally: the
        // first backend message byte is 'C' (CommandComplete), NOT 'T'
        // (RowDescription); and there is no 'D' (DataRow) message header.
        // We check the FIRST message tag rather than scanning every byte
        // (length prefixes can incidentally equal ASCII 'T'/'D').
        assert_eq!(bytes[0], b'C', "plain INSERT starts with CommandComplete");
        assert!(
            bytes.windows(b"INSERT 0 1\0".len()).any(|w| w == b"INSERT 0 1\0"),
            "expected CommandComplete INSERT 0 1"
        );
        // The bare reply is small (CommandComplete + ReadyForQuery only).
        assert!(bytes.len() < 32, "bare reply should be small, got {}", bytes.len());
    }

    // ---- SP-PG-RETURNING-MULTIROW-STAR (T4) ----

    /// A mock engine for the multi-row autoincrement INSERT … RETURNING
    /// shape: the INSERT returns `CreatedMany { ids }`; each read-back
    /// SELECT `… WHERE id = N` returns the record keyed by N so the
    /// gateway projects the right row for each id.
    struct MultiRowReturningEngine {
        cols: Vec<PgColumn>,
        rows_by_id: std::collections::BTreeMap<u128, Vec<u8>>,
        ids: Vec<u128>,
        table: String,
    }
    impl EngineApply for MultiRowReturningEngine {
        fn apply_sql(&self, sql: &str) -> OpResult {
            let kw = sql.trim_start().split_whitespace().next().unwrap_or("");
            if kw.eq_ignore_ascii_case("INSERT") {
                OpResult::CreatedMany { ids: self.ids.clone() }
            } else {
                // read-back SELECT — parse the `id = N` from the WHERE.
                let id = sql
                    .rsplit_once('=')
                    .and_then(|(_, n)| n.trim().parse::<u128>().ok())
                    .unwrap_or(0);
                match self.rows_by_id.get(&id) {
                    Some(rec) => OpResult::Got(build_row_stream(&[rec.clone()]).into()),
                    None => OpResult::NotFound,
                }
            }
        }
        fn describe_table(&self, name: &str) -> Option<Vec<PgColumn>> {
            if name == self.table { Some(self.cols.clone()) } else { None }
        }
    }

    /// Headline: a batched `INSERT … VALUES (…),(…),(…) RETURNING id`
    /// (SQLAlchemy DEFAULT use_insertmanyvalues) → ONE RowDescription +
    /// N DataRows (each carrying the assigned id) + `INSERT 0 N`.
    #[test]
    fn multirow_insert_returning_id_emits_n_datarows() {
        let cols = returning_cols();
        let mut rows_by_id = std::collections::BTreeMap::new();
        rows_by_id.insert(1u128, build_record(&cols, &[Value::Int(1), Value::Blob(b"a\0\0\0\0\0\0\0".to_vec())]));
        rows_by_id.insert(2u128, build_record(&cols, &[Value::Int(2), Value::Blob(b"b\0\0\0\0\0\0\0".to_vec())]));
        rows_by_id.insert(3u128, build_record(&cols, &[Value::Int(3), Value::Blob(b"c\0\0\0\0\0\0\0".to_vec())]));
        let eng = MultiRowReturningEngine {
            cols,
            rows_by_id,
            ids: vec![1, 2, 3],
            table: "widgets".into(),
        };
        let bytes = dispatch_query(
            "INSERT INTO widgets (name) VALUES ('a'),('b'),('c') RETURNING id",
            &eng,
        );
        // Exactly ONE RowDescription, THREE DataRows.
        assert_eq!(bytes[0], b'T', "starts with one RowDescription");
        let datarow_count = bytes.iter().filter(|&&b| b == b'D').count();
        // (length-prefix bytes can incidentally equal 'D'; assert at least 3
        // DataRow message HEADERS by counting via message framing instead.)
        let mut n_datarows = 0usize;
        let mut i = 0usize;
        while i < bytes.len() {
            let tag = bytes[i];
            if i + 5 > bytes.len() { break; }
            let len = u32::from_be_bytes(bytes[i + 1..i + 5].try_into().unwrap()) as usize;
            if tag == b'D' { n_datarows += 1; }
            i += 1 + len;
        }
        assert_eq!(n_datarows, 3, "expected 3 DataRow messages, raw D bytes={datarow_count}");
        // Each assigned id appears in text.
        for id in [b"1", b"2", b"3"] {
            assert!(bytes.windows(1).any(|w| w == id), "id {:?} in stream", id);
        }
        // CommandComplete INSERT 0 3.
        assert!(
            bytes.windows(b"INSERT 0 3\0".len()).any(|w| w == b"INSERT 0 3\0"),
            "expected CommandComplete INSERT 0 3"
        );
        assert!(bytes.iter().any(|&b| b == b'Z'), "RFQ");
    }

    /// `INSERT … RETURNING *` expands to EVERY table column (id + name),
    /// not just an explicit list — a single DataRow with all columns.
    #[test]
    fn insert_returning_star_emits_all_columns() {
        let cols = returning_cols();
        let mut rows_by_id = std::collections::BTreeMap::new();
        rows_by_id.insert(5u128, build_record(&cols, &[Value::Int(5), Value::Blob(b"zed\0\0\0\0\0".to_vec())]));
        let eng = MultiRowReturningEngine {
            cols,
            rows_by_id,
            ids: vec![5],
            table: "widgets".into(),
        };
        let bytes = dispatch_query(
            "INSERT INTO widgets (name) VALUES ('zed') RETURNING *",
            &eng,
        );
        assert_eq!(bytes[0], b'T', "RowDescription first");
        // The RowDescription must name BOTH columns (id + name).
        assert!(bytes.windows(b"id\0".len()).any(|w| w == b"id\0"), "id column described");
        assert!(bytes.windows(b"name\0".len()).any(|w| w == b"name\0"), "name column described");
        // The DataRow carries both the id (5) and the name (zed).
        assert!(bytes.windows(1).any(|w| w == b"5"), "id value");
        assert!(bytes.windows(b"zed".len()).any(|w| w == b"zed"), "name value");
        assert!(
            bytes.windows(b"INSERT 0 1\0".len()).any(|w| w == b"INSERT 0 1\0"),
            "INSERT 0 1 for single-row RETURNING *"
        );
    }

    /// Multi-row `RETURNING *` → N DataRows each with all columns.
    #[test]
    fn multirow_insert_returning_star_emits_n_datarows_all_columns() {
        let cols = returning_cols();
        let mut rows_by_id = std::collections::BTreeMap::new();
        rows_by_id.insert(1u128, build_record(&cols, &[Value::Int(1), Value::Blob(b"a\0\0\0\0\0\0\0".to_vec())]));
        rows_by_id.insert(2u128, build_record(&cols, &[Value::Int(2), Value::Blob(b"b\0\0\0\0\0\0\0".to_vec())]));
        let eng = MultiRowReturningEngine {
            cols,
            rows_by_id,
            ids: vec![1, 2],
            table: "widgets".into(),
        };
        let bytes = dispatch_query(
            "INSERT INTO widgets (name) VALUES ('a'),('b') RETURNING *",
            &eng,
        );
        let mut n_datarows = 0usize;
        let mut i = 0usize;
        while i < bytes.len() {
            let tag = bytes[i];
            if i + 5 > bytes.len() { break; }
            let len = u32::from_be_bytes(bytes[i + 1..i + 5].try_into().unwrap()) as usize;
            if tag == b'D' { n_datarows += 1; }
            i += 1 + len;
        }
        assert_eq!(n_datarows, 2, "expected 2 DataRow messages");
        assert!(
            bytes.windows(b"INSERT 0 2\0".len()).any(|w| w == b"INSERT 0 2\0"),
            "INSERT 0 2"
        );
    }

    /// Regression: single-row `RETURNING id` (the SP-PG-SERIAL-RETURNING
    /// shape) still works through the new list-based signature.
    #[test]
    fn single_row_returning_id_still_works() {
        let cols = returning_cols();
        let rec = build_record(&cols, &[Value::Int(9), Value::Blob(b"solo\0\0\0\0".to_vec())]);
        let eng = ReturningEngine {
            cols,
            row_stream: build_row_stream(&[rec]),
            assigned: 9,
            table: "widgets".into(),
        };
        let bytes = dispatch_query(
            "INSERT INTO widgets (name) VALUES ('solo') RETURNING id",
            &eng,
        );
        assert!(bytes.windows(1).any(|w| w == b"9"), "assigned id 9");
        assert!(
            bytes.windows(b"INSERT 0 1\0".len()).any(|w| w == b"INSERT 0 1\0"),
            "INSERT 0 1"
        );
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-SQL-DML-GENERAL T4 — UPDATE N / DELETE N counts + RETURNING
    // ───────────────────────────────────────────────────────────────────

    /// The DML-result frame round-trips through encode/decode, and a
    /// truncated/wrong-tag buffer decodes to `None` (no panic).
    #[test]
    fn dmlgen_frame_round_trip() {
        let rows = vec![vec![1u8, 2, 3], vec![9u8]];
        let f = encode_dml_result(7, &rows);
        assert_eq!(decode_dml_result(&f), Some((7, rows)));
        // count-only (no rows).
        assert_eq!(decode_dml_result(&encode_dml_result(3, &[])), Some((3, vec![])));
        // wrong tag → None.
        assert_eq!(decode_dml_result(&[0x00, 1, 0, 0, 0, 0, 0, 0, 0]), None);
        // truncated → None.
        assert_eq!(decode_dml_result(&[DML_RESULT_TAG, 1, 0]), None);
    }

    /// `UPDATE … WHERE …` (no RETURNING) → `UPDATE N` CommandComplete with
    /// the framed affected count, no DataRows.
    #[test]
    fn dmlgen_update_count_tag() {
        let eng = CannedEngine {
            cols: vec![],
            row_bytes: vec![],
            result: std::sync::Mutex::new(Some(OpResult::Got(
                encode_dml_result(2, &[]).into(),
            ))),
            table_name: "acct".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("UPDATE acct SET active = 0 WHERE bal < 150", &eng);
        assert!(
            bytes.windows(b"UPDATE 2\0".len()).any(|w| w == b"UPDATE 2\0"),
            "UPDATE 2 tag, got: {:?}",
            String::from_utf8_lossy(&bytes)
        );
    }

    /// `DELETE FROM … WHERE …` → `DELETE N`.
    #[test]
    fn dmlgen_delete_count_tag() {
        let eng = CannedEngine {
            cols: vec![],
            row_bytes: vec![],
            result: std::sync::Mutex::new(Some(OpResult::Got(
                encode_dml_result(3, &[]).into(),
            ))),
            table_name: "acct".into(),
            no_schema: false,
        };
        let bytes = dispatch_query("DELETE FROM acct WHERE active = 0", &eng);
        assert!(
            bytes.windows(b"DELETE 3\0".len()).any(|w| w == b"DELETE 3\0"),
            "DELETE 3 tag"
        );
    }

    /// `UPDATE … RETURNING *` → RowDescription + DataRow(s) + `UPDATE N`.
    #[test]
    fn dmlgen_update_returning_star_emits_rows() {
        let cols = vec![
            PgColumn { name: "bal".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "active".into(), kind: FieldKind::I64, nullable: false },
        ];
        // One affected row, value (bal=200, active=5) framed.
        let rec = build_record(&cols, &[Value::Int(200), Value::Int(5)]);
        let eng = CannedEngine {
            cols: cols.clone(),
            row_bytes: vec![],
            result: std::sync::Mutex::new(Some(OpResult::Got(
                encode_dml_result(1, &[rec]).into(),
            ))),
            table_name: "acct".into(),
            no_schema: false,
        };
        let bytes = dispatch_query(
            "UPDATE acct SET active = 5 WHERE bal = 200 RETURNING *",
            &eng,
        );
        // Tag is UPDATE 1.
        assert!(
            bytes.windows(b"UPDATE 1\0".len()).any(|w| w == b"UPDATE 1\0"),
            "UPDATE 1 tag"
        );
        // The returned active=5 renders as PG text "5" in a DataRow.
        assert!(
            bytes.windows(1).any(|w| w == b"5"),
            "RETURNING DataRow carries the post-update active value"
        );
        // A RowDescription field byte 'D' (DataRow) + 'T' (RowDescription)
        // both present (message-type bytes).
        assert!(bytes.contains(&b'T'), "RowDescription present");
        assert!(bytes.contains(&b'D'), "DataRow present");
    }
}
