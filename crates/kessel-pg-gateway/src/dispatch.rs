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

/// Run a single Simple Query end-to-end. Returns the full byte
/// sequence to emit to the wire (one or more PG backend messages
/// concatenated, ending with `ReadyForQuery('I')`).
///
/// The caller (the query loop in `server::run_session`) writes the
/// returned bytes to the TCP stream verbatim.
pub fn dispatch_query<E: EngineApply + ?Sized>(sql: &str, engine: &E) -> Vec<u8> {
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
    // Strip a trailing `;` so the leading-keyword heuristic + the
    // SELECT * FROM table lookup don't trip over the terminator.
    let sql_trimmed = sql.trim().trim_end_matches(';').trim();

    // Is this a `SELECT * FROM <table>` that we can render with full
    // RowDescription? `kessel-sql::select_star_table` is the lexer-
    // backed detector — returns Some(table_name) only on the
    // V1-supported whole-row shape (no projection list, no JOIN).
    let select_table = kessel_sql::select_star_table(sql_trimmed);

    let result = engine.apply_sql(sql_trimmed);
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
            // Non-SELECT success. Pick the CommandComplete tag by
            // inspecting the SQL's leading keyword. V1 doesn't yet
            // surface row counts for INSERT/UPDATE/DELETE because
            // `apply_sql` returns `Ok` without a count — the tag
            // emits 0 rows. T9 will polish this.
            let tag = infer_command_tag(sql_trimmed, 0);
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
                .unwrap_or(OpResult::Got(self.row_bytes.clone()))
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

    /// INSERT INTO ... → CommandComplete("INSERT 0 0") + RFQ. V1
    /// reports 0 rows (T9 polish).
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
        assert!(bytes.windows(b"INSERT 0 0\0".len()).any(|w| w == b"INSERT 0 0\0"));
        // Last 6 bytes are RFQ.
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// DELETE FROM ... → CommandComplete("DELETE 0") + RFQ.
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
        assert!(bytes.windows(b"DELETE 0\0".len()).any(|w| w == b"DELETE 0\0"));
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
}
