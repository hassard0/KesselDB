//! SP-PG-COPY — COPY FROM STDIN + COPY TO STDOUT dispatchers.
//!
//! **T2 (COPY FROM)** ships the COPY FROM STDIN start + CopyData
//! processing + CopyDone/CopyFail finalize functions.
//!
//! **T3 (COPY TO)** ships the COPY TO STDOUT inline-streaming
//! dispatcher.
//!
//! Both dispatchers are gateway-side wrappers around the existing
//! `dispatch::dispatch_query` Simple Query path — the engine
//! interface stays byte-untouched. COPY FROM rows synthesize one
//! `INSERT INTO <table> [(cols)] VALUES (...)` per parsed row and
//! send it through `dispatch_query` (which already handles INSERT
//! with the full SQLSTATE + row-count + error-response shape). COPY
//! TO drives the existing `SELECT * FROM <table>` path under the
//! hood and reframes the resulting DataRow stream as CopyData.

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::copy::command::{ParsedCopy, RejectReason};
use crate::copy::proto::{
    copy_tag, encode_copy_data, encode_copy_done, encode_copy_in_response,
    encode_copy_out_response,
};
use crate::copy::text::{encode_text_row, is_end_of_data_marker, parse_text_row_bytes};
use crate::copy::{CopyInState, CopyState, MAX_COPY_DATA_BUFFER};
use crate::dispatch;
use crate::engine::EngineApply;
use crate::error::{encode_error_response, SEVERITY_ERROR};
use crate::response::{encode_command_complete, encode_ready_for_query};

/// Outcome of starting a COPY FROM STDIN exchange.
#[derive(Debug)]
pub enum CopyInStartOutcome {
    /// Success — emit the bytes verbatim, then enter `CopyState::In`
    /// (the new state) and await CopyData / CopyDone / CopyFail.
    Started { bytes: Vec<u8>, state: CopyInState },
    /// Failure — emit the bytes verbatim (an ErrorResponse + RFQ);
    /// connection STAYS in Idle (no state change, caller doesn't
    /// transition).
    Failed { bytes: Vec<u8> },
}

/// Try to start a COPY FROM STDIN exchange.
///
/// Inputs:
/// - `parsed` — the recognized `ParsedCopy::From { table, columns }`.
/// - `engine` — the `EngineApply` for `describe_table` (column count
///   lookup).
///
/// Behavior:
/// 1. `engine.describe_table(&parsed.table)` — if `None`, emit
///    `ErrorResponse 42P01 undefined_table` + RFQ. Caller stays in
///    Idle.
/// 2. If `columns` was supplied, validate each column exists in the
///    schema; otherwise default to all columns in declared order.
///    Mismatch → 42703 `undefined_column`.
/// 3. Emit `CopyInResponse(G)` with `format=0` and column count =
///    chosen-columns.len(); return `CopyInStartOutcome::Started` so
///    the caller transitions to `CopyState::In(CopyInState::new(...))`.
pub fn dispatch_copy_in_start<E: EngineApply + ?Sized>(
    parsed: ParsedCopy,
    engine: &E,
) -> CopyInStartOutcome {
    let (table, columns) = match parsed {
        ParsedCopy::From { table, columns } => (table, columns),
        ParsedCopy::To { .. } => {
            // Defensive — the caller should have routed COPY TO via
            // `dispatch_copy_to`. Return a 0A000 if it didn't.
            return CopyInStartOutcome::Failed {
                bytes: error_response_then_rfq(
                    "0A000",
                    "internal error: COPY TO routed through COPY FROM dispatcher",
                ),
            };
        }
        ParsedCopy::Rejected { reason } => {
            return CopyInStartOutcome::Failed {
                bytes: error_response_then_rfq(reject_sqlstate(&reason), &reject_message(&reason)),
            };
        }
    };

    let schema_cols = match engine.describe_table(&table) {
        Some(c) => c,
        None => {
            return CopyInStartOutcome::Failed {
                bytes: error_response_then_rfq(
                    "42P01",
                    &format!("relation \"{table}\" does not exist"),
                ),
            };
        }
    };
    // Validate the supplied column list (if any) against the schema.
    let chosen_columns: Vec<String> = match columns.as_ref() {
        Some(cols) => {
            for c in cols {
                if !schema_cols.iter().any(|s| s.name == *c) {
                    return CopyInStartOutcome::Failed {
                        bytes: error_response_then_rfq(
                            "42703",
                            &format!(
                                "column \"{c}\" of relation \"{table}\" does not exist"
                            ),
                        ),
                    };
                }
            }
            cols.clone()
        }
        None => schema_cols.iter().map(|s| s.name.clone()).collect(),
    };
    let ncols = chosen_columns.len() as u16;
    let bytes = encode_copy_in_response(ncols);
    let state = CopyInState::new(table, Some(chosen_columns), ncols);
    CopyInStartOutcome::Started { bytes, state }
}

/// Outcome of processing one CopyData frame.
#[derive(Debug)]
pub enum CopyDataOutcome {
    /// Frame processed successfully (one or more rows ingested into
    /// the carry/INSERT pipeline). Caller writes nothing to the wire
    /// (PG semantics: server is silent during COPY FROM until
    /// CopyDone). `state` is the updated CopyInState (carry may have
    /// shrunk or grown; rows_ingested may have advanced).
    Continue { state: CopyInState },
    /// A row failed to parse or INSERT. Caller emits the bytes
    /// (an ErrorResponse + RFQ) AND transitions back to
    /// `CopyState::Idle` (the COPY exchange is aborted; per PG, any
    /// rows already committed STAY committed — V1 docs the
    /// divergence vs PG's all-or-nothing default; V2 SP-PG-COPY-
    /// BULKAPPLY restores PG semantics).
    Failed { bytes: Vec<u8> },
}

/// Process one `CopyData` frame.
///
/// Walks the input bytes (with the existing carry prepended),
/// splitting on `\n`. For each complete row:
/// - skip if the row is the `\.` end-of-data marker;
/// - else parse via `parse_text_row_bytes`;
/// - else synthesize `INSERT INTO <table> [(cols)] VALUES (...)`;
/// - dispatch through `dispatch::dispatch_query`;
/// - if the dispatch response contains an ErrorResponse,
///   surface the bytes + abort the COPY.
///
/// The trailing incomplete row (if any) goes back into the carry
/// buffer for the next CopyData frame.
pub fn process_copy_data<E: EngineApply + ?Sized>(
    data: &[u8],
    mut state: CopyInState,
    engine: &E,
) -> CopyDataOutcome {
    // Cap the carry+data combined size to avoid unbounded buffering.
    if state.carry.len() + data.len() > MAX_COPY_DATA_BUFFER {
        return CopyDataOutcome::Failed {
            bytes: error_response_then_rfq(
                "54000",
                "COPY row exceeds maximum buffer size",
            ),
        };
    }
    state.carry.extend_from_slice(data);

    let mut start = 0usize;
    let bytes = std::mem::take(&mut state.carry);
    let mut consumed = 0usize;
    while let Some(nl_offset) = bytes[start..].iter().position(|&b| b == b'\n') {
        let abs_nl = start + nl_offset;
        // Tolerate \r\n by trimming a trailing \r before the \n.
        let line_end = if abs_nl > start && bytes[abs_nl - 1] == b'\r' {
            abs_nl - 1
        } else {
            abs_nl
        };
        let line = &bytes[start..line_end];
        consumed = abs_nl + 1;
        start = consumed;

        if is_end_of_data_marker(line) {
            // Skip the legacy v2 marker.
            continue;
        }
        match parse_text_row_bytes(line, state.column_count as usize) {
            Ok(fields) => {
                let sql = match synthesize_insert_sql(
                    &state.table,
                    state.columns.as_deref(),
                    &fields,
                ) {
                    Ok(s) => s,
                    Err(reason) => {
                        return CopyDataOutcome::Failed {
                            bytes: error_response_then_rfq(
                                "22023",
                                &format!(
                                    "COPY row {} encode failed: {reason}",
                                    state.rows_ingested + 1
                                ),
                            ),
                        };
                    }
                };
                let resp = dispatch::dispatch_query(&sql, engine);
                // Inspect the response for an ErrorResponse.
                if let Some((sqlstate, msg)) = extract_error_response(&resp) {
                    return CopyDataOutcome::Failed {
                        bytes: error_response_then_rfq(
                            &sqlstate,
                            &format!(
                                "COPY row {}: {msg}",
                                state.rows_ingested + 1
                            ),
                        ),
                    };
                }
                state.rows_ingested += 1;
            }
            Err(e) => {
                return CopyDataOutcome::Failed {
                    bytes: error_response_then_rfq(
                        "22023",
                        &format!(
                            "COPY row {} parse failed: {e:?}",
                            state.rows_ingested + 1
                        ),
                    ),
                };
            }
        }
    }
    // Save the trailing incomplete bytes back into the carry.
    state.carry = bytes[consumed..].to_vec();
    CopyDataOutcome::Continue { state }
}

/// Finalize a successful COPY FROM STDIN exchange (CopyDone received).
/// Emits `CommandComplete("COPY N")` + `ReadyForQuery('I')`. The
/// caller transitions back to `CopyState::Idle`.
pub fn finalize_copy_in_success(state: &CopyInState) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_command_complete(&copy_tag(state.rows_ingested)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// Finalize an aborted COPY FROM STDIN exchange (CopyFail received).
/// Emits `ErrorResponse 57014 query_canceled` (with the client's
/// reason) + `ReadyForQuery('I')`. The caller transitions back to
/// `CopyState::Idle`.
pub fn finalize_copy_in_failure(reason: &str) -> Vec<u8> {
    error_response_then_rfq(
        "57014",
        &format!("COPY from stdin failed: {reason}"),
    )
}

/// Run a full `COPY <table> TO STDOUT` exchange inline.
///
/// Returns the FULL byte sequence to emit to the wire:
/// `CopyOutResponse(H) + N×CopyData(d) + CopyDone(c) +
/// CommandComplete("COPY N") + ReadyForQuery('I')`.
///
/// V1 buffers all rows via the existing `dispatch::dispatch_query`
/// path (`SELECT * FROM <table>`) and then reframes each row's
/// DataRow as a CopyData. Spec §11 weak-spot #5 (memory bound)
/// names V2 SP-A T14 streaming for the streaming-cursor variant.
pub fn dispatch_copy_to<E: EngineApply + ?Sized>(
    parsed: ParsedCopy,
    engine: &E,
) -> Vec<u8> {
    let (table, columns) = match parsed {
        ParsedCopy::To { table, columns } => (table, columns),
        ParsedCopy::From { .. } => {
            return error_response_then_rfq(
                "0A000",
                "internal error: COPY FROM routed through COPY TO dispatcher",
            );
        }
        ParsedCopy::Rejected { reason } => {
            return error_response_then_rfq(
                reject_sqlstate(&reason),
                &reject_message(&reason),
            );
        }
    };

    let schema_cols = match engine.describe_table(&table) {
        Some(c) => c,
        None => {
            return error_response_then_rfq(
                "42P01",
                &format!("relation \"{table}\" does not exist"),
            );
        }
    };
    // Validate supplied column list against the schema.
    let chosen_indices: Vec<usize> = match columns.as_ref() {
        Some(cols) => {
            let mut out = Vec::with_capacity(cols.len());
            for c in cols {
                match schema_cols.iter().position(|s| s.name == *c) {
                    Some(idx) => out.push(idx),
                    None => {
                        return error_response_then_rfq(
                            "42703",
                            &format!(
                                "column \"{c}\" of relation \"{table}\" does not exist"
                            ),
                        );
                    }
                }
            }
            out
        }
        None => (0..schema_cols.len()).collect(),
    };
    let ncols = chosen_indices.len() as u16;

    // Drive the SELECT * FROM <table> path through dispatch_query
    // and pull the row text-bytes out of the DataRow frames.
    let select_sql = format!("SELECT * FROM {}", table);
    let resp = dispatch::dispatch_query(&select_sql, engine);

    if let Some((sqlstate, msg)) = extract_error_response(&resp) {
        return error_response_then_rfq(&sqlstate, &msg);
    }

    // Parse the dispatch_query response: RowDescription + N DataRow +
    // CommandComplete + RFQ. Extract per-row, per-column text bytes
    // from the DataRow frames.
    let rows = extract_data_rows(&resp);

    // Build CopyOutResponse + N × CopyData + CopyDone +
    // CommandComplete("COPY N") + RFQ.
    let mut out = Vec::new();
    out.extend_from_slice(&encode_copy_out_response(ncols));
    let mut emitted = 0u64;
    for row in &rows {
        // Project the chosen columns from the full row.
        let projected: Vec<Option<&[u8]>> = chosen_indices
            .iter()
            .map(|&i| row.get(i).cloned().flatten())
            .collect();
        // We need the projected to own slices into `row`, not into a
        // temporary — flatten by reborrow.
        let projected_refs: Vec<Option<&[u8]>> = chosen_indices
            .iter()
            .map(|&i| row.get(i).and_then(|c| c.as_deref()))
            .collect();
        // Drop the wrongly-constructed `projected` (it borrowed
        // `cloned`). The Vec was only built to satisfy the type
        // checker exploration; we re-use `projected_refs` below.
        let _ = projected;
        let payload = encode_text_row(&projected_refs);
        out.extend_from_slice(&encode_copy_data(&payload));
        emitted += 1;
    }
    out.extend_from_slice(&encode_copy_done());
    out.extend_from_slice(&encode_command_complete(&copy_tag(emitted)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

/// Apply a `ParsedCopy` outcome at the dispatch boundary. Used by the
/// Q-dispatch branch in `server::run_session` to decide whether the
/// SQL is a COPY (and route accordingly), a rejected COPY (emit
/// error), or non-COPY (fall through to the existing dispatch).
///
/// Convenience over manual `match` to keep `server::run_session`
/// readable.
pub fn handle_copy_in_dispatch<E: EngineApply + ?Sized>(
    parsed: ParsedCopy,
    engine: &E,
) -> CopyInStartOutcome {
    dispatch_copy_in_start(parsed, engine)
}

// ─── Helpers ──────────────────────────────────────────────────────────

fn error_response_then_rfq(sqlstate: &str, message: &str) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&encode_error_response(SEVERITY_ERROR, sqlstate, message));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    out
}

fn reject_sqlstate(reason: &RejectReason) -> &'static str {
    match reason {
        // Format / source rejections are V1 capability gaps.
        RejectReason::BinaryFormat
        | RejectReason::CsvFormat
        | RejectReason::UnknownFormat { .. }
        | RejectReason::FileAccess
        | RejectReason::ProgramAccess
        | RejectReason::UnknownSource => "0A000",
    }
}

fn reject_message(reason: &RejectReason) -> String {
    match reason {
        RejectReason::BinaryFormat => {
            "COPY binary format not supported in V1 (SP-PG-COPY-BIN)".to_string()
        }
        RejectReason::CsvFormat => {
            "COPY csv format not supported in V1 (SP-PG-COPY-CSV)".to_string()
        }
        RejectReason::UnknownFormat { format } => {
            format!("COPY format \"{format}\" not recognized")
        }
        RejectReason::FileAccess => {
            "COPY FROM/TO file path not supported in V1; use STDIN/STDOUT (SP-PG-COPY-FILE)"
                .to_string()
        }
        RejectReason::ProgramAccess => {
            "COPY FROM/TO PROGRAM not supported (permanent security restriction)"
                .to_string()
        }
        RejectReason::UnknownSource => {
            "COPY source/destination must be STDIN or STDOUT".to_string()
        }
    }
}

/// Synthesize an `INSERT INTO <table> [(cols)] VALUES (...)`
/// statement from a parsed COPY row. Each NULL field renders as
/// the keyword `NULL`; each non-NULL field is single-quoted with
/// embedded single-quotes doubled (`'` → `''`), matching the
/// PG-canonical SQL-literal escape.
fn synthesize_insert_sql(
    table: &str,
    columns: Option<&[String]>,
    fields: &[Option<Vec<u8>>],
) -> Result<String, String> {
    let mut s = String::with_capacity(64);
    s.push_str("INSERT INTO ");
    s.push_str(table);
    if let Some(cols) = columns {
        s.push_str(" (");
        for (i, c) in cols.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            s.push_str(c);
        }
        s.push(')');
    }
    s.push_str(" VALUES (");
    for (i, v) in fields.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        match v {
            None => s.push_str("NULL"),
            Some(bytes) => {
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| "field is not valid UTF-8".to_string())?;
                s.push('\'');
                for c in text.chars() {
                    if c == '\'' {
                        s.push_str("''");
                    } else {
                        s.push(c);
                    }
                }
                s.push('\'');
            }
        }
    }
    s.push(')');
    Ok(s)
}

/// Walk a `dispatch::dispatch_query` byte-stream and, if the first
/// frame is `E` ErrorResponse, return `Some((sqlstate, message))`
/// extracted from the ErrorResponse payload. Returns `None` if no
/// ErrorResponse is present.
fn extract_error_response(bytes: &[u8]) -> Option<(String, String)> {
    if bytes.is_empty() || bytes[0] != b'E' {
        return None;
    }
    if bytes.len() < 5 {
        return None;
    }
    let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
    let frame_end = 1 + len;
    if bytes.len() < frame_end {
        return None;
    }
    let payload = &bytes[5..frame_end];
    // Payload is a sequence of [type:1][value:cstring]*\0. Walk it
    // to extract SQLSTATE ('C') and Message ('M').
    let mut sqlstate = String::new();
    let mut message = String::new();
    let mut i = 0;
    while i < payload.len() {
        let t = payload[i];
        if t == 0 {
            break;
        }
        i += 1;
        let start = i;
        while i < payload.len() && payload[i] != 0 {
            i += 1;
        }
        let value = std::str::from_utf8(&payload[start..i]).unwrap_or("").to_string();
        i += 1; // skip the NUL
        match t {
            b'C' => sqlstate = value,
            b'M' => message = value,
            _ => {}
        }
    }
    Some((sqlstate, message))
}

/// Walk a `dispatch::dispatch_query` byte-stream and extract per-row
/// per-column bytes. Each row is `Vec<Option<Vec<u8>>>` (None =
/// NULL, Some(bytes) = column text bytes). Used by `dispatch_copy_to`
/// to reframe DataRow as CopyData.
fn extract_data_rows(bytes: &[u8]) -> Vec<Vec<Option<Vec<u8>>>> {
    let mut rows = Vec::new();
    let mut i = 0usize;
    while i + 5 <= bytes.len() {
        let tag = bytes[i];
        let len = u32::from_be_bytes([bytes[i + 1], bytes[i + 2], bytes[i + 3], bytes[i + 4]])
            as usize;
        let frame_total = 1 + len;
        if i + frame_total > bytes.len() {
            break;
        }
        if tag == b'D' {
            let payload = &bytes[i + 5..i + frame_total];
            if payload.len() >= 2 {
                let ncols = u16::from_be_bytes([payload[0], payload[1]]) as usize;
                let mut cols = Vec::with_capacity(ncols);
                let mut p = 2;
                for _ in 0..ncols {
                    if p + 4 > payload.len() {
                        break;
                    }
                    let collen =
                        i32::from_be_bytes([payload[p], payload[p + 1], payload[p + 2], payload[p + 3]]);
                    p += 4;
                    if collen < 0 {
                        cols.push(None);
                    } else {
                        let n = collen as usize;
                        if p + n > payload.len() {
                            break;
                        }
                        cols.push(Some(payload[p..p + n].to_vec()));
                        p += n;
                    }
                }
                rows.push(cols);
            }
        }
        i += frame_total;
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::PgColumn;
    use kessel_catalog::FieldKind;
    use kessel_codec::Value;
    use kessel_proto::OpResult;

    /// A test engine that returns a fixed schema for table "t" and
    /// either succeeds or returns a canned OpResult for apply_sql.
    struct CopyTestEngine {
        cols: Vec<PgColumn>,
        result: std::sync::Mutex<Vec<OpResult>>,
        row_bytes: Vec<u8>,
        table: String,
        applied: std::sync::Mutex<Vec<String>>,
    }

    impl EngineApply for CopyTestEngine {
        fn apply_sql(&self, sql: &str) -> OpResult {
            self.applied.lock().unwrap().push(sql.to_string());
            self.result
                .lock()
                .unwrap()
                .pop()
                .unwrap_or_else(|| {
                    if sql.trim_start().to_ascii_uppercase().starts_with("INSERT") {
                        OpResult::Ok
                    } else if sql.trim_start().to_ascii_uppercase().starts_with("SELECT") {
                        OpResult::Got(self.row_bytes.clone().into())
                    } else {
                        OpResult::Ok
                    }
                })
        }
        fn describe_table(&self, name: &str) -> Option<Vec<PgColumn>> {
            if name == self.table {
                Some(self.cols.clone())
            } else {
                None
            }
        }
    }

    fn make_engine(cols: Vec<PgColumn>) -> CopyTestEngine {
        CopyTestEngine {
            cols,
            result: std::sync::Mutex::new(Vec::new()),
            row_bytes: Vec::new(),
            table: "t".to_string(),
            applied: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Build a kessel-codec encoded record for the given cols + values.
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

    fn build_row_stream(records: &[Vec<u8>]) -> Vec<u8> {
        let mut out = Vec::new();
        for r in records {
            out.extend_from_slice(&(r.len() as u32).to_le_bytes());
            out.extend_from_slice(r);
        }
        out
    }

    /// SP-PG-COPY T2: `dispatch_copy_in_start` on a known table
    /// emits `CopyInResponse(G)` with the right column count and
    /// returns the CopyInState seeded with the table + columns.
    #[test]
    fn t2_dispatch_copy_in_start_emits_copy_in_response() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(32), nullable: true },
        ];
        let eng = make_engine(cols);
        let parsed = ParsedCopy::From {
            table: "t".to_string(),
            columns: None,
        };
        match dispatch_copy_in_start(parsed, &eng) {
            CopyInStartOutcome::Started { bytes, state } => {
                // 'G' + length=11 + format=0 + ncols=2 + 2*0
                assert_eq!(bytes[0], b'G');
                assert_eq!(state.table, "t");
                assert_eq!(state.column_count, 2);
                assert_eq!(state.columns.as_deref(), Some(&["id".to_string(), "name".to_string()][..]));
                assert!(state.carry.is_empty());
                assert_eq!(state.rows_ingested, 0);
            }
            other => panic!("expected Started, got {other:?}"),
        }
    }

    /// SP-PG-COPY T2: an unknown-table COPY FROM rejects with 42P01.
    #[test]
    fn t2_dispatch_copy_in_start_unknown_table_42p01() {
        let eng = make_engine(vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }]);
        let parsed = ParsedCopy::From {
            table: "ghost".to_string(),
            columns: None,
        };
        match dispatch_copy_in_start(parsed, &eng) {
            CopyInStartOutcome::Failed { bytes } => {
                assert_eq!(bytes[0], b'E');
                assert!(bytes.windows(5).any(|w| w == b"42P01"));
                assert!(bytes.windows(5).any(|w| w == b"ghost"));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    /// SP-PG-COPY T2: a COPY FROM with a bad column name → 42703.
    #[test]
    fn t2_dispatch_copy_in_start_unknown_column_42703() {
        let eng = make_engine(vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }]);
        let parsed = ParsedCopy::From {
            table: "t".to_string(),
            columns: Some(vec!["missing".to_string()]),
        };
        match dispatch_copy_in_start(parsed, &eng) {
            CopyInStartOutcome::Failed { bytes } => {
                assert_eq!(bytes[0], b'E');
                assert!(bytes.windows(5).any(|w| w == b"42703"));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    /// SP-PG-COPY T2: a COPY FROM where the SQL recognizer surfaced
    /// `Rejected { BinaryFormat }` → 0A000 with the precise V2-pointing
    /// message.
    #[test]
    fn t2_dispatch_copy_in_start_binary_format_0a000() {
        let eng = make_engine(vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }]);
        let parsed = ParsedCopy::Rejected {
            reason: RejectReason::BinaryFormat,
        };
        match dispatch_copy_in_start(parsed, &eng) {
            CopyInStartOutcome::Failed { bytes } => {
                assert_eq!(bytes[0], b'E');
                assert!(bytes.windows(5).any(|w| w == b"0A000"));
                assert!(bytes.windows(b"SP-PG-COPY-BIN".len()).any(|w| w == b"SP-PG-COPY-BIN"));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    /// SP-PG-COPY T2: a single CopyData containing 3 rows ingests 3
    /// rows. The applied SQL for each row contains the `INSERT INTO t`
    /// shape with the right values.
    #[test]
    fn t2_process_copy_data_three_rows_ingests_three_inserts() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(32), nullable: true },
        ];
        let eng = make_engine(cols);
        let state = CopyInState::new(
            "t".to_string(),
            Some(vec!["id".to_string(), "name".to_string()]),
            2,
        );
        let data = b"1\thello\n2\tworld\n3\tfoo\n";
        match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Continue { state } => {
                assert_eq!(state.rows_ingested, 3);
                assert!(state.carry.is_empty());
                let applied = eng.applied.lock().unwrap();
                assert_eq!(applied.len(), 3);
                assert!(applied[0].contains("INSERT INTO t (id, name) VALUES ('1', 'hello')"));
                assert!(applied[1].contains("'world'"));
                assert!(applied[2].contains("'foo'"));
            }
            other => panic!("expected Continue, got {other:?}"),
        }
    }

    /// SP-PG-COPY T2: a CopyData with an incomplete trailing row
    /// stashes the partial bytes in carry; the next CopyData picks
    /// up where the first left off.
    #[test]
    fn t2_process_copy_data_carries_partial_row() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = make_engine(cols);
        let state = CopyInState::new(
            "t".to_string(),
            Some(vec!["id".to_string()]),
            1,
        );
        // Two complete rows + a partial "30" with no trailing \n.
        let data1 = b"10\n20\n30";
        let state = match process_copy_data(data1, state, &eng) {
            CopyDataOutcome::Continue { state } => {
                assert_eq!(state.rows_ingested, 2);
                assert_eq!(state.carry, b"30".to_vec());
                state
            }
            other => panic!("expected Continue, got {other:?}"),
        };
        // Now finish the partial + add one more.
        let data2 = b"\n40\n";
        match process_copy_data(data2, state, &eng) {
            CopyDataOutcome::Continue { state } => {
                assert_eq!(state.rows_ingested, 4);
                assert!(state.carry.is_empty());
            }
            other => panic!("expected Continue, got {other:?}"),
        }
        let applied = eng.applied.lock().unwrap();
        assert_eq!(applied.len(), 4);
        assert!(applied[2].contains("'30'"), "row 3 should contain '30'");
        assert!(applied[3].contains("'40'"), "row 4 should contain '40'");
    }

    /// SP-PG-COPY T2: NULL field (`\N`) renders as the `NULL` keyword
    /// in the synthesized INSERT.
    #[test]
    fn t2_process_copy_data_null_field_renders_null_keyword() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(32), nullable: true },
        ];
        let eng = make_engine(cols);
        let state = CopyInState::new(
            "t".to_string(),
            Some(vec!["id".to_string(), "name".to_string()]),
            2,
        );
        let data = b"1\t\\N\n";
        match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Continue { state } => {
                assert_eq!(state.rows_ingested, 1);
            }
            other => panic!("expected Continue, got {other:?}"),
        }
        let applied = eng.applied.lock().unwrap();
        assert_eq!(applied.len(), 1);
        assert!(applied[0].contains("VALUES ('1', NULL)"));
    }

    /// SP-PG-COPY T2: the `\.` end-of-data marker line is silently
    /// skipped — no INSERT is dispatched for it.
    #[test]
    fn t2_process_copy_data_end_of_data_marker_skipped() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = make_engine(cols);
        let state = CopyInState::new("t".to_string(), Some(vec!["id".to_string()]), 1);
        // Two real rows + the \. marker line.
        let data = b"1\n2\n\\.\n";
        match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Continue { state } => {
                assert_eq!(state.rows_ingested, 2);
            }
            other => panic!("expected Continue, got {other:?}"),
        }
        let applied = eng.applied.lock().unwrap();
        assert_eq!(applied.len(), 2);
    }

    /// SP-PG-COPY T2: field-count mismatch in a row aborts the COPY
    /// with `22023` + a row-number-tagged message.
    #[test]
    fn t2_process_copy_data_field_count_mismatch_aborts() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = make_engine(cols);
        let state = CopyInState::new("t".to_string(), Some(vec!["id".to_string()]), 1);
        let data = b"1\n2\tbadextra\n3\n";
        match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Failed { bytes } => {
                assert_eq!(bytes[0], b'E');
                assert!(bytes.windows(5).any(|w| w == b"22023"));
                // Row number tag (row 2 failed — one row ingested before).
                assert!(bytes.windows(b"row 2".len()).any(|w| w == b"row 2"));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    /// SP-PG-COPY T2: a constraint error from the engine surfaces
    /// through the COPY error path with the row number tagged.
    #[test]
    fn t2_process_copy_data_engine_error_aborts_with_row_number() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let mut eng = make_engine(cols);
        // First row succeeds (Ok), second triggers Constraint.
        *eng.result.lock().unwrap() = vec![
            OpResult::Constraint("NOT NULL violated".into()),
            OpResult::Ok,
        ];
        let state = CopyInState::new("t".to_string(), Some(vec!["id".to_string()]), 1);
        // engine.result is consumed via `pop()` so the LAST result
        // in the vec is for the FIRST row dispatch — order matches.
        let data = b"1\n2\n";
        match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Failed { bytes } => {
                assert_eq!(bytes[0], b'E');
                assert!(bytes.windows(b"row 2".len()).any(|w| w == b"row 2"));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    /// SP-PG-COPY T2: `\r\n` line endings tolerated on input.
    #[test]
    fn t2_process_copy_data_crlf_tolerated() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = make_engine(cols);
        let state = CopyInState::new("t".to_string(), Some(vec!["id".to_string()]), 1);
        let data = b"10\r\n20\r\n";
        match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Continue { state } => {
                assert_eq!(state.rows_ingested, 2);
            }
            other => panic!("expected Continue, got {other:?}"),
        }
    }

    /// SP-PG-COPY T2: `finalize_copy_in_success` emits
    /// `CommandComplete("COPY N") + RFQ` byte-correct.
    #[test]
    fn t2_finalize_copy_in_success_emits_command_complete_rfq() {
        let state = CopyInState {
            table: "t".to_string(),
            columns: None,
            column_count: 1,
            carry: Vec::new(),
            rows_ingested: 5,
        };
        let bytes = finalize_copy_in_success(&state);
        assert!(bytes.windows(b"COPY 5\0".len()).any(|w| w == b"COPY 5\0"));
        // Last 6 bytes = RFQ('I').
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// SP-PG-COPY T2: `finalize_copy_in_failure` emits
    /// `ErrorResponse 57014 + RFQ` with the client's reason in the
    /// message.
    #[test]
    fn t2_finalize_copy_in_failure_emits_error_response_57014() {
        let bytes = finalize_copy_in_failure("user aborted");
        assert_eq!(bytes[0], b'E');
        assert!(bytes.windows(5).any(|w| w == b"57014"));
        assert!(bytes.windows(b"user aborted".len()).any(|w| w == b"user aborted"));
        // RFQ at end.
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    // ─── T3 — COPY TO STDOUT ────────────────────────────────────────────

    /// SP-PG-COPY T3: an empty-table `COPY t TO STDOUT` emits
    /// `H` CopyOutResponse + `c` CopyDone + `CommandComplete("COPY 0")`
    /// + RFQ.
    #[test]
    fn t3_dispatch_copy_to_empty_table() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = CopyTestEngine {
            cols: cols.clone(),
            result: std::sync::Mutex::new(Vec::new()),
            row_bytes: Vec::new(),
            table: "t".to_string(),
            applied: std::sync::Mutex::new(Vec::new()),
        };
        let parsed = ParsedCopy::To {
            table: "t".to_string(),
            columns: None,
        };
        let bytes = dispatch_copy_to(parsed, &eng);
        // First byte = 'H'.
        assert_eq!(bytes[0], b'H');
        // CopyDone 'c' present.
        assert!(bytes.windows(5).any(|w| w == &[b'c', 0, 0, 0, 4][..]));
        // "COPY 0" tag.
        assert!(bytes.windows(b"COPY 0\0".len()).any(|w| w == b"COPY 0\0"));
        // RFQ at end.
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// SP-PG-COPY T3: a `COPY t TO STDOUT` against a 3-row engine
    /// emits H + 3×CopyData + CopyDone + CommandComplete("COPY 3") +
    /// RFQ. The DataRow bytes round-trip through encode_text_row.
    #[test]
    fn t3_dispatch_copy_to_three_rows_full_sequence() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(32), nullable: true },
        ];
        // Build records: (1, "hello"), (2, "world"), (3, "")
        let r1 = build_record(&cols, &[Value::Int(1), Value::Blob(b"hello\0\0\0".to_vec())]);
        let r2 = build_record(&cols, &[Value::Int(2), Value::Blob(b"world\0\0\0".to_vec())]);
        let r3 = build_record(&cols, &[Value::Int(3), Value::Null]);
        let stream = build_row_stream(&[r1, r2, r3]);

        let eng = CopyTestEngine {
            cols: cols.clone(),
            result: std::sync::Mutex::new(Vec::new()),
            row_bytes: stream,
            table: "t".to_string(),
            applied: std::sync::Mutex::new(Vec::new()),
        };
        let parsed = ParsedCopy::To {
            table: "t".to_string(),
            columns: None,
        };
        let bytes = dispatch_copy_to(parsed, &eng);

        // First byte = 'H' (CopyOutResponse).
        assert_eq!(bytes[0], b'H');
        // 3 CopyData frames present (count 'd' tags after position 0).
        // Skip the leading H frame.
        let h_len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
        let mut p = 1 + h_len;
        let mut copy_data_count = 0;
        while p + 5 <= bytes.len() && bytes[p] == b'd' {
            let len = u32::from_be_bytes([bytes[p + 1], bytes[p + 2], bytes[p + 3], bytes[p + 4]])
                as usize;
            copy_data_count += 1;
            p += 1 + len;
        }
        assert_eq!(copy_data_count, 3);
        // CopyDone 'c' present.
        assert!(bytes.windows(5).any(|w| w == &[b'c', 0, 0, 0, 4][..]));
        // COPY 3 tag.
        assert!(bytes.windows(b"COPY 3\0".len()).any(|w| w == b"COPY 3\0"));
        // RFQ at end.
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);

        // The first CopyData payload should be `1\thello\n`.
        let h_frame_end = 1 + h_len;
        let first_cd = &bytes[h_frame_end..];
        assert_eq!(first_cd[0], b'd');
        let cd_len = u32::from_be_bytes([first_cd[1], first_cd[2], first_cd[3], first_cd[4]])
            as usize;
        let payload = &first_cd[5..5 + (cd_len - 4)];
        assert_eq!(payload, b"1\thello\n");
    }

    /// SP-PG-COPY T3: COPY TO STDOUT on an unknown table → 42P01
    /// (without ever sending an H frame).
    #[test]
    fn t3_dispatch_copy_to_unknown_table_42p01() {
        let eng = CopyTestEngine {
            cols: vec![PgColumn {
                name: "id".into(),
                kind: FieldKind::I64,
                nullable: false,
            }],
            result: std::sync::Mutex::new(Vec::new()),
            row_bytes: Vec::new(),
            table: "t".to_string(),
            applied: std::sync::Mutex::new(Vec::new()),
        };
        let parsed = ParsedCopy::To {
            table: "ghost".to_string(),
            columns: None,
        };
        let bytes = dispatch_copy_to(parsed, &eng);
        assert_eq!(bytes[0], b'E');
        assert!(bytes.windows(5).any(|w| w == b"42P01"));
    }

    /// SP-PG-COPY T3: COPY TO STDOUT with a NULL field emits `\N`
    /// in the CopyData payload.
    #[test]
    fn t3_dispatch_copy_to_null_field_emits_backslash_n() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(32), nullable: true },
        ];
        let r1 = build_record(&cols, &[Value::Int(7), Value::Null]);
        let stream = build_row_stream(&[r1]);
        let eng = CopyTestEngine {
            cols: cols.clone(),
            result: std::sync::Mutex::new(Vec::new()),
            row_bytes: stream,
            table: "t".to_string(),
            applied: std::sync::Mutex::new(Vec::new()),
        };
        let parsed = ParsedCopy::To {
            table: "t".to_string(),
            columns: None,
        };
        let bytes = dispatch_copy_to(parsed, &eng);
        // The payload should be `7\t\N\n`.
        assert!(bytes.windows(b"7\t\\N\n".len()).any(|w| w == b"7\t\\N\n"));
    }
}
