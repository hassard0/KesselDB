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

use crate::copy::binary::{
    encode_binary_end_of_data, encode_binary_header, encode_binary_row, BinaryDecodeError,
    BinaryDecoder, BinaryState,
};
use crate::copy::command::{ParsedCopy, RejectReason};
use crate::copy::csv::{
    encode_csv_record, parse_csv_record, validate_numeric_text, CsvNumericError, CsvOptions,
    CsvParseError,
};
use crate::copy::proto::{
    copy_tag, encode_copy_data, encode_copy_done, encode_copy_in_response,
    encode_copy_in_response_binary, encode_copy_out_response,
    encode_copy_out_response_binary,
};
use crate::copy::text::{encode_text_row, is_end_of_data_marker, parse_text_row_bytes};
use crate::copy::{CopyFormat, CopyInState, MAX_COPY_DATA_BUFFER};
use crate::dispatch;
use crate::engine::EngineApply;
use crate::error::{encode_error_response, SEVERITY_ERROR};
use crate::extq::binary_results::{encode_binary_value, BinaryEncodeError};
use crate::extq::substitute::{binary_format_supported_for_oid, decode_binary_param};
use crate::response::{encode_command_complete, encode_ready_for_query};
use crate::proto::PG_TYPE_NUMERIC;
use crate::types::field_kind_to_oid;

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
    let (table, columns, format) = match parsed {
        ParsedCopy::From { table, columns, format } => (table, columns, format),
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
    // Validate the supplied column list (if any) against the schema,
    // and capture the per-column FieldKind so the row-to-INSERT
    // synthesizer can pick numeric-vs-quoted-literal rendering.
    let (chosen_columns, chosen_kinds): (Vec<String>, Vec<kessel_catalog::FieldKind>) =
        match columns.as_ref() {
            Some(cols) => {
                let mut kinds = Vec::with_capacity(cols.len());
                for c in cols {
                    match schema_cols.iter().find(|s| s.name == *c) {
                        Some(sc) => kinds.push(sc.kind),
                        None => {
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
                }
                (cols.clone(), kinds)
            }
            None => (
                schema_cols.iter().map(|s| s.name.clone()).collect(),
                schema_cols.iter().map(|s| s.kind).collect(),
            ),
        };
    // SP-PG-COPY-BIN V1 — when the format is Binary, V1 must reject
    // unsupported column types at COPY-start (UUID / JSONB / ARRAY)
    // because per-row decoding has no recovery path.
    //
    // SP-PG-COPY-BIN-NUMERIC V1 (2026-06-02) — NUMERIC is now admitted
    // here: the SP-PG-EXTQ-BIN-NUMERIC T3 wiring made
    // `binary_format_supported_for_oid` return true for PG_TYPE_NUMERIC,
    // and the per-row decoder route in `process_copy_data_binary`
    // dispatches `decode_binary_param` → `binary_numeric::decode_numeric_binary`
    // for the NUMERIC arm. The explicit pre-reject that used to live
    // here pointed to this arc; dropping it closes the follow-up.
    if format.is_binary() {
        for (name, kind) in chosen_columns.iter().zip(chosen_kinds.iter()) {
            let oid = field_kind_to_oid(*kind);
            if !binary_format_supported_for_oid(oid) {
                return CopyInStartOutcome::Failed {
                    bytes: error_response_then_rfq(
                        "0A000",
                        &format!(
                            "COPY binary: column \"{name}\" type OID {oid} not supported in V1 (SP-PG-COPY-BIN-EXTRA)"
                        ),
                    ),
                };
            }
        }
    }
    let ncols = chosen_columns.len() as u16;
    let bytes = if format.is_binary() {
        encode_copy_in_response_binary(ncols)
    } else {
        encode_copy_in_response(ncols)
    };
    let state = CopyInState::new_with_format(
        table,
        Some(chosen_columns),
        ncols,
        chosen_kinds,
        format,
    );
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
/// - else **SP-PG-COPY-BULKAPPLY V1**: append to `state.pending_rows`;
/// - when `pending_rows.len() >= state.batch_size`, drain via
///   `flush_pending_batch` — synthesize one multi-row INSERT (or fall
///   back to per-row dispatch if any pending row contains a NULL),
///   dispatch through `dispatch::dispatch_query` ONCE, and clear the
///   buffer.
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

    // SP-PG-COPY-CSV V1 — branch on the format. CSV uses a record-
    // oriented parser (a record can span multiple physical lines when
    // it contains quoted newlines); text uses the existing line-
    // oriented parser. SP-PG-COPY-BIN V1 adds the binary branch.
    if state.format.is_binary() {
        process_copy_data_binary(state, engine)
    } else if state.format.is_csv() {
        process_copy_data_csv(state, engine)
    } else {
        process_copy_data_text(state, engine)
    }
}

/// Text-format CopyData processor — original SP-PG-COPY V1 path.
fn process_copy_data_text<E: EngineApply + ?Sized>(
    mut state: CopyInState,
    engine: &E,
) -> CopyDataOutcome {
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
            Ok(mut fields) => {
                // SP-PG-COPY-CSV-NUMERIC (2026-06-02) — validate +
                // canonicalise NUMERIC columns BEFORE adding to the
                // BULKAPPLY pending buffer. NULL fields pass through
                // unchanged.
                if let Err(fail_bytes) =
                    validate_numeric_fields(&mut fields, &state, "text")
                {
                    return CopyDataOutcome::Failed { bytes: fail_bytes };
                }
                // SP-PG-COPY-BULKAPPLY V1 — buffer instead of dispatch.
                state.pending_rows.push(fields);
                if state.pending_rows.len() >= state.batch_size {
                    if let Some(fail_bytes) = flush_pending_batch(&mut state, engine) {
                        return CopyDataOutcome::Failed { bytes: fail_bytes };
                    }
                }
            }
            Err(e) => {
                // The failing row number is the next row past the
                // already-ingested + already-buffered count. Compute
                // BEFORE clearing the pending buffer.
                let failing_row =
                    state.rows_ingested + state.pending_rows.len() as u64 + 1;
                // Drop the pending buffer on parse failure — those
                // rows were never applied (PG semantics: abort).
                state.pending_rows.clear();
                return CopyDataOutcome::Failed {
                    bytes: error_response_then_rfq(
                        "22023",
                        &format!(
                            "COPY row {failing_row} parse failed: {e:?}"
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

/// SP-PG-COPY-CSV-NUMERIC (2026-06-02) — walk a row's fields, and for
/// every column whose PG type OID is `PG_TYPE_NUMERIC` (1700), run
/// `validate_numeric_text` on the field's UTF-8 contents. On success,
/// replace the field bytes with the canonical form (sign-normalised
/// decimals; canonical mixed-case `NaN`/`Infinity`/`-Infinity`). On
/// failure, return the ErrorResponse + RFQ bytes (caller emits +
/// transitions to Idle).
///
/// NULL fields pass through unchanged so the column-omit auto-NULL
/// fill semantics (SP-PG-COPY V1) keep working.
///
/// `format_label` is "text" or "csv" — embedded in the error message
/// so the operator can tell which dispatch path surfaced the
/// validation failure.
fn validate_numeric_fields(
    fields: &mut [Option<Vec<u8>>],
    state: &CopyInState,
    format_label: &str,
) -> Result<(), Vec<u8>> {
    let failing_row = state.rows_ingested + state.pending_rows.len() as u64 + 1;
    for (i, f) in fields.iter_mut().enumerate() {
        let kind = match state.column_kinds.get(i) {
            Some(k) => *k,
            None => continue,
        };
        if field_kind_to_oid(kind) != PG_TYPE_NUMERIC {
            continue;
        }
        let Some(bytes) = f.as_ref() else { continue };
        let s = match std::str::from_utf8(bytes) {
            Ok(s) => s,
            Err(_) => {
                let col_name = state
                    .columns
                    .as_ref()
                    .and_then(|c| c.get(i).cloned())
                    .unwrap_or_else(|| format!("column_{i}"));
                return Err(error_response_then_rfq(
                    "22P02",
                    &format!(
                        "COPY {format_label} row {failing_row} column \"{col_name}\" NUMERIC: field is not valid UTF-8"
                    ),
                ));
            }
        };
        match validate_numeric_text(s) {
            Ok(canonical) => {
                *f = Some(canonical.into_bytes());
            }
            Err(e) => {
                let col_name = state
                    .columns
                    .as_ref()
                    .and_then(|c| c.get(i).cloned())
                    .unwrap_or_else(|| format!("column_{i}"));
                let detail = match e {
                    CsvNumericError::Empty => {
                        "empty value (use \\N for NULL in text format, empty unquoted for CSV)"
                            .to_string()
                    }
                    CsvNumericError::BadByte { position, byte } => {
                        format!("bad byte 0x{byte:02X} at position {position}")
                    }
                    CsvNumericError::Malformed { reason } => {
                        format!("malformed ({reason})")
                    }
                    CsvNumericError::ScientificNotation => {
                        "scientific notation not supported in V1 (SP-PG-COPY-CSV-NUMERIC-SCI)"
                            .to_string()
                    }
                };
                return Err(error_response_then_rfq(
                    "22P02",
                    &format!(
                        "COPY {format_label} row {failing_row} column \"{col_name}\" NUMERIC: {detail}"
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// **SP-PG-COPY-CSV V1** — CSV-format CopyData processor. Uses the
/// record-oriented `parse_csv_record` so a CSV record containing
/// quoted newlines is reassembled correctly across CopyData frame
/// boundaries (the carry buffer holds the partial record).
///
/// HEADER mode: the first complete record after COPY-start is consumed
/// and discarded (V1 doesn't validate the header against the schema —
/// HEADER MATCH is V2 `SP-PG-COPY-CSV-HEADER-MATCH`).
fn process_copy_data_csv<E: EngineApply + ?Sized>(
    mut state: CopyInState,
    engine: &E,
) -> CopyDataOutcome {
    let opts = match &state.format {
        CopyFormat::Csv(o) => o.clone(),
        _ => unreachable!("caller branched on is_csv"),
    };
    let bytes = std::mem::take(&mut state.carry);
    let mut pos = 0usize;
    let expected = state.column_count as usize;

    loop {
        // HEADER consumption: parse the next record with expected=0
        // (no field-count enforcement) and drop it.
        let parse_expected = if state.pending_header { 0 } else { expected };
        match parse_csv_record(&bytes, pos, &opts, parse_expected) {
            Ok(Some((fields, next_pos))) => {
                pos = next_pos;
                if state.pending_header {
                    state.pending_header = false;
                    continue;
                }
                // Convert Option<String> → Option<Vec<u8>> for the
                // existing BULKAPPLY pipeline (per-row + multi-row
                // INSERT synthesizers both consume Vec<Option<Vec<u8>>>).
                let mut row: Vec<Option<Vec<u8>>> = fields
                    .into_iter()
                    .map(|f| f.map(|s| s.into_bytes()))
                    .collect();
                // SP-PG-COPY-CSV-NUMERIC (2026-06-02) — validate +
                // canonicalise NUMERIC columns BEFORE adding to the
                // BULKAPPLY pending buffer.
                if let Err(fail_bytes) =
                    validate_numeric_fields(&mut row, &state, "csv")
                {
                    return CopyDataOutcome::Failed { bytes: fail_bytes };
                }
                state.pending_rows.push(row);
                if state.pending_rows.len() >= state.batch_size {
                    if let Some(fail_bytes) = flush_pending_batch(&mut state, engine) {
                        return CopyDataOutcome::Failed { bytes: fail_bytes };
                    }
                }
            }
            Ok(None) => {
                // Partial record — save the trailing bytes into the
                // carry and wait for more data.
                state.carry = bytes[pos..].to_vec();
                return CopyDataOutcome::Continue { state };
            }
            Err(e) => {
                let failing_row =
                    state.rows_ingested + state.pending_rows.len() as u64 + 1;
                state.pending_rows.clear();
                let msg = match e {
                    CsvParseError::FieldCountMismatch { expected, actual } => {
                        format!("COPY row {failing_row} CSV field count mismatch: expected {expected}, got {actual}")
                    }
                    CsvParseError::UnterminatedQuote => {
                        format!("COPY row {failing_row} CSV: unterminated quoted field")
                    }
                    CsvParseError::TrailingEscape => {
                        format!("COPY row {failing_row} CSV: trailing escape with no value")
                    }
                    CsvParseError::NotUtf8 => {
                        format!("COPY row {failing_row} CSV: field is not valid UTF-8")
                    }
                };
                return CopyDataOutcome::Failed {
                    bytes: error_response_then_rfq("22023", &msg),
                };
            }
        }
    }
}

/// **SP-PG-COPY-BIN V1** — binary-format CopyData processor. Uses the
/// streaming `BinaryDecoder` to consume the 19-byte header (once per
/// session — flag tracked in `CopyInState::binary_header_consumed`)
/// and per-row records. Decoded binary field bytes are routed through
/// `extq::substitute::decode_binary_param` to produce text-format
/// values, then fed into the existing BULKAPPLY V1 multi-row INSERT
/// fold (the same path text + CSV use).
///
/// Trade-off: per-value binary→text round trip is wasteful for
/// throughput-heavy binary COPY (V2 `SP-PG-COPY-BIN-DIRECT` could
/// bypass via typed parameter binding). V1 prioritises correctness +
/// code reuse over throughput.
///
/// End-of-data marker: `BinaryDecoder` flips to `EndOfData` when the
/// `\xff\xff` marker is consumed; subsequent CopyData payloads (if
/// any — a well-behaved client follows the marker with CopyDone) are
/// silently dropped (V1 tolerant stance — matches text + CSV).
fn process_copy_data_binary<E: EngineApply + ?Sized>(
    mut state: CopyInState,
    engine: &E,
) -> CopyDataOutcome {
    let bytes = std::mem::take(&mut state.carry);
    let mut dec = if state.binary_header_consumed {
        BinaryDecoder::new_in_body(&bytes)
    } else {
        BinaryDecoder::new(&bytes)
    };
    // Step 1: consume the 19-byte header if we haven't yet.
    if !state.binary_header_consumed {
        match dec.consume_header() {
            Ok(true) => {
                state.binary_header_consumed = true;
            }
            Ok(false) => {
                // Not enough bytes yet — carry everything and wait.
                state.carry = bytes;
                return CopyDataOutcome::Continue { state };
            }
            Err(e) => {
                return CopyDataOutcome::Failed {
                    bytes: error_response_then_rfq(
                        binary_decode_sqlstate(&e),
                        &binary_decode_message(&e, 0),
                    ),
                };
            }
        }
    }
    // Step 2: parse per-row binary records, decode each column's
    // binary bytes back to text, then push into the BULKAPPLY pending
    // buffer. Stop on EndOfData OR on Ok(None) at row boundary.
    let expected = state.column_count as usize;
    // Snapshot the per-column OIDs for the decoder (resolved once).
    let oids: Vec<u32> = state
        .column_kinds
        .iter()
        .map(|k| field_kind_to_oid(*k))
        .collect();
    loop {
        match dec.next_row(expected) {
            Ok(Some(fields)) => {
                // Decode each column's binary bytes to text using the
                // SP-PG-EXTQ-BIN param decoder. NULL stays NULL.
                let mut row_text: Vec<Option<Vec<u8>>> = Vec::with_capacity(fields.len());
                for (i, f) in fields.iter().enumerate() {
                    match f {
                        None => row_text.push(None),
                        Some(b) => {
                            let oid = oids.get(i).copied().unwrap_or(0);
                            match decode_binary_param(b, oid) {
                                Ok(text) => row_text.push(Some(text.into_bytes())),
                                Err(e) => {
                                    let failing_row = state.rows_ingested
                                        + state.pending_rows.len() as u64
                                        + 1;
                                    state.pending_rows.clear();
                                    return CopyDataOutcome::Failed {
                                        bytes: error_response_then_rfq(
                                            "22P02",
                                            &format!(
                                                "COPY binary row {failing_row} column {i} (OID {oid}): {e:?}"
                                            ),
                                        ),
                                    };
                                }
                            }
                        }
                    }
                }
                state.pending_rows.push(row_text);
                if state.pending_rows.len() >= state.batch_size {
                    if let Some(fail_bytes) = flush_pending_batch(&mut state, engine) {
                        return CopyDataOutcome::Failed { bytes: fail_bytes };
                    }
                }
            }
            Ok(None) => {
                let new_state = dec.state();
                let cursor = dec.cursor();
                // Drop the decoder before we move bytes around.
                drop(dec);
                match new_state {
                    BinaryState::EndOfData => {
                        // V1 tolerant stance: any trailing bytes after
                        // the EOD marker are dropped (the client should
                        // send CopyDone next).
                        state.carry.clear();
                        return CopyDataOutcome::Continue { state };
                    }
                    BinaryState::Body => {
                        // Partial row — carry from where the decoder
                        // stopped.
                        state.carry = bytes[cursor..].to_vec();
                        return CopyDataOutcome::Continue { state };
                    }
                    BinaryState::Header => {
                        // Unreachable — we already consumed the header
                        // above OR returned Continue earlier.
                        state.carry = bytes;
                        return CopyDataOutcome::Continue { state };
                    }
                }
            }
            Err(e) => {
                let failing_row =
                    state.rows_ingested + state.pending_rows.len() as u64 + 1;
                state.pending_rows.clear();
                return CopyDataOutcome::Failed {
                    bytes: error_response_then_rfq(
                        binary_decode_sqlstate(&e),
                        &binary_decode_message(&e, failing_row),
                    ),
                };
            }
        }
    }
}

/// **SP-PG-COPY-BIN V1** — map `BinaryDecodeError` to a PG SQLSTATE.
fn binary_decode_sqlstate(e: &BinaryDecodeError) -> &'static str {
    match e {
        BinaryDecodeError::BadSignature
        | BinaryDecodeError::HeaderExtensionTooLarge { .. }
        | BinaryDecodeError::BadFieldLength { .. } => "08P01",
        BinaryDecodeError::UnsupportedFlags { .. } => "0A000",
        BinaryDecodeError::FieldCountMismatch { .. } | BinaryDecodeError::Truncated => "22023",
    }
}

/// **SP-PG-COPY-BIN V1** — render a `BinaryDecodeError` into a precise
/// client-facing message. `failing_row` is the 1-based row index (or 0
/// if the error happened before any row — e.g. header parsing).
fn binary_decode_message(e: &BinaryDecodeError, failing_row: u64) -> String {
    match e {
        BinaryDecodeError::BadSignature => {
            "COPY binary: bad signature (expected PGCOPY\\n\\xff\\r\\n\\0)".to_string()
        }
        BinaryDecodeError::UnsupportedFlags { flags } => {
            format!(
                "COPY binary: header flags 0x{flags:08X} not supported in V1 (SP-PG-COPY-BIN-OID)"
            )
        }
        BinaryDecodeError::HeaderExtensionTooLarge { length } => {
            format!("COPY binary: header extension length {length} exceeds 16 MiB cap")
        }
        BinaryDecodeError::FieldCountMismatch { expected, actual } => {
            format!(
                "COPY binary row {failing_row}: expected {expected} columns, got {actual}"
            )
        }
        BinaryDecodeError::BadFieldLength { length } => {
            format!("COPY binary row {failing_row}: bad field length {length}")
        }
        BinaryDecodeError::Truncated => {
            format!("COPY binary row {failing_row}: truncated")
        }
    }
}

/// **SP-PG-COPY-BULKAPPLY V1** — drain `state.pending_rows` as one
/// engine round-trip.
///
/// Strategy:
/// - If every pending row is **all-non-NULL**, synthesize one
///   multi-row `INSERT INTO t (cols) VALUES (...), (...), ...` and
///   dispatch ONCE. kessel-sql compiles multi-tuple INSERT to
///   `Op::Txn { ops: Vec<Op::Create> }`, so the apply thread sees a
///   single all-or-nothing transaction per batch.
/// - If ANY pending row contains a NULL field, fall back to V1
///   per-row dispatch (the column-omit-on-NULL trick that kessel-sql
///   relies on requires per-row column lists, which multi-row INSERT
///   can't carry). The batch is still wire-correct; the throughput
///   win is forfeited for that one batch.
///
/// On success: clears `pending_rows`, advances `rows_ingested`, and
/// returns None. The state's `batch_start_row` is bumped to the new
/// next-row position.
///
/// On failure: returns Some(bytes) — an ErrorResponse + RFQ frame
/// with the batch's row-range tagged in the message. Caller must
/// emit those bytes and transition to Idle.
///
/// Returns None when `pending_rows` is empty (no-op).
pub fn flush_pending_batch<E: EngineApply + ?Sized>(
    state: &mut CopyInState,
    engine: &E,
) -> Option<Vec<u8>> {
    if state.pending_rows.is_empty() {
        return None;
    }
    let batch_size = state.pending_rows.len();
    let batch_start = state.batch_start_row;
    let batch_end = batch_start + batch_size as u64 - 1;
    let rows = std::mem::take(&mut state.pending_rows);

    let has_null = rows.iter().any(|r| r.iter().any(|f| f.is_none()));
    let result = if has_null {
        // Fallback: per-row dispatch through the existing V1 path.
        // Each row is one `INSERT INTO t [(non_null_cols)] VALUES (...)`
        // — kessel-sql auto-fills NULL for the omitted nullable
        // columns. Atomicity: per-row (same as V1).
        flush_per_row(state, engine, &rows, batch_start)
    } else {
        // Fast path: one multi-row INSERT.
        flush_multi_row(state, engine, &rows, batch_start, batch_end)
    };

    if result.is_none() {
        // Success: advance the row counters + batch start.
        state.rows_ingested += batch_size as u64;
        state.batch_start_row = batch_end + 1;
    } else {
        // Failure: drop pending (already taken above) — defensive
        // even though we already drained.
        state.pending_rows.clear();
    }
    result
}

/// Fast path — one multi-row `INSERT INTO t (cols) VALUES (...), ...`
/// dispatched through `dispatch::dispatch_query`. kessel-sql compiles
/// to `Op::Txn { ops: Vec<Op::Create> }`, so the apply thread sees a
/// single atomic batch.
fn flush_multi_row<E: EngineApply + ?Sized>(
    state: &CopyInState,
    engine: &E,
    rows: &[Vec<Option<Vec<u8>>>],
    batch_start: u64,
    batch_end: u64,
) -> Option<Vec<u8>> {
    let sql = match synthesize_multi_row_insert_sql(
        &state.table,
        state.columns.as_deref(),
        &state.column_kinds,
        rows,
    ) {
        Ok(s) => s,
        Err(reason) => {
            return Some(error_response_then_rfq(
                "22023",
                &format!(
                    "COPY batch starting at row {batch_start}: encode failed: {reason}"
                ),
            ));
        }
    };
    let resp = dispatch::dispatch_query(&sql, engine);
    if let Some((sqlstate, msg)) = extract_error_response(&resp) {
        Some(error_response_then_rfq(
            &sqlstate,
            &format!(
                "COPY batch starting at row {batch_start} (rows {batch_start}..{batch_end}): {msg}"
            ),
        ))
    } else {
        None
    }
}

/// Fallback path — per-row dispatch. Used when any row in the batch
/// has a NULL field (kessel-sql multi-row INSERT can't express NULL
/// in a VALUES tuple via the column-omit trick because all tuples
/// share the same column list). Atomicity: per-row (same as V1).
fn flush_per_row<E: EngineApply + ?Sized>(
    state: &CopyInState,
    engine: &E,
    rows: &[Vec<Option<Vec<u8>>>],
    batch_start: u64,
) -> Option<Vec<u8>> {
    for (i, fields) in rows.iter().enumerate() {
        let row_num = batch_start + i as u64;
        let sql = match synthesize_insert_sql(
            &state.table,
            state.columns.as_deref(),
            &state.column_kinds,
            fields,
        ) {
            Ok(s) => s,
            Err(reason) => {
                return Some(error_response_then_rfq(
                    "22023",
                    &format!(
                        "COPY row {row_num} encode failed: {reason}"
                    ),
                ));
            }
        };
        let resp = dispatch::dispatch_query(&sql, engine);
        if let Some((sqlstate, msg)) = extract_error_response(&resp) {
            return Some(error_response_then_rfq(
                &sqlstate,
                &format!("COPY row {row_num}: {msg}"),
            ));
        }
    }
    None
}

/// Outcome of finalizing a COPY FROM STDIN exchange at CopyDone.
///
/// **SP-PG-COPY-BULKAPPLY V1** — CopyDone now triggers a tail-drain of
/// any rows still sitting in `pending_rows`, which can fail (a
/// constraint violation in the final partial batch). Distinguish the
/// success and failure paths so the server loop emits the right
/// bytes.
#[derive(Debug)]
pub enum CopyDoneOutcome {
    /// Tail-drain succeeded; emit the bytes (CommandComplete + RFQ).
    Ok { bytes: Vec<u8> },
    /// Tail-drain failed; emit the bytes (ErrorResponse + RFQ).
    Failed { bytes: Vec<u8> },
}

/// **SP-PG-COPY-BULKAPPLY V1** — finalize a successful COPY FROM
/// STDIN exchange (CopyDone received). Drains any rows still in
/// `pending_rows` as a final multi-row INSERT batch, then emits
/// `CommandComplete("COPY N")` + `ReadyForQuery('I')`.
///
/// If the tail-drain fails (e.g. a NOT NULL violation in the last
/// partial batch), emits `ErrorResponse + RFQ` per the standard
/// error path.
pub fn finalize_copy_in_success<E: EngineApply + ?Sized>(
    state: &mut CopyInState,
    engine: &E,
) -> CopyDoneOutcome {
    if !state.pending_rows.is_empty() {
        if let Some(fail_bytes) = flush_pending_batch(state, engine) {
            return CopyDoneOutcome::Failed { bytes: fail_bytes };
        }
    }
    CopyDoneOutcome::Ok {
        bytes: finalize_copy_in_success_no_flush(state),
    }
}

/// Inner: emit CommandComplete + RFQ. Separated so the dispatch KAT
/// can lock the byte shape independently of the flush path.
pub fn finalize_copy_in_success_no_flush(state: &CopyInState) -> Vec<u8> {
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
    let (table, columns, format) = match parsed {
        ParsedCopy::To { table, columns, format } => (table, columns, format),
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
    // Validate supplied column list against the schema. Capture the
    // chosen column names too — used for the CSV HEADER row.
    let (chosen_indices, chosen_names): (Vec<usize>, Vec<String>) = match columns.as_ref() {
        Some(cols) => {
            let mut idxs = Vec::with_capacity(cols.len());
            let mut names = Vec::with_capacity(cols.len());
            for c in cols {
                match schema_cols.iter().position(|s| s.name == *c) {
                    Some(idx) => {
                        idxs.push(idx);
                        names.push(c.clone());
                    }
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
            (idxs, names)
        }
        None => {
            let idxs: Vec<usize> = (0..schema_cols.len()).collect();
            let names: Vec<String> = schema_cols.iter().map(|s| s.name.clone()).collect();
            (idxs, names)
        }
    };
    let ncols = chosen_indices.len() as u16;

    // SP-PG-COPY-BIN V1 — pre-reject unsupported column types at
    // COPY-TO-start time (UUID / JSONB / ARRAY). Same set as
    // `binary_format_supported_for_oid`. Mirrors the COPY FROM pre-check.
    //
    // SP-PG-COPY-BIN-NUMERIC V1 (2026-06-02) — NUMERIC is now admitted:
    // the SP-PG-EXTQ-BIN-NUMERIC codec wired into
    // `encode_binary_value` handles `PG_TYPE_NUMERIC`, and the
    // per-column TO encode call site in this function dispatches
    // through it unchanged.
    if format.is_binary() {
        for (i, &idx) in chosen_indices.iter().enumerate() {
            let kind = schema_cols[idx].kind;
            let oid = field_kind_to_oid(kind);
            if !binary_format_supported_for_oid(oid) {
                return error_response_then_rfq(
                    "0A000",
                    &format!(
                        "COPY binary: column \"{}\" type OID {oid} not supported in V1 (SP-PG-COPY-BIN-EXTRA)",
                        chosen_names[i]
                    ),
                );
            }
        }
    }
    // SP-PG-COPY-BIN V1 — per-column PG type OIDs for the chosen
    // projection. Used by the binary value encoder per row.
    let chosen_oids: Vec<u32> = chosen_indices
        .iter()
        .map(|&i| field_kind_to_oid(schema_cols[i].kind))
        .collect();

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

    // Build CopyOutResponse + (optional CSV header row OR binary
    // signature header) + N × CopyData + (optional binary EOD marker)
    // + CopyDone + CommandComplete("COPY N") + RFQ.
    let mut out = Vec::new();
    if format.is_binary() {
        out.extend_from_slice(&encode_copy_out_response_binary(ncols));
        // PG §55.2.7 — emit the 19-byte signature header as the FIRST
        // CopyData payload.
        out.extend_from_slice(&encode_copy_data(&encode_binary_header()));
    } else {
        out.extend_from_slice(&encode_copy_out_response(ncols));
    }

    // SP-PG-COPY-CSV V1 — emit HEADER row first if requested.
    if let CopyFormat::Csv(opts) = &format {
        if opts.header {
            let header_refs: Vec<Option<&str>> = chosen_names
                .iter()
                .map(|n| Some(n.as_str()))
                .collect();
            let payload = encode_csv_record(&header_refs, opts);
            out.extend_from_slice(&encode_copy_data(&payload));
        }
    }

    let mut emitted = 0u64;
    for row in &rows {
        // Project the chosen columns from the full row, borrowing
        // the column bytes (`Option<&[u8]>` per column).
        let projected_refs: Vec<Option<&[u8]>> = chosen_indices
            .iter()
            .map(|&i| row.get(i).and_then(|c| c.as_deref()))
            .collect();
        let payload = match &format {
            CopyFormat::Text => encode_text_row(&projected_refs),
            CopyFormat::Csv(opts) => {
                // Convert byte refs → str refs for the CSV encoder.
                // Lossy-decode on non-UTF-8 (matches the CSV parser
                // contract).
                let str_refs: Vec<Option<String>> = projected_refs
                    .iter()
                    .map(|o| {
                        o.map(|b| String::from_utf8_lossy(b).into_owned())
                    })
                    .collect();
                let str_opt_refs: Vec<Option<&str>> =
                    str_refs.iter().map(|o| o.as_deref()).collect();
                encode_csv_record(&str_opt_refs, opts)
            }
            CopyFormat::Binary => {
                // SP-PG-COPY-BIN V1 — per-column binary encoding via the
                // existing SP-PG-EXTQ-BIN-RESULTS encoder. Decode the
                // text-format DataRow column bytes, then re-encode as
                // binary per OID.
                //
                // Owned-bytes intermediate so the borrowed slice lives
                // long enough for `encode_binary_row` (which takes
                // `&[Option<&[u8]>]`).
                let mut binary_owned: Vec<Option<Vec<u8>>> =
                    Vec::with_capacity(projected_refs.len());
                let mut encode_error: Option<BinaryEncodeError> = None;
                let mut error_col = 0usize;
                for (i, col) in projected_refs.iter().enumerate() {
                    match col {
                        None => binary_owned.push(None),
                        Some(text) => {
                            let oid = chosen_oids.get(i).copied().unwrap_or(0);
                            match encode_binary_value(text, oid) {
                                Ok(bytes) => binary_owned.push(Some(bytes)),
                                Err(e) => {
                                    encode_error = Some(e);
                                    error_col = i;
                                    break;
                                }
                            }
                        }
                    }
                }
                if let Some(e) = encode_error {
                    // Abort the whole COPY TO — emit ErrorResponse +
                    // RFQ (no CopyDone since the COPY was aborted
                    // mid-stream).
                    return error_response_then_rfq(
                        "0A000",
                        &format!(
                            "COPY binary TO row {} column {} (OID {}): {:?}",
                            emitted + 1,
                            error_col,
                            chosen_oids.get(error_col).copied().unwrap_or(0),
                            e
                        ),
                    );
                }
                let binary_refs: Vec<Option<&[u8]>> =
                    binary_owned.iter().map(|o| o.as_deref()).collect();
                encode_binary_row(&binary_refs)
            }
        };
        out.extend_from_slice(&encode_copy_data(&payload));
        emitted += 1;
    }
    // SP-PG-COPY-BIN V1 — emit the end-of-data marker as a final
    // CopyData before CopyDone.
    if format.is_binary() {
        out.extend_from_slice(&encode_copy_data(&encode_binary_end_of_data()));
    }
    out.extend_from_slice(&encode_copy_done());
    out.extend_from_slice(&encode_command_complete(&copy_tag(emitted)));
    out.extend_from_slice(&encode_ready_for_query(b'I'));
    // Defensively silence unused-import warning when no CSV path is
    // exercised at the call site.
    let _ = CsvOptions::default;
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
        | RejectReason::UnknownSource
        | RejectReason::UnsupportedCsvOption { .. } => "0A000",
        // SP-PG-COPY-CSV V1 — invalid CSV option value (e.g. multi-byte
        // DELIMITER) is a SQL-shape error, not a capability gap. Maps
        // to the canonical `22023 invalid_parameter_value`.
        RejectReason::InvalidCsvOptionValue { .. } => "22023",
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
        RejectReason::UnsupportedCsvOption { option } => {
            format!(
                "COPY csv option {option} not supported in V1 (SP-PG-COPY-CSV-FORCEQUOTE / SP-PG-COPY-CSV-ENCODING)"
            )
        }
        RejectReason::InvalidCsvOptionValue { option, value } => {
            format!("COPY csv {option} must be a single character (got '{value}')")
        }
    }
}

/// Synthesize an `INSERT INTO <table> [(cols)] VALUES (...)`
/// statement from a parsed COPY row.
///
/// **NULL handling**: kessel-sql's INSERT VALUES parser accepts ONLY
/// `Tok::Int(...)` and `Tok::Str(...)` literals — it has no `NULL`
/// keyword in the value position. SP-PG-COPY works around this by
/// OMITTING the NULL columns from BOTH the column list AND the
/// VALUES tuple — kessel-sql then auto-fills NULL for any nullable
/// omitted column (per the SP86 default-fill semantics in
/// kessel-sql::lib.rs ~line 1219-1236). This means: a NOT NULL
/// column receiving a `\N` COPY value will surface as a clean
/// "missing NOT NULL column" error at INSERT time, which is exactly
/// PG's `23502 not_null_violation` semantics.
///
/// **Non-NULL field rendering**: bare decimal for numeric `FieldKind`s
/// (I8/I16/I32/I64/I128 + U8/U16/U32/U64/U128 + Bool + Timestamp +
/// Fixed) or `'...'`-quoted (with `'` doubled) for byte-string
/// kinds (Char/Bytes/Ref/OverflowRef), matching kessel-sql's
/// `lit_to_value` type-pairing.
///
/// `kinds` MUST have the same length as `fields` — if empty (the
/// pre-T2-fix path used by tests that don't carry kinds), every
/// field falls back to the quoted form (which works for CHAR
/// columns but trips integer columns at the kessel-sql parser).
///
/// If the caller supplied an explicit column list (`COPY t (col1,
/// col2) FROM STDIN`), V1 always emits the full caller-supplied
/// column list — NULL fields are dropped from BOTH the column list
/// and the values tuple at the same position, so column/value
/// counts stay matched. If columns is `None`, V1 uses `engine.
/// describe_table`'s schema order, and drops the same way.
fn synthesize_insert_sql(
    table: &str,
    columns: Option<&[String]>,
    kinds: &[kessel_catalog::FieldKind],
    fields: &[Option<Vec<u8>>],
) -> Result<String, String> {
    // Compute the (col, kind, text) tuples for NON-NULL fields only
    // — NULL fields are dropped so kessel-sql's INSERT-omits-column
    // auto-NULL-fill applies.
    let cols_slice: Option<&[String]> = columns;
    let mut entries: Vec<(Option<&str>, kessel_catalog::FieldKind, String)> =
        Vec::with_capacity(fields.len());
    for (i, v) in fields.iter().enumerate() {
        if let Some(bytes) = v {
            let text = std::str::from_utf8(bytes)
                .map_err(|_| "field is not valid UTF-8".to_string())?
                .to_string();
            let col_name = cols_slice.and_then(|c| c.get(i).map(|s| s.as_str()));
            let kind = kinds
                .get(i)
                .copied()
                // Fallback: pick a kind that triggers quoted rendering.
                .unwrap_or(kessel_catalog::FieldKind::Char(0));
            entries.push((col_name, kind, text));
        }
    }

    let mut s = String::with_capacity(64);
    s.push_str("INSERT INTO ");
    s.push_str(table);
    // Always emit the column list if we have one OR if any column
    // was dropped (so kessel-sql knows which positions we did/didn't
    // provide). The exception: if columns is None AND every field
    // is non-NULL AND we have no schema, we omit the column list and
    // pass the values positionally — but in practice the COPY path
    // always has columns from dispatch_copy_in_start.
    let have_col_list = cols_slice.is_some();
    if have_col_list {
        s.push_str(" (");
        for (i, (col, _, _)) in entries.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            s.push_str(col.expect("col list always present when columns is Some"));
        }
        s.push(')');
    }
    s.push_str(" VALUES (");
    for (i, (_, kind, text)) in entries.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        let want_quoted = matches!(
            kind,
            kessel_catalog::FieldKind::Char(_)
                | kessel_catalog::FieldKind::Bytes(_)
                | kessel_catalog::FieldKind::Ref
                | kessel_catalog::FieldKind::OverflowRef
        );
        if want_quoted {
            s.push('\'');
            for c in text.chars() {
                if c == '\'' {
                    s.push_str("''");
                } else {
                    s.push(c);
                }
            }
            s.push('\'');
        } else {
            // Numeric / bool / timestamp / fixed — render as a bare
            // token. Trust the COPY-format input bytes are already a
            // decimal literal (PG text-format for numeric types IS
            // the decimal representation; pg_dump emits `42` not
            // `"42"`).
            s.push_str(text);
        }
    }
    s.push(')');
    Ok(s)
}

/// **SP-PG-COPY-BULKAPPLY V1** — synthesize a single multi-row
/// `INSERT INTO t (cols) VALUES (...), (...), ..., (...)`. Caller
/// MUST guarantee every row in `rows` is all-non-NULL — multi-row
/// INSERT can't carry per-tuple column lists, and kessel-sql lacks
/// a `NULL` literal in VALUES, so a NULL field would require dropping
/// a column from a tuple, which breaks the cols/values count match.
///
/// kessel-sql compiles the resulting SQL to `Op::Txn { ops: Vec<
/// Op::Create> }` (per `crates/kessel-sql/src/lib.rs` lines 1245-1260),
/// so the engine sees ONE atomic round-trip for the whole batch.
///
/// Returns `Err(...)` if any field is not valid UTF-8 (matching the
/// per-row synthesizer's contract).
fn synthesize_multi_row_insert_sql(
    table: &str,
    columns: Option<&[String]>,
    kinds: &[kessel_catalog::FieldKind],
    rows: &[Vec<Option<Vec<u8>>>],
) -> Result<String, String> {
    debug_assert!(!rows.is_empty(), "caller must pre-check empty batch");
    debug_assert!(
        rows.iter().all(|r| r.iter().all(|f| f.is_some())),
        "caller must pre-check has_null fallback before multi-row synth"
    );

    let cols_slice: Option<&[String]> = columns;

    // Pre-allocate generously: ~50 bytes per tuple is the sysbench
    // shape. 1024 rows → 50 KiB.
    let mut s = String::with_capacity(rows.len() * 64);
    s.push_str("INSERT INTO ");
    s.push_str(table);
    if let Some(cols) = cols_slice {
        s.push_str(" (");
        for (i, c) in cols.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            s.push_str(c);
        }
        s.push(')');
    }
    s.push_str(" VALUES ");
    for (ri, row) in rows.iter().enumerate() {
        if ri > 0 {
            s.push_str(", ");
        }
        s.push('(');
        for (i, v) in row.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            let bytes = v
                .as_ref()
                .expect("caller must pre-check has_null before multi-row synth");
            let text = std::str::from_utf8(bytes)
                .map_err(|_| "field is not valid UTF-8".to_string())?;
            let kind = kinds
                .get(i)
                .copied()
                .unwrap_or(kessel_catalog::FieldKind::Char(0));
            let want_quoted = matches!(
                kind,
                kessel_catalog::FieldKind::Char(_)
                    | kessel_catalog::FieldKind::Bytes(_)
                    | kessel_catalog::FieldKind::Ref
                    | kessel_catalog::FieldKind::OverflowRef
            );
            if want_quoted {
                s.push('\'');
                for c in text.chars() {
                    if c == '\'' {
                        s.push_str("''");
                    } else {
                        s.push(c);
                    }
                }
                s.push('\'');
            } else {
                s.push_str(text);
            }
        }
        s.push(')');
    }
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
            format: CopyFormat::Text,
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
            format: CopyFormat::Text,
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
            format: CopyFormat::Text,
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

    /// SP-PG-COPY T2 / SP-PG-COPY-BULKAPPLY V1: a single CopyData
    /// containing 3 rows with `batch_size=1` ingests 3 rows per-row
    /// (V1 baseline shape). The applied SQL for each row contains
    /// the `INSERT INTO t` shape with the right values.
    #[test]
    fn t2_process_copy_data_three_rows_ingests_three_inserts() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(32), nullable: true },
        ];
        let eng = make_engine(cols);
        let mut state = CopyInState::new(
            "t".to_string(),
            Some(vec!["id".to_string(), "name".to_string()]),
            2,
        );
        // Force per-row dispatch (V1 baseline shape) so this KAT's
        // SQL-shape assertion still locks the per-row synthesis path.
        state.batch_size = 1;
        let data = b"1\thello\n2\tworld\n3\tfoo\n";
        match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Continue { state } => {
                assert_eq!(state.rows_ingested, 3);
                assert!(state.carry.is_empty());
                assert!(state.pending_rows.is_empty());
                let applied = eng.applied.lock().unwrap();
                assert_eq!(applied.len(), 3);
                // CopyInState::new (no kinds) → both fields quoted +
                // both kept (neither is NULL).
                assert!(
                    applied[0].contains("INSERT INTO t (id, name) VALUES ('1', 'hello')"),
                    "unexpected SQL: {}",
                    applied[0]
                );
                assert!(applied[1].contains("'world'"));
                assert!(applied[2].contains("'foo'"));
            }
            other => panic!("expected Continue, got {other:?}"),
        }
    }

    /// SP-PG-COPY-BULKAPPLY V1: a CopyData containing 3 rows with
    /// `batch_size=1024` (the default) BUFFERS — none are applied
    /// until the flush triggers (CopyDone). After `flush_pending_batch`,
    /// the engine has seen ONE multi-row INSERT covering all 3 rows.
    #[test]
    fn bulkapply_three_rows_under_batch_size_buffers_until_flush() {
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
        // Default batch_size=1024 → 3 rows do not trigger flush.
        let data = b"1\thello\n2\tworld\n3\tfoo\n";
        let mut state = match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Continue { state } => state,
            other => panic!("expected Continue, got {other:?}"),
        };
        // No INSERT has fired yet — rows are buffered.
        assert_eq!(state.rows_ingested, 0);
        assert_eq!(state.pending_rows.len(), 3);
        assert_eq!(eng.applied.lock().unwrap().len(), 0);

        // Drain via flush_pending_batch — one multi-row INSERT to
        // the engine.
        let result = flush_pending_batch(&mut state, &eng);
        assert!(result.is_none(), "flush should succeed");
        assert_eq!(state.rows_ingested, 3);
        assert!(state.pending_rows.is_empty());
        let applied = eng.applied.lock().unwrap();
        assert_eq!(
            applied.len(),
            1,
            "expected ONE multi-row INSERT for 3 buffered rows; got {} dispatches: {:?}",
            applied.len(),
            *applied
        );
        // The single dispatched SQL must carry three VALUES tuples.
        assert!(
            applied[0].contains("VALUES ('1', 'hello'), ('2', 'world'), ('3', 'foo')"),
            "unexpected SQL: {}",
            applied[0]
        );
        // Locked: exactly 2 ", (" delimiters between 3 tuples.
        assert_eq!(applied[0].matches("), (").count(), 2);
    }

    /// SP-PG-COPY-BULKAPPLY V1: when `pending_rows` reaches
    /// `batch_size`, a flush fires automatically inside
    /// `process_copy_data`. 2 * batch_size rows + batch_size=4 →
    /// 2 multi-row INSERTs fired during processing.
    #[test]
    fn bulkapply_threshold_flush_during_processing() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = make_engine(cols);
        let mut state = CopyInState::new(
            "t".to_string(),
            Some(vec!["id".to_string()]),
            1,
        );
        state.batch_size = 4;
        // 8 rows = 2 batches at batch_size=4.
        let data = b"1\n2\n3\n4\n5\n6\n7\n8\n";
        match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Continue { state } => {
                assert_eq!(state.rows_ingested, 8);
                assert!(state.pending_rows.is_empty());
                let applied = eng.applied.lock().unwrap();
                assert_eq!(
                    applied.len(),
                    2,
                    "expected exactly 2 batches at batch_size=4 for 8 rows"
                );
                // Each batch is one multi-row INSERT with 4 tuples.
                for sql in applied.iter() {
                    assert_eq!(sql.matches("), (").count(), 3);
                }
            }
            other => panic!("expected Continue, got {other:?}"),
        }
    }

    /// SP-PG-COPY-BULKAPPLY V1: a row containing a NULL field
    /// triggers the per-row fallback for the WHOLE batch (the
    /// kessel-sql column-omit trick can't be expressed in multi-row
    /// INSERT). Flush emits N per-row INSERTs instead of 1 multi-row.
    #[test]
    fn bulkapply_null_in_batch_falls_back_to_per_row() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(32), nullable: true },
        ];
        let eng = make_engine(cols);
        let mut state = CopyInState::new(
            "t".to_string(),
            Some(vec!["id".to_string(), "name".to_string()]),
            2,
        );
        state.batch_size = 3;
        // 3 rows; middle has a NULL field.
        let data = b"1\thello\n2\t\\N\n3\tworld\n";
        match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Continue { state } => {
                assert_eq!(state.rows_ingested, 3);
                let applied = eng.applied.lock().unwrap();
                // Per-row fallback: 3 separate INSERTs.
                assert_eq!(
                    applied.len(),
                    3,
                    "NULL in batch must fall back to per-row dispatch"
                );
                // None of the dispatches is multi-row (no ", (" delim).
                for sql in applied.iter() {
                    assert!(
                        !sql.contains("), ("),
                        "per-row fallback SQL must not be multi-tuple: {sql}"
                    );
                }
            }
            other => panic!("expected Continue, got {other:?}"),
        }
    }

    /// SP-PG-COPY-BULKAPPLY V1: a constraint error in a batch surfaces
    /// the "in batch starting at row N" tag (the engine's Op::Txn
    /// failure doesn't carry the exact failing op index — see design
    /// §9 weak-spot #5).
    #[test]
    fn bulkapply_engine_error_in_batch_tags_batch_range() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = make_engine(cols);
        // The multi-row dispatch result is a single Constraint error.
        *eng.result.lock().unwrap() = vec![OpResult::Constraint(
            "UNIQUE violated".into(),
        )];
        let mut state = CopyInState::new(
            "t".to_string(),
            Some(vec!["id".to_string()]),
            1,
        );
        state.batch_size = 4;
        let data = b"1\n2\n3\n4\n";
        match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Failed { bytes } => {
                assert_eq!(bytes[0], b'E');
                // Tag must mention "batch starting at row 1".
                assert!(
                    bytes
                        .windows(b"batch starting at row 1".len())
                        .any(|w| w == b"batch starting at row 1"),
                    "expected 'batch starting at row 1' tag in error"
                );
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    /// SP-PG-COPY-BULKAPPLY V1: an empty batch is a no-op for
    /// `flush_pending_batch`. CopyDone with zero rows ingested must
    /// emit `COPY 0` and dispatch nothing to the engine.
    #[test]
    fn bulkapply_empty_batch_is_noop() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = make_engine(cols);
        let mut state = CopyInState::new(
            "t".to_string(),
            Some(vec!["id".to_string()]),
            1,
        );
        let result = flush_pending_batch(&mut state, &eng);
        assert!(result.is_none());
        assert_eq!(state.rows_ingested, 0);
        assert_eq!(eng.applied.lock().unwrap().len(), 0);
        // CopyDone path emits "COPY 0".
        match finalize_copy_in_success(&mut state, &eng) {
            CopyDoneOutcome::Ok { bytes } => {
                assert!(bytes.windows(b"COPY 0\0".len()).any(|w| w == b"COPY 0\0"));
            }
            other => panic!("expected Ok, got {other:?}"),
        }
        // Still no engine dispatch.
        assert_eq!(eng.applied.lock().unwrap().len(), 0);
    }

    /// SP-PG-COPY-BULKAPPLY V1: CopyDone with a partial tail batch
    /// (rows < batch_size) triggers a tail-drain — one multi-row
    /// INSERT fires + the COPY N tag reports the right count.
    #[test]
    fn bulkapply_copydone_drains_tail() {
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
        // 5 rows; default batch_size=1024 so they all sit in pending.
        let data = b"1\n2\n3\n4\n5\n";
        let mut state = match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Continue { state } => state,
            other => panic!("expected Continue, got {other:?}"),
        };
        assert_eq!(state.pending_rows.len(), 5);
        assert_eq!(state.rows_ingested, 0);

        match finalize_copy_in_success(&mut state, &eng) {
            CopyDoneOutcome::Ok { bytes } => {
                assert!(bytes.windows(b"COPY 5\0".len()).any(|w| w == b"COPY 5\0"));
            }
            other => panic!("expected Ok, got {other:?}"),
        }
        assert_eq!(state.rows_ingested, 5);
        let applied = eng.applied.lock().unwrap();
        assert_eq!(applied.len(), 1, "tail-drain should fire ONE multi-row INSERT");
        // 5 tuples = 4 separators.
        assert_eq!(applied[0].matches("), (").count(), 4);
    }

    /// SP-PG-COPY T2: a CopyData with an incomplete trailing row
    /// stashes the partial bytes in carry; the next CopyData picks
    /// up where the first left off. Uses batch_size=1 so each
    /// complete row dispatches immediately (V1-baseline shape).
    #[test]
    fn t2_process_copy_data_carries_partial_row() {
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let eng = make_engine(cols);
        let mut state = CopyInState::new(
            "t".to_string(),
            Some(vec!["id".to_string()]),
            1,
        );
        state.batch_size = 1; // V1-baseline (per-row) for the assertion below.
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

    /// SP-PG-COPY T2: a NULL field (`\N`) is OMITTED from both the
    /// INSERT column list and the VALUES tuple — kessel-sql's
    /// "omitted nullable column auto-fills NULL" semantics (SP86)
    /// is what V1 relies on, because kessel-sql has no `NULL`
    /// literal in INSERT VALUES. So an `id\tname` table with
    /// `1\t\N` COPY data synthesizes `INSERT INTO t (id) VALUES
    /// ('1')` — the `name` column is dropped, kessel-sql auto-fills
    /// NULL for it (because it's nullable).
    #[test]
    fn t2_process_copy_data_null_field_drops_from_insert() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(32), nullable: true },
        ];
        let eng = make_engine(cols);
        let mut state = CopyInState::new(
            "t".to_string(),
            Some(vec!["id".to_string(), "name".to_string()]),
            2,
        );
        // BULKAPPLY V1: NULL-row fallback also drops the column from
        // the per-row INSERT, but the row is BUFFERED until a flush.
        // Set batch_size=1 to make the assertion below match the
        // original V1 "applied immediately" semantics.
        state.batch_size = 1;
        let data = b"1\t\\N\n";
        match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Continue { state } => {
                assert_eq!(state.rows_ingested, 1);
            }
            other => panic!("expected Continue, got {other:?}"),
        }
        let applied = eng.applied.lock().unwrap();
        assert_eq!(applied.len(), 1);
        // V1: kinds is empty because new() doesn't carry kinds, so
        // the `id` column gets quoted. The NULL `name` column is
        // dropped from both the column list and values.
        assert!(
            applied[0].contains("INSERT INTO t (id) VALUES ('1')"),
            "unexpected SQL: {}",
            applied[0]
        );
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
        let mut state = CopyInState::new("t".to_string(), Some(vec!["id".to_string()]), 1);
        state.batch_size = 1; // V1-baseline (per-row) for the count assertion below.
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
        let mut state = CopyInState::new("t".to_string(), Some(vec!["id".to_string()]), 1);
        // V1-baseline (per-row) — keeps the "row 2" tag deterministic
        // independent of batching (BULKAPPLY would compute the same
        // tag from rows_ingested+pending+1 but the legacy assertion
        // is the simplest lock).
        state.batch_size = 1;
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
        let eng = make_engine(cols);
        // First row succeeds (Ok), second triggers Constraint.
        *eng.result.lock().unwrap() = vec![
            OpResult::Constraint("NOT NULL violated".into()),
            OpResult::Ok,
        ];
        let mut state = CopyInState::new("t".to_string(), Some(vec!["id".to_string()]), 1);
        // V1-baseline (per-row) — each row gets its own batch so the
        // per-row constraint failure tag still says "row 2".
        state.batch_size = 1;
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
        let mut state = CopyInState::new("t".to_string(), Some(vec!["id".to_string()]), 1);
        state.batch_size = 1; // V1-baseline (per-row) for the count assertion below.
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
        let mut state = CopyInState::new("t".to_string(), None, 1);
        state.rows_ingested = 5;
        // SP-PG-COPY-BULKAPPLY V1 — finalize must NOT have any
        // pending rows when called for the "already-flushed" path
        // this test exercises (the dispatch test below covers the
        // tail-drain path).
        assert!(state.pending_rows.is_empty());
        let bytes = finalize_copy_in_success_no_flush(&state);
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
            format: CopyFormat::Text,
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
            format: CopyFormat::Text,
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
            format: CopyFormat::Text,
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
            format: CopyFormat::Text,
        };
        let bytes = dispatch_copy_to(parsed, &eng);
        // The payload should be `7\t\N\n`.
        assert!(bytes.windows(b"7\t\\N\n".len()).any(|w| w == b"7\t\\N\n"));
    }

    // ─── SP-PG-COPY-CSV V1 ──────────────────────────────────────────────

    /// SP-PG-COPY-CSV T1: COPY FROM with FORMAT csv + HEADER drops the
    /// header row and ingests the data rows through the per-row /
    /// multi-row pipeline. With batch_size=1 the SQL synthesis is the
    /// V1 baseline shape (lockable in the assertion).
    #[test]
    fn csv_t1_copy_from_csv_with_header_skips_header_and_ingests() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(32), nullable: true },
        ];
        let eng = make_engine(cols);
        let csv_opts = CsvOptions {
            header: true,
            ..CsvOptions::default()
        };
        let mut state = CopyInState::new_with_format(
            "t".to_string(),
            Some(vec!["id".to_string(), "name".to_string()]),
            2,
            vec![FieldKind::I64, FieldKind::Char(32)],
            CopyFormat::Csv(csv_opts),
        );
        state.batch_size = 1;
        let data = b"id,name\n1,hello\n2,\"hello, world\"\n";
        match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Continue { state } => {
                assert_eq!(state.rows_ingested, 2);
                let applied = eng.applied.lock().unwrap();
                assert_eq!(applied.len(), 2);
                // Schema-aware: id is numeric (bare), name is CHAR (quoted).
                assert!(
                    applied[0].contains("VALUES (1, 'hello')"),
                    "unexpected SQL: {}",
                    applied[0]
                );
                // Embedded comma in the quoted CSV field must survive.
                assert!(
                    applied[1].contains("'hello, world'"),
                    "unexpected SQL: {}",
                    applied[1]
                );
            }
            other => panic!("expected Continue, got {other:?}"),
        }
    }

    /// SP-PG-COPY-CSV T1: COPY FROM CSV with an embedded-quote row
    /// decodes the doubled-quote escape correctly.
    #[test]
    fn csv_t1_copy_from_csv_doubled_quote_decoded() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(32), nullable: true },
        ];
        let eng = make_engine(cols);
        let mut state = CopyInState::new_with_format(
            "t".to_string(),
            Some(vec!["id".to_string(), "name".to_string()]),
            2,
            vec![FieldKind::I64, FieldKind::Char(32)],
            CopyFormat::Csv(CsvOptions::default()),
        );
        state.batch_size = 1;
        // `"Bob ""the builder"""` → `Bob "the builder"`
        let data = b"1,\"Bob \"\"the builder\"\"\"\n";
        match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Continue { state } => {
                assert_eq!(state.rows_ingested, 1);
                let applied = eng.applied.lock().unwrap();
                // kessel-sql quotes strings with `'` and doubles
                // embedded `'`. The CSV-decoded value `Bob "the builder"`
                // contains no single quotes so renders verbatim inside
                // the SQL literal.
                assert!(
                    applied[0].contains(r#"'Bob "the builder"'"#),
                    "unexpected SQL: {}",
                    applied[0]
                );
            }
            other => panic!("expected Continue, got {other:?}"),
        }
    }

    /// SP-PG-COPY-CSV T1: empty unquoted CSV field is NULL; the
    /// dispatcher drops it (kessel-sql NULL-omit trick).
    #[test]
    fn csv_t1_copy_from_csv_empty_unquoted_is_null() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(32), nullable: true },
        ];
        let eng = make_engine(cols);
        let mut state = CopyInState::new_with_format(
            "t".to_string(),
            Some(vec!["id".to_string(), "name".to_string()]),
            2,
            vec![FieldKind::I64, FieldKind::Char(32)],
            CopyFormat::Csv(CsvOptions::default()),
        );
        state.batch_size = 1;
        let data = b"1,\n";
        match process_copy_data(data, state, &eng) {
            CopyDataOutcome::Continue { state } => {
                assert_eq!(state.rows_ingested, 1);
                let applied = eng.applied.lock().unwrap();
                // The NULL `name` field is dropped — only id is present.
                assert!(
                    applied[0].contains("INSERT INTO t (id) VALUES (1)"),
                    "unexpected SQL: {}",
                    applied[0]
                );
            }
            other => panic!("expected Continue, got {other:?}"),
        }
    }

    /// SP-PG-COPY-CSV T1: a CSV record with a newline INSIDE a quoted
    /// field is reassembled across a frame boundary via the carry
    /// buffer.
    #[test]
    fn csv_t1_copy_from_csv_quoted_newline_carries_across_frames() {
        let cols = vec![PgColumn {
            name: "v".into(),
            kind: FieldKind::Char(64),
            nullable: false,
        }];
        let eng = make_engine(cols);
        let mut state = CopyInState::new_with_format(
            "t".to_string(),
            Some(vec!["v".to_string()]),
            1,
            vec![FieldKind::Char(64)],
            CopyFormat::Csv(CsvOptions::default()),
        );
        state.batch_size = 1;
        // First frame: opens a quoted field that doesn't close.
        let data1 = b"\"line1\n";
        let state = match process_copy_data(data1, state, &eng) {
            CopyDataOutcome::Continue { state } => state,
            other => panic!("expected Continue, got {other:?}"),
        };
        assert_eq!(state.rows_ingested, 0); // record incomplete
        assert!(!state.carry.is_empty());
        // Second frame: closes the quote + ends the record.
        let data2 = b"line2\"\n";
        match process_copy_data(data2, state, &eng) {
            CopyDataOutcome::Continue { state } => {
                assert_eq!(state.rows_ingested, 1);
            }
            other => panic!("expected Continue, got {other:?}"),
        }
        let applied = eng.applied.lock().unwrap();
        assert_eq!(applied.len(), 1);
        // The reassembled value is "line1\nline2".
        assert!(
            applied[0].contains("'line1\nline2'"),
            "unexpected SQL: {}",
            applied[0]
        );
    }

    /// SP-PG-COPY-CSV T1: COPY TO with FORMAT csv emits CSV-encoded
    /// CopyData rows (comma-separated, quoted when needed).
    #[test]
    fn csv_t1_copy_to_csv_emits_csv_payload() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(32), nullable: true },
        ];
        // Row with an embedded comma needs to round-trip as a quoted
        // CSV field.
        let r1 = build_record(
            &cols,
            &[Value::Int(1), Value::Blob(b"hello, world\0\0\0\0".to_vec())],
        );
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
            format: CopyFormat::Csv(CsvOptions::default()),
        };
        let bytes = dispatch_copy_to(parsed, &eng);
        assert_eq!(bytes[0], b'H');
        // The emitted CopyData payload must contain the quoted CSV
        // representation of `hello, world`.
        assert!(
            bytes.windows(b"\"hello, world\"".len())
                .any(|w| w == b"\"hello, world\""),
            "expected quoted CSV field in CopyData payload"
        );
        assert!(bytes.windows(b"COPY 1\0".len()).any(|w| w == b"COPY 1\0"));
    }

    /// SP-PG-COPY-CSV T1: COPY TO with FORMAT csv + HEADER emits a
    /// first CopyData containing the column names as a CSV record.
    #[test]
    fn csv_t1_copy_to_csv_with_header_emits_column_names_first() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(32), nullable: true },
        ];
        let eng = CopyTestEngine {
            cols: cols.clone(),
            result: std::sync::Mutex::new(Vec::new()),
            row_bytes: Vec::new(),
            table: "t".to_string(),
            applied: std::sync::Mutex::new(Vec::new()),
        };
        let csv_opts = CsvOptions {
            header: true,
            ..CsvOptions::default()
        };
        let parsed = ParsedCopy::To {
            table: "t".to_string(),
            columns: None,
            format: CopyFormat::Csv(csv_opts),
        };
        let bytes = dispatch_copy_to(parsed, &eng);
        assert_eq!(bytes[0], b'H');
        // The first CopyData after H carries `id,name\n`.
        let h_len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
        let p = 1 + h_len;
        assert_eq!(bytes[p], b'd');
        let cd_len =
            u32::from_be_bytes([bytes[p + 1], bytes[p + 2], bytes[p + 3], bytes[p + 4]])
                as usize;
        let payload = &bytes[p + 5..p + 5 + (cd_len - 4)];
        assert_eq!(payload, b"id,name\n");
    }

    // ─── SP-PG-COPY-BIN-NUMERIC V1 ───────────────────────────────────
    //
    // T2 integration KATs covering the dispatch-side enablement: the
    // COPY-BIN NUMERIC pre-reject is dropped, so a table with an
    // `I128` column (PG OID 1700) is admitted in both COPY FROM and
    // COPY TO binary directions. The per-row codec used by the FROM
    // decoder (`decode_binary_param`) and TO encoder (`encode_binary_value`)
    // routes through `extq::binary_numeric::{decode_numeric_binary,
    // encode_numeric_binary}` thanks to the SP-PG-EXTQ-BIN-NUMERIC
    // T3 wiring.

    /// SP-PG-COPY-BIN-NUMERIC T2: encoding the NUMERIC value `42`
    /// through `encode_binary_value` (the per-column TO encoder)
    /// produces the same wire bytes as
    /// `extq::binary_numeric::encode_numeric_binary("42")`. Locks the
    /// shared-codec invariant — the COPY TO path emits the canonical
    /// PG `numeric_send` shape.
    #[test]
    fn t1num_encode_binary_value_numeric_42_byte_equal_to_codec() {
        let via_dispatch = encode_binary_value(b"42", crate::proto::PG_TYPE_NUMERIC)
            .expect("encode_binary_value NUMERIC 42");
        let via_codec =
            crate::extq::binary_numeric::encode_numeric_binary("42").expect("codec 42");
        assert_eq!(
            via_dispatch, via_codec,
            "COPY TO NUMERIC encoder must dispatch into the same codec\
             SP-PG-EXTQ-BIN-NUMERIC ships"
        );
    }

    /// SP-PG-COPY-BIN-NUMERIC T2: decoding NUMERIC binary bytes for
    /// `42` through `decode_binary_param` (the per-column FROM decoder)
    /// returns the canonical decimal string `"42"`. Locks the
    /// shared-codec invariant on the FROM side.
    #[test]
    fn t1num_decode_binary_param_numeric_42_round_trips_to_string() {
        let bytes =
            crate::extq::binary_numeric::encode_numeric_binary("42").expect("codec encode 42");
        let text = decode_binary_param(&bytes, crate::proto::PG_TYPE_NUMERIC)
            .expect("decode_binary_param NUMERIC 42");
        assert_eq!(text, "42");
    }

    /// SP-PG-COPY-BIN-NUMERIC T2: `dispatch_copy_in_start` on a table
    /// whose schema includes an `I128` (PG_TYPE_NUMERIC) column with
    /// `FORMAT binary` returns `Started`. Pre-arc this returned
    /// `Failed { 0A000 SP-PG-COPY-BIN-NUMERIC }`.
    #[test]
    fn t1num_dispatch_copy_in_start_binary_numeric_column_admitted() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "amount".into(), kind: FieldKind::I128, nullable: true },
        ];
        let eng = make_engine(cols);
        let parsed = ParsedCopy::From {
            table: "t".to_string(),
            columns: None,
            format: CopyFormat::Binary,
        };
        match dispatch_copy_in_start(parsed, &eng) {
            CopyInStartOutcome::Started { bytes, .. } => {
                // First byte = CopyInResponse `G`.
                assert_eq!(bytes[0], b'G');
            }
            CopyInStartOutcome::Failed { bytes } => {
                panic!(
                    "expected Started for NUMERIC binary COPY FROM, got Failed: {:?}",
                    String::from_utf8_lossy(&bytes)
                );
            }
        }
    }

    /// SP-PG-COPY-BIN-NUMERIC T2: `dispatch_copy_to` on the same
    /// shape (NUMERIC column with FORMAT binary) emits a CopyOutResponse
    /// (`H` frame) and the canonical 19-byte PG binary header CopyData,
    /// instead of an ErrorResponse 0A000.
    #[test]
    fn t1num_dispatch_copy_to_binary_numeric_column_admitted() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "amount".into(), kind: FieldKind::I128, nullable: true },
        ];
        // No rows — just exercise the admission path + header emit.
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
            format: CopyFormat::Binary,
        };
        let bytes = dispatch_copy_to(parsed, &eng);
        assert_ne!(
            bytes[0], b'E',
            "expected admission (not ErrorResponse) for NUMERIC binary COPY TO; got: {}",
            String::from_utf8_lossy(&bytes)
        );
        assert_eq!(bytes[0], b'H', "expected CopyOutResponse `H` frame");
    }

    /// SP-PG-COPY-BIN-NUMERIC T2: COPY TO binary on a NUMERIC column
    /// emits CopyData carrying canonical PG `numeric_send` bytes for
    /// the row value. Single-row table with `(id BIGINT, amount I128)`
    /// = (7, 42); the second binary-encoded field MUST byte-equal
    /// `encode_numeric_binary("42")`.
    #[test]
    fn t1num_copy_to_binary_numeric_column_emits_canonical_bytes() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "amount".into(), kind: FieldKind::I128, nullable: false },
        ];
        let r1 = build_record(&cols, &[Value::Int(7), Value::Int(42)]);
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
            format: CopyFormat::Binary,
        };
        let bytes = dispatch_copy_to(parsed, &eng);
        // Locate the canonical NUMERIC payload for `42` and assert it
        // appears verbatim inside the response.
        let needle =
            crate::extq::binary_numeric::encode_numeric_binary("42").expect("codec 42");
        assert!(
            bytes.windows(needle.len()).any(|w| w == needle.as_slice()),
            "expected NUMERIC 42 wire bytes inside COPY TO binary output: {:?}",
            bytes
        );
    }

    /// SP-PG-COPY-BIN-NUMERIC T2: COPY FROM binary with a NUMERIC
    /// column accepts a row carrying a canonical NUMERIC binary value
    /// (`42`); the synthesized INSERT carries the bare decimal `42`
    /// for the `amount` column.
    #[test]
    fn t1num_copy_from_binary_numeric_column_ingests_row() {
        let cols = vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "amount".into(), kind: FieldKind::I128, nullable: false },
        ];
        let eng = make_engine(cols);

        // Build a binary COPY data frame: header + 1 row + EOD.
        let id_bytes: [u8; 8] = 7i64.to_be_bytes();
        let amount_bytes =
            crate::extq::binary_numeric::encode_numeric_binary("42").expect("codec 42");
        let row = crate::copy::binary::encode_binary_row(&[
            Some(&id_bytes),
            Some(&amount_bytes),
        ]);
        let mut stream = crate::copy::binary::encode_binary_header();
        stream.extend_from_slice(&row);
        stream.extend_from_slice(&crate::copy::binary::encode_binary_end_of_data());

        let mut state = CopyInState::new_with_format(
            "t".to_string(),
            Some(vec!["id".to_string(), "amount".to_string()]),
            2,
            vec![FieldKind::I64, FieldKind::I128],
            CopyFormat::Binary,
        );
        state.batch_size = 1; // per-row dispatch so the SQL is locked.
        match process_copy_data(&stream, state, &eng) {
            CopyDataOutcome::Continue { state } => {
                assert_eq!(state.rows_ingested, 1, "one row ingested");
                let applied = eng.applied.lock().unwrap();
                assert_eq!(applied.len(), 1);
                // Numeric column kinds render bare (no quotes); the
                // synthesizer emits `42` as a decimal literal for the
                // I128 column.
                assert!(
                    applied[0].contains("INSERT INTO t (id, amount) VALUES (7, 42)")
                        || applied[0].contains("VALUES (7, 42)"),
                    "unexpected SQL: {}",
                    applied[0]
                );
            }
            other => panic!("expected Continue, got {other:?}"),
        }
    }

    /// SP-PG-COPY-BIN-NUMERIC T2: round-trip — encode the NUMERIC
    /// binary bytes for `42`, then decode them. The decoder yields
    /// the canonical decimal `"42"`. Locks the shared-codec
    /// identity at the dispatch-call-site level.
    #[test]
    fn t1num_round_trip_encode_then_decode_through_dispatch_codecs() {
        let cases = ["0", "42", "1.5", "-3.14", "12345.6789", "0.0001"];
        for s in cases {
            let bytes = encode_binary_value(s.as_bytes(), crate::proto::PG_TYPE_NUMERIC)
                .expect("encode");
            let decoded = decode_binary_param(&bytes, crate::proto::PG_TYPE_NUMERIC)
                .expect("decode");
            assert_eq!(decoded, s, "round-trip mismatch for {s:?}");
        }
    }
}
