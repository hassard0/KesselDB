//! ErrorResponse ('E') encoder + `OpResult` → SQLSTATE map.
//!
//! **T7 status (this commit):** locks the wire envelope of the PG
//! `ErrorResponse` message + the full mapping table from spec §7.2
//! that translates a KesselDB `OpResult` into the (severity, SQLSTATE,
//! human-readable message) triple PG clients expect.
//!
//! ## Wire shape (PG §55.7 "ErrorResponse")
//!
//! ```text
//! E [length:4 BE]
//!   [field_type:1] [value:cstring]
//!   ... (repeating)
//!   \0            (zero-byte terminator after the LAST field)
//! ```
//!
//! Field types V1 emits (per PG §55.8):
//! - `S` Severity ("ERROR" / "FATAL" / "PANIC")
//! - `V` Severity (machine-readable; same value as `S` since PG 9.6)
//! - `C` SQLSTATE (5-char alphanumeric)
//! - `M` Message (human-readable; always present)
//!
//! Optional fields V1 does NOT emit yet (deferred to a follow-up
//! polish slice if a real client needs them):
//! - `D` Detail
//! - `H` Hint
//! - `P` Position
//! - `F` / `L` / `R` — Source file / line / routine (V1 NEVER emits
//!   these — they would leak Rust source-file paths).
//!
//! ## SQLSTATE map (spec §7.2)
//!
//! ```text
//! OpResult::Exists                          → 23505 (unique_violation)
//! OpResult::NotFound                        → 02000 (no_data) — see note*
//! OpResult::SchemaError(msg)                → string-match heuristic:
//!     "unknown table"        → 42P01 (undefined_table)
//!     "unknown column"       → 42703 (undefined_column)
//!     "type mismatch"        → 42804 (datatype_mismatch)
//!     "syntax"               → 42601 (syntax_error)
//!     (else)                 → 42000 (syntax_error_or_access_rule_violation)
//! OpResult::Constraint(msg)                 → string-match heuristic:
//!     "NOT NULL" / "not null"  → 23502 (not_null_violation)
//!     "UNIQUE" / "unique"      → 23505 (unique_violation)
//!     "foreign key" / "FK"     → 23503 (foreign_key_violation)
//!     "CHECK" / "check"        → 23514 (check_violation)
//!     (else)                   → 23000 (integrity_constraint_violation)
//! OpResult::Unavailable                     → FATAL 57P03 (cannot_connect_now)
//! OpResult::Unauthorized                    → FATAL 28000 (invalid_authorization_specification)
//! OpResult::TxAborted::WriteWriteConflict   → 40001 (serialization_failure)
//! OpResult::TxAborted::SnapshotOutOfRange   → 25006 (read_only_sql_transaction)
//! OpResult::TxAborted::StorageIo            → 58030 (io_error)
//! OpResult::TxAborted::DangerousStructure   → 40001 (serialization_failure)
//! default (any unknown OpResult variant)    → XX000 (internal_error)
//! ```
//!
//! \* `NotFound` is NOT an error from the SQL perspective — a SELECT
//! that matches no rows still returns a successful `RowDescription
//! + CommandComplete("SELECT 0")` response. T8 routes `NotFound` to
//! the empty-result-set path BEFORE this map is consulted. The
//! mapping table here is the "what if we ARE asked to translate
//! NotFound to an error" fallback for cases like Op::Describe of a
//! missing type, which IS an error (`undefined_table`).
//!
//! ## What this module does NOT do
//!
//! - It does NOT dispatch — T8's query loop calls `op_result_to_sqlstate`
//!   to get the triple, then calls `encode_error_response` to build
//!   the wire frame.
//! - It does NOT emit `ReadyForQuery` — that's the caller's job (after
//!   an error, V1 emits `ReadyForQuery('I')`; V2 with transaction
//!   support would emit `'E'` for failed-transaction state).
//! - It does NOT propagate Detail/Hint/Position fields yet — the
//!   `OpResult` variants don't carry rich-enough metadata for those
//!   to be useful in V1. A future `kessel-sql::SchemaErrorKind` enum
//!   would unlock more precise mapping; that's a V2 follow-up.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::proto::BE_ERROR_RESPONSE;
use kessel_proto::{AbortReason, OpResult};

/// PG severity levels per PG §55.7 "ErrorResponse". V1 emits "ERROR"
/// (recoverable — client may try another query) or "FATAL" (connection
/// is dead — close TCP after sending).
///
/// PG also defines "PANIC" but a panic from KesselDB would be a Rust
/// `panic!()` — the connection thread would have already died before
/// it could emit anything. So "PANIC" is unreachable here.
pub const SEVERITY_ERROR: &str = "ERROR";
pub const SEVERITY_FATAL: &str = "FATAL";

// ─── SQLSTATE codes V1 emits (5-char alphanumeric per PG §59) ─────────

/// Class 02 — `02000` no_data. Spec §7.2 maps `OpResult::NotFound`
/// here, but T8 routes empty-row sets through the SELECT-0-rows path
/// instead; this code is the fallback for `NotFound` on Op::Describe
/// of a missing type.
pub const SQLSTATE_NO_DATA: &str = "02000";

/// Class 23 — integrity_constraint_violation defaults.
pub const SQLSTATE_INTEGRITY_VIOLATION: &str = "23000";

/// Class 23 — `23502` not_null_violation.
pub const SQLSTATE_NOT_NULL_VIOLATION: &str = "23502";

/// Class 23 — `23503` foreign_key_violation.
pub const SQLSTATE_FOREIGN_KEY_VIOLATION: &str = "23503";

/// Class 23 — `23505` unique_violation.
pub const SQLSTATE_UNIQUE_VIOLATION: &str = "23505";

/// Class 23 — `23514` check_violation.
pub const SQLSTATE_CHECK_VIOLATION: &str = "23514";

/// Class 25 — `25006` read_only_sql_transaction.
pub const SQLSTATE_READ_ONLY_TX: &str = "25006";

/// Class 28 — `28000` invalid_authorization_specification.
pub const SQLSTATE_INVALID_AUTHORIZATION: &str = "28000";

/// Class 40 — `40001` serialization_failure (retry-able).
pub const SQLSTATE_SERIALIZATION_FAILURE: &str = "40001";

/// Class 42 — `42000` syntax_error_or_access_rule_violation (default).
pub const SQLSTATE_SYNTAX_OR_ACCESS_DEFAULT: &str = "42000";

/// Class 42 — `42601` syntax_error.
pub const SQLSTATE_SYNTAX_ERROR: &str = "42601";

/// Class 42 — `42703` undefined_column.
pub const SQLSTATE_UNDEFINED_COLUMN: &str = "42703";

/// Class 42 — `42804` datatype_mismatch.
pub const SQLSTATE_DATATYPE_MISMATCH: &str = "42804";

/// Class 42 — `42P01` undefined_table.
pub const SQLSTATE_UNDEFINED_TABLE: &str = "42P01";

/// Class 57 — `57P03` cannot_connect_now.
pub const SQLSTATE_CANNOT_CONNECT_NOW: &str = "57P03";

/// Class 58 — `58030` io_error.
pub const SQLSTATE_IO_ERROR: &str = "58030";

/// Class XX — `XX000` internal_error (the unmapped-variant fallback).
pub const SQLSTATE_INTERNAL_ERROR: &str = "XX000";

/// Class 08 — `08P01` protocol_violation. Emitted from the framing
/// layer (T8 query loop) on malformed messages, not from this map —
/// listed here so the constants live in one place.
pub const SQLSTATE_PROTOCOL_VIOLATION: &str = "08P01";

/// Class 0A — `0A000` feature_not_supported. Emitted when a client
/// requests something V1 doesn't implement (e.g. PG v2 / v4 protocol
/// version in StartupMessage, extended-query messages). Spec §3.2 +
/// §11 weak-spot #5. Listed here so the constant lives in one place.
pub const SQLSTATE_FEATURE_NOT_SUPPORTED: &str = "0A000";

/// Class 53 — `53300` too_many_connections. T13 — emitted by the
/// `kesseldb-server::serve_pg` accept loop when `pg_max_conns` is
/// exceeded; spec §8.2. The canonical PG message text is
/// "sorry, too many clients already" — every libpq-derived client
/// recognizes that phrasing.
pub const SQLSTATE_TOO_MANY_CONNECTIONS: &str = "53300";

/// Class 57 — `57014` query_canceled. T16 — emitted by
/// `run_session` when the per-connection idle-read times out before
/// the next client message arrives. Spec §9.3 — PG uses `57014`
/// for both explicit-cancel and server-initiated session
/// termination on idle (more specific than `08006`
/// connection_failure for this case). The canonical PG message text
/// is "terminating connection due to idle timeout" — matches what
/// `postgres.c::ProcessInterrupts` would emit if PG's own
/// `idle_session_timeout` GUC fires.
pub const SQLSTATE_QUERY_CANCELED: &str = "57014";

/// PG's canonical message text for `57014` query_canceled when fired
/// by the server's idle-session-timeout path. Matches the phrasing
/// PG 14+ emits when `idle_session_timeout` elapses so libpq's
/// `PQerrorMessage()` text is indistinguishable from a real PG
/// origin. Used by `encode_idle_timeout_error`.
pub const IDLE_TIMEOUT_MESSAGE: &str = "terminating connection due to idle timeout";

/// PG's canonical message text for `53300` too_many_connections. Used
/// by `encode_too_many_connections_error` so the listener and any
/// future caller emit the same string libpq's `pgstrerror.c` does.
/// Locked so a future refactor can't drift.
pub const TOO_MANY_CONNECTIONS_MESSAGE: &str = "sorry, too many clients already";

/// T13: build the `ErrorResponse('S=FATAL', 'C=53300', 'M=sorry, too
/// many clients already')` frame the cap-overflow listener writes
/// before closing the connection. Helper wrapper around
/// `encode_error_response` so callers don't have to know the canonical
/// PG message text — the listener just calls this and writes the
/// bytes verbatim.
///
/// Wire bytes (always):
/// ```text
/// E [length:4 BE] SFATAL\0 VFATAL\0 C53300\0 Msorry, too many clients already\0 \0
/// ```
///
/// Per spec §8.2 + PG `postgres.c` BackendStartup: this message MUST
/// be written BEFORE the socket is closed so the client sees a
/// wire-level rejection instead of a bare connection-refused. libpq
/// surfaces the SQLSTATE + message verbatim in `PQerrorMessage()`.
pub fn encode_too_many_connections_error() -> Vec<u8> {
    encode_error_response(
        SEVERITY_FATAL,
        SQLSTATE_TOO_MANY_CONNECTIONS,
        TOO_MANY_CONNECTIONS_MESSAGE,
    )
}

/// T16: build the `ErrorResponse('S=FATAL', 'C=57014', 'M=terminating
/// connection due to idle timeout')` frame that `run_session` writes
/// immediately before closing the connection on idle-read timeout.
/// Helper wrapper around `encode_error_response` so the timeout path
/// doesn't need to know the canonical PG message text — the session
/// loop calls this and writes the bytes verbatim.
///
/// Wire bytes (always):
/// ```text
/// E [length:4 BE] SFATAL\0 VFATAL\0 C57014\0 Mterminating connection due to idle timeout\0 \0
/// ```
///
/// Per spec §9.3 + PG `postgres.c::ProcessInterrupts`: this message
/// MUST be written BEFORE the socket is closed so the client sees a
/// wire-level rejection (with the SQLSTATE libpq's `PQerrorMessage()`
/// surfaces verbatim) instead of a bare read-EOF that some clients
/// misclassify as a transient network failure. Distinguishes a
/// server-initiated termination from a peer-RST.
pub fn encode_idle_timeout_error() -> Vec<u8> {
    encode_error_response(
        SEVERITY_FATAL,
        SQLSTATE_QUERY_CANCELED,
        IDLE_TIMEOUT_MESSAGE,
    )
}

/// Encodes a `ErrorResponse` ('E') message with the four mandatory
/// V1 fields (S, V, C, M). Wire shape per PG §55.7:
///
/// ```text
/// E [length:4 BE]
///   S<severity>\0 V<severity>\0 C<sqlstate>\0 M<message>\0
///   \0   (zero-byte terminator)
/// ```
///
/// Length is `4 (length itself) + sum(per-field-length) + 1
/// (trailing terminator)`. Each field is `[1-byte-tag] [value]
/// [\0]` — 1 + value.len() + 1 = value.len() + 2 bytes.
///
/// `severity` is "ERROR" or "FATAL" (per spec §7.2).
/// `sqlstate` is a 5-char alphanumeric code (PG §59).
/// `message` is human-readable; clients log + display it verbatim.
pub fn encode_error_response(
    severity: &str,
    sqlstate: &str,
    message: &str,
) -> Vec<u8> {
    // S field: 1 (tag) + severity.len() + 1 (NUL)
    // V field: same shape (machine-readable severity)
    // C field: 1 + sqlstate.len() + 1
    // M field: 1 + message.len() + 1
    // Then the trailing \0 terminator.
    let s_len = severity.len() + 2;
    let v_len = severity.len() + 2;
    let c_len = sqlstate.len() + 2;
    let m_len = message.len() + 2;
    let payload_len = s_len + v_len + c_len + m_len + 1; // +1 for trailing NUL
    let total_length = (4 + payload_len) as u32;
    let mut frame = Vec::with_capacity(1 + total_length as usize);
    frame.push(BE_ERROR_RESPONSE);
    frame.extend_from_slice(&total_length.to_be_bytes());
    frame.push(b'S');
    frame.extend_from_slice(severity.as_bytes());
    frame.push(0);
    frame.push(b'V');
    frame.extend_from_slice(severity.as_bytes());
    frame.push(0);
    frame.push(b'C');
    frame.extend_from_slice(sqlstate.as_bytes());
    frame.push(0);
    frame.push(b'M');
    frame.extend_from_slice(message.as_bytes());
    frame.push(0);
    // Trailing terminator (no more fields).
    frame.push(0);
    frame
}

/// Returns the per-class SQLSTATE for an `OpResult::SchemaError(msg)`
/// using the string-match heuristic from spec §7.2. The heuristic is
/// an honest compromise — `kessel-sql` doesn't yet tag its errors
/// with structured kinds. A V2 follow-up (a `kessel-sql::
/// SchemaErrorKind` enum) would let us drop the heuristic.
///
/// Ordering matters — "unknown column" must be checked before
/// "unknown table" can't false-positive on it (they don't overlap,
/// but the principle holds for future additions).
pub fn schema_error_to_sqlstate(msg: &str) -> &'static str {
    // Case-insensitive substring match.
    let lower = msg.to_ascii_lowercase();
    if lower.contains("unknown column") || lower.contains("no field") {
        SQLSTATE_UNDEFINED_COLUMN
    } else if lower.contains("unknown table")
        || lower.contains("no type")
        || lower.contains("undefined table")
    {
        SQLSTATE_UNDEFINED_TABLE
    } else if lower.contains("type mismatch") || lower.contains("datatype") {
        SQLSTATE_DATATYPE_MISMATCH
    } else if lower.contains("syntax") {
        SQLSTATE_SYNTAX_ERROR
    } else {
        SQLSTATE_SYNTAX_OR_ACCESS_DEFAULT
    }
}

/// Returns the per-class SQLSTATE for an `OpResult::Constraint(msg)`
/// using a string-match heuristic. KesselDB's constraint messages are
/// constructed in `kessel-sm` (and partly `kesseldb-server::router`)
/// without structured tagging; the substrings below cover the cases
/// the SM emits today:
///
/// - "NOT NULL" → 23502
/// - "UNIQUE" / "duplicate value" / "AddUnique: existing duplicate" → 23505
/// - "foreign key" / "FK" / "referenced by" → 23503
/// - "CHECK" → 23514
/// - default → 23000
pub fn constraint_to_sqlstate(msg: &str) -> &'static str {
    let lower = msg.to_ascii_lowercase();
    if lower.contains("not null") {
        SQLSTATE_NOT_NULL_VIOLATION
    } else if lower.contains("unique") || lower.contains("duplicate") {
        SQLSTATE_UNIQUE_VIOLATION
    } else if lower.contains("foreign key")
        || lower.contains("referenced")
        // SP-PG-DDL-FK-ENFORCE: the SM's INSERT/UPDATE enforcement message
        // is "FOREIGN KEY violated …" (caught above); the ON DELETE RESTRICT
        // block emits "ON DELETE RESTRICT: … still references type …", and
        // AddForeignKey's existing-data check emits "… dangling reference".
        // All of these are foreign_key_violation (23503).
        || lower.contains("on delete")
        || lower.contains("references type")
        || lower.contains("dangling reference")
        || lower.contains(" fk ")
    {
        SQLSTATE_FOREIGN_KEY_VIOLATION
    } else if lower.contains("check") {
        SQLSTATE_CHECK_VIOLATION
    } else {
        SQLSTATE_INTEGRITY_VIOLATION
    }
}

/// Maps a `OpResult` to its `(severity, sqlstate, message)` triple
/// per spec §7.2. The caller (T8 query loop) then passes the triple
/// to `encode_error_response`.
///
/// Returns `None` for the success variants (`Ok`, `Got`, `NotFound`,
/// `TypeCreated`, `TxCommitted`, `WatermarkAdvanced`,
/// `ActiveSnapshotReported`) — the caller's job is to know success
/// from failure and not route success here. Returning `None` for
/// success is a defensive guard: a future caller that loops through
/// EVERY `OpResult` variant and calls this function will get a
/// no-op for success cases instead of a bogus "internal_error" wire
/// frame.
pub fn op_result_to_sqlstate(
    r: &OpResult,
) -> Option<(&'static str, &'static str, String)> {
    match r {
        // Success variants — caller should not route here.
        // SP-PG-SERIAL-RETURNING: `Created` (autoincrement assign) is a
        // success result, handled by the dispatch RETURNING path.
        OpResult::Ok
        | OpResult::Got(_)
        | OpResult::NotFound
        | OpResult::TypeCreated(_)
        | OpResult::TxCommitted { .. }
        | OpResult::WatermarkAdvanced { .. }
        | OpResult::Created { .. }
        // SP-PG-RETURNING-MULTIROW-STAR: `CreatedMany` (multi-row
        // autoincrement INSERT) is a success result, handled by the
        // dispatch RETURNING path.
        | OpResult::CreatedMany { .. }
        | OpResult::ActiveSnapshotReported { .. } => None,

        OpResult::Exists => Some((
            SEVERITY_ERROR,
            SQLSTATE_UNIQUE_VIOLATION,
            "row already present".to_string(),
        )),
        OpResult::SchemaError(msg) => Some((
            SEVERITY_ERROR,
            schema_error_to_sqlstate(msg),
            msg.clone(),
        )),
        OpResult::Constraint(msg) => Some((
            SEVERITY_ERROR,
            constraint_to_sqlstate(msg),
            msg.clone(),
        )),
        OpResult::Unavailable => Some((
            SEVERITY_FATAL,
            SQLSTATE_CANNOT_CONNECT_NOW,
            "not the active primary; rotate to primary".to_string(),
        )),
        OpResult::Unauthorized => Some((
            SEVERITY_FATAL,
            SQLSTATE_INVALID_AUTHORIZATION,
            "missing or invalid token".to_string(),
        )),
        OpResult::TxAborted { reason } => {
            let (state, msg) = abort_reason_to_sqlstate(reason);
            Some((SEVERITY_ERROR, state, msg))
        }
        OpResult::WatermarkRejected { .. } => Some((
            SEVERITY_ERROR,
            SQLSTATE_INTERNAL_ERROR,
            "watermark advance rejected".to_string(),
        )),
        OpResult::ActiveSnapshotRejected { .. } => Some((
            SEVERITY_ERROR,
            SQLSTATE_INTERNAL_ERROR,
            "active-snapshot report rejected".to_string(),
        )),
    }
}

/// Maps an `AbortReason` (inside `OpResult::TxAborted`) to its
/// SQLSTATE + message per spec §7.2.
fn abort_reason_to_sqlstate(r: &AbortReason) -> (&'static str, String) {
    match r {
        AbortReason::WriteWriteConflict { .. } => (
            SQLSTATE_SERIALIZATION_FAILURE,
            "write-write conflict; retry with a fresher snapshot".to_string(),
        ),
        AbortReason::SnapshotOutOfRange => (
            SQLSTATE_READ_ONLY_TX,
            "snapshot out of range".to_string(),
        ),
        AbortReason::StorageIo { kind } => (
            SQLSTATE_IO_ERROR,
            format!("storage I/O error (kind={kind})"),
        ),
        AbortReason::DangerousStructure { .. } => (
            SQLSTATE_SERIALIZATION_FAILURE,
            "dangerous read-write dependency; retry with a fresher snapshot"
                .to_string(),
        ),
        // `AbortReason` is `#[non_exhaustive]`; future variants land
        // here until SP-PG explicitly maps them. Default `XX000`
        // (internal_error) so a real client gets a real error instead
        // of an opaque silent drop.
        _ => (
            SQLSTATE_INTERNAL_ERROR,
            "kessel-pg-gateway: unmapped AbortReason variant".to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_proto::AbortReason;

    // ───────────────────────────────────────────────────────────────────
    // T7 KATs — lock the ErrorResponse wire envelope + the OpResult →
    // SQLSTATE mapping table per spec §7.2 against PG §55.7 and §59.
    // ───────────────────────────────────────────────────────────────────

    /// Byte-locked: canonical "ERROR / 42P01 / unknown table 't'"
    /// ErrorResponse. Locks the field-order S/V/C/M, the per-field
    /// NUL terminators, the trailing zero-byte terminator, and the
    /// length-prefix arithmetic.
    #[test]
    fn t7_error_response_byte_locked_for_undefined_table() {
        let frame = encode_error_response("ERROR", "42P01", "unknown table 't'");
        // payload:
        //   S ERROR \0       1 + 5 + 1 = 7
        //   V ERROR \0       7
        //   C 42P01 \0       1 + 5 + 1 = 7
        //   M unknown table 't' \0   1 + 17 + 1 = 19
        //   \0               1
        // total payload = 7 + 7 + 7 + 19 + 1 = 41
        // length = 4 + 41 = 45
        let mut expected = Vec::new();
        expected.push(b'E');
        expected.extend_from_slice(&45u32.to_be_bytes());
        expected.extend_from_slice(b"SERROR\0");
        expected.extend_from_slice(b"VERROR\0");
        expected.extend_from_slice(b"C42P01\0");
        expected.extend_from_slice(b"Munknown table 't'\0");
        expected.push(0);
        assert_eq!(frame, expected);
    }

    /// Empty message (corner case — encoder must not crash on a 0-byte
    /// message). Locks the framing arithmetic against off-by-one bugs.
    #[test]
    fn t7_error_response_with_empty_message() {
        let frame = encode_error_response("ERROR", "XX000", "");
        // S ERROR \0     7
        // V ERROR \0     7
        // C XX000 \0     7
        // M \0           2
        // \0             1
        // payload = 24
        // length = 4 + 24 = 28
        let mut expected = Vec::new();
        expected.push(b'E');
        expected.extend_from_slice(&28u32.to_be_bytes());
        expected.extend_from_slice(b"SERROR\0");
        expected.extend_from_slice(b"VERROR\0");
        expected.extend_from_slice(b"CXX000\0");
        expected.extend_from_slice(b"M\0");
        expected.push(0);
        assert_eq!(frame, expected);
    }

    /// FATAL severity round-trip. Locks the encoder doesn't hardcode
    /// "ERROR".
    #[test]
    fn t7_error_response_fatal_severity_round_trip() {
        let frame = encode_error_response("FATAL", "57P03", "not primary");
        // First byte is 'E', then length, then 'S' tag, then "FATAL\0".
        assert_eq!(frame[0], b'E');
        // S FATAL \0   8
        // V FATAL \0   8
        // C 57P03 \0   7
        // M not primary \0    1 + 11 + 1 = 13
        // \0           1
        // payload = 8 + 8 + 7 + 13 + 1 = 37
        // wait — "FATAL" is 5 chars so S/V field = 1+5+1 = 7, NOT 8.
        // Correction: 7 + 7 + 7 + 13 + 1 = 35; length = 4 + 35 = 39.
        let length = u32::from_be_bytes(frame[1..5].try_into().unwrap());
        assert_eq!(length, 39);
        // S field value present.
        assert!(frame.windows(7).any(|w| w == b"SFATAL\0"));
        // V field value present.
        assert!(frame.windows(7).any(|w| w == b"VFATAL\0"));
    }

    /// Field-order invariant — S MUST precede V MUST precede C MUST
    /// precede M in the wire frame. Some old PG clients parsed
    /// position-dependently before PG 9.6 standardized the V field.
    #[test]
    fn t7_error_response_field_order_is_s_v_c_m() {
        let frame = encode_error_response("ERROR", "42P01", "table missing");
        // Skip the type byte + length prefix; field-tag positions live
        // in the payload.
        let payload = &frame[5..];
        let s_pos = payload.iter().position(|&b| b == b'S').unwrap();
        let v_pos = payload.iter().position(|&b| b == b'V').unwrap();
        let c_pos = payload.iter().position(|&b| b == b'C').unwrap();
        let m_pos = payload.iter().position(|&b| b == b'M').unwrap();
        assert!(s_pos < v_pos);
        assert!(v_pos < c_pos);
        assert!(c_pos < m_pos);
    }

    /// Trailing zero-byte terminator is present at the END of the
    /// frame (after the last field's NUL). PG clients use it to know
    /// "no more fields"; missing it makes libpq spin.
    #[test]
    fn t7_error_response_trailing_zero_byte_terminator() {
        let frame = encode_error_response("ERROR", "XX000", "boom");
        assert_eq!(*frame.last().unwrap(), 0);
        // Second-to-last byte is the M field's NUL terminator, so
        // the frame ends in two consecutive zeros.
        let n = frame.len();
        assert_eq!(frame[n - 2], 0);
        assert_eq!(frame[n - 1], 0);
    }

    // ─── OpResult → (severity, sqlstate, message) mapping ─────────────

    /// Exists → 23505 unique_violation ERROR.
    #[test]
    fn t7_op_result_exists_maps_to_unique_violation() {
        let r = OpResult::Exists;
        let (sev, state, _msg) = op_result_to_sqlstate(&r).expect("Exists IS an error");
        assert_eq!(sev, SEVERITY_ERROR);
        assert_eq!(state, "23505");
    }

    /// SchemaError("no type 7") → 42P01 undefined_table (via the
    /// "no type" substring in the heuristic).
    #[test]
    fn t7_schema_error_no_type_maps_to_undefined_table() {
        let r = OpResult::SchemaError("no type 7".to_string());
        let (sev, state, _) = op_result_to_sqlstate(&r).expect("SchemaError is err");
        assert_eq!(sev, SEVERITY_ERROR);
        assert_eq!(state, "42P01");
    }

    /// SchemaError("unknown column foo") → 42703 undefined_column.
    #[test]
    fn t7_schema_error_unknown_column_maps_to_undefined_column() {
        let r = OpResult::SchemaError("unknown column foo".to_string());
        let (_, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(state, "42703");
    }

    /// SchemaError("type mismatch in column id") → 42804.
    #[test]
    fn t7_schema_error_type_mismatch_maps_to_datatype_mismatch() {
        let r = OpResult::SchemaError("type mismatch in column id".to_string());
        let (_, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(state, "42804");
    }

    /// SchemaError("expected `=` (syntax)") → 42601 syntax_error.
    #[test]
    fn t7_schema_error_with_syntax_substring_maps_to_syntax_error() {
        let r = OpResult::SchemaError("expected `=` (syntax)".to_string());
        let (_, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(state, "42601");
    }

    /// SchemaError("something obscure") → 42000 default (no substring
    /// match — fallback per spec §7.2).
    #[test]
    fn t7_schema_error_unmatched_falls_back_to_42000() {
        let r = OpResult::SchemaError("something obscure".to_string());
        let (_, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(state, "42000");
    }

    /// Constraint NOT NULL → 23502.
    #[test]
    fn t7_constraint_not_null_maps_to_23502() {
        let r = OpResult::Constraint("column 'x' violates NOT NULL".to_string());
        let (_, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(state, "23502");
    }

    /// Constraint UNIQUE → 23505 (matches the same code as Exists
    /// because PG only has one unique_violation code).
    #[test]
    fn t7_constraint_unique_maps_to_23505() {
        let r = OpResult::Constraint("UNIQUE constraint on (x)".to_string());
        let (_, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(state, "23505");
    }

    /// Constraint with "duplicate value" → 23505 (SM emits this for
    /// AddUnique violations — see crates/kessel-sm/src/lib.rs:2749).
    #[test]
    fn t7_constraint_duplicate_maps_to_23505() {
        let r = OpResult::Constraint(
            "AddUnique: existing duplicate values".to_string(),
        );
        let (_, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(state, "23505");
    }

    /// Constraint with "foreign key" → 23503.
    #[test]
    fn t7_constraint_foreign_key_maps_to_23503() {
        let r = OpResult::Constraint(
            "DROP TABLE: type 7 is referenced by a foreign key".to_string(),
        );
        let (_, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(state, "23503");
    }

    /// SP-PG-DDL-FK-ENFORCE: the SM's INSERT/UPDATE FK-enforcement message
    /// "FOREIGN KEY violated on field 'x' -> type 2" → 23503 (the headline
    /// path a bad child INSERT takes through the PG wire).
    #[test]
    fn sppgddlfkenforce_insert_violation_maps_to_23503() {
        let r = OpResult::Constraint(
            "FOREIGN KEY violated on field 'author_id' -> type 2".to_string(),
        );
        let (_, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(state, "23503");
    }

    /// SP-PG-DDL-FK-ENFORCE: ON DELETE RESTRICT block on a referenced
    /// parent → 23503 (the "still references type" message must map to FK
    /// violation, not the 23000 default).
    #[test]
    fn sppgddlfkenforce_restrict_block_maps_to_23503() {
        let r = OpResult::Constraint(
            "ON DELETE RESTRICT: type 2 field 1 still references type 1"
                .to_string(),
        );
        let (_, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(state, "23503");
    }

    /// Constraint with "CHECK" → 23514.
    #[test]
    fn t7_constraint_check_maps_to_23514() {
        let r = OpResult::Constraint("CHECK failed: x >= 0".to_string());
        let (_, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(state, "23514");
    }

    /// Constraint with no recognized substring → 23000 default.
    #[test]
    fn t7_constraint_unmatched_falls_back_to_23000() {
        let r = OpResult::Constraint("opaque constraint failure".to_string());
        let (_, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(state, "23000");
    }

    /// Unavailable → 57P03 FATAL — connection-level escalation per
    /// spec §7.2; client should rotate to the primary.
    #[test]
    fn t7_unavailable_maps_to_57p03_fatal() {
        let r = OpResult::Unavailable;
        let (sev, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(sev, SEVERITY_FATAL);
        assert_eq!(state, "57P03");
    }

    /// Unauthorized → 28000 FATAL (no retry — credentials are wrong).
    #[test]
    fn t7_unauthorized_maps_to_28000_fatal() {
        let r = OpResult::Unauthorized;
        let (sev, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(sev, SEVERITY_FATAL);
        assert_eq!(state, "28000");
    }

    /// TxAborted::WriteWriteConflict → 40001 serialization_failure
    /// ERROR — client retries with a fresher snapshot.
    #[test]
    fn t7_tx_aborted_write_write_conflict_maps_to_40001() {
        let r = OpResult::TxAborted {
            reason: AbortReason::WriteWriteConflict {
                type_id: 1,
                object_id: [0u8; 16],
            },
        };
        let (sev, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(sev, SEVERITY_ERROR);
        assert_eq!(state, "40001");
    }

    /// TxAborted::SnapshotOutOfRange → 25006 read_only_sql_transaction.
    #[test]
    fn t7_tx_aborted_snapshot_out_of_range_maps_to_25006() {
        let r = OpResult::TxAborted {
            reason: AbortReason::SnapshotOutOfRange,
        };
        let (_, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(state, "25006");
    }

    /// TxAborted::StorageIo → 58030 io_error.
    #[test]
    fn t7_tx_aborted_storage_io_maps_to_58030() {
        let r = OpResult::TxAborted {
            reason: AbortReason::StorageIo { kind: 0 },
        };
        let (_, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(state, "58030");
    }

    /// TxAborted::DangerousStructure → 40001 (SSI dangerous structure;
    /// retry shape identical to write-write conflict).
    #[test]
    fn t7_tx_aborted_dangerous_structure_maps_to_40001() {
        let r = OpResult::TxAborted {
            reason: AbortReason::DangerousStructure {
                other_commit_opnum: 42,
            },
        };
        let (_, state, _) = op_result_to_sqlstate(&r).unwrap();
        assert_eq!(state, "40001");
    }

    /// Success variants return `None` (caller should not route success
    /// through the error path).
    #[test]
    fn t7_success_variants_return_none() {
        assert!(op_result_to_sqlstate(&OpResult::Ok).is_none());
        assert!(op_result_to_sqlstate(&OpResult::Got(vec![1, 2, 3].into())).is_none());
        assert!(op_result_to_sqlstate(&OpResult::NotFound).is_none());
        assert!(op_result_to_sqlstate(&OpResult::TypeCreated(7)).is_none());
        assert!(op_result_to_sqlstate(&OpResult::TxCommitted {
            commit_opnum: 100,
        })
        .is_none());
    }

    /// Full pipeline integration check — pick a SchemaError, get the
    /// triple, encode it, and assert the wire frame contains the
    /// expected SQLSTATE bytes. Smokes T7 end-to-end.
    #[test]
    fn t7_pipeline_schema_error_round_trip() {
        let r = OpResult::SchemaError("no type 99".to_string());
        let (sev, state, msg) = op_result_to_sqlstate(&r).unwrap();
        let frame = encode_error_response(sev, state, &msg);
        // The wire frame contains the SQLSTATE bytes "42P01" verbatim.
        assert!(frame.windows(5).any(|w| w == b"42P01"));
        // ... and the message verbatim.
        assert!(frame.windows(10).any(|w| w == b"no type 99"));
        // ... and the severity "ERROR".
        assert!(frame.windows(5).any(|w| w == b"ERROR"));
    }

    /// FATAL severity propagates correctly to the encoded frame.
    #[test]
    fn t7_pipeline_unavailable_emits_fatal_severity_frame() {
        let r = OpResult::Unavailable;
        let (sev, state, msg) = op_result_to_sqlstate(&r).unwrap();
        let frame = encode_error_response(sev, state, &msg);
        // S field carries "FATAL".
        assert!(frame.windows(7).any(|w| w == b"SFATAL\0"));
        // V field carries "FATAL" too (since PG 9.6 they match).
        assert!(frame.windows(7).any(|w| w == b"VFATAL\0"));
        // SQLSTATE 57P03.
        assert!(frame.windows(5).any(|w| w == b"57P03"));
    }

    /// Sanity check: the SQLSTATE constants are all exactly 5 ASCII
    /// alphanumeric characters per PG §59 SQLSTATE grammar. A 4-char
    /// or 6-char code would crash some libpq versions on string-eq
    /// dispatch.
    #[test]
    fn t7_all_sqlstate_constants_are_5_alphanumeric_chars() {
        let codes = [
            SQLSTATE_NO_DATA,
            SQLSTATE_INTEGRITY_VIOLATION,
            SQLSTATE_NOT_NULL_VIOLATION,
            SQLSTATE_FOREIGN_KEY_VIOLATION,
            SQLSTATE_UNIQUE_VIOLATION,
            SQLSTATE_CHECK_VIOLATION,
            SQLSTATE_READ_ONLY_TX,
            SQLSTATE_INVALID_AUTHORIZATION,
            SQLSTATE_SERIALIZATION_FAILURE,
            SQLSTATE_SYNTAX_OR_ACCESS_DEFAULT,
            SQLSTATE_SYNTAX_ERROR,
            SQLSTATE_UNDEFINED_COLUMN,
            SQLSTATE_DATATYPE_MISMATCH,
            SQLSTATE_UNDEFINED_TABLE,
            SQLSTATE_CANNOT_CONNECT_NOW,
            SQLSTATE_IO_ERROR,
            SQLSTATE_INTERNAL_ERROR,
            SQLSTATE_PROTOCOL_VIOLATION,
            SQLSTATE_FEATURE_NOT_SUPPORTED,
            SQLSTATE_TOO_MANY_CONNECTIONS,
        ];
        for c in codes {
            assert_eq!(c.len(), 5, "SQLSTATE {c} not 5 chars");
            assert!(
                c.chars().all(|ch| ch.is_ascii_alphanumeric()),
                "SQLSTATE {c} has non-alphanumeric"
            );
        }
    }

    // ───────────────────────────────────────────────────────────────────
    // T13 KATs — cap-overflow `53300` ErrorResponse helper. The
    // headline invariant is that the canonical PG text "sorry, too
    // many clients already" appears verbatim on the wire, along with
    // FATAL severity (so libpq closes the connection rather than
    // re-trying the query) and SQLSTATE 53300 (so structured logging
    // can switch on it).
    // ───────────────────────────────────────────────────────────────────

    /// `encode_too_many_connections_error` emits the canonical PG
    /// "sorry, too many clients already" message. Locks the spec §8.2
    /// requirement that the cap-overflow listener emits a wire-level
    /// rejection clients can recognize, not a bare TCP close.
    #[test]
    fn t13_too_many_connections_error_carries_canonical_message() {
        let frame = encode_too_many_connections_error();
        // First byte is the ErrorResponse type tag.
        assert_eq!(frame[0], b'E');
        // SQLSTATE 53300 visible on the wire (in the C field).
        assert!(
            frame.windows(b"C53300\0".len()).any(|w| w == b"C53300\0"),
            "SQLSTATE 53300 field MUST be present on the wire"
        );
        // FATAL severity in both S and V fields (PG 9.6+ duplicates).
        assert!(
            frame.windows(b"SFATAL\0".len()).any(|w| w == b"SFATAL\0"),
            "S field MUST carry FATAL severity"
        );
        assert!(
            frame.windows(b"VFATAL\0".len()).any(|w| w == b"VFATAL\0"),
            "V field MUST carry FATAL severity"
        );
        // Canonical PG message text — every libpq-derived client
        // recognizes the EXACT phrasing.
        assert!(
            frame.windows(TOO_MANY_CONNECTIONS_MESSAGE.len())
                .any(|w| w == TOO_MANY_CONNECTIONS_MESSAGE.as_bytes()),
            "canonical PG message 'sorry, too many clients already' MUST be present"
        );
    }

    /// `encode_too_many_connections_error` byte-locks the FATAL +
    /// 53300 + canonical-message frame end-to-end. Prevents a refactor
    /// from drifting away from the libpq-recognized wire text.
    #[test]
    fn t13_too_many_connections_error_byte_locked() {
        let frame = encode_too_many_connections_error();
        let expected = encode_error_response(
            SEVERITY_FATAL,
            "53300",
            TOO_MANY_CONNECTIONS_MESSAGE,
        );
        assert_eq!(frame, expected);
    }

    /// `TOO_MANY_CONNECTIONS_MESSAGE` matches PG's hard-coded
    /// `postmaster.c` text exactly. A future refactor that drifts
    /// the message (e.g. "sorry, too many connections") would break
    /// every libpq client that string-matches on the canonical text.
    #[test]
    fn t13_too_many_connections_message_matches_pg_canonical() {
        assert_eq!(
            TOO_MANY_CONNECTIONS_MESSAGE,
            "sorry, too many clients already"
        );
    }

    /// `SQLSTATE_TOO_MANY_CONNECTIONS` is the exact PG §59 class-53
    /// code: 53300. Locked so a typo (53301 / 53003 etc.) is caught
    /// at compile time of the constant + here at run time.
    #[test]
    fn t13_sqlstate_too_many_connections_is_53300() {
        assert_eq!(SQLSTATE_TOO_MANY_CONNECTIONS, "53300");
    }
}
