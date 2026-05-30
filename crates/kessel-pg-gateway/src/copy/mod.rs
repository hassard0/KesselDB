//! SP-PG-COPY — PostgreSQL `COPY FROM STDIN` / `COPY TO STDOUT`
//! bulk-load protocol (PG §55.2.5 / §55.7 / §SQL-COPY).
//!
//! **T1 status (this commit):** module scaffold —
//!
//! - `proto` submodule: backend-message encoders for `CopyInResponse`
//!   (`G`), `CopyOutResponse` (`H`), `CopyData` (`d`), `CopyDone`
//!   (`c`); frontend decoder helper for `CopyFail` (`f`) payload.
//! - `text` submodule: text-format row codec — `encode_text_row` +
//!   `parse_text_row_bytes` with all 7 §4 backslash escapes + `\N`
//!   NULL handling + `\.` end-of-data marker tolerance.
//! - `command` submodule: SQL-text recognizer for `COPY <ident>
//!   [(cols)] FROM STDIN [WITH (FORMAT text)]` and
//!   `COPY <ident> [(cols)] TO STDOUT [WITH (FORMAT text)]`.
//! - `CopyState` enum with `Idle` / `In` variants the
//!   `server::run_session` loop will own per-connection.
//!
//! All seven public surfaces are byte-locked KATs against the PG
//! §55.7 canonical shape. The dispatchers are NOT yet wired into the
//! `server::run_session` loop — T2 (COPY FROM) and T3 (COPY TO)
//! widen.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-30-kesseldb-sppgcopy-design.md`
//!
//! ## Locked invariants (T1)
//!
//! - `CopyState::default() == Idle` — a freshly-constructed
//!   connection starts in Idle and CANNOT receive a CopyData frame
//!   without first dispatching a `COPY ... FROM STDIN` Query.
//! - `encode_copy_in_response(ncols)` byte-locked vs PG §55.7 sample.
//! - `encode_copy_out_response(ncols)` byte-locked vs PG §55.7 sample.
//! - `encode_copy_done()` always 5 bytes `c [length=4]`.
//! - `encode_text_row` + `parse_text_row_bytes` round-trip for the
//!   §4 corpus.
//! - `parse_copy_command` recognizes both COPY FROM and COPY TO
//!   variants tolerantly (leading comments + trailing `;` stripped,
//!   case-insensitive verbs, optional column list, optional WITH
//!   clause).
//! - `MAX_COPY_DATA_BUFFER == PG_MAX_MESSAGE_SIZE` — the framing
//!   layer enforces this BEFORE allocation per spec §5.

#![forbid(unsafe_code)]
#![allow(dead_code)]

pub mod command;
pub mod proto;
pub mod text;

/// Spec §5 — cap on the carry buffer + per-CopyData-frame size.
/// Inherits the PG-level `PG_MAX_MESSAGE_SIZE = 16 MiB` cap; flagged
/// here so the COPY codec doesn't silently drift to a larger cap.
pub const MAX_COPY_DATA_BUFFER: usize = crate::PG_MAX_MESSAGE_SIZE;

/// Per-connection COPY state. The `server::run_session` loop branches
/// on this BEFORE inspecting the frontend tag — when in `CopyState::In`,
/// only `CopyData` / `CopyDone` / `CopyFail` / `Terminate` are valid.
///
/// Spec §3.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum CopyState {
    /// Default — not in a COPY exchange. Normal Q / extq dispatch
    /// applies. A `COPY ... FROM STDIN` Query transitions to `In`;
    /// a `COPY ... TO STDOUT` Query runs synchronously within the
    /// Q dispatch and stays in Idle.
    #[default]
    Idle,
    /// COPY FROM STDIN in flight. The server has emitted
    /// `CopyInResponse` and is awaiting `CopyData` / `CopyDone` /
    /// `CopyFail` from the client. Carries the dispatch state needed
    /// to handle subsequent CopyData frames.
    In(CopyInState),
}

/// Per-connection state for an in-flight `COPY <table> FROM STDIN`
/// exchange. Spec §3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyInState {
    /// Target table name from the `COPY <table> FROM STDIN` Query.
    /// Each parsed row gets dispatched as `INSERT INTO <table>
    /// VALUES (...)`.
    pub table: String,
    /// Optional explicit column list from `COPY <table> (col1, col2)
    /// FROM STDIN`. `None` means "all columns in declared order"
    /// (V1 looks up the schema via `engine.describe_table` to fill
    /// the INSERT column list).
    pub columns: Option<Vec<String>>,
    /// Wire-advertised column count in the `CopyInResponse` frame.
    /// Set when the table's schema is looked up at COPY-start time;
    /// each parsed row MUST have exactly this many tab-separated
    /// fields.
    pub column_count: u16,
    /// Trailing-incomplete-row bytes carried over from the previous
    /// `CopyData` frame. A row can span multiple `CopyData` frames
    /// because PG's CopyData is a binary framing, not a logical row
    /// framing. Bounded at `MAX_COPY_DATA_BUFFER`.
    pub carry: Vec<u8>,
    /// Running count of successfully-ingested rows. Becomes the
    /// `COPY N` tag at CopyDone.
    pub rows_ingested: u64,
}

impl CopyInState {
    /// Build a fresh CopyIn state for a `COPY <table> FROM STDIN`
    /// exchange.
    pub fn new(table: String, columns: Option<Vec<String>>, column_count: u16) -> Self {
        Self {
            table,
            columns,
            column_count,
            carry: Vec::new(),
            rows_ingested: 0,
        }
    }
}

impl CopyState {
    /// True iff the connection is in CopyIn state (`COPY FROM STDIN`
    /// in flight). Used by `server::run_session` to branch on every
    /// inbound message tag — in CopyIn, only CopyData/CopyDone/
    /// CopyFail/Terminate are valid.
    pub fn is_in(&self) -> bool {
        matches!(self, CopyState::In(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SP-PG-COPY T1: a fresh `CopyState` is Idle.
    #[test]
    fn t1_copy_state_default_is_idle() {
        let s: CopyState = Default::default();
        assert!(matches!(s, CopyState::Idle));
        assert!(!s.is_in());
    }

    /// SP-PG-COPY T1: a CopyIn state reports `is_in() == true`.
    #[test]
    fn t1_copy_state_in_reports_is_in_true() {
        let s = CopyState::In(CopyInState::new("t".to_string(), None, 2));
        assert!(s.is_in());
    }

    /// SP-PG-COPY T1 + spec §5: the per-frame cap matches the
    /// connection-level `PG_MAX_MESSAGE_SIZE`. Locked so the COPY
    /// codec cannot silently drift to a larger cap.
    #[test]
    fn t1_max_copy_data_buffer_matches_pg_max_message_size() {
        assert_eq!(MAX_COPY_DATA_BUFFER, crate::PG_MAX_MESSAGE_SIZE);
        // Sanity: 16 MiB.
        assert_eq!(MAX_COPY_DATA_BUFFER, 16 * 1024 * 1024);
    }

    /// SP-PG-COPY T1: `CopyInState::new` builds the state with the
    /// expected initial values (empty carry, zero rows ingested).
    #[test]
    fn t1_copy_in_state_new_initial_values() {
        let s = CopyInState::new(
            "users".to_string(),
            Some(vec!["id".to_string(), "name".to_string()]),
            2,
        );
        assert_eq!(s.table, "users");
        assert_eq!(s.columns, Some(vec!["id".to_string(), "name".to_string()]));
        assert_eq!(s.column_count, 2);
        assert!(s.carry.is_empty());
        assert_eq!(s.rows_ingested, 0);
    }
}
