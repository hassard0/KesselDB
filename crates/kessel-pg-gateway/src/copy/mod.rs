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
pub mod csv;
pub mod dispatch;
pub mod proto;
pub mod text;

/// Spec §5 — cap on the carry buffer + per-CopyData-frame size.
/// Inherits the PG-level `PG_MAX_MESSAGE_SIZE = 16 MiB` cap; flagged
/// here so the COPY codec doesn't silently drift to a larger cap.
pub const MAX_COPY_DATA_BUFFER: usize = crate::PG_MAX_MESSAGE_SIZE;

/// SP-PG-COPY-BULKAPPLY V1 — default per-COPY-session batch size for
/// the multi-row INSERT (`Op::Txn`) fold. Each batch produces a
/// single multi-row `INSERT INTO t (cols) VALUES (...), (...), ...`,
/// which kessel-sql compiles to `Op::Txn { ops: Vec<Op::Create> }`
/// (one apply round-trip + one WAL fsync per batch instead of per row).
///
/// 1024 picked per design spec §4 sizing table — the knee where the
/// per-batch fsync win saturates against the per-batch SQL-synthesis
/// cost + per-connection RSS.
///
/// Configurable per-server via the `KESSELDB_COPY_BATCH_SIZE` env
/// var at COPY-start time; clamped to `[1, 65536]`.
pub const COPY_BATCH_SIZE: usize = 1024;

/// SP-PG-COPY-BULKAPPLY V1 — hard cap on the per-session batch size.
/// 65536 rows × per-row size cap = pending-buffer RSS upper bound.
/// Any env override above this is silently clamped.
pub const COPY_BATCH_SIZE_MAX: usize = 65536;

/// SP-PG-COPY-BULKAPPLY V1 — resolve the per-session batch size from
/// the `KESSELDB_COPY_BATCH_SIZE` env var, falling back to
/// `COPY_BATCH_SIZE`. Clamped to `[1, COPY_BATCH_SIZE_MAX]`.
///
/// Pure read-only — safe to call from any thread. Invoked once per
/// `dispatch_copy_in_start` (so a long-running server picks up the
/// override on a restart but not mid-run).
pub fn resolve_copy_batch_size() -> usize {
    match std::env::var("KESSELDB_COPY_BATCH_SIZE") {
        Ok(s) => match s.trim().parse::<usize>() {
            Ok(n) if n >= 1 => n.min(COPY_BATCH_SIZE_MAX),
            _ => COPY_BATCH_SIZE,
        },
        Err(_) => COPY_BATCH_SIZE,
    }
}

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

/// SP-PG-COPY-CSV V1 — wire-format selector for a `COPY ... FROM/TO
/// STDIN/STDOUT` exchange. The dispatcher branches on this to pick the
/// row codec (`text` vs `csv`).
///
/// Spec §2.3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyFormat {
    /// PG text format — tab-separated, `\N` NULL, backslash escapes.
    /// SP-PG-COPY V1 default.
    Text,
    /// PG CSV format — RFC 4180 + PG superset (HEADER + custom
    /// DELIMITER / QUOTE / ESCAPE / NULL options). SP-PG-COPY-CSV V1.
    Csv(csv::CsvOptions),
}

impl Default for CopyFormat {
    fn default() -> Self {
        CopyFormat::Text
    }
}

impl CopyFormat {
    /// True iff this is a CSV-format COPY (any options).
    pub fn is_csv(&self) -> bool {
        matches!(self, CopyFormat::Csv(_))
    }
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
    /// Per-column KesselDB `FieldKind` for the chosen-column list,
    /// in the same order as `columns`. Used by the COPY-FROM
    /// row-to-INSERT synthesizer to pick the right SQL literal
    /// rendering — numeric kinds render as bare decimal (no quotes),
    /// string / bytes kinds render as `'...'`-quoted (with `'`
    /// doubled). Pairs 1:1 with `columns` (and has length
    /// `column_count`). Carried so the per-row INSERT dispatch
    /// doesn't have to re-query `engine.describe_table()` for every
    /// row.
    pub column_kinds: Vec<kessel_catalog::FieldKind>,
    /// Trailing-incomplete-row bytes carried over from the previous
    /// `CopyData` frame. A row can span multiple `CopyData` frames
    /// because PG's CopyData is a binary framing, not a logical row
    /// framing. Bounded at `MAX_COPY_DATA_BUFFER`.
    pub carry: Vec<u8>,
    /// Running count of successfully-ingested rows. Becomes the
    /// `COPY N` tag at CopyDone.
    pub rows_ingested: u64,
    /// **SP-PG-COPY-BULKAPPLY V1** — parsed-but-not-yet-flushed rows.
    /// Each entry is one row's fields (None = NULL). Drained when the
    /// buffer reaches `batch_size` OR at CopyDone.
    pub pending_rows: Vec<Vec<Option<Vec<u8>>>>,
    /// **SP-PG-COPY-BULKAPPLY V1** — per-session batch size, resolved
    /// once at COPY-start time from `KESSELDB_COPY_BATCH_SIZE` env
    /// (falling back to `COPY_BATCH_SIZE`).
    pub batch_size: usize,
    /// **SP-PG-COPY-BULKAPPLY V1** — row number (1-based) of the first
    /// row in the currently-pending batch. Used to build the
    /// "in batch starting at row N" tag on engine-error responses.
    pub batch_start_row: u64,
    /// **SP-PG-COPY-CSV V1** — wire format for this COPY exchange.
    /// `Text` is the SP-PG-COPY V1 default; `Csv(options)` engages the
    /// CSV codec with the resolved options.
    pub format: CopyFormat,
    /// **SP-PG-COPY-CSV V1** — set to true when this is a CSV-format
    /// COPY with `HEADER` and the first incoming record hasn't been
    /// consumed-and-discarded yet. The dispatcher flips this to false
    /// after dropping the header row.
    pub pending_header: bool,
}

impl CopyInState {
    /// Build a fresh CopyIn state for a `COPY <table> FROM STDIN`
    /// exchange.
    pub fn new(table: String, columns: Option<Vec<String>>, column_count: u16) -> Self {
        Self::new_with_kinds(table, columns, column_count, Vec::new())
    }

    /// Build a fresh CopyIn state with per-column `FieldKind`s for
    /// schema-aware SQL-literal rendering. Preferred constructor —
    /// `new()` is the back-compat shim that leaves `column_kinds`
    /// empty (the synthesizer falls back to always-quoted rendering,
    /// which works for CHAR columns but trips kessel-sql's
    /// integer-column type check).
    ///
    /// **SP-PG-COPY-BULKAPPLY V1** — also initialises the per-batch
    /// fold fields: `pending_rows` empty, `batch_size` resolved from
    /// the env (or `COPY_BATCH_SIZE` default), `batch_start_row = 1`.
    ///
    /// **SP-PG-COPY-CSV V1** — defaults `format` to Text + `pending_header`
    /// to false. Use `new_with_format` for CSV variants.
    pub fn new_with_kinds(
        table: String,
        columns: Option<Vec<String>>,
        column_count: u16,
        column_kinds: Vec<kessel_catalog::FieldKind>,
    ) -> Self {
        Self::new_with_format(
            table,
            columns,
            column_count,
            column_kinds,
            CopyFormat::Text,
        )
    }

    /// **SP-PG-COPY-CSV V1** — fully-explicit constructor including
    /// the wire format. The dispatcher uses this so a CSV-format
    /// `COPY FROM STDIN` enters CopyIn with the right codec configured.
    pub fn new_with_format(
        table: String,
        columns: Option<Vec<String>>,
        column_count: u16,
        column_kinds: Vec<kessel_catalog::FieldKind>,
        format: CopyFormat,
    ) -> Self {
        let pending_header = matches!(&format, CopyFormat::Csv(o) if o.header);
        Self {
            table,
            columns,
            column_count,
            column_kinds,
            carry: Vec::new(),
            rows_ingested: 0,
            pending_rows: Vec::new(),
            batch_size: resolve_copy_batch_size(),
            batch_start_row: 1,
            format,
            pending_header,
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
    /// expected initial values (empty carry, zero rows ingested,
    /// empty column_kinds — back-compat shim).
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
        assert!(s.column_kinds.is_empty());
        // SP-PG-COPY-BULKAPPLY V1 — pending buffer + batch fields.
        assert!(s.pending_rows.is_empty());
        assert!(s.batch_size >= 1);
        assert_eq!(s.batch_start_row, 1);
    }

    /// SP-PG-COPY-BULKAPPLY V1: the default batch size is
    /// `COPY_BATCH_SIZE` (1024) when no env override is set.
    #[test]
    fn t1_bulkapply_default_batch_size_is_1024() {
        // Defensive: a parallel test in this process might set the
        // env var. Guard by clearing for this thread first. The fact
        // that env vars are process-global means this is best-effort.
        std::env::remove_var("KESSELDB_COPY_BATCH_SIZE");
        let s = CopyInState::new("t".to_string(), None, 1);
        assert_eq!(s.batch_size, COPY_BATCH_SIZE);
        assert_eq!(COPY_BATCH_SIZE, 1024);
    }

    /// SP-PG-COPY-BULKAPPLY V1: `KESSELDB_COPY_BATCH_SIZE=N` overrides
    /// the default batch size. `resolve_copy_batch_size()` reads the
    /// env directly so the override is honored per call.
    #[test]
    fn t1_bulkapply_env_override_changes_batch_size() {
        // Setting + reading the env var is process-global. Use a
        // distinctive value + scope the test to make collisions
        // visible if they happen.
        std::env::set_var("KESSELDB_COPY_BATCH_SIZE", "256");
        let got = resolve_copy_batch_size();
        std::env::remove_var("KESSELDB_COPY_BATCH_SIZE");
        assert_eq!(got, 256);
    }

    /// SP-PG-COPY-BULKAPPLY V1: out-of-range / unparseable env values
    /// fall back to the default `COPY_BATCH_SIZE`. 0 is invalid (a
    /// 0-row batch never flushes — that'd be a bug). Above
    /// `COPY_BATCH_SIZE_MAX` clamps DOWN to the cap.
    #[test]
    fn t1_bulkapply_env_override_handles_invalid_values() {
        std::env::set_var("KESSELDB_COPY_BATCH_SIZE", "0");
        assert_eq!(resolve_copy_batch_size(), COPY_BATCH_SIZE);
        std::env::set_var("KESSELDB_COPY_BATCH_SIZE", "not-a-number");
        assert_eq!(resolve_copy_batch_size(), COPY_BATCH_SIZE);
        std::env::set_var("KESSELDB_COPY_BATCH_SIZE", "999999999");
        assert_eq!(resolve_copy_batch_size(), COPY_BATCH_SIZE_MAX);
        std::env::remove_var("KESSELDB_COPY_BATCH_SIZE");
    }

    /// SP-PG-COPY T2 fix: `CopyInState::new_with_kinds` carries the
    /// per-column FieldKinds for schema-aware SQL synthesis.
    #[test]
    fn t2_copy_in_state_new_with_kinds_carries_kinds() {
        let s = CopyInState::new_with_kinds(
            "users".to_string(),
            Some(vec!["id".to_string(), "name".to_string()]),
            2,
            vec![
                kessel_catalog::FieldKind::I64,
                kessel_catalog::FieldKind::Char(32),
            ],
        );
        assert_eq!(s.column_kinds.len(), 2);
        assert_eq!(s.column_kinds[0], kessel_catalog::FieldKind::I64);
        assert_eq!(s.column_kinds[1], kessel_catalog::FieldKind::Char(32));
    }
}
