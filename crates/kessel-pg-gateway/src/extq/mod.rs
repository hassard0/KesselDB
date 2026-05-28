//! SP-PG-EXTQ — Extended Query protocol (Parse / Bind / Describe /
//! Execute / Sync / Close / Flush).
//!
//! **T1 status (this commit):** module scaffold — per-connection
//! `SessionState` types + a `try_dispatch_extq` placeholder dispatcher
//! that recognizes the extended-query message tags but returns
//! `Err(ExtqError::NotYetImplemented)` for every one. T2..T9 widen the
//! dispatcher into real Parse/Bind/Describe/Execute/Sync/Close/Flush
//! handling per the 12-slice plan in the companion design spec.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`
//!
//! ## What this module DOES (T1)
//!
//! - Declare the `proto` (frontend decoders) + `response` (backend
//!   encoders) child modules.
//! - Define the per-connection `SessionState` + `PreparedStmt` +
//!   `Portal` + `ExecState` types the V1 plan locks (spec §3).
//! - Expose `try_dispatch_extq(state, message) -> ExtqOutcome` — the
//!   placeholder dispatcher T2+ will fill in. T1 returns
//!   `Err(ExtqError::NotYetImplemented)` for every variant so
//!   `server::run_session` has a single entry point to call once it
//!   recognizes an extq tag.
//! - Expose `recognize_extq_tag(tag)` so the run_session loop can
//!   branch into the extq path without coupling on the proto enum.
//!
//! ## What this module does NOT do yet (T2..T12)
//!
//! - The placeholder dispatcher does NOT yet store prepared statements
//!   or portals (T2 / T3).
//! - It does NOT yet do parameter substitution (T6).
//! - It does NOT yet drive Execute through `dispatch::dispatch_query`
//!   (T6).
//! - It does NOT yet implement the error-recovery state machine
//!   (skip-until-Sync) (T7).
//! - It does NOT yet implement PortalSuspended / max_rows pagination
//!   (T9).
//!
//! ## Locked invariants (T1)
//!
//! - `recognize_extq_tag` returns the same boolean answer for every
//!   `FE_PARSE / FE_BIND / FE_DESCRIBE / FE_EXECUTE / FE_SYNC /
//!   FE_CLOSE / FE_FLUSH` tag value from `crate::proto`. Future
//!   `server.rs` refactors cannot drift the tag set silently.
//! - Per-connection cap constants are public so the `server.rs`
//!   integration layer can advertise them in error messages.

#![forbid(unsafe_code)]
#![allow(dead_code)]

pub mod proto;
pub mod response;

use crate::proto::{FE_BIND, FE_CLOSE, FE_DESCRIBE, FE_EXECUTE, FE_FLUSH, FE_PARSE, FE_SYNC};
use std::collections::HashMap;

/// Spec §7.1 — per-connection cap on the number of named prepared
/// statements. Parse with a fresh name beyond this cap → `08P01
/// protocol_violation`. 4096 matches the V1 design spec; operators
/// with extreme workloads can tune in a future config-knob.
pub const MAX_PREPARED_STATEMENTS_PER_CONN: usize = 4096;

/// Spec §7.1 — per-connection cap on the number of named portals.
/// Same shape + same cap as prepared statements.
pub const MAX_PORTALS_PER_CONN: usize = 4096;

/// Spec §7.1 — per-Bind cap on the parameter count. The wire field
/// is `i16` so the protocol cap is `i16::MAX` = 32767; we accept
/// `u16::MAX` since some clients render the field unsigned.
pub const MAX_PARAMETERS_PER_BIND: usize = u16::MAX as usize;

/// Per-connection extended-query state. One instance lives next to
/// the existing `AcceptedSession` from V1, attached thread-locally
/// to the connection's reader thread (no `Arc`, no `Mutex` — strictly
/// single-owner).
///
/// Empty-name `""` is the volatile slot for both `statements` and
/// `portals`; Parse / Bind with name="" auto-drops + replaces it.
/// Named slots persist until explicit `Close` or connection close.
#[derive(Debug, Default)]
pub struct SessionState {
    /// Named + unnamed prepared statements. Key="" is the volatile slot.
    statements: HashMap<String, PreparedStmt>,
    /// Named + unnamed portals. Key="" is the volatile slot.
    portals: HashMap<String, Portal>,
    /// Spec §6 — set true on the first error of a Sync-bounded
    /// sequence; reset on `Sync`. While true, the dispatcher
    /// SKIPS every subsequent extq message (silently) until Sync.
    error_state: bool,
}

impl SessionState {
    /// Construct a fresh state. Convenience over `Default::default`
    /// for callers that don't want to import the trait.
    pub fn new() -> Self { Self::default() }

    /// Spec §3 — true while we're skipping messages after a pipelined
    /// error. T7 widens this; T1 just exposes the read.
    pub fn in_error_state(&self) -> bool { self.error_state }

    /// Count of currently-stored named statements (including the
    /// volatile "" slot). Surfaced for the cap-check in Parse (T2).
    pub fn statement_count(&self) -> usize { self.statements.len() }

    /// Count of currently-stored named portals (including the
    /// volatile "" slot). Surfaced for the cap-check in Bind (T3).
    pub fn portal_count(&self) -> usize { self.portals.len() }
}

/// A prepared statement (Parse output). Spec §3 — V1 stores the
/// SQL VERBATIM; no AST cache. The engine re-parses on every Execute
/// (acceptable for V1 because the SP47 compile-cache already
/// de-duplicates inside the engine per SQL string).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedStmt {
    /// Original SQL text from Parse, BEFORE parameter substitution.
    pub sql: String,
    /// Parameter type OIDs from Parse. May be empty (client omitted
    /// type hints) or partial. V1 ignores at Bind/Execute but echoes
    /// them in Describe 'S' → ParameterDescription replies (T4).
    pub param_oids: Vec<u32>,
}

/// A portal (Bind output) — a prepared statement plus a binding of
/// parameter values, ready for one or more Executes. Spec §3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Portal {
    /// Name of the statement this portal binds. Looked up at Execute
    /// time (not cached, because a subsequent Close 'S' on the stmt
    /// name would invalidate any cached reference).
    pub stmt_name: String,
    /// Bound parameter values, in position order. Each value is
    /// either `Some(bytes)` (the raw text-format wire bytes the
    /// client sent) or `None` (the `i32::-1` length sentinel = SQL
    /// NULL).
    pub param_values: Vec<Option<Vec<u8>>>,
    /// Per-position parameter format codes from Bind. V1 enforces
    /// every code is `FORMAT_CODE_TEXT` (0); any 1 (binary) is
    /// rejected with `0A000 feature_not_supported` at Bind time
    /// (T3). Length conventions match PG: 0 codes = "all text",
    /// 1 code = "every position the same", N codes = "per-position".
    pub param_formats: Vec<u16>,
    /// Per-position result format codes from Bind. V1 emits text
    /// always; any client-requested binary code is silently ignored
    /// in V1 — `RowDescription` carries the format_code we actually
    /// used (text=0), and clients tolerate this per PG §55.2.3.
    pub result_formats: Vec<u16>,
    /// Spec §7.2 — in-progress execution cursor. None until first
    /// Execute; then Some(buffered_rows) — V1 buffers all rows at
    /// first Execute and pages from the buffer for PortalSuspended
    /// (T9).
    pub exec_state: ExecState,
}

/// In-progress execution state for one portal. Spec §7.2.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ExecState {
    /// Portal not yet executed.
    #[default]
    Pending,
    /// Portal executed; rows buffered. `cursor` is the index of the
    /// next row to emit; the buffer's `len()` is the total row count
    /// for CommandComplete once we exhaust the buffer.
    Buffered { rows: Vec<Vec<u8>>, cursor: usize },
    /// Portal exhausted (CommandComplete already emitted). Further
    /// Executes on this portal emit `CommandComplete("SELECT 0")`
    /// per PG §55.2.3 — the libpq-tested shape for re-Executing
    /// a fully-drained portal.
    Exhausted { total: u64 },
}

/// Errors the extq dispatcher can return. Each maps to a SQLSTATE
/// the caller renders into an `ErrorResponse` frame.
#[derive(Debug, PartialEq, Eq)]
pub enum ExtqError {
    /// T1 placeholder — the message tag was recognized but the
    /// dispatcher hasn't been implemented for it yet. T2+ widens
    /// each match arm into the real handler; this variant goes away
    /// at T12 closure.
    ///
    /// Maps to SQLSTATE `0A000 feature_not_supported` at the
    /// `server::run_session` boundary — that's how V1's current
    /// extq-tag-rejection path renders today and the contract V2
    /// preserves.
    NotYetImplemented { tag: u8 },
    /// The decoder rejected the message body as malformed (length
    /// field internal-inconsistency, missing NUL terminator, etc).
    /// Maps to SQLSTATE `08P01 protocol_violation`.
    Decode { reason: &'static str },
    /// Spec §7.1 — the connection already holds the cap of named
    /// statements (or portals); Parse / Bind with a fresh name
    /// rejected. Maps to `08P01`.
    TooManyPreparedStatements,
    /// Same shape as `TooManyPreparedStatements` for portals.
    TooManyPortals,
    /// Spec §4 — bound parameter at this position used the binary
    /// format code (1), which V1 does not yet support. Maps to
    /// `0A000 feature_not_supported`.
    BinaryFormatNotSupported { position: usize },
    /// Lookup-time miss — Bind / Describe / Execute / Close named a
    /// statement or portal that doesn't exist on this connection.
    /// Maps to `26000 invalid_sql_statement_name`.
    UnknownStatement { name: String },
    /// Same shape for portals; maps to `34000 invalid_cursor_name`.
    UnknownPortal { name: String },
}

/// Outcome of dispatching one extq message. T2+ widens the
/// `Bytes` variant to carry the encoded response frame; T1 only
/// ever returns `NotYetImplemented` (or one of the validation
/// variants — though T1's placeholder dispatcher doesn't even reach
/// them).
#[derive(Debug)]
pub enum ExtqOutcome {
    /// Successful dispatch — caller emits the bytes verbatim on the
    /// wire. May be empty (e.g. `Flush` doesn't itself encode a
    /// response; T8 / T7 will route flush requests through the
    /// per-message flush call).
    Bytes(Vec<u8>),
    /// Dispatch failure — caller renders the error to an
    /// `ErrorResponse` frame and (for non-Sync messages) enters
    /// `state.error_state = true`.
    Failed(ExtqError),
    /// Sentinel — only `Sync` returns this; T7 implements. The
    /// caller emits `ReadyForQuery('I')` and clears
    /// `state.error_state`.
    SyncCompleted,
}

/// True iff `tag` is one of the seven Extended Query frontend message
/// tags from `crate::proto`. `server::run_session` calls this on every
/// inbound message tag byte to decide whether to enter the extq path.
///
/// Locked KAT — the tag set MUST stay byte-stable; a refactor that
/// drifts (e.g. drops `H` Flush from the set) breaks every ORM that
/// pipelines aggressively.
pub fn recognize_extq_tag(tag: u8) -> bool {
    matches!(
        tag,
        FE_PARSE | FE_BIND | FE_DESCRIBE | FE_EXECUTE | FE_SYNC | FE_CLOSE | FE_FLUSH,
    )
}

/// Per-message dispatcher entry point. T2+ widens each match arm.
///
/// `state` carries the per-connection extq state across the whole
/// session; `message` is the decoded frontend message (one variant
/// per tag from `extq::proto::ExtqMessage`). The return value is the
/// outcome the caller renders to wire bytes.
///
/// T1 contract: every arm returns
/// `ExtqOutcome::Failed(ExtqError::NotYetImplemented { tag })` so
/// the regression-lock test catches a half-shipped T2/T3/etc.
pub fn try_dispatch_extq(
    _state: &mut SessionState,
    message: proto::ExtqMessage,
) -> ExtqOutcome {
    use proto::ExtqMessage as M;
    let tag = match &message {
        M::Parse { .. } => FE_PARSE,
        M::Bind { .. } => FE_BIND,
        M::Describe { .. } => FE_DESCRIBE,
        M::Execute { .. } => FE_EXECUTE,
        M::Sync => FE_SYNC,
        M::Close { .. } => FE_CLOSE,
        M::Flush => FE_FLUSH,
    };
    ExtqOutcome::Failed(ExtqError::NotYetImplemented { tag })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::*;

    // ───────────────────────────────────────────────────────────────────
    // T1 KATs — lock the scaffold invariants. Every constant + state
    // type + dispatcher branch the T2+ implementation depends on is
    // pinned here so a refactor cannot silently drift the surface.
    // ───────────────────────────────────────────────────────────────────

    /// `recognize_extq_tag` accepts exactly the seven Extended Query
    /// frontend message tags from `crate::proto` and nothing else.
    /// Spec §8 + §9 wire-message inventory.
    #[test]
    fn t1_recognize_extq_tag_accepts_exactly_the_seven_extq_tags() {
        // ACCEPT — the seven extq tags.
        assert!(recognize_extq_tag(FE_PARSE));
        assert!(recognize_extq_tag(FE_BIND));
        assert!(recognize_extq_tag(FE_DESCRIBE));
        assert!(recognize_extq_tag(FE_EXECUTE));
        assert!(recognize_extq_tag(FE_SYNC));
        assert!(recognize_extq_tag(FE_CLOSE));
        assert!(recognize_extq_tag(FE_FLUSH));
        // ASCII char asserts — locks the byte values against drift.
        assert!(recognize_extq_tag(b'P'));
        assert!(recognize_extq_tag(b'B'));
        assert!(recognize_extq_tag(b'D'));
        assert!(recognize_extq_tag(b'E'));
        assert!(recognize_extq_tag(b'S'));
        assert!(recognize_extq_tag(b'C'));
        assert!(recognize_extq_tag(b'H'));

        // REJECT — Simple Query + Terminate + SCRAM + COPY tags
        // must NOT route into the extq path.
        assert!(!recognize_extq_tag(FE_QUERY));
        assert!(!recognize_extq_tag(FE_TERMINATE));
        assert!(!recognize_extq_tag(FE_PASSWORD));
        assert!(!recognize_extq_tag(FE_COPY_DATA));
        assert!(!recognize_extq_tag(FE_COPY_DONE));
        assert!(!recognize_extq_tag(FE_COPY_FAIL));
        assert!(!recognize_extq_tag(FE_FUNCTION_CALL));
        // Backend tags ALSO must NOT match — server::run_session
        // never inspects a BE tag from a client byte stream, but
        // the symmetry assertion catches a future bug where the
        // proto module's BE_XXX constants shift to collide with FE.
        assert!(!recognize_extq_tag(BE_AUTHENTICATION)); // 'R'
        assert!(!recognize_extq_tag(BE_NOTICE_RESPONSE)); // 'N'
        assert!(!recognize_extq_tag(BE_ROW_DESCRIPTION)); // 'T'
        assert!(!recognize_extq_tag(BE_PARSE_COMPLETE)); // '1'
        assert!(!recognize_extq_tag(BE_BIND_COMPLETE)); // '2'

        // RANDOM bytes must NOT match.
        for b in [0, 1, b'A', b'Z' - 1, b'a', b'z', 0xFE, 0xFF] {
            // 'B' (Bind) and 'D' (Describe), 'C' (Close), 'E' (Execute),
            // 'S' (Sync), 'H' (Flush), 'P' (Parse) — confirm none of
            // these random bytes are accidentally a valid tag.
            if !matches!(b, b'P' | b'B' | b'D' | b'E' | b'S' | b'C' | b'H') {
                assert!(!recognize_extq_tag(b),
                    "unexpected tag byte 0x{b:02X} accepted as extq");
            }
        }
    }

    /// Per-connection caps are non-zero + within a reasonable
    /// operational range. Spec §7.1 — locked so a refactor doesn't
    /// silently slash the cap to e.g. 16 (would break every ORM
    /// connection pool that pre-Parses queries).
    #[test]
    fn t1_per_connection_caps_are_locked_in_range() {
        assert_eq!(MAX_PREPARED_STATEMENTS_PER_CONN, 4096);
        assert_eq!(MAX_PORTALS_PER_CONN, 4096);
        assert!(MAX_PARAMETERS_PER_BIND >= u16::MAX as usize);
        // Sanity range — must be at least 1024 (typical ORM pool's
        // active-prepared-statement working-set) and at most 65536
        // (so per-conn RSS stays bounded under attacker pressure).
        assert!(MAX_PREPARED_STATEMENTS_PER_CONN >= 1024);
        assert!(MAX_PREPARED_STATEMENTS_PER_CONN <= 65536);
        assert!(MAX_PORTALS_PER_CONN >= 1024);
        assert!(MAX_PORTALS_PER_CONN <= 65536);
    }

    /// SessionState constructor + accessor invariants. Locked so a
    /// future T2+ implementer cannot accidentally leak the volatile
    /// "" slot at startup.
    #[test]
    fn t1_session_state_starts_empty_and_not_in_error() {
        let s = SessionState::new();
        assert_eq!(s.statement_count(), 0);
        assert_eq!(s.portal_count(), 0);
        assert!(!s.in_error_state());
        // Default impl matches `new`.
        let d = SessionState::default();
        assert_eq!(d.statement_count(), 0);
        assert_eq!(d.portal_count(), 0);
        assert!(!d.in_error_state());
    }

    /// ExecState default is `Pending` — a freshly-bound portal hasn't
    /// executed yet. T9 will lean on this default to distinguish the
    /// first-Execute path (which buffers) from re-Execute (which
    /// pages from the buffer).
    #[test]
    fn t1_exec_state_default_is_pending() {
        let e: ExecState = Default::default();
        assert!(matches!(e, ExecState::Pending));
    }

    /// `try_dispatch_extq` returns `NotYetImplemented` with the
    /// CORRECT tag byte for every variant. Locks the T1 stub against
    /// a half-shipped T2/T3/... — when T2 lands, the corresponding
    /// arm in this test flips to assert the success outcome instead.
    #[test]
    fn t1_try_dispatch_returns_not_yet_implemented_for_every_tag() {
        let mut state = SessionState::new();
        let cases: Vec<(proto::ExtqMessage, u8)> = vec![
            (
                proto::ExtqMessage::Parse {
                    name: String::new(),
                    sql: "SELECT 1".to_string(),
                    param_oids: vec![],
                },
                FE_PARSE,
            ),
            (
                proto::ExtqMessage::Bind {
                    portal: String::new(),
                    stmt: String::new(),
                    param_formats: vec![],
                    param_values: vec![],
                    result_formats: vec![],
                },
                FE_BIND,
            ),
            (
                proto::ExtqMessage::Describe {
                    target: b'S',
                    name: String::new(),
                },
                FE_DESCRIBE,
            ),
            (
                proto::ExtqMessage::Execute {
                    portal: String::new(),
                    max_rows: 0,
                },
                FE_EXECUTE,
            ),
            (proto::ExtqMessage::Sync, FE_SYNC),
            (
                proto::ExtqMessage::Close {
                    target: b'P',
                    name: String::new(),
                },
                FE_CLOSE,
            ),
            (proto::ExtqMessage::Flush, FE_FLUSH),
        ];
        for (msg, expected_tag) in cases {
            match try_dispatch_extq(&mut state, msg) {
                ExtqOutcome::Failed(ExtqError::NotYetImplemented { tag }) => {
                    assert_eq!(tag, expected_tag);
                }
                other => panic!(
                    "expected NotYetImplemented(tag={expected_tag:#x}), got {other:?}"
                ),
            }
        }
        // T1 dispatcher must NOT mutate state — it's a pure NYI stub.
        assert_eq!(state.statement_count(), 0);
        assert_eq!(state.portal_count(), 0);
        assert!(!state.in_error_state());
    }
}
