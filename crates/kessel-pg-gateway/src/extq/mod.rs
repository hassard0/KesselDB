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

    /// Read-only lookup for a stored statement by name (empty `""`
    /// is the volatile slot). Used by T2 KATs to verify the Parse
    /// arm actually stored the SQL + OID hints, and by T3+ for the
    /// Bind path to resolve the target statement. The return type
    /// keeps the storage opaque — callers can read but not mutate.
    pub fn get_statement(&self, name: &str) -> Option<&PreparedStmt> {
        self.statements.get(name)
    }

    /// SP-PG-EXTQ T3 — read-only lookup for a stored portal by name
    /// (empty `""` is the volatile slot). Mirrors `get_statement`
    /// for the Bind path; T3 KATs use this to verify the Bind arm
    /// actually stored the portal under the right key, and
    /// T5/T6/T9 will reuse it for Describe 'P' / Execute / re-
    /// Execute.
    pub fn get_portal(&self, name: &str) -> Option<&Portal> {
        self.portals.get(name)
    }

    /// SP-PG-EXTQ T3 — test-only injector for the error-recovery
    /// state. Spec §6 — `error_state = true` means the dispatcher
    /// SKIPS every subsequent extq message until it sees `Sync`.
    /// T7 wires the full state machine; T3 only needs to verify
    /// that an error-state Bind returns without processing. KATs
    /// inject the flag here; production code sets it via the
    /// dispatcher's failure paths.
    #[cfg(test)]
    fn set_error_state(&mut self, in_error: bool) {
        self.error_state = in_error;
    }
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
    /// Spec §3 — Parse received a non-empty statement name that
    /// already exists in `state.statements`. Per PG §55.2.3 the
    /// client must Close the existing statement first; V1 rejects
    /// the new Parse rather than silently replace. Maps to SQLSTATE
    /// `42P05 prepared_statement_already_exists`. (Empty-name `""`
    /// is the volatile slot — Parse with `name=""` ALWAYS replaces
    /// silently and never returns this error.)
    PreparedStatementAlreadyExists { name: String },
    /// Spec §3 — Bind received a non-empty portal name that already
    /// exists in `state.portals`. Per PG §55.2.3 the client must
    /// Close the existing portal first; V1 rejects the new Bind
    /// rather than silently replace. Maps to SQLSTATE `42P03
    /// duplicate_cursor`. (Empty-name `""` is the volatile slot —
    /// Bind with `portal=""` ALWAYS replaces silently and never
    /// returns this error.)
    DuplicateCursor { name: String },
    /// Spec §4 — Bind referenced a parameter-count that doesn't
    /// match the prepared statement's expected count. The wire
    /// `param_value_count` field disagrees with the
    /// `PreparedStmt.param_oids.len()` count from Parse. Maps to
    /// SQLSTATE `08P02 protocol_violation_parameter_count`.
    ParameterCountMismatch { expected: usize, actual: usize },
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
    /// Spec §6 — the dispatcher was in `error_state == true` and
    /// the message is NOT Sync; the message is silently dropped.
    /// Caller writes nothing on the wire and waits for the next
    /// message (which may be another skip-target or the Sync that
    /// clears the error state). T7 wires Sync's flag-clear; T3
    /// introduces the variant so Bind in error_state has the right
    /// shape today.
    Skipped,
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
/// T2 contract: the `Parse` arm is REAL — installs the prepared
/// statement under `name` (empty="" volatile slot OR a named slot)
/// and returns `ExtqOutcome::Bytes(ParseComplete)`. The other six
/// arms (Bind / Describe / Execute / Sync / Close / Flush) STILL
/// return `Failed(NotYetImplemented)` — T3..T8 widen them one at a
/// time per the SP-PG-EXTQ §10 task decomposition.
pub fn try_dispatch_extq(
    state: &mut SessionState,
    message: proto::ExtqMessage,
) -> ExtqOutcome {
    use proto::ExtqMessage as M;
    // Spec §6 — once a pipelined error has set `error_state = true`,
    // every subsequent extq message is SILENTLY DROPPED until the
    // dispatcher sees `Sync` (which clears the flag + emits RFQ —
    // T7 wires that arm). Until T7, Sync itself still hits the
    // NotYetImplemented arm below so this skip-check is the early
    // mechanism for B/D/E/C/H to "fall through" after a Bind error.
    if state.error_state {
        if matches!(message, M::Sync) {
            // T7 owns the full Sync handler (state clear + RFQ
            // emit). T3 leaves Sync on the NotYetImplemented path
            // so the run_session loop's existing Sync-NYI
            // rendering still applies.
            return ExtqOutcome::Failed(ExtqError::NotYetImplemented { tag: FE_SYNC });
        }
        return ExtqOutcome::Skipped;
    }
    match message {
        M::Parse {
            name,
            sql,
            param_oids,
        } => dispatch_parse(state, name, sql, param_oids),
        M::Bind {
            portal,
            stmt,
            param_formats,
            param_values,
            result_formats,
        } => dispatch_bind(
            state,
            portal,
            stmt,
            param_formats,
            param_values,
            result_formats,
        ),
        M::Describe { .. } => {
            ExtqOutcome::Failed(ExtqError::NotYetImplemented { tag: FE_DESCRIBE })
        }
        M::Execute { .. } => {
            ExtqOutcome::Failed(ExtqError::NotYetImplemented { tag: FE_EXECUTE })
        }
        M::Sync => ExtqOutcome::Failed(ExtqError::NotYetImplemented { tag: FE_SYNC }),
        M::Close { .. } => ExtqOutcome::Failed(ExtqError::NotYetImplemented { tag: FE_CLOSE }),
        M::Flush => ExtqOutcome::Failed(ExtqError::NotYetImplemented { tag: FE_FLUSH }),
    }
}

/// SP-PG-EXTQ T2 — real handler for the `P` Parse message.
///
/// Spec §3 + §7.1 invariants enforced here, in this order:
///
/// 1. **Cap.** If `name` is fresh (NOT already present in
///    `state.statements`) AND `state.statements.len() >=
///    MAX_PREPARED_STATEMENTS_PER_CONN`, reject with
///    `TooManyPreparedStatements` → `08P01`. The "fresh name" check
///    is what lets a client Parse + re-Parse the SAME named slot
///    without hitting the cap (the cap is a count of DISTINCT
///    slots, not a rate limit on Parse messages).
///
///    The unnamed slot `""` participates in the count too — V1 caps
///    the total HashMap size, which is correct for the §7.1
///    memory-bound guarantee.
///
/// 2. **Name collision.** If `name` is NON-EMPTY AND a statement
///    already exists at that name → `PreparedStatementAlreadyExists`
///    → `42P05`. Per PG §55.2.3 the client must `Close 'S' <name>`
///    first. V1 deliberately does NOT silently replace because the
///    silent-replace behavior would mask client bugs (the typical
///    cause is two threads sharing a connection — a real bug the
///    error message helps diagnose).
///
/// 3. **Empty-name overwrite.** If `name == ""` the volatile slot
///    is silently replaced — no error, no cap-recheck (we're not
///    growing the HashMap). Spec §3: "Parse name="" → drop any
///    existing unnamed statement, install the new one."
///
/// 4. **Store verbatim.** Per spec §3 V1 stores the SQL TEXT
///    UNCHANGED — no parse, no AST-cache, no normalization. The
///    engine re-parses on every Execute (the SP47 compile-cache
///    inside the engine de-duplicates per SQL string, so the
///    re-parse is cheap). Empty SQL is permitted per spec §12 open
///    question #5 — at Execute time it renders as
///    `EmptyQueryResponse` not `CommandComplete`, matching PG.
///
/// 5. **ParseComplete.** Emit the 5-byte `1 [length=4]` envelope
///    per spec §9. Caller writes the bytes verbatim; no flush
///    here (eager-flush is the caller's responsibility per §5).
///
/// Non-goals: this handler does NOT attempt to pre-parse the SQL.
/// Spec §3 + §10 self-review #1 explicitly defer SQL parse errors
/// to Execute time — that's where the engine actually plans the
/// SQL and where PG itself produces type-mismatch / undefined-
/// table errors. Pre-parsing here would (a) couple Parse to the
/// engine's catalog state (which can change between Parse and
/// Execute via DDL) and (b) double the parser work.
fn dispatch_parse(
    state: &mut SessionState,
    name: String,
    sql: String,
    param_oids: Vec<u32>,
) -> ExtqOutcome {
    // Spec §3 + §7.1: cap check uses the FRESH-NAME rule. A Parse
    // overwriting the volatile "" slot (or replacing a name that
    // is already present — though we reject that with 42P05 below
    // before we'd grow the map) does NOT count against the cap.
    let is_fresh_name = !state.statements.contains_key(&name);
    if is_fresh_name && state.statements.len() >= MAX_PREPARED_STATEMENTS_PER_CONN {
        return ExtqOutcome::Failed(ExtqError::TooManyPreparedStatements);
    }

    // Spec §3: named-slot collision is an error (42P05). The
    // unnamed `""` slot is the EXCEPTION — always replaced
    // silently.
    if !name.is_empty() && state.statements.contains_key(&name) {
        return ExtqOutcome::Failed(ExtqError::PreparedStatementAlreadyExists { name });
    }

    state
        .statements
        .insert(name, PreparedStmt { sql, param_oids });
    ExtqOutcome::Bytes(response::encode_parse_complete())
}

/// SP-PG-EXTQ T3 — real handler for the `B` Bind message.
///
/// Spec §3 + §4 + §7.1 invariants enforced here, in this order:
///
/// 1. **Statement lookup.** Resolve `stmt` against
///    `state.statements`. A missing statement is `UnknownStatement
///    { name }` → SQLSTATE `26000 invalid_sql_statement_name`. The
///    lookup also gives us the prepared parameter count for the
///    next step. Spec §4 / PG §55.2.3.
///
/// 2. **Binary-format rejection.** Spec §4 — V1 only accepts text-
///    format parameters (format code 0). Any binary code (1) at
///    any position is rejected with `BinaryFormatNotSupported
///    { position }` → `0A000 feature_not_supported`. The length
///    conventions match PG: `0` codes = "all positions text" (no
///    rejection), `1` code = "every position the same" (reject
///    everything if that single code is 1), `N` codes = "per-
///    position" (reject the first position where the code is 1).
///
/// 3. **Parameter-count match.** Spec §4 — the wire
///    `param_value_count` MUST equal the prepared statement's
///    `param_oids.len()` (when Parse supplied OID hints). When
///    Parse omitted OID hints (`param_oids.len() == 0`), V1 accepts
///    any count because the OIDs are advisory only — the engine
///    resolves the types at Execute. Mismatch → `ParameterCount
///    Mismatch { expected, actual }` → SQLSTATE `08P02
///    protocol_violation_parameter_count` ("number of parameters
///    does not match").
///
/// 4. **Portal-name cap + collision.** Spec §3 + §7.1. The same
///    fresh-name rule as Parse: if `portal` is fresh AND
///    `state.portals.len() >= MAX_PORTALS_PER_CONN` → `TooManyPortals`
///    → `08P01`. Non-empty name already present → `DuplicateCursor
///    { name }` → `42P03 duplicate_cursor` (the PG SQLSTATE for
///    "cursor / portal already exists"). Empty-name `""` is the
///    volatile slot — silently replaced, no error.
///
/// 5. **Store portal.** Build `Portal { stmt_name, param_values,
///    param_formats, result_formats, exec_state: ExecState::Pending
///    }` and insert into `state.portals` under `portal`.
///
/// 6. **BindComplete.** Emit the 5-byte `2 [length=4]` envelope
///    per spec §9. Caller writes the bytes verbatim.
///
/// **Error-recovery side-effect.** Spec §6 — on ANY error from this
/// helper, set `state.error_state = true` BEFORE returning. The
/// run_session loop emits the ErrorResponse + RFQ; subsequent
/// pipelined messages until Sync hit the early-skip branch at the
/// top of `try_dispatch_extq`.
fn dispatch_bind(
    state: &mut SessionState,
    portal: String,
    stmt: String,
    param_formats: Vec<u16>,
    param_values: Vec<Option<Vec<u8>>>,
    result_formats: Vec<u16>,
) -> ExtqOutcome {
    // Spec §4 + §6: every error sets error_state. Use a closure-
    // style helper to keep the side-effect adjacent to the return.
    let set_err = |s: &mut SessionState, e: ExtqError| -> ExtqOutcome {
        s.error_state = true;
        ExtqOutcome::Failed(e)
    };

    // 1. Resolve statement. Capture the expected param count so we
    //    can drop the borrow on `state.statements` before the next
    //    mutations.
    let expected_param_count = match state.statements.get(&stmt) {
        Some(prep) => prep.param_oids.len(),
        None => return set_err(state, ExtqError::UnknownStatement { name: stmt }),
    };

    // 2. Binary-format rejection per PG length conventions (§4).
    //    Bytes through the decoder are u16 already (the codes 0/1
    //    fit in either signed or unsigned).
    match param_formats.len() {
        0 => {
            // "All text" — no rejection possible.
        }
        1 => {
            // "Every position the same" — reject if that single
            // code is binary. We use position 0 in the error so the
            // client sees the offending position.
            if param_formats[0] == crate::proto::FORMAT_CODE_BINARY {
                return set_err(state, ExtqError::BinaryFormatNotSupported { position: 0 });
            }
        }
        _ => {
            // "Per-position" — find the first binary code.
            for (i, code) in param_formats.iter().enumerate() {
                if *code == crate::proto::FORMAT_CODE_BINARY {
                    return set_err(
                        state,
                        ExtqError::BinaryFormatNotSupported { position: i },
                    );
                }
            }
        }
    }

    // 3. Parameter-count match. Empty `param_oids` means Parse
    //    didn't supply hints — accept any count (the engine
    //    resolves types at Execute). When Parse DID supply
    //    OID hints the wire count MUST match.
    if expected_param_count > 0 && expected_param_count != param_values.len() {
        return set_err(
            state,
            ExtqError::ParameterCountMismatch {
                expected: expected_param_count,
                actual: param_values.len(),
            },
        );
    }

    // 4. Portal-name cap + collision. Fresh-name rule mirrors
    //    Parse: overwriting the volatile "" slot does NOT grow
    //    the map and so does NOT count against the cap. Named-
    //    collision returns 42P03 BEFORE we'd grow the map.
    let is_fresh_name = !state.portals.contains_key(&portal);
    if is_fresh_name && state.portals.len() >= MAX_PORTALS_PER_CONN {
        return set_err(state, ExtqError::TooManyPortals);
    }
    if !portal.is_empty() && state.portals.contains_key(&portal) {
        return set_err(state, ExtqError::DuplicateCursor { name: portal });
    }

    // 5. Build and store the portal. `exec_state: Pending` is the
    //    default — T6 widens to `Buffered`/`Exhausted` at first
    //    Execute. Spec §3.
    state.portals.insert(
        portal,
        Portal {
            stmt_name: stmt,
            param_values,
            param_formats,
            result_formats,
            exec_state: ExecState::Pending,
        },
    );

    // 6. BindComplete envelope (spec §9, byte-locked by the T1
    //    response.rs KAT).
    ExtqOutcome::Bytes(response::encode_bind_complete())
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

    /// SP-PG-EXTQ T3 — the FIVE still-NYI tags (Describe / Execute /
    /// Sync / Close / Flush) return `Failed(NotYetImplemented { tag })`.
    /// Parse + Bind are NO LONGER on this list — T2 implements
    /// Parse, T3 implements Bind. See `t3_dispatch_bind_unnamed_*`
    /// for the Bind-success lock. As T4..T8 land each will flip the
    /// corresponding arm of this test to its real outcome.
    #[test]
    fn t3_try_dispatch_returns_not_yet_implemented_for_the_five_non_parse_non_bind_tags() {
        let mut state = SessionState::new();
        let cases: Vec<(proto::ExtqMessage, u8)> = vec![
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
        // The five still-NYI dispatchers do NOT mutate state.
        assert_eq!(state.statement_count(), 0);
        assert_eq!(state.portal_count(), 0);
        assert!(!state.in_error_state());
    }

    // ───────────────────────────────────────────────────────────────────
    // T2 KATs — real Parse handler. Locks every spec §3 + §7.1 + §9
    // invariant the run_session integration depends on.
    // ───────────────────────────────────────────────────────────────────

    /// Spec §3 + §9: a Parse with empty name + valid SQL emits the
    /// 5-byte ParseComplete envelope (`1 00 00 00 04`) AND installs
    /// the prepared statement under the volatile `""` slot.
    #[test]
    fn t2_dispatch_parse_unnamed_emits_parse_complete_and_stores_statement() {
        let mut state = SessionState::new();
        let msg = proto::ExtqMessage::Parse {
            name: String::new(),
            sql: "SELECT 1".to_string(),
            param_oids: vec![],
        };
        match try_dispatch_extq(&mut state, msg) {
            ExtqOutcome::Bytes(b) => {
                // Byte-locked: spec §9 ParseComplete envelope.
                assert_eq!(b, vec![b'1', 0, 0, 0, 4]);
                assert_eq!(b.len(), 5);
            }
            other => panic!("expected Bytes(ParseComplete), got {other:?}"),
        }
        // Statement is stored under "" (volatile slot).
        assert_eq!(state.statement_count(), 1);
        let stored = state
            .get_statement("")
            .expect("unnamed slot has the parsed stmt");
        assert_eq!(stored.sql, "SELECT 1");
        assert_eq!(stored.param_oids, Vec::<u32>::new());
        assert!(!state.in_error_state());
    }

    /// Spec §3: a Parse with a NAMED slot stores under that name
    /// (not under `""`).
    #[test]
    fn t2_dispatch_parse_named_stores_under_supplied_name_with_oids() {
        let mut state = SessionState::new();
        let msg = proto::ExtqMessage::Parse {
            name: "pst1".to_string(),
            sql: "SELECT $1::int".to_string(),
            param_oids: vec![23 /* PG_TYPE_INT4 */],
        };
        match try_dispatch_extq(&mut state, msg) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'1', 0, 0, 0, 4]),
            other => panic!("expected Bytes(ParseComplete), got {other:?}"),
        }
        // Stored under "pst1", NOT under "".
        assert_eq!(state.statement_count(), 1);
        assert!(state.get_statement("").is_none());
        let stored = state.get_statement("pst1").expect("named slot present");
        assert_eq!(stored.sql, "SELECT $1::int");
        assert_eq!(stored.param_oids, vec![23]);
    }

    /// Spec §3: a re-Parse with a DIFFERENT SQL into the same NAMED
    /// slot is rejected with `42P05 prepared_statement_already_exists`.
    /// Client must Close the existing statement first.
    #[test]
    fn t2_dispatch_parse_named_collision_returns_already_exists() {
        let mut state = SessionState::new();
        // First Parse installs "pst1".
        let m1 = proto::ExtqMessage::Parse {
            name: "pst1".to_string(),
            sql: "SELECT 1".to_string(),
            param_oids: vec![],
        };
        match try_dispatch_extq(&mut state, m1) {
            ExtqOutcome::Bytes(_) => {}
            other => panic!("first Parse should succeed, got {other:?}"),
        }
        // Second Parse with the SAME name + DIFFERENT SQL → 42P05.
        let m2 = proto::ExtqMessage::Parse {
            name: "pst1".to_string(),
            sql: "SELECT 2".to_string(),
            param_oids: vec![],
        };
        match try_dispatch_extq(&mut state, m2) {
            ExtqOutcome::Failed(ExtqError::PreparedStatementAlreadyExists { name }) => {
                assert_eq!(name, "pst1");
            }
            other => panic!("expected PreparedStatementAlreadyExists, got {other:?}"),
        }
        // The ORIGINAL statement is still in place (no clobber).
        let stored = state.get_statement("pst1").expect("original survives");
        assert_eq!(stored.sql, "SELECT 1");
        assert_eq!(state.statement_count(), 1);
    }

    /// Spec §3: a re-Parse on the UNNAMED `""` slot OVERWRITES the
    /// previous unnamed statement silently. No error, no 42P05.
    #[test]
    fn t2_dispatch_parse_unnamed_overwrites_previous_unnamed_statement() {
        let mut state = SessionState::new();
        // First Parse: SELECT 1 into "".
        let m1 = proto::ExtqMessage::Parse {
            name: String::new(),
            sql: "SELECT 1".to_string(),
            param_oids: vec![],
        };
        assert!(matches!(
            try_dispatch_extq(&mut state, m1),
            ExtqOutcome::Bytes(_)
        ));
        // Second Parse: SELECT 2 into "" — replaces silently.
        let m2 = proto::ExtqMessage::Parse {
            name: String::new(),
            sql: "SELECT 2".to_string(),
            param_oids: vec![25 /* PG_TYPE_TEXT */],
        };
        match try_dispatch_extq(&mut state, m2) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'1', 0, 0, 0, 4]),
            other => panic!("expected Bytes(ParseComplete), got {other:?}"),
        }
        // Count is still 1 (the SAME slot got overwritten).
        assert_eq!(state.statement_count(), 1);
        let stored = state.get_statement("").expect("unnamed slot replaced");
        assert_eq!(stored.sql, "SELECT 2");
        assert_eq!(stored.param_oids, vec![25]);
    }

    /// Spec §12 open question #5: Parse with EMPTY SQL is accepted.
    /// PG itself accepts this — Execute on the resulting portal
    /// emits EmptyQueryResponse (T6 wires that). At Parse time
    /// there is no error.
    #[test]
    fn t2_dispatch_parse_empty_sql_is_accepted() {
        let mut state = SessionState::new();
        let msg = proto::ExtqMessage::Parse {
            name: String::new(),
            sql: String::new(),
            param_oids: vec![],
        };
        match try_dispatch_extq(&mut state, msg) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'1', 0, 0, 0, 4]),
            other => panic!("expected Bytes(ParseComplete), got {other:?}"),
        }
        let stored = state.get_statement("").expect("stored");
        assert_eq!(stored.sql, "");
    }

    /// Spec §3: PreparedStmt is stored VERBATIM. The SQL bytes
    /// inside the slot are byte-equal to the input — no
    /// normalization, no trimming, no parsing. Locks the §3
    /// "store verbatim" invariant against a future refactor that
    /// might "helpfully" normalize whitespace.
    #[test]
    fn t2_dispatch_parse_stores_sql_verbatim_no_normalization() {
        let mut state = SessionState::new();
        let weird_sql = "  SELECT   1  --comment\n  ;  ".to_string();
        let msg = proto::ExtqMessage::Parse {
            name: "weird".to_string(),
            sql: weird_sql.clone(),
            param_oids: vec![],
        };
        match try_dispatch_extq(&mut state, msg) {
            ExtqOutcome::Bytes(_) => {}
            other => panic!("expected ParseComplete, got {other:?}"),
        }
        let stored = state.get_statement("weird").expect("stored");
        assert_eq!(stored.sql, weird_sql);
    }

    /// Spec §7.1: Parse beyond `MAX_PREPARED_STATEMENTS_PER_CONN`
    /// distinct named slots returns `TooManyPreparedStatements`
    /// → `08P01`. The cap test uses a much smaller in-memory test
    /// instance simulating the boundary: we pre-fill `state.
    /// statements` to the cap directly via the dispatcher (one
    /// Parse per distinct name) and verify the (cap+1)-th rejects.
    /// We test at the EXACT boundary because that's the only
    /// behavior change point.
    #[test]
    fn t2_dispatch_parse_rejects_when_cap_reached() {
        // To avoid materializing 4096 entries (slow), we use the
        // public storage directly inside the test to seed cap-1
        // entries, then dispatch the LAST entry through
        // try_dispatch_extq (success), then dispatch the OVER-CAP
        // entry through try_dispatch_extq (failure). The public
        // API isn't used for the seed because in production every
        // entry arrives via Parse — but the cap-check is purely
        // arithmetic on `state.statements.len()`, so seeding from
        // inside the module is observationally identical.
        let mut state = SessionState::new();
        // Seed cap-1 distinct named statements directly (cheap).
        for i in 0..(MAX_PREPARED_STATEMENTS_PER_CONN - 1) {
            state.statements.insert(
                format!("seed_{i}"),
                PreparedStmt { sql: String::new(), param_oids: vec![] },
            );
        }
        assert_eq!(state.statement_count(), MAX_PREPARED_STATEMENTS_PER_CONN - 1);

        // The cap-th Parse SHOULD succeed (statements.len() ==
        // CAP-1 < CAP at entry, then grows to CAP).
        let at_cap = proto::ExtqMessage::Parse {
            name: "at_cap".to_string(),
            sql: "SELECT 1".to_string(),
            param_oids: vec![],
        };
        match try_dispatch_extq(&mut state, at_cap) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'1', 0, 0, 0, 4]),
            other => panic!("at-cap Parse should succeed, got {other:?}"),
        }
        assert_eq!(state.statement_count(), MAX_PREPARED_STATEMENTS_PER_CONN);

        // The (cap+1)-th Parse with a FRESH name → 08P01.
        let over_cap = proto::ExtqMessage::Parse {
            name: "over_cap".to_string(),
            sql: "SELECT 2".to_string(),
            param_oids: vec![],
        };
        match try_dispatch_extq(&mut state, over_cap) {
            ExtqOutcome::Failed(ExtqError::TooManyPreparedStatements) => {}
            other => panic!("over-cap Parse should be rejected, got {other:?}"),
        }
        // State is unchanged after the rejection.
        assert_eq!(state.statement_count(), MAX_PREPARED_STATEMENTS_PER_CONN);
        assert!(state.get_statement("over_cap").is_none());
    }

    /// Spec §7.1 + §3 corollary: when the connection is AT the cap,
    /// a re-Parse on an EXISTING name (either the unnamed `""` slot
    /// or an already-named slot, both reusing the same hash bucket)
    /// is NOT subject to the cap check — the count doesn't grow. We
    /// only test the `""` overwrite path here because the named-slot
    /// path returns 42P05 BEFORE the cap check anyway (locked by the
    /// earlier 42P05 KAT).
    #[test]
    fn t2_dispatch_parse_at_cap_allows_unnamed_overwrite() {
        let mut state = SessionState::new();
        // Pre-seed with cap-1 named statements + ONE unnamed.
        state.statements.insert(
            String::new(),
            PreparedStmt {
                sql: "OLD".to_string(),
                param_oids: vec![],
            },
        );
        for i in 0..(MAX_PREPARED_STATEMENTS_PER_CONN - 1) {
            state.statements.insert(
                format!("named_{i}"),
                PreparedStmt { sql: String::new(), param_oids: vec![] },
            );
        }
        assert_eq!(state.statement_count(), MAX_PREPARED_STATEMENTS_PER_CONN);

        // At-cap unnamed overwrite — should succeed (overwriting
        // doesn't grow the map).
        let overwrite = proto::ExtqMessage::Parse {
            name: String::new(),
            sql: "NEW".to_string(),
            param_oids: vec![],
        };
        match try_dispatch_extq(&mut state, overwrite) {
            ExtqOutcome::Bytes(_) => {}
            other => panic!("at-cap unnamed overwrite should succeed, got {other:?}"),
        }
        assert_eq!(state.statement_count(), MAX_PREPARED_STATEMENTS_PER_CONN);
        assert_eq!(state.get_statement("").unwrap().sql, "NEW");
    }

    // ───────────────────────────────────────────────────────────────────
    // T3 KATs — real Bind handler. Locks every spec §3 + §4 + §6 + §7.1
    // + §9 invariant the run_session integration depends on.
    // ───────────────────────────────────────────────────────────────────

    /// Tiny helper: install a Parsed statement under `name` so the
    /// Bind path can resolve it. Mirrors the production flow (Parse
    /// then Bind) without re-asserting Parse's KATs.
    fn seed_stmt(state: &mut SessionState, name: &str, param_oids: Vec<u32>) {
        let msg = proto::ExtqMessage::Parse {
            name: name.to_string(),
            sql: format!("SELECT 1 -- seeded under {name}"),
            param_oids,
        };
        match try_dispatch_extq(state, msg) {
            ExtqOutcome::Bytes(_) => {}
            other => panic!("seed Parse should succeed, got {other:?}"),
        }
    }

    /// Spec §3 + §9: a Bind with empty portal name + a valid (no-
    /// param) statement emits the 5-byte BindComplete envelope
    /// (`2 00 00 00 04`) AND installs a portal under the volatile
    /// `""` slot. Byte-locked against spec §9.
    #[test]
    fn t3_dispatch_bind_unnamed_emits_bind_complete_and_stores_portal() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "", vec![]);
        let msg = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: String::new(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, msg) {
            ExtqOutcome::Bytes(b) => {
                // Byte-locked: spec §9 BindComplete envelope.
                assert_eq!(b, vec![b'2', 0, 0, 0, 4]);
                assert_eq!(b.len(), 5);
            }
            other => panic!("expected Bytes(BindComplete), got {other:?}"),
        }
        // Portal stored under "" (volatile slot).
        assert_eq!(state.portal_count(), 1);
        let p = state.get_portal("").expect("unnamed portal");
        assert_eq!(p.stmt_name, "");
        assert_eq!(p.param_values, Vec::<Option<Vec<u8>>>::new());
        assert_eq!(p.param_formats, Vec::<u16>::new());
        assert_eq!(p.result_formats, Vec::<u16>::new());
        assert!(matches!(p.exec_state, ExecState::Pending));
        assert!(!state.in_error_state());
    }

    /// Spec §3: a Bind with a NAMED portal stores under that name
    /// (not under `""`). Carries through param_values + format
    /// arrays verbatim.
    #[test]
    fn t3_dispatch_bind_named_stores_under_supplied_name() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "pst1", vec![23 /* int4 */]);
        let msg = proto::ExtqMessage::Bind {
            portal: "my_portal".to_string(),
            stmt: "pst1".to_string(),
            param_formats: vec![FORMAT_CODE_TEXT],
            param_values: vec![Some(b"42".to_vec())],
            result_formats: vec![FORMAT_CODE_TEXT],
        };
        match try_dispatch_extq(&mut state, msg) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'2', 0, 0, 0, 4]),
            other => panic!("expected Bytes(BindComplete), got {other:?}"),
        }
        assert_eq!(state.portal_count(), 1);
        assert!(state.get_portal("").is_none());
        let p = state.get_portal("my_portal").expect("named portal");
        assert_eq!(p.stmt_name, "pst1");
        assert_eq!(p.param_values, vec![Some(b"42".to_vec())]);
        assert_eq!(p.param_formats, vec![FORMAT_CODE_TEXT]);
        assert_eq!(p.result_formats, vec![FORMAT_CODE_TEXT]);
    }

    /// Spec §3 / PG §55.2.3: a Bind referencing a statement that
    /// doesn't exist is `UnknownStatement` → `26000`. Error_state
    /// is set so subsequent pipelined messages skip until Sync.
    #[test]
    fn t3_dispatch_bind_missing_statement_returns_unknown_statement() {
        let mut state = SessionState::new();
        // No seed — no statements installed.
        let msg = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "does_not_exist".to_string(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, msg) {
            ExtqOutcome::Failed(ExtqError::UnknownStatement { name }) => {
                assert_eq!(name, "does_not_exist");
            }
            other => panic!("expected UnknownStatement, got {other:?}"),
        }
        // Portal NOT installed.
        assert_eq!(state.portal_count(), 0);
        // Spec §6 — error_state engaged.
        assert!(state.in_error_state());
    }

    /// Spec §4: a Bind with parameter-count mismatch against the
    /// prepared statement's OID-hint count returns `ParameterCount
    /// Mismatch` → `08P02`. The statement DID declare 2 OIDs but
    /// the wire bound 1 value.
    #[test]
    fn t3_dispatch_bind_parameter_count_mismatch_returns_08p02() {
        let mut state = SessionState::new();
        // Stmt expects 2 params.
        seed_stmt(&mut state, "ptwo", vec![23, 23]);
        let msg = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "ptwo".to_string(),
            param_formats: vec![],
            param_values: vec![Some(b"1".to_vec())], // ONE value, not two
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, msg) {
            ExtqOutcome::Failed(ExtqError::ParameterCountMismatch { expected, actual }) => {
                assert_eq!(expected, 2);
                assert_eq!(actual, 1);
            }
            other => panic!("expected ParameterCountMismatch, got {other:?}"),
        }
        assert_eq!(state.portal_count(), 0);
        assert!(state.in_error_state());
    }

    /// Spec §4 corollary: when Parse omitted OID hints (param_oids
    /// is empty), V1 accepts ANY parameter count at Bind. The OIDs
    /// are advisory only; the engine resolves types at Execute.
    /// Locked so a future tightening doesn't accidentally start
    /// rejecting clients that omit OID hints (which is the common
    /// case for psycopg/asyncpg).
    #[test]
    fn t3_dispatch_bind_no_oid_hints_accepts_any_param_count() {
        let mut state = SessionState::new();
        // Stmt parsed with ZERO OID hints.
        seed_stmt(&mut state, "noohints", vec![]);
        let msg = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "noohints".to_string(),
            param_formats: vec![],
            param_values: vec![
                Some(b"a".to_vec()),
                Some(b"b".to_vec()),
                Some(b"c".to_vec()),
            ],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, msg) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'2', 0, 0, 0, 4]),
            other => panic!("expected BindComplete, got {other:?}"),
        }
        assert_eq!(state.portal_count(), 1);
        assert!(!state.in_error_state());
    }

    /// Spec §4: a Bind with a per-position binary format code at
    /// position 1 → `BinaryFormatNotSupported { position: 1 }` →
    /// `0A000`. V2 SP-PG-EXTQ-BIN lifts this.
    #[test]
    fn t3_dispatch_bind_binary_format_per_position_rejected() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "", vec![]);
        let msg = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: String::new(),
            param_formats: vec![FORMAT_CODE_TEXT, FORMAT_CODE_BINARY],
            param_values: vec![Some(b"a".to_vec()), Some(b"b".to_vec())],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, msg) {
            ExtqOutcome::Failed(ExtqError::BinaryFormatNotSupported { position }) => {
                assert_eq!(position, 1);
            }
            other => panic!("expected BinaryFormatNotSupported, got {other:?}"),
        }
        assert_eq!(state.portal_count(), 0);
        assert!(state.in_error_state());
    }

    /// Spec §4 length conventions: `1` format code means "every
    /// position the same"; if that single code is binary, the
    /// rejection points at position 0.
    #[test]
    fn t3_dispatch_bind_single_binary_format_applies_to_all() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "", vec![]);
        let msg = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: String::new(),
            param_formats: vec![FORMAT_CODE_BINARY],
            param_values: vec![Some(b"a".to_vec()), Some(b"b".to_vec())],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, msg) {
            ExtqOutcome::Failed(ExtqError::BinaryFormatNotSupported { position }) => {
                assert_eq!(position, 0);
            }
            other => panic!("expected BinaryFormatNotSupported, got {other:?}"),
        }
        assert!(state.in_error_state());
    }

    /// Spec §3: a re-Bind with a DIFFERENT portal payload into the
    /// same NAMED portal is rejected with `42P03 duplicate_cursor`.
    /// Client must Close the existing portal first.
    #[test]
    fn t3_dispatch_bind_duplicate_named_portal_returns_42p03() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "", vec![]);
        let first = proto::ExtqMessage::Bind {
            portal: "my_portal".to_string(),
            stmt: String::new(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, first) {
            ExtqOutcome::Bytes(_) => {}
            other => panic!("first Bind should succeed, got {other:?}"),
        }
        let second = proto::ExtqMessage::Bind {
            portal: "my_portal".to_string(),
            stmt: String::new(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, second) {
            ExtqOutcome::Failed(ExtqError::DuplicateCursor { name }) => {
                assert_eq!(name, "my_portal");
            }
            other => panic!("expected DuplicateCursor, got {other:?}"),
        }
        // Original portal preserved + count still 1.
        assert_eq!(state.portal_count(), 1);
        assert!(state.in_error_state());
    }

    /// Spec §3: a re-Bind on the UNNAMED `""` slot OVERWRITES the
    /// previous unnamed portal silently. No error, no 42P03.
    #[test]
    fn t3_dispatch_bind_unnamed_overwrites_previous_unnamed_portal() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "", vec![]);
        seed_stmt(&mut state, "pst1", vec![]);
        // First Bind targets stmt "".
        let first = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: String::new(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        assert!(matches!(
            try_dispatch_extq(&mut state, first),
            ExtqOutcome::Bytes(_)
        ));
        // Second Bind targets stmt "pst1" through the unnamed
        // portal — silently replaces.
        let second = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "pst1".to_string(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, second) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'2', 0, 0, 0, 4]),
            other => panic!("expected BindComplete, got {other:?}"),
        }
        // Count is still 1 (the SAME slot got overwritten); the
        // new portal carries the new stmt_name.
        assert_eq!(state.portal_count(), 1);
        assert_eq!(state.get_portal("").unwrap().stmt_name, "pst1");
    }

    /// Spec §6: when the dispatcher is in `error_state == true`,
    /// a Bind message is silently dropped (`ExtqOutcome::Skipped`)
    /// WITHOUT mutating any state. The error_state flag is NOT
    /// cleared (only Sync clears it — T7).
    #[test]
    fn t3_dispatch_bind_in_error_state_returns_skipped_without_processing() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "", vec![]);
        state.set_error_state(true);
        let msg = proto::ExtqMessage::Bind {
            portal: "would_install".to_string(),
            stmt: String::new(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, msg) {
            ExtqOutcome::Skipped => {}
            other => panic!("expected Skipped, got {other:?}"),
        }
        // No portal installed.
        assert_eq!(state.portal_count(), 0);
        assert!(state.get_portal("would_install").is_none());
        // Error_state STILL true (only Sync clears).
        assert!(state.in_error_state());
    }

    /// Spec §7.1: Bind beyond `MAX_PORTALS_PER_CONN` distinct
    /// portal names returns `TooManyPortals` → `08P01`. Cap test
    /// uses the EXACT boundary, mirroring the T2 Parse-cap KAT.
    #[test]
    fn t3_dispatch_bind_rejects_when_portal_cap_reached() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "", vec![]);
        // Seed cap-1 distinct named portals directly.
        for i in 0..(MAX_PORTALS_PER_CONN - 1) {
            state.portals.insert(
                format!("seed_p_{i}"),
                Portal {
                    stmt_name: String::new(),
                    param_values: vec![],
                    param_formats: vec![],
                    result_formats: vec![],
                    exec_state: ExecState::Pending,
                },
            );
        }
        assert_eq!(state.portal_count(), MAX_PORTALS_PER_CONN - 1);

        // Cap-th Bind succeeds (we grow to CAP).
        let at_cap = proto::ExtqMessage::Bind {
            portal: "at_cap".to_string(),
            stmt: String::new(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, at_cap) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'2', 0, 0, 0, 4]),
            other => panic!("at-cap Bind should succeed, got {other:?}"),
        }
        assert_eq!(state.portal_count(), MAX_PORTALS_PER_CONN);

        // Over-cap Bind (fresh name) rejects with TooManyPortals.
        // Reset error_state so this Bind isn't skipped by the
        // skip-until-Sync branch.
        state.set_error_state(false);
        let over_cap = proto::ExtqMessage::Bind {
            portal: "over_cap".to_string(),
            stmt: String::new(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, over_cap) {
            ExtqOutcome::Failed(ExtqError::TooManyPortals) => {}
            other => panic!("over-cap Bind should be rejected, got {other:?}"),
        }
        assert_eq!(state.portal_count(), MAX_PORTALS_PER_CONN);
        assert!(state.get_portal("over_cap").is_none());
        assert!(state.in_error_state());
    }

    /// Spec §4: a Bind with a NULL parameter (wire length=-1, decoded
    /// as `None`) stores the None verbatim in the portal. The Execute
    /// path (T6) will substitute `NULL` for this position.
    #[test]
    fn t3_dispatch_bind_null_parameter_carries_through_as_none() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "pst1", vec![23 /* int4 */]);
        let msg = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "pst1".to_string(),
            param_formats: vec![],
            param_values: vec![None], // SQL NULL
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, msg) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'2', 0, 0, 0, 4]),
            other => panic!("expected BindComplete, got {other:?}"),
        }
        let p = state.get_portal("").expect("portal");
        assert_eq!(p.param_values, vec![None]);
    }

    /// HEADLINE T3 KAT: a Parse + Bind pipeline through the
    /// dispatcher composes correctly. Locks the T2 → T3 transition
    /// — Parse installs a stmt, Bind references it, both emit the
    /// correct byte sequences, state has both a stmt and a portal.
    #[test]
    fn t3_dispatch_parse_then_bind_composes_end_to_end() {
        let mut state = SessionState::new();
        // Parse: stmt "ps1" with one int4 param OID.
        let parse = proto::ExtqMessage::Parse {
            name: "ps1".to_string(),
            sql: "SELECT $1::int".to_string(),
            param_oids: vec![23],
        };
        let pc = try_dispatch_extq(&mut state, parse);
        match pc {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'1', 0, 0, 0, 4]),
            other => panic!("expected ParseComplete, got {other:?}"),
        }
        // Bind portal "pt1" referencing stmt "ps1" with one text-
        // format value "42".
        let bind = proto::ExtqMessage::Bind {
            portal: "pt1".to_string(),
            stmt: "ps1".to_string(),
            param_formats: vec![FORMAT_CODE_TEXT],
            param_values: vec![Some(b"42".to_vec())],
            result_formats: vec![FORMAT_CODE_TEXT],
        };
        let bc = try_dispatch_extq(&mut state, bind);
        match bc {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'2', 0, 0, 0, 4]),
            other => panic!("expected BindComplete, got {other:?}"),
        }
        assert_eq!(state.statement_count(), 1);
        assert_eq!(state.portal_count(), 1);
        let p = state.get_portal("pt1").expect("portal");
        assert_eq!(p.stmt_name, "ps1");
        assert_eq!(p.param_values, vec![Some(b"42".to_vec())]);
        assert!(!state.in_error_state());
    }
}
