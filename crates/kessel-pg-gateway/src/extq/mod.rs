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
pub mod substitute;

use crate::engine::EngineApply;
use crate::proto::{FE_BIND, FE_CLOSE, FE_DESCRIBE, FE_EXECUTE, FE_FLUSH, FE_PARSE, FE_SYNC};
use crate::response::FieldMeta;
use crate::types::field_kind_to_oid;
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

    /// SP-PG-EXTQ T7 — drop ALL session state. Used by `DISCARD ALL`
    /// gateway-side interception (`server::run_session` recognizes the
    /// SQL and calls this before the dispatch path runs). Clears
    /// statements + portals + error_state. Per PG §SQL-DISCARD, this
    /// is the connection-pool checkout-reset hook every modern ORM
    /// relies on.
    pub fn clear_all(&mut self) {
        self.statements.clear();
        self.portals.clear();
        self.error_state = false;
    }

    /// SP-PG-EXTQ T7 — drop just prepared statements. Used by
    /// `DISCARD STATEMENTS`. Portals are preserved per PG semantics
    /// (a portal already bound stays usable until explicit Close OR
    /// Sync that drops the unnamed one). `error_state` is preserved
    /// because DISCARD STATEMENTS is itself not a Sync-equivalent —
    /// only Sync clears the error-skip flag (spec §6).
    pub fn clear_statements(&mut self) {
        self.statements.clear();
    }

    /// SP-PG-EXTQ T7 — drop just portals. Used by `DISCARD PORTALS`.
    /// Statements are preserved. Same `error_state`-preservation
    /// rationale as `clear_statements`.
    pub fn clear_portals(&mut self) {
        self.portals.clear();
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
    /// SP-PG-EXTQ T5 — tracks whether `RowDescription` has already
    /// been emitted for this portal in the current Sync block. PG
    /// protocol §55.2.3: if Describe('P') ran before Execute, the
    /// `T` RowDescription was already on the wire, and Execute MUST
    /// NOT repeat it (the client only expects ONE RowDescription
    /// per portal per Sync block). Set to true by `dispatch_describe`
    /// (T4) when it emits RowDescription for a portal, and by
    /// `dispatch_execute` (T5) when it emits RowDescription itself.
    /// Reset on Sync (T5 dispatch_sync) via portal drop/refresh
    /// semantics.
    pub row_description_sent: bool,
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
    /// SP-PG-EXTQ T4 — Describe arrived with a target byte that is
    /// neither `'S'` (statement) nor `'P'` (portal). The decoder
    /// catches this at decode time (`DecodeError::BadDescribeTarget`)
    /// and the server.rs branch routes it to `08P01` via the Decode
    /// path — but if it somehow slips through (e.g. a future direct
    /// constructor of `ExtqMessage::Describe { target: ..., name: ...
    /// }` that bypasses the decoder), the dispatcher rejects it here
    /// with `08P01 protocol_violation` so the bad shape can't
    /// silently corrupt state. Carries the offending byte for
    /// operator diagnosis.
    BadDescribeTarget { target: u8 },
    /// SP-PG-EXTQ T5 — parameter substitution failed because the
    /// SQL referenced `$0` (PG `$N` indices are 1-based) or `$N`
    /// where N > the portal's bound parameter count. Maps to
    /// SQLSTATE `08P01 protocol_violation` — either the Parse SQL
    /// or the Bind value count is bugged on the client side.
    /// Carries a human-readable description for the wire message.
    SubstitutionFailed { reason: String },
    /// SP-PG-EXTQ-BIN T2 — bound parameter at this position used the
    /// binary format code (1) AND the type OID at the same position
    /// isn't one V1's `decode_binary_param` supports. Carries the
    /// position, the OID, and the V2 follow-up arc that unlocks the
    /// gap (`SP-PG-EXTQ-BIN-NUMERIC` for NUMERIC, `SP-PG-EXTQ-BIN-
    /// EXTRA` for JSONB/UUID/ARRAY/etc). Maps to SQLSTATE `0A000
    /// feature_not_supported`. Per spec §3 + §6 the dispatcher also
    /// sets `error_state = true` so the pipeline skips until Sync.
    BinaryFormatUnsupportedForType {
        position: usize,
        type_oid: u32,
        arc: &'static str,
    },
    /// SP-PG-EXTQ-BIN T2 — bound parameter at this position used the
    /// binary format code (1) BUT Parse omitted the type OID hint at
    /// that position (or supplied OID 0 = "infer"). V1 needs the OID
    /// to dispatch the binary decoder; without it, the gateway can't
    /// safely interpret the bytes. Maps to SQLSTATE `0A000
    /// feature_not_supported`. Empirically asyncpg/JDBC always send
    /// OID hints with binary format; psycopg3 too. So this rejection
    /// is defensive — it catches a (rare) misbehaving client.
    BinaryFormatRequiresTypeOidHint { position: usize },
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
    /// response; the dedicated `Flush` outcome below carries that
    /// case).
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
    /// SP-PG-EXTQ T6 — the dispatcher processed an `H` Flush message.
    /// Per PG §55.2.3 + spec §4 Flush has no associated response —
    /// the server simply pushes any pending pipelined output to the
    /// wire. The `server::run_session` loop translates this outcome
    /// into a `writer.flush()` call WITHOUT writing any bytes.
    /// Distinct from `Bytes(Vec::new())` so the caller can clearly
    /// see a flush was requested even when no bytes are pending.
    Flush,
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
/// session; `engine` is the dispatch boundary to the storage engine
/// (used by Describe for `describe_table` lookups, and by Execute for
/// SQL apply — see T6); `message` is the decoded frontend message
/// (one variant per tag from `extq::proto::ExtqMessage`). The return
/// value is the outcome the caller renders to wire bytes.
///
/// T6 contract: ALL SEVEN extq arms (Parse / Bind / Describe / Execute
/// / Sync / Close / Flush) are REAL. Zero `NotYetImplemented` returns.
/// SP-PG-EXTQ V1 message set is COMPLETE.
///
/// **Engine read-only invariant.** Describe (T4) only calls
/// `engine.describe_table()` which is the catalog-read entry point;
/// it does NOT call `apply_sql` and does NOT mutate engine state.
/// Execute (T5) uses `apply_sql` mid-pipeline, after the client's
/// Bind has stored the portal. Close + Flush (T6) do not touch the
/// engine at all.
pub fn try_dispatch_extq<E: EngineApply + ?Sized>(
    state: &mut SessionState,
    engine: &E,
    message: proto::ExtqMessage,
) -> ExtqOutcome {
    use proto::ExtqMessage as M;
    // Spec §6 — once a pipelined error has set `error_state = true`,
    // every subsequent extq message is SILENTLY DROPPED until the
    // dispatcher sees `Sync` (which clears the flag + emits RFQ).
    // Sync is the ONLY tag that breaks out of skip-until-Sync mode;
    // Close + Flush + everything else gets `Skipped` here.
    if state.error_state {
        if matches!(message, M::Sync) {
            // SP-PG-EXTQ T5 — Sync clears the error_state + emits
            // RFQ('I') regardless of whether the dispatcher was in
            // error_state or not. This is the only way out of
            // skip-until-Sync mode.
            return dispatch_sync(state);
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
        M::Describe { target, name } => dispatch_describe(state, engine, target, name),
        M::Execute { portal, max_rows } => {
            dispatch_execute(state, engine, portal, max_rows)
        }
        M::Sync => dispatch_sync(state),
        M::Close { target, name } => dispatch_close(state, target, name),
        M::Flush => dispatch_flush(),
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

    // 1. Resolve statement. Capture the expected param count + OIDs
    //    so we can drop the borrow on `state.statements` before the
    //    next mutations. SP-PG-EXTQ-BIN T2 — the OIDs are now used at
    //    Bind time (not just Execute) so the binary-format admission
    //    check can verify each binary parameter has a supported type.
    let (expected_param_count, prep_param_oids) = match state.statements.get(&stmt) {
        Some(prep) => (prep.param_oids.len(), prep.param_oids.clone()),
        None => return set_err(state, ExtqError::UnknownStatement { name: stmt }),
    };

    // 2. Binary-format admission per PG length conventions (§4) +
    //    SP-PG-EXTQ-BIN T2 per-position OID dispatch. The length
    //    conventions (0 codes = all text, 1 code = all-same, N codes
    //    = per-position) determine the effective format per position;
    //    each position with `FORMAT_CODE_BINARY` must have a
    //    Parse-supplied type OID that V1's `decode_binary_param`
    //    accepts. Unknown OID + NUMERIC reject with precise V2-arc-
    //    pointing messages (BinaryFormatUnsupportedForType); omitted
    //    OID hint rejects with BinaryFormatRequiresTypeOidHint.
    let param_count = param_values.len();
    for pos in 0..param_count {
        let effective_format = substitute::effective_format_code(&param_formats, pos);
        if effective_format != crate::proto::FORMAT_CODE_BINARY {
            continue;
        }
        // This position is binary. We need a supported OID hint.
        let type_oid = prep_param_oids.get(pos).copied().unwrap_or(0);
        if type_oid == 0 {
            return set_err(
                state,
                ExtqError::BinaryFormatRequiresTypeOidHint { position: pos },
            );
        }
        if !substitute::binary_format_supported_for_oid(type_oid) {
            return set_err(
                state,
                ExtqError::BinaryFormatUnsupportedForType {
                    position: pos,
                    type_oid,
                    arc: substitute::unsupported_binary_arc_for_oid(type_oid),
                },
            );
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
            row_description_sent: false,
        },
    );

    // 6. BindComplete envelope (spec §9, byte-locked by the T1
    //    response.rs KAT).
    ExtqOutcome::Bytes(response::encode_bind_complete())
}

/// SP-PG-EXTQ T4 — real handler for the `D` Describe message.
///
/// Describe asks the server "what's the parameter shape + result-row
/// shape" of a stored statement or portal — clients issue it BEFORE
/// Bind/Execute so they can pre-allocate row buffers + tell the
/// application layer the column metadata. Spec §4 + PG §55.2.3.
///
/// **Target byte semantics:**
///
/// - `'S'` (statement) — resolve `name` against `state.statements`.
///   Missing → `UnknownStatement` → `26000 invalid_sql_statement_name`.
///   Emit `ParameterDescription` (`t`) carrying the OIDs from Parse,
///   followed by EITHER `RowDescription` (`T`) if the SQL is a
///   `SELECT * FROM <table>` we can introspect via `describe_table`,
///   OR `NoData` (`n`) if the SQL is non-SELECT or the SELECT shape
///   doesn't match V1's `select_star_table` detector (V1 only renders
///   whole-row SELECT; spec §11 weak-spot — V2 SP-A T14 + projection).
///
/// - `'P'` (portal) — resolve `name` against `state.portals`. Missing
///   → `UnknownPortal` → `34000 invalid_cursor_name`. Then resolve the
///   portal's `stmt_name` against `state.statements` — a portal-
///   without-stmt is defensively `UnknownStatement` → `26000` (T3's
///   Bind validation prevents this in production, but the dispatcher
///   locks the regression in case a future Close 'S' before a
///   Describe 'P' on a portal that referenced that stmt got out of
///   sync). Emit `RowDescription` / `NoData` per the same shape as
///   `'S'`, **but NOT ParameterDescription** — portals have already
///   absorbed the parameter values at Bind time (spec §4 + PG
///   §55.2.3 explicitly: "Describe with a portal name returns only
///   RowDescription/NoData; ParameterDescription is statement-only
///   because the portal's parameters have been frozen by Bind").
///
/// - Anything else — `BadDescribeTarget { target }` → `08P01`. The
///   `decode_describe` path catches this at decode time (returns
///   `DecodeError::BadDescribeTarget`), but the dispatcher
///   re-validates so a direct constructor of the message variant
///   can't bypass.
///
/// **Error-recovery side-effect** (spec §6): on ANY error from this
/// helper, set `state.error_state = true` BEFORE returning so
/// subsequent pipelined messages until Sync hit the early-skip
/// branch at the top of `try_dispatch_extq`. Same shape as
/// `dispatch_bind` (T3).
///
/// **RowDescription detection.** V1 reuses the Simple Query path's
/// detection: `kessel_sql::select_star_table(sql)` returns
/// `Some(table_name)` iff the SQL is the V1-rendered shape `SELECT *
/// FROM <table>` (no projection, no JOIN). If we get a table name,
/// `engine.describe_table(&table)` gives us the columns; if EITHER
/// returns `None`, we emit `NoData` (the client can still Execute —
/// the SQL might be a CREATE/INSERT/UPDATE/DELETE/etc., which V1
/// doesn't emit RowDescription for). Locked invariant: the
/// RowDescription bytes here are byte-equal to what
/// `dispatch_query` emits for the same `SELECT * FROM <table>` —
/// guaranteed by sharing the encoder.
fn dispatch_describe<E: EngineApply + ?Sized>(
    state: &mut SessionState,
    engine: &E,
    target: u8,
    name: String,
) -> ExtqOutcome {
    let set_err = |s: &mut SessionState, e: ExtqError| -> ExtqOutcome {
        s.error_state = true;
        ExtqOutcome::Failed(e)
    };

    match target {
        crate::proto::DESCRIBE_TARGET_STATEMENT => {
            // 'S' — describe the named statement directly.
            // Resolve + extract what we need from the borrow before
            // calling the engine (the engine call would have to share
            // the &state borrow otherwise).
            let (sql, parse_param_oids) = match state.statements.get(&name) {
                Some(prep) => (prep.sql.clone(), prep.param_oids.clone()),
                None => return set_err(state, ExtqError::UnknownStatement { name }),
            };
            // SP-PG-EXTQ-BIN T3 — when Parse provided no OID hints,
            // synthesize a ParameterDescription from the SQL's `$N`
            // placeholder count. asyncpg (and the libpq prepared-stmt
            // cache flow generally) refuses to Bind any params unless
            // PD declares at least that many positions. V1 emits
            // every position as PG_TYPE_TEXT — the gateway accepts
            // TEXT binary (= UTF-8 bytes) for every type and routes
            // the substitute layer's text path, which works for the
            // SQL parser regardless of the column's actual type.
            let described_oids = if parse_param_oids.is_empty() {
                let n = substitute::count_placeholders(&sql);
                vec![crate::proto::PG_TYPE_TEXT; n]
            } else {
                parse_param_oids
            };
            let mut out = response::encode_parameter_description(&described_oids);
            out.extend_from_slice(&row_description_or_no_data_for_sql(engine, &sql));
            // SP-PG-EXTQ-BIN T3 — persist the synthesized OIDs back
            // into the stored statement so subsequent Bind +
            // dispatch_bind binary-format admission can use them
            // (without this, Bind's `expected_param_count` would
            // still see 0 and accept any count, but the binary path
            // would route through the missing-OID branch).
            if !described_oids.is_empty() {
                if let Some(prep) = state.statements.get_mut(&name) {
                    if prep.param_oids.is_empty() {
                        prep.param_oids = described_oids;
                    }
                }
            }
            ExtqOutcome::Bytes(out)
        }
        crate::proto::DESCRIBE_TARGET_PORTAL => {
            // 'P' — describe the named portal's underlying statement.
            let stmt_name = match state.portals.get(&name) {
                Some(p) => p.stmt_name.clone(),
                None => return set_err(state, ExtqError::UnknownPortal { name }),
            };
            // Defensive: a portal-without-stmt should NEVER happen
            // because T3's `dispatch_bind` rejected on UnknownStatement
            // before storing the portal. But locking the invariant
            // here is cheap and catches a future regression where
            // Close 'S' runs before Describe 'P' (planned for T8 —
            // until then the lookup must succeed).
            let sql = match state.statements.get(&stmt_name) {
                Some(prep) => prep.sql.clone(),
                None => return set_err(state, ExtqError::UnknownStatement { name: stmt_name }),
            };
            // Portals do NOT emit ParameterDescription — Bind froze
            // the parameter values, so the client already knows them.
            // Spec §4 + PG §55.2.3.
            let bytes = row_description_or_no_data_for_sql(engine, &sql);
            // SP-PG-EXTQ T5 — record that the portal got a
            // RowDescription so the upcoming Execute (if any) does
            // NOT repeat it. PG §55.2.3: Describe-then-Execute
            // emits T exactly once per portal per Sync block.
            //
            // SP-PG-EXTQ-BIN T3 fix — only set the suppression flag
            // when Describe('P') actually emitted T (not n). For
            // `SELECT * FROM t WHERE id = $1` Describe('P') runs
            // BEFORE parameter substitution; the SQL doesn't match
            // V1's `SELECT * FROM <table>` strict shape so the helper
            // returns NoData. The subsequent Execute then runs the
            // substituted SQL and gets a real RowDescription from
            // the engine — that MUST be emitted (the previous "set
            // the flag for symmetry" behavior was wrong; it caused
            // psycopg3 to receive DataRows without a preceding T).
            let described_rd =
                !bytes.is_empty() && bytes[0] == crate::proto::BE_ROW_DESCRIPTION;
            if described_rd {
                if let Some(portal) = state.portals.get_mut(&name) {
                    portal.row_description_sent = true;
                }
            }
            ExtqOutcome::Bytes(bytes)
        }
        other => set_err(state, ExtqError::BadDescribeTarget { target: other }),
    }
}

/// SP-PG-EXTQ T4 helper — compute the RowDescription bytes for a
/// stored SQL string (if the SQL is a V1-renderable `SELECT * FROM
/// <table>` whose schema the engine can describe), or the NoData
/// bytes otherwise.
///
/// V1 mirrors the Simple Query path EXACTLY:
/// - `kessel_sql::select_star_table(sql)` returns `Some(table)` iff
///   the SQL is the V1 whole-row SELECT shape.
/// - `engine.describe_table(&table)` returns the column list for the
///   PG OID conversion + FieldMeta build.
///
/// If either step yields `None`, the SQL doesn't produce a result
/// set V1 can render — emit `NoData`. INSERT/UPDATE/DELETE/CREATE/
/// DROP all flow here; so does a SELECT-with-projection that V1
/// can't introspect (the client gets NoData, which is the correct
/// "no row metadata available pre-Execute" answer; at Execute time
/// V1 will emit ErrorResponse 0A000 for the projection — but that
/// happens at Execute, NOT at Describe, so the Describe path stays
/// honest about what it knows).
///
/// **Byte-equality invariant with Simple Query.** A
/// `SELECT * FROM t` whose Describe RowDescription bytes here MUST
/// equal the RowDescription bytes the Simple Query path emits for
/// the same SQL — clients (especially asyncpg + JDBC) compare these
/// across the Extended-Query Describe response and the Simple Query
/// response to detect a server bug. We get this for free by sharing
/// `response::encode_row_description` with the Simple Query path.
fn row_description_or_no_data_for_sql<E: EngineApply + ?Sized>(
    engine: &E,
    sql: &str,
) -> Vec<u8> {
    // Same trim shape `dispatch_query` uses: strip a trailing `;` and
    // surrounding whitespace so a client `SELECT * FROM t;` matches.
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let table_name = match kessel_sql::select_star_table(trimmed) {
        Some(t) => t,
        None => return response::encode_no_data(),
    };
    let cols = match engine.describe_table(&table_name) {
        Some(c) => c,
        None => return response::encode_no_data(),
    };
    let fields: Vec<FieldMeta> = cols
        .iter()
        .map(|c| FieldMeta {
            name: c.name.clone(),
            type_oid: field_kind_to_oid(c.kind),
        })
        .collect();
    crate::response::encode_row_description(&fields)
}

/// SP-PG-EXTQ T5 — real handler for the `E` Execute message.
///
/// Per spec §4 + §7.2 + PG §55.2.3, in order:
///
/// 1. **Portal lookup.** `state.portals.get(&portal_name)` → missing
///    is `UnknownPortal` → `34000 invalid_cursor_name`. Captures the
///    bound parameter values + `row_description_sent` flag + current
///    `exec_state` for the next steps.
///
/// 2. **Statement lookup.** The portal's `stmt_name` is resolved
///    against `state.statements`. Defensive `UnknownStatement` →
///    `26000` if missing (T3's Bind already validated, but a future
///    Close 'S' between Bind + Execute could violate the invariant).
///
/// 3. **Empty SQL.** Per spec §12 OQ #5 + PG §55.2.3: an empty
///    prepared SQL Executes as `EmptyQueryResponse`. The portal is
///    then `Exhausted { total: 0 }`.
///
/// 4. **Parameter substitution.** The portal's `param_values` are
///    substituted into the prepared SQL via
///    `substitute::substitute_text_format_params`. Errors map to
///    `SubstitutionFailed` → `08P01`.
///
/// 5. **First Execute or re-Execute?** Switch on the portal's
///    `exec_state`:
///    - `Pending`: this is the first Execute — call the existing
///      `dispatch::dispatch_query` to get the canonical Simple
///      Query byte sequence (RowDescription + DataRow* +
///      CommandComplete + RFQ). PARSE that byte stream to split out
///      the RowDescription / DataRow / CommandComplete / RFQ pieces;
///      strip the RFQ (Sync emits the RFQ on its own); buffer the
///      DataRows + CommandComplete tag into the portal's
///      `Buffered { rows, cursor: 0 }` state.
///    - `Buffered { rows, cursor }`: re-Execute — emit DataRows from
///      `rows[cursor..]` up to `max_rows`, advancing cursor; if more
///      rows remain after the batch, emit `PortalSuspended` instead
///      of `CommandComplete`. If the batch exhausts, emit
///      `CommandComplete` and transition to `Exhausted { total }`.
///    - `Exhausted { total }`: PG §55.2.3 — re-Execute on a drained
///      portal emits `CommandComplete("SELECT 0")` (we emit a bare
///      `SELECT 0` tag because the total is the previously-emitted
///      count, not 0). V1 ships the conservative shape — emit a
///      `CommandComplete` tag that re-uses the original
///      `SELECT <total>` form so the client sees a consistent count.
///
/// 6. **RowDescription suppression.** PG protocol: if Describe('P')
///    OR a previous Execute already emitted RowDescription for this
///    portal in the current Sync block, the new Execute MUST NOT
///    repeat it. Track via `portal.row_description_sent`. Reset on
///    Sync (T5 dispatch_sync drops unnamed portals; named portals
///    survive but we reset their flag too).
///
/// 7. **max_rows semantics** per spec §7.2:
///    - `max_rows == 0` → emit ALL remaining DataRows + the
///      original CommandComplete; portal transitions to Exhausted.
///    - `max_rows > 0` → emit min(remaining, max_rows) DataRows;
///      if more remain, emit PortalSuspended (`s [length=4]`) and
///      leave portal in Buffered { cursor: advanced }; if exactly
///      drained, emit the original CommandComplete + Exhausted.
///    - `max_rows < 0` → treat as 0 (PG §55.2.3 doesn't spec; V1
///      picks permissive).
///
/// 8. **Error-recovery side-effect** (spec §6): on ANY error path,
///    `state.error_state = true`. Same shape as the other dispatch
///    helpers.
fn dispatch_execute<E: EngineApply + ?Sized>(
    state: &mut SessionState,
    engine: &E,
    portal_name: String,
    max_rows: i32,
) -> ExtqOutcome {
    let set_err = |s: &mut SessionState, e: ExtqError| -> ExtqOutcome {
        s.error_state = true;
        ExtqOutcome::Failed(e)
    };

    // 1. Portal lookup. Capture what we need before the engine call
    //    so we don't hold the borrow.
    let (stmt_name, exec_state, row_description_sent) =
        match state.portals.get(&portal_name) {
            Some(p) => (
                p.stmt_name.clone(),
                p.exec_state.clone(),
                p.row_description_sent,
            ),
            None => return set_err(state, ExtqError::UnknownPortal { name: portal_name }),
        };

    // 2. Statement lookup. Defensive — Bind already validated.
    //    SP-PG-EXTQ-BIN T2 — param_oids are now used at Execute time
    //    too (passed to preprocess_params so the binary-format
    //    decoder can dispatch per-position).
    let (sql, param_oids) = match state.statements.get(&stmt_name) {
        Some(prep) => (prep.sql.clone(), prep.param_oids.clone()),
        None => {
            return set_err(
                state,
                ExtqError::UnknownStatement { name: stmt_name },
            );
        }
    };

    // 3. Empty SQL → EmptyQueryResponse (spec §12 OQ #5).
    if sql.trim().is_empty() {
        // Mark portal exhausted; emit a bare EmptyQueryResponse
        // frame.
        let bytes = crate::response::encode_empty_query_response();
        if let Some(portal) = state.portals.get_mut(&portal_name) {
            portal.exec_state = ExecState::Exhausted { total: 0 };
        }
        return ExtqOutcome::Bytes(bytes);
    }

    // Decide which path: first-time execute (substitute + dispatch
    // + buffer) vs re-Execute on Buffered/Exhausted.
    let buffered_rows: Vec<Vec<u8>>;
    let mut prelude: Vec<u8>; // RowDescription + ErrorResponse-if-any (NEVER includes RFQ)
    let command_complete_bytes: Vec<u8>;
    let cursor: usize;

    match exec_state {
        ExecState::Pending => {
            // 4. Parameter substitution. SP-PG-EXTQ-BIN T2 — the
            //    preprocess step decodes binary-format params into
            //    SQL-literal `PreparedParam`s using the per-position
            //    OIDs from Parse; the substitute step then walks the
            //    SQL with the format-aware renderer. Text-only Binds
            //    flow through unchanged (preprocess returns Text
            //    variants for every position).
            let (param_refs, formats): (Vec<Option<&[u8]>>, Vec<u16>) =
                match state.portals.get(&portal_name) {
                    Some(p) => (
                        p.param_values.iter().map(|v| v.as_deref()).collect(),
                        p.param_formats.clone(),
                    ),
                    None => {
                        return set_err(
                            state,
                            ExtqError::UnknownPortal { name: portal_name },
                        );
                    }
                };
            let prepared = match substitute::preprocess_params(&param_refs, &formats, &param_oids)
            {
                Ok(p) => p,
                Err(e) => {
                    let reason = match e {
                        substitute::SubstituteError::ZeroParamIndex => {
                            "SQL referenced \\$0; PG \\$N indices are 1-based"
                                .to_string()
                        }
                        substitute::SubstituteError::ParamIndexOutOfBounds {
                            index,
                            available,
                        } => format!(
                            "SQL referenced \\${index} but only {available} parameters were bound"
                        ),
                        substitute::SubstituteError::BinaryDecode { position, reason } => {
                            format!("binary parameter at position {pos}: {reason}", pos = position + 1)
                        }
                    };
                    return set_err(state, ExtqError::SubstitutionFailed { reason });
                }
            };
            let rewritten = match substitute::substitute_params(&sql, &prepared) {
                Ok(s) => s,
                Err(e) => {
                    let reason = match e {
                        substitute::SubstituteError::ZeroParamIndex => {
                            "SQL referenced \\$0; PG \\$N indices are 1-based"
                                .to_string()
                        }
                        substitute::SubstituteError::ParamIndexOutOfBounds {
                            index,
                            available,
                        } => format!(
                            "SQL referenced \\${index} but only {available} parameters were bound"
                        ),
                        substitute::SubstituteError::BinaryDecode { position, reason } => {
                            format!("binary parameter at position {pos}: {reason}", pos = position + 1)
                        }
                    };
                    return set_err(state, ExtqError::SubstitutionFailed { reason });
                }
            };

            // 5a. First-time Execute — run through the existing
            //     Simple Query pipeline + split the result.
            let dispatched = crate::dispatch::dispatch_query(&rewritten, engine);
            let split = split_dispatch_query_bytes(&dispatched);
            prelude = split.prelude;
            buffered_rows = split.data_rows;
            command_complete_bytes = split.command_complete;
            cursor = 0;

            // Persist into the portal for future re-Execute.
            if let Some(portal) = state.portals.get_mut(&portal_name) {
                portal.exec_state = ExecState::Buffered {
                    rows: buffered_rows.clone(),
                    cursor: 0,
                };
            }
        }
        ExecState::Buffered { rows, cursor: cur } => {
            // 5b. Re-Execute — page from the existing buffer; no
            //     prelude (RowDescription was emitted last time) and
            //     no need to re-substitute / re-dispatch.
            buffered_rows = rows;
            prelude = Vec::new();
            // command_complete_bytes will be rebuilt to use the
            // original tag — we lost it when buffering. V1 emits a
            // canonical `SELECT N` based on total buffered rows for
            // re-Executes (which matches PG's cursor semantics for
            // SELECT). For non-SELECT portals the first Execute
            // exhausts the portal anyway (the buffered_rows are
            // empty), so this path is SELECT-only in practice.
            command_complete_bytes = crate::response::encode_command_complete(
                &crate::response::select_tag(buffered_rows.len() as u64),
            );
            cursor = cur;
        }
        ExecState::Exhausted { total } => {
            // 5c. Re-Execute on a drained portal — emit a bare
            //     CommandComplete. No DataRows, no PortalSuspended.
            let cc = crate::response::encode_command_complete(
                &crate::response::select_tag(total),
            );
            return ExtqOutcome::Bytes(cc);
        }
    }

    // 6. Strip RowDescription from `prelude` if Describe('P') already
    //    emitted it.
    if row_description_sent {
        prelude = strip_leading_row_description(&prelude);
    }

    // If the dispatch_query result was actually an ErrorResponse
    // (the prelude starts with 'E'), the portal's first Execute
    // surfaces it. Mark error_state + return Failed-style: but our
    // Bytes path emits it verbatim and the caller sees the wire
    // bytes. V1 ships the simpler shape — emit the ErrorResponse
    // bytes and set error_state. The CommandComplete in this case
    // is empty (we'll skip it via empty checks).
    let prelude_is_error = !prelude.is_empty() && prelude[0] == b'E';
    if prelude_is_error {
        // The dispatch_query result was an error frame. Pass it
        // through; mark error_state. Drop the buffered state — the
        // portal isn't usable for a retry.
        if let Some(portal) = state.portals.get_mut(&portal_name) {
            portal.exec_state = ExecState::Exhausted { total: 0 };
        }
        state.error_state = true;
        // Emit just the ErrorResponse — Sync will emit RFQ.
        // dispatch_query's output ends with `Z 00 00 00 05 I` after
        // the error — split_dispatch_query_bytes already stripped
        // that — so `prelude` here is the full ErrorResponse(s).
        return ExtqOutcome::Bytes(prelude);
    }

    // 7. max_rows pagination on buffered_rows[cursor..].
    let max = if max_rows <= 0 { usize::MAX } else { max_rows as usize };
    let available = buffered_rows.len().saturating_sub(cursor);
    let emit = available.min(max);
    let mut out = Vec::with_capacity(prelude.len() + emit * 32 + command_complete_bytes.len());
    out.extend_from_slice(&prelude);
    for i in cursor..cursor + emit {
        out.extend_from_slice(&buffered_rows[i]);
    }
    let new_cursor = cursor + emit;

    let more_remain = new_cursor < buffered_rows.len();
    if more_remain {
        // PortalSuspended instead of CommandComplete.
        out.extend_from_slice(&response::encode_portal_suspended());
        if let Some(portal) = state.portals.get_mut(&portal_name) {
            portal.exec_state = ExecState::Buffered {
                rows: buffered_rows,
                cursor: new_cursor,
            };
            // We DID emit RowDescription on this Execute (when prelude
            // was non-empty after the suppression check) — mark the
            // flag so the NEXT Execute doesn't repeat it.
            if !row_description_sent && !prelude.is_empty() && prelude[0] == b'T' {
                portal.row_description_sent = true;
            }
        }
    } else {
        // Drained — CommandComplete + portal Exhausted.
        out.extend_from_slice(&command_complete_bytes);
        if let Some(portal) = state.portals.get_mut(&portal_name) {
            portal.exec_state = ExecState::Exhausted {
                total: buffered_rows.len() as u64,
            };
            if !row_description_sent && !prelude.is_empty() && prelude[0] == b'T' {
                portal.row_description_sent = true;
            }
        }
    }
    ExtqOutcome::Bytes(out)
}

/// SP-PG-EXTQ T5 — split the byte stream returned by
/// `dispatch::dispatch_query` into:
///
/// - `prelude` — everything BEFORE the first `D` DataRow tag (i.e.
///   RowDescription, OR an ErrorResponse if dispatch_query
///   short-circuited to an error). For the EmptyQueryResponse path
///   the prelude is `EmptyQueryResponse` itself and there are no
///   DataRows / CommandComplete in the strict sense — the splitter
///   handles that case by emitting an empty `data_rows` + empty
///   `command_complete`.
/// - `data_rows` — the individual `D` frames as separate `Vec<u8>`,
///   each one a complete `D [length:4 BE] [body]` frame ready to
///   write to the wire.
/// - `command_complete` — the trailing `C` CommandComplete frame
///   (or `I` EmptyQueryResponse frame, which we route through
///   `command_complete` for symmetry).
///
/// The trailing `Z` ReadyForQuery frame (always 6 bytes) is STRIPPED
/// — `dispatch::dispatch_query` always appends one, but the Extended
/// Query path emits RFQ only on Sync.
///
/// **Frame walker.** Walks the byte stream tag-by-tag using the PG
/// 5-byte frame header (`tag:1 length:4 BE`). Length includes the
/// length field itself but NOT the tag. Stops on RFQ (the 'Z' frame).
struct SplitResult {
    prelude: Vec<u8>,
    data_rows: Vec<Vec<u8>>,
    command_complete: Vec<u8>,
}

fn split_dispatch_query_bytes(bytes: &[u8]) -> SplitResult {
    let mut prelude = Vec::new();
    let mut data_rows: Vec<Vec<u8>> = Vec::new();
    let mut command_complete = Vec::new();
    let mut i = 0usize;
    while i + 5 <= bytes.len() {
        let tag = bytes[i];
        let len = u32::from_be_bytes([
            bytes[i + 1],
            bytes[i + 2],
            bytes[i + 3],
            bytes[i + 4],
        ]) as usize;
        // Frame total = 1 byte tag + len bytes (len includes itself).
        let frame_total = 1 + len;
        if i + frame_total > bytes.len() {
            // Malformed — bail; caller treats as prelude.
            break;
        }
        let frame = &bytes[i..i + frame_total];
        match tag {
            b'Z' => {
                // ReadyForQuery — stop here; do not include in any
                // output (Sync emits its own RFQ).
                break;
            }
            b'D' => {
                data_rows.push(frame.to_vec());
            }
            b'C' | b'I' => {
                // CommandComplete or EmptyQueryResponse — terminal
                // success marker.
                command_complete.extend_from_slice(frame);
            }
            _ => {
                // RowDescription ('T'), ErrorResponse ('E'),
                // NoticeResponse ('N'), or any other prelude frame.
                prelude.extend_from_slice(frame);
            }
        }
        i += frame_total;
    }
    SplitResult {
        prelude,
        data_rows,
        command_complete,
    }
}

/// If the `prelude` bytes start with a `T` RowDescription frame,
/// strip it (returning the bytes AFTER the RowDescription). Otherwise
/// return the prelude unchanged. Used by `dispatch_execute` to honor
/// the PG protocol invariant that RowDescription is emitted EXACTLY
/// ONCE per portal per Sync block — if Describe('P') already emitted
/// it, Execute must not repeat it.
fn strip_leading_row_description(prelude: &[u8]) -> Vec<u8> {
    if prelude.len() >= 5 && prelude[0] == b'T' {
        let len = u32::from_be_bytes([prelude[1], prelude[2], prelude[3], prelude[4]]) as usize;
        let frame_total = 1 + len;
        if prelude.len() >= frame_total {
            return prelude[frame_total..].to_vec();
        }
    }
    prelude.to_vec()
}

/// SP-PG-EXTQ T5 — real handler for the `S` Sync message.
///
/// Per spec §6 + PG §55.2.3:
///
/// 1. **Emit ReadyForQuery('I').** V1 has no transaction-block
///    awareness (V2 SP-PG-TX would conditionally emit `'T'` or `'E'`).
///    Always `'I'` (idle).
/// 2. **Reset `error_state = false`.** Subsequent extq messages will
///    process normally again.
/// 3. **Drop unnamed `""` portal** per PG §55.2.3 — implicit-tx
///    commit at Sync boundary drops the volatile portal. The unnamed
///    statement is KEPT (PG keeps unnamed-statement across Sync but
///    drops unnamed-portal; both for V1 are bounded by connection
///    lifetime anyway).
/// 4. **Reset `row_description_sent` on every portal** — within ONE
///    Sync block, Describe('P') + Execute emit T at most once; after
///    Sync, a new Describe + Execute cycle restarts and T can be
///    emitted again. (Named portals that survive Sync get this reset
///    so their next Sync-block flow works.)
///
/// Returns `Bytes(ReadyForQuery)` so the caller writes the 6-byte
/// envelope verbatim. We use the `Bytes` variant rather than
/// `SyncCompleted` because `Bytes` is the standard wire-bytes path
/// and the run_session loop already handles it.
fn dispatch_sync(state: &mut SessionState) -> ExtqOutcome {
    // 1. RFQ('I').
    let bytes = crate::response::encode_ready_for_query(b'I');
    // 2. Reset error_state.
    state.error_state = false;
    // 3. Drop unnamed portal.
    state.portals.remove("");
    // 4. Reset row_description_sent on surviving portals.
    for p in state.portals.values_mut() {
        p.row_description_sent = false;
    }
    ExtqOutcome::Bytes(bytes)
}

/// SP-PG-EXTQ T6 — real handler for the `C` Close message.
///
/// Per spec §4 + PG §55.2.3:
///
/// - `'S'` (statement) — drop the named statement from
///   `state.statements`. **Silently no-ops** if the name doesn't
///   exist (PG §55.2.3: "It is not an error to issue Close against
///   a nonexistent statement or portal name"). Implicitly drops any
///   portals that depend on the statement is NOT done by PG — those
///   portals stay alive until the next Execute (where they'd fail
///   the defensive `UnknownStatement` lookup) or explicit Close.
///   V1 mirrors PG exactly.
/// - `'P'` (portal) — drop the named portal from `state.portals`.
///   Same silent no-op semantics if the name doesn't exist.
/// - Anything else — `BadDescribeTarget { target }` → `08P01
///   protocol_violation` (the same SQLSTATE Describe uses for bad
///   target bytes; spec §4 treats Close + Describe identically for
///   target-byte validation).
///
/// Always emits `CloseComplete` ('3') on success (5-byte envelope
/// `3 00 00 00 04` from the T1-byte-locked
/// `response::encode_close_complete`). Per PG §55.2.3 CloseComplete
/// is emitted EVEN when the Close was a no-op — the client uses
/// CloseComplete as a sync-point confirmation that the server saw
/// the Close.
///
/// **Error-recovery side-effect** (spec §6): on the bad-target error
/// path, set `state.error_state = true` BEFORE returning. The
/// silent no-op for missing-name does NOT set error_state (it's not
/// an error per PG §55.2.3).
fn dispatch_close(state: &mut SessionState, target: u8, name: String) -> ExtqOutcome {
    match target {
        crate::proto::DESCRIBE_TARGET_STATEMENT => {
            // PG §55.2.3: silent no-op if missing.
            state.statements.remove(&name);
            ExtqOutcome::Bytes(response::encode_close_complete())
        }
        crate::proto::DESCRIBE_TARGET_PORTAL => {
            state.portals.remove(&name);
            ExtqOutcome::Bytes(response::encode_close_complete())
        }
        other => {
            state.error_state = true;
            ExtqOutcome::Failed(ExtqError::BadDescribeTarget { target: other })
        }
    }
}

/// SP-PG-EXTQ T6 — real handler for the `H` Flush message.
///
/// Per spec §4 + PG §55.2.3:
///
/// Flush has no associated response. The server simply pushes any
/// pending pipelined output to the wire. The `server::run_session`
/// loop translates the `ExtqOutcome::Flush` return into a
/// `writer.flush()` call WITHOUT writing any bytes.
///
/// Flush does NOT touch `error_state` — clients use Flush to drain
/// pipelined output mid-pipeline (e.g. asyncpg + JDBC do this after
/// every Describe), and the protocol spec is explicit that only
/// Sync clears error_state.
///
/// No state mutation; no SessionState borrow required.
fn dispatch_flush() -> ExtqOutcome {
    ExtqOutcome::Flush
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::PgColumn;
    use crate::proto::*;
    use kessel_catalog::FieldKind;
    use kessel_proto::OpResult;

    // ───────────────────────────────────────────────────────────────────
    // Test engines.
    // ───────────────────────────────────────────────────────────────────

    /// A minimal engine that returns `None` for every `describe_table`
    /// lookup. Used by T1/T2/T3 dispatcher KATs that don't exercise the
    /// engine boundary — Describe (T4) is the first dispatcher that
    /// actually consults `describe_table`, so non-Describe KATs only
    /// need the engine to satisfy the type signature.
    struct NoSchemaEngine;
    impl EngineApply for NoSchemaEngine {
        fn apply_sql(&self, _sql: &str) -> OpResult {
            OpResult::SchemaError("not used".into())
        }
        fn describe_table(&self, _name: &str) -> Option<Vec<PgColumn>> {
            None
        }
    }

    /// A canned-schema engine: returns a fixed two-column shape for
    /// table "t" (id i64 NOT NULL + name char(64) NULL) — matches the
    /// classic minimal SELECT-renderable shape — and `None` for every
    /// other table name. Used by T4 KATs to verify the Describe 'S' /
    /// 'P' RowDescription bytes match the Simple Query path's bytes
    /// for `SELECT * FROM t`.
    struct CannedTwoColEngine;
    impl EngineApply for CannedTwoColEngine {
        fn apply_sql(&self, _sql: &str) -> OpResult {
            OpResult::SchemaError("not used".into())
        }
        fn describe_table(&self, name: &str) -> Option<Vec<PgColumn>> {
            if name == "t" {
                Some(vec![
                    PgColumn {
                        name: "id".into(),
                        kind: FieldKind::I64,
                        nullable: false,
                    },
                    PgColumn {
                        name: "name".into(),
                        kind: FieldKind::Char(64),
                        nullable: true,
                    },
                ])
            } else {
                None
            }
        }
    }

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

    /// SP-PG-EXTQ T6 — **ZERO tags remain NotYetImplemented**. All
    /// seven frontend Extended Query tags (Parse / Bind / Describe /
    /// Execute / Sync / Close / Flush) now dispatch through REAL
    /// handlers — the T5 "two remaining" KAT flips to a zero-NYI
    /// lock. The flipped invariant says: a fresh `SessionState` +
    /// every reachable `ExtqMessage` variant goes through a real
    /// dispatcher (Bytes/Skipped/Flush outcome — never
    /// `NotYetImplemented`).
    ///
    /// Headline: SP-PG-EXTQ V1 message set is COMPLETE. T7 + T8 are
    /// hardening (real ORM round-trip + Sync state-machine polish).
    #[test]
    fn t6_try_dispatch_no_tag_returns_not_yet_implemented_v1_complete() {
        let mut state = SessionState::new();
        // Seed a stmt + portal so the Describe + Execute + Close
        // arms have real targets to resolve. (Parse + Bind have no
        // pre-state requirement; Sync + Flush have no resolution
        // step.)
        seed_stmt_with_sql(&mut state, "ps", "SELECT * FROM t", vec![]);
        try_dispatch_extq(
            &mut state,
            &CannedTwoColEngine,
            proto::ExtqMessage::Bind {
                portal: "pt".to_string(),
                stmt: "ps".to_string(),
                param_formats: vec![],
                param_values: vec![],
                result_formats: vec![],
            },
        );
        let cases: Vec<proto::ExtqMessage> = vec![
            proto::ExtqMessage::Parse {
                name: "fresh".to_string(),
                sql: "SELECT 1".to_string(),
                param_oids: vec![],
            },
            proto::ExtqMessage::Bind {
                portal: "freshp".to_string(),
                stmt: "ps".to_string(),
                param_formats: vec![],
                param_values: vec![],
                result_formats: vec![],
            },
            proto::ExtqMessage::Describe {
                target: DESCRIBE_TARGET_STATEMENT,
                name: "ps".to_string(),
            },
            proto::ExtqMessage::Execute {
                portal: "pt".to_string(),
                max_rows: 0,
            },
            proto::ExtqMessage::Sync,
            proto::ExtqMessage::Close {
                target: DESCRIBE_TARGET_STATEMENT,
                name: "ps".to_string(),
            },
            proto::ExtqMessage::Flush,
        ];
        for msg in cases {
            let label = format!("{msg:?}");
            match try_dispatch_extq(&mut state, &CannedTwoColEngine, msg) {
                ExtqOutcome::Failed(ExtqError::NotYetImplemented { tag }) => {
                    panic!(
                        "tag 0x{tag:02X} ('{c}') for {label} returned NotYetImplemented \
                         — SP-PG-EXTQ V1 should no longer have any NYI arms",
                        c = tag as char,
                    );
                }
                _ => {}
            }
        }
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, m1) {
            ExtqOutcome::Bytes(_) => {}
            other => panic!("first Parse should succeed, got {other:?}"),
        }
        // Second Parse with the SAME name + DIFFERENT SQL → 42P05.
        let m2 = proto::ExtqMessage::Parse {
            name: "pst1".to_string(),
            sql: "SELECT 2".to_string(),
            param_oids: vec![],
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, m2) {
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
            try_dispatch_extq(&mut state, &NoSchemaEngine, m1),
            ExtqOutcome::Bytes(_)
        ));
        // Second Parse: SELECT 2 into "" — replaces silently.
        let m2 = proto::ExtqMessage::Parse {
            name: String::new(),
            sql: "SELECT 2".to_string(),
            param_oids: vec![25 /* PG_TYPE_TEXT */],
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, m2) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, at_cap) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, over_cap) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, overwrite) {
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
        seed_stmt_with_sql(
            state,
            name,
            &format!("SELECT 1 -- seeded under {name}"),
            param_oids,
        );
    }

    /// Like `seed_stmt` but the caller picks the SQL — used by T4
    /// KATs that need a Parse-then-Describe round trip on a specific
    /// SQL shape (e.g. `SELECT * FROM t` for the RowDescription path,
    /// or `INSERT INTO t VALUES (1)` for the NoData path).
    fn seed_stmt_with_sql(
        state: &mut SessionState,
        name: &str,
        sql: &str,
        param_oids: Vec<u32>,
    ) {
        let msg = proto::ExtqMessage::Parse {
            name: name.to_string(),
            sql: sql.to_string(),
            param_oids,
        };
        match try_dispatch_extq(state, &NoSchemaEngine, msg) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'2', 0, 0, 0, 4]),
            other => panic!("expected BindComplete, got {other:?}"),
        }
        assert_eq!(state.portal_count(), 1);
        assert!(!state.in_error_state());
    }

    /// SP-PG-EXTQ-BIN T2 FLIP — was T3
    /// `t3_dispatch_bind_binary_format_per_position_rejected`. After
    /// T2 the binary-format Bind path no longer outright rejects
    /// every binary param — it dispatches per-position based on the
    /// Parse-time type OID. The original test bound binary WITHOUT
    /// supplying type-OID hints, which now hits the
    /// `BinaryFormatRequiresTypeOidHint` arm at the first binary
    /// position. The OID-with-supported-type happy path is locked
    /// by the `t2bin_*` KATs below.
    #[test]
    fn t3bin_dispatch_bind_per_position_binary_without_oid_hint_rejected() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "", vec![]);
        let msg = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: String::new(),
            param_formats: vec![FORMAT_CODE_TEXT, FORMAT_CODE_BINARY],
            param_values: vec![Some(b"a".to_vec()), Some(b"b".to_vec())],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
            ExtqOutcome::Failed(ExtqError::BinaryFormatRequiresTypeOidHint { position }) => {
                assert_eq!(position, 1);
            }
            other => panic!("expected BinaryFormatRequiresTypeOidHint, got {other:?}"),
        }
        assert_eq!(state.portal_count(), 0);
        assert!(state.in_error_state());
    }

    /// SP-PG-EXTQ-BIN T2 FLIP — was T3
    /// `t3_dispatch_bind_single_binary_format_applies_to_all`. Same
    /// rationale as the per-position FLIP above: "1 format code = all
    /// positions binary" without OID hints now hits the missing-OID
    /// branch at position 0.
    #[test]
    fn t3bin_dispatch_bind_single_binary_without_oid_hint_rejected() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "", vec![]);
        let msg = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: String::new(),
            param_formats: vec![FORMAT_CODE_BINARY],
            param_values: vec![Some(b"a".to_vec()), Some(b"b".to_vec())],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
            ExtqOutcome::Failed(ExtqError::BinaryFormatRequiresTypeOidHint { position }) => {
                assert_eq!(position, 0);
            }
            other => panic!("expected BinaryFormatRequiresTypeOidHint, got {other:?}"),
        }
        assert!(state.in_error_state());
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-EXTQ-BIN T2 KATs — binary-format admission @ Bind + per-
    // position decode @ Execute. Lifts the V1 "binary always rejects"
    // stance for the V1-supported PG types (INT2/INT4/INT8/FLOAT4/
    // FLOAT8/BOOL/TEXT/VARCHAR/BYTEA/TIMESTAMPTZ). NUMERIC + unknown
    // OIDs still reject (with precise V2-follow-up arc names).
    // ───────────────────────────────────────────────────────────────────

    /// SP-PG-EXTQ-BIN T2 — a Bind with INT8 binary format + an INT8
    /// OID hint at the same position is ACCEPTED. The portal stores
    /// the raw binary bytes; Execute decodes them via
    /// `decode_binary_param` (locked by separate Execute KATs).
    #[test]
    fn t2bin_dispatch_bind_int8_binary_with_oid_hint_accepted() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "pi8", vec![PG_TYPE_INT8]);
        // Wire bytes for INT8 binary 100.
        let int8_bytes = 100i64.to_be_bytes().to_vec();
        let msg = proto::ExtqMessage::Bind {
            portal: "p".to_string(),
            stmt: "pi8".to_string(),
            param_formats: vec![FORMAT_CODE_BINARY],
            param_values: vec![Some(int8_bytes.clone())],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'2', 0, 0, 0, 4]),
            other => panic!("expected BindComplete, got {other:?}"),
        }
        assert_eq!(state.portal_count(), 1);
        let p = state.get_portal("p").expect("portal stored");
        assert_eq!(p.param_values, vec![Some(int8_bytes)]);
        assert_eq!(p.param_formats, vec![FORMAT_CODE_BINARY]);
        assert!(!state.in_error_state());
    }

    /// SP-PG-EXTQ-BIN T2 — every supported PG type accepts the binary
    /// format with a matching OID hint. The decoder happy path is
    /// locked separately in `substitute.rs`; this KAT only verifies
    /// the Bind admission check accepts the same set.
    #[test]
    fn t2bin_dispatch_bind_every_supported_oid_accepts_binary() {
        for (oid, dummy_bytes) in [
            (PG_TYPE_BOOL, vec![0x01u8]),
            (PG_TYPE_BYTEA, vec![0xDEu8, 0xAD]),
            (PG_TYPE_INT2, vec![0x00, 0x2A]),
            (PG_TYPE_INT4, vec![0xFF, 0xFF, 0xFF, 0xFF]),
            (PG_TYPE_INT8, vec![0; 8]),
            (PG_TYPE_FLOAT4, vec![0x3F, 0xC0, 0x00, 0x00]), // 1.5
            (PG_TYPE_FLOAT8, vec![0; 8]),
            (PG_TYPE_TEXT, b"hi".to_vec()),
            (PG_TYPE_VARCHAR, b"vc".to_vec()),
            (PG_TYPE_TIMESTAMPTZ, vec![0; 8]),
        ] {
            let mut state = SessionState::new();
            seed_stmt(&mut state, "ps", vec![oid]);
            let msg = proto::ExtqMessage::Bind {
                portal: "p".to_string(),
                stmt: "ps".to_string(),
                param_formats: vec![FORMAT_CODE_BINARY],
                param_values: vec![Some(dummy_bytes.clone())],
                result_formats: vec![],
            };
            match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
                ExtqOutcome::Bytes(b) => assert_eq!(
                    b,
                    vec![b'2', 0, 0, 0, 4],
                    "OID {oid} should accept binary"
                ),
                other => panic!(
                    "expected BindComplete for OID {oid}, got {other:?}"
                ),
            }
        }
    }

    /// SP-PG-EXTQ-BIN T2 — NUMERIC binary at Bind is rejected with
    /// the precise `SP-PG-EXTQ-BIN-NUMERIC` follow-up arc name so
    /// operators can grep for the gap.
    #[test]
    fn t2bin_dispatch_bind_numeric_binary_rejected_with_followup_arc() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "pn", vec![PG_TYPE_NUMERIC]);
        let msg = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "pn".to_string(),
            param_formats: vec![FORMAT_CODE_BINARY],
            param_values: vec![Some(vec![0; 8])],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
            ExtqOutcome::Failed(ExtqError::BinaryFormatUnsupportedForType {
                position,
                type_oid,
                arc,
            }) => {
                assert_eq!(position, 0);
                assert_eq!(type_oid, PG_TYPE_NUMERIC);
                assert_eq!(arc, "SP-PG-EXTQ-BIN-NUMERIC");
            }
            other => panic!("expected BinaryFormatUnsupportedForType, got {other:?}"),
        }
        assert_eq!(state.portal_count(), 0);
        assert!(state.in_error_state());
    }

    /// SP-PG-EXTQ-BIN T2 — unknown OIDs (e.g. JSONB 3802) reject with
    /// the generic `SP-PG-EXTQ-BIN-EXTRA` follow-up name.
    #[test]
    fn t2bin_dispatch_bind_unknown_oid_binary_rejected_with_extra_arc() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "pu", vec![3802 /* JSONB */]);
        let msg = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "pu".to_string(),
            param_formats: vec![FORMAT_CODE_BINARY],
            param_values: vec![Some(vec![0; 4])],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
            ExtqOutcome::Failed(ExtqError::BinaryFormatUnsupportedForType {
                position: _,
                type_oid,
                arc,
            }) => {
                assert_eq!(type_oid, 3802);
                assert_eq!(arc, "SP-PG-EXTQ-BIN-EXTRA");
            }
            other => panic!("expected BinaryFormatUnsupportedForType, got {other:?}"),
        }
        assert!(state.in_error_state());
    }

    /// SP-PG-EXTQ-BIN T2 — a mix of text + binary formats with proper
    /// OID hints is accepted. The portal stores both formats verbatim;
    /// Execute dispatches per-position.
    #[test]
    fn t2bin_dispatch_bind_mixed_text_and_binary_formats_accepted() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "pmix", vec![PG_TYPE_INT8, PG_TYPE_TEXT]);
        let int8_bytes = 7i64.to_be_bytes().to_vec();
        let msg = proto::ExtqMessage::Bind {
            portal: "pm".to_string(),
            stmt: "pmix".to_string(),
            param_formats: vec![FORMAT_CODE_BINARY, FORMAT_CODE_TEXT],
            param_values: vec![Some(int8_bytes), Some(b"hi".to_vec())],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'2', 0, 0, 0, 4]),
            other => panic!("expected BindComplete, got {other:?}"),
        }
        assert!(!state.in_error_state());
    }

    /// SP-PG-EXTQ-BIN T2 — single binary format code applied to all
    /// positions is accepted iff EVERY position has a supported OID.
    #[test]
    fn t2bin_dispatch_bind_single_binary_with_all_supported_oids_accepted() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "p2", vec![PG_TYPE_INT8, PG_TYPE_INT4]);
        let int8_bytes = 1i64.to_be_bytes().to_vec();
        let int4_bytes = 2i32.to_be_bytes().to_vec();
        let msg = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "p2".to_string(),
            param_formats: vec![FORMAT_CODE_BINARY],
            param_values: vec![Some(int8_bytes), Some(int4_bytes)],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'2', 0, 0, 0, 4]),
            other => panic!("expected BindComplete, got {other:?}"),
        }
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, first) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, second) {
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
            try_dispatch_extq(&mut state, &NoSchemaEngine, first),
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, second) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
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
                    row_description_sent: false,
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, at_cap) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, over_cap) {
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
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
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
        let pc = try_dispatch_extq(&mut state, &NoSchemaEngine, parse);
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
        let bc = try_dispatch_extq(&mut state, &NoSchemaEngine, bind);
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

    // ───────────────────────────────────────────────────────────────────
    // T4 KATs — real Describe handler. Locks every spec §4 + §6 + §9
    // invariant the run_session integration depends on.
    //
    // Two flavors:
    //   - 'S' (statement) — emit ParameterDescription + RowDescription
    //     or NoData.
    //   - 'P' (portal) — emit RowDescription or NoData (NO ParameterDescription).
    // ───────────────────────────────────────────────────────────────────

    /// Spec §4 + §9: Describe 'S' on a SELECT * FROM <table> statement
    /// emits ParameterDescription (echoing Parse's OID hints) followed
    /// by RowDescription whose column metadata matches
    /// `engine.describe_table` for that table. Byte-locked: the
    /// RowDescription bytes here are identical to what the Simple
    /// Query path emits for the same SQL — sharing the
    /// `response::encode_row_description` encoder is what gives us
    /// that equality.
    #[test]
    fn t4_dispatch_describe_statement_select_emits_param_desc_and_row_desc() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "ps1", "SELECT * FROM t", vec![23 /* int4 */]);
        let msg = proto::ExtqMessage::Describe {
            target: DESCRIBE_TARGET_STATEMENT,
            name: "ps1".to_string(),
        };
        match try_dispatch_extq(&mut state, &CannedTwoColEngine, msg) {
            ExtqOutcome::Bytes(bytes) => {
                // ParameterDescription comes FIRST: `t [length] [count:i16] [oids:i32*]`.
                assert_eq!(bytes[0], b't', "first byte is ParameterDescription tag");
                // Echo of param_oids = [23] → 11 bytes total:
                //   `t [length=10] [count=1] [oid=23]`.
                let expected_pd = response::encode_parameter_description(&[23]);
                assert!(
                    bytes.starts_with(&expected_pd),
                    "first {} bytes must be ParameterDescription({{23}})",
                    expected_pd.len()
                );
                // RowDescription follows. Byte-equal to what the
                // Simple Query path emits.
                let rd_expected = crate::response::encode_row_description(&[
                    FieldMeta {
                        name: "id".into(),
                        type_oid: field_kind_to_oid(FieldKind::I64),
                    },
                    FieldMeta {
                        name: "name".into(),
                        type_oid: field_kind_to_oid(FieldKind::Char(64)),
                    },
                ]);
                assert_eq!(&bytes[expected_pd.len()..], rd_expected.as_slice());
            }
            other => panic!("expected Bytes(ParameterDescription + RowDescription), got {other:?}"),
        }
        // Describe is read-only — no state mutation, no error.
        assert!(!state.in_error_state());
        assert_eq!(state.statement_count(), 1);
        assert_eq!(state.portal_count(), 0);
    }

    /// Spec §4: Describe 'S' on a NON-SELECT statement (INSERT here)
    /// emits ParameterDescription + NoData (`n`). The PG client uses
    /// NoData to short-circuit row-decoding setup.
    #[test]
    fn t4_dispatch_describe_statement_non_select_emits_param_desc_and_no_data() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(
            &mut state,
            "ins",
            "INSERT INTO t (id) VALUES ($1)",
            vec![23 /* int4 */],
        );
        let msg = proto::ExtqMessage::Describe {
            target: DESCRIBE_TARGET_STATEMENT,
            name: "ins".to_string(),
        };
        match try_dispatch_extq(&mut state, &CannedTwoColEngine, msg) {
            ExtqOutcome::Bytes(bytes) => {
                let expected_pd = response::encode_parameter_description(&[23]);
                assert!(bytes.starts_with(&expected_pd));
                // NoData envelope (`n [length=4]`) immediately follows.
                let expected_nd = response::encode_no_data();
                assert_eq!(&bytes[expected_pd.len()..], expected_nd.as_slice());
            }
            other => panic!("expected Bytes(PD + NoData), got {other:?}"),
        }
        assert!(!state.in_error_state());
    }

    /// Spec §4: Describe 'S' on a statement with NO Parse-time OID
    /// hints emits ParameterDescription with count=0 (the 7-byte
    /// envelope `t [length=6] [count=0]`).
    #[test]
    fn t4_dispatch_describe_statement_with_no_oid_hints_emits_empty_param_desc() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "ps_noo", "SELECT * FROM t", vec![]);
        let msg = proto::ExtqMessage::Describe {
            target: DESCRIBE_TARGET_STATEMENT,
            name: "ps_noo".to_string(),
        };
        match try_dispatch_extq(&mut state, &CannedTwoColEngine, msg) {
            ExtqOutcome::Bytes(bytes) => {
                // ParameterDescription with count=0 — byte-locked
                // 7-byte envelope.
                let empty_pd = response::encode_parameter_description(&[]);
                assert_eq!(empty_pd.len(), 7);
                assert!(bytes.starts_with(&empty_pd));
            }
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    /// Spec §4 / PG §55.2.3: Describe 'S' on a non-existent statement
    /// returns `UnknownStatement` → `26000 invalid_sql_statement_name`.
    /// Error_state is set so subsequent pipelined messages until Sync
    /// are skipped (spec §6).
    #[test]
    fn t4_dispatch_describe_statement_missing_returns_26000() {
        let mut state = SessionState::new();
        let msg = proto::ExtqMessage::Describe {
            target: DESCRIBE_TARGET_STATEMENT,
            name: "ghost".to_string(),
        };
        match try_dispatch_extq(&mut state, &CannedTwoColEngine, msg) {
            ExtqOutcome::Failed(ExtqError::UnknownStatement { name }) => {
                assert_eq!(name, "ghost");
            }
            other => panic!("expected UnknownStatement, got {other:?}"),
        }
        assert!(state.in_error_state());
    }

    /// Spec §4: Describe 'P' on a SELECT portal emits ONLY
    /// RowDescription — NO ParameterDescription (portals don't carry
    /// parameter metadata because Bind has frozen the values). Locks
    /// the spec §4 + PG §55.2.3 portal-vs-statement asymmetry.
    #[test]
    fn t4_dispatch_describe_portal_select_emits_row_desc_only() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "ps1", "SELECT * FROM t", vec![]);
        // Bind a portal "pt1" → stmt "ps1".
        let bind = proto::ExtqMessage::Bind {
            portal: "pt1".to_string(),
            stmt: "ps1".to_string(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, &CannedTwoColEngine, bind) {
            ExtqOutcome::Bytes(_) => {}
            other => panic!("seed Bind should succeed, got {other:?}"),
        }
        // Describe 'P' "pt1".
        let desc = proto::ExtqMessage::Describe {
            target: DESCRIBE_TARGET_PORTAL,
            name: "pt1".to_string(),
        };
        match try_dispatch_extq(&mut state, &CannedTwoColEngine, desc) {
            ExtqOutcome::Bytes(bytes) => {
                // First byte is RowDescription tag 'T' — NOT
                // ParameterDescription 't'. PG protocol §55.2.3.
                assert_eq!(
                    bytes[0], b'T',
                    "Describe 'P' must NOT emit ParameterDescription — first byte should be RowDescription 'T'"
                );
                // Byte-equal to the Simple-Query RowDescription for
                // the same SQL.
                let rd_expected = crate::response::encode_row_description(&[
                    FieldMeta {
                        name: "id".into(),
                        type_oid: field_kind_to_oid(FieldKind::I64),
                    },
                    FieldMeta {
                        name: "name".into(),
                        type_oid: field_kind_to_oid(FieldKind::Char(64)),
                    },
                ]);
                assert_eq!(bytes, rd_expected);
            }
            other => panic!("expected Bytes(RowDescription), got {other:?}"),
        }
        assert!(!state.in_error_state());
    }

    /// Spec §4: Describe 'P' on a non-SELECT portal emits NoData
    /// (`n`) — NO ParameterDescription, NO RowDescription. 5-byte
    /// envelope.
    #[test]
    fn t4_dispatch_describe_portal_non_select_emits_no_data() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "ins", "INSERT INTO t (id) VALUES ($1)", vec![]);
        // Bind a portal "pi" → stmt "ins".
        let bind = proto::ExtqMessage::Bind {
            portal: "pi".to_string(),
            stmt: "ins".to_string(),
            param_formats: vec![],
            param_values: vec![Some(b"1".to_vec())],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, &CannedTwoColEngine, bind) {
            ExtqOutcome::Bytes(_) => {}
            other => panic!("seed Bind should succeed, got {other:?}"),
        }
        let desc = proto::ExtqMessage::Describe {
            target: DESCRIBE_TARGET_PORTAL,
            name: "pi".to_string(),
        };
        match try_dispatch_extq(&mut state, &CannedTwoColEngine, desc) {
            ExtqOutcome::Bytes(bytes) => {
                assert_eq!(bytes, response::encode_no_data());
                assert_eq!(bytes.len(), 5);
            }
            other => panic!("expected Bytes(NoData), got {other:?}"),
        }
        assert!(!state.in_error_state());
    }

    /// Spec §4 / PG §55.2.3: Describe 'P' on a non-existent portal
    /// returns `UnknownPortal` → `34000 invalid_cursor_name`.
    /// Error_state is set per spec §6.
    #[test]
    fn t4_dispatch_describe_portal_missing_returns_34000() {
        let mut state = SessionState::new();
        let msg = proto::ExtqMessage::Describe {
            target: DESCRIBE_TARGET_PORTAL,
            name: "ghost".to_string(),
        };
        match try_dispatch_extq(&mut state, &CannedTwoColEngine, msg) {
            ExtqOutcome::Failed(ExtqError::UnknownPortal { name }) => {
                assert_eq!(name, "ghost");
            }
            other => panic!("expected UnknownPortal, got {other:?}"),
        }
        assert!(state.in_error_state());
    }

    /// Spec §6: when the dispatcher is in `error_state == true`, a
    /// Describe message is silently dropped (`ExtqOutcome::Skipped`)
    /// WITHOUT processing. The error_state flag is NOT cleared (only
    /// Sync clears it — T7).
    #[test]
    fn t4_dispatch_describe_in_error_state_returns_skipped_without_processing() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "ps1", "SELECT * FROM t", vec![]);
        state.set_error_state(true);
        let msg = proto::ExtqMessage::Describe {
            target: DESCRIBE_TARGET_STATEMENT,
            name: "ps1".to_string(),
        };
        match try_dispatch_extq(&mut state, &CannedTwoColEngine, msg) {
            ExtqOutcome::Skipped => {}
            other => panic!("expected Skipped, got {other:?}"),
        }
        assert!(state.in_error_state());
    }

    /// Spec §4: a Describe with an UNKNOWN target byte (decoder
    /// catches this, but the dispatcher locks it as a defensive
    /// rejection in case a future direct constructor of the variant
    /// bypasses the decoder) → `BadDescribeTarget { target }` →
    /// `08P01 protocol_violation`. Error_state is set per spec §6.
    #[test]
    fn t4_dispatch_describe_unknown_target_byte_returns_08p01_bad_target() {
        let mut state = SessionState::new();
        let msg = proto::ExtqMessage::Describe {
            target: b'X', // neither 'S' nor 'P'
            name: "irrelevant".to_string(),
        };
        match try_dispatch_extq(&mut state, &CannedTwoColEngine, msg) {
            ExtqOutcome::Failed(ExtqError::BadDescribeTarget { target }) => {
                assert_eq!(target, b'X');
            }
            other => panic!("expected BadDescribeTarget, got {other:?}"),
        }
        assert!(state.in_error_state());
    }

    /// HEADLINE T4 KAT: a Parse + Bind + Describe('S') round-trip
    /// through the dispatcher composes correctly. Locks the T3 → T4
    /// transition — Parse installs a stmt with OID hints, Bind stores
    /// a portal referencing it, Describe('S') emits
    /// ParameterDescription + RowDescription/NoData byte-correctly.
    /// This is the closest in-dispatcher equivalent to the
    /// `run_session` 4-message round trip (the final missing piece is
    /// Sync's ReadyForQuery — T6/T7).
    #[test]
    fn t4_dispatch_parse_bind_describe_s_round_trip_composes() {
        let mut state = SessionState::new();
        // Parse: stmt "ps1" with one int4 param OID + SELECT * FROM t.
        let parse = proto::ExtqMessage::Parse {
            name: "ps1".to_string(),
            sql: "SELECT * FROM t".to_string(),
            param_oids: vec![23],
        };
        let pc = try_dispatch_extq(&mut state, &CannedTwoColEngine, parse);
        match pc {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'1', 0, 0, 0, 4]),
            other => panic!("expected ParseComplete, got {other:?}"),
        }
        // Bind: portal "pt1" → stmt "ps1" with one text value.
        let bind = proto::ExtqMessage::Bind {
            portal: "pt1".to_string(),
            stmt: "ps1".to_string(),
            param_formats: vec![FORMAT_CODE_TEXT],
            param_values: vec![Some(b"42".to_vec())],
            result_formats: vec![FORMAT_CODE_TEXT],
        };
        let bc = try_dispatch_extq(&mut state, &CannedTwoColEngine, bind);
        match bc {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'2', 0, 0, 0, 4]),
            other => panic!("expected BindComplete, got {other:?}"),
        }
        // Describe 'S' on "ps1".
        let desc = proto::ExtqMessage::Describe {
            target: DESCRIBE_TARGET_STATEMENT,
            name: "ps1".to_string(),
        };
        let dc = try_dispatch_extq(&mut state, &CannedTwoColEngine, desc);
        match dc {
            ExtqOutcome::Bytes(bytes) => {
                let pd = response::encode_parameter_description(&[23]);
                assert!(bytes.starts_with(&pd));
                // After PD, the next byte is RowDescription 'T'.
                assert_eq!(bytes[pd.len()], b'T');
            }
            other => panic!("expected Bytes(PD + RD), got {other:?}"),
        }
        assert!(!state.in_error_state());
    }

    // ───────────────────────────────────────────────────────────────────
    // T5 KATs — real Execute + Sync handlers. Locks every spec §4 +
    // §6 + §7.2 + §9 invariant the run_session integration depends on.
    //
    // The key invariants:
    //   - Execute on unbound portal → 34000.
    //   - Execute on empty SQL → EmptyQueryResponse.
    //   - Execute substitutes $N before calling dispatch_query.
    //   - Execute strips dispatch_query's trailing RFQ.
    //   - max_rows pagination buffers + pages with PortalSuspended.
    //   - Sync emits RFQ('I') + clears error_state + drops "" portal.
    // ───────────────────────────────────────────────────────────────────

    /// A test engine that returns N kessel-codec-encoded rows for
    /// `SELECT * FROM t` and lets each KAT pre-load the row bytes.
    /// Mirrors the `CannedEngine` in dispatch.rs but cloneable so we
    /// can shape buffered-row KATs.
    struct CannedRowsEngine {
        cols: Vec<crate::engine::PgColumn>,
        row_stream: Vec<u8>,
        table_name: String,
    }
    impl EngineApply for CannedRowsEngine {
        fn apply_sql(&self, _sql: &str) -> OpResult {
            OpResult::Got(self.row_stream.clone().into())
        }
        fn describe_table(&self, name: &str) -> Option<Vec<crate::engine::PgColumn>> {
            if name == self.table_name {
                Some(self.cols.clone())
            } else {
                None
            }
        }
    }

    fn build_canned_rows(n: usize) -> CannedRowsEngine {
        use crate::engine::PgColumn;
        use kessel_catalog::{Field, FieldKind, ObjectType};
        use kessel_codec::Value;
        let cols = vec![PgColumn {
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let fields = vec![Field {
            field_id: 0,
            name: "id".into(),
            kind: FieldKind::I64,
            nullable: false,
        }];
        let ot = ObjectType::from_def("t".to_string(), fields);
        let mut stream = Vec::new();
        for i in 0..n {
            let rec = kessel_codec::encode(&ot, &[Value::Int((i + 1) as i128)]).expect("enc");
            stream.extend_from_slice(&(rec.len() as u32).to_le_bytes());
            stream.extend_from_slice(&rec);
        }
        CannedRowsEngine {
            cols,
            row_stream: stream,
            table_name: "t".into(),
        }
    }

    /// Spec §3 + PG §55.2.3: Execute on a non-existent portal returns
    /// `UnknownPortal` → `34000 invalid_cursor_name`. Error_state
    /// engaged.
    #[test]
    fn t5_dispatch_execute_unknown_portal_returns_34000() {
        let mut state = SessionState::new();
        let msg = proto::ExtqMessage::Execute {
            portal: "ghost".to_string(),
            max_rows: 0,
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
            ExtqOutcome::Failed(ExtqError::UnknownPortal { name }) => {
                assert_eq!(name, "ghost");
            }
            other => panic!("expected UnknownPortal, got {other:?}"),
        }
        assert!(state.in_error_state());
    }

    /// Spec §12 OQ #5: Execute on a portal whose statement has empty
    /// SQL emits the 5-byte `EmptyQueryResponse` envelope (`I [length=4]`).
    #[test]
    fn t5_dispatch_execute_empty_sql_emits_empty_query_response() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "ps_empty", "", vec![]);
        let bind = proto::ExtqMessage::Bind {
            portal: "p_empty".to_string(),
            stmt: "ps_empty".to_string(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        try_dispatch_extq(&mut state, &NoSchemaEngine, bind);
        let exec = proto::ExtqMessage::Execute {
            portal: "p_empty".to_string(),
            max_rows: 0,
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, exec) {
            ExtqOutcome::Bytes(b) => {
                // EmptyQueryResponse envelope: `I [length=4]`.
                assert_eq!(b, vec![b'I', 0, 0, 0, 4]);
            }
            other => panic!("expected Bytes(EmptyQueryResponse), got {other:?}"),
        }
    }

    /// HEADLINE: Execute on a SELECT portal emits the canonical wire
    /// sequence — RowDescription + DataRow×N + CommandComplete — but
    /// NO trailing RFQ (Sync emits that). Locks spec §4 + §9.
    #[test]
    fn t5_dispatch_execute_select_emits_row_desc_data_rows_and_command_complete_no_rfq() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "ps_sel", "SELECT * FROM t", vec![]);
        let bind = proto::ExtqMessage::Bind {
            portal: "p_sel".to_string(),
            stmt: "ps_sel".to_string(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        let engine = build_canned_rows(3);
        try_dispatch_extq(&mut state, &engine, bind);
        let exec = proto::ExtqMessage::Execute {
            portal: "p_sel".to_string(),
            max_rows: 0,
        };
        match try_dispatch_extq(&mut state, &engine, exec) {
            ExtqOutcome::Bytes(bytes) => {
                // Tag order: T, D, D, D, C — NO Z trailing.
                assert_eq!(bytes[0], b'T', "first byte should be RowDescription");
                assert!(!bytes.iter().any(|&b| b == b'Z'),
                    "Execute output must NOT contain RFQ — Sync emits it");
                // Count data rows.
                let d_count = bytes.iter().filter(|&&b| b == b'D').count();
                assert_eq!(d_count, 3, "should have 3 DataRow tags");
                // Final block is CommandComplete with "SELECT 3".
                assert!(
                    bytes.windows(b"SELECT 3\0".len()).any(|w| w == b"SELECT 3\0"),
                    "CommandComplete should carry 'SELECT 3'"
                );
            }
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    /// Spec §7.2 max_rows pagination: Execute(max_rows=2) on a 5-row
    /// portal emits T + 2×D + PortalSuspended; the SECOND Execute
    /// emits 2×D + PortalSuspended; the THIRD emits 1×D + CommandComplete.
    /// Locks the buffered-cursor state machine.
    #[test]
    fn t5_dispatch_execute_max_rows_pagination_emits_portal_suspended() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "ps_sel", "SELECT * FROM t", vec![]);
        let bind = proto::ExtqMessage::Bind {
            portal: "p_sel".to_string(),
            stmt: "ps_sel".to_string(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        let engine = build_canned_rows(5);
        try_dispatch_extq(&mut state, &engine, bind);

        // FIRST Execute(max_rows=2).
        let exec1 = proto::ExtqMessage::Execute {
            portal: "p_sel".to_string(),
            max_rows: 2,
        };
        match try_dispatch_extq(&mut state, &engine, exec1) {
            ExtqOutcome::Bytes(bytes) => {
                assert_eq!(bytes[0], b'T'); // RowDescription
                let d_count = bytes.iter().filter(|&&b| b == b'D').count();
                assert_eq!(d_count, 2);
                // Trailing 5 bytes = PortalSuspended `s [length=4]`.
                assert_eq!(&bytes[bytes.len() - 5..], &[b's', 0, 0, 0, 4]);
                // No CommandComplete tag yet.
                assert!(!bytes.windows(b"SELECT".len()).any(|w| w == b"SELECT"));
            }
            other => panic!("expected Bytes, got {other:?}"),
        }

        // SECOND Execute(max_rows=2) — no RowDescription (already sent),
        // 2 more DataRows + PortalSuspended.
        let exec2 = proto::ExtqMessage::Execute {
            portal: "p_sel".to_string(),
            max_rows: 2,
        };
        match try_dispatch_extq(&mut state, &engine, exec2) {
            ExtqOutcome::Bytes(bytes) => {
                assert_ne!(bytes[0], b'T', "second Execute must NOT repeat RowDescription");
                let d_count = bytes.iter().filter(|&&b| b == b'D').count();
                assert_eq!(d_count, 2);
                assert_eq!(&bytes[bytes.len() - 5..], &[b's', 0, 0, 0, 4]);
            }
            other => panic!("expected Bytes, got {other:?}"),
        }

        // THIRD Execute(max_rows=2) — 1 row + CommandComplete.
        let exec3 = proto::ExtqMessage::Execute {
            portal: "p_sel".to_string(),
            max_rows: 2,
        };
        match try_dispatch_extq(&mut state, &engine, exec3) {
            ExtqOutcome::Bytes(bytes) => {
                let d_count = bytes.iter().filter(|&&b| b == b'D').count();
                assert_eq!(d_count, 1);
                // CommandComplete carries SELECT 5 (the buffered total).
                assert!(
                    bytes.windows(b"SELECT 5\0".len()).any(|w| w == b"SELECT 5\0"),
                    "third Execute should drain with CommandComplete('SELECT 5')"
                );
            }
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    /// max_rows == 0 means "all rows" — no PortalSuspended.
    #[test]
    fn t5_dispatch_execute_max_rows_zero_emits_all_rows() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "ps_sel", "SELECT * FROM t", vec![]);
        let bind = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "ps_sel".to_string(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        let engine = build_canned_rows(4);
        try_dispatch_extq(&mut state, &engine, bind);
        let exec = proto::ExtqMessage::Execute {
            portal: String::new(),
            max_rows: 0,
        };
        match try_dispatch_extq(&mut state, &engine, exec) {
            ExtqOutcome::Bytes(bytes) => {
                let d_count = bytes.iter().filter(|&&b| b == b'D').count();
                assert_eq!(d_count, 4);
                // No PortalSuspended (`s` envelope).
                assert!(!bytes.windows(5).any(|w| w == &[b's', 0, 0, 0, 4][..]));
                // CommandComplete is present.
                assert!(bytes.windows(b"SELECT 4\0".len()).any(|w| w == b"SELECT 4\0"));
            }
            other => panic!("expected Bytes, got {other:?}"),
        }
    }

    /// Spec §6: Execute in error_state returns `Skipped` (no
    /// processing, no output bytes).
    #[test]
    fn t5_dispatch_execute_in_error_state_returns_skipped() {
        let mut state = SessionState::new();
        state.set_error_state(true);
        let exec = proto::ExtqMessage::Execute {
            portal: "any".to_string(),
            max_rows: 0,
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, exec) {
            ExtqOutcome::Skipped => {}
            other => panic!("expected Skipped, got {other:?}"),
        }
        assert!(state.in_error_state());
    }

    /// Sync emits RFQ('I') byte-locked + clears error_state.
    #[test]
    fn t5_dispatch_sync_emits_rfq_and_clears_error_state() {
        let mut state = SessionState::new();
        state.set_error_state(true);
        match try_dispatch_extq(&mut state, &NoSchemaEngine, proto::ExtqMessage::Sync) {
            ExtqOutcome::Bytes(bytes) => {
                // RFQ envelope: `Z [length=5] [status='I']` = 6 bytes.
                assert_eq!(bytes, vec![b'Z', 0, 0, 0, 5, b'I']);
            }
            other => panic!("expected Bytes(RFQ), got {other:?}"),
        }
        // error_state cleared.
        assert!(!state.in_error_state());
    }

    /// Sync emits RFQ('I') even without error_state.
    #[test]
    fn t5_dispatch_sync_when_idle_still_emits_rfq() {
        let mut state = SessionState::new();
        match try_dispatch_extq(&mut state, &NoSchemaEngine, proto::ExtqMessage::Sync) {
            ExtqOutcome::Bytes(bytes) => {
                assert_eq!(bytes, vec![b'Z', 0, 0, 0, 5, b'I']);
            }
            other => panic!("expected Bytes(RFQ), got {other:?}"),
        }
    }

    /// Sync drops the unnamed `""` portal but keeps named portals.
    /// PG §55.2.3.
    #[test]
    fn t5_dispatch_sync_drops_unnamed_portal_keeps_named() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "", "SELECT * FROM t", vec![]);
        seed_stmt_with_sql(&mut state, "named_stmt", "SELECT * FROM t", vec![]);
        // Bind unnamed + named portals.
        try_dispatch_extq(
            &mut state,
            &NoSchemaEngine,
            proto::ExtqMessage::Bind {
                portal: String::new(),
                stmt: String::new(),
                param_formats: vec![],
                param_values: vec![],
                result_formats: vec![],
            },
        );
        try_dispatch_extq(
            &mut state,
            &NoSchemaEngine,
            proto::ExtqMessage::Bind {
                portal: "named_portal".to_string(),
                stmt: "named_stmt".to_string(),
                param_formats: vec![],
                param_values: vec![],
                result_formats: vec![],
            },
        );
        assert_eq!(state.portal_count(), 2);
        // Sync.
        try_dispatch_extq(&mut state, &NoSchemaEngine, proto::ExtqMessage::Sync);
        // Unnamed portal dropped, named portal kept.
        assert_eq!(state.portal_count(), 1);
        assert!(state.get_portal("").is_none());
        assert!(state.get_portal("named_portal").is_some());
    }

    /// Parameter substitution: a portal with `Some(b"42")` binds
    /// `$1` to `'42'` and the rewritten SQL is what flows into the
    /// engine. We use a custom engine that asserts on the SQL it sees.
    #[test]
    fn t5_dispatch_execute_substitutes_parameter_into_sql() {
        struct SqlAssertEngine {
            expected_sql: String,
            cols: Vec<crate::engine::PgColumn>,
        }
        impl EngineApply for SqlAssertEngine {
            fn apply_sql(&self, sql: &str) -> OpResult {
                let trimmed = sql.trim().trim_end_matches(';').trim();
                assert_eq!(trimmed, self.expected_sql.as_str(),
                    "engine should see param-substituted SQL");
                OpResult::Got(Vec::<u8>::new().into())
            }
            fn describe_table(&self, name: &str) -> Option<Vec<crate::engine::PgColumn>> {
                if name == "t" { Some(self.cols.clone()) } else { None }
            }
        }
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "ps", "SELECT * FROM t WHERE id = $1", vec![]);
        let bind = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "ps".to_string(),
            param_formats: vec![],
            param_values: vec![Some(b"42".to_vec())],
            result_formats: vec![],
        };
        let engine = SqlAssertEngine {
            expected_sql: "SELECT * FROM t WHERE id = '42'".to_string(),
            cols: vec![crate::engine::PgColumn {
                name: "id".into(),
                kind: FieldKind::I64,
                nullable: false,
            }],
        };
        try_dispatch_extq(&mut state, &engine, bind);
        let exec = proto::ExtqMessage::Execute {
            portal: String::new(),
            max_rows: 0,
        };
        // The engine's apply_sql will assert the substituted form.
        try_dispatch_extq(&mut state, &engine, exec);
    }

    /// Parameter substitution: NULL bound value → bare `NULL` in
    /// rewritten SQL.
    #[test]
    fn t5_dispatch_execute_substitutes_null_parameter_as_bare_null() {
        struct SqlAssertEngine {
            expected_sql: String,
        }
        impl EngineApply for SqlAssertEngine {
            fn apply_sql(&self, sql: &str) -> OpResult {
                let trimmed = sql.trim().trim_end_matches(';').trim();
                assert_eq!(trimmed, self.expected_sql.as_str());
                OpResult::Got(Vec::<u8>::new().into())
            }
            fn describe_table(&self, _name: &str) -> Option<Vec<crate::engine::PgColumn>> {
                None
            }
        }
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "ps", "INSERT INTO t (x) VALUES ($1)", vec![]);
        let bind = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "ps".to_string(),
            param_formats: vec![],
            param_values: vec![None],
            result_formats: vec![],
        };
        let engine = SqlAssertEngine {
            expected_sql: "INSERT INTO t (x) VALUES (NULL)".to_string(),
        };
        try_dispatch_extq(&mut state, &engine, bind);
        let exec = proto::ExtqMessage::Execute {
            portal: String::new(),
            max_rows: 0,
        };
        try_dispatch_extq(&mut state, &engine, exec);
    }

    /// SP-PG-EXTQ-BIN T2 — Execute with an INT8 BINARY parameter
    /// decodes via `decode_binary_param` (NOT the text scanner) and
    /// rewrites `$1` to an UNQUOTED `42` literal (not `'42'` — the
    /// text path quotes everything, but binary decoded integers
    /// substitute as bare literals so the SQL parser sees a true
    /// integer not a string).
    #[test]
    fn t2bin_dispatch_execute_substitutes_int8_binary_as_bare_literal() {
        struct SqlAssertEngine {
            expected_sql: String,
        }
        impl EngineApply for SqlAssertEngine {
            fn apply_sql(&self, sql: &str) -> OpResult {
                let trimmed = sql.trim().trim_end_matches(';').trim();
                assert_eq!(
                    trimmed,
                    self.expected_sql.as_str(),
                    "engine should see bare-int param substituted"
                );
                OpResult::Got(Vec::<u8>::new().into())
            }
            fn describe_table(
                &self,
                _name: &str,
            ) -> Option<Vec<crate::engine::PgColumn>> {
                None
            }
        }
        let mut state = SessionState::new();
        seed_stmt_with_sql(
            &mut state,
            "ps",
            "SELECT * FROM t WHERE id = $1",
            vec![PG_TYPE_INT8],
        );
        let int8_bytes = 42i64.to_be_bytes().to_vec();
        let bind = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "ps".to_string(),
            param_formats: vec![FORMAT_CODE_BINARY],
            param_values: vec![Some(int8_bytes)],
            result_formats: vec![],
        };
        let engine = SqlAssertEngine {
            expected_sql: "SELECT * FROM t WHERE id = 42".to_string(),
        };
        try_dispatch_extq(&mut state, &engine, bind);
        let exec = proto::ExtqMessage::Execute {
            portal: String::new(),
            max_rows: 0,
        };
        try_dispatch_extq(&mut state, &engine, exec);
    }

    /// SP-PG-EXTQ-BIN T2 — Execute with a TEXT BINARY parameter
    /// decodes the bytes as UTF-8 and substitutes through the
    /// quoted-text path (so single-quote doubling still applies).
    #[test]
    fn t2bin_dispatch_execute_substitutes_text_binary_with_quote_escape() {
        struct SqlAssertEngine {
            expected_sql: String,
        }
        impl EngineApply for SqlAssertEngine {
            fn apply_sql(&self, sql: &str) -> OpResult {
                let trimmed = sql.trim().trim_end_matches(';').trim();
                assert_eq!(trimmed, self.expected_sql.as_str());
                OpResult::Got(Vec::<u8>::new().into())
            }
            fn describe_table(
                &self,
                _name: &str,
            ) -> Option<Vec<crate::engine::PgColumn>> {
                None
            }
        }
        let mut state = SessionState::new();
        seed_stmt_with_sql(
            &mut state,
            "ps",
            "INSERT INTO t (name) VALUES ($1)",
            vec![PG_TYPE_TEXT],
        );
        let bind = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "ps".to_string(),
            param_formats: vec![FORMAT_CODE_BINARY],
            param_values: vec![Some(b"O'Brien".to_vec())],
            result_formats: vec![],
        };
        let engine = SqlAssertEngine {
            expected_sql: "INSERT INTO t (name) VALUES ('O''Brien')".to_string(),
        };
        try_dispatch_extq(&mut state, &engine, bind);
        let exec = proto::ExtqMessage::Execute {
            portal: String::new(),
            max_rows: 0,
        };
        try_dispatch_extq(&mut state, &engine, exec);
    }

    /// SP-PG-EXTQ-BIN T2 — Execute with a BYTEA BINARY parameter
    /// substitutes `'\xHEX'::bytea` so the SQL parser knows the
    /// literal's type.
    #[test]
    fn t2bin_dispatch_execute_substitutes_bytea_binary_with_cast() {
        struct SqlAssertEngine {
            expected_sql: String,
        }
        impl EngineApply for SqlAssertEngine {
            fn apply_sql(&self, sql: &str) -> OpResult {
                let trimmed = sql.trim().trim_end_matches(';').trim();
                assert_eq!(trimmed, self.expected_sql.as_str());
                OpResult::Got(Vec::<u8>::new().into())
            }
            fn describe_table(
                &self,
                _name: &str,
            ) -> Option<Vec<crate::engine::PgColumn>> {
                None
            }
        }
        let mut state = SessionState::new();
        seed_stmt_with_sql(
            &mut state,
            "ps",
            "INSERT INTO t (b) VALUES ($1)",
            vec![PG_TYPE_BYTEA],
        );
        let bind = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "ps".to_string(),
            param_formats: vec![FORMAT_CODE_BINARY],
            param_values: vec![Some(vec![0xDE, 0xAD])],
            result_formats: vec![],
        };
        let engine = SqlAssertEngine {
            expected_sql: "INSERT INTO t (b) VALUES ('\\xdead'::bytea)"
                .to_string(),
        };
        try_dispatch_extq(&mut state, &engine, bind);
        let exec = proto::ExtqMessage::Execute {
            portal: String::new(),
            max_rows: 0,
        };
        try_dispatch_extq(&mut state, &engine, exec);
    }

    /// SP-PG-EXTQ-BIN T2 — NULL is format-agnostic: a binary-format
    /// position with `Option::None` value (the wire `length=-1`
    /// sentinel) substitutes as bare `NULL` regardless of the
    /// declared type OID.
    #[test]
    fn t2bin_dispatch_execute_null_binary_renders_as_null() {
        struct SqlAssertEngine {
            expected_sql: String,
        }
        impl EngineApply for SqlAssertEngine {
            fn apply_sql(&self, sql: &str) -> OpResult {
                let trimmed = sql.trim().trim_end_matches(';').trim();
                assert_eq!(trimmed, self.expected_sql.as_str());
                OpResult::Got(Vec::<u8>::new().into())
            }
            fn describe_table(
                &self,
                _name: &str,
            ) -> Option<Vec<crate::engine::PgColumn>> {
                None
            }
        }
        let mut state = SessionState::new();
        seed_stmt_with_sql(
            &mut state,
            "ps",
            "INSERT INTO t (id) VALUES ($1)",
            vec![PG_TYPE_INT8],
        );
        let bind = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "ps".to_string(),
            param_formats: vec![FORMAT_CODE_BINARY],
            param_values: vec![None],
            result_formats: vec![],
        };
        let engine = SqlAssertEngine {
            expected_sql: "INSERT INTO t (id) VALUES (NULL)".to_string(),
        };
        try_dispatch_extq(&mut state, &engine, bind);
        let exec = proto::ExtqMessage::Execute {
            portal: String::new(),
            max_rows: 0,
        };
        try_dispatch_extq(&mut state, &engine, exec);
    }

    /// SP-PG-EXTQ-BIN T2 regression-lock: the existing TEXT-format
    /// substitution path is byte-equal after the refactor (a Bind
    /// with empty `param_formats` still produces single-quoted
    /// `'42'`, NOT the bare-int `42` that binary INT8 would).
    #[test]
    fn t2bin_dispatch_execute_text_format_path_unchanged() {
        struct SqlAssertEngine {
            expected_sql: String,
        }
        impl EngineApply for SqlAssertEngine {
            fn apply_sql(&self, sql: &str) -> OpResult {
                let trimmed = sql.trim().trim_end_matches(';').trim();
                assert_eq!(trimmed, self.expected_sql.as_str());
                OpResult::Got(Vec::<u8>::new().into())
            }
            fn describe_table(
                &self,
                _name: &str,
            ) -> Option<Vec<crate::engine::PgColumn>> {
                None
            }
        }
        let mut state = SessionState::new();
        // No OID hints, no format codes — pure text-format flow.
        seed_stmt_with_sql(
            &mut state,
            "ps",
            "INSERT INTO t (id) VALUES ($1)",
            vec![],
        );
        let bind = proto::ExtqMessage::Bind {
            portal: String::new(),
            stmt: "ps".to_string(),
            param_formats: vec![],
            param_values: vec![Some(b"42".to_vec())],
            result_formats: vec![],
        };
        let engine = SqlAssertEngine {
            expected_sql: "INSERT INTO t (id) VALUES ('42')".to_string(),
        };
        try_dispatch_extq(&mut state, &engine, bind);
        let exec = proto::ExtqMessage::Execute {
            portal: String::new(),
            max_rows: 0,
        };
        try_dispatch_extq(&mut state, &engine, exec);
    }

    /// HEADLINE — Parse + Bind + Describe('S') + Execute + Sync emits
    /// the canonical 5-piece backend sequence:
    /// ParseComplete + BindComplete + ParameterDescription +
    /// RowDescription (from Describe; suppressed from Execute) +
    /// CommandComplete + ReadyForQuery. The byte stream is
    /// concatenated across the five dispatcher calls.
    #[test]
    fn t5_dispatch_parse_bind_describe_execute_sync_full_orm_round_trip() {
        let mut state = SessionState::new();
        let engine = build_canned_rows(2);
        // 1. Parse
        let pc = try_dispatch_extq(
            &mut state,
            &engine,
            proto::ExtqMessage::Parse {
                name: "ps1".to_string(),
                sql: "SELECT * FROM t".to_string(),
                param_oids: vec![],
            },
        );
        let pc_bytes = match pc {
            ExtqOutcome::Bytes(b) => b,
            other => panic!("Parse: {other:?}"),
        };
        // 2. Bind
        let bc = try_dispatch_extq(
            &mut state,
            &engine,
            proto::ExtqMessage::Bind {
                portal: "pt1".to_string(),
                stmt: "ps1".to_string(),
                param_formats: vec![],
                param_values: vec![],
                result_formats: vec![],
            },
        );
        let bc_bytes = match bc {
            ExtqOutcome::Bytes(b) => b,
            other => panic!("Bind: {other:?}"),
        };
        // 3. Describe 'S'
        let dc = try_dispatch_extq(
            &mut state,
            &engine,
            proto::ExtqMessage::Describe {
                target: DESCRIBE_TARGET_STATEMENT,
                name: "ps1".to_string(),
            },
        );
        let dc_bytes = match dc {
            ExtqOutcome::Bytes(b) => b,
            other => panic!("Describe: {other:?}"),
        };
        // 4. Execute
        let ec = try_dispatch_extq(
            &mut state,
            &engine,
            proto::ExtqMessage::Execute {
                portal: "pt1".to_string(),
                max_rows: 0,
            },
        );
        let ec_bytes = match ec {
            ExtqOutcome::Bytes(b) => b,
            other => panic!("Execute: {other:?}"),
        };
        // 5. Sync
        let sc = try_dispatch_extq(&mut state, &engine, proto::ExtqMessage::Sync);
        let sc_bytes = match sc {
            ExtqOutcome::Bytes(b) => b,
            other => panic!("Sync: {other:?}"),
        };

        // Concatenated wire byte stream.
        let mut wire = Vec::new();
        wire.extend_from_slice(&pc_bytes);
        wire.extend_from_slice(&bc_bytes);
        wire.extend_from_slice(&dc_bytes);
        wire.extend_from_slice(&ec_bytes);
        wire.extend_from_slice(&sc_bytes);

        // Locked checks:
        // - First byte: '1' (ParseComplete).
        assert_eq!(wire[0], b'1');
        // - Contains '2' (BindComplete) after ParseComplete.
        assert!(wire[5..].iter().take(5).any(|&b| b == b'2'));
        // - Contains 't' (ParameterDescription, empty in this case).
        assert!(wire.iter().any(|&b| b == b't'));
        // - Contains 'T' (RowDescription, from Describe — Execute did
        //   NOT repeat it because Describe('S') doesn't set
        //   row_description_sent. Actually only Describe('P') does;
        //   Describe('S') does NOT set the flag because PG always
        //   emits 'T' once per session — for SELECT * FROM t the
        //   prelude from dispatch_query will include 'T'. So 'T'
        //   appears TWICE: once from Describe('S'), once from
        //   Execute's prelude.)
        //   For V1 simplicity we ship the "T can appear twice across
        //   Describe('S') + Execute" shape — clients tolerate it
        //   (libpq + asyncpg + JDBC all do; the spec only requires
        //   T once per portal per Sync block, and Describe('S') is
        //   a STATEMENT-targeted describe, not portal-targeted).
        assert!(wire.iter().any(|&b| b == b'T'));
        // - 2 DataRow tags.
        assert_eq!(wire.iter().filter(|&&b| b == b'D').count(), 2);
        // - CommandComplete 'SELECT 2'.
        assert!(wire.windows(b"SELECT 2\0".len()).any(|w| w == b"SELECT 2\0"));
        // - Trailing 6 bytes: RFQ('I').
        assert_eq!(&wire[wire.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);

        // No 0A000 / 26000 / 34000 / 08P01 anywhere.
        for code in [b"0A000", b"26000", b"34000", b"08P01"] {
            assert!(!wire.windows(5).any(|w| w == code),
                "wire stream must not contain {code:?}");
        }
    }

    /// HEADLINE 2 — Parse + Bind + Execute + Sync (NO Describe)
    /// emits ParseComplete + BindComplete + RowDescription +
    /// DataRow* + CommandComplete + RFQ. The Execute's prelude
    /// INCLUDES RowDescription because Describe didn't pre-emit it.
    #[test]
    fn t5_dispatch_parse_bind_execute_sync_no_describe_includes_row_description() {
        let mut state = SessionState::new();
        let engine = build_canned_rows(2);
        try_dispatch_extq(
            &mut state,
            &engine,
            proto::ExtqMessage::Parse {
                name: String::new(),
                sql: "SELECT * FROM t".to_string(),
                param_oids: vec![],
            },
        );
        try_dispatch_extq(
            &mut state,
            &engine,
            proto::ExtqMessage::Bind {
                portal: String::new(),
                stmt: String::new(),
                param_formats: vec![],
                param_values: vec![],
                result_formats: vec![],
            },
        );
        let exec = try_dispatch_extq(
            &mut state,
            &engine,
            proto::ExtqMessage::Execute {
                portal: String::new(),
                max_rows: 0,
            },
        );
        let exec_bytes = match exec {
            ExtqOutcome::Bytes(b) => b,
            other => panic!("Execute: {other:?}"),
        };
        // RowDescription appears in Execute's output.
        assert_eq!(exec_bytes[0], b'T');
        // 2 DataRows.
        assert_eq!(exec_bytes.iter().filter(|&&b| b == b'D').count(), 2);
        // CommandComplete.
        assert!(
            exec_bytes
                .windows(b"SELECT 2\0".len())
                .any(|w| w == b"SELECT 2\0")
        );
    }

    /// Describe('P') + Execute: the portal's `row_description_sent`
    /// flag is set by Describe('P'), so Execute MUST NOT repeat
    /// RowDescription. Locks the spec §4 PG-protocol invariant.
    #[test]
    fn t5_dispatch_describe_p_then_execute_suppresses_row_description() {
        let mut state = SessionState::new();
        let engine = build_canned_rows(2);
        try_dispatch_extq(
            &mut state,
            &engine,
            proto::ExtqMessage::Parse {
                name: "ps".to_string(),
                sql: "SELECT * FROM t".to_string(),
                param_oids: vec![],
            },
        );
        try_dispatch_extq(
            &mut state,
            &engine,
            proto::ExtqMessage::Bind {
                portal: "pt".to_string(),
                stmt: "ps".to_string(),
                param_formats: vec![],
                param_values: vec![],
                result_formats: vec![],
            },
        );
        // Describe('P') — this should set row_description_sent.
        try_dispatch_extq(
            &mut state,
            &engine,
            proto::ExtqMessage::Describe {
                target: DESCRIBE_TARGET_PORTAL,
                name: "pt".to_string(),
            },
        );
        // Confirm flag set.
        assert!(state.get_portal("pt").unwrap().row_description_sent);
        // Execute — must NOT emit RowDescription.
        let exec = try_dispatch_extq(
            &mut state,
            &engine,
            proto::ExtqMessage::Execute {
                portal: "pt".to_string(),
                max_rows: 0,
            },
        );
        let exec_bytes = match exec {
            ExtqOutcome::Bytes(b) => b,
            other => panic!("Execute: {other:?}"),
        };
        // First byte should be 'D' (DataRow), NOT 'T' (RowDescription).
        assert_ne!(
            exec_bytes[0], b'T',
            "Execute after Describe('P') must NOT repeat RowDescription"
        );
        assert_eq!(exec_bytes[0], b'D');
    }

    // ───────────────────────────────────────────────────────────────────
    // T6 KATs — real Close + Flush handlers. Locks every spec §4 + §6 +
    // §9 invariant the run_session integration depends on.
    //
    // The key invariants:
    //   - Close('S') drops the named statement + emits CloseComplete.
    //   - Close('P') drops the named portal + emits CloseComplete.
    //   - Close on missing name is a SILENT no-op + CloseComplete
    //     (PG §55.2.3).
    //   - Close with unknown target → 08P01 + error_state engaged.
    //   - Close in error_state → Skipped (spec §6).
    //   - Flush → ExtqOutcome::Flush (no bytes, no state mutation).
    // ───────────────────────────────────────────────────────────────────

    /// Spec §4 + §9: Close('S') on an existing statement drops it from
    /// `state.statements` and emits the 5-byte CloseComplete envelope
    /// (`3 00 00 00 04`).
    #[test]
    fn t6_dispatch_close_statement_drops_existing_and_emits_close_complete() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "to_drop", "SELECT 1", vec![]);
        seed_stmt_with_sql(&mut state, "keep", "SELECT 2", vec![]);
        assert_eq!(state.statement_count(), 2);
        let msg = proto::ExtqMessage::Close {
            target: DESCRIBE_TARGET_STATEMENT,
            name: "to_drop".to_string(),
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
            ExtqOutcome::Bytes(b) => {
                // Byte-locked: spec §9 CloseComplete envelope.
                assert_eq!(b, vec![b'3', 0, 0, 0, 4]);
                assert_eq!(b.len(), 5);
            }
            other => panic!("expected Bytes(CloseComplete), got {other:?}"),
        }
        // The named stmt is gone; the other stmt stayed.
        assert_eq!(state.statement_count(), 1);
        assert!(state.get_statement("to_drop").is_none());
        assert!(state.get_statement("keep").is_some());
        assert!(!state.in_error_state());
    }

    /// Spec §4 + PG §55.2.3: Close('S') on a missing name is a SILENT
    /// no-op — emit CloseComplete, do NOT engage error_state, do NOT
    /// mutate any other state. The PG spec is explicit: "It is not
    /// an error to issue Close against a nonexistent statement or
    /// portal name". libpq + JDBC + asyncpg ALL rely on this.
    #[test]
    fn t6_dispatch_close_statement_missing_is_silent_no_op_with_close_complete() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "real", "SELECT 1", vec![]);
        let msg = proto::ExtqMessage::Close {
            target: DESCRIBE_TARGET_STATEMENT,
            name: "ghost".to_string(),
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
            ExtqOutcome::Bytes(b) => {
                assert_eq!(b, vec![b'3', 0, 0, 0, 4]);
            }
            other => panic!("expected Bytes(CloseComplete), got {other:?}"),
        }
        // The real statement is untouched.
        assert_eq!(state.statement_count(), 1);
        assert!(state.get_statement("real").is_some());
        assert!(!state.in_error_state());
    }

    /// Spec §4 + §9: Close('P') on an existing portal drops it from
    /// `state.portals` and emits CloseComplete.
    #[test]
    fn t6_dispatch_close_portal_drops_existing_and_emits_close_complete() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "ps", "SELECT 1", vec![]);
        // Bind two portals: one to drop, one to keep.
        try_dispatch_extq(
            &mut state,
            &NoSchemaEngine,
            proto::ExtqMessage::Bind {
                portal: "drop_me".to_string(),
                stmt: "ps".to_string(),
                param_formats: vec![],
                param_values: vec![],
                result_formats: vec![],
            },
        );
        try_dispatch_extq(
            &mut state,
            &NoSchemaEngine,
            proto::ExtqMessage::Bind {
                portal: "keep_me".to_string(),
                stmt: "ps".to_string(),
                param_formats: vec![],
                param_values: vec![],
                result_formats: vec![],
            },
        );
        assert_eq!(state.portal_count(), 2);
        let msg = proto::ExtqMessage::Close {
            target: DESCRIBE_TARGET_PORTAL,
            name: "drop_me".to_string(),
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
            ExtqOutcome::Bytes(b) => {
                assert_eq!(b, vec![b'3', 0, 0, 0, 4]);
            }
            other => panic!("expected Bytes(CloseComplete), got {other:?}"),
        }
        assert_eq!(state.portal_count(), 1);
        assert!(state.get_portal("drop_me").is_none());
        assert!(state.get_portal("keep_me").is_some());
        // The backing statement is untouched (Close('P') does NOT
        // cascade to the parent stmt — PG §55.2.3).
        assert!(state.get_statement("ps").is_some());
    }

    /// Spec §4 + PG §55.2.3: Close('P') on a missing portal name is a
    /// SILENT no-op + CloseComplete.
    #[test]
    fn t6_dispatch_close_portal_missing_is_silent_no_op_with_close_complete() {
        let mut state = SessionState::new();
        let msg = proto::ExtqMessage::Close {
            target: DESCRIBE_TARGET_PORTAL,
            name: "ghost_portal".to_string(),
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'3', 0, 0, 0, 4]),
            other => panic!("expected Bytes(CloseComplete), got {other:?}"),
        }
        assert!(!state.in_error_state());
    }

    /// Spec §4: Close with an UNKNOWN target byte → `BadDescribeTarget`
    /// → `08P01 protocol_violation`. Error_state engaged per spec §6
    /// (same shape as Describe with bad target).
    #[test]
    fn t6_dispatch_close_unknown_target_byte_returns_08p01_bad_target() {
        let mut state = SessionState::new();
        let msg = proto::ExtqMessage::Close {
            target: b'X', // neither 'S' nor 'P'
            name: "irrelevant".to_string(),
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
            ExtqOutcome::Failed(ExtqError::BadDescribeTarget { target }) => {
                assert_eq!(target, b'X');
            }
            other => panic!("expected BadDescribeTarget, got {other:?}"),
        }
        assert!(state.in_error_state());
    }

    /// Spec §6: when the dispatcher is in `error_state == true`, a
    /// Close message is silently dropped (`ExtqOutcome::Skipped`)
    /// WITHOUT mutating any state. The error_state flag is NOT
    /// cleared (only Sync clears it).
    #[test]
    fn t6_dispatch_close_in_error_state_returns_skipped_without_processing() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "ps", "SELECT 1", vec![]);
        state.set_error_state(true);
        let msg = proto::ExtqMessage::Close {
            target: DESCRIBE_TARGET_STATEMENT,
            name: "ps".to_string(),
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, msg) {
            ExtqOutcome::Skipped => {}
            other => panic!("expected Skipped, got {other:?}"),
        }
        // Statement still present — Close was skipped.
        assert!(state.get_statement("ps").is_some());
        assert!(state.in_error_state());
    }

    /// Spec §4 + PG §55.2.3: Flush returns `ExtqOutcome::Flush` with
    /// NO bytes and NO state mutation. The caller is expected to call
    /// `writer.flush()` without writing anything.
    #[test]
    fn t6_dispatch_flush_returns_flush_outcome_with_no_state_mutation() {
        let mut state = SessionState::new();
        // Seed some state to verify Flush doesn't mutate it.
        seed_stmt_with_sql(&mut state, "ps", "SELECT 1", vec![]);
        try_dispatch_extq(
            &mut state,
            &NoSchemaEngine,
            proto::ExtqMessage::Bind {
                portal: "pt".to_string(),
                stmt: "ps".to_string(),
                param_formats: vec![],
                param_values: vec![],
                result_formats: vec![],
            },
        );
        let stmt_count_before = state.statement_count();
        let portal_count_before = state.portal_count();
        match try_dispatch_extq(&mut state, &NoSchemaEngine, proto::ExtqMessage::Flush) {
            ExtqOutcome::Flush => {}
            other => panic!("expected ExtqOutcome::Flush, got {other:?}"),
        }
        // State unchanged.
        assert_eq!(state.statement_count(), stmt_count_before);
        assert_eq!(state.portal_count(), portal_count_before);
        assert!(!state.in_error_state());
    }

    /// Spec §6: Flush in error_state is dispatched as `Skipped` — the
    /// skip-until-Sync invariant is in force; Flush is NOT a Sync. The
    /// dispatcher's pre-check (top of `try_dispatch_extq`) catches this
    /// before reaching `dispatch_flush`.
    #[test]
    fn t6_dispatch_flush_in_error_state_returns_skipped() {
        let mut state = SessionState::new();
        state.set_error_state(true);
        match try_dispatch_extq(&mut state, &NoSchemaEngine, proto::ExtqMessage::Flush) {
            ExtqOutcome::Skipped => {}
            other => panic!("expected Skipped, got {other:?}"),
        }
        // Error_state still engaged (only Sync clears).
        assert!(state.in_error_state());
    }

    /// HEADLINE T6 KAT: a full Parse + Bind + Execute + Close + Sync
    /// pipeline through the dispatcher composes correctly. Locks the
    /// T5 → T6 transition — every extq tag now produces real bytes.
    /// The byte concat is ParseComplete + BindComplete +
    /// RowDescription + DataRow* + CommandComplete + CloseComplete +
    /// RFQ.
    #[test]
    fn t6_dispatch_parse_bind_execute_close_sync_full_round_trip() {
        let mut state = SessionState::new();
        let engine = build_canned_rows(2);
        let pc = try_dispatch_extq(
            &mut state,
            &engine,
            proto::ExtqMessage::Parse {
                name: "ps".to_string(),
                sql: "SELECT * FROM t".to_string(),
                param_oids: vec![],
            },
        );
        let pc_bytes = match pc {
            ExtqOutcome::Bytes(b) => b,
            other => panic!("Parse: {other:?}"),
        };
        let bc = try_dispatch_extq(
            &mut state,
            &engine,
            proto::ExtqMessage::Bind {
                portal: "pt".to_string(),
                stmt: "ps".to_string(),
                param_formats: vec![],
                param_values: vec![],
                result_formats: vec![],
            },
        );
        let bc_bytes = match bc {
            ExtqOutcome::Bytes(b) => b,
            other => panic!("Bind: {other:?}"),
        };
        let ec = try_dispatch_extq(
            &mut state,
            &engine,
            proto::ExtqMessage::Execute {
                portal: "pt".to_string(),
                max_rows: 0,
            },
        );
        let ec_bytes = match ec {
            ExtqOutcome::Bytes(b) => b,
            other => panic!("Execute: {other:?}"),
        };
        // Close the named portal.
        let cc = try_dispatch_extq(
            &mut state,
            &engine,
            proto::ExtqMessage::Close {
                target: DESCRIBE_TARGET_PORTAL,
                name: "pt".to_string(),
            },
        );
        let cc_bytes = match cc {
            ExtqOutcome::Bytes(b) => b,
            other => panic!("Close: {other:?}"),
        };
        let sc = try_dispatch_extq(&mut state, &engine, proto::ExtqMessage::Sync);
        let sc_bytes = match sc {
            ExtqOutcome::Bytes(b) => b,
            other => panic!("Sync: {other:?}"),
        };

        let mut wire = Vec::new();
        wire.extend_from_slice(&pc_bytes);
        wire.extend_from_slice(&bc_bytes);
        wire.extend_from_slice(&ec_bytes);
        wire.extend_from_slice(&cc_bytes);
        wire.extend_from_slice(&sc_bytes);

        // Locked checks:
        // - First byte: '1' (ParseComplete).
        assert_eq!(wire[0], b'1');
        // - CloseComplete envelope (`3 00 00 00 04`) appears.
        let close_complete: &[u8] = &[b'3', 0, 0, 0, 4];
        assert!(
            wire.windows(close_complete.len())
                .any(|w| w == close_complete),
            "wire stream must carry CloseComplete envelope after Execute"
        );
        // - Trailing 6 bytes: RFQ('I').
        assert_eq!(&wire[wire.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
        // - Portal was dropped by Close.
        assert!(state.get_portal("pt").is_none());
        // - Statement persists (Close on portal does not cascade).
        assert!(state.get_statement("ps").is_some());

        // No error-status codes anywhere.
        for code in [b"0A000", b"26000", b"34000", b"08P01"] {
            assert!(!wire.windows(5).any(|w| w == code),
                "wire stream must not contain {code:?}");
        }
    }

    /// Pipelined Close of multiple statements within one Sync block:
    /// Close('S','a') + Close('S','b') + Sync emits 2× CloseComplete
    /// followed by RFQ. Locks the order-preserving pipelining
    /// invariant + the multi-Close composition.
    #[test]
    fn t6_dispatch_pipelined_close_multiple_stmts_in_one_sync_block() {
        let mut state = SessionState::new();
        seed_stmt_with_sql(&mut state, "a", "SELECT 1", vec![]);
        seed_stmt_with_sql(&mut state, "b", "SELECT 2", vec![]);
        assert_eq!(state.statement_count(), 2);

        let c1 = try_dispatch_extq(
            &mut state,
            &NoSchemaEngine,
            proto::ExtqMessage::Close {
                target: DESCRIBE_TARGET_STATEMENT,
                name: "a".to_string(),
            },
        );
        let c1_bytes = match c1 {
            ExtqOutcome::Bytes(b) => b,
            other => panic!("Close(a): {other:?}"),
        };
        let c2 = try_dispatch_extq(
            &mut state,
            &NoSchemaEngine,
            proto::ExtqMessage::Close {
                target: DESCRIBE_TARGET_STATEMENT,
                name: "b".to_string(),
            },
        );
        let c2_bytes = match c2 {
            ExtqOutcome::Bytes(b) => b,
            other => panic!("Close(b): {other:?}"),
        };
        let s = try_dispatch_extq(&mut state, &NoSchemaEngine, proto::ExtqMessage::Sync);
        let s_bytes = match s {
            ExtqOutcome::Bytes(b) => b,
            other => panic!("Sync: {other:?}"),
        };

        let mut wire = Vec::new();
        wire.extend_from_slice(&c1_bytes);
        wire.extend_from_slice(&c2_bytes);
        wire.extend_from_slice(&s_bytes);

        // EXACTLY two CloseComplete envelopes (5 bytes each), in order,
        // followed by the 6-byte RFQ('I').
        let expected: Vec<u8> = vec![
            b'3', 0, 0, 0, 4, // CloseComplete 1
            b'3', 0, 0, 0, 4, // CloseComplete 2
            b'Z', 0, 0, 0, 5, b'I', // RFQ('I')
        ];
        assert_eq!(wire, expected);
        // Both statements dropped.
        assert_eq!(state.statement_count(), 0);
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-EXTQ T7 — Sync state-machine + error-attribution edge cases.
    //
    // The V1 message set (T1..T6) ships a working state machine — error
    // sets `state.error_state = true`, subsequent non-Sync messages
    // return `Skipped`, Sync clears the flag + emits RFQ. T7 audits the
    // less-obvious shapes:
    //
    //  - Two consecutive errors before Sync → second error MUST NOT
    //    re-emit ErrorResponse; the second message goes through Skipped.
    //  - Sync with no preceding error → bare RFQ, no error_state to
    //    clear (defensive — Sync is idempotent on a clean state).
    //  - Bind error followed by Execute on the SAME portal name → the
    //    Execute is Skipped (it cannot resolve the portal because Bind
    //    never stored it; even if a stale portal existed, Skipped wins
    //    because error_state is set).
    //  - Multiple Parse errors in one pipeline before Sync → each error
    //    is silently skipped without recursive error_state re-engagement.
    //  - After Sync clears error_state, the next Parse must succeed
    //    cleanly (no latent skip-mode bug).
    //
    // These edge cases were named in the SP-PG-EXTQ design spec §11
    // weak-spot #9 (pipelined-error attribution). T7's audit confirms
    // the existing implementation handles them correctly — no code
    // change required, just lock the invariants against future drift.
    // ───────────────────────────────────────────────────────────────────

    /// T7 audit #1: two consecutive errors before Sync. First error
    /// returns `Failed(...)` + sets `error_state = true`. Second
    /// (different) error-producing message returns `Skipped` — NOT
    /// another `Failed(...)`. Locks the no-re-emit invariant.
    #[test]
    fn t7_consecutive_errors_before_sync_second_skipped_not_re_emitted() {
        let mut state = SessionState::new();
        // First error: Bind to a non-existent statement → UnknownStatement.
        let bind1 = proto::ExtqMessage::Bind {
            portal: "p1".to_string(),
            stmt: "ghost1".to_string(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, bind1) {
            ExtqOutcome::Failed(ExtqError::UnknownStatement { name }) => {
                assert_eq!(name, "ghost1");
            }
            other => panic!("first Bind expected UnknownStatement, got {other:?}"),
        }
        assert!(state.in_error_state(), "error_state must engage on first error");

        // Second message that WOULD ALSO error (Bind to a different
        // missing statement) — but the pre-skip check intercepts it
        // BEFORE the dispatcher runs. Outcome is `Skipped`, NOT a
        // second UnknownStatement.
        let bind2 = proto::ExtqMessage::Bind {
            portal: "p2".to_string(),
            stmt: "ghost2".to_string(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, bind2) {
            ExtqOutcome::Skipped => {}
            other => panic!("second Bind expected Skipped, got {other:?}"),
        }
        // error_state still engaged (only Sync clears).
        assert!(state.in_error_state(), "error_state stays engaged through Skipped");
    }

    /// T7 audit #2: Sync after no errors — emits bare RFQ('I'), clears
    /// the (already-clear) error_state, drops unnamed portal (which
    /// didn't exist). Idempotent on a clean session — locks against
    /// future drift where Sync would side-effect more state than spec'd.
    #[test]
    fn t7_sync_with_no_preceding_error_emits_rfq_idempotent() {
        let mut state = SessionState::new();
        // Seed a NAMED statement + named portal to confirm they're
        // preserved across an "idempotent" Sync (Sync only drops the
        // unnamed "" portal per spec §3 — named state survives).
        seed_stmt(&mut state, "named_stmt", vec![]);
        // Bind a named portal.
        let bind = proto::ExtqMessage::Bind {
            portal: "named_portal".to_string(),
            stmt: "named_stmt".to_string(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        assert!(matches!(
            try_dispatch_extq(&mut state, &NoSchemaEngine, bind),
            ExtqOutcome::Bytes(_)
        ));
        assert!(!state.in_error_state(), "must start clean");
        assert_eq!(state.statement_count(), 1);
        assert_eq!(state.portal_count(), 1);

        // Sync on a clean state.
        match try_dispatch_extq(&mut state, &NoSchemaEngine, proto::ExtqMessage::Sync) {
            ExtqOutcome::Bytes(b) => {
                assert_eq!(b, vec![b'Z', 0, 0, 0, 5, b'I']);
            }
            other => panic!("Sync on clean state expected Bytes(RFQ), got {other:?}"),
        }
        // error_state still false.
        assert!(!state.in_error_state());
        // Named statement preserved.
        assert!(state.get_statement("named_stmt").is_some());
        // Named portal preserved (only "" gets dropped).
        assert!(state.get_portal("named_portal").is_some());
    }

    /// T7 audit #3: Bind error followed by Execute against the same
    /// portal name → Execute is Skipped. The portal was never stored
    /// (Bind failed before insert), AND `error_state` is engaged, so
    /// the Execute hits the pre-skip arm regardless of portal lookup.
    #[test]
    fn t7_bind_error_then_execute_same_portal_name_is_skipped() {
        let mut state = SessionState::new();
        // Bind to a non-existent statement → UnknownStatement +
        // error_state engaged. Portal name "p1" is NOT stored because
        // the dispatcher errored before insert.
        let bind = proto::ExtqMessage::Bind {
            portal: "p1".to_string(),
            stmt: "missing_stmt".to_string(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        assert!(matches!(
            try_dispatch_extq(&mut state, &NoSchemaEngine, bind),
            ExtqOutcome::Failed(ExtqError::UnknownStatement { .. })
        ));
        assert!(state.in_error_state());
        // Portal p1 was NOT stored.
        assert!(state.get_portal("p1").is_none(), "Bind must not have stored p1 on error");

        // Execute against "p1" — Skipped (error_state pre-empts).
        let exec = proto::ExtqMessage::Execute {
            portal: "p1".to_string(),
            max_rows: 0,
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, exec) {
            ExtqOutcome::Skipped => {}
            other => panic!("Execute after Bind error expected Skipped, got {other:?}"),
        }
        // No portal was ever stored, no state mutation happened.
        assert!(state.get_portal("p1").is_none());
    }

    /// T7 audit #4: multiple Parse errors in one pipeline before Sync.
    /// First Parse: an actual error doesn't happen because Parse only
    /// fails on cap overflow or named collision. Instead simulate via
    /// a Bind error (already covered) but ALSO verify that REPEATED
    /// errors of the same kind don't drift error_state recursively.
    /// Locks the "error_state is a latching bool, not a counter" shape.
    #[test]
    fn t7_repeated_errors_keep_error_state_a_latching_bool() {
        let mut state = SessionState::new();
        // Cause 3 errors in a row (each different) — each subsequent
        // must Skip, none must re-engage error_state recursively.
        let m1 = proto::ExtqMessage::Bind {
            portal: "p1".to_string(),
            stmt: "ghost1".to_string(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        let m2 = proto::ExtqMessage::Describe {
            target: DESCRIBE_TARGET_STATEMENT,
            name: "ghost2".to_string(),
        };
        let m3 = proto::ExtqMessage::Execute {
            portal: "ghost3".to_string(),
            max_rows: 0,
        };
        // First: real error.
        assert!(matches!(
            try_dispatch_extq(&mut state, &NoSchemaEngine, m1),
            ExtqOutcome::Failed(_)
        ));
        assert!(state.in_error_state());
        // Second + third: Skipped (NOT Failed). State remains latched.
        for m in [m2, m3] {
            match try_dispatch_extq(&mut state, &NoSchemaEngine, m) {
                ExtqOutcome::Skipped => {}
                other => panic!("expected Skipped, got {other:?}"),
            }
            assert!(state.in_error_state(), "error_state stays latched");
        }
    }

    /// T7 audit #5: after Sync clears error_state, the next Parse
    /// succeeds cleanly. Locks against a future bug where the skip-
    /// mode flag leaks past Sync (e.g. a future buffered-write rework
    /// that forgets to reset error_state).
    #[test]
    fn t7_sync_then_parse_after_error_succeeds_cleanly() {
        let mut state = SessionState::new();
        // Engage error_state via a Bind to a missing stmt.
        let bind = proto::ExtqMessage::Bind {
            portal: "p1".to_string(),
            stmt: "ghost".to_string(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        assert!(matches!(
            try_dispatch_extq(&mut state, &NoSchemaEngine, bind),
            ExtqOutcome::Failed(_)
        ));
        assert!(state.in_error_state());

        // Sync → bare RFQ, error_state cleared.
        match try_dispatch_extq(&mut state, &NoSchemaEngine, proto::ExtqMessage::Sync) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'Z', 0, 0, 0, 5, b'I']),
            other => panic!("Sync expected RFQ bytes, got {other:?}"),
        }
        assert!(!state.in_error_state(), "Sync must clear error_state");

        // Next Parse must succeed (no latent skip).
        let parse = proto::ExtqMessage::Parse {
            name: "ok_stmt".to_string(),
            sql: "SELECT * FROM t".to_string(),
            param_oids: vec![],
        };
        match try_dispatch_extq(&mut state, &NoSchemaEngine, parse) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'1', 0, 0, 0, 4]),
            other => panic!("post-Sync Parse expected ParseComplete, got {other:?}"),
        }
        assert!(state.get_statement("ok_stmt").is_some());
        assert!(!state.in_error_state());
    }

    /// T7 audit #6: Flush in error_state is also Skipped. Spec §6 +
    /// §4: only Sync escapes skip-until-Sync mode. Flush triggers a
    /// real wire flush in normal mode but produces NO bytes in error
    /// mode (the run_session boundary never sees a Bytes(Vec::new())
    /// or Flush outcome from this path). Locks against a future
    /// "Flush is harmless, just always run it" drift.
    #[test]
    fn t7_flush_in_error_state_is_skipped_not_flush_outcome() {
        let mut state = SessionState::new();
        state.set_error_state(true);
        match try_dispatch_extq(&mut state, &NoSchemaEngine, proto::ExtqMessage::Flush) {
            ExtqOutcome::Skipped => {}
            other => panic!("Flush in error_state expected Skipped, got {other:?}"),
        }
        assert!(state.in_error_state(), "Flush must NOT clear error_state");
    }

    /// T7 audit #7: pipelined success then error then success (with
    /// Sync between the error block and the post-error block). Tests
    /// the canonical pipeline shape every ORM emits: a series of
    /// successes ending with a Sync, then a series potentially
    /// containing an error closed with another Sync, then more
    /// successes.
    #[test]
    fn t7_pipeline_success_error_sync_success_round_trip() {
        let mut state = SessionState::new();

        // Block 1: Parse + Sync → succeeds.
        let p1 = proto::ExtqMessage::Parse {
            name: "s1".to_string(),
            sql: "SELECT * FROM t".to_string(),
            param_oids: vec![],
        };
        assert!(matches!(
            try_dispatch_extq(&mut state, &NoSchemaEngine, p1),
            ExtqOutcome::Bytes(_)
        ));
        assert!(matches!(
            try_dispatch_extq(&mut state, &NoSchemaEngine, proto::ExtqMessage::Sync),
            ExtqOutcome::Bytes(_)
        ));
        assert_eq!(state.statement_count(), 1);

        // Block 2: Bind error + (skipped Describe) + Sync.
        let bind = proto::ExtqMessage::Bind {
            portal: "p_bad".to_string(),
            stmt: "missing".to_string(),
            param_formats: vec![],
            param_values: vec![],
            result_formats: vec![],
        };
        assert!(matches!(
            try_dispatch_extq(&mut state, &NoSchemaEngine, bind),
            ExtqOutcome::Failed(_)
        ));
        let desc = proto::ExtqMessage::Describe {
            target: DESCRIBE_TARGET_STATEMENT,
            name: "s1".to_string(),
        };
        assert!(matches!(
            try_dispatch_extq(&mut state, &NoSchemaEngine, desc),
            ExtqOutcome::Skipped
        ));
        match try_dispatch_extq(&mut state, &NoSchemaEngine, proto::ExtqMessage::Sync) {
            ExtqOutcome::Bytes(b) => assert_eq!(b, vec![b'Z', 0, 0, 0, 5, b'I']),
            other => panic!("Sync expected RFQ, got {other:?}"),
        }
        assert!(!state.in_error_state());

        // Block 3: Parse new stmt + Sync — must succeed.
        let p2 = proto::ExtqMessage::Parse {
            name: "s2".to_string(),
            sql: "SELECT * FROM t".to_string(),
            param_oids: vec![],
        };
        assert!(matches!(
            try_dispatch_extq(&mut state, &NoSchemaEngine, p2),
            ExtqOutcome::Bytes(_)
        ));
        assert!(matches!(
            try_dispatch_extq(&mut state, &NoSchemaEngine, proto::ExtqMessage::Sync),
            ExtqOutcome::Bytes(_)
        ));
        // Both statements persist (Sync drops only unnamed "" portal).
        assert!(state.get_statement("s1").is_some());
        assert!(state.get_statement("s2").is_some());
    }

    /// T7 audit #8: Close in error_state is Skipped. Spec §6 reiterated
    /// across every dispatcher. Locks the rule that even "harmless"
    /// drop-state operations must wait for Sync.
    #[test]
    fn t7_close_in_error_state_is_skipped_state_preserved() {
        let mut state = SessionState::new();
        seed_stmt(&mut state, "ps", vec![]);
        state.set_error_state(true);
        let close = proto::ExtqMessage::Close {
            target: DESCRIBE_TARGET_STATEMENT,
            name: "ps".to_string(),
        };
        assert!(matches!(
            try_dispatch_extq(&mut state, &NoSchemaEngine, close),
            ExtqOutcome::Skipped
        ));
        // ps NOT dropped because Close was skipped.
        assert!(state.get_statement("ps").is_some());
        assert!(state.in_error_state());
    }
}
