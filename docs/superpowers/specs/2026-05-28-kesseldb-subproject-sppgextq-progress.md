# SP-PG-EXTQ — PostgreSQL Extended Query protocol — SP-arc Progress Tracker

Date created: 2026-05-28
Design spec: `docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`
Parent SP-arc: SP-PG (closed 2026-05-27 — Simple Query path); the SP-PG
V1 design spec §2.2 named SP-PG-EXTQ as the single biggest remaining
adoption multiplier. Every modern Postgres ORM (Prisma, Drizzle,
SQLAlchemy, sqlx, Diesel, GORM, psycopg, pgx, JDBC) hard-requires
Extended Query at the protocol-probe phase — today they refuse to
connect against KesselDB V1 even though Simple Query (psql, pgcli)
works end-to-end.

## What this SP-arc ships

V1 = "every modern PG ORM connects to KesselDB and runs prepared
statements against it." After V1 lands (T1..T12), a PG client speaking
the Extended Query subset of the v3.0 protocol can:

1. Send `P` Parse with a SQL string + optional parameter type OID
   hints; server replies `1` ParseComplete; statement is stored
   under the supplied name (or in the unnamed/volatile slot).
2. Send `B` Bind with per-position parameter format codes + text-
   format parameter values; server replies `2` BindComplete;
   portal is stored under the supplied name.
3. Send `D` Describe `'S'` (statement) or `'P'` (portal); server
   replies `t` ParameterDescription (statements only) + `T`
   RowDescription / `n` NoData.
4. Send `E` Execute with optional `max_rows` truncation; server
   replies `D` DataRow×N + either `C` CommandComplete (rows
   exhausted) or `s` PortalSuspended (`max_rows` hit and more
   rows remain — client can re-Execute the same portal to
   continue).
5. Send `S` Sync — server flushes + emits `Z` ReadyForQuery + resets
   per-Sync error state. Mandatory flush point at the end of every
   client-initiated pipeline.
6. Send `C` Close `'S'`/`'P'` — server drops the statement or
   portal + emits `3` CloseComplete.
7. Send `H` Flush — server flushes pending output without resetting
   error state.
8. Pipeline arbitrarily many P/B/D/E/C/H messages without waiting
   for replies between them; server processes in arrival order and
   emits replies in arrival order.
9. Recover from a pipelined error: server silently skips every P/
   B/D/E/C/H after the error until it sees Sync; on Sync emits
   ReadyForQuery and resumes normal processing.
10. Coexist Simple Query (`Q`) and Extended Query on the same
    connection arbitrarily.

**Out-of-scope (named, deferred — each is its own arc):**
binary-format parameters (V2 `SP-PG-EXTQ-BIN`), server-side
prepared-statement cache across reconnect (V2 `SP-PG-EXTQ-CACHE`),
parameter-AST in `kessel-sql` (V2 `SP-PG-EXTQ-PARSED`),
transaction-block awareness (V2 `SP-PG-TX`), COPY in extended-query
(V2 `SP-PG-COPY`), large-object protocol (permanent hard pass),
real streaming cursors (SP-A T14 streaming-rows). See design spec §2.2.

## Slice plan (mirrors design spec §10)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec (816 LoC, 10 weak-spots + 5 open questions) + scaffold (`crates/kessel-pg-gateway/src/extq/mod.rs` with `SessionState`/`PreparedStmt`/`Portal`/`ExecState` + locked caps + `recognize_extq_tag` + placeholder `try_dispatch_extq` returning `NotYetImplemented`, `extq/proto.rs` with decoders for all 7 frontend messages, `extq/response.rs` with encoders for ParseComplete/BindComplete/CloseComplete/NoData/PortalSuspended/ParameterDescription) + `proto.rs` BE_CLOSE_COMPLETE constant + `server.rs::run_session` routes recognized extq tags to `try_dispatch_extq` and renders NYI as `0A000` while keeping the session alive (tolerant probe-then-fall-back unlocks SQLAlchemy/psycopg/JDBC probe pattern) + 37 KATs locking spec invariants. | **DONE** | `3691242` (spec) + `975c696` (scaffold) |
| **T2** | Parse + ParseComplete e2e: real `try_dispatch_extq` arm for `P`; named/unnamed statement storage in `SessionState.statements`; ParseComplete emit; `08P01` for `MAX_PREPARED_STATEMENTS_PER_CONN` overflow + decode errors; `42P05 prepared_statement_already_exists` for non-empty-name collision; lock the "Parse stores SQL VERBATIM" invariant; flip T1 regression-lock to "T2 emits ParseComplete for valid Parse". | **DONE** | `688f961` (dispatcher + KATs) + `1b7ad07` (server.rs wire-up) |
| **T3** | Bind + BindComplete e2e: portal storage in `SessionState.portals`; per-position param-format validation (V1 rejects format code 1 with `0A000`); param-value extraction including NULL sentinel (`length=-1`); BindComplete emit; cap enforcement. | **OPEN** | — |
| **T4** | Describe 'S' → ParameterDescription + RowDescription/NoData: schema lookup via existing `EngineApply::describe_table` + `kessel_sql::select_star_table`; emit ParameterDescription with the OID hints from Parse (or empty if Parse didn't provide); NoData for non-SELECT statements. | **OPEN** | — |
| **T5** | Describe 'P' → RowDescription/NoData: same shape as Describe 'S' but no ParameterDescription (portals don't carry parameter info per PG spec — Bind already substituted). | **OPEN** | — |
| **T6** | Execute + parameter substitution + result streaming: text-format substitution via new `extq/substitute.rs` (~15 KATs against the §4 edge corpus); dispatch through `dispatch::dispatch_query`; emit DataRow* + CommandComplete; portal cursor state machine. | **OPEN** | — |
| **T7** | Sync + ReadyForQuery + error recovery state machine: flush per-Sync output buffer; reset `error_state` on Sync; `08P01` for unsupported subprotocol tags inside a Sync block; emit `Z('I')`; the SkipUntilSync loop in `try_dispatch_extq`. | **OPEN** | — |
| **T8** | Close ('S'/'P') + CloseComplete + Flush: drop stmt/portal from `SessionState`; CloseComplete emit; Flush is a no-op-emit that triggers a stream flush at the `server::run_session` boundary. | **OPEN** | — |
| **T9** | max_rows pagination + PortalSuspended + cursor preservation: Execute(max_rows=N) buffers + pages; PortalSuspended emit; second Execute on same portal continues from buffered cursor. | **OPEN** | — |
| **T10** | Pipelining stress test + real libpq round-trip: 100-message pipeline through one connection; ordering preserved; output buffer correctness under interleaved P/B/D/E/C/H. Manual psql verification of PREPARE/EXECUTE simple-query path (regression check that SP-PG V1 didn't break). | **OPEN** | — |
| **T11** | SQLAlchemy/psycopg connect-probe end-to-end: real `engine.connect()` against a running kesseldb-server with pg-gateway feature; capture probe sequence; assert NO `08P01` in response stream; commit recorded transcript as a fixture. | **OPEN** | — |
| **T12** | JDBC / Drizzle / Prisma compat smoke + USAGE update + arc closure: doc results in USAGE.md, log any compat gaps as named follow-ups, mark this progress tracker → CLOSED, update STATUS.md row + bullet. | **OPEN** | — |

Optional / V2 follow-ups (each its own arc):

- **SP-PG-EXTQ-BIN (V2)** — binary-format parameters (format code 1).
  ~2 slices. int / float / bool / text / timestamp first; numeric
  last (PG binary numeric is base-10000 variable-length-digit and
  bug-prone).
- **SP-PG-EXTQ-CACHE (V2)** — server-side prepared-statement cache
  that survives reconnect. Almost no ORM relies on this — they all
  re-Parse on reconnect — but libpq supports it and a future
  high-stmt-churn workload may want it. ~2 slices.
- **SP-PG-EXTQ-PARSED (V2)** — extend `kessel-sql` with a parameter-
  AST node so `$1` is a typed placeholder the planner sees, not a
  literal substituted at Execute time. Eliminates the SQL-text
  substitution attack surface (spec §11 weak-spot #2) + improves
  error messages. ~2-3 slices.
- **SP-PG-TX (V2)** — transaction-block awareness: `Z('T')` /
  `Z('E')` status bytes; implicit-tx semantics where extended-query
  messages within one Sync form an implicit transaction. ~2 slices.
- **SP-PG-COPY (V2)** — COPY FROM STDIN / COPY TO STDOUT bulk
  protocol. ~2-3 slices.

## T1 — what landed (2026-05-28, commits `3691242` + `975c696`)

**Two commits, ~2273 LoC net delta:**

### Commit `3691242` — design spec (816 lines, no code change):
`docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`
covers:

- **§1 Context** — the failing SQLAlchemy probe sequence captured
  against KesselDB V1 (the headline motivation); full PG-client
  ecosystem table (15 rows: psql, pgcli, psycopg, SQLAlchemy,
  Django ORM, JDBC, pgx, lib/pq, Node pg, postgres.js, tokio-
  postgres, sqlx, Prisma, Drizzle, dbt) — every row except the
  first two unlocked by this arc.
- **§2 Scope** — V1 in (12 items: full message set, named/unnamed
  stmts+portals, text-format params, SQL-text substitution at
  Execute, pipelining, error recovery via Sync, statement+portal
  reuse, PortalSuspended pagination, lifecycle auto-drop, memory
  bounds, coexistence with Simple Query); V1 out (5 named V2+
  arcs).
- **§3 Wire-state machine** — per-connection `SessionState` with
  `HashMap<String, PreparedStmt>` + `HashMap<String, Portal>` +
  `error_state: bool`; `PreparedStmt { sql, param_oids }`;
  `Portal { stmt_name, param_values: Vec<Option<Vec<u8>>>,
  param_formats, result_formats, exec_state }`; `ExecState ::=
  Pending | Buffered { rows, cursor } | Exhausted { total }`;
  empty-name `""` is the volatile slot for both statements and
  portals.
- **§4 Parameter substitution** — text-format `$N` replacement at
  Execute time with single-quote escaping (`'` → `''`); NULL
  renders as bare `NULL`; 7-row substitution table + 5 documented
  edge cases (identifier substitution forbidden, NULL-in-WHERE
  three-valued logic, binary-format reject, quoted-`$1`-in-
  comments, parameter-used-multiple-times).
- **§5 Pipelining** — request-pipelined not concurrent; server
  processes + emits in arrival order; eager-flush per-message in
  V1 (simpler, matches V1's existing run_session shape).
- **§6 Error recovery** — single ErrorResponse emit + skip-until-
  Sync loop; on Sync emit `Z('I')` (V1) — V2 SP-PG-TX would emit
  `Z('E')` failed-tx.
- **§7 Memory bounds** — `MAX_PREPARED_STATEMENTS_PER_CONN=4096`,
  `MAX_PORTALS_PER_CONN=4096`, `MAX_PARAMETERS_PER_BIND=u16::MAX`,
  SQL-text cap inherits V1's `PG_MAX_MESSAGE_SIZE=16 MiB`;
  max_rows pagination is buffered-then-paged in V1.
- **§8 Wire decoders** — 7 decoders + canonical wire formats per
  PG §55.7.
- **§9 Wire encoders** — 6 encoders (5 trivial 5-byte envelopes +
  ParameterDescription with variable-length OID list).
- **§10 Task decomposition** — T1..T12 with per-slice KAT-delta
  estimates (~60-90 KATs total V1).
- **§11 Self-review — 10 weak spots**: (1) SQL-text substitution
  brittleness; (2) SQL-injection prevention via escape (V2
  SP-PG-EXTQ-PARSED eliminates); (3) unnamed-portal allocator
  churn under pathological pipelining; (4) no flow control on
  Execute streaming; (5) PortalSuspended buffered-not-real (SP-A
  T14 fixes); (6) DISCARD ALL ignored; (7) SP47 epoch coupling
  needed for V2 caching; (8) no cancel during long Execute;
  (9) pipelined-skip-after-error makes errors hard to attribute;
  (10) OID hints from Parse ignored at Bind.
- **§12 Open questions** — DISCARD ALL Simple-Query interception;
  server-side PREPARE SQL (orthogonal to extq); max_rows=1 fetch-
  one pattern; per-connection stmt count × connection pool
  multiplication; empty-SQL Parse semantics.
- **§13 Acceptance criteria** — 11 items including psql PREPARE/
  EXECUTE smoke, psycopg round-trip, SQLAlchemy probe, Prisma
  `db pull`, 100-message pipelining, error-recovery, memory-cap
  enforcement, no Simple-Query regression, zero-dep stance, seed-7
  green, tree-grep empty.

### Commit `975c696` — scaffold (1457 LoC across 6 files):

**`crates/kessel-pg-gateway/src/extq/mod.rs` (445 LoC):**
- `MAX_PREPARED_STATEMENTS_PER_CONN = 4096`
- `MAX_PORTALS_PER_CONN = 4096`
- `MAX_PARAMETERS_PER_BIND = u16::MAX as usize`
- `SessionState { statements, portals, error_state }` per-connection
  state struct with `new()`/`statement_count()`/`portal_count()`/
  `in_error_state()` read-only accessors.
- `PreparedStmt { sql, param_oids }` — V1 stores SQL VERBATIM (no
  AST cache); engine re-parses on every Execute (SP47 compile-
  cache de-duplicates inside the engine).
- `Portal { stmt_name, param_values, param_formats, result_formats,
  exec_state }`.
- `ExecState ::= Pending | Buffered { rows, cursor } | Exhausted
  { total }` (default = Pending).
- `ExtqError ::= NotYetImplemented { tag } | Decode { reason } |
  TooManyPreparedStatements | TooManyPortals | BinaryFormatNotSupported
  { position } | UnknownStatement { name } | UnknownPortal { name }`.
- `ExtqOutcome ::= Bytes(Vec<u8>) | Failed(ExtqError) | SyncCompleted`.
- `recognize_extq_tag(tag) -> bool` — returns true iff `tag` is one
  of the seven FE_PARSE / FE_BIND / FE_DESCRIBE / FE_EXECUTE /
  FE_SYNC / FE_CLOSE / FE_FLUSH tags from `crate::proto`.
- `try_dispatch_extq(state, message) -> ExtqOutcome` — placeholder
  dispatcher. Every variant returns `Failed(NotYetImplemented {
  tag })`. T2+ widens each arm.
- **5 KATs** locking: `recognize_extq_tag` accepts exactly the 7
  extq tags + rejects everything else (including all 14 frontend
  + all 15 backend tags from `proto.rs`); caps are in
  `[1024, 65536]` range; `SessionState::new()` starts empty and
  not in error; `ExecState` default is Pending; `try_dispatch_extq`
  returns the correct tag in `NotYetImplemented { tag }` for every
  ExtqMessage variant + does NOT mutate state.

**`crates/kessel-pg-gateway/src/extq/proto.rs` (692 LoC):**
- `ExtqMessage` enum — 7 variants (Parse / Bind / Describe /
  Execute / Sync / Close / Flush) each carrying the decoded fields
  per PG §55.7.
- `DecodeError ::= UnexpectedEnd | MissingNul | InvalidUtf8 |
  NegativeCount | BadDescribeTarget | TrailingBytes`.
- `decode_parse(body) -> Result<ExtqMessage, DecodeError>` —
  `[name:cstring] [sql:cstring] [count:i16] [oid:u32]*`.
- `decode_bind(body)` — portal + stmt cstrings + per-position
  param-format codes + per-position param values (length-prefixed;
  `-1` length = NULL) + per-position result-format codes.
- `decode_describe(body)` — target byte `'S'`/`'P'` + name cstring;
  rejects bad target with `BadDescribeTarget`.
- `decode_execute(body)` — portal cstring + max_rows i32.
- `decode_sync(body)` — empty body validator.
- `decode_close(body)` — same shape as Describe.
- `decode_flush(body)` — empty body validator.
- Internal zero-dep `Cursor<'a>` byte reader (private) — mirrors
  the `query::parse_query_body` pattern.
- **19 KATs** covering canonical libpq byte patterns + every
  rejection branch (missing-NUL, truncated-OID, invalid-UTF-8,
  bad describe target, trailing bytes on Sync/Flush) + a
  libpq-canonical Parse+Bind+Execute+Sync pipeline that locks
  the four decoders compose end-to-end.

**`crates/kessel-pg-gateway/src/extq/response.rs` (220 LoC):**
- `encode_parse_complete()` → 5 bytes `1 [length=4]`.
- `encode_bind_complete()` → 5 bytes `2 [length=4]`.
- `encode_close_complete()` → 5 bytes `3 [length=4]`.
- `encode_no_data()` → 5 bytes `n [length=4]`.
- `encode_portal_suspended()` → 5 bytes `s [length=4]`.
- `encode_parameter_description(oids)` → `t [length] [count:i16]
  [oid:i32]*` (7 + 4·N bytes total).
- **9 KATs** byte-locking each encoder against PG §55.7 canonical
  shape + a "tags are distinct" cross-check + an "all trivial-
  envelope lengths are 4" cross-check.

**`crates/kessel-pg-gateway/src/proto.rs` (+6 LoC):**
- `BE_CLOSE_COMPLETE: u8 = b'3'` constant (only BE tag from the
  spec missing from V1's catalog) + KAT.

**`crates/kessel-pg-gateway/src/server.rs` (+93 LoC):**
- New branch in `run_session` query loop:
  `other if crate::extq::recognize_extq_tag(other) => { ... }`.
  Calls `try_dispatch_extq` (T1 returns NYI), renders as
  `0A000 feature_not_supported` ErrorResponse + ReadyForQuery,
  KEEPS the session alive (`continue` not `return Err`). This
  unblocks SQLAlchemy/psycopg/JDBC probe-then-fall-back patterns
  where the client sends ONE extq probe and, if rejected, falls
  back to Simple Query.
- Existing T8 regression KAT `t8_run_session_unknown_message_tag_emits_08p01`
  FLIPPED to `t1_extq_run_session_parse_tag_emits_0a000_and_stays_alive`
  to lock the new tolerant behavior. New KAT
  `t1_run_session_genuinely_unknown_tag_still_closes_with_08p01`
  uses a backend-only `Z` tag to verify the old "unknown = close"
  behavior is preserved for real protocol violations.

**`crates/kessel-pg-gateway/src/lib.rs` (+1 LoC):**
- `pub mod extq;` declaration.

### Test counts (release on vulcan, 2026-05-28):

| Surface | Before T1 | After T1 | Delta |
|---|---|---|---|
| `kessel-pg-gateway` crate | 337 | 374 | +37 |
| Workspace default | 1792 | 1829 | +37 |
| Workspace `--features pg-gateway` | 1820 | 1857 | +37 |
| Workspace `--all-features` | 1875 | 1912 | +37 |

`kessel-sim` seed-7 GREEN (3 / 3); default tree-grep EMPTY (no new
external deps); `#![forbid(unsafe_code)]` honored across all new
modules.

## T2 — what landed (2026-05-28, commits `688f961` + `1b7ad07`)

**Two commits, +674 LoC net delta across 2 files (mod.rs +388, server.rs +286 incl. KATs):**

### Commit `688f961` — Parse dispatcher arm + KATs

`crates/kessel-pg-gateway/src/extq/mod.rs`:

- New `ExtqError::PreparedStatementAlreadyExists { name: String }`
  variant — Spec §3 / PG §55.2.3: re-Parse on a NON-EMPTY name
  that already exists rejects with SQLSTATE `42P05`. The empty-
  name `""` slot is the volatile exception (silently replaced).
- `try_dispatch_extq` Parse arm now calls a real `dispatch_parse`
  helper that enforces, in order:
  1. **Cap check** (fresh-name only): if `name` is fresh AND
     `state.statements.len() >= MAX_PREPARED_STATEMENTS_PER_CONN`
     → `TooManyPreparedStatements` → `08P01`. The fresh-name rule
     is intentional — overwriting the unnamed `""` slot (or any
     existing named slot, though that returns 42P05 first) does
     NOT grow the map and so does NOT count against the cap.
  2. **Name collision** (named only): non-empty name already
     present → `PreparedStatementAlreadyExists` → `42P05`. The
     original statement is preserved (no clobber).
  3. **Store verbatim**: `PreparedStmt { sql, param_oids }`
     inserted into `state.statements`. No SQL parse, no AST
     cache, no normalization — spec §3 + spec §10 self-review
     #1 explicitly defer SQL parse errors to Execute time so the
     engine catalog state at Execute (not Parse) governs error
     messages.
  4. **ParseComplete emit**: 5-byte `1 [length=4]` envelope from
     the existing `response::encode_parse_complete` (byte-locked
     in T1 KATs).
- New `SessionState::get_statement(name) -> Option<&PreparedStmt>`
  read-only accessor — T2 KATs use this to confirm stored state;
  T3+ Bind path will reuse it to resolve the target statement
  without exposing the storage HashMap.
- The other six dispatch arms (Bind / Describe / Execute / Sync /
  Close / Flush) STILL return `NotYetImplemented` — T3..T8 widen
  them per the §10 task decomposition.

**+8 KATs (lib-level):**

1. T1 `t1_try_dispatch_returns_not_yet_implemented_for_every_tag`
   FLIPPED → T2 `t2_try_dispatch_returns_not_yet_implemented_for_the_six_non_parse_tags`
   (Parse arm removed; the six remaining tags still locked as NYI).
2. T2 `t2_dispatch_parse_unnamed_emits_parse_complete_and_stores_statement`
   — happy path, byte-locked output + state mutation.
3. T2 `t2_dispatch_parse_named_stores_under_supplied_name_with_oids`
   — named-slot storage + OID carry-through.
4. T2 `t2_dispatch_parse_named_collision_returns_already_exists`
   — 42P05 + original-stmt-preserved invariant.
5. T2 `t2_dispatch_parse_unnamed_overwrites_previous_unnamed_statement`
   — silent replace path.
6. T2 `t2_dispatch_parse_empty_sql_is_accepted` — §12 OQ #5.
7. T2 `t2_dispatch_parse_stores_sql_verbatim_no_normalization`
   — §3 verbatim-storage invariant.
8. T2 `t2_dispatch_parse_rejects_when_cap_reached` — at-cap
   success + over-cap rejection on the EXACT boundary.
9. T2 `t2_dispatch_parse_at_cap_allows_unnamed_overwrite` — cap
   check applies to FRESH names only; overwriting at-cap is fine.

### Commit `1b7ad07` — server.rs wire-up + KATs

`crates/kessel-pg-gateway/src/server.rs`:

- New `let mut extq_state = crate::extq::SessionState::new();` at
  the START of `run_session` (after the SCRAM handshake). The
  state lives for the lifetime of the connection and drops
  cleanly on Terminate / EOF / I/O error per spec §3.
- The extq tag branch now decodes the body via the matching
  `extq::proto::decode_*` per the tag, dispatches through
  `try_dispatch_extq`, and routes the outcome:
  - `Bytes(ParseComplete)` → write verbatim, flush.
  - `Failed(NotYetImplemented { tag })` → `0A000` ErrorResponse
    + RFQ (stay alive).
  - `Failed(TooManyPreparedStatements)` → `08P01` with the cap
    in the message + RFQ.
  - `Failed(PreparedStatementAlreadyExists { name })` → `42P05`
    + RFQ.
  - `Failed(Decode { reason })` / decoder pre-dispatch rejection
    → `08P01` + RFQ.
  - `SyncCompleted` → bare `Z('I')` RFQ (defensive — T7 owns
    Sync; today Sync hits NYI before reaching this branch, but
    the match is exhaustive).
- The connection STAYS ALIVE across every extq rejection (the T1
  "tolerant probe-then-fall-back" contract is preserved). A
  genuinely-unknown tag (e.g. backend `Z`) still closes with
  `08P01` via the existing T1 invariant.

**+3 KATs (server-level, net +2 after the T1 flip):**

1. T1 `t1_extq_run_session_parse_tag_emits_0a000_and_stays_alive`
   FLIPPED → T2 `t2_extq_run_session_parse_tag_emits_parse_complete`:
   a valid Parse body now produces the 5-byte ParseComplete envelope
   (`1 00 00 00 04`) on the wire instead of `0A000`. No `08P01`
   (extq stays alive). **Headline byte-locked KAT** for SP-PG-EXTQ
   §13 acceptance criteria #2 (psql `\bind` extended-query path
   emits a parseable response).
2. NEW T2 `t2_extq_run_session_bind_tag_still_emits_0a000_and_stays_alive`
   — a Bind body STILL renders `0A000` + stays alive (locks the
   "haven not half-shipped T3" invariant; flips when T3 lands).
3. NEW T2 `t2_extq_run_session_parse_malformed_body_emits_08p01_and_stays_alive`
   — a Parse body that the decoder rejects (missing-NUL in the
   name cstring) emits `08P01` and the session stays alive. The
   5-byte ParseComplete envelope must NOT appear (the dispatcher
   never ran on a malformed body).

### Test counts (release on vulcan, 2026-05-28):

| Surface | Before T2 | After T2 | Delta |
|---|---|---|---|
| `kessel-pg-gateway` lib | 374 | 384 | +10 |
| Workspace default | 1842 | 1857 | +15 |
| Workspace `--features pg-gateway` | 1870 | 1885 | +15 |
| Workspace `--all-features` | 1925 | 1940 | +15 |

`kessel-sim` seed-7 GREEN (3 / 3); default tree-grep EMPTY (no new
external deps); `#![forbid(unsafe_code)]` honored across all touched
modules. CI green at every commit.

### Headline question — does Parse + Sync round-trip via `run_session` emit ParseComplete + RFQ byte-correct?

**Parse → ParseComplete: YES.** The 5-byte `1 00 00 00 04` envelope
appears on the wire after a valid Parse body; locked by
`t2_extq_run_session_parse_tag_emits_parse_complete` byte-for-byte.

**Sync → RFQ: PARTIAL.** Sync currently hits the still-NYI arm,
which renders `0A000 feature_not_supported` ErrorResponse +
ReadyForQuery('I'). The RFQ envelope itself IS byte-correct
(`Z 00 00 00 05 I`) — the 0A000 ErrorResponse BEFORE it is the
T7 gap. After T7 wires the Sync handler, the round-trip will be:
Parse → ParseComplete bytes → Sync → bare RFQ('I') (no
intermediate ErrorResponse).

## Next session pickup — SP-PG-EXTQ T3

**Slice scope**: Bind + BindComplete e2e —
- Implement `dispatch_bind(state, portal, stmt, param_formats,
  param_values, result_formats)`:
  - Validate the named statement exists in `state.statements`
    (`UnknownStatement { name: stmt }` → `26000 invalid_sql_
    statement_name`).
  - Per-position param-format validation: any code `== 1` (binary)
    → `BinaryFormatNotSupported { position }` → `0A000`. Length
    conventions match PG: 0 codes = "all text", 1 code = "every
    position the same", N codes = "per-position" (N must equal
    `param_values.len()`).
  - Per-portal cap: `MAX_PORTALS_PER_CONN` (4096) → `08P01`. The
    fresh-name rule applies (overwriting the volatile `""` portal
    does NOT grow the map).
  - Empty-name `""` overwrites the unnamed portal silently
    (matching the unnamed-statement shape in T2).
  - Store `Portal { stmt_name, param_values, param_formats,
    result_formats, exec_state: ExecState::Pending }`.
  - Emit BindComplete bytes (5-byte `2 [length=4]` envelope).
- Wire into `server::run_session`'s extq Bind branch so the
  rendered byte sequence is BindComplete on the wire.
- Flip the T2 `t2_try_dispatch_returns_not_yet_implemented_for_the_six_non_parse_tags`
  KAT to remove Bind from the still-NYI list. Add ~5-8 T3 KATs
  for the dispatcher + add 2-3 T3 server.rs KATs flipping the
  Bind-emits-0A000 lock to Bind-emits-BindComplete.

Estimated +5-8 lib KATs + 2-3 server KATs (~+8-10 total).
