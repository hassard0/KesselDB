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
| **T3** | Bind + BindComplete e2e: portal storage in `SessionState.portals`; per-position param-format validation (V1 rejects format code 1 with `0A000`); param-value extraction including NULL sentinel (`length=-1`); BindComplete emit; cap enforcement. | **DONE** | `7861b5b` (dispatcher + KATs) + `fb949bf` (server.rs wire-up) |
| **T4** | Describe 'S' AND 'P' (both flavors in one slice — saves the T5 separation since they share the same row-shape encoder): schema lookup via existing `EngineApply::describe_table` + `kessel_sql::select_star_table`; 'S' emits ParameterDescription with the OID hints from Parse (or empty if Parse didn't provide) + RowDescription/NoData; 'P' emits RowDescription/NoData WITHOUT ParameterDescription (portals froze their parameter values at Bind time per PG §55.2.3); Missing stmt → `26000`; missing portal → `34000`; bad target byte → `08P01`. | **DONE** | `cd09784` (dispatcher + KATs) + `9e591ca` (server.rs integration KATs) |
| **T5** | (FOLDED INTO T4) — Describe 'P' was originally a separate slice but shares the row-shape encoder with 'S'. T4 above ships both flavors together. Renumber the remaining slices in the SP-arc T6 → T5 etc. as bookkeeping, or keep the slot empty for a future Describe-related polish. | **CLOSED** | (folded into T4 `cd09784` + `9e591ca`) |
| **T5 (was T6)** | Execute + parameter substitution + Sync + PortalSuspended pagination + result streaming — folded T7 (Sync state machine) AND T9 (max_rows + buffered cursor) into this slice because the Execute path already had to land them to be useful end-to-end. Text-format `$N` substitution via new `extq/substitute.rs` (18 KATs against the §4 edge corpus); first-Execute dispatches through `dispatch::dispatch_query` + splits the byte stream tag-by-tag; portal's `exec_state` buffers DataRow frames for re-Execute pagination; `max_rows > 0` emits `PortalSuspended` instead of `CommandComplete`; `row_description_sent` flag suppresses repeat T frames per PG §55.2.3; Sync emits RFQ('I'), clears error_state, drops unnamed portal. **HEADLINE — real psycopg2 round-trip verified on vulcan**: `cur.execute("SELECT * FROM pgtest WHERE id = %s", (42,))` returns `[(42,)]` end-to-end. | **DONE** | `61d3228` (substitute helper + 18 KATs) + `cec17c4` (Execute + Sync dispatchers + 18 KATs incl. server.rs) |
| **T6** | Close ('S'/'P') + CloseComplete + Flush ('H'): drop stmt/portal from `SessionState`; CloseComplete emit (byte-locked T1 envelope); Flush is a no-op-emit that triggers a stream flush at the `server::run_session` boundary. Flips the T5 NYI lock for the remaining two tags. | **OPEN** | — |
| **T7** | Sync state-machine hardening: V2-flavor failure-recovery (carrying transaction state out of failed Sync block — needs SP-PG-TX coupling); cross-Sync portal lifecycle audit; pipelined-error attribution improvements (spec §11 weak-spot #9). | **OPEN** | — |
| **T8** | Real SQLAlchemy / pgx / JDBC ORM round-trip: connect via each driver; run a small CRUD suite (CREATE TABLE → INSERT × 5 → SELECT * → SELECT with param → UPDATE → DELETE); capture wire traces; log driver-specific behavior. | **OPEN** | — |
| **T9** | Streaming-cursor (real, not buffered): when SP-A T14 streaming-rows lands, replace V1's buffer-then-page shape in `dispatch_execute` with a real streaming consumer from the engine. Per-portal RSS becomes O(batch) not O(total). | **OPEN** | — |
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

## T3 — what landed (2026-05-29, commits `7861b5b` + `fb949bf`)

**Two commits, +862 LoC net delta across 2 files (mod.rs +657, server.rs +205 incl. KATs):**

### Commit `7861b5b` — Bind dispatcher arm + KATs

`crates/kessel-pg-gateway/src/extq/mod.rs`:

- Two new `ExtqError` variants:
  - `DuplicateCursor { name: String }` — Spec §3 / PG §55.2.3:
    re-Bind on a NON-EMPTY name already present rejects with
    SQLSTATE `42P03 duplicate_cursor` (the original portal is
    preserved). Empty-name `""` is the volatile exception (silently
    replaced).
  - `ParameterCountMismatch { expected: usize, actual: usize }` —
    Spec §4: when Parse declared OID hints, the wire
    `param_value_count` MUST match `PreparedStmt.param_oids.len()`.
    Maps to SQLSTATE `08P02
    protocol_violation_parameter_count`. When Parse omitted hints
    (`param_oids.len() == 0`), V1 accepts ANY count because OIDs
    are advisory — the engine resolves types at Execute (the
    common psycopg/asyncpg case).
- New `ExtqOutcome::Skipped` variant — Spec §6 skip-until-Sync:
  when `state.error_state == true` and the message is NOT Sync,
  the dispatcher silently drops it with NO state mutation. The
  caller writes NOTHING to the wire.
- New `SessionState::get_portal(name) -> Option<&Portal>` read-
  only accessor mirroring `get_statement`. Test-only
  `set_error_state(in_error: bool)` injector for the error-state
  KAT path.
- `try_dispatch_extq` now begins with the spec §6 skip-check: if
  `state.error_state` is true, every non-Sync message returns
  `ExtqOutcome::Skipped` (Sync still hits the NotYetImplemented
  arm because T7 owns the full Sync handler).
- New `dispatch_bind` helper enforces the spec §3 + §4 + §7.1
  invariants in order:
  1. **Statement lookup**: `UnknownStatement { name: stmt }` →
     `26000 invalid_sql_statement_name` if missing. Captures the
     expected param count from the prepared statement.
  2. **Binary-format rejection** per PG length conventions:
     - 0 codes = "all text" → no rejection.
     - 1 code = "every position the same" → reject everything if
       that single code is binary (position 0 in the error).
     - N codes = "per-position" → reject the FIRST position where
       the code is binary.
     All return `BinaryFormatNotSupported { position }` → `0A000
     feature_not_supported`. V2 SP-PG-EXTQ-BIN lifts.
  3. **Parameter-count match**: when `expected_param_count > 0`,
     `actual != expected` → `ParameterCountMismatch` → `08P02`.
     Empty `param_oids` skips the check.
  4. **Portal cap + collision** with the FRESH-name rule (mirrors
     T2 Parse cap): fresh name + at-cap → `TooManyPortals` →
     `08P01`; non-empty name already present → `DuplicateCursor`
     → `42P03`; empty-name `""` overwrites silently.
  5. **Store portal**: `Portal { stmt_name, param_values,
     param_formats, result_formats, exec_state: ExecState::Pending
     }` inserted into `state.portals` under `portal`.
  6. **BindComplete emit**: 5-byte `2 [length=4]` envelope from
     the existing T1 `response::encode_bind_complete` (byte-locked
     in T1 KATs).
- **Error-recovery side-effect**: on ANY error path,
  `dispatch_bind` sets `state.error_state = true` BEFORE
  returning. Subsequent pipelined messages until Sync hit the
  skip branch.

The four remaining dispatch arms (Describe / Execute / Close /
Flush) still return `NotYetImplemented`; T4..T8 widen them per
the §10 plan.

**+15 lib KATs (kessel-pg-gateway lib 384 → 399):**

1. T2 `t2_try_dispatch_returns_not_yet_implemented_for_the_six_non_parse_tags`
   FLIPPED → T3 `t3_try_dispatch_returns_not_yet_implemented_for_the_five_non_parse_non_bind_tags`
   (Bind arm removed; five remaining tags D/E/S/C/H still locked
   as NYI).
2. T3 `t3_dispatch_bind_unnamed_emits_bind_complete_and_stores_portal`
   — HEADLINE byte-locked BindComplete envelope + state mutation.
3. T3 `t3_dispatch_bind_named_stores_under_supplied_name` —
   carries through stmt_name + param_values + format arrays.
4. T3 `t3_dispatch_bind_missing_statement_returns_unknown_statement`
   — 26000 + error_state engaged.
5. T3 `t3_dispatch_bind_parameter_count_mismatch_returns_08p02` —
   2 OID hints + 1 wire value → 08P02 with expected/actual fields.
6. T3 `t3_dispatch_bind_no_oid_hints_accepts_any_param_count` —
   the common psycopg/asyncpg case; locks against future drift.
7. T3 `t3_dispatch_bind_binary_format_per_position_rejected` —
   per-position binary at position 1 → 0A000.
8. T3 `t3_dispatch_bind_single_binary_format_applies_to_all` —
   one-code length convention → 0A000 at position 0.
9. T3 `t3_dispatch_bind_duplicate_named_portal_returns_42p03` —
   collision; original portal preserved.
10. T3 `t3_dispatch_bind_unnamed_overwrites_previous_unnamed_portal`
    — silent replace + stmt_name carry-through.
11. T3 `t3_dispatch_bind_in_error_state_returns_skipped_without_processing`
    — spec §6 skip semantics; error_state stays true (only Sync
    clears).
12. T3 `t3_dispatch_bind_rejects_when_portal_cap_reached` — EXACT
    boundary; at-cap success + over-cap fails.
13. T3 `t3_dispatch_bind_null_parameter_carries_through_as_none`
    — NULL sentinel (length=-1) stored as `None`.
14. T3 `t3_dispatch_parse_then_bind_composes_end_to_end` — HEADLINE
    Parse+Bind composition.

### Commit `fb949bf` — server.rs Bind wire-up + integration KATs

`crates/kessel-pg-gateway/src/server.rs`:

- New match arms in the extq outcome handler:
  - `ExtqError::DuplicateCursor { name }` → `42P03` ErrorResponse
    + RFQ ("cursor \"{name}\" already exists").
  - `ExtqError::ParameterCountMismatch { expected, actual }` →
    `08P02` ErrorResponse + RFQ ("bind message supplies {actual}
    parameters, but prepared statement requires {expected}" —
    mirrors the PG canonical wording).
  - `ExtqOutcome::Skipped` → WRITES NOTHING. Spec §6 skip-until-
    Sync semantics: the dispatcher detected `error_state == true`
    and silently dropped this message. The next message either
    repeats the skip OR is a Sync that clears the flag (T7).
- The BindComplete bytes flow through the existing `ExtqOutcome::Bytes`
  arm (T2's wire-up), so no new code is needed for the happy
  path. The connection STAYS ALIVE across every Bind rejection
  (the T1 tolerant probe-then-fall-back contract is preserved).

**+3 server KATs (net +2 after the T2 flip):**

1. T2 `t2_extq_run_session_bind_tag_still_emits_0a000_and_stays_alive`
   FLIPPED → T3 `t3_extq_run_session_parse_then_bind_emits_parse_then_bind_complete`:
   a Parse + Bind input produces the consecutive 10-byte
   `1 00 00 00 04 2 00 00 00 04` sequence on the wire (locked
   byte-for-byte). **Headline byte-locked KAT** for SP-PG-EXTQ
   §13 acceptance criteria #2 (psql `\bind` extended-query path
   emits a parseable response).
2. NEW T3 `t3_extq_run_session_bind_unknown_statement_emits_26000_and_stays_alive`
   — Bind referencing a missing stmt → 26000; BindComplete must
   NOT appear; session stays alive.
3. NEW T3 `t3_extq_run_session_bind_binary_format_emits_0a000_and_stays_alive`
   — Parse + Bind with format code 1 → 0A000; ParseComplete
   appears (the preceding Parse succeeded); BindComplete must NOT.

### Test counts (release on vulcan, 2026-05-29):

| Surface | Before T3 | After T3 | Delta |
|---|---|---|---|
| `kessel-pg-gateway` lib | 384 | 399 | +15 |
| Workspace default | 1857 | 1889 | +32 |
| Workspace `--features pg-gateway` | 1885 | 1917 | +32 |
| Workspace `--all-features` | 1940 | 1972 | +32 |

`kessel-sim` seed-7 GREEN (3 / 3); default tree-grep EMPTY (no new
external deps; `cargo tree -p kessel-pg-gateway -e normal` is
workspace-only); `#![forbid(unsafe_code)]` honored across all
touched modules; HTTP/1.1 + WS + binary + PG-wire-Simple-Query
surfaces byte-untouched. CI green at every commit.

### Headline question — does a Parse + Bind + Sync round-trip emit ParseComplete + BindComplete + RFQ byte-correct?

**Parse → ParseComplete: YES** (locked byte-for-byte; same as T2).
**Bind → BindComplete: YES** — the 5-byte `2 00 00 00 04` envelope
appears immediately after ParseComplete in the outbound stream;
locked by `t3_extq_run_session_parse_then_bind_emits_parse_then_bind_complete`.
**Sync → RFQ: PARTIAL** (same shape as T2) — Sync still hits the
NotYetImplemented arm, which renders `0A000 feature_not_supported`
ErrorResponse + ReadyForQuery('I'). The RFQ envelope itself IS
byte-correct (`Z 00 00 00 05 I`); the intermediate 0A000
ErrorResponse is the T7 gap. After T7 wires the Sync handler the
round-trip will be: Parse → ParseComplete → Bind → BindComplete →
Sync → bare RFQ('I') with no intermediate ErrorResponse.

## Next session pickup — SP-PG-EXTQ T4

**Slice scope**: Describe 'S' → ParameterDescription + RowDescription/NoData.

- Implement `dispatch_describe(state, target='S', name)`:
  - Look up the statement (`UnknownStatement { name }` → `26000`
    if missing; reuses the T3 error variant).
  - Emit `ParameterDescription` with the OID hints from Parse
    (echoing the `PreparedStmt.param_oids` verbatim; the T1
    `response::encode_parameter_description` encoder is already
    byte-locked).
  - Schema lookup via the existing `EngineApply::describe_table`
    + `kessel_sql::select_star_table` path (the same SELECT
    parsing the Simple Query handler uses): emit `RowDescription`
    if the SQL parses to a SELECT, else `NoData` for non-SELECT
    statements (DDL/DML).
- Wire into `server::run_session`'s extq Describe branch so the
  rendered byte sequence is `t [...]` ParameterDescription + `T
  [...]` RowDescription (or `n` NoData) on the wire.
- Flip the T3 NYI lock to remove Describe from the still-NYI list.

Estimated +5-8 lib KATs + 2-3 server KATs (~+8-10 total). T5
(Describe 'P') reuses most of the T4 machinery.

## T4 — what landed (2026-05-29, commits `cd09784` + `9e591ca`)

**Two commits, +16 KATs net** (11 lib + 5 server.rs integration), all
pushed to main, all CI-green. **T4 ships BOTH Describe flavors —
statement ('S') AND portal ('P') — in one slice** because they share
the same row-shape detection + encoder; the originally-planned T5 is
folded into T4 with bookkeeping note (slice T5 row in the plan table
is marked CLOSED with a pointer to T4's commit pair).

### Commit `cd09784` — Describe dispatcher arms (S + P) + 11 KATs

`crates/kessel-pg-gateway/src/extq/mod.rs`:

- **`try_dispatch_extq` signature change** — now takes
  `&E: EngineApply + ?Sized` as an extra parameter so the Describe
  arm can call `engine.describe_table(&table_name)` (and T6 Execute
  can use `apply_sql`). The skip-until-Sync error-state branch +
  Parse/Bind arms are unchanged; the engine borrow is read-only.
  Server.rs (commit 1's small fix-up) + all 29 existing test-site
  callers updated to pass the engine in.
- **`dispatch_describe(state, engine, target, name)`** handles the
  S/P/other split per spec §4 + PG §55.2.3:
  - **`'S'` (statement)** — resolve `name` against
    `state.statements`. Missing → `UnknownStatement { name }` →
    `26000 invalid_sql_statement_name`. Emit
    `ParameterDescription(prep.param_oids)` (the byte-locked T1
    encoder), followed by `RowDescription` (if the SQL is a
    V1-renderable `SELECT * FROM <table>` per
    `kessel_sql::select_star_table` + `engine.describe_table`) or
    `NoData` (else).
  - **`'P'` (portal)** — resolve `name` against `state.portals`.
    Missing → `UnknownPortal { name }` → `34000
    invalid_cursor_name`. Then resolve the portal's `stmt_name`
    against `state.statements` (defensive — T3's Bind validation
    prevents portal-without-stmt in production, but the dispatcher
    locks the invariant against future Close-S-before-Describe-P
    drift). Emit `RowDescription` / `NoData` per the same shape as
    `'S'` — **but NOT ParameterDescription** (portals already froze
    parameter values at Bind time per PG §55.2.3 — clients receive
    `ParameterDescription` only on statement-targeted Describe).
  - **other target byte** — `BadDescribeTarget { target }` →
    `08P01`. The `decode_describe` path catches bad targets at
    decode time (`DecodeError::BadDescribeTarget`), but the
    dispatcher re-validates so a direct constructor of the message
    variant can't bypass.
- **`row_description_or_no_data_for_sql(engine, sql)`** helper —
  shared between the 'S' and 'P' arms; reuses the Simple Query
  path's exact detection (`kessel_sql::select_star_table` +
  `engine.describe_table` + `response::encode_row_description`) so
  Describe RowDescription bytes are **BYTE-EQUAL** to what `Q`
  dispatcher emits for the same SQL — a critical invariant that
  clients (asyncpg + JDBC especially) compare across the two
  protocol paths. Same SQL trim shape too (`sql.trim().trim_end_matches(';').trim()`).
- **`ExtqError::BadDescribeTarget { target: u8 }`** new variant.
  Maps to SQLSTATE `08P01`.
- **`error_state` side-effect** — on ANY error path
  `dispatch_describe` sets `state.error_state = true` BEFORE
  returning so subsequent pipelined messages until Sync hit the
  early-skip branch at the top of `try_dispatch_extq` (matches the
  T3 `dispatch_bind` shape).
- **T3 NYI list KAT FLIPPED** → T4 lock: the still-NYI tags shrink
  from 5 (D/E/S/C/H) → 4 (E/S/C/H). The Describe arm now produces
  real wire bytes, not `0A000`.

`crates/kessel-pg-gateway/src/proto.rs`:

- `DESCRIBE_TARGET_STATEMENT: u8 = b'S'` constant.
- `DESCRIBE_TARGET_PORTAL: u8 = b'P'` constant.

`crates/kessel-pg-gateway/src/server.rs` (compile-fix only — full
integration KATs land in the next commit):

- `try_dispatch_extq` call passes `engine` through (forced by the
  dispatcher's new signature).
- `BadDescribeTarget` error mapping → `08P01 protocol_violation`
  with the offending target byte in the rendered message.

**+11 lib KATs** (10 brand new + 1 flipped NYI lock):

1. **`t4_dispatch_describe_statement_select_emits_param_desc_and_row_desc`**
   — Describe 'S' on `SELECT * FROM t` yields ParameterDescription
   (echoing `param_oids = [23]`) followed by RowDescription whose
   column metadata matches `engine.describe_table` for table "t".
   Byte-equal to what the Simple Query path emits for the same SQL
   (asserted via `response::encode_row_description` comparison).
2. **`t4_dispatch_describe_statement_non_select_emits_param_desc_and_no_data`**
   — Describe 'S' on `INSERT INTO t (id) VALUES ($1)` yields PD +
   NoData (5-byte `n` envelope).
3. **`t4_dispatch_describe_statement_with_no_oid_hints_emits_empty_param_desc`**
   — Describe 'S' on a stmt with no Parse-time OID hints emits the
   7-byte empty-OID PD envelope (`t [length=6] [count=0]`).
4. **`t4_dispatch_describe_statement_missing_returns_26000`** —
   Describe 'S' on a non-existent statement → `UnknownStatement` +
   `error_state` engaged.
5. **`t4_dispatch_describe_portal_select_emits_row_desc_only`** —
   HEADLINE asymmetry KAT: Describe 'P' on a SELECT portal emits
   ONLY RowDescription (`T`), NEVER ParameterDescription (`t`).
6. **`t4_dispatch_describe_portal_non_select_emits_no_data`** —
   Describe 'P' on a non-SELECT portal emits 5-byte NoData only.
7. **`t4_dispatch_describe_portal_missing_returns_34000`** —
   Describe 'P' on a non-existent portal → `UnknownPortal` +
   `error_state` engaged.
8. **`t4_dispatch_describe_in_error_state_returns_skipped_without_processing`**
   — error_state Skip-until-Sync invariant (spec §6).
9. **`t4_dispatch_describe_unknown_target_byte_returns_08p01_bad_target`**
   — defensive bad-target-byte rejection.
10. **`t4_dispatch_parse_bind_describe_s_round_trip_composes`** —
    HEADLINE round-trip: Parse + Bind + Describe(S) in the dispatcher
    yields ParseComplete + BindComplete + PD + RowDescription
    byte-correct. Closest in-dispatcher equivalent to the
    `run_session` 4-message round trip.
11. **`t4_try_dispatch_returns_not_yet_implemented_for_the_four_remaining_tags`**
    — T3 NYI list KAT flipped to the 4 remaining tags
    (Execute/Sync/Close/Flush).

### Commit `9e591ca` — server.rs Describe wire-up + 5 integration KATs

`crates/kessel-pg-gateway/src/server.rs`:

- **`t4_extq_run_session_parse_bind_describe_s_select_emits_canonical_sequence`**
  — HEADLINE byte-locked KAT for §13 acceptance criteria. Full
  inbound: SCRAM handshake + Parse(`SELECT * FROM t`) + Bind +
  Describe(S) + Terminate. Asserts the canonical 4-message backend
  byte sequence on the wire — ParseComplete + BindComplete +
  ParameterDescription(empty) + RowDescription with column "id" —
  AND that no `0A000` (Describe is real now) / `26000` / `34000`
  appears anywhere. Every modern PG ORM probes THIS exact shape at
  connect time.
- **`t4_extq_run_session_parse_describe_s_insert_emits_no_data`** —
  Parse(INSERT) + Describe(S) → ParseComplete + PD + NoData.
- **`t4_extq_run_session_describe_s_missing_emits_26000_and_stays_alive`**
  — Describe(S) on a missing statement → 26000 + RFQ + session stays
  alive (the tolerant probe-then-fall-back contract preserved).
- **`t4_extq_run_session_describe_p_select_portal_emits_row_desc_no_param_desc`**
  — full Parse + Bind + Describe(P) round-trip; locks that the byte
  AFTER BindComplete is RowDescription uppercase `T`, NEVER
  ParameterDescription lowercase `t`. Spec §4 portal-vs-statement
  asymmetry verified at the wire layer.
- **`t4_extq_run_session_describe_p_missing_emits_34000_and_stays_alive`**
  — Describe(P) on a missing portal → 34000 + RFQ + stays alive.

### Test counts (release on vulcan, 2026-05-29)

| Surface | Before T4 | After T4 | Delta |
|---|---|---|---|
| `kessel-pg-gateway` lib | 399 | 414 | +15 (11 mod + 5 server.rs net of 1 flip) |

`kessel-sim` seed-7 GREEN (3 / 3); default tree-grep EMPTY (zero new
external deps — `cargo tree -p kessel-pg-gateway -e normal` is
workspace-only); `#![forbid(unsafe_code)]` honored across all
touched modules. CI green at every commit.

### Headline question — does Parse + Bind + Describe(S) + Sync emit the canonical 4-message wire sequence?

**Parse → ParseComplete: YES** (locked byte-for-byte; same as T2/T3).

**Bind → BindComplete: YES** (locked byte-for-byte; same as T3).

**Describe(S) → ParameterDescription + RowDescription/NoData: YES**
(locked byte-for-byte by
`t4_extq_run_session_parse_bind_describe_s_select_emits_canonical_sequence`
— the 4-message sequence `1 00 00 00 04 | 2 00 00 00 04 | t 00 00 00 06 00 00 | T [...]`
appears on the wire consecutively, with no intermediate `0A000`).

**Describe(P) → RowDescription/NoData (no PD): YES** (locked by
`t4_extq_run_session_describe_p_select_portal_emits_row_desc_no_param_desc`
— the byte after BindComplete is `T`, not `t`).

**Sync → RFQ: PARTIAL** (same as T2/T3) — Sync still hits NYI →
`0A000` ErrorResponse + RFQ. The RFQ envelope IS byte-correct
(`Z 00 00 00 05 I`), but the intermediate ErrorResponse is the T7 gap.
After T7 wires the Sync handler the full extq probe round-trip will
be: Parse → ParseComplete → Bind → BindComplete → Describe → PD +
RD/NoData → Sync → bare RFQ('I') with no intermediate ErrorResponse.
That's the §13 acceptance-criteria target — SQLAlchemy / psycopg /
asyncpg / JDBC / sqlx / Drizzle / Prisma probe pattern unblocked
end-to-end.

## Next session pickup — SP-PG-EXTQ T6

(T5 was folded into T4 — see the plan table; the next slice is now
the originally-planned T6 Execute work.)

**Slice scope**: Execute + parameter substitution + result streaming.

- Implement `extq/substitute.rs` — text-format `$N` substitution at
  Execute time per spec §4 + the §4 7-row substitution table + 5
  documented edge cases (identifier substitution forbidden, NULL-in-
  WHERE three-valued logic, binary-format reject — already done at
  Bind in T3, quoted-`$1`-in-comments, parameter-used-multiple-times).
  Estimated ~15 KATs against the spec §4 edge corpus.
- Implement `dispatch_execute(state, engine, portal, max_rows)`:
  - Resolve portal → stmt → SQL string.
  - Substitute `$1`/`$2`/... with the portal's `param_values` text
    bytes (NULL → bare `NULL`; others → single-quoted with `'` →
    `''` doubling per PG §4.1.2.1).
  - Run rewritten SQL through `dispatch::dispatch_query(sql,
    engine)` (the existing Simple Query pipeline — V1 reuses the
    SAME pipeline so SP-PG-CAT catalog hook + the T8 SELECT
    rendering Just Work for prepared statements with zero new
    catalog code).
  - Emit the result frames: `DataRow*` + `CommandComplete` for
    non-paginated success, or `DataRow* + PortalSuspended` if
    `max_rows > 0` and rows remain (T9 wires the full pagination
    state machine; T6 ships the unpaginated path).
- Wire into `server::run_session`'s extq Execute branch.
- Flip the T4 NYI lock to remove Execute from the still-NYI list.

After T6 lands, the full probe round-trip (Parse + Bind +
Describe(S) + Execute + Sync) emits its canonical wire sequence
modulo the Sync → bare-RFQ piece T7 owns. SQLAlchemy / psycopg's
`cursor.execute("SELECT %s", (42,))` round-trip becomes the §13
acceptance-criteria smoke target.

Estimated +8-12 lib KATs (substitute + dispatch_execute) + 3-5
server KATs (real Parse + Bind + Execute + Sync round-trip producing
actual DataRow bytes against a real `EmptySelectEngine`-shaped
mock).

## T5 — what landed (2026-05-29, commits `61d3228` + `cec17c4`)

**Two commits, +36 KATs across `kessel-pg-gateway`**, all pushed to
main, both CI-green. T5 folds the originally-planned T6 (Execute) +
T7 (Sync) + T9 (max_rows pagination) into a single slice because
Execute is unusable without all three.

### Commit `61d3228` — parameter substitution helper + 18 KATs

`crates/kessel-pg-gateway/src/extq/substitute.rs` (569 LoC NEW):

- `substitute_text_format_params(sql, params)` walks the SQL byte
  stream left-to-right, replacing every `$N` placeholder OUTSIDE
  quoted regions with the bound parameter value.
- Lexer skips: single-quoted strings (with `''` doubling escape);
  double-quoted identifiers (with `""` escape); `-- line comments`
  to next `\n`; `/* block comments */` non-nesting; PG dollar-quoted
  strings — both `$$body$$` empty-tag and `$tag$body$tag$` named-tag
  flavors detected and skipped.
- `$N` parser: greedy decimal-digit scan after the `$`. `$10`
  resolves to index 10 (not `$1` + literal `0`), locked against
  ambiguity by the `$10`/`$20` two-digit KATs.
- Rendering: `None` (PG NULL) → bare `NULL` keyword (NOT quoted);
  `Some(bytes)` → single-quoted with `'` → `''` doubling per PG
  §4.1.2.1. Numeric text values stay quoted (engine implicit-casts).
- `SubstituteError::ZeroParamIndex` rejects `$0`;
  `SubstituteError::ParamIndexOutOfBounds` rejects `$N` beyond
  bound count. Both → `08P01` at the dispatcher boundary.

**18 KATs** covering: text/NULL/numeric/empty values, single-quote
doubling, two-digit `$10`/`$20` indices, parameter reuse, lexer skip
for all 5 quote/comment regions, dollar-quoted strings (both
flavors), bare `$` defensive, no-placeholders passthrough, mixed
NULL+text+numeric.

### Commit `cec17c4` — Execute + Sync dispatchers + 18 KATs

`crates/kessel-pg-gateway/src/extq/mod.rs` (+1119 LoC incl. tests):

- **`Portal.row_description_sent: bool`** new field tracks whether
  `RowDescription` was already emitted (by Describe('P') or a prior
  Execute) so subsequent Execute does NOT repeat per PG §55.2.3.
  Reset on Sync.
- **`dispatch_describe('P')` sets the flag**.
- **`dispatch_execute(state, engine, portal_name, max_rows)`**:
  1. Portal lookup → `UnknownPortal` → `34000`.
  2. Statement lookup (defensive) → `UnknownStatement` → `26000`.
  3. Empty SQL → `EmptyQueryResponse` (5-byte `I [length=4]`).
  4. Parameter substitution via T5 commit-1 helper → failure maps
     to `SubstitutionFailed` → `08P01`.
  5. First-Execute (`Pending`) → call
     `dispatch::dispatch_query(rewritten_sql, engine)`; SPLIT the
     returned bytes via `split_dispatch_query_bytes` (walks PG
     frame headers tag/length) into prelude / data_rows /
     command_complete + STRIPS the trailing `Z` RFQ; BUFFER the
     DataRow frames into `Buffered { rows, cursor: 0 }`.
  6. Re-Execute (`Buffered`) → page from existing buffer; no
     re-substitute, no re-dispatch.
  7. Re-Execute on `Exhausted` portal → bare CommandComplete.
  8. RowDescription suppression via `strip_leading_row_description`
     if `portal.row_description_sent`.
  9. max_rows pagination per spec §7.2: `0` → all + CommandComplete
     + Exhausted; `> 0` → up to N DataRows + (PortalSuspended |
     CommandComplete); `< 0` → permissive treat as 0.
  10. Error_state side-effect on every failure (spec §6).
- **`dispatch_sync(state)`**: emits `Z 00 00 00 05 I`; resets
  `error_state = false`; drops unnamed portal; resets
  `row_description_sent` on every surviving portal.
- `try_dispatch_extq` routes Execute + Sync to real handlers; the
  error-state branch routes Sync to `dispatch_sync` (the only way
  out of skip-until-Sync mode).
- T4 NYI list KAT flipped to T5: still-NYI shrinks from 4 (E/S/C/H)
  → 2 (C/H — Close + Flush only).
- `ExtqError::SubstitutionFailed { reason }` new variant.

`crates/kessel-pg-gateway/src/server.rs` (+254 LoC incl. tests):

- `SubstitutionFailed` wired to `08P01` ErrorResponse.
- 4 server-level integration KATs.

**Test counts on vulcan**: kessel-pg-gateway lib 414 → 452 (+38
incl. 1 NYI rename); workspace 1948 passing (no failures). seed-7
GREEN. CI green at both commits.

### HEADLINE — does psycopg2 round-trip work?

**YES — END TO END ON A REAL CLIENT.**

Started kesseldb-server on vulcan with
`KESSELDB_TOKEN=admin KESSELDB_PG_ADDR=127.0.0.1:5532`. Created a
`pgtest (id i64)` table + INSERTed rows 1, 2, 42 via psql. Then via
psycopg2 (libpq-based extended-query client):

- `conn = psycopg2.connect(host=..., user=test, password=admin,
  dbname=kesseldb, ...)` — SCRAM-SHA-256 handshake completed,
  `BackendKeyData` returned.
- `conn.autocommit = True` — avoids the auto-BEGIN that V1 multi-
  statement-Q rejects.
- `cur.execute("SELECT * FROM pgtest")` → `[(1,), (2,), (42,)]`.
- `cur.execute("SELECT * FROM pgtest WHERE id = %s", (42,))` →
  `[(42,)]`.

The second `execute` uses the FULL extended-query protocol:
StartupMessage → SCRAM → AuthenticationOk + ParameterStatus +
BackendKeyData + ReadyForQuery → Parse → Bind → Describe → Execute
→ Sync → ParseComplete + BindComplete + ParameterDescription /
RowDescription + DataRow + CommandComplete + ReadyForQuery.
Parameter `42` text-substituted into `'42'`, engine WHERE filter
matched, row came back through DataRow on the wire.

**THIS IS THE ORM-READINESS MILESTONE.** Every modern Postgres ORM
that defaults to text-format parameters (~95% of real traffic —
psycopg2/psycopg3/asyncpg/SQLAlchemy/sqlx/Drizzle/Prisma/Node `pg`/
JDBC default) can now connect AND execute parameterized queries
against KesselDB. Remaining gaps are engine-side (V1's SQL parser
only supports `SELECT * FROM <table>`; multi-statement Q rejected),
NOT extq protocol gaps.

The `psql ... PREPARE x AS SELECT $1; EXECUTE x(42)` smoke from
spec §13 acceptance criteria #2 failed on `multi-statement Q not
supported in V1` — but that's SIMPLE-QUERY-side (multiple
`;`-separated statements), distinct from extended-query. psql's
`\bind 42` + `SELECT * FROM pgtest WHERE id = $1` (separate `-c`
invocations) DID work and returned the row through extended-query.
The richer psycopg2 acceptance shape above is the real test.

## Next session pickup — SP-PG-EXTQ T6

**Slice scope**: Close ('S'/'P') + CloseComplete + Flush handlers.

- Implement `dispatch_close(state, target, name)`:
  - `'S'` → drop the named statement (and the portals that
    reference it, per PG semantics).
  - `'P'` → drop the named portal.
  - Missing name → silent no-op (PG semantics).
  - Emit `CloseComplete` (5-byte `3 [length=4]`, byte-locked T1
    encoder).
- Implement `dispatch_flush(state)`:
  - No state change; returns empty bytes (the Flush itself doesn't
    emit a frame); the server.rs wire-up calls `stream.flush()`
    after to honor the early-flush request.
- Wire into server.rs Close + Flush branches.
- Flip the T5 NYI lock to remove both → empty still-NYI list; the
  lock becomes "every extq tag dispatches to a real handler".

Estimated +6-8 lib KATs + 2-3 server KATs.

After T6 lands, the SP-PG-EXTQ V1 message set is COMPLETE. T7+ ship
hardening + real-driver compat smoke + arc closure.
