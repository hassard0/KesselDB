# SP-PG-EXTQ — PostgreSQL Extended Query protocol (Parse / Bind / Describe / Execute / Sync / Close / Flush) — DESIGN

**Status:** design — scopes the SP-PG V1 follow-up named in
`docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`
§2.2 (V2 follow-ups) and §10 T19. SP-PG V1 closed at commit `2026-05-27`;
this arc owns the SINGLE biggest remaining adoption multiplier — Extended
Query is the prepared-statement protocol every modern Postgres ORM
hard-requires.

Companion progress tracker:
`docs/superpowers/specs/2026-05-28-kesseldb-subproject-sppgextq-progress.md`.

**Builds on:**
- **SP-PG V1** (`docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`)
  — the Simple Query path, SCRAM-SHA-256 auth, framing rules,
  `proto.rs` message-tag catalog, `error.rs` SQLSTATE map,
  `response.rs` backend-message encoders, the
  `engine::EngineApply::{apply_sql, apply_sql_with_count, describe_table}`
  trait surface, and the `server::run_session` per-connection accept
  loop are already in tree and unchanged. SP-PG-EXTQ adds a PARALLEL
  dispatch path inside the SAME `run_session` loop — Simple Query
  (`Q` tag) and Extended Query (`P` / `B` / `D` / `E` / `S` / `C` /
  `H` tags) coexist on the same connection.
- **SP-PG-CAT** (`docs/superpowers/specs/2026-05-27-kesseldb-sppgcat-pg-catalog-design.md`)
  — `pg_catalog` / `information_schema` synthesizers. SP-PG-EXTQ's
  Bind path goes through the SAME `dispatch_query` entry point that
  consults the catalog hook FIRST, so prepared statements that target
  `pg_catalog.*` Just Work without a second copy of the catalog hook.
- **`kessel-sql`** — the SQL parser already in tree. SP-PG-EXTQ does
  NOT extend the parser; parameter substitution is purely TEXTUAL —
  `$1` literal in the SQL string → bound value's PG text-format bytes
  at Execute time. See §4 for the substitution rules and edge cases.
- **`kessel-pg-gateway::dispatch::dispatch_query`** — the Simple Query
  pipeline. SP-PG-EXTQ's Execute calls the SAME pipeline with the
  parameter-substituted SQL string. One dispatch entry point; one
  code path that gets all the existing T7/T8/T9 polish.

---

## 1. Context — why Extended Query

SP-PG V1 closed with the headline pentest result that `psql -h
localhost "SELECT 1"` works end-to-end. But the moment you point a
modern ORM at KesselDB — Prisma, Drizzle, SQLAlchemy, sqlx, Diesel,
GORM, psycopg, pgx, JDBC PreparedStatement — the connection refuses
at the protocol-probe phase, because every one of them sends
`Parse` / `Bind` / `Describe` / `Execute` / `Sync` messages instead
of (or interleaved with) Simple Query. V1 rejects these tags with
`08P01 protocol_violation` and closes the connection. The ORM logs
a generic "protocol error" and gives up.

This is the single biggest adoption multiplier remaining for the PG
wire surface. Simple Query is what `psql` and `pgcli` and bash
scripts use; Extended Query is what every PRODUCTION app uses.

### 1.1 What actually fails today (probe sequence)

Running the canonical SQLAlchemy probe against KesselDB V1:

```python
import sqlalchemy
engine = sqlalchemy.create_engine(
    "postgresql://test:token@localhost:5432/kessel"
)
with engine.connect() as conn:
    conn.execute(sqlalchemy.text("SELECT 1"))
```

The wire trace (captured against the V1 gateway):

```
C → S: StartupMessage (user=test, database=kessel)
S → C: AuthenticationSASL "SCRAM-SHA-256"
... full SCRAM-SHA-256 handshake completes ...
S → C: AuthenticationOk + ParameterStatus×8 + BackendKeyData + ReadyForQuery('I')
C → S: P  Parse "" "SELECT pg_catalog.version()" 0 params
       D  Describe 'S' ""
       S  Sync
S → C: E  ErrorResponse  S=ERROR  C=08P01  M="unsupported message tag: 0x50"
       Z  ReadyForQuery 'I'
[connection drops; SQLAlchemy raises OperationalError]
```

The connection never makes it to the application code. Same shape
for psycopg's `cursor.execute("SELECT %s", (42,))`, JDBC's
`PreparedStatement.setInt(1, 42)`, sqlx's compile-time-prepared
queries, GORM's auto-migrate probe, and Prisma's `prisma db pull`.

Even when the application doesn't use prepared statements explicitly,
most drivers issue ONE Parse/Describe/Sync probe at connect time to
discover `version()` / `current_schema()` / session GUCs — that
probe is what fails today.

### 1.2 What the ecosystem unlocks

| Surface | Today (V1) | After SP-PG-EXTQ |
|---|---|---|
| **psql** | works | unchanged |
| **pgcli** | works | unchanged |
| **psycopg2 / psycopg3 / asyncpg** | refuses to connect | full `cursor.execute(sql, params)` |
| **SQLAlchemy** | refuses to connect | full ORM (Session, Query, Model) |
| **Django ORM** | refuses to connect | full ORM |
| **JDBC PG driver** | refuses to connect | full `PreparedStatement` |
| **Go `pgx`** | refuses to connect | full `Conn.Query(ctx, sql, args...)` |
| **Go `lib/pq` / `sqlx-pg`** | refuses to connect | full `db.Prepare(sql).Exec(args...)` |
| **Node `pg`** | refuses to connect | full `client.query(sql, values)` |
| **Node `postgres.js`** | refuses to connect | full `sql\`SELECT $\{x\}\`` |
| **Rust `tokio-postgres` / `postgres`** | refuses to connect | full `client.prepare_typed` + `query` |
| **Rust `sqlx` (PG mode)** | refuses to connect | full compile-time-checked queries |
| **Prisma** | refuses to connect | `prisma db pull` works, full client |
| **Drizzle** | refuses to connect | full ORM |
| **GORM (Go)** | refuses to connect | full ORM |
| **Diesel (Rust)** | refuses to connect | full ORM |
| **dbt-postgres / Airbyte-PG / Fivetran** | refuses to connect | works |

Every row above is unlocked by SP-PG-EXTQ V1 (text-format
parameters; see §2.2 for the V2-deferred binary-format addendum).

## 2. Scope

### 2.1 V1 — what's in (this arc, T1..T12)

1. **Full Extended Query message set** — `P` Parse, `B` Bind, `D`
   Describe, `E` Execute, `S` Sync, `C` Close, `H` Flush. Every tag
   the libpq client may send during a prepared-statement flow.
2. **Named + unnamed prepared statements**. A `Parse` with name=""
   produces the UNNAMED (volatile) statement — every subsequent
   `Parse name=""` overwrites it. A `Parse` with name="stmt_42"
   produces a NAMED statement that persists until explicit `Close`
   or connection close.
3. **Named + unnamed portals**. Same shape — `Bind` portal="" is
   volatile; `Bind portal="p_42"` is named.
4. **Text-format parameters only** (PG format code 0). Binary-format
   parameters (format code 1) are V2 (named follow-up `SP-PG-EXTQ-BIN`).
   The wire surface carries the format-codes array; V1 REJECTS
   format code 1 with `0A000 feature_not_supported`. Text format
   covers ~95% of real-world ORM traffic (every ORM that uses libpq's
   default `PQexecParams` text path, which is the default for
   psycopg2, asyncpg, Node `pg`, JDBC default, sqlx, Drizzle, Prisma,
   etc.). Binary is an OPTIMIZATION drivers can negotiate but rarely
   do for OLTP-shaped traffic.
5. **SQL-text parameter substitution at Execute time**. `$1` / `$2`
   / ... literals in the prepared SQL are replaced by the bound
   parameter value's PG text-format bytes, with single-quote escaping
   (the `'` → `''` doubling that PG uses for string literals). NULL
   parameter (length=-1 wire sentinel) renders as `NULL`. See §4 for
   the substitution rules and edge cases.
6. **Pipelining**. Client may send `P / B / D / E / S` (or any
   combination ending in `S`) without waiting for replies between
   messages. Server processes in arrival order, emits replies in
   arrival order, batches flushes between messages. `Sync` is the
   only mandatory flush point; `Flush` (`H`) requests an early
   flush. See §5.
7. **Error recovery via Sync**. Any error during Parse/Bind/Execute/
   Describe emits one `ErrorResponse` frame and the server then
   SKIPS all further extended-query messages until it sees `S` Sync.
   After Sync, emit `ReadyForQuery('I')` (V1 has no transaction-block
   awareness; the spec §6 error-recovery `Z('E')` failed-tx status
   is V2 SP-PG-TX) and resume normal processing. See §6.
8. **Statement + portal reuse**. A single Parse may be followed by
   N Bind/Execute pairs (the cached-prepared-statement use case
   ORMs depend on for fast repeated queries). Each Bind produces a
   fresh portal carrying its parameter values; Executes against
   different portals are independent.
9. **PortalSuspended for max_rows truncation**. `Execute` carries
   a `max_rows: i32`. If `max_rows == 0`, return all rows. If
   `max_rows > 0`, return up to that many `DataRow`s, then emit
   `PortalSuspended` ('s') instead of `CommandComplete`. The same
   portal can be re-executed to fetch the next batch — V1 BUFFERS
   the full result-set in the portal at first Execute and pages
   from the buffer (the engine doesn't stream rows yet; same gap
   as SP-PG V1 spec §11 weak-spot #3 — V2 SP-A T14 will fix). See §7.
10. **Lifecycle**. Statements + portals auto-drop on connection
    close. Empty-name (unnamed) statement/portal are special-cased
    as auto-dropped on the next Parse/Bind with the same empty
    name. Explicit `Close` ('C') drops a named statement or portal
    and emits `CloseComplete` ('3'). See §3.
11. **Memory bounds**. Per-connection cap on `HashMap<String,
    PreparedStmt>` size (4096 named stmts) — `08P01` rejection if
    exceeded. Same cap for portals. SQL-text cap = existing
    `PG_MAX_MESSAGE_SIZE = 16 MiB` (inherited from V1 framing).
    See §8.
12. **Coexistence with Simple Query**. The SAME connection can mix
    `Q` (Simple Query) and the Extended Query pipeline arbitrarily.
    `Q` always flushes any pipelined extended-query state and emits
    its own `ReadyForQuery` per V1 spec §3 — i.e. `Q` is itself a
    sync point. Documented; locked by §12 KAT.

### 2.2 V1 — what's out (named V2+ follow-ups — each is its own arc)

- **Binary-format parameters** (PG format code 1) — V2 `SP-PG-EXTQ-BIN`.
  Estimated 2 slices. int / float / bool / text / timestamp first;
  numeric last (PG binary numeric is base-10000 variable-length-digit
  and bug-prone). Wire surface is present in V1 (the per-parameter
  format codes array is parsed and validated) but format code 1
  rejected with `0A000 feature_not_supported`. Most ORM defaults are
  text; the few drivers that opt into binary (e.g. asyncpg with
  `prepare()` cache) will fall back when the server rejects binary.
- **Server-side prepared-statement cache across reconnect** — V2
  `SP-PG-EXTQ-CACHE`. libpq has a "prepared name across reconnect"
  shape that almost no ORM relies on (they all re-Parse on reconnect).
  V1 statements + portals are PER-CONNECTION and auto-drop on
  connection close. Documented limitation.
- **COPY in extended-query** — V2 `SP-PG-COPY` (named in SP-PG V1
  §2.2). COPY is its own subprotocol; SP-PG-EXTQ doesn't touch it.
- **Large object protocol** (`lo_open`, `lo_read`, …) — deprecated
  by PG itself; permanent hard pass.
- **Cursor implementation (real streaming)** — V1 BUFFERS the full
  result-set in the portal at first Execute and pages from the
  buffer. A real cursor (stream rows from the engine as the client
  Executes) is SP-A T14 (streaming-rows arc, named in SP-PG V1
  spec §11 weak-spot #3). When SP-A T14 lands, SP-PG-EXTQ's portal
  layer rewires to consume the engine stream incrementally — no
  protocol change.
- **Transaction-block awareness** (`Z('T')` and `Z('E')` status
  bytes; implicit-tx semantics where extended-query messages within
  one `Sync` form an implicit txn) — V2 `SP-PG-TX`. V1 emits
  `Z('I')` after every Sync regardless. Spec §6 documents the
  trade-off.
- **RETURNING-clause support** — already V2 SP-PG-RETURNING per
  SP-PG V1 §2.2. Independent of extended-query; both V2 follow-ups
  cooperate when both land.
- **Parameter-typed SQL parser**. V1 does the parameter substitution
  textually at Execute time; V2 `SP-PG-EXTQ-PARSED` would extend
  `kessel-sql` with a parameter-AST node so `$1` is a typed
  placeholder the planner can see (better error messages, no quote
  escaping pitfalls). For V1 we ship the text-substitution shape
  because it requires zero engine changes — locked tradeoff per §11.

## 3. Wire-state machine — per-connection state

A `kessel_pg_gateway::extq::SessionState` lives next to the existing
`AcceptedSession` from V1, attached to the connection thread-locally
(no `Arc`, no `Mutex` — strictly thread-local) and accessed only
from the connection's reader thread:

```rust
pub struct SessionState {
    /// Named + unnamed prepared statements. Empty-name "" is the
    /// volatile slot; Parse with name="" auto-drops + replaces it.
    statements: HashMap<String, PreparedStmt>,
    /// Named + unnamed portals. Empty-name "" is the volatile slot.
    portals: HashMap<String, Portal>,
    /// Set true on the first error of a Sync-bounded sequence;
    /// reset on Sync. While true, the dispatcher SKIPS all
    /// extended-query messages until it sees `S` Sync.
    error_state: bool,
}

pub struct PreparedStmt {
    /// Original SQL text from Parse, BEFORE parameter substitution.
    /// V1 does NOT compile-cache an AST — the SQL is re-parsed by
    /// the engine on every Execute (acceptable for V1; SP47 compile-
    /// cache already deduplicates inside the engine).
    sql: String,
    /// Parameter type OIDs from Parse. May be empty (client omitted
    /// type hints) or partial (only some positions typed). V1
    /// ignores the OIDs at Bind/Execute (text substitution doesn't
    /// need them); they're carried so ParameterDescription can
    /// echo them back to clients that issued Describe 'S'.
    param_oids: Vec<u32>,
}

pub struct Portal {
    /// Statement name this portal binds. Looked up at Execute time
    /// (not cached at Bind, because a subsequent Close 'S' on the
    /// stmt name would invalidate the cached reference).
    stmt_name: String,
    /// Bound parameter values, in position order. Each value is
    /// either Some(bytes) (the raw text-format wire bytes the
    /// client sent) or None (the i32 -1 length sentinel = SQL NULL).
    param_values: Vec<Option<Vec<u8>>>,
    /// Per-position parameter format codes from Bind. V1 enforces
    /// every position is 0 (text); any 1 is rejected with `0A000`.
    /// Length conventions match PG: 0 codes = "all text", 1 code =
    /// "every position the same", N codes = "per-position".
    param_formats: Vec<u16>,
    /// Per-position result format codes from Bind. V1 emits text
    /// always; any client-requested binary code is silently ignored
    /// in V1 (clients tolerate this — the format-code field in
    /// RowDescription tells them what they actually got). V2 with
    /// binary result format adds enforcement.
    result_formats: Vec<u16>,
    /// In-progress execution cursor. None until first Execute;
    /// then Some(buffered_rows) — V1 buffers all rows at first
    /// Execute and pages from the buffer for PortalSuspended.
    exec_state: ExecState,
}

pub enum ExecState {
    /// Portal not yet executed.
    Pending,
    /// Portal executed; rows buffered. `cursor` is the index of
    /// the next row to emit; `total` is the row count for the
    /// CommandComplete tag once we exhaust the buffer.
    Buffered { rows: Vec<Vec<u8>>, cursor: usize },
    /// Portal exhausted (CommandComplete already emitted). Further
    /// Executes on this portal emit CommandComplete("SELECT 0")
    /// per PG §55.2.3 (the libpq-tested shape).
    Exhausted { total: u64 },
}
```

Empty-name semantics:

- `Parse name=""` → drop any existing unnamed statement, install
  the new one. Atomic: parse-then-replace, not replace-then-parse
  (so a Parse-error on the new SQL preserves the old unnamed slot;
  documented edge case).
- `Bind portal=""` → drop any existing unnamed portal, install
  the new one. Same atomic shape.
- `Close 'S' name=""` → drop the unnamed statement (if present).
- `Close 'P' name=""` → drop the unnamed portal.

## 4. Parameter substitution at Execute time

V1's parameter pipeline:

1. At `Bind`, the client sends N parameter values as text-format
   bytes. Each value is `[length:i32 BE][bytes:length]` where
   `length == -1` means SQL NULL.
2. The portal stores `param_values: Vec<Option<Vec<u8>>>`.
3. At `Execute`, the prepared SQL string is rewritten by replacing
   each `$1`, `$2`, ... placeholder with the corresponding bound
   value's text-format-bytes, applying SQL-text escaping rules.
4. The rewritten SQL is passed verbatim to `engine.apply_sql` (the
   same entry point the Simple Query path uses).

Substitution rules (V1):

| Bound value | Rendered SQL | Notes |
|---|---|---|
| NULL (length=-1) | `NULL` literal | Bare keyword; not quoted |
| empty bytes (length=0) | `''` (empty single-quoted string) | Matches PG's text-format |
| `"hello"` | `'hello'` | Wrap in single quotes |
| `"O'Brien"` | `'O''Brien'` | RFC: double single quotes |
| `"42"` | `'42'` | V1 always quotes — let the SQL parser coerce to int |
| `"-3.14"` | `'-3.14'` | Same — text format universally string-shaped |
| `"\\xDEADBEEF"` | `'\\xDEADBEEF'` | bytea text format passes through |
| `"true"` / `"false"` | `'true'` / `'false'` | Same |

**Why quote everything?** The libpq protocol's text format is
**already string-shaped at the wire** — for a `SELECT $1::int`
query the client sends the int as the ASCII bytes `"42"`. The
KesselDB parser accepts `'42'` as either a quoted string or an
implicit-cast integer literal, so wrapping every text-format param
in single quotes works for every type — strings, ints, floats,
bools, bytea — without the substitution layer needing to know the
column's PG type. (Optional refinement: if `param_oids[i]` says
`INT8`/`INT4`/`INT2`/`BOOL` we could emit the value unquoted to
save the cast — V1 ships the always-quote shape because it's the
simplest correctness-preserving substitution; the optimization is
a follow-up if profiling shows it matters.)

The single-quote escaping is RFC: PG §4.1.2.1 "String Constants —
to include a single-quote character within a string constant,
write two adjacent single quotes". `'O''Brien'` is "O'Brien"
verbatim per the PG SQL spec.

**Edge cases V1 documents but doesn't perfect:**

- **Identifier substitution.** `SELECT * FROM $1` where $1 = "users"
  doesn't work — the substitution always wraps in quotes, producing
  `SELECT * FROM 'users'` which fails. PG itself forbids identifier
  substitution via Parse parameters; clients that want dynamic
  table names use server-side SQL formatting or `format()`. V1
  ships the same rule.
- **NULL inside an expression.** `WHERE x = $1` when $1 is NULL
  renders as `WHERE x = NULL` which is always FALSE per SQL's
  three-valued logic. RFC: the client is supposed to send `WHERE
  x IS NULL` for the NULL case; psycopg2/sqlalchemy do this
  automatically.
- **Binary-format parameters.** Wire format code 1 → V1 rejects
  the whole Bind with `ErrorResponse  C=0A000  M="binary-format
  parameters not supported in V1; client must request text-format
  (format code 0)"`. The client typically retries with text.
- **Quoted-identifier-containing-$1.** The substitution is purely
  literal: `SELECT "col$1"` would NOT substitute (no parameter
  there). The substitution targets unquoted `$1`/`$2`/... numeric
  tokens only. The engine SQL parser already disambiguates
  identifiers — V1's `substitute_params` walks the SQL text and
  only replaces `$N` outside quoted regions (single-quote and
  double-quote regions both). Edge case: comments. `-- $1 here`
  must not substitute; V1 strips line comments at substitution
  time and skips them.
- **Same parameter used multiple times.** `WHERE x = $1 OR y = $1`
  with $1=42 → `WHERE x = '42' OR y = '42'`. Locked KAT.

V1 ships a 1-2 KB substitution module (`extq/substitute.rs`) with
~15 KATs covering every row of the table above plus the edge cases.

## 5. Pipelining — the protocol's request-pipelined shape

Extended Query is REQUEST-pipelined, not concurrent. The client may
send multiple messages back-to-back without waiting for the
server's reply between them. The server processes them in arrival
order and emits replies in arrival order. The protocol has NO
in-band reordering, NO message IDs, NO multiplexed concurrency.

The server's reader thread receives a Sync-bounded sequence of
messages, processes each, accumulates the responses in a single
output buffer, and flushes either:

- After every message (eager-flush mode — simpler, used by V1
  to match what the existing `run_session` loop does for Simple
  Query). The per-message flush is cheap because TCP_NODELAY is
  set; the OS may coalesce.
- On `H` Flush — explicit client request to flush early.
- On `S` Sync — mandatory flush + emit `ReadyForQuery`.

The trade-off (eager-flush vs accumulate-then-flush-on-Sync) is
purely a latency/throughput knob. V1 eager-flushes per message for
simplicity + minimum latency; V2 may add an accumulate-then-flush
mode behind a config knob. The wire BYTES are identical either way.

Locked invariant: ordering. If the client sends
`P P B B D D E E S`, the server emits
`1 1 2 2 t T t T D... C D... C Z` in exactly that order. The
`extq::dispatch_message` function appends to a per-Sync output
buffer; the run loop drains the buffer between messages.

## 6. Error recovery — Sync resets

The PG error-recovery state machine (PG §55.2.3 "Error Handling
in the Pipeline"):

1. Any error during P/B/D/E emits ONE `ErrorResponse` frame.
2. The server then SKIPS every subsequent P/B/D/E/C/H message
   silently until it sees `S` Sync.
3. On Sync, emit `ReadyForQuery('I')` (V1) — for V2 with
   transaction-block awareness this would be `Z('E')` failed-tx.
4. Resume normal processing on the next message.

This is what lets clients pipeline `P / B / E` without
intermediate error checks — if the Parse fails, the client's
Bind+Execute targeting that Parse's stmt name also "fail" (server
skips them), and the client sees one ErrorResponse + one
ReadyForQuery, then loops back to the next attempted query.

V1 implements this in `extq::dispatch_message`:

```rust
fn dispatch_message(state: &mut SessionState, msg: ExtqMessage, ...) {
    if state.error_state {
        match msg {
            ExtqMessage::Sync => {
                state.error_state = false;
                emit_ready_for_query(b'I');
            }
            _ => {} // silently skip
        }
        return;
    }
    match msg {
        ExtqMessage::Parse { ... } => match try_parse(...) {
            Ok(stmt) => { state.statements.insert(name, stmt); emit_parse_complete(); }
            Err(e) => { state.error_state = true; emit_error_response(e); }
        },
        // ... and so on
    }
}
```

Documented limitation: V1 has no implicit-transaction semantics
inside a Sync-bounded sequence. PG implicitly opens a transaction
on the first Parse/Bind/Execute and commits it on Sync; V1's
engine commits per-Execute (one apply_sql call = one auto-commit).
Documented as the V2 SP-PG-TX follow-up. For OLTP-shaped traffic
this difference is invisible — the only client-visible delta is
that V1's `Z` status byte is always `'I'` even mid-Sync-sequence,
where PG would emit `'T'` for in-transaction.

## 7. Memory bounds + max_rows pagination

### 7.1 Bounds

- `MAX_PREPARED_STATEMENTS_PER_CONN = 4096` — Parse with a fresh
  name when at the cap → `ErrorResponse  C=08P01  M="too many
  prepared statements (max 4096 per connection)"`.
- `MAX_PORTALS_PER_CONN = 4096` — same shape for Bind.
- `MAX_SQL_TEXT_BYTES = PG_MAX_MESSAGE_SIZE = 16 MiB` — inherited
  from V1's existing message-length cap. A `P` message with a 100
  MiB SQL string is rejected at the framing layer BEFORE it reaches
  the extq dispatcher.
- `MAX_PARAMETERS_PER_BIND = 65535` — the wire field is `i16` so
  this is the protocol cap; V1 doesn't impose a tighter limit.
- `MAX_BUFFERED_ROWS_PER_PORTAL = unbounded in V1` — V1 inherits
  the engine's per-`apply_sql` materialization limit (which is the
  same OS-memory-bound limit Simple Query has today). When SP-A
  T14 streams from the engine, this becomes a real per-portal
  ring-buffer bound; documented in §11.

### 7.2 max_rows pagination — PortalSuspended

Execute carries `max_rows: i32`:

- `max_rows == 0` → return all rows; emit `CommandComplete` at
  the end.
- `max_rows > 0` → emit up to `max_rows` DataRows; if more rows
  remain, emit `PortalSuspended` ('s') instead of CommandComplete
  and leave `exec_state = Buffered { cursor: max_rows }`. The
  client's next Execute on the SAME portal continues from
  `cursor` and emits up to its own `max_rows` more DataRows.
- `max_rows < 0` → treated as `max_rows == 0` (PG itself doesn't
  spec this; V1 picks the permissive shape).

V1's `exec_state` BUFFERS the full result-set at the first Execute
because `engine.apply_sql` materializes its result before
returning. A subsequent Execute on the same portal pages from the
buffer — `cursor` indexes into `rows` — without re-calling the
engine. This is correct (the portal preserves the snapshot taken
at first Execute) and memory-bounded per-portal-execution. The
"true cursor" shape (the engine streams as the client paginates)
arrives with SP-A T14 streaming-rows.

Locked KAT: Execute(portal, max_rows=2) on a 5-row result emits
RowDescription + DataRow×2 + PortalSuspended; a second Execute
(same portal, max_rows=2) emits DataRow×2 + PortalSuspended; a
third (max_rows=2) emits DataRow×1 + CommandComplete("SELECT 5").

## 8. Wire message decoders (`extq/proto.rs`)

Seven new decoders, one per frontend Extended Query tag. Each takes
the message body (`length` already stripped by the framing layer
in `server::run_session`) and returns a typed `ExtqMessage` enum.

Each decoder validates field counts + length-internal-consistency
before returning. Malformed messages return `ExtqError::*` which
the dispatcher converts to `ErrorResponse('08P01' protocol_violation,
"<reason>")` and enters error-recovery state.

| Tag | Decoder | Returns |
|---|---|---|
| `P` Parse | `decode_parse(body)` | `ExtqMessage::Parse { name, sql, param_oids: Vec<u32> }` |
| `B` Bind | `decode_bind(body)` | `ExtqMessage::Bind { portal, stmt, param_formats: Vec<u16>, param_values: Vec<Option<Vec<u8>>>, result_formats: Vec<u16> }` |
| `D` Describe | `decode_describe(body)` | `ExtqMessage::Describe { target: 'S' | 'P', name }` |
| `E` Execute | `decode_execute(body)` | `ExtqMessage::Execute { portal, max_rows: i32 }` |
| `S` Sync | `decode_sync(body)` | `ExtqMessage::Sync` (body is empty; just a marker) |
| `C` Close | `decode_close(body)` | `ExtqMessage::Close { target: 'S' | 'P', name }` |
| `H` Flush | `decode_flush(body)` | `ExtqMessage::Flush` (body is empty; just a marker) |

Wire formats (canonical PG §55.7):

```text
P  Parse:
   [name:cstring] [sql:cstring] [param_count:i16] [param_oid:i32]*

B  Bind:
   [portal:cstring] [stmt:cstring]
   [param_format_count:i16] [param_format:i16]*
   [param_value_count:i16] [(param_length:i32 [bytes:param_length])]*
   [result_format_count:i16] [result_format:i16]*

D  Describe:
   [target:i8 = 'S'|'P'] [name:cstring]

E  Execute:
   [portal:cstring] [max_rows:i32]

S  Sync:  (empty body)

C  Close:
   [target:i8 = 'S'|'P'] [name:cstring]

H  Flush: (empty body)
```

10 KATs in `extq/proto.rs` lock each decoder against the canonical
libpq-source byte patterns (`src/interfaces/libpq/fe-exec.c` for
the encoder side; `src/backend/tcop/postgres.c` for the decoder
side; both publicly mirrored on github.com/postgres/postgres).

## 9. Wire message encoders (`extq/response.rs`)

Six new backend-message encoders. Each returns `Vec<u8>` containing
the full wire frame (type byte + length prefix + payload).

| Tag | Encoder | Length | Notes |
|---|---|---|---|
| `1` ParseComplete | `encode_parse_complete()` | 5 bytes | `1 [length=4]` empty body |
| `2` BindComplete | `encode_bind_complete()` | 5 bytes | `2 [length=4]` empty body |
| `3` CloseComplete | `encode_close_complete()` | 5 bytes | `3 [length=4]` empty body |
| `n` NoData | `encode_no_data()` | 5 bytes | `n [length=4]` empty body |
| `s` PortalSuspended | `encode_portal_suspended()` | 5 bytes | `s [length=4]` empty body |
| `t` ParameterDescription | `encode_parameter_description(oids)` | 7 + 4·N | `t [length] [count:i16] [oid:i32]*` |

`RowDescription` ('T') / `DataRow` ('D') / `CommandComplete` ('C') /
`ReadyForQuery` ('Z') / `ErrorResponse` ('E') / `EmptyQueryResponse`
('I') already exist in V1's `response.rs` / `error.rs` and are
re-used unchanged.

Five of the six encoders are trivial (4-byte-length empty-body
envelopes) — they're locked individually as KATs because byte-flip
regressions would silently break every PG client.

## 10. Task decomposition (T1-T12)

| T# | Scope | KAT delta | Real-wire ship? |
|---|---|---|---|
| **T1** | (this commit) Design spec + scaffold: `extq/mod.rs` (state types + placeholder `try_dispatch_extq` returning `Err(NotYetImplemented)`), `extq/proto.rs` (decoders for P/B/D/E/S/C/H + 10 KATs against PG §55.7 canonical byte patterns), `extq/response.rs` (encoders for ParseComplete/BindComplete/CloseComplete/NoData/PortalSuspended/ParameterDescription + 6 KATs), `extq/substitute.rs` (parameter substitution skeleton + ~2 KATs), `lib.rs` re-exports, `server.rs` extends `run_session` to recognize extq tags + route to `try_dispatch_extq` (still returns NYI in T1) | +10-15 | NO — scaffolding only; the dispatcher returns NYI |
| **T2** | Parse + ParseComplete e2e: real `try_dispatch_extq` for `P`; named/unnamed statement storage; ParseComplete emit; `08P01` for cap-overflow + decode errors; lock the "Parse stores SQL verbatim" invariant; +5 KATs | +5-8 | YES — server accepts Parse |
| **T3** | Bind + BindComplete e2e: portal storage; per-position param-format validation (V1 rejects code 1); param-value extraction including NULL sentinel; BindComplete emit; `0A000` for binary-format param request; +5 KATs | +5-8 | YES — server accepts Bind |
| **T4** | Describe 'S' → ParameterDescription + RowDescription/NoData: schema lookup via existing `EngineApply::describe_table` + `kessel_sql::select_star_table`; emit ParameterDescription with the param OIDs from Parse (or empty if Parse didn't provide); NoData for non-SELECT statements; +4 KATs | +4-6 | YES — Describe 'S' works |
| **T5** | Describe 'P' → RowDescription/NoData: same shape as Describe 'S' but no ParameterDescription (portals don't have parameter info per the PG spec — Bind already substituted); +3 KATs | +3-5 | YES — Describe 'P' works |
| **T6** | Execute + parameter substitution + result streaming: text-format param substitution via `extq/substitute.rs`; dispatch through the existing `dispatch_query` SQL path; emit DataRow* + CommandComplete; portal cursor state machine; +8 KATs | +8-12 | YES — full Execute works |
| **T7** | Sync + ReadyForQuery + error recovery state machine: flush per-Sync output; reset error_state on Sync; `08P01` for unsupported subprotocol tags inside a Sync block; emit `Z('I')`; the SkipUntilSync loop body; +5 KATs | +5-8 | YES — pipelining works |
| **T8** | Close ('S'/'P') + CloseComplete + Flush: drop stmt/portal; CloseComplete emit; Flush is a no-op-emit that just triggers a stream flush; +4 KATs | +4-6 | YES — Close + Flush work |
| **T9** | max_rows pagination + PortalSuspended + cursor preservation: Execute(max_rows=N) buffers + pages; PortalSuspended emit; second Execute on same portal continues from buffered cursor; +5 KATs | +5-8 | YES — paginated Execute works |
| **T10** | Pipelining stress test + real libpq round-trip: 100-message pipeline through one connection; orderings preserved; output buffer correctness under interleaved messages; manual psql `PREPARE x AS SELECT $1; EXECUTE x(42)` verification (uses simple-query PREPARE/EXECUTE, NOT extended-query — but proves the engine apply_sql substitution path holds); +4 KATs | +4-6 | YES — pipelined and tested |
| **T11** | SQLAlchemy/psycopg connect-probe end-to-end: real `engine.connect()` against a running kesseldb-server with pg-gateway feature; capture the probe sequence; assert NO `08P01` in the response stream; commit the recorded transcript as a `tests/sqlalchemy_probe_*.pcap`-like fixture; +3 KATs | +3-5 | YES — SQLAlchemy connects |
| **T12** | JDBC / Drizzle / Prisma compat smoke + USAGE update + SP-PG-EXTQ arc closure: doc results in USAGE.md, log any compat gaps as named follow-ups, update SP-PG-EXTQ progress tracker → CLOSED, update STATUS.md row + bullet | +0-3 | YES — multi-client compat confirmed |

Estimated V1 total: **~60-90 KATs across 12 slices**.

V2 follow-ups (each its own arc):
- **SP-PG-EXTQ-BIN** — binary-format parameters (format code 1)
- **SP-PG-EXTQ-CACHE** — server-side prepared-statement cache that survives reconnect
- **SP-PG-EXTQ-PARSED** — `kessel-sql` parameter-AST node (replaces text substitution)
- **SP-PG-TX** — transaction-block awareness (`Z('T')` and `Z('E')`)

## 11. Self-review — 7+ weak spots of this V1 design

1. **SQL-text parameter substitution is brittle**. The substitution
   walks the SQL text replacing `$N` outside quoted regions. Edge
   cases the algorithm handles correctly today (single-quoted
   strings, double-quoted identifiers, `--` line comments, `/* */`
   block comments) but a future SQL extension could break. The
   honest fix is V2 `SP-PG-EXTQ-PARSED` — extend `kessel-sql` with
   a `Parameter(usize)` AST node so the placeholder is a typed
   primitive the planner sees, not a literal that needs textual
   substitution. Documented; locked by §4 KATs against the known
   edge corpus.

2. **No structured AST means SQL-injection prevention relies on the
   text substitution's `'` → `''` escaping**. Specifically, a
   client bound value containing a `'` that the V1 escape rule
   missed would let the value escape the quoted region in the
   rewritten SQL. Mitigation: V1's substitution does the doubling
   in a single pass — no regex, no partial replacement, no
   conditional logic that could skip an escape. KATs lock the
   `O'Brien` / `bobby);` adversarial cases. Still a real attack
   surface — V2 `SP-PG-EXTQ-PARSED` removes the attack surface
   entirely by never serializing the value into SQL text.

3. **Auto-drop of unnamed portals + their cursor state could leak
   under pathological pipelining**. A client that issues
   `Bind portal="" / Execute / Bind portal="" / Execute / ...`
   tight-loop creates and drops the unnamed portal on each iteration,
   which is correct but creates allocator churn. V1 doesn't pool;
   profiling under realistic ORM workload would tell us if pooling
   matters. Documented; not pre-optimized.

4. **No flow control on Execute streaming**. V1's Execute emits
   DataRows as fast as the OS write buffer accepts. A slow client
   (mobile network) Executing a large SELECT could pin server
   memory in the OS write queue. SP-WS T5's `mpsc::sync_channel`
   backpressure pattern is the right shape; V1 inherits the existing
   V1 simple-query "write straight to stream" shape and the same
   limitation. Documented; the SP-PG V1 spec §8.5 send-queue work
   (deferred there as T15 — perf follow-up) covers both Simple
   Query and Extended Query when it lands.

5. **PortalSuspended is a real cursor; V1 buffers all rows first**.
   The semantic invariant (a portal preserves its snapshot across
   re-Executes) is correctly emulated by buffering. The MEMORY
   invariant (per-portal RSS proportional to total result size,
   not per-page batch size) is not. A client that Binds a portal
   over a 10M-row SELECT and Executes 10 rows at a time pays for
   10M rows of RSS at the first Execute. The real fix is SP-A T14
   streaming-rows, named in SP-PG V1 §11 #3. V1's buffering shape
   is correct for OLTP-shaped traffic (small result sets) and
   honest about the limit for analytical traffic.

6. **Most ORMs use `DISCARD ALL` after disconnect — V1 ignores
   it**. `DISCARD ALL` is a server-side reset command that drops
   all prepared statements, sequences, etc. In V1, statements +
   portals auto-drop on connection close anyway, so the ORM
   behavior (ORM doesn't connection-pool sessions cross-checkout
   without DISCARD) Just Works. Documented; the SQL string
   `DISCARD ALL` flows through `apply_sql` and the engine returns
   `42601 syntax_error` which the ORM tolerates because it's
   defensive. Locked by a §12 KAT.

7. **The compile-cache SP47 epoch invalidation must apply to the
   prepared-stmt cache too**. SP47's compile cache de-duplicates
   parsed-AST across `apply_sql` calls and invalidates on schema
   change (CREATE TABLE / ALTER TABLE / DROP TABLE). V1's prepared
   statements DON'T cache the AST — they re-parse on every Execute,
   so SP47 invalidation already covers them transparently. BUT a
   future V2 (SP-PG-EXTQ-PARSED) that caches the AST inside the
   prepared statement would need to subscribe to the SP47 epoch
   counter and invalidate on schema-version change. Documented as
   a V2 must-do.

8. **No client-side cancel during a long Execute**. PG supports
   `CancelRequest` (separate TCP connection, uses BackendKeyData)
   to interrupt a running query. SP-PG V1 generates the
   BackendKeyData but doesn't action CancelRequest (V2 SP-PG-T24).
   SP-PG-EXTQ inherits the same limitation — a long Execute can't
   be cancelled. Documented; same V2 fix.

9. **Pipelined error in the middle of a Sync block silently drops
   the rest of the messages**. This is the protocol — PG itself
   does it. But the client may not realize that Bind(stmt="x")
   was silently skipped because Parse(stmt="x") failed. Mitigation:
   the single ErrorResponse the server emits names the failing
   message (we include the position the error happened in the
   Sync block); the client correlates by Sync-block ordering. Good
   PG client libraries (libpq, JDBC PG driver) do this correctly;
   bespoke clients might miss. Documented in USAGE.md.

10. **Parameter-OID hints from Parse are ignored**. The wire field
    is parsed and stored but V1 doesn't validate that a bound
    value's text-format matches its claimed OID. Clients that
    claim `INT8` and bind `"not an int"` get the SQL-parser's
    type-mismatch error at Execute, not a Bind-time error. Honest
    trade-off — type-checking at Bind would require V1 to
    pre-validate every text-format-→-type coercion before passing
    the SQL to the engine, doubling the parser work. The behavior
    matches PG itself (PG also defers some type-validation until
    Execute when Bind only declared inferred types). Documented.

## 12. Open questions

- **DISCARD ALL semantics in V1**. `DISCARD ALL` should drop all
  prepared statements + portals. V1 routes the SQL through
  `apply_sql` which the engine rejects with `42601`. The
  pragmatically-correct V1 behavior is: intercept `DISCARD ALL`
  at the Simple-Query layer (BEFORE engine apply_sql), clear the
  `SessionState`, emit `CommandComplete: DISCARD ALL`. T7 may
  include this; deferred to T12 if it adds scope.

- **`PREPARE` / `EXECUTE` SQL statements**. PG SQL itself has
  server-side `PREPARE name AS SELECT $1; EXECUTE name(42);` (this
  is the Simple-Query path's prepared-statement shape, distinct
  from the Extended Query protocol). V1 PG-wire today routes these
  to `engine.apply_sql` which doesn't support them — `42601`. The
  honest gap is "Extended Query lands but Simple Query PREPARE
  doesn't" — for adoption that's fine (every ORM uses the
  Extended Query protocol, not SQL-level PREPARE). Documented;
  V2 `SP-PG-SQL-PREPARE` would close.

- **`max_rows == 1` as the "fetch one then suspend" pattern**.
  Some clients use Execute(max_rows=1) repeatedly to iterate over
  result sets. V1's buffer-then-page approach handles this
  correctly; performance is acceptable (RAM scales with total
  rows, not per-Execute). Locked by §7 KAT.

- **Connection-level prepared-statement count limit interaction
  with ORM connection pools**. Most pools default to 10-50
  connections per app instance, each potentially holding 10-100
  prepared statements. 50 × 100 = 5000 per server. V1 cap is
  4096 per connection (well above) — total prepared statements
  across a server with 256 connections would be theoretically up
  to 256 × 4096 = 1M. Per-connection RSS is bounded; aggregate
  RSS is a multiplication. Documented; operators with high-stmt
  workloads can tune `MAX_PREPARED_STATEMENTS_PER_CONN` down.

- **Behavior on Parse of an empty SQL string**. PG itself accepts
  this — it's a "no-op prepared statement" that emits ParseComplete
  but, when Bound + Executed, emits EmptyQueryResponse instead of
  CommandComplete. V1 follows this rule. Locked KAT.

## 13. Acceptance criteria

V1 (T1-T12) ships when:

1. **psql PREPARE/EXECUTE simple-query path** still works (regression
   check — SP-PG V1 already passes this; the extq additions don't
   regress the simple-query path).
2. **psql extended-query smoke**: `psql -h localhost
   "PREPARE x AS SELECT \$1; EXECUTE x(42)"` returns `42`. (psql's
   `\bind` extension uses the wire-level Extended Query path.)
3. **psycopg2/psycopg3 round-trip**:
   ```python
   conn = psycopg2.connect("...")
   cur = conn.cursor()
   cur.execute("SELECT %s", (42,))
   assert cur.fetchone() == (42,)
   ```
   succeeds.
4. **SQLAlchemy probe**: `engine = sqlalchemy.create_engine(url);
   with engine.connect() as conn: conn.execute(sqlalchemy.text(
   "SELECT 1"))` succeeds without `08P01` in the wire response.
5. **Prisma**: `prisma db pull` against KesselDB returns a schema
   (one or more of the SP-PG-CAT pg_catalog-introspection queries
   come back as Parse/Bind/Execute sequences).
6. **Pipelining**: 100 chained Parse/Bind/Execute (interleaved
   different stmts) within one Sync block complete with the right
   per-message reply ordering and one ReadyForQuery at the end.
7. **Error recovery**: a deliberate Parse-error in the middle of
   a pipelined Sync block silently skips the subsequent B/E/D and
   resumes after Sync.
8. **Memory bounds**: a client issuing 5000 Parses on one
   connection gets `08P01` on the 4097th and the connection
   remains alive for subsequent (legitimate) traffic.
9. **No regression on Simple Query**: 1820 pg-gateway-featured
   tests still pass; 1875 all-features still pass.
10. **Zero-dep stance preserved**: `cargo tree -p kessel-pg-gateway
    -e normal` shows only workspace crates. No tokio, no async-rt,
    no pgwire crate.
11. **seed-7 GREEN**, default tree-grep EMPTY, CI green at every
    commit on this arc.

## 14. References

- SP-PG V1 design spec: `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`
- SP-PG V1 progress: `docs/superpowers/specs/2026-05-27-kesseldb-subproject-sppg-progress.md`
- SP-PG-CAT design spec: `docs/superpowers/specs/2026-05-27-kesseldb-sppgcat-pg-catalog-design.md`
- PostgreSQL Documentation §55.2.3 — Extended Query
- PostgreSQL Documentation §55.7 — Message Formats (every tag + field-layout cited above)
- libpq source `src/interfaces/libpq/fe-exec.c` — frontend message encoders V1 mirrors
- PostgreSQL source `src/backend/tcop/postgres.c` — backend message dispatcher V1 mirrors
- `crates/kessel-pg-gateway/src/proto.rs` — message-tag constants (already in V1)
- `crates/kessel-pg-gateway/src/dispatch.rs` — Simple Query path V1 ships
- `crates/kessel-pg-gateway/src/server.rs::run_session` — per-connection loop the new extq tags hook into
