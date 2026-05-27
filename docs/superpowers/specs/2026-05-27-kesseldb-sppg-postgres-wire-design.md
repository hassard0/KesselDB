# SP-PG — PostgreSQL wire protocol support for KesselDB — DESIGN

**Status:** design — scopes the PG-wire V1 work into ~22 concrete task
slices and locks the wire-protocol invariants the implementation tasks
will de-risk one at a time. Companion progress tracker at
`docs/superpowers/specs/2026-05-27-kesseldb-subproject-sppg-progress.md`.

**Builds on:**
- **SP156 scoping** (`docs/superpowers/specs/2026-05-26-kesseldb-http2-ws-pgwire-scoping.md`)
  — picked PG-wire as the highest-user-value of the remaining wire
  surfaces (~25-30 slices, split across "Phase 1 demo / Phase 2
  production"). This spec is the next-level-down of SP156 §4 + §7.2 +
  §8.3.
- **SP-WS** (`docs/superpowers/specs/2026-05-26-kesseldb-spws-websocket-design.md`)
  — sibling wire surface that JUST closed. SP-PG mirrors SP-WS's
  reader/writer-thread session loop, bounded `mpsc::sync_channel` send
  queue, per-connection `std::thread`, monotonic `std::time::Instant`
  heartbeat, and the "design spec → scaffold (T1) → handshake (T2) →
  framing (T3/T4) → session loop (T5) → app wire-up (T6)" T-slice
  shape. Where SP-WS frames carried `Op::encode()`/`OpResult::encode()`
  bytes opaquely, SP-PG dispatches PG SQL into `kessel-sql` + maps
  KesselDB `OpResult` back into PG row + error messages. The
  ws-scaffold pattern (`ws::handle_upgrade` returning
  `Err(WsError::NotYetImplemented)` until T2 flips it) carries over.
- **SP141 / SP147 / SP148** — HTTP/1.1 gateway. PG-wire is NOT in the
  HTTP gateway; it's a SIBLING listener (separate port, separate
  protocol surface). The integration shape (per-connection
  `std::thread`, bounded `DEFAULT_MAX_CONNS=1024`, `Arc<dyn
  EngineApply>` dispatch) is the same, but the listener loop is
  independent — a misbehaving `psql` cannot starve HTTP clients.
- **`kessel-sql`** — the SQL parser already in-tree, deterministic,
  zero-dep. PG-wire's simple-query path forwards the SQL text into
  `EngineApply::apply_sql` unchanged. KesselDB's SQL dialect is the
  V1 PG-wire surface; if a PG-shaped statement (`RETURNING`, `ON
  CONFLICT`, …) isn't supported by `kessel-sql`, the gateway returns
  a PG ErrorResponse with SQLSTATE `0A000` (feature_not_supported) —
  growing the SQL surface to match PG is a SEPARATE arc (SP-PG-SQL).
- **`kessel-client::format_result_json`** — the existing
  `OpResult → JSON` adapter. PG-wire ships a SIBLING adapter
  `format_result_pg(&OpResult, &mut FrameSink)` that emits
  `RowDescription` + 0..N `DataRow` + `CommandComplete` per the
  Postgres §55 message catalog — entirely separate code path from
  the JSON adapter; the shared seam is the `OpResult` variant
  vocabulary.
- **`kessel-crypto`** — SHA-256 + HMAC-SHA-256 already in-tree (from
  SigV4 + the SP-WS handshake-key chain). SCRAM-SHA-256 needs PBKDF2
  (HMAC-SHA-256 iterated 4096 times) — net-new code, fits the
  existing zero-dep pattern. RFC 5802 SCRAM channel binding is V1
  out-of-scope.

---

## 1. Context — why PG wire

SP156 §4 picked PG wire as THE single biggest adoption multiplier for
KesselDB after the HTTP gateway + WebSocket. If `psql
'postgresql://kesseldb:5432/kessel'` works, KesselDB inherits — at zero
new-client-SDK cost — the entire Postgres tooling ecosystem:

| Surface | What we unlock |
|---|---|
| **CLI** | `psql`, `pgcli` |
| **Admin GUIs** | DBeaver, pgAdmin, DataGrip, TablePlus, Postico |
| **BI / dashboards** | Tableau, Metabase, Looker, Grafana, Mode, Superset, Redash, Hex |
| **ORMs** | SQLAlchemy, Django, Rails AR, Prisma, Drizzle, GORM, Diesel, sqlx |
| **Python** | psycopg2, psycopg3, asyncpg |
| **Java/Kotlin** | JDBC PG driver, R2DBC |
| **Node** | `pg`, `postgres.js` |
| **Go** | `pgx`, `lib/pq`, `sqlx-pg` |
| **Rust** | `tokio-postgres`, `postgres`, `sqlx` (PG mode) |
| **ETL** | dbt-postgres, Fivetran-PG-source, Airbyte-PG, Singer-tap-postgres |
| **ODBC** | every BI/IDE that speaks ODBC via the PG driver |

No other single wire surface comes close. "We speak PG wire" is the
sentence every Postgres-compatible engine on the market leads with
(Cockroach, Yugabyte, AlloyDB, Aurora-PG, Materialize, RisingWave,
Greenplum, …) and it is the adoption lever.

SP156 §4 also called out the COST honestly: getting from "psql
connects, simple SELECT works" (Phase 1, ~10 slices) to "DBeaver/
Metabase work for real workloads" (Phase 2, ~15 slices) is two arcs.
This design covers PHASE 1 (V1) only. Phase 2 + `pg_catalog` stubs +
extended query (Parse/Bind/Execute) + COPY are NAMED FOLLOW-UPS, not
V1 features.

The cost-vs-value case for PG wire (vs HTTP/2 vs HTTP/3 vs WebSocket)
is laid out in SP156 §6. SP-WS shipped first because it de-risks the
long-lived-connection plumbing (reader/writer-thread split, bounded
send queue, monotonic-clock heartbeat); SP-PG REUSES that pattern.

## 2. Scope

### 2.1 V1 — what's in

1. **Listener** on a dedicated TCP port (default 5432, configurable
   via `PgGatewayConfig.listen_addr`). Bind separately from the HTTP
   gateway listener; per-connection `std::thread` mirrors
   `serve()::handle_one`. Connection cap defaults to
   `DEFAULT_MAX_PG_CONNS = 256` (smaller than HTTP's 1024 — PG
   clients hold connections longer; 256 is enough for typical
   workloads while keeping per-thread overhead bounded). The PG
   listener and HTTP listener do NOT share the cap (a misbehaving
   pgcli cannot starve HTTP clients).
2. **Postgres Frontend/Backend protocol v3.0** (protocol version
   integer 196608) — the only version libpq has spoken since 2003.
3. **Startup handshake** — read StartupMessage, validate protocol
   version, accept parameters (user, database, application_name,
   client_encoding, …), respond with the AuthenticationSASL
   challenge.
4. **SCRAM-SHA-256 auth** (RFC 5802 + RFC 7677) — V1 ships SCRAM as
   the ONLY auth mechanism. Rationale: modern (PG 10+ default since
   2017), doesn't transmit the password in cleartext, requires no
   TLS, and uses primitives we already have (SHA-256 + HMAC-SHA-256;
   we add PBKDF2). Plain cleartext password auth and MD5 auth
   (deprecated by PG 14+) are V1 NON-GOALS. See §6 + §7 for the auth
   bridge that maps SCRAM credentials to the same Bearer token the
   HTTP gateway accepts, eliminating dual credential storage.
5. **`ParameterStatus` + `BackendKeyData` + `ReadyForQuery`
   greeting** after auth success — minimum set: `server_version`,
   `server_encoding`, `client_encoding`, `DateStyle`, `TimeZone`,
   `integer_datetimes`, `standard_conforming_strings`,
   `application_name`. `BackendKeyData` carries a (pid, secret)
   pair that V1 generates but doesn't action — query-cancellation is
   a V2 (post-V1) feature.
6. **Simple Query protocol** (RFC equivalent: PG §55.2.2). Client
   sends a `Q` message with one or more semicolon-separated SQL
   statements; server responds with one
   (`RowDescription` + 0..N `DataRow` + `CommandComplete`) cycle per
   statement, ending in `ReadyForQuery('I')`. V1 supports ONE
   statement per `Q` (multi-statement `Q` rejected with `42601`
   syntax_error). Streaming: server emits `DataRow` per result row
   as it materializes — does NOT buffer the entire result set before
   sending.
7. **SELECT / INSERT / UPDATE / DELETE** end-to-end. The SQL text
   forwards verbatim into `EngineApply::apply_sql(&str)` → the
   returned `OpResult` is translated into the matching PG response
   stream by `format_result_pg`.
8. **Type-OID mapping for KesselDB FieldKinds** (§5). Text-format
   wire encoding only in V1 (every column is sent as the PG text
   representation; clients that prefer binary format get text and
   parse it). Binary-format is a V2 feature.
9. **`ErrorResponse` with SQLSTATE codes** (§7). Every KesselDB
   `OpResult` variant that isn't success maps to a PG SQLSTATE code +
   severity + message. The SQLSTATE catalog is pinned in this spec
   (§7.2) so clients (which switch on SQLSTATE) get stable behavior.
10. **`Terminate` (`X`) message handling** — client requests graceful
    close; server replies nothing, closes TCP. Idle timeout (default
    600s, configurable via `PgGatewayConfig.idle_timeout`) catches
    clients that vanish without `X`.
11. **Backpressure / send queue** — same `mpsc::sync_channel` pattern
    as SP-WS, with `PG_SEND_QUEUE_BOUND = 64` (deeper than WS's 16
    because PG streams `DataRow` per row — a SELECT returning 10K
    rows must not deadlock if the OS write buffer is slow to drain).
    Queue overflow → close session + log an error; never silent drop.
12. **Per-connection thread cap** — bounded by
    `DEFAULT_MAX_PG_CONNS`. Excess attempts get `E` (error response,
    `53300` too_many_connections), then close.

### 2.2 V1 — what's out (named, deferred — each is its own arc)

These are explicitly NOT V1 features. Each is named so future scoping
finds the design call without re-litigating:

- **Extended Query protocol** (Parse / Bind / Describe / Execute /
  Sync / Close — message tags `P` / `B` / `D` / `E` / `S` / `C`).
  Mandatory for every ORM and prepared-statement client. V2 — owns
  its own design spec (`SP-PG-EXTQ`) because the per-portal /
  per-statement state model is a real engine extension, not just a
  wire-layer addition. Estimated 3-5 slices.
- **Binary format wire encoding** (per-column, per-direction
  format-code 0=text/1=binary negotiated in `Bind`). V1 ships text
  format only. V2 follow-up estimated 2 slices (int / float / bool /
  text / timestamp / timestamptz first; `numeric` last because PG
  binary numeric is base-10000 variable-length-digit and bug-prone).
- **`pg_catalog.*` introspection stubs** (pg_type, pg_class,
  pg_attribute, pg_namespace, pg_proc). psql `\dt` issues queries
  against these and crashes if they're missing; pgAdmin/DBeaver
  issue ~50 introspection queries on connect. V1 returns an
  ErrorResponse with `42P01` (undefined_table) for `pg_catalog.*`
  queries — psql `\d` won't work but bare `SELECT 1` does. V2 ships
  empty single-row stubs; V3 ships real introspection.
- **`COPY FROM STDIN` / `COPY TO STDOUT`** (message tags `d` / `c` /
  `f` / `G` / `H` / `W`). Bulk-load protocol flow used by `psql
  \copy` and every bulk loader. V2 follow-up estimated 2-3 slices.
- **`LISTEN` / `NOTIFY`** — Postgres pub/sub built into the wire.
  We don't have changefeeds yet. Hard pass until we do.
- **Replication protocol** (`START_REPLICATION`, WAL streaming).
  Out of scope indefinitely — KesselDB has VSR, not WAL replication.
- **`CancelRequest`** — query cancellation on a separate TCP
  connection using `BackendKeyData`. V1 generates the key pair but
  takes no action on incoming `CancelRequest` (logs + ignores).
  Cancel-key table + interrupt-an-in-progress-engine-apply is a real
  engine change; V2.
- **GSSAPI / Kerberos auth** — enterprise SSO. Skip indefinitely;
  delegate to a sidecar proxy.
- **LDAP auth** — same rationale: delegate to a sidecar bind tool.
- **Cert-based auth** — trivial on top of the TLS feature; bundle
  with TLS slice.
- **TLS** (SSLRequest 8-byte preface + TLS handshake on the same
  port). V1 plaintext only. V2 wires TLS using the existing
  `rustls` feature gate. Estimated 1 slice.
- **MD5 auth** — deprecated by PG 14+, used by legacy clients only.
  V1 advertises SCRAM-only; if a client requests MD5 the handshake
  fails with `28P01` (invalid_password). V2 may add MD5 if a real
  legacy consumer appears.
- **Cleartext password auth** — never V1. Even on TLS, cleartext
  password is structurally weaker than SCRAM and removes the
  KesselDB-Bearer ↔ PG-SCRAM equivalence (§6) we want to ship as
  the single auth-source-of-truth invariant. V2 only if forced.
- **GUCs / `SET timezone = …` session state**. V1 accepts the
  `SET` statement as a no-op (returns `CommandComplete: SET`) but
  does NOT actually rewrite subsequent timestamp formatting.
  Documented in spec §10. V2.
- **`RETURNING`** on INSERT/UPDATE/DELETE — V1 returns the standard
  `CommandComplete: INSERT 0 N` tag; clients that want
  RETURNING get an ErrorResponse `0A000` from `kessel-sql`. V2.
- **Server-side pipelining** — concurrent multiple-in-flight
  requests per connection. V1 is lockstep (one `Q` → one full
  response cycle → next `Q`). V2 with extended-query support.
- **Per-frame replay protection** — the HTTP gateway's
  `X-Kessel-Client-Id` + `X-Kessel-Req-Seq` headers are HTTP-layer
  concepts; V1 PG-wire does NOT promote them into the PG message
  protocol (no such headers exist). PG clients that need exactly-
  once should use a connection-pool-supported transaction wrapper.

## 3. Wire protocol — PG message catalog that applies in V1

| Direction | Tag | Name | V1 disposition |
|---|---|---|---|
| ← C | — | StartupMessage | Required first message (no tag byte; just length + protocol_version + key/value pairs ending in `\0\0`) |
| ← C | — | SSLRequest | V1 replies `N` (no TLS); TLS in V2 |
| → S | `R` | Authentication | V1 emits AuthenticationSASL ({mech:"SCRAM-SHA-256"\0}\0) → SASLContinue → SASLFinal → AuthenticationOk |
| ← C | `p` | PasswordMessage | V1: SCRAM client-first / client-final messages carried as `p` payload (RFC 5802 + PG SASL framing) |
| → S | `S` | ParameterStatus | V1 emits server_version, server_encoding, client_encoding, DateStyle, TimeZone, integer_datetimes, standard_conforming_strings, application_name |
| → S | `K` | BackendKeyData | V1 emits (pid:u32, secret:u32); pid generated; secret = `kessel-crypto::sha256(client_addr || timestamp_ns || process_nonce)[..4]` |
| → S | `Z` | ReadyForQuery | V1 emits 'I' (idle) — V1 has no transaction-block state |
| ← C | `Q` | Query (Simple) | V1 accepts; single statement only (multi-statement → 42601) |
| → S | `T` | RowDescription | V1 emits per result set; field-format=0 (text) always |
| → S | `D` | DataRow | V1 emits one per row, streaming |
| → S | `C` | CommandComplete | V1 emits "SELECT N" / "INSERT 0 N" / "UPDATE N" / "DELETE N" / "SET" / "CREATE TABLE" tags |
| → S | `E` | ErrorResponse | V1 emits Severity + SQLSTATE + Message + (optional) Detail / Hint / Position |
| → S | `N` | NoticeResponse | V1 may emit during auth (`server_version` translation note); otherwise reserved |
| → S | `I` | EmptyQueryResponse | V1 emits when the parser sees only whitespace/comments (PG §55.2.2 §3) |
| ← C | `X` | Terminate | V1 closes TCP gracefully on receipt |
| ← C | — | CancelRequest | V1 accepts (separate TCP connection, no tag), logs, takes no action (V2 feature) |
| ← C | `P` `B` `D` `E` `S` `C` `H` | Extended Query | V1 rejects with ErrorResponse `0A000` feature_not_supported; V2 implements |
| ← C → S | `d` `c` `f` `G` `H` `W` | COPY | V1 rejects with `0A000`; V2 implements |
| ← C → S | `F` `v` | FunctionCall | Deprecated since PG 8.0; V1 rejects with `0A000`; never V2 |

### 3.1 Message framing

Every message after the StartupMessage / SSLRequest is:

```
[type:1 byte][length:4 byte BE — includes length itself but NOT type][payload]
```

The StartupMessage and SSLRequest are the only messages WITHOUT a
type byte — they begin with the 4-byte BE length directly. This is
because they pre-date the v3 protocol's type-byte discipline; libpq
keeps it for back-compat.

**Endianness:** ALL multi-byte integers are network byte order
(big-endian). KesselDB internally uses little-endian for `Op::encode`,
so the PG-wire encoder explicitly converts at the boundary. The
existing `kessel-http-gateway::ws::frame` decoder demonstrates the
same BE pattern (RFC 6455 §5 frame lengths are BE) — copy that style.

**Length semantics:** the 4-byte length INCLUDES itself (length 4 =
empty payload) but EXCLUDES the type byte. A length less than 4 is a
protocol violation → close with `08P01` protocol_violation.

**Cap:** every incoming message length is checked against
`PG_MAX_MESSAGE_SIZE = 16 MiB` (matches `ws::frame::MAX_PAYLOAD` for
operational uniformity) BEFORE allocation. A client claiming a 1 GiB
message gets a clean rejection, never `Vec::with_capacity(1 GiB)`.
Same defense shape as SP-WS T4.

### 3.2 StartupMessage layout

```
length:u32 BE | protocol_version:u32 BE | (key:NUL-terminated UTF-8 + value:NUL-terminated UTF-8)* | \0
```

- `protocol_version == 196608` (0x00030000 = major 3 minor 0). Any
  other value → respond with one of: SSL-pre-negotiation magic
  (80877103 → reply 'N' for "no TLS, continue with cleartext"),
  Cancel magic (80877102 → handle as CancelRequest), or
  ErrorResponse `0A000` feature_not_supported for an unknown
  version.
- Parameters MUST include `user`. May include `database`,
  `application_name`, `client_encoding`, `options`, `replication`,
  etc. V1 acts on: `user` (carried into SCRAM challenge as the
  authentication username), `database` (logged, otherwise ignored —
  KesselDB has one logical database), `application_name` (echoed
  back in `ParameterStatus`), `client_encoding` (logged; V1 hard-
  codes server_encoding = UTF-8 and expects client_encoding = UTF-8;
  any other value → `22023` invalid_parameter_value).
- Length field includes itself; payload length = total - 4.

### 3.3 Authentication subflow (SCRAM-SHA-256)

Single-mechanism advertisement keeps the V1 wire small. RFC 5802 +
RFC 7677 + PG §55.3.

```
S → C: R 8+len(mech) "SCRAM-SHA-256\0\0"        (AuthenticationSASL)
C → S: p len "SCRAM-SHA-256\0" len4 client_first
S → C: R 8+len(server_first) server_first        (AuthenticationSASLContinue)
C → S: p len client_final
S → C: R 8+len(server_final) server_final        (AuthenticationSASLFinal)
S → C: R 8 0                                     (AuthenticationOk)
```

Where:
- `client_first` per RFC 5802 §5.1: `n,,n=<user>,r=<client_nonce>`
  (no channel binding in V1 — `n,,` is the GS2 header for "client
  doesn't support channel binding").
- `server_first` per RFC 5802 §5.1: `r=<client_nonce + server_nonce>,
  s=<salt b64>,i=<iter_count>` where iter_count = 4096 (PG default).
- `client_final` per RFC 5802 §5.1: `c=biws,r=<combined nonce>,
  p=<client_proof_b64>` (biws = base64("n,,") — the channel-binding
  data the client claims to have sent).
- `server_final` per RFC 5802 §5.1: `v=<server_signature_b64>`.

V1 implementation note: the SCRAM mechanism state machine is
straightforward when SHA-256 + HMAC-SHA-256 are already available
(they are in `kessel-crypto`). The one new primitive is PBKDF2-HMAC-
SHA-256 (RFC 8018 §5.2) for `SaltedPassword = PBKDF2(password,
salt, i=4096, dkLen=32)`. PBKDF2 is ~20 lines: iterate HMAC-SHA-256
4096 times against a 1-block input, XOR-fold into the output. No new
external dependency.

### 3.4 Bearer ↔ SCRAM bridge

The HTTP gateway authenticates via a single shared-secret Bearer
token (`ServerConfig.token`). V1 PG-wire reuses the SAME token: the
PG server treats `password = ServerConfig.token` for every user. The
SCRAM exchange is unmodified — the server simply uses the Bearer
token as the password input to PBKDF2.

Rationale:
- **One credential surface.** Operators don't manage a separate
  PG-password database; rotating `ServerConfig.token` rotates both
  HTTP-Bearer and PG-SCRAM credentials atomically.
- **No cleartext exposure.** SCRAM means the wire never carries the
  token in cleartext; the client proves knowledge via the HMAC
  protocol. Even tcpdump cannot recover the token from a SCRAM
  exchange (modulo replay-after-recording, mitigated by per-session
  random server nonce).
- **Bearer = SCRAM password** semantics: psql users connect with
  `PGPASSWORD=$KESSEL_TOKEN psql -h host -p 5432 -U any
  postgresql://`. The `user` parameter is ignored (V1 has no user
  table); SCRAM's salt is `kessel-crypto::sha256(server_nonce
  || token)[..16]` — deterministic per server session, not stored
  on disk.

Open mode (no `ServerConfig.token` set): V1 PG-wire REJECTS
connections with `28000` invalid_authorization_specification. Open
mode on the HTTP gateway is "no auth required" — that doesn't
translate cleanly to PG, which has no "skip auth" message in the
flow. Documented gap; operators who want open-mode PG run a TLS-
terminating reverse proxy that injects a token, or set
`PgGatewayConfig.allow_anonymous = true` (V1 ships this knob OFF by
default; if ON the server emits AuthenticationOk immediately after
StartupMessage without a SCRAM challenge — useful for local dev,
NEVER for prod).

## 4. SQL surface — dispatch into `kessel-sql`

V1 PG-wire's simple-query path is a thin shell around
`EngineApply::apply_sql(sql: &str) -> OpResult`:

```text
Q message arrives with SQL bytes
  ↓ split semicolons (V1 = single-statement only)
  ↓ trim leading whitespace + comments
  ↓ if empty → EmptyQueryResponse + ReadyForQuery + return
  ↓ engine.apply_sql(sql) → OpResult
  ↓ format_result_pg(&op_result, &mut sink)
  ↓     sink writes T (RowDescription) + 0..N D (DataRow) + C (CommandComplete)
  ↓     OR E (ErrorResponse) + ReadyForQuery
  ↓ ReadyForQuery('I')
```

Key design points:

- **No SQL rewriting at the PG-wire layer.** `kessel-sql` is the
  authoritative parser. If a SQL statement isn't supported,
  `kessel-sql` returns a `SchemaError(String)` or `Constraint(String)`
  in the `OpResult` and the PG-wire layer translates that into the
  appropriate SQLSTATE (see §7.2). Growing the SQL surface to match
  PG is a SEPARATE arc, named SP-PG-SQL.
- **Type-OID extraction.** `OpResult::Got(bytes)` from a SELECT
  carries the column data per KesselDB's wire format (schema header
  + row bytes). V1 needs to know the table's schema to build
  `RowDescription` — this requires a sibling adapter call
  `engine.describe_table(name) -> Option<TableSchema>` so the
  PG-wire layer can map FieldKind → PG type OID + column name +
  width. New trait method on `EngineApply` ships in T8.
- **Streaming.** `EngineApply::apply_sql` currently materializes the
  full result into `OpResult::Got(Vec<u8>)` — same shape as the
  HTTP gateway. V1 PG-wire walks the materialized bytes per row,
  emitting `DataRow` as it goes. True streaming-from-the-engine
  (sending `DataRow` before the full result is materialized) is the
  same problem SP-A T14 addresses for WebSocket; the PG-wire layer
  will inherit that engine change when it lands. V1 ships per-row
  emit at the WIRE layer (cheap memory ceiling for the client) but
  materialize-then-iterate at the engine layer.
- **Scatter-scan integration.** When the engine is sharded (SP-A),
  `apply_sql` already routes through `Route::Scatter` for cross-
  shard SELECTs. PG-wire inherits this for free — the same scatter-
  gather merge that powers `/v1/sql` powers PG-wire SELECTs.
- **No GUC respect.** V1 ignores `SET timezone = ...` (returns
  `CommandComplete: SET` and moves on). `TimeZone` advertised in
  `ParameterStatus` is `UTC` always. Documented gap.

## 5. PG type OID mapping (locked V1 table)

The full PG type catalog has 600+ entries; V1 ships the subset that
matches KesselDB's `FieldKind`. Mapping is hard-coded in
`crates/kessel-pg-gateway/src/types.rs` and locked by KATs.

| KesselDB FieldKind | PG type name | OID | Wire width (text) | Notes |
|---|---|---|---|---|
| `Bool` | bool | 16 | `t`/`f` | RFC: PG always uses `t`/`f` not `true`/`false` |
| `U8` | int2 | 21 | decimal | unsigned in KesselDB; PG int2 is signed i16 — values 128..=255 map to negative |
| `U16` | int4 | 23 | decimal | unsigned in KesselDB; PG int4 signed |
| `U32` | int8 | 20 | decimal | unsigned → PG int8 always fits |
| `U64` | int8 | 20 | decimal | unsigned → may overflow PG int8; documented; emit error `22003` if > i64::MAX |
| `U128` | numeric | 1700 | decimal string | PG numeric is arbitrary-precision; text format trivial; V2 binary deferred |
| `I8` | int2 | 21 | decimal | |
| `I16` | int2 | 21 | decimal | |
| `I32` | int4 | 23 | decimal | |
| `I64` | int8 | 20 | decimal | |
| `I128` | numeric | 1700 | decimal string | same notes as U128 |
| `Fixed { scale }` | numeric | 1700 | decimal string with `scale` digits | text-format always renders as `<int>.<scale>` |
| `Char(n)` | text | 25 | UTF-8 bytes | V1 ships text not bpchar/varchar — simpler; clients accept |
| `Bytes(n)` | bytea | 17 | `\\x<hex>` | PG bytea text format: backslash-x followed by hex |
| `Timestamp` | timestamptz | 1184 | `YYYY-MM-DD HH:MM:SS.ffffff+00` | KesselDB stores u64 ns; emit ISO-8601 with timezone +00 |
| `Ref` | bytea | 17 | `\\x<32-hex>` | 16-byte ObjectId rendered as bytea text |
| `OverflowRef` | bytea | 17 | `\\x<16-hex>` | 8-byte handle rendered as bytea text |

NULL renders as the empty 4-byte length `0xFFFFFFFF` per PG wire (NOT
empty string); the null bitmap from KesselDB's row header drives this.

### 5.1 Format codes

V1 emits ALL columns as TEXT format (PG format code 0). The
`RowDescription` per-column format-code field is 0 always. Even
clients that advertise binary-format preference in `Bind` (V2) will
get text in V1's simple-query path — simple-query has no Bind, no
format negotiation; clients always get the text format the server
chooses.

### 5.2 Open question — `numeric` arbitrary precision

KesselDB doesn't have arbitrary-precision decimal. V1 maps
`U128`/`I128`/`Fixed` to PG `numeric` and emits the decimal-string
representation — clients reading via psycopg2's `Decimal` adapter
get correct values up to 128 bits of precision. Values larger than
that (which can't exist in KesselDB anyway) are a non-issue.

If a future KesselDB type genuinely needs arbitrary precision
(SP-NUM, hypothetical), the type-OID map gains a new row.

## 6. Auth — the SCRAM-only stance

V1 ships SCRAM-SHA-256 as the ONLY authentication mechanism, with
the Bearer-token bridge (§3.4) as the credential source.

Rationale summary:

- **SCRAM is the modern PG default.** PG 10 (2017) made SCRAM the
  default for new installs. Every libpq since 2017 ships SCRAM
  support. Every JDBC driver since 42.2.0 (2018) ships SCRAM
  support. Every Go `pgx` since 2018 ships SCRAM support. We
  are NOT cutting off a meaningful fraction of clients by going
  SCRAM-only.
- **SCRAM doesn't transmit the password.** Cleartext password
  even on TLS leaks to anyone who breaks TLS (rare but real); MD5
  is broken against rainbow tables; SCRAM uses HMAC + a per-session
  random nonce, so even a recorded SCRAM exchange can't be replayed.
- **PBKDF2 4096 iterations** matches PG's default
  `password_encryption = scram-sha-256`. We don't need a higher
  iteration count for the V1 threat model (the Bearer token is
  high-entropy by construction; PBKDF2 here is "make the protocol
  look like PG" rather than "make a low-entropy password hard to
  brute-force").
- **Channel binding** (`SCRAM-SHA-256-PLUS`) is V2 — requires
  pulling the TLS `tls-server-end-point` channel-binding data from
  rustls. Rustls exposes this in 0.23 via `tls_server_end_point()`
  — wiring is straightforward but is TLS-feature-gated work.

### 6.1 Multi-user model

V1 has ONE shared-secret (the Bearer token). Multiple PG `user`
names map to the same credential. The `user` field in StartupMessage
is logged + carried through SCRAM but is NOT used for authorization
— all authenticated PG connections have the same privileges as a
Bearer-token HTTP request.

Multi-user is a SEPARATE arc (SP-PG-USERS). Requires:
1. A user table in the engine (CREATE USER / ALTER USER / DROP USER
   DDL).
2. Per-user salt + iteration count (stored in the engine, fetched
   in `server_first`).
3. Per-user privilege model (which today doesn't exist in KesselDB).

V1 deliberately defers all of this. Documented in §10.

### 6.2 Failure modes

| Scenario | PG response | SQLSTATE |
|---|---|---|
| Wrong password (client_proof verification fails) | ErrorResponse | `28P01` invalid_password |
| Missing `user` field in StartupMessage | ErrorResponse | `28000` invalid_authorization_specification |
| Bearer-token not set on server (open mode) + `allow_anonymous=false` | ErrorResponse | `28000` invalid_authorization_specification |
| Client requests non-SCRAM mechanism | ErrorResponse | `28P01` invalid_password (or fall back to SASL mechanism unsupported) |
| Client sends MD5 password message | ErrorResponse | `28P01` invalid_password |
| Client sends cleartext password | ErrorResponse | `28P01` invalid_password |

All auth failures close TCP immediately after the ErrorResponse + a
brief `ReadyForQuery` is NOT sent (per PG §55.2 — failed auth → no
ReadyForQuery, just close).

## 7. Errors — SQLSTATE catalog mapping

### 7.1 ErrorResponse message format (PG §55.2.6)

```
E [length:u32 BE]
  S<Severity>\0       (e.g. "ERROR", "FATAL", "PANIC")
  V<Severity>\0       (since PG 9.6 — same value as S, separate field)
  C<SQLSTATE>\0       (5-char alphanumeric code)
  M<Message>\0        (human-readable message)
  D<Detail>\0         (optional, multi-line)
  H<Hint>\0           (optional, single-line "try this")
  P<Position>\0       (optional, byte offset into the SQL text)
  F<File>\0           (optional, source file — V1 omits)
  L<Line>\0           (optional, source line — V1 omits)
  R<Routine>\0        (optional, source function — V1 omits)
  \0                  (terminator)
```

V1 always emits S, V, C, M. D / H / P emit when KesselDB provides
the detail (e.g. constraint name from `OpResult::Constraint`).
F / L / R never emit (would leak Rust source-file paths).

### 7.2 OpResult → SQLSTATE mapping

| KesselDB OpResult | PG severity | SQLSTATE | Message format |
|---|---|---|---|
| `Ok` | — | — | (success path; CommandComplete instead) |
| `Got(bytes)` | — | — | (success path; RowDescription + DataRow* + CommandComplete) |
| `Exists` | ERROR | `23505` unique_violation | "row already present" |
| `NotFound` | — | — | (success path with 0-row result) |
| `TypeCreated(tid)` | — | — | (success path; CommandComplete "CREATE TABLE") |
| `SchemaError(msg)` | ERROR | `42601` syntax_error / `42P01` undefined_table / `42703` undefined_column (heuristic from msg) | msg as-is |
| `Constraint(msg)` | ERROR | `23000` integrity_constraint_violation (or `23502` not_null / `23505` unique heuristic) | msg as-is |
| `Unavailable` | FATAL | `57P03` cannot_connect_now | "not the active primary; rotate to primary" |
| `Unauthorized` | FATAL | `28000` invalid_authorization_specification | "missing or invalid token" |
| `TxAborted { reason: WriteWriteConflict }` | ERROR | `40001` serialization_failure | reason as-is |
| `TxAborted { reason: SnapshotOutOfRange }` | ERROR | `25006` read_only_sql_transaction | reason as-is |
| `TxAborted { reason: StorageIo }` | ERROR | `58030` io_error | reason as-is |
| `TxCommitted { commit_opnum }` | — | — | (CommandComplete "COMMIT") |
| Unknown / future variant | ERROR | `XX000` internal_error | "kessel-pg-gateway: unmapped OpResult variant" |

The heuristic on `SchemaError` parses the message for substrings:
"unknown table" → `42P01`, "unknown column" → `42703`, "type
mismatch" → `42804` datatype_mismatch, "syntax" → `42601`. Default
`42000` syntax_error_or_access_rule_violation. This is the same
shape `format_result_json` uses for the HTTP layer's
distinguished-error case (a sibling helper, not shared code).

The heuristic is an honest compromise — `kessel-sql` doesn't
currently tag its errors with structured kinds. A V2 follow-up
(`kessel-sql` returning a `SchemaErrorKind` enum) would let us drop
the heuristic and emit precise SQLSTATEs.

## 8. Integration — where in the workspace

### 8.1 New crate `kessel-pg-gateway`

Mirrors `kessel-http-gateway`'s shape. Zero external deps (workspace
crates only — `kessel-proto`, `kessel-client`, `kessel-crypto`).

```
crates/kessel-pg-gateway/
├── Cargo.toml          (workspace member, no external deps)
└── src/
    ├── lib.rs          (module declarations, doc pointing at this spec)
    ├── proto.rs        (message-type tags, protocol constants, length helpers)
    ├── server.rs       (listener accept loop, per-connection thread spawn)
    ├── startup.rs      (StartupMessage parser, ParameterStatus emit) — T2
    ├── auth.rs         (SCRAM-SHA-256 state machine, PBKDF2) — T2
    ├── query.rs        (Q message handler, statement-loop dispatch) — T3-T9
    ├── types.rs        (FieldKind ↔ PG OID mapping table, text-format render) — T4-T5
    ├── encode.rs       (RowDescription, DataRow, CommandComplete, ReadyForQuery encoders) — T5-T6
    ├── error.rs        (ErrorResponse encoder, OpResult→SQLSTATE map) — T7
    └── session.rs      (per-connection session loop: reader thread + writer thread + bounded send queue) — T15-T16
```

Locked constants in `lib.rs`:
- `PG_GATEWAY_DEFAULT_PORT: u16 = 5432`
- `PG_PROTOCOL_VERSION_3_0: u32 = 196608`
- `PG_SSL_REQUEST_CODE: u32 = 80877103`
- `PG_CANCEL_REQUEST_CODE: u32 = 80877102`
- `PG_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024`
- `PG_SEND_QUEUE_BOUND: usize = 64`
- `DEFAULT_MAX_PG_CONNS: usize = 256`
- `PG_DEFAULT_IDLE_TIMEOUT_SECS: u64 = 600`
- `PG_DEFAULT_SCRAM_ITERATIONS: u32 = 4096`
- `SUPPORTED_SASL_MECH: &str = "SCRAM-SHA-256"`

### 8.2 Wire into `kesseldb-server` behind `pg-gateway` feature

Mirror the `http-gateway` feature in `kesseldb-server/Cargo.toml`:

```toml
[dependencies]
kessel-pg-gateway = { path = "../kessel-pg-gateway", optional = true }

[features]
default = []
pg-gateway = ["dep:kessel-pg-gateway"]
```

The `kesseldb-server` binary's `main.rs` spawns the PG listener
behind `#[cfg(feature = "pg-gateway")]`, parallel to the HTTP listener
spawn (already exists behind `http-gateway`). Default `cargo build`
links no extra crate. Default `cargo test` has no new tests in the
default workspace (the gateway's own tests run inside its own crate
which IS in the workspace `members` list).

### 8.3 Per-connection thread model

Mirrors `kessel-http-gateway::server::handle_one`:

```rust
fn serve(addr: &str, engine: Arc<dyn EngineApply>, cfg: PgGatewayConfig)
    -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    let conn_count = Arc::new(AtomicUsize::new(0));
    for incoming in listener.incoming() {
        let s = incoming?;
        if conn_count.fetch_add(1, AcqRel) >= cfg.max_conns {
            conn_count.fetch_sub(1, AcqRel);
            write_too_many_connections(&mut s)?;
            continue;
        }
        let engine = engine.clone();
        let conn_count = conn_count.clone();
        let cfg = cfg.clone();
        std::thread::spawn(move || {
            let _guard = ConnGuard(conn_count); // decrements on drop
            let _ = handle_one(s, engine, cfg);
        });
    }
    Ok(())
}
```

### 8.4 Per-connection state machine (T15-T16)

```
ConnectStart
  ↓ read 4-byte length + 4-byte protocol_version
  ↓ branch:
       80877103 (SSL) → reply 'N' → loop back to ConnectStart on same socket
       80877102 (Cancel) → log + close
       196608   (3.0)   → startup_complete
       *                → ErrorResponse 0A000 + close
  ↓
StartupComplete (parsed key/value pairs)
  ↓ run SCRAM exchange (4 round-trips)
  ↓ on success: emit ParameterStatus×N + BackendKeyData + ReadyForQuery('I')
  ↓ on failure: emit ErrorResponse 28P01 + close
  ↓
QueryLoop (idle, waiting for next request)
  ↓ read message type byte + length
  ↓ branch on type:
       'Q' → handle_query → emit T/D*/C/E + Z('I') → loop
       'X' → close cleanly
       'P'|'B'|'D'|'E'|'S'|'C' → ErrorResponse 0A000 + Z('I') → loop (extended query unsupported in V1)
       *   → ErrorResponse 08P01 protocol_violation + close
  ↓ on idle_timeout: emit ErrorResponse 57014 query_canceled + close
```

### 8.5 Session loop concurrency

Per-connection: single reader thread (the caller thread), single
writer thread (spawned via `TcpStream::try_clone()`), `mpsc::sync_
channel::<Vec<u8>>(PG_SEND_QUEUE_BOUND=64)` between them. Same shape
as SP-WS. Heartbeat is NOT applicable — PG-wire has no ping/pong —
but the read-timeout-driven idle-check tick (every 1s) lets the
session enforce `idle_timeout`.

### 8.6 TLS — future feature gate

V2 will mirror the existing `tls` rustls feature. The SSLRequest
preface is V1-aware (the listener always replies 'N'); V2 will reply
'S' if `cfg.tls` is set, perform the rustls handshake, then re-read
the StartupMessage on the upgraded stream. Architectural seam
documented; no V1 code change required.

## 9. Acceptance criteria

V1 (T2 through T18) ships when:

1. **psql connectivity:** `PGPASSWORD=$KESSEL_TOKEN psql -h localhost
   -p 5432 -U test "SELECT 1"` returns `1` and exits 0. Hand-test
   documented in `docs/USAGE.md` after T10 lands; KAT covers the same
   shape with an in-process synthetic peer.
2. **psql interactive:** `psql -h localhost -p 5432 -U test` connects,
   issues `SELECT 1`, returns `1`, then exits via `\q`. `\dt` reports
   "did not find any relations" (or PG error 42P01 with a clear
   message) — does NOT crash psql.
3. **CRUD round-trip:** CREATE TABLE / INSERT / SELECT / UPDATE /
   DELETE all work via psql. Documented in T9.
4. **JDBC connectivity:** the standard `org.postgresql:postgresql`
   driver opens a connection, runs `SELECT 1`, returns `1`. Manual
   smoke test in T11.
5. **Pentest matrix:** 10+ adversarial inputs (truncated messages,
   oversized payloads, malformed SCRAM messages, auth replay,
   SQL injection in `user` field, version-mismatch, control-byte
   smuggling, …) pass — close codes match contract, no panics, no
   leaked threads. T14.
6. **No regression:** all 1450 default + 1483 featured tests still
   pass. `tree-grep` still empty (no new external deps).
   seed-7 still green.
7. **Zero-dep stance preserved:** `cargo tree -p kessel-pg-gateway -e
   normal` shows only workspace crates. `cargo tree -p kesseldb-server
   --features pg-gateway -e normal` shows no new external entries.
8. **HTTP gateway untouched:** existing HTTP/1.1 + WebSocket clients
   work unchanged. PG-wire is additive.

## 10. Task decomposition (T1..T18 V1, T19+ V2 follow-ups)

| T# | Scope | KAT delta (approx) | Real-wire ship? |
|---|---|---|---|
| **T1** | This design spec + scaffold (`kessel-pg-gateway` crate, `proto.rs` constants, `server.rs` placeholder `accept` returning `Err("not yet implemented")`, locked constants, 3-5 KATs for protocol constants + message-type tags + protocol-version magic + framing rules) | +3-6 | NO — scaffolding only |
| **T2** | Startup handshake + SCRAM-SHA-256 auth: StartupMessage parser, ParameterStatus / BackendKeyData / ReadyForQuery emit, SCRAM 4-round-trip state machine, PBKDF2-HMAC-SHA-256 in `kessel-crypto`, Bearer-token bridge | +12-18 | YES — psql gets past auth |
| **T3** | Simple Query: `Q` message parser, SQL-text dispatch into `EngineApply::apply_sql`, EmptyQueryResponse for whitespace-only | +6-8 | YES — server accepts queries |
| **T4** | PG type-OID mapping table + text-format renderer (FieldKind → bytes); locked KAT per FieldKind | +8-12 | NO — used by T5 |
| **T5** | RowDescription + DataRow encoders (per-row streaming emit; uses T4 type table) | +8-10 | YES — server emits result rows |
| **T6** | CommandComplete + ReadyForQuery encoders (tag formats: "SELECT N" / "INSERT 0 N" / "UPDATE N" / "DELETE N" / "SET" / "CREATE TABLE") | +5-8 | YES — server completes statements |
| **T7** | ErrorResponse encoder + OpResult→SQLSTATE map (full table from §7.2) | +8-12 | YES — server reports errors |
| **T8** | SELECT end-to-end: schema lookup (`EngineApply::describe_table` new trait method) + SELECT * FROM table → real result | +6-10 | YES — psql SELECT works |
| **T9** | INSERT / UPDATE / DELETE end-to-end via simple-query | +6-10 | YES — psql CRUD works |
| **T10** | psql compatibility hand-test + USAGE.md sample-session + KAT-level synthetic peer that drives the full handshake → query → close sequence | +4-8 | YES — `psql ... "SELECT 1"` returns `1` |
| **T11** | pgcli / DBeaver / JDBC compatibility smoke (manual; doc results) — one real client per smoke + log any compat gaps as named follow-ups | +0-4 | YES — third-party clients connect |
| **T12** | Listener integration: `kesseldb-server` `pg-gateway` feature flag, `main.rs` spawn parallel to HTTP listener, port config | +3-6 | YES — `kesseldb` binary listens on 5432 |
| **T13** | Bounded connection cap + per-connection thread cap (`DEFAULT_MAX_PG_CONNS=256`); too-many-connections ErrorResponse | +4-6 | YES — server doesn't OOM |
| **T14** | Pentest sweep — 10+ adversarial inputs: truncated startup, oversized message length, malformed SCRAM, auth replay, SQLi in `user`, version 0/1/2/4/65535, NUL in payload, U+0000 in SQL, extended-query message in V1, repeated handshake on already-authed connection | +10-15 | YES — V1 is hardened |
| **T15** | Per-connection reader/writer-thread split + bounded send queue + close-on-overflow (mirror SP-WS T5 shape) | +8-10 | YES — server is concurrent-safe |
| **T16** | Idle timeout + graceful Terminate handling | +4-6 | YES — sessions close cleanly |
| **T17** | Scatter-scan integration — verify cross-shard SELECTs work over PG-wire (uses existing SP-A plumbing; KAT only) | +2-4 | YES — sharded SELECT works |
| **T18** | Docs: ARCHITECTURE.md §Listeners gains PG-wire row; USAGE.md §PG gateway sample-session; README mention of psql connectivity | +0-2 | YES — V1 documented |

Estimated V1 total: **~95-135 KATs across 18 slices**.

Post-V1 (V2 — DBeaver/Metabase/Tableau-ready):

| T# | Scope | Estimate |
|---|---|---|
| **T19** | Extended Query — Parse / Bind / Describe / Execute / Sync / Close (separate design spec `SP-PG-EXTQ`) | ~3-5 slices |
| **T20** | Binary-format wire encoding (int/float/bool/text/timestamp first; numeric last) | ~2 slices |
| **T21** | Minimal `pg_catalog` stubs (pg_type, pg_class, pg_attribute, pg_namespace) — enough for psql `\dt` / `\d <table>` | ~2 slices |
| **T22** | `current_setting()` / `version()` / `current_schema()` / `current_database()` built-in functions | ~1 slice |
| **T23** | `RETURNING` on INSERT/UPDATE/DELETE — requires `kessel-sql` extension | ~1 slice |
| **T24** | Query cancellation (CancelRequest table + interrupt in-progress engine apply) | ~1 slice |
| **T25** | GUC plumbing (`SET timezone = '...'` session state + apply to text-format render) | ~1 slice |
| **T26** | COPY FROM STDIN / COPY TO STDOUT — bulk protocol flow | ~2-3 slices |
| **T27** | TLS — SSLRequest 'S' reply + rustls handshake, behind existing `tls` feature gate | ~1 slice |
| **T28** | MD5 auth fallback for legacy clients (P3 — only if needed) | ~1 slice |

V1 (T1-T18) ships psql + a JDBC driver + simple CRUD. V2 (T19-T28)
ships full BI/ORM compat.

## 11. Self-review — weak spots of this design

1. **Bearer ↔ SCRAM bridge means one credential rotates two surfaces
   atomically — which is also the only knob.** If an operator wants
   "rotate HTTP token but keep old PG password working during a
   maintenance window," they can't. The bridge is a deliberate
   simplification (one secret to manage); V2 SP-PG-USERS introduces
   per-user PG credentials separate from the Bearer token. Until
   then, password rotation is HTTP-and-PG-together.

2. **SchemaError → SQLSTATE heuristic is string-matching.** Without
   a structured `SchemaErrorKind` enum in `kessel-sql`, the
   "syntax error" vs "undefined table" vs "undefined column" choice
   is regex-on-the-message. A client switching on SQLSTATE for
   recovery logic could be surprised when a future `kessel-sql`
   change rewords an error message and shifts a SQLSTATE bucket.
   The honest fix is a V2 follow-up (`SP-PG-SQL-ERRORS`) that adds
   the `SchemaErrorKind` enum to `kessel-sql`. Documented in §7.2.

3. **No streaming-from-the-engine.** V1 PG-wire emits `DataRow` per
   row at the wire layer but the underlying `EngineApply::apply_sql`
   materializes the entire result into `OpResult::Got(Vec<u8>)`
   before returning. For a SELECT returning 10M rows, peak memory
   is 10M-rows-worth, not bounded by `PG_SEND_QUEUE_BOUND * row_size`.
   Same gap as SP-WS T5's wire-streams-but-engine-buffers shape;
   the SP-A T14 streaming-rows arc fixes it for both surfaces
   simultaneously. V1 ships the practical-for-OLTP-sized-results
   thing; large analytical queries hit OS memory before they hit a
   wire limit.

4. **Type-OID mapping pins KesselDB's unsigned ints as signed PG
   ints.** `U64` → PG `int8` (signed i64) means values > 9.2 EiB
   overflow PG int8. V1 emits `22003` numeric_value_out_of_range
   for values > i64::MAX. The alternative (mapping U64 → PG
   `numeric`) is correct but breaks every JDBC driver expecting
   `getLong()` to work; the signed-int pragmatic choice is the
   right one. Documented in §5 table; T14 pentest covers the
   overflow edge.

5. **Single-statement Q-message restriction.** PG simple-query
   allows multiple semicolon-separated statements in one `Q`
   (psql `\copy ... ; SELECT count(*) ...`). V1 rejects multi-
   statement with `42601` to keep the FIFO response-stream model
   simple — a multi-statement `Q` would emit multiple
   (T/D*/C) tuples back-to-back terminated by ONE `Z` at the end,
   not one `Z` per statement. The shape is unambiguous in the RFC,
   just adds state to the dispatch loop. V2 follow-up.

6. **`SET` is a no-op.** A client running `SET timezone = 'UTC'`
   gets `CommandComplete: SET` but the next timestamp query still
   renders in the hard-coded server timezone (UTC). For clients
   that explicitly check `SHOW timezone` after a SET, this returns
   the OLD value — confusing. V1 documents the gap and accepts that
   clients which depend on session-GUC state (Metabase, DBeaver
   sometimes) will see UTC regardless of what they SET. V2 fixes
   with a per-session GUC dict on the gateway.

7. **`allow_anonymous` exists as a config knob but is dangerous.**
   §3.4 ships `PgGatewayConfig.allow_anonymous = false` by default.
   If an operator flips it on, the PG-wire emits AuthenticationOk
   immediately and any TCP connection can run arbitrary SQL. A
   security-paranoid stance would NEVER ship this knob (force
   operators to set a token); a pragmatic stance ships it OFF-by-
   default for local dev. Same tradeoff as `--no-auth` in many
   tools; documented prominently in USAGE.md as "NEVER for prod".

8. **No `pg_catalog` in V1 means GUI tools choke on connect.**
   pgAdmin and DBeaver issue ~50 introspection queries against
   `pg_catalog.*` on the first connect. V1 returns `42P01`
   undefined_table for each. pgAdmin in particular may refuse to
   show the connection in its UI without these. The honest gap is
   "V1 supports CLI clients (psql, pgcli) and language-driver
   programmatic clients (JDBC, psycopg, pgx) but NOT GUI admin
   tools." V2 fixes; V1 documents the boundary clearly in USAGE.md.

9. **PG-wire and HTTP gateway can drift on auth semantics.** The
   HTTP gateway accepts the Bearer token via header; PG-wire
   converts it through SCRAM. If a future change tightens HTTP
   auth (e.g. requires token-prefix `kessel_v1_`), the PG-wire
   side might not learn. V1 mitigation: a single
   `auth::check_bearer(token, supplied) -> bool` helper that BOTH
   gateways call. New arc-overlapping seam.

10. **The pentest matrix is V1-thin.** Spec §8.7 (SP-WS) shipped
    10 pentests; this design names 10 too (T14) but they're
    higher-cost-each because PG message framing is richer than
    WebSocket framing. A "real" pentest matrix is 30+ inputs (PG
    has spent decades being attacked); V1 ships the obvious
    smoke. T14 + future pentest sweeps (SP-PG-PENTEST follow-up
    arcs) close the gap incrementally.

11. **`server_version` lying carries product risk.** Reporting
    "14.0" (or any specific PG version) makes clients gate features
    on it. Reporting "1.0 (KesselDB-via-PG-3.0)" makes some clients
    refuse to connect. V1 picks "14.0 (KesselDB-1.0)" as a
    pragmatic middle — looks like PG 14 for the version-string-
    parser fastpath, but the suffix tells humans the truth. T11
    pentest verifies clients accept the suffix; if a client chokes
    on the suffix, the workaround is to ship a plain "14.0" or
    "16.0" (operator config knob, default-suffixed).

## 12. Open questions

- **Bearer-token rotation grace period.** If `ServerConfig.token`
  rotates while a SCRAM exchange is mid-flight, the new token
  invalidates the in-progress challenge. V1 accepts the race (the
  client retries; the human notices). V2 may want a 60s grace
  window.
- **`user` field semantics when multi-user lands.** V1 logs and
  ignores `user`; V2 SP-PG-USERS will use it. The V1 → V2 transition
  needs to not break any V1 client whose connection string sets
  `user=postgres`. Plan: V2 retains the V1 fallback "if `user` is
  unknown, accept under Bearer credentials" behind a config knob
  during the transition.
- **DDL statements end-to-end via PG-wire.** psql `CREATE TABLE`
  expects `CommandComplete: CREATE TABLE`. KesselDB's
  `OpResult::TypeCreated(tid)` doesn't carry the SQL statement
  type. V1 dispatches DDL through `apply_sql` and infers the
  CommandComplete tag from the SQL text's leading keyword
  (`CREATE TABLE` / `DROP TABLE` / `ALTER TABLE` / `CREATE INDEX`
  / …). This is a string-match on the SQL header; works for
  well-formed SQL, misclassifies obscure inputs. Documented as
  T9 follow-up; T7 makes it slightly nicer by reserving a "DDL
  result" path.
- **Cancel-key generation.** V1 generates a (pid, secret) pair
  via `kessel-crypto::sha256` but never references it again
  (CancelRequest is V2). Is generating the pair worth the cycles
  if we won't action it? Yes — clients (psql especially) expect
  the BackendKeyData message and will refuse to enter QueryLoop
  without it. Cheap, locked.
- **Empty database accepted as `dbname=kessel` vs error?** PG
  clients almost always send a `database` parameter (often the
  username if no DB specified). V1 accepts ANY dbname (logs +
  ignores). An operator who wants to enforce "only `kessel` is
  valid" gets a config knob in V2.

## 13. References

- SP156 scoping: `docs/superpowers/specs/2026-05-26-kesseldb-http2-ws-pgwire-scoping.md`
- SP-WS design (mirror): `docs/superpowers/specs/2026-05-26-kesseldb-spws-websocket-design.md`
- SP-WS progress (template): `docs/superpowers/specs/2026-05-26-kesseldb-subproject-spws-progress.md`
- SP141 HTTP gateway: `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md`
- PostgreSQL Documentation §55 — Frontend/Backend Protocol (v3.0)
  (the authoritative source for every message tag + payload layout
  cited in this spec)
- RFC 5802 — Salted Challenge Response Authentication Mechanism (SCRAM)
- RFC 7677 — SCRAM-SHA-256 + SCRAM-SHA-256-PLUS
- RFC 8018 §5.2 — PBKDF2 (the password-based key derivation function
  SCRAM uses; one new primitive in `kessel-crypto`)
- RFC 4648 — base64 (already in `kessel-crypto` from SP-WS)
- PG SQLSTATE Appendix A — the complete error code catalog from
  which V1 picks ~15 codes
- `crates/kessel-sql/` — the SQL parser PG-wire dispatches into
- `crates/kessel-proto/` — the OpResult vocabulary PG-wire translates
- `crates/kessel-client/src/lib.rs::format_result_json` — the
  sibling adapter shape `format_result_pg` mirrors
