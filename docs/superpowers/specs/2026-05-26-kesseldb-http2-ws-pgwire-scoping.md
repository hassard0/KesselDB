# KesselDB — Subproject 156: HTTP/2 vs WebSocket vs PostgreSQL wire compat — SCOPING

**Status:** scoping — closes the LAST open SP141 follow-up (#4 — "HTTP/2 / gRPC / WebSocket / SSE / PostgreSQL wire compat — V1 non-goal of SP141; own slice if a real consumer asks") as **EVALUATED** (recommendation produced), not implemented. A future executing session reads this cold and decides which direction to attack next (or none).

**Builds on:**
- SP141 (`docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md`) — the HTTP/1.1 gateway this doc evaluates *expanding from*.
- SP142 + SP144H + SP147 + SP148 (hardening, gap closures, keep-alive, pentest tightening) — SP141 follow-ups #1, #2, #3, #6, #7, #8, #9 already closed; #4 is the last open one. #5 closed in SP147.
- The shipped binary wire (`kessel-proto`) — bidirectional persistent TCP today; relevant for WebSocket comparison.
- The shipped TLS pattern (rustls behind opt-in `tls` cargo feature) — the only existing exception to the workspace's zero-dep stance, and the precedent if any of these directions needs a feature-gated dep.

**Process note.** Per `feedback_cani_autonomous_build` (mandate substitution) + `feedback_kesseldb_autonomous_build`, brainstorm gate substituted: the user explicitly said "write a scoping doc that evaluates the 3-4 directions and recommends which (if any) to pursue, including zero-dep cost analysis". This is a SCOPING ticket. No code ships. A recommendation ships.

---

## 1. Problem

### 1.1 What the SP141 HTTP/1.1 gateway covers today

After SP141 + SP142 + SP144H + SP147 + SP148:

- `POST /v1/sql` (text SQL body) — JSON OpResult response
- `POST /v1/op` (binary `Op::encode()` body) — JSON OpResult response
- `GET /v1/health` — JSON liveness
- `GET /v1/metrics` — Prometheus text v0.0.4
- HTTP/1.1 keep-alive (SP147) with per-connection request cap
- Opt-in TLS via the existing rustls feature gate
- Token-mode auth via `Authorization: Bearer …` (RFC 6750 §2.1 case-insensitive)
- Exactly-once `X-Kessel-Client-Id` / `X-Kessel-Req-Seq` session binding

That's a clean, RFC 9112-compliant HTTP/1.1 surface, zero-dep (the gateway crate's `cargo tree` is empty save for workspace crates + rustls-when-featured). Curl / requests / fetch / any vanilla HTTP/1.1 client works.

### 1.2 What it does NOT cover (the SP141 #4 surface)

Four directions named in SP141 §non-goals, none yet implemented:

1. **HTTP/2** — h2 multiplexing, server push, HPACK header compression. Some consumers (gRPC clients especially) only speak h2.
2. **WebSocket** (RFC 6455) — persistent bidirectional message stream. Required for browser-direct clients (browsers cannot open raw TCP sockets) and for streaming subscribe / push-notification patterns.
3. **Server-Sent Events (SSE)** — one-way text/event-stream push from server to client. Lighter than WebSocket; HTTP/1.1-compatible.
4. **PostgreSQL wire protocol** (the heavyweight one) — speak PG protocol natively so `psql`, libpq, psycopg2/3, JDBC/pgx, ODBC, and the entire BI/ORM ecosystem connects to KesselDB without an adapter.

And implicitly:

5. **HTTP/3** (QUIC) — explicitly excluded from this scoping doc (see §6).

### 1.3 Use cases driving each

| Direction | Real driver | Who asks for it |
|---|---|---|
| HTTP/2 | gRPC clients; throughput-bound concurrent fan-in | Anyone moving from grpcio / gRPC-Java / gRPC-Go |
| WebSocket | Browser apps (no raw TCP); live result streams; subscribe/notify | Web frontend developers; dashboarding tools |
| SSE | Lightweight server→client push (alerts, metrics tail) | Same as WebSocket, but one-way |
| PG wire | "Drop-in for Postgres" marketing claim; instant BI/ORM/IDE adoption | Every Postgres user on earth (the killer claim) |
| HTTP/3 | Mobile / lossy-network clients | Almost nobody for an OLTP DB today |

### 1.4 Why this is a scoping doc, not an implementation slice

Each of these is a multi-task arc on its own. Some (PG wire) are a multi-arc effort. Picking the wrong one wastes 20+ task slices. The job of SP156 is to apply cost-vs-value triage *before* committing a session to one. We want one clear "recommended next" and one clear "eventually" and one or two clear "no/defer".

---

## 2. Direction A: HTTP/2

### 2.1 What HTTP/2 actually buys

- **Multiplexing**: multiple logical requests over one TCP connection without head-of-line blocking at the HTTP layer (still blocked at TCP packet loss — that's HTTP/3's job).
- **HPACK header compression**: dynamic header table; repeated headers (Authorization, X-Kessel-Client-Id) cost a single integer reference after the first request.
- **Binary framing**: each request/response is a stream of typed frames (HEADERS, DATA, RST_STREAM, SETTINGS, WINDOW_UPDATE, PING, GOAWAY, …).
- **Server push** (deprecated in practice — Chrome removed it 2022). Skip.
- **Flow control**: per-stream and per-connection window updates. Bidirectional credit-based.
- **Required for gRPC over the wire** (gRPC-web works on h2 too, but it's an HTTP/1.1-compat shim with limits).

### 2.2 What KesselDB clients actually do today

The binary wire (`kessel-proto`) over persistent TCP already does request multiplexing in the sense that matters: many ops over one socket, in-order, with the exactly-once session binding for idempotency. There is no head-of-line blocking pain because KesselDB ops are serially applied per-client anyway (the engine's apply path is FIFO per session-id).

The HTTP/1.1 gateway with SP147 keep-alive lets curl / requests / fetch reuse the connection too. The thing keep-alive cannot do is *interleave* responses — request 2's response waits for request 1's response. That's the HTTP/2 win.

### 2.3 Pros

- Mature ecosystem; every major language has a client.
- Real win for high-concurrency many-requests-per-client workloads (less of a win for KesselDB's typical workload, which is fewer, larger requests).
- The cost of TLS handshake is paid once; subsequent requests are cheap.
- HPACK dramatically reduces repeated header overhead — relevant if KesselDB ever ships hot per-request auth tokens.

### 2.4 Cons

- **HPACK is genuinely hard to hand-roll.** RFC 7541 dynamic table semantics + Huffman string encoding/decoding + the static table (61 entries) + table eviction + reference-set arithmetic. There are no shortcuts. Real-world implementations (h2 crate, nghttp2) are thousands of lines.
- **HTTP/2 frame parser** is straightforward but voluminous: 10 frame types, settings negotiation, prior-knowledge vs upgrade-from-1.1 vs ALPN, stream state machine (idle → open → half-closed (local/remote) → closed with reset transitions).
- **Flow control** is a state machine bug magnet. Every DATA frame consumes a window; WINDOW_UPDATE frames refill it; running out of credit deadlocks. Both per-stream AND per-connection.
- **No browser uses HTTP/2 to talk to a server without TLS in practice** — every browser implementation requires ALPN-negotiated h2 over TLS (h2c — plaintext h2 — is server-to-server only). So HTTP/2 effectively requires TLS already, which the SP141 gateway feature-gates.
- **The only existing exception to zero-dep stance is rustls.** Adding `h2` or `hyper` adds ~200 transitive deps via `tokio` + `bytes` + `http` + `tracing` + … that's a different scale than rustls's footprint.

### 2.5 Zero-dep cost (hand-rolled)

Rough task decomposition:

| Task | Cost |
|---|---|
| T0: baseline + ALPN negotiation hook into rustls feature | 1 slice |
| T1: HTTP/2 frame parser (10 types + length prefixing + flags) | 2 slices |
| T2: SETTINGS negotiation + connection preface ("PRI * HTTP/2.0 …") | 1 slice |
| T3: HPACK static table (61 entries hard-coded) + integer encoding | 1 slice |
| T4: HPACK dynamic table + reference tracking + eviction | 2 slices |
| T5: HPACK Huffman encode/decode (256-entry table from RFC 7541 Appendix B) | 1 slice |
| T6: Stream state machine + RST_STREAM | 1 slice |
| T7: Per-stream and per-connection flow control (WINDOW_UPDATE) | 2 slices |
| T8: Routing into existing `EngineApply` trait | 1 slice |
| T9: Pentest matrix (frame fuzz, HPACK fuzz, flow control deadlock cases) | 2 slices |
| T10: e2e oracle (real h2 client — curl --http2, nghttp) | 1-2 slices |
| **Total** | **~15 slices** |

That's a real arc — comparable to the SP125-SP140 zstd / compression sequence. And HPACK Huffman alone is *the kind of code where wrong-by-one-bit silently corrupts headers and you find out via mysterious test failures three weeks later*.

### 2.6 Workaround for users today (the killer argument)

Any TLS-terminating reverse proxy (haproxy, nginx, envoy, traefik, caddy) translates HTTP/2 → HTTP/1.1 with zero code. The KesselDB gateway sits behind it; the proxy speaks h2 to the world; KesselDB speaks 1.1 to the proxy on localhost or a private interface. The user gets HTTP/2 throughput characteristics *for HTTP-over-the-WAN* and KesselDB stays minimal.

For gRPC specifically: yes, gRPC requires h2 end-to-end (the proxy can't translate gRPC to JSON). But KesselDB doesn't speak gRPC anyway — adding gRPC is a separate project (define `.proto`, generate stubs, define service surface, …). HTTP/2-without-gRPC is the same workload as HTTP/1.1-with-keep-alive for our shape.

### 2.7 Verdict

**Defer.** The HPACK cost is wildly out of proportion to the user value when haproxy/nginx solves it for free. Revisit only if (a) someone genuinely needs end-to-end gRPC against KesselDB, or (b) a workload appears where the keep-alive HOL-blocking matters and a reverse proxy isn't acceptable.

---

## 3. Direction B: WebSocket

### 3.1 What WebSocket actually is

RFC 6455 — a framing protocol layered on top of a single HTTP upgrade handshake. After the handshake (`Upgrade: websocket` + `Sec-WebSocket-Key` + `Sec-WebSocket-Accept` with the SHA-1 magic), the TCP connection is no longer HTTP. It's a bidirectional stream of WebSocket frames: text, binary, ping, pong, close.

Each frame:

- 2-byte minimum header: FIN bit, RSV1-3 (zero for vanilla), 4-bit opcode (0x1 text, 0x2 binary, 0x8 close, 0x9 ping, 0xA pong, 0x0 continuation), MASK bit, 7-bit length (with 16-bit or 64-bit extension for longer frames).
- 4-byte mask (client→server frames MUST be masked; server→client MUST NOT).
- Payload, optionally XOR'd byte-by-byte against the 4-byte mask.

Close handshake: either side sends a close frame (opcode 0x8) with a 2-byte status code + UTF-8 reason; the other side replies with a close frame; both close TCP.

### 3.2 Pros

- **Browser direct client.** This is THE pro. Browsers cannot open raw TCP sockets — `WebSocket` is the only persistent bidirectional channel available to JavaScript in a browser. If we want a KesselDB web UI / dashboard / interactive query console that doesn't relay through an app server, this is the only path.
- **Streaming results.** Today the HTTP gateway buffers the entire OpResult and returns it as one JSON blob. A WebSocket lets the server emit rows as they materialize (relevant for SP-A scatter scans with `LIMIT 1M` — start streaming at row 1, not row 999,999).
- **Subscribe / push notifications.** A future "watch this table" surface (changefeed-style) needs server→client push. Polling /v1/sql is wasteful and racy.
- **Cheaper than HTTP/2 server push** (which doesn't exist in practice anymore).
- **Simpler protocol than HTTP/2.** Frame parser is ~200 lines; mask XOR is one loop; close handshake is a 2-frame exchange. No flow control to deadlock-trap; no HPACK; no stream state machine.

### 3.3 Cons

- The binary wire (`kessel-proto`) over persistent TCP already does bidirectional. **What specifically does WebSocket add over the binary wire?**
  - Answer: browser reachability. The binary wire is raw TCP — browsers can't open it. WebSocket exists for that exact reason.
  - Secondary: an in-flight `LIMIT` cancellation surface for scatter scans (SP-A), where the router needs to push "stop" to a shard and pull rows back as they come. The binary wire could be extended to do this too, but it's a per-frame state thing — WebSocket's framing is purpose-built for it.
- WebSocket masking (client→server XOR) is mandatory and pointlessly expensive (it's defense against HTTP-proxy injection attacks from 2011). Every incoming byte goes through an XOR; for a 10 MB upload that's 10M XORs. Negligible compared to the SQL execution time, but worth knowing.
- Subprotocols (`Sec-WebSocket-Protocol`) — we'd want to pick one (e.g. `kessel-v1`) so future versioning has a hook. Cheap.
- Ping/pong heartbeats — we need to send periodic pings to detect dead connections. Cheap timer.
- Permessage-deflate (RFC 7692) — optional WebSocket compression extension. Hard pass; not worth the LZ77 sliding-window state. Advertise no extensions in the handshake response.

### 3.4 Zero-dep cost (hand-rolled)

| Task | Cost |
|---|---|
| T0: baseline + scaffold inside the existing gateway crate | 1 slice |
| T1: HTTP/1.1 upgrade handshake (101 + Sec-WebSocket-Accept SHA-1 magic) | 1 slice |
| T2: RFC 6455 frame parser (FIN/opcode/mask/length) + mask XOR | 1 slice |
| T3: Close handshake + ping/pong + fragmented messages (continuation frames) | 1 slice |
| T4: Subprotocol negotiation (`Sec-WebSocket-Protocol: kessel-v1`) + message router into `EngineApply` | 1 slice |
| T5: Pentest matrix (fuzz frames, oversized payloads, invalid UTF-8 in text frames, malformed close codes, server-MUST-NOT-mask check) + e2e oracle (real WS client — e.g. websocat or a tiny std-only client) | 1-2 slices |
| **Total** | **~5-6 slices** |

We already have SHA-1 in the workspace (used by VSR fingerprinting). We already have HTTP/1.1 parse from SP141 — the upgrade handshake is a tiny extension. The frame parser is RFC 6455 §5 cleanly transcribed.

### 3.5 What about SSE?

SSE (Server-Sent Events, `text/event-stream`) is HTTP/1.1-compat, server→client only, line-delimited. It's a 50-line implementation on top of the existing gateway. **It's worth shipping as a tiny add-on to the WebSocket slice** (or even before it), since it covers the "live metrics tail" / "live event stream" use case without WebSocket framing complexity.

The trade-off:
- SSE = server → client only (push). One-way.
- WebSocket = bidirectional.

For browser-direct interactive SQL, we need WebSocket. For "tail the metrics endpoint" or "subscribe to a changefeed", SSE suffices.

**Recommendation: ship SSE as a T-task within the WebSocket slice** (or as a tiny pre-cursor — 1 slice on its own). They share the "long-lived response" lifecycle plumbing.

### 3.6 Verdict

**Yes, next.** Smallest cost in the matrix, real user value (browser clients), fits the zero-dep stance (RFC 6455 is the right complexity to hand-roll), composes with future scatter-scan streaming.

---

## 4. Direction C: PostgreSQL wire protocol

### 4.1 What "speak PG wire" actually means

The Postgres frontend/backend protocol (current = v3.0, documented in the PG manual §55). Two phases:

**Startup**:
- Client connects (TCP or unix socket).
- Optional SSLRequest (8 bytes, magic number) — server replies 'S' (yes) or 'N' (no); if 'S', TLS handshake follows.
- StartupMessage: protocol version (196608 = 3.0) + key/value parameters (user, database, application_name, client_encoding, …).
- Authentication exchange: server sends `AuthenticationOk` / `AuthenticationCleartextPassword` / `AuthenticationMD5Password` / `AuthenticationSASL` (SCRAM-SHA-256) / `AuthenticationGSS` / `AuthenticationKerberosV5`. SCRAM is the modern default (PG 10+). MD5 is legacy but still widely used. Cleartext only over TLS.
- Server sends `ParameterStatus` for each GUC the client expects (server_version, client_encoding, DateStyle, IntervalStyle, TimeZone, integer_datetimes, standard_conforming_strings, …) — the libpq client *parses these* and uses them to decide things like binary date format. Lying about server_version (claiming "16.0") risks libpq invoking PG16-specific behaviour the gateway doesn't support; telling the truth ("KesselDB-via-PG3.0") risks clients refusing to connect.
- `BackendKeyData` (4 bytes pid + 4 bytes secret key, used for query cancellation).
- `ReadyForQuery` ('I' for idle).

**Query phase** has two modes:

**Simple query** (Q message): SQL text → multiple result rows → `CommandComplete` → `ReadyForQuery`. Easiest. libpq's `PQexec` uses this.

**Extended query** (the painful one): `Parse` (statement name + SQL + param types) → `Bind` (portal name + statement + param values) → `Describe` → `Execute` → `Sync`. Used by libpq's `PQprepare` / `PQexecPrepared`, psycopg2's parameterised queries, every ORM's prepared-statement path. Mandatory for serious clients.

**COPY**: bulk in/out — entirely separate message flow with `CopyInResponse` / `CopyData` / `CopyDone` / `CopyOutResponse`. Used by psql's `\copy` and every PG-aware bulk loader.

**Error / notice**: `ErrorResponse` and `NoticeResponse` use a tag-based field format ('S' severity, 'C' SQLSTATE code, 'M' message, 'D' detail, 'H' hint, 'P' position, 'p' internal position, 'q' internal query, 'W' where, 's' schema name, 't' table name, 'c' column name, 'd' data type name, 'n' constraint name, 'F' file, 'L' line, 'R' routine). SQLSTATE is a real RFC-style catalog — clients switch on it. Returning "22P02" (invalid text representation) lets pgx know it's a coercion error and not a connection error.

### 4.2 Why "drop-in Postgres compat" is the killer feature

If `psql 'postgresql://user:pass@kesseldb:5432/kessel'` works, KesselDB inherits:

- All BI tools: Tableau, Metabase, Looker, Grafana, Mode, Redash, Hex, Superset.
- All ORMs: SQLAlchemy, Django ORM, Rails AR, Prisma, Drizzle, GORM, Diesel, sqlx (PG mode).
- All admin GUIs: pgAdmin, DBeaver, DataGrip, TablePlus, Postico.
- All ETL: dbt-postgres, Fivetran-PG-source, Airbyte-PG, Singer-tap-postgres.
- The Python ecosystem (psycopg2/3, asyncpg).
- The Java ecosystem (JDBC PG driver, R2DBC).
- The Node ecosystem (pg, postgres.js).
- The Go ecosystem (pgx, lib/pq).
- The Rust ecosystem (sqlx, tokio-postgres, postgres).

No other single direction comes close. "We speak PG wire" is the marketing claim every Postgres-compatible DB (CockroachDB, YugabyteDB, AlloyDB, Aurora-PG, Materialize, RisingWave, Greenplum, …) leads with. It is THE adoption lever.

### 4.3 Pros

- Largest existing client ecosystem on earth. See above.
- Zero new client SDKs for users — they keep their existing libpq-based code.
- ODBC / JDBC connectivity for free via the existing PG ODBC/JDBC drivers.
- Marketing: "drop-in compatible with PostgreSQL clients" is a sentence that converts.

### 4.4 Cons

- **The SQL surface has to grow to match expectations.** Clients expect:
  - PG type oids (16=bool, 20=int8, 21=int2, 23=int4, 25=text, 700=float4, 701=float8, 1043=varchar, 1082=date, 1114=timestamp, 1184=timestamptz, 1700=numeric, …) in `RowDescription`. KesselDB has its own type system today (`Value`); a mapping layer is needed, AND types KesselDB doesn't natively have (`numeric` arbitrary-precision, `interval`, `tsvector`, `jsonb` proper, arrays-of-T, ranges) have to either be supported or rejected with a specific SQLSTATE.
  - PG SQL dialect: `LIMIT … OFFSET …` (we have this), `RETURNING` (we don't), `ON CONFLICT DO UPDATE` (we don't), `WITH … AS (…) SELECT …` CTEs (partial), window functions (none), `LATERAL` joins (none), `DISTINCT ON` (none).
  - PG functions: clients send `SELECT current_setting('TimeZone')`, `SELECT version()`, `SELECT current_schema()`, `SELECT pg_catalog.pg_table_def(…)`. pgAdmin and DBeaver alone issue ~50 introspection queries on connect. The compatibility floor is "answer enough of these to not crash the client", which is its own ongoing arc.
  - `pg_catalog.*` system views: even returning empty `pg_class` / `pg_attribute` / `pg_type` is a multi-task effort. Some clients hard-require non-empty type catalogs.
  - Error codes: returning a generic error tag isn't enough; clients switch on SQLSTATE. "23505" (unique violation), "23503" (foreign key violation), "42P01" (undefined table), "42703" (undefined column), "22P02" (invalid text representation) — a real catalog mapping.

- **Auth alone is 5-8 tasks.**
  - MD5 (PG legacy, still widely used): `md5(md5(password + username) + salt)` — easy.
  - SCRAM-SHA-256 (PG default since v10): RFC 5802 + RFC 7677 — multi-round-trip, requires HMAC-SHA-256 + PBKDF2 + base64. We have SHA-256 already, but PBKDF2 (4096 iterations of HMAC-SHA-256) is new code. SCRAM channel binding (`SCRAM-SHA-256-PLUS`) over TLS requires `tls-server-end-point` channel-binding data from the TLS handshake — rustls exposes it but it's plumbing.
  - GSSAPI / Kerberos: skip (enterprise; we punt to MD5/SCRAM).
  - LDAP: skip (delegated to a sidecar bind tool in production anyway).
  - Cert auth: trivial on top of rustls TLS feature — but a separate path.

- **Extended query protocol** is the painful one. Parse/Bind/Describe/Execute/Sync is where every "I implemented PG wire in a weekend" project falls over. State per portal, per statement, per session. `Sync` resets error state. `Parse` empty-statement-name means unnamed prepared statement (overwrite-on-next-Parse). `Bind` parameter format codes (0=text, 1=binary) per-parameter. `Describe` returns a `RowDescription` AND `ParameterDescription`. Each prepared statement holds onto its plan in a way KesselDB's compile-and-execute path doesn't currently model.

- **COPY**: bulk import/export is a separate protocol flow. `\copy` in psql alone is a real testing target. Punt to a follow-up, but call it out — users expect it.

- **Wire encoding**: every value goes over the wire in either text format (PG's text representation — e.g. `1.5` for float, `2026-05-26 12:00:00+00` for timestamptz, `t`/`f` for bool) or binary format (PG's binary representation — big-endian, type-specific). Clients choose per-column. Getting `numeric` binary wrong (variable-length decimal digits in base 10000) is a classic implementation bug.

- **Query cancellation**: `CancelRequest` is sent on a *new* TCP connection with the `BackendKeyData` from the original session. We'd need a process-wide cancel-key table.

- **GUCs (parameters)**: `SET timezone = 'UTC'` from a client expects the server to remember it for the session and apply it to subsequent timestamp formatting. That's session state we don't currently model.

### 4.5 Zero-dep cost (hand-rolled)

| Task | Cost |
|---|---|
| T0: scaffold `kessel-pg-gateway` crate + listener + startup msg parser | 1 slice |
| T1: cleartext auth + ParameterStatus + BackendKeyData + ReadyForQuery | 1 slice |
| T2: MD5 auth (md5(md5(pw+user) + salt) — needs MD5 in the workspace; we have SHA-256, MD5 is RFC 1321, ~150 lines hand-rolled) | 1 slice |
| T3: SCRAM-SHA-256 auth (RFC 5802 + RFC 7677) — PBKDF2 + HMAC-SHA-256 + base64 | 2 slices |
| T4: Simple query protocol (Q → RowDescription → DataRow* → CommandComplete → ReadyForQuery) | 2 slices |
| T5: PG type oid mapping for KesselDB's `Value` types (text format only at first) | 1 slice |
| T6: Error response with SQLSTATE catalog mapping | 1 slice |
| T7: Extended query — Parse / Bind / Describe / Execute / Sync (text format params + results) | 3 slices |
| T8: Binary format wire encoding for int / float / bool / text / timestamp | 2 slices |
| T9: Minimal `pg_catalog` stubs (pg_type, pg_class, pg_attribute, pg_namespace) — empty or single-row, just enough for psql `\dt` not to crash | 2 slices |
| T10: `current_setting()` / `version()` / `current_schema()` / `current_database()` builtin functions | 1 slice |
| T11: `RETURNING` clause on INSERT/UPDATE/DELETE | 1 slice |
| T12: Query cancellation (CancelRequest on new TCP + cancel-key table) | 1 slice |
| T13: GUC plumbing (`SET timezone = …` session state) | 1 slice |
| T14: COPY (in + out) — bulk protocol flow | 2-3 slices |
| T15: Pentest matrix (truncated messages, oversize payloads, malformed types, auth replay) | 2 slices |
| T16: e2e oracle (real libpq via psql/psycopg + pgx + JDBC connectivity smoke tests) | 2 slices |
| **Total** | **~25-30 slices** |

That's the size of an entire multi-arc effort — comparable in scale to "build a SQL engine" or "build the binary wire". We are not adding a feature; we are adding a second protocol surface that has to mirror the SQL feature set.

### 4.6 The realistic scope of "PG-compatible enough"

Two phases:

**Phase 1 — "psql connects, basic queries work."** ~T0..T6. ~10 slices. Enables `psql`, simple `SELECT` and `INSERT`, error reporting. NOT enough for any real ORM or BI tool, but enough to demo "drop-in PG client".

**Phase 2 — "DBeaver / Metabase / Tableau works."** T7..T16. ~15 more slices. Extended query protocol, binary format, minimal pg_catalog, RETURNING, GUCs, cancellation, COPY. This is where real adoption happens.

A two-phase rollout lets us ship Phase 1 as a credibility demonstrator (the marketing screenshot) and Phase 2 as the production-grade arc.

### 4.7 Workaround for users today

There isn't a clean one. A PG-protocol shim that translates PG wire → HTTP-JSON is a real project (multiple companies have shipped it as a product — pgcat-style proxies). It's strictly harder than us implementing PG wire natively, because the shim has to maintain session state on behalf of a stateless HTTP backend. **Negative workaround**: this is the direction where "you can do it with a reverse proxy" is *false*.

### 4.8 Verdict

**Eventually, in a Phase 1 + Phase 2 split.** Highest user value of all four directions. Largest cost. Right answer is "not next, but next-after-next". Picking it up after WebSocket gives us a chunk of streaming/long-lived-connection plumbing reusable for PG's session model (especially LISTEN/NOTIFY if we ever get there).

---

## 5. Direction D: HTTP/3

### 5.1 Why it's a non-goal

HTTP/3 = HTTP/2 semantics over QUIC instead of TCP. QUIC is:

- A connection-oriented transport over UDP (port 443 typically).
- Includes its own crypto handshake (TLS 1.3 baked in, no separate TLS).
- Has its own multi-stream framing, flow control, retransmission, congestion control, 0-RTT resumption.
- Defined across half a dozen RFCs (9000, 9001, 9002, 9114, …).

The smallest real QUIC implementation (quiche, quinn, neqo) is tens of thousands of lines of code. There is no hand-rolled-in-a-weekend version of QUIC; the protocol is approximately as complex as TCP + TLS + HTTP/2 combined.

### 5.2 What HTTP/3 buys

- Resilience to packet loss on mobile / lossy networks (no TCP head-of-line blocking).
- 0-RTT connection resumption.
- Migration across IP addresses (mobile handoff).

### 5.3 What KesselDB clients care about

Almost none of this. KesselDB is an OLTP DB; its clients are servers on stable networks. Mobile clients connect through an app server, not directly to the DB.

### 5.4 Verdict

**No, ever.** Reverse-proxy this if anyone wants it. Caddy/haproxy/nginx all speak HTTP/3 today.

---

## 6. Recommendation matrix

| Direction | User value | Implementation cost (zero-dep) | Workaround exists | Zero-dep fit | Recommend? |
|---|---|---|---|---|---|
| HTTP/2 | medium (mostly redundant with keep-alive for our shape) | high (~15 tasks; HPACK is painful) | yes — haproxy / nginx / envoy reverse-proxy translates h2→1.1 transparently | poor — HPACK Huffman + dynamic table is the kind of code that bites silently | **defer** (revisit if a real gRPC consumer asks) |
| WebSocket | medium-high (browser direct clients; future streaming/subscribe) | low-medium (~5-6 tasks; RFC 6455 is right-sized to hand-roll) | partial — server-side relays exist but are heavy | good — frame parser is ~200 lines, no dynamic state magnetic for bugs | **YES, next** |
| SSE | low-medium (lightweight push) | very low (~1 task on top of HTTP/1.1) | partial — long-polling exists | excellent — text/event-stream is a 50-line addition | **yes, bundle with WebSocket slice** (or precede it) |
| PG wire | very high (instant ecosystem adoption; the killer claim) | very high (~25-30 tasks across two phases) | no — translating proxies are harder than native | OK conceptually (it's bytes), but the *scope* (SQL surface, type catalog, auth, extended query) is the cost | **eventually** (Phase 1 = 10 tasks demo; Phase 2 = 15 tasks production) |
| HTTP/3 | low (mobile-network specific) | extreme (QUIC = TCP+TLS+HTTP/2 combined in complexity) | yes — reverse proxy speaks h3 today | no | **no, indefinitely** |

---

## 7. Recommended path forward

### 7.1 Next slice: WebSocket + SSE bundle

**SP-WS** (working name): RFC 6455 WebSocket + RFC-equivalent SSE on the existing HTTP/1.1 gateway.

- ~5-6 task slices (see §3.4) + ~1 slice for SSE.
- Zero new dependencies (SHA-1 already in workspace; HTTP/1.1 upgrade leverages SP141 parser).
- Real user value: browser-direct clients become possible; opens the door to interactive consoles and live result streaming.
- Composes forward with SP-A scatter scan (`LIMIT` cancellation can use the WS close-on-cancel path for the streaming variant).
- Composes forward with PG wire (the long-lived connection lifecycle, frame state machine discipline, and pentest harness shape carry over).

### 7.2 After WebSocket: PG wire compat — Phase 1

**SP-PG1** (working name): startup + auth (MD5 + cleartext) + simple query + minimal `pg_catalog` stubs.

- ~10 task slices.
- Goal: `psql 'postgresql://kessel/db' -c 'SELECT 1'` works. `INSERT … VALUES … ` works. Errors return correct SQLSTATE. `\dt` doesn't crash psql even if it returns empty.
- Demo target: a Hacker News screenshot of `psql` connected to KesselDB.

### 7.3 After Phase 1: PG wire compat — Phase 2

**SP-PG2** (working name): extended query + binary format + GUCs + RETURNING + COPY + cancellation + SCRAM-SHA-256.

- ~15 task slices.
- Goal: Metabase / DBeaver / Tableau connect and work for non-exotic queries. psycopg2 prepared statements work. JDBC pg-driver works.
- Production target.

### 7.4 HTTP/2

Stays a documented reverse-proxy concern. Note in `docs/USAGE.md` and `docs/ARCHITECTURE.md`:

> KesselDB speaks HTTP/1.1 on the HTTP gateway. For HTTP/2 client compatibility, terminate HTTP/2 at a reverse proxy (haproxy/nginx/envoy/caddy) and forward HTTP/1.1 to the gateway. The proxy MUST send `Connection: close` or honor SP147 keep-alive; both work.

If a real gRPC consumer ever asks, revisit and either (a) bite the hand-rolled cost, or (b) gate `h2` behind a cargo feature the way `rustls` is gated today — explicitly accepting the ~200-dep weight as the price of gRPC support, because there is no zero-dep path to HPACK that's worth the engineering time.

### 7.5 HTTP/3

Documented non-goal. Add to `docs/superpowers/specs/NON_GOALS.md` (or equivalent) so it stops appearing in future scoping conversations.

---

## 8. Open questions per direction

Items each direction would need to resolve before its first task slice. These are NOT decisions to make in this scoping doc — they're flagged so the future executing session knows where the design judgment calls are.

### 8.1 WebSocket open questions

- **Subprotocol name.** `kessel-v1`? `kessel-sql-v1`? `kessel-binary-v1`? Recommend: `kessel-v1` (one protocol negotiated via the subprotocol; payloads carry their own type tag).
- **Message format.** Text frames carrying JSON `{op: …, args: …}`, OR binary frames carrying `kessel-proto` Op-encoded bytes, OR support both via subprotocol negotiation? Recommend: both, via subprotocols `kessel-v1-json` and `kessel-v1-binary`.
- **Permessage-deflate.** Advertise no extensions (hard pass). Confirm.
- **Auth.** First message MUST be an auth frame (`{type: "auth", token: "…"}`)? Or use the upgrade-request `Authorization` header? Recommend: upgrade-request header, falling back to first-message-must-be-auth if cookies-only browser clients require it.
- **Max message size.** Reuse `http_max_body`? Add a separate `ws_max_message`?
- **Idle timeout / keep-alive ping interval.** 30s ping, 90s no-pong-then-close?
- **Backpressure.** If the WS write side fills up, do we drop messages or close with code 1009 (message too big) / 1011 (server error)? Recommend: bounded send queue, close with 1011 on overflow.
- **Streaming SQL results vs single-response.** First slice = request/response (1 op → 1 response message). Streaming-rows (1 op → N row messages → 1 done message) is a follow-up.

### 8.2 SSE open questions

- **Endpoint.** `GET /v1/events`? `GET /v1/sse`? Recommend: `GET /v1/events` (RFC-flavored — sse is the *transport* not the *content*).
- **Auth.** Same as REST: `Authorization: Bearer`.
- **Reconnect / `Last-Event-Id`.** Standard SSE feature — client sends `Last-Event-Id` on reconnect; server resumes from there. Do we support? Recommend: stub it (read the header, ignore the value, start fresh) — real resume needs a per-subscription cursor we don't have yet.
- **Event types.** What events do we actually emit in V1? Recommend: metrics-tick (every 5s, emit `/v1/metrics` snapshot as a `metrics` event) as a smoke-test driver; richer events are follow-ups.

### 8.3 PG wire Phase 1 open questions

- **Listener port.** Standard PG = 5432. We'd want a config knob; default to 5432 when the PG gateway feature is enabled?
- **What `server_version` do we report?** "16.0 (KesselDB-via-PG3.0)"? "1.0 (KesselDB)"? Some clients gate features on version-string parsing. Recommend: report a real PG-shaped version string we control (`14.0 (KesselDB ${KESSEL_VERSION})`) and add an integration test that asserts `psql` doesn't choke.
- **Which `pg_catalog` stubs are the minimum?** psql `\dt` issues a query against `pg_class` JOIN `pg_namespace`. pgAdmin issues *many* more. Recommend: ship enough for psql `\dt` / `\d <table>` in Phase 1; pgAdmin / DBeaver in Phase 2.
- **Type mapping.** KesselDB's `Value::Bool` → PG `bool` (oid 16) trivially. `Value::Int` → `int8` (oid 20) or `int4` (oid 23)? KesselDB ints are 64-bit; PG `int4` is 32-bit. Recommend: always map to `int8` to avoid truncation; clients that expect `int4` get a cast notice in their schema introspection (acceptable).
- **`numeric`.** KesselDB doesn't have arbitrary-precision decimal. Either reject `numeric` columns with SQLSTATE 0A000 (feature_not_supported) or map to `float8` with a documented precision-loss caveat. Recommend: reject in Phase 1, revisit in Phase 2 (probably ship a real `numeric` type at that point).
- **Auth method default.** MD5 (easy, legacy) or SCRAM-SHA-256 (modern, harder)? Recommend: support both, advertise SCRAM first per PG's own pg_hba.conf default since v10. MD5 fallback for old clients.
- **Database name.** PG clients connect with `dbname=…`. KesselDB has one logical database. Recommend: accept any dbname, log a warning if non-default. Or strict-match a configured name.

### 8.4 PG wire Phase 2 open questions

- **Extended query state model.** Where do prepared statements live? Per-session map keyed by statement-name? How is `Parse` with empty statement-name (unnamed) handled? Implementation pattern needed.
- **Portals.** PG has separate "prepared statement" vs "portal" (a bound statement with parameter values, ready to execute). Do we need both or can we collapse them for V1?
- **Binary format.** Per-column, per-direction, negotiated in `Bind`. Which types do we ship binary for first? Recommend: int4 / int8 / float4 / float8 / bool / text / timestamp / timestamptz. `numeric` last (variable-length decimal).
- **`RETURNING`.** Needs schema-extension on KesselDB's existing INSERT/UPDATE/DELETE path. Where in the engine does this hook in?
- **`COPY FROM STDIN` / `COPY TO STDOUT`.** Bulk-protocol message flow. The engine has a bulk-insert path already (parquet writer); plumbing CopyData frames into it is a real arc.
- **Cancellation.** The 32-bit pid + 32-bit secret-key model is process-wide. Need a cancel-key table on the gateway shared across sessions, and a way to interrupt an in-progress engine apply. The engine apply path is currently uninterruptible — that's a real engine change.
- **GUCs.** `SET timezone = 'UTC'` affects how timestamps render in text format. Session-local state on the gateway. `SHOW timezone` returns it. Recommend: a small per-session GUC dict on the gateway; only apply timezone-affecting GUCs to text-format rendering (not to engine semantics, which stay UTC).
- **NOTIFY / LISTEN.** Postgres has pub/sub built into the wire protocol. We don't have changefeeds yet. Hard pass on this in Phase 2; revisit when we have a changefeed surface.

### 8.5 HTTP/2 (if we ever revisit)

- **Cargo-feature gate?** `h2-gateway` feature pulling in the `h2` crate? Or hand-rolled?
- **ALPN coordination with the existing `tls` rustls feature.** Probably just add `h2` to the ALPN protos list.
- **gRPC scope.** If HTTP/2 is purely for gRPC, then we also need a `.proto` file, service definitions, and a code-generator pipeline. That's a whole separate arc.

---

## 9. Cross-links

- SP141 row: `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md` (this scoping doc closes follow-up #4).
- SP142 / SP144H / SP147 / SP148: the closed SP141 follow-ups #1/2/3/5/6/7/8/9.
- STATUS row: `docs/STATUS.md` (SP156 row, after SP155).
- USAGE: `docs/USAGE.md` §HTTP gateway (the HTTP/2-via-reverse-proxy note will land here when WebSocket ships).
- ARCHITECTURE: `docs/ARCHITECTURE.md` §Listeners (will gain a WebSocket row after SP-WS).
- Memory: `memory/project_kesseldb.md` (SP156 block — scoping outcome, recommended next = WebSocket).

---

## 10. Status check

This document is a SCOPING decision, not an implementation. The deliverable is the recommendation matrix in §6, the recommended path in §7, and the open-questions catalog in §8.

**SP141 follow-up #4 is now CLOSED-AS-SCOPED.** A future session that wants to attack any of these directions reads §6 + §7 + the relevant §8 subsection and proceeds.

**Recommended next slice when the user wants to attack #4: SP-WS (WebSocket + SSE bundle, ~6-7 tasks, zero-dep, browser direct clients).** PG wire is the longer-term high-value target but should NOT be picked up first — WebSocket de-risks the long-lived-connection plumbing that PG wire will reuse.
