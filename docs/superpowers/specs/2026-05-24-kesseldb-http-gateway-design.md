# KesselDB — Subproject 141: HTTP/1.1 wire gateway

**Status:** design — approved, spec self-reviewed; implementation plan to follow.

**Builds on:** the shipped binary wire protocol (`kessel-proto`, `kesseldb-server`), the
existing token-mode auth (`ServerConfig.token` + `[0xFC]++token` constant-time compare),
the opt-in `tls` feature (rustls server config), the stable OpResult→JSON contract
(`kessel-client::format_result_json`), and the zero-dep HTTP/1.1 client primitives in
`kessel-fetch::http`.

**Process note (autonomous mandate substitution).** Per the standing KesselDB
autonomous-build mandate (`feedback_kesseldb_autonomous_build`: "BOLD documented
decisions, no per-slice approval, keep the two-stage review gate") the user-review-of-
spec gate from the standard brainstorming flow is substituted by a single up-front
approval pass on the design proposal (recorded in conversation, design approved as-is)
and this written record. The two-stage spec-then-quality subagent review gate during
implementation is retained verbatim. Any later objection from the user revises the spec
and re-enters the standard flow.

---

## 1. Problem

KesselDB exposes its wire protocol today as a length-prefixed binary frame:
`[0xFE]++SQL` for a plain SQL request, `[0xFC]++token` for the optional auth handshake,
or a bare `Op::encode()` for a typed op, with the response always `OpResult::encode()`.
The binary protocol is the right default for the OLTP hot path — a pipelined batch
delivers ~52 K ops/s on the reference Linux server (per `docs/STATUS.md` SP69), and the
frame is ~10 B of header overhead per op. It is intentionally not HTTP.

That choice is wrong for **adoption convenience, ops integration, and long-tail use
cases**: a one-off curl, a Kubernetes liveness probe, a Lambda function with only
HTTP egress, a browser developer-tools call, a corporate firewall that only passes
HTTP/HTTPS, a Prometheus scrape, a load balancer that needs `/health`. Today every one
of those requires either a TLS-terminating reverse proxy with a custom plugin, a
sidecar that translates HTTP to the binary frame, or a fresh language port of the
binary client.

This subproject adds an **opt-in HTTP/1.1 gateway** that translates HTTP requests into
the same `Op::decode → engine apply → OpResult` pipeline the binary listener already
uses. The binary listener is byte-untouched.

---

## 2. Goals and non-goals

**Goals (V1).**

- A second listener, opt-in via `--features http-gateway` on `kesseldb-server`, default
  port `:6789` plaintext (and `:6790` HTTPS with `tls`), that serves four routes:
  - `POST /v1/sql` — `text/plain` SQL body, JSON `OpResult` response.
  - `POST /v1/op` — `application/x-kessel-op` binary `Op::encode()` body, JSON
    `OpResult` response (full Op coverage without a per-variant JSON deserializer).
  - `GET /v1/health` — liveness JSON `{primary, view, op_number, role}`.
  - `GET /v1/metrics` — Prometheus text v0.0.4, cardinality-bounded.
- `Authorization: Bearer <token>` mapped to the existing `ServerConfig.token`
  constant-time compare; `OpResult::Unauthorized` → HTTP `401`.
- Optional exactly-once via two headers, `X-Kessel-Client-Id` + `X-Kessel-Req-Seq`,
  bound to a `ClusterClient`-style identity per HTTP request — semantics identical to
  the binary session frame, retries deduplicate identically.
- JSON response body always produced by `kessel-client::format_result_json` — one
  stable contract, no second serializer.
- Zero external (non-workspace) dependencies for the gateway crate
  (`crates/kessel-http-gateway/Cargo.toml [dependencies]` lists ONLY workspace
  members: `kessel-proto`, `kessel-client`. **No dep on `kesseldb-server`** —
  the engine-apply interface is a trait `EngineApply` defined in the gateway
  crate; `kesseldb-server::EngineHandle` `impl`s it on the server side. This
  inverts the dependency so the optional feature link goes
  `kesseldb-server → kessel-http-gateway` only — no cycle. No `hyper` /
  `httparse` / `h2` / `tokio` / `mio` / `socket2` / any third-party HTTP-related
  crate is added.).
- Default `cargo build` of `kesseldb-server` does not compile the gateway crate;
  `cargo tree -p kesseldb-server` (no features) links no HTTP server framework.
- Every existing oracle stays green untouched: binary serve, pipelined batch
  (`SP69`), token-mode auth, TLS binary, cluster/MVCC/replication-determinism
  (`large_seed_corpus_is_deterministic_and_converges`), `seed-7` liveness, all 7
  Parquet pyarrow e2e oracles.

**Non-goals (named, deferred).**

- HTTP/2 / gRPC. They require `h2` and a protobuf framework — substantial deps that
  break the zero-dep stance. Defer to a later slice if a real consumer asks.
- WebSocket / SSE / long-poll / chunked streaming responses. Single-request →
  single-response only in V1. Pipelined batches stay on the binary protocol.
- PostgreSQL wire compatibility. Separate slice if requested; the work is orthogonal.
- JSON-body `POST /v1/op` with a per-variant deserializer (30+ Op variants). The
  binary-body variant gives full coverage; `/v1/sql` gives JSON-friendly access.
- HTTP between replicas / VSR. Inter-replica traffic stays binary by design
  (determinism, perf, the reference SP69 pipeline number).

---

## 3. Architecture

A new workspace member `kessel-http-gateway`, opt-in via `kesseldb-server`'s
`http-gateway` cargo feature. When the feature is on, `serve_cfg` in
`kesseldb-server::lib` spawns a sibling thread that runs the HTTP listener on a
dedicated TCP port (default `:6789`, configurable via `ServerConfig.http_addr:
Option<SocketAddr>`). The HTTP listener uses **one thread per connection**, matching
the binary loop's concurrency model. The same `max_conns` and `max_inflight` knobs
already in `ServerConfig` apply across both listeners — total in-flight to the engine
is bounded honestly across binary + HTTP.

With `--features http-gateway,tls`, an HTTPS listener on `:6790` reuses the same
`(cert_pem, key_pem)` already in `ServerConfig.tls` — no separate cert configuration.

The gateway translates each HTTP request into the existing single-Op apply path:

```
HTTP request
   │
   ▼
parse request line + headers + body (kessel-http-gateway::parse)
   │
   ▼
auth check (Bearer ↔ ServerConfig.token, ct_eq)
   │
   ▼
route dispatch (kessel-http-gateway::routes)
   │     ├─ /v1/sql  → wrap body as kessel-proto SQL frame, call engine apply
   │     ├─ /v1/op   → Op::decode(body), call engine apply
   │     ├─ /v1/health → snapshot cluster state (no engine apply)
   │     └─ /v1/metrics → snapshot counters/gauges (no engine apply)
   │
   ▼
OpResult (or health/metrics struct)
   │
   ▼
kessel_client::format_result_json (or text-format metrics writer)
   │
   ▼
HTTP response (status, headers, body, Connection: close)
```

The engine apply path is **byte-identical** to the binary path. The gateway is a
translation layer; it does not implement Op semantics, MVCC, replication, or any
deterministic state-machine logic.

### 3.1 Crate layout

| File | Responsibility |
|---|---|
| `crates/kessel-http-gateway/Cargo.toml` | `[dependencies]` lists ONLY workspace members (`kessel-proto`, `kessel-client`); zero external crates; **no dep on `kesseldb-server`** (the apply interface is a local trait — see §3.2). |
| `crates/kessel-http-gateway/src/lib.rs` | Re-exports, public `serve(listener, engine, cfg)` entry point, top-level docs. |
| `crates/kessel-http-gateway/src/parse.rs` | Request line, headers, body. Caps + typed errors. |
| `crates/kessel-http-gateway/src/response.rs` | Response writer (status line, headers, JSON body or text body, `Connection: close`). |
| `crates/kessel-http-gateway/src/routes.rs` | Four route handlers; auth check; error mapping. |
| `crates/kessel-http-gateway/src/server.rs` | `TcpListener::accept` loop, per-conn thread, in-flight semaphore (shared with binary via `Arc<Semaphore>` injected from server). |
| `crates/kessel-http-gateway/tests/parse_kats.rs` | Hand-built request bytes asserting each parser case (Content-Length, chunked, missing headers, bad request line, header overrun, body overrun, Bearer extraction, exactly-once header extraction). |
| `crates/kessel-http-gateway/tests/e2e_curl.rs` | End-to-end: spawn server with `http-gateway` feature, send raw HTTP/1.1 requests via `TcpStream`, assert response bytes. Covers all 4 routes, auth/no-auth, exactly-once dedup, large body rejection. |
| `crates/kessel-http-gateway/tests/pentest.rs` | Adversarial: malformed request line, Content-Length lies (under/over), oversized headers (>64 KiB), oversized body (>8 MiB), bad chunked, missing `Host`, invalid UTF-8 in SQL body, undecodable Op bytes, missing/bad Bearer, Bearer-on-open-server, non-hex client-id, non-decimal req-seq, conflicting `Transfer-Encoding` + `Content-Length`. Every case asserts a typed HTTP response with no panic / no OOM / no protocol-state corruption. |

`crates/kesseldb-server/Cargo.toml` gains:

```toml
[features]
default = []
tls = [...]                                       # unchanged
http-gateway = ["dep:kessel-http-gateway"]        # NEW

[dependencies]
kessel-http-gateway = { path = "../kessel-http-gateway", optional = true }
```

`crates/kesseldb-server/src/lib.rs` gains a conditional thread spawn inside `serve_cfg`:

```rust
#[cfg(feature = "http-gateway")]
if let Some(http_addr) = cfg.http_addr {
    let listener = TcpListener::bind(http_addr)?;
    let engine_clone = engine.clone();
    let cfg_clone = cfg.clone();
    std::thread::spawn(move || {
        kessel_http_gateway::serve(listener, engine_clone, cfg_clone)
    });
}
```

`ServerConfig` gains two additive fields with `None`/default values
(so existing call sites compile unchanged):

```rust
pub http_addr: Option<SocketAddr>,        // None = no HTTP gateway
pub http_tls_addr: Option<SocketAddr>,    // None = no HTTPS gateway
```

### 3.2 Boundary discipline

- The gateway crate **does not** depend on `kessel-sm`, `kessel-storage`, or any
  deterministic state-machine code. It depends on `kessel-proto` (for `Op`/`OpResult`)
  and `kessel-client` (for `format_result_json`). It **does not** depend on
  `kesseldb-server` either — that would create a cycle with the optional
  `http-gateway` feature on `kesseldb-server`. Instead the gateway defines a small
  apply trait in `src/engine.rs`:

  ```rust
  pub trait EngineApply: Send + Sync + 'static {
      fn apply_op(&self, op: kessel_proto::Op) -> kessel_proto::OpResult;
      fn apply_op_with_session(
          &self,
          client: kessel_proto::ClientId,
          req: u64,
          op: kessel_proto::Op,
      ) -> kessel_proto::OpResult;
      fn apply_sql(&self, sql: &str) -> kessel_proto::OpResult;
      fn snapshot_health(&self) -> HealthSnapshot;
      fn snapshot_metrics(&self) -> MetricsSnapshot;
  }
  ```

  `kesseldb-server::EngineHandle` `impl EngineApply` on the server side. The
  `serve(listener, engine: Arc<dyn EngineApply>, cfg)` entry-point takes a trait
  object so the gateway has no concrete dependency on the engine implementation.
- The four routes are isolated handler functions; each takes parsed request + the
  `&dyn EngineApply` and returns a `Response` value. No route helper holds
  connection state or shared mutable globals.

---

## 4. Wire details

### 4.1 Request

**Request line.** `<METHOD> <PATH> HTTP/1.1\r\n`. METHOD ∈ `{GET, POST}`; any other →
HTTP `405`. PATH ∈ `{/v1/sql, /v1/op, /v1/health, /v1/metrics}`; any other → HTTP `404`.

**Headers.** Up to **64 KiB** total head before `\r\n\r\n` (mirrors the slack in
`kessel-fetch::http::MAX_HEADER_SLACK`). Header names case-insensitive (ASCII only).
Required headers:
- `Host:` — value validated to non-empty ASCII, no IPv6 literal (`[`-prefix rejected
  exactly as `kessel-fetch::http::parse_target` does, for symmetry); absent → `400`.

Optional headers honored:
- `Authorization: Bearer <token>` — token extracted, `ct_eq`-compared to
  `ServerConfig.token`.
- `Content-Type:` — for `/v1/sql` MUST be `text/plain` (with or without
  `; charset=utf-8`), else `415`. For `/v1/op` MUST be `application/x-kessel-op` or
  `application/octet-stream`, else `415`.
- `Content-Length:` — required on every `POST`; absent → `411`. Validated as an ASCII
  decimal `u64` ≤ `max_request_body` (default `8 * 1024 * 1024`, configurable via
  `ServerConfig.http_max_body`). Lying length (declared larger than received before
  socket close) → `400`. Declared > cap → `413`.
- `Transfer-Encoding: chunked` — supported, mutually exclusive with `Content-Length`
  (sending both → `400`); decoded by reusing the `dechunk` shape from
  `kessel-fetch::http`. Final dechunked size capped at the same `max_request_body`.
- `Expect: 100-continue` — V1 unsupported; if present and `Content-Length` > 0 we
  reply `417`. (Documented; safe to add later.)
- `Connection:` — we send `Connection: close` on every response; the request value is
  ignored (we always close).
- `X-Kessel-Client-Id: <32 hex chars>` and `X-Kessel-Req-Seq: <decimal u64>` —
  both-or-neither. Either alone → `400`. Together: bind to a temporary
  `ClusterClient`-style identity so the engine's existing per-client dedup map
  treats `(client_id, req_seq)` exactly as it would a binary session frame.

**Body.**
- `POST /v1/sql`: body is the SQL text (validated UTF-8; non-UTF-8 → `400`). Passed
  verbatim to the engine's SQL apply path (the one already used by the
  `[0xFE]++SQL` binary frame).
- `POST /v1/op`: body is the binary `Op::encode()` bytes. We call
  `Op::decode(body).ok_or(BadOp)` and apply. `None` → `400` with
  `{"status":"error","message":"undecodable Op bytes"}`.

### 4.2 Response

**Status line.** `HTTP/1.1 <code> <reason>\r\n`. Reasons are the canonical IANA
reasons for the codes we emit (200 OK, 400 Bad Request, 401 Unauthorized, 404 Not
Found, 405 Method Not Allowed, 411 Length Required, 413 Payload Too Large, 414 URI
Too Long, 415 Unsupported Media Type, 417 Expectation Failed, 429 Too Many Requests,
500 Internal Server Error, 503 Service Unavailable).

**Headers.** Every response carries:
- `Content-Type: application/json; charset=utf-8` (for `/v1/sql`, `/v1/op`,
  `/v1/health`) or `text/plain; version=0.0.4; charset=utf-8` (for `/v1/metrics`,
  Prometheus text-format).
- `Content-Length: <decimal>`.
- `Connection: close`.
- `Server: kesseldb/<crate-version>`.

**Body.**
- `POST /v1/sql` and `POST /v1/op` always return `format_result_json(&op_result)`
  verbatim — the contract is locked. HTTP status is one of:
  - `200` for every variant **except** `Unauthorized` (→ `401`) and `Unavailable`
    (→ `503` when cluster-wide, `429` when local in-flight cap was hit). The status
    field in the JSON body still carries the semantic, so a strict client can rely on
    JSON alone and ignore HTTP status if it prefers.
- `GET /v1/health` returns
  `{"status":"ok","primary":<bool>,"view":<u64>,"op_number":<u64>,"role":"primary"|"backup"}`
  with HTTP `200` when the cluster has a primary, HTTP `503` when no primary is known
  (e.g. mid-view-change).
- `GET /v1/metrics` returns Prometheus text v0.0.4 (one HELP + TYPE + sample line set
  per metric); HTTP `200`. See §6.

### 4.3 Exactly-once semantics

When both `X-Kessel-Client-Id` and `X-Kessel-Req-Seq` are present, the gateway treats
the request identically to a binary session frame: the engine's per-`ClientId`
dedup map keyed by `(client_id, req_seq)` returns the cached `OpResult` if the same
`(id, seq)` was already applied. Two practical implications:

- **Idempotent retries are safe.** A client can POST the same `(id, seq)` after a
  network timeout and get the original result, no double-apply.
- **The dedup window is shared with the binary protocol.** A client that started on
  the binary listener and later retries on the HTTP gateway with the same
  `(client_id, req_seq)` deduplicates correctly. (Practically rare; specified for
  completeness.)

When the headers are absent, the request is "fire-and-forget" — each POST is a fresh
op with no dedup, identical to today's `[0xFE]++SQL` binary frame (no session). The
caller is responsible for idempotency, exactly as today.

The `client_id` is a 32-character lowercase hex `u128` (e.g.
`0123456789abcdef0123456789abcdef`); any other shape → `400`. The `req_seq` is a
decimal `u64`; non-decimal or overflow → `400`.

### 4.4 Error mapping

| Outcome | HTTP status | JSON body |
|---|---|---|
| `OpResult::Ok` (and every non-error variant) | `200` | `format_result_json(&r)` |
| `OpResult::Unauthorized` | `401` | `{"status":"unauthorized"}` |
| `OpResult::Unavailable` (engine in-flight cap) | `429` | `{"status":"unavailable"}` |
| `OpResult::Unavailable` (cluster — no primary) | `503` | `{"status":"unavailable"}` |
| `OpResult::SchemaError(msg)` | `200` | `{"status":"error","message":"…"}` |
| `OpResult::Constraint(msg)` | `200` | `{"status":"constraint","message":"…"}` |
| Malformed HTTP request line | `400` | `{"status":"error","message":"bad request line"}` |
| Missing `Host` | `400` | `{"status":"error","message":"missing Host"}` |
| Method other than GET/POST | `405` | `{"status":"error","message":"method not allowed"}` |
| Path other than the 4 routes | `404` | `{"status":"error","message":"not found"}` |
| Missing `Content-Length` on POST | `411` | `{"status":"error","message":"length required"}` |
| Body exceeds `http_max_body` cap | `413` | `{"status":"error","message":"payload too large"}` |
| Request line / headers exceed 64 KiB | `414` | `{"status":"error","message":"URI too long"}` |
| Wrong `Content-Type` | `415` | `{"status":"error","message":"unsupported media type"}` |
| `Expect: 100-continue` (V1 unsupported) | `417` | `{"status":"error","message":"expectation failed"}` |
| `Op::decode` returned None | `400` | `{"status":"error","message":"undecodable Op bytes"}` |
| Bad header value (non-UTF-8, conflicting TE+CL, bad client-id hex, etc.) | `400` | `{"status":"error","message":"<specific>"}` |
| Internal panic (guard; should never fire) | `500` | `{"status":"error","message":"internal"}` |

The HTTP status carries an operational signal (load balancers can act on 5xx);
the JSON body carries the semantic.

---

## 5. Auth model

- **Open mode** (`ServerConfig.token == None`): every well-formed HTTP request is
  accepted on the gateway, identically to the binary listener. The `Authorization`
  header, if present, is ignored.
- **Token mode** (`ServerConfig.token == Some(t)`): every request must carry
  `Authorization: Bearer <hex-or-base64-token>`. The token bytes are extracted (the
  literal characters after `Bearer `, trimmed); compared with `ct_eq(&extracted, &t)`
  — the same constant-time path the binary listener uses. Mismatch → HTTP `401` with
  `{"status":"unauthorized"}`. Missing header in token mode → HTTP `401` with
  `{"status":"unauthorized"}`.
- **HTTPS** does not change auth; it only encrypts. A plaintext gateway on a private
  network is the same security posture as today's plaintext binary listener.
- `/v1/health` and `/v1/metrics` honor the same auth rule. (No "anonymous" health
  check: in token mode, even health requires Bearer. Operators who want unauth'd
  health put a sidecar in front, or run open-mode on a private network — same trade-
  off as today.)

---

## 6. `/v1/metrics` (Prometheus text v0.0.4)

Six metrics, all process-lifetime counters or gauges; **no labels with unbounded
cardinality**. The total label cardinality is `4 (paths) × ~8 (status classes) +
~16 (op kinds) + 5 (singletons)` < 80 series, regardless of traffic shape.

```
# HELP kesseldb_ops_total Number of Ops applied since process start.
# TYPE kesseldb_ops_total counter
kesseldb_ops_total{kind="create"} 1234
kesseldb_ops_total{kind="put"} 56789
...

# HELP kesseldb_inflight Number of Ops currently in flight to the engine.
# TYPE kesseldb_inflight gauge
kesseldb_inflight 7

# HELP kesseldb_last_op_number Highest applied op_number on this replica.
# TYPE kesseldb_last_op_number gauge
kesseldb_last_op_number 4291

# HELP kesseldb_view_number Current VSR view number.
# TYPE kesseldb_view_number gauge
kesseldb_view_number 17

# HELP kesseldb_is_primary 1 if this replica is the primary in the current view.
# TYPE kesseldb_is_primary gauge
kesseldb_is_primary 1

# HELP kesseldb_http_requests_total HTTP gateway requests by path and status.
# TYPE kesseldb_http_requests_total counter
kesseldb_http_requests_total{path="/v1/sql",status="200"} 1023
kesseldb_http_requests_total{path="/v1/sql",status="400"} 4
kesseldb_http_requests_total{path="/v1/op",status="200"} 19
kesseldb_http_requests_total{path="/v1/health",status="200"} 5821
kesseldb_http_requests_total{path="/v1/metrics",status="200"} 5821
```

The op-kind list is the closed set already enumerated by `Op::kind()` (~30 entries);
this is bounded by construction and does not grow with workload.

`Op` kind names are emitted from a static `match` over `Op::kind()`; counters are kept
in an `Arc<[AtomicU64; N]>` indexed by kind, atomically incremented after a successful
apply, never on a parse error.

The counters live in `kesseldb-server` (single source of truth across the binary and
HTTP listeners); the gateway reads a snapshot via an `Arc<MetricsSnapshot>` handle.

---

## 7. `/v1/health`

`GET /v1/health` reads a snapshot of the cluster state from the existing
`kesseldb-server` cluster module (no engine apply, no Op submitted). Response shape:

```json
{
  "status": "ok",
  "primary": true,
  "view": 17,
  "op_number": 4291,
  "role": "primary"
}
```

`role ∈ {"primary","backup"}`. When the cluster is mid-view-change and no primary is
known, the response is HTTP `503` with `{"status":"unavailable"}` — usable as a
k8s readiness probe.

This endpoint never blocks; the snapshot is a cheap lock-and-copy of three integers
plus a bool from the existing cluster state.

---

## 8. Security posture

- **Constant-time auth.** Token compared via `ct_eq` (the same helper the binary
  listener uses). No early-return on first differing byte.
- **Bounded buffering.** Header bytes capped at 64 KiB before `\r\n\r\n`; body capped
  at `http_max_body` (default 8 MiB, configurable). A lying `Content-Length` cannot
  exhaust memory — we cap on receive, not on declared length.
- **No `unsafe`.** `#![forbid(unsafe_code)]` at the crate root, same as every other
  KesselDB crate.
- **No panic on adversarial input.** Every parser path uses `checked_get` /
  `try_into` / `parse::<T>()`-with-error-map; the only `unwrap` allowed in the crate
  is the statically-infallible `[u8; N]` slice-to-array via `try_into().unwrap()`,
  matching the existing project rule.
- **Chunked decode is bounded.** We reuse the dechunk shape from `kessel-fetch::http`
  with its `b.len() < size + 2` guard against missing trailing CRLF, capping output
  at `http_max_body`.
- **Engine isolation.** A malformed HTTP request never reaches the engine apply path
  — it is rejected at parse with a typed HTTP error and a counter increment.
- **Apply-path discipline.** Once an Op is decoded, it enters the engine via the
  same `Arc<EngineHandle>::apply` the binary listener uses — same in-flight
  semaphore, same dedup map, same VSR submission, same deterministic SM. The gateway
  has no second apply path.
- **TLS reuse.** Cert + key come from the existing `ServerConfig.tls`. There is no
  second cert store, no second roots bundle, no `webpki` divergence.

### 8.1 Pentest matrix (mandatory T5)

Each row asserts a typed HTTP response (specific status + body shape), no panic, no
OOM, no protocol-state corruption (the listener accepts the next connection cleanly).

| Input | Expected |
|---|---|
| Request line missing `HTTP/1.1` | `400 bad request line` |
| Method `DELETE` | `405 method not allowed` |
| Path `/v2/sql` | `404 not found` |
| Missing `Host` header | `400 missing Host` |
| `Host: [::1]` (IPv6 literal) | `400 IPv6 literal Host not supported` |
| Header total > 64 KiB | `414 URI too long` |
| `POST /v1/sql` no `Content-Length`, no `TE: chunked` | `411 length required` |
| `Content-Length: 99999999` (above cap) | `413 payload too large` |
| `Content-Length: 100`, actual body 50 bytes then close | `400 short body` |
| `Content-Length: 5` + `Transfer-Encoding: chunked` | `400 conflicting framing` |
| `Transfer-Encoding: chunked` with bad chunk size | `400 bad chunk` |
| Chunked body total > cap | `413 payload too large` |
| `POST /v1/sql` body non-UTF-8 | `400 invalid UTF-8` |
| `POST /v1/op` body 1 byte (undecodable Op) | `400 undecodable Op bytes` |
| `POST /v1/sql` with `Content-Type: application/json` | `415 unsupported media type` |
| Token mode, no `Authorization` | `401 unauthorized` |
| Token mode, `Authorization: Bearer wrong` | `401 unauthorized` |
| Open mode, `Authorization: Bearer anything` | `200` (header ignored) |
| `X-Kessel-Client-Id` alone (no req-seq) | `400 both client-id and req-seq required` |
| `X-Kessel-Client-Id: GG...` (non-hex) | `400 bad client-id` |
| `X-Kessel-Req-Seq: notanumber` | `400 bad req-seq` |
| `Expect: 100-continue` | `417 expectation failed` |
| Engine in-flight cap saturated | `429 unavailable` |
| No primary in cluster (request hits while view-change in flight) | `503 unavailable` |
| Two requests pipelined on one TCP connection (HTTP/1.1 keep-alive style) | First handled; we send `Connection: close` so second times out cleanly without state corruption |

---

## 9. Test plan

T0 (baseline) records measured pre-slice `cargo test --workspace --release` total +
`large_seed_corpus_is_deterministic_and_converges` + `cargo tree -p kesseldb-server`
(no features) — the absolute baseline number is whatever T0 measures (the SP100-style
per-slice +DELTA is authoritative, not a guessed absolute). The T6 docs slice
reconciles measured `before → after` honestly (no zero-delta dishonesty — adding a
new crate with KAT + e2e + pentest tests raises the count and we record the real
DELTA).

T-tasks (the plan formalizes T0..T7):

- **T0:** Determinism baseline (record measured workspace test count, seed-7 green,
  default `cargo tree` cleanliness).
- **T1:** Scaffold `kessel-http-gateway` crate (Cargo.toml, lib.rs, `#![forbid(unsafe_code)]`,
  empty `[dependencies]`); workspace registration; default-build still byte-identical.
- **T2:** `parse.rs` request-line + headers + Content-Length body, with hand-built KATs
  (every parser branch). No body cap yet to keep T2 focused; T3 wires caps.
- **T3:** `parse.rs` chunked decode + body caps + header cap + Bearer + exactly-once
  header extraction, with hand-built KATs covering the §4.1 matrix.
- **T4:** `response.rs` + `routes.rs` + `server.rs`: four route handlers, response
  writer, accept loop, in-flight semaphore handshake with `kesseldb-server`. Wire
  `kesseldb-server` `http-gateway` feature + `ServerConfig.http_addr`. Add the e2e
  `tests/e2e_curl.rs` covering all 4 routes happy-path, auth on/off, exactly-once
  dedup pin, JSON contract pin (assert `format_result_json` output exactly), HTTPS
  smoke (feature-gated on `tls`).
- **T5:** `tests/pentest.rs` — every row of §8.1 with explicit assertion of status +
  body + listener-still-accepting-next-connection.
- **T6:** `/v1/metrics` (counters + gauges + Prometheus text writer) + `/v1/health`
  if not already in T4; integration test scraping `/v1/metrics` after a known op
  sequence and asserting the exact text output.
- **T7:** Docs slice — `docs/STATUS.md` row, `docs/USAGE.md` §HTTP gateway,
  `README.md` capability matrix, `docs/ARCHITECTURE.md` listener layout, the SP141
  internal record (`docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md`)
  mirroring the SP140 record convention, memory append + MEMORY.md line update
  (memory files outside repo — never `git add`).

Every kernel-adjacent task is gated by `cargo test --workspace --release FAILED=0` +
`large_seed_corpus_is_deterministic_and_converges` green + the existing oracles green.
The two-stage subagent review (spec-then-quality) runs between every task. The plan
is executed via `superpowers:subagent-driven-development`.

### 9.1 Determinism / engine isolation tests

Two locks the spec reviewer must verify in T4:

- **Apply-path identity.** A POST `/v1/sql` and a binary `[0xFE]++SQL` with identical
  SQL bytes must produce byte-identical `OpResult::encode()` bytes (asserted by
  decoding the JSON back via `format_result_json` round-trip).
- **Op-path identity.** A POST `/v1/op` with body = `Op::encode(o)` and a binary
  bare-Op frame with the same `Op::encode(o)` must produce byte-identical
  `OpResult::encode()` bytes.

### 9.2 Pentest fault-injection (T5)

Reuses the existing project pentest helpers (`catch_unwind` + `well_behaved` style
locks). Every adversarial input must return Err / typed HTTP response; the listener
must accept the next connection on the same port cleanly (verified by a follow-up
benign request in the same test).

---

## 10. Documentation deltas (T7)

- **`docs/USAGE.md`** — new §"HTTP gateway" with:
  - The four routes and example `curl` commands.
  - The `Authorization: Bearer` and `X-Kessel-Client-Id` / `X-Kessel-Req-Seq` header
    contract.
  - The error mapping table (§4.4).
  - The Prometheus metrics list (§6).
  - The opt-in note (`--features http-gateway`) and the default build still being
    zero-dep.
  - The note from today's §"Transport encryption" — the existing line "deploy
    behind a TLS-terminating reverse proxy" gains a sibling sentence: "or build with
    `--features http-gateway,tls` to terminate HTTPS in-process on `:6790`".
- **`docs/STATUS.md`** — one row after the SP140 row: `SP141 HTTP gateway: shipped
  …`. The Production-readiness-gate section gets a small note that HTTP gateway is
  available as an opt-in for k8s/HAProxy / curl / Prometheus integration; the
  binary protocol remains the deterministic hot path.
- **`README.md`** — capability matrix gains a row "HTTP/1.1 gateway (opt-in): full
  Op surface + SQL + health + metrics". Test-count line updated honestly.
- **`docs/ARCHITECTURE.md`** — crate list gains `kessel-http-gateway`; the
  "Listeners" section (or its equivalent) gains a paragraph describing the two
  sibling listener threads and how they share `max_inflight`.
- **Memory** — append a `## SP141 — HTTP/1.1 wire gateway` block to
  `memory/project_kesseldb.md` summarizing the route surface + auth model + opt-in
  feature flag + UNCHANGED-binary-protocol invariant; update the one-line index entry
  in `MEMORY.md`. Memory files live outside the repo and are never `git add`-ed.

---

## 11. Open questions (none blocking V1)

Documented here so future slices have a clear pickup point; none of these block this
slice.

- **Persistent connections / `keep-alive`.** V1 sends `Connection: close` on every
  response. A follow-up can implement true HTTP/1.1 keep-alive when a real consumer
  asks; the request-side keep-alive parser is trivial, the response-side state
  machine is the work.
- **Per-route auth scopes.** V1 has one token. A follow-up can layer per-route or
  per-Op auth (e.g. read-only Bearer that can't `CreateType`) on top.
- **Per-tenant routing.** Out of scope; KesselDB is single-tenant by design at the
  server level. Multi-tenant orchestration lives outside the gateway.
- **`OpenAPI` / discovery doc.** A `/v1/openapi.json` route is trivially additive
  and doesn't change any V1 invariant. Defer to a follow-up if real consumers want
  it; the four routes are stable enough to document by hand in `USAGE.md` in the
  meantime.
- **WebSocket / SSE.** A clean follow-up slice (`SP141b`?) when a real pipelined-
  over-HTTP need appears. The binary protocol covers it today.

---

## 12. Acceptance criteria

The slice is "done" when, against a checkout of `kesseldb-server` built with
`--features http-gateway` (and separately with `--features http-gateway,tls`):

1. All §4.1 / §4.2 route + header behaviors pass their KAT + e2e tests.
2. All §8.1 pentest rows pass.
3. `cargo test --workspace --release` is green; FAILED=0.
4. `large_seed_corpus_is_deterministic_and_converges` is green.
5. Default `cargo build` of `kesseldb-server` is byte-identical to before (no new
   crate linked, no new dep pulled, `cargo tree -p kesseldb-server` unchanged).
6. The 7 Parquet pyarrow e2e oracles, the 2 external-source oracles, the TLS oracle,
   and the objstore oracle all remain green untouched.
7. `docs/USAGE.md` / `docs/STATUS.md` / `README.md` / `docs/ARCHITECTURE.md` are
   updated honestly with the measured DELTA reconciliation in the SP141 internal
   record.
8. A live `curl -i http://localhost:6789/v1/health` against a running
   `kesseldb-server --http-addr 127.0.0.1:6789` returns the documented JSON.
9. A live `curl -X POST --data-binary 'SELECT 1' http://localhost:6789/v1/sql`
   against the same server returns `200 {"status":"ok","value":1}` (or the relevant
   `format_result_json` shape for a SELECT 1).
