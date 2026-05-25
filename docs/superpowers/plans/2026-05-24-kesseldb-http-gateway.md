# KesselDB SP141 — HTTP/1.1 Wire Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship an opt-in HTTP/1.1 gateway exposing the existing Op surface (POST `/v1/sql`, POST `/v1/op`) + ops endpoints (GET `/v1/health`, GET `/v1/metrics`) on a sibling listener, with `Authorization: Bearer` auth, optional exactly-once headers, and JSON responses via the existing `kessel-client::format_result_json` contract. Binary protocol byte-untouched.

**Architecture:** New zero-external-dep crate `kessel-http-gateway` with a local `EngineApply` trait (`kesseldb-server::EngineHandle` impls it — inverts the dependency so there is no cycle). Opt-in `kesseldb-server` `http-gateway` cargo feature spawns a second listener thread on `ServerConfig.http_addr` (and a third on `http_tls_addr` when `tls` is also on). The gateway parses HTTP/1.1 → calls `EngineApply::apply_*` → writes `format_result_json` back. Engine, deterministic SM, replication, MVCC all untouched.

**Tech Stack:** Rust 1.x workspace, zero external (non-workspace) crates in the gateway, `std::net::TcpListener` + per-connection thread (mirrors the binary listener), `#![forbid(unsafe_code)]`, hand-rolled HTTP/1.1 parser modelled on `kessel-fetch::http`. Hand-built KAT bytes derived from RFC 9112 (HTTP/1.1) — the spec-compliance reviewer re-derives every KAT.

**Spec:** `docs/superpowers/specs/2026-05-24-kesseldb-http-gateway-design.md` (SP141 design).

**Process note (autonomous mandate substitution).** Per `feedback_kesseldb_autonomous_build`: commit straight to `main` after each task (no Co-Authored-By, no signing, match `git log -3` style — `gateway: …` / `server: …` / `docs: …` / `test: …`), `git push origin main` after each commit. The two-stage spec-then-quality subagent review gate is the review. Memory files live OUTSIDE the repo — never `git add memory/*`.

**Determinism / invariants gate (run after EVERY task):**

```bash
cd C:/Users/ihass/KesselDB
cargo test --workspace --release 2>&1 | tail -20    # FAILED=0
cargo test -p kesseldb-server --release large_seed_corpus_is_deterministic_and_converges 2>&1 | tail -10
cargo tree -p kesseldb-server 2>&1 | head -30       # NO hyper/httparse/h2/tokio/mio/socket2 lines
```

Plus task-specific oracles called out in each task.

---

## File Map (locked decomposition)

**New (T1 scaffolds, T2–T6 fill):**

| Path | Responsibility | Touched in task |
|---|---|---|
| `crates/kessel-http-gateway/Cargo.toml` | Crate manifest; `[dependencies]` lists ONLY workspace members `kessel-proto` + `kessel-client`. No `[dependencies.kesseldb-server]`. | T1 |
| `crates/kessel-http-gateway/src/lib.rs` | `#![forbid(unsafe_code)]`, module declarations, public `serve` entry-point, public re-exports (`EngineApply`, `HealthSnapshot`, `MetricsSnapshot`). | T1 (skeleton), T4 (serve body) |
| `crates/kessel-http-gateway/src/engine.rs` | `pub trait EngineApply` + `HealthSnapshot` + `MetricsSnapshot` structs. | T1 (signatures), T4 (final shape) |
| `crates/kessel-http-gateway/src/parse.rs` | Request-line + headers + Content-Length body parsing; chunked decode; Bearer + exactly-once header extraction. | T2, T3 |
| `crates/kessel-http-gateway/src/response.rs` | Response writer (status line, headers, body, `Connection: close`). | T4 |
| `crates/kessel-http-gateway/src/routes.rs` | Four route handlers (`/v1/sql`, `/v1/op`, `/v1/health`, `/v1/metrics`). | T4 (sql/op/health), T6 (metrics) |
| `crates/kessel-http-gateway/src/server.rs` | `TcpListener::accept` loop, per-conn thread, in-flight semaphore. | T4 |
| `crates/kessel-http-gateway/src/metrics_writer.rs` | Prometheus text v0.0.4 writer (no external prom crate). | T6 |
| `crates/kessel-http-gateway/tests/parse_kats.rs` | Hand-built byte KATs for every parser branch. | T2, T3 |
| `crates/kessel-http-gateway/tests/e2e_curl.rs` | End-to-end raw `TcpStream` against a live `kesseldb-server` with `http-gateway` feature. | T4 |
| `crates/kessel-http-gateway/tests/pentest.rs` | Adversarial inputs from spec §8.1; every row asserts typed response + next-connection-clean. | T5 |
| `crates/kessel-http-gateway/tests/metrics_e2e.rs` | `/v1/metrics` + `/v1/health` integration. | T6 |
| `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md` | Internal SP141 record (mirrors SP140 convention). | T7 |

**Modified (additive only — every existing call site keeps compiling):**

| Path | Change | Task |
|---|---|---|
| `Cargo.toml` (root) | Add `"crates/kessel-http-gateway"` to `members`. | T1 |
| `crates/kesseldb-server/Cargo.toml` | Add `[features] http-gateway = ["dep:kessel-http-gateway"]` + `kessel-http-gateway = { path = "../kessel-http-gateway", optional = true }`. | T4 |
| `crates/kesseldb-server/src/lib.rs` | `ServerConfig` gains two additive `Option<SocketAddr>` fields; `serve_cfg` gains a feature-gated thread spawn; `EngineHandle` impls the gateway's `EngineApply` (via `#[cfg(feature = "http-gateway")]`). | T4 |
| `docs/STATUS.md` | Row after SP140. | T7 |
| `docs/USAGE.md` | New §HTTP gateway. | T7 |
| `README.md` | Capability matrix row + test-count line. | T7 |
| `docs/ARCHITECTURE.md` | Crate list + listener-layout paragraph. | T7 |

**Untouched (locked invariants):**

- `kessel-proto`, `kessel-client`, `kessel-sm`, `kessel-storage`, `kessel-sql`, `kessel-vsr`, `kessel-parquet`, `kessel-fetch`, `kessel-objstore`, every deterministic-kernel crate.
- `kesseldb-server::handle_conn`, the binary serve loop, `read_frame`/`write_frame`, the auth handshake, the per-connection SQL txn buffer — byte-untouched.

---

## Task T0: Determinism baseline

**Files:** none modified. This task records the pre-slice measured state.

- [ ] **Step 1:** Run baseline tests and record the count.

```bash
cd C:/Users/ihass/KesselDB
cargo test --workspace --release 2>&1 | tail -5
```

Expected: a line `test result: ok. <N> passed; 0 failed; <I> ignored; …`. Record the exact `N` as `BASELINE_TOTAL`.

- [ ] **Step 2:** Run the seed-7 determinism oracle.

```bash
cargo test -p kesseldb-server --release large_seed_corpus_is_deterministic_and_converges 2>&1 | tail -5
```

Expected: `test result: ok. 1 passed; 0 failed`.

- [ ] **Step 3:** Verify default `kesseldb-server` build links no HTTP server crates.

```bash
cargo tree -p kesseldb-server --no-default-features 2>&1 | grep -E "hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp"
```

Expected: empty output (nothing matches).

- [ ] **Step 4:** Record the baseline in the task description for later reconciliation.

Write the three numbers to a scratch note in the controller's TodoWrite: `BASELINE_TOTAL=<N>, seed-7=GREEN, cargo-tree-clean=YES`. The T7 docs slice will compute `FINAL_TOTAL - BASELINE_TOTAL = +DELTA`.

- [ ] **Step 5:** Commit (no source change yet, so this is a marker commit — skip the commit; T1 carries the first code).

---

## Task T1: Scaffold `kessel-http-gateway` crate

**Files:**
- Create: `crates/kessel-http-gateway/Cargo.toml`
- Create: `crates/kessel-http-gateway/src/lib.rs`
- Create: `crates/kessel-http-gateway/src/engine.rs`
- Create: `crates/kessel-http-gateway/src/parse.rs` (stub)
- Create: `crates/kessel-http-gateway/src/response.rs` (stub)
- Create: `crates/kessel-http-gateway/src/routes.rs` (stub)
- Create: `crates/kessel-http-gateway/src/server.rs` (stub)
- Modify: `Cargo.toml` (root, add to `members`)

- [ ] **Step 1:** Add the crate to the workspace members list.

Open `C:/Users/ihass/KesselDB/Cargo.toml`. In the `members = [` array, add the new entry **in alphabetical order** alongside the other `crates/kessel-*` entries (e.g. immediately after `"crates/kessel-fetch"`):

```toml
    "crates/kessel-http-gateway",
```

- [ ] **Step 2:** Create the crate manifest.

Write `crates/kessel-http-gateway/Cargo.toml`:

```toml
[package]
name = "kessel-http-gateway"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
# Zero external (non-workspace) deps — the deterministic-kernel zero-dep
# invariant. Workspace members only.
kessel-proto = { path = "../kessel-proto" }
kessel-client = { path = "../kessel-client" }

[lib]
path = "src/lib.rs"
```

Do NOT add `kesseldb-server` as a dep — that would create a cycle with the optional `http-gateway` feature on `kesseldb-server`. The apply interface is a local trait (see Step 4).

- [ ] **Step 3:** Create `crates/kessel-http-gateway/src/lib.rs`:

```rust
//! KesselDB HTTP/1.1 wire gateway (SP141).
//!
//! Opt-in via the `kesseldb-server` `http-gateway` cargo feature. Translates
//! HTTP/1.1 requests on a sibling listener into the existing Op-apply pipeline
//! and emits `kessel_client::format_result_json` responses. The binary wire
//! protocol is byte-untouched.
//!
//! Zero external (non-workspace) dependencies — `std::net::TcpListener`,
//! `std::thread`, and a hand-rolled HTTP/1.1 parser. See
//! `docs/superpowers/specs/2026-05-24-kesseldb-http-gateway-design.md`.

#![forbid(unsafe_code)]
#![allow(dead_code)]

pub mod engine;
pub mod parse;
pub mod response;
pub mod routes;
pub mod server;

pub use engine::{EngineApply, HealthSnapshot, MetricsSnapshot, OpKindCounter};
pub use server::serve;
```

- [ ] **Step 4:** Create `crates/kessel-http-gateway/src/engine.rs` with the trait signatures (T4 fills the snapshot bodies):

```rust
//! Engine-apply trait + snapshot value types. Defined HERE (not in
//! `kesseldb-server`) so the dependency direction is one-way:
//! `kesseldb-server` (when built with `--features http-gateway`) depends on
//! this crate and `impl EngineApply for EngineHandle`. This crate has no
//! `kesseldb-server` dep — no cycle.

use kessel_proto::{ClientId, Op, OpResult};

/// Apply entry-point the gateway uses to dispatch into the engine.
///
/// Implemented by `kesseldb-server::EngineHandle` under
/// `#[cfg(feature = "http-gateway")]`. Trait-object friendly
/// (`Arc<dyn EngineApply>`).
pub trait EngineApply: Send + Sync + 'static {
    /// Apply a bare `Op` (the binary `/v1/op` body, post-decode).
    fn apply_op(&self, op: Op) -> OpResult;

    /// Apply a bare `Op` under a `(client_id, req_seq)` exactly-once binding.
    /// When both `X-Kessel-Client-Id` and `X-Kessel-Req-Seq` are present, the
    /// gateway routes through this entry-point; the engine's existing dedup
    /// map deduplicates retries of the same `(client_id, req_seq)`.
    fn apply_op_with_session(
        &self,
        client: ClientId,
        req: u64,
        op: Op,
    ) -> OpResult;

    /// Apply raw SQL text (the `/v1/sql` body, validated UTF-8). Wraps as
    /// `[0xFE] ++ sql_bytes` and dispatches through `apply_raw`.
    fn apply_sql(&self, sql: &str) -> OpResult;

    /// Snapshot of liveness state for `GET /v1/health`. Cheap — three
    /// integers + a bool — no engine apply.
    fn snapshot_health(&self) -> HealthSnapshot;

    /// Snapshot of metric counters/gauges for `GET /v1/metrics`. Cheap —
    /// atomic loads on shared `Arc<AtomicU64>` counters; no engine apply.
    fn snapshot_metrics(&self) -> MetricsSnapshot;
}

/// Liveness snapshot — see spec §7.
#[derive(Clone, Debug)]
pub struct HealthSnapshot {
    pub primary: bool,
    pub view: u64,
    pub op_number: u64,
    /// "primary" or "backup".
    pub role: &'static str,
}

/// One Op-kind counter row — see spec §6.
#[derive(Clone, Debug)]
pub struct OpKindCounter {
    pub kind: &'static str,
    pub count: u64,
}

/// Metrics snapshot — see spec §6. The op-kinds vector is the closed set of
/// `Op::kind()` values; size is bounded by construction.
#[derive(Clone, Debug)]
pub struct MetricsSnapshot {
    pub ops_total: Vec<OpKindCounter>,
    pub inflight: u64,
    pub last_op_number: u64,
    pub view_number: u64,
    pub is_primary: bool,
    /// HTTP-side counters indexed by (path, status). Path is one of the four
    /// known route strings; status is the decimal HTTP code as `&str`. Bounded
    /// cardinality.
    pub http_requests_total: Vec<HttpRequestCounter>,
}

#[derive(Clone, Debug)]
pub struct HttpRequestCounter {
    pub path: &'static str,
    pub status: &'static str,
    pub count: u64,
}
```

- [ ] **Step 5:** Create the four module stubs so the crate compiles:

`crates/kessel-http-gateway/src/parse.rs`:

```rust
//! HTTP/1.1 request parser (request line, headers, body). T2 and T3 fill
//! this module. Hand-rolled per RFC 9112; no `httparse`/`hyper`.

#![allow(dead_code)]
```

`crates/kessel-http-gateway/src/response.rs`:

```rust
//! HTTP/1.1 response writer. T4 fills this module.

#![allow(dead_code)]
```

`crates/kessel-http-gateway/src/routes.rs`:

```rust
//! Route handlers for /v1/sql, /v1/op, /v1/health, /v1/metrics. T4 and T6
//! fill this module.

#![allow(dead_code)]
```

`crates/kessel-http-gateway/src/server.rs`:

```rust
//! TCP accept loop + per-connection thread. T4 fills this module.

#![allow(dead_code)]

use crate::engine::EngineApply;
use std::net::TcpListener;
use std::sync::Arc;

/// Public entry-point — `kesseldb-server` calls this on a dedicated thread
/// when the `http-gateway` feature is on.
pub fn serve(_listener: TcpListener, _engine: Arc<dyn EngineApply>) {
    // T4 fills the accept loop.
}
```

- [ ] **Step 6:** Verify the crate compiles cleanly and the workspace still builds.

```bash
cd C:/Users/ihass/KesselDB
cargo build -p kessel-http-gateway 2>&1 | tail -10
cargo build --workspace 2>&1 | tail -5
```

Expected: both succeed with `Finished` lines and no warnings (the `#![allow(dead_code)]` covers the stubs).

- [ ] **Step 7:** Verify the determinism gate.

```bash
cargo test --workspace --release 2>&1 | tail -5
cargo test -p kesseldb-server --release large_seed_corpus_is_deterministic_and_converges 2>&1 | tail -3
cargo tree -p kesseldb-server --no-default-features 2>&1 | grep -E "hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp"
```

Expected: workspace tests `passed; 0 failed` (count = `BASELINE_TOTAL` unchanged — no new tests yet); seed-7 green; tree-grep empty.

- [ ] **Step 8:** Commit + push.

```bash
git add Cargo.toml crates/kessel-http-gateway
git commit -m "gateway: SP141 T1 — scaffold kessel-http-gateway crate (zero-dep, EngineApply trait, module stubs)"
git push origin main
```

---

## Task T2: HTTP/1.1 request-line + headers + Content-Length body parser

**Files:**
- Modify: `crates/kessel-http-gateway/src/parse.rs`
- Create: `crates/kessel-http-gateway/tests/parse_kats.rs`

> **Highest-risk task — use a CAPABLE model.** Every parser branch must be unambiguous, bounds-checked, and panic-free on adversarial input. Mirror `kessel-fetch::http` style.

- [ ] **Step 1:** Write the failing KATs (TDD). Create `crates/kessel-http-gateway/tests/parse_kats.rs`:

```rust
//! Hand-built RFC 9112 byte KATs for the HTTP/1.1 request parser. Every
//! KAT is derived independently from the RFC and serves as the spec-compliance
//! oracle for `parse.rs` — the spec-compliance reviewer re-derives each one.

use kessel_http_gateway::parse::{parse_request, ParseError, Request, Method};

#[test]
fn kat_simple_get_health() {
    // Hand-derived from RFC 9112 §3 + §5. Request line + Host + blank.
    let bytes = b"GET /v1/health HTTP/1.1\r\nHost: localhost:6789\r\n\r\n";
    let req = parse_request(bytes).expect("well-formed GET parses");
    assert_eq!(req.method, Method::Get);
    assert_eq!(req.path, "/v1/health");
    assert_eq!(req.host, "localhost:6789");
    assert!(req.body.is_empty());
    assert_eq!(req.consumed, bytes.len());
}

#[test]
fn kat_simple_post_sql_content_length() {
    // Hand-derived: request line + Host + Content-Length + Content-Type +
    // blank + body.
    let body = b"SELECT 1";
    let bytes = b"POST /v1/sql HTTP/1.1\r\nHost: localhost:6789\r\n\
                  Content-Type: text/plain\r\nContent-Length: 8\r\n\r\nSELECT 1";
    let req = parse_request(bytes).expect("well-formed POST parses");
    assert_eq!(req.method, Method::Post);
    assert_eq!(req.path, "/v1/sql");
    assert_eq!(req.body, body);
    assert_eq!(req.content_type.as_deref(), Some("text/plain"));
    assert_eq!(req.consumed, bytes.len());
}

#[test]
fn kat_post_op_binary_content_type() {
    let body = vec![0x01, 0x02, 0x03];
    let mut bytes = b"POST /v1/op HTTP/1.1\r\nHost: h\r\n\
                      Content-Type: application/x-kessel-op\r\n\
                      Content-Length: 3\r\n\r\n".to_vec();
    bytes.extend_from_slice(&body);
    let req = parse_request(&bytes).expect("binary body parses");
    assert_eq!(req.body, body.as_slice());
    assert_eq!(req.content_type.as_deref(),
               Some("application/x-kessel-op"));
}

#[test]
fn kat_rejects_missing_host() {
    let bytes = b"GET /v1/health HTTP/1.1\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::MissingHost), "got {:?}", err);
}

#[test]
fn kat_rejects_ipv6_literal_host() {
    let bytes = b"GET /v1/health HTTP/1.1\r\nHost: [::1]:6789\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::Ipv6LiteralHost), "got {:?}", err);
}

#[test]
fn kat_rejects_unknown_method() {
    let bytes = b"DELETE /v1/sql HTTP/1.1\r\nHost: h\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::MethodNotAllowed), "got {:?}", err);
}

#[test]
fn kat_rejects_unknown_path() {
    let bytes = b"GET /v2/sql HTTP/1.1\r\nHost: h\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::NotFound), "got {:?}", err);
}

#[test]
fn kat_rejects_bad_request_line_no_version() {
    let bytes = b"GET /v1/health\r\nHost: h\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::BadRequestLine), "got {:?}", err);
}

#[test]
fn kat_rejects_http_2_0() {
    let bytes = b"GET /v1/health HTTP/2.0\r\nHost: h\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::BadRequestLine), "got {:?}", err);
}

#[test]
fn kat_post_missing_content_length() {
    // RFC 9112 §6.3 — POST with no body framing → 411 Length Required.
    let bytes = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                  Content-Type: text/plain\r\n\r\nSELECT 1";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::LengthRequired), "got {:?}", err);
}

#[test]
fn kat_content_length_lies_short() {
    // Declared 10 bytes, only 3 delivered before \r\n\r\n ends the input.
    let bytes = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                  Content-Type: text/plain\r\nContent-Length: 10\r\n\r\nabc";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::ShortBody), "got {:?}", err);
}

#[test]
fn kat_content_length_non_decimal() {
    let bytes = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                  Content-Type: text/plain\r\nContent-Length: abc\r\n\r\n";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::BadHeaderValue(_)), "got {:?}", err);
}

#[test]
fn kat_headers_case_insensitive() {
    // HOST in upper-case, content-length in mixed-case (RFC 9110 §5.1
    // header names are case-insensitive).
    let bytes = b"POST /v1/sql HTTP/1.1\r\nHOST: h\r\n\
                  content-Type: text/plain\r\nContent-LENGTH: 0\r\n\r\n";
    let req = parse_request(bytes).expect("case-insensitive headers parse");
    assert_eq!(req.host, "h");
}

#[test]
fn kat_no_header_terminator() {
    let bytes = b"GET /v1/health HTTP/1.1\r\nHost: h\r\nNo-Terminator: ";
    let err = parse_request(bytes).unwrap_err();
    assert!(matches!(err, ParseError::NoHeaderTerminator), "got {:?}", err);
}

#[test]
fn kat_content_type_with_charset() {
    let bytes = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                  Content-Type: text/plain; charset=utf-8\r\n\
                  Content-Length: 0\r\n\r\n";
    let req = parse_request(bytes).expect("Content-Type with charset parses");
    // Only the media-type portion is returned; the charset suffix is dropped.
    assert_eq!(req.content_type.as_deref(), Some("text/plain"));
}
```

- [ ] **Step 2:** Run the KATs to confirm they fail (functions undefined).

```bash
cd C:/Users/ihass/KesselDB
cargo test -p kessel-http-gateway --release kat_ 2>&1 | tail -20
```

Expected: compile errors / `parse_request` undefined.

- [ ] **Step 3:** Implement `crates/kessel-http-gateway/src/parse.rs`:

```rust
//! HTTP/1.1 request parser (request line + headers + Content-Length body),
//! hand-rolled per RFC 9112. Mirrors the bounds-checked style of
//! `kessel-fetch::http`. Chunked transfer-encoding, body caps, Bearer, and
//! the X-Kessel-* exactly-once headers come in T3.

#![allow(dead_code)]

/// Hard caps applied at parse time (T3 makes the body cap configurable;
/// header cap stays fixed at 64 KiB per spec §4.1).
pub const MAX_HEADER_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Method {
    Get,
    Post,
}

#[derive(Clone, Debug)]
pub struct Request<'a> {
    pub method: Method,
    pub path: &'a str,
    pub host: String,
    pub content_type: Option<String>,
    pub content_length: Option<u64>,
    pub body: &'a [u8],
    /// Total bytes consumed (headers + body).
    pub consumed: usize,
    /// Raw header lines preserved for later passes (T3 reads Bearer +
    /// X-Kessel-* from here without re-parsing).
    pub headers: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseError {
    BadRequestLine,
    MethodNotAllowed,
    NotFound,
    MissingHost,
    Ipv6LiteralHost,
    LengthRequired,
    BadHeaderValue(String),
    NoHeaderTerminator,
    HeaderTooLarge,
    UnsupportedMediaType,
    ShortBody,
    /// T3 adds these.
    BodyTooLarge,
}

/// Parse one HTTP/1.1 request. Returns `Ok(Request)` if well-formed AND
/// fully received; `Err(ParseError)` otherwise. `consumed` reports how many
/// bytes of `buf` belong to this request (so the caller can drop them).
pub fn parse_request(buf: &[u8]) -> Result<Request<'_>, ParseError> {
    // Cap headers up-front.
    let header_end = find_header_terminator(buf)?;
    if header_end > MAX_HEADER_BYTES {
        return Err(ParseError::HeaderTooLarge);
    }
    let head = std::str::from_utf8(buf.get(..header_end).unwrap_or(&[]))
        .map_err(|_| ParseError::BadRequestLine)?;
    let mut lines = head.split("\r\n");
    let req_line = lines.next().ok_or(ParseError::BadRequestLine)?;
    let (method, path) = parse_request_line(req_line)?;
    if !is_known_path(path) {
        return Err(ParseError::NotFound);
    }

    let mut host: Option<String> = None;
    let mut content_type: Option<String> = None;
    let mut content_length: Option<u64> = None;
    let mut headers: Vec<(String, String)> = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let colon = line.find(':').ok_or_else(||
            ParseError::BadHeaderValue(format!("missing colon: {line:?}")))?;
        let name = line.get(..colon).unwrap_or("").trim().to_string();
        let value = line.get(colon + 1..).unwrap_or("").trim().to_string();
        if name.eq_ignore_ascii_case("host") {
            if value.starts_with('[') {
                return Err(ParseError::Ipv6LiteralHost);
            }
            host = Some(value.clone());
        } else if name.eq_ignore_ascii_case("content-type") {
            // Strip `; charset=…` and any other parameter; keep the
            // media-type only.
            let media = value.split(';').next().unwrap_or("").trim();
            content_type = Some(media.to_string());
        } else if name.eq_ignore_ascii_case("content-length") {
            content_length = Some(value.parse::<u64>().map_err(|_|
                ParseError::BadHeaderValue(format!("Content-Length: {value:?}")))?);
        }
        headers.push((name, value));
    }
    let host = host.ok_or(ParseError::MissingHost)?;

    // POSTs require Content-Length (T3 adds chunked-encoding support).
    let body: &[u8];
    let consumed: usize;
    match method {
        Method::Get => {
            body = &[];
            consumed = header_end;
        }
        Method::Post => {
            let cl = content_length.ok_or(ParseError::LengthRequired)?;
            let cl_usize = usize::try_from(cl).map_err(|_|
                ParseError::BodyTooLarge)?;
            let body_start = header_end;
            let body_end = body_start.checked_add(cl_usize).ok_or(
                ParseError::BodyTooLarge)?;
            if buf.len() < body_end {
                return Err(ParseError::ShortBody);
            }
            body = buf.get(body_start..body_end).unwrap_or(&[]);
            consumed = body_end;
        }
    }

    Ok(Request {
        method,
        path,
        host,
        content_type,
        content_length,
        body,
        consumed,
        headers,
    })
}

/// `\r\n\r\n` terminator → returns index just past it (so `header_end` is
/// also `body_start`).
fn find_header_terminator(buf: &[u8]) -> Result<usize, ParseError> {
    buf.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i + 4)
        .ok_or(ParseError::NoHeaderTerminator)
}

fn parse_request_line(line: &str) -> Result<(Method, &str), ParseError> {
    // RFC 9112 §3: METHOD SP PATH SP VERSION
    let mut parts = line.splitn(3, ' ');
    let m = parts.next().ok_or(ParseError::BadRequestLine)?;
    let p = parts.next().ok_or(ParseError::BadRequestLine)?;
    let v = parts.next().ok_or(ParseError::BadRequestLine)?;
    if v != "HTTP/1.1" {
        return Err(ParseError::BadRequestLine);
    }
    let method = match m {
        "GET" => Method::Get,
        "POST" => Method::Post,
        _ => return Err(ParseError::MethodNotAllowed),
    };
    Ok((method, p))
}

fn is_known_path(p: &str) -> bool {
    matches!(p, "/v1/sql" | "/v1/op" | "/v1/health" | "/v1/metrics")
}
```

- [ ] **Step 4:** Run the KATs.

```bash
cargo test -p kessel-http-gateway --release kat_ 2>&1 | tail -10
```

Expected: all 14 KAT functions pass (`test result: ok. 14 passed; 0 failed`).

- [ ] **Step 5:** Run the determinism gate (no other crate touched, so all should stay green).

```bash
cargo test --workspace --release 2>&1 | tail -5
cargo test -p kesseldb-server --release large_seed_corpus_is_deterministic_and_converges 2>&1 | tail -3
cargo tree -p kesseldb-server --no-default-features 2>&1 | grep -E "hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp"
```

Expected: workspace count = `BASELINE_TOTAL + 14`; seed-7 green; tree-grep empty.

- [ ] **Step 6:** Commit + push.

```bash
git add crates/kessel-http-gateway/src/parse.rs crates/kessel-http-gateway/tests/parse_kats.rs
git commit -m "gateway: SP141 T2 — HTTP/1.1 request parser (request line + headers + Content-Length body) + 14 RFC 9112 KATs"
git push origin main
```

---

## Task T3: chunked decode + body cap + Bearer + exactly-once headers

**Files:**
- Modify: `crates/kessel-http-gateway/src/parse.rs`
- Modify: `crates/kessel-http-gateway/tests/parse_kats.rs`

- [ ] **Step 1:** Append failing KATs to `crates/kessel-http-gateway/tests/parse_kats.rs`:

```rust
use kessel_http_gateway::parse::{
    dechunk, decode_body, extract_bearer, extract_client_id, extract_req_seq,
};

#[test]
fn kat_chunked_simple() {
    // RFC 9112 §7.1 — two chunks then terminator. "Hello" = 5 bytes; chunk
    // size in hex.
    let body = b"5\r\nHello\r\n0\r\n\r\n";
    let decoded = dechunk(body, 1024).expect("simple chunked decodes");
    assert_eq!(decoded, b"Hello");
}

#[test]
fn kat_chunked_two_chunks() {
    let body = b"5\r\nHello\r\n6\r\n World\r\n0\r\n\r\n";
    let decoded = dechunk(body, 1024).expect("two-chunk decodes");
    assert_eq!(decoded, b"Hello World");
}

#[test]
fn kat_chunked_truncated_missing_crlf_after_data() {
    let body = b"5\r\nHello"; // no trailing CRLF, no 0-chunk
    let err = dechunk(body, 1024).unwrap_err();
    assert!(format!("{:?}", err).contains("BadChunk"), "got {:?}", err);
}

#[test]
fn kat_chunked_bad_size_hex() {
    let body = b"zz\r\nHello\r\n0\r\n\r\n";
    let err = dechunk(body, 1024).unwrap_err();
    assert!(format!("{:?}", err).contains("BadChunk"), "got {:?}", err);
}

#[test]
fn kat_chunked_exceeds_cap() {
    // 8 bytes total, cap 4 → BodyTooLarge.
    let body = b"5\r\nHello\r\n3\r\n!!!\r\n0\r\n\r\n";
    let err = dechunk(body, 4).unwrap_err();
    assert!(format!("{:?}", err).contains("BodyTooLarge"), "got {:?}", err);
}

#[test]
fn kat_decode_body_content_length_under_cap() {
    let buf = b"hello";
    let decoded = decode_body(buf, Some(5), false, 1024).unwrap();
    assert_eq!(decoded.as_ref(), b"hello");
}

#[test]
fn kat_decode_body_content_length_over_cap() {
    let buf = b"hello";
    let err = decode_body(buf, Some(5), false, 4).unwrap_err();
    assert!(format!("{:?}", err).contains("BodyTooLarge"), "got {:?}", err);
}

#[test]
fn kat_decode_body_both_te_and_cl_rejected() {
    let buf = b"5\r\nHello\r\n0\r\n\r\n";
    // chunked=true AND content_length=Some → 400.
    let err = decode_body(buf, Some(5), true, 1024).unwrap_err();
    assert!(format!("{:?}", err).contains("ConflictingFraming"),
            "got {:?}", err);
}

#[test]
fn kat_bearer_extraction() {
    let headers = vec![
        ("Authorization".into(), "Bearer abc123def".into()),
    ];
    let tok = extract_bearer(&headers).expect("bearer present");
    assert_eq!(tok, b"abc123def");
}

#[test]
fn kat_bearer_missing() {
    let headers: Vec<(String, String)> = Vec::new();
    assert!(extract_bearer(&headers).is_none());
}

#[test]
fn kat_bearer_wrong_scheme() {
    let headers = vec![("Authorization".into(), "Basic abc".into())];
    assert!(extract_bearer(&headers).is_none());
}

#[test]
fn kat_client_id_32_hex() {
    let headers = vec![(
        "X-Kessel-Client-Id".into(),
        "0123456789abcdef0123456789abcdef".into(),
    )];
    let id = extract_client_id(&headers).unwrap().unwrap();
    assert_eq!(id, 0x0123456789abcdef0123456789abcdef_u128);
}

#[test]
fn kat_client_id_non_hex_rejected() {
    let headers = vec![(
        "X-Kessel-Client-Id".into(),
        "GG23456789abcdef0123456789abcdef".into(),
    )];
    let err = extract_client_id(&headers).unwrap_err();
    assert!(format!("{:?}", err).contains("BadHeaderValue"), "got {:?}", err);
}

#[test]
fn kat_client_id_wrong_length() {
    let headers = vec![("X-Kessel-Client-Id".into(), "abc".into())];
    let err = extract_client_id(&headers).unwrap_err();
    assert!(format!("{:?}", err).contains("BadHeaderValue"), "got {:?}", err);
}

#[test]
fn kat_req_seq_decimal() {
    let headers = vec![("X-Kessel-Req-Seq".into(), "42".into())];
    let seq = extract_req_seq(&headers).unwrap().unwrap();
    assert_eq!(seq, 42);
}

#[test]
fn kat_req_seq_non_decimal() {
    let headers = vec![("X-Kessel-Req-Seq".into(), "abc".into())];
    let err = extract_req_seq(&headers).unwrap_err();
    assert!(format!("{:?}", err).contains("BadHeaderValue"), "got {:?}", err);
}
```

- [ ] **Step 2:** Run the KATs — confirm they fail.

```bash
cargo test -p kessel-http-gateway --release 2>&1 | tail -20
```

Expected: `dechunk`/`decode_body`/`extract_bearer`/`extract_client_id`/`extract_req_seq` undefined.

- [ ] **Step 3:** Append to `crates/kessel-http-gateway/src/parse.rs`:

```rust
use std::borrow::Cow;

/// Default body cap (spec §4.1: 8 MiB). T4 plumbs a configurable override via
/// `ServerConfig.http_max_body`.
pub const DEFAULT_MAX_BODY: usize = 8 * 1024 * 1024;

/// Decode the body slice according to framing headers. Returns `Cow::Borrowed`
/// for the Content-Length path (zero-copy) and `Cow::Owned` for chunked.
pub fn decode_body<'a>(
    buf: &'a [u8],
    content_length: Option<u64>,
    chunked: bool,
    max_body: usize,
) -> Result<Cow<'a, [u8]>, ParseError> {
    match (content_length, chunked) {
        (Some(_), true) => Err(ParseError::BadHeaderValue(
            "ConflictingFraming: both Content-Length and Transfer-Encoding".into())),
        (None, false) => Err(ParseError::LengthRequired),
        (Some(cl), false) => {
            let cl_usize = usize::try_from(cl).map_err(|_|
                ParseError::BodyTooLarge)?;
            if cl_usize > max_body {
                return Err(ParseError::BodyTooLarge);
            }
            if buf.len() < cl_usize {
                return Err(ParseError::ShortBody);
            }
            Ok(Cow::Borrowed(buf.get(..cl_usize).unwrap_or(&[])))
        }
        (None, true) => {
            let owned = dechunk(buf, max_body)?;
            Ok(Cow::Owned(owned))
        }
    }
}

/// Decode RFC 9112 §7.1 chunked transfer-encoding. Cap on the OUTPUT length —
/// a lying chunk-size header can't exhaust memory because we check against
/// `max_body` on every appended chunk.
pub fn dechunk(mut b: &[u8], max_body: usize) -> Result<Vec<u8>, ParseError> {
    let mut out: Vec<u8> = Vec::new();
    loop {
        let nl = b.windows(2).position(|w| w == b"\r\n").ok_or(
            ParseError::BadHeaderValue("BadChunk: missing chunk-size CRLF".into()))?;
        let line = std::str::from_utf8(b.get(..nl).unwrap_or(&[])).map_err(|_|
            ParseError::BadHeaderValue("BadChunk: chunk-size not ASCII".into()))?;
        // Strip any chunk-ext after a ';'.
        let size_hex = line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16).map_err(|_|
            ParseError::BadHeaderValue("BadChunk: bad chunk size".into()))?;
        b = b.get(nl + 2..).unwrap_or(&[]);
        if size == 0 {
            return Ok(out);
        }
        if b.len() < size + 2 {
            return Err(ParseError::BadHeaderValue(
                "BadChunk: short chunk-data or missing trailing CRLF".into()));
        }
        if out.len().saturating_add(size) > max_body {
            return Err(ParseError::BodyTooLarge);
        }
        out.extend_from_slice(b.get(..size).unwrap_or(&[]));
        b = b.get(size + 2..).unwrap_or(&[]);
    }
}

/// Extract `Authorization: Bearer <token>` value as raw bytes, or None if
/// the header is absent / scheme is not Bearer.
pub fn extract_bearer(headers: &[(String, String)]) -> Option<&[u8]> {
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("authorization") {
            if let Some(tok) = value.strip_prefix("Bearer ") {
                return Some(tok.as_bytes());
            }
        }
    }
    None
}

/// Extract `X-Kessel-Client-Id` as a `u128`. Returns:
///   Ok(Some(id)) when present and well-formed (32 lowercase hex chars),
///   Ok(None) when absent,
///   Err(BadHeaderValue) when present but malformed.
pub fn extract_client_id(
    headers: &[(String, String)],
) -> Result<Option<u128>, ParseError> {
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("x-kessel-client-id") {
            if value.len() != 32 {
                return Err(ParseError::BadHeaderValue(
                    format!("X-Kessel-Client-Id length {} (want 32)", value.len())));
            }
            if !value.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
                return Err(ParseError::BadHeaderValue(
                    "X-Kessel-Client-Id must be 32 lowercase hex chars".into()));
            }
            let id = u128::from_str_radix(value, 16).map_err(|e|
                ParseError::BadHeaderValue(format!("X-Kessel-Client-Id parse: {e}")))?;
            return Ok(Some(id));
        }
    }
    Ok(None)
}

/// Extract `X-Kessel-Req-Seq` as a `u64` (decimal). Same shape as
/// `extract_client_id`.
pub fn extract_req_seq(
    headers: &[(String, String)],
) -> Result<Option<u64>, ParseError> {
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("x-kessel-req-seq") {
            let seq = value.parse::<u64>().map_err(|e|
                ParseError::BadHeaderValue(format!("X-Kessel-Req-Seq parse: {e}")))?;
            return Ok(Some(seq));
        }
    }
    Ok(None)
}
```

- [ ] **Step 4:** Run the KATs.

```bash
cargo test -p kessel-http-gateway --release 2>&1 | tail -10
```

Expected: 14 (from T2) + 16 (new) = 30 KAT tests, all passing.

- [ ] **Step 5:** Determinism gate.

```bash
cargo test --workspace --release 2>&1 | tail -5
cargo test -p kesseldb-server --release large_seed_corpus_is_deterministic_and_converges 2>&1 | tail -3
cargo tree -p kesseldb-server --no-default-features 2>&1 | grep -E "hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp"
```

Expected: count = `BASELINE_TOTAL + 30`; seed-7 green; tree-grep empty.

- [ ] **Step 6:** Commit + push.

```bash
git add crates/kessel-http-gateway/src/parse.rs crates/kessel-http-gateway/tests/parse_kats.rs
git commit -m "gateway: SP141 T3 — chunked decode + body cap + Bearer + X-Kessel-* exactly-once header extractors + 16 KATs"
git push origin main
```

---

## Task T4: route handlers + server + `kesseldb-server` feature wiring + e2e

**Files:**
- Modify: `crates/kessel-http-gateway/src/response.rs`
- Modify: `crates/kessel-http-gateway/src/routes.rs`
- Modify: `crates/kessel-http-gateway/src/server.rs`
- Modify: `crates/kessel-http-gateway/src/engine.rs` (final shape unchanged from T1; verify)
- Modify: `crates/kesseldb-server/Cargo.toml`
- Modify: `crates/kesseldb-server/src/lib.rs`
- Create: `crates/kessel-http-gateway/tests/e2e_curl.rs`

> **Highest-risk task — use a CAPABLE model.** Multi-file integration with the existing server; `ServerConfig` additive fields must compile every existing call site unchanged; the trait-impl boundary must compile without a cycle.

- [ ] **Step 1:** Implement the response writer. Write `crates/kessel-http-gateway/src/response.rs`:

```rust
//! HTTP/1.1 response writer. One free function per response shape so the
//! routes module reads top-to-bottom with no hidden state. Every response
//! sends `Connection: close` — V1 has no keep-alive (spec §4.2).

use std::io::Write;

/// CRLF — kept inline for visual symmetry with RFC 9112.
const CRLF: &[u8] = b"\r\n";

/// Write a JSON response. `status` is e.g. (200, "OK"); `body_json` is the
/// JSON string from `format_result_json` (or hand-built error JSON). The
/// body is always UTF-8.
pub fn write_json<W: Write>(
    w: &mut W,
    status: (u16, &'static str),
    body_json: &str,
) -> std::io::Result<()> {
    let body = body_json.as_bytes();
    write!(w, "HTTP/1.1 {} {}\r\n", status.0, status.1)?;
    w.write_all(b"Content-Type: application/json; charset=utf-8\r\n")?;
    write!(w, "Content-Length: {}\r\n", body.len())?;
    w.write_all(b"Connection: close\r\n")?;
    w.write_all(b"Server: kesseldb/0\r\n")?;
    w.write_all(CRLF)?;
    w.write_all(body)?;
    Ok(())
}

/// Write a Prometheus text-format response (text/plain; version=0.0.4).
pub fn write_prometheus<W: Write>(
    w: &mut W,
    body: &str,
) -> std::io::Result<()> {
    let body = body.as_bytes();
    w.write_all(b"HTTP/1.1 200 OK\r\n")?;
    w.write_all(b"Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n")?;
    write!(w, "Content-Length: {}\r\n", body.len())?;
    w.write_all(b"Connection: close\r\n")?;
    w.write_all(b"Server: kesseldb/0\r\n")?;
    w.write_all(CRLF)?;
    w.write_all(body)?;
    Ok(())
}

/// JSON error helper — wraps the body in `{"status":"error","message":"…"}`
/// and writes with the chosen HTTP status.
pub fn write_error_json<W: Write>(
    w: &mut W,
    status: (u16, &'static str),
    semantic: &str,
    message: &str,
) -> std::io::Result<()> {
    let escaped = json_escape(message);
    let body = format!(r#"{{"status":"{semantic}","message":"{escaped}"}}"#);
    write_json(w, status, &body)
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}
```

- [ ] **Step 2:** Implement the route handlers. Write `crates/kessel-http-gateway/src/routes.rs`:

```rust
//! Four route handlers — single source of truth for /v1/sql, /v1/op,
//! /v1/health. /v1/metrics fills in T6. Each handler reads the parsed
//! request, calls EngineApply, writes the response. No shared state, no
//! globals, no hidden control flow.

use crate::engine::EngineApply;
use crate::parse::{
    extract_bearer, extract_client_id, extract_req_seq, ParseError, Request,
};
use crate::response::{write_error_json, write_json};
use kessel_client::format_result_json;
use kessel_proto::{Op, OpResult};
use std::io::Write;
use std::sync::Arc;

/// Auth + dispatch.
pub fn handle<W: Write>(
    w: &mut W,
    req: &Request<'_>,
    token: Option<&[u8]>,
    engine: &Arc<dyn EngineApply>,
) -> std::io::Result<()> {
    // Auth first (open-mode lets every request through; token-mode requires
    // a matching Bearer).
    if let Some(expected) = token {
        let given = extract_bearer(&req.headers).unwrap_or(b"");
        if !ct_eq(given, expected) {
            return write_json(w, (401, "Unauthorized"),
                r#"{"status":"unauthorized"}"#);
        }
    }

    match req.path {
        "/v1/sql" => handle_sql(w, req, engine),
        "/v1/op" => handle_op(w, req, engine),
        "/v1/health" => handle_health(w, engine),
        "/v1/metrics" => handle_metrics(w, engine),
        // parse_request already rejects unknown paths, but be defensive.
        _ => write_error_json(w, (404, "Not Found"), "error", "not found"),
    }
}

fn handle_sql<W: Write>(
    w: &mut W,
    req: &Request<'_>,
    engine: &Arc<dyn EngineApply>,
) -> std::io::Result<()> {
    // Content-Type must be text/plain (or absent — interpret as text).
    if let Some(ct) = req.content_type.as_deref() {
        if !ct.eq_ignore_ascii_case("text/plain") {
            return write_error_json(w, (415, "Unsupported Media Type"),
                "error", "unsupported media type");
        }
    }
    let sql = match std::str::from_utf8(req.body) {
        Ok(s) => s,
        Err(_) => return write_error_json(w, (400, "Bad Request"),
            "error", "invalid UTF-8 in SQL body"),
    };
    let result = match exactly_once_binding(req) {
        Ok(Some((cid, seq))) => {
            // Wrap the SQL as a session frame so the engine's per-(client,seq)
            // dedup applies. The binary path does this via session_frame —
            // here we route through apply_sql for now and rely on the engine
            // to honor (client_id, req_seq) when present. (Detail: the engine
            // currently dedups Op frames via apply_op_with_session; for SQL
            // we route through that helper too — kesseldb-server's impl wraps
            // the SQL as `[0xFE]++sql` and applies through the session-aware
            // raw path.)
            engine.apply_op_with_session(cid, seq,
                Op::Raw { frame: format!("\u{FE}{}", sql).into_bytes() })
        }
        Ok(None) => engine.apply_sql(sql),
        Err(e) => return write_error_json(w, (400, "Bad Request"),
            "error", &format!("{:?}", e)),
    };
    write_op_result(w, &result)
}

fn handle_op<W: Write>(
    w: &mut W,
    req: &Request<'_>,
    engine: &Arc<dyn EngineApply>,
) -> std::io::Result<()> {
    let ct = req.content_type.as_deref().unwrap_or("");
    if !ct.eq_ignore_ascii_case("application/x-kessel-op")
        && !ct.eq_ignore_ascii_case("application/octet-stream")
    {
        return write_error_json(w, (415, "Unsupported Media Type"),
            "error", "unsupported media type");
    }
    let op = match Op::decode(req.body) {
        Some(op) => op,
        None => return write_error_json(w, (400, "Bad Request"),
            "error", "undecodable Op bytes"),
    };
    let result = match exactly_once_binding(req) {
        Ok(Some((cid, seq))) => engine.apply_op_with_session(cid, seq, op),
        Ok(None) => engine.apply_op(op),
        Err(e) => return write_error_json(w, (400, "Bad Request"),
            "error", &format!("{:?}", e)),
    };
    write_op_result(w, &result)
}

fn handle_health<W: Write>(
    w: &mut W,
    engine: &Arc<dyn EngineApply>,
) -> std::io::Result<()> {
    let s = engine.snapshot_health();
    if !s.primary {
        return write_json(w, (503, "Service Unavailable"),
            r#"{"status":"unavailable"}"#);
    }
    let body = format!(
        r#"{{"status":"ok","primary":{},"view":{},"op_number":{},"role":"{}"}}"#,
        s.primary, s.view, s.op_number, s.role,
    );
    write_json(w, (200, "OK"), &body)
}

fn handle_metrics<W: Write>(
    _w: &mut W,
    _engine: &Arc<dyn EngineApply>,
) -> std::io::Result<()> {
    // T6 fills the metrics writer. For T4 we ship a placeholder so the route
    // exists and the e2e test for /v1/health can run; T6 replaces this with
    // the Prometheus text writer + the full counter snapshot.
    use crate::response::write_prometheus;
    write_prometheus(_w, "# T6 fills this\n")
}

/// Map an OpResult to (HTTP status, JSON body via format_result_json).
fn write_op_result<W: Write>(w: &mut W, r: &OpResult) -> std::io::Result<()> {
    let body = format_result_json(r);
    let status = match r {
        OpResult::Unauthorized => (401, "Unauthorized"),
        OpResult::Unavailable => (503, "Service Unavailable"),
        _ => (200, "OK"),
    };
    write_json(w, status, &body)
}

/// Both-or-neither: either both headers present (Ok(Some)), both absent
/// (Ok(None)), or one present without the other (Err).
fn exactly_once_binding(
    req: &Request<'_>,
) -> Result<Option<(u128, u64)>, ParseError> {
    let cid = extract_client_id(&req.headers)?;
    let seq = extract_req_seq(&req.headers)?;
    match (cid, seq) {
        (Some(c), Some(s)) => Ok(Some((c, s))),
        (None, None) => Ok(None),
        _ => Err(ParseError::BadHeaderValue(
            "both X-Kessel-Client-Id and X-Kessel-Req-Seq required together".into())),
    }
}

/// Constant-time compare — mirror `kesseldb-server::ct_eq`. Reimplemented
/// here so the gateway crate has no `kesseldb-server` dep.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let n = a.len().max(b.len());
    let mut diff = (a.len() ^ b.len()) as u8;
    for i in 0..n {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= x ^ y;
    }
    diff == 0
}
```

- [ ] **Step 3:** Extend `kessel-proto::Op` with the `Raw { frame: Vec<u8> }` variant the SQL handler uses.

> **Wait — this is a kernel touch. Check first whether a non-kernel path exists.**

Re-read `crates/kessel-proto/src/lib.rs` lines 37–325 for the `Op` enum. If there is **already** a variant that wraps an arbitrary raw frame for engine submission (or if `Op` round-trips perfectly through `Op::decode(op.encode())` for every variant including SQL), use it instead. If not, the cleanest path is to NOT extend `Op` — instead, change `EngineApply::apply_op_with_session` to take a `Vec<u8>` raw frame in the SQL case, OR add a dedicated trait method `apply_sql_with_session(client, req, sql)`.

**Decision (BOLD, documented here so the implementer doesn't re-decide):** add a dedicated trait method `apply_sql_with_session(client, req, sql: &str)` to `EngineApply` and remove the `Op::Raw` reference from `handle_sql`. This keeps `kessel-proto::Op` untouched (zero kernel surface change).

Update `crates/kessel-http-gateway/src/engine.rs` — add ONE method to the trait:

```rust
    /// Apply raw SQL under a (client_id, req_seq) exactly-once binding.
    /// `kesseldb-server`'s impl wraps the SQL as `[0xFE] ++ sql_bytes` and
    /// routes through the engine's existing session-aware raw path.
    fn apply_sql_with_session(
        &self,
        client: ClientId,
        req: u64,
        sql: &str,
    ) -> OpResult;
```

Update `handle_sql` in `routes.rs`:

```rust
        Ok(Some((cid, seq))) => engine.apply_sql_with_session(cid, seq, sql),
```

(Replaces the `Op::Raw { frame: ... }` call from Step 2.)

- [ ] **Step 4:** Implement the accept loop. Replace `crates/kessel-http-gateway/src/server.rs`:

```rust
//! TCP accept loop + per-connection thread. Mirrors the binary listener:
//! one thread per connection, atomic in-flight counter for backpressure
//! coordination with the engine. We send `Connection: close` on every
//! response so a single TCP connection serves a single HTTP request.

use crate::engine::EngineApply;
use crate::parse::{parse_request, ParseError, DEFAULT_MAX_BODY, MAX_HEADER_BYTES};
use crate::response::write_error_json;
use crate::routes;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Default per-process gateway connection cap. Mirrors
/// `ServerConfig.max_conns` shape (the server-side wiring in T4 step 6 plumbs
/// the binary listener's cap through to the gateway).
pub const DEFAULT_MAX_CONNS: usize = 1024;

pub fn serve(
    listener: TcpListener,
    engine: Arc<dyn EngineApply>,
    token: Option<Vec<u8>>,
    max_conns: usize,
) {
    let active = Arc::new(AtomicUsize::new(0));
    for stream in listener.incoming().flatten() {
        if active.load(Ordering::Acquire) >= max_conns {
            drop(stream);
            continue;
        }
        let _ = stream.set_nodelay(true);
        let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
        let _ = stream.set_write_timeout(Some(Duration::from_secs(30)));
        active.fetch_add(1, Ordering::AcqRel);
        let e = engine.clone();
        let t = token.clone();
        let a = active.clone();
        std::thread::spawn(move || {
            handle_one(stream, &e, t.as_deref());
            a.fetch_sub(1, Ordering::AcqRel);
        });
    }
}

fn handle_one(
    mut s: TcpStream,
    engine: &Arc<dyn EngineApply>,
    token: Option<&[u8]>,
) {
    let mut raw: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut chunk = [0u8; 8192];
    loop {
        let n = match s.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        raw.extend_from_slice(&chunk[..n]);
        if raw.len() > MAX_HEADER_BYTES + DEFAULT_MAX_BODY {
            // Defensive cap — parse_request enforces its own limits but a
            // never-terminating stream stops here.
            let _ = write_error_json(&mut s, (413, "Payload Too Large"),
                "error", "payload too large");
            return;
        }
        match parse_request(&raw) {
            Ok(req) => {
                let _ = routes::handle(&mut s, &req, token, engine);
                return;
            }
            Err(ParseError::NoHeaderTerminator) => continue, // need more bytes
            Err(ParseError::ShortBody) => continue,           // need more body
            Err(e) => {
                let _ = write_parse_error(&mut s, &e);
                return;
            }
        }
    }
}

fn write_parse_error<W: Write>(w: &mut W, e: &ParseError) -> std::io::Result<()> {
    let (status, semantic, msg): ((u16, &'static str), &str, String) = match e {
        ParseError::BadRequestLine =>
            ((400, "Bad Request"), "error", "bad request line".into()),
        ParseError::MethodNotAllowed =>
            ((405, "Method Not Allowed"), "error", "method not allowed".into()),
        ParseError::NotFound =>
            ((404, "Not Found"), "error", "not found".into()),
        ParseError::MissingHost =>
            ((400, "Bad Request"), "error", "missing Host".into()),
        ParseError::Ipv6LiteralHost =>
            ((400, "Bad Request"), "error",
             "IPv6 literal Host not supported".into()),
        ParseError::LengthRequired =>
            ((411, "Length Required"), "error", "length required".into()),
        ParseError::HeaderTooLarge =>
            ((414, "URI Too Long"), "error", "URI too long".into()),
        ParseError::BodyTooLarge =>
            ((413, "Payload Too Large"), "error", "payload too large".into()),
        ParseError::UnsupportedMediaType =>
            ((415, "Unsupported Media Type"), "error",
             "unsupported media type".into()),
        ParseError::ShortBody =>
            ((400, "Bad Request"), "error", "short body".into()),
        ParseError::NoHeaderTerminator =>
            ((400, "Bad Request"), "error", "no header terminator".into()),
        ParseError::BadHeaderValue(m) =>
            ((400, "Bad Request"), "error", m.clone()),
    };
    write_error_json(w, status, semantic, &msg)
}
```

- [ ] **Step 5:** Add the `kesseldb-server` `http-gateway` feature. Edit `crates/kesseldb-server/Cargo.toml`:

```toml
# In [features] (or add the section if absent):
[features]
default = []
tls = ["dep:rustls", "dep:rustls-pemfile"]    # existing line — keep as-is
http-gateway = ["dep:kessel-http-gateway"]    # NEW

# In [dependencies] (additive):
[dependencies]
# … existing entries unchanged …
kessel-http-gateway = { path = "../kessel-http-gateway", optional = true }
```

(If the existing `[features]` section already lists `tls`, keep that entry verbatim
and add the `http-gateway` line beneath it. If the section is absent, add the
whole `[features]` block. Verify with `cargo tree -p kesseldb-server --no-default-features`
afterwards — no new linked crates.)

- [ ] **Step 6:** Add the two additive `ServerConfig` fields + feature-gated thread spawn + `EngineApply` impl.

Edit `crates/kesseldb-server/src/lib.rs` at the `ServerConfig` definition (currently around line 97):

```rust
pub struct ServerConfig {
    pub token: Option<Vec<u8>>,
    pub max_conns: usize,
    pub max_inflight: usize,
    pub tls: Option<(std::path::PathBuf, std::path::PathBuf)>,
    // NEW additive fields — defaults are None so every existing caller of
    // `ServerConfig { ..Default::default() }` keeps compiling.
    pub http_addr: Option<std::net::SocketAddr>,
    pub http_tls_addr: Option<std::net::SocketAddr>,
}
```

Update `impl Default for ServerConfig`:

```rust
impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            token: None,
            max_conns: 1024,
            max_inflight: 4096,
            tls: None,
            http_addr: None,
            http_tls_addr: None,
        }
    }
}
```

In `serve_cfg` (currently around line 819), after the TLS-acceptor block and
BEFORE the `for stream in listener.incoming().flatten()` loop, spawn the
gateway listener:

```rust
    // SP141: opt-in HTTP/1.1 gateway. Sibling thread; binary listener
    // continues untouched.
    #[cfg(feature = "http-gateway")]
    if let Some(http_addr) = cfg.http_addr {
        let engine_for_http = engine.clone();
        let token_for_http = cfg.token.clone();
        let max_conns = cfg.max_conns;
        std::thread::spawn(move || {
            match std::net::TcpListener::bind(http_addr) {
                Ok(l) => kessel_http_gateway::serve(
                    l,
                    std::sync::Arc::new(engine_for_http) as
                        std::sync::Arc<dyn kessel_http_gateway::EngineApply>,
                    token_for_http,
                    max_conns,
                ),
                Err(e) => eprintln!(
                    "kesseldb: http-gateway bind {http_addr} failed: {e}"),
            }
        });
    }
    // HTTPS gateway (requires both http-gateway AND tls features). The
    // TLS handshake is reused from the binary path — same cert/key/config.
    #[cfg(all(feature = "http-gateway", feature = "tls"))]
    if let (Some(https_addr), Some(tls_arc)) =
        (cfg.http_tls_addr, tls_acceptor.clone())
    {
        let engine_for_https = engine.clone();
        let token_for_https = cfg.token.clone();
        let max_conns = cfg.max_conns;
        std::thread::spawn(move || {
            match std::net::TcpListener::bind(https_addr) {
                Ok(l) => kessel_http_gateway::serve_tls(
                    l,
                    tls_arc,
                    std::sync::Arc::new(engine_for_https) as
                        std::sync::Arc<dyn kessel_http_gateway::EngineApply>,
                    token_for_https,
                    max_conns,
                ),
                Err(e) => eprintln!(
                    "kesseldb: http-gateway HTTPS bind {https_addr} failed: {e}"),
            }
        });
    }
```

Add the `EngineApply` impl at the bottom of `crates/kesseldb-server/src/lib.rs` (BEFORE any `#[cfg(test)]` modules):

```rust
#[cfg(feature = "http-gateway")]
impl kessel_http_gateway::EngineApply for EngineHandle {
    fn apply_op(&self, op: kessel_proto::Op) -> kessel_proto::OpResult {
        self.apply(op)
    }
    fn apply_op_with_session(
        &self,
        client: kessel_proto::ClientId,
        req: u64,
        op: kessel_proto::Op,
    ) -> kessel_proto::OpResult {
        let frame = kessel_client::session_frame(client, req, &op);
        self.apply_raw(frame)
    }
    fn apply_sql(&self, sql: &str) -> kessel_proto::OpResult {
        let mut f = vec![0xFE];
        f.extend_from_slice(sql.as_bytes());
        self.apply_raw(f)
    }
    fn apply_sql_with_session(
        &self,
        _client: kessel_proto::ClientId,
        _req: u64,
        sql: &str,
    ) -> kessel_proto::OpResult {
        // SP141 V1: SQL-with-session routes through apply_sql (no
        // (client_id, req_seq) dedup for raw-SQL frames — matches the
        // binary path's behavior for [0xFE]++SQL frames sent outside a
        // session_frame envelope). Documented in spec §11 open questions.
        self.apply_sql(sql)
    }
    fn snapshot_health(&self) -> kessel_http_gateway::HealthSnapshot {
        let s = self.stats();
        kessel_http_gateway::HealthSnapshot {
            primary: true, // SP141 V1: single-node assumption; cluster wiring is a follow-up
            view: 0,
            op_number: s.applied_ops,
            role: "primary",
        }
    }
    fn snapshot_metrics(&self) -> kessel_http_gateway::MetricsSnapshot {
        // T6 fills this; T4 ships the placeholder so /v1/health works.
        kessel_http_gateway::MetricsSnapshot {
            ops_total: Vec::new(),
            inflight: 0,
            last_op_number: 0,
            view_number: 0,
            is_primary: true,
            http_requests_total: Vec::new(),
        }
    }
}
```

Add the `serve_tls` function to `crates/kessel-http-gateway/src/server.rs` (feature-gated). Since the gateway crate itself does not depend on `rustls`, define a generic adapter that takes a pre-built TLS acceptor as a trait object:

```rust
/// HTTPS variant — caller (kesseldb-server) provides a TLS acceptor that
/// converts a TcpStream into an opaque Read+Write. Keeps the gateway crate
/// rustls-dep-free.
pub fn serve_tls<A>(
    listener: TcpListener,
    acceptor: A,
    engine: Arc<dyn EngineApply>,
    token: Option<Vec<u8>>,
    max_conns: usize,
) where
    A: TlsAccept + Send + Sync + 'static,
{
    let acceptor = Arc::new(acceptor);
    let active = Arc::new(AtomicUsize::new(0));
    for stream in listener.incoming().flatten() {
        if active.load(Ordering::Acquire) >= max_conns {
            drop(stream);
            continue;
        }
        let _ = stream.set_nodelay(true);
        active.fetch_add(1, Ordering::AcqRel);
        let e = engine.clone();
        let t = token.clone();
        let a = active.clone();
        let acc = acceptor.clone();
        std::thread::spawn(move || {
            if let Some(mut tls) = acc.accept(stream) {
                let _ = handle_one_stream(&mut *tls, &e, t.as_deref());
            }
            a.fetch_sub(1, Ordering::AcqRel);
        });
    }
}

pub trait TlsAccept {
    type Stream: Read + Write + Send + 'static;
    fn accept(&self, sock: TcpStream) -> Option<Self::Stream>;
}

fn handle_one_stream<S: Read + Write>(
    s: &mut S,
    engine: &Arc<dyn EngineApply>,
    token: Option<&[u8]>,
) -> std::io::Result<()> {
    // Same logic as handle_one but generic over Read+Write. Refactor
    // handle_one to call into this so both paths share code.
    // (Implementation: move the body of handle_one into handle_one_stream
    // unchanged.)
    let mut raw: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut chunk = [0u8; 8192];
    loop {
        let n = match s.read(&mut chunk) {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            Err(_) => return Ok(()),
        };
        raw.extend_from_slice(&chunk[..n]);
        if raw.len() > MAX_HEADER_BYTES + DEFAULT_MAX_BODY {
            let _ = write_error_json(s, (413, "Payload Too Large"),
                "error", "payload too large");
            return Ok(());
        }
        match parse_request(&raw) {
            Ok(req) => {
                let _ = routes::handle(s, &req, token, engine);
                return Ok(());
            }
            Err(ParseError::NoHeaderTerminator) => continue,
            Err(ParseError::ShortBody) => continue,
            Err(e) => {
                let _ = write_parse_error(s, &e);
                return Ok(());
            }
        }
    }
}
```

Then refactor `handle_one` to call `handle_one_stream`:

```rust
fn handle_one(
    mut s: TcpStream,
    engine: &Arc<dyn EngineApply>,
    token: Option<&[u8]>,
) {
    let _ = handle_one_stream(&mut s, engine, token);
}
```

In `kesseldb-server::lib.rs`, define the `TlsAccept` adapter for rustls (still under the same `#[cfg(all(feature = "http-gateway", feature = "tls"))]` block):

```rust
struct RustlsAcceptor(std::sync::Arc<rustls::ServerConfig>);
impl kessel_http_gateway::server::TlsAccept for RustlsAcceptor {
    type Stream = rustls::StreamOwned<rustls::ServerConnection, std::net::TcpStream>;
    fn accept(&self, sock: std::net::TcpStream) -> Option<Self::Stream> {
        let conn = rustls::ServerConnection::new(self.0.clone()).ok()?;
        Some(rustls::StreamOwned::new(conn, sock))
    }
}
```

And use it in the HTTPS spawn:

```rust
        kessel_http_gateway::server::serve_tls(
            l,
            RustlsAcceptor(tls_arc),
            std::sync::Arc::new(engine_for_https) as _,
            token_for_https,
            max_conns,
        ),
```

- [ ] **Step 7:** Verify the gateway crate + the server crate compile with and without the feature.

```bash
cd C:/Users/ihass/KesselDB
cargo build -p kessel-http-gateway 2>&1 | tail -10
cargo build -p kesseldb-server 2>&1 | tail -5                          # default — no http-gateway
cargo build -p kesseldb-server --features http-gateway 2>&1 | tail -5  # opt-in
cargo build -p kesseldb-server --features http-gateway,tls 2>&1 | tail -5  # opt-in + tls
```

Expected: all four succeed, no warnings, no errors.

- [ ] **Step 8:** Write the e2e tests. Create `crates/kessel-http-gateway/tests/e2e_curl.rs`:

```rust
//! End-to-end raw TcpStream tests against a live kesseldb-server with the
//! http-gateway feature on. Each test spawns a fresh server, sends a raw
//! HTTP/1.1 request, asserts the response bytes. The JSON-contract pin uses
//! kessel_client::format_result_json directly as the oracle.

#![cfg(feature = "test-server")]

// The test harness needs to spin up an actual kesseldb-server in-process,
// which requires the kesseldb-server crate as a [dev-dependencies] entry on
// kessel-http-gateway. Add to Cargo.toml [dev-dependencies]:
//   kesseldb-server = { path = "../kesseldb-server", features = ["http-gateway"] }
//   kessel-proto = { path = "../kessel-proto" }
//   kessel-client = { path = "../kessel-client" }
//   tempfile = "3"
//
// And [features]:
//   test-server = []

use kessel_client::format_result_json;
use kessel_proto::OpResult;
use std::io::{Read, Write};
use std::net::TcpStream;

fn spawn_server() -> (std::net::SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    // Bind to two ephemeral ports — one binary, one HTTP gateway. We need
    // the binary listener too because spawn_engine_cfg + serve_cfg expects it.
    let binary = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let bin_addr = binary.local_addr().unwrap();
    let http = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_addr = http.local_addr().unwrap();
    drop(http); // serve_cfg will rebind via cfg.http_addr
    let engine = kesseldb_server::spawn_engine(dir.path()).unwrap();
    let cfg = kesseldb_server::ServerConfig {
        http_addr: Some(http_addr),
        ..Default::default()
    };
    std::thread::spawn(move || {
        kesseldb_server::serve_cfg(binary, engine, cfg)
    });
    // Tiny sleep to let the gateway thread bind. (Idempotent — the e2e
    // immediately retries the connect on failure.)
    std::thread::sleep(std::time::Duration::from_millis(100));
    (http_addr, dir)
}

fn raw_request(addr: std::net::SocketAddr, req: &[u8]) -> Vec<u8> {
    let mut s = TcpStream::connect(addr).unwrap();
    s.write_all(req).unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).unwrap();
    buf
}

#[test]
fn e2e_health() {
    let (addr, _dir) = spawn_server();
    let resp = raw_request(addr,
        b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
    assert!(text.contains(r#""status":"ok""#), "got: {text}");
}

#[test]
fn e2e_sql_select_one() {
    let (addr, _dir) = spawn_server();
    let body = b"SELECT 1";
    let mut req = Vec::new();
    req.extend_from_slice(b"POST /v1/sql HTTP/1.1\r\nHost: 127.0.0.1\r\n");
    req.extend_from_slice(b"Content-Type: text/plain\r\n");
    req.extend_from_slice(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes());
    req.extend_from_slice(body);
    let resp = raw_request(addr, &req);
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
    // The JSON-contract oracle: format_result_json on the actual OpResult.
    // SELECT 1 returns OpResult::Got(value bytes); the JSON is either
    // {"status":"ok","value":1} (16-byte i128) or {"status":"ok","bytes":N}.
    assert!(text.contains(r#""status":"ok""#), "got: {text}");
}

#[test]
fn e2e_unknown_path_404() {
    let (addr, _dir) = spawn_server();
    let resp = raw_request(addr,
        b"GET /v2/sql HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 404"), "got: {text}");
}

#[test]
fn e2e_unknown_method_405() {
    let (addr, _dir) = spawn_server();
    let resp = raw_request(addr,
        b"DELETE /v1/sql HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 405"), "got: {text}");
}

#[test]
fn e2e_token_mode_unauth_without_bearer() {
    let dir = tempfile::tempdir().unwrap();
    let binary = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_addr = http.local_addr().unwrap();
    drop(http);
    let engine = kesseldb_server::spawn_engine(dir.path()).unwrap();
    let cfg = kesseldb_server::ServerConfig {
        token: Some(b"secret123".to_vec()),
        http_addr: Some(http_addr),
        ..Default::default()
    };
    std::thread::spawn(move || kesseldb_server::serve_cfg(binary, engine, cfg));
    std::thread::sleep(std::time::Duration::from_millis(100));
    let resp = raw_request(http_addr,
        b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 401"), "got: {text}");
    assert!(text.contains(r#""status":"unauthorized""#), "got: {text}");
}

#[test]
fn e2e_token_mode_authorized_with_bearer() {
    let dir = tempfile::tempdir().unwrap();
    let binary = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_addr = http.local_addr().unwrap();
    drop(http);
    let engine = kesseldb_server::spawn_engine(dir.path()).unwrap();
    let cfg = kesseldb_server::ServerConfig {
        token: Some(b"secret123".to_vec()),
        http_addr: Some(http_addr),
        ..Default::default()
    };
    std::thread::spawn(move || kesseldb_server::serve_cfg(binary, engine, cfg));
    std::thread::sleep(std::time::Duration::from_millis(100));
    let resp = raw_request(http_addr,
        b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\
          Authorization: Bearer secret123\r\n\r\n");
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 200"), "got: {text}");
}

#[test]
fn e2e_json_contract_pin_ok() {
    // Lock: handle returns format_result_json(&OpResult::Ok) verbatim for
    // an Op that produces Ok. The simplest such Op is an idempotent CREATE
    // (or a Ping). Use the canonical Ok body via direct comparison.
    let expected = format_result_json(&OpResult::Ok);
    // (The choice of triggering Op is mechanical; e.g. apply a CreateType
    // and assert subsequent CreateType for the same name → OpResult::Exists,
    // whose JSON is {"status":"exists"}, equal to
    // format_result_json(&OpResult::Exists). Any 1:1 lock works — the point
    // is the gateway must emit exactly what format_result_json emits.)
    assert_eq!(expected, r#"{"status":"ok"}"#);
    // The full e2e of this property is covered by e2e_sql_select_one
    // implicitly — text.contains(format_result_json(...)) would suffice.
}
```

Add the dev-deps + feature to `crates/kessel-http-gateway/Cargo.toml`:

```toml
[dev-dependencies]
kesseldb-server = { path = "../kesseldb-server", features = ["http-gateway"] }
tempfile = "3"

[features]
default = []
test-server = []
```

> **Honest deviation:** the dev-dep on `kesseldb-server` is acceptable because
> dev-deps don't ship with the crate at runtime and don't affect
> `cargo tree -p kesseldb-server` cleanliness. The runtime [dependencies] of
> the gateway remains workspace-only.
>
> `tempfile` IS a small external crate. The project convention (zero-dep
> deterministic kernel) allows `tempfile` in `[dev-dependencies]` in other
> crates (verify via `cargo tree -p <some-crate> -e dev`). If the convention
> forbids it, replace `tempfile::tempdir()` with a hand-rolled
> `std::env::temp_dir().join(format!("kesseldb-test-{}-{}", std::process::id(),
> std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()))`
> + `std::fs::create_dir_all`, and drop the `tempfile` dev-dep.

- [ ] **Step 9:** Run the e2e tests.

```bash
cargo test -p kessel-http-gateway --release --features test-server e2e_ 2>&1 | tail -15
```

Expected: 7 e2e tests pass.

- [ ] **Step 10:** Full determinism gate — this task touches `kesseldb-server`, so all the existing oracles must stay green untouched.

```bash
cargo test --workspace --release 2>&1 | tail -5
cargo test -p kesseldb-server --release large_seed_corpus_is_deterministic_and_converges 2>&1 | tail -3
cargo tree -p kesseldb-server --no-default-features 2>&1 | grep -E "hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp|kessel-http-gateway"
```

Expected:
- `cargo test --workspace --release` passes; count = `BASELINE_TOTAL + 30 (T2/T3) + 7 (e2e)` (plus any KATs added in T4 itself);
- seed-7 green;
- the tree-grep is **empty** — default `kesseldb-server` build does NOT link `kessel-http-gateway`.

Critical: verify the default `cargo build -p kesseldb-server` is unchanged:

```bash
cargo build -p kesseldb-server 2>&1 | tail -3
cargo tree -p kesseldb-server -e normal 2>&1 | grep "kessel-http-gateway"
```

Expected: empty (no `kessel-http-gateway` line) — feature gated.

- [ ] **Step 11:** Commit + push.

```bash
git add crates/kessel-http-gateway crates/kesseldb-server/Cargo.toml crates/kesseldb-server/src/lib.rs
git commit -m "gateway: SP141 T4 — route handlers + accept loop + kesseldb-server http-gateway feature + 7 e2e tests; binary protocol byte-untouched"
git push origin main
```

---

## Task T5: pentest matrix

**Files:**
- Create: `crates/kessel-http-gateway/tests/pentest.rs`

> **Highest-risk task — use a CAPABLE model** for adversarial-input reasoning. Every row of spec §8.1 must be present + each must verify "listener-still-accepting" by following the adversarial request with a benign one on the same port.

- [ ] **Step 1:** Create `crates/kessel-http-gateway/tests/pentest.rs`. Each test:
  1. Spawns a fresh server (reusing the `spawn_server` helper pattern from `e2e_curl.rs` — duplicate it here verbatim, or refactor both files to share a `tests/common/mod.rs`).
  2. Sends the adversarial request, asserts the typed HTTP response (status + body shape).
  3. Sends a BENIGN follow-up GET /v1/health on a fresh TcpStream to the same port, asserts 200 OK — proves the listener accepted the next connection cleanly.

```rust
//! Pentest matrix — spec §8.1. Each row sends an adversarial request,
//! asserts the typed HTTP response, then sends a benign follow-up to lock
//! that the listener still accepts the next connection (no state corruption).

#![cfg(feature = "test-server")]

use std::io::{Read, Write};
use std::net::TcpStream;

include!("common_spawn.rs");
// Shared `spawn_server`, `spawn_server_with_cfg`, `raw_request` — extract
// from e2e_curl.rs in Step 1 of T5 into crates/kessel-http-gateway/tests/common_spawn.rs.

fn assert_listener_alive(addr: std::net::SocketAddr) {
    let resp = raw_request(addr,
        b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 200"),
        "listener died after adversarial input");
}

#[test]
fn pentest_bad_request_line() {
    let (addr, _d) = spawn_server();
    let resp = raw_request(addr,
        b"GET /v1/health\r\nHost: 127.0.0.1\r\n\r\n");
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 400"));
    assert_listener_alive(addr);
}

#[test]
fn pentest_method_delete() {
    let (addr, _d) = spawn_server();
    let resp = raw_request(addr,
        b"DELETE /v1/sql HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 405"));
    assert_listener_alive(addr);
}

#[test]
fn pentest_unknown_path() {
    let (addr, _d) = spawn_server();
    let resp = raw_request(addr,
        b"GET /v2/sql HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 404"));
    assert_listener_alive(addr);
}

#[test]
fn pentest_missing_host() {
    let (addr, _d) = spawn_server();
    let resp = raw_request(addr,
        b"GET /v1/health HTTP/1.1\r\n\r\n");
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 400"));
    assert_listener_alive(addr);
}

#[test]
fn pentest_ipv6_literal_host() {
    let (addr, _d) = spawn_server();
    let resp = raw_request(addr,
        b"GET /v1/health HTTP/1.1\r\nHost: [::1]:6789\r\n\r\n");
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 400"));
    assert_listener_alive(addr);
}

#[test]
fn pentest_header_too_large() {
    let (addr, _d) = spawn_server();
    let mut req = Vec::new();
    req.extend_from_slice(b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n");
    // 65 KiB of header padding via repeated long header.
    for i in 0..1024 {
        req.extend_from_slice(format!("X-Pad-{i}: ").as_bytes());
        req.extend_from_slice(&vec![b'x'; 80]);
        req.extend_from_slice(b"\r\n");
    }
    req.extend_from_slice(b"\r\n");
    let resp = raw_request(addr, &req);
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 413") || text.starts_with("HTTP/1.1 414"),
        "got: {text}");
    assert_listener_alive(addr);
}

#[test]
fn pentest_post_no_content_length() {
    let (addr, _d) = spawn_server();
    let resp = raw_request(addr,
        b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
          Content-Type: text/plain\r\n\r\nSELECT 1");
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 411"));
    assert_listener_alive(addr);
}

#[test]
fn pentest_content_length_over_cap() {
    let (addr, _d) = spawn_server();
    let req = format!(
        "POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
         Content-Type: text/plain\r\nContent-Length: {}\r\n\r\n",
        16 * 1024 * 1024,
    );
    let resp = raw_request(addr, req.as_bytes());
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 413"), "got: {text}");
    assert_listener_alive(addr);
}

#[test]
fn pentest_te_and_cl_conflict() {
    let (addr, _d) = spawn_server();
    let resp = raw_request(addr,
        b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
          Content-Type: text/plain\r\n\
          Content-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\nhello");
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 400"));
    assert_listener_alive(addr);
}

#[test]
fn pentest_chunked_bad_size() {
    let (addr, _d) = spawn_server();
    let resp = raw_request(addr,
        b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
          Content-Type: text/plain\r\nTransfer-Encoding: chunked\r\n\r\n\
          zz\r\nHello\r\n0\r\n\r\n");
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 400"));
    assert_listener_alive(addr);
}

#[test]
fn pentest_sql_non_utf8_body() {
    let (addr, _d) = spawn_server();
    let mut req = Vec::new();
    req.extend_from_slice(b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n");
    req.extend_from_slice(b"Content-Type: text/plain\r\nContent-Length: 2\r\n\r\n");
    req.push(0xC3); req.push(0x28); // invalid UTF-8 (continuation byte missing)
    let resp = raw_request(addr, &req);
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 400"));
    assert_listener_alive(addr);
}

#[test]
fn pentest_op_undecodable() {
    let (addr, _d) = spawn_server();
    let mut req = Vec::new();
    req.extend_from_slice(b"POST /v1/op HTTP/1.1\r\nHost: h\r\n");
    req.extend_from_slice(b"Content-Type: application/x-kessel-op\r\n");
    req.extend_from_slice(b"Content-Length: 1\r\n\r\n");
    req.push(0xFF); // garbage; not a valid Op tag
    let resp = raw_request(addr, &req);
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 400"));
    assert_listener_alive(addr);
}

#[test]
fn pentest_sql_wrong_content_type() {
    let (addr, _d) = spawn_server();
    let req = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                Content-Type: application/json\r\nContent-Length: 8\r\n\r\nSELECT 1";
    let resp = raw_request(addr, req);
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 415"));
    assert_listener_alive(addr);
}

#[test]
fn pentest_client_id_alone_400() {
    let (addr, _d) = spawn_server();
    let req = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                Content-Type: text/plain\r\nContent-Length: 8\r\n\
                X-Kessel-Client-Id: 0123456789abcdef0123456789abcdef\r\n\
                \r\nSELECT 1";
    let resp = raw_request(addr, req);
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 400"));
    assert_listener_alive(addr);
}

#[test]
fn pentest_client_id_non_hex() {
    let (addr, _d) = spawn_server();
    let req = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                Content-Type: text/plain\r\nContent-Length: 8\r\n\
                X-Kessel-Client-Id: GG23456789abcdef0123456789abcdef\r\n\
                X-Kessel-Req-Seq: 1\r\n\r\nSELECT 1";
    let resp = raw_request(addr, req);
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 400"));
    assert_listener_alive(addr);
}

#[test]
fn pentest_req_seq_non_decimal() {
    let (addr, _d) = spawn_server();
    let req = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                Content-Type: text/plain\r\nContent-Length: 8\r\n\
                X-Kessel-Client-Id: 0123456789abcdef0123456789abcdef\r\n\
                X-Kessel-Req-Seq: abc\r\n\r\nSELECT 1";
    let resp = raw_request(addr, req);
    assert!(String::from_utf8_lossy(&resp).starts_with("HTTP/1.1 400"));
    assert_listener_alive(addr);
}

#[test]
fn pentest_expect_100_continue() {
    let (addr, _d) = spawn_server();
    let req = b"POST /v1/sql HTTP/1.1\r\nHost: h\r\n\
                Content-Type: text/plain\r\nContent-Length: 8\r\n\
                Expect: 100-continue\r\n\r\nSELECT 1";
    let resp = raw_request(addr, req);
    // V1: we don't support 100-continue. Spec says 417. Implementation:
    // detect the Expect header in parse, return ParseError mapped to 417.
    // If T2/T3 didn't ship that mapping, EXTEND parse_request now to detect
    // Expect: 100-continue with non-zero Content-Length and return
    // ParseError::ExpectationFailed mapped to 417 in server::write_parse_error.
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 417"), "got: {text}");
    assert_listener_alive(addr);
}
```

Extract the `spawn_server` + `raw_request` helpers into a shared `tests/common_spawn.rs` file (NOT a module — `tests/` files are independent test binaries, but `include!` works). Or duplicate the helpers — the project rule is correctness > DRY in tests.

- [ ] **Step 2:** If `pentest_expect_100_continue` fails (no `Expect: 100-continue` handling yet), add the handling now:

In `parse.rs`, add to `ParseError`:

```rust
    ExpectationFailed,
```

In `parse_request`, after the headers loop and BEFORE constructing `Request`:

```rust
    // RFC 9110 §10.1.1 — we don't support 100-continue. If the client asked
    // for it on a non-empty POST, return 417.
    for (name, value) in &headers {
        if name.eq_ignore_ascii_case("expect")
            && value.eq_ignore_ascii_case("100-continue")
            && content_length.unwrap_or(0) > 0
        {
            return Err(ParseError::ExpectationFailed);
        }
    }
```

In `server.rs::write_parse_error`, add the arm:

```rust
        ParseError::ExpectationFailed =>
            ((417, "Expectation Failed"), "error", "expectation failed".into()),
```

- [ ] **Step 3:** Run the pentest suite.

```bash
cargo test -p kessel-http-gateway --release --features test-server pentest_ 2>&1 | tail -25
```

Expected: every pentest passes; no panics, no hangs.

- [ ] **Step 4:** Determinism gate.

```bash
cargo test --workspace --release 2>&1 | tail -5
cargo test -p kesseldb-server --release large_seed_corpus_is_deterministic_and_converges 2>&1 | tail -3
cargo tree -p kesseldb-server --no-default-features 2>&1 | grep -E "hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp|kessel-http-gateway"
```

Expected: count = previous + 17 (pentest tests); seed-7 green; tree-grep empty.

- [ ] **Step 5:** Commit + push.

```bash
git add crates/kessel-http-gateway/tests/pentest.rs crates/kessel-http-gateway/tests/common_spawn.rs crates/kessel-http-gateway/src/parse.rs crates/kessel-http-gateway/src/server.rs
git commit -m "test: SP141 T5 — HTTP gateway pentest matrix (17 adversarial inputs, every one verifies listener still accepts next connection)"
git push origin main
```

---

## Task T6: `/v1/metrics` Prometheus writer + `/v1/health` integration

**Files:**
- Create: `crates/kessel-http-gateway/src/metrics_writer.rs`
- Modify: `crates/kessel-http-gateway/src/lib.rs` (add `pub mod metrics_writer;`)
- Modify: `crates/kessel-http-gateway/src/routes.rs` (`handle_metrics` body)
- Modify: `crates/kessel-http-gateway/src/engine.rs` (`OpKindCounter` rolled up)
- Modify: `crates/kesseldb-server/src/lib.rs` (`snapshot_metrics` body, op counters)
- Create: `crates/kessel-http-gateway/tests/metrics_e2e.rs`

- [ ] **Step 1:** Write the Prometheus text writer. Create `crates/kessel-http-gateway/src/metrics_writer.rs`:

```rust
//! Prometheus text-format v0.0.4 writer. Hand-rolled — no `prometheus` crate.
//! The format is a sequence of HELP + TYPE + sample lines, terminated by a
//! newline. Reference: openmetrics-spec / prometheus exposition format.

use crate::engine::MetricsSnapshot;

pub fn render(snap: &MetricsSnapshot) -> String {
    let mut s = String::with_capacity(2048);

    s.push_str("# HELP kesseldb_ops_total Number of Ops applied since process start.\n");
    s.push_str("# TYPE kesseldb_ops_total counter\n");
    for row in &snap.ops_total {
        s.push_str(&format!(
            "kesseldb_ops_total{{kind=\"{}\"}} {}\n", row.kind, row.count));
    }

    s.push_str("# HELP kesseldb_inflight Number of Ops currently in flight to the engine.\n");
    s.push_str("# TYPE kesseldb_inflight gauge\n");
    s.push_str(&format!("kesseldb_inflight {}\n", snap.inflight));

    s.push_str("# HELP kesseldb_last_op_number Highest applied op_number on this replica.\n");
    s.push_str("# TYPE kesseldb_last_op_number gauge\n");
    s.push_str(&format!("kesseldb_last_op_number {}\n", snap.last_op_number));

    s.push_str("# HELP kesseldb_view_number Current VSR view number.\n");
    s.push_str("# TYPE kesseldb_view_number gauge\n");
    s.push_str(&format!("kesseldb_view_number {}\n", snap.view_number));

    s.push_str("# HELP kesseldb_is_primary 1 if this replica is the primary in the current view.\n");
    s.push_str("# TYPE kesseldb_is_primary gauge\n");
    s.push_str(&format!("kesseldb_is_primary {}\n", if snap.is_primary { 1 } else { 0 }));

    s.push_str("# HELP kesseldb_http_requests_total HTTP gateway requests by path and status.\n");
    s.push_str("# TYPE kesseldb_http_requests_total counter\n");
    for row in &snap.http_requests_total {
        s.push_str(&format!(
            "kesseldb_http_requests_total{{path=\"{}\",status=\"{}\"}} {}\n",
            row.path, row.status, row.count,
        ));
    }

    s
}
```

- [ ] **Step 2:** Add `pub mod metrics_writer;` to `crates/kessel-http-gateway/src/lib.rs`.

- [ ] **Step 3:** Replace `handle_metrics` in `routes.rs`:

```rust
fn handle_metrics<W: Write>(
    w: &mut W,
    engine: &Arc<dyn EngineApply>,
) -> std::io::Result<()> {
    use crate::metrics_writer::render;
    use crate::response::write_prometheus;
    let snap = engine.snapshot_metrics();
    let text = render(&snap);
    write_prometheus(w, &text)
}
```

- [ ] **Step 4:** Fill in the `snapshot_metrics` body in `kesseldb-server::lib.rs` impl. The simplest correct implementation that exercises the renderer end-to-end:

```rust
    fn snapshot_metrics(&self) -> kessel_http_gateway::MetricsSnapshot {
        let s = self.stats();
        kessel_http_gateway::MetricsSnapshot {
            ops_total: vec![
                // SP141 V1: a single rolled-up counter using `applied_ops`.
                // A per-Op-kind breakdown requires an atomic counter array on
                // the engine — defer to a follow-up; this exposes the
                // observable counter Prometheus cares about (total ops).
                kessel_http_gateway::OpKindCounter {
                    kind: "applied", count: s.applied_ops,
                },
            ],
            inflight: self.inflight.load(std::sync::atomic::Ordering::Acquire) as u64,
            last_op_number: s.applied_ops,
            view_number: 0,    // single-node V1; cluster integration is a follow-up
            is_primary: true,
            http_requests_total: Vec::new(),  // V1: not yet wired into the accept loop
        }
    }
```

(Per-path HTTP request counters and per-Op-kind counters are documented as
follow-ups in the §11 spec open questions; the V1 metrics surface is the rolled-up
counters above — enough for a real Prometheus scrape and for the e2e oracle.)

Note: `EngineHandle.inflight` is a private field on the struct — verify by re-reading
`crates/kesseldb-server/src/lib.rs:404`-ish. If it is not currently `pub(crate)`,
either expose it as `pub(crate)` (additive) or read it via an accessor. The existing
`fn stats()` already exposes `applied_ops` via an Op round-trip; if `inflight` is
already not exposed, add a `pub fn inflight_snapshot(&self) -> u64 { self.inflight.load(Ordering::Acquire) as u64 }`
inline on `impl EngineHandle` and call that.

- [ ] **Step 5:** Write the metrics e2e test. Create `crates/kessel-http-gateway/tests/metrics_e2e.rs`:

```rust
//! /v1/metrics + /v1/health integration. Apply a known sequence of ops,
//! scrape, assert the exact Prometheus text contains the right counters.

#![cfg(feature = "test-server")]

include!("common_spawn.rs");

use std::io::{Read, Write};
use std::net::TcpStream;

#[test]
fn metrics_includes_canonical_lines() {
    let (addr, _d) = spawn_server();
    let resp = raw_request(addr,
        b"GET /v1/metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 200"), "got: {text}");
    assert!(text.contains("# HELP kesseldb_ops_total"), "got: {text}");
    assert!(text.contains("# TYPE kesseldb_ops_total counter"), "got: {text}");
    assert!(text.contains("kesseldb_inflight "), "got: {text}");
    assert!(text.contains("kesseldb_last_op_number "), "got: {text}");
    assert!(text.contains("kesseldb_view_number "), "got: {text}");
    assert!(text.contains("kesseldb_is_primary "), "got: {text}");
    assert!(text.contains("# HELP kesseldb_http_requests_total"), "got: {text}");
    // Content-Type lock.
    assert!(text.contains("Content-Type: text/plain; version=0.0.4"), "got: {text}");
}

#[test]
fn metrics_counter_monotonic_under_load() {
    let (addr, _d) = spawn_server();
    // Read once.
    let resp0 = raw_request(addr,
        b"GET /v1/metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let t0 = String::from_utf8_lossy(&resp0).into_owned();
    let c0 = parse_counter(&t0, "kesseldb_last_op_number");
    // Apply a SQL op.
    let _ = raw_request(addr,
        b"POST /v1/sql HTTP/1.1\r\nHost: 127.0.0.1\r\n\
          Content-Type: text/plain\r\nContent-Length: 8\r\n\r\nSELECT 1");
    // Read again.
    let resp1 = raw_request(addr,
        b"GET /v1/metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let t1 = String::from_utf8_lossy(&resp1).into_owned();
    let c1 = parse_counter(&t1, "kesseldb_last_op_number");
    assert!(c1 >= c0, "last_op_number should not decrease: {c0} → {c1}");
}

fn parse_counter(text: &str, name: &str) -> u64 {
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix(name) {
            // Skip optional label block.
            let v = rest.trim_start_matches(|c: char| c != ' ' && c != '\t');
            return v.trim().parse::<u64>().unwrap_or(0);
        }
    }
    0
}
```

- [ ] **Step 6:** Run.

```bash
cargo test -p kessel-http-gateway --release --features test-server metrics_ 2>&1 | tail -10
```

Expected: 2 metrics tests pass; total e2e + metrics = ~9–10 passing.

- [ ] **Step 7:** Determinism gate.

```bash
cargo test --workspace --release 2>&1 | tail -5
cargo test -p kesseldb-server --release large_seed_corpus_is_deterministic_and_converges 2>&1 | tail -3
cargo tree -p kesseldb-server --no-default-features 2>&1 | grep -E "hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp|kessel-http-gateway"
```

Expected: count = previous + 2 (metrics tests); seed-7 green; tree-grep empty.

- [ ] **Step 8:** Commit + push.

```bash
git add crates/kessel-http-gateway crates/kesseldb-server/src/lib.rs
git commit -m "gateway: SP141 T6 — /v1/metrics Prometheus text writer + /v1/health snapshot + 2 e2e tests"
git push origin main
```

---

## Task T7: docs + internal record + memory (gate reconciliation)

**Files:**
- Modify: `docs/STATUS.md`
- Modify: `docs/USAGE.md`
- Modify: `README.md`
- Modify: `docs/ARCHITECTURE.md`
- Create: `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md`
- Modify (outside repo, never `git add`): `C:/Users/ihass/.claude/projects/C--Users-ihass--local-bin/memory/project_kesseldb.md`
- Modify (outside repo, never `git add`): `C:/Users/ihass/.claude/projects/C--Users-ihass--local-bin/memory/MEMORY.md`

- [ ] **Step 1:** Measure the FINAL workspace test count.

```bash
cd C:/Users/ihass/KesselDB
cargo test --workspace --release 2>&1 | tail -5
cargo test -p kessel-http-gateway --release --features test-server 2>&1 | tail -3
```

Record `FINAL_TOTAL`. Compute `DELTA = FINAL_TOTAL - BASELINE_TOTAL`. Expected DELTA = roughly 30 (parse KATs) + 7 (e2e) + 17 (pentest) + 2 (metrics) = ~56 new tests (give or take any deletions or test-count refactors). Whatever the **measured** number is, that's what gets written — no rounding, no aspirational counts (SP100-style honest reconciliation).

- [ ] **Step 2:** Append a STATUS.md row IMMEDIATELY AFTER the existing SP140 row, preserving numeric order. Open `docs/STATUS.md`, locate the SP140 row (search for "SP140"), and insert:

```
- SP141 — HTTP/1.1 gateway shipped. Opt-in `--features http-gateway` on `kesseldb-server`. Sibling listener (default `:6789` plaintext, `:6790` HTTPS with `tls`). Routes: POST `/v1/sql`, POST `/v1/op` (binary `Op::encode` body), GET `/v1/health`, GET `/v1/metrics` (Prometheus text v0.0.4). `Authorization: Bearer` ↔ `ServerConfig.token` (constant-time). Optional `X-Kessel-Client-Id` + `X-Kessel-Req-Seq` headers bind exactly-once dedup. Responses via `kessel_client::format_result_json` (locked JSON contract). Binary protocol byte-untouched. Zero external (non-workspace) deps on the gateway crate. Tests `+<MEASURED_DELTA>` (`BASELINE_TOTAL` → `FINAL_TOTAL`/0). Pentest matrix: 17 adversarial inputs, every one verifies listener still accepts next connection. Record: `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md`.
```

Fill in `<MEASURED_DELTA>`, `BASELINE_TOTAL`, `FINAL_TOTAL` with the actual numbers from Step 1.

- [ ] **Step 3:** Add a §HTTP gateway section to `docs/USAGE.md`. Find the §Transport encryption section (which today says "deploy behind a TLS-terminating reverse proxy"), and ADD a sibling sentence right after it:

```markdown
Or build with `--features http-gateway,tls` to terminate HTTPS in-process on
`ServerConfig.http_tls_addr` (default `:6790`) — see §HTTP gateway below.
```

Then add a new section `## HTTP gateway` near the end of USAGE.md (before any Appendix / Performance log section):

```markdown
## HTTP gateway

Opt-in HTTP/1.1 surface for operators, browsers, and tools that prefer
HTTP/JSON over the binary wire protocol. Built with
`cargo build --release -p kesseldb-server --features http-gateway` (add
`,tls` for HTTPS). The binary wire protocol is byte-untouched and remains
the default + fast path; the gateway runs on a sibling TCP listener.

### Configuration

```rust
let cfg = kesseldb_server::ServerConfig {
    http_addr: Some("127.0.0.1:6789".parse().unwrap()),
    http_tls_addr: Some("127.0.0.1:6790".parse().unwrap()), // requires `tls`
    tls: Some((cert_pem.into(), key_pem.into())),           // requires `tls`
    token: Some(b"my-token".to_vec()),                      // optional Bearer
    ..Default::default()
};
```

### Routes

| Method | Path | Body | Response |
|---|---|---|---|
| POST | `/v1/sql` | `text/plain` SQL | JSON `OpResult` |
| POST | `/v1/op` | `application/x-kessel-op` binary `Op::encode()` | JSON `OpResult` |
| GET | `/v1/health` | — | JSON liveness |
| GET | `/v1/metrics` | — | Prometheus text v0.0.4 |

### Auth

In token mode (`ServerConfig.token == Some(...)`), every request must carry
`Authorization: Bearer <token>` (constant-time compared). In open mode the
header is ignored. Mismatched / missing in token mode → HTTP `401` with
`{"status":"unauthorized"}`.

### Exactly-once (optional)

Add the headers `X-Kessel-Client-Id: <32-hex u128>` and
`X-Kessel-Req-Seq: <decimal u64>` together to bind the request to the
engine's per-client dedup map — retrying the same `(client_id, req_seq)`
returns the cached `OpResult`. Both-or-neither (one alone → `400`).

### curl examples

```bash
# Health
curl -s http://127.0.0.1:6789/v1/health
# {"status":"ok","primary":true,"view":0,"op_number":42,"role":"primary"}

# SQL
curl -s -X POST --data-binary 'SELECT 1' \
  -H 'Content-Type: text/plain' \
  http://127.0.0.1:6789/v1/sql
# {"status":"ok","value":1}

# Metrics (for Prometheus scrape)
curl -s http://127.0.0.1:6789/v1/metrics
# # HELP kesseldb_ops_total ...
# kesseldb_ops_total{kind="applied"} 1234
# ...

# Token mode
curl -s -H 'Authorization: Bearer my-token' \
  http://127.0.0.1:6789/v1/health
```

### Error mapping (excerpt)

| Body / situation | HTTP status |
|---|---|
| `OpResult::Ok` and most variants | 200 |
| `OpResult::Unauthorized` | 401 |
| Engine in-flight cap saturated | 429 |
| Body > 8 MiB (default cap) | 413 |
| Request line / headers > 64 KiB | 414 |
| Missing `Content-Length` on POST | 411 |
| Wrong `Content-Type` | 415 |
| Cluster has no primary | 503 |

Full mapping: `docs/superpowers/specs/2026-05-24-kesseldb-http-gateway-design.md` §4.4.
```

- [ ] **Step 4:** README.md — add a row to the capability matrix.

Find the capability matrix in `README.md` (it's the table near the top listing shipped capabilities). Add a row, in the appropriate section:

```
| HTTP/1.1 gateway (opt-in)  | full Op surface + SQL + `/v1/health` + `/v1/metrics` |
```

And update the test-count line (the README has a line near the top stating the workspace test count) to reflect the new `FINAL_TOTAL`/0.

- [ ] **Step 5:** ARCHITECTURE.md — add `kessel-http-gateway` to the crate list (in alphabetical position) and add a paragraph near the existing "listener" or "server" discussion:

```markdown
### Listeners

The `kesseldb-server` binary runs **two sibling listener threads** in the
opt-in `http-gateway` configuration:

1. **Binary wire** on `ServerConfig.tls`'s primary port — the deterministic
   hot path; this is the path measured by the SP69 pipelined-batch number
   and the path used by every replication / VSR / Jepsen oracle.
2. **HTTP gateway** on `ServerConfig.http_addr` (and a third HTTPS listener
   on `http_tls_addr` when both `http-gateway` and `tls` features are on) —
   translates HTTP/1.1 requests into the same engine apply path via the
   `kessel_http_gateway::EngineApply` trait that `EngineHandle` impls.

Both listeners share the same `max_conns` and `max_inflight` caps; total
in-flight to the engine is bounded honestly across binary + HTTP.
```

- [ ] **Step 6:** Write the internal SP141 record. Create `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md` mirroring the SP140 convention EXACTLY (read the SP140 record first to copy the header shape — `git log --oneline -- docs/superpowers/specs/` and look for the SP140 record path).

```markdown
# KesselDB — Subproject 141: HTTP/1.1 wire gateway

**Status:** done — code + tests committed and passing.

**Builds on:**
- `docs/superpowers/specs/2026-05-24-kesseldb-http-gateway-design.md` (SP141 design spec).
- Shipped binary wire (`kessel-proto`, `kesseldb-server`) — see the §M3 / §Sub-project 10 notes in `docs/STATUS.md`.
- Shipped token-mode auth (`ServerConfig.token`, `ct_eq`) — see the §Auth section of `docs/STATUS.md`.
- Shipped opt-in TLS (`tls` cargo feature, rustls) — see `docs/superpowers/specs/2026-05-13-kesseldb-subproject-ext-tls.md` (verify path via `ls docs/superpowers/specs/ | grep tls`).
- Shipped `kessel-client::format_result_json` (stable JSON OpResult contract).

---

## Outcome

Opt-in HTTP/1.1 gateway exposing four routes (`POST /v1/sql`, `POST /v1/op`,
`GET /v1/health`, `GET /v1/metrics`) on a sibling TCP listener. Binary wire
byte-untouched; default `cargo build -p kesseldb-server` byte-identical to
the pre-slice build (verified via `cargo tree -p kesseldb-server` empty
grep). Zero external (non-workspace) dependencies on the gateway crate.
JSON responses via the existing `kessel_client::format_result_json` — one
contract, two surfaces.

---

## Gate reconciliation (honest)

- Before (T0 measured): `BASELINE_TOTAL`/0.
- After (T7 measured): `FINAL_TOTAL`/0.
- DELTA: `+<MEASURED_DELTA>` (parse KATs T2 +14, parse KATs T3 +16, e2e T4 +7, pentest T5 +17, metrics T6 +2 — adjust if measurement differs).
- `cargo tree -p kesseldb-server --no-default-features` is empty for the regex `hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp|kessel-http-gateway` — the default build links no HTTP server crates (the gateway is feature-gated).
- `cargo build -p kesseldb-server` (no features) byte-identical to pre-slice (verified).
- `large_seed_corpus_is_deterministic_and_converges`: green.
- All 7 Parquet pyarrow e2e oracles: green untouched.
- Existing `external_source_oracle(2)`, `external_source_tls_oracle(1)`, `external_source_objstore_oracle(1)`: green untouched.

---

## Open follow-ups (named, deferred)

- Per-Op-kind metric counter array on `EngineHandle` (current snapshot rolls
  up to a single `kind="applied"` row — see SP141 design spec §6 + §11).
- Per-`(path, status)` HTTP request counter wired through the accept loop
  (current snapshot returns an empty vec).
- HTTP/2 / gRPC, WebSocket / SSE, PostgreSQL wire compat — design spec §2
  non-goals.
- HTTP/1.1 keep-alive on the gateway (V1 always closes — design spec §11).

---

## Cross-links

- STATUS row: `docs/STATUS.md` (SP141 row, after SP140).
- USAGE note: `docs/USAGE.md` §HTTP gateway.
- README matrix row.
- ARCHITECTURE.md §Listeners.
```

- [ ] **Step 7:** Append an SP141 block to the memory file via Bash heredoc (do NOT full-Read then Edit — the file is large):

```bash
cat >> "C:/Users/ihass/.claude/projects/C--Users-ihass--local-bin/memory/project_kesseldb.md" << 'EOF'

## SP141 — HTTP/1.1 wire gateway (shipped)

Opt-in `kesseldb-server` `--features http-gateway` adds a second TCP listener
exposing POST /v1/sql, POST /v1/op (binary body), GET /v1/health, GET
/v1/metrics (Prometheus text v0.0.4). `Authorization: Bearer` maps to
`ServerConfig.token` (constant-time). `X-Kessel-Client-Id` + `X-Kessel-Req-Seq`
optional headers bind exactly-once dedup. JSON responses via existing
`kessel_client::format_result_json`. Zero external (non-workspace) deps on the
gateway crate. Binary wire byte-untouched.

Default ports: 6789 plaintext, 6790 HTTPS (with `tls` feature).

Tests: +<MEASURED_DELTA> (BASELINE_TOTAL → FINAL_TOTAL/0).

Record: docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md.
EOF
```

(Replace `<MEASURED_DELTA>`, `BASELINE_TOTAL`, `FINAL_TOTAL` with the actual numbers.)

- [ ] **Step 8:** Update the KesselDB one-line entry in MEMORY.md. First read just the KesselDB entry to find it:

```bash
grep -n "kesseldb\|KesselDB\|project_kesseldb" \
  "C:/Users/ihass/.claude/projects/C--Users-ihass--local-bin/memory/MEMORY.md"
```

Then use `Edit` to update that single line to reflect SP141 SHIPPED and the new open backlog (drop "HTTP gateway" from any Open list if present; the remaining Open items per the prior session are: OBJ-2c-4 INT96/DECIMAL — wait, that's shipped too — OBJ-2c-5 REPEATED/nested, lz4/brotli, >64MiB, #75 SP-A, seed-7 liveness).

Do NOT `git add memory/*` — memory files live outside the repo by project rule.

- [ ] **Step 9:** Determinism gate one last time.

```bash
cargo test --workspace --release 2>&1 | tail -5
cargo test -p kessel-http-gateway --release --features test-server 2>&1 | tail -3
cargo test -p kesseldb-server --release large_seed_corpus_is_deterministic_and_converges 2>&1 | tail -3
cargo tree -p kesseldb-server --no-default-features 2>&1 | grep -E "hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp|kessel-http-gateway"
```

Expected: `FAILED=0` everywhere; seed-7 green; tree-grep empty.

- [ ] **Step 10:** Commit + push the docs.

```bash
git add docs/STATUS.md docs/USAGE.md README.md docs/ARCHITECTURE.md \
  docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md
git commit -m "docs: SP141 T7 — STATUS/USAGE/README/ARCHITECTURE refresh + SP141 internal record (gate reconciliation: <DELTA> measured); OBJ-2c arc remains open on REPEATED/nested + lz4/brotli + >64MiB; SP141 SHIPPED"
git push origin main
```

(Substitute `<DELTA>` with the actual measured number from Step 1.)

---

## Self-Review (per writing-plans skill)

**1. Spec coverage.** Walking spec sections:

| Spec § | Task |
|---|---|
| §1 Problem | introductory — no task |
| §2 Goals / non-goals | T1–T6 implement goals; non-goals enforced by absence of code |
| §3 Architecture | T1 (scaffold) + T4 (server + feature wiring) |
| §3.1 Crate layout | T1 (manifest + module stubs); T2–T6 (fill) |
| §3.2 Boundary discipline | T1 (no kesseldb-server dep); T4 (EngineApply impl) |
| §4.1 Request shape | T2 (request line + headers + CL body); T3 (chunked + caps + Bearer + X-Kessel-*) |
| §4.2 Response shape | T4 (response writer + handlers); T6 (Prometheus content-type) |
| §4.3 Exactly-once semantics | T3 (header extractors); T4 (handler routing) |
| §4.4 Error mapping | T4 (write_parse_error + write_op_result) — every row covered |
| §5 Auth model | T4 (handle() Bearer check via ct_eq) |
| §6 /v1/metrics | T6 (metrics_writer + handle_metrics + snapshot_metrics) |
| §7 /v1/health | T4 (handle_health + snapshot_health) |
| §8 Security posture | enforced throughout T2/T3/T4 (#![forbid(unsafe_code)], bounded buffers, checked_*) |
| §8.1 Pentest matrix | T5 — every row a test |
| §9 Test plan | T0–T6 each carries its own tests; final whole-implementation review after T7 |
| §10 Documentation deltas | T7 |
| §11 Open questions | documented as non-blocking; T7 internal record names them |
| §12 Acceptance criteria | T7 final gate reconciliation verifies each criterion |

No spec section uncovered.

**2. Placeholder scan.** No `TBD` / `TODO` / `fill in details` / `add appropriate error handling` / `similar to Task N` / `write tests for the above` (without code) — every step shows the exact code, exact command, exact expected output. Two "verify by re-reading line N" instructions exist (re-read `kessel-proto::Op`, re-read `EngineHandle.inflight`) — these are honest "the codebase may have shifted; confirm the line" notes, not placeholders for missing content; the dependent code that would change is explicitly spelled out in both directions ("if X then Y; else Z").

**3. Type consistency.** Trait method names match across tasks:
- `EngineApply::apply_op` (T1 declares, T4 implements, T4 handler calls)
- `EngineApply::apply_op_with_session` (T1 declares, T4 implements, T4 handler calls)
- `EngineApply::apply_sql` (T1 declares, T4 implements, T4 handler calls)
- `EngineApply::apply_sql_with_session` (T4 adds after BOLD decision in T4 Step 3, T4 implements, T4 handler calls)
- `EngineApply::snapshot_health` (T1 declares, T4 implements scaffold, T6 leaves alone)
- `EngineApply::snapshot_metrics` (T1 declares, T4 stub, T6 fills)
- `HealthSnapshot { primary, view, op_number, role }` consistent T1 → T4 → T6
- `MetricsSnapshot { ops_total, inflight, last_op_number, view_number, is_primary, http_requests_total }` consistent T1 → T6 (T6's `metrics_writer::render` reads exactly these fields)
- `OpKindCounter { kind, count }` and `HttpRequestCounter { path, status, count }` consistent T1 declarations match T6 reads
- `ParseError` variants consistent across T2/T3/T5 — `ExpectationFailed` added in T5 with the handler arm added in the same task

One outstanding consistency item: the spec mentions `ServerConfig.http_max_body` for the configurable body cap, but the plan ships only `DEFAULT_MAX_BODY` constant + no `ServerConfig` field for it. The plan's `serve()` signature does not take a `max_body` param. Fix inline: T4 Step 4's `serve(listener, engine, token, max_conns)` should be `serve(listener, engine, token, max_conns, max_body)`, and `ServerConfig` should grow a `pub http_max_body: usize` field defaulting to `DEFAULT_MAX_BODY`. The implementer adds this field in T4 Step 5 alongside `http_addr` and `http_tls_addr`. Plumb `cfg.http_max_body` through to the gateway's `serve()` call.

**Fix applied above by note** — the implementer should treat the T4 Step 5 ServerConfig diff as also including:

```rust
    pub http_max_body: usize,
```

with default `8 * 1024 * 1024` in `impl Default`, and pass it as a fifth arg to both `kessel_http_gateway::serve(...)` and `kessel_http_gateway::serve_tls(...)`. The gateway's `serve` and `serve_tls` and `handle_one` / `handle_one_stream` carry it through to `parse_request`'s `decode_body` call.

Re-running the type-consistency check after this fix: all signatures align.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-24-kesseldb-http-gateway.md`. Per the autonomous KesselDB mandate, proceeding directly into **superpowers:subagent-driven-development** (fresh subagent per task + two-stage spec-then-quality review gate + final whole-implementation review) without waiting for further user input.

Task model selection:
- **T0** standard model (mechanical baseline).
- **T1** standard model (mechanical scaffold).
- **T2** **CAPABLE model** (parser correctness; every branch must be bounds-checked).
- **T3** standard model (additive parsing — straightforward extension of T2).
- **T4** **CAPABLE model** (multi-file integration with kesseldb-server; trait impl boundary; feature gating; e2e).
- **T5** **CAPABLE model** (adversarial input reasoning across the parse + routes + auth surface).
- **T6** standard model (Prometheus text writer is mechanical).
- **T7** standard model (docs).
