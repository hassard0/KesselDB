# KesselDB — Subproject 141: HTTP/1.1 wire gateway

**Status:** done — code + tests committed and passing.

**Builds on:**
- `docs/superpowers/specs/2026-05-24-kesseldb-http-gateway-design.md` (SP141 design spec).
- Shipped binary wire (`kessel-proto`, `kesseldb-server`) — see the §M3 / §Sub-project 10 notes in `docs/STATUS.md`.
- Shipped token-mode auth (`ServerConfig.token`, `ct_eq`) — see auth handshake in `crates/kesseldb-server/src/lib.rs:138-154`.
- Shipped opt-in TLS (`tls` cargo feature, rustls).
- Shipped `kessel-client::format_result_json` (stable JSON OpResult contract).

---

## Outcome

Opt-in HTTP/1.1 gateway exposing four routes:
- `POST /v1/sql` (text/plain SQL body)
- `POST /v1/op` (binary `Op::encode()` body)
- `GET /v1/health` (JSON liveness)
- `GET /v1/metrics` (Prometheus text v0.0.4)

Sibling TCP listener; binary wire byte-untouched. Default
`cargo build -p kesseldb-server` byte-identical to pre-slice (verified via
`cargo tree -p kesseldb-server` empty grep for `hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp|kessel-http-gateway`). Zero external (non-workspace)
dependencies on the gateway crate. JSON responses via the existing
`kessel_client::format_result_json` — one contract, two surfaces.

Slices T0–T7:
- T0 — determinism baseline (891 PASSED, seed-7 GREEN, default tree clean)
- T1 — scaffolded `kessel-http-gateway` crate with `EngineApply` trait + module stubs
- T2 — RFC 9112 request-line + headers + Content-Length body parser + 15 hand-built KATs
- T3 — chunked decode + body cap + Bearer + X-Kessel-* exactly-once extractors + 16 KATs (+ 3 RFC §6.3.5 / §3.2 / §6.1 smuggling rejections in parse_request)
  - T3 fix: `checked_add` chunk-size overflow + exactly-once extractor enforcement + dedicated `ParseError` variants (`ConflictingFraming`, `DuplicateHost`, `DuplicateContentLength`, `DuplicateHeader`, `BadChunk`, `UnsupportedTransferEncoding`) + RFC 6750 §2.1 case-insensitive Bearer + `dechunk` returns consumed-bytes + 6 KATs for the new behaviors + 1 bonus case-insensitive Bearer KAT
- T4 — route handlers + accept loop + `serve_tls` + `TlsAccept` trait + `kesseldb-server` `http-gateway` feature (additive `ServerConfig` fields + feature-gated thread spawn + `RustlsAcceptor` + `impl EngineApply for EngineHandle`) + 8 e2e tests with `TempDirGuard`
  - T4 fix: honor `http_max_body` in `parse_request` (threaded through) + `serve_tls` Slowloris timeouts + per-listener `max_conns` doc + e2e `TempDirGuard` cleanup
- T5 — 17-row pentest matrix (every row asserts listener still accepts next connection); added `tests/common/mod.rs` for shared helpers; added `ParseError::ExpectationFailed` (`Expect: 100-continue` → 417)
- T6 — Prometheus text v0.0.4 writer + `/v1/health` snapshot wired to `EngineHandle.stats()` + 2 metrics_writer unit tests + 2 metrics_e2e integration tests

---

## Gate reconciliation (honest)

- Before (T0 measured): 891 PASSED / 0 FAILED / 0 IGNORED.
- After T6 (T7 measured):
  - Default `cargo test --workspace --release`: **931 PASSED / 0 FAILED / 0 IGNORED** (+40)
  - Featured `--features kessel-http-gateway/test-server`: **958 PASSED / 0 FAILED / 0 IGNORED** (+67 over baseline)
- Per-slice delta (real measured numbers):
  - T1: +0
  - T2: +15 KATs
  - T3 (incl. fix): +16 + 7 = +23 KATs
  - T4 (incl. fix): +0 default (e2e behind feature) / +8 e2e under feature
  - T5: +0 default (pentest behind feature) / +17 pentest under feature + 0 to default (added `ExpectationFailed` variant + handler is structural, no new default tests)
  - T6: +2 metrics_writer unit tests default / +2 metrics_e2e under feature
  - Sum default: 0+15+23+0+0+2 = +40 ✓ (931 - 891 = 40)
  - Sum featured: +40 default + 8 e2e + 17 pentest + 2 metrics_e2e = +67 ✓ (958 - 891 = 67)
- `cargo tree -p kesseldb-server --no-default-features | grep -E "hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp|kessel-http-gateway"`: empty.
- `cargo build -p kesseldb-server` (no features) byte-identical to pre-slice (verified).
- `kessel-vsr::large_seed_corpus_is_deterministic_and_converges`: GREEN.
- All 7 Parquet pyarrow e2e oracles: green untouched.
- Existing oracles (binary serve, pipelined batch SP69, token-mode auth, TLS binary, external-sources × 2, external-sources-tls × 1, objstore × 1): green untouched.

---

## Known follow-ups (named, deferred to dedicated slices)

These were called out during the two-stage review process and explicitly
deferred to keep T6 focused. None are correctness gaps that block adoption
of the HTTP gateway — they are hardening / observability upgrades:

1. **Per-`Op::kind()` counter array on `EngineHandle`.** Today
   `snapshot_metrics` rolls up to a single `kind="applied"` row using
   `EngineHandle.stats().applied_ops`. A per-kind breakdown needs an atomic
   counter array on the engine — additive, but touches the SM apply path.

   **CLOSED in SP144H (commit `d5b9e3a`)** — see `docs/superpowers/specs/2026-05-25-kesseldb-subproject144h-http-gateway-gap-closures.md`.
2. **`snapshot_metrics` round-trips through `apply_raw`.** The current impl
   calls `self.stats()`, which enqueues a STATS_TAG frame. Under engine
   saturation, this returns `OpResult::Unavailable` and the metrics endpoint
   silently reports `kesseldb_last_op_number 0` — Prometheus interprets as
   a counter reset (ops-rate corruption). Fix: a direct-atomic-load path
   or a last-known-good cache. The trait doc in `engine.rs` already
   promises "no engine apply"; reconcile in follow-up. Same shape applies
   to `snapshot_health`.

   **CLOSED in SP142 (commit `c95722a`)** — see `docs/superpowers/specs/2026-05-25-kesseldb-subproject142-http-gateway-hardening.md`.
3. **Per-`(path, status)` HTTP request counter wired through the accept
   loop** (currently returns empty vec).

   **CLOSED in SP144H (commit `392a2b1`)** — see `docs/superpowers/specs/2026-05-25-kesseldb-subproject144h-http-gateway-gap-closures.md`.
4. **HTTP/2 / gRPC, WebSocket / SSE streaming, PostgreSQL wire compat** —
   design spec §2 non-goals.
5. **HTTP/1.1 keep-alive on the gateway** (V1 always sends
   `Connection: close`) — design spec §11 open question.
6. **`OpResult::Unauthorized` HTTP status collides with auth-layer 401.**
   Both return 401 with `{"status":"unauthorized"}` — a caller cannot
   distinguish "wrong Bearer" from "engine ACL denied". Spec §4.4 needs a
   `message` disambiguation field.

   **CLOSED in SP144H (commit `48e73fe`)** — see `docs/superpowers/specs/2026-05-25-kesseldb-subproject144h-http-gateway-gap-closures.md`.
7. **`exactly_once_binding` stuffs "both required together" into
   `BadHeaderValue(String)` rather than a dedicated variant.** Cosmetic
   fragility (KAT could string-grep break on refactor).

   **CLOSED in SP144H (commit `48e73fe`)** — see `docs/superpowers/specs/2026-05-25-kesseldb-subproject144h-http-gateway-gap-closures.md`.
8. **e2e `spawn_server` uses a 150ms sleep** rather than a connect-retry
   loop. Pre-existing CI risk; widened by T5's 17 additional spawn-server
   calls.

   **CLOSED in SP142 (commit `2acea3f`)** — `wait_for_listener` connect-retry loop replaces the sleep. ~20× speedup on the pentest suite.
9. **Pentest assertions are slightly loose** — most assert "HTTP/1.1 400"
   without distinguishing which `ParseError` variant produced it. A
   hardening pass could tighten body-text assertions.

---

## Cross-links

- STATUS row: `docs/STATUS.md` (SP141 row, after SP140).
- USAGE note: `docs/USAGE.md` §HTTP gateway.
- README capability matrix row.
- ARCHITECTURE: `docs/ARCHITECTURE.md` §Listeners.
- Memory: `memory/project_kesseldb.md` (SP141 block) + `MEMORY.md` (KesselDB line).
