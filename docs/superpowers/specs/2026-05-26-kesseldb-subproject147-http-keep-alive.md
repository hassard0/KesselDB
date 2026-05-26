# KesselDB — Subproject 147: HTTP/1.1 keep-alive on the gateway

**Status:** done — code + tests committed and passing.

**Builds on:**
- `docs/superpowers/specs/2026-05-26-kesseldb-http-keep-alive-design.md` (SP147 design spec).
- `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md` (SP141 — the HTTP gateway this slice extends; follow-up #5 closed here).
- `docs/superpowers/specs/2026-05-25-kesseldb-subproject144h-http-gateway-gap-closures.md` (SP144H — closed 4 SP141 follow-ups; sets the response-writer + routes shape this slice plumbs `keep_alive: bool` through).

---

## Outcome

**Follow-up #5 (HTTP/1.1 keep-alive) → CLOSED.**

SP141 always sent `Connection: close` on every response so every HTTP request opened + closed a fresh TCP connection. SP147 makes the gateway honor HTTP/1.1 persistent-connection semantics per RFC 9112 §9.3 (HTTP/1.1 is persistent by default; explicit `Connection: close` overrides).

- `parse::wants_close(headers: &[(String, String)]) -> bool` — RFC 9110 §7.6.1 comma-separated `Connection` header tokeniser; returns true iff the `close` token (case-insensitive) appears in any value. (The legacy HTTP/1.0 `keep-alive` token is treated as an explicit affirmative for clarity, though absence is already the persistent default.)
- All 6 `write_*` helpers in `response.rs` (`write_json`, `write_prometheus`, `write_error_json` + their `_counted` SP144H wrappers) take an explicit `keep_alive: bool`; `true` emits `Connection: keep-alive`, `false` emits `Connection: close`.
- `routes::handle` computes `keep_alive = !wants_close(&req.headers)` ONCE and threads it through every path; the function's return type is now `Result<bool, io::Error>` where the bool is "close this TCP connection after the response landed" (sources of close: explicit `Connection: close` from the client; T3 cap; T3 abuse / parse errors).
- `server::handle_one_stream` is now a per-connection LOOP: read → parse → handle → if close_after return → drain consumed bytes → next iteration. Sources of clean close: explicit `Connection: close`, idle timeout (existing `set_read_timeout(30s)`), EOF on the socket, parse error (defensive), `requests_served >= max_requests_per_conn`, payload-too-large abuse.
- Buffer drain via `Request.consumed` handles the rare glued-packet shape where one `read()` returns bytes from two requests; non-consumed bytes carry forward to the next iteration's parse attempt.
- `ServerConfig.http_max_requests_per_conn: usize` (default 1000) — additive field on `ServerConfig`; prevents a single client from monopolizing one TCP connection forever. Threaded through `kesseldb_server::serve_cfg` → `kessel_http_gateway::serve()` + `kessel_http_gateway::serve_tls()` → `handle_one_stream`. `DEFAULT_MAX_REQUESTS_PER_CONN: usize = 1000` constant exported from the gateway crate.

---

## Gate reconciliation (honest)

- Before (SP144 ship): 1023 PASSED / 0 / 0 default; 1052 / 0 / 0 featured.
- After SP147 T5 (measured): **1029** PASSED / 0 / 0 default (+6); **1062** PASSED / 0 / 0 featured (+10).
- Per-slice delta:
  - T1 `parse::wants_close`: +6 KATs (default).
  - T2 `write_*` keep_alive plumbing: +0 (refactor only — existing 17 pentest + 8 e2e + 2 metrics_e2e tests preserved; in T2 still single-shot per-connection because the loop is wired in T3).
  - T3 `handle_one_stream` loop + cap + drain: +0 (loop behavior covered by T4 e2e tests).
  - T4 e2e keep-alive: +4 e2e tests (`keepalive_two_requests_same_connection`, `keepalive_explicit_close_closes_after_response`, `keepalive_legacy_http_keep_alive_token_recognized`, `keepalive_many_requests_on_one_connection`) — behind `test-server` feature, so +0 default / +4 featured.
  - T5 docs: +0.
  - Sum default: 6 ✓ (1023 → 1029).
  - Sum featured: 6 KATs + 4 e2e = 10 ✓ (1052 → 1062).
- `cargo tree -p kesseldb-server --no-default-features | grep -E "hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp|kessel-http-gateway"`: empty.
- All 3 `kesseldb-server` build combos clean: no-features, `--features http-gateway`, `--features http-gateway,tls`.
- `kessel-vsr::large_seed_corpus_is_deterministic_and_converges`: GREEN.
- All SP140-SP144 oracles + SP141 17 pentest + 8 e2e + 2 metrics_e2e tests: green untouched.

---

## Backward-compat note (the legacy `raw_request` test helper)

The default switch from "always close" to "keep-alive when client doesn't send `Connection: close`" (RFC 9112 §9.3) IS a behavior change. The existing `tests/common/mod.rs::raw_request` helper used `read_to_end` after writing the request, which depended on the server closing after each response. Without modification, the 6 existing e2e tests + 11 pentest tests that go through `raw_request` would have hung to the 5s read timeout on every connection.

Fix: `raw_request` now injects `Connection: close\r\n` into the request right before the header terminator. This preserves the single-shot semantic at the helper boundary without rewriting any test bodies — every existing e2e / pentest call site continues to work byte-identically. The new SP147 T4 keep-alive tests bypass `raw_request` and drive the raw `TcpStream` directly (with a `read_one_response` helper that does bounded Content-Length-framed reads instead of `read_to_end`) so they actually exercise the per-connection loop in `server::handle_one_stream`.

---

## Honest scope / non-goals

V1 keep-alive does NOT include:

- **HTTP/1.1 pipelining** (sending multiple requests before reading responses). The buffer-drain mechanism via `Request.consumed` would handle it structurally for the rare glued-packet shape, but no test exercises it deliberately and real clients almost never use pipelining.
- **HTTP/2 multiplexing** — SP141 follow-up #4 (separate slice).
- **Chunked response bodies** — all responses still use `Content-Length`.
- **Per-connection idle timeout configurability** — fixed at 30s via the existing `set_read_timeout(30s)`.
- **Cap-hit observability metric** — the cap-hit path currently closes silently. A future slice could bump a counter (e.g. `kesseldb_http_conn_cap_hits_total`) when `requests_served >= max_requests_per_conn`.

---

## Remaining SP141 follow-ups (2 still open)

After SP147 closes #5 (and SP142 closed #2 + #8, SP144H closed #1, #3, #6, #7), only two SP141 follow-ups remain:

- **#4**: HTTP/2 / gRPC / WebSocket / SSE / PostgreSQL wire compat — non-goal of SP141; own slice if a real consumer asks.
- **#9**: Pentest body-text assertions tightening — cosmetic hardening; small slice.

Both are non-blocking; the gateway is production-ready post-SP147 with persistent connections + per-connection request cap.

---

## Cross-links

- STATUS row: `docs/STATUS.md` (SP147 row, after SP146).
- SP141 internal record: `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md` (follow-up #5 now marked CLOSED with backlink here).
- Design spec: `docs/superpowers/specs/2026-05-26-kesseldb-http-keep-alive-design.md`.
- Memory: `memory/project_kesseldb.md` (SP147 block) + `MEMORY.md` (KesselDB line).
