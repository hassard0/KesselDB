# KesselDB — Subproject 147: HTTP/1.1 keep-alive on the gateway

**Status:** design — approved by autonomous mandate substitution.

**Builds on:**
- `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md` (SP141).
- `docs/superpowers/specs/2026-05-25-kesseldb-subproject144h-http-gateway-gap-closures.md` (SP144H — closed 4 SP141 follow-ups).

**Process note.** Per `feedback_kesseldb_autonomous_build`, brainstorm gate substituted. User explicitly requested "let's tackle the http keep alive".

---

## 1. Problem

SP141 ships HTTP/1.1 always sending `Connection: close` — every HTTP request opens + closes a TCP connection. SP141 internal record follow-up #5 deferred this:

> #5 HTTP/1.1 keep-alive on the gateway (V1 always sends `Connection: close`) — design spec §11 open question. Needs response-side state machine work.

For production HTTP gateway use (especially Prometheus scrape every N seconds, or any high-frequency `/v1/health` probe), the per-request TCP handshake + TLS handshake (for HTTPS) dominates wall-clock time. **Keep-alive amortizes setup over many requests** on one connection.

Measured today (vulcan, SP144H): sequential 1000 `/v1/health` curls = **128 req/s** (each curl process spawns + new TCP). With keep-alive, a real connection-pooling client should hit 10K+ req/s on the same hardware.

## 2. Goals and non-goals

**Goals (V1).**

- Parse incoming `Connection:` request header:
  - `Connection: close` → respond once + close TCP (current behavior).
  - `Connection: keep-alive` OR absent (HTTP/1.1 default per RFC 9112 §9.3) → respond + keep TCP open for the next request.
- Emit `Connection: keep-alive` or `Connection: close` response header per the negotiation.
- Loop the per-connection thread to read N requests until either (a) client closes, (b) idle timeout (configurable, default 30s), (c) per-connection request cap (configurable, default 1000), (d) response triggers close (parse error, oversized body, etc.).
- Reset the per-request buffer between requests (don't leak prior request bytes into the next parse).
- TLS keep-alive (`serve_tls`) honors the same negotiation.
- HTTP request counter (per-(path,status) from SP144H T2) still bumps per request (not per TCP connection).

**Non-goals (named).**

- HTTP/1.1 pipelining (sending multiple requests before reading responses). Spec/clients rarely use it; keep-alive without pipelining is the standard production shape.
- HTTP/2 multiplexing. Separate slice.
- Connection-pool exhaustion DoS protection beyond the existing per-listener `max_conns` cap. Per-connection `MAX_REQUESTS_PER_CONN` cap is the new defense.
- Chunked response bodies. All responses still use Content-Length.

## 3. Architecture

### 3.1 Per-connection loop

Current `handle_one_stream` is single-shot:
```rust
fn handle_one_stream(s, engine, token, max_body) {
    // read until parse succeeds, write response, return.
}
```

SP147 makes it a loop:
```rust
fn handle_one_stream(s, engine, token, max_body, max_requests_per_conn) {
    let mut requests_served = 0;
    loop {
        if requests_served >= max_requests_per_conn { return; }
        // read request
        match parse_request(&raw) {
            Ok(req) => {
                let close = wants_close(&req);
                routes::handle(s, &req, token, engine, http_counters)?;
                requests_served += 1;
                if close { return; }
                // Reset buffer for next request, continue loop.
                raw.clear();
            }
            Err(...) => { write_error; return; }  // unchanged
        }
    }
}
```

The `wants_close` helper:
```rust
fn wants_close(req: &Request) -> bool {
    for (name, value) in &req.headers {
        if name.eq_ignore_ascii_case("connection") {
            if value.eq_ignore_ascii_case("close") {
                return true;
            }
            if value.eq_ignore_ascii_case("keep-alive") {
                return false;
            }
        }
    }
    // HTTP/1.1 default per RFC 9112 §9.3: persistent unless explicitly closed.
    false
}
```

### 3.2 Response Connection header

The current `write_json` / `write_prometheus` / `write_error_json` helpers always emit `Connection: close`. Make this conditional:

```rust
pub fn write_json<W: Write>(
    w: &mut W,
    status: (u16, &'static str),
    body_json: &str,
    keep_alive: bool,
) -> std::io::Result<()> {
    let body = body_json.as_bytes();
    write!(w, "HTTP/1.1 {} {}\r\n", status.0, status.1)?;
    w.write_all(b"Content-Type: application/json; charset=utf-8\r\n")?;
    write!(w, "Content-Length: {}\r\n", body.len())?;
    w.write_all(if keep_alive {
        b"Connection: keep-alive\r\n"
    } else {
        b"Connection: close\r\n"
    })?;
    w.write_all(b"Server: kesseldb/0\r\n")?;
    w.write_all(b"\r\n")?;
    w.write_all(body)?;
    Ok(())
}
```

The `_counted` variants in `response.rs` take an extra `keep_alive` arg too. The route handlers in `routes::handle` compute `keep_alive = !wants_close(req)` once and pass it through.

### 3.3 Buffer reset between requests

After a successful parse + handle, the buffer may contain bytes BEYOND the consumed request (pipelined — out of V1 scope, but defensive: drain to next request). Since `Request.consumed` already tracks "how many bytes belong to this request", drain:

```rust
let req = parse_request(&raw)?;
let _ = routes::handle(...);
raw.drain(..req.consumed);  // keep any trailing bytes for next iteration
```

This handles the edge case of a TCP packet containing 2 requests glued together (rare with real clients but possible).

### 3.4 Idle timeout

Already implemented: `set_read_timeout(Some(30s))` on the TcpStream is set in `serve`. After a successful response, the next `s.read()` will block until either bytes arrive or the 30s timeout fires (returning `WouldBlock`/`TimedOut`). Treat that as a clean connection close (return Ok).

The current `handle_one_stream` already handles `Err(_) => return Ok(())` — covers all read errors including timeouts. SP147 doesn't change this.

### 3.5 Per-connection request cap

Add `MAX_REQUESTS_PER_CONN: usize = 1000` constant. Configurable via `ServerConfig.http_max_requests_per_conn` (default 1000). Prevents a single peer from monopolizing one connection forever.

### 3.6 Tests

- Unit: `wants_close` tests for all combinations.
- e2e: send 2 sequential requests on the SAME TcpStream, both succeed, both return `Connection: keep-alive`.
- e2e: send `Connection: close` request, response returns `Connection: close`, second request on same socket fails.
- e2e: send 1001 requests on one connection, the 1001st gets `Connection: close` (cap hit).
- Pentest: oversized header on the SECOND request — listener still returns the typed 414, still accepts a fresh connection.

## 4. Files

| Path | Change | Task |
|---|---|---|
| `crates/kessel-http-gateway/src/parse.rs` | Add `wants_close` helper | T1 |
| `crates/kessel-http-gateway/src/response.rs` | All `write_*` helpers gain `keep_alive: bool` | T2 |
| `crates/kessel-http-gateway/src/routes.rs` | Compute keep_alive once, pass through | T2 |
| `crates/kessel-http-gateway/src/server.rs` | `handle_one_stream` loops; cap; drain | T3 |
| `crates/kesseldb-server/src/lib.rs` | `ServerConfig.http_max_requests_per_conn` additive | T3 |
| `crates/kessel-http-gateway/tests/e2e_curl.rs` | 3-4 keep-alive e2e tests | T4 |
| `docs/STATUS.md` / `docs/USAGE.md` / SP141 internal record / memory | T5 docs | T5 |

## 5. Task decomposition

- **T0**: Baseline (1023/0/0 default, 1052/0/0 featured post-SP144).
- **T1**: `wants_close` parser helper + unit tests.
- **T2**: `write_*` helpers + `routes::handle` + `write_op_result` + `write_parse_error` all gain `keep_alive: bool`. Default unchanged behavior (when keep_alive=false, identical to today).
- **T3**: `handle_one_stream` loop + `MAX_REQUESTS_PER_CONN` + `ServerConfig.http_max_requests_per_conn` additive field.
- **T4**: 3-4 e2e keep-alive tests (sequential requests on one socket, explicit close, request cap).
- **T5**: docs + STATUS row + SP141 follow-up #5 marked CLOSED + memory.

## 6. Acceptance criteria

1. `wants_close(req)` returns false for HTTP/1.1 without `Connection:` header (per RFC 9112 §9.3 default).
2. Returns false for `Connection: keep-alive`. Returns true for `Connection: close`.
3. `write_*` helpers emit `Connection: keep-alive` when keep_alive=true.
4. `handle_one_stream` loops, processing N requests per connection until close/timeout/cap.
5. Per-connection request cap (default 1000) enforced — 1001st request gets `Connection: close`.
6. Buffer drain handles inter-request residue.
7. All existing 17 pentest tests + 8 e2e tests + 2 metrics_e2e tests still pass (keep-alive is backward-compatible).
8. New e2e test: 2 sequential requests on one TcpStream both succeed.
9. Workspace 1023/0/0 default unchanged (new tests are featured); featured 1052/0/0 + N.
10. seed-7 GREEN. Default tree-grep EMPTY.
