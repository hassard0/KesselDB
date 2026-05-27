# SP-WS — WebSocket support for the HTTP gateway — SP-arc Progress Tracker

Date created: 2026-05-26
Design spec: `docs/superpowers/specs/2026-05-26-kesseldb-spws-websocket-design.md`
Scoping doc: `docs/superpowers/specs/2026-05-26-kesseldb-http2-ws-pgwire-scoping.md`
TaskList: closes SP141 follow-up **#4** (WebSocket arm of the SP156
recommendation; ahead of the eventual PG-wire and the deferred HTTP/2).

## What this SP-arc ships

V1 RFC 6455 WebSocket on the existing HTTP/1.1 gateway. Browser-direct
clients can open `wss://kesseldb.example/v1/ws`, negotiate the
`kessel-op-v1` subprotocol, and exchange binary frames carrying
`Op::encode()` / `OpResult::encode()` bytes against the same
`EngineApply` the HTTP routes use.

**Out-of-scope (named, deferred):** permessage-deflate (RFC 7692),
fragmented messages (continuation frames), streaming row responses
(SP-A T14 follow-up), cookie/first-message auth, JSON-over-WebSocket
(`kessel-op-v1-json` future subprotocol), HTTP/2 + WebSocket
(RFC 8441). See spec §2.2.

## Slice plan (mirrors design spec §10)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec (~707 lines, 8 weak-spots + 4 open questions) + scaffold module (`ws.rs` with placeholder `handle_upgrade` returning `Err(WsError::NotYetImplemented)`) + `crypto.rs` shim + `kessel-crypto::sha1` + `kessel-crypto::base64_encode` + RFC 6455 §1.3 canonical handshake KAT + RFC 3174 §A.5 SHA-1 KATs + RFC 4648 §10 base64 KATs + WS predicate (`is_websocket_upgrade`) + locked constants (WEBSOCKET_PATH, SUBPROTOCOL_V1, WS_SEND_QUEUE_BOUND). | **DONE** | `2bc3570` (spec) + `22ea9c1` (scaffold) |
| T2 | Handshake parser: validate `Sec-WebSocket-Key` / `Sec-WebSocket-Version: 13` / path=/v1/ws / Authorization; build the `HTTP/1.1 101 Switching Protocols` response bytes; wire `routes.rs::handle` arm; flip the T1 regression-lock KAT to "handshake completes" | OPEN | — |
| T3 | Frame encoder: `encode_server_frame(opcode, payload)` (binary / ping / pong / close); structurally no server-side masking | OPEN | — |
| T4 | Frame decoder: strict per spec §4.2 / §8 — MASK from client, RSV bits zero, control frames ≤ 125 bytes + FIN=1, no fragmentation, oversize → 1009 | OPEN | — |
| T5 | Per-connection session loop: reader thread + writer thread + bounded `mpsc::sync_channel(WS_SEND_QUEUE_BOUND=16)` + ping/pong heartbeat (30s) + idle timeout + graceful close handshake | OPEN | — |
| T6 | `kessel-op-v1` subprotocol wire-up + real-WebSocket-client e2e test (`tests/ws_e2e.rs`) + 10-pentest matrix (spec §8.7) | OPEN | — |

Optional / deferred:
- **T7 (optional)** — SSE (`/v1/events`, `text/event-stream`) as the SP156 §3.5 bundled add-on (~1 slice)
- **T8 (optional)** — streaming row chunks under `kessel-op-v1` (the SP-A T14 follow-up; gates on a separate design spec)

## T1 — what landed (2026-05-26, commits `2bc3570` + `22ea9c1`)

**Two commits, ~610 LoC net delta (excluding the 707-line spec doc):**

**Commit `2bc3570` — design spec** (707 lines, no code change):
`docs/superpowers/specs/2026-05-26-kesseldb-spws-websocket-design.md`
covers context (§1), scope V1+deferred (§2), RFC 6455 sections that
apply (§3), frame implementation subset (§4) with target encoder +
decoder API shapes, `kessel-op-v1` subprotocol (§5) with binary-only
rationale, integration into `routes.rs::handle` (§6) with path choice
`/v1/ws` + session-loop sketch, backpressure (§7), security including
the 10-pentest matrix derived from RFC 6455 §10 (§8), close behavior
including idle timeout + ping/pong heartbeat (§9), 6-task decomposition
(§10), 6 acceptance criteria (§11), 8 weak-spots self-review (§12),
4 open questions (§13). Wire-protocol invariants locked in §3-§5 so
T2-T6 implement against a fixed contract.

**Commit `22ea9c1` — scaffold:**

- **kessel-crypto (+2 KATs, ~80 LoC):**
  - `sha1()` per RFC 3174 / FIPS 180-1 — pure-Rust, zero-dep,
    `#![forbid(unsafe_code)]`. Doc-comment narrows usage to the
    RFC 6455 §4.2.2 handshake-completion proof (NOT a security
    primitive — SHA-1 is collision-broken).
  - `base64_encode()` per RFC 4648 standard alphabet. Doc-comment
    notes the duplication with `kessel-objstore::b64::encode` exists
    because objstore is feature-gated; a future workspace cleanup
    can consolidate.
  - 2 new KATs: `sha1_rfc3174_known_answer_vectors` (3 RFC 3174 §A.5
    vectors), `base64_encode_rfc4648_vectors` (RFC 4648 §10 vectors).
- **kessel-http-gateway crypto.rs (+3 KATs, ~110 LoC):**
  - `WEBSOCKET_ACCEPT_GUID` constant — locked byte-for-byte by KAT.
  - `sec_websocket_accept(client_key) -> String` computes
    `base64(sha1(client_key + GUID))` — locked by RFC 6455 §1.3
    canonical example KAT.
  - Output-shape KAT: always 28 chars with exactly one `=` pad
    (base64 of a 20-byte digest).
- **kessel-http-gateway ws.rs (+8 KATs, ~330 LoC):**
  - Locked constants: `WS_SEND_QUEUE_BOUND = 16`,
    `WEBSOCKET_PATH = "/v1/ws"`, `SUBPROTOCOL_V1 = "kessel-op-v1"`.
  - `WsError` enum (currently only `NotYetImplemented`).
  - `handle_upgrade<S: Read + Write>(stream, req, token, engine)`
    signature locked + scaffold returns `Err(NotYetImplemented)`
    without touching the stream.
  - `is_websocket_upgrade(headers)` predicate gating on RFC 6455 §4.1
    + RFC 9110 §7.6.1 / §7.8 (both `Upgrade: websocket` AND
    `Connection: Upgrade`, case-insensitive name + token, comma-list-
    aware for the browser-shape `Connection: keep-alive, Upgrade`).
  - 8 KATs: 3 constant locks + 4 predicate cases (canonical handshake,
    multi-token Connection, missing Upgrade, missing Connection, case
    insensitivity) + 1 T1 regression-lock
    (`t1_handle_upgrade_returns_not_yet_implemented_stub`) that mirrors
    the SP-A T1 stub-lock pattern: T2 MUST update this test alongside
    the real handshake response — flipping this lock is the gate that
    catches a half-shipped T2.

**KAT delta:** +13 total (+2 kessel-crypto + +3 gateway/crypto + +8
gateway/ws). All RFC-derived, all locking spec invariants.

**Zero-dep stance preserved:** no new external deps. kessel-crypto
stays at 0 external deps. kessel-http-gateway adds one workspace-only
dep (kessel-crypto). `cargo tree -p kesseldb-server -e normal` shows
no new entries.

**Test counts:**
- kessel-crypto: 4 → 6 (+2)
- kessel-http-gateway: 0 → 14 (+14)
- Workspace default: 1366 → 1381 (+15)
- Workspace featured: 1399 → 1414 (+15)

seed-7 GREEN. tree-grep EMPTY. `#![forbid(unsafe_code)]` honored
throughout the new code. All prior tests pass.

**What T1 deliberately did NOT do:**
- No real handshake validation — T2.
- No frame encoder/decoder — T3/T4.
- No session loop — T5.
- No `routes.rs` arm wiring `handle_upgrade` — T2 (deferred so a
  half-shipped T2 is impossible; today the placeholder is reachable
  only from the T1 regression-lock test).
- No real-WebSocket-client e2e test — T6.
- No browser harness (acceptance #3 — manual verification per spec
  §11).

## Pickup hint for the next session

T2 is the next slice. Concrete shape:

1. Add `WEBSOCKET_PATH` to `parse::is_known_path` so `GET /v1/ws`
   parses as a known route (currently rejected with 404).
2. Add the upgrade arm to `routes::handle`:
   ```rust
   match req.path {
       ws::WEBSOCKET_PATH if ws::is_websocket_upgrade(&req.headers) => {
           ws::handle_upgrade(s, &req, token, engine)?;
           return Ok(true); // close HTTP loop; WS owns the socket
       }
       …
   }
   ```
3. In `ws.rs::handle_upgrade`: validate `Sec-WebSocket-Version: 13`
   (else 426 + `Sec-WebSocket-Version: 13` header), extract
   `Sec-WebSocket-Key`, optionally read `Sec-WebSocket-Protocol`, ct-eq
   the Bearer token. Build the 101 response bytes (using
   `crypto::sec_websocket_accept(client_key)` for the accept header).
   Write the response. For T2: return after sending the response
   (session loop is T5). Flip the
   `t1_handle_upgrade_returns_not_yet_implemented_stub` regression-
   lock test to the T2 "handshake completes" version.
4. New KATs for T2: handshake response bytes (3-5 byte-equality
   locks against hand-derived RFC 6455 §1.3 examples), missing-key →
   400, wrong-version → 426 + correct header, missing-bearer → 401.

Target KAT delta for T2: +6-10.

## References

- Design: `docs/superpowers/specs/2026-05-26-kesseldb-spws-websocket-design.md` (707 lines)
- Scoping: `docs/superpowers/specs/2026-05-26-kesseldb-http2-ws-pgwire-scoping.md`
- Scaffold module: `crates/kessel-http-gateway/src/ws.rs`
- Crypto shim: `crates/kessel-http-gateway/src/crypto.rs`
- SHA-1 + base64 primitives: `crates/kessel-crypto/src/lib.rs`
- HTTP/1.1 parser (where T2 hooks in): `crates/kessel-http-gateway/src/parse.rs`
- Routes dispatch (where T2 wires the arm): `crates/kessel-http-gateway/src/routes.rs`
- SP141 follow-ups: `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md` (closes #4)
