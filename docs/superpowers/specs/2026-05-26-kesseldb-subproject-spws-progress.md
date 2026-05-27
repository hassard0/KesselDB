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
| **T2** | Handshake parser: validate `Sec-WebSocket-Key` (base64 → 16 bytes) / `Sec-WebSocket-Version: 13` (wrong-version 400 + version hint) / path=/v1/ws / GET-only (POST → 405) / Authorization (token-mode 401) / `Sec-WebSocket-Protocol` (negotiate kessel-op-v1 case-insensitively, echo canonical constant, only-unknown → 400, absent → omit); build the `HTTP/1.1 101 Switching Protocols` response bytes per RFC 6455 §4.2.2 (locked byte-for-byte against §1.3 canonical example); wire `routes.rs::handle` arm with `close_after = true`; add `kessel-crypto::base64_decode` for key validation; flip the T1 regression-lock KAT to "handshake completes" (`t2_successful_handshake_returns_101_with_canonical_accept`). | **DONE** | `de5bbb3` |
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

## T2 — what landed (2026-05-26, commit `de5bbb3`)

**One commit, ~615 LoC net delta across 4 files:**

- **kessel-crypto (+3 KATs, ~70 LoC):**
  - `base64_decode()` per RFC 4648 — strict standard-alphabet decoder
    that returns `None` for any malformed input (wrong length, URL-safe
    `-_` chars, embedded whitespace, misplaced pads). Used by the
    handshake parser to validate that `Sec-WebSocket-Key` base64-
    decodes to exactly 16 bytes per RFC 6455 §4.1.
  - +3 KATs: `base64_decode_rfc4648_vectors_round_trip` (every §10
    encode vector decodes back to the original), `base64_decode_rejects_malformed_inputs`
    (8 distinct rejection shapes locked), `base64_decode_rfc6455_sample_key_is_16_bytes`
    (the canonical handshake key `dGhlIHNhbXBsZSBub25jZQ==` decodes
    to `the sample nonce` = 16 bytes).
- **kessel-http-gateway parse.rs (~10 LoC):**
  - `is_known_path` now recognizes `/v1/ws` (was 404 before). With
    a defense-in-depth comment explaining that the upgrade arm in
    `routes::handle` gates on `is_websocket_upgrade(&headers)`, so a
    plain `GET /v1/ws` (no Upgrade header) still falls through to the
    catch-all 404 — only a true upgrade attempt reaches the WS handler.
- **kessel-http-gateway routes.rs (~30 LoC):**
  - New upgrade arm BEFORE the path table: `if req.path ==
    ws::WEBSOCKET_PATH && ws::is_websocket_upgrade(&req.headers)` →
    call `ws::handle_upgrade(w, req, token, engine)` and return
    `Ok(true)` (`close_after = true`). Both the success path (stream
    is no longer HTTP) AND the failure path (error response carried
    `Connection: close`) require the HTTP keep-alive loop to exit.
- **kessel-http-gateway ws.rs (+14 KATs, ~430 LoC):**
  - **Real `handle_upgrade` implementation** replaces the T1 placeholder:
    - GET-only (POST/PUT/DELETE → 405)
    - Auth FIRST per `routes::handle` parity — token mode: Bearer
      mismatch / missing → 401 with `Connection: close` body
    - Defense-in-depth re-validation of `Upgrade: websocket` +
      `Connection: upgrade` (the routes-side `is_websocket_upgrade` is
      the fast gate; this is the slow gate that produces a clean 400
      if the routes-side gate is bypassed by a future refactor)
    - `Sec-WebSocket-Version: 13` validation — wrong/absent → 400 with
      `Sec-WebSocket-Version: 13` response header so the client knows
      which version we speak (RFC 6455 §4.4)
    - `Sec-WebSocket-Key` present + `kessel_crypto::base64_decode`
      yields exactly 16 bytes → else 400
    - `Sec-WebSocket-Protocol` negotiation per spec §5.1 / §5.2: header
      absent → omit from response (default kessel-op-v1 semantics);
      header present + contains kessel-op-v1 (case-insensitive match)
      → echo the LOCKED canonical constant (NOT the client's raw
      casing); header present + offers exist + none known → 400
  - **101 response is byte-correct vs RFC 6455 §4.2.2 canonical
    example**: status line + `Upgrade: websocket` + `Connection:
    Upgrade` + `Sec-WebSocket-Accept` (via the existing T1
    `sec_websocket_accept(key)`) + optional `Sec-WebSocket-Protocol`
    + bare CRLF terminator. NO `Content-Length`, NO `Server` header —
    those bytes would be interpreted as the first WebSocket frame
    payload by a strict client.
  - `WsError` enum widened: `HandshakeFailed(u16)` + `Io(ErrorKind)`
    replace the T1 `NotYetImplemented` sentinel. The T1 stub
    regression-lock is REMOVED and replaced by
    `t2_successful_handshake_returns_101_with_canonical_accept` which
    locks the response byte-for-byte against the RFC §1.3 canonical
    example (client key `dGhlIHNhbXBsZSBub25jZQ==` → accept
    `s3pPLMBiTxaQ9kYGzzhZRbK+xOo=`).
  - Stream-type bound relaxed from `Read + Write` to `Write` (T2 only
    writes; doc-comment notes T5 will widen back for the session loop).
  - +14 KATs:
    - +1 constant lock: `websocket_version_constant_is_13_per_rfc6455`
    - +12 T2 handshake KATs: successful canonical handshake (locks
      status line + headers + canonical accept + no Content-Length +
      bare CRLF terminator + no Sec-WebSocket-Protocol when none
      offered); missing key → 400; non-16-byte key → 400; wrong
      version → 400 + version hint header; missing Upgrade → 400;
      missing Connection: upgrade → 400; Bearer mismatch → 401;
      missing Bearer → 401; matching Bearer → 101; subprotocol
      offered + accepted → echoed canonical constant; subprotocol
      header present + only-unknown → 400; subprotocol match is
      case-insensitive; POST method → 405
    - +1 explicit negative invariant: `t2_no_subprotocol_offered_response_omits_header`

**KAT delta:** +17 total (+3 kessel-crypto + +14 gateway/ws). All
RFC-derived (RFC 6455 §1.3 + §4.1 + §4.2.2 + RFC 4648 §10),
locking the wire-protocol contract that T3-T5 build on.

**Zero-dep stance preserved:** no new external deps. kessel-crypto
stays at 0 external deps. kessel-http-gateway still depends only on
kessel-crypto + kessel-client + kessel-proto. `cargo tree -p
kesseldb-server -e normal` shows no new entries.

**Test counts:**
- kessel-crypto: 6 → 9 (+3)
- kessel-http-gateway: 14 → 28 (+14)
- Workspace default: 1381 → 1398 (+17)
- Workspace featured: 1414 → 1431 (+17)

seed-7 GREEN. tree-grep EMPTY. `#![forbid(unsafe_code)]` honored
throughout the new code. All prior tests pass.

**What T2 deliberately did NOT do:**
- Frame encoder — T3.
- Frame decoder — T4.
- Per-connection session loop (reader thread + writer thread + ping/
  pong heartbeat + idle timeout + close handshake) — T5.
- `kessel-op-v1` subprotocol wire-up + e2e test + 10-pentest matrix
  — T6.

**Post-T2 behavior:** a WebSocket client can connect to `/v1/ws` and
receive a correct 101 response. After 101 the server writes nothing
further — the stream is open but blocks on read (no session loop
yet). The client either gets a clean close when the gateway drops
the connection, or its first frame send is ignored. That's T2's
intended deliverable per the design spec §10 row "T2: YES — handshake
completes".

## Pickup hint for the next session

T3 is the next slice — **frame encoder** per RFC 6455 §5. Concrete
shape (mirrors the spec §4.3 target API):

1. New `ws::frame` module (~150 LoC). Public surface:
   ```rust
   pub enum Opcode { Continuation, Text, Binary, Close, Ping, Pong }
   pub fn encode_server_frame(opcode: Opcode, payload: &[u8]) -> Vec<u8>;
   pub fn encode_close_frame(code: u16, reason: &str) -> Vec<u8>;
   pub fn encode_ping_frame(payload: &[u8]) -> Vec<u8>;
   pub fn encode_pong_frame(payload: &[u8]) -> Vec<u8>;
   ```
   Server-side frames MUST NOT be masked (spec §4.3 / RFC 6455 §5.3):
   the encoder structurally enforces this — no masked variant exists
   for server-side encoding.
2. Frame header per spec §4.1 RFC 6455 §5.2: FIN=1, RSV1/2/3=0, 4-bit
   opcode, MASK=0, 7-bit length OR 7+16-bit OR 7+64-bit extended
   length, payload. Three length branches:
   - payload.len() ≤ 125: 1-byte length
   - 126 ≤ payload.len() ≤ 65535: 0x7E + 2-byte BE length
   - payload.len() > 65535: 0x7F + 8-byte BE length
3. KATs (target +6-8):
   - Empty binary frame: `[0x82, 0x00]` (FIN | binary, length 0)
   - 5-byte binary frame: `[0x82, 0x05, ...payload]`
   - 200-byte binary frame: `[0x82, 0x7E, 0x00, 0xC8, ...payload]`
     (16-bit length branch)
   - 70000-byte binary frame: `[0x82, 0x7F, 0x00, 0x00, 0x00, 0x00,
     0x00, 0x01, 0x11, 0x70, ...payload]` (64-bit length branch)
   - Ping frame with empty payload: `[0x89, 0x00]`
   - Pong frame echoes payload: `[0x8A, len, ...payload]`
   - Close frame with code 1000 (Normal): `[0x88, 0x02, 0x03, 0xE8]`
   - Close frame with code + reason: `[0x88, len, 0xMM, 0xNN, ...utf8]`

Target KAT delta for T3: +6-8.

Frame encoding is structurally simpler than the handshake (no header
parsing, just byte layout) but the length-branch boundaries are the
load-bearing surface; the KATs above sweep each one.

## References

- Design: `docs/superpowers/specs/2026-05-26-kesseldb-spws-websocket-design.md` (707 lines)
- Scoping: `docs/superpowers/specs/2026-05-26-kesseldb-http2-ws-pgwire-scoping.md`
- Scaffold module: `crates/kessel-http-gateway/src/ws.rs`
- Crypto shim: `crates/kessel-http-gateway/src/crypto.rs`
- SHA-1 + base64 primitives: `crates/kessel-crypto/src/lib.rs`
- HTTP/1.1 parser (where T2 hooks in): `crates/kessel-http-gateway/src/parse.rs`
- Routes dispatch (where T2 wires the arm): `crates/kessel-http-gateway/src/routes.rs`
- SP141 follow-ups: `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md` (closes #4)
