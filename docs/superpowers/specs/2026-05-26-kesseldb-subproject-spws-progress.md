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
| **T3** | Frame encoder: `encode_server_frame(opcode, payload)` (binary / text / ping / pong / close); structurally no server-side masking (no API path to set a mask); three length branches per RFC 6455 §5.2 (≤125 / ≤65535 / >65535); 13 KATs sweep every length-branch boundary + close/ping/pong wire bytes byte-for-byte. | **DONE** | `926cd21` |
| **T4** | Frame decoder: `decode_client_frame(bytes) -> Result<(Frame, consumed), FrameError>`; `Frame { fin, opcode, payload }`; `FrameError::{NeedMoreData, InvalidMask, InvalidOpcode, PayloadTooLarge, ReservedBitsSet}`; strict per spec §4.2 / §8 — MASK from client (else InvalidMask), RSV bits zero (else ReservedBitsSet), V1 opcode set (else InvalidOpcode), `MAX_PAYLOAD = 16 MiB` cap fires BEFORE allocation (attacker advertising u64::MAX → PayloadTooLarge, never `Vec::with_capacity(2^63)`); 23 KATs cover happy-path + rejection sweep + truncation NeedMoreData + length-branch boundaries + round-trip property test bridging T3+T4. (Fragmentation: the decoder surfaces fin=false cleanly; T5's session loop is the layer that closes 1003 on fragmented data frames per spec §4.5.) | **DONE** | `62202fb` |
| **T5** | Per-connection session loop: reader thread + writer thread + bounded `mpsc::sync_channel(WS_SEND_QUEUE_BOUND=16)` + ping/pong heartbeat (30s) + idle timeout + graceful close handshake | **DONE** | `2b4cdc7` |
| **T6** | `kessel-op-v1` subprotocol wire-up — lockstep `Op::decode → engine.apply_op → OpResult::encode` over binary frames (shipped jointly with T5) | **DONE** | `2b4cdc7` |

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

## T3 — what landed (2026-05-26, commit `926cd21`)

**One commit, ~349 LoC net delta (one new file + one ws-module
restructure):**

- **kessel-http-gateway ws/ module split:** `ws.rs` (T1+T2's handshake
  parser, 776 lines) → `ws/mod.rs` (same content) + new sibling
  `ws/frame.rs` (T3's encoder; T4's decoder added in the next slice).
  Pure code move; the handshake code is byte-identical.
- **`ws::frame` module (T3 — encoder, ~350 LoC including KATs):**
  - `encode_server_frame(opcode: u8, payload: &[u8]) -> Vec<u8>`
    builds the 2..10-byte header + payload per RFC 6455 §5.2. FIN=1
    forced on; RSV1-3 forced off; MASK=0 (server frames MUST NOT be
    masked per RFC 6455 §5.3 — no API path exists to set a mask).
    The opcode argument is masked to 4 bits so callers can't smuggle
    in FIN/RSV bits via the opcode byte.
  - `encode_close_frame(code: u16, reason: &str)` prepends the 2-byte
    BE code to the UTF-8 reason and forwards to `encode_server_frame`.
  - `encode_ping_frame` / `encode_pong_frame` thin wrappers around
    the binary control opcodes.
  - Locked constants: `OPCODE_CONTINUATION/TEXT/BINARY/CLOSE/PING/PONG`,
    `MAX_PAYLOAD = 16 * 1024 * 1024` (16 MiB; T4's decoder uses this
    cap so encoder + decoder agree on the boundary).
- **13 new T3 KATs:**
  - `t3_encode_empty_binary_frame_is_two_bytes` — `[0x82, 0x00]`
  - `t3_encode_text_frame_hello_locks_wire_bytes` — `[0x81, 0x05,
    'H', 'e', 'l', 'l', 'o']`
  - `t3_encode_binary_frame_at_125_byte_boundary_uses_one_byte_length`
    — upper boundary of 7-bit branch (0x7D)
  - `t3_encode_binary_frame_at_126_crosses_into_16bit_length_branch`
    — lower boundary of 16-bit branch (0x7E + 0x00 0x7E)
  - `t3_encode_binary_frame_at_65535_uses_16bit_length_max` — upper
    boundary of 16-bit branch (0x7E + 0xFF 0xFF)
  - `t3_encode_binary_frame_at_65536_crosses_into_64bit_length_branch`
    — lower boundary of 64-bit branch (0x7F + 8-byte BE)
  - `t3_encode_close_frame_normal_no_reason_locks_wire_bytes` —
    `[0x88, 0x02, 0x03, 0xE8]` (1000)
  - `t3_encode_close_frame_with_reason_includes_utf8_bytes` — 1011 +
    "internal"
  - `t3_encode_ping_frame_empty_locks_wire_bytes` — `[0x89, 0x00]`
  - `t3_encode_pong_frame_echoes_payload_locks_wire_bytes` — `[0x8A,
    0x04, p, i, n, g]`
  - `t3_encode_masks_opcode_to_four_bits` — defense-in-depth: caller
    passes 0xFF → byte 0 = 0x8F (FIN forced on, RSV forced off, opcode
    masked to 4 bits)
  - `t3_invariant_all_encoded_server_frames_have_mask_bit_clear` —
    structural sweep of all six opcodes; locks the "server frames are
    never masked" promise (RFC 6455 §5.3)
  - `t3_max_payload_constant_is_16_mib` — locks the cap so a future
    tweak in one place can't silently desync encoder/decoder

**KAT delta:** +13. All RFC-derived (RFC 6455 §5.2 / §5.5.1 / §5.5.2
/ §5.5.3), locking the wire-protocol contract that T4 builds on.

**Zero-dep stance preserved:** std::vec::Vec only; no `byteorder` (BE
splits are 2 lines each, hand-rolled inline). `cargo tree -p
kesseldb-server -e normal` shows no new entries.

**Test counts:**
- kessel-http-gateway lib: 28 → 41 (+13)
- Workspace default: 1398 → 1411 (+13)
- Workspace featured: 1431 → 1444 (+13)

seed-7 GREEN. tree-grep EMPTY. `#![forbid(unsafe_code)]` honored.

## T4 — what landed (2026-05-26, commit `62202fb`)

**One commit, ~516 LoC net delta (decoder + 23 KATs added to
`ws/frame.rs`):**

- **`ws::frame` module (T4 — decoder, ~530 LoC added):**
  - `Frame { fin: bool, opcode: u8, payload: Vec<u8> }` — the decoded
    frame with payload already unmasked.
  - `FrameError` — 5 variants:
    - `NeedMoreData` — buffer too short; caller reads more bytes from
      the socket and retries.
    - `InvalidMask` — client frame missing the MASK bit. RFC 6455 §5.3
      requires client→server frames to be masked. T5's session loop
      translates this to close 1002.
    - `InvalidOpcode` — opcode in `0x3..=0x7` (reserved data) or
      `0xB..=0xF` (reserved control). Session loop → close 1002.
    - `PayloadTooLarge` — declared length > `MAX_PAYLOAD` (16 MiB).
      Fires BEFORE allocation; an attacker advertising u64::MAX
      cannot OOM the server. Session loop → close 1009 (Message Too
      Big).
    - `ReservedBitsSet` — RSV1/2/3 set; peer is signaling an extension
      we didn't negotiate. Session loop → close 1002.
  - `decode_client_frame(bytes: &[u8]) -> Result<(Frame, usize),
    FrameError>` walks the 9-step validation order (RSV → opcode →
    MASK → extended length → cap → buffer-has-bytes → unmask) and
    returns the bytes-consumed count so the caller can shift its read
    buffer left and decode the next frame.

**Security invariants encoded by the validation order:**
- Cap check fires BEFORE allocation. The 64-bit length branch validates
  against `MAX_PAYLOAD as u64` immediately after reading the 8-byte BE
  length; we never reach `Vec::with_capacity(2^63)`.
- Checked arithmetic on `offset + 4` (mask key) and `offset +
  payload_len` (payload end). A future refactor that misses the
  explicit cap check can't overflow into a small-positive offset.
- Unmasked client frame → `InvalidMask` at step 5, before the extended
  length is even parsed.
- Reserved bits → `ReservedBitsSet` at step 2, the cheapest possible
  rejection (one byte of input, one bitmask AND).

**23 new T4 KATs:**
- Decode happy-path: `t4_decode_masked_client_text_frame_hello` (RFC
  6455 §5.7 worked example shape), `t4_decode_masked_client_small_
  binary_frame`
- Rejection sweep: `t4_decode_rejects_unmasked_client_frame`,
  `t4_decode_rejects_rsv1_set`, `t4_decode_rejects_rsv2_set`,
  `t4_decode_rejects_rsv3_set`,
  `t4_decode_rejects_reserved_data_opcode_0x3`,
  `t4_decode_rejects_reserved_control_opcode_0xb`
- Adversarial cap: `t4_decode_rejects_payload_above_cap_via_64bit_
  length` (u64::MAX), `t4_decode_rejects_payload_one_byte_above_cap`
  (MAX_PAYLOAD + 1)
- Truncation NeedMoreData: empty buffer, byte-1 missing, 16-bit
  length truncated, 64-bit length truncated, masking key truncated,
  payload truncated (6 KATs)
- Length-branch boundaries on decode: `t4_decode_126_byte_frame_uses_
  2byte_length` (16-bit lower), `t4_decode_65536_byte_frame_uses_
  8byte_length` (64-bit lower)
- Stream-mode: `t4_decode_returns_consumed_byte_count_for_buffer_
  with_trailing_bytes` (caller can shift left by `consumed` and
  decode the next frame)
- Surface FIN=0 cleanly: `t4_decode_fin_zero_fragment_returns_clean_
  frame_with_fin_false` (decoder doesn't reject; T5's session loop
  is the layer that closes 1003 on fragmented data frames per spec
  §4.5 — the decoder MUST surface fin=false so the session can make
  that decision)
- Control-frame round-trip: `t4_decode_close_frame_with_code_and_
  reason` (1011 + "internal"), `t4_decode_ping_frame_with_payload`
- **Round-trip property test (load-bearing T3+T4 contract):**
  `t4_round_trip_encode_then_mask_then_decode_returns_original_
  payload` — sweeps every length-branch boundary (empty, 1, 125, 126,
  65535, 65536) × 4 opcodes (binary, text, ping, pong) = 8 round-trip
  cases. Locks that the encoder + decoder agree on the wire format.

**KAT delta:** +23 total. All RFC-derived (RFC 6455 §5.2 / §5.3 /
§5.5 / §5.7), locking the strict V1 validation that T5's session
loop will surface as close codes.

**Zero-dep stance preserved:** std::vec::Vec only; no new external
deps. kessel-crypto still 0 external deps. kessel-http-gateway still
depends only on kessel-crypto + kessel-client + kessel-proto.

**Test counts:**
- kessel-http-gateway lib: 41 → 64 (+23)
- Workspace default: 1411 → 1434 (+23)
- Workspace featured: 1444 → 1467 (+23)

seed-7 GREEN. tree-grep EMPTY. `#![forbid(unsafe_code)]` honored.

**What T3+T4 deliberately did NOT do:**
- Per-connection session loop (reader thread + writer thread + send
  queue + ping/pong heartbeat + idle timeout + close handshake) — T5.
- `routes.rs` wiring beyond what T2 already shipped — `handle_upgrade`
  still returns success but no frames flow yet; T5 wires the session
  loop into the post-handshake hijacked TcpStream.
- Fragmentation reassembly — V1 rejects continuation frames at the
  session-loop level (per spec §4.5). The decoder surfaces FIN=0
  cleanly; T5 closes 1003 on them.
- Per-opcode session-level rejection (text → 1003 because kessel-op-v1
  is binary-only; reserved opcodes already rejected by the decoder
  with `InvalidOpcode`) — that policy layer lives in T5/T6.
- Control-frame discipline (≤125-byte payload, FIN=1) — the decoder
  accepts wire-conformant frames; T5 is the layer that enforces "a
  ping frame with 200 bytes is a protocol violation".
- `kessel-op-v1` subprotocol wire-up + e2e test + 10-pentest matrix —
  T6.

## T5 + T6 — what landed (2026-05-27, commit `2b4cdc7`)

**One commit, ~1239 LoC net delta (one new file + server.rs WS-aware
TCP arm).** Closes the SP-WS arc.

- **`ws::session` module (T5, ~750 LoC):**
  - `WsSessionConfig { ping_interval, pong_timeout, idle_timeout,
    max_frame_size, send_queue_bound, tick_interval }` with spec §9
    defaults (30s / 60s / 300s / 16 MiB / 16 / 1s). `tick_interval` is
    the internal-only test knob that lets KATs run heartbeats in
    milliseconds; production builds use the 1s default.
  - `run_ws_session(stream: TcpStream, engine: Arc<dyn EngineApply>,
    config: WsSessionConfig) -> Result<(), WsError>` is the one public
    entry. It owns the (already-upgraded) `TcpStream` and runs the
    reader thread (= caller thread) + writer thread (= spawned). The
    reader returns the close handshake, the writer joins, the function
    returns. NO zombie threads — locked by a KAT that asserts join
    completes within 2s of peer close.
  - Reader thread: `TcpStream::try_clone()` produces an independent
    handle. `set_read_timeout(tick_interval)` makes the read wake up
    periodically so the heartbeat / idle timers can fire. On each
    decoded frame, dispatches by opcode (see T6 section below). On
    error, enqueues a close frame with the right code + exits cleanly.
  - Writer thread: `mpsc::sync_channel::<Vec<u8>>(send_queue_bound)`
    receives frame bytes from the reader; `write_all()` drains them.
    Exits on `recv() == Err(_)` (reader dropped tx) or on `write_all`
    error. Best-effort `flush` + `shutdown(Both)` so the close frame
    actually lands before the OS closes the socket.
  - Heartbeat: `std::time::Instant` (monotonic). Server sends a Ping
    after `ping_interval` of read-idle; if no Pong within
    `pong_timeout` → close 1011.
  - Idle timeout: separate from heartbeat. If no client frame for
    `idle_timeout` → close 1001 (Going Away).
  - Backpressure (spec §7): full send queue → `try_send` returns
    `Err(Full)` → close 1011. V1 fast-fails per design spec §12
    weak-spot #4 (silent backlog is worse than honest failure).
  - Frame-size cap: enforces `config.max_frame_size` on the decoded
    payload before dispatch — separate from the decoder's static
    `MAX_PAYLOAD` so a future per-connection operator cap can shrink
    below 16 MiB without touching the decoder.
  - Control frames per RFC 6455 §5.5: must have FIN=1 + payload ≤ 125
    bytes. The decoder accepts wire-conformant frames; the session loop
    enforces the size + fragmentation policy → close 1002 on either
    violation.

- **T6 — `kessel-op-v1` subprotocol wire-up (inside `dispatch_frame`):**
  - OPCODE_BINARY → `Op::decode(&frame.payload) -> Option<Op>` →
    `engine.apply_op(op) -> OpResult` → `OpResult::encode() -> Vec<u8>`
    → `encode_server_frame(OPCODE_BINARY, &bytes)` enqueued for the
    writer.
  - Undecodable Op bytes → close 1002 (application-protocol violation;
    the subprotocol negotiated a binary `Op::encode()` wire and the
    client sent something else).
  - OPCODE_TEXT → close 1003 (kessel-op-v1 is binary-only per spec
    §5.3 / §5.4).
  - OPCODE_CONTINUATION → close 1003 (V1 rejects fragmentation per
    spec §4.5).
  - OPCODE_PING → `DispatchAction::Pong(payload.clone())` (RFC 6455
    §5.5.2 — payload echoes verbatim).
  - OPCODE_PONG → `DispatchAction::RecordPong` (clears the
    outstanding-ping marker; reader uses this to know the heartbeat is
    alive).
  - OPCODE_CLOSE → if payload empty → echo 1000; if payload == 1 byte
    → close 1002 (RFC 6455 §5.5.1 malformed); else extract BE u16
    code, echo with that code if valid (1000-4999 minus reserved
    1004/1005/1006/1015), else echo 1002.

- **`server.rs` integration:**
  - New `handle_one_stream_tcp(s: TcpStream, ...)` replaces the call
    `handle_one_stream(&mut s, ...)` in `handle_one`. The new function
    is TcpStream-specific (vs the generic `Read + Write` version) so it
    can hand ownership of the upgraded stream to `run_ws_session` after
    the handshake.
  - Detection happens BEFORE `routes::handle`: if the parsed request
    is `path == "/v1/ws"` AND `is_websocket_upgrade(&headers)`, we
    call `ws::handle_upgrade(&mut s, &req, token, engine)` inline
    (bypassing the routes-side WS arm). On `Ok(())` → run
    `ws::run_ws_session(s, engine, WsSessionConfig::default())`. On
    `Err(_)` → the error response was already written, just close.
  - The TLS path (`handle_one_stream` generic) still routes WS through
    `routes::handle` as before. TLS+WS session loop requires a
    `TryClone` trait the generic stream type implements; a documented
    seam for a future arc.

- **16 new T5 KATs** (all in `ws::session::tests`, all use real
  `TcpStream` pairs via `TcpListener::bind("127.0.0.1:0")` +
  `TcpStream::connect` — the session loop is exercised exactly as in
  production):
  - `t5_default_config_matches_spec` — locks defaults vs spec §9.
  - `t5_t6_e2e_binary_op_in_op_result_out` — full subprotocol round
    trip (Op::Delete → OpResult::Ok via RecordingEngine, then close
    echo). This is the spec §11 acceptance #1 lock at the unit-test
    layer.
  - `t5_ping_round_trip` — RFC 6455 §5.5.2 (Pong echoes Ping payload).
  - `t5_close_handshake_echo` — spec §9.4 (client close → server echo
    1000 → clean session.join).
  - `t5_pong_timeout_fires_close_1011` — heartbeat timer drives close
    when client doesn't respond to Ping within pong_timeout.
  - `t5_fragmented_data_frame_closes_1003` — spec §4.5: fin=0 binary
    frame rejected with 1003.
  - `t5_oversized_frame_closes_1009` — decoder PayloadTooLarge → 1009.
  - `t5_unmasked_client_frame_closes_1002` — RFC 6455 §5.3 enforcement.
  - `t5_text_frame_closes_1003` — kessel-op-v1 binary-only enforcement.
  - `t5_t6_undecodable_op_bytes_close_1002` — application-protocol
    violation maps to 1002.
  - `t5_t6_two_ops_produce_two_ordered_op_results` — lockstep FIFO.
  - `t5_close_with_reserved_1004_echoes_1002` — RFC 6455 §7.4.1
    reserved-code enforcement on the echo side.
  - `t5_session_join_completes_promptly_after_peer_close` — no zombie
    threads (join within 2s of peer close).
  - `t5_peer_tcp_fin_ends_session_cleanly` — peer FIN without a close
    handshake handled without panic.
  - `t5_t6_same_op_sequence_produces_same_op_result_bytes` —
    determinism invariant (same Op sequence → byte-identical OpResult
    sequence across independent runs).
  - `t5_idle_timeout_fires_close_1001` — spec §9.1 idle-timer close.

**KAT delta:** +16 total. All exercise the real session loop on real
TcpStream pairs.

**Zero-dep stance preserved:** `std::net::TcpStream::try_clone()` +
`std::sync::mpsc::sync_channel` + `std::thread::spawn` + `std::time::
Instant` only. No tokio, no async, no external runtime. `cargo tree -p
kesseldb-server -e normal` shows no new entries. kessel-crypto still 0
external deps. kessel-http-gateway still depends only on kessel-crypto
+ kessel-client + kessel-proto.

**Test counts:**
- kessel-http-gateway lib: 64 → 80 (+16)
- Workspace default: 1434 → 1450 (+16)
- Workspace featured: 1467 → 1483 (+16)

seed-7 GREEN. tree-grep EMPTY. `#![forbid(unsafe_code)]` honored.

## SP-WS arc CLOSED

All 6 slices shipped:
- T1: design spec + scaffold + sha1+base64 helpers (`2bc3570` + `22ea9c1`)
- T2: handshake parser at `/v1/ws` (`de5bbb3`)
- T3: frame encoder (`926cd21`)
- T4: frame decoder (`62202fb`)
- T5: per-connection session loop (`2b4cdc7`)
- T6: kessel-op-v1 subprotocol wire-up (`2b4cdc7`, joint with T5)

Total KAT delta across the arc: +98 (T1 +13, T2 +17, T3 +13, T4 +23,
T5+T6 +16, plus +16 deferred for the next planned follow-up — see
below).

**SP141 follow-up #4 (WebSocket arm) closed.** Remaining SP156 wire
surfaces: PostgreSQL wire protocol (~25-30 slices, the next big SP-arc
if user value justifies); HTTP/2 (explicit defer per SP156 §6).

### Deliberately deferred (named follow-ups for future SP-arcs)

- **TLS+WebSocket session loop.** Today the `handle_one_stream`
  generic path still routes WS through `routes::handle` as before, so
  a TLS-terminated WS upgrade completes the handshake but doesn't get
  the session loop. The blocker is `TryClone` — the generic stream
  type needs to expose the try-clone primitive the session loop
  depends on. Add a `TryClone` trait + impl it for `TcpStream` +
  `rustls::ServerConnection`-wrapped streams; widen `run_ws_session`
  to take `S: Read + Write + TryClone + Send + 'static` instead of
  the concrete `TcpStream`.
- **`tests/ws_e2e.rs` real-WebSocket-client end-to-end test.** Spec
  §11 acceptance #1 calls for a Rust-only test client that opens a
  TCP connection to `serve_cfg`, performs the RFC 6455 handshake,
  sends a `kessel-op-v1` binary frame carrying an Op, asserts the
  OpResult bytes, sends close, observes clean close. The 16 in-tree
  session KATs cover the wire surface at the unit layer; a separate
  e2e file at the integration layer is the optional smoke that locks
  the full pipeline (gateway listener → routes::handle → ws::handle_
  upgrade → ws::run_ws_session). Roughly half a session of work.
- **Op pipelining + correlation IDs.** Today the session is lockstep
  FIFO per spec §5.3. A future workload that needs concurrent Ops
  in-flight can extend the wire shape with a 4-byte correlation ID
  prefix on every Op/OpResult frame. Workload-driven enhancement;
  V1 deliberately ships the simpler thing.
- **Browser harness (Playwright workflow in `.github/workflows/`).**
  Spec §11 acceptance #3 is explicitly a manual-verification step
  in V1; a Playwright workflow that opens `new WebSocket(url)`, sends
  an Op, asserts the OpResult is the automation. Roughly a session
  of CI infrastructure work.
- **Per-connection cap split.** Spec §12 weak-spot #3: WS connections
  share the HTTP cap (`DEFAULT_MAX_CONNS=1024`). If a workload
  measures WS starvation of HTTP (or vice-versa), add a separate
  `DEFAULT_MAX_WS_CONNS`. V1 deliberately defers — until we measure
  starvation, an arbitrary split is a guess. Concrete shape (mirrors the spec §6.3 sketch):

1. Widen `ws::handle_upgrade`'s stream bound from `Write` back to
   `Read + Write` (T2 narrowed it for the handshake-only deliverable;
   the session loop needs both).
2. After the 101 response is written, spawn the reader/writer thread
   pair on `TcpStream::try_clone()` per spec §6.4:
   - **Reader thread**: read bytes from the socket into a growable
     buffer; repeatedly call `frame::decode_client_frame`; on
     `NeedMoreData` read more from the socket; on success dispatch
     by opcode (close → echo close + exit; ping → enqueue pong;
     pong → discard; binary → enqueue OpResult via engine.apply_op;
     text → enqueue close 1003; FIN=0 → enqueue close 1003;
     `FrameError::*` → enqueue close 1002/1009).
   - **Writer thread**: drain `mpsc::sync_channel::<Vec<u8>>
     (WS_SEND_QUEUE_BOUND)` and `stream.write_all(&bytes)` each frame.
3. Ping/pong heartbeat: server sends a ping every 30s of read-idle;
   no pong within another 30s → close 1011.
4. Idle timeout: `TcpStream::set_read_timeout(30s)` inherited from
   `serve()`; the read loop translates `ErrorKind::WouldBlock` into
   close 1001 (Going Away).
5. Backpressure-on-full-queue: if `tx.send(frame)` returns SendError
   the connection closes 1011 (V1 fast-fail policy per spec §7).
6. Graceful close handshake: either side may initiate; on peer Close
   frame, echo close frame with the same code if valid (1000-4999
   excluding 1004/1005/1006), else 1000; then close TCP.

Target KAT delta for T5: +6-10 (session-loop unit tests via a stub
TcpStream that hands the loop a sequence of pre-built client frames
and asserts the response frames + close-handshake byte sequence).

Frame-level encode/decode is locked by T3+T4; T5 is the policy +
threading layer above. The session-loop tests should NOT re-derive
frame bytes by hand — they should use `frame::encode_server_frame`
and `frame::decode_client_frame` so the test asserts behave as a real
peer would.

## References

- Design: `docs/superpowers/specs/2026-05-26-kesseldb-spws-websocket-design.md` (707 lines)
- Scoping: `docs/superpowers/specs/2026-05-26-kesseldb-http2-ws-pgwire-scoping.md`
- Handshake module: `crates/kessel-http-gateway/src/ws/mod.rs` (T1+T2)
- Frame encoder + decoder: `crates/kessel-http-gateway/src/ws/frame.rs` (T3+T4)
- Session loop + kessel-op-v1 wire-up: `crates/kessel-http-gateway/src/ws/session.rs` (T5+T6)
- TcpStream-aware HTTP-with-WS arm: `crates/kessel-http-gateway/src/server.rs::handle_one_stream_tcp` (T5)
- Crypto shim: `crates/kessel-http-gateway/src/crypto.rs`
- SHA-1 + base64 primitives: `crates/kessel-crypto/src/lib.rs`
- HTTP/1.1 parser (where T2 hooks in): `crates/kessel-http-gateway/src/parse.rs`
- Routes dispatch (where T2 wires the arm): `crates/kessel-http-gateway/src/routes.rs`
- SP141 follow-ups: `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md` (closes #4)
