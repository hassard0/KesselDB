# SP-WS — WebSocket support for the KesselDB HTTP gateway — DESIGN

**Status:** design — scopes the WebSocket V1 work into ~5-6 task slices and
locks the wire-protocol invariants that the implementation tasks will
de-risk one at a time. Companion progress tracker at
`docs/superpowers/specs/2026-05-26-kesseldb-subproject-spws-progress.md`.

**Builds on:**
- **SP156 scoping** (`docs/superpowers/specs/2026-05-26-kesseldb-http2-ws-pgwire-scoping.md`)
  — picked WebSocket as the next wire surface (~5-6 tasks) ahead of PG wire
  (~25-30) and HTTP/2 (defer). This spec is the next-level-down of SP156 §3
  + §7.1 + §8.1.
- **SP141 / SP144H / SP147 / SP148** — HTTP/1.1 gateway shape. SP-WS hangs
  off the *same* `serve()` accept loop and the *same* `parse_request`
  parser; the upgrade dance is an additive arm in `routes.rs::handle`, not
  a new listener.
- **SP-A T6 / T7** — the per-connection `Arc<AtomicBool>` cancel + bounded
  `mpsc::sync_channel(SHARD_BACKPRESSURE_BOUND=4)` pattern. WebSocket's
  per-connection send-queue + idle-timeout close reuses the same shape
  (the constant name will differ — `WS_SEND_QUEUE_BOUND` — but the
  rationale is identical).
- **Existing crypto helpers:** `kessel-objstore::b64` (base64 encode +
  decode) reused for `Sec-WebSocket-Accept`. **SHA-1 does NOT exist in
  the workspace yet** — this spec adds it. (`kessel-objstore` uses
  HMAC-SHA-256 for SigV4; AWS does not use SHA-1.) The new `crypto.rs`
  helper module ships under `kessel-http-gateway` (the only consumer in
  V1) with the same zero-dep stance as the rest of the workspace.

---

## 1. Context — why WebSocket

SP156 §3 picked WebSocket as the right next wire surface for three
concrete user-value drivers, in declining order of immediacy:

1. **Browser-direct clients.** Browsers cannot open raw TCP sockets — the
   binary wire (`kessel-proto`) is unreachable from JavaScript. The only
   persistent bidirectional channel a browser can open is a WebSocket.
   Without SP-WS, every browser UI (a future query console, a future
   live-metrics dashboard, a future row-stream viewer for SP-A scans)
   has to relay through an app-server middle tier. SP-WS removes that
   tier.

2. **Push-style notifications.** Today the HTTP gateway is strict
   request/response: a client sends an `Op`, gets one `OpResult`, the
   request is done. A future "subscribe to this table" / "watch this
   metric" surface (changefeed-flavored) needs server→client push.
   Polling `/v1/sql` is wasteful and racy. SSE (HTTP/1.1
   `text/event-stream`) is server→client only; WebSocket is the
   bidirectional answer.

3. **Streaming scan results.** SP-A scatter-scans (router-side) buffer
   the entire merged result before emitting one `OpResult::Got(...)`. For
   a `LIMIT 1M` against a wide table that's a real materialization step;
   for a `LIMIT` cancellation against a slow shard that's wasted work.
   WebSocket framing lets the server emit one row-frame at a time and a
   client send `cancel` mid-scan. SP-WS V1 ships request/response only
   (one `Op` frame → one `OpResult` frame); the streaming variant is a
   named follow-up arc (SP-A T14 in the SP-A progress doc — currently
   gated on SP-WS landing for the wire surface).

The cost case for WebSocket (vs HTTP/2 vs PG-wire vs HTTP/3) is laid out
in SP156 §6 — WebSocket scores low-medium implementation cost,
medium-high user value, good zero-dep fit. SP156's recommendation was
"yes, next", and SP-A closing freed up the SP-arc slot.

## 2. Scope

### 2.1 V1 — what's in

1. **RFC 6455 §4 handshake** — `Upgrade: websocket` + `Connection:
   Upgrade` + `Sec-WebSocket-Key` + `Sec-WebSocket-Version: 13` →
   `HTTP/1.1 101 Switching Protocols` + `Upgrade: websocket` +
   `Connection: Upgrade` + `Sec-WebSocket-Accept: <base64(sha1(key +
   magic))>`. Strict: reject any version other than `13`, reject any
   non-GET method, reject any path other than `/v1/ws`, reject a
   missing/malformed `Sec-WebSocket-Key`.
2. **RFC 6455 §5 frame parser + encoder.** Vanilla frames only — FIN
   bit, 4-bit opcode (text 0x1, binary 0x2, close 0x8, ping 0x9, pong
   0xA, continuation 0x0), MASK bit + 4-byte mask (client→server frames
   MUST be masked; server→client MUST NOT — V1 enforces both
   directions), 7-bit / 16-bit / 64-bit length. RSV1/2/3 MUST be zero
   (no extensions).
3. **Subprotocol negotiation** — V1 advertises exactly one subprotocol:
   `kessel-op-v1` (binary). Handshake requests carrying
   `Sec-WebSocket-Protocol: kessel-op-v1` get it echoed back. Requests
   carrying nothing (or a list missing `kessel-op-v1`) get the
   subprotocol header omitted — RFC 6455 §1.3 — and the connection
   defaults to `kessel-op-v1` semantics anyway. Requests carrying ONLY
   subprotocols we don't know are rejected with `400 Bad Request` at
   handshake time.
4. **Message format under `kessel-op-v1`** — every client→server message
   is a single binary frame carrying `Op::encode()` bytes. Every
   server→client message is a single binary frame carrying
   `OpResult::encode()` bytes. NO JSON over the WebSocket in V1 (we have
   `/v1/sql` + `/v1/op` for JSON consumers; the WebSocket is the
   binary-wire-via-browser path).
5. **Per-connection session loop** — one std::thread per WebSocket
   connection (mirrors `handle_one_stream`). Read a frame, decode an
   `Op`, dispatch to the existing `EngineApply`, encode the `OpResult`
   into a binary frame, write the frame, loop. Same `Arc<dyn
   EngineApply>` the HTTP routes use. Same `Authorization: Bearer` token
   check (lifted from the upgrade request's headers, ct-eq'd once at
   handshake, NOT re-checked per frame — the open connection itself
   represents the auth grant).
6. **Control frames** — Ping (0x9) gets an immediate Pong (0xA) with
   identical payload. Pong (0xA) is consumed and discarded. Close (0x8)
   triggers the close handshake (echo close frame with the same status
   code if peer sent one in the well-formed range, else echo 1000
   Normal; then close TCP). Ping/pong heartbeat: server sends a Ping
   every 30s of read-idle; if no Pong arrives within another 30s the
   server closes with code 1011.
7. **Idle timeout** — the underlying TcpStream keeps its
   `set_read_timeout(30s)` from `serve()`, so a connection that hasn't
   read any frame in 30s gets caught at the read boundary. (This
   complements the ping/pong heartbeat — they fire on different
   conditions.)
8. **Backpressure** — bounded `mpsc::sync_channel(WS_SEND_QUEUE_BOUND =
   16)` per-connection send queue (the engine-apply thread sends frames
   into the queue; a dedicated writer thread drains it to the socket).
   Bound chosen larger than SP-A's `SHARD_BACKPRESSURE_BOUND=4` because
   per-message latency dominates per-message size and we want to absorb
   ping/pong + close traffic alongside data. If the queue is full and a
   sender attempts to `send`, the connection closes with code 1011
   (Internal Server Error) — V1 prefers fast-fail to silent backlog.
9. **Same Bearer auth as HTTP** — token mode (when `ServerConfig.token`
   is set) requires `Authorization: Bearer <token>` in the upgrade
   request, ct-eq'd once. Open mode (no token) accepts any handshake.
10. **Frame size cap** — every server-decoded frame's payload length is
    capped at `http_max_body` (default 8 MiB, configurable via
    `ServerConfig.http_max_body`). Oversize → close with code 1009
    (Message Too Big).

### 2.2 V1 — what's out (named, deferred)

These are NOT V1 features. Each is a named follow-up so future scoping
finds the design call without re-litigating:

- **Permessage-deflate** (RFC 7692 compression extension). V1 advertises
  no extensions. Hard pass on the LZ77 sliding-window state. A future
  `SP-WS-DEFLATE` arc can revisit.
- **Fragmented messages** (continuation frames, opcode 0x0). V1
  rejects: every data frame MUST have `FIN = 1`. Browsers don't
  fragment by default; tools that do (e.g. some `websocat` flags) get a
  close-1003 (Unsupported Data). Follow-up if a real consumer needs
  fragmentation.
- **Streaming row responses.** V1 is one `Op` frame → one `OpResult`
  frame. Streaming (many `OpResultChunk` frames between a request and a
  terminal `OpResultDone`) is the SP-A T14 follow-up arc.
- **Cookie / first-message auth.** V1 requires the `Authorization:
  Bearer` header on the upgrade request. Browsers can set `Authorization`
  via JS for non-WebSocket fetches but cannot for WebSocket constructors
  (the spec is hostile to it). A future `kessel-op-v1-cookie` subprotocol
  variant can carry first-message auth, but V1 deliberately does NOT
  ship that — operators who need browser-direct cookie auth use a
  reverse proxy to inject Bearer headers.
- **Subprotocols other than `kessel-op-v1`** — a future `kessel-op-v1-json`
  (text frames carrying JSON `Op` shape) is plausible but defer.
- **HTTP/2 + WebSocket** (RFC 8441 — bootstrap-WebSocket-over-h2). H2
  itself is deferred per SP156; this composes with that decision.
- **Per-frame auth replay protection.** The session-bind headers
  `X-Kessel-Client-Id` + `X-Kessel-Req-Seq` are HTTP-layer concepts; V1
  WebSocket does not promote them into the frame protocol. If exactly-once
  semantics matter, the binary wire's session model already carries them
  inside `Op`.

## 3. Wire protocol — RFC 6455 sections that apply

| RFC 6455 section | Topic | V1 disposition |
|---|---|---|
| §1.3 | Subprotocol negotiation (`Sec-WebSocket-Protocol`) | Negotiate one: `kessel-op-v1` |
| §4.1 | Client handshake | Validate strictly: method=GET, version=13, key well-formed, path=/v1/ws |
| §4.2.1 | Server-side handshake checks | Reject malformed → 400; reject wrong version → 426 + `Sec-WebSocket-Version: 13` |
| §4.2.2 | Server response (101 Switching Protocols) | Hand-built response writer; locked by KAT |
| §5.1 | Overview of framing | Frame format = 2..14 byte header + payload |
| §5.2 | Base framing protocol | Decode FIN, RSV1-3, opcode, MASK, length, mask-key, payload |
| §5.3 | Client→server masking | Client frames MUST be masked; server enforces |
| §5.4 | Fragmentation | V1 rejects continuation (close 1003) |
| §5.5 | Control frames (close/ping/pong) | All three handled |
| §5.5.1 | Close (0x8) | 2-byte status code + UTF-8 reason; valid close codes 1000-4999 |
| §5.5.2 | Ping (0x9) | Echo as Pong with same payload |
| §5.5.3 | Pong (0xA) | Consume + discard |
| §5.6 | Data frames | Binary (0x2) — V1 uses; text (0x1) — V1 rejects |
| §5.8 | Extensibility (RSV bits) | RSV1/2/3 MUST be zero (no extensions) |
| §7.1 | Closing handshake | Send close frame; wait for peer close; close TCP |
| §7.4 | Status codes | V1 uses 1000 (Normal), 1001 (Going Away), 1002 (Protocol Error), 1003 (Unsupported Data), 1008 (Policy Violation), 1009 (Message Too Big), 1011 (Internal Error) |
| §10.3 | Attacks on infrastructure | Masking + same-origin not required for non-browser; we still enforce mask-from-client |

RFC 6455 §10 (security considerations) is the threat model SP-WS T6's
pentest matrix is derived from.

## 4. Frame implementation — the zero-dep subset

### 4.1 Frame header layout

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-------+-+-------------+-------------------------------+
|F|R|R|R| opcode|M| Payload len |    Extended payload length    |
|I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
|N|V|V|V|       |S|             |   (if payload len==126/127)   |
| |1|2|3|       |K|             |                               |
+-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
|     Extended payload length continued, if payload len == 127  |
+ - - - - - - - - - - - - - - - +-------------------------------+
|                               |Masking-key, if MASK set to 1  |
+-------------------------------+-------------------------------+
| Masking-key (continued)       |          Payload Data         |
+-------------------------------- - - - - - - - - - - - - - - - +
:                     Payload Data continued ...                :
+ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
|                     Payload Data continued ...                |
+---------------------------------------------------------------+
```

### 4.2 Decoder API (target shape for SP-WS T4)

```rust
pub enum Opcode {
    Continuation, // 0x0 — V1 rejects
    Text,         // 0x1 — V1 rejects (binary subprotocol)
    Binary,       // 0x2
    Close,        // 0x8
    Ping,         // 0x9
    Pong,         // 0xA
    // 0x3..=0x7 reserved data frames — V1 rejects
    // 0xB..=0xF reserved control frames — V1 rejects
}

pub struct Frame {
    pub fin: bool,
    pub opcode: Opcode,
    pub payload: Vec<u8>,
}

pub enum DecodeError {
    Incomplete,                    // need more bytes
    ReservedBitSet,                // RSV1/2/3 ≠ 0
    UnmaskedClientFrame,           // server-side: MUST be masked
    ReservedOpcode(u8),            // 0x3..=0x7 or 0xB..=0xF
    ControlFrameTooLarge,          // control frames must fit in 125 bytes
    ControlFrameFragmented,        // control frames must have FIN=1
    FragmentedDataFrame,           // V1 rejects fragmentation
    PayloadTooLarge(u64),          // > max_payload
    BadCloseFrame,                 // close with 1 byte (must be 0 or ≥2)
}

pub fn decode_frame_from_client(
    buf: &[u8],
    max_payload: usize,
) -> Result<(Frame, usize /* consumed */), DecodeError>;
```

### 4.3 Encoder API (target shape for SP-WS T3)

```rust
pub fn encode_server_frame(opcode: Opcode, payload: &[u8]) -> Vec<u8>;
// Server frames MUST NOT be masked (RFC 6455 §5.3); the encoder
// hard-enforces this — no masked variant exists for the server side.

pub fn encode_close_frame(code: u16, reason: &str) -> Vec<u8>;
pub fn encode_ping_frame(payload: &[u8]) -> Vec<u8>;
pub fn encode_pong_frame(payload: &[u8]) -> Vec<u8>;
```

### 4.4 Mask XOR

```rust
fn unmask_in_place(payload: &mut [u8], key: [u8; 4]) {
    for (i, b) in payload.iter_mut().enumerate() {
        *b ^= key[i & 3];
    }
}
```

Simple, byte-by-byte. A real implementation could optimize via SIMD or
word-at-a-time XOR — V1 does not (zero-dep + clarity-first). For an 8
MiB max payload this is one cache-line-friendly walk; no real-world
SQL load is mask-XOR-bound.

### 4.5 What we deliberately don't implement

- **Fragmented decode.** A frame with `FIN = 0` (or opcode 0x0
  continuation) surfaces `DecodeError::FragmentedDataFrame` → session
  closes with 1003. We are NOT reassembling cross-frame messages in V1.
- **Permessage-deflate / RSV1 = compressed.** Any `RSV1/2/3 = 1` →
  `DecodeError::ReservedBitSet` → close 1002.
- **Server-side mask SEND.** Server frames never carry a mask. The
  encoder API has no parameter for it.
- **Per-message MAX-budget across fragments.** Since we reject
  fragmentation, the max-payload check at the single-frame boundary is
  the entire budget — simpler and stricter than per-message-across-
  fragments accounting.

## 5. Subprotocol — `kessel-op-v1`

### 5.1 Negotiation

Client handshake includes `Sec-WebSocket-Protocol: kessel-op-v1`
(possibly alongside other names in a comma-separated list per RFC 6455
§4.1). Server echoes back `Sec-WebSocket-Protocol: kessel-op-v1` if the
list contains it; otherwise responds with NO `Sec-WebSocket-Protocol`
header and the connection defaults to `kessel-op-v1` semantics anyway
(see §5.2 below for why we don't reject the empty-protocol case — it's
the path browsers take when constructing `new WebSocket(url)` without
naming a protocol). If the list is non-empty AND contains zero known
protocols, reject with `400 Bad Request` at handshake.

### 5.2 Default semantics under no-protocol-named handshakes

Browser code commonly writes:

```js
const ws = new WebSocket("wss://kesseldb.example/v1/ws");
ws.binaryType = "arraybuffer";
ws.send(opBytes);
ws.onmessage = (e) => handleResult(new Uint8Array(e.data));
```

…which sends no `Sec-WebSocket-Protocol` header at all. Rejecting that
would force every consumer to name a subprotocol they don't otherwise
care about. So the default-when-unnamed = `kessel-op-v1` — binary
frames carrying `Op::encode()` in, `OpResult::encode()` out. The
subprotocol name exists for forward compatibility (so a future
`kessel-op-v2` can be negotiated alongside without breaking V1
consumers).

### 5.3 Message shape under `kessel-op-v1`

**Client→server:** one binary frame per `Op`. Frame payload =
`Op::encode()` bytes (the same bytes the binary wire would carry post
length-prefix-strip). Text frames (0x1) → close 1003. Multiple
client frames per WebSocket connection are allowed (mirror to HTTP/1.1
keep-alive); each is processed independently in FIFO order.

**Server→client:** one binary frame per `OpResult`. Frame payload =
`OpResult::encode()` bytes. The server may also send ping (0x9) and
close (0x8) frames at any time.

**No request-id correlation.** The connection is FIFO — client must wait
for the server's response frame before sending the next request, OR
must implement its own request-id correlation atop the byte payloads.
V1 does the simple thing: the server processes requests serially and
emits responses in arrival order.

### 5.4 Why binary, not JSON, in V1

The JSON shape (`format_result_json`) is already shipped for HTTP
clients. The WebSocket exists for the cases JSON-over-HTTP can't serve —
push streams, low-latency interactive clients, browsers that want
zero-copy bytes-in/bytes-out. JSON encode/decode overhead would defeat
the latency case. A future `kessel-op-v1-json` subprotocol can add the
JSON shape; V1 ships only binary.

## 6. Integration — where in the gateway

### 6.1 Path choice — `/v1/ws`

A dedicated path. Other options considered + rejected:

- **Add upgrade arm to every existing route** (e.g. POST /v1/sql with
  Upgrade header). Rejected — the upgrade is GET-only per RFC 6455 §4.1,
  and conflating it with the JSON routes would break the route-table
  invariant from SP141 (path → fixed handler).
- **Path `/v1/op` for binary frames** mirroring `/v1/op` HTTP semantics.
  Rejected — risks confusion between "HTTP POST body" and "WebSocket
  frame body" at the route table level.

`/v1/ws` is the dedicated path. Listed in `is_known_path` after T2.

### 6.2 Where in `server.rs::handle_one_stream`

The HTTP/1.1 parser (`parse_request`) already extracts the
`Upgrade: websocket` header into `req.headers`. The dispatch site is
`routes::handle`:

```rust
match req.path {
    "/v1/ws" if is_websocket_upgrade(&req.headers) => {
        let close_after_handshake = ws::handle_upgrade(s, &req, token, engine)?;
        return Ok(true); // Always close the HTTP-side loop; WS runs on the hijacked stream
    }
    "/v1/sql" => …,
    …
}
```

Critical: `ws::handle_upgrade` HIJACKS the TcpStream. After it returns
(successfully or with handshake-rejection), the per-connection HTTP loop
in `handle_one_stream` MUST exit — there are no more HTTP requests on
this socket; the bytes after the handshake response are either
WebSocket frames (handled by the inner WS session loop) or nothing.

### 6.3 The WebSocket session loop

Mirrors `handle_one_stream`:

```rust
fn ws_session_loop(stream: &mut TcpStream, engine: Arc<dyn EngineApply>,
                   max_payload: usize) -> std::io::Result<()> {
    let (tx, rx) = mpsc::sync_channel::<Vec<u8>>(WS_SEND_QUEUE_BOUND);
    // Dedicated writer thread drains rx -> socket. The reader thread is THIS
    // thread; it dispatches client frames to engine.apply_op and pushes
    // encoded OpResult frames into tx.
    let writer_handle = spawn_writer(stream.try_clone()?, rx);
    let mut frame_buf = Vec::with_capacity(8192);
    loop {
        let frame = read_frame_into(stream, &mut frame_buf, max_payload)?;
        match frame.opcode {
            Opcode::Binary => dispatch_op(&engine, &frame.payload, &tx)?,
            Opcode::Close  => { send_close_echo(&tx, &frame.payload)?; break; }
            Opcode::Ping   => send_pong(&tx, &frame.payload)?,
            Opcode::Pong   => { /* discard */ }
            _              => { close_with(&tx, 1003, "unsupported")?; break; }
        }
    }
    drop(tx);
    let _ = writer_handle.join();
    Ok(())
}
```

The reader-thread + writer-thread split keeps the ping/pong heartbeat
honest (the writer can flush pings without waiting on the reader's
in-progress `apply_op`) and matches SP-A T6's pattern of separating
"compute" from "wire".

### 6.4 TcpStream lifecycle

Std-only: `TcpStream::try_clone()` returns a second handle on the same
socket, one for the reader thread, one for the writer thread. Closing
the writer's clone (when `tx` drops) is observed by the reader on the
next read attempt (peer closed). Same shape SP-A T6 uses.

## 7. Backpressure

Per-connection bounded `mpsc::sync_channel(WS_SEND_QUEUE_BOUND = 16)`.
The bound is chosen larger than SP-A's `SHARD_BACKPRESSURE_BOUND=4`
because:
- A WebSocket connection's send queue is shared across data frames AND
  control frames (ping, pong, close). Reserving 12 slots for data + a
  few for control avoids head-of-line blocking on a ping during a slow
  drain.
- Per-frame size (post-Op-decode) is typically smaller than an entire
  SP-A scatter result; tolerating a deeper queue costs less memory.
- Browsers buffer aggressively; absorbing a small burst is more useful
  than instantly closing on the first slow client.

The behavior on full-queue + `send` is `SendError` → close 1011
(Internal Server Error). V1 prefers fast-fail to silent backlog; later
arcs can add `try_send` + drop-oldest pings if a real workload needs
it. This is the explicit honest gap surfaced by T6's pentest matrix.

## 8. Security

### 8.1 Auth

Same `Authorization: Bearer <token>` token mode as the HTTP routes.
Checked ONCE at the upgrade handshake by reusing
`parse::extract_bearer` (already exists, already ct-eq-vs-token
validated). The open connection itself represents the auth grant;
post-handshake frames are NOT re-auth'd.

Threat-model rationale: WebSocket has no per-frame metadata for auth
without inventing a sub-protocol that carries it; the handshake is the
only auth boundary that fits the RFC. An attacker who somehow ALSO has
the Bearer token can already POST to `/v1/op` — the WebSocket is not
a new privilege escalation vector.

Token-mismatch / missing-bearer → `401 Unauthorized` (HTTP response,
NOT a WebSocket close — the upgrade hasn't completed yet, so the wire
is still HTTP).

### 8.2 Mask enforcement

Client→server frames MUST have MASK=1 with a 4-byte mask key. If the
peer sends an unmasked frame, V1 closes immediately with 1002
(Protocol Error). This is the RFC 6455 §5.3 invariant and SP-WS T6
pentest #1 locks it.

Server→client frames MUST have MASK=0. The encoder API does not
expose a masked variant — structural enforcement.

### 8.3 Payload size cap

Every decoded frame's payload length is checked against `max_payload =
ServerConfig.http_max_body` (default 8 MiB). Oversize → close 1009.
Lying length headers (e.g. claiming a 64-bit length larger than the
process can possibly hold) are rejected BEFORE any allocation — the
decoder checks the length field against `max_payload` first, then
checks `buf.len() >= header_len + payload_len`. Same defense shape as
SP141's chunked-decode `Vec::with_capacity` cap.

### 8.4 Reserved bits + opcodes

Any RSV1/2/3 = 1 → close 1002 (we negotiated no extensions; the peer
is signaling we did). Opcodes 0x3..=0x7 (reserved data) and 0xB..=0xF
(reserved control) → close 1002.

### 8.5 Control-frame discipline

Per RFC 6455 §5.5: control frames MUST have payload ≤ 125 bytes AND
MUST have FIN=1. Both checked in the decoder; violation → close 1002.

### 8.6 Close frame discipline

Per RFC 6455 §5.5.1: close-frame payload is either empty or ≥ 2 bytes
(2-byte BE status code + optional UTF-8 reason). One-byte payload →
close 1002. Status codes valid range is 1000-4999, with 1004/1005/1006
reserved (not on-the-wire); peer sending a reserved code → echo close
1002.

### 8.7 Pentest matrix (T6)

T6 ships 10 pentests modeled on SP-A T8's shape (one mock peer per
attack, one `assert!`-locking the contract):

1. Unmasked client frame → close 1002, TCP closes promptly.
2. Frame claiming 9-byte (1<<63) payload length → rejected pre-alloc.
3. Continuation frame at message boundary → close 1003.
4. RSV1=1 frame → close 1002.
5. Reserved opcode 0x5 → close 1002.
6. Control frame (ping) with 200-byte payload → close 1002.
7. Close frame with 1-byte payload → close 1002.
8. Close frame with status 1004 (reserved) → echo close 1002.
9. Oversize binary frame (max_payload + 1) → close 1009.
10. Handshake without `Sec-WebSocket-Key` → HTTP 400, TCP closes.

T6's set is the V1 baseline; future arcs can add fuzz-style adversarial
inputs (`websocket-test-data` style).

## 9. Close behavior

### 9.1 Idle timeout

`TcpStream::set_read_timeout(30s)` (inherited from `serve()`) is the
outer guard. The WS read loop's `read_frame_into` will fail with
`ErrorKind::WouldBlock` on a 30s read-idle peer; the loop treats that as
"client disappeared", sends close 1001 (Going Away), and closes TCP.

### 9.2 Ping/pong heartbeat

Server sends a ping every 30s of no client traffic. If no pong arrives
in another 30s, server closes with 1011. (Implementation: the writer
thread checks `last_client_activity` against a monotonic clock; a small
seam — same shape as SP-A's per-shard timeout.)

### 9.3 Graceful shutdown

On server shutdown (a higher-level signal), the writer thread sends
close 1001 to every active WS connection, then closes the TcpStream.
The reader thread observes the close and exits. V1 acceptance: NO
session is required to flush in-flight `OpResult` frames before shutdown
— a frame queued in the channel but not yet written is allowed to drop.

### 9.4 Close handshake initiation

Either side may initiate the close. The peer that receives a close
frame MUST respond with a close frame of its own (echoing the status
code if it was valid, else 1000) and then close the TCP connection.
V1 implements both initiator and responder paths.

## 10. Task decomposition

| T# | Scope | KAT delta (approx) | Real-wire ship? |
|---|---|---|---|
| **T1** | This design spec + scaffolding module + `crypto.rs` (sha1) + `Sec-WebSocket-Accept` KATs + placeholder `handle_upgrade` | +3-6 | NO — placeholder errors |
| **T2** | Handshake parser: read upgrade request headers, validate key/version/protocol, produce the 101 response bytes, route the handshake into `routes::handle` | +6-10 | YES — handshake completes |
| **T3** | Frame encoder (binary, ping, pong, close); mask-XOR (server-side: no-op, but the helper exists for tests) | +6-8 | YES — server can send frames |
| **T4** | Frame decoder; strict validation per §4.2 / §8 | +10-15 | YES — server can read frames |
| **T5** | Per-connection session loop (reader thread + writer thread + send queue + ping heartbeat + idle timeout + close handshake) | +6-10 | YES — sessions hold open |
| **T6** | `kessel-op-v1` subprotocol wire-up + end-to-end test (real WebSocket client decoding `OpResult` bytes) + 10-pentest matrix | +10-15 | YES — full V1 |

Optional / deferred:
- **T7 (optional)** — SSE (`/v1/events`, text/event-stream) as the SP156
  §3.5 bundled add-on. ~1 slice if a consumer asks.
- **T8 (optional)** — streaming row chunks under `kessel-op-v1` (the
  SP-A T14 follow-up). Wire-protocol shape: server emits N binary
  frames carrying `OpResultChunk` bytes, terminating with one
  `OpResultDone` frame. Big enough to be its own design spec.

T1 ships now (this session). T2-T6 are one-per-session; the full V1
arc is ~5-6 sessions per the SP156 estimate.

## 11. Acceptance criteria

V1 ships when:

1. **End-to-end test:** a Rust-only test client (`tests/ws_e2e.rs` once
   T6 lands) opens a TCP connection to the gateway, performs the RFC
   6455 handshake, sends a `kessel-op-v1` binary frame carrying
   `Op::encode(&Op::Health)`, receives a binary frame carrying
   `OpResult::encode(&OpResult::Healthy)`, sends a close frame, receives
   a close frame back, observes clean TCP close. Locks the full V1 wire.
2. **Pentest matrix:** all 10 pentests in §8.7 pass (close codes match
   exactly, TCP closes promptly, no panics, no leaked threads).
3. **Browser smoke test:** a hand-built HTML page using
   `new WebSocket(...)` connects, sends `Op::Health`, receives
   `OpResult::Healthy`. NOT automated in V1 (no browser harness in the
   workspace); manual verification documented in
   `docs/USAGE.md` follow-up.
4. **No regression:** all 1366 default + 1399 featured tests still pass.
   `tree-grep` still empty. seed-7 still green.
5. **Zero-dep stance preserved:** no tungstenite, no tokio-tungstenite,
   no tokio. The `cargo tree -p kesseldb-server -e normal` output has
   no new entries.
6. **Existing pyarrow / curl / fetch clients still work.** WebSocket is
   additive — the HTTP/1.1 surface is byte-untouched.

## 12. Self-review — weak spots of this design

1. **No browser harness.** Acceptance #3 is a manual step. Without a
   playwright-or-similar test, we lock the wire bytes but not the
   real-world "curl `wss://...` from a browser" path. If a browser
   client somehow chokes on a valid frame we ship (e.g. a quirk in our
   close-handshake timing), we'd find out via user report not CI. The
   honest fix is a Playwright workflow under `.github/workflows/`,
   which is a real follow-up arc, not V1. Documented as such.

2. **Per-frame auth replay.** §8.1 says "the handshake is the only auth
   boundary." That's defensible for token-mode, but if a malicious
   middlebox between the client and gateway can hijack the TCP stream
   AFTER the handshake (e.g. via TLS-strip-then-re-encrypt), the
   attacker rides on the original auth grant. The mitigation is TLS
   (which the gateway already supports via `serve_tls`); the honest
   gap is "without TLS, post-handshake frames are auth'd transitively
   by TCP integrity only." Same gap as plain HTTP/1.1.

3. **Connection cap is shared with HTTP/1.1.** The
   `DEFAULT_MAX_CONNS=1024` cap in `serve()` counts WS sessions and
   HTTP/1.1 sessions in the same bucket. A WS-heavy workload could
   starve incoming HTTP requests (or vice versa). The honest fix is
   a separate WS cap, which V1 deliberately defers to keep the
   integration surface small — until we measure starvation in
   practice, an arbitrary split would be a guess.

4. **Send-queue close-on-overflow is harsh.** §7 closes the connection
   with 1011 the instant a frame can't fit in the bound. A more
   forgiving policy (try-send-or-drop-pong, try-send-or-drop-non-
   critical) exists in production WS libraries. V1 chose the simple
   thing on the rationale that "drop frames + tell no one" leaks
   protocol state (peer expects every ping → pong; missing pong looks
   like dead peer). 1011-then-close at least surfaces the failure
   honestly. If real workloads hit this, the policy is the right
   place to add nuance — but ONLY after we have a workload to
   calibrate against.

5. **No fragmentation support means no streaming-by-design.** A future
   consumer that wants to stream a 100 MiB result body via one logical
   message split across many frames cannot — V1 rejects continuation
   frames. The path forward is the SP-A T14 streaming-rows arc, where
   "many distinct logical messages" replaces "one fragmented message."
   This is design-by-restriction; we name the gap so future scoping
   knows where to look.

6. **Heartbeat clock is std-only.** The 30s ping interval reads
   `std::time::Instant::now()` — fine for typical wall-clock cases but
   poorly behaved if the OS clock jumps. Same caveat as every other
   `Instant`-driven timer in the workspace. Documented gap; the right
   fix (a monotonic-clock seam) is a workspace-wide concern, not a
   WebSocket-specific one.

7. **Subprotocol default-when-unnamed is a polite-fiction.** §5.2
   defaults to `kessel-op-v1` when the client names no protocol. This
   makes browser code easy but means a future `kessel-op-v2` cannot
   be the default-when-unnamed (back-compat would break existing
   no-protocol-named clients). The right migration is to ALWAYS require
   a named subprotocol from V2 forward, which means V1 sets the precedent
   we'll later regret. Honest tradeoff; the alternative (requiring V1
   clients to name `kessel-op-v1`) is worse for adoption.

8. **`/v1/ws` is the ONLY upgrade path.** If we later want a websocket-
   based metrics surface (`/v1/ws/metrics`), we'd need a routing layer
   inside the WS dispatch. V1's `is_websocket_upgrade(&req.headers) +
   req.path == "/v1/ws"` arm is hard-coded. The fix is a path table
   inside `ws.rs::handle_upgrade`; we name the seam but don't build it.

## 13. Open questions

- **Connection cap split.** Should `DEFAULT_MAX_WS_CONNS` be a separate
  config knob, or share with `DEFAULT_MAX_CONNS`? V1 picks share-with;
  T5 may re-litigate.
- **Close frame UTF-8 strictness.** RFC 6455 §5.5.1 requires the reason
  string be valid UTF-8. V1 sends only ASCII reasons (which are
  trivially valid UTF-8); validating incoming reason strings adds an
  unaligned UTF-8 walk on every close. Probably fine to defer.
- **Per-frame engine token check vs handshake-only.** §8.1 picks
  handshake-only. If a future operator workflow rotates tokens mid-
  connection, this needs a revisit (or a "send close 4000 with new-
  token-required" out-of-band mechanism).
- **Ping payload echo budget.** Pings of payload up to 125 bytes are
  allowed (§5.5). Server echoes payload verbatim as the pong. If a
  peer pings 125 bytes repeatedly, the writer thread is forced to
  always have a 125-byte buffer; bounded by `WS_SEND_QUEUE_BOUND * 125`
  = 2000 bytes worst-case per connection. Acceptable.

## 14. References

- SP156 scoping: `docs/superpowers/specs/2026-05-26-kesseldb-http2-ws-pgwire-scoping.md`
- SP141 HTTP gateway: `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md`
- SP147 HTTP/1.1 keep-alive: `docs/superpowers/specs/2026-05-26-kesseldb-subproject147-http-keep-alive.md`
- SP-A scatter-scan progress: `docs/superpowers/specs/2026-05-26-kesseldb-subproject-spa-progress.md`
- RFC 6455 — The WebSocket Protocol
- RFC 9110 / 9112 — HTTP Semantics / HTTP/1.1 (Upgrade mechanism)
- RFC 7692 — WebSocket Compression Extensions (V1 non-goal)
- RFC 8441 — Bootstrapping WebSockets with HTTP/2 (V1 non-goal)
