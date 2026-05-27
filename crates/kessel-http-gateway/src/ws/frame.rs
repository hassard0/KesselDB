//! SP-WS T3 + T4 — WebSocket frame encoder + decoder per RFC 6455 §5.
//!
//! This module implements the byte-level WebSocket frame layer that sits
//! between the handshake (T2, `super::handle_upgrade`) and the session
//! loop (T5 — not yet shipped). It is deliberately split into two
//! halves:
//!
//! - **Encoder (T3)** — `encode_server_frame` + `encode_close_frame` +
//!   `encode_ping_frame` + `encode_pong_frame`. Server-side frames per
//!   RFC 6455 §5.3 MUST NOT carry a mask; the encoder structurally
//!   enforces this — no API path exists to produce a masked server
//!   frame.
//! - **Decoder (T4)** — `decode_client_frame` reverses the encoding and
//!   validates the strict V1 invariants: client frames MUST be masked
//!   (RFC 6455 §5.3, else `FrameError::InvalidMask`), reserved bits
//!   RSV1/2/3 MUST be zero (no extensions negotiated, else
//!   `FrameError::ReservedBitsSet`), the opcode must be one of the six
//!   V1-supported values (else `FrameError::InvalidOpcode`), the
//!   declared payload length must not exceed `MAX_PAYLOAD` (16 MiB,
//!   matching the gateway's `max_body`, else
//!   `FrameError::PayloadTooLarge`).
//!
//! The decoder is stream-oriented: when the supplied byte slice is
//! shorter than the frame would need (header + length + masking key +
//! payload), it returns `FrameError::NeedMoreData` so the caller can
//! read more bytes from the socket and retry. On success it returns
//! `(Frame, usize)` where the `usize` is the number of bytes consumed —
//! the caller can shift its read buffer left by that many bytes and
//! attempt another decode.
//!
//! ## What this module deliberately does NOT do
//!
//! - **Session loop** — T5. This module is just byte-level encode +
//!   decode; the reader-thread / writer-thread / send-queue / ping
//!   heartbeat / idle timeout / close handshake all live in T5.
//! - **Fragmentation reassembly** — V1 rejects continuation frames at
//!   the session-loop level (per spec §4.5). The decoder still returns
//!   `fin = false` frames if the wire has them; the session loop
//!   surfaces those as a 1003 close. (Per spec §4.2: continuation
//!   opcode 0x0 is a valid wire-level opcode the decoder accepts —
//!   "this is a continuation frame", not "this is malformed". The
//!   session loop is the layer that rejects them.)
//! - **Subprotocol semantics** — T6. The encoder/decoder don't know
//!   what `Op::encode()` looks like; they just move bytes.
//! - **Per-frame auth** — never. Auth is handshake-only per spec §8.1.
//!
//! ## Zero-dep stance
//!
//! `std::vec::Vec` only. No external crates, no `byteorder` (we hand-
//! write the BE u16 / u64 splits inline — they're 2 lines each).

#![forbid(unsafe_code)]
#![allow(dead_code)]

/// Max frame payload size the decoder will accept, in bytes. Matches
/// the HTTP gateway's `http_max_body` default (16 MiB). Locked here as
/// a module-level constant so the encoder (T3) and the decoder (T4)
/// agree on the boundary.
///
/// This is the V1 default; T5 will widen the decoder API to accept a
/// per-connection cap parameter (matching the gateway's
/// `ServerConfig.http_max_body`).
pub const MAX_PAYLOAD: usize = 16 * 1024 * 1024;

// --- RFC 6455 §5.2 wire-level opcodes ---------------------------------

/// Continuation frame. V1 emits/accepts at the encoder/decoder
/// boundary; the session loop (T5) rejects with close 1003 per spec
/// §4.5 because V1 doesn't fragment.
pub const OPCODE_CONTINUATION: u8 = 0x0;
/// Text data frame. V1 accepts on the decoder; the subprotocol layer
/// (T6) rejects with close 1003 because `kessel-op-v1` is binary-only.
pub const OPCODE_TEXT: u8 = 0x1;
/// Binary data frame. V1 carries `Op::encode()` / `OpResult::encode()`
/// bytes in these.
pub const OPCODE_BINARY: u8 = 0x2;
/// Connection close frame. V1 handles per spec §9.4.
pub const OPCODE_CLOSE: u8 = 0x8;
/// Ping control frame. V1 echoes payload back as a pong per RFC 6455
/// §5.5.2.
pub const OPCODE_PING: u8 = 0x9;
/// Pong control frame. V1 consumes + discards (server doesn't track
/// outstanding pings in V1; T5 may revisit if heartbeat needs it).
pub const OPCODE_PONG: u8 = 0xA;

// --- Encoder API (T3) -------------------------------------------------

/// Encode a server-to-client WebSocket frame per RFC 6455 §5.2.
///
/// The frame is FIN=1 (V1 never fragments outbound — spec §4.5), all
/// reserved bits zero (V1 negotiates no extensions), MASK=0 (server
/// frames MUST NOT be masked per RFC 6455 §5.3 — the encoder
/// structurally enforces this; no mask parameter exists).
///
/// Length encoding per RFC 6455 §5.2:
///   - payload.len() ≤ 125 → 1 byte (the length itself)
///   - 126 ≤ payload.len() ≤ 65535 → 0x7E + 2-byte BE length (3 bytes)
///   - payload.len() > 65535 → 0x7F + 8-byte BE length (9 bytes)
///
/// Total wire size = 2 (header) + 0/2/8 (extended length) + payload.len().
///
/// `opcode` is the 4-bit opcode (`OPCODE_BINARY`, `OPCODE_TEXT`,
/// `OPCODE_CLOSE`, `OPCODE_PING`, `OPCODE_PONG`, `OPCODE_CONTINUATION`).
/// The encoder masks `opcode` with 0x0F so the caller can't accidentally
/// set FIN/RSV bits via the opcode byte. Callers should NOT use
/// `OPCODE_CONTINUATION` from server-side in V1 (spec §4.5 — V1 never
/// fragments) but the encoder doesn't reject it — that's a session-loop
/// concern.
pub fn encode_server_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    let len = payload.len();
    // Pre-size the output vector exactly to avoid reallocations. The
    // header is 2 bytes + 0/2/8 extended length + 0 mask (server frames
    // unmasked). The payload follows.
    let header_extra = if len <= 125 {
        0
    } else if len <= 65535 {
        2
    } else {
        8
    };
    let mut buf = Vec::with_capacity(2 + header_extra + len);
    // Byte 0: FIN(1) | RSV1(0) | RSV2(0) | RSV3(0) | opcode(4).
    // FIN = 0x80. Mask opcode to 4 bits so the caller can't smuggle in
    // FIN/RSV bits via the opcode argument.
    buf.push(0x80 | (opcode & 0x0F));
    // Byte 1+: MASK(0) | len(7). Server frames are NEVER masked, so the
    // MASK bit (0x80) is always 0.
    if len <= 125 {
        buf.push(len as u8);
    } else if len <= 65535 {
        buf.push(0x7E); // 126 = "next 2 bytes are BE u16 length"
        buf.push((len >> 8) as u8);
        buf.push((len & 0xFF) as u8);
    } else {
        buf.push(0x7F); // 127 = "next 8 bytes are BE u64 length"
        let len64 = len as u64;
        buf.push((len64 >> 56) as u8);
        buf.push((len64 >> 48) as u8);
        buf.push((len64 >> 40) as u8);
        buf.push((len64 >> 32) as u8);
        buf.push((len64 >> 24) as u8);
        buf.push((len64 >> 16) as u8);
        buf.push((len64 >> 8) as u8);
        buf.push((len64 & 0xFF) as u8);
    }
    buf.extend_from_slice(payload);
    buf
}

/// Encode a Close frame per RFC 6455 §5.5.1.
///
/// Payload = 2-byte BE status code + UTF-8 reason string. The reason
/// is allowed to be empty (in which case the payload is exactly 2
/// bytes). RFC 6455 §5.5.1 also allows a fully-empty payload (no status
/// code) — V1 does not emit that shape; we always include the status.
///
/// Control frames per RFC 6455 §5.5 MUST have payload ≤ 125 bytes; the
/// caller is responsible for ensuring `2 + reason.len() ≤ 125` (i.e.
/// reason ≤ 123 bytes). The encoder does not validate this — the
/// session loop (T5) is the layer that bounds reason length.
pub fn encode_close_frame(code: u16, reason: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(2 + reason.len());
    payload.push((code >> 8) as u8);
    payload.push((code & 0xFF) as u8);
    payload.extend_from_slice(reason.as_bytes());
    encode_server_frame(OPCODE_CLOSE, &payload)
}

/// Encode a Ping control frame per RFC 6455 §5.5.2.
///
/// Payload is application-defined. Per §5.5 it MUST be ≤ 125 bytes —
/// not validated here; the session loop (T5) bounds it.
pub fn encode_ping_frame(payload: &[u8]) -> Vec<u8> {
    encode_server_frame(OPCODE_PING, payload)
}

/// Encode a Pong control frame per RFC 6455 §5.5.3.
///
/// When responding to a peer's Ping, payload MUST be byte-identical to
/// the Ping's payload per §5.5.3. The encoder doesn't check this — the
/// session loop (T5) reads the Ping's payload and passes it verbatim
/// here.
pub fn encode_pong_frame(payload: &[u8]) -> Vec<u8> {
    encode_server_frame(OPCODE_PONG, payload)
}

// --- Decoder API (T4) -------------------------------------------------

/// A decoded WebSocket frame. The `payload` is already unmasked (the
/// decoder applies the XOR using the 4-byte masking key from the wire).
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Frame {
    /// FIN bit. For V1 the session loop expects FIN=1 on data frames;
    /// a FIN=0 frame is the leading edge of a fragmented message and
    /// the session loop rejects with close 1003.
    pub fin: bool,
    /// Wire-level 4-bit opcode (one of `OPCODE_*`). Validated to be in
    /// the V1 set: continuation/text/binary/close/ping/pong.
    pub opcode: u8,
    /// Unmasked payload bytes. The decoder already XOR'd off the
    /// masking key from the wire.
    pub payload: Vec<u8>,
}

/// Failure modes the decoder may report.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum FrameError {
    /// The supplied byte slice is too short to fully decode a frame.
    /// The caller should read more bytes from the socket and retry.
    /// (V1 doesn't tell the caller HOW MANY more bytes; a refinement
    /// for T5 could return the byte-count hint so the caller can pre-
    /// size its read.)
    NeedMoreData,
    /// The MASK bit on the byte-1 of the frame is 0. RFC 6455 §5.3
    /// requires client→server frames to be masked; an unmasked frame
    /// from a client is a protocol violation. The session loop must
    /// close with code 1002.
    InvalidMask,
    /// The 4-bit opcode is not in the V1 set. Reserved opcodes
    /// (0x3..=0x7 for data, 0xB..=0xF for control) per RFC 6455 §5.2
    /// surface here. The session loop must close with code 1002.
    InvalidOpcode,
    /// The declared payload length exceeds `MAX_PAYLOAD` (16 MiB). The
    /// check fires BEFORE any allocation — an attacker advertising
    /// `u64::MAX` cannot OOM the server. The session loop must close
    /// with code 1009 (Message Too Big).
    PayloadTooLarge,
    /// At least one of RSV1/2/3 is set on the frame header. V1
    /// negotiates no extensions, so a peer setting any RSV bit is
    /// signaling an extension we didn't negotiate. The session loop
    /// must close with code 1002.
    ReservedBitsSet,
}

/// Decode a single WebSocket frame from a client (the only direction
/// V1 reads from). Returns `(Frame, consumed)` where `consumed` is the
/// number of bytes the caller should advance past in its read buffer
/// before attempting another decode.
///
/// Validation order (matters — earlier checks short-circuit cheaper):
///
/// 1. Header byte 0 present.
/// 2. RSV1/2/3 zero — else `ReservedBitsSet`.
/// 3. Opcode in V1 set — else `InvalidOpcode`.
/// 4. Header byte 1 present.
/// 5. MASK=1 — else `InvalidMask`.
/// 6. Extended length bytes present (if 7-bit length is 126/127).
/// 7. Declared payload length ≤ `MAX_PAYLOAD` — else `PayloadTooLarge`.
/// 8. Masking key + payload bytes all present in the buffer — else
///    `NeedMoreData`.
/// 9. Unmask payload via XOR with the 4-byte key.
///
/// The check ordering puts cheap structural failures (reserved bits,
/// invalid opcode, missing mask) before expensive ones (waiting on the
/// full payload to arrive). An attacker can't waste server memory by
/// sending a malformed header with a giant claimed length — we reject
/// the length-too-large case at step 7, BEFORE allocating any payload
/// buffer (step 8 only validates the buffer slice has enough bytes; we
/// don't allocate until step 9).
pub fn decode_client_frame(bytes: &[u8]) -> Result<(Frame, usize), FrameError> {
    // Step 1: header byte 0 must be present.
    if bytes.is_empty() {
        return Err(FrameError::NeedMoreData);
    }
    let b0 = bytes[0];
    // Step 2: RSV1/2/3 (bits 0x40 | 0x20 | 0x10) must all be zero. V1
    // negotiated no extensions.
    if b0 & 0x70 != 0 {
        return Err(FrameError::ReservedBitsSet);
    }
    let fin = (b0 & 0x80) != 0;
    let opcode = b0 & 0x0F;
    // Step 3: opcode must be in the V1 set. RFC 6455 §5.2:
    //   0x0 = continuation, 0x1 = text, 0x2 = binary,
    //   0x3..=0x7 = reserved data, 0x8 = close, 0x9 = ping,
    //   0xA = pong, 0xB..=0xF = reserved control.
    match opcode {
        OPCODE_CONTINUATION | OPCODE_TEXT | OPCODE_BINARY |
        OPCODE_CLOSE | OPCODE_PING | OPCODE_PONG => {}
        _ => return Err(FrameError::InvalidOpcode),
    }
    // Step 4: header byte 1 must be present.
    if bytes.len() < 2 {
        return Err(FrameError::NeedMoreData);
    }
    let b1 = bytes[1];
    // Step 5: MASK bit must be 1 for client frames per RFC 6455 §5.3.
    if b1 & 0x80 == 0 {
        return Err(FrameError::InvalidMask);
    }
    let len7 = b1 & 0x7F;
    let mut offset = 2usize;
    // Step 6: parse extended length per the 7-bit length sentinel.
    let payload_len: usize = match len7 {
        126 => {
            // 16-bit BE extended length.
            if bytes.len() < offset + 2 {
                return Err(FrameError::NeedMoreData);
            }
            let l = ((bytes[offset] as usize) << 8) | (bytes[offset + 1] as usize);
            offset += 2;
            l
        }
        127 => {
            // 64-bit BE extended length. Validate it fits in usize (on
            // 32-bit platforms `u64` may exceed `usize::MAX`).
            if bytes.len() < offset + 8 {
                return Err(FrameError::NeedMoreData);
            }
            let mut l: u64 = 0;
            for i in 0..8 {
                l = (l << 8) | (bytes[offset + i] as u64);
            }
            offset += 8;
            // Step 7 (early): if the declared length exceeds the
            // platform's usize OR our payload cap, reject BEFORE any
            // allocation. RFC 6455 §5.2 says the high bit of the 64-bit
            // length MUST be 0; we also check usize-fit + MAX_PAYLOAD.
            if l > MAX_PAYLOAD as u64 {
                return Err(FrameError::PayloadTooLarge);
            }
            l as usize
        }
        n => n as usize,
    };
    // Step 7: enforce the payload cap. (The 64-bit branch above already
    // checks; the 7-bit + 16-bit branches stay under 65535 < MAX_PAYLOAD
    // so this is structurally redundant for those, but kept for the
    // explicit invariant the test sweep locks.)
    if payload_len > MAX_PAYLOAD {
        return Err(FrameError::PayloadTooLarge);
    }
    // Step 8: masking key (4 bytes) + payload (payload_len bytes) must
    // all be present in the buffer. Use checked arithmetic so a future
    // refactor that forgets to clamp len at step 7 doesn't overflow
    // into a small-positive offset.
    let mask_start = offset;
    let payload_start = match mask_start.checked_add(4) {
        Some(v) => v,
        None => return Err(FrameError::PayloadTooLarge),
    };
    let frame_end = match payload_start.checked_add(payload_len) {
        Some(v) => v,
        None => return Err(FrameError::PayloadTooLarge),
    };
    if bytes.len() < frame_end {
        return Err(FrameError::NeedMoreData);
    }
    let mask = [
        bytes[mask_start],
        bytes[mask_start + 1],
        bytes[mask_start + 2],
        bytes[mask_start + 3],
    ];
    // Step 9: unmask the payload. RFC 6455 §5.3: `unmasked[i] =
    // masked[i] XOR mask[i % 4]`. Allocate the payload vec exactly
    // (we've already validated payload_len ≤ MAX_PAYLOAD).
    let mut payload = Vec::with_capacity(payload_len);
    for i in 0..payload_len {
        payload.push(bytes[payload_start + i] ^ mask[i & 0x03]);
    }
    Ok((Frame { fin, opcode, payload }, frame_end))
}

// --- Tests ------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------- T3 encoder KATs ---------------------------

    /// RFC 6455 §5.2: empty binary frame is exactly `[0x82, 0x00]`.
    /// 0x82 = FIN(1) | RSV(000) | binary(0x2). 0x00 = MASK(0) | len(0).
    #[test]
    fn t3_encode_empty_binary_frame_is_two_bytes() {
        let got = encode_server_frame(OPCODE_BINARY, &[]);
        assert_eq!(got, vec![0x82, 0x00],
            "empty binary frame must be [0x82, 0x00] per RFC 6455 §5.2");
    }

    /// RFC 6455 §5.2: a 5-byte text payload "Hello" encodes as
    /// `[0x81, 0x05, 'H', 'e', 'l', 'l', 'o']`. (Locks both opcode
    /// 0x1 = text and the 7-bit length branch boundary.)
    #[test]
    fn t3_encode_text_frame_hello_locks_wire_bytes() {
        let got = encode_server_frame(OPCODE_TEXT, b"Hello");
        assert_eq!(got, vec![0x81, 0x05, b'H', b'e', b'l', b'l', b'o'],
            "5-byte text frame must lock byte-for-byte vs RFC 6455 §5.2 example");
    }

    /// RFC 6455 §5.2: payload length 125 (the maximum value of the
    /// 7-bit length field) still uses the 1-byte length encoding. This
    /// is the upper boundary of the 7-bit branch.
    #[test]
    fn t3_encode_binary_frame_at_125_byte_boundary_uses_one_byte_length() {
        let payload = vec![0xAB; 125];
        let got = encode_server_frame(OPCODE_BINARY, &payload);
        assert_eq!(got.len(), 2 + 125,
            "125-byte payload uses 1-byte length; total = 2 + 125");
        assert_eq!(got[0], 0x82, "FIN | binary opcode");
        assert_eq!(got[1], 0x7D, "7-bit length encodes 125 (0x7D)");
        assert_eq!(&got[2..], &payload[..],
            "payload follows immediately, byte-for-byte");
    }

    /// RFC 6455 §5.2: payload length 126 crosses into the 16-bit
    /// extended length branch (0x7E sentinel + 2 BE bytes). The first
    /// length byte transitions from 0x7D (125) to 0x7E (126).
    #[test]
    fn t3_encode_binary_frame_at_126_crosses_into_16bit_length_branch() {
        let payload = vec![0xCD; 126];
        let got = encode_server_frame(OPCODE_BINARY, &payload);
        assert_eq!(got.len(), 2 + 2 + 126,
            "126-byte payload uses 2-byte extended length; total = 4 + 126");
        assert_eq!(got[0], 0x82);
        assert_eq!(got[1], 0x7E,
            "7-bit length 126 = sentinel for 16-bit extended length");
        assert_eq!(got[2], 0x00);
        assert_eq!(got[3], 0x7E,
            "extended length (16-bit BE) = 126 = 0x007E");
        assert_eq!(&got[4..], &payload[..]);
    }

    /// RFC 6455 §5.2: payload length 65535 is the upper boundary of
    /// the 16-bit extended-length branch. Encoded as `0x7E, 0xFF, 0xFF
    /// + payload`.
    #[test]
    fn t3_encode_binary_frame_at_65535_uses_16bit_length_max() {
        let payload = vec![0xEF; 65535];
        let got = encode_server_frame(OPCODE_BINARY, &payload);
        assert_eq!(got.len(), 2 + 2 + 65535);
        assert_eq!(got[0], 0x82);
        assert_eq!(got[1], 0x7E);
        assert_eq!(got[2], 0xFF);
        assert_eq!(got[3], 0xFF);
        assert_eq!(&got[4..], &payload[..]);
    }

    /// RFC 6455 §5.2: payload length 65536 crosses into the 64-bit
    /// extended-length branch (0x7F sentinel + 8 BE bytes).
    #[test]
    fn t3_encode_binary_frame_at_65536_crosses_into_64bit_length_branch() {
        let payload = vec![0x11; 65536];
        let got = encode_server_frame(OPCODE_BINARY, &payload);
        assert_eq!(got.len(), 2 + 8 + 65536);
        assert_eq!(got[0], 0x82);
        assert_eq!(got[1], 0x7F,
            "7-bit length 127 = sentinel for 64-bit extended length");
        // 65536 = 0x0000_0000_0001_0000 in BE.
        assert_eq!(&got[2..10], &[0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00][..]);
        assert_eq!(&got[10..], &payload[..]);
    }

    /// RFC 6455 §5.5.1: a Close frame with code 1000 (Normal Closure)
    /// and no reason is `[0x88, 0x02, 0x03, 0xE8]`. 0x88 = FIN | close.
    /// 0x02 = 2-byte payload. 0x03E8 = 1000 BE.
    #[test]
    fn t3_encode_close_frame_normal_no_reason_locks_wire_bytes() {
        let got = encode_close_frame(1000, "");
        assert_eq!(got, vec![0x88, 0x02, 0x03, 0xE8],
            "Close(1000) with empty reason must be [0x88, 0x02, 0x03, 0xE8]");
    }

    /// RFC 6455 §5.5.1: a Close frame with code 1011 and a reason
    /// "internal" emits the 2-byte code BE + the UTF-8 reason bytes.
    #[test]
    fn t3_encode_close_frame_with_reason_includes_utf8_bytes() {
        let got = encode_close_frame(1011, "internal");
        // 0x88 = FIN | close. Payload = 2 + 8 = 10 bytes.
        assert_eq!(got[0], 0x88);
        assert_eq!(got[1], 10, "payload length = 2 code bytes + 8 reason bytes");
        assert_eq!(got[2], 0x03);
        assert_eq!(got[3], 0xF3, "1011 = 0x03F3");
        assert_eq!(&got[4..], b"internal");
    }

    /// RFC 6455 §5.5.2: a Ping frame with empty payload is `[0x89, 0x00]`.
    #[test]
    fn t3_encode_ping_frame_empty_locks_wire_bytes() {
        let got = encode_ping_frame(&[]);
        assert_eq!(got, vec![0x89, 0x00]);
    }

    /// RFC 6455 §5.5.3: a Pong frame echoes the ping's payload. Here
    /// the wire bytes for a 4-byte ping payload "ping" → pong are
    /// `[0x8A, 0x04, 'p', 'i', 'n', 'g']`.
    #[test]
    fn t3_encode_pong_frame_echoes_payload_locks_wire_bytes() {
        let got = encode_pong_frame(b"ping");
        assert_eq!(got, vec![0x8A, 0x04, b'p', b'i', b'n', b'g']);
    }

    /// Defense-in-depth: the encoder masks the opcode argument with
    /// 0x0F so a caller who accidentally OR's in FIN/RSV bits (e.g.
    /// passing 0x82 thinking it's "binary already-FIN") doesn't
    /// duplicate them. The FIN bit is always set by the encoder; the
    /// RSV bits are always cleared.
    #[test]
    fn t3_encode_masks_opcode_to_four_bits() {
        // Caller passes opcode 0xFF (all bits set). The encoder
        // should still emit FIN(1)|RSV(000)|opcode(0xF) = 0x8F as byte
        // 0. (opcode 0xF is reserved-control; this test just locks the
        // MASK behavior, NOT that the encoder validates opcode values.)
        let got = encode_server_frame(0xFF, &[]);
        assert_eq!(got[0], 0x8F,
            "opcode masked to 4 bits; FIN forced on; RSV forced off");
    }

    /// Invariant: server frames the encoder produces have MASK bit
    /// CLEAR. This sweeps the four control + two data opcodes to lock
    /// the structural "server frames are never masked" promise (RFC
    /// 6455 §5.3).
    #[test]
    fn t3_invariant_all_encoded_server_frames_have_mask_bit_clear() {
        let payloads: Vec<(u8, &[u8])> = vec![
            (OPCODE_BINARY, b""),
            (OPCODE_BINARY, b"data"),
            (OPCODE_TEXT, b"text"),
            (OPCODE_CLOSE, b"\x03\xE8"),
            (OPCODE_PING, b"p"),
            (OPCODE_PONG, b"p"),
        ];
        for (opcode, p) in payloads {
            let encoded = encode_server_frame(opcode, p);
            assert!(encoded.len() >= 2);
            assert_eq!(encoded[1] & 0x80, 0,
                "MASK bit (0x80) of byte 1 must be 0 for server frame opcode {opcode:#x}");
        }
    }

    /// Invariant: MAX_PAYLOAD is 16 MiB — the agreed cap matching the
    /// HTTP gateway's `max_body`. Locked here so a future tweak in one
    /// place can't silently desync the other. (Used by the T4 decoder
    /// to enforce the per-frame size cap.)
    #[test]
    fn t3_max_payload_constant_is_16_mib() {
        assert_eq!(MAX_PAYLOAD, 16 * 1024 * 1024);
        assert_eq!(MAX_PAYLOAD, 16_777_216);
    }

    // -------------------- T4 decoder KATs ---------------------------

    /// Helper: build a masked client frame on the wire from a server-
    /// encoded frame + a 4-byte mask key. The header byte 1 has its
    /// MASK bit (0x80) set; the mask key is inserted after the length
    /// bytes; the payload is XOR'd with the mask key. This lets us
    /// build wire bytes a real client would have produced.
    fn add_client_mask(server_encoded: &[u8], mask: [u8; 4]) -> Vec<u8> {
        assert!(server_encoded.len() >= 2);
        let b0 = server_encoded[0];
        let b1 = server_encoded[1];
        let len7 = b1 & 0x7F;
        let header_extra = match len7 {
            126 => 2,
            127 => 8,
            _ => 0,
        };
        let payload_start = 2 + header_extra;
        let payload = &server_encoded[payload_start..];
        let mut out = Vec::with_capacity(server_encoded.len() + 4);
        out.push(b0);
        out.push(b1 | 0x80);
        out.extend_from_slice(&server_encoded[2..payload_start]);
        out.extend_from_slice(&mask);
        for (i, b) in payload.iter().enumerate() {
            out.push(b ^ mask[i & 3]);
        }
        out
    }

    /// Decode a masked client text frame "Hello" — the worked example
    /// shape from RFC 6455 §5.7.
    #[test]
    fn t4_decode_masked_client_text_frame_hello() {
        // Build the wire bytes a client would send: 0x81 (FIN|text),
        // 0x85 (MASK|5), mask=0x37,0xFA,0x21,0x3D, payload XOR'd.
        let mask = [0x37, 0xFA, 0x21, 0x3D];
        let server = encode_server_frame(OPCODE_TEXT, b"Hello");
        let wire = add_client_mask(&server, mask);
        let (frame, consumed) = decode_client_frame(&wire).expect("decode ok");
        assert!(frame.fin, "FIN bit must decode to true");
        assert_eq!(frame.opcode, OPCODE_TEXT);
        assert_eq!(frame.payload, b"Hello",
            "decoded payload must equal original after unmask");
        assert_eq!(consumed, wire.len(),
            "consumed = full frame length");
    }

    /// Decode a small (10-byte) masked binary frame.
    #[test]
    fn t4_decode_masked_client_small_binary_frame() {
        let payload: [u8; 10] = [0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0xFF, 0x12, 0x34, 0x56, 0x78];
        let server = encode_server_frame(OPCODE_BINARY, &payload);
        let wire = add_client_mask(&server, [0xAA, 0xBB, 0xCC, 0xDD]);
        let (frame, consumed) = decode_client_frame(&wire).expect("decode ok");
        assert!(frame.fin);
        assert_eq!(frame.opcode, OPCODE_BINARY);
        assert_eq!(frame.payload, payload);
        assert_eq!(consumed, wire.len());
    }

    /// RFC 6455 §5.3: an unmasked client frame is a protocol violation.
    /// The decoder must reject with `InvalidMask` BEFORE allocating
    /// any payload buffer.
    #[test]
    fn t4_decode_rejects_unmasked_client_frame() {
        // Use a server-encoded frame as-is (no mask bit set) — this is
        // exactly what an attacker would send.
        let wire = encode_server_frame(OPCODE_BINARY, b"unmasked");
        let result = decode_client_frame(&wire);
        assert_eq!(result, Err(FrameError::InvalidMask),
            "client frame without MASK bit must be rejected per RFC 6455 §5.3");
    }

    /// RFC 6455 §5.2: if RSV1 (0x40) is set, the peer is signaling an
    /// extension we didn't negotiate. V1 advertises no extensions →
    /// reject.
    #[test]
    fn t4_decode_rejects_rsv1_set() {
        // 0xC2 = FIN | RSV1 | binary. b1 = 0x80 (MASK|len=0). 4-byte
        // mask key follows. No payload.
        let wire = vec![0xC2, 0x80, 0x00, 0x00, 0x00, 0x00];
        let result = decode_client_frame(&wire);
        assert_eq!(result, Err(FrameError::ReservedBitsSet));
    }

    /// RFC 6455 §5.2: RSV2 (0x20) set → reject.
    #[test]
    fn t4_decode_rejects_rsv2_set() {
        let wire = vec![0xA2, 0x80, 0x00, 0x00, 0x00, 0x00];
        let result = decode_client_frame(&wire);
        assert_eq!(result, Err(FrameError::ReservedBitsSet));
    }

    /// RFC 6455 §5.2: RSV3 (0x10) set → reject.
    #[test]
    fn t4_decode_rejects_rsv3_set() {
        let wire = vec![0x92, 0x80, 0x00, 0x00, 0x00, 0x00];
        let result = decode_client_frame(&wire);
        assert_eq!(result, Err(FrameError::ReservedBitsSet));
    }

    /// RFC 6455 §5.2: opcode 0x3 is reserved-data; reject.
    #[test]
    fn t4_decode_rejects_reserved_data_opcode_0x3() {
        let wire = vec![0x83, 0x80, 0x00, 0x00, 0x00, 0x00];
        let result = decode_client_frame(&wire);
        assert_eq!(result, Err(FrameError::InvalidOpcode));
    }

    /// RFC 6455 §5.2: opcode 0xB is reserved-control; reject.
    #[test]
    fn t4_decode_rejects_reserved_control_opcode_0xb() {
        let wire = vec![0x8B, 0x80, 0x00, 0x00, 0x00, 0x00];
        let result = decode_client_frame(&wire);
        assert_eq!(result, Err(FrameError::InvalidOpcode));
    }

    /// Adversarial: an attacker advertises a 9-byte (~16 EiB) payload
    /// length via the 64-bit branch. The decoder must reject BEFORE
    /// allocating — `Vec::with_capacity(2^63)` would OOM the server.
    #[test]
    fn t4_decode_rejects_payload_above_cap_via_64bit_length() {
        // 0x82 = FIN|binary. 0xFF = MASK|127 (64-bit length sentinel).
        // 8-byte BE length = u64::MAX. Mask + payload absent (we never
        // get that far).
        let wire = vec![
            0x82, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        ];
        let result = decode_client_frame(&wire);
        assert_eq!(result, Err(FrameError::PayloadTooLarge),
            "huge declared length must be rejected pre-allocation");
    }

    /// Adversarial: 64-bit length exactly at MAX_PAYLOAD + 1.
    #[test]
    fn t4_decode_rejects_payload_one_byte_above_cap() {
        let too_big = (MAX_PAYLOAD as u64) + 1;
        let wire = vec![
            0x82, 0xFF,
            (too_big >> 56) as u8, (too_big >> 48) as u8,
            (too_big >> 40) as u8, (too_big >> 32) as u8,
            (too_big >> 24) as u8, (too_big >> 16) as u8,
            (too_big >> 8) as u8, (too_big & 0xFF) as u8,
        ];
        let result = decode_client_frame(&wire);
        assert_eq!(result, Err(FrameError::PayloadTooLarge));
    }

    /// Decoder returns `NeedMoreData` when the buffer is empty.
    #[test]
    fn t4_decode_empty_buffer_needs_more_data() {
        let result = decode_client_frame(&[]);
        assert_eq!(result, Err(FrameError::NeedMoreData));
    }

    /// Decoder returns `NeedMoreData` when only byte 0 is present (the
    /// length byte is missing).
    #[test]
    fn t4_decode_truncated_at_byte_1_needs_more_data() {
        let result = decode_client_frame(&[0x82]);
        assert_eq!(result, Err(FrameError::NeedMoreData));
    }

    /// Decoder returns `NeedMoreData` when the 16-bit extended length
    /// is truncated.
    #[test]
    fn t4_decode_truncated_16bit_length_needs_more_data() {
        // 0x82|0xFE (len=126 = 16-bit ext) + only 1 of 2 length bytes.
        let result = decode_client_frame(&[0x82, 0xFE, 0x00]);
        assert_eq!(result, Err(FrameError::NeedMoreData));
    }

    /// Decoder returns `NeedMoreData` when the 64-bit extended length
    /// is truncated.
    #[test]
    fn t4_decode_truncated_64bit_length_needs_more_data() {
        let result = decode_client_frame(&[0x82, 0xFF, 0x00, 0x00, 0x00]);
        assert_eq!(result, Err(FrameError::NeedMoreData));
    }

    /// Decoder returns `NeedMoreData` when the masking key is
    /// truncated.
    #[test]
    fn t4_decode_truncated_masking_key_needs_more_data() {
        // 0x82|0x80 (MASK|len=0) + only 2 of 4 mask bytes.
        let result = decode_client_frame(&[0x82, 0x80, 0xAA, 0xBB]);
        assert_eq!(result, Err(FrameError::NeedMoreData));
    }

    /// Decoder returns `NeedMoreData` when the payload is truncated.
    #[test]
    fn t4_decode_truncated_payload_needs_more_data() {
        // 0x82|0x85 (MASK|len=5) + 4-byte mask + only 3 of 5 payload bytes.
        let wire = vec![0x82, 0x85, 0x00, 0x00, 0x00, 0x00, 0xAA, 0xBB, 0xCC];
        let result = decode_client_frame(&wire);
        assert_eq!(result, Err(FrameError::NeedMoreData));
    }

    /// 126-byte payload uses the 2-byte extended length branch on
    /// decode. Locks the lower boundary of the 16-bit branch.
    #[test]
    fn t4_decode_126_byte_frame_uses_2byte_length() {
        let payload = vec![0x42; 126];
        let server = encode_server_frame(OPCODE_BINARY, &payload);
        let wire = add_client_mask(&server, [0x11, 0x22, 0x33, 0x44]);
        let (frame, consumed) = decode_client_frame(&wire).expect("decode ok");
        assert_eq!(frame.opcode, OPCODE_BINARY);
        assert_eq!(frame.payload, payload);
        assert_eq!(consumed, wire.len());
    }

    /// 65536-byte payload uses the 8-byte extended length branch on
    /// decode. Locks the lower boundary of the 64-bit branch.
    #[test]
    fn t4_decode_65536_byte_frame_uses_8byte_length() {
        let payload = vec![0x77; 65536];
        let server = encode_server_frame(OPCODE_BINARY, &payload);
        let wire = add_client_mask(&server, [0xDE, 0xAD, 0xBE, 0xEF]);
        let (frame, consumed) = decode_client_frame(&wire).expect("decode ok");
        assert_eq!(frame.opcode, OPCODE_BINARY);
        assert_eq!(frame.payload.len(), 65536);
        assert_eq!(frame.payload, payload);
        assert_eq!(consumed, wire.len());
    }

    /// `consumed` reports the right number of bytes when the buffer
    /// contains MORE than one frame's worth — caller can shift left
    /// by `consumed` and decode the next frame.
    #[test]
    fn t4_decode_returns_consumed_byte_count_for_buffer_with_trailing_bytes() {
        let server = encode_server_frame(OPCODE_BINARY, b"abc");
        let wire = add_client_mask(&server, [0x01, 0x02, 0x03, 0x04]);
        let mut buf = wire.clone();
        buf.extend_from_slice(&[0xFF, 0xFF, 0xFF]); // bytes from next frame
        let (frame, consumed) = decode_client_frame(&buf).expect("decode ok");
        assert_eq!(frame.payload, b"abc");
        assert_eq!(consumed, wire.len(),
            "consumed must point at end of THIS frame, leaving trailing bytes");
        assert!(consumed < buf.len(),
            "trailing bytes must remain for the next decode call");
    }

    /// FIN=0 (fragment) is a structurally valid frame; the decoder
    /// returns it cleanly. The session loop (T5) is the layer that
    /// closes 1003 on fragmented data frames per spec §4.5; the decoder
    /// MUST surface `fin = false` so the session can make that decision.
    #[test]
    fn t4_decode_fin_zero_fragment_returns_clean_frame_with_fin_false() {
        // b0 = 0x02 (FIN=0, opcode=binary). b1 = 0x80 (MASK, len=0).
        let wire = vec![0x02, 0x80, 0x00, 0x00, 0x00, 0x00];
        let (frame, _consumed) = decode_client_frame(&wire).expect("decode ok");
        assert!(!frame.fin, "FIN=0 must decode to fin=false");
        assert_eq!(frame.opcode, OPCODE_BINARY);
        assert_eq!(frame.payload, &[] as &[u8]);
    }

    /// Decode a Close frame the encoder produced (round-trip control
    /// frame).
    #[test]
    fn t4_decode_close_frame_with_code_and_reason() {
        let server = encode_close_frame(1011, "internal");
        let wire = add_client_mask(&server, [0xA0, 0xB0, 0xC0, 0xD0]);
        let (frame, _consumed) = decode_client_frame(&wire).expect("decode ok");
        assert_eq!(frame.opcode, OPCODE_CLOSE);
        // Payload = 2-byte BE code + reason bytes.
        assert_eq!(frame.payload[0], 0x03);
        assert_eq!(frame.payload[1], 0xF3, "1011 = 0x03F3");
        assert_eq!(&frame.payload[2..], b"internal");
    }

    /// Decode a Ping frame; locks Ping opcode round-trip.
    #[test]
    fn t4_decode_ping_frame_with_payload() {
        let server = encode_ping_frame(b"keepalive");
        let wire = add_client_mask(&server, [0xCA, 0xFE, 0xBA, 0xBE]);
        let (frame, _consumed) = decode_client_frame(&wire).expect("decode ok");
        assert_eq!(frame.opcode, OPCODE_PING);
        assert_eq!(frame.payload, b"keepalive");
    }

    /// Property: encode_server_frame + add_client_mask + decode_client_frame
    /// yields the original payload. Sweeps every length-branch boundary
    /// + a handful of opcodes; locks the encoder + decoder agree on the
    /// wire format. This is the round-trip test the design spec calls
    /// out as the load-bearing contract between T3 and T4.
    #[test]
    fn t4_round_trip_encode_then_mask_then_decode_returns_original_payload() {
        let mask = [0x37, 0xFA, 0x21, 0x3D];
        let cases: Vec<(u8, Vec<u8>)> = vec![
            (OPCODE_BINARY, vec![]),
            (OPCODE_BINARY, vec![0x42; 1]),
            (OPCODE_BINARY, vec![0x42; 125]),     // 7-bit upper boundary
            (OPCODE_TEXT, vec![0xAB; 126]),       // 16-bit lower boundary
            (OPCODE_BINARY, vec![0xCD; 65535]),   // 16-bit upper boundary
            (OPCODE_BINARY, vec![0xEF; 65536]),   // 64-bit lower boundary
            (OPCODE_PING, b"ping-data".to_vec()),
            (OPCODE_PONG, b"pong-data".to_vec()),
        ];
        for (opcode, payload) in cases {
            let server = encode_server_frame(opcode, &payload);
            let wire = add_client_mask(&server, mask);
            let (frame, consumed) = decode_client_frame(&wire)
                .unwrap_or_else(|e| panic!("decode failed for opcode {opcode:#x}, \
                    payload.len()={}: {e:?}", payload.len()));
            assert!(frame.fin, "encode always sets FIN");
            assert_eq!(frame.opcode, opcode, "opcode round-trip");
            assert_eq!(frame.payload, payload,
                "payload round-trip for opcode {opcode:#x}, len={}",
                payload.len());
            assert_eq!(consumed, wire.len(),
                "consumed must equal wire length for opcode {opcode:#x}");
        }
    }
}
