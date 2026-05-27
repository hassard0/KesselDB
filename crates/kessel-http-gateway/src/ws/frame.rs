//! SP-WS T3 — WebSocket frame encoder per RFC 6455 §5.
//!
//! This module implements the byte-level WebSocket frame encoder that
//! sits between the handshake (T2, `super::handle_upgrade`) and the
//! session loop (T5 — not yet shipped). T3 covers the server-side
//! encoder only; the matching client-side decoder is T4 (added in the
//! next slice).
//!
//! - **Encoder (T3, this slice)** — `encode_server_frame` +
//!   `encode_close_frame` + `encode_ping_frame` + `encode_pong_frame`.
//!   Server-side frames per RFC 6455 §5.3 MUST NOT carry a mask; the
//!   encoder structurally enforces this — no API path exists to
//!   produce a masked server frame.
//!
//! ## What this module deliberately does NOT do
//!
//! - **Frame decoder** — T4 (next slice).
//! - **Session loop** — T5. This module is just byte-level encode +
//!   decode; the reader-thread / writer-thread / send-queue / ping
//!   heartbeat / idle timeout / close handshake all live in T5.
//! - **Subprotocol semantics** — T6. The encoder doesn't know what
//!   `Op::encode()` looks like; it just moves bytes.
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
}
