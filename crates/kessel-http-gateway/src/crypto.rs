//! SP-WS T1: thin helper layered over `kessel-crypto` for the RFC 6455
//! §4.2.2 `Sec-WebSocket-Accept` handshake-completion proof
//! (`base64(sha1(client_key + magic_guid))`).
//!
//! All crypto primitives (SHA-1 + base64) live in `kessel-crypto`; this
//! module exists only to (a) name the magic GUID, (b) pin the
//! concatenation order, and (c) lock the canonical RFC 6455 §1.3
//! handshake example with a project-local KAT so a future kessel-crypto
//! refactor that changes the SHA-1 or base64 surface surfaces the break
//! at the WebSocket boundary (not somewhere obscure).
//!
//! Design notes:
//! `docs/superpowers/specs/2026-05-26-kesseldb-spws-websocket-design.md`
//! §4 (frame implementation) consumes this; §5 (subprotocol) for the
//! handshake-completion proof's role.

#![forbid(unsafe_code)]

/// RFC 6455 §4.2.2 — server appends this fixed GUID to the client's
/// `Sec-WebSocket-Key` value (raw base64 string, NOT decoded), SHA-1s the
/// concatenation, then base64-encodes the digest. The result is the
/// `Sec-WebSocket-Accept` response header. The GUID is the published
/// RFC constant — DO NOT alter.
pub const WEBSOCKET_ACCEPT_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Compute the `Sec-WebSocket-Accept` header value for an upgrade
/// request carrying the given client `Sec-WebSocket-Key`. Returns the
/// base64-encoded 20-byte SHA-1 digest of
/// `client_key || WEBSOCKET_ACCEPT_GUID`.
///
/// The client key is treated as the raw header value (per RFC 6455
/// §4.2.2 the key arrives as base64-of-16-random-bytes, but the server
/// treats it as an opaque ASCII string for the concat). This function
/// does NOT validate the client key shape — that's the handshake
/// parser's job (SP-WS T2).
pub fn sec_websocket_accept(client_key: &str) -> String {
    let mut data = String::with_capacity(
        client_key.len() + WEBSOCKET_ACCEPT_GUID.len(),
    );
    data.push_str(client_key);
    data.push_str(WEBSOCKET_ACCEPT_GUID);
    let digest = kessel_crypto::sha1(data.as_bytes());
    kessel_crypto::base64_encode(&digest)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 6455 §1.3 — the canonical handshake KAT used in every
    /// WebSocket implementation. The client sends
    /// `Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==`; the server MUST
    /// reply with
    /// `Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=`. Locks the
    /// `Sec-WebSocket-Accept` computation against the RFC's own example
    /// — any breakage in `kessel_crypto::sha1` OR `base64_encode` OR
    /// the GUID constant OR the concatenation order is caught here.
    #[test]
    fn rfc6455_sec_websocket_accept_canonical_example() {
        let client_key = "dGhlIHNhbXBsZSBub25jZQ==";
        let want = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
        assert_eq!(
            sec_websocket_accept(client_key),
            want,
            "RFC 6455 §1.3 canonical Sec-WebSocket-Accept mismatch",
        );
    }

    /// Lock the magic GUID value byte-for-byte. RFC 6455 §4.2.2
    /// mandates this exact string; a typo here would silently break
    /// every browser client. The string format is 8-4-4-4-12 hex chars
    /// separated by hyphens = 36 chars total.
    #[test]
    fn websocket_accept_guid_constant_is_rfc_6455_value() {
        assert_eq!(
            WEBSOCKET_ACCEPT_GUID, "258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
            "WEBSOCKET_ACCEPT_GUID must match RFC 6455 §4.2.2 exactly",
        );
        assert_eq!(
            WEBSOCKET_ACCEPT_GUID.len(),
            36,
            "GUID is 36 chars (8-4-4-4-12 hex + 4 dashes)",
        );
    }

    /// `Sec-WebSocket-Accept` is base64 of a 20-byte SHA-1 digest, which
    /// always encodes to 28 chars (4 chars per 3 input bytes;
    /// 20 = 6×3 + 2 ⇒ 7 groups of 4 = 28 with exactly one `=` pad).
    /// Locks the output-length invariant against any future encoding
    /// regression (e.g. dropping pads, switching to url-safe alphabet).
    #[test]
    fn sec_websocket_accept_is_always_28_chars_with_one_pad() {
        for key in [
            "dGhlIHNhbXBsZSBub25jZQ==",
            "x3JJHMbDL1EzLkh9GBhXDw==",
            "AAAAAAAAAAAAAAAAAAAAAA==",
            "",
        ] {
            let accept = sec_websocket_accept(key);
            assert_eq!(
                accept.len(),
                28,
                "Sec-WebSocket-Accept must be 28 chars for key {key:?}: {accept:?}",
            );
            assert!(
                accept.ends_with('='),
                "Sec-WebSocket-Accept must end with '=' (base64 of 20 bytes); got {accept:?}",
            );
            assert!(
                !accept.ends_with("=="),
                "Sec-WebSocket-Accept has exactly ONE '=' pad (base64 of 20 bytes); got {accept:?}",
            );
        }
    }
}
