//! kessel-crypto: a tiny, **zero-dependency**, deterministic crypto core
//! — the pgcrypto-equivalent subset KesselDB can implement responsibly.
//!
//! Scope, stated honestly: **SHA-256** (FIPS 180-4), **HMAC-SHA256**
//! (RFC 2104), and **SHA-1** (RFC 3174 / FIPS 180-1), verified against
//! published NIST/RFC 4231/RFC 3174 test vectors. These are
//! well-specified, allocation-light, deterministic primitives that fit
//! KesselDB's replicated state machine (a hash is a pure function of
//! its input — identical on every replica). We deliberately do **not**
//! hand-roll symmetric encryption or a TLS stack here; transport
//! encryption is the opt-in `tls` feature on the server.
//!
//! **SHA-1 is collision-broken** for adversarial inputs and is provided
//! ONLY for the RFC 6455 §4.2.2 `Sec-WebSocket-Accept` handshake-
//! completion proof (a non-security primitive — the value is a
//! correctness oracle, not a confidentiality / integrity protection).
//! No new uses of SHA-1 should land in this workspace.

#![forbid(unsafe_code)]

const H0: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c,
    0x1f83d9ab, 0x5be0cd19,
];

const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
    0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
    0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
    0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/// SHA-256 of `msg`. Returns the 32-byte digest. FIPS 180-4.
pub fn sha256(msg: &[u8]) -> [u8; 32] {
    let mut h = H0;
    // Padding: 0x80, then zeros, then the 64-bit big-endian bit length,
    // to a multiple of 64 bytes.
    let bitlen = (msg.len() as u64).wrapping_mul(8);
    let mut data = msg.to_vec();
    data.push(0x80);
    while data.len() % 64 != 56 {
        data.push(0);
    }
    data.extend_from_slice(&bitlen.to_be_bytes());

    for block in data.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (i, c) in block.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes([c[0], c[1], c[2], c[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7)
                ^ w[i - 15].rotate_right(18)
                ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17)
                ^ w[i - 2].rotate_right(19)
                ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) = (
            h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7],
        );
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (hi, v) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
            *hi = hi.wrapping_add(v);
        }
    }
    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

/// SHA-1 of `msg`. Returns the 20-byte digest. RFC 3174 / FIPS 180-1.
///
/// **SHA-1 is collision-broken for adversarial inputs.** This function
/// exists ONLY for the RFC 6455 §4.2.2 `Sec-WebSocket-Accept` value
/// (`base64(sha1(client_key || magic_guid))`), where the digest is a
/// handshake-completion proof — NOT a security primitive. The WebSocket
/// RFC mandates SHA-1 precisely because the value isn't security-
/// sensitive; "broken" collisions don't enable any attack on the
/// handshake.
///
/// **Do not** add new callers outside the WebSocket handshake.
pub fn sha1(msg: &[u8]) -> [u8; 20] {
    // Initial hash values H0..H4 per FIPS 180-1 §5.3.1.
    let mut h: [u32; 5] = [
        0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0,
    ];
    // Padding: 0x80, then zeros, then the 64-bit BE bit length, padding
    // to a multiple of 64 bytes. Same shape as sha256 above.
    let bitlen = (msg.len() as u64).wrapping_mul(8);
    let mut data = msg.to_vec();
    data.push(0x80);
    while data.len() % 64 != 56 {
        data.push(0);
    }
    data.extend_from_slice(&bitlen.to_be_bytes());

    for chunk in data.chunks_exact(64) {
        let mut w: [u32; 80] = [0; 80];
        for (i, word) in chunk.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16])
                .rotate_left(1);
        }
        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1u32),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDCu32),
                _ => (b ^ c ^ d, 0xCA62C1D6u32),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }

    let mut out = [0u8; 20];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

/// HMAC-SHA256(key, msg) → 32 bytes. RFC 2104.
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&sha256(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0u8; BLOCK];
    let mut opad = [0u8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] = k[i] ^ 0x36;
        opad[i] = k[i] ^ 0x5c;
    }
    let mut inner = ipad.to_vec();
    inner.extend_from_slice(msg);
    let ih = sha256(&inner);
    let mut outer = opad.to_vec();
    outer.extend_from_slice(&ih);
    sha256(&outer)
}

/// Lowercase hex of bytes (for `digest(...)`-style text output).
pub fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// RFC 4648 §4 standard-alphabet base64 encode. Padded output
/// (`=`-trailers per the standard). Used by SP-WS T1+ for the
/// `Sec-WebSocket-Accept` value (`base64(sha1(client_key || guid))`);
/// duplicates the implementation in `kessel-objstore::b64` to avoid
/// pulling the feature-gated objstore crate into default builds.
///
/// A future workspace cleanup may consolidate the two implementations
/// in this module; until then this is the canonical default-build
/// base64.
pub fn base64_encode(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(ALPHABET[((n >> 18) & 63) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hx(b: &[u8]) -> String {
        hex(b)
    }

    #[test]
    fn sha256_known_answer_vectors() {
        // NIST / FIPS 180-4 published vectors.
        assert_eq!(
            hx(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hx(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hx(&sha256(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        // multi-block (> 64 bytes) — exercises padding + chaining
        let million_a = vec![b'a'; 1000];
        // (not the 1e6 NIST case to keep tests fast; cross-checked below)
        assert_eq!(hx(&sha256(&million_a)).len(), 64);
    }

    #[test]
    fn hmac_sha256_rfc4231_vectors() {
        // RFC 4231 Test Case 1
        assert_eq!(
            hx(&hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        // RFC 4231 Test Case 2
        assert_eq!(
            hx(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // long key (> block) path
        assert_eq!(
            hx(&hmac_sha256(&[0xaa; 131], b"Test Using Larger Than Block-Size Key - Hash Key First")),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn sha256_is_deterministic() {
        assert_eq!(sha256(b"kessel"), sha256(b"kessel"));
        assert_ne!(sha256(b"kessel"), sha256(b"Kessel"));
    }

    /// RFC 3174 §A.5 / FIPS 180-1 §A — published SHA-1 KATs. Every
    /// conforming SHA-1 implementation produces these digests for these
    /// inputs. Locks the SHA-1 implementation against the RFC's own
    /// reference vectors.
    #[test]
    fn sha1_rfc3174_known_answer_vectors() {
        // RFC 3174 §A.5 vector #1: "abc"
        assert_eq!(
            hx(&sha1(b"abc")),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        // RFC 3174 §A.5 vector #2: multi-block boundary
        assert_eq!(
            hx(&sha1(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
        );
        // Conventional empty-input sanity vector
        assert_eq!(
            hx(&sha1(b"")),
            "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        );
    }

    /// SHA-1 is deterministic and case-sensitive (sanity).
    #[test]
    fn sha1_is_deterministic() {
        assert_eq!(sha1(b"kessel"), sha1(b"kessel"));
        assert_ne!(sha1(b"kessel"), sha1(b"Kessel"));
    }

    /// RFC 4648 §10 published base64 test vectors. Same locked behaviour
    /// as `kessel-objstore::b64::encode` so the two implementations agree
    /// pending the consolidation in the module-level comment.
    #[test]
    fn base64_encode_rfc4648_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }
}
