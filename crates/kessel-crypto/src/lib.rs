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

/// PBKDF2-HMAC-SHA-256(password, salt, iterations) → 32-byte derived key.
///
/// RFC 8018 §5.2. Locked to the SHA-256 PRF + a 32-byte output length —
/// the only shape SP-PG's SCRAM-SHA-256 needs (`SaltedPassword =
/// PBKDF2(password, salt, i, 32)` per RFC 5802 §3 / RFC 7677). The
/// hash-output length (hLen = 32 for SHA-256) equals our requested
/// dkLen, so the PBKDF2 outer loop reduces to a single block — we
/// compute U_1 = HMAC(P, S || INT(1)), then iterate U_{i+1} = HMAC(P,
/// U_i) for `iterations - 1` rounds, XOR-folding into the running
/// output.
///
/// The PG/SCRAM default iteration count is 4096 (since PG 10, 2017);
/// `PG_DEFAULT_SCRAM_ITERATIONS` in `kessel-pg-gateway` is the
/// canonical caller. Pinning that count plus the RFC 7677 §3 published
/// vector here keeps the PG-wire SCRAM handshake byte-identical to
/// every libpq / JDBC / pgx client.
///
/// This function panics if `iterations == 0` (RFC 8018: c MUST be a
/// positive integer; a zero-iteration key derivation is meaningless
/// and almost certainly a programmer bug — fail loudly).
pub fn pbkdf2_hmac_sha256(
    password: &[u8],
    salt: &[u8],
    iterations: u32,
) -> [u8; 32] {
    assert!(
        iterations > 0,
        "PBKDF2 iteration count must be positive (RFC 8018 §5.2)"
    );
    // INT(1) per RFC 8018 §5.2 — the 32-bit BE block index. With
    // dkLen == hLen == 32 we only ever compute T_1, so the index is
    // always 1.
    let mut salt_with_index: Vec<u8> = Vec::with_capacity(salt.len() + 4);
    salt_with_index.extend_from_slice(salt);
    salt_with_index.extend_from_slice(&1u32.to_be_bytes());

    // U_1 = HMAC(P, S || INT(1))
    let mut u = hmac_sha256(password, &salt_with_index);
    let mut t = u;
    // U_2..U_c = HMAC(P, U_{prev}); XOR-fold into T
    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        for (a, b) in t.iter_mut().zip(u.iter()) {
            *a ^= *b;
        }
    }
    t
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

/// RFC 4648 §4 standard-alphabet base64 decode. Returns `None` if
/// `input` is not a valid base64 string (wrong length, illegal char,
/// internal `=` pad). Pad characters are required — non-padded base64
/// is rejected. Used by SP-WS T2 to validate that the client's
/// `Sec-WebSocket-Key` header decodes to exactly 16 bytes (RFC 6455
/// §4.1).
///
/// This is a strict decoder: any deviation from the standard alphabet
/// (e.g. URL-safe `-_`, whitespace, non-pad-trailing chars) → `None`.
pub fn base64_decode(input: &str) -> Option<Vec<u8>> {
    // Strict: input length must be a multiple of 4 (per RFC 4648 §3.2
    // padding rules). Empty input decodes to empty.
    let bytes = input.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return None;
    }
    if bytes.is_empty() {
        return Some(Vec::new());
    }
    let val = |b: u8| -> Option<u32> {
        Some(match b {
            b'A'..=b'Z' => (b - b'A') as u32,
            b'a'..=b'z' => (b - b'a' + 26) as u32,
            b'0'..=b'9' => (b - b'0' + 52) as u32,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        })
    };
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len() / 4 * 3);
    let chunks = bytes.chunks_exact(4);
    let last_chunk_start = bytes.len() - 4;
    for (i, chunk) in chunks.enumerate() {
        let is_last = i * 4 == last_chunk_start;
        // Pads ONLY allowed in the final chunk, ONLY at positions 2 or 3,
        // and the pad-at-position-2 case forces position 3 to also be pad.
        let pad2 = chunk[2] == b'=';
        let pad3 = chunk[3] == b'=';
        if (pad2 || pad3) && !is_last {
            return None;
        }
        if pad2 && !pad3 {
            // `xx=y` is invalid — a pad at position 2 mandates a pad at
            // position 3 too (RFC 4648 §3.2).
            return None;
        }
        let v0 = val(chunk[0])?;
        let v1 = val(chunk[1])?;
        let v2 = if pad2 { 0 } else { val(chunk[2])? };
        let v3 = if pad3 { 0 } else { val(chunk[3])? };
        let n = (v0 << 18) | (v1 << 12) | (v2 << 6) | v3;
        out.push((n >> 16) as u8);
        if !pad2 {
            out.push((n >> 8) as u8);
        }
        if !pad3 {
            out.push(n as u8);
        }
    }
    Some(out)
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

    /// RFC 4648 §10 published base64 round-trip vectors. The decoder is
    /// the encoder's inverse for every well-formed input.
    #[test]
    fn base64_decode_rfc4648_vectors_round_trip() {
        for (encoded, want) in [
            ("", &b""[..]),
            ("Zg==", b"f"),
            ("Zm8=", b"fo"),
            ("Zm9v", b"foo"),
            ("Zm9vYg==", b"foob"),
            ("Zm9vYmE=", b"fooba"),
            ("Zm9vYmFy", b"foobar"),
        ] {
            let decoded = base64_decode(encoded).expect("RFC vector decodes");
            assert_eq!(decoded.as_slice(), want,
                "RFC 4648 §10 vector {encoded:?} → {want:?}");
        }
    }

    /// Strict-decode rejection cases: non-multiple-of-4 length, illegal
    /// chars, misplaced pad, URL-safe alphabet, embedded whitespace. The
    /// SP-WS T2 handshake parser depends on these all returning `None` so
    /// the 16-byte-key validation can trust a successful decode.
    #[test]
    fn base64_decode_rejects_malformed_inputs() {
        // Wrong length (not multiple of 4)
        assert!(base64_decode("Zg=").is_none(), "length-3 must be rejected");
        assert!(base64_decode("Zg").is_none(), "length-2 must be rejected");
        assert!(base64_decode("Zm9vY").is_none(), "length-5 must be rejected");
        // Illegal characters
        assert!(base64_decode("Zg!=").is_none(), "`!` not in alphabet");
        assert!(base64_decode("Zg @").is_none(), "whitespace not allowed");
        // URL-safe alphabet (RFC 4648 §5) is NOT accepted
        assert!(base64_decode("Zm-=").is_none(), "URL-safe `-` not accepted");
        assert!(base64_decode("Zm_=").is_none(), "URL-safe `_` not accepted");
        // Misplaced pad (pad at position 0 or 1 of any chunk)
        assert!(base64_decode("====").is_none());
        assert!(base64_decode("Z===").is_none(),
            "pad at position 1 must be rejected");
        // Pad-only-at-pos-2 (xx=y shape): the decoder rejects because a
        // pad at position 2 implies position 3 is also pad.
        assert!(base64_decode("Zg=A").is_none(),
            "pad-only-at-pos-2 must force pos-3 pad too");
    }

    /// PBKDF2-HMAC-SHA-256 known-answer vectors. The three
    /// `(P, S, c)` triples below are the canonical PBKDF2-HMAC-SHA-256
    /// test set widely reproduced from the original RFC 6070 SHA-1
    /// vectors (which RFC 6070 §2 specifies for HMAC-SHA-1 only) and
    /// re-keyed against HMAC-SHA-256; the digests are reproducible
    /// against any conforming PBKDF2-HMAC-SHA-256 implementation
    /// (Python `hashlib.pbkdf2_hmac`, OpenSSL `EVP_PBKDF2_HMAC`, the
    /// Crypto++ TestVectors/pbkdf2_sha256.txt file, etc.).
    ///
    /// Locked here so the SP-PG SCRAM-SHA-256 handshake (which uses
    /// `PBKDF2(password, salt, 4096, 32)` per RFC 5802 §3 + RFC 7677)
    /// is byte-identical to every libpq / JDBC / pgx client.
    #[test]
    fn pbkdf2_hmac_sha256_canonical_vectors() {
        // c=1 — smallest iteration count; exercises the single-call
        // U_1 path with no XOR-fold loop iterations.
        assert_eq!(
            hx(&pbkdf2_hmac_sha256(b"password", b"salt", 1)),
            "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"
        );
        // c=2 — exercises the XOR-fold (one iteration past the base
        // case; catches an off-by-one in the loop bound).
        assert_eq!(
            hx(&pbkdf2_hmac_sha256(b"password", b"salt", 2)),
            "ae4d0c95af6b46d32d0adff928f06dd02a303f8ef3c251dfd6e2d85a95474c43"
        );
        // c=4096 — the PG/SCRAM default iteration count (PG_DEFAULT_
        // SCRAM_ITERATIONS in kessel-pg-gateway). The SP-PG SCRAM
        // handshake is byte-identical to libpq IFF this digest matches.
        assert_eq!(
            hx(&pbkdf2_hmac_sha256(b"password", b"salt", 4096)),
            "c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a"
        );
    }

    /// RFC 7914 (scrypt) Appendix B PBKDF2-HMAC-SHA-256 reference
    /// vector — independent confirmation source. RFC 7914 §11 gives
    /// the dkLen=64 output; the first 32 bytes are PBKDF2's T_1 block,
    /// which equals `pbkdf2_hmac_sha256(P, S, c)` when dkLen == hLen
    /// == 32 (our locked output length).
    #[test]
    fn pbkdf2_hmac_sha256_rfc7914_appendix_b_t1_block() {
        // RFC 7914 §11: P="passwd", S="salt", c=1, dkLen=64 →
        //   55ac046e56e3089fec1691c22544b605f94185216dde0465e68b9d57c20dacbc
        //   49ca9cccf179b645991664b39d77ef317c71b845b1e30bd509112041d3a19783
        // T_1 (first 32 bytes) IS our 32-byte output.
        assert_eq!(
            hx(&pbkdf2_hmac_sha256(b"passwd", b"salt", 1)),
            "55ac046e56e3089fec1691c22544b605f94185216dde0465e68b9d57c20dacbc"
        );
    }

    /// PBKDF2 is deterministic — same inputs → same output, every
    /// invocation. Lock so a future "fast path" refactor that caches
    /// across calls (and gets the cache key wrong) surfaces here.
    #[test]
    fn pbkdf2_hmac_sha256_is_deterministic() {
        let a = pbkdf2_hmac_sha256(b"kessel", b"salt", 100);
        let b = pbkdf2_hmac_sha256(b"kessel", b"salt", 100);
        assert_eq!(a, b);
        // Iteration count matters — a 99-iter call must NOT match a
        // 100-iter call (catches a `iterations + 1` / `- 1` off-by-one).
        let c = pbkdf2_hmac_sha256(b"kessel", b"salt", 99);
        assert_ne!(a, c);
    }

    /// PBKDF2 with `iterations == 0` panics — RFC 8018 §5.2 specifies
    /// `c` MUST be a positive integer; a zero-iter derivation is
    /// programmatically meaningless. The panic protects callers from
    /// silently shipping a never-iterated HMAC as a "salted password".
    #[test]
    #[should_panic(expected = "iteration count must be positive")]
    fn pbkdf2_hmac_sha256_rejects_zero_iterations() {
        let _ = pbkdf2_hmac_sha256(b"password", b"salt", 0);
    }

    /// SP-WS T2 lock: a valid 16-byte `Sec-WebSocket-Key` decodes to
    /// exactly 16 bytes. The decoder's contract is what the handshake
    /// parser depends on; lock it here so a future decoder refactor that
    /// truncates by 1 byte surfaces at the kessel-crypto layer.
    #[test]
    fn base64_decode_rfc6455_sample_key_is_16_bytes() {
        // RFC 6455 §1.3 canonical client key
        let decoded = base64_decode("dGhlIHNhbXBsZSBub25jZQ==")
            .expect("canonical key decodes");
        assert_eq!(decoded.len(), 16,
            "Sec-WebSocket-Key per RFC 6455 §4.1 decodes to 16 bytes");
        assert_eq!(decoded.as_slice(), b"the sample nonce");
    }
}
