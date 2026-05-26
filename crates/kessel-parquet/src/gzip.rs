//! Pure RFC 1952 (gzip member) + RFC 1951 (DEFLATE inflate)
//! decompressor for Parquet GZIP pages. Zero deps, iterative
//! (no recursion), bounds-checked, 256 MiB hard cap, CRC32-verified.
//! Never panics / OOM-aborts / stack-overflows.
#![allow(dead_code)]

use crate::PqError;

fn bad(s: &str) -> PqError {
    PqError::Bad(s.to_string())
}

/// Hard cap on a single decompressed page. Mirrors
/// snappy::SNAPPY_MAX_DECOMP (same value & rationale; separate const
/// so gzip.rs stays self-contained — the sibling-module convention).
///
/// SP151: bumped from 64 → 256 MiB in lockstep with SNAPPY_MAX_DECOMP /
/// ZSTD_MAX_DECOMP. User-facing knob is `crate::DEFAULT_MAX_PAGE_SIZE`.
pub(crate) const GZIP_MAX_DECOMP: usize = 256 << 20; // 256 MiB

// ── CRC-32/ISO-HDLC ─────────────────────────────────────────────────

/// CRC-32/ISO-HDLC (polynomial 0xEDB88320 reflected).
/// Builds a 256-entry table on each call (constant-fold friendly;
/// avoids any static/global mutable state).
fn crc32(data: &[u8]) -> u32 {
    let mut tbl = [0u32; 256];
    for n in 0..256usize {
        let mut c = n as u32;
        for _ in 0..8 {
            c = if c & 1 == 1 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
        }
        tbl[n] = c;
    }
    let mut c: u32 = 0xFFFF_FFFF;
    for &b in data {
        c = tbl[((c ^ b as u32) & 0xff) as usize] ^ (c >> 8);
    }
    c ^ 0xFFFF_FFFF
}

// ── BitReader (LSB-first) ────────────────────────────────────────────

struct BitReader<'a> {
    data: &'a [u8],
    byte: usize, // current byte index
    bit: u32,    // how many bits consumed in current byte (0..=7)
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        BitReader { data, byte: 0, bit: 0 }
    }

    /// Read `n` bits (LSB-first, 0 ≤ n ≤ 16). Returns Err on truncation.
    fn bits(&mut self, n: u32) -> Result<u32, PqError> {
        let mut result: u32 = 0;
        for i in 0..n {
            if self.bit == 8 {
                self.byte += 1;
                self.bit = 0;
            }
            let byte_val = *self
                .data
                .get(self.byte)
                .ok_or_else(|| bad("deflate truncated (bit read)"))?;
            let bit_val = (byte_val >> self.bit) & 1;
            result |= (bit_val as u32) << i;
            self.bit += 1;
        }
        Ok(result)
    }

    /// Align to the next byte boundary (discard remaining bits in current byte).
    fn align_to_byte(&mut self) {
        if self.bit > 0 {
            self.byte += 1;
            self.bit = 0;
        }
    }

    /// Read a u16 in little-endian from the byte stream (must be aligned).
    fn read_u16_le(&mut self) -> Result<u16, PqError> {
        let lo = *self
            .data
            .get(self.byte)
            .ok_or_else(|| bad("deflate stored: truncated len lo"))?;
        let hi = *self
            .data
            .get(self.byte + 1)
            .ok_or_else(|| bad("deflate stored: truncated len hi"))?;
        self.byte += 2;
        Ok(u16::from_le_bytes([lo, hi]))
    }
}

// ── Canonical Huffman decoder ────────────────────────────────────────

const MAX_BITS: usize = 15;

struct Huff {
    // For each symbol: (code, code_len). We store pairs sorted by symbol.
    // Decode is bit-at-a-time: accumulate bits until we match.
    // We use a direct table approach: store per-length the first code
    // and first symbol index (bl_count / next_code RFC §3.2.2).
    //
    // symbols[i] = symbol value for symbol-sorted index i
    // lens[i]    = bit length for that symbol
    // For decode: track (accumulated_code, bits_read); at each bit length
    // check if accumulated_code is in [first_code[len] .. first_code[len]+count[len]).
    //
    // We store in a parallel array sorted by (len, symbol) for fast decode.
    entries: Vec<(u32, u8, u16)>, // (canonical_code, code_len, symbol)
    // first_code[len] = first canonical code for that bit length
    first_code: [u32; MAX_BITS + 1],
    // first_entry[len] = index into entries[] for that bit length's start
    first_entry: [usize; MAX_BITS + 1],
    // count[len] = number of symbols with that bit length
    count: [usize; MAX_BITS + 1],
}

impl Huff {
    fn build(lens: &[u8]) -> Result<Huff, PqError> {
        // RFC 1951 §3.2.2 canonical Huffman construction
        let mut bl_count = [0u32; MAX_BITS + 1];
        for &l in lens {
            if l as usize > MAX_BITS {
                return Err(bad("huffman: code length exceeds MAX_BITS"));
            }
            if l > 0 {
                bl_count[l as usize] += 1;
            }
        }

        // RFC 1951 §3.2.2 Kraft inequality: reject over-subscribed code tables.
        // An over-subscribed table has more codes than the prefix space allows,
        // which means two or more symbols share the same code — silent corruption.
        // left>0 (incomplete) is tolerated: decode() returns Err on any unmatched
        // pattern; only over-subscription is a build-time reject.
        // Note: an all-zero lens slice (e.g. an absent distance tree) starts
        // left=1 and never subtracts anything — stays ≥0, correctly returns Ok.
        {
            let mut left: i64 = 1;
            for bits in 1..=MAX_BITS {
                left <<= 1;
                left -= bl_count[bits] as i64;
                if left < 0 {
                    return Err(bad("huffman: over-subscribed code"));
                }
            }
        }

        let mut next_code = [0u32; MAX_BITS + 1];
        let mut code: u32 = 0;
        for bits in 1..=MAX_BITS {
            code = (code + bl_count[bits - 1]) << 1;
            next_code[bits] = code;
        }

        // Build sorted entries (by len then symbol)
        let mut entries: Vec<(u32, u8, u16)> = Vec::new();
        for (sym, &l) in lens.iter().enumerate() {
            if l > 0 {
                let c = next_code[l as usize];
                entries.push((c, l, sym as u16));
                next_code[l as usize] += 1;
            }
        }
        // Sort by (code_len, code) for organized lookup
        entries.sort_by_key(|&(c, l, _)| (l, c));

        // Build first_code / first_entry / count for bit-at-a-time decode
        let mut first_code = [0u32; MAX_BITS + 1];
        let mut first_entry = [0usize; MAX_BITS + 1];
        let mut count = [0usize; MAX_BITS + 1];

        // recompute first-code-per-length: next_code[] was consumed (mutated) during symbol-code assignment above.
        let mut code2: u32 = 0;
        for bits in 1..=MAX_BITS {
            code2 = (code2 + bl_count[bits - 1]) << 1;
            first_code[bits] = code2;
        }

        // Walk entries (sorted by len) to fill first_entry and count
        let mut idx = 0usize;
        for bits in 1..=MAX_BITS {
            first_entry[bits] = idx;
            while idx < entries.len() && entries[idx].1 == bits as u8 {
                count[bits] += 1;
                idx += 1;
            }
        }

        Ok(Huff { entries, first_code, first_entry, count })
    }

    /// Bit-at-a-time canonical Huffman decode.
    fn decode(&self, br: &mut BitReader<'_>) -> Result<u16, PqError> {
        let mut code: u32 = 0;
        for len in 1..=MAX_BITS {
            let bit = br.bits(1)?;
            code = (code << 1) | bit;
            if self.count[len] > 0 && code >= self.first_code[len] {
                let offset = (code - self.first_code[len]) as usize;
                if offset < self.count[len] {
                    let entry_idx = self.first_entry[len] + offset;
                    return Ok(self.entries[entry_idx].2);
                }
            }
        }
        Err(bad("huffman: no code matched (corrupt stream)"))
    }
}

// ── RFC 1951 §3.2.5 length/distance tables ──────────────────────────

// Length codes 257..285: base length + extra bits
// Index = symbol - 257
static LEN_BASE: [u32; 29] = [
    3, 4, 5, 6, 7, 8, 9, 10, // 257..264, 0 extra
    11, 13, 15, 17,           // 265..268, 1 extra
    19, 23, 27, 31,           // 269..272, 2 extra
    35, 43, 51, 59,           // 273..276, 3 extra
    67, 83, 99, 115,          // 277..280, 4 extra
    131, 163, 195, 227,       // 281..284, 5 extra
    258,                      // 285, 0 extra
];

static LEN_EXTRA: [u32; 29] = [
    0, 0, 0, 0, 0, 0, 0, 0, // 257..264
    1, 1, 1, 1,              // 265..268
    2, 2, 2, 2,              // 269..272
    3, 3, 3, 3,              // 273..276
    4, 4, 4, 4,              // 277..280
    5, 5, 5, 5,              // 281..284
    0,                       // 285
];

// Distance codes 0..29: base distance + extra bits
static DIST_BASE: [u32; 30] = [
    1, 2, 3, 4,         // 0..3, 0 extra
    5, 7,               // 4..5, 1 extra
    9, 13,              // 6..7, 2 extra
    17, 25,             // 8..9, 3 extra
    33, 49,             // 10..11, 4 extra
    65, 97,             // 12..13, 5 extra
    129, 193,           // 14..15, 6 extra
    257, 385,           // 16..17, 7 extra
    513, 769,           // 18..19, 8 extra
    1025, 1537,         // 20..21, 9 extra
    2049, 3073,         // 22..23, 10 extra
    4097, 6145,         // 24..25, 11 extra
    8193, 12289,        // 26..27, 12 extra
    16385, 24577,       // 28..29, 13 extra
];

static DIST_EXTRA: [u32; 30] = [
    0, 0, 0, 0,  // 0..3
    1, 1,        // 4..5
    2, 2,        // 6..7
    3, 3,        // 8..9
    4, 4,        // 10..11
    5, 5,        // 12..13
    6, 6,        // 14..15
    7, 7,        // 16..17
    8, 8,        // 18..19
    9, 9,        // 20..21
    10, 10,      // 22..23
    11, 11,      // 24..25
    12, 12,      // 26..27
    13, 13,      // 28..29
];

// ── RFC 1951 §3.2.6 fixed Huffman code lengths ───────────────────────

fn fixed_litlen_lengths() -> Vec<u8> {
    let mut lens = vec![0u8; 288];
    for i in 0..=143usize   { lens[i] = 8; }
    for i in 144..=255usize { lens[i] = 9; }
    for i in 256..=279usize { lens[i] = 7; }
    for i in 280..=287usize { lens[i] = 8; }
    lens
}

fn fixed_dist_lengths() -> Vec<u8> {
    vec![5u8; 32]
}

// ── RFC 1951 code-length Huffman permutation ─────────────────────────

// The order in which code lengths are read for the code-length alphabet
static CLCL_ORDER: [usize; 19] = [
    16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
];

// ── inflate ──────────────────────────────────────────────────────────

/// Decompress a raw DEFLATE stream (RFC 1951). `expected_len` is the
/// authority; output must equal it exactly. Iterative — no recursion.
fn inflate(deflate: &[u8], expected_len: usize) -> Result<Vec<u8>, PqError> {
    let mut br = BitReader::new(deflate);
    let mut out: Vec<u8> = Vec::with_capacity(expected_len);

    loop {
        let bfinal = br.bits(1)?;
        let btype = br.bits(2)?;

        match btype {
            0 => {
                // STORED block
                br.align_to_byte();
                let len = br.read_u16_le()?;
                let nlen = br.read_u16_le()?;
                if nlen != (!len & 0xFFFF) {
                    return Err(bad("deflate stored: NLEN != ~LEN"));
                }
                for _ in 0..len {
                    if out.len() >= expected_len {
                        return Err(bad("deflate overproduce (stored)"));
                    }
                    let b = br.bits(8)? as u8;
                    out.push(b);
                }
            }
            1 => {
                // Fixed Huffman
                let litlen_lens = fixed_litlen_lengths();
                let dist_lens = fixed_dist_lengths();
                let litlen = Huff::build(&litlen_lens)?;
                let dist = Huff::build(&dist_lens)?;
                inflate_symbols(&mut br, &litlen, &dist, &mut out, expected_len)?;
            }
            2 => {
                // Dynamic Huffman
                let hlit = br.bits(5)? + 257;
                let hdist = br.bits(5)? + 1;
                let hclen = br.bits(4)? + 4;

                // Read code-length code lengths
                let mut cl_lens = [0u8; 19];
                for i in 0..hclen as usize {
                    cl_lens[CLCL_ORDER[i]] = br.bits(3)? as u8;
                }
                let cl_huff = Huff::build(&cl_lens)?;

                // Decode hlit + hdist code lengths
                let total = (hlit + hdist) as usize;
                let mut all_lens: Vec<u8> = Vec::with_capacity(total);
                while all_lens.len() < total {
                    let sym = cl_huff.decode(&mut br)?;
                    match sym {
                        0..=15 => {
                            all_lens.push(sym as u8);
                        }
                        16 => {
                            // Copy previous 3 + bits(2) times
                            if all_lens.is_empty() {
                                return Err(bad("deflate dyn: repeat before first symbol"));
                            }
                            let prev = *all_lens.last().unwrap(); // safe: is_empty() guard above
                            let count = br.bits(2)? + 3;
                            for _ in 0..count {
                                if all_lens.len() >= total {
                                    return Err(bad("deflate dyn: code lengths overrun"));
                                }
                                all_lens.push(prev);
                            }
                        }
                        17 => {
                            // Zero 3 + bits(3) times
                            let count = br.bits(3)? + 3;
                            for _ in 0..count {
                                if all_lens.len() >= total {
                                    return Err(bad("deflate dyn: code lengths overrun (17)"));
                                }
                                all_lens.push(0);
                            }
                        }
                        18 => {
                            // Zero 11 + bits(7) times
                            let count = br.bits(7)? + 11;
                            for _ in 0..count {
                                if all_lens.len() >= total {
                                    return Err(bad("deflate dyn: code lengths overrun (18)"));
                                }
                                all_lens.push(0);
                            }
                        }
                        _ => {
                            return Err(bad("deflate dyn: invalid code-length symbol"));
                        }
                    }
                }

                let litlen_lens = &all_lens[..hlit as usize];
                let dist_lens = &all_lens[hlit as usize..];
                let litlen = Huff::build(litlen_lens)?;
                let dist = Huff::build(dist_lens)?;
                inflate_symbols(&mut br, &litlen, &dist, &mut out, expected_len)?;
            }
            3 => {
                return Err(bad("deflate reserved block type"));
            }
            // bits(2) yields 0..=3; all covered above — this arm is mathematically unreachable.
            _ => unreachable!(),
        }

        if bfinal == 1 {
            break;
        }
    }

    if out.len() != expected_len {
        return Err(bad("deflate length mismatch"));
    }
    Ok(out)
}

/// Decode lit/len/dist symbols for one DEFLATE block (fixed or dynamic).
/// Modifies `out` in place. Iterative — no recursion.
fn inflate_symbols(
    br: &mut BitReader<'_>,
    litlen: &Huff,
    dist: &Huff,
    out: &mut Vec<u8>,
    expected_len: usize,
) -> Result<(), PqError> {
    loop {
        let s = litlen.decode(br)?;
        if s < 256 {
            // Literal byte
            if out.len() >= expected_len {
                return Err(bad("deflate overproduce (literal)"));
            }
            out.push(s as u8);
        } else if s == 256 {
            // End of block
            break;
        } else if s <= 285 {
            // Length/distance back-reference
            let idx = (s - 257) as usize;
            if idx >= LEN_BASE.len() {
                return Err(bad("deflate: length symbol out of range"));
            }
            let length = LEN_BASE[idx] + br.bits(LEN_EXTRA[idx])?;

            let d = dist.decode(br)?;
            if d as usize >= DIST_BASE.len() {
                return Err(bad("deflate: distance code out of range"));
            }
            let distance = DIST_BASE[d as usize] + br.bits(DIST_EXTRA[d as usize])?;

            if distance == 0 || distance as usize > out.len() {
                return Err(bad("deflate: back-reference distance out of range"));
            }

            // Byte-wise overlapping copy (handles distance < length correctly)
            for _ in 0..length {
                if out.len() >= expected_len {
                    return Err(bad("deflate overproduce (backref)"));
                }
                let back_pos = out.len() - distance as usize;
                let b = out[back_pos];
                out.push(b);
            }
        } else {
            return Err(bad("deflate: symbol > 285"));
        }
    }
    Ok(())
}

// ── RFC 1952 gzip member decompressor ───────────────────────────────

/// Decompress one RFC 1952 gzip member. `expected_len` is the Parquet
/// page header's `uncompressed_page_size` — used as both allocation
/// authority and an ISIZE cross-check. Rejects members where ISIZE ≠
/// expected_len or CRC32 mismatches.
pub fn decompress(src: &[u8], expected_len: usize) -> Result<Vec<u8>, PqError> {
    // Hard cap before any allocation
    if expected_len > GZIP_MAX_DECOMP {
        return Err(PqError::Unsupported(format!(
            "gzip page {expected_len} exceeds {GZIP_MAX_DECOMP} cap: OBJ-2c"
        )));
    }

    // Minimum gzip member: 10-byte header + 0 deflate + 8-byte trailer
    if src.len() < 18 {
        return Err(bad("gzip: member too short"));
    }

    // Magic bytes and method
    if src.get(0).copied() != Some(0x1f) || src.get(1).copied() != Some(0x8b) {
        return Err(bad("gzip magic"));
    }
    if src.get(2).copied() != Some(8) {
        return Err(PqError::Unsupported(
            "gzip method != deflate: OBJ-2c".into(),
        ));
    }

    let flg = *src.get(3).ok_or_else(|| bad("gzip: truncated header (flg)"))?;
    let mut pos: usize = 10; // skip fixed 10-byte header

    // FEXTRA (bit 2)
    if flg & 0x04 != 0 {
        let lo = *src.get(pos).ok_or_else(|| bad("gzip: FEXTRA xlen truncated lo"))?;
        let hi = *src.get(pos + 1).ok_or_else(|| bad("gzip: FEXTRA xlen truncated hi"))?;
        let xlen = u16::from_le_bytes([lo, hi]) as usize;
        pos = pos
            .checked_add(2)
            .and_then(|p| p.checked_add(xlen))
            .ok_or_else(|| bad("gzip: FEXTRA pos overflow"))?;
    }

    // FNAME (bit 3): NUL-terminated string
    if flg & 0x08 != 0 {
        loop {
            if pos >= src.len().saturating_sub(8) {
                return Err(bad("gzip: FNAME not NUL-terminated before trailer"));
            }
            let b = *src.get(pos).ok_or_else(|| bad("gzip: FNAME truncated"))?;
            pos += 1;
            if b == 0 {
                break;
            }
        }
    }

    // FCOMMENT (bit 4): NUL-terminated string
    if flg & 0x10 != 0 {
        loop {
            if pos >= src.len().saturating_sub(8) {
                return Err(bad("gzip: FCOMMENT not NUL-terminated before trailer"));
            }
            let b = *src.get(pos).ok_or_else(|| bad("gzip: FCOMMENT truncated"))?;
            pos += 1;
            if b == 0 {
                break;
            }
        }
    }

    // FHCRC (bit 1): 2-byte header CRC (we skip, not verify)
    if flg & 0x02 != 0 {
        pos = pos.checked_add(2).ok_or_else(|| bad("gzip: FHCRC pos overflow"))?;
    }

    if pos > src.len().saturating_sub(8) {
        return Err(bad("gzip: header overruns trailer region"));
    }

    // Trailer: last 8 bytes = CRC32(4 LE) | ISIZE(4 LE)
    let trailer_start = src.len() - 8;
    let trailer = src
        .get(trailer_start..src.len())
        .ok_or_else(|| bad("gzip: trailer truncated"))?;
    let crc_stored = u32::from_le_bytes(
        trailer[0..4].try_into().unwrap(), // statically infallible: slice len == 4
    );
    let isize_stored = u32::from_le_bytes(
        trailer[4..8].try_into().unwrap(), // statically infallible: slice len == 4
    );

    // ISIZE cross-check
    if isize_stored != expected_len as u32 {
        return Err(bad("gzip isize"));
    }

    // Deflate payload is between header-end and trailer
    let deflate = src
        .get(pos..trailer_start)
        .ok_or_else(|| bad("gzip: deflate region invalid"))?;

    let out = inflate(deflate, expected_len)?;

    // CRC32 verify
    if crc32(&out) != crc_stored {
        return Err(bad("gzip crc mismatch"));
    }

    Ok(out)
}

// ── Tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // CRC-32/ISO-HDLC universal check value (RFC 3309 / zlib): the
    // CRC of b"123456789" is 0xCBF43926. Independent published
    // authority — pins crc32() non-self-referentially.
    #[test]
    fn kat_crc32_canonical_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    // RFC 1951 §3.2.4 STORED block, hand-derived:
    // first DEFLATE byte low bits = BFINAL(1) | BTYPE(00) = 0b001
    //   → byte 0x01 (remaining bits ignored / skip to byte boundary)
    // LEN = 5 (LE u16) = 05 00 ; NLEN = !5 = 0xFFFA (LE) = FA FF
    // then 5 raw bytes "hello".
    #[test]
    fn kat_inflate_stored_block() {
        let deflate = [
            0x01, 0x05, 0x00, 0xFA, 0xFF,
            b'h', b'e', b'l', b'l', b'o',
        ];
        assert_eq!(inflate(&deflate, 5).unwrap(), b"hello".to_vec());
    }

    // Python zlib reference (raw DEFLATE, fixed Huffman) of
    // b"hello world" — captured via the Task-1 Step-2 command.
    #[test]
    fn kat_inflate_fixed_huffman() {
        let deflate: &[u8] = &[
            0xcb,0x48,0xcd,0xc9,0xc9,0x57,0x28,0xcf,0x2f,0xca,0x49,0x01,0x00
        ];
        assert_eq!(
            inflate(deflate, 11).unwrap(),
            b"hello world".to_vec()
        );
    }

    // Python zlib reference (raw DEFLATE, dynamic Huffman) of
    // payload = bytes((i*7+3)%251 for i in range(400)).
    #[test]
    fn kat_inflate_dynamic_huffman() {
        let deflate: &[u8] = &[
            0x63,0xe6,0x12,0x94,0x90,0x57,0xd3,0x35,0xb1,0x76,0xf2,0x0c,
            0x08,0x8f,0x4b,0xcd,0x29,0xae,0x6a,0xec,0xe8,0x9f,0x36,0x77,
            0xc9,0xea,0x4d,0x3b,0x0f,0x1c,0x3f,0x77,0xf5,0xce,0xe3,0x57,
            0x1f,0x7f,0xb0,0x70,0x0b,0x49,0x2a,0xa8,0xeb,0x99,0xda,0x38,
            0x7b,0x05,0x46,0xc4,0xa7,0xe5,0x96,0x54,0x37,0x75,0x4e,0x98,
            0x3e,0x6f,0xe9,0x9a,0xcd,0xbb,0x0e,0x9e,0x38,0x7f,0xed,0xee,
            0x93,0xd7,0x9f,0x7e,0xb2,0xf2,0x08,0x4b,0x29,0x6a,0xe8,0x9b,
            0xd9,0xba,0x78,0x07,0x45,0x26,0xa4,0xe7,0x95,0xd6,0x34,0x77,
            0x4d,0x9c,0x31,0x7f,0xd9,0xda,0x2d,0xbb,0x0f,0x9d,0xbc,0x70,
            0xfd,0xde,0xd3,0x37,0x9f,0x7f,0xb1,0xf1,0x8a,0x48,0x2b,0x69,
            0x1a,0x98,0xdb,0xb9,0xfa,0x04,0x47,0x25,0x66,0xe4,0x97,0xd5,
            0xb6,0x74,0x4f,0x9a,0xb9,0x60,0xf9,0xba,0xad,0x7b,0x0e,0x9f,
            0xba,0x78,0xe3,0xfe,0xb3,0xb7,0x5f,0x18,0xd8,0xf9,0x44,0x65,
            0x94,0xb5,0x0c,0x2d,0xec,0xdd,0x7c,0x43,0xa2,0x93,0x32,0x0b,
            0xca,0xeb,0x5a,0x7b,0x26,0xcf,0x5a,0xb8,0x62,0xfd,0xb6,0xbd,
            0x47,0x4e,0x5f,0xba,0xf9,0xe0,0xf9,0xbb,0xaf,0x8c,0x1c,0xfc,
            0x62,0xb2,0x2a,0xda,0x46,0x96,0x0e,0xee,0x7e,0xa1,0x31,0xc9,
            0x59,0x85,0x15,0xf5,0x6d,0xbd,0x53,0x66,0x2f,0x5a,0xb9,0x61,
            0xfb,0xbe,0xa3,0x67,0x2e,0xdf,0x7a,0xf8,0xe2,0xfd,0x37,0x26,
            0x4e,0x01,0x71,0x39,0x55,0x1d,0x63,0x2b,0x47,0x0f,0xff,0xb0,
            0xd8,0x94,0xec,0xa2,0xca,0x86,0xf6,0xbe,0xa9,0x73,0x16,0xaf,
            0xda,0xb8,0x63,0xff,0xb1,0xb3,0x57,0x6e,0x3f,0x7a,0xf9,0xe1,
            0x3b,0xf3,0x60,0xf4,0x3a,0x00
        ];
        let want: Vec<u8> =
            (0..400u32).map(|i| ((i * 7 + 3) % 251) as u8).collect();
        assert_eq!(inflate(deflate, 400).unwrap(), want);
    }

    // Overlapping back-reference (RLE) correctness: zlib raw deflate
    // of b"a"*8 — captured via:
    //   python -c "import zlib;co=zlib.compressobj(9,8,-15);
    //   ..."
    // Proves byte-wise overlapping copy (distance 1 < length).
    #[test]
    fn kat_inflate_overlapping_backref() {
        let deflate: &[u8] = &[0x4b,0x4c,0x84,0x00,0x00];
        assert_eq!(inflate(deflate, 8).unwrap(), vec![b'a'; 8]);
    }

    // Full RFC 1952 gzip member of b"AB" (python gzip.compress).
    #[test]
    fn kat_decompress_gzip_member() {
        let member: &[u8] = &[
            0x1f,0x8b,0x08,0x00,0xc5,0x9b,0x0c,0x6a,0x02,0xff,
            0x73,0x74,0x02,0x00,0x07,0x4c,0x69,0x30,0x02,0x00,0x00,0x00
        ];
        assert_eq!(decompress(member, 2).unwrap(), b"AB".to_vec());
    }

    // ISIZE mismatch → Bad. Take GZIP_AB, pass wrong expected_len.
    #[test]
    fn kat_isize_mismatch_is_bad() {
        let member: &[u8] = &[
            0x1f,0x8b,0x08,0x00,0xc5,0x9b,0x0c,0x6a,0x02,0xff,
            0x73,0x74,0x02,0x00,0x07,0x4c,0x69,0x30,0x02,0x00,0x00,0x00
        ];
        assert!(matches!(decompress(member, 99), Err(PqError::Bad(_))));
    }

    // Over-cap → Unsupported BEFORE allocation.
    #[test]
    fn kat_over_cap_is_unsupported() {
        let member: &[u8] = &[
            0x1f,0x8b,0x08,0x00,0xc5,0x9b,0x0c,0x6a,0x02,0xff,
            0x73,0x74,0x02,0x00,0x07,0x4c,0x69,0x30,0x02,0x00,0x00,0x00
        ];
        assert!(matches!(
            decompress(member, GZIP_MAX_DECOMP + 1),
            Err(PqError::Unsupported(_))
        ));
    }
}

// ── PENTEST PASS — adversarial lock tests ─────────────────────────────
//
// Gzip page bytes are operator/network-source-controlled. Each hostile
// case is wrapped in catch_unwind asserting no panic/OOM/stack-overflow
// AND a typed Result (Bad or Unsupported). The positive correctness
// locks assert exact Ok plaintext — a failure there means the decoder
// is wrong, NEVER weaken a positive lock.
//
// Conventions:
//  - Valid reference member "GZIP_AB": python gzip.compress(b"AB"),
//    decompresses to b"AB", expected_len=2.  Used for over_cap,
//    bomb_bounded (trailer ISIZE patched), isize_mismatch,
//    crc_mismatch (trailer CRC byte flipped).
//  - All hostile gzip members are hand-constructed from RFC 1951/1952.
//  - The four positive inflate locks reuse the Task-1 KAT vectors.
//  - The gzip ∘ dict ∘ OPTIONAL composition is already proven by the
//    T4 gzip_nullable roundtrip; the gzip unit pentest covers the
//    decompress() + inflate() functions specifically.
#[cfg(test)]
mod pentest {
    use super::*;

    // ── Helper ────────────────────────────────────────────────────────
    //
    // nb(src, expected_len):
    //   Wrap decompress(src, expected_len) in catch_unwind.
    //   Assert: (1) no panic/unwind; (2) result is Err(Bad) or Err(Unsupported).
    //   Mirrors the snappy.rs mod pentest `nb` shape exactly.
    fn nb(src: &[u8], expected: usize) {
        let s = src.to_vec();
        let r = std::panic::catch_unwind(move || decompress(&s, expected));
        assert!(r.is_ok(), "must NOT panic/OOM-unwind");
        assert!(
            matches!(r.unwrap(),
                Err(PqError::Bad(_)) | Err(PqError::Unsupported(_))),
            "hostile input must be a typed error"
        );
    }

    // ── over_cap ──────────────────────────────────────────────────────
    //
    // A valid gzip member (GZIP_AB) with expected_len = GZIP_MAX_DECOMP+1.
    // The cap check fires before any alloc → Unsupported. No multi-GB
    // allocation attempted.
    #[test]
    fn over_cap() {
        // GZIP_AB: python gzip.compress(b"AB") — decompresses to b"AB"
        // (expected_len=2 in normal use; here we pass an over-cap value).
        let member: &[u8] = &[
            0x1f,0x8b,0x08,0x00,0xc5,0x9b,0x0c,0x6a,0x02,0xff,
            0x73,0x74,0x02,0x00,0x07,0x4c,0x69,0x30,0x02,0x00,0x00,0x00,
        ];
        nb(member, GZIP_MAX_DECOMP + 1);
    }

    // ── bomb_bounded ──────────────────────────────────────────────────
    //
    // GZIP_AB with the ISIZE trailer field patched to 100 (LE u32).
    // expected_len=100 (within cap). ISIZE check passes (100==100).
    // Vec::with_capacity(100) is a safe 100-byte alloc — no OOM risk.
    // inflate() runs: the DEFLATE stream legitimately produces 2 bytes
    // (b"AB") then emits end-of-block; the inflate loop exits normally.
    // The post-loop guard `out.len()(2) != expected_len(100)` then
    // returns Bad("deflate length mismatch"). Detection is the
    // post-loop length check, not a mid-stream abort.
    //
    // Hand-construction: GZIP_AB bytes 18-21 are the ISIZE LE u32.
    // Original ISIZE = 02 00 00 00 (=2). Patched = 64 00 00 00 (=100).
    // CRC bytes 14-17 are left as crc32(b"AB")=0x30694c07; inflate
    // fails before the CRC check so CRC value is irrelevant to this path.
    #[test]
    fn bomb_bounded() {
        let member: &[u8] = &[
            0x1f,0x8b,0x08,0x00,0xc5,0x9b,0x0c,0x6a,0x02,0xff,
            0x73,0x74,0x02,0x00,0x07,0x4c,0x69,0x30,
            0x64,0x00,0x00,0x00, // ISIZE=100 (patched from 2)
        ];
        nb(member, 100);
    }

    // ── bad_magic ─────────────────────────────────────────────────────
    //
    // 18 bytes not starting with 1f 8b → Bad("gzip magic").
    #[test]
    fn bad_magic() {
        let member: &[u8] = &[
            0x00,0x00,0x08,0x00,0x00,0x00,0x00,0x00,0x00,0xff,
            0x73,0x74,0x02,0x00,0x07,0x4c,0x69,0x30,
        ];
        nb(member, 2);
    }

    // ── cm_not_deflate ────────────────────────────────────────────────
    //
    // A member with src[2]=0x09 (CM != 8 / not DEFLATE).
    // → Unsupported("gzip method != deflate: OBJ-2c").
    // Hand-build: valid magic (1f 8b), CM=0x09, rest can be zero
    // (padded to 18 bytes so the length check passes).
    #[test]
    fn cm_not_deflate() {
        let member: &[u8] = &[
            0x1f,0x8b,0x09,0x00,0x00,0x00,0x00,0x00,0x00,0xff,
            0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
        ];
        nb(member, 1);
    }

    // ── truncated_header ──────────────────────────────────────────────
    //
    // Only 2 bytes [1f 8b] — member too short (<18) → Bad.
    #[test]
    fn truncated_header() {
        nb(&[0x1f, 0x8b], 1);
    }

    // ── lying_fextra ─────────────────────────────────────────────────
    //
    // FLG bit 2 (FEXTRA) set; XLEN = 0xffff (65535).
    // After reading XLEN the header parser advances pos to
    // 10 + 2 + 65535 = 65547; the guard
    // `pos (65547) > src.len().saturating_sub(8) (= 18-8 = 10)`
    // fires → Bad("gzip: header overruns trailer region").
    // Member is 18 bytes (minimum); last 8 bytes are the "trailer"
    // region — none of it is reachable.
    //
    // RFC 1952: FLG bit 2 = FEXTRA; XLEN is LE u16 at pos=10.
    // Byte layout:
    //   [0]1f [1]8b [2]08 [3]04(FLG) [4..7]MTIME [8]XFL [9]OS=ff
    //   [10]ff [11]ff  ← XLEN = 65535 (LE)
    //   [12..17] padding
    #[test]
    fn lying_fextra() {
        let member: &[u8] = &[
            0x1f,0x8b,0x08,0x04,0x00,0x00,0x00,0x00,0x00,0xff,
            0xff,0xff,                          // XLEN = 65535
            0x00,0x00,0x00,0x00,0x00,0x00,      // padding to 18 bytes
        ];
        nb(member, 1);
    }

    // ── unterminated_fname ────────────────────────────────────────────
    //
    // FLG bit 3 (FNAME) set; no NUL byte before the 8-byte trailer
    // region (positions 10..src.len()-8). The scan immediately hits
    // the `pos >= src.len()-8` guard → Bad("gzip: FNAME not
    // NUL-terminated before trailer").
    //
    // With 18 bytes: src.len()-8 = 10. FNAME scan starts at pos=10,
    // immediately 10 >= 10 → Bad.
    #[test]
    fn unterminated_fname() {
        let member: &[u8] = &[
            0x1f,0x8b,0x08,0x08,0x00,0x00,0x00,0x00,0x00,0xff,
            0x41,0x42,0x43,0x44,0x45,0x46,0x47,0x48, // "ABCDEFGH", no NUL
        ];
        nb(member, 1);
    }

    // ── truncated_deflate ─────────────────────────────────────────────
    //
    // Valid gzip header (1f 8b 08 00 … os=ff, pos=10) but DEFLATE
    // stream cut to 1 byte [0x73] before the 8-byte trailer.
    // 0x73 = 0b01110011: bit0=1(BFINAL), bits1-2=01(BTYPE=fixed).
    // Then the fixed-Huffman litlen decode tries to read 7 bits (for
    // codes 256-279) starting at bit3. Only bits3..7 (5 bits) are
    // present in byte 0x73; the next byte is the trailer (treated as
    // outside deflate). BitReader hits end → Bad("deflate truncated
    // (bit read)").
    // ISIZE = 2 (trailer), expected_len = 2 → ISIZE check passes.
    // CRC = 0 (placeholder; inflate fails first, CRC never checked).
    //
    // Byte layout (19 bytes):
    //   [0..9] standard header, [10] 0x73 (deflate 1 byte),
    //   [11..14] 00 00 00 00 (CRC placeholder),
    //   [15..18] 02 00 00 00 (ISIZE=2)
    #[test]
    fn truncated_deflate() {
        let member: &[u8] = &[
            0x1f,0x8b,0x08,0x00,0x00,0x00,0x00,0x00,0x00,0xff,
            0x73,                               // 1 deflate byte
            0x00,0x00,0x00,0x00,                // CRC placeholder
            0x02,0x00,0x00,0x00,                // ISIZE=2
        ];
        nb(member, 2);
    }

    // ── reserved_btype ───────────────────────────────────────────────
    //
    // DEFLATE first byte = 0x07: bit0=1(BFINAL), bits1-2=11(BTYPE=3,
    // reserved) → Bad("deflate reserved block type") immediately.
    // The inflate() function never reads further.
    // ISIZE = 1 (= expected_len), CRC = 0 (inflate fails, CRC unchecked).
    //
    // Byte layout (20 bytes):
    //   [0..9] standard header, [10] 0x07 (reserved btype),
    //   [11..14] 00 00 00 00 (CRC placeholder),
    //   [15..18] 01 00 00 00 (ISIZE=1), [19] padding to ≥18
    // Wait: len = 19 ≥ 18 ✓.
    #[test]
    fn reserved_btype() {
        let member: &[u8] = &[
            0x1f,0x8b,0x08,0x00,0x00,0x00,0x00,0x00,0x00,0xff,
            0x07,                               // BFINAL=1,BTYPE=3(reserved)
            0x00,0x00,0x00,0x00,                // CRC placeholder
            0x01,0x00,0x00,0x00,                // ISIZE=1
        ];
        nb(member, 1);
    }

    // ── bad_dynamic_huffman ───────────────────────────────────────────
    //
    // A dynamic block whose code-length stream overruns HLIT+HDIST=258.
    // Hand-crafted from RFC 1951:
    //
    //   BFINAL=1, BTYPE=10 (dynamic):
    //     bit0=1, bits1-2=01 (remember bits1=BTYPE[0]=0, bits2=BTYPE[1]=1
    //     for BTYPE=2). Wait: btype=br.bits(2) reads bit1 then bit2:
    //     result = bit1|(bit2<<1). For btype=2: bit1=0, bit2=1. So
    //     byte0 bits: bit0=1(BFINAL), bit1=0(BTYPE[0]), bit2=1(BTYPE[1]).
    //
    //   Bit sequence:
    //     bit0=1  BFINAL
    //     bit1=0  BTYPE[0]   → BTYPE = (bit1=0)|(bit2<<1=2) = 2 = dynamic
    //     bit2=1  BTYPE[1]
    //     bits3-7 = HLIT(5 bits) = 0 → HLIT=257
    //     bits8-12 = HDIST(5 bits) = 0 → HDIST=1
    //     bits13-16 = HCLEN(4 bits) = 0 → HCLEN=4
    //     code-length lengths for [16,17,18,0] (4 entries × 3 bits):
    //       cl[16]=0 (bits17-19=0,0,0)
    //       cl[17]=0 (bits20-22=0,0,0)
    //       cl[18]=1 (bits23-25 = 1,0,0 LSB-first = value 1)
    //       cl[0]=0  (bits26-28=0,0,0)
    //     CL Huffman: only symbol 18 has len=1, code=0 (1-bit: bit=0).
    //     CL decode loop needs to emit 258 lengths total.
    //       Read bit29=0 → symbol 18 → bits(7) extra: bits30-36=1111111=127
    //         → emit 11+127=138 zeros. Count=138.
    //       Read bit37=0 → symbol 18 → bits(7) extra: bits38-44=1111111=127
    //         → would emit 138 more → total 276 > 258 →
    //         Bad("deflate dyn: code lengths overrun (18)").
    //
    //   Byte layout (bit-packed, LSB-first within each byte):
    //     byte0 = bits0-7: 1,0,1,0,0,0,0,0 → 0b00000101 = 0x05
    //       (bit0=BFINAL=1, bit1=BTYPE[0]=0, bit2=BTYPE[1]=1,
    //        bits3-7=HLIT bits0-4=0)
    //     byte1 = bits8-15: 0,0,0,0,0,0,0,0 → 0x00
    //       (bits8-12=HDIST=0, bits13-15=HCLEN bits0-2=0)
    //     byte2 = bits16-23: 0,0,0,0,0,0,0,1 → 0b10000000 = 0x80
    //       (bit16=HCLEN bit3=0; bits17-19=cl[16]=0,0,0;
    //        bits20-22=cl[17]=0,0,0; bit23=cl[18]bit0=1)
    //     byte3 = bits24-31: 0,0,0,0,0,0,0,1 → 0b11000000... wait:
    //       bit24=cl[18]bit1=0, bit25=cl[18]bit2=0,
    //       bits26-28=cl[0]=0,0,0, bit29=CL_code_for_sym18=0,
    //       bits30-31=extra bits0-1=1,1
    //       → 0b11000000 = 0xC0
    //     byte4 = bits32-39: 1,1,1,1,1,0,1,1
    //       bit32=extra bit2=1, bit33=bit3=1, bit34=bit4=1,
    //       bit35=bit5=1, bit36=bit6=1 (extra done, 7 bits=1111111),
    //       bit37=CL_code_for_sym18(2nd)=0,
    //       bits38-39=extra2 bits0-1=1,1
    //       → 0b11011111 = 0xDF
    //     byte5 = bits40-47: 1,1,1,1,1,... (extra2 bits2-6=1,1,1,1,1; rest don't matter)
    //       → at least 0x1F (low 5 bits=1; guard fires before reading further)
    //
    //   Gzip wrapper: ISIZE=10(=expected_len), CRC=0 (inflate fails first).
    #[test]
    fn bad_dynamic_huffman() {
        let member: &[u8] = &[
            0x1f,0x8b,0x08,0x00,0x00,0x00,0x00,0x00,0x00,0xff, // header
            0x05,0x00,0x80,0xC0,0xDF,0x1F,                     // deflate: dyn block
            0x00,0x00,0x00,0x00,                                // CRC placeholder
            0x0a,0x00,0x00,0x00,                                // ISIZE=10
        ];
        nb(member, 10);
    }

    // ── distance_before_output ────────────────────────────────────────
    //
    // A fixed-Huffman stream whose first emitted symbol is a
    // length/distance back-reference with distance > out.len() (=0).
    // → Bad("deflate: back-reference distance out of range").
    //
    // Hand-crafted DEFLATE (RFC 1951 fixed Huffman, §3.2.6):
    //
    //   BFINAL=1, BTYPE=01 (fixed Huffman): bit0=1, bit1=1(BTYPE[0]), bit2=0(BTYPE[1]).
    //   Huffman reads begin at bit3.
    //
    //   Litlen decode (7-bit canonical codes, first_code[7]=0):
    //     The decoder accumulates one bit at a time (MSB-first canonical order).
    //     bits3..9 = 0,0,0,0,0,0,1 → code=1 at length 7.
    //     first_code[7]=0, offset=1-0=1, entries[1].symbol=257. ✓
    //     Symbol 257: length base=3, EXTRA_LEN[0]=0 extra bits → length=3.
    //
    //   Dist decode (5-bit codes, first_code[5]=0):
    //     bits10..14 = 0,0,0,0,0 → code=0 at length 5 → dist_code=0.
    //     distance = DIST_BASE[0] + bits(DIST_EXTRA[0]) = 1 + 0 = 1.
    //
    //   out.len()=0 at this point → distance(1) > out.len()(0)
    //     → Bad("deflate: back-reference distance out of range"). ✓
    //
    //   Byte layout (LSB-first within each byte, RFC 1951 §3.1.1):
    //     byte0 = 0x03 = 0b00000011:
    //       bit0=BFINAL=1, bit1=BTYPE[0]=1(fixed), bit2=BTYPE[1]=0,
    //       bits3-7=litlen accumulator bits 0-4 (all 0).
    //     byte1 = 0x02 = 0b00000010:
    //       bit0=litlen acc bit5=0, bit1=litlen acc bit6=1 → code=1, sym=257;
    //       bits2-6=dist accumulator bits 0-4 (all 0) → dist_code=0.
    //
    //   Gzip wrapper: ISIZE=3 (= expected_len), CRC=0 (inflate fails, CRC unchecked).
    #[test]
    fn distance_before_output() {
        let member: &[u8] = &[
            0x1f,0x8b,0x08,0x00,0x00,0x00,0x00,0x00,0x00,0xff, // header
            0x03,0x02,                                          // deflate: fixed, sym257, dist0
            0x00,0x00,0x00,0x00,                                // CRC placeholder
            0x03,0x00,0x00,0x00,                                // ISIZE=3
        ];
        nb(member, 3);
    }

    // ── stored_nlen_mismatch ──────────────────────────────────────────
    //
    // STORED block with NLEN != ~LEN.
    // RFC 1951 §3.2.4: after align, read LEN (LE u16) then NLEN (LE u16).
    // Require NLEN == !LEN & 0xFFFF.
    //
    // BFINAL=1, BTYPE=00 (stored): byte 0x01 (bit0=1=BFINAL, bits1-2=00=BTYPE=0).
    // After align: LEN=0x0500 (5 LE → bytes 05 00 but wait, LE means byte0=lo=5,
    // byte1=hi=0, so LEN=5). NLEN=0x0000 (should be !5=0xFFFA): bytes 00 00.
    // Expected_len=1, ISIZE=1 in trailer. Inflate fails at NLEN check.
    //
    // Gzip wrapper (21 bytes):
    //   [0..9] header, [10] 0x01 (BFINAL+BTYPE=stored), [11..12] 05 00 (LEN=5),
    //   [13..14] 00 00 (NLEN=0 ≠ 0xFFFA), [15..18] 00 00 00 00 (CRC),
    //   [19..22] 01 00 00 00 (ISIZE=1)
    //   Wait that's 23 bytes, ≥ 18 ✓.
    #[test]
    fn stored_nlen_mismatch() {
        let member: &[u8] = &[
            0x1f,0x8b,0x08,0x00,0x00,0x00,0x00,0x00,0x00,0xff, // header
            0x01,                                               // BFINAL=1, BTYPE=0 (stored)
            0x05,0x00,                                          // LEN=5
            0x00,0x00,                                          // NLEN=0 (should be 0xFFFA)
            0x00,0x00,0x00,0x00,                                // CRC placeholder
            0x01,0x00,0x00,0x00,                                // ISIZE=1
        ];
        nb(member, 1);
    }

    // ── isize_mismatch ────────────────────────────────────────────────
    //
    // GZIP_AB (ISIZE=2 in trailer) with expected_len=99 → ISIZE(2) ≠ 99
    // → Bad("gzip isize"). No inflation attempted.
    #[test]
    fn isize_mismatch() {
        let member: &[u8] = &[
            0x1f,0x8b,0x08,0x00,0xc5,0x9b,0x0c,0x6a,0x02,0xff,
            0x73,0x74,0x02,0x00,0x07,0x4c,0x69,0x30,0x02,0x00,0x00,0x00,
        ];
        nb(member, 99);
    }

    // ── crc_mismatch ─────────────────────────────────────────────────
    //
    // GZIP_AB with the first CRC byte in the trailer flipped
    // (0x07 → 0x08). inflate() succeeds and produces b"AB".
    // crc32(b"AB") = 0x30694c07 ≠ stored 0x30694c08 →
    // Bad("gzip crc mismatch"). This genuinely exercises the CRC
    // verification path: inflate produces output first, then CRC rejects.
    //
    // Trailer original: [07 4c 69 30] CRC | [02 00 00 00] ISIZE.
    // Patched:          [08 4c 69 30] CRC (first byte 07→08).
    #[test]
    fn crc_mismatch() {
        let member: &[u8] = &[
            0x1f,0x8b,0x08,0x00,0xc5,0x9b,0x0c,0x6a,0x02,0xff,
            0x73,0x74,0x02,0x00,
            0x08,0x4c,0x69,0x30, // CRC first byte flipped 07→08
            0x02,0x00,0x00,0x00, // ISIZE=2
        ];
        // Pass expected_len=2 so ISIZE check passes; then inflate→Ok(b"AB");
        // then CRC check fires → Bad.
        nb(member, 2);
    }

    // ── Positive correctness locks ────────────────────────────────────
    //
    // MUST decode Ok with exact plaintext. A failure here means the
    // decoder has a bug — NEVER weaken these assertions.

    // STORED block: inflate([0x01,0x05,0x00,0xFA,0xFF,'h','e','l','l','o'], 5)
    // → b"hello" (reuses Task-1 KAT vector; hand-derived from RFC 1951).
    #[test]
    fn positive_stored_block() {
        let deflate = [
            0x01, 0x05, 0x00, 0xFA, 0xFF,
            b'h', b'e', b'l', b'l', b'o',
        ];
        let r = std::panic::catch_unwind(move || inflate(&deflate, 5));
        assert!(r.is_ok(), "stored block must not panic");
        assert_eq!(r.unwrap().unwrap(), b"hello".to_vec(),
            "stored block must decode to b\"hello\"");
    }

    // Fixed Huffman: zlib raw DEFLATE of b"hello world" (Task-1 KAT).
    #[test]
    fn positive_fixed_huffman() {
        let deflate: &[u8] = &[
            0xcb,0x48,0xcd,0xc9,0xc9,0x57,0x28,0xcf,0x2f,0xca,0x49,0x01,0x00,
        ];
        let d = deflate.to_vec();
        let r = std::panic::catch_unwind(move || inflate(&d, 11));
        assert!(r.is_ok(), "fixed Huffman must not panic");
        assert_eq!(r.unwrap().unwrap(), b"hello world".to_vec(),
            "fixed Huffman must decode to b\"hello world\"");
    }

    // Dynamic Huffman: zlib raw DEFLATE of bytes((i*7+3)%251 for i in range(400))
    // (Task-1 KAT).
    #[test]
    fn positive_dynamic_huffman() {
        let deflate: &[u8] = &[
            0x63,0xe6,0x12,0x94,0x90,0x57,0xd3,0x35,0xb1,0x76,0xf2,0x0c,
            0x08,0x8f,0x4b,0xcd,0x29,0xae,0x6a,0xec,0xe8,0x9f,0x36,0x77,
            0xc9,0xea,0x4d,0x3b,0x0f,0x1c,0x3f,0x77,0xf5,0xce,0xe3,0x57,
            0x1f,0x7f,0xb0,0x70,0x0b,0x49,0x2a,0xa8,0xeb,0x99,0xda,0x38,
            0x7b,0x05,0x46,0xc4,0xa7,0xe5,0x96,0x54,0x37,0x75,0x4e,0x98,
            0x3e,0x6f,0xe9,0x9a,0xcd,0xbb,0x0e,0x9e,0x38,0x7f,0xed,0xee,
            0x93,0xd7,0x9f,0x7e,0xb2,0xf2,0x08,0x4b,0x29,0x6a,0xe8,0x9b,
            0xd9,0xba,0x78,0x07,0x45,0x26,0xa4,0xe7,0x95,0xd6,0x34,0x77,
            0x4d,0x9c,0x31,0x7f,0xd9,0xda,0x2d,0xbb,0x0f,0x9d,0xbc,0x70,
            0xfd,0xde,0xd3,0x37,0x9f,0x7f,0xb1,0xf1,0x8a,0x48,0x2b,0x69,
            0x1a,0x98,0xdb,0xb9,0xfa,0x04,0x47,0x25,0x66,0xe4,0x97,0xd5,
            0xb6,0x74,0x4f,0x9a,0xb9,0x60,0xf9,0xba,0xad,0x7b,0x0e,0x9f,
            0xba,0x78,0xe3,0xfe,0xb3,0xb7,0x5f,0x18,0xd8,0xf9,0x44,0x65,
            0x94,0xb5,0x0c,0x2d,0xec,0xdd,0x7c,0x43,0xa2,0x93,0x32,0x0b,
            0xca,0xeb,0x5a,0x7b,0x26,0xcf,0x5a,0xb8,0x62,0xfd,0xb6,0xbd,
            0x47,0x4e,0x5f,0xba,0xf9,0xe0,0xf9,0xbb,0xaf,0x8c,0x1c,0xfc,
            0x62,0xb2,0x2a,0xda,0x46,0x96,0x0e,0xee,0x7e,0xa1,0x31,0xc9,
            0x59,0x85,0x15,0xf5,0x6d,0xbd,0x53,0x66,0x2f,0x5a,0xb9,0x61,
            0xfb,0xbe,0xa3,0x67,0x2e,0xdf,0x7a,0xf8,0xe2,0xfd,0x37,0x26,
            0x4e,0x01,0x71,0x39,0x55,0x1d,0x63,0x2b,0x47,0x0f,0xff,0xb0,
            0xd8,0x94,0xec,0xa2,0xca,0x86,0xf6,0xbe,0xa9,0x73,0x16,0xaf,
            0xda,0xb8,0x63,0xff,0xb1,0xb3,0x57,0x6e,0x3f,0x7a,0xf9,0xe1,
            0x3b,0xf3,0x60,0xf4,0x3a,0x00,
        ];
        let want: Vec<u8> =
            (0..400u32).map(|i| ((i * 7 + 3) % 251) as u8).collect();
        let d = deflate.to_vec();
        let r = std::panic::catch_unwind(move || inflate(&d, 400));
        assert!(r.is_ok(), "dynamic Huffman must not panic");
        assert_eq!(r.unwrap().unwrap(), want,
            "dynamic Huffman must decode to the expected payload");
    }

    // Overlapping back-reference (RLE): zlib raw DEFLATE of b"a"*8
    // (Task-1 KAT). Proves byte-wise overlapping copy (distance 1 < length 7).
    #[test]
    fn positive_overlapping_backref() {
        let deflate: &[u8] = &[0x4b, 0x4c, 0x84, 0x00, 0x00];
        let d = deflate.to_vec();
        let r = std::panic::catch_unwind(move || inflate(&d, 8));
        assert!(r.is_ok(), "overlapping backref must not panic");
        assert_eq!(r.unwrap().unwrap(), vec![b'a'; 8],
            "overlapping backref must decode to 8 'a' bytes");
    }
}
