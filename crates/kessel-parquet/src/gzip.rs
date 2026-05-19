//! Pure RFC 1952 (gzip member) + RFC 1951 (DEFLATE inflate)
//! decompressor for Parquet GZIP pages. Zero deps, iterative
//! (no recursion), bounds-checked, 64 MiB hard cap, CRC32-verified.
//! Never panics / OOM-aborts / stack-overflows.
#![allow(dead_code)]

use crate::PqError;

fn bad(s: &str) -> PqError {
    PqError::Bad(s.to_string())
}

/// Hard cap on a single decompressed page. Mirrors
/// snappy::SNAPPY_MAX_DECOMP (same value & rationale; separate const
/// so gzip.rs stays self-contained — the sibling-module convention).
pub(crate) const GZIP_MAX_DECOMP: usize = 64 << 20; // 64 MiB

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
