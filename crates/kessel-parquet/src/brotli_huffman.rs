//! Brotli prefix-code (Huffman) decoder — RFC 7932 §3.4 + §3.5.
//!
//! Brotli's prefix codes come in two shapes:
//!   - **Simple prefix code** (§3.4): NSYM ≤ 4 symbols, special-cased
//!     bit patterns. 4 sub-shapes by NSYM value.
//!   - **Complex prefix code** (§3.5): canonical Huffman code described
//!     by its own RLE-coded code-length sequence (which is itself
//!     decoded using a small prefix code). This is the workhorse.
//!
//! ## SP154 Layer 5 scope
//!
//! This commit ships the **simple prefix code** path (RFC 7932 §3.4)
//! end-to-end:
//!   - 2-bit "code type" header (= 0b01 for simple per §3.4 ordering).
//!     Actually per RFC §3.4 the first 2 bits read are NOT the code-type
//!     selector; instead the prefix-code reader reads the first 2 bits
//!     and if value == 1 → simple, else complex (4 sub-types by HSKIP).
//!   - 2 bits NSYM-1 (1..=4 symbols)
//!   - NSYM symbols each read in ALPHABET_BITS bits
//!   - 1 tree-select bit (only when NSYM=4)
//!   - Symbol code lengths per the §3.4 table:
//!       NSYM=1: length 0 for the single symbol (zero-bit code)
//!       NSYM=2: lengths 1, 1
//!       NSYM=3: lengths 1, 2, 2
//!       NSYM=4 tree-select=0: lengths 2, 2, 2, 2
//!       NSYM=4 tree-select=1: lengths 1, 2, 3, 3
//!
//! The **complex prefix code** path (§3.5) is DEFERRED to a future
//! SP154 slice — it requires:
//!   - HSKIP (4 bits) to select the leading code-length code
//!   - 18-entry code-length code table decoded inline (recursive
//!     prefix-code structure)
//!   - RLE-coded run of code lengths for the actual alphabet
//!   - Canonical-prefix-code reconstruction (sorted-symbol-by-length
//!     ordering with `nextcode[len]` walk)
//!
//! When the metablock header path eventually exercises complex codes
//! (Layer 6+), the decoder will surface
//! `BrotliError::ComplexPrefixCodeNotYetSupported`. For Layer 5 in
//! isolation, the simple-code path is testable directly via this
//! module's KAT suite.
//!
//! ## Canonical prefix-code reconstruction (§3.3)
//!
//! Given (symbol, code-length) pairs, the canonical prefix code is
//! built per RFC 7932 §3.3:
//!   1. Sort by (code-length, symbol).
//!   2. Assign codes via the `nextcode[len]` table:
//!        code = nextcode[len]; nextcode[len] += 1; left-shift to
//!        next length appropriately.
//!
//! Decoding then walks the tree bit-by-bit OR uses a flat lookup
//! table indexed by `read_bits(max_length)`. For Layer 5 we use the
//! straightforward tree-walk approach since simple codes have
//! max_length ≤ 3.
//!
//! ## Safety
//!
//! `#![forbid(unsafe_code)]`; bounds-checked arithmetic; typed errors
//! for every failure mode (no panics).

#![allow(dead_code)]

use crate::brotli_bit_reader::{BitReader, BitReaderError};

/// Typed errors for the Huffman / prefix-code decoder.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HuffmanError {
    /// Bit reader surfaced an error.
    BitReader(BitReaderError),
    /// Complex prefix code path is deferred to a future SP154 slice.
    /// Carries the HSKIP value for diagnostics.
    ComplexPrefixCodeNotYetSupported { hskip: u8 },
    /// Simple prefix code had duplicate symbols (per RFC §3.4 sym2 != sym1, etc).
    DuplicateSymbol { sym: u32 },
    /// Symbol value exceeded the declared alphabet size.
    SymbolOutOfRange { sym: u32, alphabet_size: u32 },
    /// Built code is not a valid canonical prefix code (inconsistent
    /// code-length sequence). Currently unreachable for the simple
    /// path (the §3.4 length tables are always valid), but kept as a
    /// non_exhaustive variant for the complex-code follow-up.
    NotCanonical,
    /// While decoding a symbol, walked past the table without finding
    /// a leaf. Indicates corrupt bit stream.
    InvalidCode,
}

impl core::fmt::Display for HuffmanError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for HuffmanError {}

impl From<BitReaderError> for HuffmanError {
    fn from(e: BitReaderError) -> Self {
        HuffmanError::BitReader(e)
    }
}

/// A canonical prefix code (decoded form). For Layer 5 we represent
/// it as a flat (symbol, code, code_length) table; the decoder walks
/// it linearly. For Layer 6+ a faster lookup-table form will be
/// added. Max 256 entries is plenty for V1.
#[derive(Debug, Clone)]
pub(crate) struct PrefixCode {
    /// Each entry: (symbol, code, code_length_bits). Sorted by
    /// (code_length, symbol) ascending.
    entries: Vec<(u32, u32, u8)>,
}

impl PrefixCode {
    /// Build a canonical prefix code from `(symbol, code_length)` pairs
    /// per RFC 7932 §3.3. Pairs with code_length=0 are SKIPPED (= symbol
    /// is not present in the code). Returns the entry table sorted by
    /// (length, symbol). For a code with a single entry of length 0,
    /// returns a degenerate code (the §3.4 NSYM=1 zero-bit case).
    pub(crate) fn from_symbol_lengths(
        pairs: &[(u32, u8)],
    ) -> Result<PrefixCode, HuffmanError> {
        // Filter out zero-length entries and stable-sort by (length, symbol).
        let mut entries: Vec<(u32, u8)> = pairs
            .iter()
            .copied()
            .filter(|&(_, l)| l > 0)
            .collect();
        entries.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        // Special-case the zero-bit single-symbol code (§3.4 NSYM=1).
        if entries.is_empty() {
            // All-zero lengths input — must be the NSYM=1 case. Find
            // the unique symbol with length 0 and return a degenerate
            // entry.
            let zero_len: Vec<u32> = pairs
                .iter()
                .filter(|&&(_, l)| l == 0)
                .map(|&(s, _)| s)
                .collect();
            if zero_len.len() == 1 {
                return Ok(PrefixCode {
                    entries: vec![(zero_len[0], 0, 0)],
                });
            }
            return Err(HuffmanError::NotCanonical);
        }
        // Assign canonical codes per §3.3.
        let max_len = entries.last().unwrap().1;
        let mut bl_count: Vec<u32> = vec![0; (max_len as usize) + 1];
        for &(_, l) in &entries {
            bl_count[l as usize] = bl_count[l as usize].saturating_add(1);
        }
        // next_code[len] = first code of that length per §3.3 algorithm.
        let mut next_code: Vec<u32> = vec![0; (max_len as usize) + 2];
        let mut code: u32 = 0;
        for bits in 1..=(max_len as usize) {
            code = (code + bl_count[bits - 1])
                .checked_shl(1)
                .ok_or(HuffmanError::NotCanonical)?;
            next_code[bits] = code;
        }
        let mut out = Vec::with_capacity(entries.len());
        for &(sym, len) in &entries {
            let c = next_code[len as usize];
            next_code[len as usize] = next_code[len as usize].saturating_add(1);
            out.push((sym, c, len));
        }
        Ok(PrefixCode { entries: out })
    }

    /// Decode the next symbol from the bit stream. For the zero-bit
    /// degenerate code (§3.4 NSYM=1), no bits are consumed.
    pub(crate) fn decode_symbol(&self, r: &mut BitReader) -> Result<u32, HuffmanError> {
        if self.entries.len() == 1 && self.entries[0].2 == 0 {
            // Zero-bit code: return the single symbol without consuming bits.
            return Ok(self.entries[0].0);
        }
        // Brotli codes are read MSB-first per RFC 7932 §3.3 (the canonical
        // codes are constructed left-to-right; bits are read with the
        // FIRST bit being the most significant). Within each byte the
        // bit-reader is LSB-first; we accumulate bit-by-bit into a code,
        // shifting left to add new bits. Walk the entries to find the
        // matching (code, len).
        //
        // For Layer 5's simple codes (max_len ≤ 3), the linear walk
        // per symbol bit is fine. A flat lookup-table form is the
        // optimization for Layer 6+.
        let mut code: u32 = 0;
        let mut len: u8 = 0;
        let max_len = self.entries.iter().map(|e| e.2).max().unwrap_or(0);
        while len < max_len {
            let b = r.read_one_bit()? as u32;
            code = (code << 1) | b;
            len += 1;
            // Try to match against entries of the current length.
            for &(sym, c, l) in &self.entries {
                if l == len && c == code {
                    return Ok(sym);
                }
            }
        }
        Err(HuffmanError::InvalidCode)
    }

    /// Number of distinct symbols in this code.
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Decode a SIMPLE prefix code per RFC 7932 §3.4.
///
/// On entry: bit reader is positioned just AFTER the 2-bit "code type
/// indicator" (= 01 LSB-first; if it's 00/10/11 the code is complex).
///
/// Reads:
///   - 2 bits NSYM-1 (so NSYM = 1..=4)
///   - NSYM × `alphabet_bits` bits for the symbols
///   - 1 bit tree-select (only if NSYM=4)
///
/// Returns the constructed `PrefixCode`.
///
/// Per RFC 7932 §3.4, symbols of equal code length must be assigned
/// canonical codes in SORTED SYMBOL ORDER, not order-of-appearance.
/// `from_symbol_lengths` handles this via its (length, symbol) sort.
pub(crate) fn decode_simple_prefix_code(
    r: &mut BitReader,
    alphabet_bits: u8,
    alphabet_size: u32,
) -> Result<PrefixCode, HuffmanError> {
    let nsym = (r.read_bits(2)? + 1) as u8; // 1..=4
    let mut syms: Vec<u32> = Vec::with_capacity(nsym as usize);
    for _ in 0..nsym {
        let s = r.read_bits(alphabet_bits)?;
        if s >= alphabet_size {
            return Err(HuffmanError::SymbolOutOfRange {
                sym: s,
                alphabet_size,
            });
        }
        if syms.contains(&s) {
            return Err(HuffmanError::DuplicateSymbol { sym: s });
        }
        syms.push(s);
    }
    let lengths: Vec<u8> = match nsym {
        1 => vec![0],
        2 => vec![1, 1],
        3 => vec![1, 2, 2],
        4 => {
            let tree_select = r.read_one_bit()?;
            if tree_select == 0 {
                vec![2, 2, 2, 2]
            } else {
                vec![1, 2, 3, 3]
            }
        }
        _ => unreachable!("nsym is 1..=4"),
    };
    let pairs: Vec<(u32, u8)> = syms.into_iter().zip(lengths.into_iter()).collect();
    PrefixCode::from_symbol_lengths(&pairs)
}

/// Entry point: decode either a simple or complex prefix code from
/// the bit stream per RFC 7932 §3.3-§3.5.
///
/// First reads 2 bits: if value == 1 → simple code (§3.4); else →
/// complex code (§3.5) with HSKIP = (value).
///
/// For Layer 5, the complex-code path returns
/// `ComplexPrefixCodeNotYetSupported`.
pub(crate) fn decode_prefix_code(
    r: &mut BitReader,
    alphabet_bits: u8,
    alphabet_size: u32,
) -> Result<PrefixCode, HuffmanError> {
    let type_code = r.read_bits(2)? as u8;
    if type_code == 1 {
        decode_simple_prefix_code(r, alphabet_bits, alphabet_size)
    } else {
        // type_code in {0, 2, 3} → complex prefix code with HSKIP = type_code.
        // Per RFC §3.5: HSKIP determines the leading skipped code-length
        // codes (0=skip 0; 2=skip 2; 3=skip 3). Decoding the actual
        // 18-entry code-length code + the RLE'd alphabet code lengths
        // is the Layer 5-followup (substantial: ~1-2 sessions of work).
        Err(HuffmanError::ComplexPrefixCodeNotYetSupported { hskip: type_code })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// §3.4 NSYM=1 single-symbol zero-bit code: a stream encoding
    /// "this prefix code has exactly one symbol = 5" with alphabet_bits=4.
    /// Stream after the leading 2-bit type indicator (already consumed):
    ///   NSYM-1 = 0 (2 bits): bit0=0 bit1=0
    ///   symbol = 5 (4 bits LSB-first): bit2=1 bit3=0 bit4=1 bit5=0
    /// → byte 0 = bit 2 + bit 4 = 0b0001_0100 = 0x14
    /// After decoding, calling decode_symbol consumes NO further bits
    /// and returns 5.
    #[test]
    fn simple_prefix_code_nsym1_returns_zero_bit_symbol() {
        let bytes = [0x14u8];
        let mut r = BitReader::new(&bytes);
        let code = decode_simple_prefix_code(&mut r, 4, 16).unwrap();
        assert_eq!(code.len(), 1);
        // Decoding doesn't consume any further bits.
        let pos_before = r.bit_pos();
        let s = code.decode_symbol(&mut r).unwrap();
        assert_eq!(s, 5);
        assert_eq!(r.bit_pos(), pos_before, "NSYM=1 must not read bits");
    }

    /// §3.4 NSYM=2: lengths 1,1. Two symbols, each 1 bit. Canonical
    /// assignment: sorted-by-symbol gets code 0; other gets code 1.
    /// Stream:
    ///   NSYM-1 = 1 (2 bits): bit0=1 bit1=0
    ///   sym1 = 7 (4 bits LSB-first): bit2=1 bit3=1 bit4=1 bit5=0
    ///   sym2 = 3 (4 bits LSB-first): bit6=1 bit7=1 bit8=0 bit9=0
    ///   then to test decode: 1 bit input '0' → sorted-first sym = 3
    ///                      followed by 1 bit '1' → sym 7
    /// Bit 10 = 0, Bit 11 = 1. Bits 12+ irrelevant.
    /// Byte 0: bits 0, 2, 3, 4, 6, 7 set = 0b1101_1101 = 0xDD.
    ///   Detailed: bit 0=1, bit 1=0, bit 2=1, bit 3=1, bit 4=1, bit 5=0,
    ///             bit 6=1, bit 7=1.
    /// Byte 1: bit 8=0, bit 9=0, bit 10=0, bit 11=1, bits 12..15 irrelevant=0.
    ///   → byte 1 = bit 11 = 0b0000_1000 = 0x08.
    #[test]
    fn simple_prefix_code_nsym2_two_symbols_one_bit_each() {
        let bytes = [0xDDu8, 0x08];
        let mut r = BitReader::new(&bytes);
        let code = decode_simple_prefix_code(&mut r, 4, 16).unwrap();
        assert_eq!(code.len(), 2);
        // Sorted symbols: 3 then 7. Code 0 → 3; code 1 → 7.
        let s_first = code.decode_symbol(&mut r).unwrap();
        let s_second = code.decode_symbol(&mut r).unwrap();
        assert_eq!(s_first, 3, "bit 0 should decode to sorted-first sym=3");
        assert_eq!(s_second, 7, "bit 1 should decode to sorted-second sym=7");
    }

    /// §3.4 NSYM=3: lengths 1,2,2 IN ORDER OF APPEARANCE. The first
    /// symbol declared gets length 1; the next two get length 2.
    /// Per §3.3 canonical assignment: among same-length symbols, codes
    /// are assigned in sorted-symbol order.
    ///
    /// Stream (alphabet_bits=3 → 8-symbol alphabet):
    ///   NSYM-1 = 2 (2 bits): bit0=0 bit1=1
    ///   sym1 = 5 (3 bits LSB-first): bit2=1 bit3=0 bit4=1   ← length 1
    ///   sym2 = 2 (3 bits LSB-first): bit5=0 bit6=1 bit7=0   ← length 2
    ///   sym3 = 6 (3 bits LSB-first): bit8=0 bit9=1 bit10=1  ← length 2
    ///
    /// Canonical code:
    ///   sym 5 (len 1) → code 0 → MSB '0'
    ///   sym 2 (len 2, sorted first among len-2) → code 2 → MSB '10'
    ///   sym 6 (len 2, sorted second) → code 3 → MSB '11'
    ///
    /// Decode test bits at positions 11..16:
    ///   bit 11 = 0  → decode → sym 5  (code '0')
    ///   bits 12,13 = 1,0 → decode → sym 2 (code '10' MSB-first)
    ///   bits 14,15 = 1,1 → decode → sym 6 (code '11')
    ///
    /// Byte 0 bits 0..7:
    ///   bit0=0 bit1=1 bit2=1 bit3=0 bit4=1 bit5=0 bit6=1 bit7=0
    ///   = 0b0101_0110 = 0x56
    /// Byte 1 bits 8..15:
    ///   bit8=0 bit9=1 bit10=1 bit11=0 bit12=1 bit13=0 bit14=1 bit15=1
    ///   = 0b1101_0110 = 0xD6
    #[test]
    fn simple_prefix_code_nsym3_lengths_1_2_2() {
        let bytes = [0x56u8, 0xD6];
        let mut r = BitReader::new(&bytes);
        let code = decode_simple_prefix_code(&mut r, 3, 8).unwrap();
        assert_eq!(code.len(), 3);
        let s1 = code.decode_symbol(&mut r).unwrap();
        let s2 = code.decode_symbol(&mut r).unwrap();
        let s3 = code.decode_symbol(&mut r).unwrap();
        assert_eq!(s1, 5, "code '0' decodes to length-1 sym 5 (declared first)");
        assert_eq!(s2, 2, "code '10' decodes to sym 2 (sorted first among len-2)");
        assert_eq!(s3, 6, "code '11' decodes to sym 6 (sorted second among len-2)");
    }

    /// §3.4 NSYM=4 tree-select=0: all 4 symbols length 2. Sorted symbols
    /// get canonical codes 00, 01, 10, 11 (MSB-first).
    ///
    /// Stream (alphabet_bits=2 → 4-symbol alphabet):
    ///   NSYM-1 = 3 (2 bits): bit0=1 bit1=1
    ///   sym1=3 (2 bits): bit2=1 bit3=1
    ///   sym2=0 (2 bits): bit4=0 bit5=0
    ///   sym3=2 (2 bits): bit6=0 bit7=1
    ///   sym4=1 (2 bits): bit8=1 bit9=0
    ///   tree-select = 0 (1 bit): bit10=0
    ///   then 4 symbols of 2 bits each: '00' → sym 0; '01' → sym 1;
    ///                                  '10' → sym 2; '11' → sym 3.
    /// Decode bits 11..18: 0,0, 0,1, 1,0, 1,1
    ///
    /// Byte 0 bits 0..7: 1,1,1,1,0,0,0,1 = 0b1000_1111 = 0x8F
    /// Byte 1 bits 8..15: 1,0,0,0,0,0,1,1
    ///   bit 11 = 0, bit 12 = 0, bit 13 = 0, bit 14 = 0, bit 15 = 0?
    ///   wait let me re-derive bits 8..15.
    ///   bit 8 = 1 (low bit of sym4=1)
    ///   bit 9 = 0
    ///   bit 10 = 0 (tree-select)
    ///   bit 11 = 0 (decode bit 1)
    ///   bit 12 = 0 (decode bit 2)
    ///   bit 13 = 0 (decode bit 3 — first bit of '01' MSB-first is '0')
    ///   bit 14 = 1 (decode bit 4 — second bit of '01' is '1')
    ///
    ///   Wait — when decode_symbol calls read_one_bit, the FIRST bit
    ///   read is the MSB of the canonical code. The bit reader is
    ///   LSB-first inside bytes, so we feed it bits in the same order
    ///   they will be consumed. For decoding 4 symbols (0,1,2,3) at
    ///   2 bits each, the bit-sequence I want is:
    ///     decode 0: read bits '0','0' → code 0.  bits 11=0, 12=0.
    ///     decode 1: read bits '0','1' → code 1.  bits 13=0, 14=1.
    ///     decode 2: read bits '1','0' → code 2.  bits 15=1, 16=0.
    ///     decode 3: read bits '1','1' → code 3.  bits 17=1, 18=1.
    ///
    /// So bits 11..18 LSB-first within byte 1 (bit 11=bit3-of-byte1):
    ///   bit 8=1 bit 9=0 bit 10=0 bit 11=0 bit 12=0 bit 13=0 bit 14=1 bit 15=1
    ///   = 0b1100_0001 = 0xC1
    /// Byte 2 bits 16..18: bit 16=0 bit 17=1 bit 18=1
    ///   = 0b0000_0110 = 0x06
    #[test]
    fn simple_prefix_code_nsym4_treeselect0_all_length_2() {
        let bytes = [0x8Fu8, 0xC1, 0x06];
        let mut r = BitReader::new(&bytes);
        let code = decode_simple_prefix_code(&mut r, 2, 4).unwrap();
        assert_eq!(code.len(), 4);
        let mut got = vec![];
        for _ in 0..4 {
            got.push(code.decode_symbol(&mut r).unwrap());
        }
        assert_eq!(got, vec![0, 1, 2, 3]);
    }

    /// §3.4 NSYM=4 tree-select=1: lengths 1,2,3,3.
    /// Stream:
    ///   NSYM-1 = 3 (2 bits): bit0=1 bit1=1
    ///   sym1=0 (2 bits): bit2=0 bit3=0
    ///   sym2=1 (2 bits): bit4=1 bit5=0
    ///   sym3=2 (2 bits): bit6=0 bit7=1
    ///   sym4=3 (2 bits): bit8=1 bit9=1
    ///   tree-select=1 (1 bit): bit10=1
    /// Sorted symbols are already [0,1,2,3]. Lengths 1,2,3,3 →
    /// canonical codes:
    ///   sym 0 len 1  → code '0'
    ///   sym 1 len 2  → code '10'
    ///   sym 2 len 3  → code '110'
    ///   sym 3 len 3  → code '111'
    ///
    /// Decode test: bit '0' → sym 0; bits '1','0' → sym 1;
    ///              bits '1','1','0' → sym 2; bits '1','1','1' → sym 3.
    ///
    /// Byte 0: bit0=1 bit1=1 bit2=0 bit3=0 bit4=1 bit5=0 bit6=0 bit7=1
    ///   = 0b1001_0011 = 0x93
    /// Byte 1: bit8=1 bit9=1 bit10=1 bit11=0 bit12=1 bit13=0 bit14=1 bit15=1
    ///   bits 11..15 are decode bits:
    ///     bit 11 = 0 (sym 0 = '0')
    ///     bit 12 = 1 (sym 1 first bit)
    ///     bit 13 = 0 (sym 1 second bit)
    ///     bit 14 = 1 (sym 2 first bit)
    ///     bit 15 = 1 (sym 2 second bit)
    ///   = bit8=1 bit9=1 bit10=1 bit11=0 bit12=1 bit13=0 bit14=1 bit15=1
    ///   = 0b1101_0111 = 0xD7
    /// Byte 2 (decode bits continued): bit 16 = 0 (sym 2 third bit),
    ///   bit 17 = 1 (sym 3 first), bit 18 = 1 (sym 3 second),
    ///   bit 19 = 1 (sym 3 third).
    ///   = bit16=0 bit17=1 bit18=1 bit19=1 = 0b0000_1110 = 0x0E
    #[test]
    fn simple_prefix_code_nsym4_treeselect1_lengths_1_2_3_3() {
        let bytes = [0x93u8, 0xD7, 0x0E];
        let mut r = BitReader::new(&bytes);
        let code = decode_simple_prefix_code(&mut r, 2, 4).unwrap();
        assert_eq!(code.len(), 4);
        let mut got = vec![];
        for _ in 0..4 {
            got.push(code.decode_symbol(&mut r).unwrap());
        }
        assert_eq!(got, vec![0, 1, 2, 3]);
    }

    /// Complex prefix code (any of type 0, 2, 3) currently rejected
    /// with a typed error. Pins the boundary that the Layer 6+ work
    /// will need to close.
    #[test]
    fn complex_prefix_code_surfaces_typed_unsupported() {
        // 2-bit type=0 → complex code with HSKIP=0.
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let err = decode_prefix_code(&mut r, 4, 16).unwrap_err();
        assert!(
            matches!(err, HuffmanError::ComplexPrefixCodeNotYetSupported { hskip: 0 }),
            "expected ComplexPrefixCodeNotYetSupported, got {err:?}"
        );

        // 2-bit type=2 → HSKIP=2.
        let bytes = [0x02u8];
        let mut r = BitReader::new(&bytes);
        let err = decode_prefix_code(&mut r, 4, 16).unwrap_err();
        assert!(
            matches!(err, HuffmanError::ComplexPrefixCodeNotYetSupported { hskip: 2 }),
            "expected hskip=2, got {err:?}"
        );

        // 2-bit type=3 → HSKIP=3.
        let bytes = [0x03u8];
        let mut r = BitReader::new(&bytes);
        let err = decode_prefix_code(&mut r, 4, 16).unwrap_err();
        assert!(
            matches!(err, HuffmanError::ComplexPrefixCodeNotYetSupported { hskip: 3 }),
            "expected hskip=3, got {err:?}"
        );
    }

    /// decode_prefix_code dispatches to simple-code path on type=1.
    #[test]
    fn decode_prefix_code_dispatches_to_simple_on_type_1() {
        // 2-bit type=1 → simple.
        //   bit 0 = 1 bit 1 = 0 → value 1 LSB-first.
        //   then NSYM-1=0 (bits 2,3 = 0,0).
        //   then 4-bit symbol = 9: bit 4 = 1 bit 5 = 0 bit 6 = 0 bit 7 = 1.
        // Byte 0: bits 0, 4, 7 set = 0b1001_0001 = 0x91
        let bytes = [0x91u8];
        let mut r = BitReader::new(&bytes);
        let code = decode_prefix_code(&mut r, 4, 16).unwrap();
        assert_eq!(code.len(), 1);
        let s = code.decode_symbol(&mut r).unwrap();
        assert_eq!(s, 9);
    }

    /// Pentest: duplicate symbols in a simple code → typed error.
    ///   NSYM-1 = 1 (bit 0=1 bit 1=0)
    ///   sym 1 = 5 (4 bits LSB-first: 1,0,1,0)
    ///   sym 2 = 5 (4 bits LSB-first: 1,0,1,0)  ← duplicate
    /// Byte 0: bits 0, 2, 4, 6 set = 0b0101_0101 = 0x55
    /// Byte 1: bits 0, 2 set = 0b0000_0101 = 0x05
    #[test]
    fn pentest_simple_prefix_code_duplicate_symbol_rejected() {
        let bytes = [0x55u8, 0x05];
        let mut r = BitReader::new(&bytes);
        let err = decode_simple_prefix_code(&mut r, 4, 16).unwrap_err();
        assert!(
            matches!(err, HuffmanError::DuplicateSymbol { sym: 5 }),
            "expected DuplicateSymbol(5), got {err:?}"
        );
    }

    /// Pentest: symbol out of alphabet range → typed error.
    ///   alphabet_size = 8, alphabet_bits = 4 (can encode up to 15).
    ///   NSYM-1 = 0 (bits 0,1 = 0).
    ///   symbol = 12 (4 bits LSB-first: 0,0,1,1) → bit2=0 bit3=0 bit4=1 bit5=1.
    /// Byte 0: bits 4,5 set = 0b0011_0000 = 0x30
    #[test]
    fn pentest_simple_prefix_code_symbol_out_of_range_rejected() {
        let bytes = [0x30u8];
        let mut r = BitReader::new(&bytes);
        let err = decode_simple_prefix_code(&mut r, 4, 8).unwrap_err();
        assert!(
            matches!(err, HuffmanError::SymbolOutOfRange { sym: 12, alphabet_size: 8 }),
            "expected SymbolOutOfRange{{sym=12,alpha=8}}, got {err:?}"
        );
    }

    /// Canonical code construction: a deliberately-constructed
    /// length sequence pins the (length, symbol) sort. Lengths
    /// {sym 7: 1, sym 3: 2, sym 5: 2}: sorted gives sym 7 → code 0,
    /// then by symbol sym 3 (= 0b10), sym 5 (= 0b11).
    #[test]
    fn canonical_prefix_code_sorts_by_length_then_symbol() {
        let pairs = vec![(7u32, 1u8), (5u32, 2u8), (3u32, 2u8)];
        let code = PrefixCode::from_symbol_lengths(&pairs).unwrap();
        assert_eq!(code.len(), 3);
        // After sort: (7,_,1), (3,_,2), (5,_,2).
        // canonical codes: 7 → 0, 3 → 10 (=2), 5 → 11 (=3).
        let bytes = [0u8, 0u8]; // 16 bits all zero — will decode 7 first.
        let mut r = BitReader::new(&bytes);
        let s = code.decode_symbol(&mut r).unwrap();
        assert_eq!(s, 7, "first bit '0' must decode to length-1 sym 7");
    }
}
