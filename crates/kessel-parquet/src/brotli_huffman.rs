//! Brotli prefix-code (Huffman) decoder — RFC 7932 §3.4 + §3.5.
//!
//! Brotli's prefix codes come in two shapes:
//!   - **Simple prefix code** (§3.4): NSYM ≤ 4 symbols, special-cased
//!     bit patterns. 4 sub-shapes by NSYM value.
//!   - **Complex prefix code** (§3.5): canonical Huffman code described
//!     by its own RLE-coded code-length sequence (which is itself
//!     decoded using a small prefix code). This is the workhorse.
//!
//! ## SP154 Layer 5 + 5b scope
//!
//! Layer 5 (commit `4753fad`) shipped the **simple prefix code** path
//! (RFC 7932 §3.4) end-to-end:
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
//! Layer 5b (THIS commit) ships the **complex prefix code** path
//! (RFC 7932 §3.5):
//!   - 2-bit HSKIP (number of leading code-length-code symbols to skip)
//!   - 18 code-length-code lengths (in the fixed order
//!     1,2,3,4,0,5,17,6,16,7,8,9,10,11,12,13,14,15), each encoded with
//!     the fixed 6-symbol code from §3.5
//!   - RLE-decoded run of `alphabet_size` code lengths for the main
//!     alphabet (symbols 16/17 = repeat-previous / repeat-zero with
//!     extra-bit-extended counts)
//!   - Canonical-prefix-code reconstruction (§3.3) over the resulting
//!     `(symbol, length)` pairs.
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
    /// (Retained for historical L5 boundary; L5b makes this unreachable
    /// from `decode_prefix_code` but kept as a typed variant for callers
    /// that still surface complex-code errors during V1 testing.)
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
    /// A repeat code (16/17) in a complex prefix code's length sequence
    /// asked for more lengths than the declared alphabet size. Per
    /// RFC 7932 §3.5: "If the number of times to repeat the previous
    /// length or repeat a zero length would result in more lengths in
    /// total than the number of symbols in the alphabet, then the stream
    /// should be rejected as invalid."
    RepeatOverrunsAlphabet { needed: u32, alphabet_size: u32 },
    /// A complex prefix code's code-length sequence finished before
    /// reaching the declared alphabet size (per RFC 7932 §3.5 the sum
    /// `(32768 >> code_length)` over non-zero code lengths must equal
    /// 32768; a sequence that finishes early indicates a corrupt stream).
    /// Currently surfaced when fewer than `alphabet_size` lengths are
    /// produced AND the Kraft sum is non-32768.
    UnderfilledAlphabet { produced: u32, alphabet_size: u32 },
    /// Complex prefix code: the inner 18-symbol code-length code had
    /// fewer than 2 non-zero entries AND was not the single-symbol
    /// "all symbols have length 0 except one with length N" case
    /// (per RFC 7932 §3.5: "A complex prefix code must have at least
    /// two non-zero code lengths."). This error variant is reserved
    /// for the OUTER alphabet code-length sequence — the inner 18-entry
    /// code is allowed to be single-symbol.
    InsufficientNonzeroLengths,
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

/// Fixed RFC 7932 §3.5 "code length code" — decode one of the 6
/// possible code-length values (0..=5) from the bit stream. The listed
/// RFC code words are parsed right-to-left, which equates to LSB-first
/// stream reading. The decode tree (in stream-bit-by-bit order):
///
/// ```text
///   bit1=0:
///     bit2=0           → sym 0 (length value 0)
///     bit2=1           → sym 3 (length value 3)
///   bit1=1:
///     bit2=0           → sym 4 (length value 4)
///     bit2=1:
///       bit3=0         → sym 2 (length value 2)
///       bit3=1:
///         bit4=0       → sym 1 (length value 1)
///         bit4=1       → sym 5 (length value 5)
/// ```
///
/// Returns the decoded code-length value (0..=5).
fn read_code_length_code(r: &mut BitReader) -> Result<u8, HuffmanError> {
    let b1 = r.read_one_bit()?;
    let b2 = r.read_one_bit()?;
    if b1 == 0 {
        if b2 == 0 {
            Ok(0)
        } else {
            Ok(3)
        }
    } else if b2 == 0 {
        Ok(4)
    } else {
        let b3 = r.read_one_bit()?;
        if b3 == 0 {
            Ok(2)
        } else {
            let b4 = r.read_one_bit()?;
            if b4 == 0 {
                Ok(1)
            } else {
                Ok(5)
            }
        }
    }
}

/// RFC 7932 §3.5: the 18-symbol code-length alphabet read order.
/// The first HSKIP entries are skipped (treated as zero); the remaining
/// entries are read sequentially using the §3.5 fixed 6-symbol code.
pub(crate) const CODE_LENGTH_ORDER: [u8; 18] = [
    1, 2, 3, 4, 0, 5, 17, 6, 16, 7, 8, 9, 10, 11, 12, 13, 14, 15,
];

/// Decode a COMPLEX prefix code per RFC 7932 §3.5.
///
/// On entry: bit reader is positioned just AFTER the 2-bit "type code"
/// dispatch byte (= HSKIP value: 0, 2, or 3). `hskip` is the value the
/// dispatcher already read.
///
/// Reads:
///   - 18 code-length-code lengths (in the fixed §3.5 order; first HSKIP
///     are skipped). Each length is 2..=4 bits via the fixed §3.5 code.
///     Trailing zeros may be omitted if Kraft sum reaches 32 early.
///   - The resulting 18-symbol code is canonicalized via §3.3.
///   - Using the 18-symbol code, decode `alphabet_size` main code lengths
///     with RLE semantics:
///       symbols 0..=15 → direct length (0 = symbol not in code)
///       symbol 16     → repeat previous-non-zero (or 8 if none yet)
///                       3..=6 times (2 extra bits)
///       symbol 17     → repeat zero 3..=10 times (3 extra bits)
///       Modification: consecutive 16s extend the same run as
///         `count = 4 * (count - 2) + extra_bits`. Same for 17 with `8 *`.
///   - The resulting main-alphabet `(symbol, length)` pairs are
///     canonicalized via §3.3.
///
/// Special "single non-zero length" case (per RFC §3.5): if exactly ONE
/// of the produced lengths is non-zero, the code is a zero-bit code
/// emitting that single symbol. We delegate this via
/// `PrefixCode::from_symbol_lengths`'s zero-bit handling.
pub(crate) fn decode_complex_prefix_code(
    r: &mut BitReader,
    hskip: u8,
    alphabet_size: u32,
) -> Result<PrefixCode, HuffmanError> {
    // Step 1: read up to (18 - hskip) code-length-code lengths in the
    // fixed order. Per RFC §3.5: "If there are at least two non-zero
    // code lengths, any trailing zero code lengths are omitted, i.e.,
    // the last code length in the sequence must be non-zero. In this
    // case, the sum of (32 >> code length) over all the non-zero code
    // lengths must equal to 32."
    //
    // We read lengths and track the partial Kraft sum (over `32 >> len`).
    // Stop when the sum reaches 32 (full code), or when we've exhausted
    // all 18 positions. If sum < 32 at end, the stream is invalid.
    let mut clc_lengths = [0u8; 18]; // indexed by symbol (= code-length value 0..=17)
    let mut nonzero_count = 0u32;
    let mut kraft_sum: u32 = 0;
    let mut idx = hskip as usize;
    while idx < 18 {
        let v = read_code_length_code(r)?; // 0..=5
        let sym = CODE_LENGTH_ORDER[idx] as usize;
        clc_lengths[sym] = v;
        if v > 0 {
            nonzero_count += 1;
            kraft_sum = kraft_sum
                .checked_add(32u32 >> v)
                .ok_or(HuffmanError::NotCanonical)?;
        }
        idx += 1;
        // Early termination: per RFC, once Kraft sum reaches 32 AND we
        // have at least 2 non-zero lengths (i.e. a valid complete code),
        // any trailing zeros are omitted. Single-non-zero is also valid
        // (the "all-same-length symbol" case); the RFC §3.5 text says
        // "if there was only one non-zero code length, then the prefix
        // code has one symbol whose code has zero length" — meaning the
        // *result* of the 18-entry code is degenerate. We still need to
        // read all 18 entries in that case (or do we?). Per RFC: "If
        // there are at least two non-zero code lengths, any trailing zero
        // code lengths are omitted". So for the single-non-zero case we
        // must read all 18 entries; for ≥2 we can terminate at sum=32.
        if nonzero_count >= 2 && kraft_sum >= 32 {
            break;
        }
    }
    if nonzero_count >= 2 && kraft_sum != 32 {
        // Sum exceeded 32 or never reached 32 — invalid.
        return Err(HuffmanError::NotCanonical);
    }
    // Step 2: build the 18-symbol code (the "code-length code") via §3.3
    // canonical reconstruction. Symbols are 0..=17 (the code-length
    // alphabet). Zero-length entries are filtered by from_symbol_lengths.
    let clc_pairs: Vec<(u32, u8)> = (0u32..18)
        .map(|s| (s, clc_lengths[s as usize]))
        .collect();
    let clc = PrefixCode::from_symbol_lengths(&clc_pairs)?;
    // Step 3: read `alphabet_size` main code lengths using the CLC.
    let mut main_lengths: Vec<u8> = Vec::with_capacity(alphabet_size as usize);
    // For RLE state per RFC §3.5:
    //   prev_nonzero_len = "the previous non-zero code length"; defaults to 8
    //     before any code length code lengths are read (per the
    //     "single-non-zero is 16 repeating 8" passage).
    //   prev_repeat = the most recent run-extension code (Some(16), Some(17),
    //     or None). Used to detect "consecutive 16s" / "consecutive 17s"
    //     for count modification.
    //   prev_repeat_count = the count of the most recent run if
    //     prev_repeat is set.
    let mut prev_nonzero_len: u8 = 8;
    let mut prev_repeat: Option<u8> = None; // 16 or 17
    let mut prev_repeat_count: u32 = 0;
    // Main-alphabet Kraft sum check: (32768 >> length) summed over
    // non-zero lengths must equal 32768 at end.
    let mut main_kraft: u32 = 0;
    while (main_lengths.len() as u32) < alphabet_size {
        let sym = clc.decode_symbol(r)?; // 0..=17
        if sym <= 15 {
            // Direct length.
            let l = sym as u8;
            main_lengths.push(l);
            if l > 0 {
                main_kraft = main_kraft
                    .checked_add(32768u32 >> l)
                    .ok_or(HuffmanError::NotCanonical)?;
                prev_nonzero_len = l;
            }
            prev_repeat = None;
            prev_repeat_count = 0;
        } else if sym == 16 {
            // Repeat previous non-zero length 3..=6 times.
            let extra = r.read_bits(2)?;
            let new_count: u32 = if prev_repeat == Some(16) {
                // Modify previous repeat count.
                4u32.checked_mul(prev_repeat_count.checked_sub(2).unwrap_or(0))
                    .ok_or(HuffmanError::NotCanonical)?
                    .checked_add(3 + extra)
                    .ok_or(HuffmanError::NotCanonical)?
            } else {
                3 + extra
            };
            // Number of *additional* lengths to emit.
            let delta: u32 = if prev_repeat == Some(16) {
                new_count - prev_repeat_count
            } else {
                new_count
            };
            // Check overrun BEFORE emitting.
            let after = (main_lengths.len() as u32)
                .checked_add(delta)
                .ok_or(HuffmanError::NotCanonical)?;
            if after > alphabet_size {
                return Err(HuffmanError::RepeatOverrunsAlphabet {
                    needed: after,
                    alphabet_size,
                });
            }
            for _ in 0..delta {
                main_lengths.push(prev_nonzero_len);
                main_kraft = main_kraft
                    .checked_add(32768u32 >> prev_nonzero_len)
                    .ok_or(HuffmanError::NotCanonical)?;
            }
            prev_repeat = Some(16);
            prev_repeat_count = new_count;
        } else if sym == 17 {
            // Repeat zero 3..=10 times.
            let extra = r.read_bits(3)?;
            let new_count: u32 = if prev_repeat == Some(17) {
                8u32.checked_mul(prev_repeat_count.checked_sub(2).unwrap_or(0))
                    .ok_or(HuffmanError::NotCanonical)?
                    .checked_add(3 + extra)
                    .ok_or(HuffmanError::NotCanonical)?
            } else {
                3 + extra
            };
            let delta: u32 = if prev_repeat == Some(17) {
                new_count - prev_repeat_count
            } else {
                new_count
            };
            let after = (main_lengths.len() as u32)
                .checked_add(delta)
                .ok_or(HuffmanError::NotCanonical)?;
            if after > alphabet_size {
                return Err(HuffmanError::RepeatOverrunsAlphabet {
                    needed: after,
                    alphabet_size,
                });
            }
            for _ in 0..delta {
                main_lengths.push(0);
            }
            prev_repeat = Some(17);
            prev_repeat_count = new_count;
        } else {
            // Unreachable: CLC produces symbols in 0..=17. The 18-symbol
            // alphabet is exhaustively covered above.
            return Err(HuffmanError::SymbolOutOfRange {
                sym,
                alphabet_size: 18,
            });
        }
    }
    debug_assert_eq!(main_lengths.len() as u32, alphabet_size);
    // Per RFC §3.5: the main-alphabet Kraft sum must equal 32768 unless
    // the code is the degenerate single-non-zero-length case (which is
    // also a valid Brotli code; from_symbol_lengths handles the zero-bit
    // degenerate where ALL lengths are zero but exactly one symbol is
    // "tagged" as the implicit one).
    //
    // Note: the RFC §3.5 single-non-zero case happens when EXACTLY one
    // of the main lengths is non-zero, regardless of its value. The
    // resulting code emits that symbol with no bits consumed. This is
    // analogous to the simple-code NSYM=1 zero-bit case.
    //
    // We surface non-32768 Kraft as NotCanonical UNLESS exactly one
    // length is non-zero (single-symbol case) OR all lengths are zero
    // (which we reject as InsufficientNonzeroLengths — a valid Brotli
    // stream can't have a zero-symbol alphabet).
    let nonzero_main: u32 = main_lengths.iter().filter(|&&l| l != 0).count() as u32;
    if nonzero_main == 0 {
        return Err(HuffmanError::InsufficientNonzeroLengths);
    }
    if nonzero_main >= 2 && main_kraft != 32768 {
        return Err(HuffmanError::NotCanonical);
    }
    // Build the canonical code. For the single-non-zero case, override
    // by handing a single (sym, length=0) zero-bit code pair.
    if nonzero_main == 1 {
        // Find the single non-zero symbol; return a zero-bit code on it.
        let sym = main_lengths
            .iter()
            .enumerate()
            .find(|(_, &l)| l != 0)
            .map(|(i, _)| i as u32)
            .unwrap();
        let pairs = vec![(sym, 0u8)];
        return PrefixCode::from_symbol_lengths(&pairs);
    }
    let pairs: Vec<(u32, u8)> = main_lengths
        .iter()
        .enumerate()
        .map(|(i, &l)| (i as u32, l))
        .collect();
    PrefixCode::from_symbol_lengths(&pairs)
}

/// Entry point: decode either a simple or complex prefix code from
/// the bit stream per RFC 7932 §3.3-§3.5.
///
/// First reads 2 bits: if value == 1 → simple code (§3.4); else →
/// complex code (§3.5) with HSKIP = (value).
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
        // codes (0=skip 0; 2=skip 2; 3=skip 3).
        decode_complex_prefix_code(r, type_code, alphabet_size)
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

    // SP154 L5b: the previous "complex code surfaces typed Unsupported"
    // test was deleted — L5b implements the complex-code path. The
    // typed `ComplexPrefixCodeNotYetSupported` variant is retained as
    // non_exhaustive for callers (e.g. compressed-metablock V1) that
    // may still want to bail early when not all subsidiary alphabets
    // are wired up. See the new `complex_prefix_code_*` KATs below.

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

    // -------------------------- L5b KATs --------------------------
    //
    // Each L5b KAT is HAND-DERIVED from RFC 7932 §3.5 — no encoder used.
    // The RFC §3.5 fixed 6-symbol "code length code" (parsed RIGHT-TO-LEFT
    // which equates to LSB-first stream order):
    //
    //   Symbol (= code-length value) | Code (stream bits, LSB-first)
    //   ---------------------------- | -----------------------------
    //   0                            | "00"   (2 bits: 0, 0)
    //   1                            | "1110" (4 bits: 1, 1, 1, 0)
    //   2                            | "110"  (3 bits: 1, 1, 0)
    //   3                            | "01"   (2 bits: 0, 1)
    //   4                            | "10"   (2 bits: 1, 0)
    //   5                            | "1111" (4 bits: 1, 1, 1, 1)
    //
    // Kraft check: 1/4 + 1/16 + 1/8 + 1/4 + 1/4 + 1/16 = 1.0 ✓.

    /// L5b KAT-1: read_code_length_code decodes all 6 symbols at the
    /// RFC §3.5 bit patterns. Per RFC §3.5: "the bits are parsed from
    /// right to left" — so the RFC's listed code "01" reads as STREAM
    /// bits (bit 0, bit 1) = (rightmost-char-of-listed, then-left, ...).
    /// E.g. "01" listed → char[1]='0', char[0]='1' → stream "1,0".
    ///
    /// Stream bit patterns per the RFC §3.5 table:
    ///   sym 0: listed "00"   → stream "0,0"       (2 bits)
    ///   sym 1: listed "0111" → stream "1,1,1,0"   (4 bits)
    ///   sym 2: listed "011"  → stream "1,1,0"     (3 bits)
    ///   sym 3: listed "10"   → stream "0,1"       (2 bits)
    ///   sym 4: listed "01"   → stream "1,0"       (2 bits)
    ///   sym 5: listed "1111" → stream "1,1,1,1"   (4 bits)
    ///
    /// Test pack order: sym 0, sym 3, sym 4, sym 2, sym 1, sym 5.
    ///   bit 0=0, 1=0  (sym 0)
    ///   bit 2=0, 3=1  (sym 3)
    ///   bit 4=1, 5=0  (sym 4)
    ///   bit 6=1, 7=1, 8=0  (sym 2)
    ///   bit 9=1, 10=1, 11=1, 12=0  (sym 1)
    ///   bit 13=1, 14=1, 15=1, 16=1  (sym 5)
    ///
    /// Byte 0 (bits 0..7): 0,0,0,1,1,0,1,1 → 8+16+64+128 = 216 = 0xD8
    /// Byte 1 (bits 8..15): 0,1,1,1,0,1,1,1 → 2+4+8+32+64+128 = 238 = 0xEE
    /// Byte 2 (bit 16): 1,0,0,0,0,0,0,0 → 1 = 0x01
    #[test]
    fn read_code_length_code_decodes_all_six_symbols() {
        let bytes = [0xD8u8, 0xEE, 0x01];
        let mut r = BitReader::new(&bytes);
        assert_eq!(read_code_length_code(&mut r).unwrap(), 0);
        assert_eq!(read_code_length_code(&mut r).unwrap(), 3);
        assert_eq!(read_code_length_code(&mut r).unwrap(), 4);
        assert_eq!(read_code_length_code(&mut r).unwrap(), 2);
        assert_eq!(read_code_length_code(&mut r).unwrap(), 1);
        assert_eq!(read_code_length_code(&mut r).unwrap(), 5);
    }

    /// L5b KAT-2: simplest 2-symbol complex prefix code, HSKIP=0.
    /// Main alphabet has 2 symbols, both with length 1.
    ///
    /// Hand-derivation:
    ///   - HSKIP=0 (caller already consumed the 2-bit dispatcher).
    ///   - CLC reads (in fixed §3.5 order):
    ///       pos 0 (CLC-sym 1, "main length 1"): length 1 (bits: 1,1,1,0)
    ///       pos 1 (CLC-sym 2): length 0 (bits: 0,0)
    ///       pos 2 (CLC-sym 3): length 0 (bits: 0,0)
    ///       pos 3 (CLC-sym 4): length 0 (bits: 0,0)
    ///       pos 4 (CLC-sym 0, "main length 0"): length 1 (bits: 1,1,1,0)
    ///       → at this point nonzero_count=2, Kraft sum = 16+16 = 32 → STOP
    ///   - CLC canonical: sym 0 (len 1, sorted-first) → code MSB "0",
    ///                    sym 1 (len 1, sorted-second) → code MSB "1".
    ///   - Decode 2 main lengths via CLC:
    ///       bit "1" → CLC-sym 1 → main len 1   (main sym 0 gets length 1)
    ///       bit "1" → CLC-sym 1 → main len 1   (main sym 1 gets length 1)
    ///   - Main canonical: sym 0 (len 1) → MSB "0"; sym 1 (len 1) → MSB "1".
    ///
    /// Stream bits 0..18 (LSB-first within bytes):
    ///   bit  0: CLC pos 0, bit a = 1
    ///   bit  1:           bit b = 1
    ///   bit  2:           bit c = 1
    ///   bit  3:           bit d = 0
    ///   bit  4: CLC pos 1, bit a = 0
    ///   bit  5:           bit b = 0
    ///   bit  6: CLC pos 2, bit a = 0
    ///   bit  7:           bit b = 0
    ///   bit  8: CLC pos 3, bit a = 0
    ///   bit  9:           bit b = 0
    ///   bit 10: CLC pos 4, bit a = 1
    ///   bit 11:           bit b = 1
    ///   bit 12:           bit c = 1
    ///   bit 13:           bit d = 0
    ///   bit 14: main-len read 1 = 1
    ///   bit 15: main-len read 2 = 1
    ///
    /// (Decoder doesn't actually use bits 16+ for code construction.)
    ///
    /// Byte 0 LSB-first bits 0..7: 1,1,1,0,0,0,0,0 → value = 1+2+4 = 7 → 0x07.
    /// Byte 1 LSB-first bits 8..15: 0,0,1,1,1,0,1,1 → value = 4+8+16+64+128 = 220 = 0xDC.
    #[test]
    fn complex_prefix_code_minimal_two_symbols() {
        let bytes = [0x07u8, 0xDC];
        let mut r = BitReader::new(&bytes);
        let code = decode_complex_prefix_code(&mut r, 0, 2).unwrap();
        assert_eq!(code.len(), 2);
        // Decode test: 2 more bits "0", "1" → main syms 0, 1.
        // Place them at bits 16..17 in a continuation buffer.
        // The decoder is positioned at bit_pos = 16 after the above
        // header. We extend the stream with one more byte for decode.
        // Re-run from scratch with a longer buffer.
        let bytes = [0x07u8, 0xDC, 0x02];
        // Byte 2 bits 16,17: bit 16=0, bit 17=1 → value 2 → 0x02.
        let mut r = BitReader::new(&bytes);
        let code = decode_complex_prefix_code(&mut r, 0, 2).unwrap();
        let s1 = code.decode_symbol(&mut r).unwrap();
        let s2 = code.decode_symbol(&mut r).unwrap();
        assert_eq!(s1, 0, "first decoded main symbol via bit '0'");
        assert_eq!(s2, 1, "second decoded main symbol via bit '1'");
    }

    /// L5b KAT-3: HSKIP=2 skips the first 2 entries (CLC-syms 1 and 2 get
    /// length 0 implicitly). Builds a CLC where CLC-sym 3 has length 2,
    /// CLC-sym 4 has length 2, CLC-sym 0 has length 2, CLC-sym 17 has
    /// length 2 (Kraft 1/4*4 = 1 = sum 32 → ok).
    ///
    /// Read order after HSKIP=2: CLC-sym 3 (pos 2), CLC-sym 4 (pos 3),
    /// CLC-sym 0 (pos 4), CLC-sym 5 (pos 5), CLC-sym 17 (pos 6).
    ///
    /// Plan: assign length 2 to CLC-sym 3, length 2 to CLC-sym 4,
    /// length 2 to CLC-sym 0, length 0 to CLC-sym 5, length 2 to
    /// CLC-sym 17. At that point Kraft = 4 * 8 = 32 → STOP after pos 6.
    /// Total CLC bits = 3 + 3 + 3 + 2 + 3 = 14 bits.
    ///
    /// Canonical CLC (sorted by length, then symbol):
    ///   CLC-sym 0 (len 2) → MSB code "00"
    ///   CLC-sym 3 (len 2) → MSB code "01"
    ///   CLC-sym 4 (len 2) → MSB code "10"
    ///   CLC-sym 17 (len 2) → MSB code "11"
    ///
    /// Then we want a 5-symbol main alphabet. Let's set main symbols
    /// 0,1,2,3,4 with lengths 0,0,3,3,4 (Kraft sum = 0 + 0 + 4096 + 4096
    /// + 2048 = 10240 ≠ 32768 — doesn't work).
    ///
    /// Simpler: main alphabet = 4 symbols. Use CLC to produce lengths
    /// [3, 3, 3, 3] → Kraft = 4*4096 = 16384 ≠ 32768. Bad.
    ///
    /// Try lengths [2, 2, 2, 2]: Kraft = 4 * 8192 = 32768 ✓. We'd
    /// emit CLC-sym 2 four times — but we don't have CLC-sym 2 in our
    /// code! Let's redo the CLC plan.
    ///
    /// Revised plan: HSKIP=2; assign CLC-sym 3 length 1, CLC-sym 17
    /// length 1 (Kraft 16+16=32 → STOP at pos 6 after reading 5 entries).
    /// Length 1 codes are 4 bits each (stream "1,1,1,0").
    ///
    /// CLC read sequence (pos 2..6):
    ///   pos 2 (CLC-sym 3): length 1 — 4 bits "1110" LSB
    ///   pos 3 (CLC-sym 4): length 0 — 2 bits "00"
    ///   pos 4 (CLC-sym 0): length 0 — 2 bits "00"
    ///   pos 5 (CLC-sym 5): length 0 — 2 bits "00"
    ///   pos 6 (CLC-sym 17): length 1 — 4 bits "1110"
    /// Total: 14 bits.
    ///
    /// Canonical CLC: sym 3 (len 1) → "0"; sym 17 (len 1) → "1".
    ///
    /// Main alphabet plan: alphabet_size = 4. Decode 4 main lengths.
    /// To emit length 3 four times in a row: emit CLC-sym 3 once + use
    /// RLE? But sym 16 repeats previous non-zero, sym 17 repeats zero.
    /// To get 4 lengths of value 3, we'd need to emit CLC-sym 3 four
    /// times (4 stream bits "0").
    ///
    /// That gives main_lengths = [3, 3, 3, 3]. Kraft = 4 * 4096 = 16384.
    /// Not 32768 → NotCanonical reject!
    ///
    /// Take a step back. For a valid 4-symbol code with Kraft=32768,
    /// lengths must satisfy sum(32768>>len) = 32768. Lengths {2,2,2,2}
    /// → 8192 * 4 = 32768 ✓. So we need 4 main lengths of 2.
    ///
    /// To emit "2" four times, the CLC must encode the value 2. Let's
    /// re-revise: CLC-sym 2 length 1, CLC-sym 17 length 1 (still Kraft 32).
    /// HSKIP=2 skips CLC-sym 1, CLC-sym 2. CRAP — we can't reach CLC-sym 2
    /// with HSKIP=2! HSKIP=2 makes CLC-syms 1 and 2 implicitly zero.
    ///
    /// So HSKIP=2 KATs need a CLC that emits values OTHER than 1 or 2.
    /// E.g. emit lengths-of-3 (CLC-sym 3) and 0s/zeros via CLC-sym 17.
    ///
    /// Final plan: HSKIP=2, alphabet_size=8. CLC-sym 3 length 1,
    /// CLC-sym 17 length 1. Emit 8 main lengths of value 3 directly:
    /// 8 * (32768 >> 3) = 8 * 4096 = 32768 ✓.
    ///
    /// Stream bits for the 8-bit-main-alphabet code construction:
    /// Each main length read = 1 CLC bit (CLC-sym 3 = "0", CLC-sym 17 = "1").
    /// All 8 should be CLC-sym 3 → 8 stream bits "0,0,0,0,0,0,0,0".
    ///
    /// Full layout (HSKIP=2 dispatcher already consumed by caller):
    ///   bits 0..3:  CLC pos 2 (sym 3) len 1 → "1,1,1,0"
    ///   bits 4..5:  CLC pos 3 (sym 4) len 0 → "0,0"
    ///   bits 6..7:  CLC pos 4 (sym 0) len 0 → "0,0"
    ///   bits 8..9:  CLC pos 5 (sym 5) len 0 → "0,0"
    ///   bits 10..13: CLC pos 6 (sym 17) len 1 → "1,1,1,0"
    ///   (Kraft = 32, nonzero=2 → STOP)
    ///   bits 14..21: 8 main lengths, each "0" CLC bit = CLC-sym 3 → main len 3.
    ///   bits 22+: 8 main decode bits to test (placeholder)
    ///
    /// Byte 0 (bits 0..7): 1,1,1,0,0,0,0,0 → value 1+2+4 = 7 → 0x07.
    /// Byte 1 (bits 8..15): 0,0,1,1,1,0,0,0 → value 4+8+16 = 28 → 0x1C.
    /// Byte 2 (bits 16..23): 0,0,0,0,0,0,X,X (X bits 22-23) → see below.
    ///
    /// For main alphabet of 8 syms each with length 3: canonical codes
    /// (sorted by length then symbol) are:
    ///   sym 0 → 000
    ///   sym 1 → 001
    ///   sym 2 → 010
    ///   sym 3 → 011
    ///   sym 4 → 100
    ///   sym 5 → 101
    ///   sym 6 → 110
    ///   sym 7 → 111
    ///
    /// Test decode: bits "000" → sym 0. We just need to read it once.
    /// Place decode bits 22..24 = "0", "0", "0" → decode sym 0.
    /// bit 22 = 0, bit 23 = 0, bit 24 = 0.
    ///
    /// Byte 2 bits 16..23 = all 0 → 0x00. Byte 3 bit 24 = 0 → 0x00.
    #[test]
    fn complex_prefix_code_hskip_2_eight_syms_all_length_3() {
        let bytes = [0x07u8, 0x1C, 0x00, 0x00];
        let mut r = BitReader::new(&bytes);
        let code = decode_complex_prefix_code(&mut r, 2, 8).unwrap();
        assert_eq!(code.len(), 8);
        let s = code.decode_symbol(&mut r).unwrap();
        assert_eq!(s, 0, "MSB '000' decodes to length-3 sym 0");
    }

    /// L5b KAT-4: symbol-16 RLE (repeat-previous-nonzero). Main alphabet
    /// of 4 symbols, all length 2. Use CLC to emit length 2 once, then
    /// CLC-sym 16 with extra_bits=0 → "repeat 3 times" = 3 more emissions.
    /// Total emissions = 1 + 3 = 4 ✓.
    ///
    /// Plan:
    ///   HSKIP=0. CLC-sym 2 length 1 (4 bits "1,1,1,0"), CLC-sym 16
    ///   length 1 (4 bits "1,1,1,0"). Other slots length 0.
    ///
    ///   Read order: CLC-sym 1 (pos 0), 2 (pos 1), 3 (pos 2), 4 (pos 3),
    ///   0 (pos 4), 5 (pos 5), 17 (pos 6), 6 (pos 7), 16 (pos 8), ...
    ///
    ///   To put length 1 on CLC-sym 2 and CLC-sym 16, we need to read
    ///   through pos 0 (len 0), pos 1 (len 1), pos 2 (len 0), pos 3
    ///   (len 0), pos 4 (len 0), pos 5 (len 0), pos 6 (len 0), pos 7
    ///   (len 0), pos 8 (len 1). At that point Kraft = 16+16 = 32 → STOP.
    ///
    ///   But we have a lot of zeros — that's a lot of bits (2 bits each).
    ///   Total CLC bits = 2 + 4 + 2 + 2 + 2 + 2 + 2 + 2 + 4 = 22 bits.
    ///
    /// CLC canonical: sym 2 (len 1) → MSB "0"; sym 16 (len 1) → MSB "1".
    ///
    /// Main reads:
    ///   First read: CLC-sym 2 (CLC bit "0") → main len 2.
    ///     main_lengths = [2]. prev_nonzero=2.
    ///   Second read: CLC-sym 16 (CLC bit "1") → repeat-previous.
    ///     2 extra bits = 0 → count = 3 + 0 = 3. Emit 3 more length-2s.
    ///     main_lengths = [2,2,2,2]. Loop exits at length 4.
    ///
    /// Main canonical: all 4 syms length 2 → MSB codes 00, 01, 10, 11.
    ///
    /// Full bit layout (HSKIP=0 already consumed by dispatcher):
    ///   bits 0..1:  CLC pos 0 (sym 1) len 0 → "0,0"
    ///   bits 2..5:  CLC pos 1 (sym 2) len 1 → "1,1,1,0"
    ///   bits 6..7:  CLC pos 2 (sym 3) len 0 → "0,0"
    ///   bits 8..9:  CLC pos 3 (sym 4) len 0 → "0,0"
    ///   bits 10..11: CLC pos 4 (sym 0) len 0 → "0,0"
    ///   bits 12..13: CLC pos 5 (sym 5) len 0 → "0,0"
    ///   bits 14..15: CLC pos 6 (sym 17) len 0 → "0,0"
    ///   bits 16..17: CLC pos 7 (sym 6) len 0 → "0,0"
    ///   bits 18..21: CLC pos 8 (sym 16) len 1 → "1,1,1,0"
    ///   (Kraft = 32, nonzero=2 → STOP)
    ///   bit 22: main read 1 = "0" (CLC-sym 2 → main len 2)
    ///   bit 23: main read 2 = "1" (CLC-sym 16 → repeat previous)
    ///   bits 24..25: 2 extra bits = 0,0 → count = 3.
    ///   bits 26..27: 2 decode bits "0,0" → main sym 0
    ///
    /// Byte 0 bits 0..7: 0,0,1,1,1,0,0,0 → 4+8+16 = 28 → 0x1C
    /// Byte 1 bits 8..15: 0,0,0,0,0,0,0,0 → 0x00
    /// Byte 2 bits 16..23: 0,0,1,1,1,0,0,1 → 4+8+16+128 = 156 → 0x9C
    /// Byte 3 bits 24..27: 0,0,0,0 → 0x00
    #[test]
    fn complex_prefix_code_symbol_16_repeat_previous() {
        let bytes = [0x1Cu8, 0x00, 0x9C, 0x00];
        let mut r = BitReader::new(&bytes);
        let code = decode_complex_prefix_code(&mut r, 0, 4).unwrap();
        assert_eq!(code.len(), 4);
        // Decode bit "0,0" (MSB) → main sym 0.
        let s = code.decode_symbol(&mut r).unwrap();
        assert_eq!(s, 0);
    }

    /// L5b KAT-5: symbol-17 RLE (repeat-zero). Main alphabet of 5
    /// symbols where the first 3 are absent (length 0) and the last 2
    /// have length 1. CLC needs to emit value 1 (= main length 1) and
    /// value 17 (= zero-repeat).
    ///
    /// Plan:
    ///   HSKIP=0. CLC-sym 1 length 1, CLC-sym 17 length 1.
    ///   Both will be in canonical code: sym 1 (len 1) → "0", sym 17
    ///   (len 1) → "1".
    ///
    ///   CLC read order: pos 0 (sym 1), pos 1 (sym 2), pos 2 (sym 3),
    ///   pos 3 (sym 4), pos 4 (sym 0), pos 5 (sym 5), pos 6 (sym 17).
    ///   CLC-sym 1 → length 1 (pos 0, 4 bits "1,1,1,0").
    ///   CLC-sym 17 → length 1 (pos 6, 4 bits "1,1,1,0").
    ///   Others length 0 (2 bits each, "0,0").
    ///   Kraft = 16+16 = 32 → STOP at pos 6.
    ///   Total CLC bits = 4+2+2+2+2+2+4 = 18 bits.
    ///
    ///   Main reads (alphabet_size=5):
    ///     read 1: CLC-sym 17 (CLC bit "1") + 3 extra bits = 0,0,0 →
    ///       count = 3 + 0 = 3. Emit 3 zeros. main_lengths=[0,0,0].
    ///     read 2: CLC-sym 1 (CLC bit "0") → main len 1. main=[0,0,0,1].
    ///     read 3: CLC-sym 1 (CLC bit "0") → main len 1. main=[0,0,0,1,1].
    ///   Loop exits at length 5.
    ///
    ///   Main canonical: sym 3 (len 1) → "0"; sym 4 (len 1) → "1".
    ///
    /// Full bit layout (HSKIP=0):
    ///   bits 0..3:  CLC pos 0 (sym 1) len 1 → "1,1,1,0"
    ///   bits 4..5:  CLC pos 1 (sym 2) len 0 → "0,0"
    ///   bits 6..7:  CLC pos 2 (sym 3) len 0 → "0,0"
    ///   bits 8..9:  CLC pos 3 (sym 4) len 0 → "0,0"
    ///   bits 10..11: CLC pos 4 (sym 0) len 0 → "0,0"
    ///   bits 12..13: CLC pos 5 (sym 5) len 0 → "0,0"
    ///   bits 14..17: CLC pos 6 (sym 17) len 1 → "1,1,1,0"
    ///   bit 18: main read 1 = "1" (sym 17, repeat-zero)
    ///   bits 19..21: 3 extra bits = "0,0,0"
    ///   bit 22: main read 2 = "0" (sym 1, length 1)
    ///   bit 23: main read 3 = "0" (sym 1, length 1)
    ///   bit 24: decode test, "0" → main sym 3.
    ///
    /// Byte 0 bits 0..7: 1,1,1,0,0,0,0,0 → 0x07.
    /// Byte 1 bits 8..15: 0,0,0,0,0,0,1,1 → 64+128 = 192 → 0xC0.
    /// Byte 2 bits 16..23: 1,0,1,0,0,0,0,0 → 1+4 = 5 → 0x05.
    /// Byte 3 bits 24..31: 0,0,0,0,0,0,0,0 → 0x00.
    #[test]
    fn complex_prefix_code_symbol_17_repeat_zero() {
        let bytes = [0x07u8, 0xC0, 0x05, 0x00];
        let mut r = BitReader::new(&bytes);
        let code = decode_complex_prefix_code(&mut r, 0, 5).unwrap();
        assert_eq!(code.len(), 2);
        // Decode bit "0" → main sym 3.
        let s = code.decode_symbol(&mut r).unwrap();
        assert_eq!(s, 3);
    }

    /// L5b KAT-6: complex prefix code dispatched via `decode_prefix_code`
    /// using a leading 2-bit '00' (HSKIP=0). Reuses the KAT-2 layout
    /// but shifted by 2 bits to account for the dispatcher's type-code read.
    /// Uses alphabet_size=2 (same as KAT-2).
    ///
    /// Stream bit values (bit 0 is the LSB of byte 0):
    ///   bit 0..1: 0, 0  (type code = HSKIP=0)
    ///   bit 2..5: 1,1,1,0  (CLC pos 0 sym 1 len 1)
    ///   bit 6..7: 0,0  (CLC pos 1 sym 2 len 0)
    ///   bit 8..9: 0,0  (CLC pos 2 sym 3 len 0)
    ///   bit 10..11: 0,0  (CLC pos 3 sym 4 len 0)
    ///   bit 12..15: 1,1,1,0  (CLC pos 4 sym 0 len 1)
    ///   bit 16..17: 1, 1  (main lens via CLC-sym 1)
    ///   bit 18..19: 0, 1  (decode test → main syms 0, 1)
    ///
    /// Byte 0 bits 0..7: 0,0,1,1,1,0,0,0 → 4+8+16 = 28 → 0x1C
    /// Byte 1 bits 8..15: 0,0,0,0,1,1,1,0 → 16+32+64 = 112 → 0x70
    /// Byte 2 bits 16..19: 1,1,0,1 → 1+2+8 = 11 → 0x0B
    #[test]
    fn decode_prefix_code_dispatches_to_complex_on_type_0() {
        let bytes = [0x1Cu8, 0x70, 0x0B];
        let mut r = BitReader::new(&bytes);
        let code = decode_prefix_code(&mut r, 4, 2).unwrap();
        assert_eq!(code.len(), 2);
        let s1 = code.decode_symbol(&mut r).unwrap();
        let s2 = code.decode_symbol(&mut r).unwrap();
        assert_eq!(s1, 0);
        assert_eq!(s2, 1);
    }

    /// L5b KAT-7 (pentest): symbol-17 RLE that overruns the declared
    /// alphabet size → typed RepeatOverrunsAlphabet error.
    ///
    /// Plan: same CLC as KAT-5, but ask for alphabet_size=2 (smaller
    /// than the 3 zeros that the first sym-17 emit would produce).
    /// The decoder must reject before pushing past alphabet_size.
    ///
    /// Reuse byte-0 + byte-1 from KAT-5 (CLC) + byte 2 first read
    /// only — emit CLC-sym 17 with extra=0 (count=3, but alphabet=2).
    #[test]
    fn pentest_complex_prefix_code_symbol_17_overruns_alphabet() {
        // KAT-5 bytes through bit 21 are sufficient for the error.
        let bytes = [0x07u8, 0xC0, 0x05, 0x00];
        let mut r = BitReader::new(&bytes);
        let err = decode_complex_prefix_code(&mut r, 0, 2).unwrap_err();
        assert!(
            matches!(err, HuffmanError::RepeatOverrunsAlphabet { needed: 3, alphabet_size: 2 }),
            "expected RepeatOverrunsAlphabet needed=3 alpha=2, got {err:?}"
        );
    }
}
