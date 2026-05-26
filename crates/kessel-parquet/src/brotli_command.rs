//! Brotli insert-and-copy command alphabet — RFC 7932 §5.
//!
//! A compressed metablock is driven by a sequence of "insert-and-copy"
//! commands. Each command produces a triple
//! `(insert_length, copy_length, distance_or_implicit)`:
//!
//!   - **insert** `insert_length` literal bytes (decoded via the literal
//!     prefix code(s) — see L8 context maps)
//!   - **copy** `copy_length` bytes from a back-reference at `distance`
//!     bytes earlier in the output buffer (the distance is decoded
//!     separately via the distance prefix code) — UNLESS the command
//!     signals "implicit distance" in which case the previously-used
//!     distance is reused with no further decode.
//!
//! ## Encoding (RFC 7932 §5)
//!
//! The command alphabet has **704 symbols** = 11 cells × 64 codes per
//! cell. Each command symbol C ∈ [0, 704) decomposes per the reference
//! decoder's `kCmdLut` initialiser (Google brotli `c/dec/prefix.c`):
//!
//! ```text
//!   cell_idx  = C >> 6                          (0..10)
//!   cell_pos  = kCellPos[cell_idx]              (lookup, 11 entries)
//!   copy_code = ((cell_pos << 3) & 0x18) + (C & 0x7)        (0..23)
//!   insert_code = (cell_pos & 0x18) + ((C >> 3) & 0x7)      (0..23)
//!   distance_implicit = cell_idx < 2
//! ```
//!
//! where `kCellPos = [0, 1, 0, 1, 8, 9, 2, 16, 10, 17, 18]`.
//!
//! Given the (insert_code, copy_code), the actual lengths are then:
//!
//! ```text
//!   insert_length = INSERT_OFFSET[insert_code] + read_bits(INSERT_EXTRA_BITS[insert_code])
//!   copy_length   = COPY_OFFSET[copy_code]   + read_bits(COPY_EXTRA_BITS[copy_code])
//! ```
//!
//! The extra-bits and offset tables are computed once at decoder startup
//! from the 24-entry extra-bits arrays per RFC §5.2. They are encoded
//! here as constant slices (hand-derived and unit-tested below — the
//! same values as the reference decoder produces).
//!
//! ## SP154 L9 scope (this commit)
//!
//! V1 ships:
//!   - The four 24-entry constant tables (INSERT_EXTRA_BITS,
//!     INSERT_OFFSET, COPY_EXTRA_BITS, COPY_OFFSET).
//!   - The 11-entry `CELL_POS` table.
//!   - `decompose_command_code(cmd) -> CommandComponents` — pure
//!     bit-arithmetic that turns a command symbol into its
//!     (insert_code, copy_code, distance_implicit) triple.
//!   - `decode_insert_length(br, insert_code) -> u32` — base + extras.
//!   - `decode_copy_length(br, copy_code) -> u32` — base + extras.
//!   - `decode_command_components(br, cmd_code) -> (insert_len, copy_len,
//!     dist_implicit)` — composes the above given a pre-decoded command
//!     symbol.
//!
//! What this does NOT ship (deferred to L11 — compressed-metablock
//! orchestration):
//!   - Reading the command symbol via the command prefix code (the
//!     prefix code itself is decoded via the existing §3.4 / §3.5
//!     machinery in `brotli_huffman.rs`; the caller passes the decoded
//!     symbol to `decompose_command_code`).
//!   - Distance decoding (distance prefix code + NPOSTFIX/NDIRECT
//!     translation per RFC §4). The L7 helper `decode_distance_params`
//!     reads the params; the actual distance translation is a separate
//!     L9-followup sub-piece.
//!   - The LZ77 EXECUTION loop that consumes the (insert, copy,
//!     distance) triples and writes to the ring buffer.
//!
//! ## Safety
//!
//! `#![forbid(unsafe_code)]`; bounds-checked arithmetic; typed errors
//! for every failure mode (no panics).

#![allow(dead_code)]

use crate::brotli::BrotliError;
use crate::brotli_bit_reader::BitReader;

/// Number of symbols in the Brotli insert-and-copy command alphabet.
/// RFC 7932 §5: 704 symbols (= 11 cells × 64 codes per cell).
pub(crate) const NUM_COMMAND_SYMBOLS: u32 = 704;

/// Per-insert-length-code extra-bits count (RFC 7932 §5, Table 2).
/// 24 entries indexed by `insert_code` 0..=23.
pub(crate) const INSERT_EXTRA_BITS: [u8; 24] = [
    0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 7, 8, 9, 10, 12, 14, 24,
];

/// Per-copy-length-code extra-bits count (RFC 7932 §5, Table 3).
/// 24 entries indexed by `copy_code` 0..=23.
pub(crate) const COPY_EXTRA_BITS: [u8; 24] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 7, 8, 9, 10, 24,
];

/// Per-insert-length-code base length (RFC 7932 §5, Table 2). Computed
/// from `INSERT_EXTRA_BITS` via cumulative sum starting at 0:
///   INSERT_OFFSET[i+1] = INSERT_OFFSET[i] + (1 << INSERT_EXTRA_BITS[i])
/// Pinned by `insert_offsets_match_reference_table` KAT.
pub(crate) const INSERT_OFFSET: [u32; 24] = [
    0, 1, 2, 3, 4, 5, 6, 8, 10, 14, 18, 26, 34, 50, 66, 98, 130, 194, 322, 578, 1090, 2114, 6210,
    22594,
];

/// Per-copy-length-code base length (RFC 7932 §5, Table 3). Computed
/// from `COPY_EXTRA_BITS` via cumulative sum starting at 2:
///   COPY_OFFSET[i+1] = COPY_OFFSET[i] + (1 << COPY_EXTRA_BITS[i])
/// Pinned by `copy_offsets_match_reference_table` KAT.
pub(crate) const COPY_OFFSET: [u32; 24] = [
    2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 18, 22, 30, 38, 54, 70, 102, 134, 198, 326, 582, 1094, 2118,
];

/// `kCellPos` from the reference decoder. 11 entries indexed by
/// `cell_idx = command_code >> 6` (cell_idx 0..=10).
/// `cell_pos = CELL_POS[cell_idx]` is then used to recover the insert
/// and copy code ranges per the §5 cell-decomposition formula.
pub(crate) const CELL_POS: [u8; 11] = [0, 1, 0, 1, 8, 9, 2, 16, 10, 17, 18];

/// Decomposed command-code components per RFC 7932 §5.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommandComponents {
    /// Insert-length code (0..=23); used to look up base + extras in
    /// `INSERT_OFFSET` / `INSERT_EXTRA_BITS`.
    pub(crate) insert_code: u8,
    /// Copy-length code (0..=23); used to look up base + extras in
    /// `COPY_OFFSET` / `COPY_EXTRA_BITS`.
    pub(crate) copy_code: u8,
    /// True iff this command uses the IMPLICIT (= last-used) distance.
    /// Per the reference decoder: `distance_implicit = (cell_idx < 2)`.
    /// When true, the decoder does NOT read a distance symbol — instead
    /// it reuses the previously-emitted distance with no further bits.
    pub(crate) distance_implicit: bool,
}

/// Typed errors specific to insert-and-copy command decoding. Wraps
/// `BrotliError` for nested bit-reader / length failures.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BrotliCommandError {
    /// Inner brotli / bit-reader error.
    Inner(BrotliError),
    /// Command symbol exceeded `NUM_COMMAND_SYMBOLS` (= 704).
    CommandSymbolOutOfRange { sym: u32 },
    /// Insert-length or copy-length code exceeded 23 — only reachable
    /// via direct test-shim calls (the `decompose_command_code` path
    /// always yields 0..=23 by construction).
    LengthCodeOutOfRange { code: u8 },
}

impl core::fmt::Display for BrotliCommandError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for BrotliCommandError {}

impl From<BrotliError> for BrotliCommandError {
    fn from(e: BrotliError) -> Self {
        BrotliCommandError::Inner(e)
    }
}

/// Decompose a 704-alphabet command symbol into its
/// `(insert_code, copy_code, distance_implicit)` triple per RFC 7932 §5.
///
/// Mirrors the reference decoder's `kCmdLut` initialiser exactly:
///
/// ```text
///   cell_idx  = sym >> 6                  (0..=10)
///   cell_pos  = CELL_POS[cell_idx]        (lookup)
///   copy_code   = ((cell_pos << 3) & 0x18) + (sym & 0x7)
///   insert_code = (cell_pos & 0x18) + ((sym >> 3) & 0x7)
///   distance_implicit = (cell_idx < 2)
/// ```
///
/// Caller-supplied `sym` MUST be in [0, 704); otherwise
/// `CommandSymbolOutOfRange` is surfaced.
pub(crate) fn decompose_command_code(sym: u32) -> Result<CommandComponents, BrotliCommandError> {
    if sym >= NUM_COMMAND_SYMBOLS {
        return Err(BrotliCommandError::CommandSymbolOutOfRange { sym });
    }
    let cell_idx = (sym >> 6) as usize; // 0..=10
    let cell_pos = CELL_POS[cell_idx] as u32; // 0..=18 (selected values)
    let copy_code = (((cell_pos << 3) & 0x18) + (sym & 0x7)) as u8;
    let insert_code = ((cell_pos & 0x18) + ((sym >> 3) & 0x7)) as u8;
    let distance_implicit = cell_idx < 2;
    Ok(CommandComponents {
        insert_code,
        copy_code,
        distance_implicit,
    })
}

/// Decode an insert-length value given a pre-decoded insert-length code.
///
/// Reads `INSERT_EXTRA_BITS[insert_code]` extra bits LSB-first from the
/// stream and adds them to `INSERT_OFFSET[insert_code]`. Insert lengths
/// range from 0 (code 0, 0 extras) up to 16,799,809 (code 23 with 24
/// extras set to 0xFF_FFFF).
pub(crate) fn decode_insert_length(
    r: &mut BitReader,
    insert_code: u8,
) -> Result<u32, BrotliCommandError> {
    let idx = insert_code as usize;
    if idx >= 24 {
        return Err(BrotliCommandError::LengthCodeOutOfRange { code: insert_code });
    }
    let extras = INSERT_EXTRA_BITS[idx];
    let extra_value = if extras == 0 {
        0
    } else {
        r.read_bits(extras).map_err(BrotliError::from)?
    };
    Ok(INSERT_OFFSET[idx]
        .checked_add(extra_value)
        .ok_or(BrotliCommandError::Inner(BrotliError::UnexpectedEof))?)
}

/// Decode a copy-length value given a pre-decoded copy-length code.
///
/// Reads `COPY_EXTRA_BITS[copy_code]` extra bits LSB-first from the
/// stream and adds them to `COPY_OFFSET[copy_code]`. Copy lengths range
/// from 2 (code 0, 0 extras) up to 16,779,333 (code 23 with 24 extras
/// set to 0xFF_FFFF).
pub(crate) fn decode_copy_length(
    r: &mut BitReader,
    copy_code: u8,
) -> Result<u32, BrotliCommandError> {
    let idx = copy_code as usize;
    if idx >= 24 {
        return Err(BrotliCommandError::LengthCodeOutOfRange { code: copy_code });
    }
    let extras = COPY_EXTRA_BITS[idx];
    let extra_value = if extras == 0 {
        0
    } else {
        r.read_bits(extras).map_err(BrotliError::from)?
    };
    Ok(COPY_OFFSET[idx]
        .checked_add(extra_value)
        .ok_or(BrotliCommandError::Inner(BrotliError::UnexpectedEof))?)
}

/// Decoded insert-and-copy command values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DecodedCommand {
    /// Number of literal bytes to insert before the copy.
    pub(crate) insert_length: u32,
    /// Number of bytes to copy from the back-reference distance.
    pub(crate) copy_length: u32,
    /// True iff the distance is implicit (= last-used distance, no
    /// distance symbol to be read). False iff a distance symbol must
    /// follow in the bit stream (L9 distance decode).
    pub(crate) distance_implicit: bool,
}

/// Full command-symbol decode given a pre-decoded 704-alphabet symbol.
///
/// Composes `decompose_command_code` + `decode_insert_length` +
/// `decode_copy_length`. This is the function the L11 orchestration
/// loop will call once per command.
pub(crate) fn decode_command_components(
    r: &mut BitReader,
    sym: u32,
) -> Result<DecodedCommand, BrotliCommandError> {
    let components = decompose_command_code(sym)?;
    let insert_length = decode_insert_length(r, components.insert_code)?;
    let copy_length = decode_copy_length(r, components.copy_code)?;
    Ok(DecodedCommand {
        insert_length,
        copy_length,
        distance_implicit: components.distance_implicit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// L9 KAT-1: INSERT_OFFSET table matches the reference decoder
    /// `insert_length_offsets[]` initialiser exactly. Computed once
    /// at startup in `BrotliDecoderInitCmdLut` (Google brotli
    /// `c/dec/prefix.c`).
    #[test]
    fn insert_offsets_match_reference_table() {
        // Re-compute from the extra-bits table and compare.
        let mut computed = [0u32; 24];
        for i in 0..23 {
            computed[i + 1] = computed[i] + (1u32 << INSERT_EXTRA_BITS[i]);
        }
        assert_eq!(
            INSERT_OFFSET, computed,
            "INSERT_OFFSET must equal cumulative sum of (1 << INSERT_EXTRA_BITS[i]) starting at 0"
        );
        // Specific anchor values per the reference:
        assert_eq!(INSERT_OFFSET[0], 0);
        assert_eq!(INSERT_OFFSET[6], 6);
        assert_eq!(INSERT_OFFSET[12], 34);
        assert_eq!(INSERT_OFFSET[23], 22594);
    }

    /// L9 KAT-2: COPY_OFFSET table matches the reference decoder
    /// `copy_length_offsets[]` initialiser exactly. Starts at 2 (the
    /// Brotli minimum-match length per RFC §5).
    #[test]
    fn copy_offsets_match_reference_table() {
        let mut computed = [0u32; 24];
        computed[0] = 2;
        for i in 0..23 {
            computed[i + 1] = computed[i] + (1u32 << COPY_EXTRA_BITS[i]);
        }
        assert_eq!(
            COPY_OFFSET, computed,
            "COPY_OFFSET must equal cumulative sum of (1 << COPY_EXTRA_BITS[i]) starting at 2"
        );
        // Specific anchor values per the reference:
        assert_eq!(COPY_OFFSET[0], 2);
        assert_eq!(COPY_OFFSET[8], 10);
        assert_eq!(COPY_OFFSET[14], 38);
        assert_eq!(COPY_OFFSET[23], 2118);
    }

    /// L9 KAT-3: command symbol 0 decomposes to
    /// (insert_code=0, copy_code=0, distance_implicit=true).
    /// cell_idx = 0 >> 6 = 0; cell_pos = CELL_POS[0] = 0.
    /// copy_code = (0<<3) & 0x18 + (0 & 0x7) = 0.
    /// insert_code = (0 & 0x18) + ((0 >> 3) & 0x7) = 0.
    /// distance_implicit = (0 < 2) = true.
    #[test]
    fn decompose_command_code_symbol_zero() {
        let c = decompose_command_code(0).unwrap();
        assert_eq!(c.insert_code, 0);
        assert_eq!(c.copy_code, 0);
        assert!(c.distance_implicit);
    }

    /// L9 KAT-4: command symbol 7 decomposes to
    /// (insert_code=0, copy_code=7, distance_implicit=true).
    /// cell_idx = 0; cell_pos = 0; copy_code = 0 + (7 & 0x7) = 7;
    /// insert_code = 0 + ((7>>3) & 0x7) = 0.
    #[test]
    fn decompose_command_code_symbol_seven() {
        let c = decompose_command_code(7).unwrap();
        assert_eq!(c.insert_code, 0);
        assert_eq!(c.copy_code, 7);
        assert!(c.distance_implicit);
    }

    /// L9 KAT-5: command symbol 63 decomposes to
    /// (insert_code=7, copy_code=7, distance_implicit=true).
    /// cell_idx = 63>>6 = 0; cell_pos=0.
    /// copy_code = 0 + (63 & 0x7) = 7.
    /// insert_code = 0 + ((63>>3) & 0x7) = (7 & 0x7) = 7.
    #[test]
    fn decompose_command_code_symbol_63() {
        let c = decompose_command_code(63).unwrap();
        assert_eq!(c.insert_code, 7);
        assert_eq!(c.copy_code, 7);
        assert!(c.distance_implicit);
    }

    /// L9 KAT-6: command symbol 64 — first symbol of cell_idx=1.
    /// cell_pos = CELL_POS[1] = 1.
    /// copy_code = (1<<3) & 0x18 + (64 & 0x7) = 8 + 0 = 8.
    /// insert_code = (1 & 0x18) + ((64>>3) & 0x7) = 0 + 0 = 0.
    /// distance_implicit = (1 < 2) = true.
    #[test]
    fn decompose_command_code_symbol_64() {
        let c = decompose_command_code(64).unwrap();
        assert_eq!(c.insert_code, 0);
        assert_eq!(c.copy_code, 8);
        assert!(c.distance_implicit);
    }

    /// L9 KAT-7: command symbol 128 — first symbol of cell_idx=2;
    /// distance_implicit FLIPS to false (cell_idx >= 2).
    /// cell_pos = CELL_POS[2] = 0.
    /// copy_code = (0<<3) & 0x18 + 0 = 0.
    /// insert_code = 0 + ((128>>3) & 0x7) = (16 & 0x7) = 0.
    /// distance_implicit = (2 < 2) = false.
    #[test]
    fn decompose_command_code_symbol_128_distance_explicit() {
        let c = decompose_command_code(128).unwrap();
        assert_eq!(c.insert_code, 0);
        assert_eq!(c.copy_code, 0);
        assert!(
            !c.distance_implicit,
            "symbol 128 (cell_idx=2) must have explicit distance"
        );
    }

    /// L9 KAT-8: command symbol 703 (the very last symbol).
    /// cell_idx = 703 >> 6 = 10; cell_pos = CELL_POS[10] = 18.
    /// copy_code = (18<<3) & 0x18 + (703 & 0x7) = (144 & 0x18) + 7 = 0x10 + 7 = 23.
    /// insert_code = (18 & 0x18) + ((703>>3) & 0x7) = 0x10 + (87 & 7) = 16 + 7 = 23.
    /// distance_implicit = (10 < 2) = false.
    #[test]
    fn decompose_command_code_symbol_703_max() {
        let c = decompose_command_code(703).unwrap();
        assert_eq!(c.insert_code, 23);
        assert_eq!(c.copy_code, 23);
        assert!(!c.distance_implicit);
    }

    /// L9 KAT-9: command symbol 704 → out-of-range (typed error).
    #[test]
    fn decompose_command_code_704_out_of_range() {
        let err = decompose_command_code(704).unwrap_err();
        match err {
            BrotliCommandError::CommandSymbolOutOfRange { sym } => assert_eq!(sym, 704),
            other => panic!("expected CommandSymbolOutOfRange, got {other:?}"),
        }
    }

    /// L9 KAT-10: `decode_insert_length(0)` consumes 0 bits and returns
    /// 0 (the most common short-literal case).
    #[test]
    fn decode_insert_length_code_zero_returns_zero() {
        let bytes = [0xFFu8]; // any byte — should not be consumed
        let mut r = BitReader::new(&bytes);
        let n = decode_insert_length(&mut r, 0).unwrap();
        assert_eq!(n, 0);
        assert_eq!(r.bit_pos(), 0, "code 0 consumes 0 extra bits");
    }

    /// L9 KAT-11: `decode_insert_length(6)` (1 extra bit) — reads 1 bit
    /// and returns INSERT_OFFSET[6] + bit = 6 + bit.
    /// Byte = 0x01 (bit 0 = 1) → returns 7.
    #[test]
    fn decode_insert_length_code_six_one_extra_bit_set() {
        let bytes = [0x01u8];
        let mut r = BitReader::new(&bytes);
        let n = decode_insert_length(&mut r, 6).unwrap();
        assert_eq!(n, 7);
        assert_eq!(r.bit_pos(), 1);
    }

    /// L9 KAT-12: `decode_insert_length(12)` (4 extra bits, offset 34).
    /// Byte 0x0A = 0b0000_1010. read_bits(4) LSB-first → bit 0=0,
    /// bit 1=1, bit 2=0, bit 3=1 → value 0b1010 = 10.
    /// Total = INSERT_OFFSET[12] + 10 = 34 + 10 = 44.
    #[test]
    fn decode_insert_length_code_twelve_four_extra_bits() {
        let bytes = [0x0Au8];
        let mut r = BitReader::new(&bytes);
        let n = decode_insert_length(&mut r, 12).unwrap();
        assert_eq!(n, 44);
        assert_eq!(r.bit_pos(), 4);
    }

    /// L9 KAT-13: `decode_copy_length(0)` returns COPY_OFFSET[0] = 2.
    /// (Brotli's minimum match length is 2.)
    #[test]
    fn decode_copy_length_code_zero_returns_two() {
        let bytes = [0xFFu8];
        let mut r = BitReader::new(&bytes);
        let n = decode_copy_length(&mut r, 0).unwrap();
        assert_eq!(n, 2);
        assert_eq!(r.bit_pos(), 0);
    }

    /// L9 KAT-14: `decode_copy_length(10)` (2 extra bits, offset=14) with
    /// stream bits 1,1 (value 3) → 14 + 3 = 17.
    /// Byte: bits 0,1 set → 0x03.
    #[test]
    fn decode_copy_length_code_ten_two_extra_bits_max() {
        let bytes = [0x03u8];
        let mut r = BitReader::new(&bytes);
        let n = decode_copy_length(&mut r, 10).unwrap();
        assert_eq!(n, 17);
        assert_eq!(r.bit_pos(), 2);
    }

    /// L9 KAT-15: composed `decode_command_components` for sym=0 → consumes
    /// 0 bits and returns insert_length=0, copy_length=2, distance_implicit=true.
    #[test]
    fn decode_command_components_symbol_zero_minimal() {
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let dc = decode_command_components(&mut r, 0).unwrap();
        assert_eq!(dc.insert_length, 0);
        assert_eq!(dc.copy_length, 2);
        assert!(dc.distance_implicit);
        assert_eq!(r.bit_pos(), 0, "sym 0 has zero-bit extras for both fields");
    }

    /// L9 KAT-16: composed decode for sym=128 (cell_idx=2 → explicit
    /// distance) — same fast-path zero-extras (insert_code=0,
    /// copy_code=0) but distance_implicit FLIPS to false.
    #[test]
    fn decode_command_components_symbol_128_explicit_distance() {
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let dc = decode_command_components(&mut r, 128).unwrap();
        assert_eq!(dc.insert_length, 0);
        assert_eq!(dc.copy_length, 2);
        assert!(!dc.distance_implicit);
    }

    /// L9 KAT-17: composed decode for sym=703 (max symbol) — reads 24
    /// extra bits for insert (code 23) + 24 extras for copy (code 23) =
    /// 48 extras total. With all extras = 0, insert_length =
    /// INSERT_OFFSET[23] = 22594; copy_length = COPY_OFFSET[23] = 2118.
    /// 48 bits = 6 bytes of 0x00.
    #[test]
    fn decode_command_components_symbol_703_max_with_zero_extras() {
        let bytes = [0u8; 6];
        let mut r = BitReader::new(&bytes);
        let dc = decode_command_components(&mut r, 703).unwrap();
        assert_eq!(dc.insert_length, 22594);
        assert_eq!(dc.copy_length, 2118);
        assert!(!dc.distance_implicit);
        assert_eq!(r.bit_pos(), 48);
    }

    /// Pentest: `decode_insert_length(24)` (out-of-range code) returns
    /// typed `LengthCodeOutOfRange`. Per the doc: this is only reachable
    /// via direct test-shim calls — the `decompose_command_code` path
    /// always produces codes in 0..=23 by construction. The pentest
    /// locks the bounds-check.
    #[test]
    fn pentest_decode_insert_length_code_24_out_of_range() {
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let err = decode_insert_length(&mut r, 24).unwrap_err();
        match err {
            BrotliCommandError::LengthCodeOutOfRange { code } => assert_eq!(code, 24),
            other => panic!("expected LengthCodeOutOfRange, got {other:?}"),
        }
    }

    /// Pentest: `decode_copy_length(99)` (out-of-range code) returns
    /// typed `LengthCodeOutOfRange`.
    #[test]
    fn pentest_decode_copy_length_code_99_out_of_range() {
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let err = decode_copy_length(&mut r, 99).unwrap_err();
        match err {
            BrotliCommandError::LengthCodeOutOfRange { code } => assert_eq!(code, 99),
            other => panic!("expected LengthCodeOutOfRange, got {other:?}"),
        }
    }

    /// Pentest: insufficient stream bits for the extras → typed Inner
    /// BitReader UnexpectedEof bubbles through. insert_code=22 needs 14
    /// extras → stream of 1 byte (8 bits) is too short.
    #[test]
    fn pentest_decode_insert_length_truncated_stream() {
        let bytes = [0xFFu8]; // 8 bits, need 14
        let mut r = BitReader::new(&bytes);
        let err = decode_insert_length(&mut r, 22).unwrap_err();
        assert!(
            matches!(
                err,
                BrotliCommandError::Inner(BrotliError::BitReader(
                    crate::brotli_bit_reader::BitReaderError::UnexpectedEof
                ))
            ),
            "expected Inner BitReader UnexpectedEof, got {err:?}"
        );
    }

    /// Sanity: every command symbol 0..=703 decomposes to insert_code ≤ 23
    /// and copy_code ≤ 23 — the decomposition is guaranteed surjective
    /// onto the 24×24 grid for cell_idx >= 2 (and surjective onto a
    /// subset for cell_idx < 2). Locks no out-of-range bug across the
    /// full alphabet.
    #[test]
    fn all_704_command_symbols_decompose_to_valid_codes() {
        for sym in 0..NUM_COMMAND_SYMBOLS {
            let c = decompose_command_code(sym).unwrap();
            assert!(c.insert_code < 24, "sym {sym} → insert_code {}", c.insert_code);
            assert!(c.copy_code < 24, "sym {sym} → copy_code {}", c.copy_code);
            // cell_idx < 2 ⟺ distance_implicit (= 128 symbols)
            let cell_idx = sym >> 6;
            assert_eq!(
                c.distance_implicit,
                cell_idx < 2,
                "sym {sym} distance_implicit mismatch"
            );
        }
    }

    /// Sanity: the (insert_code, copy_code, distance_implicit) tuples
    /// have the expected counts for the 11 cell positions per RFC §5.
    /// Specifically each cell yields 64 symbols. The 11 cells cover
    /// 11 × 64 = 704 total — exactly NUM_COMMAND_SYMBOLS.
    #[test]
    fn cell_count_check() {
        assert_eq!(CELL_POS.len() * 64, NUM_COMMAND_SYMBOLS as usize);
    }
}
