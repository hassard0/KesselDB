//! Sequences section header parser for zstd — **SP132 slice of the
//! OBJ-2c-2 arc**.
//!
//! Authority: RFC 8478 §5.4.1 (Sequences_Section_Header).
//!
//! What this module ships (SP132):
//!
//!   1. **`SeqSymbolMode`** — discriminator for the LL/OF/ML FSE mode
//!      per RFC §5.4.1.2: Predefined / RLE / FseCompressed / Repeat.
//!
//!   2. **`SequencesHeader`** — parsed `Number_of_Sequences` + the 3
//!      mode codes + `header_len` byte count.
//!
//!   3. **`parse_sequences_header`** — 2-to-4-byte header decoder. The
//!      first byte (or first 1-3 of `Number_of_Sequences` VLQ encoding
//!      per RFC §5.4.1.1) gives the sequence count; the next byte is
//!      the Symbol_Compression_Modes (3 × 2 bits + 2 reserved bits per
//!      RFC §5.4.1.2). Reserved bits MUST be zero.
//!
//! Out of scope (deferred to subsequent slices):
//!   - Predefined FSE table constants for LL/OF/ML (RFC §3.1.1.3.2.1.1) →
//!     SP133 paired with the per-mode FSE table loader.
//!   - Sequence bitstream decode (3 interleaved FSE state machines) → SP134.
//!   - Sequence execution (literals copy + back-reference resolution +
//!     repeat-offset slots) → SP135.
//!
//! Determinism: pure transforms over input bytes. Bounds-checked
//! throughout; typed `ZstdError` on every failure; never panics on
//! attacker input.

#![allow(dead_code)]

use crate::zstd::ZstdError;

/// FSE-mode discriminator for one of the 3 sequence-symbol classes
/// (Literal_Lengths / Offsets / Match_Lengths) per RFC §5.4.1.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeqSymbolMode {
    /// Predefined FSE table per RFC §3.1.1.3.2.1.1 (no inline description).
    Predefined = 0,
    /// 1-byte literal RLE — every sequence emits the same value.
    Rle = 1,
    /// FSE table description follows inline (standard FSE encoding).
    FseCompressed = 2,
    /// Reuse the previous block's FSE table for this code.
    Repeat = 3,
}

impl SeqSymbolMode {
    fn from_bits(v: u8) -> Self {
        match v & 0b11 {
            0 => SeqSymbolMode::Predefined,
            1 => SeqSymbolMode::Rle,
            2 => SeqSymbolMode::FseCompressed,
            3 => SeqSymbolMode::Repeat,
            _ => unreachable!(),
        }
    }
}

/// Parsed sequences section header.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SequencesHeader {
    /// Number of sequences in the block. May be 0 (no sequences →
    /// the literals section alone forms the block output).
    pub num_sequences: u32,
    pub ll_mode: SeqSymbolMode,
    pub of_mode: SeqSymbolMode,
    pub ml_mode: SeqSymbolMode,
    /// Number of bytes consumed by the header (= 1, 2, 3, or 4):
    ///   1 if num_sequences = 0 (and NO Symbol_Compression_Modes byte —
    ///     when num_sequences=0 the sequences section ends immediately
    ///     per RFC §5.4.1, no modes byte is encoded).
    ///   2 if num_sequences in 1..=127 (1 VLQ byte + 1 modes byte).
    ///   3 if num_sequences in 128..=32767 (2 VLQ bytes + 1 modes byte).
    ///   4 if num_sequences in 32768..=(2^17 + 32767) (3 VLQ bytes + 1).
    pub header_len: usize,
}

/// Maximum decodable num_sequences per RFC §5.4.1.1 = 2^17 + 32767 - 1
/// (the spec caps the field at this value; values beyond should never
/// appear in valid zstd output).
pub(crate) const MAX_NUM_SEQUENCES: u32 = (1u32 << 17) + 32767;

// ============================================================================
// Predefined FSE tables for LL/OF/ML per RFC 8478 §3.1.1.3.2.1.1.
// ============================================================================

/// Literal Length default normalized distribution — accuracy_log = 6,
/// 36 entries per RFC §3.1.1.3.2.1.1 ("Literal Length Default Distribution").
pub(crate) const LL_DEFAULT_COUNTS: &[i16] = &[
     4,  3,  2,  2,  2,  2,  2,  2,
     2,  2,  2,  2,  2,  1,  1,  1,
     2,  2,  2,  2,  2,  2,  2,  2,
     2,  3,  2,  1,  1,  1,  1,  1,
    -1, -1, -1, -1,
];
pub(crate) const LL_DEFAULT_ACCURACY_LOG: u32 = 6;

/// Offset default normalized distribution — accuracy_log = 5, 28 entries
/// per RFC §3.1.1.3.2.1.1 ("Offset Codes Default Distribution").
pub(crate) const OF_DEFAULT_COUNTS: &[i16] = &[
     1,  1,  1,  1,  1,  1,  2,  2,
     2,  1,  1,  1,  1,  1,  1,  1,
     1,  1,  1,  1,  1,  1,  1, -1,
    -1, -1, -1, -1,
];
pub(crate) const OF_DEFAULT_ACCURACY_LOG: u32 = 5;

/// Match Length default normalized distribution — accuracy_log = 6,
/// 53 entries per RFC §3.1.1.3.2.1.1 ("Match Length Default Distribution").
pub(crate) const ML_DEFAULT_COUNTS: &[i16] = &[
     1,  4,  3,  2,  2,  2,  2,  2,
     2,  1,  1,  1,  1,  1,  1,  1,
     1,  1,  1,  1,  1,  1,  1,  1,
     1,  1,  1,  1,  1,  1,  1,  1,
     1,  1,  1,  1,  1,  1,  1,  1,
     1,  1,  1,  1,  1,  1,  1,  1,
     1,  1, -1, -1, -1,
];
pub(crate) const ML_DEFAULT_ACCURACY_LOG: u32 = 6;

/// Symbol-class discriminator for the predefined-table selection +
/// max-symbol-value bounds per RFC §3.1.1.3.2.1.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeqSymbolClass {
    LiteralLength,
    Offset,
    MatchLength,
}

impl SeqSymbolClass {
    pub fn default_counts(self) -> &'static [i16] {
        match self {
            SeqSymbolClass::LiteralLength => LL_DEFAULT_COUNTS,
            SeqSymbolClass::Offset => OF_DEFAULT_COUNTS,
            SeqSymbolClass::MatchLength => ML_DEFAULT_COUNTS,
        }
    }
    pub fn default_accuracy_log(self) -> u32 {
        match self {
            SeqSymbolClass::LiteralLength => LL_DEFAULT_ACCURACY_LOG,
            SeqSymbolClass::Offset => OF_DEFAULT_ACCURACY_LOG,
            SeqSymbolClass::MatchLength => ML_DEFAULT_ACCURACY_LOG,
        }
    }
    /// Max symbol value (= max FSE alphabet index) for this class —
    /// derived from the predefined table length minus 1.
    pub fn max_symbol_value(self) -> u32 {
        (self.default_counts().len() - 1) as u32
    }
    /// Max accuracy_log permitted for FseCompressed mode per RFC §5.4.2.
    pub fn max_accuracy_log(self) -> u32 {
        match self {
            SeqSymbolClass::LiteralLength => 9,
            SeqSymbolClass::Offset => 8,
            SeqSymbolClass::MatchLength => 9,
        }
    }
}

/// Load the FSE table for one of the 3 sequence symbol classes per
/// `mode` per RFC §5.4.2. Returns the built table + the number of
/// input bytes consumed by the mode's payload (0 for Predefined and
/// Repeat; 1 for Rle; variable for FseCompressed).
///
/// `prev` is the previous block's table for this code; passed `None`
/// for the first sequences-block in a frame. Repeat mode without a
/// previous table → typed err.
pub(crate) fn load_fse_table_for_mode(
    class: SeqSymbolClass,
    mode: SeqSymbolMode,
    input: &[u8],
    prev: Option<&crate::zstd_fse::FseTable>,
) -> Result<(crate::zstd_fse::FseTable, usize), ZstdError> {
    use crate::zstd_fse::{build_fse_table, parse_normalized_counts, ForwardBitReader};
    match mode {
        SeqSymbolMode::Predefined => {
            let table = build_fse_table(class.default_counts(), class.default_accuracy_log())?;
            Ok((table, 0))
        }
        SeqSymbolMode::Rle => {
            if input.is_empty() {
                return Err(ZstdError::UnexpectedEof);
            }
            let sym = input[0];
            if (sym as u32) > class.max_symbol_value() {
                return Err(ZstdError::UnexpectedEof);
            }
            // Build a degenerate 1-entry table with accuracy_log=0; every
            // state lands on this single symbol with nb_bits=0.
            // build_fse_table expects log >= 1 typically; we synthesize the
            // table directly here.
            let table = crate::zstd_fse::FseTable {
                accuracy_log: 0,
                entries: vec![crate::zstd_fse::FseEntry {
                    symbol: sym,
                    nb_bits: 0,
                    base_state: 0,
                }],
            };
            Ok((table, 1))
        }
        SeqSymbolMode::FseCompressed => {
            let mut fr = ForwardBitReader::new(input);
            let normalized = parse_normalized_counts(&mut fr, class.max_symbol_value())?;
            if normalized.accuracy_log > class.max_accuracy_log() {
                return Err(ZstdError::UnexpectedEof);
            }
            let table = build_fse_table(&normalized.counts, normalized.accuracy_log)?;
            let consumed = (fr.bit_pos() + 7) / 8;
            Ok((table, consumed))
        }
        SeqSymbolMode::Repeat => {
            match prev {
                Some(table) => Ok((table.clone(), 0)),
                None => Err(ZstdError::UnexpectedEof),
            }
        }
    }
}

// ============================================================================
// LL / ML baseline + extra-bits tables for value reconstruction per
// RFC §5.4.3 Table 1 (Literal Length code → baseline + extra_bits) and
// Table 2 (Match Length code → baseline + extra_bits).
//
// For Offset, the rule is simpler: offset_value = (1 << code) + read_bits(code)
// (where `code` is itself the FSE-decoded symbol; max code is 31). No const
// table needed.
// ============================================================================

pub(crate) const LL_BASELINES: [u32; 36] = [
    0, 1, 2, 3, 4, 5, 6, 7,
    8, 9, 10, 11, 12, 13, 14, 15,
    16, 18, 20, 22, 24, 28, 32, 40,
    48, 64, 128, 256, 512, 1024, 2048, 4096,
    8192, 16384, 32768, 65536,
];
pub(crate) const LL_EXTRA_BITS: [u32; 36] = [
    0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
    1, 1, 1, 1, 2, 2, 3, 3,
    4, 6, 7, 8, 9, 10, 11, 12,
    13, 14, 15, 16,
];

pub(crate) const ML_BASELINES: [u32; 53] = [
    3, 4, 5, 6, 7, 8, 9, 10,
    11, 12, 13, 14, 15, 16, 17, 18,
    19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31, 32, 33, 34,
    35, 37, 39, 41, 43, 47, 51, 59,
    67, 83, 99, 131, 259, 515, 1027, 2051,
    4099, 8195, 16387, 32771, 65539,
];
pub(crate) const ML_EXTRA_BITS: [u32; 53] = [
    0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
    1, 1, 1, 1, 2, 2, 3, 3,
    4, 4, 5, 7, 8, 9, 10, 11,
    12, 13, 14, 15, 16,
];

/// One decoded sequence per RFC §5.4.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Sequence {
    /// Number of literal bytes to copy from the literals section
    /// BEFORE the back-reference.
    pub literal_length: u32,
    /// Raw offset value (see RFC §5.4.3): 0..=3 → repeat-offset slot
    /// reference; >= 4 → real offset = raw - 3.
    pub offset: u32,
    /// Match length of the back-reference (always >= 3 per spec — but
    /// our decoder propagates whatever the bitstream says; validation
    /// is sequence-execution's job).
    pub match_length: u32,
}

/// Decode `num_sequences` sequences from a reverse bitstream using the
/// 3 FSE tables. RFC §5.4.3 decode order (per sequence):
///
///   1. Read OF_EXTRA_BITS[of_state.symbol] bits → offset extra bits.
///      offset = (1 << of_state.symbol) + extra (since OF code IS the bit width).
///   2. Read ML_EXTRA_BITS[ml_state.symbol] bits → ml extra bits.
///      match_length = ML_BASELINES[ml_state.symbol] + ml_extra.
///   3. Read LL_EXTRA_BITS[ll_state.symbol] bits → ll extra bits.
///      literal_length = LL_BASELINES[ll_state.symbol] + ll_extra.
///   4. For sequences > 0 remaining, step the state machines in order:
///      ll, then ml, then of.
pub(crate) fn decode_sequences_stream(
    input: &[u8],
    ll_table: &crate::zstd_fse::FseTable,
    of_table: &crate::zstd_fse::FseTable,
    ml_table: &crate::zstd_fse::FseTable,
    num_sequences: u32,
) -> Result<Vec<Sequence>, ZstdError> {
    use crate::zstd_fse::{FseState, ReverseBitReader};
    if num_sequences == 0 {
        return Ok(Vec::new());
    }
    let mut rr = ReverseBitReader::new(input)?;
    // Initialize state machines in spec order: LL, then OF, then ML.
    let mut ll_state = FseState::init(ll_table, &mut rr)?;
    let mut of_state = FseState::init(of_table, &mut rr)?;
    let mut ml_state = FseState::init(ml_table, &mut rr)?;

    let mut out: Vec<Sequence> = Vec::with_capacity(num_sequences as usize);
    for seq_idx in 0..num_sequences {
        let ll_sym = ll_state.current_symbol(ll_table);
        let of_sym = of_state.current_symbol(of_table);
        let ml_sym = ml_state.current_symbol(ml_table);
        if (ll_sym as usize) >= LL_BASELINES.len()
            || (ml_sym as usize) >= ML_BASELINES.len()
        {
            return Err(ZstdError::UnexpectedEof);
        }
        if (of_sym as u32) > 31 {
            // Spec bound — offset codes > 31 are invalid (would produce
            // an offset > 2^32 which overflows our u32).
            return Err(ZstdError::UnexpectedEof);
        }
        // Read extra bits per RFC §5.4.3 (offset, then ml, then ll).
        let of_extra = rr.read_bits(of_sym as u32)?;
        let offset = (1u32 << of_sym) + of_extra;
        let ml_extra = rr.read_bits(ML_EXTRA_BITS[ml_sym as usize])?;
        let match_length = ML_BASELINES[ml_sym as usize] + ml_extra;
        let ll_extra = rr.read_bits(LL_EXTRA_BITS[ll_sym as usize])?;
        let literal_length = LL_BASELINES[ll_sym as usize] + ll_extra;
        out.push(Sequence {
            literal_length,
            offset,
            match_length,
        });
        // Don't step after the LAST sequence — RFC §5.4.3.
        if seq_idx + 1 < num_sequences {
            // Step order: LL, then ML, then OF.
            ll_state.step(ll_table, &mut rr)?;
            ml_state.step(ml_table, &mut rr)?;
            of_state.step(of_table, &mut rr)?;
        }
    }
    Ok(out)
}

/// Parse the sequences section header per RFC §5.4.1.
pub(crate) fn parse_sequences_header(input: &[u8]) -> Result<SequencesHeader, ZstdError> {
    if input.is_empty() {
        return Err(ZstdError::UnexpectedEof);
    }
    let b0 = input[0];
    // Number_of_Sequences VLQ per RFC §5.4.1.1:
    //   b0 < 128             : num_sequences = b0;            1-byte VLQ
    //   b0 < 255             : num_sequences = ((b0 - 128) << 8) + b1 + 128;
    //                                                          2-byte VLQ
    //   b0 == 255            : num_sequences = b1 + (b2 << 8) + 0x7F00;
    //                                                          3-byte VLQ
    let (num_sequences, vlq_len) = if b0 < 128 {
        (b0 as u32, 1usize)
    } else if b0 < 255 {
        if input.len() < 2 {
            return Err(ZstdError::UnexpectedEof);
        }
        let n = (((b0 as u32) - 128) << 8) + (input[1] as u32) + 128;
        (n, 2usize)
    } else {
        // b0 == 255
        if input.len() < 3 {
            return Err(ZstdError::UnexpectedEof);
        }
        let n = (input[1] as u32) + ((input[2] as u32) << 8) + 0x7F00;
        (n, 3usize)
    };
    if num_sequences > MAX_NUM_SEQUENCES {
        return Err(ZstdError::UnexpectedEof);
    }
    // Per RFC §5.4.1: when num_sequences == 0 the sequences section ends
    // immediately — NO Symbol_Compression_Modes byte is encoded and the
    // block's output is just the literals section.
    if num_sequences == 0 {
        return Ok(SequencesHeader {
            num_sequences: 0,
            ll_mode: SeqSymbolMode::Predefined,
            of_mode: SeqSymbolMode::Predefined,
            ml_mode: SeqSymbolMode::Predefined,
            header_len: vlq_len,
        });
    }
    // Symbol_Compression_Modes byte at offset `vlq_len`:
    //   bits 7-6 : Literals_Lengths_Mode
    //   bits 5-4 : Offsets_Mode
    //   bits 3-2 : Match_Lengths_Mode
    //   bits 1-0 : Reserved  (must be 0)
    if input.len() < vlq_len + 1 {
        return Err(ZstdError::UnexpectedEof);
    }
    let modes = input[vlq_len];
    if modes & 0b11 != 0 {
        // Reserved bits not zero.
        return Err(ZstdError::UnexpectedEof);
    }
    let ll_mode = SeqSymbolMode::from_bits(modes >> 6);
    let of_mode = SeqSymbolMode::from_bits(modes >> 4);
    let ml_mode = SeqSymbolMode::from_bits(modes >> 2);
    Ok(SequencesHeader {
        num_sequences,
        ll_mode,
        of_mode,
        ml_mode,
        header_len: vlq_len + 1,
    })
}

// ============================================================================
// KATs — hand-derived from RFC 8478 §5.4.1.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// SP132-KAT-1: num_sequences = 0 → 1-byte header, no modes byte.
    #[test]
    fn sp132_kat_num_sequences_zero_one_byte_header() {
        let h = parse_sequences_header(&[0x00u8]).unwrap();
        assert_eq!(h.num_sequences, 0);
        assert_eq!(h.header_len, 1);
        // Default modes (when no modes byte is read) = Predefined for all 3.
        assert_eq!(h.ll_mode, SeqSymbolMode::Predefined);
        assert_eq!(h.of_mode, SeqSymbolMode::Predefined);
        assert_eq!(h.ml_mode, SeqSymbolMode::Predefined);
    }

    /// SP132-KAT-2: num_sequences = 50 (1-byte VLQ) + all-Predefined modes.
    /// b0 = 50 = 0x32. Modes byte = 0b00_00_00_00 = 0x00.
    #[test]
    fn sp132_kat_small_count_predefined_modes() {
        let h = parse_sequences_header(&[0x32u8, 0x00u8]).unwrap();
        assert_eq!(h.num_sequences, 50);
        assert_eq!(h.header_len, 2);
        assert_eq!(h.ll_mode, SeqSymbolMode::Predefined);
        assert_eq!(h.of_mode, SeqSymbolMode::Predefined);
        assert_eq!(h.ml_mode, SeqSymbolMode::Predefined);
    }

    /// SP132-KAT-3: num_sequences = 200 → 2-byte VLQ.
    /// Encoding: b0 = ((200 - 128) >> 8) + 128 = 0 + 128 = 128;
    ///           b1 = (200 - 128) & 0xFF = 72 = 0x48.
    /// Wait that's wrong. Re-derive: per spec
    ///   n = ((b0 - 128) << 8) + b1 + 128
    ///   200 - 128 = 72. So we need (b0 - 128) << 8 + b1 + 128 = 200.
    ///   (b0 - 128) << 8 + b1 = 72.
    ///   With b0 in 128..255: b0 - 128 in 0..127. Smallest: b0 = 128 → 0
    ///   shifted = 0; b1 = 72 = 0x48.
    /// → b0 = 0x80, b1 = 0x48.
    /// Modes = 0x80 (LL=Rle, others=Predefined): bits 7-6 = 0b10 = 2 (FseCompressed!).
    /// Hmm 0b10 = 2 = FseCompressed. Let me use 0x40 = 0b01_00_00_00 →
    /// LL_mode = 0b01 = Rle.
    #[test]
    fn sp132_kat_two_byte_vlq_with_rle_ll_mode() {
        let h = parse_sequences_header(&[0x80u8, 0x48u8, 0x40u8]).unwrap();
        assert_eq!(h.num_sequences, 200);
        assert_eq!(h.header_len, 3);
        assert_eq!(h.ll_mode, SeqSymbolMode::Rle);
        assert_eq!(h.of_mode, SeqSymbolMode::Predefined);
        assert_eq!(h.ml_mode, SeqSymbolMode::Predefined);
    }

    /// SP132-KAT-4: num_sequences = 32767 → upper edge of 2-byte VLQ.
    /// n = ((b0 - 128) << 8) + b1 + 128 = 32767.
    /// → (b0 - 128) << 8 + b1 = 32639 = 0x7F7F.
    /// b0 - 128 = 0x7F → b0 = 0xFF... but b0 = 255 means 3-byte VLQ!
    /// So 2-byte VLQ tops at b0 = 254 → max n = ((254-128) << 8) + 255 + 128
    ///                                       = (126 << 8) + 383 = 32256 + 383 = 32639.
    /// So 32639 is the 2-byte ceiling. Use that.
    /// b0 = 0xFE, b1 = 0xFF. Modes = 0x00 (all Predefined).
    #[test]
    fn sp132_kat_two_byte_vlq_max_value() {
        let h = parse_sequences_header(&[0xFEu8, 0xFFu8, 0x00u8]).unwrap();
        assert_eq!(h.num_sequences, 32639);
        assert_eq!(h.header_len, 3);
    }

    /// SP132-KAT-5: num_sequences = 32640 → smallest 3-byte VLQ.
    /// n = b1 + (b2 << 8) + 0x7F00 = 32640.
    ///   → b1 + (b2 << 8) = 32640 - 32512 = 128.
    ///   → b1 = 128, b2 = 0; OR b1 = 0, b2 = 0... wait 0x7F00 = 32512.
    ///   So b1 + (b2 << 8) = 128 = 0x80. b1 = 0x80, b2 = 0x00.
    /// b0 = 0xFF (3-byte VLQ marker), b1 = 0x80, b2 = 0x00, modes = 0x00.
    #[test]
    fn sp132_kat_three_byte_vlq_min_value() {
        let h = parse_sequences_header(&[0xFFu8, 0x80u8, 0x00u8, 0x00u8]).unwrap();
        assert_eq!(h.num_sequences, 32640);
        assert_eq!(h.header_len, 4);
    }

    /// SP132-KAT-6: all 4 mode codes set.
    /// LL = Rle(1), OF = FseCompressed(2), ML = Repeat(3) →
    /// modes byte = (1<<6) | (2<<4) | (3<<2) | 0 = 0x40 | 0x20 | 0x0C = 0x6C.
    #[test]
    fn sp132_kat_all_four_modes() {
        let h = parse_sequences_header(&[0x05u8, 0x6Cu8]).unwrap();
        assert_eq!(h.num_sequences, 5);
        assert_eq!(h.ll_mode, SeqSymbolMode::Rle);
        assert_eq!(h.of_mode, SeqSymbolMode::FseCompressed);
        assert_eq!(h.ml_mode, SeqSymbolMode::Repeat);
    }

    /// SP132-KAT-7: reserved bits (bits 1-0) non-zero → typed err.
    #[test]
    fn sp132_kat_reserved_bits_set_traps() {
        // modes byte 0x01 = reserved bit set
        assert_eq!(
            parse_sequences_header(&[0x05u8, 0x01u8]).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP132-KAT-8: empty input → typed UnexpectedEof.
    #[test]
    fn sp132_kat_empty_input_traps() {
        assert_eq!(
            parse_sequences_header(&[]).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP132-KAT-9: truncated 2-byte VLQ (b0 = 0x80 = 2-byte marker) but
    /// only 1 byte total → typed err.
    #[test]
    fn sp132_kat_truncated_two_byte_vlq() {
        assert_eq!(
            parse_sequences_header(&[0x80u8]).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP132-KAT-10: truncated 3-byte VLQ (b0 = 0xFF) with only 2 bytes.
    #[test]
    fn sp132_kat_truncated_three_byte_vlq() {
        assert_eq!(
            parse_sequences_header(&[0xFFu8, 0x00u8]).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP132-KAT-11: missing modes byte after valid 1-byte VLQ with
    /// num_sequences > 0 → typed err.
    #[test]
    fn sp132_kat_missing_modes_byte() {
        assert_eq!(
            parse_sequences_header(&[0x05u8]).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP132-KAT-12: deterministic — same input twice → identical output.
    #[test]
    fn sp132_kat_deterministic_repeat() {
        let bytes = [0x05u8, 0x6Cu8];
        let h1 = parse_sequences_header(&bytes).unwrap();
        let h2 = parse_sequences_header(&bytes).unwrap();
        assert_eq!(h1.num_sequences, h2.num_sequences);
        assert_eq!(h1.ll_mode, h2.ll_mode);
        assert_eq!(h1.of_mode, h2.of_mode);
        assert_eq!(h1.ml_mode, h2.ml_mode);
    }

    // ========================================================================
    // SP133 KATs — predefined FSE tables + 4-mode dispatcher.
    // ========================================================================

    /// SP133-KAT-1: predefined LL table sizes correctly.
    /// LL: 36 entries, accuracy_log = 6.
    #[test]
    fn sp133_kat_ll_predefined_table_builds() {
        let (table, consumed) = load_fse_table_for_mode(
            SeqSymbolClass::LiteralLength,
            SeqSymbolMode::Predefined,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(table.accuracy_log, 6);
        assert_eq!(table.entries.len(), 64); // 1 << 6
        assert_eq!(consumed, 0);
    }

    /// SP133-KAT-2: predefined OF table sizes correctly.
    /// OF: 28 entries, accuracy_log = 5.
    #[test]
    fn sp133_kat_of_predefined_table_builds() {
        let (table, consumed) = load_fse_table_for_mode(
            SeqSymbolClass::Offset,
            SeqSymbolMode::Predefined,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(table.accuracy_log, 5);
        assert_eq!(table.entries.len(), 32);
        assert_eq!(consumed, 0);
    }

    /// SP133-KAT-3: predefined ML table sizes correctly.
    /// ML: 53 entries, accuracy_log = 6.
    #[test]
    fn sp133_kat_ml_predefined_table_builds() {
        let (table, consumed) = load_fse_table_for_mode(
            SeqSymbolClass::MatchLength,
            SeqSymbolMode::Predefined,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(table.accuracy_log, 6);
        assert_eq!(table.entries.len(), 64);
        assert_eq!(consumed, 0);
    }

    /// SP133-KAT-4: Rle mode reads 1 byte = the single symbol. The
    /// returned table is degenerate: 1 entry with nb_bits=0.
    #[test]
    fn sp133_kat_rle_mode_one_byte_payload() {
        let (table, consumed) = load_fse_table_for_mode(
            SeqSymbolClass::LiteralLength,
            SeqSymbolMode::Rle,
            &[0x05u8, 0xFF, 0xFF],
            None,
        )
        .unwrap();
        assert_eq!(consumed, 1);
        assert_eq!(table.accuracy_log, 0);
        assert_eq!(table.entries.len(), 1);
        assert_eq!(table.entries[0].symbol, 5);
        assert_eq!(table.entries[0].nb_bits, 0);
    }

    /// SP133-KAT-5: Rle mode rejects out-of-range symbol.
    /// LL max symbol value = 35 (36 entries indexed 0..35). Try sym=100.
    #[test]
    fn sp133_kat_rle_mode_oob_symbol_traps() {
        assert_eq!(
            load_fse_table_for_mode(
                SeqSymbolClass::LiteralLength,
                SeqSymbolMode::Rle,
                &[100u8],
                None,
            )
            .unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP133-KAT-6: Rle mode empty input → typed err.
    #[test]
    fn sp133_kat_rle_mode_empty_input_traps() {
        assert_eq!(
            load_fse_table_for_mode(
                SeqSymbolClass::LiteralLength,
                SeqSymbolMode::Rle,
                &[],
                None,
            )
            .unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP133-KAT-7: Repeat mode without prev table → typed err.
    #[test]
    fn sp133_kat_repeat_without_prev_traps() {
        assert_eq!(
            load_fse_table_for_mode(
                SeqSymbolClass::LiteralLength,
                SeqSymbolMode::Repeat,
                &[],
                None,
            )
            .unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP133-KAT-8: Repeat mode with prev table → clones the prev table.
    #[test]
    fn sp133_kat_repeat_with_prev_clones_table() {
        let (predefined, _) = load_fse_table_for_mode(
            SeqSymbolClass::LiteralLength,
            SeqSymbolMode::Predefined,
            &[],
            None,
        )
        .unwrap();
        let (cloned, consumed) = load_fse_table_for_mode(
            SeqSymbolClass::LiteralLength,
            SeqSymbolMode::Repeat,
            &[],
            Some(&predefined),
        )
        .unwrap();
        assert_eq!(consumed, 0);
        assert_eq!(cloned.accuracy_log, predefined.accuracy_log);
        assert_eq!(cloned.entries.len(), predefined.entries.len());
    }

    /// SP133-KAT-9: deterministic — predefined LL table built twice is
    /// byte-identical.
    #[test]
    fn sp133_kat_predefined_deterministic_repeat() {
        let (t1, _) = load_fse_table_for_mode(
            SeqSymbolClass::LiteralLength,
            SeqSymbolMode::Predefined,
            &[],
            None,
        )
        .unwrap();
        let (t2, _) = load_fse_table_for_mode(
            SeqSymbolClass::LiteralLength,
            SeqSymbolMode::Predefined,
            &[],
            None,
        )
        .unwrap();
        assert_eq!(t1.accuracy_log, t2.accuracy_log);
        assert_eq!(t1.entries, t2.entries);
    }

    /// SP133-KAT-10: class accessors return correct values.
    #[test]
    fn sp133_kat_class_accessors() {
        assert_eq!(SeqSymbolClass::LiteralLength.max_symbol_value(), 35);
        assert_eq!(SeqSymbolClass::Offset.max_symbol_value(), 27);
        assert_eq!(SeqSymbolClass::MatchLength.max_symbol_value(), 52);
        assert_eq!(SeqSymbolClass::LiteralLength.default_accuracy_log(), 6);
        assert_eq!(SeqSymbolClass::Offset.default_accuracy_log(), 5);
        assert_eq!(SeqSymbolClass::MatchLength.default_accuracy_log(), 6);
    }

    // ========================================================================
    // SP134 KATs — 3-interleaved sequence stream decoder.
    // ========================================================================

    /// SP134-KAT-1: num_sequences = 0 → empty output without touching bitstream.
    #[test]
    fn sp134_kat_zero_sequences_yields_empty() {
        let (ll, _) = load_fse_table_for_mode(SeqSymbolClass::LiteralLength, SeqSymbolMode::Predefined, &[], None).unwrap();
        let (of, _) = load_fse_table_for_mode(SeqSymbolClass::Offset, SeqSymbolMode::Predefined, &[], None).unwrap();
        let (ml, _) = load_fse_table_for_mode(SeqSymbolClass::MatchLength, SeqSymbolMode::Predefined, &[], None).unwrap();
        let out = decode_sequences_stream(&[], &ll, &of, &ml, 0).unwrap();
        assert!(out.is_empty());
    }

    /// SP134-KAT-2: empty input with num_sequences > 0 → typed err.
    #[test]
    fn sp134_kat_empty_input_with_sequences_traps() {
        let (ll, _) = load_fse_table_for_mode(SeqSymbolClass::LiteralLength, SeqSymbolMode::Predefined, &[], None).unwrap();
        let (of, _) = load_fse_table_for_mode(SeqSymbolClass::Offset, SeqSymbolMode::Predefined, &[], None).unwrap();
        let (ml, _) = load_fse_table_for_mode(SeqSymbolClass::MatchLength, SeqSymbolMode::Predefined, &[], None).unwrap();
        assert_eq!(
            decode_sequences_stream(&[], &ll, &of, &ml, 1).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP134-KAT-3: insufficient bits for state machine init → typed err.
    /// Predefined LL/ML need accuracy_log=6 = 6 bits per init; OF
    /// accuracy_log=5 = 5 bits. Total 17 bits for 3 inits + more for
    /// extra-bits reads. A 1-byte payload [0x80] has pad_bit=7 → 7
    /// payload bits, insufficient for 17-bit init.
    #[test]
    fn sp134_kat_insufficient_init_bits_traps() {
        let (ll, _) = load_fse_table_for_mode(SeqSymbolClass::LiteralLength, SeqSymbolMode::Predefined, &[], None).unwrap();
        let (of, _) = load_fse_table_for_mode(SeqSymbolClass::Offset, SeqSymbolMode::Predefined, &[], None).unwrap();
        let (ml, _) = load_fse_table_for_mode(SeqSymbolClass::MatchLength, SeqSymbolMode::Predefined, &[], None).unwrap();
        assert_eq!(
            decode_sequences_stream(&[0x80u8], &ll, &of, &ml, 1).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP134-KAT-4: LL/ML baseline + extra_bits tables are well-formed.
    /// The baseline of code N == baseline of N-1 + 2^(extra_bits[N-1])
    /// for the "predictable" portion (codes 16+ for LL, 32+ for ML where
    /// extra_bits transition from 0 to non-zero). Sanity-check known
    /// values from RFC §5.4.3 Table 1:
    ///   LL baseline[16] = 16, extra_bits = 1
    ///   LL baseline[20] = 24, extra_bits = 2
    ///   LL baseline[35] = 65536, extra_bits = 16 → max_ll = 65536 + (2^16 - 1) = 131071
    /// And RFC §5.4.3 Table 2:
    ///   ML baseline[32] = 35, extra_bits = 1
    ///   ML baseline[52] = 65539, extra_bits = 16 → max_ml = 65539 + 65535 = 131074
    #[test]
    fn sp134_kat_baseline_extra_bits_tables_correct() {
        assert_eq!(LL_BASELINES[16], 16);
        assert_eq!(LL_EXTRA_BITS[16], 1);
        assert_eq!(LL_BASELINES[20], 24);
        assert_eq!(LL_EXTRA_BITS[20], 2);
        assert_eq!(LL_BASELINES[35], 65536);
        assert_eq!(LL_EXTRA_BITS[35], 16);
        assert_eq!(ML_BASELINES[32], 35);
        assert_eq!(ML_EXTRA_BITS[32], 1);
        assert_eq!(ML_BASELINES[52], 65539);
        assert_eq!(ML_EXTRA_BITS[52], 16);
    }

    /// SP134-KAT-5: deterministic — same input + same tables → same output.
    /// Use ABUNDANT bits (8 bytes of 0x80...) — even if decode produces
    /// "garbage" sequences (we didn't hand-craft a valid sequence stream),
    /// both runs MUST produce the same output. Either both Err or both
    /// Ok with identical Vec<Sequence>.
    #[test]
    fn sp134_kat_deterministic_repeat() {
        let (ll, _) = load_fse_table_for_mode(SeqSymbolClass::LiteralLength, SeqSymbolMode::Predefined, &[], None).unwrap();
        let (of, _) = load_fse_table_for_mode(SeqSymbolClass::Offset, SeqSymbolMode::Predefined, &[], None).unwrap();
        let (ml, _) = load_fse_table_for_mode(SeqSymbolClass::MatchLength, SeqSymbolMode::Predefined, &[], None).unwrap();
        let bytes = [0xFFu8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x80];
        let r1 = decode_sequences_stream(&bytes, &ll, &of, &ml, 1);
        let r2 = decode_sequences_stream(&bytes, &ll, &of, &ml, 1);
        assert_eq!(r1.is_ok(), r2.is_ok());
        if let (Ok(s1), Ok(s2)) = (&r1, &r2) {
            assert_eq!(s1, s2);
        }
    }
}
