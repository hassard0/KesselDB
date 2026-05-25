//! Huffman tree decoder for zstd literals — **SP128 slice of the OBJ-2c-2 arc**.
//!
//! Authority: RFC 8478 §4.2.1 (Huffman_Tree_Description) + the upstream
//! `facebook/zstd` educational decoder cross-checked at
//! `educational_decoder/zstd_decompress.c::HUF_init_dtable`.
//!
//! What this module ships (SP128):
//!
//!   1. **Direct-weight header parsing** (header byte 128..=255): the
//!      header byte minus 127 = `number_of_symbols`; weights packed
//!      2-per-byte (4-bit nibbles, HIGH nibble first per RFC §4.2.1.1).
//!
//!   2. **Max_Number_of_Bits + implicit-weight derivation**. The
//!      canonical zstd invariant (per the educational decoder) is:
//!
//!        `Σ 2^(weight - 1) over all weights > 0 = 2^Max_Number_of_Bits`
//!
//!      The implicit last weight fills the gap so the total sum is a
//!      power of two. When the explicit `Σ 2^(weight-1)` is already a
//!      power of two, `Max_Number_of_Bits` is bumped by 1 so the
//!      implicit weight is non-zero.
//!
//!      (NOTE: RFC 8478's spec text writes `Σ 2^weight = 2^max_bits`.
//!      That phrasing is OFF-BY-ONE relative to the reference
//!      implementation; the educational decoder + libzstd both use
//!      `2^(weight-1)`. Using the RFC's literal phrasing leads to a
//!      Kraft sum of 1/2 — an under-subscribed tree. We use the
//!      implementation-correct convention here.)
//!
//!   3. **Canonical code construction** (RFC §4.2.2): per symbol,
//!      `number_of_bits = max_bits + 1 - weight` if `weight > 0`, else 0
//!      (symbol absent). Codes assigned in ascending (length, symbol)
//!      order so each code occupies `1 << (max_bits - number_of_bits)`
//!      consecutive slots of the lookup table.
//!
//!   4. **Decode lookup table**: `1 << max_bits` entries; each entry
//!      holds `(symbol, bits_consumed)` so the bitstream decoder
//!      (SP129) can read `max_bits` bits, index the table, emit the
//!      symbol, advance the stream by `bits_consumed`.
//!
//! Scope cleanly bounded — the **FSE-weight tree path** (header byte
//! 0..=127, two interleaved FSE state machines decoding the weights
//! from a reverse bitstream) defers to **SP129** where it ships paired
//! with the Huffman bitstream decoder + Compressed/Treeless literal
//! payload decode. `parse_huffman_tree` traps FSE-weight headers with
//! the typed `ZstdError::FseWeightHuffmanNotYetSupported` so the
//! deferred-scope boundary is inspectable.
//!
//! Determinism: pure transforms over input bytes. Bounds-checked
//! throughout — typed errors, no panics on attacker bytes.

#![allow(dead_code)]

use crate::zstd::ZstdError;

/// Weights are 0..=11; code lengths are 1..=12 (`max_bits + 1 - weight`
/// with `weight >= 1` and `max_bits <= 11`).
pub(crate) const MAX_HUFFMAN_BITS: u32 = 11;
pub(crate) const MAX_HUFFMAN_SYMBOLS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HuffmanEntry {
    pub symbol: u8,
    pub bits: u8,
}

#[derive(Debug, Clone)]
pub(crate) struct HuffmanTree {
    pub max_bits: u32,
    pub num_symbols: usize,
    pub bits_per_symbol: Vec<u8>,
    pub decode_table: Vec<HuffmanEntry>,
}

/// Parse a Huffman tree description from `input` per RFC 8478 §4.2.1.
/// Returns the built tree + the number of input bytes consumed.
pub(crate) fn parse_huffman_tree(input: &[u8]) -> Result<(HuffmanTree, usize), ZstdError> {
    if input.is_empty() {
        return Err(ZstdError::UnexpectedEof);
    }
    let header_byte = input[0];
    if header_byte < 128 {
        // FSE-weight path — RFC §4.2.1.1.
        return parse_fse_weight_huffman_tree(input);
    }

    // Direct-weight path.
    let number_of_symbols = (header_byte - 127) as usize;
    if number_of_symbols == 0 || number_of_symbols > MAX_HUFFMAN_SYMBOLS - 1 {
        // The implicit last symbol brings the total to (number_of_symbols + 1),
        // which must be <= 256.
        return Err(ZstdError::UnexpectedEof);
    }
    let weight_bytes = (number_of_symbols + 1) / 2;
    let total_consumed = 1 + weight_bytes;
    if input.len() < total_consumed {
        return Err(ZstdError::UnexpectedEof);
    }
    let mut weights: Vec<u8> = Vec::with_capacity(number_of_symbols + 1);
    for i in 0..number_of_symbols {
        let byte = input[1 + i / 2];
        let nibble = if i % 2 == 0 {
            (byte >> 4) & 0x0F
        } else {
            byte & 0x0F
        };
        if nibble as u32 > MAX_HUFFMAN_BITS {
            return Err(ZstdError::UnexpectedEof);
        }
        weights.push(nibble);
    }
    let (weights, max_bits) = compute_last_weight_and_max_bits(weights)?;
    let tree = build_huffman_tree_from_weights(&weights, max_bits)?;
    Ok((tree, total_consumed))
}

/// FSE-weight Huffman tree decoder per RFC 8478 §4.2.1.1 (the path where
/// the header byte is in `0..=127` and is interpreted as the byte length
/// of the FSE-encoded weight stream that follows).
///
/// The weight stream layout:
///   bytes 0..1                : FSE table description start (forward LSB-first)
///   bytes ... up to N         : FSE normalized counts (variable bit width)
///   byte N (byte-aligned)..end: REVERSE bitstream of FSE-state-decoded weights
///
/// Two FSE state machines (state1 + state2) alternately decode weight
/// symbols from the reverse bitstream. The decoded sequence of weights
/// — terminated when the reverse bitstream is exhausted — feeds into
/// the same canonical-code construction as the direct-weight path.
fn parse_fse_weight_huffman_tree(input: &[u8]) -> Result<(HuffmanTree, usize), ZstdError> {
    use crate::zstd_fse::{
        build_fse_table, parse_normalized_counts, FseState, ForwardBitReader, ReverseBitReader,
    };
    if input.is_empty() {
        return Err(ZstdError::UnexpectedEof);
    }
    let header_byte = input[0];
    if header_byte >= 128 {
        return Err(ZstdError::UnexpectedEof);
    }
    let compressed_weights_size = header_byte as usize;
    let total_consumed = 1 + compressed_weights_size;
    if input.len() < total_consumed {
        return Err(ZstdError::UnexpectedEof);
    }
    let weight_stream = &input[1..total_consumed];

    // Parse the FSE table description from the FORWARD bit stream at the
    // start of weight_stream. max_symbol_value = MAX_HUFFMAN_BITS (11)
    // because the FSE alphabet here is the weight values 0..=11.
    let mut fr = ForwardBitReader::new(weight_stream);
    let normalized = parse_normalized_counts(&mut fr, MAX_HUFFMAN_BITS)?;
    // RFC §4.2.1.1: FSE-weight accuracy_log is 5 or 6.
    if normalized.accuracy_log < 5 || normalized.accuracy_log > 6 {
        return Err(ZstdError::UnexpectedEof);
    }
    let fse_table = build_fse_table(&normalized.counts, normalized.accuracy_log)?;

    // The bytes AFTER the FSE table description (byte-aligned) form the
    // reverse bitstream. The forward bit reader's bit_pos has been
    // aligned to the next byte by parse_normalized_counts.
    let table_end_byte = fr.bit_pos() / 8;
    if table_end_byte > weight_stream.len() {
        return Err(ZstdError::UnexpectedEof);
    }
    let reverse_payload = &weight_stream[table_end_byte..];
    let mut rr = ReverseBitReader::new(reverse_payload)?;

    // Initialize the two state machines.
    let mut state1 = FseState::init(&fse_table, &mut rr)?;
    let mut state2 = FseState::init(&fse_table, &mut rr)?;

    // Decode weights via the two-interleaved FSE pattern. Cap at
    // MAX_HUFFMAN_SYMBOLS - 1 (the implicit last weight is added by
    // compute_last_weight_and_max_bits).
    let mut weights: Vec<u8> = Vec::new();
    let mut emit_state1_next = true;
    loop {
        // Emit current state's symbol.
        let sym = if emit_state1_next {
            state1.current_symbol(&fse_table)
        } else {
            state2.current_symbol(&fse_table)
        };
        weights.push(sym);
        if weights.len() >= MAX_HUFFMAN_SYMBOLS - 1 {
            // Can't fit any more explicit weights (the implicit last
            // weight would push to MAX_HUFFMAN_SYMBOLS).
            break;
        }
        // Try to step the current state's machine. If insufficient bits,
        // we're done (the current symbol was the last one to emit).
        let entry = if emit_state1_next {
            state1.current_entry(&fse_table)
        } else {
            state2.current_entry(&fse_table)
        };
        if rr.remaining() < entry.nb_bits as usize {
            break;
        }
        if emit_state1_next {
            state1.step(&fse_table, &mut rr)?;
        } else {
            state2.step(&fse_table, &mut rr)?;
        }
        emit_state1_next = !emit_state1_next;
    }

    let (weights, max_bits) = compute_last_weight_and_max_bits(weights)?;
    let tree = build_huffman_tree_from_weights(&weights, max_bits)?;
    Ok((tree, total_consumed))
}

/// Append the implicit last weight + derive `Max_Number_of_Bits`.
///
/// Per the libzstd educational decoder convention: sum
/// `Σ 2^(weight - 1)` over the EXPLICIT non-zero weights; choose
/// `max_bits` such that the implicit weight's contribution
/// `2^(last_weight - 1) = 2^max_bits - sum` is a valid power of two
/// > 0. When `sum` is already a power of two we bump `max_bits` by 1
/// so the implicit weight is `log2(sum) + 1` (the implicit symbol
/// duplicates the sum).
fn compute_last_weight_and_max_bits(mut weights: Vec<u8>) -> Result<(Vec<u8>, u32), ZstdError> {
    let mut sum: u64 = 0;
    for &w in &weights {
        if w == 0 {
            continue;
        }
        if w as u32 > MAX_HUFFMAN_BITS {
            return Err(ZstdError::UnexpectedEof);
        }
        sum = sum.saturating_add(1u64 << (w - 1));
    }
    if sum == 0 {
        return Err(ZstdError::UnexpectedEof);
    }
    // max_bits per the implementation convention:
    //   if sum is a power of 2: max_bits = log2(sum) + 1
    //   else:                   max_bits = ceil(log2(sum))
    let log2_floor = 63 - sum.leading_zeros() as u32;
    let max_bits = if sum.is_power_of_two() {
        log2_floor + 1
    } else {
        log2_floor + 1 // ceil(log2(sum)) when sum is not a power of 2
    };
    if max_bits > MAX_HUFFMAN_BITS {
        return Err(ZstdError::UnexpectedEof);
    }
    let total = 1u64 << max_bits;
    let missing = total - sum;
    if missing == 0 || !missing.is_power_of_two() {
        // Implicit-weight slot must be a positive power of two.
        return Err(ZstdError::UnexpectedEof);
    }
    let last_weight_minus_1 = 63 - missing.leading_zeros() as u32;
    let last_weight = last_weight_minus_1 + 1;
    if last_weight > MAX_HUFFMAN_BITS {
        return Err(ZstdError::UnexpectedEof);
    }
    weights.push(last_weight as u8);
    Ok((weights, max_bits))
}

/// Build the Huffman decode lookup table given per-symbol weights +
/// the derived `max_bits` (= `Max_Number_of_Bits`).
fn build_huffman_tree_from_weights(weights: &[u8], max_bits: u32) -> Result<HuffmanTree, ZstdError> {
    if weights.is_empty() || max_bits == 0 || max_bits > MAX_HUFFMAN_BITS {
        return Err(ZstdError::UnexpectedEof);
    }
    let bits_per_symbol: Vec<u8> = weights
        .iter()
        .map(|&w| if w == 0 { 0u8 } else { max_bits as u8 + 1 - w })
        .collect();

    let table_size = 1usize << max_bits;
    let mut decode_table = vec![HuffmanEntry { symbol: 0, bits: 0 }; table_size];

    // Group symbols by code length so canonical assignment walks
    // (length ASC, symbol ASC).
    let mut symbols_by_length: Vec<Vec<u8>> = vec![Vec::new(); (max_bits + 2) as usize];
    for (sym, &nb) in bits_per_symbol.iter().enumerate() {
        if nb > 0 {
            if sym >= MAX_HUFFMAN_SYMBOLS {
                return Err(ZstdError::UnexpectedEof);
            }
            symbols_by_length[nb as usize].push(sym as u8);
        }
    }

    let mut next_code: u32 = 0;
    for nb in 1..=max_bits {
        let nb_us = nb as usize;
        if nb_us >= symbols_by_length.len() {
            continue;
        }
        for &sym in &symbols_by_length[nb_us] {
            let slots = 1u32 << (max_bits - nb);
            let start = (next_code * slots) as usize;
            let end = start + slots as usize;
            if end > table_size {
                return Err(ZstdError::UnexpectedEof);
            }
            for slot in start..end {
                decode_table[slot] = HuffmanEntry { symbol: sym, bits: nb as u8 };
            }
            next_code += 1;
        }
        next_code <<= 1;
    }

    Ok(HuffmanTree {
        max_bits,
        num_symbols: weights.len(),
        bits_per_symbol,
        decode_table,
    })
}

// ============================================================================
// KATs — derived from the libzstd educational decoder convention.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// SP128-KAT-1: FSE-weight header (byte < 128) routes to the
    /// FSE-weight parser (SP131). With a single-byte input declaring
    /// compressed_weights_size=5 but no following bytes, the parser
    /// traps with typed UnexpectedEof.
    #[test]
    fn sp128_kat_fse_weight_header_truncated_traps() {
        assert_eq!(
            parse_huffman_tree(&[0x05u8]).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP128-KAT-2: empty input → typed UnexpectedEof.
    #[test]
    fn sp128_kat_empty_input_traps() {
        assert_eq!(parse_huffman_tree(&[]).unwrap_err(), ZstdError::UnexpectedEof);
    }

    /// SP128-KAT-3: single explicit weight = 1 → implicit last = 1.
    /// header_byte = 128 (1 explicit). weight[0] = 1 → high nibble of
    /// weight byte = 1, low nibble unused = 0. Weight byte = 0x10.
    /// Σ 2^(w-1) for explicit = 2^0 = 1. sum=1 is pow2 → max_bits =
    /// log2(1) + 1 = 1. total = 2^1 = 2. missing = 2 - 1 = 1 = 2^0 →
    /// last_weight = 0 + 1 = 1. Final weights = [1, 1]. max_bits = 1.
    /// Both syms get nb = 1 + 1 - 1 = 1. table size = 2. Codes: sym 0 → "0",
    /// sym 1 → "1"; each occupies 1 slot.
    #[test]
    fn sp128_kat_single_explicit_weight_one() {
        let (tree, consumed) = parse_huffman_tree(&[0x80u8, 0x10u8]).unwrap();
        assert_eq!(consumed, 2);
        assert_eq!(tree.max_bits, 1);
        assert_eq!(tree.num_symbols, 2);
        assert_eq!(tree.bits_per_symbol, vec![1u8, 1u8]);
        assert_eq!(tree.decode_table.len(), 2);
        // canonical: sym 0 at slot 0, sym 1 at slot 1.
        assert_eq!(tree.decode_table[0], HuffmanEntry { symbol: 0, bits: 1 });
        assert_eq!(tree.decode_table[1], HuffmanEntry { symbol: 1, bits: 1 });
    }

    /// SP128-KAT-4: 3 explicit weights all = 1 → implicit = 1; 4-sym
    /// uniform 2-bit tree.
    /// header_byte = 130 (3 explicit). 3 weights all = 1 packed:
    /// byte[1] = 0x11 (high=1, low=1); byte[2] = 0x10 (high=1, low=unused).
    /// Σ 2^(w-1) explicit = 1+1+1 = 3 (not pow2). max_bits = ceil(log2(3)) = 2.
    /// missing = 4 - 3 = 1 = 2^0 → last_weight = 1. Final 4 weights all = 1.
    /// All nb = 2 + 1 - 1 = 2. table size = 4. Each code occupies 1 slot.
    /// Canonical: sym 0 → "00" (slot 0); sym 1 → "01" (slot 1); sym 2 → "10"
    /// (slot 2); sym 3 → "11" (slot 3).
    #[test]
    fn sp128_kat_three_explicit_uniform_weights() {
        let (tree, consumed) = parse_huffman_tree(&[0x82u8, 0x11u8, 0x10u8]).unwrap();
        assert_eq!(consumed, 3);
        assert_eq!(tree.max_bits, 2);
        assert_eq!(tree.num_symbols, 4);
        assert_eq!(tree.bits_per_symbol, vec![1u8, 1u8, 1u8, 1u8]
            .iter().map(|&_| 2u8).collect::<Vec<_>>());
        assert_eq!(tree.decode_table.len(), 4);
        for (slot, expected_sym) in [(0, 0), (1, 1), (2, 2), (3, 3)] {
            assert_eq!(
                tree.decode_table[slot],
                HuffmanEntry { symbol: expected_sym, bits: 2 },
                "slot {slot}"
            );
        }
    }

    /// SP128-KAT-5: skewed distribution — explicit [2, 1, 1] + implicit.
    /// Σ 2^(w-1) = 2^1 + 2^0 + 2^0 = 4 (pow2) → max_bits = log2(4) + 1 = 3.
    /// missing = 8 - 4 = 4 = 2^2 → last_weight = 3.
    /// Final weights = [2, 1, 1, 3].
    /// nb = max_bits + 1 - w = [3+1-2, 3+1-1, 3+1-1, 3+1-3] = [2, 3, 3, 1].
    /// table size = 8.
    /// Canonical assignment (length ASC, symbol ASC):
    ///   length 1: sym 3 → "0" → 4 slots (slots 0..3) (1 code × 2^2 = 4 slots).
    ///   length 2: sym 0 → "10" → 2 slots (slots 4..5).
    ///   length 3: sym 1 → "110" → 1 slot (slot 6).
    ///   length 3: sym 2 → "111" → 1 slot (slot 7).
    /// Total slots = 4 + 2 + 1 + 1 = 8 = table size. ✓
    ///
    /// Header byte = 130 (3 explicit); 3 weights = [2, 1, 1] packed:
    /// byte[1] = 0x21 (high=2 low=1); byte[2] = 0x10 (high=1 low=unused).
    #[test]
    fn sp128_kat_skewed_distribution() {
        let (tree, consumed) = parse_huffman_tree(&[0x82u8, 0x21u8, 0x10u8]).unwrap();
        assert_eq!(consumed, 3);
        assert_eq!(tree.max_bits, 3);
        assert_eq!(tree.num_symbols, 4);
        assert_eq!(tree.bits_per_symbol, vec![2u8, 3u8, 3u8, 1u8]);
        assert_eq!(tree.decode_table.len(), 8);
        // length 1: sym 3 → "0..." → slots 0..3 all sym 3.
        for slot in 0..4 {
            assert_eq!(tree.decode_table[slot], HuffmanEntry { symbol: 3, bits: 1 });
        }
        // length 2: sym 0 → "10." → slots 4..5 = sym 0.
        for slot in 4..6 {
            assert_eq!(tree.decode_table[slot], HuffmanEntry { symbol: 0, bits: 2 });
        }
        // length 3: sym 1 → "110" → slot 6.
        assert_eq!(tree.decode_table[6], HuffmanEntry { symbol: 1, bits: 3 });
        // length 3: sym 2 → "111" → slot 7.
        assert_eq!(tree.decode_table[7], HuffmanEntry { symbol: 2, bits: 3 });
    }

    /// SP128-KAT-6: deterministic — same input twice → identical tree.
    /// Uses a direct-weight header (0x82 = 130) so the result is a
    /// well-defined Huffman tree, not a deferred trap.
    #[test]
    fn sp128_kat_deterministic_repeat() {
        let r1 = parse_huffman_tree(&[0x82u8, 0x21u8, 0x10u8]).unwrap().0;
        let r2 = parse_huffman_tree(&[0x82u8, 0x21u8, 0x10u8]).unwrap().0;
        assert_eq!(r1.max_bits, r2.max_bits);
        assert_eq!(r1.bits_per_symbol, r2.bits_per_symbol);
        assert_eq!(r1.decode_table, r2.decode_table);
    }

    /// SP128-KAT-7: truncated direct-weight tree → typed err.
    /// header_byte = 130 (3 explicit syms) needs 1 + 2 = 3 bytes; give only header.
    #[test]
    fn sp128_kat_direct_weight_truncated_traps() {
        assert_eq!(
            parse_huffman_tree(&[0x82u8]).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP128-KAT-8: direct-weight with out-of-range weight (> 11) traps.
    /// header_byte = 128 (1 explicit); weight nibble = 0xC = 12.
    #[test]
    fn sp128_kat_direct_weight_out_of_range_traps() {
        assert_eq!(
            parse_huffman_tree(&[0x80u8, 0xC0u8]).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP128-KAT-9: explicit sum with no valid implicit slot → typed err.
    /// Construct weights such that 2^max_bits - sum is NOT a power of two.
    /// 3 explicit weights = [1, 2, 1]: Σ 2^(w-1) = 1+2+1 = 4 (pow2)
    /// → max_bits = 3, missing = 4 = 2^2 → last_weight = 3 (valid).
    /// To get a NON-pow-2 missing: 3 weights = [2, 2, 1]:
    /// Σ = 2+2+1 = 5 (not pow2); max_bits = 3; missing = 8-5 = 3.
    /// 3 is not a power of 2 → REJECT.
    /// header_byte = 130; weights packed: byte[1]=0x22, byte[2]=0x10.
    #[test]
    fn sp128_kat_invalid_missing_not_power_of_two_traps() {
        assert_eq!(
            parse_huffman_tree(&[0x82u8, 0x22u8, 0x10u8]).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    // ========================================================================
    // SP131 KATs — FSE-weight Huffman tree path.
    // ========================================================================

    /// SP131-KAT-1: FSE-weight header byte 0 = compressed_weights_size 0
    /// → empty weight stream → trap (no FSE table description bytes).
    #[test]
    fn sp131_kat_fse_weight_zero_compressed_size_traps() {
        // header_byte = 0; no following bytes needed but the parser
        // must report a malformed/empty FSE table.
        assert_eq!(
            parse_huffman_tree(&[0x00u8]).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP131-KAT-2: FSE-weight declared compressed_weights_size = 5 but
    /// only 1 byte (header) provided → truncated trap.
    #[test]
    fn sp131_kat_fse_weight_declared_size_overruns_input() {
        assert_eq!(
            parse_huffman_tree(&[0x05u8]).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP131-KAT-3: FSE-weight declared compressed_weights_size = 3 with
    /// 3 follower bytes — exercises the code path through to FSE table
    /// parsing. With 3 random bytes the FSE table description will
    /// almost certainly fail validation (accuracy_log out of range or
    /// normalized counts inconsistent); we just assert it doesn't
    /// PANIC and returns a typed error.
    #[test]
    fn sp131_kat_fse_weight_invalid_table_returns_typed_err() {
        let bytes = [0x03u8, 0x00, 0x00, 0x00];
        match parse_huffman_tree(&bytes) {
            Err(_) => {} // any typed err is fine — no panic is the assertion
            Ok(_) => panic!("expected error from garbage FSE-weight table"),
        }
    }

    /// SP131-KAT-4: FSE-weight path is DETERMINISTIC — same bytes twice
    /// → same Result variant (both error in the same way).
    #[test]
    fn sp131_kat_fse_weight_deterministic_repeat() {
        let bytes = [0x05u8, 0x11, 0x22, 0x33, 0x44, 0x55];
        let r1 = parse_huffman_tree(&bytes);
        let r2 = parse_huffman_tree(&bytes);
        assert_eq!(r1.is_err(), r2.is_err());
        if let (Err(e1), Err(e2)) = (&r1, &r2) {
            assert_eq!(format!("{e1:?}"), format!("{e2:?}"));
        }
    }

    /// SP128-KAT-10: tree contains 0-weight (absent) symbol.
    /// 3 explicit weights = [1, 0, 1]: Σ 2^(w-1) (skipping 0) = 1+1 = 2 (pow2)
    /// → max_bits = 2, missing = 2 = 2^1 → last_weight = 2.
    /// Final weights = [1, 0, 1, 2]. nb = [2+1-1, 0, 2+1-1, 2+1-2] = [2, 0, 2, 1].
    /// table size = 4. Canonical:
    ///   length 1: sym 3 → "0." → 2 slots (0..1).
    ///   length 2: sym 0 → "10" → 1 slot (2).
    ///   length 2: sym 2 → "11" → 1 slot (3).
    /// header byte=130; weights packed: byte[1]=0x10 (1,0), byte[2]=0x10 (1, unused).
    #[test]
    fn sp128_kat_weight_zero_absent_symbol() {
        let (tree, _) = parse_huffman_tree(&[0x82u8, 0x10u8, 0x10u8]).unwrap();
        assert_eq!(tree.max_bits, 2);
        assert_eq!(tree.bits_per_symbol, vec![2u8, 0u8, 2u8, 1u8]);
        assert_eq!(tree.decode_table[0], HuffmanEntry { symbol: 3, bits: 1 });
        assert_eq!(tree.decode_table[1], HuffmanEntry { symbol: 3, bits: 1 });
        assert_eq!(tree.decode_table[2], HuffmanEntry { symbol: 0, bits: 2 });
        assert_eq!(tree.decode_table[3], HuffmanEntry { symbol: 2, bits: 2 });
    }
}
