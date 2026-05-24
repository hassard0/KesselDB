//! Single-stream Huffman bitstream decoder + Compressed-literals payload
//! decoder for zstd — **SP129 slice of the OBJ-2c-2 arc**.
//!
//! Authority: RFC 8478 §4.2.2 (Huffman Coded Streams) + §5.3.4
//! (Huffman Compressed Literals).
//!
//! What this module ships (SP129):
//!
//!   1. **`decode_huffman_stream`** — single-stream Huffman decoder. Reads
//!      from a reverse MSB-first bitstream (the SP126 `ReverseBitReader`),
//!      indexes the SP128 `HuffmanTree::decode_table` by the next
//!      `max_bits` bits, emits the entry's symbol, advances the stream
//!      by `entry.bits` bits, repeats until the stream is exhausted or
//!      `regenerated_size` symbols have been emitted.
//!
//!   2. **`decode_compressed_literals_single_stream`** — wraps SP127's
//!      literals-header parsing + SP128's tree decode + the bitstream
//!      decoder. Handles size_format=00 (Compressed, 1 stream) only —
//!      the 4-stream jump-table path defers to SP130.
//!
//! Scope cleanly bounded:
//!   - 4-stream Huffman (size_format ∈ {01, 10, 11}) → SP130.
//!   - FSE-weight Huffman tree (header byte 0..=127) → SP131 (still traps
//!     with `FseWeightHuffmanNotYetSupported` from SP128).
//!   - Treeless literal mode (block_type=3) → SP132.
//!
//! Determinism: pure transforms over input bytes. Bounds-checked
//! throughout — typed errors propagate; no panics on attacker bytes.

#![allow(dead_code)]

use crate::zstd::ZstdError;
use crate::zstd_fse::ReverseBitReader;
use crate::zstd_huffman::{parse_huffman_tree, HuffmanTree};
use crate::zstd_literals::{parse_literals_header, LiteralsBlockType, LITERALS_MAX_SIZE};

/// Decode a single Huffman bitstream from `payload` into a `Vec<u8>` of
/// `regenerated_size` symbols using `tree`.
///
/// The payload's LAST byte holds the padding marker (highest set bit);
/// payload bytes are consumed in REVERSE (from the last byte backward,
/// MSB-first within each byte). Reads `tree.max_bits` bits, indexes the
/// `decode_table`, emits the symbol, advances by `entry.bits`.
///
/// The loop terminates when either `regenerated_size` symbols have been
/// emitted OR the bitstream has fewer than `tree.max_bits` payload bits
/// remaining. For the final few symbols where remaining bits <
/// `tree.max_bits`, we pad the index with zeros (RFC §4.2.2: "the
/// missing bits are assumed to be zero, which is the canonical Huffman
/// invariant that shorter codes decode correctly with trailing zero
/// padding").
pub(crate) fn decode_huffman_stream(
    payload: &[u8],
    tree: &HuffmanTree,
    regenerated_size: usize,
) -> Result<Vec<u8>, ZstdError> {
    if regenerated_size > LITERALS_MAX_SIZE {
        return Err(ZstdError::DecompressionBomb {
            decoded: regenerated_size,
            cap: LITERALS_MAX_SIZE,
        });
    }
    if tree.max_bits == 0 || tree.decode_table.is_empty() {
        return Err(ZstdError::UnexpectedEof);
    }
    let mut out: Vec<u8> = Vec::with_capacity(regenerated_size);
    if regenerated_size == 0 {
        return Ok(out);
    }
    let mut reader = ReverseBitReader::new(payload)?;
    let max_bits = tree.max_bits;
    while out.len() < regenerated_size {
        let remaining = reader.remaining();
        // Build a `max_bits`-wide index. When fewer than max_bits remain,
        // read what's there and left-shift to pad with zeros (so the
        // index addresses the SAME slot as a shorter canonical code).
        let (index, bits_read) = if remaining == 0 {
            // No more bits and we still owe symbols — malformed input
            // (the bitstream should have at least enough bits to cover
            // the remaining symbol with its actual code length).
            return Err(ZstdError::UnexpectedEof);
        } else if (remaining as u32) >= max_bits {
            let v = reader.read_bits(max_bits)?;
            (v as usize, max_bits)
        } else {
            let bits = remaining as u32;
            let v = reader.read_bits(bits)?;
            // Left-shift to pad with zeros at the LOW end so the index
            // points to the canonical decode-table slot.
            ((v << (max_bits - bits)) as usize, bits)
        };
        if index >= tree.decode_table.len() {
            return Err(ZstdError::UnexpectedEof);
        }
        let entry = tree.decode_table[index];
        if entry.bits == 0 {
            return Err(ZstdError::UnexpectedEof);
        }
        out.push(entry.symbol);
        // We "consumed" entry.bits bits semantically, but we already
        // read `bits_read` from the stream. If bits_read > entry.bits,
        // return the excess to the stream.
        if (bits_read as u8) > entry.bits {
            let excess = (bits_read as u8) - entry.bits;
            // ReverseBitReader doesn't expose an explicit "unread";
            // emulate by rewinding bit_pos directly via a small helper.
            reader.rewind(excess as u32);
        }
    }
    Ok(out)
}

/// Decode a 4-stream Huffman bitstream per RFC 8478 §4.2.2.
///
/// Layout of `input` (the post-tree-description bitstream bytes):
///   bytes 0..2  : `jump1` u16-LE — byte length of stream 1
///   bytes 2..4  : `jump2` u16-LE — byte length of stream 2
///   bytes 4..6  : `jump3` u16-LE — byte length of stream 3
///   bytes 6..(6+jump1)        : stream 1 reverse bitstream
///   bytes (6+jump1)..(+jump2) : stream 2 reverse bitstream
///   bytes (+jump2)..(+jump3)  : stream 3 reverse bitstream
///   bytes (+jump3)..end       : stream 4 reverse bitstream
///
/// Per-stream decoded sizes:
///   streams 1, 2, 3 : `(regenerated_size + 3) / 4` bytes each
///   stream 4        : `regenerated_size - 3 * per_stream` bytes (the rest)
///
/// Output = concatenation of (stream1, stream2, stream3, stream4) decoded.
pub(crate) fn decode_huffman_4streams(
    input: &[u8],
    tree: &HuffmanTree,
    regenerated_size: usize,
) -> Result<Vec<u8>, ZstdError> {
    if regenerated_size > LITERALS_MAX_SIZE {
        return Err(ZstdError::DecompressionBomb {
            decoded: regenerated_size,
            cap: LITERALS_MAX_SIZE,
        });
    }
    if input.len() < 6 {
        return Err(ZstdError::UnexpectedEof);
    }
    let jump1 = u16::from_le_bytes([input[0], input[1]]) as usize;
    let jump2 = u16::from_le_bytes([input[2], input[3]]) as usize;
    let jump3 = u16::from_le_bytes([input[4], input[5]]) as usize;
    let after_jump = &input[6..];
    // Validate that jumps fit within the remaining input (stream 4 takes
    // whatever's left; jump1+jump2+jump3 MUST be <= after_jump.len()).
    let sum_first_three = jump1
        .checked_add(jump2)
        .and_then(|v| v.checked_add(jump3))
        .ok_or(ZstdError::UnexpectedEof)?;
    if sum_first_three > after_jump.len() {
        return Err(ZstdError::UnexpectedEof);
    }
    let stream1 = &after_jump[..jump1];
    let stream2 = &after_jump[jump1..jump1 + jump2];
    let stream3 = &after_jump[jump1 + jump2..jump1 + jump2 + jump3];
    let stream4 = &after_jump[jump1 + jump2 + jump3..];

    // Per-stream regenerated sizes.
    let per = (regenerated_size + 3) / 4;
    let last = regenerated_size
        .checked_sub(3 * per)
        .ok_or(ZstdError::UnexpectedEof)?;

    let mut out = Vec::with_capacity(regenerated_size);
    out.extend(decode_huffman_stream(stream1, tree, per)?);
    out.extend(decode_huffman_stream(stream2, tree, per)?);
    out.extend(decode_huffman_stream(stream3, tree, per)?);
    out.extend(decode_huffman_stream(stream4, tree, last)?);
    Ok(out)
}

/// Decode a Compressed literals section per RFC 8478 §5.3.4 — dispatches
/// to single-stream or 4-stream decode based on the literals header's
/// `num_streams` field.
///
/// `input` MUST begin at the literals section header. Returns the decoded
/// literals + the total bytes consumed by the section.
pub(crate) fn decode_compressed_literals(
    input: &[u8],
) -> Result<(Vec<u8>, usize), ZstdError> {
    let header = parse_literals_header(input)?;
    if header.block_type != LiteralsBlockType::Compressed {
        return Err(ZstdError::LiteralsBlockTypeNotYetSupported {
            block_type: header.block_type as u8,
        });
    }
    let after_header = &input[header.header_len..];
    let comp_size = header.compressed_size as usize;
    if after_header.len() < comp_size {
        return Err(ZstdError::UnexpectedEof);
    }
    let stream_bytes = &after_header[..comp_size];
    let (tree, tree_consumed) = parse_huffman_tree(stream_bytes)?;
    let bitstream = &stream_bytes[tree_consumed..];
    let decoded = match header.num_streams {
        1 => decode_huffman_stream(bitstream, &tree, header.regenerated_size as usize)?,
        4 => decode_huffman_4streams(bitstream, &tree, header.regenerated_size as usize)?,
        _ => return Err(ZstdError::UnexpectedEof),
    };
    let total_consumed = header.header_len + comp_size;
    Ok((decoded, total_consumed))
}

/// Decode a Treeless literals section per RFC 8478 §5.3.5 — same layout
/// as Compressed but with NO Huffman tree description (the previous
/// block's tree is reused). The caller supplies `prev_tree`.
///
/// `input` MUST begin at the literals section header. Returns the
/// decoded literals + the total bytes consumed by the section.
pub(crate) fn decode_treeless_literals(
    input: &[u8],
    prev_tree: &HuffmanTree,
) -> Result<(Vec<u8>, usize), ZstdError> {
    let header = parse_literals_header(input)?;
    if header.block_type != LiteralsBlockType::Treeless {
        return Err(ZstdError::LiteralsBlockTypeNotYetSupported {
            block_type: header.block_type as u8,
        });
    }
    let after_header = &input[header.header_len..];
    let comp_size = header.compressed_size as usize;
    if after_header.len() < comp_size {
        return Err(ZstdError::UnexpectedEof);
    }
    let bitstream = &after_header[..comp_size];
    let decoded = match header.num_streams {
        1 => decode_huffman_stream(bitstream, prev_tree, header.regenerated_size as usize)?,
        4 => decode_huffman_4streams(bitstream, prev_tree, header.regenerated_size as usize)?,
        _ => return Err(ZstdError::UnexpectedEof),
    };
    let total_consumed = header.header_len + comp_size;
    Ok((decoded, total_consumed))
}

/// Compatibility wrapper for SP129 callers — same semantics as
/// `decode_compressed_literals` but rejects 4-stream with the SP129
/// sentinel marker for any caller still expecting single-stream-only.
pub(crate) fn decode_compressed_literals_single_stream(
    input: &[u8],
) -> Result<(Vec<u8>, usize), ZstdError> {
    let header = parse_literals_header(input)?;
    if header.block_type != LiteralsBlockType::Compressed {
        return Err(ZstdError::LiteralsBlockTypeNotYetSupported {
            block_type: header.block_type as u8,
        });
    }
    if header.num_streams != 1 {
        return Err(ZstdError::LiteralsBlockTypeNotYetSupported {
            block_type: 0xFE,
        });
    }
    decode_compressed_literals(input)
}

// ============================================================================
// KATs.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zstd_fse::ReverseBitReader;
    use crate::zstd_huffman::HuffmanEntry;

    /// Helper: build a 2-symbol uniform 1-bit-code tree (the smallest
    /// well-formed Huffman tree this codebase produces).
    /// max_bits=1; decode_table = [{sym:0, bits:1}, {sym:1, bits:1}].
    fn uniform_2sym_tree() -> HuffmanTree {
        HuffmanTree {
            max_bits: 1,
            num_symbols: 2,
            bits_per_symbol: vec![1, 1],
            decode_table: vec![
                HuffmanEntry { symbol: 0, bits: 1 },
                HuffmanEntry { symbol: 1, bits: 1 },
            ],
        }
    }

    /// Helper: build a 4-symbol uniform 2-bit-code tree.
    /// max_bits=2; decode_table = [{sym:0, bits:2}, {sym:1, bits:2},
    /// {sym:2, bits:2}, {sym:3, bits:2}].
    fn uniform_4sym_tree() -> HuffmanTree {
        HuffmanTree {
            max_bits: 2,
            num_symbols: 4,
            bits_per_symbol: vec![2, 2, 2, 2],
            decode_table: vec![
                HuffmanEntry { symbol: 0, bits: 2 },
                HuffmanEntry { symbol: 1, bits: 2 },
                HuffmanEntry { symbol: 2, bits: 2 },
                HuffmanEntry { symbol: 3, bits: 2 },
            ],
        }
    }

    /// SP129-KAT-1: empty regenerated_size returns empty output without
    /// touching the bitstream.
    #[test]
    fn sp129_kat_empty_regenerated_size_yields_empty() {
        let tree = uniform_2sym_tree();
        let out = decode_huffman_stream(&[0x80u8], &tree, 0).unwrap();
        assert_eq!(out, Vec::<u8>::new());
    }

    /// SP129-KAT-2: 1-bit-code tree, payload single byte 0b1100_0001.
    /// pad_bit = 7 (MSB=1); payload bits (MSB-first below pad) = bits 6..0
    /// of 0b1100_0001 = 1,0,0,0,0,0,1.
    /// With max_bits=1, each step reads 1 bit + emits 1 symbol:
    ///   bit 0 = 1 → sym 1
    ///   bit 1 = 0 → sym 0
    ///   bit 2 = 0 → sym 0
    ///   bit 3 = 0 → sym 0
    ///   bit 4 = 0 → sym 0
    ///   bit 5 = 0 → sym 0
    ///   bit 6 = 1 → sym 1
    /// → decoded = [1, 0, 0, 0, 0, 0, 1].
    #[test]
    fn sp129_kat_single_bit_codes_decode_correctly() {
        let tree = uniform_2sym_tree();
        let out = decode_huffman_stream(&[0b1100_0001u8], &tree, 7).unwrap();
        assert_eq!(out, vec![1u8, 0, 0, 0, 0, 0, 1]);
    }

    /// SP129-KAT-3: 2-bit-code tree, payload chosen to decode to
    /// [0, 1, 2, 3]. With 4-sym uniform tree, codes are 00/01/10/11.
    /// Reverse bitstream MSB-first reads:
    ///   first 2 bits → "00" → sym 0
    ///   next 2 bits → "01" → sym 1
    ///   next 2 bits → "10" → sym 2
    ///   next 2 bits → "11" → sym 3
    /// Total payload = 8 bits. With pad bit + 8 payload = 9 bits total.
    /// Use 2 bytes: [b0, b1] with b1 carrying pad+1 payload bit, b0 the other 7.
    /// pad_bit needs to be at bit 0 (so b1's bit 0 = pad marker; no payload
    /// bits below; payload starts at b0's bit 7). Wait — pad_bit IS the
    /// highest set bit, must be in 0..=7. If we want exactly 8 payload bits,
    /// last byte must carry 0 payload bits (pad_bit = 0 means last byte
    /// is 0x01 with no bits below); then b0 carries all 8 payload bits.
    /// Reading reverse MSB-first from b0 yields b0's bit 7 first, then bit 6, etc.
    /// We want the FIRST 2 bits read (MSB-first) to be "00" = decoded as 0:
    ///   b0 bits 7,6 = 0, 0.
    /// Next 2 bits (5,4) = 0, 1 → sym 1.
    /// Next 2 bits (3,2) = 1, 0 → sym 2.
    /// Next 2 bits (1,0) = 1, 1 → sym 3.
    /// → b0 = 0b00_01_10_11 = 0x1B.
    /// b1 = 0x01 (pad marker only).
    #[test]
    fn sp129_kat_two_bit_codes_decode_correctly() {
        let tree = uniform_4sym_tree();
        let out = decode_huffman_stream(&[0x1Bu8, 0x01u8], &tree, 4).unwrap();
        assert_eq!(out, vec![0u8, 1, 2, 3]);
    }

    /// SP129-KAT-4: requesting more symbols than the bitstream supplies
    /// → typed UnexpectedEof.
    /// payload = single byte 0x01 → pad_bit = 0 (only bit 0 set, which IS
    /// the padding marker) → 0 payload bits. Requesting 1 symbol with 0
    /// available bits must trap.
    #[test]
    fn sp129_kat_insufficient_bits_traps() {
        let tree = uniform_2sym_tree();
        assert_eq!(
            decode_huffman_stream(&[0x01u8], &tree, 1).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP129-KAT-5: regenerated_size beyond LITERALS_MAX_SIZE → typed bomb.
    #[test]
    fn sp129_kat_bomb_cap_traps() {
        let tree = uniform_2sym_tree();
        let err = decode_huffman_stream(&[0x80u8], &tree, LITERALS_MAX_SIZE + 1).unwrap_err();
        match err {
            ZstdError::DecompressionBomb { decoded, cap } => {
                assert_eq!(decoded, LITERALS_MAX_SIZE + 1);
                assert_eq!(cap, LITERALS_MAX_SIZE);
            }
            other => panic!("expected DecompressionBomb; got {other:?}"),
        }
    }

    /// SP129-KAT-6: deterministic — same input twice → identical output.
    #[test]
    fn sp129_kat_deterministic_repeat() {
        let tree = uniform_4sym_tree();
        let r1 = decode_huffman_stream(&[0x1Bu8, 0x01u8], &tree, 4).unwrap();
        let r2 = decode_huffman_stream(&[0x1Bu8, 0x01u8], &tree, 4).unwrap();
        assert_eq!(r1, r2);
    }

    /// SP129-KAT-7: decode_compressed_literals_single_stream rejects
    /// non-Compressed block types.
    /// Raw header byte 0x10 (block_type=0, size_format=00, regen=2)
    /// → returns LiteralsBlockTypeNotYetSupported{block_type:0}.
    #[test]
    fn sp129_kat_non_compressed_block_rejected() {
        let bytes = [0x10u8, b'a', b'b'];
        match decode_compressed_literals_single_stream(&bytes).unwrap_err() {
            ZstdError::LiteralsBlockTypeNotYetSupported { block_type } => {
                assert_eq!(block_type, 0); // Raw
            }
            other => panic!("expected LiteralsBlockTypeNotYetSupported; got {other:?}"),
        }
    }

    /// SP129-KAT-8: decode_compressed_literals_single_stream rejects
    /// 4-stream variants with the marker sentinel 0xFE.
    /// Compressed/size_format=01 header (3-byte) → 4 streams expected;
    /// we trap before touching the tree.
    /// regen=5, comp=3, block_type=10 (Compressed), size_format=01.
    /// combined = (5 << 4) | (3 << 14) | (0b01 << 2) | 0b10
    ///          = 0x50 | 0xC000 | 0x04 | 0x02 = 0xC056.
    /// b0 = 0x56, b1 = 0xC0, b2 = 0x00.
    #[test]
    fn sp129_kat_four_stream_variant_deferred() {
        let bytes = [0x56u8, 0xC0u8, 0x00u8, 0xFFu8, 0xFFu8, 0xFFu8];
        match decode_compressed_literals_single_stream(&bytes).unwrap_err() {
            ZstdError::LiteralsBlockTypeNotYetSupported { block_type: 0xFE } => {}
            other => panic!("expected 4-stream sentinel; got {other:?}"),
        }
    }

    /// SP129-KAT-9: an empty Huffman tree (no symbols) → typed error.
    #[test]
    fn sp129_kat_empty_tree_traps() {
        let tree = HuffmanTree {
            max_bits: 0,
            num_symbols: 0,
            bits_per_symbol: vec![],
            decode_table: vec![],
        };
        assert_eq!(
            decode_huffman_stream(&[0x80u8], &tree, 1).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    // ========================================================================
    // SP130 KATs — 4-stream Huffman bitstream decoder.
    // ========================================================================

    /// SP130-KAT-1: truncated input (<6 bytes for jump table) → typed err.
    #[test]
    fn sp130_kat_jump_table_truncated_traps() {
        let tree = uniform_2sym_tree();
        assert_eq!(
            decode_huffman_4streams(&[0x00u8; 5], &tree, 4).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP130-KAT-2: jump values sum > available bytes → typed err.
    /// Jump table = [10, 10, 10] = 30; but only 6 bytes after = stream 4
    /// would have NEGATIVE size. Trap.
    #[test]
    fn sp130_kat_jump_overrun_traps() {
        let tree = uniform_2sym_tree();
        let mut bytes = vec![10u8, 0, 10, 0, 10, 0]; // jumps 10/10/10
        bytes.extend(vec![0x80u8; 6]); // only 6 bytes after jump table
        assert_eq!(
            decode_huffman_4streams(&bytes, &tree, 16).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP130-KAT-3: regen=0 → all streams decode to empty. Jump = [0,0,0];
    /// 4 empty streams need 0 bytes after the jump table. per_stream =
    /// (0+3)/4 = 0, last = 0 - 0 = 0. All `decode_huffman_stream`
    /// invocations with regen=0 short-circuit to empty.
    #[test]
    fn sp130_kat_regen_zero_yields_empty() {
        let tree = uniform_2sym_tree();
        let bytes = vec![0u8; 6]; // jump table all zeros
        let out = decode_huffman_4streams(&bytes, &tree, 0).unwrap();
        assert_eq!(out, Vec::<u8>::new());
    }

    /// SP130-KAT-4: regen beyond LITERALS_MAX_SIZE → typed bomb.
    #[test]
    fn sp130_kat_bomb_cap_traps() {
        let tree = uniform_2sym_tree();
        let bytes = vec![0u8; 6];
        let err = decode_huffman_4streams(&bytes, &tree, LITERALS_MAX_SIZE + 1).unwrap_err();
        match err {
            ZstdError::DecompressionBomb { decoded, cap } => {
                assert_eq!(decoded, LITERALS_MAX_SIZE + 1);
                assert_eq!(cap, LITERALS_MAX_SIZE);
            }
            other => panic!("expected DecompressionBomb; got {other:?}"),
        }
    }

    /// SP130-KAT-5: 4 identical streams each decoding to 2 symbols.
    /// Use uniform_4sym_tree (max_bits=2). Each stream = [0x1B, 0x01]
    /// (from SP129-KAT-3) decodes to [0, 1, 2, 3]. But we want each
    /// stream to produce 2 symbols (per_stream = (8+3)/4 = 2; last = 8-6 = 2).
    /// For 2 symbols at max_bits=2: each stream needs 4 payload bits.
    /// Use payload byte 0x1B with pad in a separate byte: stream = [0x1B, 0x01]
    /// where 0x01 = pad_bit=0 (single payload of 4 bits all from 0x1B):
    /// Reading reverse MSB-first from 0x1B = 0b0001_1011: bits 7..0 = 0001_1011.
    /// pad in 0x01 = bit 0; payload in 0x1B (8 bits): MSB first = 0,0,0,1,1,0,1,1.
    /// First 2 bits = 00 → sym 0; next 2 = 01 → sym 1; next 2 = 10 → sym 2;
    /// last 2 = 11 → sym 3. → decoded = [0,1,2,3] (4 symbols).
    /// With per_stream = 2, decoder reads only first 2 symbols: [0, 1].
    /// 4 streams × [0, 1] = output [0,1, 0,1, 0,1, 0,1] (8 syms).
    /// Each stream length = 2 bytes. jumps = [2, 2, 2]. Total layout:
    ///   bytes 0..6 = [2,0, 2,0, 2,0]
    ///   bytes 6..8 = stream1 = [0x1B, 0x01]
    ///   bytes 8..10 = stream2 = [0x1B, 0x01]
    ///   bytes 10..12 = stream3 = [0x1B, 0x01]
    ///   bytes 12..14 = stream4 = [0x1B, 0x01]
    #[test]
    fn sp130_kat_four_identical_streams_concat() {
        let tree = uniform_4sym_tree();
        let mut bytes = vec![2u8, 0, 2, 0, 2, 0]; // jumps
        bytes.extend(&[0x1Bu8, 0x01u8]); // stream 1
        bytes.extend(&[0x1Bu8, 0x01u8]); // stream 2
        bytes.extend(&[0x1Bu8, 0x01u8]); // stream 3
        bytes.extend(&[0x1Bu8, 0x01u8]); // stream 4
        let out = decode_huffman_4streams(&bytes, &tree, 8).unwrap();
        assert_eq!(out, vec![0u8, 1, 0, 1, 0, 1, 0, 1]);
    }

    /// SP130-KAT-6: deterministic — same input twice → identical output.
    #[test]
    fn sp130_kat_deterministic_repeat() {
        let tree = uniform_4sym_tree();
        let mut bytes = vec![2u8, 0, 2, 0, 2, 0];
        for _ in 0..4 {
            bytes.extend(&[0x1Bu8, 0x01u8]);
        }
        let r1 = decode_huffman_4streams(&bytes, &tree, 8).unwrap();
        let r2 = decode_huffman_4streams(&bytes, &tree, 8).unwrap();
        assert_eq!(r1, r2);
    }

    /// SP130-KAT-7: decode_compressed_literals (the dispatcher) routes
    /// num_streams=1 to the single-stream decoder. Reuses the SP129
    /// single-stream test path indirectly — we verify the dispatcher
    /// rejects non-Compressed types identically to single-stream.
    #[test]
    fn sp130_kat_dispatcher_rejects_non_compressed() {
        // Raw header byte 0x10 (regen=2)
        let bytes = [0x10u8, b'a', b'b'];
        match decode_compressed_literals(&bytes).unwrap_err() {
            ZstdError::LiteralsBlockTypeNotYetSupported { block_type } => {
                assert_eq!(block_type, 0); // Raw
            }
            other => panic!("expected LiteralsBlockTypeNotYetSupported; got {other:?}"),
        }
    }

    // ========================================================================
    // SP131 KATs — Treeless literal mode.
    // ========================================================================

    /// SP131-KAT-T1: decode_treeless_literals rejects non-Treeless types
    /// (here: Raw with header 0x10 → block_type=0).
    #[test]
    fn sp131_kat_treeless_rejects_non_treeless() {
        let tree = uniform_2sym_tree();
        let bytes = [0x10u8, b'a', b'b'];
        match decode_treeless_literals(&bytes, &tree).unwrap_err() {
            ZstdError::LiteralsBlockTypeNotYetSupported { block_type } => {
                assert_eq!(block_type, 0); // Raw
            }
            other => panic!("expected LiteralsBlockTypeNotYetSupported; got {other:?}"),
        }
    }

    /// SP131-KAT-T2: Treeless single-stream decode with the SP129 KAT-3
    /// bitstream + a uniform_4sym_tree. Header = Treeless / size_format=00
    /// / 1-stream / regen=4 / comp=2.
    /// combined = (4 << 4) | (2 << 14) | (0b00 << 2) | 0b11
    ///          = 0x40 | 0x8000 | 0x00 | 0x03 = 0x8043.
    /// b0 = 0x43, b1 = 0x80, b2 = 0x00. Bitstream = [0x1B, 0x01].
    /// Expected decoded = [0, 1, 2, 3] (same as SP129-KAT-3).
    #[test]
    fn sp131_kat_treeless_single_stream_decodes() {
        let tree = uniform_4sym_tree();
        let bytes = [0x43u8, 0x80, 0x00, 0x1B, 0x01];
        let (out, consumed) = decode_treeless_literals(&bytes, &tree).unwrap();
        assert_eq!(out, vec![0u8, 1, 2, 3]);
        assert_eq!(consumed, 5); // 3-byte header + 2-byte bitstream
    }

    /// SP131-KAT-T3: Treeless 4-stream decode. Header = Treeless /
    /// size_format=01 / 4-stream / regen=8 / comp=14 (6 jump + 4 × 2 = 14).
    /// combined = (8 << 4) | (14 << 14) | (0b01 << 2) | 0b11
    ///          = 0x80 | 0x38000 | 0x04 | 0x03 = 0x38087.
    /// b0 = 0x87, b1 = 0x80, b2 = 0x03.
    /// Then jump table [2,0, 2,0, 2,0] + 4 × [0x1B, 0x01] = 14 bytes.
    /// Per-stream output = [0, 1] each → concat = [0,1,0,1,0,1,0,1].
    #[test]
    fn sp131_kat_treeless_four_stream_decodes() {
        let tree = uniform_4sym_tree();
        let mut bytes = vec![0x87u8, 0x80, 0x03]; // Treeless header
        bytes.extend(&[2u8, 0, 2, 0, 2, 0]); // jump table
        for _ in 0..4 {
            bytes.extend(&[0x1Bu8, 0x01u8]);
        }
        let (out, _) = decode_treeless_literals(&bytes, &tree).unwrap();
        assert_eq!(out, vec![0u8, 1, 0, 1, 0, 1, 0, 1]);
    }

    /// SP131-KAT-T4: deterministic — same input + same tree twice → same output.
    #[test]
    fn sp131_kat_treeless_deterministic_repeat() {
        let tree = uniform_4sym_tree();
        let bytes = [0x43u8, 0x80, 0x00, 0x1B, 0x01];
        let r1 = decode_treeless_literals(&bytes, &tree).unwrap().0;
        let r2 = decode_treeless_literals(&bytes, &tree).unwrap().0;
        assert_eq!(r1, r2);
    }
}
