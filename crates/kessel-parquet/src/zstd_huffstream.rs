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

/// Decode a single-stream Compressed literals section (block_type=2,
/// size_format=00). `input` MUST begin at the literals section header.
/// Returns the decoded literals + the total bytes consumed by the
/// section (header + tree description + bitstream).
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
        // 4-stream variant defers to SP130.
        return Err(ZstdError::LiteralsBlockTypeNotYetSupported {
            block_type: 0xFE, // marker: "Compressed-4stream not yet supported"
        });
    }
    let after_header = &input[header.header_len..];
    let comp_size = header.compressed_size as usize;
    if after_header.len() < comp_size {
        return Err(ZstdError::UnexpectedEof);
    }
    let stream_bytes = &after_header[..comp_size];
    // Tree description starts at byte 0 of the stream; parse_huffman_tree
    // returns the byte count it consumed; the rest is the Huffman
    // bitstream.
    let (tree, tree_consumed) = parse_huffman_tree(stream_bytes)?;
    let bitstream = &stream_bytes[tree_consumed..];
    let decoded = decode_huffman_stream(bitstream, &tree, header.regenerated_size as usize)?;
    let total_consumed = header.header_len + comp_size;
    Ok((decoded, total_consumed))
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
}
