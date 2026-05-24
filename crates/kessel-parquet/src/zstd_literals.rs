//! Literals section header + Raw + RLE literal-mode decoders for zstd —
//! **SP127 slice of the OBJ-2c-2 arc**.
//!
//! Authority: RFC 8478 §5.3.1 (Literals_Section_Header) + §5.3.2 (Raw_Literals)
//! + §5.3.3 (RLE_Literals).
//!
//! What this module ships (SP127):
//!
//!   1. **`parse_literals_header`** — 1-to-5-byte variable-length header
//!      decoder per RFC §5.3.1. The first byte encodes the literals
//!      block type (2 bits) + size-format (2 bits). For Raw/RLE modes
//!      the size-format dictates a 1/2/3-byte header carrying only the
//!      regenerated_size (no streams). For Compressed/Treeless modes the
//!      size-format dictates a 3/4/5-byte header carrying both
//!      regenerated_size + compressed_size + 1-or-4 stream count.
//!
//!   2. **`decode_raw_literals`** — RFC §5.3.2: copies `regenerated_size`
//!      bytes from the input stream straight to the literals buffer.
//!
//!   3. **`decode_rle_literals`** — RFC §5.3.3: reads 1 byte from the
//!      input, repeats it `regenerated_size` times into the literals
//!      buffer.
//!
//! Scope cleanly bounded — Compressed + Treeless literal modes (RFC
//! §5.3.4 + §5.3.5) need Huffman tree decoding (SP128) and the Huffman
//! bitstream decoder (SP129), both of which are downstream slices.
//! `parse_literals_header` IS aware of those modes (returns the parsed
//! structure correctly); the actual payload decode for them returns
//! `ZstdError::LiteralsBlockTypeNotYetSupported` with the carried type.
//!
//! Determinism: pure transforms of input bytes. Bounds-checked: typed
//! errors throughout; no panics on attacker input.

#![allow(dead_code)]

use crate::zstd::ZstdError;

/// Maximum decoded literals size per RFC §5.3.1 (the literals section
/// of a single compressed block is bounded by the block size). We cap
/// at 128 KiB to align with the zstd block cap shipped at SP125.
pub(crate) const LITERALS_MAX_SIZE: usize = 128 * 1024;

/// Literals block type per RFC §5.3.1.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiteralsBlockType {
    Raw = 0,
    Rle = 1,
    Compressed = 2,
    Treeless = 3,
}

/// Parsed literals section header.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LiteralsHeader {
    pub block_type: LiteralsBlockType,
    /// Number of bytes the decoded literals occupy.
    pub regenerated_size: u32,
    /// For Compressed/Treeless: bytes of input that follow the header
    /// (excluding the header itself). Zero for Raw (it equals
    /// regenerated_size by definition; we return 0 to signal "header
    /// itself carries no compressed_size field"). Zero for RLE
    /// (compressed payload is always 1 byte after the header).
    pub compressed_size: u32,
    /// For Compressed/Treeless with size_format = 2 or 3 → 4 streams
    /// with a 6-byte jump table; else 1 stream. Always 1 for Raw/RLE.
    pub num_streams: u8,
    /// Header length in bytes (1/2/3 for Raw/RLE; 3/4/5 for Comp/Tree).
    pub header_len: usize,
}

/// Parse the literals section header per RFC §5.3.1.
///
/// `input` must start at the header's first byte. Returns the parsed
/// header (which records `header_len` so the caller can advance past it).
pub(crate) fn parse_literals_header(input: &[u8]) -> Result<LiteralsHeader, ZstdError> {
    if input.is_empty() {
        return Err(ZstdError::UnexpectedEof);
    }
    let b0 = input[0];
    // Per RFC §5.3.1.1:
    //   bits 1-0 : Literals_Block_Type
    //   bits 3-2 : Size_Format
    //   bits ... : payload, depending on type + size_format
    let block_type = match b0 & 0b11 {
        0 => LiteralsBlockType::Raw,
        1 => LiteralsBlockType::Rle,
        2 => LiteralsBlockType::Compressed,
        3 => LiteralsBlockType::Treeless,
        _ => unreachable!(),
    };
    let size_format = (b0 >> 2) & 0b11;

    let (regenerated_size, compressed_size, num_streams, header_len) =
        match block_type {
            LiteralsBlockType::Raw | LiteralsBlockType::Rle => {
                // RFC §5.3.1.1: for Raw/RLE the size_format encodes a
                // 5/12/20-bit regenerated_size across a 1/2/3-byte header.
                //
                // size_format == 00 OR == 10:
                //   1 byte; regenerated_size = b0 >> 3 (5 bits).
                //   The two encodings are equivalent for Raw/RLE — the
                //   spec allows either to mean "5-bit single-byte form".
                //
                // size_format == 01:
                //   2 bytes; regenerated_size = (b0 >> 4) | (b1 << 4)  (12 bits).
                //
                // size_format == 11:
                //   3 bytes; regenerated_size = (b0 >> 4) | (b1 << 4) | (b2 << 12)
                //                                                      (20 bits).
                let (regen, hdr_len) = match size_format {
                    0b00 | 0b10 => {
                        ((b0 >> 3) as u32, 1usize)
                    }
                    0b01 => {
                        if input.len() < 2 {
                            return Err(ZstdError::UnexpectedEof);
                        }
                        let regen = ((b0 as u32) >> 4) | ((input[1] as u32) << 4);
                        (regen, 2usize)
                    }
                    0b11 => {
                        if input.len() < 3 {
                            return Err(ZstdError::UnexpectedEof);
                        }
                        let regen = ((b0 as u32) >> 4)
                            | ((input[1] as u32) << 4)
                            | ((input[2] as u32) << 12);
                        (regen, 3usize)
                    }
                    _ => unreachable!(),
                };
                // Raw/RLE: always 1 stream; compressed_size carries
                // no header field (Raw payload = regenerated_size bytes;
                // RLE payload = always 1 byte after the header).
                (regen, 0u32, 1u8, hdr_len)
            }
            LiteralsBlockType::Compressed | LiteralsBlockType::Treeless => {
                // RFC §5.3.1.1 Compressed/Treeless: size_format encodes
                // num_streams + sizes across a 3/4/5-byte header. The
                // total payload width:
                //   size_format == 00: 3 bytes; 1 stream;
                //     regen + comp each = 10 bits.
                //   size_format == 01: 3 bytes; 4 streams;
                //     regen + comp each = 10 bits.
                //   size_format == 10: 4 bytes; 4 streams;
                //     regen + comp each = 14 bits.
                //   size_format == 11: 5 bytes; 4 streams;
                //     regen + comp each = 18 bits.
                match size_format {
                    0b00 | 0b01 => {
                        // 3-byte header. Bit layout:
                        //   b0 bits 7..4 = regen[3..0]
                        //   b1 bits 7..0 = regen[11..4]  (upper 4 of regen + lower 4 of comp)
                        //   ... actually let me re-trace:
                        // The RFC lays the bits out as a 24-bit little-endian field
                        // [b0,b1,b2] with the literal-header bits 0..3 already consumed:
                        //   bits  0-1  : block_type     (b0 & 0b11)
                        //   bits  2-3  : size_format    ((b0 >> 2) & 0b11)
                        //   bits  4-13 : regen_size     (10 bits)
                        //   bits 14-23 : compressed_size (10 bits)
                        if input.len() < 3 {
                            return Err(ZstdError::UnexpectedEof);
                        }
                        let combined: u32 = (b0 as u32)
                            | ((input[1] as u32) << 8)
                            | ((input[2] as u32) << 16);
                        let regen = (combined >> 4) & 0x3FF;
                        let comp = (combined >> 14) & 0x3FF;
                        let streams = if size_format == 0b00 { 1u8 } else { 4u8 };
                        (regen, comp, streams, 3usize)
                    }
                    0b10 => {
                        // 4-byte header. 32-bit LE field:
                        //   bits  0-1  : block_type
                        //   bits  2-3  : size_format
                        //   bits  4-17 : regen_size     (14 bits)
                        //   bits 18-31 : compressed_size (14 bits)
                        if input.len() < 4 {
                            return Err(ZstdError::UnexpectedEof);
                        }
                        let combined: u32 = (b0 as u32)
                            | ((input[1] as u32) << 8)
                            | ((input[2] as u32) << 16)
                            | ((input[3] as u32) << 24);
                        let regen = (combined >> 4) & 0x3FFF;
                        let comp = (combined >> 18) & 0x3FFF;
                        (regen, comp, 4u8, 4usize)
                    }
                    0b11 => {
                        // 5-byte header. 40-bit LE field:
                        //   bits  0-1  : block_type
                        //   bits  2-3  : size_format
                        //   bits  4-21 : regen_size     (18 bits)
                        //   bits 22-39 : compressed_size (18 bits)
                        if input.len() < 5 {
                            return Err(ZstdError::UnexpectedEof);
                        }
                        let combined: u64 = (b0 as u64)
                            | ((input[1] as u64) << 8)
                            | ((input[2] as u64) << 16)
                            | ((input[3] as u64) << 24)
                            | ((input[4] as u64) << 32);
                        let regen = ((combined >> 4) & 0x3FFFF) as u32;
                        let comp = ((combined >> 22) & 0x3FFFF) as u32;
                        (regen, comp, 4u8, 5usize)
                    }
                    _ => unreachable!(),
                }
            }
        };

    if regenerated_size as usize > LITERALS_MAX_SIZE {
        return Err(ZstdError::DecompressionBomb {
            decoded: regenerated_size as usize,
            cap: LITERALS_MAX_SIZE,
        });
    }

    Ok(LiteralsHeader {
        block_type,
        regenerated_size,
        compressed_size,
        num_streams,
        header_len,
    })
}

/// Decode Raw_Literals per RFC §5.3.2 — `regenerated_size` bytes copied
/// from `payload` straight to the output buffer.
pub(crate) fn decode_raw_literals(
    payload: &[u8],
    regenerated_size: usize,
) -> Result<Vec<u8>, ZstdError> {
    if payload.len() < regenerated_size {
        return Err(ZstdError::UnexpectedEof);
    }
    if regenerated_size > LITERALS_MAX_SIZE {
        return Err(ZstdError::DecompressionBomb {
            decoded: regenerated_size,
            cap: LITERALS_MAX_SIZE,
        });
    }
    Ok(payload[..regenerated_size].to_vec())
}

/// Decode RLE_Literals per RFC §5.3.3 — 1 byte read from `payload`,
/// repeated `regenerated_size` times.
pub(crate) fn decode_rle_literals(
    payload: &[u8],
    regenerated_size: usize,
) -> Result<Vec<u8>, ZstdError> {
    if payload.is_empty() {
        return Err(ZstdError::UnexpectedEof);
    }
    if regenerated_size > LITERALS_MAX_SIZE {
        return Err(ZstdError::DecompressionBomb {
            decoded: regenerated_size,
            cap: LITERALS_MAX_SIZE,
        });
    }
    Ok(vec![payload[0]; regenerated_size])
}

// ============================================================================
// KATs — hand-derived from RFC 8478 §5.3.1.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// SP127-KAT-1: Raw / size_format=00 / 1-byte header.
    /// b0 = block_type=00 | size_format=00 | regen[4..0]=0b01010 (=10)
    ///    = 0b0101_0000 | 0b00 | 0b00 = 0b01010000 = 0x50.
    /// regen_size = (0x50 >> 3) = 10.
    #[test]
    fn sp127_kat_raw_size_format_00_one_byte_header() {
        let h = parse_literals_header(&[0x50u8]).unwrap();
        assert_eq!(h.block_type, LiteralsBlockType::Raw);
        assert_eq!(h.regenerated_size, 10);
        assert_eq!(h.header_len, 1);
        assert_eq!(h.num_streams, 1);
    }

    /// SP127-KAT-2: Raw / size_format=01 / 2-byte header.
    /// regen_size to encode: 200 (12-bit field).
    /// 200 = 0b1100_1000 = 0xC8. Field layout: low 4 bits at b0[7..4], high 8 at b1.
    /// regen[3..0] = 0b1000 (low nibble = 8); regen[11..4] = 0b1100 (high byte's
    /// low 8 = 0x0C). So b0 = (0b1000 << 4) | (size_format=01 << 2) | block_type=00
    ///                       = 0x80 | 0x04 | 0x00 = 0x84.
    /// b1 = 0x0C.
    /// regen = (0x84 >> 4) | (0x0C << 4) = 0x8 | 0xC0 = 0xC8 = 200. ✓
    #[test]
    fn sp127_kat_raw_size_format_01_two_byte_header() {
        let h = parse_literals_header(&[0x84u8, 0x0Cu8]).unwrap();
        assert_eq!(h.block_type, LiteralsBlockType::Raw);
        assert_eq!(h.regenerated_size, 200);
        assert_eq!(h.header_len, 2);
    }

    /// SP127-KAT-3: Raw / size_format=11 / 3-byte header.
    /// regen_size to encode: 100_000 (20-bit field).
    /// 100_000 = 0x186A0.
    ///   bits  3..0  = 0x0 (low nibble of 100_000 = 0)
    ///   bits 11..4  = 0x6A (next 8 bits)
    ///   bits 19..12 = 0x18 (next 8 bits)
    /// b0 = (0x0 << 4) | (size_format=11 << 2) | block_type=00 = 0x00 | 0x0C | 0x00 = 0x0C.
    /// b1 = 0x6A.
    /// b2 = 0x18.
    /// regen = (0x0C >> 4) | (0x6A << 4) | (0x18 << 12) = 0 | 0x6A0 | 0x18000 = 0x186A0 = 100_000. ✓
    #[test]
    fn sp127_kat_raw_size_format_11_three_byte_header() {
        let h = parse_literals_header(&[0x0Cu8, 0x6Au8, 0x18u8]).unwrap();
        assert_eq!(h.block_type, LiteralsBlockType::Raw);
        assert_eq!(h.regenerated_size, 100_000);
        assert_eq!(h.header_len, 3);
    }

    /// SP127-KAT-4: RLE / size_format=00 / 1-byte header (regen=5).
    /// b0 = (5 << 3) | (size_format=00 << 2) | block_type=01
    ///    = 0x28 | 0x00 | 0x01 = 0x29.
    #[test]
    fn sp127_kat_rle_size_format_00_one_byte_header() {
        let h = parse_literals_header(&[0x29u8]).unwrap();
        assert_eq!(h.block_type, LiteralsBlockType::Rle);
        assert_eq!(h.regenerated_size, 5);
        assert_eq!(h.header_len, 1);
    }

    /// SP127-KAT-5: Compressed / size_format=00 / 3-byte / 1-stream.
    /// regen=100, comp=80 (both 10-bit).
    /// Combined 24-bit LE field:
    ///   bits  0-1 : block_type=10 (Compressed)
    ///   bits  2-3 : size_format=00
    ///   bits  4-13: regen=100 (binary 00_0110_0100)
    ///   bits 14-23: comp=80 (binary 00_0101_0000)
    /// combined = (100 << 4) | (80 << 14) | 0b0010
    ///          = 0x640 | 0x140000 | 0x02
    ///          = 0x140642
    /// b0 = 0x42, b1 = 0x06, b2 = 0x14.
    #[test]
    fn sp127_kat_compressed_size_format_00_three_byte_one_stream() {
        let h = parse_literals_header(&[0x42u8, 0x06u8, 0x14u8]).unwrap();
        assert_eq!(h.block_type, LiteralsBlockType::Compressed);
        assert_eq!(h.regenerated_size, 100);
        assert_eq!(h.compressed_size, 80);
        assert_eq!(h.num_streams, 1);
        assert_eq!(h.header_len, 3);
    }

    /// SP127-KAT-6: Compressed / size_format=01 / 3-byte / 4-stream.
    /// regen=200, comp=150, block_type=10, size_format=01.
    /// combined = (200 << 4) | (150 << 14) | (0b01 << 2) | 0b10
    ///          = 0xC80 | 0x258000 | 0x04 | 0x02
    ///          = 0x258C86
    /// b0 = 0x86, b1 = 0x8C, b2 = 0x25.
    #[test]
    fn sp127_kat_compressed_size_format_01_three_byte_four_stream() {
        let h = parse_literals_header(&[0x86u8, 0x8Cu8, 0x25u8]).unwrap();
        assert_eq!(h.block_type, LiteralsBlockType::Compressed);
        assert_eq!(h.regenerated_size, 200);
        assert_eq!(h.compressed_size, 150);
        assert_eq!(h.num_streams, 4);
        assert_eq!(h.header_len, 3);
    }

    /// SP127-KAT-7: Compressed / size_format=10 / 4-byte / 4-stream.
    /// regen=10000 (14-bit), comp=8000 (14-bit), block_type=10, size_format=10.
    /// combined = (10000 << 4) | (8000 << 18) | (0b10 << 2) | 0b10
    ///          = 0x27100 | 0x7D000000 | 0x08 | 0x02
    ///          = 0x7D02710A
    /// b0 = 0x0A, b1 = 0x71, b2 = 0x02, b3 = 0x7D.
    #[test]
    fn sp127_kat_compressed_size_format_10_four_byte_header() {
        let h = parse_literals_header(&[0x0Au8, 0x71u8, 0x02u8, 0x7Du8]).unwrap();
        assert_eq!(h.block_type, LiteralsBlockType::Compressed);
        assert_eq!(h.regenerated_size, 10000);
        assert_eq!(h.compressed_size, 8000);
        assert_eq!(h.num_streams, 4);
        assert_eq!(h.header_len, 4);
    }

    /// SP127-KAT-8: Treeless / size_format=00 / 3-byte / 1-stream.
    /// regen=50, comp=40, block_type=11 (Treeless), size_format=00.
    /// combined = (50 << 4) | (40 << 14) | (0b00 << 2) | 0b11
    ///          = 0x320 | 0xA0000 | 0x00 | 0x03
    ///          = 0xA0323
    /// b0 = 0x23, b1 = 0x03, b2 = 0x0A.
    #[test]
    fn sp127_kat_treeless_size_format_00_three_byte_one_stream() {
        let h = parse_literals_header(&[0x23u8, 0x03u8, 0x0Au8]).unwrap();
        assert_eq!(h.block_type, LiteralsBlockType::Treeless);
        assert_eq!(h.regenerated_size, 50);
        assert_eq!(h.compressed_size, 40);
        assert_eq!(h.num_streams, 1);
        assert_eq!(h.header_len, 3);
    }

    /// SP127-KAT-9: empty input → typed UnexpectedEof.
    #[test]
    fn sp127_kat_empty_input_traps() {
        assert_eq!(parse_literals_header(&[]).unwrap_err(), ZstdError::UnexpectedEof);
    }

    /// SP127-KAT-10: truncated 3-byte header → typed UnexpectedEof.
    #[test]
    fn sp127_kat_truncated_compressed_header_traps() {
        // size_format=00 Compressed wants 3 bytes; give 2.
        assert_eq!(
            parse_literals_header(&[0x42u8, 0x06u8]).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP127-KAT-11: regen_size beyond LITERALS_MAX_SIZE → typed bomb.
    /// Build a 3-byte Raw header with regen = 0xFFFFF = 1_048_575 (max for 20-bit).
    /// regen[3..0] = 0xF, regen[11..4] = 0xFF, regen[19..12] = 0xFF.
    /// b0 = (0xF << 4) | (0b11 << 2) | 0b00 = 0xF0 | 0x0C = 0xFC.
    /// b1 = 0xFF. b2 = 0xFF.
    #[test]
    fn sp127_kat_regen_beyond_cap_traps() {
        let err = parse_literals_header(&[0xFCu8, 0xFFu8, 0xFFu8]).unwrap_err();
        match err {
            ZstdError::DecompressionBomb { decoded, cap } => {
                assert_eq!(decoded, 1_048_575);
                assert_eq!(cap, LITERALS_MAX_SIZE);
            }
            other => panic!("expected DecompressionBomb; got {other:?}"),
        }
    }

    /// SP127-KAT-12: Raw literals decode = byte-copy.
    #[test]
    fn sp127_kat_decode_raw_literals_byte_copy() {
        let out = decode_raw_literals(b"abcdefghij_extra", 10).unwrap();
        assert_eq!(out, b"abcdefghij");
    }

    /// SP127-KAT-13: RLE literals decode = 1-byte repeat.
    #[test]
    fn sp127_kat_decode_rle_literals_repeat() {
        let out = decode_rle_literals(b"X_extra", 50).unwrap();
        assert_eq!(out.len(), 50);
        assert!(out.iter().all(|&b| b == b'X'));
    }

    /// SP127-KAT-14: Raw decode truncated payload → typed err.
    #[test]
    fn sp127_kat_decode_raw_literals_truncated_traps() {
        assert_eq!(
            decode_raw_literals(b"abc", 10).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP127-KAT-15: deterministic — same input twice → identical output.
    #[test]
    fn sp127_kat_decode_deterministic_repeat() {
        let r1 = decode_raw_literals(b"deterministic_x", 13).unwrap();
        let r2 = decode_raw_literals(b"deterministic_x", 13).unwrap();
        assert_eq!(r1, r2);
        let s1 = decode_rle_literals(b"Y", 100).unwrap();
        let s2 = decode_rle_literals(b"Y", 100).unwrap();
        assert_eq!(s1, s2);
    }
}
