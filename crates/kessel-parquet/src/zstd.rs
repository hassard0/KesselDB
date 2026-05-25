//! Pure-Rust zero-dep zstd decompressor for Parquet pages — **the
//! OBJ-2c-2 slice**.
//!
//! Authority: **RFC 8478** (Zstandard Compression and the application/zstd
//! Media Type) and the upstream `facebook/zstd` reference implementation
//! cross-checked. This module implements RFC 8478 §3 (frame format) +
//! §3.1.1.1 (frame header) + §3.1.1.2 (blocks) for the formats actually
//! produced by Parquet writers (single frame per page; raw + RLE + compressed
//! blocks).
//!
//! Honest scope per slice:
//!   - **SP125 (this commit)**: frame header decode + block header decode +
//!     raw-block + RLE-block decompression. Compressed blocks (the format
//!     Parquet actually USES in production) trap with typed
//!     `ZstdError::CompressedBlockNotYetSupported` — explicit deferral
//!     to the next slice. Real-world Parquet zstd files WILL trap on
//!     compressed blocks; this slice ships the harness + boundary lock.
//!     Subsequent slices SP126 (FSE bitstream + tables), SP127 (Huffman
//!     literals), SP128 (sequences section), SP129 (sequence execution)
//!     fill in the compressed-block decoder.
//!
//! Determinism: same input bytes → identical output bytes on every
//! invocation. No allocator non-determinism (we pre-size output by the
//! decoded Frame_Content_Size when present; otherwise grow Vec which is
//! still deterministic). No host calls / no clocks / no float.
//!
//! Bounds-checking: every byte read goes through the `Cursor` helper which
//! returns `ZstdError::UnexpectedEof` on overrun. Decompressed-size cap
//! `ZSTD_MAX_DECOMP` defends against decompression bombs (a 100-byte input
//! claiming a multi-GB Frame_Content_Size is rejected before allocation).
//!
//! Safety: `#![forbid(unsafe_code)]`; no panics on attacker bytes; typed
//! `ZstdError` for every failure mode.

#![allow(dead_code)]
#![forbid(unsafe_code)]

/// Hard cap on a single decompressed zstd frame. 64 MiB matches the
/// Snappy + GZIP page cap shipped at SP104/SP106. Defeats a
/// decompression-bomb header that lies about Frame_Content_Size.
pub(crate) const ZSTD_MAX_DECOMP: usize = 64 << 20;

/// zstd frame magic per RFC 8478 §3.1.1: `0xFD 0x2F 0xB5 0x28` on the
/// wire (LE u32 = `0xFD2FB528`). Encoded as the BYTE SEQUENCE in the
/// stream — the first 4 bytes of any zstd frame.
pub(crate) const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Typed errors. `#[non_exhaustive]` so future slices (compressed-block
/// + skipping frames) can add variants without breaking changes.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZstdError {
    /// Input ran past its declared length mid-decode.
    UnexpectedEof,
    /// First 4 bytes are not the zstd frame magic. Surfaces what we saw.
    BadMagic([u8; 4]),
    /// Reserved bit in the Frame_Header_Descriptor is set (RFC 8478 §3.1.1.1
    /// bit 2 must be 0). Carries the descriptor byte for diagnostics.
    ReservedFrameHeaderBit(u8),
    /// Dictionary IDs are not supported in this slice (Parquet zstd never
    /// uses them in production; full RFC support is a follow-up).
    DictionaryNotSupported(u32),
    /// Frame Content Size claimed > ZSTD_MAX_DECOMP. Bomb defense.
    FrameContentSizeTooLarge { claimed: u64, cap: usize },
    /// Block_Type = 3 (Reserved per RFC). Always invalid.
    ReservedBlockType,
    /// Block_Size > Block_Maximum_Size (128 KiB for raw + RLE blocks per
    /// RFC 8478 §3.1.1.2). Carries the claimed size for diagnostics.
    BlockSizeTooLarge { claimed: usize, max: usize },
    /// Compressed-block decoding is NOT YET IMPLEMENTED in this slice
    /// (SP125 scaffold). The block header is correctly decoded; the
    /// payload (literals + sequences) is the SP126-SP129 follow-up work.
    /// Carries the block's Block_Size for diagnostics.
    CompressedBlockNotYetSupported { block_size: usize },
    /// Frame ended (Last_Block bit set) but decoded size doesn't match
    /// the declared Frame_Content_Size.
    SizeMismatch { declared: u64, decoded: usize },
    /// Decoded output would exceed ZSTD_MAX_DECOMP mid-block.
    DecompressionBomb { decoded: usize, cap: usize },
    /// Trailing Content_Checksum (4 bytes XXH64-low) ran past input.
    /// We DECODE+VERIFY the size of the trailer; full XXH64 verification
    /// is deferred to a follow-up (the decoded bytes are the
    /// authoritative output regardless).
    TrailingChecksumTruncated,
    /// Literals section block type is recognized but the decoder for it
    /// is not yet implemented in the current slice. Carries the type
    /// discriminator (RFC §5.3.1.1: 0=Raw, 1=RLE, 2=Compressed, 3=Treeless).
    /// Compressed/Treeless modes (which need Huffman) land at SP129.
    LiteralsBlockTypeNotYetSupported { block_type: u8 },
    /// Huffman tree description uses the FSE-weight encoding (header byte
    /// 0..=127); decoder for that path defers to SP129 paired with the
    /// Huffman bitstream + Compressed/Treeless literal payload decoder.
    FseWeightHuffmanNotYetSupported { header_byte: u8 },
}

impl core::fmt::Display for ZstdError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ZstdError {}

/// Bounds-checked byte reader.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    fn pos(&self) -> usize {
        self.pos
    }
    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }
    fn read_byte(&mut self) -> Result<u8, ZstdError> {
        let b = *self.buf.get(self.pos).ok_or(ZstdError::UnexpectedEof)?;
        self.pos += 1;
        Ok(b)
    }
    fn read_n(&mut self, n: usize) -> Result<&'a [u8], ZstdError> {
        let end = self.pos.checked_add(n).ok_or(ZstdError::UnexpectedEof)?;
        let s = self.buf.get(self.pos..end).ok_or(ZstdError::UnexpectedEof)?;
        self.pos = end;
        Ok(s)
    }
    fn read_u16_le(&mut self) -> Result<u16, ZstdError> {
        let b = self.read_n(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }
    fn read_u32_le(&mut self) -> Result<u32, ZstdError> {
        let b = self.read_n(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn read_u64_le(&mut self) -> Result<u64, ZstdError> {
        let b = self.read_n(8)?;
        Ok(u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }
}

/// Parsed frame header per RFC 8478 §3.1.1.1.
#[derive(Debug, Clone, Copy)]
struct FrameHeader {
    /// Some(N) iff Frame_Content_Size was declared in the header.
    /// `None` means "size unknown" (rare; Parquet writers always declare).
    frame_content_size: Option<u64>,
    /// True iff Single_Segment_flag is set. When true, the Window_Descriptor
    /// is OMITTED and the FCS field is required + present + serves as window.
    single_segment: bool,
    /// True iff a 4-byte XXH64-low checksum trails the last block.
    content_checksum: bool,
    /// Effective window size in bytes (defines max back-reference distance).
    /// When single_segment: window_size = frame_content_size.
    /// Otherwise: derived from Window_Descriptor byte (RFC §3.1.1.1.2).
    window_size: u64,
}

fn decode_frame_header(c: &mut Cursor) -> Result<FrameHeader, ZstdError> {
    let fhd = c.read_byte()?;
    // RFC 8478 §3.1.1.1.1 Frame_Header_Descriptor bit layout:
    //   bits 7-6 : Frame_Content_Size_flag
    //   bit  5   : Single_Segment_flag
    //   bit  4   : Unused_bit
    //   bit  3   : Reserved_bit  (must be 0; nonzero = malformed frame)
    //   bit  2   : Content_Checksum_flag
    //   bits 1-0 : Dictionary_ID_flag
    let fcs_flag = (fhd >> 6) & 0b11;
    let single_segment = (fhd >> 5) & 0b1 != 0;
    let reserved = (fhd >> 3) & 0b1;
    if reserved != 0 {
        return Err(ZstdError::ReservedFrameHeaderBit(fhd));
    }
    let content_checksum = (fhd >> 2) & 0b1 != 0;
    let dict_id_flag = fhd & 0b11;

    // Window_Descriptor — omitted when single_segment is set.
    let window_size: u64 = if single_segment {
        // Size will be filled in from FCS below; placeholder.
        0
    } else {
        let wd = c.read_byte()?;
        // Per RFC §3.1.1.1.2:
        //   Exponent = (wd >> 3) & 0x1F
        //   Mantissa = wd & 0x07
        //   Window_Log = 10 + Exponent
        //   Window_Base = 1 << Window_Log
        //   Window_Add = (Window_Base / 8) * Mantissa
        //   Window_Size = Window_Base + Window_Add
        let exponent = (wd >> 3) & 0x1F;
        let mantissa = (wd & 0x07) as u64;
        let window_log = 10u32 + exponent as u32;
        if window_log > 41 {
            // Window too large; declare bomb to avoid huge OOM-risk windows.
            return Err(ZstdError::FrameContentSizeTooLarge {
                claimed: 1u64 << 41,
                cap: ZSTD_MAX_DECOMP,
            });
        }
        let window_base = 1u64 << window_log;
        window_base + (window_base / 8) * mantissa
    };

    // Dictionary_ID — present iff dict_id_flag != 0.
    if dict_id_flag != 0 {
        let n_bytes = match dict_id_flag {
            1 => 1usize,
            2 => 2,
            3 => 4,
            _ => unreachable!(),
        };
        let mut id: u32 = 0;
        for (i, &b) in c.read_n(n_bytes)?.iter().enumerate() {
            id |= (b as u32) << (i * 8);
        }
        return Err(ZstdError::DictionaryNotSupported(id));
    }

    // Frame_Content_Size — present iff fcs_flag != 0 OR single_segment is set.
    let fcs: Option<u64> = match (fcs_flag, single_segment) {
        (0, false) => None,
        (0, true) => Some(c.read_byte()? as u64), // 1 byte
        (1, _) => Some(c.read_u16_le()? as u64 + 256), // 2 bytes + 256 per RFC §3.1.1.1.4
        (2, _) => Some(c.read_u32_le()? as u64), // 4 bytes
        (3, _) => Some(c.read_u64_le()?), // 8 bytes
        _ => unreachable!(),
    };

    // Bomb defense.
    if let Some(n) = fcs {
        if n > ZSTD_MAX_DECOMP as u64 {
            return Err(ZstdError::FrameContentSizeTooLarge {
                claimed: n,
                cap: ZSTD_MAX_DECOMP,
            });
        }
    }

    let window_size = if single_segment {
        fcs.unwrap_or(0)
    } else {
        window_size
    };

    Ok(FrameHeader {
        frame_content_size: fcs,
        single_segment,
        content_checksum,
        window_size,
    })
}

/// Block_Maximum_Size per RFC 8478 §3.1.1.2 — 128 KiB. Applies to
/// raw + RLE + compressed (decompressed-size cap).
pub(crate) const BLOCK_MAX_SIZE: usize = 128 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockType {
    Raw,
    Rle,
    Compressed,
}

#[derive(Debug, Clone, Copy)]
struct BlockHeader {
    last_block: bool,
    block_type: BlockType,
    /// For Raw + Compressed: the number of BYTES in the block payload.
    /// For RLE: ALWAYS 1 byte payload (the RLE byte); `block_size`
    /// carries the REPEAT count (= decompressed size).
    block_size: usize,
}

fn decode_block_header(c: &mut Cursor) -> Result<BlockHeader, ZstdError> {
    let b = c.read_n(3)?;
    // 3-byte little-endian header per RFC §3.1.1.2:
    //   bit 0       : Last_Block
    //   bits 1-2    : Block_Type
    //   bits 3-23   : Block_Size (21 bits)
    let hdr = (b[0] as u32) | ((b[1] as u32) << 8) | ((b[2] as u32) << 16);
    let last_block = (hdr & 0x1) != 0;
    let bt = (hdr >> 1) & 0x3;
    let size = (hdr >> 3) as usize;
    let block_type = match bt {
        0 => BlockType::Raw,
        1 => BlockType::Rle,
        2 => BlockType::Compressed,
        _ => return Err(ZstdError::ReservedBlockType),
    };
    // Block_Maximum_Size enforced PER spec §3.1.1.2:
    //   Raw / Compressed:   block_size IS the byte count in the input stream
    //                       AND the decompressed output is bounded by
    //                       Block_Maximum_Size (128 KiB).
    //   RLE:                block_size IS the decompressed (repeat) count;
    //                       payload is exactly 1 input byte.
    // For Raw the input byte count == output byte count, so a single check
    // against BLOCK_MAX_SIZE is sufficient for the input-side bound. For RLE
    // the cap bounds the output. For Compressed the input cap is also
    // BLOCK_MAX_SIZE per spec (the COMPRESSED size never exceeds this).
    if size > BLOCK_MAX_SIZE {
        return Err(ZstdError::BlockSizeTooLarge {
            claimed: size,
            max: BLOCK_MAX_SIZE,
        });
    }
    Ok(BlockHeader { last_block, block_type, block_size: size })
}

/// Cross-block decoder state per RFC 8478 §3.1.1.2:
///   - Compressed blocks may use Treeless literals (no inline Huffman
///     tree description; reuses the PREVIOUS Compressed block's tree).
///   - Sequences may use Repeat mode for LL/OF/ML FSE tables (reuses
///     the PREVIOUS Compressed block's table for that code).
///   - The 3-slot repeat-offset window carries across all blocks in
///     a frame.
struct ZstdDecoderState {
    prev_huffman_tree: Option<crate::zstd_huffman::HuffmanTree>,
    prev_ll_table: Option<crate::zstd_fse::FseTable>,
    prev_of_table: Option<crate::zstd_fse::FseTable>,
    prev_ml_table: Option<crate::zstd_fse::FseTable>,
    repeats: crate::zstd_seqexec::RepeatOffsets,
}

impl ZstdDecoderState {
    fn new() -> Self {
        Self {
            prev_huffman_tree: None,
            prev_ll_table: None,
            prev_of_table: None,
            prev_ml_table: None,
            repeats: crate::zstd_seqexec::RepeatOffsets::new(),
        }
    }
}

/// Decode one Compressed block per RFC §5.3-§5.4. `block_bytes` is the
/// `block_size` bytes following the 3-byte block header; emit the
/// decoded bytes into `out`. Mutates the cross-block state to track
/// the Huffman tree (for Treeless) and LL/OF/ML FSE tables (for
/// Repeat) for any subsequent Compressed blocks in this frame.
fn decompress_compressed_block(
    block_bytes: &[u8],
    state: &mut ZstdDecoderState,
    out: &mut Vec<u8>,
) -> Result<(), ZstdError> {
    use crate::zstd_huffstream::{decode_compressed_literals, decode_treeless_literals};
    use crate::zstd_literals::{
        decode_raw_literals, decode_rle_literals, parse_literals_header, LiteralsBlockType,
    };
    use crate::zstd_sequences::{
        decode_sequences_stream, load_fse_table_for_mode, parse_sequences_header, SeqSymbolClass,
    };
    use crate::zstd_seqexec::execute_sequences;

    // 1. Literals section.
    let lit_header = parse_literals_header(block_bytes)?;
    let (literals, lit_consumed) = match lit_header.block_type {
        LiteralsBlockType::Raw => {
            let after_header = &block_bytes[lit_header.header_len..];
            let lit = decode_raw_literals(after_header, lit_header.regenerated_size as usize)?;
            (lit, lit_header.header_len + lit_header.regenerated_size as usize)
        }
        LiteralsBlockType::Rle => {
            let after_header = &block_bytes[lit_header.header_len..];
            let lit = decode_rle_literals(after_header, lit_header.regenerated_size as usize)?;
            (lit, lit_header.header_len + 1)
        }
        LiteralsBlockType::Compressed => {
            let (decoded, consumed) = decode_compressed_literals(block_bytes)?;
            // Capture the Huffman tree for any subsequent Treeless block.
            // decode_compressed_literals doesn't return the tree directly;
            // we re-parse it here to keep the API simple (the tree
            // description bytes start at lit_header.header_len).
            let tree_bytes = &block_bytes[lit_header.header_len
                ..lit_header.header_len + lit_header.compressed_size as usize];
            let (tree, _) = crate::zstd_huffman::parse_huffman_tree(tree_bytes)?;
            state.prev_huffman_tree = Some(tree);
            (decoded, consumed)
        }
        LiteralsBlockType::Treeless => {
            let prev_tree = state
                .prev_huffman_tree
                .as_ref()
                .ok_or(ZstdError::UnexpectedEof)?;
            let (decoded, consumed) = decode_treeless_literals(block_bytes, prev_tree)?;
            (decoded, consumed)
        }
    };

    // 2. Sequences section.
    let after_literals = &block_bytes[lit_consumed..];
    let seq_header = parse_sequences_header(after_literals)?;
    if seq_header.num_sequences == 0 {
        // No sequences → block output = literals only.
        if out.len().saturating_add(literals.len()) > ZSTD_MAX_DECOMP {
            return Err(ZstdError::DecompressionBomb {
                decoded: out.len() + literals.len(),
                cap: ZSTD_MAX_DECOMP,
            });
        }
        out.extend_from_slice(&literals);
        return Ok(());
    }

    // 3. Load LL/OF/ML FSE tables per their mode codes.
    let after_seq_header = &after_literals[seq_header.header_len..];
    let (ll_table, ll_consumed) = load_fse_table_for_mode(
        SeqSymbolClass::LiteralLength,
        seq_header.ll_mode,
        after_seq_header,
        state.prev_ll_table.as_ref(),
    )?;
    let after_ll = &after_seq_header[ll_consumed..];
    let (of_table, of_consumed) = load_fse_table_for_mode(
        SeqSymbolClass::Offset,
        seq_header.of_mode,
        after_ll,
        state.prev_of_table.as_ref(),
    )?;
    let after_of = &after_ll[of_consumed..];
    let (ml_table, ml_consumed) = load_fse_table_for_mode(
        SeqSymbolClass::MatchLength,
        seq_header.ml_mode,
        after_of,
        state.prev_ml_table.as_ref(),
    )?;
    let bitstream = &after_of[ml_consumed..];

    // 4. Decode the sequence stream.
    let sequences =
        decode_sequences_stream(bitstream, &ll_table, &of_table, &ml_table, seq_header.num_sequences)?;

    // Capture tables for subsequent Repeat-mode blocks.
    state.prev_ll_table = Some(ll_table);
    state.prev_of_table = Some(of_table);
    state.prev_ml_table = Some(ml_table);

    // 5. Execute sequences (literals copy + LZ77 back-reference).
    execute_sequences(&sequences, &literals, &mut state.repeats, out, ZSTD_MAX_DECOMP)?;
    Ok(())
}

/// Decompress a single zstd-framed input.
///
/// **SP136**: full pipeline — frame header + Raw + RLE + Compressed
/// blocks (all 4 literal modes × both Huffman tree paths × 1/4-stream
/// + all 4 sequence-table modes + LZ77 sequence execution + 3-slot
/// repeat-offset window). Cross-block state (prev Huffman tree for
/// Treeless / prev FSE tables for Repeat / repeats) tracked per frame.
pub fn decompress(input: &[u8]) -> Result<Vec<u8>, ZstdError> {
    let mut c = Cursor::new(input);

    let magic_bytes = c.read_n(4)?;
    let mut magic = [0u8; 4];
    magic.copy_from_slice(magic_bytes);
    if magic != ZSTD_MAGIC {
        return Err(ZstdError::BadMagic(magic));
    }

    let header = decode_frame_header(&mut c)?;

    let mut out: Vec<u8> = match header.frame_content_size {
        Some(n) => Vec::with_capacity(n as usize),
        None => Vec::new(),
    };
    let mut state = ZstdDecoderState::new();

    loop {
        let block_header = decode_block_header(&mut c)?;
        match block_header.block_type {
            BlockType::Raw => {
                let payload = c.read_n(block_header.block_size)?;
                if out.len().saturating_add(payload.len()) > ZSTD_MAX_DECOMP {
                    return Err(ZstdError::DecompressionBomb {
                        decoded: out.len() + payload.len(),
                        cap: ZSTD_MAX_DECOMP,
                    });
                }
                out.extend_from_slice(payload);
            }
            BlockType::Rle => {
                let byte = c.read_byte()?;
                let count = block_header.block_size;
                if out.len().saturating_add(count) > ZSTD_MAX_DECOMP {
                    return Err(ZstdError::DecompressionBomb {
                        decoded: out.len() + count,
                        cap: ZSTD_MAX_DECOMP,
                    });
                }
                out.extend(core::iter::repeat(byte).take(count));
            }
            BlockType::Compressed => {
                let block_bytes = c.read_n(block_header.block_size)?;
                decompress_compressed_block(block_bytes, &mut state, &mut out)?;
            }
        }
        if block_header.last_block {
            break;
        }
    }

    // FCS check (when declared).
    if let Some(declared) = header.frame_content_size {
        if out.len() as u64 != declared {
            return Err(ZstdError::SizeMismatch {
                declared,
                decoded: out.len(),
            });
        }
    }

    // Content_Checksum (4 bytes XXH64-low) if declared. Size-check only;
    // full XXH64 verification deferred (the decompressed bytes are the
    // authoritative output; the checksum is a transport-integrity
    // mechanism that Parquet writers may or may not produce).
    if header.content_checksum {
        if c.remaining() < 4 {
            return Err(ZstdError::TrailingChecksumTruncated);
        }
        let _ck = c.read_u32_le()?;
    }

    Ok(out)
}

// ============================================================================
// KATs — hand-derived from RFC 8478 with byte-level annotations.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal zstd frame envelope (magic + minimum-form header
    /// + a single Raw last-block + payload). Used by raw-block KATs.
    fn build_raw_block_frame(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        // Magic
        out.extend_from_slice(&ZSTD_MAGIC);
        // Frame header descriptor:
        //   single_segment=1 (no window descriptor)
        //   FCS_flag=0 (with single_segment → 1-byte FCS field)
        //   no checksum, no dict
        //   bit 5 = single_segment = 1; rest = 0
        let fhd = 0b00100000u8;
        out.push(fhd);
        // FCS = payload.len() as u8 (single_segment+FCS_flag=0 → 1 byte FCS)
        assert!(payload.len() < 256, "test helper: payload too large for 1-byte FCS");
        out.push(payload.len() as u8);
        // Block header: Last_Block=1, Block_Type=Raw(0), Block_Size=payload.len()
        let hdr = 0x1u32 | ((payload.len() as u32) << 3);
        out.push((hdr & 0xFF) as u8);
        out.push(((hdr >> 8) & 0xFF) as u8);
        out.push(((hdr >> 16) & 0xFF) as u8);
        // Payload
        out.extend_from_slice(payload);
        out
    }

    /// SP125-KAT-1: minimal raw-block roundtrip (5-byte payload).
    #[test]
    fn sp125_kat_raw_block_5_bytes() {
        let payload = b"hello";
        let frame = build_raw_block_frame(payload);
        assert_eq!(decompress(&frame).unwrap(), payload);
    }

    /// SP125-KAT-2: empty raw block decodes to empty output.
    #[test]
    fn sp125_kat_raw_block_empty() {
        let frame = build_raw_block_frame(&[]);
        assert_eq!(decompress(&frame).unwrap(), b"");
    }

    /// SP125-KAT-3: bad magic → BadMagic with the bytes seen.
    #[test]
    fn sp125_kat_bad_magic() {
        let bad = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00];
        match decompress(&bad).unwrap_err() {
            ZstdError::BadMagic(bytes) => {
                assert_eq!(bytes, [0xDE, 0xAD, 0xBE, 0xEF]);
            }
            other => panic!("expected BadMagic; got {other:?}"),
        }
    }

    /// SP125-KAT-4: RLE block — 1-byte payload (the byte to repeat),
    /// block_size = repeat count. Build a 200-byte 'X' RLE.
    #[test]
    fn sp125_kat_rle_block_200_bytes() {
        let mut out = Vec::new();
        out.extend_from_slice(&ZSTD_MAGIC);
        // single_segment=1, FCS_flag=0 → 1-byte FCS field
        out.push(0b00100000u8);
        out.push(200u8); // FCS
        // Block header: Last_Block=1, Block_Type=RLE(1), Block_Size=200 (repeat count)
        let hdr = 0x1u32 | (1u32 << 1) | (200u32 << 3);
        out.push((hdr & 0xFF) as u8);
        out.push(((hdr >> 8) & 0xFF) as u8);
        out.push(((hdr >> 16) & 0xFF) as u8);
        // Payload: 1 byte = the byte to repeat
        out.push(b'X');
        let decoded = decompress(&out).unwrap();
        assert_eq!(decoded.len(), 200);
        assert!(decoded.iter().all(|&b| b == b'X'));
    }

    /// SP125-KAT-5: multi-block frame (3 raw blocks). Last block has
    /// Last_Block bit set. Each block contributes to output in order.
    #[test]
    fn sp125_kat_multi_block_frame() {
        let mut out = Vec::new();
        out.extend_from_slice(&ZSTD_MAGIC);
        // single_segment=1, FCS_flag=0 → 1-byte FCS
        out.push(0b00100000u8);
        out.push(15u8); // FCS = 5 + 5 + 5
        // 3 raw blocks of 5 bytes each
        for (i, payload) in [b"AAAAA", b"BBBBB", b"CCCCC"].iter().enumerate() {
            let last = if i == 2 { 1u32 } else { 0 };
            let hdr = last | (5u32 << 3);
            out.push((hdr & 0xFF) as u8);
            out.push(((hdr >> 8) & 0xFF) as u8);
            out.push(((hdr >> 16) & 0xFF) as u8);
            out.extend_from_slice(*payload);
        }
        assert_eq!(decompress(&out).unwrap(), b"AAAAABBBBBCCCCC");
    }

    /// SP125-KAT-6: reserved block type (3) → ReservedBlockType.
    #[test]
    fn sp125_kat_reserved_block_type_traps() {
        let mut out = Vec::new();
        out.extend_from_slice(&ZSTD_MAGIC);
        out.push(0b00100000u8);
        out.push(0u8); // FCS = 0
        // Block_Type = 3 (reserved); Last_Block=1; Block_Size=0
        let hdr = 0x1u32 | (3u32 << 1);
        out.push((hdr & 0xFF) as u8);
        out.push(((hdr >> 8) & 0xFF) as u8);
        out.push(((hdr >> 16) & 0xFF) as u8);
        assert_eq!(decompress(&out).unwrap_err(), ZstdError::ReservedBlockType);
    }

    /// SP125-KAT-7 (revised by SP136): compressed block path is now
    /// WIRED through the full pipeline. A zero-byte compressed block
    /// has no literals/sequences sections to parse → typed UnexpectedEof
    /// on the empty literals header parse. The KAT now locks the wire:
    /// the Compressed arm no longer traps with the SP125-scaffold
    /// `CompressedBlockNotYetSupported` marker; instead it dispatches
    /// to the SP136 driver which reports a typed pipeline error on
    /// malformed input.
    #[test]
    fn sp125_kat_compressed_block_wired_by_sp136() {
        let mut out = Vec::new();
        out.extend_from_slice(&ZSTD_MAGIC);
        out.push(0b00100000u8);
        out.push(0u8);
        // Block_Type = 2 (Compressed); Last_Block=1; Block_Size=0
        let hdr = 0x1u32 | (2u32 << 1);
        out.push((hdr & 0xFF) as u8);
        out.push(((hdr >> 8) & 0xFF) as u8);
        out.push(((hdr >> 16) & 0xFF) as u8);
        // An empty compressed block has no valid literals section header
        // (the first byte is missing) → typed UnexpectedEof from the
        // SP136-wired pipeline.
        assert_eq!(
            decompress(&out).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP125-KAT-8: dictionary-ID frame is rejected with typed error
    /// (Parquet zstd never uses dictionaries).
    #[test]
    fn sp125_kat_dictionary_rejected() {
        let mut out = Vec::new();
        out.extend_from_slice(&ZSTD_MAGIC);
        // single_segment=1, FCS_flag=0, dict_id_flag=1 (1-byte dict id)
        let fhd = 0b00100001u8;
        out.push(fhd);
        out.push(0x42u8); // dict ID = 0x42
        out.push(0u8);    // FCS = 0
        match decompress(&out).unwrap_err() {
            ZstdError::DictionaryNotSupported(0x42) => {}
            other => panic!("expected DictionaryNotSupported(0x42); got {other:?}"),
        }
    }

    /// SP125-KAT-9: bomb defense — declared Frame_Content_Size beyond cap
    /// is rejected at header parse time (BEFORE allocation).
    #[test]
    fn sp125_kat_decompression_bomb_fcs_rejected() {
        let mut out = Vec::new();
        out.extend_from_slice(&ZSTD_MAGIC);
        // FCS_flag=3 (8 bytes) + single_segment=0 (window desc present)
        let fhd = 0b11000000u8;
        out.push(fhd);
        // Window descriptor: smallest legal (window_log=10)
        out.push(0x00);
        // 8-byte FCS = u64::MAX
        out.extend_from_slice(&u64::MAX.to_le_bytes());
        match decompress(&out).unwrap_err() {
            ZstdError::FrameContentSizeTooLarge { claimed: u64::MAX, .. } => {}
            other => panic!("expected FrameContentSizeTooLarge; got {other:?}"),
        }
    }

    /// SP125-KAT-10: reserved bit (bit 3) in Frame_Header_Descriptor
    /// is rejected per RFC 8478 §3.1.1.1.1.
    #[test]
    fn sp125_kat_reserved_bit_traps() {
        let mut out = Vec::new();
        out.extend_from_slice(&ZSTD_MAGIC);
        // single_segment=1 (bit 5), reserved-bit=1 (bit 3) → must trap.
        let fhd = 0b00101000u8;
        out.push(fhd);
        assert_eq!(
            decompress(&out).unwrap_err(),
            ZstdError::ReservedFrameHeaderBit(0b00101000u8)
        );
    }

    /// SP125-KAT-11: bounds-checked decode — truncated input gives a
    /// typed error, never a panic.
    #[test]
    fn sp125_kat_truncated_input_is_typed_error() {
        // Just the magic, no header → UnexpectedEof.
        let bytes = ZSTD_MAGIC.to_vec();
        assert_eq!(decompress(&bytes).unwrap_err(), ZstdError::UnexpectedEof);
    }

    /// SP125-KAT-12: block_size > 128 KiB → BlockSizeTooLarge.
    #[test]
    fn sp125_kat_block_size_too_large() {
        let mut out = Vec::new();
        out.extend_from_slice(&ZSTD_MAGIC);
        // single_segment=1, FCS_flag=2 (4-byte FCS)
        let fhd = 0b10100000u8;
        out.push(fhd);
        // FCS = a value that's at the cap so it doesn't bomb-trip first
        out.extend_from_slice(&((BLOCK_MAX_SIZE as u32) + 1).to_le_bytes());
        // Block header: Last_Block=1, Block_Type=Raw, Block_Size = 128 KiB + 1
        let bs = (BLOCK_MAX_SIZE + 1) as u32;
        let hdr = 0x1u32 | (bs << 3);
        out.push((hdr & 0xFF) as u8);
        out.push(((hdr >> 8) & 0xFF) as u8);
        out.push(((hdr >> 16) & 0xFF) as u8);
        match decompress(&out).unwrap_err() {
            ZstdError::BlockSizeTooLarge { claimed, max } => {
                assert_eq!(claimed, BLOCK_MAX_SIZE + 1);
                assert_eq!(max, BLOCK_MAX_SIZE);
            }
            other => panic!("expected BlockSizeTooLarge; got {other:?}"),
        }
    }

    /// SP125-KAT-13: content checksum trailer is size-checked.
    /// Frame with checksum flag but no trailing 4 bytes → typed error.
    #[test]
    fn sp125_kat_checksum_trailer_truncated() {
        let mut out = Vec::new();
        out.extend_from_slice(&ZSTD_MAGIC);
        // single_segment=1 (bit 5), FCS_flag=0, content_checksum=1 (bit 2)
        let fhd = 0b00100100u8;
        out.push(fhd);
        out.push(0u8); // FCS = 0
        // Block header: Last_Block=1, Raw, size=0
        out.push(0x01);
        out.push(0x00);
        out.push(0x00);
        // (no trailing checksum bytes — should trap)
        assert_eq!(
            decompress(&out).unwrap_err(),
            ZstdError::TrailingChecksumTruncated
        );
    }

    /// SP125-KAT-14: deterministic — same input twice = identical output.
    #[test]
    fn sp125_kat_deterministic_repeat() {
        let frame = build_raw_block_frame(b"deterministic");
        let r1 = decompress(&frame).unwrap();
        let r2 = decompress(&frame).unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r1, b"deterministic");
    }

    /// SP136-E2E-DIAG-2: the EXACT zstd frame from the pyarrow zstd_plain
    /// fixture (5 INT64 PLAIN). FHD=0x20 (single_segment=1, FCS_flag=0
    /// → 1-byte FCS=0x2e=46). One Compressed block of 30 bytes. Decoded
    /// by the reference zstd tool to 46 bytes.
    ///
    /// **SP137 FIXED**: the FSE `(nb_bits, base_state)` per-cell
    /// computation in SP126's `build_fse_table` was approximated with
    /// a max_state-overflow fallback that produced wrong `nb_bits` for
    /// power-of-two-count symbols. The corrected algorithm (canonical
    /// libzstd `FSE_buildDTable_internal`) uses
    /// `nb_bits = L - high_bit_position(next_state)` and
    /// `base_state = (next_state << nb_bits) - table_size`. The fix
    /// landed in SP137 and this test now passes — it locks the
    /// pyarrow-specific encoding-corner that surfaced the bug.
    #[test]
    fn sp136_kat_decode_pyarrow_parquet_frame() {
        let compressed: &[u8] = &[
            0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x2e, 0xf5, 0x00, 0x00,
            0xb0, 0x02, 0x00, 0x00, 0x00, 0x0a, 0x01, 0x01, 0x00,
            0x02, 0x00, 0x03, 0x00, 0x04, 0x00, 0x05, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x14, 0x00, 0x03,
            0x18, 0x63, 0x2e,
        ];
        let decoded = decompress(compressed).expect("decode pyarrow Parquet frame");
        // Expected: 6-byte prefix (likely Parquet PLAIN encoding overhead)
        // + 5 × u64-LE = 46 bytes total. The first 6 bytes should match
        // the reference tool's output: 02 00 00 00 0a 01.
        assert_eq!(decoded.len(), 46);
        assert_eq!(&decoded[0..6], &[0x02, 0x00, 0x00, 0x00, 0x0a, 0x01]);
        // INT64 values 1..=5 at offsets 6, 14, 22, 30, 38.
        for (i, v) in [1u64, 2, 3, 4, 5].iter().enumerate() {
            let off = 6 + i * 8;
            let bytes: [u8; 8] = decoded[off..off + 8].try_into().unwrap();
            assert_eq!(u64::from_le_bytes(bytes), *v);
        }
    }

    /// SP137-FIX-LOCK: step-by-step pipeline assertion through the
    /// pyarrow Parquet frame that originally surfaced the SP126 FSE
    /// `base_state` bug. Locks the full pipeline contract at each
    /// stage (literals header → literals payload → sequences header →
    /// LL/OF/ML FSE tables → sequence stream → execution) against
    /// known-correct intermediate values. Run as part of the regular
    /// gate (no `#[ignore]`).
    #[test]
    fn sp137_fix_lock_pyarrow_frame_pipeline() {
        use crate::zstd_literals::{
            decode_raw_literals, parse_literals_header, LiteralsBlockType,
        };
        use crate::zstd_sequences::{
            decode_sequences_stream, load_fse_table_for_mode, parse_sequences_header,
            SeqSymbolClass, SeqSymbolMode,
        };
        use crate::zstd_seqexec::{execute_sequences, RepeatOffsets};
        // 30-byte compressed block from the SP136 pyarrow zstd_plain
        // fixture (bytes 9..39 of the 39-byte zstd frame).
        let block: &[u8] = &[
            0xb0, 0x02, 0x00, 0x00, 0x00, 0x0a, 0x01, 0x01, 0x00, 0x02,
            0x00, 0x03, 0x00, 0x04, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x04, 0x14, 0x00, 0x03, 0x18, 0x63, 0x2e,
        ];
        // Literals: Raw, regen=22, 1-byte header.
        let lit_hdr = parse_literals_header(block).unwrap();
        assert_eq!(lit_hdr.block_type, LiteralsBlockType::Raw);
        assert_eq!(lit_hdr.regenerated_size, 22);
        assert_eq!(lit_hdr.header_len, 1);
        let literals =
            decode_raw_literals(&block[1..], 22).unwrap();
        assert_eq!(literals.len(), 22);
        // Sequences: 4 sequences, LL=Predefined, OF=RLE(0), ML=RLE(3).
        let seq_hdr = parse_sequences_header(&block[23..]).unwrap();
        assert_eq!(seq_hdr.num_sequences, 4);
        assert_eq!(seq_hdr.ll_mode, SeqSymbolMode::Predefined);
        assert_eq!(seq_hdr.of_mode, SeqSymbolMode::Rle);
        assert_eq!(seq_hdr.ml_mode, SeqSymbolMode::Rle);
        assert_eq!(seq_hdr.header_len, 2);
        // LL/OF/ML tables.
        let after = &block[25..];
        let (ll_table, ll_n) = load_fse_table_for_mode(
            SeqSymbolClass::LiteralLength, seq_hdr.ll_mode, after, None,
        ).unwrap();
        assert_eq!(ll_table.accuracy_log, 6);
        assert_eq!(ll_table.entries.len(), 64);
        // SP137 LOCK: LL[28] after the canonical FSE construction.
        assert_eq!(ll_table.entries[28].symbol, 8);
        assert_eq!(ll_table.entries[28].nb_bits, 5);
        assert_eq!(ll_table.entries[28].base_state, 0);
        let (of_table, of_n) = load_fse_table_for_mode(
            SeqSymbolClass::Offset, seq_hdr.of_mode, &after[ll_n..], None,
        ).unwrap();
        assert_eq!(of_table.accuracy_log, 0);
        assert_eq!(of_n, 1);
        let (ml_table, ml_n) = load_fse_table_for_mode(
            SeqSymbolClass::MatchLength, seq_hdr.ml_mode, &after[ll_n + of_n..], None,
        ).unwrap();
        assert_eq!(ml_table.accuracy_log, 0);
        assert_eq!(ml_n, 1);
        let bitstream = &after[ll_n + of_n + ml_n..];
        assert_eq!(bitstream.len(), 3); // [0x18, 0x63, 0x2e]
        // Sequence decode: expected [LL=8, LL=2, LL=2, LL=2] all with
        // offset=1 + ml=6 per the SP137 fix.
        let seqs = decode_sequences_stream(
            bitstream, &ll_table, &of_table, &ml_table, seq_hdr.num_sequences,
        ).unwrap();
        assert_eq!(seqs.len(), 4);
        assert_eq!(seqs[0].literal_length, 8);
        assert_eq!(seqs[1].literal_length, 2);
        assert_eq!(seqs[2].literal_length, 2);
        assert_eq!(seqs[3].literal_length, 2);
        for s in &seqs {
            assert_eq!(s.offset, 1);
            assert_eq!(s.match_length, 6);
        }
        // Execute: 46 bytes output matching the reference zstd tool.
        let mut repeats = RepeatOffsets::new();
        let mut out: Vec<u8> = Vec::new();
        let n = execute_sequences(
            &seqs, &literals, &mut repeats, &mut out, 64 * 1024,
        ).unwrap();
        assert_eq!(n, 46);
        // Spot-check the 5 INT64 values at the expected offsets.
        let expected_prefix = [0x02, 0x00, 0x00, 0x00, 0x0a, 0x01];
        assert_eq!(&out[0..6], &expected_prefix[..]);
        for (i, v) in [1u64, 2, 3, 4, 5].iter().enumerate() {
            let off = 6 + i * 8;
            let bytes: [u8; 8] = out[off..off + 8].try_into().unwrap();
            assert_eq!(u64::from_le_bytes(bytes), *v);
        }
    }

    /// SP136-E2E-DIAG-1: known-good zstd stream produced by the reference
    /// `zstd -3` CLI for input `"hello hello hello hello world\n"` (30 bytes).
    /// If this fails, the bug is in the SP125-SP135 decoder itself
    /// (not in the Parquet wire).
    #[test]
    fn sp136_kat_decode_reference_stream_hello() {
        let compressed: &[u8] = &[
            0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0x95, 0x00, 0x00,
            0x60, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f,
            0x72, 0x6c, 0x64, 0x0a, 0x01, 0x00, 0xf1, 0x4a, 0x11,
            0xa2, 0x6c, 0x06, 0x32,
        ];
        let decoded = decompress(compressed).expect("decode reference stream");
        assert_eq!(&decoded[..], b"hello hello hello hello world\n");
    }
}
