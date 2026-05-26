//! Pure-Rust zero-dep Brotli decompressor for Parquet pages — **SP154
//! sub-project**.
//!
//! Authority: **RFC 7932** (Brotli Compressed Data Format). Brotli is
//! Parquet codec id 4. Pre-SP154 the codec was recognized at meta-decode
//! time (SP150) and `page_payload` returned a typed `Unsupported` error
//! naming the dedicated multi-week SP-arc. SP154 IS that arc.
//!
//! ## Honest scope per slice
//!
//! Brotli is a much larger spec than zstd: ~129 pages of RFC vs ~30, and
//! the decoder needs static-dictionary support (≈122 KB of constants),
//! context modes, IMTF transform, insert-and-copy commands, and a ring
//! buffer with wraparound. SP154 ships in layers (each a self-contained
//! commit), modeled after the SP125-SP140 zstd arc:
//!
//!   - **Layer 1**: bit reader (`brotli_bit_reader.rs`).
//!   - **Layer 2**: stream header (WBITS).
//!   - **Layer 3**: metablock framing (ISLAST / ISLASTEMPTY / MNIBBLES /
//!     MLEN / ISUNCOMPRESSED).
//!   - **Layer 4**: uncompressed metablock body (byte-aligned raw copy).
//!   - **Layer 5**: Huffman tree decoding (simple + complex prefix codes).
//!   - Layers 6-12 (DEFERRED): block-type / block-length codes, distance
//!     code parameters, static dictionary, context modes, insert-and-copy
//!     commands, compressed-metablock orchestration, ring buffer.
//!
//! When the function is called from `page_payload`, this V1 successfully
//! decodes Brotli files that contain ONLY uncompressed metablocks (rare
//! but valid per RFC 7932 §9.2). Any compressed metablock surfaces as a
//! typed `BrotliError::CompressedMetablockNotYetSupported` with the
//! SP154-followup pointer; that error is converted to the existing
//! `PqError::Unsupported` at the call site so pyarrow-emitted files (which
//! ALWAYS use compressed metablocks) keep rejecting with a refined
//! message rather than a generic "decoder is a multi-week SP-arc".
//!
//! ## Determinism
//!
//! Same input bytes → identical output bytes on every invocation. No
//! allocator non-determinism (Vec growth is deterministic; no HashMap).
//! No host calls / no clocks / no float.
//!
//! ## Bounds-checking
//!
//! Every bit read goes through `BitReader` which returns
//! `BitReaderError::UnexpectedEof` on overrun. Decompressed-size cap
//! `BROTLI_MAX_DECOMP` defends against decompression bombs.
//!
//! ## Safety
//!
//! `#![forbid(unsafe_code)]`; no panics on attacker bytes; typed
//! `BrotliError` for every failure mode.

#![allow(dead_code)]
#![forbid(unsafe_code)]

use crate::brotli_bit_reader::{BitReader, BitReaderError};

/// Hard cap on a single decompressed Brotli page. Matches the
/// Snappy + GZIP + zstd + LZ4 page caps (SP151).
pub(crate) const BROTLI_MAX_DECOMP: usize = 256 << 20;

/// Typed errors for the Brotli decoder. `#[non_exhaustive]` so future
/// SP154 slices (compressed metablocks, static dictionary, IMTF) can add
/// variants without breaking changes.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BrotliError {
    /// Input ran past its declared length mid-decode.
    UnexpectedEof,
    /// Bit reader surfaced an internal error (e.g. requested > 32 bits).
    BitReader(BitReaderError),
    /// WBITS encoding was reserved (per RFC 7932 §9.1: the encoding
    /// `0,0,0,1` with the 4 leading-pattern bits all-zero means
    /// "next 3 bits encode WBITS-17 in {1..7}"; an encoded WBITS value
    /// of 9 is reserved). Carries the raw decoded value for diagnostics.
    ReservedWbits(u32),
    /// Compressed metablock decoder is not yet implemented in V1. The
    /// metablock framing (ISLAST/MNIBBLES/MLEN) IS correctly decoded;
    /// the literal/insert-and-copy/distance commands are the SP154
    /// follow-up work. Carries MLEN for diagnostics.
    CompressedMetablockNotYetSupported { mlen: u32 },
    /// Decoder output would exceed `BROTLI_MAX_DECOMP`.
    DecompressionBomb { decoded: usize, cap: usize },
    /// Final decoded size doesn't match the caller's expected size
    /// (Parquet page header carries `uncompressed_page_size`).
    SizeMismatch { expected: usize, actual: usize },
    /// Reserved bit in metablock framing was set (per RFC 7932 §9.2:
    /// when MNIBBLES=0, the next bit must be 0; otherwise = reserved).
    ReservedBit,
    /// Empty metablock declared MLEN > 0 (RFC 7932 §9.2: ISLASTEMPTY
    /// metablocks MUST NOT carry data; MLEN field is absent).
    InvalidEmptyMetablock,
    /// MNIBBLES=0 ⇒ MLEN omitted, but the spec disallows a non-last
    /// metablock with MNIBBLES=0. Carries ISLAST for diagnostics.
    NonLastMetablockMustHaveLength { islast: bool },
}

impl core::fmt::Display for BrotliError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for BrotliError {}

impl From<BitReaderError> for BrotliError {
    fn from(e: BitReaderError) -> Self {
        BrotliError::BitReader(e)
    }
}

/// Parsed stream header per RFC 7932 §9.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StreamHeader {
    /// Effective window log (10..=24). Window size = `(1 << wbits) - 16`.
    pub(crate) wbits: u8,
}

/// Decode the WBITS stream header per RFC 7932 §9.1.
///
/// Encoding (RFC 7932 §9.1):
///   - 1 bit '0'                              → WBITS = 16
///   - 1 bit '1' + 3 bits N (LSB-first, 1..=7) → WBITS = 17 + N (= 18..=24)
///   - 1 bit '1' + 3 bits '000' + 3 bits W:
///       W = 0..=5  → WBITS = 10 + W  (= 10..=15)
///       W = 6      → reserved
///       W = 7      → WBITS = 17
///
/// All KAT branches pinned by `stream_header_wbits_*` tests below.
pub(crate) fn decode_stream_header(r: &mut BitReader) -> Result<StreamHeader, BrotliError> {
    // RFC 7932 §9.1 table — implemented per the spec's prefix encoding
    // and cross-checked against the reference implementation.
    //
    // Read bit 0. If it's 0 → WBITS=16 (the most common short encoding;
    // this is the size pyarrow / brotli quality<11 typically emit for
    // ≤64 KiB pages).
    let b0 = r.read_one_bit()?;
    if b0 == 0 {
        return Ok(StreamHeader { wbits: 16 });
    }
    // bit-0 = 1. Read the next 3 bits.
    let n3 = r.read_bits(3)? as u8;
    if n3 != 0 {
        // 17 + N maps N=1..=7 to WBITS=18..=24.
        return Ok(StreamHeader { wbits: 17 + n3 });
    }
    // bit-0 = 1, next 3 = 0  →  pattern so far is '0001' in stream-order.
    // Per RFC 7932 §9.1 this branch reads 3 MORE bits encoding WBITS-10:
    //   3-bit value 0..=5  → WBITS = 10..=15  (N=0 → 10, N=1 → 11, ...,
    //                                            N=5 → 15)
    //   3-bit value 6      → reserved
    //   3-bit value 7      → WBITS = 17
    //
    // (NOTE: the reserved encoding 0b0001 in the original spec's notation
    // is a *4-bit* short read followed by 3-bit selectors; the value '0001'
    // shown in the RFC table corresponds to the case where the 3-bit
    // extension is also reserved. We surface this via ReservedWbits.)
    let n3b = r.read_bits(3)? as u8;
    let wbits = match n3b {
        0 => 10,
        1 => 11,
        2 => 12,
        3 => 13,
        4 => 14,
        5 => 15,
        6 => return Err(BrotliError::ReservedWbits(n3b as u32)),
        7 => 17,
        _ => unreachable!("read_bits(3) returns 0..=7"),
    };
    Ok(StreamHeader { wbits })
}

/// Parsed metablock header (the bits BEFORE the metablock body).
///
/// Per RFC 7932 §9.2 layout (corrected MNIBBLES table per the RFC):
///   ISLAST (1 bit)
///   if ISLAST: ISLASTEMPTY (1 bit). If 1 → end of stream (no body).
///   if !ISLASTEMPTY:
///     MNIBBLES_CODE (2 bits LSB-first) mapped via the RFC §9.2 table:
///       '00' → 4 nibbles
///       '01' → 5 nibbles
///       '10' → 6 nibbles
///       '11' → 0 (= skip-region metablock; empty body)
///     if MNIBBLES > 0:
///       MLEN = read(MNIBBLES * 4 bits) + 1
///       if !ISLAST: ISUNCOMPRESSED (1 bit)  (an ISLAST metablock is
///                                            always compressed)
///     else (MNIBBLES = 0):
///       reserved (1 bit, must be 0)
///       MSKIPLEN (2 bits)
///       if MSKIPLEN > 0: MSKIPBYTES = read(MSKIPLEN*8 bits) + 1
///       align to byte, skip MSKIPBYTES raw bytes; continue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetablockHeader {
    /// True iff this is the final metablock of the stream.
    pub(crate) is_last: bool,
    /// True iff `is_last && is_last_empty` — the stream ends with no
    /// further data.
    pub(crate) is_last_empty: bool,
    /// True iff the body is raw bytes (skip to byte boundary, then read
    /// MLEN bytes). False iff the body is compressed (RFC 7932 §9.3+).
    pub(crate) is_uncompressed: bool,
    /// Body length in bytes. 0 for empty metablocks. Up to 2^24 (16 MiB)
    /// for the largest single metablock per the 6-nibble encoding.
    pub(crate) mlen: u32,
}

/// Decode the metablock header per RFC 7932 §9.2.
///
/// MNIBBLES encoding (RFC 7932 §9.2 table) — the 2-bit field read
/// LSB-first maps to nibble counts via a NON-trivial mapping:
///   bit pattern '00' (LSB-first value 0) → MNIBBLES = 4
///   bit pattern '01' (LSB-first value 1) → MNIBBLES = 5
///   bit pattern '10' (LSB-first value 2) → MNIBBLES = 6
///   bit pattern '11' (LSB-first value 3) → MNIBBLES = 0 (empty / skip-region)
///
/// When MNIBBLES=0 (the '11' case), the metablock is a skip-region:
///   read 1 reserved bit (must be 0)
///   read 2 bits MSKIPLEN
///   if MSKIPLEN != 0: read MSKIPLEN*8 bits = MSKIPBYTES (else MSKIPBYTES=0)
///   align to byte boundary
///   skip MSKIPBYTES bytes
///   continue with next metablock header
pub(crate) fn decode_metablock_header(r: &mut BitReader) -> Result<MetablockHeader, BrotliError> {
    let is_last = r.read_one_bit()? != 0;
    let is_last_empty = if is_last { r.read_one_bit()? != 0 } else { false };
    if is_last_empty {
        return Ok(MetablockHeader {
            is_last: true,
            is_last_empty: true,
            is_uncompressed: false,
            mlen: 0,
        });
    }
    // Read MNIBBLES_CODE (2 bits, LSB-first integer 0..=3).
    let mnibbles_code = r.read_bits(2)? as u8;
    // Map to actual MNIBBLES count via the RFC 7932 §9.2 table.
    let n_nibbles: u8 = match mnibbles_code {
        0 => 4,
        1 => 5,
        2 => 6,
        3 => 0, // skip-region (empty metablock)
        _ => unreachable!("read_bits(2) returns 0..=3"),
    };
    if n_nibbles == 0 {
        // Skip-region metablock per RFC 7932 §9.2.
        let reserved = r.read_one_bit()?;
        if reserved != 0 {
            return Err(BrotliError::ReservedBit);
        }
        let mskiplen = r.read_bits(2)? as u8;
        let mskipbytes = if mskiplen == 0 {
            0
        } else {
            r.read_bits(mskiplen * 8)? + 1
        };
        // Align to byte boundary, skip mskipbytes bytes.
        r.align_to_byte();
        let _ = r.read_aligned_bytes(mskipbytes as usize)?;
        // Skip metablocks carry no data, are NOT compressed (no body),
        // and don't terminate the stream — they're a noop pass-through
        // for the caller's metablock loop. Return mlen=0 + uncompressed=true
        // so the loop just continues. If is_last is set, the caller still
        // breaks out (the canonical reference allows is_last on a skip
        // metablock — the stream ends after the skip).
        return Ok(MetablockHeader {
            is_last,
            is_last_empty: false,
            is_uncompressed: true,
            mlen: 0,
        });
    }
    let mlen_minus_1 = r.read_bits(n_nibbles * 4)?;
    let mlen = mlen_minus_1
        .checked_add(1)
        .ok_or(BrotliError::UnexpectedEof)?;
    let is_uncompressed = if !is_last {
        r.read_one_bit()? != 0
    } else {
        false
    };
    Ok(MetablockHeader {
        is_last,
        is_last_empty: false,
        is_uncompressed,
        mlen,
    })
}

/// Decode an NBLTYPES variable-length code per RFC 7932 §9.2.
///
/// Encoding (listed right-to-left = stream LSB-first):
///
/// ```text
///   stream bit "0"                   → NBLTYPES = 1
///   stream bits "1,0,0,0"            → NBLTYPES = 2
///   stream bits "1,1,0,0,b"          → NBLTYPES = 3 + b     (3..=4)
///   stream bits "1,0,1,0,bb"         → NBLTYPES = 5 + bb    (5..=8)
///   stream bits "1,1,1,0,bbb"        → NBLTYPES = 9 + bbb   (9..=16)
///   stream bits "1,0,0,1,bbbb"       → NBLTYPES = 17 + ...  (17..=32)
///   stream bits "1,1,0,1,bbbbb"      → NBLTYPES = 33 + ...  (33..=64)
///   stream bits "1,0,1,1,bbbbbb"     → NBLTYPES = 65 + ...  (65..=128)
///   stream bits "1,1,1,1,bbbbbbb"    → NBLTYPES = 129 + ... (129..=256)
/// ```
///
/// Algorithm: read 1 bit. If 0 → NBLTYPES=1. Else read 3 more bits as
/// LSB-first 3-bit value `n` (0..=7); read `n` extra bits as LSB-first
/// integer `e`; NBLTYPES = (1 << n) + 1 + e.
///
/// Used three times in the metablock header (literal / insert-and-copy /
/// distance block-type counts).
pub(crate) fn decode_nbltypes(r: &mut BitReader) -> Result<u32, BrotliError> {
    let b = r.read_one_bit()?;
    if b == 0 {
        return Ok(1);
    }
    let n = r.read_bits(3)?; // 0..=7
    let e = if n == 0 { 0 } else { r.read_bits(n as u8)? };
    let value = (1u32 << n)
        .checked_add(1)
        .ok_or(BrotliError::UnexpectedEof)?
        .checked_add(e)
        .ok_or(BrotliError::UnexpectedEof)?;
    Ok(value)
}

/// Parsed distance-code parameters per RFC 7932 §4.
///
/// `NPOSTFIX` (0..=3) is read as 2 bits directly.
/// `NDIRECT` is read as a 4-bit "high nibble" value that is then
/// LEFT-SHIFTED by NPOSTFIX bits to give the final NDIRECT (0..=120).
///
/// V1 scope: read and surface the values; the caller decides whether
/// to reject non-default (non-zero) combinations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DistanceParams {
    /// Postfix-bits count, 0..=3.
    pub(crate) npostfix: u8,
    /// Direct-distance-code count, 0..=120 (= 15 << 3 for NPOSTFIX=3).
    pub(crate) ndirect: u8,
}

/// Decode the (NPOSTFIX, NDIRECT) pair from the bit stream per RFC 7932 §4.
///
/// 2 bits NPOSTFIX → 0..=3
/// 4 bits NDIRECT_HIGH → NDIRECT = NDIRECT_HIGH << NPOSTFIX (max 120 when
///   NDIRECT_HIGH=15 and NPOSTFIX=3).
pub(crate) fn decode_distance_params(r: &mut BitReader) -> Result<DistanceParams, BrotliError> {
    let npostfix = r.read_bits(2)? as u8; // 0..=3
    let ndirect_high = r.read_bits(4)? as u8; // 0..=15
    let ndirect = ndirect_high << npostfix; // 0..=120
    Ok(DistanceParams { npostfix, ndirect })
}

/// Decompress a Brotli stream to bytes.
///
/// V1 scope (SP154 layers 1-5): handles streams composed of ONLY
/// uncompressed metablocks. Any compressed metablock encountered
/// surfaces `BrotliError::CompressedMetablockNotYetSupported`.
///
/// `expected_size` is the caller's expected decompressed size (Parquet
/// page header carries `uncompressed_page_size`). The decoder rejects
/// outputs that exceed `BROTLI_MAX_DECOMP` early and verifies size at
/// the end. If `expected_size` is None, no size verification is done.
pub(crate) fn decompress_inner(
    input: &[u8],
    expected_size: Option<usize>,
) -> Result<Vec<u8>, BrotliError> {
    // Early bomb defense: if caller declares expected size > cap, refuse.
    if let Some(sz) = expected_size {
        if sz > BROTLI_MAX_DECOMP {
            return Err(BrotliError::DecompressionBomb {
                decoded: sz,
                cap: BROTLI_MAX_DECOMP,
            });
        }
    }
    let mut r = BitReader::new(input);
    let _hdr = decode_stream_header(&mut r)?;
    let mut out: Vec<u8> = if let Some(sz) = expected_size {
        Vec::with_capacity(sz.min(BROTLI_MAX_DECOMP))
    } else {
        Vec::new()
    };
    loop {
        let mb = decode_metablock_header(&mut r)?;
        if mb.is_last_empty {
            break;
        }
        if mb.mlen == 0 {
            // Padding-only empty metablock (MNIBBLES=0 branch).
            if mb.is_last {
                break;
            }
            continue;
        }
        if !mb.is_uncompressed {
            // Compressed metablocks are the SP154-followup work.
            return Err(BrotliError::CompressedMetablockNotYetSupported { mlen: mb.mlen });
        }
        // Uncompressed: align to byte boundary, then read MLEN bytes raw.
        r.align_to_byte();
        let body = r.read_aligned_bytes(mb.mlen as usize)?;
        // Bomb defense mid-stream.
        let new_len = out
            .len()
            .checked_add(body.len())
            .ok_or(BrotliError::UnexpectedEof)?;
        if new_len > BROTLI_MAX_DECOMP {
            return Err(BrotliError::DecompressionBomb {
                decoded: new_len,
                cap: BROTLI_MAX_DECOMP,
            });
        }
        out.extend_from_slice(body);
        if mb.is_last {
            break;
        }
    }
    if let Some(sz) = expected_size {
        if out.len() != sz {
            return Err(BrotliError::SizeMismatch {
                expected: sz,
                actual: out.len(),
            });
        }
    }
    Ok(out)
}

/// Public-ish entry point: decompress with a caller-supplied expected
/// size (Parquet `uncompressed_page_size`).
pub(crate) fn decompress(input: &[u8], expected_size: usize) -> Result<Vec<u8>, BrotliError> {
    decompress_inner(input, Some(expected_size))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stream-header KAT: a single 0 bit → WBITS=16.
    #[test]
    fn stream_header_single_zero_bit_means_wbits_16() {
        // Byte: 0b0000_0000 = 0x00. bit 0 = 0 → WBITS=16.
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let h = decode_stream_header(&mut r).unwrap();
        assert_eq!(h.wbits, 16);
        assert_eq!(r.bit_pos(), 1, "consumed exactly 1 bit");
    }

    /// Stream-header KAT: bit-0=1, next-3=1 → WBITS = 17 + 1 = 18.
    /// Byte LSB-first: bit0=1, bit1=1, bit2=0, bit3=0 → 0b0000_0011 = 0x03.
    #[test]
    fn stream_header_wbits_18_via_1_then_001() {
        let bytes = [0x03u8];
        let mut r = BitReader::new(&bytes);
        let h = decode_stream_header(&mut r).unwrap();
        assert_eq!(h.wbits, 18);
        assert_eq!(r.bit_pos(), 4);
    }

    /// Stream-header KAT: bit-0=1, next-3=7 → WBITS = 17 + 7 = 24.
    /// Byte LSB-first: bit0=1, bit1=1, bit2=1, bit3=1 → 0b0000_1111 = 0x0F.
    #[test]
    fn stream_header_wbits_24_via_1_then_111() {
        let bytes = [0x0Fu8];
        let mut r = BitReader::new(&bytes);
        let h = decode_stream_header(&mut r).unwrap();
        assert_eq!(h.wbits, 24);
        assert_eq!(r.bit_pos(), 4);
    }

    /// Stream-header KAT: bit-0=1, next-3=0 (i.e. 0001 prefix), then 3-bit
    /// value 0 → WBITS=10.
    /// Bits 0..7 of byte 0 LSB-first: 1,0,0,0, 0,0,0,_  → 0b0000_0001 = 0x01.
    #[test]
    fn stream_header_wbits_10_via_0001_then_000() {
        let bytes = [0x01u8];
        let mut r = BitReader::new(&bytes);
        let h = decode_stream_header(&mut r).unwrap();
        assert_eq!(h.wbits, 10);
        assert_eq!(r.bit_pos(), 7);
    }

    /// Stream-header KAT: bit-0=1, next-3=0, then 3-bit value 5 → WBITS=15.
    /// Bits 0..7 LSB-first: 1,0,0,0, 1,0,1,_  → bits 0,4,6 set → 0x51.
    #[test]
    fn stream_header_wbits_15_via_0001_then_101() {
        let bytes = [0x51u8];
        let mut r = BitReader::new(&bytes);
        let h = decode_stream_header(&mut r).unwrap();
        assert_eq!(h.wbits, 15);
        assert_eq!(r.bit_pos(), 7);
    }

    /// Stream-header KAT: bit-0=1, next-3=0, then 3-bit value 7 → WBITS=17.
    /// Bits 0..7 LSB-first: 1,0,0,0, 1,1,1,_  → bits 0,4,5,6 set → 0x71.
    #[test]
    fn stream_header_wbits_17_via_0001_then_111() {
        let bytes = [0x71u8];
        let mut r = BitReader::new(&bytes);
        let h = decode_stream_header(&mut r).unwrap();
        assert_eq!(h.wbits, 17);
        assert_eq!(r.bit_pos(), 7);
    }

    /// Stream-header KAT: bit-0=1, next-3=0, then 3-bit value 6 → reserved.
    /// Bits 0..7 LSB-first: 1,0,0,0, 0,1,1,_  → bits 0,5,6 set → 0x61.
    #[test]
    fn stream_header_reserved_via_0001_then_110() {
        let bytes = [0x61u8];
        let mut r = BitReader::new(&bytes);
        let err = decode_stream_header(&mut r).unwrap_err();
        assert!(
            matches!(err, BrotliError::ReservedWbits(6)),
            "expected ReservedWbits(6), got {err:?}"
        );
    }

    /// Metablock-header KAT: is_last=1 + is_last_empty=1 → end of stream.
    /// Within a 2-bit read at bit-pos N: byte 0 = 0b..._..._11_xx (bits 0,1).
    /// Standalone header: byte 0 = 0b0000_0011 = 0x03.
    #[test]
    fn metablock_header_islast_empty_is_end_of_stream() {
        let bytes = [0x03u8];
        let mut r = BitReader::new(&bytes);
        let mb = decode_metablock_header(&mut r).unwrap();
        assert!(mb.is_last);
        assert!(mb.is_last_empty);
        assert_eq!(mb.mlen, 0);
        assert_eq!(r.bit_pos(), 2);
    }

    /// End-to-end KAT: minimal Brotli stream that decompresses to the
    /// empty byte sequence. RFC 7932's smallest valid stream.
    ///   1-bit WBITS=16  → '0'   (bit 0)
    ///   1-bit ISLAST=1  → '1'   (bit 1)
    ///   1-bit ISLASTEMPTY=1 → '1'  (bit 2)
    /// Total: 3 bits set in pattern: bit0=0 bit1=1 bit2=1 → 0b110 = 6.
    /// Padded to a byte: 0x06.
    #[test]
    fn end_to_end_empty_stream() {
        let bytes = [0x06u8];
        let out = decompress_inner(&bytes, Some(0)).unwrap();
        assert_eq!(out, Vec::<u8>::new());
    }

    /// End-to-end KAT: a hand-crafted Brotli stream that contains ONE
    /// uncompressed metablock with the body `b"hello"` (5 bytes).
    ///
    /// RFC 7932 §9.2 MNIBBLES encoding table (2-bit LSB-first):
    ///   '00' (LSB-first value 0) → 4 nibbles
    ///   '01' (value 1)           → 5 nibbles
    ///   '10' (value 2)           → 6 nibbles
    ///   '11' (value 3)           → 0 nibbles (skip-region / empty)
    ///
    /// ISUNCOMPRESSED is only read when !ISLAST. So this stream MUST
    /// use a non-last uncompressed metablock followed by an ISLAST+
    /// ISLASTEMPTY tail.
    ///
    /// Bit layout (LSB-first within each byte):
    ///   bit 0: WBITS = '0' (WBITS=16)
    ///   bit 1: ISLAST = '0' (not last)
    ///   bits 2-3: MNIBBLES_CODE = '00' LSB-first (value 0 → 4 nibbles)
    ///   bits 4-19: MLEN-1 in 16 bits LSB-first = 4 (=5-1)
    ///       bit 4=0 bit 5=0 bit 6=1 bit 7=0 bit 8..19=0
    ///   bit 20: ISUNCOMPRESSED = '1'
    ///   align to byte boundary → bit_pos = 24
    ///   bytes 3..8 = b"hello" (5 raw bytes), bit_pos at end = 64
    ///   bit 64: ISLAST = '1'
    ///   bit 65: ISLASTEMPTY = '1'  → end of stream
    ///
    /// Byte 0: bits set at bit 6 → 0b0100_0000 = 0x40.
    /// Byte 1 = 0x00 (MLEN bits 4..11 all 0).
    /// Byte 2 = bit 4 set (ISUNCOMPRESSED) → 0b0001_0000 = 0x10.
    /// Bytes 3..7 = "hello" = 0x68 0x65 0x6C 0x6C 0x6F.
    /// Byte 8 = bits 0,1 set = 0b0000_0011 = 0x03.
    #[test]
    fn end_to_end_uncompressed_hello() {
        let stream = [
            0x40, 0x00, 0x10, b'h', b'e', b'l', b'l', b'o', 0x03,
        ];
        let out = decompress_inner(&stream, Some(5)).unwrap();
        assert_eq!(out, b"hello".to_vec());
    }

    /// End-to-end KAT: a stream that has TWO uncompressed metablocks
    /// "abc" + "DEF" before the final ISLASTEMPTY marker.
    /// Hand-crafted; pins the multi-metablock loop. With MLEN=3 (-1=2):
    ///   Block-1 header bits (after WBITS=0, ISLAST=0):
    ///     MNIBBLES_CODE=01 (4 nibbles), MLEN-1 = 2 in 16 bits, ISUNCOMPRESSED=1
    ///   Body: 0x61 0x62 0x63
    ///   Block-2 same; body: 0x44 0x45 0x46
    ///   Then ISLAST=1 ISLASTEMPTY=1 final marker.
    #[test]
    fn end_to_end_two_uncompressed_metablocks() {
        // RFC 7932 §9.2 MNIBBLES table: '00' LSB-first = 4 nibbles.
        // Byte 0 bits 0..7 (block 1 header start):
        //   bit0=0 WBITS, bit1=0 ISLAST, bit2=0 bit3=0 MNIBBLES_CODE='00'(=4 nibbles),
        //   bit4=0 bit5=1 bit6=0 bit7=0 (MLEN bits 0..3 = 0b0010 = 2)
        // Byte 0 LSB-first: bit 5 set = 0b0010_0000 = 0x20
        let b0 = 0x20;
        // Byte 1: MLEN bits 4..11 = all 0 → 0x00.
        let b1 = 0x00;
        // Byte 2: MLEN bits 12..15 = 0; ISUNCOMPRESSED = 1 at bit 20 of stream
        //   (= bit 4 of byte 2); align pads bits 21..23 with 0.
        let b2 = 0x10;
        // Bytes 3..5 = "abc"
        let b3 = b'a';
        let b4 = b'b';
        let b5 = b'c';
        // Block 2 starts at bit 48 (byte 6). The header layout SHIFTS by 1
        // vs block 1: there is no WBITS at the front of a non-first
        // metablock. So byte 6 has:
        //   bit 0 = ISLAST = 0
        //   bit 1 = MNIBBLES_CODE bit 0 = 0
        //   bit 2 = MNIBBLES_CODE bit 1 = 0
        //   bits 3..7 = MLEN bits 0..4
        // For MLEN-1 = 2 = 0b00010, MLEN bit 1 is set (stream bit 52,
        // = byte 6 bit 4). So byte 6 = only bit 4 set = 0x10.
        let b6 = 0x10;
        // Byte 7: MLEN bits 5..12, all 0.
        let b7 = 0x00;
        // Byte 8: bits 0..3 = MLEN bits 13..15+pad-zero (= 0); bit 3 =
        // ISUNCOMPRESSED (= 1, at stream bit 67). So byte 8 = bit 3 =
        // 0b0000_1000 = 0x08.
        let b8 = 0x08;
        // Bytes 9..11 = "DEF"
        let b9 = b'D';
        let b10 = b'E';
        let b11 = b'F';
        // Tail: at bit 96, ISLAST=1 ISLASTEMPTY=1 → byte 12 = 0x03.
        let b12 = 0x03;
        let stream = [b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12];
        let out = decompress_inner(&stream, Some(6)).unwrap();
        assert_eq!(out, b"abcDEF".to_vec());
    }

    /// pyarrow-shape rejection: a stream whose first metablock is
    /// COMPRESSED must surface CompressedMetablockNotYetSupported (NOT
    /// panic, NOT silently miscount). Hand-crafted minimal compressed
    /// header. This is the active V1 boundary that the SP150 named-
    /// followup test in fixture_roundtrip.rs depends on.
    ///
    /// Stream (post-fix MNIBBLES encoding):
    ///   bit 0: WBITS=0 (WBITS=16)
    ///   bit 1: ISLAST=0
    ///   bits 2-3: MNIBBLES_CODE='00' (= 4 nibbles per RFC §9.2 table)
    ///   bits 4-19: MLEN-1 = 9 in 16 bits LSB-first
    ///       bit 4=1 bit 5=0 bit 6=0 bit 7=1, rest 0
    ///   bit 20: ISUNCOMPRESSED = 0 (compressed)
    /// Byte 0: bits 4 + 7 set = 0b1001_0000 = 0x90.
    /// Byte 1 = 0x00. Byte 2 = 0x00 (bit 20 = 0).
    #[test]
    fn compressed_metablock_surfaces_typed_error() {
        let stream = [0x90u8, 0x00, 0x00];
        let err = decompress_inner(&stream, Some(10)).unwrap_err();
        match err {
            BrotliError::CompressedMetablockNotYetSupported { mlen } => {
                assert_eq!(mlen, 10);
            }
            other => panic!("expected CompressedMetablockNotYetSupported, got {other:?}"),
        }
    }

    /// Bomb-defense: declared expected_size > BROTLI_MAX_DECOMP must
    /// reject up-front without allocating.
    #[test]
    fn bomb_defense_oversized_expected_rejects() {
        let stream = [0x06u8]; // empty stream
        let err = decompress_inner(&stream, Some(BROTLI_MAX_DECOMP + 1)).unwrap_err();
        match err {
            BrotliError::DecompressionBomb { decoded, cap } => {
                assert_eq!(decoded, BROTLI_MAX_DECOMP + 1);
                assert_eq!(cap, BROTLI_MAX_DECOMP);
            }
            other => panic!("expected DecompressionBomb, got {other:?}"),
        }
    }

    /// Size-mismatch lock: declared expected_size 7 but stream decodes
    /// to 5 ("hello") → typed SizeMismatch error.
    #[test]
    fn size_mismatch_surfaces_typed_error() {
        let stream = [
            0x40, 0x00, 0x10, b'h', b'e', b'l', b'l', b'o', 0x03,
        ];
        let err = decompress_inner(&stream, Some(7)).unwrap_err();
        match err {
            BrotliError::SizeMismatch { expected, actual } => {
                assert_eq!(expected, 7);
                assert_eq!(actual, 5);
            }
            other => panic!("expected SizeMismatch, got {other:?}"),
        }
    }

    /// Pentest: empty input slice → typed UnexpectedEof (via BitReader).
    #[test]
    fn pentest_empty_input_typed_eof() {
        let stream: [u8; 0] = [];
        let err = decompress_inner(&stream, Some(0)).unwrap_err();
        assert!(
            matches!(err, BrotliError::BitReader(BitReaderError::UnexpectedEof)),
            "expected BitReader(UnexpectedEof), got {err:?}"
        );
    }

    /// Pentest: truncated body (declared MLEN=5 but only 3 body bytes
    /// available) → typed UnexpectedEof. Adapt the "hello" KAT: cut to
    /// 6 bytes (header + 3 body bytes).
    #[test]
    fn pentest_truncated_body_typed_eof() {
        let stream = [0x40u8, 0x00, 0x10, b'h', b'e', b'l'];
        let err = decompress_inner(&stream, None).unwrap_err();
        assert!(
            matches!(err, BrotliError::BitReader(BitReaderError::UnexpectedEof)),
            "expected BitReader(UnexpectedEof), got {err:?}"
        );
    }

    // -------------------------- L6 KATs --------------------------
    //
    // L6 NBLTYPES variable-length code per RFC 7932 §9.2. The listed
    // codes are parsed right-to-left, so e.g. listed "0001" → stream
    // bits "1, 0, 0, 0".

    /// L6 KAT-1: a single '0' bit → NBLTYPES = 1 (the common-case
    /// "no block-type partitioning" encoding for pyarrow-shape pages).
    #[test]
    fn nbltypes_single_zero_means_one() {
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let n = decode_nbltypes(&mut r).unwrap();
        assert_eq!(n, 1);
        assert_eq!(r.bit_pos(), 1);
    }

    /// L6 KAT-2: stream "1,0,0,0" (= listed "0001") → NBLTYPES = 2.
    /// Byte 0 LSB-first: bit 0 = 1, bits 1..3 = 0 → 0x01.
    #[test]
    fn nbltypes_two_via_1_then_000() {
        let bytes = [0x01u8];
        let mut r = BitReader::new(&bytes);
        let n = decode_nbltypes(&mut r).unwrap();
        assert_eq!(n, 2);
        assert_eq!(r.bit_pos(), 4);
    }

    /// L6 KAT-3: stream "1,1,1,0,1,1,0" (= listed "0110111") → NBLTYPES = 12.
    /// Worked example from RFC 7932 §9.2: "0110111 has the value 12".
    ///
    /// Stream bits 0..6: 1, 1, 1, 0, 1, 1, 0
    /// Byte 0 LSB-first: bits 0=1, 1=1, 2=1, 3=0, 4=1, 5=1, 6=0
    /// → 1 + 2 + 4 + 16 + 32 = 55 = 0x37
    #[test]
    fn nbltypes_twelve_via_rfc_example() {
        let bytes = [0x37u8];
        let mut r = BitReader::new(&bytes);
        let n = decode_nbltypes(&mut r).unwrap();
        assert_eq!(n, 12);
        assert_eq!(r.bit_pos(), 7);
    }

    /// L6 KAT-4: NBLTYPES = 16 (max of n=3 bucket).
    /// Stream: "1,1,1,0" (prefix n=3) + "1,1,1" (3 extras = 7).
    /// Value = (1<<3) + 1 + 7 = 16.
    /// Byte 0 bits 0..6: 1,1,1,0,1,1,1 → 1+2+4+16+32+64 = 119 → 0x77.
    #[test]
    fn nbltypes_sixteen_max_of_third_bucket() {
        let bytes = [0x77u8];
        let mut r = BitReader::new(&bytes);
        let n = decode_nbltypes(&mut r).unwrap();
        assert_eq!(n, 16);
        assert_eq!(r.bit_pos(), 7);
    }

    /// L6 KAT-5: NBLTYPES = 256 (max possible — bucket n=7).
    /// Stream: "1,1,1,1" (prefix n=7) + 7 extras all 1 = 127.
    /// Value = (1<<7) + 1 + 127 = 256.
    /// Stream 11 bits all 1; byte 0 = 0xFF, byte 1 bits 0..2 = 1,1,1 → 0x07.
    #[test]
    fn nbltypes_max_value_256() {
        let bytes = [0xFFu8, 0x07];
        let mut r = BitReader::new(&bytes);
        let n = decode_nbltypes(&mut r).unwrap();
        assert_eq!(n, 256);
        assert_eq!(r.bit_pos(), 11);
    }

    // -------------------------- L7 KATs --------------------------
    //
    // L7 distance-code parameters per RFC 7932 §4 / §9.2.

    /// L7 KAT-1: NPOSTFIX=0, NDIRECT=0 (the default that pyarrow files
    /// virtually always use). Stream: 2 bits 0 + 4 bits 0 = 6 zero bits.
    /// Byte 0 = 0x00.
    #[test]
    fn distance_params_default_zero_zero() {
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let dp = decode_distance_params(&mut r).unwrap();
        assert_eq!(dp.npostfix, 0);
        assert_eq!(dp.ndirect, 0);
        assert_eq!(r.bit_pos(), 6);
    }

    /// L7 KAT-2: NPOSTFIX=3, NDIRECT_HIGH=15 → NDIRECT = 15 << 3 = 120.
    /// Stream: 2 bits 3 (=1,1) + 4 bits 15 (=1,1,1,1) = 6 ones.
    /// Byte 0 LSB-first bits 0..5 = 1 → 1+2+4+8+16+32 = 63 = 0x3F.
    #[test]
    fn distance_params_max_npostfix_max_ndirect() {
        let bytes = [0x3Fu8];
        let mut r = BitReader::new(&bytes);
        let dp = decode_distance_params(&mut r).unwrap();
        assert_eq!(dp.npostfix, 3);
        assert_eq!(dp.ndirect, 120);
        assert_eq!(r.bit_pos(), 6);
    }

    /// L7 KAT-3: NPOSTFIX=1, NDIRECT_HIGH=4 → NDIRECT = 4 << 1 = 8.
    /// Stream: 2 bits 1 (=1,0) + 4 bits 4 (=0,0,1,0).
    /// Byte 0 LSB-first: bit 0=1, bit 1=0, bit 2=0, bit 3=0, bit 4=1,
    /// bit 5=0, bits 6..=7=0 → 1 + 16 = 17 = 0x11.
    #[test]
    fn distance_params_mid_values() {
        let bytes = [0x11u8];
        let mut r = BitReader::new(&bytes);
        let dp = decode_distance_params(&mut r).unwrap();
        assert_eq!(dp.npostfix, 1);
        assert_eq!(dp.ndirect, 8);
        assert_eq!(r.bit_pos(), 6);
    }

    /// Pentest: a stream whose declared expected_size is gigantic but
    /// at the BROTLI_MAX_DECOMP boundary EXACTLY → must NOT reject (the
    /// cap is inclusive — capacity reservation clamps to cap).
    #[test]
    fn boundary_expected_size_at_exact_cap_does_not_reject() {
        // Use an empty stream; declared size matches actual (0), but the
        // cap-check is on expected_size first. We can't actually allocate
        // BROTLI_MAX_DECOMP for a real test, so instead we lock that
        // expected_size == BROTLI_MAX_DECOMP passes the bomb-check
        // (it will fail SizeMismatch later — that's fine, different code
        // path).
        let stream = [0x06u8]; // empty stream
        let err = decompress_inner(&stream, Some(BROTLI_MAX_DECOMP)).unwrap_err();
        // Should be SizeMismatch, NOT DecompressionBomb.
        assert!(
            matches!(err, BrotliError::SizeMismatch { .. }),
            "boundary expected_size should reach size-check, got {err:?}"
        );
    }
}
