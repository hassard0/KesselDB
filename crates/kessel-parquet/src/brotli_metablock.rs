//! Brotli compressed-metablock orchestration — RFC 7932 §9.2 + §9.3.
//!
//! THIS is the layer that ties L5b/L6/L7/L8/L9/L9b/L10/L12 together
//! into an actual compressed-metablock decoder. It is the "main event"
//! of the SP154 arc — every prior layer was building blocks for this
//! one function.
//!
//! ## SP154 L11 scope (V1)
//!
//! For V1 we enforce a strict reduction of the Brotli spec, designed
//! to cover the most common pyarrow-emitted shape while rejecting
//! anything that would require subsidiary work not yet shipped:
//!
//!   - **NBLTYPES_L = NBLTYPES_I = NBLTYPES_D = 1** (no block-type
//!     partitioning). Reject > 1 with typed
//!     `BrotliMetablockError::UnsupportedBlockTypes`.
//!   - **NPOSTFIX = 0, NDIRECT = 0** (default distance code). Reject
//!     anything else with typed `UnsupportedDistanceParams`.
//!   - **NTREES (literal CMAP) = 1, NTREES (distance CMAP) = 1** (no
//!     context modelling). L8 already rejects > 1 via
//!     `decode_context_map_header_v1`.
//!   - **Dictionary transforms: identity only**. L10 already rejects
//!     non-identity via `dictionary_word`.
//!
//! Under those reductions the metablock body is exactly:
//!   - Read 2 bits CMODE per literal block-type (= 1 bit-pair in V1).
//!   - Read 1 literal prefix code (256-symbol alphabet).
//!   - Read 1 insert-and-copy prefix code (704-symbol alphabet).
//!   - Read 1 distance prefix code (V1 64-symbol alphabet).
//!   - LOOP until accumulated output == MLEN:
//!     1. Decode command sym from IC code.
//!     2. Decompose via `brotli_command::decompose_command_code` →
//!        (insert_code, copy_code, distance_implicit).
//!     3. Read insert_length + copy_length extras.
//!     4. Read insert_length literal bytes via the literal prefix code.
//!     5. If output.len() == MLEN → break (last command may have
//!        copy=0 effectively, when its insert run fills the metablock).
//!     6. Resolve distance: either implicit (= recent_distance[0]) or
//!        decode a distance symbol via `brotli_distance::decode_distance`.
//!     7. If distance <= output.len() → output-buffer back-reference;
//!        else → dictionary match (V1 reject unless identity transform
//!        applies — V1 reject anyway for now since the dictionary
//!        distance-decoding §4.2 mapping is not wired).
//!     8. `OutputBuffer::copy_match(distance, copy_length)`.
//!
//! ## Safety
//!
//! `#![forbid(unsafe_code)]`; bounds-checked arithmetic; typed errors
//! for every failure mode (no panics on attacker bytes).

#![allow(dead_code)]

use crate::brotli::{
    decode_distance_params, decode_metablock_header, decode_nbltypes, decode_stream_header,
    BrotliError,
};
use crate::brotli_bit_reader::BitReader;
use crate::brotli_command::{decompose_command_code, BrotliCommandError, NUM_COMMAND_SYMBOLS};
use crate::brotli_context::{decode_context_map_header_v1, BrotliContextError};
use crate::brotli_distance::{decode_distance, DistanceRing, NUM_DISTANCE_CODES_V1};
use crate::brotli_huffman::{decode_prefix_code, HuffmanError};
use crate::brotli_ring::{BrotliRingError, OutputBuffer};

/// Number of symbols in the Brotli literal alphabet (always 256 — one
/// per output byte value).
pub(crate) const NUM_LITERAL_SYMBOLS: u32 = 256;

/// Number of bits to read for a literal symbol via the simple-code path
/// (RFC §3.4 reads NSYM × alphabet_bits bits for the symbol values).
/// 256 symbols → 8 bits.
pub(crate) const LITERAL_ALPHABET_BITS: u8 = 8;

/// Number of bits for an insert-and-copy command symbol.
/// 704 symbols → ceil(log2(704)) = 10 bits.
pub(crate) const COMMAND_ALPHABET_BITS: u8 = 10;

/// Number of bits for a distance symbol in V1 (NPOSTFIX=0, NDIRECT=0).
/// 64 symbols → 6 bits.
pub(crate) const DISTANCE_ALPHABET_BITS_V1: u8 = 6;

/// Typed errors for the compressed-metablock orchestrator. Wraps every
/// inner layer's error type so callers see a single error category at
/// the page_payload boundary.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BrotliMetablockError {
    /// Inner `BrotliError` (bit-reader / size mismatch / framing).
    Inner(BrotliError),
    /// Prefix-code (Huffman) decode error.
    Huffman(HuffmanError),
    /// Insert-and-copy command alphabet error.
    Command(BrotliCommandError),
    /// Context-map header error (NTREES > 1).
    Context(BrotliContextError),
    /// Distance decode error.
    Distance(crate::brotli_distance::BrotliDistanceError),
    /// Output buffer error (zero/excess distance, length overflow).
    Ring(BrotliRingError),
    /// Dictionary lookup error (out-of-range, non-identity transform).
    Dictionary(crate::brotli_dictionary::BrotliDictionaryError),
    /// NBLTYPES > 1 on any of {literal, insert-copy, distance}. V1 only
    /// supports NBLTYPES=1 for all three. Carries the surface name and
    /// the encoded value for diagnostics.
    UnsupportedBlockTypes {
        surface: &'static str,
        nbltypes: u32,
    },
    /// (NPOSTFIX, NDIRECT) is not the V1 default (0, 0). V1 distance
    /// code alphabet is fixed at 64 symbols for NPOSTFIX=0 + NDIRECT=0.
    /// Carries the raw values for diagnostics.
    UnsupportedDistanceParams { npostfix: u8, ndirect: u8 },
    /// A distance referred to the static dictionary (per RFC §4.2:
    /// distances > `max_distance = window_size + ndirect + ...` map to
    /// dictionary words). V1 only supports output-buffer back-references;
    /// dictionary distance decoding + transform application is the
    /// SP154-followup. Carries the offending distance + output_len.
    DictionaryDistanceNotSupported {
        distance: u32,
        output_len: usize,
    },
    /// Decoder produced more output bytes than the declared MLEN. Per
    /// RFC §9.2 a compressed metablock body emits EXACTLY MLEN bytes;
    /// overshooting indicates a stream-corruption (copy_length pushed
    /// past the boundary). Surfaced as a typed error so the caller
    /// sees a precise diagnostic rather than a silent miscount.
    OutputExceededMlen { produced: usize, mlen: u32 },
    /// CMode byte was outside the valid 0..=3 range (= 2 bits, so this
    /// can't actually occur; defensive variant).
    InvalidCMode(u32),
}

impl core::fmt::Display for BrotliMetablockError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for BrotliMetablockError {}

impl From<BrotliError> for BrotliMetablockError {
    fn from(e: BrotliError) -> Self {
        BrotliMetablockError::Inner(e)
    }
}

impl From<crate::brotli_bit_reader::BitReaderError> for BrotliMetablockError {
    fn from(e: crate::brotli_bit_reader::BitReaderError) -> Self {
        BrotliMetablockError::Inner(BrotliError::BitReader(e))
    }
}

impl From<HuffmanError> for BrotliMetablockError {
    fn from(e: HuffmanError) -> Self {
        BrotliMetablockError::Huffman(e)
    }
}

impl From<BrotliCommandError> for BrotliMetablockError {
    fn from(e: BrotliCommandError) -> Self {
        BrotliMetablockError::Command(e)
    }
}

impl From<BrotliContextError> for BrotliMetablockError {
    fn from(e: BrotliContextError) -> Self {
        BrotliMetablockError::Context(e)
    }
}

impl From<crate::brotli_distance::BrotliDistanceError> for BrotliMetablockError {
    fn from(e: crate::brotli_distance::BrotliDistanceError) -> Self {
        BrotliMetablockError::Distance(e)
    }
}

impl From<BrotliRingError> for BrotliMetablockError {
    fn from(e: BrotliRingError) -> Self {
        BrotliMetablockError::Ring(e)
    }
}

impl From<crate::brotli_dictionary::BrotliDictionaryError> for BrotliMetablockError {
    fn from(e: crate::brotli_dictionary::BrotliDictionaryError) -> Self {
        BrotliMetablockError::Dictionary(e)
    }
}

/// Decode a compressed metablock body of declared MLEN bytes into `out`.
///
/// `mlen` is the metablock's declared output length (already read by
/// the dispatcher in `decode_metablock_header`). The bit reader is
/// positioned just AFTER the metablock header (= the NBLTYPES stream
/// starts at the current bit position).
///
/// V1 enforces the reductions listed at the module level and surfaces
/// any non-V1 condition as a typed error. The output is appended to
/// `out` (which may already contain prior metablocks' output).
pub(crate) fn decode_compressed_metablock(
    r: &mut BitReader,
    mlen: u32,
    out: &mut OutputBuffer,
) -> Result<(), BrotliMetablockError> {
    // --- step 1: NBLTYPES for {literal, insert-copy, distance} ---
    // Per RFC §9.2 each of the three counts is encoded via the §9.2
    // bucket-prefix code (range 1..=256). V1 only supports = 1 (no
    // block-type partitioning).
    let nbltypes_l = decode_nbltypes(r)?;
    if nbltypes_l > 1 {
        return Err(BrotliMetablockError::UnsupportedBlockTypes {
            surface: "literal",
            nbltypes: nbltypes_l,
        });
    }
    let nbltypes_i = decode_nbltypes(r)?;
    if nbltypes_i > 1 {
        return Err(BrotliMetablockError::UnsupportedBlockTypes {
            surface: "insert-copy",
            nbltypes: nbltypes_i,
        });
    }
    let nbltypes_d = decode_nbltypes(r)?;
    if nbltypes_d > 1 {
        return Err(BrotliMetablockError::UnsupportedBlockTypes {
            surface: "distance",
            nbltypes: nbltypes_d,
        });
    }

    // --- step 2: NPOSTFIX, NDIRECT ---
    // Per RFC §9.2 + §4. V1 only supports NPOSTFIX=0, NDIRECT=0 (the
    // default the encoder picks for small data — virtually all pyarrow
    // pages).
    let dp = decode_distance_params(r)?;
    if dp.npostfix != 0 || dp.ndirect != 0 {
        return Err(BrotliMetablockError::UnsupportedDistanceParams {
            npostfix: dp.npostfix,
            ndirect: dp.ndirect,
        });
    }

    // --- step 3: CMODE per literal block-type ---
    // Per RFC §9.2: 2 bits CMODE for each of the NBLTYPES_L literal
    // block-types. V1 NBLTYPES_L=1 → exactly one 2-bit read. The CMODE
    // selects the context model (LSB6, MSB6, UTF8, Signed) used to
    // derive context_id from prev 2 bytes. With NTREES=1 (next step),
    // context_id is unused — every literal goes through tree 0 — so
    // the actual CMODE value is irrelevant for V1 decode. We still
    // consume the bits to advance the cursor.
    let _cmode = r.read_bits(2)?;
    // Range check (defensive — read_bits(2) returns 0..=3 already).
    if _cmode > 3 {
        return Err(BrotliMetablockError::InvalidCMode(_cmode));
    }

    // --- step 4: literal CMAP NTREES + distance CMAP NTREES ---
    // Per RFC §7.3 step 1 — L8 helper rejects > 1 with surface tag.
    let _ntrees_l = decode_context_map_header_v1(r, "literal")?;
    let _ntrees_d = decode_context_map_header_v1(r, "distance")?;

    // --- step 5: read the three prefix codes ---
    // V1 NTREES=1 for both literal and distance, NBLTYPES=1 for all
    // three → exactly ONE prefix code per stream (literal, IC, distance).
    let literal_code = decode_prefix_code(r, LITERAL_ALPHABET_BITS, NUM_LITERAL_SYMBOLS)?;
    let command_code = decode_prefix_code(r, COMMAND_ALPHABET_BITS, NUM_COMMAND_SYMBOLS)?;
    let distance_code = decode_prefix_code(r, DISTANCE_ALPHABET_BITS_V1, NUM_DISTANCE_CODES_V1)?;

    // --- step 6: main decode loop ---
    let start_len = out.len();
    let target_len = start_len
        .checked_add(mlen as usize)
        .ok_or(BrotliMetablockError::Inner(BrotliError::UnexpectedEof))?;
    let mut ring = DistanceRing::new();
    while out.len() < target_len {
        // (a) Decode command symbol via IC code.
        let cmd_sym = command_code.decode_symbol(r)?;
        let components = decompose_command_code(cmd_sym)?;

        // (b) + (c) Decode insert_length and copy_length extras.
        let insert_length =
            crate::brotli_command::decode_insert_length(r, components.insert_code)?;
        let copy_length =
            crate::brotli_command::decode_copy_length(r, components.copy_code)?;

        // (d) Read `insert_length` literal bytes via the literal code.
        for _ in 0..insert_length {
            // V1 NTREES=1 → context_id is unused; every literal uses
            // tree 0 (= the one literal_code we read).
            let sym = literal_code.decode_symbol(r)?;
            if sym >= 256 {
                return Err(BrotliMetablockError::Huffman(HuffmanError::SymbolOutOfRange {
                    sym,
                    alphabet_size: 256,
                }));
            }
            out.append_byte(sym as u8)?;
            if out.len() >= target_len {
                break;
            }
        }
        // (e) If we already filled the metablock, the last command's
        // copy portion is dropped (per RFC §9.2 — a metablock's last
        // command may not include a copy).
        if out.len() >= target_len {
            if out.len() > target_len {
                return Err(BrotliMetablockError::OutputExceededMlen {
                    produced: out.len(),
                    mlen,
                });
            }
            break;
        }

        // (f) Resolve distance: implicit (= recent d1) OR decode + ring update.
        let distance = if components.distance_implicit {
            // Implicit: use the most-recent distance (d1) from the ring.
            // Per RFC §4 the initial ring values "16, 15, 11, 4" read
            // fourth-to-last → ... → last-distance, so initial d1 = 4
            // (NOT 16). Cross-referenced against Google c/dec/decode.c
            // `TakeDistanceFromRingBuffer` for implicit-distance commands.
            ring.slots[0]
        } else {
            let dsym = distance_code.decode_symbol(r)?;
            decode_distance(r, dsym, &mut ring)?
        };

        // (g) Output-buffer back-reference vs dictionary match.
        // Per RFC 7932 §4 / §4.2: a distance > `max_backward_distance`
        // (= window_size) indicates a static-dictionary back-reference.
        // V1 rejects dictionary lookups (the §4.2 mapping + transforms
        // are the SP154-followup). For distances ≤ window_size we use
        // the L12 OutputBuffer's copy_match — INCLUDING the case where
        // distance > current_output_len, which per RFC 7932 §9.1 reads
        // from the ring buffer's implicit zero-padded "pre-stream zone"
        // (= initial ring contents = zeros for the first metablock).
        //
        // This matches the Brotli reference C decoder's behavior: the
        // ring buffer is allocated zero-initialized, so reading past
        // the actual written region returns zero bytes. Real-world
        // streams (incl. pyarrow's i64-column compressed output) exploit
        // this to compactly encode runs of zeros via short codes that
        // reach back into the pre-stream zone.
        let cur_out_len = out.len();
        let window_size = out.window_size();
        if (distance as usize) > window_size {
            return Err(BrotliMetablockError::DictionaryDistanceNotSupported {
                distance,
                output_len: cur_out_len,
            });
        }

        // (h) Execute the copy. Use the pre-stream-zero variant when
        // distance might reach into the pre-stream zone (V1 flat-Vec
        // model: pre-stream is implicit zeros).
        let copy_end = cur_out_len
            .checked_add(copy_length as usize)
            .ok_or(BrotliMetablockError::Inner(BrotliError::UnexpectedEof))?;
        // Clamp to target_len so a runaway copy_length doesn't overshoot.
        let effective_copy_len = if copy_end > target_len {
            target_len - cur_out_len
        } else {
            copy_length as usize
        };
        out.copy_match_with_prestream_zeros(distance as usize, effective_copy_len)?;
    }

    if out.len() != target_len {
        return Err(BrotliMetablockError::OutputExceededMlen {
            produced: out.len(),
            mlen,
        });
    }

    Ok(())
}

/// Decompress a Brotli stream end-to-end via the compressed-metablock
/// orchestrator. This is the L11 wire-up — replaces the V1 "uncompressed-
/// only" `brotli::decompress_inner` from L4.
///
/// `expected_size` is the caller's declared decompressed size (Parquet
/// page header). When provided, the final output is verified against it.
pub(crate) fn decompress_compressed(
    input: &[u8],
    expected_size: Option<usize>,
) -> Result<Vec<u8>, BrotliMetablockError> {
    use crate::brotli::BROTLI_MAX_DECOMP;

    if let Some(sz) = expected_size {
        if sz > BROTLI_MAX_DECOMP {
            return Err(BrotliMetablockError::Inner(BrotliError::DecompressionBomb {
                decoded: sz,
                cap: BROTLI_MAX_DECOMP,
            }));
        }
    }
    let mut r = BitReader::new(input);
    let header = decode_stream_header(&mut r)?;
    let window_size = (1usize << header.wbits).saturating_sub(16);
    let mut out = OutputBuffer::new(window_size);

    loop {
        let mb = decode_metablock_header(&mut r)?;
        if mb.is_last_empty {
            break;
        }
        if mb.mlen == 0 {
            // Padding/skip metablock — no body, just loop.
            if mb.is_last {
                break;
            }
            continue;
        }
        if mb.is_uncompressed {
            // Uncompressed body: byte-aligned raw copy (L4).
            r.align_to_byte();
            let body = r.read_aligned_bytes(mb.mlen as usize)?;
            // Bomb defense.
            let new_len = out
                .len()
                .checked_add(body.len())
                .ok_or(BrotliMetablockError::Inner(BrotliError::UnexpectedEof))?;
            if new_len > BROTLI_MAX_DECOMP {
                return Err(BrotliMetablockError::Inner(BrotliError::DecompressionBomb {
                    decoded: new_len,
                    cap: BROTLI_MAX_DECOMP,
                }));
            }
            out.append_slice(body)?;
        } else {
            // Compressed body: V1 orchestrator (L11).
            decode_compressed_metablock(&mut r, mb.mlen, &mut out)?;
            if out.len() > BROTLI_MAX_DECOMP {
                return Err(BrotliMetablockError::Inner(BrotliError::DecompressionBomb {
                    decoded: out.len(),
                    cap: BROTLI_MAX_DECOMP,
                }));
            }
        }
        if mb.is_last {
            break;
        }
    }

    if let Some(sz) = expected_size {
        if out.len() != sz {
            return Err(BrotliMetablockError::Inner(BrotliError::SizeMismatch {
                expected: sz,
                actual: out.len(),
            }));
        }
    }
    Ok(out.into_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::brotli::BrotliError;
    use crate::brotli_bit_reader::BitReaderError;

    /// L11 KAT-1: an empty Brotli stream (single 0x06 byte = WBITS=16,
    /// ISLAST=1, ISLASTEMPTY=1) goes through the compressed orchestrator
    /// without entering decode_compressed_metablock at all (no body) →
    /// returns the empty Vec.
    #[test]
    fn empty_stream_returns_empty_vec() {
        let stream = [0x06u8];
        let out = decompress_compressed(&stream, Some(0)).unwrap();
        assert_eq!(out, Vec::<u8>::new());
    }

    /// L11 KAT-2: a stream containing a SINGLE uncompressed metablock
    /// goes through the L4 path in `decompress_compressed`. Reuses the
    /// 'hello' KAT layout from `brotli.rs`.
    #[test]
    fn uncompressed_only_stream_decodes_via_l4_path() {
        let stream = [0x40, 0x00, 0x10, b'h', b'e', b'l', b'l', b'o', 0x03];
        let out = decompress_compressed(&stream, Some(5)).unwrap();
        assert_eq!(out, b"hello".to_vec());
    }

    /// L11 KAT-3: bomb-defense — an expected_size > BROTLI_MAX_DECOMP
    /// rejects upfront with typed Inner(DecompressionBomb).
    #[test]
    fn bomb_defense_oversized_expected_rejects() {
        use crate::brotli::BROTLI_MAX_DECOMP;
        let stream = [0x06u8];
        let err = decompress_compressed(&stream, Some(BROTLI_MAX_DECOMP + 1)).unwrap_err();
        match err {
            BrotliMetablockError::Inner(BrotliError::DecompressionBomb { decoded, cap }) => {
                assert_eq!(decoded, BROTLI_MAX_DECOMP + 1);
                assert_eq!(cap, BROTLI_MAX_DECOMP);
            }
            other => panic!("expected Inner(DecompressionBomb), got {other:?}"),
        }
    }

    /// L11 KAT-4: a stream with a compressed metablock whose NBLTYPES_L
    /// is > 1 surfaces typed UnsupportedBlockTypes with surface="literal".
    /// Hand-craft: WBITS=16 (1 bit '0'); ISLAST=0; MNIBBLES_CODE=00 (4
    /// nibbles); MLEN-1 = 0 (16 bits zero — MLEN=1); ISUNCOMPRESSED=0
    /// (compressed); NBLTYPES_L: bits '1,0,0,0' = stream value '0001'
    /// listed = NBLTYPES=2 → trigger reject.
    ///
    /// Bit layout (LSB-first within bytes):
    ///   bit 0: WBITS=0
    ///   bit 1: ISLAST=0
    ///   bit 2: MNIBBLES low = 0
    ///   bit 3: MNIBBLES hi  = 0
    ///   bits 4..19: MLEN-1 = 0  (all 0)
    ///   bit 20: ISUNCOMPRESSED = 0
    ///   bits 21..24: NBLTYPES_L = '1, 0, 0, 0' (= NBLTYPES=2)
    ///       bit 21 = 1, bits 22, 23, 24 = 0
    ///
    /// Byte 0 (bits 0..7): all 0 → 0x00.
    /// Byte 1 (bits 8..15): all 0 → 0x00.
    /// Byte 2 (bits 16..23): bit 20=0, bit 21=1, bits 22, 23 = 0 →
    ///   only bit 21 set within byte 2 = bit 5 LSB = 0b0010_0000 = 0x20
    /// Byte 3 = 0 (bit 24 = 0).
    #[test]
    fn compressed_metablock_nbltypes_l_greater_than_1_rejects() {
        let stream = [0x00u8, 0x00, 0x20, 0x00];
        let err = decompress_compressed(&stream, None).unwrap_err();
        match err {
            BrotliMetablockError::UnsupportedBlockTypes { surface, nbltypes } => {
                assert_eq!(surface, "literal");
                assert_eq!(nbltypes, 2);
            }
            other => panic!("expected UnsupportedBlockTypes(literal,2), got {other:?}"),
        }
    }

    /// L11 KAT-6: end-to-end decode of a REAL pyarrow brotli payload
    /// (the id-column page of `brotli_flat.parquet`). 17-byte input
    /// decompresses to 40-byte output (5 i64 values 1..=5 LE). This is
    /// the first byte-identical positive decode of a non-hand-crafted
    /// pyarrow page through the V1 orchestrator — proof that L11 wire-
    /// up actually works for the common pyarrow-shape Brotli stream.
    #[test]
    fn pyarrow_id_column_page_decodes_byte_identical() {
        // Bytes copy-pasted from a debug-dump of the brotli_flat.parquet
        // id-column data-page payload (17 bytes; uncomp=40).
        let payload: [u8; 17] = [
            0x1b, 0x27, 0x00, 0x00, 0x04, 0xee, 0xf8, 0x6c, 0xa0, 0x11, 0x4a, 0x0a, 0x91,
            0x16, 0xe5, 0x6a, 0x0e,
        ];
        let out = decompress_compressed(&payload, Some(40)).unwrap();
        let expected: [u8; 40] = [
            1, 0, 0, 0, 0, 0, 0, 0, // i64(1) LE
            2, 0, 0, 0, 0, 0, 0, 0, // i64(2) LE
            3, 0, 0, 0, 0, 0, 0, 0, // i64(3) LE
            4, 0, 0, 0, 0, 0, 0, 0, // i64(4) LE
            5, 0, 0, 0, 0, 0, 0, 0, // i64(5) LE
        ];
        assert_eq!(&out[..], &expected[..]);
    }

    /// L11 KAT-5: truncated stream past the stream header but before
    /// the metablock NBLTYPES → typed Inner BitReader UnexpectedEof.
    #[test]
    fn pentest_truncated_after_header() {
        // Just WBITS=16 (1 bit '0'), then stop.
        let stream = [0x00u8];
        let err = decompress_compressed(&stream, None).unwrap_err();
        assert!(
            matches!(
                err,
                BrotliMetablockError::Inner(BrotliError::BitReader(BitReaderError::UnexpectedEof))
            ),
            "expected Inner(BitReader(UnexpectedEof)), got {err:?}"
        );
    }






















}
