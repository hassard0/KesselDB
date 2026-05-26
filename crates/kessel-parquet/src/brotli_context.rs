//! Brotli context-map decoding — RFC 7932 §7.
//!
//! Each compressed metablock declares THREE block-type partitions
//! (literals, insert-and-copy commands, distances). For literals and
//! distances there is additionally a **context map** that selects one of
//! N prefix-code trees per output position based on a "context id"
//! derived from recent output bytes.
//!
//! The context map is encoded per RFC 7932 §7.3 as:
//!
//! 1. **NTREES**: a variable-length code identical in shape to NBLTYPES
//!    (RFC §9.2 bucket-prefix encoding, range 1..=256). When NTREES=1
//!    there is no context mapping (every context id maps to tree 0) and
//!    the CMAP body is omitted entirely.
//!
//! 2. **RLEMAX**: present only when NTREES > 1. 1 bit; if 1, read 4 more
//!    bits encoding RLEMAX-1 (RLEMAX in 1..=16). If the first bit is 0,
//!    RLEMAX = 0 (no RLE compression of the CMAP body).
//!
//! 3. **CMAP body**: a sequence of `alphabet_size` symbols decoded via a
//!    complex prefix code over the alphabet [0, NTREES + RLEMAX). Symbols
//!    in [0, RLEMAX] encode runs of zeros (lengths 1..=2^(RLEMAX)-1 with
//!    extra bits); symbols in [RLEMAX+1, NTREES+RLEMAX) are direct values
//!    (with `value - RLEMAX` being the tree index).  After the raw
//!    sequence is produced, an **Inverse Move-To-Front** transform is
//!    applied (RFC §7.3 IMTF).
//!
//! 4. **CMAPNESTED**: a single bit after the CMAP body that, when 1,
//!    triggers a fresh IMTF inversion (rarely used by encoders).
//!
//! ## SP154 L8 scope (this commit)
//!
//! V1 implements only step 1 — the **NTREES read** — for both the
//! literal-context-map AND the distance-context-map. When either NTREES
//! is > 1, a typed `BrotliContextError::UnsupportedMultipleTrees` is
//! surfaced naming the SP154 follow-up that will ship steps 2-4
//! (RLEMAX, CMAP body decode, IMTF inversion).
//!
//! This is the common-case shape for pyarrow-emitted Parquet pages:
//! NTREES is virtually always 1 because Parquet's columnar shape doesn't
//! benefit from context modelling. The CMAP body + IMTF inversion is
//! deferred to a later sub-slice when we hit a real-world file that uses
//! it.
//!
//! ## Safety
//!
//! `#![forbid(unsafe_code)]`; bounds-checked arithmetic; typed errors
//! for every failure mode (no panics).

#![allow(dead_code)]

use crate::brotli::{decode_nbltypes, BrotliError};
use crate::brotli_bit_reader::BitReader;

/// Typed errors specific to context-map decoding. Wraps `BrotliError`
/// for nested bit-reader failures.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BrotliContextError {
    /// Inner brotli error (typically `BrotliError::BitReader(...)`).
    Inner(BrotliError),
    /// NTREES > 1 — context map body + IMTF inversion are the SP154
    /// follow-up work. Carries the encoded NTREES value + the surface
    /// name ("literal" / "distance") for diagnostics.
    UnsupportedMultipleTrees { surface: &'static str, ntrees: u32 },
}

impl core::fmt::Display for BrotliContextError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for BrotliContextError {}

impl From<BrotliError> for BrotliContextError {
    fn from(e: BrotliError) -> Self {
        BrotliContextError::Inner(e)
    }
}

/// Decode an NTREES variable-length code per RFC 7932 §7.3.
///
/// The encoding is **identical in shape** to NBLTYPES (RFC §9.2 — see
/// `brotli::decode_nbltypes`): a single '0' bit means NTREES=1; a '1'
/// bit followed by 3 bits N selects a bucket; N=0 yields NTREES=2; N>0
/// reads N more bits as the in-bucket offset, giving NTREES in
/// (2^N+1)..=2^(N+1). The full range is 1..=256.
pub(crate) fn decode_ntrees(r: &mut BitReader) -> Result<u32, BrotliError> {
    // Re-use the NBLTYPES decoder — the spec explicitly shares the
    // variable-length encoding between §9.2 and §7.3.
    decode_nbltypes(r)
}

/// Decode a context-map header per RFC 7932 §7.3, V1 (NTREES=1 only).
///
/// On success: returns the parsed NTREES (always = 1 in V1).
///
/// On NTREES > 1: returns `UnsupportedMultipleTrees` naming the surface
/// (`"literal"` or `"distance"`) for diagnostic messages.
///
/// `surface` is the surface name for diagnostics; pass `"literal"` or
/// `"distance"`.
pub(crate) fn decode_context_map_header_v1(
    r: &mut BitReader,
    surface: &'static str,
) -> Result<u32, BrotliContextError> {
    let ntrees = decode_ntrees(r)?;
    if ntrees > 1 {
        return Err(BrotliContextError::UnsupportedMultipleTrees { surface, ntrees });
    }
    Ok(ntrees)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// L8 KAT-1: trivial NTREES = 1 via single '0' bit.
    /// Byte 0 = 0x00. The header decoder succeeds and returns 1; no
    /// further CMAP body is read.
    #[test]
    fn read_ntrees_trivial_one() {
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let n = decode_context_map_header_v1(&mut r, "literal").unwrap();
        assert_eq!(n, 1);
        assert_eq!(r.bit_pos(), 1, "consumed exactly 1 bit for the '0' single-bit code");
    }

    /// L8 KAT-2: NTREES = 2 (stream "1,0,0,0") → typed
    /// UnsupportedMultipleTrees with surface="literal" and ntrees=2.
    /// Byte 0 = bit 0 set = 0x01.
    #[test]
    fn read_ntrees_larger_rejects_with_followup() {
        let bytes = [0x01u8];
        let mut r = BitReader::new(&bytes);
        let err = decode_context_map_header_v1(&mut r, "literal").unwrap_err();
        match err {
            BrotliContextError::UnsupportedMultipleTrees { surface, ntrees } => {
                assert_eq!(surface, "literal");
                assert_eq!(ntrees, 2);
            }
            other => panic!("expected UnsupportedMultipleTrees, got {other:?}"),
        }
    }

    /// L8 KAT-3: NTREES=12 via the RFC §9.2 worked example "0110111"
    /// (= stream "1,1,1,0,1,1,0"). Surface = "distance" — confirms the
    /// surface tag is propagated for the distance-context-map call site.
    /// Byte 0 = 0x37 (per the existing nbltypes_twelve_via_rfc_example
    /// KAT in brotli.rs).
    #[test]
    fn read_ntrees_twelve_rejects_with_distance_surface() {
        let bytes = [0x37u8];
        let mut r = BitReader::new(&bytes);
        let err = decode_context_map_header_v1(&mut r, "distance").unwrap_err();
        match err {
            BrotliContextError::UnsupportedMultipleTrees { surface, ntrees } => {
                assert_eq!(surface, "distance");
                assert_eq!(ntrees, 12);
            }
            other => panic!("expected UnsupportedMultipleTrees, got {other:?}"),
        }
    }

    /// L8 KAT-4: NTREES=256 (max possible per RFC §9.2 bucket-7) →
    /// rejected. Stream = 11 bits all 1; byte 0 = 0xFF, byte 1 = 0x07.
    #[test]
    fn read_ntrees_max_256_rejects() {
        let bytes = [0xFFu8, 0x07];
        let mut r = BitReader::new(&bytes);
        let err = decode_context_map_header_v1(&mut r, "literal").unwrap_err();
        match err {
            BrotliContextError::UnsupportedMultipleTrees { ntrees, .. } => {
                assert_eq!(ntrees, 256);
            }
            other => panic!("expected UnsupportedMultipleTrees, got {other:?}"),
        }
    }

    /// L8 KAT-5: decode_ntrees standalone (without the V1 reject) returns
    /// the raw value — useful for the future L11 wire-up to feed into the
    /// CMAP-body decoder. Reuse byte 0x37 = NTREES=12.
    #[test]
    fn decode_ntrees_standalone_returns_raw_value() {
        let bytes = [0x37u8];
        let mut r = BitReader::new(&bytes);
        let n = decode_ntrees(&mut r).unwrap();
        assert_eq!(n, 12);
    }

    /// Pentest: empty input → typed BitReader UnexpectedEof bubbles
    /// through Inner.
    #[test]
    fn pentest_empty_input_typed_eof() {
        let bytes: [u8; 0] = [];
        let mut r = BitReader::new(&bytes);
        let err = decode_context_map_header_v1(&mut r, "literal").unwrap_err();
        assert!(
            matches!(
                err,
                BrotliContextError::Inner(BrotliError::BitReader(
                    crate::brotli_bit_reader::BitReaderError::UnexpectedEof
                ))
            ),
            "expected Inner BitReader UnexpectedEof, got {err:?}"
        );
    }
}
