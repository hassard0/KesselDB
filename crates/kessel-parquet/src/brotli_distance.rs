//! Brotli distance prefix code translation — RFC 7932 §4.
//!
//! Each insert-and-copy command that signals an EXPLICIT distance
//! (= `distance_implicit == false`, per L9) is followed by a distance
//! prefix code symbol. The distance alphabet size is
//!
//! ```text
//!   16 + NDIRECT + (48 << NPOSTFIX)
//! ```
//!
//! per RFC 7932 §4. SP154 V1 only supports the default Brotli shape
//! (NPOSTFIX=0, NDIRECT=0); other combinations are rejected at L7
//! (`decode_distance_params` surfaces the values; the L11 orchestrator
//! is where the non-default reject happens). With NPOSTFIX=0 + NDIRECT=0
//! the alphabet is exactly **64 symbols** (= 16 + 0 + 48).
//!
//! ## Distance-code semantics (RFC 7932 §4)
//!
//! - **Symbols 0..=15** ("short codes"): select from the 4-entry
//!   recent-distances ring (most-recent = `d1`, then `d2`, `d3`, `d4`).
//!   The 16 short codes are a fixed table of (ring-slot, ± delta)
//!   pairs:
//!
//!   | Code | Distance              |   | Code | Distance              |
//!   |------|-----------------------|---|------|-----------------------|
//!   |  0   | `d1`                  |   |  8   | `d1 - 3`              |
//!   |  1   | `d2`                  |   |  9   | `d1 + 3`              |
//!   |  2   | `d3`                  |   | 10   | `d2 - 1`              |
//!   |  3   | `d4`                  |   | 11   | `d2 + 1`              |
//!   |  4   | `d1 - 1`              |   | 12   | `d2 - 2`              |
//!   |  5   | `d1 + 1`              |   | 13   | `d2 + 2`              |
//!   |  6   | `d1 - 2`              |   | 14   | `d2 - 3`              |
//!   |  7   | `d1 + 2`              |   | 15   | `d2 + 3`              |
//!
//!   This is encoded as two parallel 16-entry tables:
//!   `SHORT_CODE_RING_INDEX` (0 = d1, 1 = d2, 2 = d3, 3 = d4) and
//!   `SHORT_CODE_VALUE_OFFSET` (the ± delta).
//!
//!   A short code that yields `distance <= 0` is invalid (typed
//!   `BrotliDistanceError::InvalidShortDistance`). The L11
//!   orchestrator further enforces `distance <= ring_buffer_size`.
//!
//! - **Symbols >= 16** ("direct codes" with extras): with NPOSTFIX=0,
//!   NDIRECT=0 the formula reduces to:
//!
//!   ```text
//!     ndistbits = 1 + ((c - 16) >> 1)
//!     offset    = ((2 + ((c - 16) & 1)) << ndistbits) - 4
//!     distance  = offset + extras + 1
//!   ```
//!
//!   `ndistbits` extra bits are read LSB-first. Concrete sub-ranges:
//!
//!   | Code  | ndistbits | distance range          |
//!   |-------|-----------|-------------------------|
//!   | 16    | 1         | 1..=2                   |
//!   | 17    | 1         | 3..=4                   |
//!   | 18    | 2         | 5..=8                   |
//!   | 19    | 2         | 9..=12                  |
//!   | 20    | 3         | 13..=20                 |
//!   | 21    | 3         | 21..=28                 |
//!   | ...   | ...       | ...                     |
//!   | 62    | 24        | 33,554,429..=50,331,644 |
//!   | 63    | 24        | 50,331,645..=67,108,860 |
//!
//! ## SP154 L9b scope (this file)
//!
//! V1 ships pure translation:
//!   - `SHORT_CODE_RING_INDEX` + `SHORT_CODE_VALUE_OFFSET` tables.
//!   - `DistanceRing` — the 4-entry recent-distances ring with the
//!     Brotli initial values `[16, 15, 11, 4]` per RFC 7932 §4.
//!   - `translate_short_distance(sym, &ring)` — symbol 0..=15 → distance.
//!   - `translate_direct_distance(r, sym)` — symbol 16..=63 → distance,
//!     reads the extras from the bit stream (NPOSTFIX=0, NDIRECT=0).
//!   - `decode_distance(r, sym, ring)` — single entry point that
//!     dispatches on the symbol and (on success) updates the ring with
//!     the new distance per RFC §4.
//!
//! V1 does NOT ship:
//!   - Non-default NPOSTFIX or NDIRECT (rejected at L11 wire-up). The
//!     full formula would be:
//!       `ndistbits = 1 + ((c - ND - 16) >> (P + 1))`
//!       `hcode = (c - ND - 16) >> P`
//!       `lcode = (c - ND - 16) & ((1 << P) - 1)`
//!       `offset = ((2 + (hcode & 1)) << ndistbits) - 4`
//!       `distance = ((offset + extras) << P) + lcode + ND + 1`
//!   - The 0..=NDIRECT-1 direct-distance fast path (between short codes
//!     16-15 and the extras range).
//!   - Reading the distance prefix code itself — that's the
//!     `brotli_huffman` machinery + the L11 context-tree lookup.
//!
//! ## Safety
//!
//! `#![forbid(unsafe_code)]`; bounds-checked arithmetic; typed errors
//! for every failure mode (no panics).

#![allow(dead_code)]

use crate::brotli::BrotliError;
use crate::brotli_bit_reader::BitReader;

/// Number of "short distance" symbols (the 0..=15 range that selects
/// from the recent-distances ring with small ± deltas).
pub(crate) const NUM_SHORT_DISTANCE_CODES: u32 = 16;

/// Number of distance symbols in V1 (NPOSTFIX=0, NDIRECT=0):
/// 16 short codes + 48 direct-code-with-extras = 64.
pub(crate) const NUM_DISTANCE_CODES_V1: u32 = 64;

/// Brotli initial state of the recent-distances ring per RFC 7932 §4:
/// "Initially the four entries of the ring buffer are 16, 15, 11, 4."
///
/// `RING_INIT[0]` is `d1` (most-recent semantically — although the
/// "first" distance the decoder reads will *replace* d1; until then,
/// short-code 0 = 16 is the documented Brotli starting distance).
pub(crate) const RING_INIT: [u32; 4] = [16, 15, 11, 4];

/// Per-short-code recent-distance ring SLOT INDEX. 0 = d1 (most-recent),
/// 1 = d2, 2 = d3, 3 = d4. Indexed by the short-code symbol 0..=15.
///
/// Per RFC 7932 §4: short codes 0..=3 each select a different ring slot;
/// codes 4..=9 all use d1 (with ± 1..=3 deltas); codes 10..=15 all use
/// d2 (with ± 1..=3 deltas). Pinned by the
/// `short_code_ring_index_table_matches_rfc` KAT below.
pub(crate) const SHORT_CODE_RING_INDEX: [u8; 16] = [
    0, 1, 2, 3, // d1, d2, d3, d4
    0, 0, 0, 0, // d1 ± 1, ± 2
    0, 0, // d1 ± 3
    1, 1, 1, 1, // d2 ± 1, ± 2
    1, 1, // d2 ± 3
];

/// Per-short-code ± delta added to the ring entry. Signed (i32 fits
/// the -3..=+3 range easily) but stored as i8 for compactness.
/// Pinned by `short_code_value_offset_table_matches_rfc` KAT below.
pub(crate) const SHORT_CODE_VALUE_OFFSET: [i8; 16] = [
    0, 0, 0, 0, // d1, d2, d3, d4
    -1, 1, -2, 2, // d1 ± 1, ± 2
    -3, 3, // d1 ± 3
    -1, 1, -2, 2, // d2 ± 1, ± 2
    -3, 3, // d2 ± 3
];

/// 4-entry recent-distances ring per RFC 7932 §4.
///
/// Conceptually `slots[0]` is d1 (most-recent), `slots[1]` is d2,
/// `slots[2]` is d3, `slots[3]` is d4. The Brotli spec defines initial
/// values `[16, 15, 11, 4]` (`RING_INIT`).
///
/// `push(d)` shifts: d4 ← d3, d3 ← d2, d2 ← d1, d1 ← d. (Concrete
/// implementations may use a circular index, but the semantics are
/// what the spec guarantees.)
///
/// Per RFC 7932 §4: short code 0 (= "use d1 unchanged") MUST NOT
/// update the ring; the orchestrator decides this. Direct codes
/// always update the ring. The `decode_distance` function returns
/// the new distance and an `update_ring` flag so the caller can
/// decide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DistanceRing {
    /// `slots[0]` = d1 (most-recent), `slots[1]` = d2, `slots[2]` = d3,
    /// `slots[3]` = d4.
    pub(crate) slots: [u32; 4],
}

impl DistanceRing {
    /// New ring initialised to the Brotli RFC 7932 §4 defaults.
    pub(crate) fn new() -> Self {
        Self { slots: RING_INIT }
    }

    /// Push a new most-recent distance, shifting older entries out.
    ///   d1 ← d, d2 ← old_d1, d3 ← old_d2, d4 ← old_d3 (old d4 evicted).
    pub(crate) fn push(&mut self, d: u32) {
        self.slots = [d, self.slots[0], self.slots[1], self.slots[2]];
    }
}

impl Default for DistanceRing {
    fn default() -> Self {
        Self::new()
    }
}

/// Typed errors specific to distance-code decoding. Wraps
/// `BrotliError` for nested bit-reader failures.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BrotliDistanceError {
    /// Inner brotli / bit-reader error.
    Inner(BrotliError),
    /// Distance symbol exceeded the V1 alphabet size (= 64 symbols for
    /// NPOSTFIX=0 + NDIRECT=0).
    DistanceSymbolOutOfRange { sym: u32 },
    /// Short-code translation yielded distance <= 0 (e.g. code 4 → d1-1
    /// when d1 = 1 → distance = 0). Per RFC 7932 §4 the resulting
    /// distance MUST be >= 1; otherwise the stream is malformed.
    InvalidShortDistance { sym: u8, computed: i64 },
}

impl core::fmt::Display for BrotliDistanceError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for BrotliDistanceError {}

impl From<BrotliError> for BrotliDistanceError {
    fn from(e: BrotliError) -> Self {
        BrotliDistanceError::Inner(e)
    }
}

/// Translate a SHORT distance symbol (0..=15) using the recent-distances
/// ring. Returns the resulting distance (>= 1) and a flag indicating
/// whether the ring should be updated:
///   - symbol 0 (= "reuse d1") does NOT update the ring per RFC §4.
///   - symbols 1..=15 DO update the ring.
///
/// A computed distance of <= 0 surfaces `InvalidShortDistance` (e.g. if
/// the encoder asked for "d1 - 3" when d1 was 1).
pub(crate) fn translate_short_distance(
    sym: u8,
    ring: &DistanceRing,
) -> Result<(u32, bool), BrotliDistanceError> {
    if sym >= 16 {
        return Err(BrotliDistanceError::DistanceSymbolOutOfRange { sym: sym as u32 });
    }
    let idx = SHORT_CODE_RING_INDEX[sym as usize] as usize;
    let offset = SHORT_CODE_VALUE_OFFSET[sym as usize] as i64;
    let base = ring.slots[idx] as i64;
    let computed = base + offset;
    if computed <= 0 {
        return Err(BrotliDistanceError::InvalidShortDistance { sym, computed });
    }
    // Symbol 0 does not update the ring. All other short codes do.
    let update_ring = sym != 0;
    Ok((computed as u32, update_ring))
}

/// Translate a DIRECT distance symbol (16..=63) by reading the
/// appropriate number of extra bits from the stream.
///
/// V1 = NPOSTFIX=0, NDIRECT=0. Formula:
///
/// ```text
///   ndistbits = 1 + ((c - 16) >> 1)
///   offset    = ((2 + ((c - 16) & 1)) << ndistbits) - 4
///   distance  = offset + extras + 1
/// ```
///
/// Range per code is documented in the module-level table. All direct
/// codes UPDATE the recent-distances ring (returned via the bool).
pub(crate) fn translate_direct_distance(
    r: &mut BitReader,
    sym: u32,
) -> Result<(u32, bool), BrotliDistanceError> {
    if sym < 16 || sym >= NUM_DISTANCE_CODES_V1 {
        return Err(BrotliDistanceError::DistanceSymbolOutOfRange { sym });
    }
    let c = sym - 16; // 0..=47
    let ndistbits = (1 + (c >> 1)) as u8; // 1..=24
    let offset_base: i64 = ((2 + ((c & 1) as i64)) << ndistbits) - 4;
    let extras = r.read_bits(ndistbits).map_err(BrotliError::from)?;
    let distance_i64 = offset_base + extras as i64 + 1;
    // Direct codes always yield distance >= 1 (offset_base + extras + 1
    // with offset_base >= 0 for ndistbits >= 1). u32 fits the full range
    // (max ~67M for c=47/ndistbits=24).
    let distance = u32::try_from(distance_i64)
        .map_err(|_| BrotliDistanceError::Inner(BrotliError::UnexpectedEof))?;
    Ok((distance, true))
}

/// Decode a distance symbol (0..=63 for V1) into an actual distance
/// value, updating the recent-distances ring as required by RFC §4.
///
/// Dispatches on the symbol value:
///   - sym < 16  → `translate_short_distance`
///   - sym >= 16 → `translate_direct_distance` (reads extras)
///
/// Returns the decoded distance. The ring is updated in-place (except
/// for short-code 0, which reuses d1 without updating per RFC §4).
pub(crate) fn decode_distance(
    r: &mut BitReader,
    sym: u32,
    ring: &mut DistanceRing,
) -> Result<u32, BrotliDistanceError> {
    if sym >= NUM_DISTANCE_CODES_V1 {
        return Err(BrotliDistanceError::DistanceSymbolOutOfRange { sym });
    }
    let (distance, update_ring) = if sym < 16 {
        translate_short_distance(sym as u8, ring)?
    } else {
        translate_direct_distance(r, sym)?
    };
    if update_ring {
        ring.push(distance);
    }
    Ok(distance)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// L9b KAT-1: short-code ring index table matches RFC 7932 §4.
    /// Pinned values: codes 0..=3 select distinct slots; codes 4..=9
    /// all use d1 (slot 0); codes 10..=15 all use d2 (slot 1).
    #[test]
    fn short_code_ring_index_table_matches_rfc() {
        assert_eq!(SHORT_CODE_RING_INDEX[0], 0, "code 0 → d1");
        assert_eq!(SHORT_CODE_RING_INDEX[1], 1, "code 1 → d2");
        assert_eq!(SHORT_CODE_RING_INDEX[2], 2, "code 2 → d3");
        assert_eq!(SHORT_CODE_RING_INDEX[3], 3, "code 3 → d4");
        for c in 4..=9 {
            assert_eq!(SHORT_CODE_RING_INDEX[c], 0, "code {c} → d1");
        }
        for c in 10..=15 {
            assert_eq!(SHORT_CODE_RING_INDEX[c], 1, "code {c} → d2");
        }
    }

    /// L9b KAT-2: short-code value-offset table matches RFC 7932 §4
    /// (the ± 1, ± 2, ± 3 deltas).
    #[test]
    fn short_code_value_offset_table_matches_rfc() {
        // Codes 0..=3: zero delta (direct ring lookup).
        assert_eq!(SHORT_CODE_VALUE_OFFSET[0], 0);
        assert_eq!(SHORT_CODE_VALUE_OFFSET[1], 0);
        assert_eq!(SHORT_CODE_VALUE_OFFSET[2], 0);
        assert_eq!(SHORT_CODE_VALUE_OFFSET[3], 0);
        // Codes 4..=9 (d1 group): -1, +1, -2, +2, -3, +3
        assert_eq!(SHORT_CODE_VALUE_OFFSET[4], -1);
        assert_eq!(SHORT_CODE_VALUE_OFFSET[5], 1);
        assert_eq!(SHORT_CODE_VALUE_OFFSET[6], -2);
        assert_eq!(SHORT_CODE_VALUE_OFFSET[7], 2);
        assert_eq!(SHORT_CODE_VALUE_OFFSET[8], -3);
        assert_eq!(SHORT_CODE_VALUE_OFFSET[9], 3);
        // Codes 10..=15 (d2 group): -1, +1, -2, +2, -3, +3
        assert_eq!(SHORT_CODE_VALUE_OFFSET[10], -1);
        assert_eq!(SHORT_CODE_VALUE_OFFSET[11], 1);
        assert_eq!(SHORT_CODE_VALUE_OFFSET[12], -2);
        assert_eq!(SHORT_CODE_VALUE_OFFSET[13], 2);
        assert_eq!(SHORT_CODE_VALUE_OFFSET[14], -3);
        assert_eq!(SHORT_CODE_VALUE_OFFSET[15], 3);
    }

    /// L9b KAT-3: ring `new()` returns the RFC 7932 §4 initial values
    /// `[16, 15, 11, 4]`.
    #[test]
    fn ring_new_uses_rfc_initial_values() {
        let r = DistanceRing::new();
        assert_eq!(r.slots, [16, 15, 11, 4]);
    }

    /// L9b KAT-4: ring `push(D)` shifts: d1 ← D, d2 ← old_d1, etc.;
    /// old d4 is evicted.
    #[test]
    fn ring_push_shifts_in_correct_order() {
        let mut r = DistanceRing::new();
        r.push(100);
        assert_eq!(r.slots, [100, 16, 15, 11]);
        r.push(200);
        assert_eq!(r.slots, [200, 100, 16, 15]);
    }

    /// L9b KAT-5: short-code 0 → d1 unchanged; does NOT update the ring.
    /// (Per RFC §4 the "reuse d1" path is the ONLY short code that
    /// preserves the ring contents.)
    #[test]
    fn short_code_zero_returns_d1_without_updating_ring() {
        let ring = DistanceRing::new();
        let (d, update) = translate_short_distance(0, &ring).unwrap();
        assert_eq!(d, 16, "code 0 → d1 = 16 (initial)");
        assert!(!update, "code 0 must NOT update the ring");
    }

    /// L9b KAT-6: short-code 1 → d2 = 15; updates the ring.
    /// Short-code 2 → d3 = 11; updates the ring.
    /// Short-code 3 → d4 = 4; updates the ring.
    #[test]
    fn short_codes_1_2_3_select_d2_d3_d4() {
        let ring = DistanceRing::new();
        let (d, u) = translate_short_distance(1, &ring).unwrap();
        assert_eq!((d, u), (15, true));
        let (d, u) = translate_short_distance(2, &ring).unwrap();
        assert_eq!((d, u), (11, true));
        let (d, u) = translate_short_distance(3, &ring).unwrap();
        assert_eq!((d, u), (4, true));
    }

    /// L9b KAT-7: short-code 4 → d1 - 1 = 15 (with initial d1=16);
    /// updates the ring. (Verifies the ± delta arithmetic.)
    #[test]
    fn short_code_four_returns_d1_minus_one() {
        let ring = DistanceRing::new();
        let (d, u) = translate_short_distance(4, &ring).unwrap();
        assert_eq!((d, u), (15, true));
    }

    /// L9b KAT-8: short-code 5 → d1 + 1 = 17.
    /// Short-code 9 → d1 + 3 = 19. (d1 group max-delta.)
    #[test]
    fn short_code_d1_plus_deltas() {
        let ring = DistanceRing::new();
        let (d, _) = translate_short_distance(5, &ring).unwrap();
        assert_eq!(d, 17, "d1 + 1 = 17");
        let (d, _) = translate_short_distance(9, &ring).unwrap();
        assert_eq!(d, 19, "d1 + 3 = 19");
    }

    /// L9b KAT-9: short-code 10 → d2 - 1 = 14 (d2=15).
    /// Short-code 15 → d2 + 3 = 18.
    #[test]
    fn short_code_d2_deltas() {
        let ring = DistanceRing::new();
        let (d, _) = translate_short_distance(10, &ring).unwrap();
        assert_eq!(d, 14, "d2 - 1 = 14");
        let (d, _) = translate_short_distance(15, &ring).unwrap();
        assert_eq!(d, 18, "d2 + 3 = 18");
    }

    /// L9b KAT-10: short code that would yield distance <= 0 surfaces
    /// `InvalidShortDistance`. Force this by constructing a ring where
    /// d1 = 1 and then asking for code 8 (= d1 - 3 = -2).
    #[test]
    fn short_code_invalid_negative_distance_rejects() {
        let ring = DistanceRing { slots: [1, 15, 11, 4] };
        let err = translate_short_distance(8, &ring).unwrap_err();
        match err {
            BrotliDistanceError::InvalidShortDistance { sym, computed } => {
                assert_eq!(sym, 8);
                assert_eq!(computed, -2);
            }
            other => panic!("expected InvalidShortDistance, got {other:?}"),
        }
    }

    /// L9b KAT-11: short code 16 (= boundary, out of short range) rejects.
    #[test]
    fn short_code_sixteen_out_of_range() {
        let ring = DistanceRing::new();
        let err = translate_short_distance(16, &ring).unwrap_err();
        match err {
            BrotliDistanceError::DistanceSymbolOutOfRange { sym } => assert_eq!(sym, 16),
            other => panic!("expected DistanceSymbolOutOfRange, got {other:?}"),
        }
    }

    /// L9b KAT-12: direct code 16 with extras=0 → distance = 1.
    /// ndistbits = 1 + (0 >> 1) = 1; offset = ((2 + 0) << 1) - 4 = 0;
    /// distance = 0 + 0 + 1 = 1. (Stream bit: 0.)
    #[test]
    fn direct_code_16_extras_zero_distance_one() {
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let (d, u) = translate_direct_distance(&mut r, 16).unwrap();
        assert_eq!(d, 1);
        assert!(u);
        assert_eq!(r.bit_pos(), 1);
    }

    /// L9b KAT-13: direct code 16 with extras=1 → distance = 2.
    /// (Stream bit: 1.)
    #[test]
    fn direct_code_16_extras_one_distance_two() {
        let bytes = [0x01u8];
        let mut r = BitReader::new(&bytes);
        let (d, _) = translate_direct_distance(&mut r, 16).unwrap();
        assert_eq!(d, 2);
        assert_eq!(r.bit_pos(), 1);
    }

    /// L9b KAT-14: direct code 17 → range 3..=4.
    /// ndistbits = 1 + (1 >> 1) = 1; offset = ((2 + 1) << 1) - 4 = 2;
    /// extras = 0 → distance = 3; extras = 1 → distance = 4.
    #[test]
    fn direct_code_17_range_three_to_four() {
        // extras = 0 → 3
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let (d, _) = translate_direct_distance(&mut r, 17).unwrap();
        assert_eq!(d, 3);
        // extras = 1 → 4
        let bytes = [0x01u8];
        let mut r = BitReader::new(&bytes);
        let (d, _) = translate_direct_distance(&mut r, 17).unwrap();
        assert_eq!(d, 4);
    }

    /// L9b KAT-15: direct code 18 → range 5..=8.
    /// ndistbits = 1 + (2 >> 1) = 2; offset = ((2 + 0) << 2) - 4 = 4;
    /// extras = 0..=3 → 5..=8.
    #[test]
    fn direct_code_18_range_five_to_eight() {
        for (extras, expected) in [(0u8, 5u32), (1, 6), (2, 7), (3, 8)] {
            let bytes = [extras];
            let mut r = BitReader::new(&bytes);
            let (d, _) = translate_direct_distance(&mut r, 18).unwrap();
            assert_eq!(d, expected, "code 18 extras={extras} → distance={expected}");
            assert_eq!(r.bit_pos(), 2);
        }
    }

    /// L9b KAT-16: direct code 19 → range 9..=12.
    /// ndistbits = 1 + (3 >> 1) = 2; offset = ((2 + 1) << 2) - 4 = 8.
    #[test]
    fn direct_code_19_range_nine_to_twelve() {
        for (extras, expected) in [(0u8, 9u32), (1, 10), (2, 11), (3, 12)] {
            let bytes = [extras];
            let mut r = BitReader::new(&bytes);
            let (d, _) = translate_direct_distance(&mut r, 19).unwrap();
            assert_eq!(d, expected, "code 19 extras={extras} → distance={expected}");
        }
    }

    /// L9b KAT-17: direct code 20 → range 13..=20 (3 extra bits).
    /// ndistbits = 1 + (4 >> 1) = 3; offset = ((2 + 0) << 3) - 4 = 12;
    /// extras 0..=7 → 13..=20.
    #[test]
    fn direct_code_20_three_extra_bits() {
        // extras = 7 → 12 + 7 + 1 = 20
        let bytes = [0x07u8];
        let mut r = BitReader::new(&bytes);
        let (d, _) = translate_direct_distance(&mut r, 20).unwrap();
        assert_eq!(d, 20);
        assert_eq!(r.bit_pos(), 3);
    }

    /// L9b KAT-18: direct code 63 (the max V1 symbol) has 24 extra bits.
    /// ndistbits = 1 + ((63 - 16) >> 1) = 1 + 23 = 24.
    /// offset = ((2 + ((63 - 16) & 1)) << 24) - 4 = ((2+1)<<24) - 4
    ///        = 50_331_648 - 4 = 50_331_644.
    /// extras = 0 → distance = 50_331_644 + 0 + 1 = 50_331_645.
    /// (Need 3 bytes = 24 bits of zeros.)
    #[test]
    fn direct_code_63_max_with_zero_extras() {
        let bytes = [0u8; 3];
        let mut r = BitReader::new(&bytes);
        let (d, _) = translate_direct_distance(&mut r, 63).unwrap();
        assert_eq!(d, 50_331_645);
        assert_eq!(r.bit_pos(), 24);
    }

    /// L9b KAT-19: direct code 64 (out of V1 range) rejects.
    #[test]
    fn direct_code_64_out_of_range() {
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let err = translate_direct_distance(&mut r, 64).unwrap_err();
        match err {
            BrotliDistanceError::DistanceSymbolOutOfRange { sym } => assert_eq!(sym, 64),
            other => panic!("expected DistanceSymbolOutOfRange, got {other:?}"),
        }
    }

    /// L9b KAT-20: direct code 15 routed to direct path rejects (< 16).
    /// Mirrors the boundary check on the other side.
    #[test]
    fn direct_code_below_sixteen_rejects() {
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let err = translate_direct_distance(&mut r, 15).unwrap_err();
        match err {
            BrotliDistanceError::DistanceSymbolOutOfRange { sym } => assert_eq!(sym, 15),
            other => panic!("expected DistanceSymbolOutOfRange, got {other:?}"),
        }
    }

    /// L9b KAT-21: `decode_distance` dispatches on short range:
    /// sym=1 → d2 = 15 with ring update.
    #[test]
    fn decode_distance_dispatches_short_path() {
        let mut ring = DistanceRing::new();
        let bytes = [0x00u8]; // unused for short codes
        let mut r = BitReader::new(&bytes);
        let d = decode_distance(&mut r, 1, &mut ring).unwrap();
        assert_eq!(d, 15);
        // Ring should have shifted: d1 = 15, d2 = old d1 = 16, etc.
        assert_eq!(ring.slots, [15, 16, 15, 11]);
        // No bits consumed for short codes.
        assert_eq!(r.bit_pos(), 0);
    }

    /// L9b KAT-22: `decode_distance` for sym=0 ("reuse d1") does NOT
    /// update the ring.
    #[test]
    fn decode_distance_short_zero_preserves_ring() {
        let mut ring = DistanceRing::new();
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let d = decode_distance(&mut r, 0, &mut ring).unwrap();
        assert_eq!(d, 16);
        assert_eq!(ring.slots, [16, 15, 11, 4], "ring must be unchanged");
        assert_eq!(r.bit_pos(), 0);
    }

    /// L9b KAT-23: `decode_distance` dispatches on direct range:
    /// sym=16 with extras=0 → distance=1; updates ring.
    #[test]
    fn decode_distance_dispatches_direct_path() {
        let mut ring = DistanceRing::new();
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let d = decode_distance(&mut r, 16, &mut ring).unwrap();
        assert_eq!(d, 1);
        assert_eq!(ring.slots, [1, 16, 15, 11], "ring must shift in the new distance");
        assert_eq!(r.bit_pos(), 1);
    }

    /// L9b KAT-24: `decode_distance` for an out-of-V1-range symbol
    /// (>= 64) rejects with typed `DistanceSymbolOutOfRange`.
    #[test]
    fn decode_distance_above_v1_rejects() {
        let mut ring = DistanceRing::new();
        let bytes = [0x00u8];
        let mut r = BitReader::new(&bytes);
        let err = decode_distance(&mut r, 64, &mut ring).unwrap_err();
        match err {
            BrotliDistanceError::DistanceSymbolOutOfRange { sym } => assert_eq!(sym, 64),
            other => panic!("expected DistanceSymbolOutOfRange, got {other:?}"),
        }
    }

    /// Pentest: insufficient stream bits for direct-code extras →
    /// typed Inner BitReader UnexpectedEof bubbles through. code 62
    /// needs 24 extras (= 3 bytes); stream of 2 bytes is too short.
    #[test]
    fn pentest_direct_code_truncated_extras_stream() {
        let bytes = [0xFFu8, 0xFF]; // 16 bits, need 24
        let mut r = BitReader::new(&bytes);
        let err = translate_direct_distance(&mut r, 62).unwrap_err();
        assert!(
            matches!(
                err,
                BrotliDistanceError::Inner(BrotliError::BitReader(
                    crate::brotli_bit_reader::BitReaderError::UnexpectedEof
                ))
            ),
            "expected Inner BitReader UnexpectedEof, got {err:?}"
        );
    }

    /// Sanity sweep: every V1 distance symbol 16..=63 with extras=0
    /// produces a distance in the documented sub-range AND the
    /// per-code ranges form a non-overlapping, monotonically increasing
    /// partition of [1, ~67M]. Locks the formula against off-by-one.
    #[test]
    fn all_direct_codes_yield_non_overlapping_monotonic_ranges() {
        let mut prev_max = 0u64; // distance 0 is invalid; first range starts at 1
        for sym in 16u32..NUM_DISTANCE_CODES_V1 {
            // ndistbits = 1 + ((c - 16) >> 1)
            let c = sym - 16;
            let ndistbits = 1 + (c >> 1);
            // Min: extras = 0
            let bytes = vec![0u8; ((ndistbits + 7) / 8) as usize];
            let mut r = BitReader::new(&bytes);
            let (min_d, _) = translate_direct_distance(&mut r, sym).unwrap();
            // Max: extras = (1 << ndistbits) - 1
            let max_bytes = vec![0xFFu8; ((ndistbits + 7) / 8) as usize];
            let mut r = BitReader::new(&max_bytes);
            // The read consumes exactly ndistbits LSB-first; the
            // remaining high bits of the last byte are ignored.
            let (max_d, _) = translate_direct_distance(&mut r, sym).unwrap();
            assert!(
                min_d as u64 > prev_max,
                "sym {sym}: min {min_d} must exceed prev_max {prev_max}"
            );
            assert!(
                max_d >= min_d,
                "sym {sym}: max {max_d} must be >= min {min_d}"
            );
            prev_max = max_d as u64;
        }
        // The very last (sym=63) max ranges to (3 << 24) - 4 + (1<<24 - 1) + 1
        // = 50_331_644 + 16_777_215 + 1 = 67_108_860.
        assert_eq!(prev_max, 67_108_860);
    }

    /// Sanity: `decode_distance` ring update is consistent — after a
    /// direct decode of distance D, short-code 0 next returns D.
    #[test]
    fn after_direct_decode_short_zero_returns_new_d1() {
        let mut ring = DistanceRing::new();
        let bytes = [0x07u8]; // direct code 20 extras=7 → distance=20
        let mut r = BitReader::new(&bytes);
        let d = decode_distance(&mut r, 20, &mut ring).unwrap();
        assert_eq!(d, 20);
        // Now use short-code 0 → should yield the same 20.
        let bytes2 = [0u8];
        let mut r2 = BitReader::new(&bytes2);
        let d2 = decode_distance(&mut r2, 0, &mut ring).unwrap();
        assert_eq!(d2, 20, "short-code 0 must return the most-recent distance");
    }
}
