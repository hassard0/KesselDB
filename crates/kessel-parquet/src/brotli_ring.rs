//! Brotli output buffer — RFC 7932 §9.1 (V1 simplified "flat-Vec" model).
//!
//! ## Background
//!
//! Brotli's output is conceptually a **circular ring buffer** of size
//! `window_size = (1 << WBITS) - 16`. Each decoded byte is appended to
//! the ring; LZ77 match-copy commands then back-reference earlier output
//! bytes by `distance` (how far back from the current write position).
//! For streams whose total output exceeds the window the OLDEST bytes
//! get overwritten — but back-references can still legally reach as far
//! back as `min(window_size, output_so_far)`.
//!
//! ## SP154 L12 scope — V1 simplification
//!
//! For V1 we ship a **flat `Vec<u8>`** model: the output buffer just
//! grows linearly, with back-references indexing `output[output.len() -
//! distance..]`. This is byte-identical to the ring model whenever the
//! total decoded output fits in memory (which it MUST for V1 — Parquet
//! pages have `BROTLI_MAX_DECOMP = 256 MiB` cap per SP151). The full
//! ring with wraparound is only needed for streaming-decode of streams
//! that exceed `window_size` AND can't be held in memory — way out of
//! V1 scope (Parquet page decode is one-shot anyway).
//!
//! Concretely the flat model:
//!   - is byte-identical to ring for any stream where total output ≤
//!     `window_size` (= no wraparound ever happens)
//!   - is byte-identical to ring for any stream where total output >
//!     `window_size` AND all back-references stay within the most-recent
//!     `window_size` bytes (= the RFC-mandated case — distances ≤
//!     `window_size` always)
//!   - differs from ring only in MEMORY usage (we hold the full output,
//!     not just the most-recent window). For 256 MiB page cap this is
//!     bounded.
//!
//! ## API
//!
//! `OutputBuffer::new(window_size)` constructs an empty buffer with the
//! given window hint (used for capacity reservation only — the flat
//! model doesn't enforce a wraparound size). `append_byte` / `append_slice`
//! grow the buffer. `lookback(distance)` returns the byte `distance`
//! positions before the current write head. `copy_match(distance, length)`
//! is the LZ77 RLE-aware match copy with the same byte-by-byte forward
//! semantics as LZ4's overlapping-copy.
//!
//! ## Safety
//!
//! `#![forbid(unsafe_code)]`; bounds-checked arithmetic; typed errors
//! for every failure mode (no panics on attacker bytes).

#![allow(dead_code)]

/// Typed errors for the Brotli output buffer.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BrotliRingError {
    /// `lookback(distance)` or `copy_match(distance, _)` was called with
    /// `distance == 0` — invalid per RFC 7932 §4 (distances are >= 1).
    ZeroDistance,
    /// `lookback(distance)` or `copy_match(distance, _)` was called with
    /// `distance > output.len()` — the back-reference points past the
    /// start of the stream.
    DistanceExceedsOutput { distance: usize, output_len: usize },
    /// Cumulative output length would exceed `usize::MAX` (catastrophic;
    /// only reachable on platforms with tiny address spaces).
    OutputLengthOverflow,
}

impl core::fmt::Display for BrotliRingError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for BrotliRingError {}

/// V1 flat-Vec Brotli output buffer. See module docs for the ring-vs-
/// flat tradeoff.
#[derive(Debug, Clone)]
pub(crate) struct OutputBuffer {
    /// Accumulated output bytes (linear, no wraparound).
    out: Vec<u8>,
    /// Window-size hint from the stream header (`(1 << WBITS) - 16`).
    /// In V1 this only seeds the initial capacity; back-reference
    /// validation uses `out.len()` rather than this constant. Future
    /// ring-with-wraparound variant will enforce `distance <= window_size`.
    window_size: usize,
}

impl OutputBuffer {
    /// Construct an empty buffer with the given window-size hint.
    /// `window_size` per RFC 7932 §9.1 is `(1 << WBITS) - 16`; for V1
    /// it only affects initial capacity reservation.
    pub(crate) fn new(window_size: usize) -> Self {
        // Reserve a modest fraction of the window up front (full reservation
        // could be wasteful for tiny pages — most Brotli pages are
        // < 64 KiB even when WBITS=24 → 16 MiB window).
        let initial_cap = window_size.min(64 * 1024);
        Self {
            out: Vec::with_capacity(initial_cap),
            window_size,
        }
    }

    /// Append a single byte at the current write head.
    pub(crate) fn append_byte(&mut self, b: u8) -> Result<(), BrotliRingError> {
        self.out
            .len()
            .checked_add(1)
            .ok_or(BrotliRingError::OutputLengthOverflow)?;
        self.out.push(b);
        Ok(())
    }

    /// Append `slice` at the current write head.
    pub(crate) fn append_slice(&mut self, slice: &[u8]) -> Result<(), BrotliRingError> {
        self.out
            .len()
            .checked_add(slice.len())
            .ok_or(BrotliRingError::OutputLengthOverflow)?;
        self.out.extend_from_slice(slice);
        Ok(())
    }

    /// Look up the byte at `distance` positions BEFORE the current
    /// write head (so `lookback(1)` is the last appended byte).
    /// Returns `ZeroDistance` for distance=0 and `DistanceExceedsOutput`
    /// for distance > output.len().
    pub(crate) fn lookback(&self, distance: usize) -> Result<u8, BrotliRingError> {
        if distance == 0 {
            return Err(BrotliRingError::ZeroDistance);
        }
        if distance > self.out.len() {
            return Err(BrotliRingError::DistanceExceedsOutput {
                distance,
                output_len: self.out.len(),
            });
        }
        // Safe by the bounds-check above.
        let idx = self.out.len() - distance;
        Ok(self.out[idx])
    }

    /// LZ77 RLE-aware match copy: copy `length` bytes from `distance`
    /// behind the current write head to the write head.
    ///
    /// When `distance < length` (the overlapping/RLE case), the
    /// already-copied bytes become the SOURCE for subsequent copies —
    /// this is how Brotli encodes runs of the same character efficiently.
    /// E.g. `copy_match(1, 5)` after one 'A' produces "AAAAA" — each
    /// copied 'A' becomes available as the next source byte. The byte-
    /// by-byte forward loop below is essential for correctness here;
    /// a bulk `extend_from_slice(&out[start..end])` would NOT capture
    /// the RLE semantics.
    pub(crate) fn copy_match(
        &mut self,
        distance: usize,
        length: usize,
    ) -> Result<(), BrotliRingError> {
        if distance == 0 {
            return Err(BrotliRingError::ZeroDistance);
        }
        if distance > self.out.len() {
            return Err(BrotliRingError::DistanceExceedsOutput {
                distance,
                output_len: self.out.len(),
            });
        }
        // Bomb defense: ensure the post-copy length doesn't overflow.
        self.out
            .len()
            .checked_add(length)
            .ok_or(BrotliRingError::OutputLengthOverflow)?;
        // Byte-by-byte forward copy preserves RLE semantics.
        for _ in 0..length {
            // `distance` is the OFFSET back from the current end. As bytes
            // are appended, the source index advances in lock-step, so we
            // re-derive it each iteration.
            let src_idx = self.out.len() - distance;
            let b = self.out[src_idx];
            self.out.push(b);
        }
        Ok(())
    }

    /// LZ77 match copy with PRE-STREAM ZERO PADDING (Brotli ring-buffer
    /// semantics, RFC 7932 §4 / §9.1). When `distance > current_output_len`
    /// the read positions into the ring buffer's "pre-stream zone" — for
    /// the FIRST metablock this zone is implicitly all zeros (= initial
    /// ring buffer state). For subsequent metablocks it would contain the
    /// previous metablock's tail, BUT in V1 we use a flat-Vec model where
    /// all prior metablock data IS retained in `self.out`, so the
    /// pre-stream case only triggers when distance exceeds the entire
    /// accumulated stream.
    ///
    /// Behavior:
    ///   - For each byte of `length`, if `current_offset >= distance` →
    ///     read from `out[out.len() - distance]` (in-stream byte).
    ///   - Else → emit byte 0x00 (= initial ring buffer value).
    /// This matches the Brotli reference C decoder's ring-buffer behavior
    /// when distance points into the implicit zero-padded pre-stream.
    ///
    /// Used by L11 orchestrator for distances where
    /// `distance <= max_backward_distance` (= window_size) AND
    /// `distance > current_output_len` — the "back-reference into
    /// pre-stream zone" case.
    pub(crate) fn copy_match_with_prestream_zeros(
        &mut self,
        distance: usize,
        length: usize,
    ) -> Result<(), BrotliRingError> {
        if distance == 0 {
            return Err(BrotliRingError::ZeroDistance);
        }
        self.out
            .len()
            .checked_add(length)
            .ok_or(BrotliRingError::OutputLengthOverflow)?;
        for _ in 0..length {
            let cur_len = self.out.len();
            if distance <= cur_len {
                let src_idx = cur_len - distance;
                let b = self.out[src_idx];
                self.out.push(b);
            } else {
                // Pre-stream zone → zero.
                self.out.push(0);
            }
        }
        Ok(())
    }

    /// Current accumulated output as a slice.
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.out
    }

    /// Current output length in bytes (= current write-head offset).
    pub(crate) fn len(&self) -> usize {
        self.out.len()
    }

    /// True iff the buffer is empty (= no bytes appended yet).
    pub(crate) fn is_empty(&self) -> bool {
        self.out.is_empty()
    }

    /// Consume the buffer and return the underlying `Vec<u8>`. This is
    /// what the L11 orchestrator returns to the caller at end-of-stream.
    pub(crate) fn into_vec(self) -> Vec<u8> {
        self.out
    }

    /// The window-size hint passed at construction. Used by the L11
    /// orchestrator to decide whether a back-reference is an output-
    /// buffer copy (`distance <= window_size`) or a dictionary match
    /// (`distance > window_size + ndirect`). V1 always uses the
    /// output-buffer branch for in-range distances.
    pub(crate) fn window_size(&self) -> usize {
        self.window_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// L12 KAT-1: a newly constructed buffer is empty.
    #[test]
    fn new_buffer_is_empty() {
        let b = OutputBuffer::new(65520);
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
        assert_eq!(b.as_slice(), b"");
        assert_eq!(b.window_size(), 65520);
    }

    /// L12 KAT-2: `append_byte` + `append_slice` accumulate output in
    /// order; `as_slice` returns the full prefix.
    #[test]
    fn append_byte_and_slice_accumulate() {
        let mut b = OutputBuffer::new(1024);
        b.append_byte(b'h').unwrap();
        b.append_slice(b"ello").unwrap();
        assert_eq!(b.as_slice(), b"hello");
        assert_eq!(b.len(), 5);
    }

    /// L12 KAT-3: `lookback(1)` returns the last-appended byte;
    /// `lookback(N)` returns the byte N positions back.
    #[test]
    fn lookback_returns_recent_bytes() {
        let mut b = OutputBuffer::new(1024);
        b.append_slice(b"abcdef").unwrap();
        assert_eq!(b.lookback(1).unwrap(), b'f');
        assert_eq!(b.lookback(2).unwrap(), b'e');
        assert_eq!(b.lookback(6).unwrap(), b'a');
    }

    /// L12 KAT-4: `lookback(0)` rejects with typed ZeroDistance.
    #[test]
    fn lookback_zero_distance_rejects() {
        let mut b = OutputBuffer::new(1024);
        b.append_byte(b'x').unwrap();
        let err = b.lookback(0).unwrap_err();
        assert!(matches!(err, BrotliRingError::ZeroDistance));
    }

    /// L12 KAT-5: `lookback(N)` for N > output.len() rejects with typed
    /// DistanceExceedsOutput carrying the diagnostic fields.
    #[test]
    fn lookback_past_start_rejects() {
        let mut b = OutputBuffer::new(1024);
        b.append_slice(b"abc").unwrap();
        let err = b.lookback(4).unwrap_err();
        match err {
            BrotliRingError::DistanceExceedsOutput {
                distance,
                output_len,
            } => {
                assert_eq!(distance, 4);
                assert_eq!(output_len, 3);
            }
            other => panic!("expected DistanceExceedsOutput, got {other:?}"),
        }
    }

    /// L12 KAT-6: non-overlapping `copy_match` is a simple back-copy.
    /// Output "abcdef"; copy_match(distance=6, length=3) should append
    /// "abc" → "abcdefabc".
    #[test]
    fn copy_match_non_overlapping_simple_copy() {
        let mut b = OutputBuffer::new(1024);
        b.append_slice(b"abcdef").unwrap();
        b.copy_match(6, 3).unwrap();
        assert_eq!(b.as_slice(), b"abcdefabc");
    }

    /// L12 KAT-7: overlapping `copy_match` with distance=1 produces RLE.
    /// Output "A"; copy_match(distance=1, length=4) should append 4 'A's
    /// → "AAAAA". This is the LZ77 RLE pattern — each copied byte
    /// becomes the source for the next.
    #[test]
    fn copy_match_distance_one_produces_rle() {
        let mut b = OutputBuffer::new(1024);
        b.append_byte(b'A').unwrap();
        b.copy_match(1, 4).unwrap();
        assert_eq!(b.as_slice(), b"AAAAA");
    }

    /// L12 KAT-8: overlapping `copy_match` with distance=2 produces a
    /// 2-byte alternating pattern. Output "AB"; copy_match(distance=2,
    /// length=6) should produce "ABABABAB" (= "AB" + 6 more from the
    /// "AB" pattern).
    #[test]
    fn copy_match_distance_two_produces_alternating_pattern() {
        let mut b = OutputBuffer::new(1024);
        b.append_slice(b"AB").unwrap();
        b.copy_match(2, 6).unwrap();
        assert_eq!(b.as_slice(), b"ABABABAB");
    }

    /// L12 KAT-9: `copy_match(0, _)` rejects with typed ZeroDistance.
    #[test]
    fn copy_match_zero_distance_rejects() {
        let mut b = OutputBuffer::new(1024);
        b.append_byte(b'x').unwrap();
        let err = b.copy_match(0, 3).unwrap_err();
        assert!(matches!(err, BrotliRingError::ZeroDistance));
    }

    /// L12 KAT-10: `copy_match(N, _)` for N > output.len() rejects with
    /// typed DistanceExceedsOutput.
    #[test]
    fn copy_match_past_start_rejects() {
        let mut b = OutputBuffer::new(1024);
        b.append_slice(b"ab").unwrap();
        let err = b.copy_match(5, 1).unwrap_err();
        match err {
            BrotliRingError::DistanceExceedsOutput {
                distance,
                output_len,
            } => {
                assert_eq!(distance, 5);
                assert_eq!(output_len, 2);
            }
            other => panic!("expected DistanceExceedsOutput, got {other:?}"),
        }
    }

    /// L12 KAT-11: `copy_match` with length=0 is a no-op (does not modify
    /// the buffer). Edge case — Brotli's minimum copy length is 2 per RFC §5,
    /// so this branch is never reached in real streams; but the API stays
    /// defensive.
    #[test]
    fn copy_match_zero_length_is_noop() {
        let mut b = OutputBuffer::new(1024);
        b.append_slice(b"abc").unwrap();
        b.copy_match(2, 0).unwrap();
        assert_eq!(b.as_slice(), b"abc");
        assert_eq!(b.len(), 3);
    }

    /// L12 KAT-12: `into_vec` consumes the buffer and returns the
    /// underlying Vec — what the L11 orchestrator returns at end-of-
    /// stream.
    #[test]
    fn into_vec_returns_underlying_buffer() {
        let mut b = OutputBuffer::new(1024);
        b.append_slice(b"hello").unwrap();
        let v = b.into_vec();
        assert_eq!(v, b"hello".to_vec());
    }

    /// Sanity sweep: a sequence of append + copy_match operations
    /// reproduces a known LZ77 expansion. Decode of "ABABAB" as
    /// (literal "AB") + (copy distance=2 length=4) → "ABABAB".
    #[test]
    fn sequence_of_literal_and_copy_reproduces_lz77_expansion() {
        let mut b = OutputBuffer::new(1024);
        b.append_slice(b"AB").unwrap();
        b.copy_match(2, 4).unwrap();
        assert_eq!(b.as_slice(), b"ABABAB");
    }

    /// Pentest: window_size 0 still constructs a valid (empty) buffer.
    /// The flat-Vec model has no window enforcement, so this is fine.
    #[test]
    fn pentest_window_size_zero_still_works() {
        let mut b = OutputBuffer::new(0);
        b.append_byte(b'x').unwrap();
        assert_eq!(b.as_slice(), b"x");
        assert_eq!(b.window_size(), 0);
    }
}
