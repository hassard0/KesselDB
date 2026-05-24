//! Sequence execution for zstd — **SP135 slice of the OBJ-2c-2 arc**.
//!
//! Authority: RFC 8478 §5.4.4 (Sequence Execution).
//!
//! What this module ships (SP135):
//!
//!   1. **`RepeatOffsets`** — 3-slot repeat-offset window per RFC §5.4.4.
//!      Initialized to `[1, 4, 8]` at the start of each frame.
//!
//!   2. **`execute_sequences`** — the LZ77-style decoder driver. For
//!      each sequence:
//!        - Copy `literal_length` bytes from the literals buffer to the
//!          output buffer.
//!        - Compute the REAL offset (RFC §5.4.4 semantics):
//!            raw > 3              : real = raw - 3; rotate-in to slot 0
//!            raw in 1..=3, ll > 0 : real = repeat_offsets[raw-1]; rotate
//!            raw == 1, ll == 0    : real = repeat_offsets[1] (1-shift)
//!            raw == 2, ll == 0    : real = repeat_offsets[2] (2-shift)
//!            raw == 3, ll == 0    : real = repeat_offsets[0] - 1 (decrement)
//!        - Copy `match_length` bytes from `output[len - real..]` to
//!          `output`, BYTE-BY-BYTE (the source and destination overlap
//!          when real < match_length — the canonical LZ77 self-referential
//!          extension pattern, intentional).
//!      After all sequences, copy any remaining literal bytes
//!      (literals[literals_pos..]) to the output.
//!
//! Determinism: pure transforms; same inputs always yield identical
//! output bytes. Bounds-checked: every literals_pos / output_index /
//! real-offset access is validated; typed errors on every overrun /
//! invalid offset; never panics on attacker bytes.

#![allow(dead_code)]

use crate::zstd::ZstdError;
use crate::zstd_sequences::Sequence;

/// 3-slot repeat-offset window per RFC §5.4.4. The slots are NAMED
/// `offset_1`, `offset_2`, `offset_3` in spec text; we use `slots[0..2]`.
/// At frame start: `[1, 4, 8]`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RepeatOffsets {
    pub slots: [u32; 3],
}

impl Default for RepeatOffsets {
    fn default() -> Self {
        Self { slots: [1, 4, 8] }
    }
}

impl RepeatOffsets {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Resolve the real offset for a sequence per RFC §5.4.4 + update the
/// repeat-offset window in place. Returns the real offset to use for
/// the back-reference copy.
fn resolve_offset_and_update_repeats(
    raw_offset: u32,
    literal_length: u32,
    repeats: &mut RepeatOffsets,
) -> Result<u32, ZstdError> {
    // RFC §5.4.4: raw_offset == 0 is invalid (per the encoding
    // raw_offset is always >= 1).
    if raw_offset == 0 {
        return Err(ZstdError::UnexpectedEof);
    }
    let real_offset: u32;
    let new_slots: [u32; 3];
    if raw_offset > 3 {
        // Normal case: real offset = raw - 3; rotate into slot 0.
        real_offset = raw_offset - 3;
        new_slots = [real_offset, repeats.slots[0], repeats.slots[1]];
    } else if literal_length > 0 {
        // Repeat-slot lookup with rotation. raw_offset in 1..=3 maps to
        // slot index 0..=2.
        let idx = (raw_offset - 1) as usize;
        real_offset = repeats.slots[idx];
        if real_offset == 0 {
            return Err(ZstdError::UnexpectedEof);
        }
        // Rotate: new_slots[0] = the selected slot; remaining slots
        // shifted down preserving order, dropping the duplicated.
        new_slots = match raw_offset {
            1 => [repeats.slots[0], repeats.slots[1], repeats.slots[2]],
            2 => [repeats.slots[1], repeats.slots[0], repeats.slots[2]],
            3 => [repeats.slots[2], repeats.slots[0], repeats.slots[1]],
            _ => unreachable!(),
        };
    } else {
        // raw_offset in 1..=3, literal_length == 0: SPECIAL semantics
        // per RFC §5.4.4.
        match raw_offset {
            1 => {
                real_offset = repeats.slots[1];
                if real_offset == 0 {
                    return Err(ZstdError::UnexpectedEof);
                }
                new_slots = [repeats.slots[1], repeats.slots[0], repeats.slots[2]];
            }
            2 => {
                real_offset = repeats.slots[2];
                if real_offset == 0 {
                    return Err(ZstdError::UnexpectedEof);
                }
                new_slots = [repeats.slots[2], repeats.slots[0], repeats.slots[1]];
            }
            3 => {
                real_offset = repeats.slots[0].checked_sub(1).ok_or(ZstdError::UnexpectedEof)?;
                if real_offset == 0 {
                    return Err(ZstdError::UnexpectedEof);
                }
                new_slots = [real_offset, repeats.slots[0], repeats.slots[1]];
            }
            _ => unreachable!(),
        }
    }
    repeats.slots = new_slots;
    Ok(real_offset)
}

/// Execute a sequence of (literal_length, raw_offset, match_length)
/// triples, copying from `literals` + back-references into `out`.
/// Maintains a 3-slot repeat-offset window across calls; pass
/// `RepeatOffsets::new()` for the first sequences-block in a frame.
pub(crate) fn execute_sequences(
    sequences: &[Sequence],
    literals: &[u8],
    repeats: &mut RepeatOffsets,
    out: &mut Vec<u8>,
    output_cap: usize,
) -> Result<usize, ZstdError> {
    let mut literals_pos = 0usize;
    for seq in sequences {
        let ll = seq.literal_length as usize;
        let ml = seq.match_length as usize;
        // 1. Copy literals.
        if literals_pos.checked_add(ll).ok_or(ZstdError::UnexpectedEof)?
            > literals.len()
        {
            return Err(ZstdError::UnexpectedEof);
        }
        if out.len().checked_add(ll).ok_or(ZstdError::UnexpectedEof)? > output_cap {
            return Err(ZstdError::DecompressionBomb {
                decoded: out.len() + ll,
                cap: output_cap,
            });
        }
        out.extend_from_slice(&literals[literals_pos..literals_pos + ll]);
        literals_pos += ll;

        // 2. Resolve real offset + update repeats.
        let real_offset =
            resolve_offset_and_update_repeats(seq.offset, seq.literal_length, repeats)?;

        // 3. Copy match — byte-by-byte for overlap safety (the source
        // and dest can overlap when real_offset < match_length; this is
        // canonical LZ77 self-referential extension).
        if (real_offset as usize) > out.len() {
            return Err(ZstdError::UnexpectedEof);
        }
        if out.len().checked_add(ml).ok_or(ZstdError::UnexpectedEof)? > output_cap {
            return Err(ZstdError::DecompressionBomb {
                decoded: out.len() + ml,
                cap: output_cap,
            });
        }
        let src_start = out.len() - real_offset as usize;
        for i in 0..ml {
            let byte = out[src_start + i];
            out.push(byte);
        }
    }
    // 4. Copy any remaining literals (post-last-sequence tail).
    let tail = &literals[literals_pos..];
    if out.len().checked_add(tail.len()).ok_or(ZstdError::UnexpectedEof)? > output_cap {
        return Err(ZstdError::DecompressionBomb {
            decoded: out.len() + tail.len(),
            cap: output_cap,
        });
    }
    out.extend_from_slice(tail);
    Ok(out.len())
}

// ============================================================================
// KATs — hand-derived from RFC 8478 §5.4.4.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(ll: u32, of: u32, ml: u32) -> Sequence {
        Sequence { literal_length: ll, offset: of, match_length: ml }
    }

    /// SP135-KAT-1: empty sequences list → output is just the literals.
    #[test]
    fn sp135_kat_empty_sequences_copies_literals_tail() {
        let mut repeats = RepeatOffsets::new();
        let mut out = Vec::new();
        let n = execute_sequences(&[], b"hello", &mut repeats, &mut out, 1024).unwrap();
        assert_eq!(n, 5);
        assert_eq!(out, b"hello");
    }

    /// SP135-KAT-2: 1 sequence with raw_offset > 3 (literal copy + back-ref).
    /// Literals = "ABCDE". Sequence: ll=2, raw_offset=2+3=5, ml=2.
    ///   1. Copy 2 literals "AB" → out = "AB".
    ///   2. Resolve offset: raw=5 > 3 → real=2.
    ///   3. Copy 2 bytes from out[len-2..] = "AB" → out = "ABAB".
    ///   4. Copy remaining literals "CDE" → out = "ABABCDE".
    #[test]
    fn sp135_kat_normal_back_reference() {
        let mut repeats = RepeatOffsets::new();
        let mut out = Vec::new();
        let seqs = [seq(2, 5, 2)];
        let n = execute_sequences(&seqs, b"ABCDE", &mut repeats, &mut out, 1024).unwrap();
        assert_eq!(n, 7);
        assert_eq!(out, b"ABABCDE");
        // After this sequence: repeats[0] = 2 (the new real offset).
        assert_eq!(repeats.slots[0], 2);
        assert_eq!(repeats.slots[1], 1); // shifted from old slot 0
        assert_eq!(repeats.slots[2], 4); // shifted from old slot 1
    }

    /// SP135-KAT-3: overlapping back-reference (real_offset < match_length).
    /// Literals = "X". Sequence: ll=1, raw_offset=4 (real=1), ml=4.
    ///   1. Copy 1 literal "X" → out = "X".
    ///   2. real=1.
    ///   3. Copy 4 bytes from out[len-1..] BYTE-BY-BYTE. Source overlaps
    ///      with dest — canonical LZ77 self-ref extension:
    ///        i=0: copy out[0]='X' → out = "XX"
    ///        i=1: copy out[1]='X' → out = "XXX"
    ///        i=2: copy out[2]='X' → out = "XXXX"
    ///        i=3: copy out[3]='X' → out = "XXXXX"
    ///   4. No more literals → out = "XXXXX".
    #[test]
    fn sp135_kat_overlapping_back_reference() {
        let mut repeats = RepeatOffsets::new();
        let mut out = Vec::new();
        let seqs = [seq(1, 4, 4)];
        let n = execute_sequences(&seqs, b"X", &mut repeats, &mut out, 1024).unwrap();
        assert_eq!(n, 5);
        assert_eq!(out, b"XXXXX");
    }

    /// SP135-KAT-4: repeat-offset slot 1 with literal_length > 0.
    /// After SP135-KAT-2 the repeats are [2, 1, 4]. A second sequence
    /// with raw=1, ll=1, ml=2 should reuse repeat[0] = 2.
    /// We'll construct a fresh test: literals = "ABCDEFG"; seqs = [
    ///   (ll=2, raw=5, ml=2),  // sets repeat[0] = 2
    ///   (ll=1, raw=1, ml=2),  // uses repeat[0] = 2
    /// ]
    /// Trace:
    ///   1. ll=2 → "AB" copied; out = "AB".
    ///      real = 5-3 = 2; copy 2 from out[0..2] = "AB" → out = "ABAB".
    ///      repeats = [2, 1, 4].
    ///   2. ll=1 → "C" copied; out = "ABABC".
    ///      raw=1, ll>0 → real = repeats[0] = 2; copy 2 from out[3..5] = "BC" → out = "ABABCBC".
    ///      repeats[0] unchanged on raw=1 (slot 1 doesn't rotate).
    ///   3. Tail = "DEFG"; out = "ABABCBCDEFG".
    #[test]
    fn sp135_kat_repeat_offset_slot_one() {
        let mut repeats = RepeatOffsets::new();
        let mut out = Vec::new();
        let seqs = [seq(2, 5, 2), seq(1, 1, 2)];
        let n = execute_sequences(&seqs, b"ABCDEFG", &mut repeats, &mut out, 1024).unwrap();
        assert_eq!(n, 11);
        assert_eq!(out, b"ABABCBCDEFG");
    }

    /// SP135-KAT-5: raw_offset > output length traps.
    /// out has 0 bytes; a back-reference with real=1 → out.len() < 1 → trap.
    #[test]
    fn sp135_kat_offset_beyond_output_traps() {
        let mut repeats = RepeatOffsets::new();
        let mut out = Vec::new();
        let seqs = [seq(0, 4, 1)]; // ll=0, raw=4 → real=1, but out is empty
        assert_eq!(
            execute_sequences(&seqs, b"", &mut repeats, &mut out, 1024).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP135-KAT-6: literal_length exceeds available literals → typed err.
    #[test]
    fn sp135_kat_literal_overrun_traps() {
        let mut repeats = RepeatOffsets::new();
        let mut out = Vec::new();
        let seqs = [seq(10, 5, 1)]; // ll=10 but literals has only 3 bytes
        assert_eq!(
            execute_sequences(&seqs, b"ABC", &mut repeats, &mut out, 1024).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }

    /// SP135-KAT-7: output exceeds cap → typed DecompressionBomb.
    #[test]
    fn sp135_kat_output_beyond_cap_traps() {
        let mut repeats = RepeatOffsets::new();
        let mut out = Vec::new();
        let seqs = [seq(5, 5, 0)];
        let err = execute_sequences(&seqs, b"HELLO", &mut repeats, &mut out, 3).unwrap_err();
        match err {
            ZstdError::DecompressionBomb { decoded, cap: 3 } => {
                assert_eq!(decoded, 5);
            }
            other => panic!("expected DecompressionBomb; got {other:?}"),
        }
    }

    /// SP135-KAT-8: deterministic — same inputs → identical output.
    #[test]
    fn sp135_kat_deterministic_repeat() {
        let seqs = [seq(2, 5, 2), seq(1, 1, 2)];
        let lit = b"ABCDEFG";
        let mut repeats1 = RepeatOffsets::new();
        let mut out1 = Vec::new();
        execute_sequences(&seqs, lit, &mut repeats1, &mut out1, 1024).unwrap();
        let mut repeats2 = RepeatOffsets::new();
        let mut out2 = Vec::new();
        execute_sequences(&seqs, lit, &mut repeats2, &mut out2, 1024).unwrap();
        assert_eq!(out1, out2);
        assert_eq!(repeats1.slots, repeats2.slots);
    }

    /// SP135-KAT-9: initial repeat offsets are [1, 4, 8] per RFC §5.4.4.
    #[test]
    fn sp135_kat_initial_repeats_are_1_4_8() {
        let r = RepeatOffsets::new();
        assert_eq!(r.slots, [1, 4, 8]);
    }

    /// SP135-KAT-10: raw_offset = 0 is invalid → trap.
    #[test]
    fn sp135_kat_raw_offset_zero_traps() {
        let mut repeats = RepeatOffsets::new();
        let mut out = Vec::new();
        let seqs = [seq(1, 0, 1)];
        assert_eq!(
            execute_sequences(&seqs, b"A", &mut repeats, &mut out, 1024).unwrap_err(),
            ZstdError::UnexpectedEof
        );
    }
}
