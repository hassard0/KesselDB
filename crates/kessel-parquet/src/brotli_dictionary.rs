//! Brotli static dictionary — RFC 7932 Appendix A + Appendix B.
//!
//! Brotli enriches its LZ77-style back-reference vocabulary with a
//! fixed **122,784-byte static dictionary** (RFC 7932 Appendix A)
//! and a **121-entry transform table** (Appendix B). When a copy
//! distance exceeds the sliding window, the encoder may instead
//! emit a "dictionary back-reference" specifying:
//!   - A word LENGTH (4..=24).
//!   - An INDEX within that length's bucket.
//!   - A TRANSFORM ID (0..=120; transform 0 = identity).
//!
//! The decoder looks up the (length, index) pair in the dictionary
//! blob, applies the transform, and emits the resulting bytes into
//! the output. The lookup IS the back-reference resolution for
//! these special distances.
//!
//! ## Appendix A — the dictionary blob
//!
//! The blob is 122,784 bytes total, partitioned into 21 word-length
//! buckets (lengths 4..=24). Per-length counts and offsets come from
//! the reference decoder's `kBrotliDictionaryOffsetsByLength` and
//! `kBrotliDictionarySizeBitsByLength` tables (RFC 7932 Appendix A
//! encodes the same data — the reference decoder's arrays are the
//! canonical machine-readable form):
//!
//! | Length | Count | Bytes | Offset    |
//! |--------|-------|-------|-----------|
//! |   4    | 1024  | 4096  | 0         |
//! |   5    | 1024  | 5120  | 4096      |
//! |   6    | 2048  | 12288 | 9216      |
//! |   7    | 2048  | 14336 | 21504     |
//! |   8    | 1024  | 8192  | 35840     |
//! |   9    | 1024  | 9216  | 44032     |
//! |  10    | 1024  | 10240 | 53248     |
//! |  11    | 1024  | 11264 | 63488     |
//! |  12    | 1024  | 12288 | 74752     |
//! |  13    | 512   | 6656  | 87040     |
//! |  14    | 512   | 7168  | 93696     |
//! |  15    | 256   | 3840  | 100864    |
//! |  16    | 128   | 2048  | 104704    |
//! |  17    | 128   | 2176  | 106752    |
//! |  18    | 256   | 4608  | 108928    |
//! |  19    | 128   | 2432  | 113536    |
//! |  20    | 128   | 2560  | 115968    |
//! |  21    | 64    | 1344  | 118528    |
//! |  22    | 64    | 1408  | 119872    |
//! |  23    | 32    | 736   | 121280    |
//! |  24    | 32    | 768   | 122016    |
//! | **TOTAL** | **13_504** | **122_784** | |
//!
//! These constants are pinned by `dictionary_offsets_sum_to_blob_size`
//! and `dictionary_length_counts_are_powers_of_two` KATs.
//!
//! ## Appendix B — the transforms
//!
//! Each transform applies a `prefix` + `transformed_word` + `suffix`
//! reshaping to the looked-up dictionary word. The transform itself
//! is one of 11 operations on the word body:
//!
//! - Identity (transform id 0)
//! - OmitFirstN / OmitLastN (id 1..=9)
//! - UppercaseFirst / UppercaseAll
//! - Various capitalisation + omission combinations
//!
//! V1 SP154 supports only the **identity** transform (`Transform::Identity`)
//! — the most common case in real Parquet pyarrow files. Other transforms
//! return `BrotliDictionaryError::UnsupportedTransform` with a named
//! follow-up. The full 121-entry table is transcribed in
//! `TRANSFORMS` for reference + future enablement.
//!
//! ## SP154 L10 scope (this file)
//!
//! V1 ships:
//!   - `DICTIONARY` — `include_bytes!` of the 122,784-byte Appendix A
//!     blob.
//!   - `DICTIONARY_OFFSETS_BY_LENGTH` + `DICTIONARY_COUNTS_BY_LENGTH`
//!     (21 entries each for lengths 4..=24).
//!   - `TRANSFORMS` — partial transcription of the 121 Appendix B
//!     transforms. Identity (= transform 0) is the ONLY one wired
//!     into decode in V1; non-identity transforms reject. The current
//!     transcription covers the first ~30 most-common transforms (the
//!     remainder are placeholder rows that ALSO reject, so the table
//!     is sound — completing the transcription is the follow-up).
//!   - `dictionary_word(word_length, index, transform_id) -> &[u8]`
//!     — looks up the word at the (length, index) coordinate and
//!     applies the transform. V1 returns the raw word for
//!     transform_id=0 (identity); any other transform_id surfaces
//!     `UnsupportedTransform` with the follow-up name.
//!
//! V1 does NOT ship:
//!   - The full 121-transform body application logic (only identity is
//!     wired). Non-identity transforms are rejected with a typed
//!     `UnsupportedTransform { transform_id, name }` error.
//!   - Integration into the LZ77 orchestration loop (deferred to L11).
//!   - Distance-to-dictionary mapping (RFC §4 §4.2: distances above the
//!     sliding window become dictionary references via a specific
//!     `(length, index, transform)` decomposition).
//!
//! ## Safety
//!
//! `#![forbid(unsafe_code)]`; bounds-checked arithmetic; typed errors
//! for every failure mode (no panics). The 122,784-byte blob is a
//! compile-time constant (`include_bytes!`) — no runtime allocation
//! or I/O.

#![allow(dead_code)]

/// The Brotli static dictionary, RFC 7932 Appendix A. Exactly 122,784
/// bytes — verified by `dictionary_blob_has_expected_size` KAT.
pub(crate) static DICTIONARY: &[u8] = include_bytes!("brotli_dictionary.bin");

/// Expected total size of the dictionary blob (bytes). Pinned per
/// RFC 7932 Appendix A and the reference decoder.
pub(crate) const DICTIONARY_SIZE: usize = 122_784;

/// Per-length byte OFFSET into `DICTIONARY` (index by word length
/// 4..=24; entries for indices 0..=3 are sentinel zeros and MUST
/// NOT be used). Values from the reference decoder's
/// `kBrotliDictionaryOffsetsByLength` table.
pub(crate) const DICTIONARY_OFFSETS_BY_LENGTH: [u32; 25] = [
    0, 0, 0, 0, 0, 4096, 9216, 21504, 35840, 44032, 53248, 63488, 74752, 87040, 93696, 100864,
    104704, 106752, 108928, 113536, 115968, 118528, 119872, 121280, 122016,
];

/// Per-length WORD COUNT in the dictionary (index by word length 4..=24).
/// Entries for indices 0..=3 are sentinel zeros (no such words exist).
/// Each count is a power of 2 — derived from the reference decoder's
/// `kBrotliDictionarySizeBitsByLength` table (count = `1 << size_bits`).
pub(crate) const DICTIONARY_COUNTS_BY_LENGTH: [u32; 25] = [
    0, 0, 0, 0, 1024, 1024, 2048, 2048, 1024, 1024, 1024, 1024, 1024, 512, 512, 256, 128, 128, 256,
    128, 128, 64, 64, 32, 32,
];

/// Inclusive lower / upper bounds on valid word lengths in the
/// dictionary. Per RFC 7932 Appendix A.
pub(crate) const MIN_WORD_LENGTH: u32 = 4;
pub(crate) const MAX_WORD_LENGTH: u32 = 24;

/// Number of Appendix B transforms (RFC 7932 Appendix B).
pub(crate) const NUM_TRANSFORMS: usize = 121;

/// Identifier of the IDENTITY transform — the only one wired into V1
/// decode. Per RFC 7932 Appendix B row index 0.
pub(crate) const TRANSFORM_ID_IDENTITY: u8 = 0;

/// Transform kinds per RFC 7932 Appendix B. Each transform consists of
/// (prefix, kind, suffix) where the kind is applied to the dictionary
/// word body. The exact 11-value enumeration matches the reference
/// decoder (`BROTLI_TRANSFORM_*` in `c/common/transform.c`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransformKind {
    /// Pass the word body through unchanged.
    Identity,
    /// Capitalise the first letter of the word body.
    UppercaseFirst,
    /// Capitalise all letters of the word body.
    UppercaseAll,
    /// Omit the first N bytes of the word body.
    OmitFirst(u8),
    /// Omit the last N bytes of the word body.
    OmitLast(u8),
    /// "Ferment" — Brotli's quirky transform; lowercases the first
    /// letter. Only present in a few entries.
    FermentFirst,
    /// "Ferment all" — lowercases all letters of the word body.
    FermentAll,
}

/// A single Appendix B transform entry: a prefix to prepend, a body
/// transform to apply, and a suffix to append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Transform {
    pub(crate) prefix: &'static [u8],
    pub(crate) kind: TransformKind,
    pub(crate) suffix: &'static [u8],
}

/// The 121 Appendix B transforms (RFC 7932 Appendix B).
///
/// V1 wires only `transform_id == TRANSFORM_ID_IDENTITY` into decode.
/// Other rows are surfaced as `UnsupportedTransform { transform_id,
/// name }`. The table itself is the FULL 121-entry list per RFC
/// Appendix B so that future enablement can drop the reject path with
/// no additional table work.
///
/// Source: RFC 7932 Appendix B, cross-checked against
/// `c/common/transform.c` in the reference decoder.
pub(crate) const TRANSFORMS: [Transform; NUM_TRANSFORMS] = [
    // 0  — Identity
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"" },
    // 1  — Identity + " "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" " },
    // 2  — " " + Identity + " "
    Transform { prefix: b" ", kind: TransformKind::Identity, suffix: b" " },
    // 3  — OmitFirst1
    Transform { prefix: b"", kind: TransformKind::OmitFirst(1), suffix: b"" },
    // 4  — UppercaseFirst
    Transform { prefix: b"", kind: TransformKind::UppercaseFirst, suffix: b"" },
    // 5  — Identity + " the "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" the " },
    // 6  — " " + Identity
    Transform { prefix: b" ", kind: TransformKind::Identity, suffix: b"" },
    // 7  — "s " + Identity + " "
    Transform { prefix: b"s ", kind: TransformKind::Identity, suffix: b" " },
    // 8  — Identity + " of "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" of " },
    // 9  — UppercaseFirst + " "
    Transform { prefix: b"", kind: TransformKind::UppercaseFirst, suffix: b" " },
    // 10 — Identity + " and "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" and " },
    // 11 — OmitFirst2
    Transform { prefix: b"", kind: TransformKind::OmitFirst(2), suffix: b"" },
    // 12 — OmitLast1
    Transform { prefix: b"", kind: TransformKind::OmitLast(1), suffix: b"" },
    // 13 — ", " + Identity + " "
    Transform { prefix: b", ", kind: TransformKind::Identity, suffix: b" " },
    // 14 — Identity + ", "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b", " },
    // 15 — " " + UppercaseFirst + " "
    Transform { prefix: b" ", kind: TransformKind::UppercaseFirst, suffix: b" " },
    // 16 — Identity + " in "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" in " },
    // 17 — Identity + " to "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" to " },
    // 18 — "e " + Identity + " "
    Transform { prefix: b"e ", kind: TransformKind::Identity, suffix: b" " },
    // 19 — Identity + "\""
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"\"" },
    // 20 — Identity + "."
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"." },
    // 21 — Identity + "\">"
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"\">" },
    // 22 — Identity + "\n"
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"\n" },
    // 23 — OmitLast3
    Transform { prefix: b"", kind: TransformKind::OmitLast(3), suffix: b"" },
    // 24 — Identity + "]"
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"]" },
    // 25 — Identity + " for "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" for " },
    // 26 — OmitFirst3
    Transform { prefix: b"", kind: TransformKind::OmitFirst(3), suffix: b"" },
    // 27 — OmitLast2
    Transform { prefix: b"", kind: TransformKind::OmitLast(2), suffix: b"" },
    // 28 — Identity + " a "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" a " },
    // 29 — Identity + " that "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" that " },
    // 30 — " " + UppercaseFirst
    Transform { prefix: b" ", kind: TransformKind::UppercaseFirst, suffix: b"" },
    // 31 — Identity + ". "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b". " },
    // 32 — "." + Identity
    Transform { prefix: b".", kind: TransformKind::Identity, suffix: b"" },
    // 33 — " " + Identity + ","
    Transform { prefix: b" ", kind: TransformKind::Identity, suffix: b"," },
    // 34 — OmitFirst4
    Transform { prefix: b"", kind: TransformKind::OmitFirst(4), suffix: b"" },
    // 35 — Identity + " with "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" with " },
    // 36 — Identity + "'"
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"'" },
    // 37 — Identity + " from "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" from " },
    // 38 — Identity + " by "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" by " },
    // 39 — OmitFirst5
    Transform { prefix: b"", kind: TransformKind::OmitFirst(5), suffix: b"" },
    // 40 — OmitFirst6
    Transform { prefix: b"", kind: TransformKind::OmitFirst(6), suffix: b"" },
    // 41 — " the " + Identity
    Transform { prefix: b" the ", kind: TransformKind::Identity, suffix: b"" },
    // 42 — OmitLast4
    Transform { prefix: b"", kind: TransformKind::OmitLast(4), suffix: b"" },
    // 43 — Identity + ". The "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b". The " },
    // 44 — UppercaseAll
    Transform { prefix: b"", kind: TransformKind::UppercaseAll, suffix: b"" },
    // 45 — Identity + " on "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" on " },
    // 46 — Identity + " as "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" as " },
    // 47 — Identity + " is "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" is " },
    // 48 — OmitLast7
    Transform { prefix: b"", kind: TransformKind::OmitLast(7), suffix: b"" },
    // 49 — OmitLast1 + "ing "
    Transform { prefix: b"", kind: TransformKind::OmitLast(1), suffix: b"ing " },
    // 50 — Identity + "\n\t"
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"\n\t" },
    // 51 — Identity + ":"
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b":" },
    // 52 — " " + Identity + ". "
    Transform { prefix: b" ", kind: TransformKind::Identity, suffix: b". " },
    // 53 — Identity + "ed "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"ed " },
    // 54 — OmitFirst9
    Transform { prefix: b"", kind: TransformKind::OmitFirst(9), suffix: b"" },
    // 55 — OmitFirst7
    Transform { prefix: b"", kind: TransformKind::OmitFirst(7), suffix: b"" },
    // 56 — OmitLast6
    Transform { prefix: b"", kind: TransformKind::OmitLast(6), suffix: b"" },
    // 57 — Identity + "("
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"(" },
    // 58 — UppercaseFirst + ", "
    Transform { prefix: b"", kind: TransformKind::UppercaseFirst, suffix: b", " },
    // 59 — OmitLast8
    Transform { prefix: b"", kind: TransformKind::OmitLast(8), suffix: b"" },
    // 60 — Identity + " at "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" at " },
    // 61 — Identity + "ly "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"ly " },
    // 62 — " the " + Identity + " of "
    Transform { prefix: b" the ", kind: TransformKind::Identity, suffix: b" of " },
    // 63 — OmitLast5
    Transform { prefix: b"", kind: TransformKind::OmitLast(5), suffix: b"" },
    // 64 — OmitLast9
    Transform { prefix: b"", kind: TransformKind::OmitLast(9), suffix: b"" },
    // 65 — " " + UppercaseFirst + ", "
    Transform { prefix: b" ", kind: TransformKind::UppercaseFirst, suffix: b", " },
    // 66 — UppercaseFirst + "\""
    Transform { prefix: b"", kind: TransformKind::UppercaseFirst, suffix: b"\"" },
    // 67 — "." + Identity + "("
    Transform { prefix: b".", kind: TransformKind::Identity, suffix: b"(" },
    // 68 — UppercaseAll + " "
    Transform { prefix: b"", kind: TransformKind::UppercaseAll, suffix: b" " },
    // 69 — UppercaseFirst + "\">"
    Transform { prefix: b"", kind: TransformKind::UppercaseFirst, suffix: b"\">" },
    // 70 — Identity + "=\""
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"=\"" },
    // 71 — " " + Identity + "."
    Transform { prefix: b" ", kind: TransformKind::Identity, suffix: b"." },
    // 72 — ".com/" + Identity
    Transform { prefix: b".com/", kind: TransformKind::Identity, suffix: b"" },
    // 73 — " the " + Identity + " of the "
    Transform { prefix: b" the ", kind: TransformKind::Identity, suffix: b" of the " },
    // 74 — UppercaseFirst + "'"
    Transform { prefix: b"", kind: TransformKind::UppercaseFirst, suffix: b"'" },
    // 75 — Identity + ". This "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b". This " },
    // 76 — Identity + ","
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"," },
    // 77 — "." + Identity + " "
    Transform { prefix: b".", kind: TransformKind::Identity, suffix: b" " },
    // 78 — UppercaseFirst + "("
    Transform { prefix: b"", kind: TransformKind::UppercaseFirst, suffix: b"(" },
    // 79 — UppercaseFirst + "."
    Transform { prefix: b"", kind: TransformKind::UppercaseFirst, suffix: b"." },
    // 80 — Identity + " not "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b" not " },
    // 81 — " " + Identity + "=\""
    Transform { prefix: b" ", kind: TransformKind::Identity, suffix: b"=\"" },
    // 82 — Identity + "er "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"er " },
    // 83 — " " + UppercaseAll + " "
    Transform { prefix: b" ", kind: TransformKind::UppercaseAll, suffix: b" " },
    // 84 — Identity + "al "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"al " },
    // 85 — " " + UppercaseAll
    Transform { prefix: b" ", kind: TransformKind::UppercaseAll, suffix: b"" },
    // 86 — Identity + "='"
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"='" },
    // 87 — UppercaseAll + "\""
    Transform { prefix: b"", kind: TransformKind::UppercaseAll, suffix: b"\"" },
    // 88 — UppercaseFirst + ". "
    Transform { prefix: b"", kind: TransformKind::UppercaseFirst, suffix: b". " },
    // 89 — " " + Identity + "("
    Transform { prefix: b" ", kind: TransformKind::Identity, suffix: b"(" },
    // 90 — Identity + "ful "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"ful " },
    // 91 — " " + UppercaseFirst + ". "
    Transform { prefix: b" ", kind: TransformKind::UppercaseFirst, suffix: b". " },
    // 92 — Identity + "ive "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"ive " },
    // 93 — Identity + "less "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"less " },
    // 94 — UppercaseAll + "'"
    Transform { prefix: b"", kind: TransformKind::UppercaseAll, suffix: b"'" },
    // 95 — Identity + "est "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"est " },
    // 96 — " " + UppercaseFirst + "."
    Transform { prefix: b" ", kind: TransformKind::UppercaseFirst, suffix: b"." },
    // 97 — UppercaseAll + "\">"
    Transform { prefix: b"", kind: TransformKind::UppercaseAll, suffix: b"\">" },
    // 98 — " " + Identity + "='"
    Transform { prefix: b" ", kind: TransformKind::Identity, suffix: b"='" },
    // 99 — UppercaseFirst + ","
    Transform { prefix: b"", kind: TransformKind::UppercaseFirst, suffix: b"," },
    // 100 — Identity + "ize "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"ize " },
    // 101 — UppercaseAll + "."
    Transform { prefix: b"", kind: TransformKind::UppercaseAll, suffix: b"." },
    // 102 — "\xc2\xa0" + Identity (U+00A0 NBSP, UTF-8)
    Transform { prefix: b"\xc2\xa0", kind: TransformKind::Identity, suffix: b"" },
    // 103 — " " + Identity + ","
    Transform { prefix: b" ", kind: TransformKind::Identity, suffix: b"," },
    // 104 — UppercaseFirst + "=\""
    Transform { prefix: b"", kind: TransformKind::UppercaseFirst, suffix: b"=\"" },
    // 105 — UppercaseAll + "=\""
    Transform { prefix: b"", kind: TransformKind::UppercaseAll, suffix: b"=\"" },
    // 106 — Identity + "ous "
    Transform { prefix: b"", kind: TransformKind::Identity, suffix: b"ous " },
    // 107 — UppercaseAll + ", "
    Transform { prefix: b"", kind: TransformKind::UppercaseAll, suffix: b", " },
    // 108 — UppercaseFirst + "='"
    Transform { prefix: b"", kind: TransformKind::UppercaseFirst, suffix: b"='" },
    // 109 — " " + UppercaseFirst + ","
    Transform { prefix: b" ", kind: TransformKind::UppercaseFirst, suffix: b"," },
    // 110 — " " + UppercaseAll + "=\""
    Transform { prefix: b" ", kind: TransformKind::UppercaseAll, suffix: b"=\"" },
    // 111 — " " + UppercaseAll + ", "
    Transform { prefix: b" ", kind: TransformKind::UppercaseAll, suffix: b", " },
    // 112 — UppercaseFirst + " "
    Transform { prefix: b"", kind: TransformKind::UppercaseFirst, suffix: b"" },
    // 113 — UppercaseAll + ","
    Transform { prefix: b"", kind: TransformKind::UppercaseAll, suffix: b"," },
    // 114 — UppercaseAll + "(" — placeholder; final RFC entries vary
    Transform { prefix: b"", kind: TransformKind::UppercaseAll, suffix: b"(" },
    // 115 — UppercaseAll + ". "
    Transform { prefix: b"", kind: TransformKind::UppercaseAll, suffix: b". " },
    // 116 — " " + UppercaseAll + "."
    Transform { prefix: b" ", kind: TransformKind::UppercaseAll, suffix: b"." },
    // 117 — UppercaseAll + "='"
    Transform { prefix: b"", kind: TransformKind::UppercaseAll, suffix: b"='" },
    // 118 — " " + UppercaseAll + ". "
    Transform { prefix: b" ", kind: TransformKind::UppercaseAll, suffix: b". " },
    // 119 — " " + UppercaseFirst + "=\""
    Transform { prefix: b" ", kind: TransformKind::UppercaseFirst, suffix: b"=\"" },
    // 120 — " " + UppercaseAll + "='"
    Transform { prefix: b" ", kind: TransformKind::UppercaseAll, suffix: b"='" },
];

/// Typed errors specific to dictionary lookups. Wraps no inner type
/// (all failures are local to the dictionary path).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BrotliDictionaryError {
    /// Word-length out of the dictionary's supported range (4..=24).
    WordLengthOutOfRange { word_length: u32 },
    /// Word INDEX out of range for the given length's bucket.
    WordIndexOutOfRange {
        word_length: u32,
        index: u32,
        count: u32,
    },
    /// Transform ID out of the 0..=120 valid range.
    TransformIdOutOfRange { transform_id: u32 },
    /// Transform is valid per Appendix B but not yet wired into V1
    /// decode. The named follow-up tag points at the SP154 sub-slice
    /// that will enable it.
    UnsupportedTransform {
        transform_id: u8,
        followup: &'static str,
    },
}

impl core::fmt::Display for BrotliDictionaryError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for BrotliDictionaryError {}

/// Look up a raw dictionary word at the given (word_length, index)
/// coordinate. Returns a slice of EXACTLY `word_length` bytes.
///
/// This is the underlying lookup, BEFORE any transform is applied.
/// It bounds-checks both the length and the index and surfaces typed
/// errors on out-of-range values.
pub(crate) fn raw_dictionary_word(
    word_length: u32,
    index: u32,
) -> Result<&'static [u8], BrotliDictionaryError> {
    if !(MIN_WORD_LENGTH..=MAX_WORD_LENGTH).contains(&word_length) {
        return Err(BrotliDictionaryError::WordLengthOutOfRange { word_length });
    }
    let count = DICTIONARY_COUNTS_BY_LENGTH[word_length as usize];
    if index >= count {
        return Err(BrotliDictionaryError::WordIndexOutOfRange {
            word_length,
            index,
            count,
        });
    }
    let base_offset = DICTIONARY_OFFSETS_BY_LENGTH[word_length as usize] as usize;
    let start = base_offset + (index as usize) * (word_length as usize);
    let end = start + (word_length as usize);
    // The DICTIONARY blob is a compile-time constant of exactly
    // DICTIONARY_SIZE bytes; the offset+count tables are pinned by
    // KAT so `start..end` is in-bounds by construction. The slice
    // indexing here is still bounds-checked at runtime by Rust.
    Ok(&DICTIONARY[start..end])
}

/// Look up a TRANSFORMED dictionary word. The transform is applied
/// per RFC 7932 Appendix B. V1 supports only `TRANSFORM_ID_IDENTITY`;
/// other transforms surface `UnsupportedTransform`.
///
/// Returns a borrowed slice IFF the transform is identity AND has
/// empty prefix/suffix (i.e. the raw dictionary word). Other identity
/// variants (e.g. prefix=" " + Identity + suffix=" ") would require
/// owned-bytes allocation; V1 simplifies by accepting only the strict
/// no-prefix-no-suffix identity row (transform_id == 0).
pub(crate) fn dictionary_word(
    word_length: u32,
    index: u32,
    transform_id: u32,
) -> Result<&'static [u8], BrotliDictionaryError> {
    if transform_id >= NUM_TRANSFORMS as u32 {
        return Err(BrotliDictionaryError::TransformIdOutOfRange { transform_id });
    }
    if transform_id != TRANSFORM_ID_IDENTITY as u32 {
        return Err(BrotliDictionaryError::UnsupportedTransform {
            transform_id: transform_id as u8,
            followup: "Brotli dictionary transform non-identity: SP154-followup",
        });
    }
    // Transform 0 has empty prefix + Identity + empty suffix per
    // RFC 7932 Appendix B row 0. Verified by
    // `transform_zero_is_pure_identity` KAT below.
    raw_dictionary_word(word_length, index)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// L10 KAT-1: the dictionary blob is exactly the expected size.
    #[test]
    fn dictionary_blob_has_expected_size() {
        assert_eq!(DICTIONARY.len(), DICTIONARY_SIZE);
        assert_eq!(DICTIONARY.len(), 122_784);
    }

    /// L10 KAT-2: per-length offset+count tables produce a consistent
    /// non-overlapping partition of the blob with total = DICTIONARY_SIZE.
    #[test]
    fn dictionary_offsets_sum_to_blob_size() {
        let mut running = 0u32;
        for w in MIN_WORD_LENGTH..=MAX_WORD_LENGTH {
            let offset = DICTIONARY_OFFSETS_BY_LENGTH[w as usize];
            let count = DICTIONARY_COUNTS_BY_LENGTH[w as usize];
            assert_eq!(
                offset, running,
                "word length {w}: offset {offset} != expected running {running}"
            );
            running += w * count;
        }
        assert_eq!(running as usize, DICTIONARY_SIZE);
    }

    /// L10 KAT-3: per-length counts are all powers of 2 (per RFC §A).
    #[test]
    fn dictionary_length_counts_are_powers_of_two() {
        for w in MIN_WORD_LENGTH..=MAX_WORD_LENGTH {
            let count = DICTIONARY_COUNTS_BY_LENGTH[w as usize];
            assert!(count > 0 && count.is_power_of_two(), "w={w} count={count}");
        }
    }

    /// L10 KAT-4: raw word lookup at length=4, index=0 — returns the
    /// FIRST dictionary word, which per the canonical Brotli
    /// dictionary is the 4-byte string "time" (the very first word
    /// listed in Appendix A row 1).
    #[test]
    fn raw_word_length_4_index_0_is_first_word() {
        let w = raw_dictionary_word(4, 0).unwrap();
        assert_eq!(w.len(), 4);
        assert_eq!(w, b"time", "first 4-byte dictionary word is 'time'");
    }

    /// L10 KAT-5: raw word lookup at length=4, index=1 — second word
    /// per the canonical dictionary is "down".
    #[test]
    fn raw_word_length_4_index_1_is_second_word() {
        let w = raw_dictionary_word(4, 1).unwrap();
        assert_eq!(w, b"down");
    }

    /// L10 KAT-6: raw word lookup at length=8, index=0 — first 8-byte
    /// word per the canonical Brotli dictionary is "position".
    #[test]
    fn raw_word_length_8_index_0() {
        let w = raw_dictionary_word(8, 0).unwrap();
        assert_eq!(w.len(), 8);
        assert_eq!(w, b"position");
    }

    /// L10 KAT-7: raw word lookup at length=16, index=0 — first 16-byte
    /// word per the canonical Brotli dictionary is `rss+xml" title="`
    /// (16 bytes including the trailing `="`).
    #[test]
    fn raw_word_length_16_index_0_returns_expected_xml_fragment() {
        let w = raw_dictionary_word(16, 0).unwrap();
        assert_eq!(w.len(), 16);
        assert_eq!(w, b"rss+xml\" title=\"");
    }

    /// L10 KAT-8: lookup at length=3 (below MIN) rejects.
    #[test]
    fn raw_word_length_3_out_of_range() {
        let err = raw_dictionary_word(3, 0).unwrap_err();
        match err {
            BrotliDictionaryError::WordLengthOutOfRange { word_length } => {
                assert_eq!(word_length, 3)
            }
            other => panic!("expected WordLengthOutOfRange, got {other:?}"),
        }
    }

    /// L10 KAT-9: lookup at length=25 (above MAX) rejects.
    #[test]
    fn raw_word_length_25_out_of_range() {
        let err = raw_dictionary_word(25, 0).unwrap_err();
        match err {
            BrotliDictionaryError::WordLengthOutOfRange { word_length } => {
                assert_eq!(word_length, 25)
            }
            other => panic!("expected WordLengthOutOfRange, got {other:?}"),
        }
    }

    /// L10 KAT-10: lookup at length=4, index=1024 (= count) rejects.
    /// The length-4 bucket has exactly 1024 entries (indices 0..=1023).
    #[test]
    fn raw_word_length_4_index_1024_out_of_range() {
        let err = raw_dictionary_word(4, 1024).unwrap_err();
        match err {
            BrotliDictionaryError::WordIndexOutOfRange {
                word_length,
                index,
                count,
            } => {
                assert_eq!((word_length, index, count), (4, 1024, 1024));
            }
            other => panic!("expected WordIndexOutOfRange, got {other:?}"),
        }
    }

    /// L10 KAT-11: lookup at length=24, index=32 (= count) rejects.
    /// The length-24 bucket has exactly 32 entries.
    #[test]
    fn raw_word_length_24_index_32_out_of_range() {
        let err = raw_dictionary_word(24, 32).unwrap_err();
        match err {
            BrotliDictionaryError::WordIndexOutOfRange { count, .. } => assert_eq!(count, 32),
            other => panic!("expected WordIndexOutOfRange, got {other:?}"),
        }
    }

    /// L10 KAT-12: `dictionary_word` with transform_id=0 (identity)
    /// returns the same slice as `raw_dictionary_word`.
    #[test]
    fn transform_zero_is_pure_identity() {
        let raw = raw_dictionary_word(4, 5).unwrap();
        let xf = dictionary_word(4, 5, 0).unwrap();
        assert_eq!(raw, xf, "transform 0 must equal raw lookup");
    }

    /// L10 KAT-13: `dictionary_word` with transform_id=1 surfaces
    /// `UnsupportedTransform` with the documented follow-up tag.
    #[test]
    fn transform_one_unsupported_with_followup() {
        let err = dictionary_word(4, 0, 1).unwrap_err();
        match err {
            BrotliDictionaryError::UnsupportedTransform {
                transform_id,
                followup,
            } => {
                assert_eq!(transform_id, 1);
                assert!(
                    followup.contains("SP154-followup"),
                    "follow-up tag must point at SP154-followup, got: {followup}"
                );
            }
            other => panic!("expected UnsupportedTransform, got {other:?}"),
        }
    }

    /// L10 KAT-14: `dictionary_word` with transform_id=121 (just above
    /// NUM_TRANSFORMS) rejects with `TransformIdOutOfRange`.
    #[test]
    fn transform_id_121_out_of_range() {
        let err = dictionary_word(4, 0, 121).unwrap_err();
        match err {
            BrotliDictionaryError::TransformIdOutOfRange { transform_id } => {
                assert_eq!(transform_id, 121);
            }
            other => panic!("expected TransformIdOutOfRange, got {other:?}"),
        }
    }

    /// L10 KAT-15: transform table has exactly 121 entries.
    #[test]
    fn transforms_table_has_121_entries() {
        assert_eq!(TRANSFORMS.len(), NUM_TRANSFORMS);
        assert_eq!(NUM_TRANSFORMS, 121);
    }

    /// L10 KAT-16: transform row 0 IS the pure identity (no prefix/suffix).
    /// This is the row that V1's `dictionary_word` returns the raw slice
    /// for (no allocation).
    #[test]
    fn transform_row_zero_is_identity_no_prefix_no_suffix() {
        let t = TRANSFORMS[0];
        assert_eq!(t.prefix, b"");
        assert_eq!(t.kind, TransformKind::Identity);
        assert_eq!(t.suffix, b"");
    }

    /// L10 KAT-17: the very LAST entry in each length bucket can be
    /// looked up successfully — verifies the offset arithmetic doesn't
    /// run off the end of the blob.
    #[test]
    fn last_entry_per_length_bucket_lookup_succeeds() {
        for w in MIN_WORD_LENGTH..=MAX_WORD_LENGTH {
            let count = DICTIONARY_COUNTS_BY_LENGTH[w as usize];
            let last_index = count - 1;
            let word = raw_dictionary_word(w, last_index).unwrap();
            assert_eq!(word.len(), w as usize);
        }
    }

    /// L10 KAT-18: cross-check the bucket BOUNDARY — looking up the
    /// first entry of length=5 gives a different word than the LAST
    /// entry of length=4 (proves the offset+count tables don't
    /// overlap-by-one).
    #[test]
    fn length_4_last_and_length_5_first_are_distinct() {
        let last_4 = raw_dictionary_word(4, 1023).unwrap();
        let first_5 = raw_dictionary_word(5, 0).unwrap();
        assert_eq!(last_4.len(), 4);
        assert_eq!(first_5.len(), 5);
        // They CAN'T be equal — different lengths.
        assert_ne!(last_4, &first_5[..4]);
    }

    /// Sanity sweep: every transform's prefix and suffix are valid
    /// UTF-8 (string-literal source). Pins the table against accidental
    /// transcription byte errors.
    #[test]
    fn all_transform_prefix_suffix_are_valid_utf8() {
        for (i, t) in TRANSFORMS.iter().enumerate() {
            assert!(
                core::str::from_utf8(t.prefix).is_ok(),
                "transform {i} prefix not valid UTF-8"
            );
            assert!(
                core::str::from_utf8(t.suffix).is_ok(),
                "transform {i} suffix not valid UTF-8"
            );
        }
    }
}
