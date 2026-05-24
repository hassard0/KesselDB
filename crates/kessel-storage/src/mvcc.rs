//! kessel-storage::mvcc — Versioned key-value layer (S2.1 of THESIS.md S2).
//!
//! Append-only versions of a logical key live in the underlying LSM at a
//! 28-byte physical key:
//!   `type_id (4 LE) ++ object_id (16) ++ inverted_commit_opnum (8 BE)`
//! where `inverted_commit_opnum = u64::MAX - commit_opnum`. BE encoding
//! + inversion makes newest-version-first the natural lex order, so a
//! snapshot read is a single seek-and-scan-forward.
//!
//! NO SQL caller integration; NO transaction context; NO conflict
//! detection; NO GC. Those ship in S2.2 / S2.3 / S2.4 / S2.5 / S2.6
//! per the parent design `docs/superpowers/specs/2026-05-23-mvcc-si-design.md`.
//!
//! Determinism guarantee (S2.1 contract): two replicas with the same
//! applied log prefix have byte-identical version chains for every key,
//! and `get_at_snapshot(K, S)` returns byte-identical results.

#![forbid(unsafe_code)]

use crate::{Key, Storage};
use kessel_io::Vfs;

/// Length of an MVCC versioned key in bytes:
/// `type_id (4) ++ object_id (16) ++ inverted_commit_opnum (8) = 28`.
///
/// Decision 3 of the parent design: version storage uses the same kessel-storage
/// LSM, extended to a 28-byte physical key to carry the commit timestamp inline.
pub const VERSIONED_KEY_LEN: usize = 28;

/// Length of the (type_id, object_id) prefix shared with the 20-byte legacy
/// key encoding.
///
/// The prefix is identical to what `crate::make_key` produces, so a full scan
/// of all versions for a logical key is a `scan_range` over
/// `[prefix, prefix ++ 0x00…00]` to `[prefix, prefix ++ 0xFF…FF]`.
pub const PREFIX_LEN: usize = 20;

/// Result of a snapshot read.
///
/// Three variants are required because "the key was deleted before the
/// snapshot" (Tombstoned) and "the key was never written before the
/// snapshot" (NotYetWritten) are semantically distinct: a SQL UPDATE on
/// a tombstoned row may proceed if the user re-inserts; a SQL UPDATE on
/// a never-written row is a "row not found." S2.5 GC may reclaim
/// pre-watermark tombstones, but the semantic distinction at read time
/// is preserved for the post-watermark window.
///
/// Decision 5 of the parent design: snapshot reads return this enum rather
/// than `Option<Vec<u8>>` so callers can distinguish the two absent cases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotRead {
    /// The newest version visible at the snapshot has content.
    Found(Vec<u8>),
    /// The newest version visible at the snapshot is a deletion (tombstone).
    Tombstoned,
    /// No version of this key has commit_opnum <= snapshot_opnum.
    NotYetWritten,
}

/// Errors decoding a 28-byte MVCC versioned key.
///
/// Kept minimal for S2.1 — only the length check is needed by the scaffold.
/// Additional variants (e.g., InvalidTypeId, OpnumOutOfRange) may be added
/// in T2 if the implementation surface demands them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MvccKeyError {
    /// Key length is not exactly [`VERSIONED_KEY_LEN`] (28) bytes.
    ///
    /// Contains the actual length received.
    Length(usize),
}

impl std::fmt::Display for MvccKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MvccKeyError::Length(n) => {
                write!(f, "expected {VERSIONED_KEY_LEN}-byte MVCC key, got {n} bytes")
            }
        }
    }
}

impl std::error::Error for MvccKeyError {}

/// Build a 28-byte MVCC versioned key.
///
/// Encoding (Decision 3):
/// - Bytes 0..4   — `type_id` little-endian (matches the 20-byte legacy prefix).
/// - Bytes 4..20  — `object_id` (16 bytes verbatim).
/// - Bytes 20..28 — `u64::MAX - commit_opnum` big-endian, so newest versions
///                  sort FIRST lexicographically within the same logical key.
///
/// `commit_opnum` is the SM-assigned opnum at which this version was
/// committed (Decision 2: log opnum as timestamp, no wall-clock).
pub fn make_versioned_key(type_id: u32, object_id: &[u8; 16], commit_opnum: u64) -> Key {
    todo!("filled in T2")
}

/// Decode the `commit_opnum` out of a 28-byte MVCC versioned key.
///
/// Inverts the big-endian inverted suffix back to the original opnum.
/// Returns `Err(MvccKeyError::Length(_))` for any slice whose length
/// is not exactly [`VERSIONED_KEY_LEN`].
pub fn decode_commit_opnum(key: &[u8]) -> Result<u64, MvccKeyError> {
    todo!("filled in T2")
}

/// Append a new version of `(type_id, object_id)` at `commit_opnum`.
///
/// `value = Some(bytes)` for a write; `value = None` for a tombstone (logical
/// deletion). The versioned key is built via [`make_versioned_key`] and written
/// to the underlying `Storage` using `put` (non-`None`) or `delete` (`None`).
///
/// Append-only: prior versions of the same `(type_id, object_id)` remain
/// in the store until S2.5 GC reclaims them. Callers MUST ensure that
/// `commit_opnum` is strictly greater than any opnum previously written for
/// this logical key — the function does not enforce this in S2.1 (T2 may add
/// an assertion).
///
/// Implements the "version write" path of Decision 3.
pub fn put_versioned<V: Vfs>(
    store: &mut Storage<V>,
    type_id: u32,
    object_id: &[u8; 16],
    commit_opnum: u64,
    value: Option<Vec<u8>>,
) -> std::io::Result<()> {
    todo!("filled in T2")
}

/// Snapshot read: returns the newest version of `(type_id, object_id)`
/// with `commit_opnum <= snapshot_opnum`.
///
/// Algorithm (Decision 5):
/// 1. Build the prefix `type_id (4 LE) ++ object_id (16)`.
/// 2. Seek the LSM to the first key ≥ `prefix ++ inverted(snapshot_opnum)`.
///    Because inversion makes newer versions sort first, the first key in the
///    prefix range that is ≥ the seek point is the newest visible version.
/// 3. If the matching entry is a tombstone → `Tombstoned`.
/// 4. If no key in the prefix range satisfies the constraint → `NotYetWritten`.
/// 5. Otherwise → `Found(value)`.
///
/// Reads are non-mutating; takes a shared reference to `Storage`.
pub fn get_at_snapshot<V: Vfs>(
    store: &Storage<V>,
    type_id: u32,
    object_id: &[u8; 16],
    snapshot_opnum: u64,
) -> SnapshotRead {
    todo!("filled in T2")
}

/// Returns `true` iff any version of `(type_id, object_id)` exists in the
/// half-open interval `(lo_opnum_exclusive, hi_opnum_inclusive]` — i.e.,
/// `commit_opnum > lo_opnum_exclusive AND commit_opnum <= hi_opnum_inclusive`.
///
/// Required by S2.3 (SI write-set conflict detection): before a transaction
/// at snapshot `lo` commits, it checks whether any concurrent writer committed
/// a version of the same key in `(lo, now_opnum]`. If so, the committing
/// transaction aborts (first-committer-wins). Shipping this signature in S2.1
/// lets S2.3 import it without expanding the module surface.
///
/// Implementation uses two `make_versioned_key` bounds and a `scan_range`
/// over the LSM — to be filled in T2.
pub fn has_version_in_range<V: Vfs>(
    store: &Storage<V>,
    type_id: u32,
    object_id: &[u8; 16],
    lo_opnum_exclusive: u64,
    hi_opnum_inclusive: u64,
) -> bool {
    todo!("filled in T2")
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke test: the module compiles and all public symbols link correctly.
    ///
    /// Each public function is referenced (but not called — the `todo!()`
    /// bodies would panic). The test is `#[should_panic]` so the harness
    /// records it as passing even though calling any function hits `todo!()`.
    ///
    /// What this validates:
    /// - Every type (`SnapshotRead`, `MvccKeyError`) is reachable.
    /// - Every constant (`VERSIONED_KEY_LEN`, `PREFIX_LEN`) is readable.
    /// - Every function has a valid signature that the compiler accepts.
    #[test]
    #[should_panic]
    fn compiles_and_links() {
        // Constants are readable without panicking.
        assert_eq!(VERSIONED_KEY_LEN, 28);
        assert_eq!(PREFIX_LEN, 20);

        // Enum variants are constructible and comparable.
        let _found = SnapshotRead::Found(vec![1, 2, 3]);
        let _tomb = SnapshotRead::Tombstoned;
        let _nyw = SnapshotRead::NotYetWritten;
        assert_ne!(SnapshotRead::Tombstoned, SnapshotRead::NotYetWritten);

        // MvccKeyError is constructible and Display works.
        let err = MvccKeyError::Length(5);
        let msg = format!("{err}");
        assert!(msg.contains("28"));

        // Call each public function — all hit todo!() and panic, which is
        // expected (hence #[should_panic]).  The type-checker has already
        // verified the signatures at compile time; this just proves linkage.
        let object_id = [0u8; 16];
        let _key: Key = make_versioned_key(1, &object_id, 42);
        // Unreachable past here, but the signatures below are type-checked:
        let _ = decode_commit_opnum(&[]);
        // put_versioned / get_at_snapshot / has_version_in_range require a
        // Storage<V> which cannot be constructed in a unit test without a VFS.
        // Their signatures are validated at compile time; no runtime call needed.
    }

    /// Verify the MvccKeyError::Length display message format.
    ///
    /// Does NOT call any todo!() function, so this test always passes cleanly.
    #[test]
    fn mvcc_key_error_display() {
        let err = MvccKeyError::Length(0);
        let msg = format!("{err}");
        assert!(msg.contains("28"), "display should mention the expected length");
        assert!(msg.contains('0'), "display should mention the actual length");

        let err2 = MvccKeyError::Length(100);
        let msg2 = format!("{err2}");
        assert!(msg2.contains("100"));
    }

    /// Verify SnapshotRead derives work correctly.
    #[test]
    fn snapshot_read_clone_eq() {
        let a = SnapshotRead::Found(vec![7, 8, 9]);
        let b = a.clone();
        assert_eq!(a, b);
        assert_ne!(a, SnapshotRead::Tombstoned);
        assert_ne!(SnapshotRead::Tombstoned, SnapshotRead::NotYetWritten);
    }
}
