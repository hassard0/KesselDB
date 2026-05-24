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
/// `[prefix, prefix ++ 0x00..00]` to `[prefix, prefix ++ 0xFF..FF]`.
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
    // Inversion: u64::MAX - commit_opnum (== !commit_opnum for u64).
    // Newer opnums produce smaller inverted values, so BE encoding makes
    // newer versions lex-sort BEFORE older ones within the same prefix.
    let inverted = u64::MAX - commit_opnum;
    let mut k = Vec::with_capacity(VERSIONED_KEY_LEN);
    k.extend_from_slice(&type_id.to_le_bytes());
    k.extend_from_slice(object_id);
    k.extend_from_slice(&inverted.to_be_bytes());
    k
}

/// Decode the `commit_opnum` out of a 28-byte MVCC versioned key.
///
/// Inverts the big-endian inverted suffix back to the original opnum.
/// Returns `Err(MvccKeyError::Length(_))` for any slice whose length
/// is not exactly [`VERSIONED_KEY_LEN`].
pub fn decode_commit_opnum(key: &[u8]) -> Result<u64, MvccKeyError> {
    if key.len() != VERSIONED_KEY_LEN {
        return Err(MvccKeyError::Length(key.len()));
    }
    // Length-checked above — this try_into().unwrap() is statically
    // infallible: key[PREFIX_LEN..VERSIONED_KEY_LEN] is exactly 8 bytes.
    let be_bytes: [u8; 8] = key[PREFIX_LEN..VERSIONED_KEY_LEN]
        .try_into()
        .expect("slice is exactly 8 bytes after length check");
    let inverted = u64::from_be_bytes(be_bytes);
    // Invert back: u64::MAX - inverted == original commit_opnum.
    Ok(u64::MAX - inverted)
}

/// Append a new version of `(type_id, object_id)` at `commit_opnum`.
///
/// `value = Some(bytes)` for a write; `value = None` for a tombstone (logical
/// deletion). The versioned key is built via [`make_versioned_key`] and written
/// to the underlying `Storage` using `put_entry_versioned` which accepts
/// `Option<Vec<u8>>` so tombstones flow naturally.
///
/// Append-only: prior versions of the same `(type_id, object_id)` remain
/// in the store until S2.5 GC reclaims them. Callers MUST ensure that
/// `commit_opnum` is strictly greater than any opnum previously written for
/// this logical key — the function does not enforce this in S2.1.
pub fn put_versioned<V: Vfs>(
    store: &mut Storage<V>,
    type_id: u32,
    object_id: &[u8; 16],
    commit_opnum: u64,
    value: Option<Vec<u8>>,
) -> std::io::Result<()> {
    let key = make_versioned_key(type_id, object_id, commit_opnum);
    store.put_entry_versioned(commit_opnum, key, value)
}

/// Snapshot read: returns the newest version of `(type_id, object_id)`
/// with `commit_opnum <= snapshot_opnum`.
///
/// Algorithm (Decision 5):
/// 1. Build the full prefix `type_id (4 LE) ++ object_id (16)`.
/// 2. Scan the LSM over the (type_id, object_id) prefix range — keys come
///    out in ascending lex order, which equals DESCENDING commit_opnum order
///    because of the inversion. The first key whose decoded commit_opnum is
///    <= snapshot_opnum is the newest visible version.
/// 3. If the matching entry is a tombstone (`None`) -> `Tombstoned`.
/// 4. If no key in the prefix range satisfies the constraint -> `NotYetWritten`.
/// 5. Otherwise -> `Found(value)`.
///
/// Reads are non-mutating; takes a shared reference to `Storage`.
pub fn get_at_snapshot<V: Vfs>(
    store: &Storage<V>,
    type_id: u32,
    object_id: &[u8; 16],
    snapshot_opnum: u64,
) -> SnapshotRead {
    // Build the inclusive [lo, hi] bounds spanning all versions for this
    // logical key. Inverted suffix 0x00..00 corresponds to commit_opnum=u64::MAX
    // (newest possible); 0xFF..FF corresponds to commit_opnum=0 (oldest).
    // BTreeMap range scan yields ascending lex = descending opnum.
    let mut lo = Vec::with_capacity(VERSIONED_KEY_LEN);
    lo.extend_from_slice(&type_id.to_le_bytes());
    lo.extend_from_slice(object_id);
    lo.extend_from_slice(&[0x00u8; 8]); // inverted(u64::MAX) = newest-possible

    let mut hi = Vec::with_capacity(VERSIONED_KEY_LEN);
    hi.extend_from_slice(&type_id.to_le_bytes());
    hi.extend_from_slice(object_id);
    hi.extend_from_slice(&[0xFFu8; 8]); // inverted(0) = oldest-possible

    // scan_range_versions yields tombstones as None; keys are in ascending
    // lex order = descending commit_opnum order.
    for (k, v) in store.scan_range_versions(&lo, &hi) {
        match decode_commit_opnum(&k) {
            Ok(c) if c <= snapshot_opnum => {
                return match v {
                    Some(bytes) => SnapshotRead::Found(bytes),
                    None => SnapshotRead::Tombstoned,
                };
            }
            _ => {
                // commit_opnum > snapshot_opnum: this version is too new.
                // Continue scanning to find an older one.
                continue;
            }
        }
    }
    SnapshotRead::NotYetWritten
}

/// Returns `true` iff any version of `(type_id, object_id)` exists in the
/// half-open interval `(lo_opnum_exclusive, hi_opnum_inclusive]` — i.e.,
/// `commit_opnum > lo_opnum_exclusive AND commit_opnum <= hi_opnum_inclusive`.
///
/// Required by S2.3 (SI write-set conflict detection): before a transaction
/// at snapshot `lo` commits, it checks whether any concurrent writer committed
/// a version of the same key in `(lo, now_opnum]`. If so, the committing
/// transaction aborts (first-committer-wins).
pub fn has_version_in_range<V: Vfs>(
    store: &Storage<V>,
    type_id: u32,
    object_id: &[u8; 16],
    lo_opnum_exclusive: u64,
    hi_opnum_inclusive: u64,
) -> bool {
    // Build the full prefix range (same as get_at_snapshot).
    let mut lo = Vec::with_capacity(VERSIONED_KEY_LEN);
    lo.extend_from_slice(&type_id.to_le_bytes());
    lo.extend_from_slice(object_id);
    lo.extend_from_slice(&[0x00u8; 8]);

    let mut hi = Vec::with_capacity(VERSIONED_KEY_LEN);
    hi.extend_from_slice(&type_id.to_le_bytes());
    hi.extend_from_slice(object_id);
    hi.extend_from_slice(&[0xFFu8; 8]);

    for (k, _v) in store.scan_range_versions(&lo, &hi) {
        match decode_commit_opnum(&k) {
            Ok(c) => {
                // Scan is in descending opnum order.
                // If c <= lo_opnum_exclusive, all remaining entries are
                // also <= lo_opnum_exclusive (outside the window).
                if c <= lo_opnum_exclusive {
                    return false;
                }
                if c <= hi_opnum_inclusive {
                    // c > lo_opnum_exclusive AND c <= hi_opnum_inclusive: match.
                    return true;
                }
                // c > hi_opnum_inclusive: still too new, keep scanning.
            }
            Err(_) => continue, // malformed key — skip
        }
    }
    false
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Storage;
    use kessel_io::MemVfs;

    /// Helper: build a deterministic 16-byte object_id from a single byte.
    fn obj(n: u8) -> [u8; 16] {
        let mut a = [0u8; 16];
        a[15] = n;
        a
    }

    // -----------------------------------------------------------------------
    // T2.5.1: Key encoding round-trip — boundary opnums + samples.
    //
    // KAT derivation for opnum=1:
    //   inverted = u64::MAX - 1 = 0xFFFF_FFFF_FFFF_FFFE
    //   BE bytes: [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE]
    //   decode: from_be_bytes([FF,FF,FF,FF,FF,FF,FF,FE]) = 0xFFFFFFFFFFFFFFFE
    //           u64::MAX - 0xFFFFFFFFFFFFFFFE = 1 (round-trips correctly)
    //
    // KAT derivation for opnum=u64::MAX:
    //   inverted = u64::MAX - u64::MAX = 0x0000_0000_0000_0000
    //   BE bytes: [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
    //   => u64::MAX sorts FIRST (lex) = newest-first ordering correct.
    // -----------------------------------------------------------------------
    #[test]
    fn versioned_key_roundtrip_boundary_opnums() {
        let oid = obj(1);
        for &c in &[0u64, 1, 2, u64::MAX - 1, u64::MAX, 1 << 20, 1 << 40] {
            let k = make_versioned_key(7, &oid, c);
            assert_eq!(k.len(), VERSIONED_KEY_LEN, "key must be 28 bytes for opnum={c}");
            assert_eq!(
                decode_commit_opnum(&k),
                Ok(c),
                "round-trip must recover opnum={c}"
            );
        }

        // Spot-check KAT bytes for opnum=1:
        //   type_id=7 LE = [7, 0, 0, 0]
        //   object_id[15]=1 = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1]
        //   inverted(1) = 0xFFFFFFFFFFFFFFFE -> BE = [FF,FF,FF,FF,FF,FF,FF,FE]
        let k1 = make_versioned_key(7, &obj(1), 1u64);
        assert_eq!(&k1[0..4], &[7u8, 0, 0, 0]);
        assert_eq!(&k1[20..28], &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE]);

        // Spot-check KAT bytes for opnum=u64::MAX:
        //   inverted(u64::MAX) = 0 -> BE = [00,00,00,00,00,00,00,00]
        let k_max = make_versioned_key(7, &obj(1), u64::MAX);
        assert_eq!(&k_max[20..28], &[0x00u8; 8]);
    }

    // -----------------------------------------------------------------------
    // T2.5.2: Length-validation — decode_commit_opnum rejects keys != 28 bytes.
    // -----------------------------------------------------------------------
    #[test]
    fn decode_rejects_non_28_byte_keys() {
        assert_eq!(decode_commit_opnum(&[]), Err(MvccKeyError::Length(0)));
        assert_eq!(
            decode_commit_opnum(&[0u8; 20]),
            Err(MvccKeyError::Length(20))
        );
        assert_eq!(
            decode_commit_opnum(&[0u8; 27]),
            Err(MvccKeyError::Length(27))
        );
        assert_eq!(
            decode_commit_opnum(&[0u8; 29]),
            Err(MvccKeyError::Length(29))
        );
        // Exactly 28 bytes must succeed.
        assert!(decode_commit_opnum(&[0u8; 28]).is_ok());
    }

    // -----------------------------------------------------------------------
    // T2.5.3: Newer versions lex-sort BEFORE older versions (inverted-BE).
    //
    // Derivation:
    //   opnum=200 -> inverted = u64::MAX - 200 = 0xFFFFFFFFFFFFFF37 (BE last byte 0x37)
    //   opnum=100 -> inverted = u64::MAX - 100 = 0xFFFFFFFFFFFFFF9B (BE last byte 0x9B)
    //   0x...37 < 0x...9B => k(opnum=200) < k(opnum=100) => newer-first ✓
    // -----------------------------------------------------------------------
    #[test]
    fn newer_versions_sort_first() {
        let oid = obj(2);
        let k_old = make_versioned_key(7, &oid, 100);
        let k_new = make_versioned_key(7, &oid, 200);
        assert!(
            k_new < k_old,
            "newer commit_opnum must sort earlier (lex) than older"
        );
        let k_newest = make_versioned_key(7, &oid, 300);
        assert!(k_newest < k_new, "opnum=300 must sort before opnum=200");
    }

    // -----------------------------------------------------------------------
    // T2.5.4: Single put + snapshot read.
    // -----------------------------------------------------------------------
    #[test]
    fn put_then_get_at_snapshot_returns_value() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        put_versioned(&mut store, 7, &obj(3), 10, Some(b"v1".to_vec())).unwrap();

        // At-version snapshot.
        match get_at_snapshot(&store, 7, &obj(3), 10) {
            SnapshotRead::Found(b) => assert_eq!(b, b"v1"),
            other => panic!("expected Found, got {:?}", other),
        }
        // Snapshot BEFORE the write: NotYetWritten.
        assert_eq!(
            get_at_snapshot(&store, 7, &obj(3), 9),
            SnapshotRead::NotYetWritten
        );
        // Snapshot AFTER the write: still Found.
        match get_at_snapshot(&store, 7, &obj(3), 999) {
            SnapshotRead::Found(b) => assert_eq!(b, b"v1"),
            other => panic!("expected Found at snapshot=999, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // T2.5.5: Multiple versions coexist; snapshot reads choose the correct one.
    //   write at 10->"a", 20->"b", 30->"c"
    //   snapshot=9  -> NotYetWritten
    //   snapshot=10 -> "a";  snapshot=15 -> "a" (newest <=15 is 10)
    //   snapshot=20 -> "b";  snapshot=29 -> "b" (newest <=29 is 20)
    //   snapshot=30 -> "c";  snapshot=9999 -> "c"
    // -----------------------------------------------------------------------
    #[test]
    fn multiple_versions_coexist_snapshot_reads_choose_correct_one() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let oid = obj(4);
        put_versioned(&mut store, 7, &oid, 10, Some(b"a".to_vec())).unwrap();
        put_versioned(&mut store, 7, &oid, 20, Some(b"b".to_vec())).unwrap();
        put_versioned(&mut store, 7, &oid, 30, Some(b"c".to_vec())).unwrap();

        assert!(matches!(
            get_at_snapshot(&store, 7, &oid, 9),
            SnapshotRead::NotYetWritten
        ));
        match get_at_snapshot(&store, 7, &oid, 10) {
            SnapshotRead::Found(b) => assert_eq!(b, b"a"),
            o => panic!("snapshot=10: {:?}", o),
        }
        match get_at_snapshot(&store, 7, &oid, 15) {
            SnapshotRead::Found(b) => assert_eq!(b, b"a"),
            o => panic!("snapshot=15: {:?}", o),
        }
        match get_at_snapshot(&store, 7, &oid, 20) {
            SnapshotRead::Found(b) => assert_eq!(b, b"b"),
            o => panic!("snapshot=20: {:?}", o),
        }
        match get_at_snapshot(&store, 7, &oid, 29) {
            SnapshotRead::Found(b) => assert_eq!(b, b"b"),
            o => panic!("snapshot=29: {:?}", o),
        }
        match get_at_snapshot(&store, 7, &oid, 30) {
            SnapshotRead::Found(b) => assert_eq!(b, b"c"),
            o => panic!("snapshot=30: {:?}", o),
        }
        match get_at_snapshot(&store, 7, &oid, 9999) {
            SnapshotRead::Found(b) => assert_eq!(b, b"c"),
            o => panic!("snapshot=9999: {:?}", o),
        }
    }

    // -----------------------------------------------------------------------
    // T2.5.6: Tombstone is observable at and after the snapshot it was written.
    //   write at 10->"a", tombstone at 20, write at 30->"c"
    //   snapshot=10 -> Found("a")
    //   snapshot=20 -> Tombstoned
    //   snapshot=25 -> Tombstoned  (newest <=25 is opnum=20 tombstone)
    //   snapshot=30 -> Found("c")
    // -----------------------------------------------------------------------
    #[test]
    fn tombstone_is_observable_at_its_snapshot() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let oid = obj(5);
        put_versioned(&mut store, 7, &oid, 10, Some(b"a".to_vec())).unwrap();
        put_versioned(&mut store, 7, &oid, 20, None).unwrap(); // tombstone
        put_versioned(&mut store, 7, &oid, 30, Some(b"c".to_vec())).unwrap();

        match get_at_snapshot(&store, 7, &oid, 10) {
            SnapshotRead::Found(b) => assert_eq!(b, b"a"),
            o => panic!("snapshot=10: {:?}", o),
        }
        assert_eq!(
            get_at_snapshot(&store, 7, &oid, 20),
            SnapshotRead::Tombstoned,
            "snapshot=20 must see the tombstone"
        );
        assert_eq!(
            get_at_snapshot(&store, 7, &oid, 25),
            SnapshotRead::Tombstoned,
            "snapshot=25 must still see the tombstone"
        );
        match get_at_snapshot(&store, 7, &oid, 30) {
            SnapshotRead::Found(b) => assert_eq!(b, b"c"),
            o => panic!("snapshot=30: {:?}", o),
        }
    }

    // -----------------------------------------------------------------------
    // T2.5.7: has_version_in_range half-open interval (lo_excl, hi_incl].
    //   write at opnums 10 and 20.
    //   (10, 20] contains 20      -> true
    //   (20, 30] nothing          -> false
    //   (9,  10] contains 10      -> true
    //   (0,   9] nothing          -> false
    //   (10, 19] nothing          -> false (10 excluded; 20 above hi)
    // -----------------------------------------------------------------------
    #[test]
    fn has_version_in_range_half_open_lo() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let oid = obj(8);
        put_versioned(&mut store, 7, &oid, 10, Some(b"v".to_vec())).unwrap();
        put_versioned(&mut store, 7, &oid, 20, Some(b"v".to_vec())).unwrap();

        assert!(
            has_version_in_range(&store, 7, &oid, 10, 20),
            "(10,20] should be true"
        );
        assert!(
            !has_version_in_range(&store, 7, &oid, 20, 30),
            "(20,30] should be false"
        );
        assert!(
            has_version_in_range(&store, 7, &oid, 9, 10),
            "(9,10] should be true"
        );
        assert!(
            !has_version_in_range(&store, 7, &oid, 0, 9),
            "(0,9] should be false"
        );
        assert!(
            !has_version_in_range(&store, 7, &oid, 10, 19),
            "(10,19] should be false"
        );
    }

    // -----------------------------------------------------------------------
    // Bonus: MvccKeyError display + SnapshotRead derives (non-todo tests).
    // -----------------------------------------------------------------------
    #[test]
    fn mvcc_key_error_display() {
        let err = MvccKeyError::Length(0);
        let msg = format!("{err}");
        assert!(msg.contains("28"), "display should mention expected length");
        assert!(msg.contains('0'), "display should mention actual length");
        let err2 = MvccKeyError::Length(100);
        assert!(format!("{err2}").contains("100"));
    }

    #[test]
    fn snapshot_read_clone_eq() {
        let a = SnapshotRead::Found(vec![7, 8, 9]);
        let b = a.clone();
        assert_eq!(a, b);
        assert_ne!(a, SnapshotRead::Tombstoned);
        assert_ne!(SnapshotRead::Tombstoned, SnapshotRead::NotYetWritten);
    }

    // -----------------------------------------------------------------------
    // T4.1: Snapshot read of a never-written (type_id, object_id) → NotYetWritten.
    //
    // KAT derivation: no put_versioned calls reference type_id=7 / obj(99).
    // All snapshot windows must see an empty version chain → NotYetWritten.
    // Checks snapshot=0 (absolute minimum) and snapshot=1000 (far future).
    // -----------------------------------------------------------------------
    #[test]
    fn never_written_prefix_returns_not_yet_written() {
        let store = Storage::open(MemVfs::new()).unwrap();
        assert_eq!(
            get_at_snapshot(&store, 7, &obj(99), 1000),
            SnapshotRead::NotYetWritten,
            "large snapshot on empty key must be NotYetWritten"
        );
        assert_eq!(
            get_at_snapshot(&store, 7, &obj(99), 0),
            SnapshotRead::NotYetWritten,
            "snapshot=0 on empty key must be NotYetWritten"
        );
    }

    // -----------------------------------------------------------------------
    // T4.2: Snapshot far beyond the latest written opnum returns the latest value.
    //
    // KAT derivation:
    //   write opnum=10 value="v10"
    //   snapshot=u64::MAX: newest version with commit_opnum ≤ u64::MAX is opnum=10
    //     → Found("v10")
    //   snapshot=1<<40 (=1_099_511_627_776) >> 10: same reasoning → Found("v10")
    //   snapshot=9: no version with commit_opnum ≤ 9 exists → NotYetWritten
    // -----------------------------------------------------------------------
    #[test]
    fn snapshot_beyond_max_written_returns_latest() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let oid = obj(100);
        put_versioned(&mut store, 7, &oid, 10, Some(b"v10".to_vec())).unwrap();

        match get_at_snapshot(&store, 7, &oid, u64::MAX) {
            SnapshotRead::Found(b) => assert_eq!(b, b"v10", "snapshot=u64::MAX must return v10"),
            o => panic!("expected Found(v10) at snapshot=u64::MAX, got {:?}", o),
        }
        match get_at_snapshot(&store, 7, &oid, 1u64 << 40) {
            SnapshotRead::Found(b) => assert_eq!(b, b"v10", "snapshot=1<<40 must return v10"),
            o => panic!("expected Found(v10) at snapshot=1<<40, got {:?}", o),
        }
        // Sanity: snapshot before the single write.
        assert_eq!(
            get_at_snapshot(&store, 7, &oid, 9),
            SnapshotRead::NotYetWritten,
            "snapshot=9 must be NotYetWritten (write was at opnum=10)"
        );
    }

    // -----------------------------------------------------------------------
    // T4.3: Snapshot exactly AT a commit_opnum returns that version (inclusive boundary).
    //
    // KAT derivation:
    //   write opnum=50 value="x"
    //   snapshot=50: newest version with commit_opnum ≤ 50 is opnum=50 → Found("x")
    //   snapshot=49: no version with commit_opnum ≤ 49 exists → NotYetWritten
    //
    // This test pins the INCLUSIVE boundary of the ≤ predicate — snapshot==commit_opnum
    // must resolve, not be excluded.
    // -----------------------------------------------------------------------
    #[test]
    fn snapshot_at_commit_opnum_returns_that_version() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let oid = obj(101);
        put_versioned(&mut store, 7, &oid, 50, Some(b"x".to_vec())).unwrap();

        match get_at_snapshot(&store, 7, &oid, 50) {
            SnapshotRead::Found(b) => assert_eq!(b, b"x", "snapshot=50 must find value at opnum=50"),
            o => panic!("expected Found at snapshot==commit_opnum=50, got {:?}", o),
        }
        assert_eq!(
            get_at_snapshot(&store, 7, &oid, 49),
            SnapshotRead::NotYetWritten,
            "snapshot=49 must be NotYetWritten (write is at opnum=50, exclusive)"
        );
    }

    // -----------------------------------------------------------------------
    // T4.4: Many versions on one key; exhaustive sweep of all snapshot values.
    //
    // KAT derivation:
    //   writes: (5,"a"), (10,"b"), (15,"c"), (20,"d"), (25,"e"), (30,"f")
    //   For each snapshot s in 0..=40, expected = max(opnum | opnum ≤ s).value
    //     s=0..4   → None          → NotYetWritten
    //     s=5..9   → opnum=5       → Found("a")
    //     s=10..14 → opnum=10      → Found("b")
    //     s=15..19 → opnum=15      → Found("c")
    //     s=20..24 → opnum=20      → Found("d")
    //     s=25..29 → opnum=25      → Found("e")
    //     s=30..40 → opnum=30      → Found("f")
    //
    // The expected value is derived inline from the `writes` slice — same rule
    // as the implementation ("newest commit_opnum ≤ snapshot wins") — making
    // this a full KAT sweep rather than a tautological self-check.
    // -----------------------------------------------------------------------
    #[test]
    fn many_versions_many_snapshots_exhaustive() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let oid = obj(102);
        let writes: Vec<(u64, &[u8])> = vec![
            (5, b"a"),
            (10, b"b"),
            (15, b"c"),
            (20, b"d"),
            (25, b"e"),
            (30, b"f"),
        ];
        for &(c, v) in &writes {
            put_versioned(&mut store, 7, &oid, c, Some(v.to_vec())).unwrap();
        }
        for snap in 0u64..=40 {
            // Hand-derive: take max opnum ≤ snap from the writes list.
            let expected: Option<&[u8]> = writes
                .iter()
                .filter(|(c, _)| *c <= snap)
                .max_by_key(|(c, _)| *c)
                .map(|(_, v)| *v);
            match (expected, get_at_snapshot(&store, 7, &oid, snap)) {
                (None, SnapshotRead::NotYetWritten) => {}
                (Some(e), SnapshotRead::Found(b)) => {
                    assert_eq!(b, e, "wrong version at snap={}", snap);
                }
                (e, r) => panic!("snap={}: expected {:?} got {:?}", snap, e, r),
            }
        }
    }

    // -----------------------------------------------------------------------
    // T4.5: Write → tombstone → write-again lifecycle sweep.
    //
    // KAT derivation:
    //   opnum=10 → Found("v1")
    //   opnum=20 → None (tombstone)
    //   opnum=30 → Found("v3")
    //
    //   snapshot=9:  no version ≤ 9              → NotYetWritten
    //   snapshot=10: newest ≤ 10 is opnum=10     → Found("v1")
    //   snapshot=19: newest ≤ 19 is opnum=10     → Found("v1")  (tombstone is at 20)
    //   snapshot=20: newest ≤ 20 is opnum=20     → Tombstoned
    //   snapshot=25: newest ≤ 25 is opnum=20     → Tombstoned   (revival at 30)
    //   snapshot=30: newest ≤ 30 is opnum=30     → Found("v3")
    //   snapshot=99: newest ≤ 99 is opnum=30     → Found("v3")
    // -----------------------------------------------------------------------
    #[test]
    fn write_after_tombstone_revives_key() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let oid = obj(103);
        put_versioned(&mut store, 7, &oid, 10, Some(b"v1".to_vec())).unwrap();
        put_versioned(&mut store, 7, &oid, 20, None).unwrap(); // tombstone
        put_versioned(&mut store, 7, &oid, 30, Some(b"v3".to_vec())).unwrap();

        assert_eq!(
            get_at_snapshot(&store, 7, &oid, 9),
            SnapshotRead::NotYetWritten,
            "snapshot=9: key not yet written"
        );
        match get_at_snapshot(&store, 7, &oid, 10) {
            SnapshotRead::Found(b) => assert_eq!(b, b"v1", "snapshot=10 → v1"),
            o => panic!("snapshot=10: expected Found(v1), got {:?}", o),
        }
        match get_at_snapshot(&store, 7, &oid, 19) {
            SnapshotRead::Found(b) => assert_eq!(b, b"v1", "snapshot=19 → v1 (tombstone at 20)"),
            o => panic!("snapshot=19: expected Found(v1), got {:?}", o),
        }
        assert_eq!(
            get_at_snapshot(&store, 7, &oid, 20),
            SnapshotRead::Tombstoned,
            "snapshot=20: tombstone visible"
        );
        assert_eq!(
            get_at_snapshot(&store, 7, &oid, 25),
            SnapshotRead::Tombstoned,
            "snapshot=25: still tombstoned (revival at 30)"
        );
        match get_at_snapshot(&store, 7, &oid, 30) {
            SnapshotRead::Found(b) => assert_eq!(b, b"v3", "snapshot=30 → v3 (revival)"),
            o => panic!("snapshot=30: expected Found(v3), got {:?}", o),
        }
        match get_at_snapshot(&store, 7, &oid, 99) {
            SnapshotRead::Found(b) => assert_eq!(b, b"v3", "snapshot=99 → v3"),
            o => panic!("snapshot=99: expected Found(v3), got {:?}", o),
        }
    }

    // -----------------------------------------------------------------------
    // T4.6: Out-of-order put_versioned calls — calls arrive with non-monotonic
    // commit_opnums. The storage layer sorts by key (inverted opnum), so even
    // when puts arrive 30→10→20, all three versions coexist correctly and
    // snapshot reads return the right version at every query point.
    //
    // KAT derivation:
    //   put order: opnum=30→"c", opnum=10→"a", opnum=20→"b"
    //   logical version chain (sorted by opnum): 10→"a", 20→"b", 30→"c"
    //
    //   snapshot=9:  newest ≤ 9 is nothing        → NotYetWritten
    //   snapshot=10: newest ≤ 10 is opnum=10      → Found("a")
    //   snapshot=15: newest ≤ 15 is opnum=10      → Found("a")
    //   snapshot=20: newest ≤ 20 is opnum=20      → Found("b")
    //   snapshot=25: newest ≤ 25 is opnum=20      → Found("b")
    //   snapshot=30: newest ≤ 30 is opnum=30      → Found("c")
    //   snapshot=99: newest ≤ 99 is opnum=30      → Found("c")
    // -----------------------------------------------------------------------
    #[test]
    fn out_of_order_writes_still_yield_correct_snapshot_reads() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let oid = obj(104);
        // Deliberately insert in reverse / scrambled order.
        put_versioned(&mut store, 7, &oid, 30, Some(b"c".to_vec())).unwrap();
        put_versioned(&mut store, 7, &oid, 10, Some(b"a".to_vec())).unwrap();
        put_versioned(&mut store, 7, &oid, 20, Some(b"b".to_vec())).unwrap();

        assert_eq!(
            get_at_snapshot(&store, 7, &oid, 9),
            SnapshotRead::NotYetWritten,
            "snapshot=9: nothing written yet"
        );
        match get_at_snapshot(&store, 7, &oid, 10) {
            SnapshotRead::Found(b) => assert_eq!(b, b"a", "snapshot=10 → a"),
            o => panic!("snapshot=10: expected Found(a), got {:?}", o),
        }
        match get_at_snapshot(&store, 7, &oid, 15) {
            SnapshotRead::Found(b) => assert_eq!(b, b"a", "snapshot=15 → a (next write at 20)"),
            o => panic!("snapshot=15: expected Found(a), got {:?}", o),
        }
        match get_at_snapshot(&store, 7, &oid, 20) {
            SnapshotRead::Found(b) => assert_eq!(b, b"b", "snapshot=20 → b"),
            o => panic!("snapshot=20: expected Found(b), got {:?}", o),
        }
        match get_at_snapshot(&store, 7, &oid, 25) {
            SnapshotRead::Found(b) => assert_eq!(b, b"b", "snapshot=25 → b (next write at 30)"),
            o => panic!("snapshot=25: expected Found(b), got {:?}", o),
        }
        match get_at_snapshot(&store, 7, &oid, 30) {
            SnapshotRead::Found(b) => assert_eq!(b, b"c", "snapshot=30 → c"),
            o => panic!("snapshot=30: expected Found(c), got {:?}", o),
        }
        match get_at_snapshot(&store, 7, &oid, 99) {
            SnapshotRead::Found(b) => assert_eq!(b, b"c", "snapshot=99 → c"),
            o => panic!("snapshot=99: expected Found(c), got {:?}", o),
        }
    }
}
