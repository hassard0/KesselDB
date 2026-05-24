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

/// SP114 / S2.5: Garbage-collect MVCC versions whose commit_opnum is
/// strictly less than `low_water_mark`. Returns the count of versions
/// deleted. See S2.5 design Decision 3.
///
/// Algorithm: scan the full versioned-storage range, decode every
/// 28-byte key's commit_opnum, delete every entry whose commit_opnum
/// < low_water_mark. Tombstones are also reclaimed.
///
/// Complexity: O(N) where N is the total number of versioned entries.
/// Range-pruning / bloom-filter optimisations are S2.X follow-ups.
///
/// Determinism: scan order is BTreeMap-deterministic (sorted by
/// 28-byte key); deletion order is therefore deterministic; the
/// resulting LSM state is byte-identical across replicas given the
/// same pre-GC state + the same low_water_mark.
pub fn delete_versions_older_than<V: Vfs>(
    store: &mut Storage<V>,
    low_water_mark: u64,
) -> Result<usize, MvccKeyError> {
    // Build the FULL versioned-key range. The 28-byte key space is
    // bounded lex-min = [0x00; 28] and lex-max = [0xFF; 28]; legacy
    // 20-byte keys produced by `make_key` cannot satisfy the length
    // check inside `decode_commit_opnum` and are skipped on the
    // length-error branch, so a "full scan" is safe even when both
    // 20-byte and 28-byte keys coexist in the LSM (the 20-byte keys
    // sort INSIDE the 28-byte range because lex-comparison treats the
    // shorter key as a strict prefix; `decode_commit_opnum` rejects
    // them with `MvccKeyError::Length(_)` and we skip).
    let lo: Key = vec![0x00u8; VERSIONED_KEY_LEN];
    let hi: Key = vec![0xFFu8; VERSIONED_KEY_LEN];

    // Collect keys to delete in a Vec first (avoid mutating storage
    // mid-scan; `scan_range_versions` already returns owned Vec). The
    // scan order is BTreeMap-deterministic (sorted ASCENDING by 28-byte
    // key), so `to_delete` is deterministic across replicas, and so is
    // the subsequent delete iteration.
    let mut to_delete: Vec<Key> = Vec::new();
    for (k, _v) in store.scan_range_versions(&lo, &hi) {
        match decode_commit_opnum(&k) {
            // Strict-less-than (Decision 3): a version at EXACTLY
            // `low_water_mark` is PRESERVED — it remains the oldest
            // serveable version for any `Tx::begin(snapshot=low_water_mark)`.
            Ok(c) if c < low_water_mark => to_delete.push(k),
            Ok(_) => continue,
            // Malformed key (length != 28) — skip. The 20-byte legacy
            // catalog/data keys land here on the length-error branch.
            Err(_) => continue,
        }
    }

    let count = to_delete.len();
    // Deletion: `Storage::delete` writes a tombstone at `op_number =
    // low_water_mark`. This is the deterministic "apply timestamp" of
    // the GC: every replica's apply path calls into this function with
    // the same `low_water_mark`, so every replica records tombstones
    // under the same op_number. NB: `Storage::delete` can only fail on
    // WAL I/O; the only `MvccKeyError` variant is `Length` — repurposed
    // defensively here as a generic "storage delete failed" signal so
    // the SM apply arm can reject the op atomically.
    for k in to_delete {
        store
            .delete(low_water_mark, k)
            .map_err(|_| MvccKeyError::Length(0))?;
    }
    Ok(count)
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

// ----------------------------------------------------------------------------
// Pentest: hostile-input + adversarial-correctness locks (SP110 T5).
//
// Every attacker-controlled byte of the MVCC storage layer surface is
// reasoned about independently:
//   * type_id (the 4 LE bytes)         — extremes, adjacent prefixes
//   * object_id (16 bytes verbatim)    — adjacent prefixes (last-byte bleed)
//   * commit_opnum (8 BE inverted)     — 0 and u64::MAX boundaries; arbitrary
//                                        out-of-order arrival
//   * snapshot_opnum                   — 0, u64::MAX, exact-match boundaries
//   * decode input length              — every reachable non-28 length
//   * write/read ordering              — reverse-order puts vs has_version_in_range
//   * legacy/versioned key coexistence — 20-byte vs 28-byte non-collision
//
// Each HOSTILE test wraps the call in `catch_unwind` and asserts:
//   (a) NO panic / NO unwind (so OOM-via-panic is also caught here);
//   (b) a TYPED `Err(MvccKeyError::...)` OR the EXACT correct `Ok(...)` /
//       `SnapshotRead::Found(value)` per the scenario.
// Each POSITIVE correctness lock asserts the EXACT value (no
// `matches!(.., Found(_))` shortcuts).
//
// Discipline: hostile bytes (e.g., the [0x00;8] / [0xFF;8] suffixes,
// the adjacent type_id LE encodings) are HAND-DERIVED from the encoding
// recipe in this file's doc-comment, NOT taken from the production
// encoder's output. A failure here is either a real vulnerability
// (BLOCKED, report; never weaken the encoder/decoder to silence) or a
// test bug (fix the test). Production code in this file MUST NOT change
// to make a pentest pass.
#[cfg(test)]
mod pentest_mvcc {
    use super::*;
    use crate::{make_key, Storage};
    use kessel_io::MemVfs;

    /// Helper: deterministic 16-byte object_id from a single byte.
    fn obj(n: u8) -> [u8; 16] {
        let mut a = [0u8; 16];
        a[15] = n;
        a
    }

    /// Mirrors SP107 `pentest_v2::no_panic_typed_err`: run a closure under
    /// `catch_unwind`, assert it did NOT panic AND yielded a typed
    /// `Err(MvccKeyError::Length(_))`. Used by the malformed-length lock.
    fn decode_no_panic_typed_err(key: Vec<u8>, expected_len: usize) {
        let r = std::panic::catch_unwind(move || decode_commit_opnum(&key));
        assert!(
            r.is_ok(),
            "decode_commit_opnum must NOT panic on hostile input of length {expected_len}"
        );
        match r.unwrap() {
            Err(MvccKeyError::Length(n)) => assert_eq!(
                n, expected_len,
                "Length error must report actual length {expected_len}, got {n}"
            ),
            other => panic!(
                "decode_commit_opnum on len={expected_len} must return Err(MvccKeyError::Length), got {other:?}"
            ),
        }
    }

    // -------------------------------------------------------------------
    // T5.1: commit_opnum = u64::MAX (the inverted suffix is [0x00; 8],
    // which equals the `lo` bound of the scan range exactly).
    //
    // KAT derivation:
    //   inverted = u64::MAX - u64::MAX = 0
    //   BE bytes of suffix: [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
    //   key suffix == lo's suffix [0x00;8] → matched by inclusive lo bound.
    //   snapshot=u64::MAX is the only snapshot ≥ commit_opnum=u64::MAX,
    //   so reads at snapshot < u64::MAX must be NotYetWritten.
    //
    // Hostile angle: an off-by-one in the lo bound (exclusive vs inclusive)
    // would silently drop the u64::MAX version. This test pins inclusive.
    // -------------------------------------------------------------------
    #[test]
    fn t5_1_commit_opnum_u64_max_no_panic_and_round_trips() {
        let r = std::panic::catch_unwind(|| {
            let mut store = Storage::open(MemVfs::new()).unwrap();
            put_versioned(&mut store, 7, &obj(1), u64::MAX, Some(b"max".to_vec())).unwrap();
            // Exact-suffix sanity (KAT): key suffix MUST be all-zeros BE.
            let k = make_versioned_key(7, &obj(1), u64::MAX);
            assert_eq!(&k[PREFIX_LEN..VERSIONED_KEY_LEN], &[0x00u8; 8]);
            // Snapshot at the commit point must Found exactly "max".
            let at_max = get_at_snapshot(&store, 7, &obj(1), u64::MAX);
            // Snapshot one below must NOT see it (commit_opnum > snapshot).
            let below = get_at_snapshot(&store, 7, &obj(1), u64::MAX - 1);
            (at_max, below)
        });
        assert!(r.is_ok(), "u64::MAX commit_opnum must NOT panic");
        let (at_max, below) = r.unwrap();
        match at_max {
            SnapshotRead::Found(b) => assert_eq!(b, b"max", "snap=u64::MAX must Found(max)"),
            o => panic!("snap=u64::MAX expected Found(max), got {o:?}"),
        }
        assert_eq!(
            below,
            SnapshotRead::NotYetWritten,
            "snap=u64::MAX-1 must NOT see a write committed at u64::MAX"
        );
    }

    // -------------------------------------------------------------------
    // T5.2: commit_opnum = 0 (the inverted suffix is [0xFF; 8], which
    // equals the `hi` bound of the scan range exactly).
    //
    // KAT derivation:
    //   inverted = u64::MAX - 0 = u64::MAX
    //   BE bytes of suffix: [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
    //   key suffix == hi's suffix [0xFF;8] → matched by inclusive hi bound.
    //   This is the LAST 28-byte key of the (type_id, object_id) prefix.
    //
    // Hostile angle: an off-by-one in the hi bound would silently drop
    // the opnum=0 version. This test pins inclusive.
    // -------------------------------------------------------------------
    #[test]
    fn t5_2_commit_opnum_zero_no_panic_and_round_trips() {
        let r = std::panic::catch_unwind(|| {
            let mut store = Storage::open(MemVfs::new()).unwrap();
            put_versioned(&mut store, 7, &obj(2), 0, Some(b"zero".to_vec())).unwrap();
            // Exact-suffix sanity (KAT): key suffix MUST be all-ones BE.
            let k = make_versioned_key(7, &obj(2), 0);
            assert_eq!(&k[PREFIX_LEN..VERSIONED_KEY_LEN], &[0xFFu8; 8]);
            // Every snapshot ≥ 0 must Found "zero".
            let s0 = get_at_snapshot(&store, 7, &obj(2), 0);
            let s1 = get_at_snapshot(&store, 7, &obj(2), 1);
            let s_max = get_at_snapshot(&store, 7, &obj(2), u64::MAX);
            (s0, s1, s_max)
        });
        assert!(r.is_ok(), "opnum=0 must NOT panic");
        let (s0, s1, s_max) = r.unwrap();
        match s0 {
            SnapshotRead::Found(b) => assert_eq!(b, b"zero", "snap=0 must Found(zero)"),
            o => panic!("snap=0 expected Found(zero), got {o:?}"),
        }
        match s1 {
            SnapshotRead::Found(b) => assert_eq!(b, b"zero", "snap=1 must Found(zero)"),
            o => panic!("snap=1 expected Found(zero), got {o:?}"),
        }
        match s_max {
            SnapshotRead::Found(b) => assert_eq!(b, b"zero", "snap=u64::MAX must Found(zero)"),
            o => panic!("snap=u64::MAX expected Found(zero), got {o:?}"),
        }
    }

    // -------------------------------------------------------------------
    // T5.3: decode_commit_opnum rejects EVERY non-28 byte length with a
    // typed `Err(MvccKeyError::Length(n))`, never panics.
    //
    // KAT lengths (per the T5 plan): 0, 1, 20 (legacy prefix length), 27
    // (just under), 29 (just over), 100 (far over). Each is wrapped in
    // `catch_unwind` and the returned length-field is verified to equal
    // the input length (so an attacker cannot smuggle a wrong-length
    // report into the error path).
    //
    // Hostile angle: a careless `try_into().unwrap()` on a shorter slice
    // would panic, and `&key[20..28]` on a sub-28 slice would index-OOB.
    // -------------------------------------------------------------------
    #[test]
    fn t5_3_decode_rejects_every_non_28_length_no_panic() {
        for &len in &[0usize, 1, 19, 20, 21, 27, 29, 36, 100, 1024] {
            decode_no_panic_typed_err(vec![0u8; len], len);
        }
        // Also lock: a 28-byte all-zeros buffer is accepted (decodes to
        // commit_opnum = u64::MAX - 0 = u64::MAX). Hand-derived KAT.
        let r = std::panic::catch_unwind(|| decode_commit_opnum(&[0u8; VERSIONED_KEY_LEN]));
        assert!(r.is_ok(), "28-byte input must NOT panic");
        assert_eq!(
            r.unwrap(),
            Ok(u64::MAX),
            "decode([0;28]) must yield u64::MAX (inversion identity)"
        );
        // And a 28-byte buffer whose suffix is all-ones decodes to 0.
        let mut all_ones_suffix = [0u8; VERSIONED_KEY_LEN];
        for b in &mut all_ones_suffix[PREFIX_LEN..] {
            *b = 0xFF;
        }
        let r2 = std::panic::catch_unwind(move || decode_commit_opnum(&all_ones_suffix));
        assert!(r2.is_ok(), "28-byte input must NOT panic");
        assert_eq!(
            r2.unwrap(),
            Ok(0u64),
            "decode([_,_,_,_, 0xFF;8 suffix]) must yield commit_opnum=0"
        );
    }

    // -------------------------------------------------------------------
    // T5.4a: Two object_ids that share the first 15 bytes (only the last
    // byte differs) MUST NOT bleed into each other's snapshot reads.
    //
    // KAT derivation:
    //   obj(1) = [0,…,0, 1]; obj(2) = [0,…,0, 2]. Their full 20-byte
    //   prefixes differ in the LAST byte → adjacent in the BTreeMap.
    //   A bug in lo/hi construction (e.g., not including the full prefix
    //   in the bounds) would cause obj(1)'s scan to also yield obj(2)'s
    //   key. This test pins isolation.
    // -------------------------------------------------------------------
    #[test]
    fn t5_4a_adjacent_object_id_prefixes_do_not_bleed() {
        let r = std::panic::catch_unwind(|| {
            let mut store = Storage::open(MemVfs::new()).unwrap();
            put_versioned(&mut store, 7, &obj(1), 10, Some(b"obj1".to_vec())).unwrap();
            put_versioned(&mut store, 7, &obj(2), 10, Some(b"obj2".to_vec())).unwrap();
            put_versioned(&mut store, 7, &obj(3), 10, Some(b"obj3".to_vec())).unwrap();
            (
                get_at_snapshot(&store, 7, &obj(1), 100),
                get_at_snapshot(&store, 7, &obj(2), 100),
                get_at_snapshot(&store, 7, &obj(3), 100),
            )
        });
        assert!(r.is_ok(), "adjacent-prefix reads must NOT panic");
        let (r1, r2, r3) = r.unwrap();
        match r1 {
            SnapshotRead::Found(b) => assert_eq!(b, b"obj1", "obj(1) must Found(obj1) exactly"),
            o => panic!("obj(1): {o:?}"),
        }
        match r2 {
            SnapshotRead::Found(b) => assert_eq!(b, b"obj2", "obj(2) must Found(obj2) exactly"),
            o => panic!("obj(2): {o:?}"),
        }
        match r3 {
            SnapshotRead::Found(b) => assert_eq!(b, b"obj3", "obj(3) must Found(obj3) exactly"),
            o => panic!("obj(3): {o:?}"),
        }
    }

    // -------------------------------------------------------------------
    // T5.4b: Two type_ids that share the first 3 LE bytes (only the last
    // byte differs) MUST NOT bleed. Specifically:
    //   tA = 0x0100_0000 → LE [0x00, 0x00, 0x00, 0x01]
    //   tB = 0x0100_0001 → LE [0x01, 0x00, 0x00, 0x01]
    // These differ in the FIRST LE byte; adjacency in BTreeMap lex order
    // depends on the full 4-byte LE. Combined with the same object_id,
    // their 20-byte prefixes differ in byte[0] only.
    //
    // Hostile angle: a scan that didn't include the FULL type_id+object_id
    // in lo/hi could pick up tB's versions when querying tA.
    // -------------------------------------------------------------------
    #[test]
    fn t5_4b_adjacent_type_id_prefixes_do_not_bleed() {
        let r = std::panic::catch_unwind(|| {
            let mut store = Storage::open(MemVfs::new()).unwrap();
            let oid = obj(42);
            let ta: u32 = 0x0100_0000;
            let tb: u32 = 0x0100_0001;
            put_versioned(&mut store, ta, &oid, 10, Some(b"ta".to_vec())).unwrap();
            put_versioned(&mut store, tb, &oid, 10, Some(b"tb".to_vec())).unwrap();
            (
                get_at_snapshot(&store, ta, &oid, 100),
                get_at_snapshot(&store, tb, &oid, 100),
            )
        });
        assert!(r.is_ok(), "adjacent type_id reads must NOT panic");
        let (ra, rb) = r.unwrap();
        match ra {
            SnapshotRead::Found(b) => assert_eq!(b, b"ta", "type_id=0x01000000 must Found(ta)"),
            o => panic!("type_id=ta: {o:?}"),
        }
        match rb {
            SnapshotRead::Found(b) => assert_eq!(b, b"tb", "type_id=0x01000001 must Found(tb)"),
            o => panic!("type_id=tb: {o:?}"),
        }
    }

    // -------------------------------------------------------------------
    // T5.5: type_id = u32::MAX boundary. Encoding LE = [0xFF;4], which
    // is the lex-maximum 4-byte prefix; if any callsite confused the
    // type_id bound with a sentinel, this would surface it.
    //
    // KAT: write+read at type_id=u32::MAX, also verify another type_id
    // (u32::MAX - 1) doesn't bleed (adjacent in lex).
    // -------------------------------------------------------------------
    #[test]
    fn t5_5_type_id_u32_max_no_panic_and_no_bleed_with_adjacent() {
        let r = std::panic::catch_unwind(|| {
            let mut store = Storage::open(MemVfs::new()).unwrap();
            put_versioned(&mut store, u32::MAX, &obj(3), 5, Some(b"x".to_vec())).unwrap();
            put_versioned(&mut store, u32::MAX - 1, &obj(3), 5, Some(b"y".to_vec())).unwrap();
            // KAT prefix bytes for type_id=u32::MAX (LE all-ones).
            let k = make_versioned_key(u32::MAX, &obj(3), 5);
            assert_eq!(&k[0..4], &[0xFFu8; 4]);
            (
                get_at_snapshot(&store, u32::MAX, &obj(3), 10),
                get_at_snapshot(&store, u32::MAX - 1, &obj(3), 10),
                // Reading at a *completely unrelated* type_id must NOT
                // bleed from either of the above.
                get_at_snapshot(&store, 0u32, &obj(3), 10),
            )
        });
        assert!(r.is_ok(), "type_id=u32::MAX must NOT panic");
        let (rmax, rmax_minus_1, runrelated) = r.unwrap();
        match rmax {
            SnapshotRead::Found(b) => assert_eq!(b, b"x", "type_id=u32::MAX must Found(x)"),
            o => panic!("type_id=u32::MAX: {o:?}"),
        }
        match rmax_minus_1 {
            SnapshotRead::Found(b) => assert_eq!(b, b"y", "type_id=u32::MAX-1 must Found(y)"),
            o => panic!("type_id=u32::MAX-1: {o:?}"),
        }
        assert_eq!(
            runrelated,
            SnapshotRead::NotYetWritten,
            "unrelated type_id=0 must NOT inherit u32::MAX-area writes"
        );
    }

    // -------------------------------------------------------------------
    // T5.6: Writes arrive in arbitrary order. has_version_in_range MUST
    // return the same answer as if the writes had arrived monotonically.
    //
    // The pentest twist (beyond T4.6 correctness): a window
    // (lo_excl, hi_incl] is queried AFTER older opnums are written that
    // would naively be skipped if the scan assumed a monotonic write log.
    //
    // KAT scenario:
    //   sequence of writes (in arrival order): opnum=30, 10, 50, 20, 40
    //   logical chain (by opnum): 10,20,30,40,50.
    //
    //   query (15, 35]:  expected true   (matches 20, 30)
    //   query (35, 49]:  expected true   (matches 40)
    //   query (50, 60]:  expected false  (50 excluded; nothing > 50)
    //   query (0, 9]:    expected false  (nothing ≤ 9)
    //   query (10, 10]:  expected false  (half-open: 10 excluded; no version > 10 AND ≤ 10)
    //   query (9, 10]:   expected true   (10 included)
    //   query (40, 50]:  expected true   (50 included)
    // -------------------------------------------------------------------
    #[test]
    fn t5_6_reverse_order_writes_have_version_in_range_consistent() {
        let r = std::panic::catch_unwind(|| {
            let mut store = Storage::open(MemVfs::new()).unwrap();
            let oid = obj(7);
            // Deliberately scrambled arrival order.
            for &c in &[30u64, 10, 50, 20, 40] {
                put_versioned(&mut store, 7, &oid, c, Some(b"v".to_vec())).unwrap();
            }
            (
                has_version_in_range(&store, 7, &oid, 15, 35),
                has_version_in_range(&store, 7, &oid, 35, 49),
                has_version_in_range(&store, 7, &oid, 50, 60),
                has_version_in_range(&store, 7, &oid, 0, 9),
                has_version_in_range(&store, 7, &oid, 10, 10),
                has_version_in_range(&store, 7, &oid, 9, 10),
                has_version_in_range(&store, 7, &oid, 40, 50),
            )
        });
        assert!(r.is_ok(), "scrambled-arrival has_version_in_range must NOT panic");
        let (q1, q2, q3, q4, q5, q6, q7) = r.unwrap();
        assert!(q1, "(15,35] must be true (matches opnum 20, 30)");
        assert!(q2, "(35,49] must be true (matches opnum 40)");
        assert!(!q3, "(50,60] must be false (50 excluded by lo)");
        assert!(!q4, "(0,9] must be false (no version ≤ 9)");
        assert!(!q5, "(10,10] must be false (empty half-open window)");
        assert!(q6, "(9,10] must be true (10 included by hi)");
        assert!(q7, "(40,50] must be true (50 included by hi)");
    }

    // -------------------------------------------------------------------
    // T5.7: Legacy 20-byte keys and MVCC 28-byte keys MUST NOT collide
    // for the same (type_id, object_id). This is the S2.6 cutover
    // prerequisite — the byte-identity invariant requires that an MVCC
    // write never mutates a legacy-written key.
    //
    // KAT derivation:
    //   legacy = type_id(4 LE) ++ object_id(16)               [20 bytes]
    //   mvcc   = type_id(4 LE) ++ object_id(16) ++ inv(8 BE)  [28 bytes]
    //   Vec<u8> equality includes length → length mismatch alone
    //   guarantees inequality, but we also independently confirm:
    //     (a) the 20-byte prefix matches the first 20 bytes of EVERY
    //         28-byte key for the same (type_id, object_id),
    //     (b) a legacy-key write is invisible to the MVCC reader, and
    //     (c) MVCC writes for the same (type_id, object_id) do not appear
    //         as a legacy 20-byte key when scanned by the legacy path.
    //
    // Hostile angle: an encoder bug that omitted the inverted suffix when
    // commit_opnum==0 (because inversion → u64::MAX → "no trailing
    // bytes"?) would collapse a 28-byte key to 20 bytes and collide
    // with the legacy path. This test pins length and lock byte content.
    // -------------------------------------------------------------------
    #[test]
    fn t5_7_legacy_20byte_and_mvcc_28byte_keys_never_collide() {
        let r = std::panic::catch_unwind(|| {
            let legacy = make_key(7, &obj(8));
            let mvcc_v0 = make_versioned_key(7, &obj(8), 0);
            let mvcc_vmax = make_versioned_key(7, &obj(8), u64::MAX);
            let mvcc_v_mid = make_versioned_key(7, &obj(8), 0x1234_5678_9ABC_DEF0);
            // Length locks.
            assert_eq!(legacy.len(), 20, "legacy key MUST be 20 bytes");
            assert_eq!(mvcc_v0.len(), 28, "MVCC key MUST be 28 bytes (opnum=0)");
            assert_eq!(
                mvcc_vmax.len(),
                28,
                "MVCC key MUST be 28 bytes (opnum=u64::MAX)"
            );
            assert_eq!(mvcc_v_mid.len(), 28, "MVCC key MUST be 28 bytes (mid opnum)");
            // Prefix-match locks (the first 20 bytes are shared by design).
            assert_eq!(&legacy[..], &mvcc_v0[..PREFIX_LEN]);
            assert_eq!(&legacy[..], &mvcc_vmax[..PREFIX_LEN]);
            assert_eq!(&legacy[..], &mvcc_v_mid[..PREFIX_LEN]);
            // Inequality locks (Vec<u8> equality includes length).
            assert_ne!(legacy, mvcc_v0);
            assert_ne!(legacy, mvcc_vmax);
            assert_ne!(legacy, mvcc_v_mid);
            // Suffix locks (independent of the prefix).
            assert_eq!(&mvcc_v0[PREFIX_LEN..], &[0xFFu8; 8]);
            assert_eq!(&mvcc_vmax[PREFIX_LEN..], &[0x00u8; 8]);
            // Mid opnum: inv = u64::MAX - 0x1234_5678_9ABC_DEF0
            //          = 0xEDCB_A987_6543_210F
            // BE bytes: [0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F]
            assert_eq!(
                &mvcc_v_mid[PREFIX_LEN..],
                &[0xED, 0xCB, 0xA9, 0x87, 0x65, 0x43, 0x21, 0x0F]
            );
        });
        assert!(r.is_ok(), "legacy-vs-MVCC key construction must NOT panic");
        r.unwrap();
    }

    // -------------------------------------------------------------------
    // T5.7b: Live coexistence — write a legacy 20-byte key AND a 28-byte
    // MVCC key under the same (type_id, object_id), then verify the
    // legacy read path and the MVCC read path each see only their own
    // entries. This is the S2.6 cutover invariant in motion.
    //
    // KAT scenario:
    //   1. legacy put @ (type_id=7, obj(9)) := "legacy_val"     [20-byte key, opnum=1]
    //   2. mvcc   put @ (type_id=7, obj(9), opnum=5) := "mvcc_v5"  [28-byte key]
    //
    //   Expectations:
    //     * Scan over the legacy 20-byte key exactly yields "legacy_val".
    //     * get_at_snapshot(7, obj(9), 100) yields Found("mvcc_v5").
    //     * The legacy entry is NOT visible to get_at_snapshot (its key
    //       length is 20, decode_commit_opnum would return Length(20),
    //       which the scan handler skips via the `Err(_) => continue`
    //       branch in get_at_snapshot).
    //     * The MVCC entry is NOT visible to the legacy read (different
    //       key bytes; legacy seek for a 20-byte key cannot match the
    //       28-byte entry).
    //
    // Hostile angle: if get_at_snapshot's scan_range_versions emitted a
    // legacy 20-byte key, decode_commit_opnum would either panic
    // (defended by T5.3) or be silently misinterpreted (defended HERE
    // — the legacy key value MUST NOT leak into MVCC snapshot reads).
    // -------------------------------------------------------------------
    #[test]
    fn t5_7b_legacy_and_mvcc_coexist_without_interference() {
        let r = std::panic::catch_unwind(|| {
            let mut store = Storage::open(MemVfs::new()).unwrap();
            // 1. Legacy put: 20-byte key via crate::make_key, value "legacy_val".
            //    Use put_entry_versioned with opnum=1 so the WAL accepts it;
            //    the LEGACY shape is the KEY length (20), not the opnum field.
            let legacy_key = make_key(7, &obj(9));
            assert_eq!(legacy_key.len(), 20, "legacy key sanity");
            store
                .put_entry_versioned(1, legacy_key.clone(), Some(b"legacy_val".to_vec()))
                .expect("legacy put must succeed");
            // 2. MVCC put: 28-byte key at opnum=5, value "mvcc_v5".
            put_versioned(&mut store, 7, &obj(9), 5, Some(b"mvcc_v5".to_vec()))
                .expect("mvcc put must succeed");
            // Now scan the legacy key range: hi == lo+1 sentinel suffix
            // would cross into MVCC territory, so we use the EXACT legacy
            // key as both lo and hi (inclusive single-point read).
            let legacy_scan = store.scan_range_versions(&legacy_key, &legacy_key);
            // And MVCC snapshot read.
            let mvcc_read = get_at_snapshot(&store, 7, &obj(9), 100);
            // And MVCC snapshot at the legacy opnum (= 1) — MUST be
            // NotYetWritten because the legacy key has length 20 and
            // get_at_snapshot's decode_commit_opnum skips non-28-byte
            // keys via the `Err(_) => continue` branch. The MVCC write
            // is at opnum=5 > 1, so it's also not visible at snap=1.
            let mvcc_at_snap_1 = get_at_snapshot(&store, 7, &obj(9), 1);
            (legacy_scan, mvcc_read, mvcc_at_snap_1)
        });
        assert!(r.is_ok(), "legacy+MVCC coexistence must NOT panic");
        let (legacy_scan, mvcc_read, mvcc_at_snap_1) = r.unwrap();
        // Legacy scan: EXACTLY one entry, with value "legacy_val".
        assert_eq!(
            legacy_scan.len(),
            1,
            "legacy single-point scan must return exactly one entry, got {legacy_scan:?}"
        );
        assert_eq!(
            legacy_scan[0].0.len(),
            20,
            "legacy scan must return a 20-byte key (not the 28-byte MVCC variant)"
        );
        assert_eq!(
            legacy_scan[0].1.as_deref(),
            Some(b"legacy_val".as_slice()),
            "legacy scan must yield the legacy value, not the MVCC value"
        );
        // MVCC read at snap=100: Found("mvcc_v5") EXACTLY.
        match mvcc_read {
            SnapshotRead::Found(b) => assert_eq!(
                b, b"mvcc_v5",
                "MVCC snap=100 must Found(mvcc_v5) — NOT the legacy_val"
            ),
            o => panic!("MVCC snap=100 expected Found(mvcc_v5), got {o:?}"),
        }
        // MVCC read at snap=1: NotYetWritten (MVCC write is at opnum=5;
        // legacy 20-byte key is invisible to MVCC's 28-byte decoder).
        assert_eq!(
            mvcc_at_snap_1,
            SnapshotRead::NotYetWritten,
            "MVCC snap=1 must be NotYetWritten — legacy_val MUST NOT leak in"
        );
    }

    // ========================================================================
    // SP114 / S2.5 T2 — GC + watermark KATs (4 of 11).
    //
    // Each KAT carries a leading Claim / Workload / Expected comment block
    // deriving the expected outcome from the watermark contract:
    //   - delete_versions_older_than uses STRICT less-than:
    //     versions with commit_opnum < low_water_mark are deleted;
    //     a version at commit_opnum == low_water_mark is PRESERVED
    //     (Decision 3). It returns the deletion count.
    //   - Storage::low_water_mark() / set_low_water_mark are the
    //     Tx-side accessor symmetry the SM apply arm uses to sync
    //     the storage's view with the SM's.
    // ========================================================================

    /// KAT-1 (plan): delete_versions_older_than on empty storage.
    /// Claim:    A fresh Storage with no versioned writes has nothing
    ///           to delete; the primitive must return Ok(0) for any
    ///           low_water_mark (including 0, MAX, and mid-range).
    /// Workload: Open an empty Storage. Call delete_versions_older_than
    ///           with low_water_mark in {0, 1, 100, u64::MAX}.
    /// Expected: Ok(0) for every call; the storage version-scan range
    ///           remains empty afterward (no spurious tombstones
    ///           emitted — the function only deletes keys it visited).
    #[test]
    fn kat_delete_versions_older_than_empty_storage() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        for &lwm in &[0u64, 1, 100, u64::MAX] {
            let n = delete_versions_older_than(&mut store, lwm).unwrap();
            assert_eq!(n, 0, "empty storage: lwm={lwm} must return Ok(0)");
        }
        // Sanity: the version-scan range remains empty.
        let lo = vec![0x00u8; VERSIONED_KEY_LEN];
        let hi = vec![0xFFu8; VERSIONED_KEY_LEN];
        let rows = store.scan_range_versions(&lo, &hi);
        assert!(
            rows.iter().all(|(_, v)| v.is_none() || v.is_some() == false),
            "empty storage must remain empty (no spurious tombstones)",
        );
        // Stronger: scan_range_versions on empty storage returns empty.
        assert!(rows.is_empty(), "empty storage scan must be empty");
    }

    /// KAT-7 (plan): delete_versions_older_than reclaims pre-watermark
    /// versions and returns the count.
    /// Claim:    With versions at commit_opnums {1,2,3,4,5,6,7},
    ///           low_water_mark = 4 must reclaim opnums {1,2,3}
    ///           (strict less-than) — three deletions, count == 3.
    ///           Versions at opnums {4,5,6,7} are PRESERVED (Decision 3
    ///           — strict less-than; at-watermark serveable).
    /// Workload: Write 7 versions of the same key at opnums 1..=7.
    ///           Call delete_versions_older_than(low_water_mark = 4).
    /// Expected: returns Ok(3). Subsequent get_at_snapshot at snap >=
    ///           4 still finds the v4..v7 versions; the version-scan
    ///           range no longer contains the v1..v3 ORIGINAL entries
    ///           (the storage carries tombstones for them, but their
    ///           original Some(value) payloads are gone).
    #[test]
    fn kat_delete_versions_older_than_count() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let oid = obj(50);
        for c in 1u64..=7 {
            let v = format!("v{c}").into_bytes();
            put_versioned(&mut store, 7, &oid, c, Some(v)).unwrap();
        }
        // Hand-derived count: opnums {1,2,3} are strictly less than
        // low_water_mark=4 ⇒ 3 deletions. opnums {4,5,6,7} preserved.
        let n = delete_versions_older_than(&mut store, 4).unwrap();
        assert_eq!(n, 3, "must delete exactly the 3 versions with opnum < 4");
        // Post-GC: snap=4 still finds v4 (at-watermark serveable).
        match get_at_snapshot(&store, 7, &oid, 4) {
            SnapshotRead::Found(b) => assert_eq!(b, b"v4", "v4 must survive at the watermark"),
            o => panic!("snap=4 expected Found(v4), got {o:?}"),
        }
        // Post-GC: snap=7 still finds v7 (newest preserved).
        match get_at_snapshot(&store, 7, &oid, 7) {
            SnapshotRead::Found(b) => assert_eq!(b, b"v7", "v7 must survive (newest)"),
            o => panic!("snap=7 expected Found(v7), got {o:?}"),
        }
    }

    /// KAT-8 (plan): version at EXACTLY low_water_mark is preserved
    /// (strict-less-than semantics — Decision 3).
    /// Claim:    A version at commit_opnum == low_water_mark is NOT
    ///           reclaimed; only commit_opnum < low_water_mark goes.
    /// Workload: Write a single version at opnum=5. Call
    ///           delete_versions_older_than(low_water_mark = 5).
    /// Expected: Ok(0) (nothing to delete; the at-watermark version
    ///           is preserved). Subsequent get_at_snapshot(snap=5)
    ///           still Found("v5"); subsequent get_at_snapshot(snap=4)
    ///           is NotYetWritten (no version <= 4 exists).
    #[test]
    fn kat_delete_versions_older_than_preserves_at_watermark() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let oid = obj(51);
        put_versioned(&mut store, 7, &oid, 5, Some(b"v5".to_vec())).unwrap();
        let n = delete_versions_older_than(&mut store, 5).unwrap();
        assert_eq!(n, 0, "at-watermark version MUST be preserved (strict <)");
        match get_at_snapshot(&store, 7, &oid, 5) {
            SnapshotRead::Found(b) => assert_eq!(b, b"v5", "snap=5 must still Found(v5)"),
            o => panic!("snap=5 expected Found(v5) — at-watermark serveable, got {o:?}"),
        }
        assert_eq!(
            get_at_snapshot(&store, 7, &oid, 4),
            SnapshotRead::NotYetWritten,
            "snap=4 must be NotYetWritten (no version <= 4 exists)",
        );
    }

    /// KAT-10 (plan): Storage::low_water_mark accessor symmetry.
    /// Claim:    A newly-opened Storage has low_water_mark() == 0
    ///           (the SP114 T1 default). After set_low_water_mark(W),
    ///           low_water_mark() returns W. The accessor is a
    ///           transparent pass-through (no normalisation, no
    ///           validation — those are the SM apply arm's job).
    /// Workload: Open Storage. Read low_water_mark() (expect 0).
    ///           set_low_water_mark(42); re-read (expect 42).
    ///           set_low_water_mark(0); re-read (expect 0 — accessor
    ///           does NOT enforce monotonicity at this layer).
    /// Expected: 0, then 42, then 0.
    #[test]
    fn kat_storage_low_water_mark_accessor() {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        assert_eq!(store.low_water_mark(), 0, "fresh Storage default == 0");
        store.set_low_water_mark(42);
        assert_eq!(store.low_water_mark(), 42, "set then read symmetry");
        store.set_low_water_mark(0);
        assert_eq!(
            store.low_water_mark(),
            0,
            "accessor does NOT validate monotonicity (SM apply arm's job)",
        );
    }
}
