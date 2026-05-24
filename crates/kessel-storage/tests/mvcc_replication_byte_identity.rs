//! SP110 T3 — 3-replica byte-identity integration tests for the MVCC layer.
//!
//! THESIS-FIT CHECK: the verifiable-behavior pillar's headline claim is that
//! "same log prefix → byte-identical MVCC state on every replica." These tests
//! lock that claim at the binary level: three independent `Storage<MemVfs>`
//! instances each apply an identical sequence of `put_versioned` calls and must
//! produce:
//!   1. Byte-identical `get_at_snapshot` results for every (key, snapshot) pair.
//!   2. Byte-identical `has_version_in_range` results for every range tested.
//!   3. Byte-identical `dump_all_versions` maps (all stored versioned keys +
//!      their values enumerated directly out of `scan_range_versions`).
//!
//! A fourth test covers the *lagging-replica* scenario: a replica that has only
//! applied the first K of N ops must agree with fully-applied replicas at every
//! snapshot ≤ opnum(K) — bounded-staleness linearizability per Decision 2.
//!
//! KAT derivation philosophy: expected byte sequences and `SnapshotRead` values
//! are hand-derived from the log sequence + the key encoding recipe in
//! `mvcc::make_versioned_key`. They are NOT derived by running one replica and
//! checking another against it (that would be a tautology, not a KAT).
//!
//! Decision 2 (log opnum as timestamp; no wall-clock):
//!   commit_opnum is the SM opnum at apply time; a snapshot is just an opnum.
//! Decision 3 (key encoding):
//!   28-byte physical key = type_id(4 LE) ++ object_id(16) ++ inverted_opnum(8 BE)
//!   inverted_opnum = u64::MAX - commit_opnum → newest-first lex order.

#![forbid(unsafe_code)]

use kessel_io::MemVfs;
use kessel_storage::{
    mvcc::{
        decode_commit_opnum, get_at_snapshot, has_version_in_range, make_versioned_key,
        put_versioned, SnapshotRead, PREFIX_LEN, VERSIONED_KEY_LEN,
    },
    Storage,
};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Helper: build a 16-byte object_id from a single u32 discriminant.
//
// Same recipe as the plan's spec; the discriminant is packed into the last
// 4 bytes (big-endian) so obj(1) != obj(2) etc. in a clearly visible way.
// ---------------------------------------------------------------------------
fn obj(n: u32) -> [u8; 16] {
    let mut a = [0u8; 16];
    a[12..16].copy_from_slice(&n.to_be_bytes());
    a
}

// ---------------------------------------------------------------------------
// Helper: dump_all_versions
//
// Enumerates EVERY versioned key present in the store (across ALL logical
// keys) by performing a full scan over the versioned-key space:
//   lo = [0x00 * VERSIONED_KEY_LEN]   (smallest possible 28-byte key)
//   hi = [0xFF * VERSIONED_KEY_LEN]   (largest possible 28-byte key)
//
// Returns a `BTreeMap<Vec<u8>, Option<Vec<u8>>>` mapping each physical
// 28-byte versioned key to its stored value (None = tombstone).
//
// IMPORTANT: this helper scans the raw LSM, not the MVCC API — it exposes
// the actual stored bytes so replicas can be compared at the physical level,
// not just through the semantic read API. Byte-identical maps mean
// byte-identical LSM state.
// ---------------------------------------------------------------------------
fn dump_all_versions<V: kessel_io::Vfs>(
    store: &Storage<V>,
) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
    let lo = vec![0x00u8; VERSIONED_KEY_LEN];
    let hi = vec![0xFFu8; VERSIONED_KEY_LEN];
    store
        .scan_range_versions(&lo, &hi)
        .into_iter()
        // Only keep entries whose key is exactly VERSIONED_KEY_LEN bytes —
        // legacy 20-byte keys live below the versioned-key space and should
        // not appear here (none are written in these tests), but guard anyway.
        .filter(|(k, _)| k.len() == VERSIONED_KEY_LEN)
        .collect()
}

// ---------------------------------------------------------------------------
// KAT derivation reference (hand-calculated):
//
// make_versioned_key(type_id=7, object_id=obj(1), commit_opnum=10):
//   type_id  = 7 → LE4 = [0x07, 0x00, 0x00, 0x00]
//   object_id = obj(1): bytes 0..12 = 0x00, bytes 12..16 = [0,0,0,1] (BE u32)
//             = [00,00,00,00, 00,00,00,00, 00,00,00,00, 00,00,00,01]
//   inverted(10) = u64::MAX - 10 = 0xFFFFFFFFFFFFFFF5 → BE8
//             = [0xFF,0xFF,0xFF,0xFF, 0xFF,0xFF,0xFF,0xF5]
//   full key  = [07,00,00,00, 00,00,00,00, 00,00,00,00, 00,00,00,00, 00,00,00,01,
//                FF,FF,FF,FF, FF,FF,FF,F5]   (28 bytes)
//
// make_versioned_key(type_id=7, object_id=obj(2), commit_opnum=11):
//   object_id = obj(2): last 4 bytes = [0,0,0,2]
//   inverted(11) = u64::MAX - 11 = 0xFFFFFFFFFFFFFFF4 → BE8 last byte 0xF4
//   suffix    = [0xFF,0xFF,0xFF,0xFF, 0xFF,0xFF,0xFF,0xF4]
//
// make_versioned_key(type_id=7, object_id=obj(2), commit_opnum=30) [tombstone]:
//   inverted(30) = u64::MAX - 30 = 0xFFFFFFFFFFFFFFE1 → last byte 0xE1
//   suffix    = [0xFF,0xFF,0xFF,0xFF, 0xFF,0xFF,0xFF,0xE1]
//   value     = None (tombstone)
// ---------------------------------------------------------------------------

// ===========================================================================
// Test 1: Three replicas × same 8-op log prefix → byte-identical version chains
//         and byte-identical snapshot reads over every key × snapshot pair.
//
// Scenario: 3 keys (obj(1), obj(2), obj(3)), 8 ops interleaved, one tombstone.
// Snapshot range checked: 0..=70 for every key + a key never written (obj(99)).
//
// This is NOT a tautology: the assertion is `assert_eq!(replica_A, replica_B)`
// AND `assert_eq!(replica_B, replica_C)`. If the key encoding, memtable merge,
// or scan_range_versions had any replica-local non-determinism (e.g., random
// HashMap ordering, wall-clock seeding) the three dumps would diverge and the
// test would fail. The `dump_all_versions` check goes further: it asserts the
// raw LSM physical bytes are identical, not just the semantic read results.
// ===========================================================================
#[test]
fn three_replicas_same_log_prefix_byte_identical_versions_and_reads() {
    // Build the deterministic log prefix: 8 ops, 3 logical keys.
    // Values are hand-chosen to be human-readable and test-distinct.
    let ops: Vec<(u32, [u8; 16], u64, Option<Vec<u8>>)> = vec![
        (7, obj(1), 10, Some(b"a-v10".to_vec())),
        (7, obj(2), 11, Some(b"b-v11".to_vec())),
        (7, obj(1), 20, Some(b"a-v20".to_vec())),
        (7, obj(3), 21, Some(b"c-v21".to_vec())),
        (7, obj(2), 30, None),                      // tombstone for obj(2)
        (7, obj(1), 40, Some(b"a-v40".to_vec())),
        (7, obj(2), 50, Some(b"b-v50".to_vec())),
        (7, obj(3), 60, Some(b"c-v60".to_vec())),
    ];

    // Instantiate 3 independent Storage<MemVfs> replicas.
    let mut stores: [Storage<MemVfs>; 3] = [
        Storage::open(MemVfs::new()).unwrap(),
        Storage::open(MemVfs::new()).unwrap(),
        Storage::open(MemVfs::new()).unwrap(),
    ];

    // Replay the identical log prefix against every replica.
    for store in stores.iter_mut() {
        for (t, o, c, v) in &ops {
            put_versioned(store, *t, o, *c, v.clone()).unwrap();
        }
    }

    // -----------------------------------------------------------------------
    // Physical-level assertion: dump_all_versions must be byte-identical
    // across all three replicas. This checks the LSM raw byte state, not
    // just the semantic read results.
    // -----------------------------------------------------------------------
    let dump0 = dump_all_versions(&stores[0]);
    let dump1 = dump_all_versions(&stores[1]);
    let dump2 = dump_all_versions(&stores[2]);

    assert_eq!(
        dump0, dump1,
        "replica 0 and 1 LSM dumps diverge (byte-identical physical state violated)"
    );
    assert_eq!(
        dump1, dump2,
        "replica 1 and 2 LSM dumps diverge (byte-identical physical state violated)"
    );

    // The dump must contain exactly 8 versioned entries (one per op).
    assert_eq!(
        dump0.len(),
        8,
        "expected exactly 8 versioned LSM entries, got {}",
        dump0.len()
    );

    // -----------------------------------------------------------------------
    // KAT spot-checks on the raw physical keys in the dump.
    //
    // KAT 1: make_versioned_key(7, obj(1), 10)
    //   type_id=7 LE = [07,00,00,00]
    //   obj(1)  12 zeros ++ [00,00,00,01]
    //   inverted(10) = 0xFFFFFFFFFFFFFFF5 BE = [FF,FF,FF,FF,FF,FF,FF,F5]
    // -----------------------------------------------------------------------
    let kat_key_obj1_op10 = make_versioned_key(7, &obj(1), 10);
    assert_eq!(
        &kat_key_obj1_op10[0..4],
        &[0x07u8, 0x00, 0x00, 0x00],
        "type_id bytes mismatch for obj(1) op10"
    );
    assert_eq!(
        &kat_key_obj1_op10[12..20],
        &[0x00u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01],
        "object_id trailing bytes mismatch for obj(1)"
    );
    assert_eq!(
        &kat_key_obj1_op10[20..28],
        &[0xFFu8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xF5],
        "inverted opnum bytes for commit_opnum=10 must be [FF..F5]"
    );
    assert!(
        dump0.contains_key(&kat_key_obj1_op10),
        "dump must contain KAT key for obj(1) op10"
    );
    assert_eq!(
        dump0[&kat_key_obj1_op10],
        Some(b"a-v10".to_vec()),
        "KAT value for obj(1) op10 must be b\"a-v10\""
    );

    // KAT 2: tombstone key for obj(2) at opnum=30
    //   inverted(30) = u64::MAX - 30 = 0xFFFFFFFFFFFFFFE1 BE last byte 0xE1
    let kat_key_obj2_tomb = make_versioned_key(7, &obj(2), 30);
    assert_eq!(
        &kat_key_obj2_tomb[20..28],
        &[0xFFu8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xE1],
        "inverted opnum bytes for commit_opnum=30 must be [FF..E1]"
    );
    assert!(
        dump0.contains_key(&kat_key_obj2_tomb),
        "dump must contain tombstone key for obj(2) op30"
    );
    assert_eq!(
        dump0[&kat_key_obj2_tomb],
        None,
        "tombstone entry for obj(2) op30 must have value=None"
    );

    // -----------------------------------------------------------------------
    // Semantic assertion: get_at_snapshot must be byte-identical across
    // all 3 replicas for every (key, snapshot) pair in 0..=70.
    // -----------------------------------------------------------------------
    let test_keys: &[(u32, [u8; 16])] = &[
        (7, obj(1)),
        (7, obj(2)),
        (7, obj(3)),
        (7, obj(99)), // never written — must be NotYetWritten across all replicas
    ];

    for snap in 0u64..=70 {
        for (t, o) in test_keys {
            let r0 = get_at_snapshot(&stores[0], *t, o, snap);
            let r1 = get_at_snapshot(&stores[1], *t, o, snap);
            let r2 = get_at_snapshot(&stores[2], *t, o, snap);
            assert_eq!(
                r0, r1,
                "replicas 0 vs 1 diverge at snap={snap} type_id={t} oid={o:?}"
            );
            assert_eq!(
                r1, r2,
                "replicas 1 vs 2 diverge at snap={snap} type_id={t} oid={o:?}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // KAT spot-checks on specific (snap, key) pairs — hand-derived expected
    // values. These are absolute assertions against known-correct results,
    // not relative cross-replica comparisons.
    // -----------------------------------------------------------------------

    // obj(1): writes at 10, 20, 40.
    //   snap=9  → NotYetWritten (nothing ≤ 9)
    //   snap=10 → Found("a-v10")  (newest ≤ 10 is opnum 10)
    //   snap=19 → Found("a-v10")  (newest ≤ 19 is opnum 10)
    //   snap=20 → Found("a-v20")  (newest ≤ 20 is opnum 20)
    //   snap=39 → Found("a-v20")  (newest ≤ 39 is opnum 20)
    //   snap=40 → Found("a-v40")  (newest ≤ 40 is opnum 40)
    //   snap=70 → Found("a-v40")  (newest ≤ 70 is opnum 40)
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(1), 9),
        SnapshotRead::NotYetWritten,
        "KAT: obj(1) snap=9 must be NotYetWritten"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(1), 10),
        SnapshotRead::Found(b"a-v10".to_vec()),
        "KAT: obj(1) snap=10 must be Found(a-v10)"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(1), 19),
        SnapshotRead::Found(b"a-v10".to_vec()),
        "KAT: obj(1) snap=19 must be Found(a-v10)"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(1), 20),
        SnapshotRead::Found(b"a-v20".to_vec()),
        "KAT: obj(1) snap=20 must be Found(a-v20)"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(1), 39),
        SnapshotRead::Found(b"a-v20".to_vec()),
        "KAT: obj(1) snap=39 must be Found(a-v20)"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(1), 40),
        SnapshotRead::Found(b"a-v40".to_vec()),
        "KAT: obj(1) snap=40 must be Found(a-v40)"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(1), 70),
        SnapshotRead::Found(b"a-v40".to_vec()),
        "KAT: obj(1) snap=70 must be Found(a-v40)"
    );

    // obj(2): write at 11 → tombstone at 30 → write at 50.
    //   snap=10 → NotYetWritten (nothing ≤ 10)
    //   snap=11 → Found("b-v11")
    //   snap=29 → Found("b-v11") (newest ≤ 29 is opnum 11)
    //   snap=30 → Tombstoned     (newest ≤ 30 is opnum 30 tombstone)
    //   snap=49 → Tombstoned     (newest ≤ 49 is opnum 30 tombstone)
    //   snap=50 → Found("b-v50") (newest ≤ 50 is opnum 50)
    //   snap=70 → Found("b-v50")
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(2), 10),
        SnapshotRead::NotYetWritten,
        "KAT: obj(2) snap=10 must be NotYetWritten"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(2), 11),
        SnapshotRead::Found(b"b-v11".to_vec()),
        "KAT: obj(2) snap=11 must be Found(b-v11)"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(2), 29),
        SnapshotRead::Found(b"b-v11".to_vec()),
        "KAT: obj(2) snap=29 must be Found(b-v11)"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(2), 30),
        SnapshotRead::Tombstoned,
        "KAT: obj(2) snap=30 must be Tombstoned"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(2), 49),
        SnapshotRead::Tombstoned,
        "KAT: obj(2) snap=49 must be Tombstoned"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(2), 50),
        SnapshotRead::Found(b"b-v50".to_vec()),
        "KAT: obj(2) snap=50 must be Found(b-v50)"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(2), 70),
        SnapshotRead::Found(b"b-v50".to_vec()),
        "KAT: obj(2) snap=70 must be Found(b-v50)"
    );

    // obj(3): write at 21 → write at 60.
    //   snap=20 → NotYetWritten
    //   snap=21 → Found("c-v21")
    //   snap=59 → Found("c-v21")
    //   snap=60 → Found("c-v60")
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(3), 20),
        SnapshotRead::NotYetWritten,
        "KAT: obj(3) snap=20 must be NotYetWritten"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(3), 21),
        SnapshotRead::Found(b"c-v21".to_vec()),
        "KAT: obj(3) snap=21 must be Found(c-v21)"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(3), 59),
        SnapshotRead::Found(b"c-v21".to_vec()),
        "KAT: obj(3) snap=59 must be Found(c-v21)"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(3), 60),
        SnapshotRead::Found(b"c-v60".to_vec()),
        "KAT: obj(3) snap=60 must be Found(c-v60)"
    );

    // obj(99): never written — always NotYetWritten regardless of snapshot.
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(99), 0),
        SnapshotRead::NotYetWritten,
        "KAT: obj(99) snap=0 must be NotYetWritten"
    );
    assert_eq!(
        get_at_snapshot(&stores[0], 7, &obj(99), 70),
        SnapshotRead::NotYetWritten,
        "KAT: obj(99) snap=70 must be NotYetWritten"
    );

    // -----------------------------------------------------------------------
    // has_version_in_range byte-identity across all 3 replicas.
    // All three replicas must agree on every range query result.
    // -----------------------------------------------------------------------
    let range_queries: &[(u32, [u8; 16], u64, u64)] = &[
        // obj(1): versions at 10, 20, 40.
        (7, obj(1), 0, 9),   // (0,9]   → false  (nothing ≤ 9 and > 0)
        (7, obj(1), 9, 10),  // (9,10]  → true   (opnum 10 is in window)
        (7, obj(1), 10, 20), // (10,20] → true   (opnum 20)
        (7, obj(1), 20, 39), // (20,39] → false  (nothing in (20,39])
        (7, obj(1), 39, 40), // (39,40] → true   (opnum 40)
        (7, obj(1), 40, 70), // (40,70] → false  (nothing after 40)
        // obj(2): versions at 11, 30 (tombstone), 50.
        (7, obj(2), 0, 10),  // (0,10]  → false
        (7, obj(2), 10, 11), // (10,11] → true   (opnum 11)
        (7, obj(2), 11, 29), // (11,29] → false
        (7, obj(2), 29, 30), // (29,30] → true   (opnum 30 tombstone)
        (7, obj(2), 30, 49), // (30,49] → false
        (7, obj(2), 49, 50), // (49,50] → true   (opnum 50)
        // obj(99): never written.
        (7, obj(99), 0, 70), // always false
    ];

    for &(t, ref o, lo_ex, hi_in) in range_queries {
        let v0 = has_version_in_range(&stores[0], t, o, lo_ex, hi_in);
        let v1 = has_version_in_range(&stores[1], t, o, lo_ex, hi_in);
        let v2 = has_version_in_range(&stores[2], t, o, lo_ex, hi_in);
        assert_eq!(
            v0, v1,
            "has_version_in_range diverges R0 vs R1: type={t} lo={lo_ex} hi={hi_in}"
        );
        assert_eq!(
            v1, v2,
            "has_version_in_range diverges R1 vs R2: type={t} lo={lo_ex} hi={hi_in}"
        );
    }

    // KAT absolute checks on has_version_in_range (hand-derived):
    // obj(1) at window (9,10] = true; (20,39] = false; (10,20] = true.
    assert!(
        has_version_in_range(&stores[0], 7, &obj(1), 9, 10),
        "KAT: obj(1) (9,10] must be true (opnum 10 in window)"
    );
    assert!(
        !has_version_in_range(&stores[0], 7, &obj(1), 20, 39),
        "KAT: obj(1) (20,39] must be false (no opnum in window)"
    );
    assert!(
        has_version_in_range(&stores[0], 7, &obj(2), 29, 30),
        "KAT: obj(2) (29,30] must be true (tombstone at opnum 30)"
    );
}

// ===========================================================================
// Test 2: Tombstone-then-rewrite produces byte-identical state across 3 replicas.
//
// Distinct from Test 1's multi-key scenario: this focuses on a single key
// undergoing write → delete → re-write, ensuring the tombstone + subsequent
// live version coexist correctly in the LSM and are read identically by all
// replicas.
//
// KAT derivation:
//   opnum=5  → Found("first")   at snap ≥ 5
//   opnum=10 → Tombstoned       at snap ∈ [10, 14]
//   opnum=15 → Found("reborn")  at snap ≥ 15
//   snap=4   → NotYetWritten
//   dump contains 3 entries (3 LSM records for 3 opnums)
// ===========================================================================
#[test]
fn three_replicas_tombstone_then_rewrite_byte_identical() {
    let ops: Vec<(u32, [u8; 16], u64, Option<Vec<u8>>)> = vec![
        (7, obj(10), 5, Some(b"first".to_vec())),
        (7, obj(10), 10, None),                       // tombstone
        (7, obj(10), 15, Some(b"reborn".to_vec())),
    ];

    let mut stores: [Storage<MemVfs>; 3] = [
        Storage::open(MemVfs::new()).unwrap(),
        Storage::open(MemVfs::new()).unwrap(),
        Storage::open(MemVfs::new()).unwrap(),
    ];

    for store in stores.iter_mut() {
        for (t, o, c, v) in &ops {
            put_versioned(store, *t, o, *c, v.clone()).unwrap();
        }
    }

    // Physical-level: all 3 replica dumps are byte-identical.
    let dump0 = dump_all_versions(&stores[0]);
    let dump1 = dump_all_versions(&stores[1]);
    let dump2 = dump_all_versions(&stores[2]);
    assert_eq!(dump0, dump1, "Test2: replica 0 vs 1 LSM dump diverges");
    assert_eq!(dump1, dump2, "Test2: replica 1 vs 2 LSM dump diverges");
    assert_eq!(dump0.len(), 3, "Test2: expected exactly 3 LSM entries");

    // KAT absolute checks (hand-derived), verified against all 3 replicas.
    let expected: &[(u64, SnapshotRead)] = &[
        (4, SnapshotRead::NotYetWritten),
        (5, SnapshotRead::Found(b"first".to_vec())),
        (9, SnapshotRead::Found(b"first".to_vec())),
        (10, SnapshotRead::Tombstoned),
        (14, SnapshotRead::Tombstoned),
        (15, SnapshotRead::Found(b"reborn".to_vec())),
        (100, SnapshotRead::Found(b"reborn".to_vec())),
    ];

    for &(snap, ref want) in expected {
        for (idx, store) in stores.iter().enumerate() {
            let got = get_at_snapshot(store, 7, &obj(10), snap);
            assert_eq!(
                &got, want,
                "Test2 KAT failed: replica={idx} snap={snap} expected={want:?} got={got:?}"
            );
        }
    }
}

// ===========================================================================
// Test 3: Independent (non-conflicting) keys applied in log order — all 3
//         replicas converge regardless of which key was written "first" in
//         wall-clock terms (irrelevant: the log dictates order).
//
// Three distinct keys at distinct opnums; the log order is the canonical order.
// Each replica applies them in the same log sequence and must produce identical
// results. This test emphasizes that there is NO wall-clock influence on the
// versioned key encoding.
//
// KAT derivation:
//   obj(20) at op 100 → Found("key20-v100") at snap ≥ 100
//   obj(21) at op 200 → Found("key21-v200") at snap ≥ 200
//   obj(22) at op 300 → Found("key22-v300") at snap ≥ 300
//   Inverted opnum suffixes:
//     inverted(100) = 0xFFFFFFFFFFFFFF9B → last byte 0x9B
//     inverted(200) = 0xFFFFFFFFFFFFFF37 → last byte 0x37
//     inverted(300) = 0xFFFFFFFFFFFFFED3 → last byte 0xD3
//   Cross-key isolation: obj(20) snap=50 → NotYetWritten (not ≤ 100 yet)
// ===========================================================================
#[test]
fn three_replicas_independent_keys_log_order_byte_identical() {
    let ops: Vec<(u32, [u8; 16], u64, Option<Vec<u8>>)> = vec![
        (7, obj(20), 100, Some(b"key20-v100".to_vec())),
        (7, obj(21), 200, Some(b"key21-v200".to_vec())),
        (7, obj(22), 300, Some(b"key22-v300".to_vec())),
    ];

    let mut stores: [Storage<MemVfs>; 3] = [
        Storage::open(MemVfs::new()).unwrap(),
        Storage::open(MemVfs::new()).unwrap(),
        Storage::open(MemVfs::new()).unwrap(),
    ];

    for store in stores.iter_mut() {
        for (t, o, c, v) in &ops {
            put_versioned(store, *t, o, *c, v.clone()).unwrap();
        }
    }

    // Physical dump byte-identity.
    let dump0 = dump_all_versions(&stores[0]);
    let dump1 = dump_all_versions(&stores[1]);
    let dump2 = dump_all_versions(&stores[2]);
    assert_eq!(dump0, dump1, "Test3: replica 0 vs 1 dump diverges");
    assert_eq!(dump1, dump2, "Test3: replica 1 vs 2 dump diverges");
    assert_eq!(dump0.len(), 3, "Test3: expected 3 versioned entries");

    // KAT: inverted opnum byte check for obj(20) at opnum=100.
    //   inverted(100) = u64::MAX - 100 = 0xFFFFFFFFFFFFFF9B
    //   last BE byte = 0x9B
    let k20_100 = make_versioned_key(7, &obj(20), 100);
    assert_eq!(
        k20_100[27],
        0x9Bu8,
        "KAT: inverted(100) last byte must be 0x9B"
    );
    assert!(dump0.contains_key(&k20_100), "Test3: KAT key for obj(20) op100 must be in dump");

    // KAT: inverted opnum for obj(21) at 200 → last byte 0x37.
    let k21_200 = make_versioned_key(7, &obj(21), 200);
    assert_eq!(
        k21_200[27],
        0x37u8,
        "KAT: inverted(200) last byte must be 0x37"
    );

    // KAT: inverted opnum for obj(22) at 300 → last byte 0xD3.
    //   inverted(300) = u64::MAX - 300 = 0xFFFFFFFFFFFFFED3
    let k22_300 = make_versioned_key(7, &obj(22), 300);
    assert_eq!(
        k22_300[27],
        0xD3u8,
        "KAT: inverted(300) last byte must be 0xD3"
    );

    // Semantic cross-replica checks.
    let checks: &[(u32, [u8; 16], u64, SnapshotRead)] = &[
        (7, obj(20), 99, SnapshotRead::NotYetWritten),
        (7, obj(20), 100, SnapshotRead::Found(b"key20-v100".to_vec())),
        (7, obj(21), 199, SnapshotRead::NotYetWritten),
        (7, obj(21), 200, SnapshotRead::Found(b"key21-v200".to_vec())),
        (7, obj(22), 299, SnapshotRead::NotYetWritten),
        (7, obj(22), 300, SnapshotRead::Found(b"key22-v300".to_vec())),
        // Cross-key isolation: obj(20) must not be visible at snap=50
        (7, obj(20), 50, SnapshotRead::NotYetWritten),
    ];

    for (t, o, snap, want) in checks {
        for (idx, store) in stores.iter().enumerate() {
            let got = get_at_snapshot(store, *t, o, *snap);
            assert_eq!(
                got, *want,
                "Test3: replica={idx} key=obj({t}) snap={snap} expected={want:?} got={got:?}",
                t = if *o == obj(20) { 20u32 } else if *o == obj(21) { 21 } else { 22 }
            );
        }
    }
}

// ===========================================================================
// Test 4: Lagging-replica prefix-consistency (bounded-staleness linearizability).
//
// "leader" applies all N ops; "lagger" applies only the first K ops.
// At every snapshot ≤ opnum(last-lagger-op), the lagger and leader must return
// byte-identical results — the lagger is not divergent, just bounded-stale.
//
// Scenario: 3 ops for obj(1) at opnums 10, 20, 30. Lagger applies only 2.
//   lagger applied through opnum 20; leader applied through opnum 30.
//   At snap ≤ 20: lagger and leader agree exactly.
//   At snap > 20: lagger returns the result of its prefix (snap 20 or prior
//   version) — this is the SI permission to lag; we document it but do NOT
//   assert equality there (that would be wrong — the lagger hasn't applied op 30).
//
// Additional scenario: lagger applied only 1 op (through opnum 10).
//   At snap ≤ 10: agreement with fully-applied leader.
//   At snap=11..20: lagger sees snap=10 version ("a-v10"); leader sees "a-v20"
//     — documented divergence, NOT asserted as equal.
//
// This test locks Decision 2's "bounded-staleness linearizable reads" claim:
// the lagger's state is a valid consistent prefix of the leader's state.
// ===========================================================================
#[test]
fn lagging_replica_prefix_consistency_agrees_at_and_below_applied_prefix() {
    let all_ops: Vec<(u32, [u8; 16], u64, Option<Vec<u8>>)> = vec![
        (7, obj(30), 10, Some(b"a-v10".to_vec())),
        (7, obj(30), 20, Some(b"a-v20".to_vec())),
        (7, obj(30), 30, Some(b"a-v30".to_vec())),
    ];

    // Leader applies all 3 ops.
    let mut leader = Storage::open(MemVfs::new()).unwrap();
    for (t, o, c, v) in &all_ops {
        put_versioned(&mut leader, *t, o, *c, v.clone()).unwrap();
    }

    // Lagger-2: applies first 2 ops (through opnum 20).
    let mut lagger_k2 = Storage::open(MemVfs::new()).unwrap();
    for (t, o, c, v) in all_ops.iter().take(2) {
        put_versioned(&mut lagger_k2, *t, o, *c, v.clone()).unwrap();
    }

    // Lagger-1: applies only first 1 op (through opnum 10).
    let mut lagger_k1 = Storage::open(MemVfs::new()).unwrap();
    for (t, o, c, v) in all_ops.iter().take(1) {
        put_versioned(&mut lagger_k1, *t, o, *c, v.clone()).unwrap();
    }

    // -----------------------------------------------------------------------
    // Assertion group 1: lagger_k2 agrees with leader at snap ≤ 20.
    //
    // KAT: at snap=0..=9 → NotYetWritten on both (opnum 10 not yet visible)
    //       at snap=10..=19 → Found("a-v10") on both
    //       at snap=20 → Found("a-v20") on both
    //
    // These are absolute KAT values derived from the log sequence, not from
    // one replica's output being mirrored to the other.
    // -----------------------------------------------------------------------
    let prefix_2_checks: &[(u64, SnapshotRead)] = &[
        (0, SnapshotRead::NotYetWritten),
        (9, SnapshotRead::NotYetWritten),
        (10, SnapshotRead::Found(b"a-v10".to_vec())),
        (15, SnapshotRead::Found(b"a-v10".to_vec())),
        (19, SnapshotRead::Found(b"a-v10".to_vec())),
        (20, SnapshotRead::Found(b"a-v20".to_vec())),
    ];

    for (snap, want) in prefix_2_checks {
        let got_leader = get_at_snapshot(&leader, 7, &obj(30), *snap);
        let got_lagger = get_at_snapshot(&lagger_k2, 7, &obj(30), *snap);

        // KAT assertion: both must equal the known-correct expected value.
        assert_eq!(
            got_leader, *want,
            "Test4 KAT leader: snap={snap} expected={want:?} got={got_leader:?}"
        );
        assert_eq!(
            got_lagger, *want,
            "Test4 KAT lagger_k2: snap={snap} expected={want:?} got={got_lagger:?}"
        );

        // Cross-replica agreement at snap ≤ applied prefix (20).
        assert_eq!(
            got_leader, got_lagger,
            "Test4: lagger_k2 diverges from leader at snap={snap} (within applied prefix)"
        );
    }

    // -----------------------------------------------------------------------
    // Assertion group 2: lagger_k1 agrees with leader at snap ≤ 10.
    //
    // KAT: snap=0..=9 → NotYetWritten; snap=10 → Found("a-v10")
    // -----------------------------------------------------------------------
    let prefix_1_checks: &[(u64, SnapshotRead)] = &[
        (0, SnapshotRead::NotYetWritten),
        (9, SnapshotRead::NotYetWritten),
        (10, SnapshotRead::Found(b"a-v10".to_vec())),
    ];

    for (snap, want) in prefix_1_checks {
        let got_leader = get_at_snapshot(&leader, 7, &obj(30), *snap);
        let got_lagger = get_at_snapshot(&lagger_k1, 7, &obj(30), *snap);

        assert_eq!(
            got_leader, *want,
            "Test4 KAT leader (group2): snap={snap} expected={want:?}"
        );
        assert_eq!(
            got_lagger, *want,
            "Test4 KAT lagger_k1 (group2): snap={snap} expected={want:?}"
        );
        assert_eq!(
            got_leader, got_lagger,
            "Test4: lagger_k1 diverges from leader at snap={snap} (within applied prefix)"
        );
    }

    // -----------------------------------------------------------------------
    // Document (not assert) the permitted lag: at snap=21..30 the lagger_k2
    // legitimately returns the "a-v20" version (opnum 20 is the newest it
    // has), while the leader returns "a-v20" at snap 21..29 and "a-v30" at
    // snap=30. Both are internally consistent with their respective prefixes.
    // The SI model (Decision 2) permits this bounded-staleness gap; it is
    // NOT a divergence — the lagger is a valid consistent snapshot at its
    // applied prefix boundary.
    // -----------------------------------------------------------------------

    // Verify the leader has the final version at snap=30 (sanity check).
    assert_eq!(
        get_at_snapshot(&leader, 7, &obj(30), 30),
        SnapshotRead::Found(b"a-v30".to_vec()),
        "KAT: leader must see a-v30 at snap=30"
    );

    // Verify lagger_k2 stops at opnum 20 (its applied prefix).
    assert_eq!(
        get_at_snapshot(&lagger_k2, 7, &obj(30), 30),
        SnapshotRead::Found(b"a-v20".to_vec()),
        "KAT: lagger_k2 at snap=30 must see a-v20 (its newest applied version)"
    );

    // -----------------------------------------------------------------------
    // Dump-level assertion: lagger_k2's physical LSM has exactly 2 entries;
    // leader has exactly 3. This confirms the prefix is truly partial, not
    // a coincidental same-value situation.
    // -----------------------------------------------------------------------
    let dump_leader = dump_all_versions(&leader);
    let dump_lagger2 = dump_all_versions(&lagger_k2);
    let dump_lagger1 = dump_all_versions(&lagger_k1);

    assert_eq!(
        dump_leader.len(),
        3,
        "Test4: leader must have 3 versioned entries"
    );
    assert_eq!(
        dump_lagger2.len(),
        2,
        "Test4: lagger_k2 must have exactly 2 versioned entries (applied prefix K=2)"
    );
    assert_eq!(
        dump_lagger1.len(),
        1,
        "Test4: lagger_k1 must have exactly 1 versioned entry (applied prefix K=1)"
    );

    // The lagger_k2's 2 entries must be a physical subset of the leader's 3.
    for (k, v) in &dump_lagger2 {
        assert_eq!(
            dump_leader.get(k),
            Some(v),
            "Test4: lagger_k2 entry not found in leader dump (divergence!)"
        );
    }
    // Same for lagger_k1.
    for (k, v) in &dump_lagger1 {
        assert_eq!(
            dump_leader.get(k),
            Some(v),
            "Test4: lagger_k1 entry not found in leader dump (divergence!)"
        );
    }
}

// ===========================================================================
// Test 5: decode_commit_opnum round-trip on all 8 versioned keys from Test 1.
//
// Verifies that the key encoding is losslessly invertible for every opnum in
// the log sequence — a KAT for the physical key format guaranteeing the
// encoding is deterministic in both directions.
// ===========================================================================
#[test]
fn all_log_opnums_round_trip_through_key_encoding() {
    // All opnums from Test 1's log prefix.
    let cases: &[(u32, [u8; 16], u64)] = &[
        (7, obj(1), 10),
        (7, obj(2), 11),
        (7, obj(1), 20),
        (7, obj(3), 21),
        (7, obj(2), 30),
        (7, obj(1), 40),
        (7, obj(2), 50),
        (7, obj(3), 60),
    ];

    for &(t, ref o, c) in cases {
        let k = make_versioned_key(t, o, c);
        assert_eq!(
            k.len(),
            VERSIONED_KEY_LEN,
            "KAT: key for opnum={c} must be {VERSIONED_KEY_LEN} bytes"
        );
        let decoded = decode_commit_opnum(&k).expect("decode must not fail on valid key");
        assert_eq!(
            decoded, c,
            "KAT: decoded opnum={decoded} must equal original={c}"
        );
        // The prefix bytes must carry the correct type_id and object_id.
        let prefix_bytes: [u8; PREFIX_LEN] = k[..PREFIX_LEN]
            .try_into()
            .expect("PREFIX_LEN bytes from key");
        let expected_prefix = {
            let mut p = Vec::with_capacity(PREFIX_LEN);
            p.extend_from_slice(&t.to_le_bytes());
            p.extend_from_slice(o);
            p
        };
        assert_eq!(
            &prefix_bytes,
            expected_prefix.as_slice(),
            "KAT: prefix bytes must match type_id+object_id for opnum={c}"
        );
    }
}
