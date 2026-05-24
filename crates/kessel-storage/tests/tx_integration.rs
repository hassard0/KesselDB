//! Integration tests for kessel-storage::tx — exercises the Tx layer at
//! the public-API level (no `super::` access). Proves the S2.2 contract:
//!
//!   IT-1: Snapshot pin survives concurrent writes. A Tx pinned at
//!         snapshot=10 sees only versions with commit_opnum ≤ 10, even
//!         when the Store later holds versions at opnum=20. Demonstrated
//!         via two Tx lifetimes (the borrow-checker prevents holding &Tx
//!         while writing &mut store, so a second Tx at the old snapshot
//!         is used to prove the pin).
//!
//!   IT-2: Multi-Tx at the same snapshot produce byte-identical read
//!         results AND byte-identical read_set contents (the thesis-fit
//!         replayable pillar at the Tx layer).
//!
//!   IT-3: Read-set size grows monotonically as distinct keys are read;
//!         re-reading a key already in the set does NOT increment size
//!         (BTreeSet set-semantics), but reading a new key does.
//!
//!   IT-4: Tombstone is observable in the read_set, and two Tx at
//!         different snapshots can have identical read_set key-sets
//!         while returning different SnapshotRead outcomes — demonstrating
//!         that read_set is not tied to the read outcome.
//!
//! KAT derivation philosophy: every expected value is hand-derived from
//! the log sequence + the Tx contract. No test derives its expectation
//! by running another Tx and comparing — that would be a tautology.
//!
//! A regression where Tx::read accidentally used the latest_opnum at
//! read-time instead of the pinned snapshot_opnum would cause IT-1 to
//! return Found([0xBB]) instead of Found([0xAA]), failing immediately.

#![forbid(unsafe_code)]

use kessel_io::MemVfs;
use kessel_storage::{
    mvcc::{put_versioned, SnapshotRead},
    tx::Tx,
    Storage,
};

// ---------------------------------------------------------------------------
// Helper: 16-byte object_id with `n` in the last byte.
// obj(1) != obj(2) etc., clearly distinguishable in assertion messages.
// ---------------------------------------------------------------------------
fn obj(n: u8) -> [u8; 16] {
    let mut a = [0u8; 16];
    a[15] = n;
    a
}

// ---------------------------------------------------------------------------
// IT-1: Snapshot pin survives concurrent writes.
//
// Scenario (hand-derived):
//   opnum=0:  put_versioned(type_id=1, obj(1), opnum=0, value=[0xAA])
//   opnum=20: put_versioned(type_id=1, obj(1), opnum=20, value=[0xBB])
//
//   Tx-A pinned at snapshot=10 (begins BEFORE opnum=20 is in the store):
//     read(1, obj(1)) → Found([0xAA])   ← snapshot≤10 version
//
//   Tx-B pinned at snapshot=10 (begins AFTER opnum=20 is in the store):
//     read(1, obj(1)) → Found([0xAA])   ← STILL snapshot≤10 version
//
// Tx-A proves the forward case: a future write arrived in the store while
// Tx-A would have been live. Tx-B proves the backward case: a Tx begun
// AFTER the future write still pins correctly at snapshot=10.
//
// Note: the borrow-checker prevents `&mut store` (put_versioned) while
// a Tx holds `&store`. We demonstrate the pin property by using two
// sequential Tx lifetimes around the writes. This is the correct
// composition: in production the SM serializes put_versioned calls and
// Tx::begin calls; the pin is the pinned opnum, not the wall-clock.
//
// Regression trap: if Tx::read accidentally calls
//   mvcc::get_at_snapshot(store, type_id, obj, latest_opnum_at_read_time)
// instead of the pinned snapshot_opnum, BOTH Tx-A and Tx-B would return
// Found([0xBB]) — this test catches that bug.
// ---------------------------------------------------------------------------
#[test]
fn integration_snapshot_pin_survives_concurrent_writes() {
    let mut store = Storage::open(MemVfs::new()).unwrap();

    // Write at opnum=0: the "before-snapshot" version.
    put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xAA])).unwrap();

    // --- Tx-A: pin at snapshot=10, read BEFORE the opnum=20 write exists ---
    {
        let mut tx_a = Tx::begin(&store, 10);
        // Hand-derived: snapshot=10 sees opnum=0 ([0xAA]). No opnum=20 yet.
        match tx_a.read(1, &obj(1)) {
            SnapshotRead::Found(v) => assert_eq!(
                v,
                vec![0xAA],
                "IT-1 Tx-A: snapshot=10 must yield [0xAA] (opnum=0), not any future version"
            ),
            other => panic!(
                "IT-1 Tx-A: expected Found([0xAA]) at snapshot=10; got {:?}",
                other
            ),
        }
        // Tx-A is dropped here, releasing the &store borrow.
    }

    // Write at opnum=20: the "after-snapshot" version. This is now in the
    // store. A buggy Tx at snapshot=10 would accidentally see this version.
    put_versioned(&mut store, 1, &obj(1), 20, Some(vec![0xBB])).unwrap();
    // Write at opnum=25: another post-snapshot version.
    put_versioned(&mut store, 1, &obj(1), 25, Some(vec![0xCC])).unwrap();

    // --- Tx-B: pin at snapshot=10, AFTER opnum=20 and opnum=25 exist ---
    // This is the regression-critical case: the store now holds opnum=0,
    // opnum=20, opnum=25 for (type_id=1, obj(1)). A Tx pinned at snapshot=10
    // must return Found([0xAA]) — the opnum=0 version — NOT [0xBB] or [0xCC].
    {
        let mut tx_b = Tx::begin(&store, 10);
        // Hand-derived: snapshot=10 sees only versions with commit_opnum ≤ 10.
        // opnum=0 ≤ 10 → visible; opnum=20 > 10 → invisible; opnum=25 > 10 → invisible.
        // get_at_snapshot scans newest-first; the first version with opnum ≤ 10 is opnum=0.
        // Expected: Found([0xAA]).
        match tx_b.read(1, &obj(1)) {
            SnapshotRead::Found(v) => assert_eq!(
                v,
                vec![0xAA],
                "IT-1 Tx-B: snapshot=10 must still yield [0xAA]; store has opnum=20,25 but they are > snapshot"
            ),
            other => panic!(
                "IT-1 Tx-B: expected Found([0xAA]) at snapshot=10 with opnum=20,25 in store; got {:?}",
                other
            ),
        }
        // Verify read_set recorded the key correctly.
        assert!(
            tx_b.read_set().contains(&(1u32, obj(1))),
            "IT-1 Tx-B: read_set must contain (1, obj(1))"
        );
        assert_eq!(tx_b.read_set().len(), 1, "IT-1 Tx-B: read_set must have exactly 1 entry");
        tx_b.abort();
    }

    // Sanity check: a Tx at snapshot=20 DOES see [0xBB] (opnum=20 is now visible).
    // This proves the store is correct and the pin is the only thing differentiating outcomes.
    {
        let mut tx_late = Tx::begin(&store, 20);
        match tx_late.read(1, &obj(1)) {
            SnapshotRead::Found(v) => assert_eq!(
                v,
                vec![0xBB],
                "IT-1 sanity: snapshot=20 must yield [0xBB] (opnum=20)"
            ),
            other => panic!(
                "IT-1 sanity: expected Found([0xBB]) at snapshot=20; got {:?}",
                other
            ),
        }
        tx_late.abort();
    }
}

// ---------------------------------------------------------------------------
// IT-2: Multi-Tx at the same snapshot produce byte-identical read results
//       AND byte-identical read_set contents.
//
// Scenario (hand-derived):
//   opnum=0: put_versioned(type_id=1, obj(1), opnum=0, value=[0xA1])
//   opnum=1: put_versioned(type_id=1, obj(2), opnum=1, value=[0xA2])
//   opnum=2: put_versioned(type_id=2, obj(1), opnum=2, value=[0xB1])
//   opnum=3: put_versioned(type_id=3, obj(7), opnum=3, value=[0xC7])  ← > snapshot
//
//   Three Tx instances, each pinned at snapshot=2. Same read sequence:
//     read(type_id=1, obj(1)) → Found([0xA1])   ← opnum=0 ≤ 2
//     read(type_id=1, obj(2)) → Found([0xA2])   ← opnum=1 ≤ 2
//     read(type_id=2, obj(1)) → Found([0xB1])   ← opnum=2 ≤ 2
//     read(type_id=3, obj(7)) → NotYetWritten   ← opnum=3 > snapshot=2
//
//   All three Tx must return identical SnapshotRead values per read.
//   All three Tx must have byte-identical read_set after the same reads.
//
// This is the thesis-fit replayable pillar: deterministic replay of the same
// read sequence on the same snapshot produces byte-identical Tx state. Three
// independent Tx instances eliminate the trivial self-comparison.
// ---------------------------------------------------------------------------
#[test]
fn integration_multi_tx_same_snapshot_byte_identity() {
    let mut store = Storage::open(MemVfs::new()).unwrap();

    // Set up store state.
    put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xA1])).unwrap();
    put_versioned(&mut store, 1, &obj(2), 1, Some(vec![0xA2])).unwrap();
    put_versioned(&mut store, 2, &obj(1), 2, Some(vec![0xB1])).unwrap();
    put_versioned(&mut store, 3, &obj(7), 3, Some(vec![0xC7])).unwrap(); // opnum=3 > snapshot=2

    // Three independent Tx instances, all pinned at snapshot=2.
    let mut tx_a = Tx::begin(&store, 2);
    let mut tx_b = Tx::begin(&store, 2);
    let mut tx_c = Tx::begin(&store, 2);

    // --- Same read sequence on each Tx. ---

    // Read 1: type_id=1, obj(1) → hand-derived: opnum=0 ≤ 2 → Found([0xA1]).
    let a_r1 = tx_a.read(1, &obj(1));
    let b_r1 = tx_b.read(1, &obj(1));
    let c_r1 = tx_c.read(1, &obj(1));

    // Read 2: type_id=1, obj(2) → hand-derived: opnum=1 ≤ 2 → Found([0xA2]).
    let a_r2 = tx_a.read(1, &obj(2));
    let b_r2 = tx_b.read(1, &obj(2));
    let c_r2 = tx_c.read(1, &obj(2));

    // Read 3: type_id=2, obj(1) → hand-derived: opnum=2 ≤ 2 → Found([0xB1]).
    let a_r3 = tx_a.read(2, &obj(1));
    let b_r3 = tx_b.read(2, &obj(1));
    let c_r3 = tx_c.read(2, &obj(1));

    // Read 4: type_id=3, obj(7) → hand-derived: opnum=3 > snapshot=2 → NotYetWritten.
    let a_r4 = tx_a.read(3, &obj(7));
    let b_r4 = tx_b.read(3, &obj(7));
    let c_r4 = tx_c.read(3, &obj(7));

    // --- Exact value assertions (hand-derived KATs, NOT derived from tx_a). ---
    assert_eq!(
        a_r1,
        SnapshotRead::Found(vec![0xA1]),
        "IT-2 tx_a r1: expected Found([0xA1])"
    );
    assert_eq!(
        a_r2,
        SnapshotRead::Found(vec![0xA2]),
        "IT-2 tx_a r2: expected Found([0xA2])"
    );
    assert_eq!(
        a_r3,
        SnapshotRead::Found(vec![0xB1]),
        "IT-2 tx_a r3: expected Found([0xB1])"
    );
    assert_eq!(
        a_r4,
        SnapshotRead::NotYetWritten,
        "IT-2 tx_a r4: opnum=3 > snapshot=2, must be NotYetWritten"
    );

    // --- Cross-Tx byte-identity assertions. ---
    assert_eq!(a_r1, b_r1, "IT-2: tx_a r1 must equal tx_b r1 (byte-identity)");
    assert_eq!(a_r1, c_r1, "IT-2: tx_a r1 must equal tx_c r1 (byte-identity)");
    assert_eq!(a_r2, b_r2, "IT-2: tx_a r2 must equal tx_b r2 (byte-identity)");
    assert_eq!(a_r2, c_r2, "IT-2: tx_a r2 must equal tx_c r2 (byte-identity)");
    assert_eq!(a_r3, b_r3, "IT-2: tx_a r3 must equal tx_b r3 (byte-identity)");
    assert_eq!(a_r3, c_r3, "IT-2: tx_a r3 must equal tx_c r3 (byte-identity)");
    assert_eq!(a_r4, b_r4, "IT-2: tx_a r4 must equal tx_b r4 (byte-identity)");
    assert_eq!(a_r4, c_r4, "IT-2: tx_a r4 must equal tx_c r4 (byte-identity)");

    // --- Read-set byte-identity: all three Tx must have the same BTreeSet. ---
    // Hand-derived: 4 distinct reads → 4 entries:
    //   (1, obj(1)), (1, obj(2)), (2, obj(1)), (3, obj(7))
    assert_eq!(tx_a.read_set().len(), 4, "IT-2 tx_a: 4 distinct reads → 4 entries");
    assert_eq!(tx_b.read_set().len(), 4, "IT-2 tx_b: 4 distinct reads → 4 entries");
    assert_eq!(tx_c.read_set().len(), 4, "IT-2 tx_c: 4 distinct reads → 4 entries");

    assert_eq!(
        tx_a.read_set(),
        tx_b.read_set(),
        "IT-2: tx_a.read_set must be byte-identical to tx_b.read_set"
    );
    assert_eq!(
        tx_a.read_set(),
        tx_c.read_set(),
        "IT-2: tx_a.read_set must be byte-identical to tx_c.read_set"
    );

    // Verify exact read_set contents (hand-derived, not inferred from reads above).
    use std::collections::BTreeSet;
    let expected_read_set: BTreeSet<(u32, [u8; 16])> = [
        (1u32, obj(1)),
        (1u32, obj(2)),
        (2u32, obj(1)),
        (3u32, obj(7)),
    ]
    .into_iter()
    .collect();

    assert_eq!(
        tx_a.read_set(),
        &expected_read_set,
        "IT-2: tx_a read_set must match hand-derived expected set"
    );

    tx_a.abort();
    tx_b.abort();
    tx_c.abort();
}

// ---------------------------------------------------------------------------
// IT-3: Read-set grows monotonically; re-read does NOT increase size.
//
// Scenario (hand-derived):
//   Store: keys (type_id=1, obj(0..5)) at opnum 0..4.
//   Tx at snapshot=9.
//
//   After read(1, obj(0)): read_set.len() == 1
//   After read(1, obj(1)): read_set.len() == 2
//   After read(1, obj(2)): read_set.len() == 3
//   After read(1, obj(3)): read_set.len() == 4
//   After read(1, obj(4)): read_set.len() == 5
//   Re-read(1, obj(0)):    read_set.len() == 5  ← set semantics, no dup
//   Re-read(1, obj(2)):    read_set.len() == 5  ← same
//   read(1, obj(5)):       read_set.len() == 6  ← new distinct key
//
// This validates Decision 3 (BTreeSet deduplication) at the integration
// boundary and confirms the SSI invariant: re-reads don't bloat the
// conflict-check surface.
// ---------------------------------------------------------------------------
#[test]
fn integration_read_set_monotonic_growth() {
    let mut store = Storage::open(MemVfs::new()).unwrap();

    // Write 6 distinct keys at opnums 0–5.
    for i in 0u8..6 {
        put_versioned(&mut store, 1, &obj(i), i as u64, Some(vec![i])).unwrap();
    }

    let mut tx = Tx::begin(&store, 9); // snapshot=9 sees all 6 versions.

    // Read 5 distinct keys; verify len grows 0→1→2→3→4→5.
    for i in 0u8..5 {
        let _ = tx.read(1, &obj(i));
        let expected_len = (i as usize) + 1;
        assert_eq!(
            tx.read_set().len(),
            expected_len,
            "IT-3: after reading obj({i}), read_set.len() must be {expected_len}"
        );
    }
    assert_eq!(tx.read_set().len(), 5, "IT-3: 5 distinct reads → len==5");

    // Re-read two already-seen keys; len must stay at 5 (set semantics).
    let _ = tx.read(1, &obj(0));
    assert_eq!(
        tx.read_set().len(),
        5,
        "IT-3: re-read of obj(0) must not grow read_set; set semantics"
    );
    let _ = tx.read(1, &obj(2));
    assert_eq!(
        tx.read_set().len(),
        5,
        "IT-3: re-read of obj(2) must not grow read_set; set semantics"
    );

    // Read a 6th distinct key; len must jump to 6.
    let _ = tx.read(1, &obj(5));
    assert_eq!(
        tx.read_set().len(),
        6,
        "IT-3: reading new obj(5) must grow read_set to 6"
    );

    // Final exact check: read_set contains exactly obj(0)..obj(5).
    for i in 0u8..6 {
        assert!(
            tx.read_set().contains(&(1u32, obj(i))),
            "IT-3: read_set must contain (1, obj({i}))"
        );
    }

    tx.abort();
}

// ---------------------------------------------------------------------------
// IT-4: Tombstone-in-read-set + observability across two snapshot Tx.
//
// Scenario (hand-derived):
//   opnum=10: put_versioned(type_id=5, obj(99), opnum=10, value=[0xFF])  ← live
//   opnum=20: put_versioned(type_id=5, obj(99), opnum=20, None)          ← tombstone
//
//   Tx-High pinned at snapshot=25:
//     read(5, obj(99)) → Tombstoned   ← opnum=20 is the newest ≤ 25, it's a tombstone
//     read_set contains (5, obj(99))
//
//   Tx-Low pinned at snapshot=15:
//     read(5, obj(99)) → Found([0xFF]) ← opnum=10 is newest ≤ 15, it's live
//     read_set contains (5, obj(99))
//
//   Key property: Tx-High.read_set == Tx-Low.read_set  (same key tracked)
//                 but Tx-High returned Tombstoned, Tx-Low returned Found([0xFF]).
//
// This demonstrates Decision 4: read_set tracks observations regardless of
// outcome. SSI in S2.4 needs anti-dependencies on absent/dead keys.
//
// It also verifies the integration-level tombstone path is correct —
// a regression where tombstone entries were not returned (e.g., skipped
// in get_at_snapshot) would make Tx-High return NotYetWritten instead.
// ---------------------------------------------------------------------------
#[test]
fn integration_tombstone_in_read_set_observability() {
    let mut store = Storage::open(MemVfs::new()).unwrap();

    // Write live version at opnum=10.
    put_versioned(&mut store, 5, &obj(99), 10, Some(vec![0xFF])).unwrap();
    // Write tombstone at opnum=20.
    put_versioned(&mut store, 5, &obj(99), 20, None).unwrap();

    // --- Tx-High: snapshot=25, sees tombstone as newest version ≤ 25. ---
    let read_high: SnapshotRead;
    let read_set_high: std::collections::BTreeSet<(u32, [u8; 16])>;
    {
        let mut tx_high = Tx::begin(&store, 25);
        // Hand-derived: opnum=20 ≤ 25 → newest visible → tombstone.
        read_high = tx_high.read(5, &obj(99));
        assert_eq!(
            read_high,
            SnapshotRead::Tombstoned,
            "IT-4 Tx-High: snapshot=25 must return Tombstoned (opnum=20 tombstone is newest ≤ 25)"
        );
        assert_eq!(
            tx_high.read_set().len(),
            1,
            "IT-4 Tx-High: tombstone read must enter read_set"
        );
        assert!(
            tx_high.read_set().contains(&(5u32, obj(99))),
            "IT-4 Tx-High: read_set must contain (5, obj(99)) after Tombstoned read"
        );
        read_set_high = tx_high.read_set().clone();
        tx_high.abort();
    }

    // --- Tx-Low: snapshot=15, sees live version at opnum=10 as newest ≤ 15. ---
    let read_low: SnapshotRead;
    let read_set_low: std::collections::BTreeSet<(u32, [u8; 16])>;
    {
        let mut tx_low = Tx::begin(&store, 15);
        // Hand-derived: opnum=20 > 15 → invisible; opnum=10 ≤ 15 → visible → Found([0xFF]).
        read_low = tx_low.read(5, &obj(99));
        assert_eq!(
            read_low,
            SnapshotRead::Found(vec![0xFF]),
            "IT-4 Tx-Low: snapshot=15 must return Found([0xFF]) (opnum=10 is newest ≤ 15)"
        );
        assert_eq!(
            tx_low.read_set().len(),
            1,
            "IT-4 Tx-Low: live-version read must enter read_set"
        );
        assert!(
            tx_low.read_set().contains(&(5u32, obj(99))),
            "IT-4 Tx-Low: read_set must contain (5, obj(99)) after Found read"
        );
        read_set_low = tx_low.read_set().clone();
        tx_low.abort();
    }

    // --- Key property: SAME read_set key, DIFFERENT read outcomes. ---
    assert_eq!(
        read_set_high,
        read_set_low,
        "IT-4: Tx-High and Tx-Low read_sets must be identical (same key observed); \
         outcomes differ (Tombstoned vs Found) but read_set tracks the observation, not the outcome"
    );

    // Explicitly confirm the two reads DID differ in outcome (not a tautology).
    assert_ne!(
        read_high,
        read_low,
        "IT-4: Tx-High and Tx-Low must return DIFFERENT SnapshotRead variants \
         (Tombstoned vs Found); identical read_sets but different outcomes"
    );
}
