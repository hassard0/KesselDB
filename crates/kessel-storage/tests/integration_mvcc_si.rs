//! SP112 T3 — Integration tests for SI conflict detection + 3-replica
//! byte-identity at the MVCC / Tx commit layer.
//!
//! This is the thesis-fit centerpiece test layer for Snapshot Isolation.
//! The headline claim: three replicas running `Tx::commit` (the same logic
//! the SM apply path for `Op::CommitTx` delegates to) over the SAME log
//! sequence produce BYTE-IDENTICAL MVCC state — even in the presence of
//! concurrent write-write conflicts. The deterministic conflict verdict
//! (first-committer-wins per commit_opnum ordering) is what makes SI work
//! without distributed coordination.
//!
//! Per the plan, IT-5 (SM-apply ↔ Tx-commit byte-equivalence) lives in
//! `crates/kessel-sm/src/lib.rs`'s internal `#[cfg(test)]` module because
//! `kessel-storage` cannot depend on `kessel-sm` (that would be circular:
//! kessel-sm depends on kessel-storage). The SM internal test accesses the
//! private `self.storage` field directly, which avoids adding a new public
//! API surface. This placement is documented in the SP112 record.
//!
//! Test index:
//!   IT-1 — read-your-writes: a write buffered in the same Tx shadows the
//!           snapshot value on subsequent reads in that Tx.
//!   IT-2 — disjoint write-sets both commit: Tx_A writes k1, Tx_B writes
//!           k2 (different keys). Both commit `Committed` on every replica.
//!   IT-3 — overlapping write-sets: Tx_A commits first (lower commit_opnum),
//!           Tx_B writes the same key at a stale snapshot → `Aborted` on
//!           every replica. The committed version is Tx_A's.
//!   IT-4 — 3-replica byte-identity for SI commits (THE HEADLINE): three
//!           independent `Storage<MemVfs>` instances apply the same Tx
//!           sequence (disjoint commit + conflicting abort) and produce
//!           byte-identical `dump_all_versions` maps.
//!
//! KAT discipline: every expected `TxCommitOutcome` variant is hand-derived
//! from the log sequence and the SI first-committer-wins rule. No test
//! derives its expectation by running one path and comparing it to another —
//! each assertion is an independently-derived ground truth.
//!
//! References:
//!   - parent S2 design: docs/superpowers/specs/2026-05-23-mvcc-si-design.md
//!   - S2.3 plan:        docs/superpowers/plans/2026-05-24-mvcc-si-s2-3.md
//!   - SP110 T3 byte-identity convention: mvcc_replication_byte_identity.rs

#![forbid(unsafe_code)]

use kessel_io::MemVfs;
use kessel_storage::{
    mvcc::{get_at_snapshot, put_versioned, SnapshotRead, VERSIONED_KEY_LEN},
    tx::{Tx, TxCommitOutcome},
    Storage,
};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Helper: build a 16-byte object_id from a u8 discriminant.
//
// Same recipe as SP110 T3 and tx_integration.rs: discriminant in the last
// byte, rest zeroed. obj(1) != obj(2) etc. in a clearly visible way in
// assertion failure output.
// ---------------------------------------------------------------------------
fn obj(n: u8) -> [u8; 16] {
    let mut a = [0u8; 16];
    a[15] = n;
    a
}

// ---------------------------------------------------------------------------
// Helper: dump_all_versions — reused from SP110 T3 convention.
//
// Enumerates EVERY versioned physical key present in storage by scanning
// the full versioned-key range [0x00*28, 0xFF*28]. Returns a
// `BTreeMap<Vec<u8>, Option<Vec<u8>>>` mapping each 28-byte physical
// versioned key to its stored value (None = tombstone).
//
// IMPORTANT: this scans the raw LSM bytes, not the MVCC semantic API.
// Byte-identical maps mean byte-identical LSM state — that is the binary
// claim this test suite makes.
// ---------------------------------------------------------------------------
fn dump_all_versions<V: kessel_io::Vfs>(
    store: &Storage<V>,
) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
    let lo = vec![0x00u8; VERSIONED_KEY_LEN];
    let hi = vec![0xFFu8; VERSIONED_KEY_LEN];
    store
        .scan_range_versions(&lo, &hi)
        .into_iter()
        // Guard: only 28-byte versioned keys (legacy keys are shorter and
        // should not appear in these tests, but filter defensively).
        .filter(|(k, _)| k.len() == VERSIONED_KEY_LEN)
        .collect()
}

// ---------------------------------------------------------------------------
// IT-1: Read-your-writes — a write buffered in the same Tx shadows the
//       snapshot value on subsequent reads in that Tx (and a tombstone write
//       shadows too).
//
// Scenario (hand-derived):
//   opnum=0: put_versioned(type_id=1, obj(1), opnum=0, value=[0xAA])
//   Tx at snapshot=0, begin_rw:
//     read(1, obj(1)) → Found([0xAA])     ← snapshot value
//     write(1, obj(1), Some([0xBB]))       ← buffer
//     read(1, obj(1)) → Found([0xBB])     ← RYW overlay, NOT [0xAA]
//     write(1, obj(1), None)              ← buffer tombstone (coalesce)
//     read(1, obj(1)) → Tombstoned        ← tombstone overlay
//   tx.abort() — no writes reach storage
//
// Regression trap: if the RYW overlay is absent, the second read returns
// Found([0xAA]) instead of Found([0xBB]). If coalesce is wrong, the
// tombstone read returns Found([0xBB]).
// ---------------------------------------------------------------------------
#[test]
fn it_read_your_writes_integration() {
    let mut store = Storage::open(MemVfs::new()).unwrap();
    // Seed: install [0xAA] at opnum=0.
    put_versioned(&mut store, 1, &obj(1), 0, Some(vec![0xAA])).unwrap();

    // Begin a write-capable Tx at snapshot=0.
    let mut tx = Tx::begin_rw(&mut store, 0);

    // First read: snapshot value [0xAA] (no buffered write yet).
    match tx.read(1, &obj(1)) {
        SnapshotRead::Found(v) => assert_eq!(v, vec![0xAA], "IT-1: first read should be snapshot [0xAA]"),
        other => panic!("IT-1: expected Found([0xAA]), got {other:?}"),
    }

    // Buffer a write: [0xBB].
    tx.write(1, &obj(1), Some(vec![0xBB]));

    // Second read: must return [0xBB] from the RYW overlay, NOT [0xAA].
    match tx.read(1, &obj(1)) {
        SnapshotRead::Found(v) => assert_eq!(v, vec![0xBB], "IT-1: RYW must shadow snapshot with [0xBB]"),
        other => panic!("IT-1: expected Found([0xBB]) from RYW overlay, got {other:?}"),
    }

    // Coalesce: overwrite with a tombstone (last-write-wins within Tx).
    tx.write(1, &obj(1), None);

    // Third read: tombstone overlay must be visible.
    match tx.read(1, &obj(1)) {
        SnapshotRead::Tombstoned => {} // correct
        other => panic!("IT-1: expected Tombstoned after tombstone write, got {other:?}"),
    }

    // Abort: no writes reach storage — seed [0xAA] is still the committed value.
    tx.abort();

    // Verify the abort was a true no-op: seed value still visible at snapshot=0.
    match get_at_snapshot(&store, 1, &obj(1), 0) {
        SnapshotRead::Found(v) => assert_eq!(v, vec![0xAA], "IT-1: aborted writes must not persist"),
        other => panic!("IT-1: post-abort storage should still have [0xAA], got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// IT-2: Disjoint write-sets both commit — no conflict.
//
// Scenario (hand-derived, first-committer-wins SI rule):
//   Empty store, snapshot=0 for both Tx.
//   Tx_A: write(1, obj(1), [0xAA]); commit at opnum=1 → Committed { 1 }
//   Tx_B: write(1, obj(2), [0xBB]); commit at opnum=2 → Committed { 2 }
//     Conflict check for Tx_B: key=(1,obj(2)), window=(snapshot=0, hi=1].
//     has_version_in_range checks whether any version of (1,obj(2)) was
//     committed in (0, 1]. Answer: NO (Tx_A wrote (1,obj(1)), different key).
//     → Committed.
//
//   At snapshot=2: both [0xAA] at (1,obj(1)) and [0xBB] at (1,obj(2)) visible.
//
// Regression trap: if the conflict check incorrectly matches unrelated keys,
// Tx_B would return Aborted instead of Committed.
// ---------------------------------------------------------------------------
#[test]
fn it_disjoint_write_sets_both_commit() {
    let mut store = Storage::open(MemVfs::new()).unwrap();

    // Tx_A: write (1, obj(1)) → commit at opnum=1.
    {
        let mut tx_a = Tx::begin_rw(&mut store, 0);
        tx_a.write(1, &obj(1), Some(vec![0xAA]));
        let out_a = tx_a.commit(1).expect("IT-2: Tx_A commit must not return TxError");
        assert_eq!(
            out_a,
            TxCommitOutcome::Committed { commit_opnum: 1 },
            "IT-2: Tx_A (writes obj(1)) must commit — no prior version of obj(1) in (0,0]"
        );
    }

    // Tx_B: write (1, obj(2)) → commit at opnum=2.
    // snapshot=0 (started from the same snapshot as Tx_A — simulates
    // concurrent Tx that both began before either committed).
    {
        let mut tx_b = Tx::begin_rw(&mut store, 0);
        tx_b.write(1, &obj(2), Some(vec![0xBB]));
        let out_b = tx_b.commit(2).expect("IT-2: Tx_B commit must not return TxError");
        assert_eq!(
            out_b,
            TxCommitOutcome::Committed { commit_opnum: 2 },
            "IT-2: Tx_B (writes obj(2), different key) must commit — disjoint write-set"
        );
    }

    // Verify: both versions visible at snapshot=2.
    match get_at_snapshot(&store, 1, &obj(1), 2) {
        SnapshotRead::Found(v) => assert_eq!(v, vec![0xAA], "IT-2: obj(1) must be [0xAA] at snap=2"),
        other => panic!("IT-2: obj(1) not visible at snap=2: {other:?}"),
    }
    match get_at_snapshot(&store, 1, &obj(2), 2) {
        SnapshotRead::Found(v) => assert_eq!(v, vec![0xBB], "IT-2: obj(2) must be [0xBB] at snap=2"),
        other => panic!("IT-2: obj(2) not visible at snap=2: {other:?}"),
    }

    // Verify obj(1) is NOT visible at snapshot=0 (pre-commit).
    match get_at_snapshot(&store, 1, &obj(1), 0) {
        SnapshotRead::NotYetWritten => {} // correct: opnum=1 > snapshot=0
        other => panic!("IT-2: obj(1) should be NotYetWritten at snap=0, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// IT-3: Overlapping write-sets — first committer wins, second aborts.
//
// Scenario (hand-derived, first-committer-wins SI rule):
//   Empty store.
//   Tx_A: snapshot=0, write(1, obj(1), [0xAA]); commit at opnum=20 → Committed
//     Conflict check: window=(0, 19]. No version of (1,obj(1)) in (0,19] → OK.
//   Tx_B: snapshot=0, write(1, obj(1), [0xBB]); commit at opnum=30 → Aborted
//     Conflict check: window=(0, 29].
//     has_version_in_range(1, obj(1), lo=0, hi=29) finds the version at
//     opnum=20 (committed by Tx_A). → Aborted { conflicting_key=(1,obj(1)) }.
//
//   After: only Tx_A's [0xAA] at opnum=20 is in storage. Tx_B's [0xBB] was
//   never installed.
//
// This is the SI write-write conflict rule: the SECOND writer (by commit
// ordering, not wall-clock) always loses when it attempts to write a key
// that was written after its snapshot.
//
// Regression trap: if the conflict check is missing, Tx_B would return
// Committed and install [0xBB] at opnum=30, overwriting Tx_A's intent.
// ---------------------------------------------------------------------------
#[test]
fn it_overlap_aborts_second_committer() {
    let mut store = Storage::open(MemVfs::new()).unwrap();

    // Tx_A: snapshot=0, write obj(1) → commit at opnum=20. First committer wins.
    {
        let mut tx_a = Tx::begin_rw(&mut store, 0);
        tx_a.write(1, &obj(1), Some(vec![0xAA]));
        let out_a = tx_a.commit(20).expect("IT-3: Tx_A commit must not return TxError");
        assert_eq!(
            out_a,
            TxCommitOutcome::Committed { commit_opnum: 20 },
            "IT-3: Tx_A (first committer) must commit at opnum=20"
        );
    }

    // Tx_B: snapshot=0 (stale — both Tx had the same empty starting state),
    // writes the SAME key obj(1) → commit at opnum=30. Must abort.
    {
        let mut tx_b = Tx::begin_rw(&mut store, 0);
        tx_b.write(1, &obj(1), Some(vec![0xBB]));
        let out_b = tx_b.commit(30).expect("IT-3: Tx_B commit must not return TxError");
        assert_eq!(
            out_b,
            TxCommitOutcome::Aborted { conflicting_key: (1u32, obj(1)) },
            "IT-3: Tx_B (second committer, same key) must abort with conflicting_key=(1,obj(1))"
        );
    }

    // Verify: only Tx_A's value survives. Tx_B's [0xBB] was never installed.
    match get_at_snapshot(&store, 1, &obj(1), 30) {
        SnapshotRead::Found(v) => assert_eq!(v, vec![0xAA], "IT-3: committed value must be Tx_A's [0xAA]"),
        other => panic!("IT-3: expected Found([0xAA]) after overlap-abort, got {other:?}"),
    }

    // Verify [0xBB] (Tx_B's value) was NEVER installed at any snapshot.
    // We confirm by checking snapshot=30 returns [0xAA], not [0xBB].
    // Additional: at snapshot=19 (before Tx_A's commit), obj(1) should be absent.
    match get_at_snapshot(&store, 1, &obj(1), 19) {
        SnapshotRead::NotYetWritten => {} // correct: Tx_A commits at opnum=20
        other => panic!("IT-3: obj(1) should be NotYetWritten at snap=19, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// IT-4: 3-replica byte-identity for SI commits — THE HEADLINE TEST.
//
// This is the thesis-fit centerpiece (parent S2 design Decision 4 + S2.3
// Decision 4). Three independent `Storage<MemVfs>` instances each apply
// the SAME sequence of `Tx::commit` calls (seeded identically). After
// applying a workload that includes:
//   (a) a disjoint commit that succeeds on every replica, and
//   (b) a conflicting abort that is detected identically on every replica,
// the `dump_all_versions` BTreeMap of each replica must be byte-identical.
//
// Why this proves the claim:
//   - Each replica is a separate `Storage<MemVfs>` — no shared state.
//   - The deterministic conflict check (has_version_in_range) on identical
//     LSM state at the same opnum MUST reach the same verdict on all replicas.
//   - If any replica committed Tx_3 (the conflicting abort) instead of
//     aborting it, its dump would differ from the other two at the
//     (1,obj(1), opnum=30) versioned key slot — the assert_eq! would catch it.
//
// Workload (hand-derived):
//   Seed: put_versioned(1, obj(1), opnum=0, [0xAA])
//         put_versioned(2, obj(2), opnum=0, [0xBB])
//   Tx_1: snapshot=0, write(1,obj(3),[0xCC]); commit opnum=1 → Committed { 1 }
//   Tx_2: snapshot=1, write(2,obj(4),[0xDD]); commit opnum=2 → Committed { 2 }
//   Tx_3: snapshot=0, write(1,obj(1),[0xEE]); commit opnum=3 → Aborted
//     (because Tx_1 didn't touch obj(1), but the SEED put_versioned at
//     opnum=0 installed (1,obj(1)) — and snapshot=0 means the conflict
//     window is (0,2]. Wait: has_version_in_range checks for versions
//     committed STRICTLY AFTER snapshot and STRICTLY BEFORE commit. The
//     window is (snapshot_opnum, commit_opnum-1] = (0, 2].
//     The seed put_versioned used opnum=0, which is NOT in (0,2].
//     So Tx_3 would NOT conflict with the seed.
//
// To make Tx_3 conflict, we need a version at opnum in (0,2] for key
// (1,obj(1)). Let Tx_1 write (1,obj(1),[0xCC]) at opnum=1 instead.
//
// REVISED workload (ensuring a genuine conflict for Tx_3):
//   Seed: put_versioned(2, obj(2), opnum=0, [0xBB])   ← type_id=2, different key
//   Tx_1: snapshot=0, write(1,obj(1),[0xCC]); commit opnum=1 → Committed { 1 }
//   Tx_2: snapshot=1, write(2,obj(2),[0xDD]); commit opnum=2 → Committed { 2 }
//   Tx_3: snapshot=0, write(1,obj(1),[0xEE]); commit opnum=3 → Aborted
//     Conflict check: window=(0,2]. Key=(1,obj(1)). Tx_1 committed (1,obj(1))
//     at opnum=1 ∈ (0,2]. → Aborted { conflicting_key=(1,obj(1)) }. CORRECT.
//
// Expected final dump_all_versions for each replica:
//   versioned_key(2, obj(2), opnum=0) → Some([0xBB])   (seed)
//   versioned_key(1, obj(1), opnum=1) → Some([0xCC])   (Tx_1)
//   versioned_key(2, obj(2), opnum=2) → Some([0xDD])   (Tx_2)
//   (no entry for Tx_3's [0xEE] at opnum=3 — aborted, never installed)
//
// The 3-replica byte-identity assert: r1_dump == r2_dump == r3_dump.
// ---------------------------------------------------------------------------
#[test]
fn it_three_replica_byte_identity_for_si_commits() {
    // Build one replica: seed + apply the SI workload.
    // Returns (dump, Tx_1 outcome, Tx_2 outcome, Tx_3 outcome).
    fn build_and_apply() -> (
        BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        TxCommitOutcome,
        TxCommitOutcome,
        TxCommitOutcome,
    ) {
        let mut store = Storage::open(MemVfs::new()).unwrap();

        // Seed: type_id=2, obj(2) at opnum=0. (type_id=1 stays empty so
        // Tx_1's commit of (1,obj(1)) is the FIRST write to that key.)
        put_versioned(&mut store, 2, &obj(2), 0, Some(vec![0xBB])).unwrap();

        // Tx_1: snapshot=0, write (1,obj(1),[0xCC]), commit at opnum=1.
        // Expected: Committed { 1 }. Conflict window=(0,0] is empty (hi=0,
        // and the only version at opnum=0 is on type_id=2 — different type).
        let out1 = {
            let mut tx1 = Tx::begin_rw(&mut store, 0);
            tx1.write(1, &obj(1), Some(vec![0xCC]));
            tx1.commit(1).expect("IT-4: Tx_1 commit must not TxError")
        };

        // Tx_2: snapshot=1, write (2,obj(2),[0xDD]), commit at opnum=2.
        // Expected: Committed { 2 }. Conflict window=(1,1]. No version of
        // (2,obj(2)) committed at opnum=1 (Tx_1 wrote type_id=1). → OK.
        let out2 = {
            let mut tx2 = Tx::begin_rw(&mut store, 1);
            tx2.write(2, &obj(2), Some(vec![0xDD]));
            tx2.commit(2).expect("IT-4: Tx_2 commit must not TxError")
        };

        // Tx_3: snapshot=0, write (1,obj(1),[0xEE]), commit at opnum=3.
        // Expected: Aborted { conflicting_key=(1,obj(1)) }.
        // Conflict window=(0,2]. has_version_in_range(1, obj(1), 0, 2) finds
        // the version installed by Tx_1 at opnum=1 ∈ (0,2]. → Conflict.
        let out3 = {
            let mut tx3 = Tx::begin_rw(&mut store, 0);
            tx3.write(1, &obj(1), Some(vec![0xEE]));
            tx3.commit(3).expect("IT-4: Tx_3 commit must not TxError")
        };

        let dump = dump_all_versions(&store);
        (dump, out1, out2, out3)
    }

    // Run on three independent replicas.
    let (dump_r1, out1_r1, out2_r1, out3_r1) = build_and_apply();
    let (dump_r2, out1_r2, out2_r2, out3_r2) = build_and_apply();
    let (dump_r3, out1_r3, out2_r3, out3_r3) = build_and_apply();

    // ---- KAT: assert exact outcomes on EVERY replica (not just one) ----

    // Tx_1 must be Committed { commit_opnum: 1 } on all three replicas.
    assert_eq!(out1_r1, TxCommitOutcome::Committed { commit_opnum: 1 }, "IT-4 r1: Tx_1 must commit");
    assert_eq!(out1_r2, TxCommitOutcome::Committed { commit_opnum: 1 }, "IT-4 r2: Tx_1 must commit");
    assert_eq!(out1_r3, TxCommitOutcome::Committed { commit_opnum: 1 }, "IT-4 r3: Tx_1 must commit");

    // Tx_2 must be Committed { commit_opnum: 2 } on all three replicas.
    assert_eq!(out2_r1, TxCommitOutcome::Committed { commit_opnum: 2 }, "IT-4 r1: Tx_2 must commit");
    assert_eq!(out2_r2, TxCommitOutcome::Committed { commit_opnum: 2 }, "IT-4 r2: Tx_2 must commit");
    assert_eq!(out2_r3, TxCommitOutcome::Committed { commit_opnum: 2 }, "IT-4 r3: Tx_2 must commit");

    // Tx_3 must be Aborted { conflicting_key=(1,obj(1)) } on all three replicas.
    // This is the SI determinism claim: every replica independently detects the
    // same conflict at the same opnum and arrives at the same Aborted verdict.
    let expected_abort = TxCommitOutcome::Aborted { conflicting_key: (1u32, obj(1)) };
    assert_eq!(out3_r1, expected_abort, "IT-4 r1: Tx_3 must abort (conflict with Tx_1)");
    assert_eq!(out3_r2, expected_abort, "IT-4 r2: Tx_3 must abort (conflict with Tx_1)");
    assert_eq!(out3_r3, expected_abort, "IT-4 r3: Tx_3 must abort (conflict with Tx_1)");

    // ---- HEADLINE: 3-replica byte-identity assertion ----
    //
    // All three replicas applied the SAME sequence of Tx commits and arrived
    // at the SAME verdict for each. Their MVCC state must be byte-identical
    // at the physical LSM level. If any replica diverged (e.g., incorrectly
    // committed Tx_3), its dump would have an extra entry at the versioned key
    // (1, obj(1), opnum=3) — the assert_eq! would catch it.
    assert_eq!(
        dump_r1, dump_r2,
        "IT-4 (THESIS-FIT): replicas 1 and 2 must be byte-identical after SI workload"
    );
    assert_eq!(
        dump_r1, dump_r3,
        "IT-4 (THESIS-FIT): replicas 1 and 3 must be byte-identical after SI workload"
    );

    // ---- KAT: verify the exact expected entries in the dump ----
    //
    // Expected versioned keys (hand-derived from the workload above):
    //   seed:  (type_id=2, obj(2), opnum=0) → Some([0xBB])
    //   Tx_1:  (type_id=1, obj(1), opnum=1) → Some([0xCC])
    //   Tx_2:  (type_id=2, obj(2), opnum=2) → Some([0xDD])
    //   Tx_3:  NOT PRESENT (aborted, never installed)
    //
    // The 4th entry that must be ABSENT: (type_id=1, obj(1), opnum=3) → [0xEE].
    // We verify absence by checking the dump has exactly 3 entries.
    assert_eq!(
        dump_r1.len(), 3,
        "IT-4: dump must have exactly 3 versioned entries (seed + Tx_1 + Tx_2; Tx_3 aborted)"
    );

    // Verify no versioned key for Tx_3's [0xEE] exists at opnum=3 on any replica.
    // The versioned key for (type_id=1, obj(1), opnum=3) would be present if
    // the abort was incorrectly treated as a commit.
    use kessel_storage::mvcc::make_versioned_key;
    let tx3_key = make_versioned_key(1, &obj(1), 3);
    assert!(
        !dump_r1.contains_key(tx3_key.as_slice()),
        "IT-4: Tx_3's aborted write must NOT appear in r1 dump"
    );
    assert!(
        !dump_r2.contains_key(tx3_key.as_slice()),
        "IT-4: Tx_3's aborted write must NOT appear in r2 dump"
    );
    assert!(
        !dump_r3.contains_key(tx3_key.as_slice()),
        "IT-4: Tx_3's aborted write must NOT appear in r3 dump"
    );
}
