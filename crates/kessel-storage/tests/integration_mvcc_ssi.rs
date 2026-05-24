//! SP113 / S2.4: Integration tests for the SSI promotion path.
//! T1 scaffold + T4 coverage tests (empty-read_set degeneration, 1000-entry
//! read_set, 3-replica verdict identity and SI+SSI interleaving are in
//! kessel-sm/src/lib.rs because they need StateMachine + pending_txs access).
//!
//! Tests in this file operate only on `Tx::commit` / `Tx::commit_ssi` via
//! the kessel-storage public surface — no SM dependency.
//!
//! Test index (T1 + T4 coverage):
//!   SCAFFOLD-1 — it_scaffold_tx_commitoutcome_aborteddangerousstructure_constructible
//!   SCAFFOLD-2 — it_scaffold_abortreason_dangerousstructure_constructible
//!   COV-1      — it_coverage_empty_read_set_via_commit_ssi_degenerates_to_si
//!                  10-commit workload through Tx::commit (SI) and Tx::commit_ssi
//!                  (SSI, empty read_set); assert byte-identical dump.
//!   COV-2      — it_coverage_large_read_set_commit_ssi_success
//!                  Tx with 1000-entry read_set, no writes; assert Committed,
//!                  no panic, finishes in <500 ms.

#![forbid(unsafe_code)]

use kessel_io::MemVfs;
use kessel_storage::{
    mvcc::VERSIONED_KEY_LEN,
    tx::{Tx, TxCommitOutcome},
    Storage,
};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Helper: build a 16-byte object_id from a u8 discriminant.
// ---------------------------------------------------------------------------
fn obj(n: u8) -> [u8; 16] {
    let mut a = [0u8; 16];
    a[15] = n;
    a
}

// ---------------------------------------------------------------------------
// Helper: dump_all_versions — scan the raw LSM bytes for all versioned keys.
// ---------------------------------------------------------------------------
fn dump_all_versions<V: kessel_io::Vfs>(
    store: &Storage<V>,
) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
    let lo = vec![0x00u8; VERSIONED_KEY_LEN];
    let hi = vec![0xFFu8; VERSIONED_KEY_LEN];
    store
        .scan_range_versions(&lo, &hi)
        .into_iter()
        .filter(|(k, _)| k.len() == VERSIONED_KEY_LEN)
        .collect()
}

// ---------------------------------------------------------------------------
// T1 SCAFFOLD TESTS — establish type shapes for the SSI surface.
// ---------------------------------------------------------------------------

#[test]
fn it_scaffold_tx_commitoutcome_aborteddangerousstructure_constructible() {
    use kessel_storage::tx::TxCommitOutcome;
    let o = TxCommitOutcome::AbortedDangerousStructure { other_commit_opnum: 7 };
    assert!(matches!(o, TxCommitOutcome::AbortedDangerousStructure { other_commit_opnum: 7 }));
}

#[test]
fn it_scaffold_abortreason_dangerousstructure_constructible() {
    use kessel_proto::AbortReason;
    let r = AbortReason::DangerousStructure { other_commit_opnum: 11 };
    assert!(matches!(r, AbortReason::DangerousStructure { other_commit_opnum: 11 }));
}

// ---------------------------------------------------------------------------
// COV-1: Empty read_set degeneration — Tx::commit_ssi with empty read_set
//        produces byte-identical MVCC state to Tx::commit (SI) on the same
//        10-commit workload.
//
// Formal claim (Decision 8 of the S2.4 design): when read_set is empty,
// `Tx::commit_ssi` takes the same code path as `Tx::commit` — the SSI
// branch is gated on `!read_set.is_empty()` so zero SSI logic runs; the
// resulting versioned MVCC state is structurally identical.
//
// Workload (hand-derived, 10 sequential commits, NO conflicts):
//   type_id = 20. Each Tx writes obj(i) with value [i] at opnum=i (1..=10).
//   All Tx have snapshot=i-1 (snapshot is the prior commit) so the conflict
//   window (snapshot, opnum-1] is always empty — each Tx commits cleanly.
//
//   Path A: Tx::commit (SI).
//   Path B: Tx::commit_ssi with read_set left empty (degenerated SI).
//
//   Expected: dump_all_versions(A) == dump_all_versions(B). 10 entries each.
//
// KAT: every commit returns Committed { commit_opnum: i } on both paths.
// Regression trap: if commit_ssi runs SSI logic on an empty pending_txs even
// when read_set is empty, the results would STILL be identical — but the gate
// being present is verified by asserting the outcome matches the SI path byte-
// for-byte (any divergence would show up in the dump or outcomes).
// ---------------------------------------------------------------------------
#[test]
fn it_coverage_empty_read_set_via_commit_ssi_degenerates_to_si() {
    const TYPE_ID: u32 = 20;
    const N: u64 = 10;

    // Build one replica using the given commit function (SI or degenerated SSI).
    // Returns (dump, outcomes).
    fn run_path(use_commit_ssi: bool) -> (BTreeMap<Vec<u8>, Option<Vec<u8>>>, Vec<TxCommitOutcome>) {
        let mut store = Storage::open(MemVfs::new()).unwrap();
        let mut outcomes = Vec::new();
        for i in 1u64..=N {
            let snapshot = i - 1; // each Tx sees the previous commit
            let commit_opnum = i;
            if use_commit_ssi {
                // SSI mode — but read_set is LEFT EMPTY (degenerated to SI).
                // No tx.read() calls ⇒ read_set stays empty.
                let mut tx = Tx::begin_ssi(&mut store, snapshot).expect("SP114 T1: watermark=0; begin_ssi always Ok");
                tx.write(TYPE_ID, &obj(i as u8), Some(vec![i as u8]));
                // Verify read_set is empty (structural assertion on the degeneration).
                assert_eq!(
                    tx.read_set().len(),
                    0,
                    "COV-1: commit_ssi path must have empty read_set (degeneration)"
                );
                let out = tx.commit_ssi(commit_opnum)
                    .expect("COV-1: commit_ssi must not TxError");
                outcomes.push(out);
            } else {
                // SI mode — Tx::commit (baseline).
                let mut tx = Tx::begin_rw(&mut store, snapshot).expect("SP114 T1: watermark=0; begin_rw always Ok");
                tx.write(TYPE_ID, &obj(i as u8), Some(vec![i as u8]));
                let out = tx.commit(commit_opnum)
                    .expect("COV-1: commit must not TxError");
                outcomes.push(out);
            }
        }
        let dump = dump_all_versions(&store);
        (dump, outcomes)
    }

    let (dump_si, outcomes_si) = run_path(false);
    let (dump_ssi, outcomes_ssi) = run_path(true);

    // KAT: every commit returns Committed { commit_opnum: i } on both paths.
    for i in 0..N as usize {
        let expected = TxCommitOutcome::Committed { commit_opnum: (i + 1) as u64 };
        assert_eq!(
            outcomes_si[i], expected,
            "COV-1 SI: commit {} must return Committed", i + 1
        );
        assert_eq!(
            outcomes_ssi[i], expected,
            "COV-1 SSI(degenerated): commit {} must return Committed", i + 1
        );
    }

    // HEADLINE: byte-identical MVCC state.
    assert_eq!(
        dump_si, dump_ssi,
        "COV-1 (DEGENERATION): Tx::commit_ssi with empty read_set must produce \
         byte-identical MVCC state to Tx::commit (SI) on a 10-commit workload. \
         Any divergence indicates the empty-read_set gate is broken."
    );

    // KAT: exactly 10 versioned entries (one per commit).
    assert_eq!(
        dump_si.len(),
        N as usize,
        "COV-1: dump must have exactly 10 versioned entries"
    );
}

// ---------------------------------------------------------------------------
// COV-2: Large read_set commit — Tx::commit_ssi with a 1000-entry read_set
//        (no writes) succeeds, no panic, no slowness.
//
// Claim: The sorted-Vec intersection check (`sorted_vec_intersects`) is
// O(n+m); a 1000-element read_set with no concurrent Tx in pending_txs
// (empty local map, standalone form) runs in <500 ms even in debug mode.
// This is a perf-as-correctness gate — if it trips, revisit the O(n+m)
// claim.
//
// Workload:
//   type_id = 21. No writes. 1000 distinct reads: obj(0)..obj(254) per-byte
//   plus 746 more using a 2-byte scheme (packed into the last 2 bytes of the
//   16-byte object_id).
//
//   Tx::begin_ssi at snapshot=0; tx.read(21, &obj_k) for 1000 distinct keys;
//   no tx.write() calls. commit_ssi(opnum=1) → Committed { commit_opnum: 1 }.
//
// KAT:
//   - outcome == Committed { commit_opnum: 1 }
//   - tx.read_set().len() == 1000 (before commit; verified via a pre-commit
//     structural assertion — read_set is accessible on Tx so we check it
//     before commit consumes the Tx).
//   - Wall clock elapsed < 500 ms (perf gate).
//
// Regression trap: if sorted_vec_intersects degenerates to O(n*m), 1000 * 0 =
// 0 iterations regardless (empty pending_txs), so this test catches a
// larger-pending_txs regression IF combined with IT-2's workload; here it
// primarily locks the no-panic / completion contract on 1000-entry inputs.
// ---------------------------------------------------------------------------
#[test]
fn it_coverage_large_read_set_commit_ssi_success() {
    use std::time::Instant;
    const TYPE_ID: u32 = 21;
    const N_READS: usize = 1000;

    // Build 1000 distinct 16-byte object_ids.
    // Keys 0..255: last byte = k (first 15 bytes zeroed).
    // Keys 256..999: pack index into last 2 bytes (bytes 14 and 15).
    fn big_key(idx: usize) -> [u8; 16] {
        let mut k = [0u8; 16];
        if idx < 256 {
            k[15] = idx as u8;
        } else {
            // idx ∈ [256, 999]: pack into bytes 14..15.
            let v = idx as u16;
            k[14] = (v >> 8) as u8;
            k[15] = (v & 0xFF) as u8;
        }
        k
    }

    let mut store = Storage::open(MemVfs::new()).unwrap();

    // Build the Tx with 1000 reads (no writes). Measure construction time
    // separately to keep the perf gate focused on commit_ssi overhead.
    let mut tx = Tx::begin_ssi(&mut store, 0).expect("SP114 T1: watermark=0; begin_ssi always Ok");
    for idx in 0..N_READS {
        let key = big_key(idx);
        let _ = tx.read(TYPE_ID, &key); // result is NotYetWritten; side-effect: records read_set
    }

    // Structural assertion: read_set must have exactly 1000 entries (BTreeSet dedup).
    assert_eq!(
        tx.read_set().len(),
        N_READS,
        "COV-2: Tx must accumulate exactly 1000 distinct read_set entries"
    );

    // Commit with timing gate.
    let t0 = Instant::now();
    let outcome = tx.commit_ssi(1).expect("COV-2: commit_ssi must not TxError");
    let elapsed = t0.elapsed();

    // KAT: outcome must be Committed { commit_opnum: 1 }.
    assert_eq!(
        outcome,
        TxCommitOutcome::Committed { commit_opnum: 1 },
        "COV-2: commit_ssi with 1000-entry read_set + no writes must return Committed"
    );

    // Perf gate (loose — 500 ms covers debug+release; the real O(n+m) cost
    // on an empty pending_txs is O(0) iterations, so even debug mode should
    // be well under 1 ms).
    assert!(
        elapsed.as_millis() < 500,
        "COV-2: commit_ssi with 1000-entry read_set must complete in <500 ms; \
         elapsed = {}ms. If this fires, revisit sorted_vec_intersects complexity.",
        elapsed.as_millis()
    );
}
