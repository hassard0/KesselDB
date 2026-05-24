//! kessel-storage::ssi — Cahill SSI dangerous-structure detector
//! (SP113 / S2.4).
//!
//! THE single source of truth for the SSI conflict-detection algorithm.
//! Two callers compose against this module:
//!   1. `kessel_sm::StateMachine::apply` (the production / replicated
//!      path): drives the algorithm over the SM's authoritative
//!      `pending_txs` map.
//!   2. `kessel_storage::tx::Tx::commit_ssi` (the standalone /
//!      single-process testability path): drives the algorithm over a
//!      LOCAL empty `pending_txs` map. This means the standalone form
//!      always reports `Committed` for non-conflicting commits — it
//!      cannot derive rw-edges against an empty pending_txs. This is
//!      fine: the SM path is the production path, and the standalone
//!      form documents the Tx surface + composes byte-identically with
//!      the SM form on the empty-pending_txs case (verified by T3's
//!      byte-equivalence test).
//!
//! Why a shared module (mirrors SP112 T2's `TxStore::Shared|Exclusive`
//! split discipline): a single source of truth for the Cahill algorithm
//! is essential for the byte-identity property (Tx::commit_ssi and
//! SM apply must reach the same verdict on the same inputs; the
//! easiest way to guarantee that is to call the same function).
//!
//! Algorithm (Cahill, 2008 — Serializable Snapshot Isolation):
//!   - Tx_A →rw Tx_B (rw-antidependency): Tx_B wrote a key that Tx_A
//!     had read (Tx_A's snapshot would have shown the pre-Tx_B
//!     version; Tx_B's commit invalidates Tx_A's read-set).
//!   - A Tx is the PIVOT of a dangerous structure iff it has BOTH an
//!     incoming rw-edge AND an outgoing rw-edge:
//!         Tx_in →rw Tx_pivot →rw Tx_out.
//!   - Cahill's theorem: aborting any one of {Tx_in, Tx_pivot,
//!     Tx_out} preserves serializability. KesselDB picks Decision 3:
//!     abort the LATEST committer (the one whose commit is currently
//!     being applied) — this is the only choice that does not require
//!     undoing an already-applied commit.
//!
//! Deterministic by construction:
//!   - `BTreeMap::range` iterates in sorted order.
//!   - `sorted_vec_intersects` is two-pointer O(n+m) on sorted slices.
//!   - No hashing, no allocator state.
//!
//! Refs:
//!   - parent design: docs/superpowers/specs/2026-05-24-mvcc-si-s2-4-design.md
//!     Decisions 1 (Cahill), 2 (pending_txs shape), 3 (abort-latest),
//!     4 (additive Op extension), 5 (MAX_TX_AGE window), 6 (per-call
//!     API), 8 (empty-read_set degeneration).
//!   - plan: docs/superpowers/plans/2026-05-24-mvcc-si-s2-4.md T2.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

/// SP113 / S2.4: Per-committed-Tx record retained in the SM's
/// `pending_txs` window for SSI rw-edge derivation. Keys-only
/// read_set + write_set halves the memory footprint vs the wire
/// shape (the SSI algorithm operates on key sets, not values).
///
/// Lives in kessel-storage so that BOTH the SM apply path AND
/// `Tx::commit_ssi`'s standalone form can refer to one type. The SM
/// re-exports it as `kessel_sm::PendingTxRecord` for the existing
/// SP113 T1 scaffold's callers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingTxRecord {
    pub snapshot_opnum: u64,
    /// Sorted by (type_id, object_id) for deterministic iteration.
    pub read_set: Vec<(u32, [u8; 16])>,
    /// Sorted by (type_id, object_id) for deterministic iteration.
    /// Keys only — values discarded (rw-edges are over keys).
    pub write_set: Vec<(u32, [u8; 16])>,
    /// Cahill SSI per-Tx tag: TRUE iff this Tx has an outgoing rw-edge
    /// to some later committer (i.e. some later Tx wrote a key this
    /// Tx had read).
    pub has_outgoing_rw: bool,
    /// Cahill SSI per-Tx tag: TRUE iff this Tx has an incoming rw-edge
    /// from some earlier committer (i.e. some earlier Tx's read overlaps
    /// this Tx's write).
    pub has_incoming_rw: bool,
}

/// SP113 / S2.4: O(n + m) intersection check on two sorted slices.
/// Returns TRUE iff the slices share at least one element.
///
/// Deterministic: no hashing, no allocator state. Two-pointer walk.
///
/// Caller MUST guarantee both inputs are sorted ascending. The
/// `Tx::write_set` (BTreeMap) and `Tx::read_set` (BTreeSet) yield
/// sorted iteration by construction; the `Vec` collected from those
/// is therefore sorted. The wire-decoded `Op::CommitTx { read_set }`
/// is sorted by the encoder discipline (see `Tx::commit_ssi`).
pub fn sorted_vec_intersects<T: Ord>(a: &[T], b: &[T]) -> bool {
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Equal => return true,
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
        }
    }
    false
}

/// SP113 / S2.4: Cahill SSI dangerous-structure detector.
///
/// Walks every pending Tx CONCURRENT with the committing Tx
/// (concurrent ⇔ `pending.commit_opnum > snapshot_opnum` of THIS Tx
/// AND `pending.commit_opnum < commit_opnum` of THIS Tx), updates
/// per-Tx rw-edge tags in place, and decides whether a dangerous
/// structure has formed.
///
/// The committing Tx (THIS) is the pivot of a dangerous structure
/// iff it has BOTH an outgoing rw-edge AND an incoming rw-edge after
/// the walk. Cahill (2008): abort one of the three Txs in the
/// structure to preserve serializability. Per Decision 3 (abort-the-
/// latest): abort THIS — return `Some(other_commit_opnum)` naming the
/// other Tx in the dangerous chain (surfaced for observability; does
/// not affect the verdict).
///
/// Side effect: when a rw-edge is recorded from THIS to a pending
/// Tx_other (THIS read what Tx_other wrote), Tx_other gains
/// `has_incoming_rw = true`. When a rw-edge is recorded from
/// Tx_other to THIS (Tx_other read what THIS wrote), Tx_other gains
/// `has_outgoing_rw = true`. ALSO: if any pre-existing pending Tx
/// now has BOTH tags set (it just became a pivot because of THIS
/// commit), this is ALSO a dangerous structure — but per Decision 3
/// we still abort the LATEST committer (THIS), not the pre-existing
/// pivot (which would require undoing an already-applied commit).
///
/// Returns:
///   - `Some(other_commit_opnum)` if a dangerous structure was
///     detected (THIS should abort). The other_commit_opnum is one
///     of the Txs in the chain; surfaced for debugging.
///   - `None` if no dangerous structure was detected. THIS may
///     proceed to install its writes.
///
/// Caller MUST: pass `read_set` and `write_set` as sorted slices
/// (sorted by (type_id, object_id) ascending). `pending_txs` is a
/// `BTreeMap` so its iteration is deterministic by construction.
///
/// Note on `read_set` / `write_set` parameter shape: both are keys
/// only — `(type_id, [u8; 16])`. The SM apply arm derives `write_set`
/// from the wire `Op::CommitTx { write_set: Vec<(u32, [u8;16],
/// Option<Vec<u8>>)> }` by stripping values; this is the same shape
/// stored in `PendingTxRecord.write_set`.
pub fn detect_dangerous_structure(
    pending_txs: &mut BTreeMap<u64, PendingTxRecord>,
    snapshot_opnum: u64,
    read_set: &[(u32, [u8; 16])],
    write_set: &[(u32, [u8; 16])],
    commit_opnum: u64,
) -> Option<u64> {
    // Concurrent Tx: commit_opnum strictly in (snapshot_opnum,
    // commit_opnum). The upper bound is exclusive because at apply
    // time THIS Tx's commit_opnum slot is not yet populated; the
    // lower bound is exclusive because a Tx whose commit was visible
    // to THIS Tx's snapshot is NOT concurrent.
    //
    // BTreeMap::range with `lo_range..hi_range` (half-open).
    let lo_range = snapshot_opnum.saturating_add(1);
    let hi_range = commit_opnum; // exclusive upper

    if lo_range >= hi_range {
        // No concurrent Tx possible — no rw-edges can form.
        return None;
    }

    let mut has_outgoing = false;
    let mut has_incoming = false;
    let mut other_commit_opnum: u64 = 0;

    // Collect concurrent (commit_opnum, snapshot_of_record) pairs so
    // we can mutate pending_txs in the second pass without holding
    // an immutable borrow across the loop body. Vec is sorted by key
    // (BTreeMap::range iterates in order).
    let concurrent: Vec<(u64, Vec<(u32, [u8; 16])>, Vec<(u32, [u8; 16])>)> = pending_txs
        .range(lo_range..hi_range)
        .map(|(k, v)| (*k, v.write_set.clone(), v.read_set.clone()))
        .collect();

    for (a_commit, a_write_set, a_read_set) in &concurrent {
        // (a) Tx_A wrote a key THIS Tx had read?
        //     ⇒ THIS →rw Tx_A (THIS has an outgoing rw-edge;
        //                       Tx_A gains an incoming rw-edge).
        if sorted_vec_intersects(a_write_set, read_set) {
            has_outgoing = true;
            other_commit_opnum = *a_commit;
            if let Some(rec) = pending_txs.get_mut(a_commit) {
                rec.has_incoming_rw = true;
            }
        }
        // (b) THIS Tx's write would invalidate a key Tx_A had read?
        //     ⇒ Tx_A →rw THIS (THIS has an incoming rw-edge;
        //                       Tx_A gains an outgoing rw-edge).
        if sorted_vec_intersects(write_set, a_read_set) {
            has_incoming = true;
            other_commit_opnum = *a_commit;
            if let Some(rec) = pending_txs.get_mut(a_commit) {
                rec.has_outgoing_rw = true;
            }
        }
    }

    // Cahill dangerous-structure check (1): THIS is the pivot of
    // {Tx_in →rw THIS →rw Tx_out}. Abort THIS (Decision 3).
    if has_outgoing && has_incoming {
        return Some(other_commit_opnum);
    }

    // Cahill dangerous-structure check (2): a pre-existing pending
    // Tx_X became a pivot because of THIS commit (the edge updates
    // above flipped its second tag). The structure is then
    // {... →rw Tx_X →rw ...} including THIS. Per Decision 3 we
    // ALSO abort THIS (the latest committer) — undoing Tx_X is not
    // possible in the append-only versioned-storage model. Inspect
    // ONLY the pending Tx range we just touched; any change to a tag
    // happened during the loop above.
    for a_commit in concurrent.iter().map(|(k, _, _)| *k) {
        if let Some(rec) = pending_txs.get(&a_commit) {
            if rec.has_incoming_rw && rec.has_outgoing_rw {
                return Some(a_commit);
            }
        }
    }

    None
}

/// SP113 / S2.4: Window truncation. Evict every pending Tx whose
/// `commit_opnum` is older than `current_commit_opnum - max_tx_age`.
///
/// Uses `BTreeMap::split_off(&threshold)` which returns the map of
/// keys `>= threshold`. We KEEP those (recent commits within the
/// window) and DROP the lower half (old commits past the horizon).
///
/// Per Decision 5: `max_tx_age` is a fixed bound (4096 in
/// production). S2.5 watermark protocol supersedes with a dynamic
/// horizon driven by the slowest live snapshot.
pub fn prune_pending_txs(
    pending_txs: &mut BTreeMap<u64, PendingTxRecord>,
    current_commit_opnum: u64,
    max_tx_age: u64,
) {
    let threshold = current_commit_opnum.saturating_sub(max_tx_age);
    // split_off returns the right half (keys >= threshold); keep it.
    let kept = pending_txs.split_off(&threshold);
    *pending_txs = kept;
}

/// SP114 / S2.5: Prune pending_txs records whose commit_opnum is
/// strictly less than `low_water_mark`. Replaces SP113's fixed
/// MAX_TX_AGE-driven prune AT THE WATERMARK-ADVANCE SEAM ONLY.
/// (SP113's `prune_pending_txs(MAX_TX_AGE)` is RETAINED on the
/// commit-apply seam as a fallback ceiling per Decision 4.)
///
/// Correctness: a Tx evicted at low_water_mark cannot participate in
/// any dangerous structure with a still-live reader, because by
/// definition low_water_mark = min(active_snapshot_opnum) — every
/// live reader pins a snapshot >= low_water_mark; an evicted Tx's
/// commit_opnum < low_water_mark, so no live reader's snapshot is
/// older than the evicted Tx's commit. The Cahill rw-edge
/// concurrent-Tx condition (snapshot < pending.commit_opnum)
/// requires snapshot < (some pending Tx's commit_opnum); for an
/// evicted record this is provably FALSE. This is the formal
/// closure of the SP113 bounded-window false-negative.
///
/// Determinism: BTreeMap::split_off — deterministic across replicas.
pub fn prune_pending_txs_by_watermark(
    _pending_txs: &mut BTreeMap<u64, PendingTxRecord>,
    _low_water_mark: u64,
) {
    todo!("S2.5 T2: implement watermark-driven prune")
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- sorted_vec_intersects ----

    #[test]
    fn sorted_vec_intersects_both_empty_is_false() {
        let a: Vec<u32> = vec![];
        let b: Vec<u32> = vec![];
        assert!(!sorted_vec_intersects(&a, &b));
    }

    #[test]
    fn sorted_vec_intersects_one_empty_is_false() {
        assert!(!sorted_vec_intersects(&[1u32, 2, 3], &[]));
        assert!(!sorted_vec_intersects::<u32>(&[], &[1, 2, 3]));
    }

    #[test]
    fn sorted_vec_intersects_disjoint_is_false() {
        assert!(!sorted_vec_intersects(&[1u32, 3, 5], &[2u32, 4, 6]));
    }

    #[test]
    fn sorted_vec_intersects_single_overlap_is_true() {
        assert!(sorted_vec_intersects(&[1u32, 4, 9], &[2u32, 4, 6]));
    }

    #[test]
    fn sorted_vec_intersects_full_overlap_is_true() {
        assert!(sorted_vec_intersects(&[1u32, 2, 3], &[1u32, 2, 3]));
    }

    #[test]
    fn sorted_vec_intersects_first_element_overlap() {
        assert!(sorted_vec_intersects(&[1u32, 5, 9], &[1u32]));
        assert!(sorted_vec_intersects(&[1u32], &[1u32, 5, 9]));
    }

    #[test]
    fn sorted_vec_intersects_last_element_overlap() {
        assert!(sorted_vec_intersects(&[1u32, 5, 9], &[9u32]));
        assert!(sorted_vec_intersects(&[9u32], &[1u32, 5, 9]));
    }

    // ---- prune_pending_txs ----

    fn rec_at(snapshot: u64) -> PendingTxRecord {
        PendingTxRecord {
            snapshot_opnum: snapshot,
            read_set: vec![],
            write_set: vec![],
            has_incoming_rw: false,
            has_outgoing_rw: false,
        }
    }

    #[test]
    fn prune_pending_txs_evicts_below_threshold() {
        // Threshold = current(100) - max_age(10) = 90. KEEP keys >= 90.
        let mut map: BTreeMap<u64, PendingTxRecord> = BTreeMap::new();
        for k in [50u64, 80, 89, 90, 91, 100] {
            map.insert(k, rec_at(0));
        }
        prune_pending_txs(&mut map, 100, 10);
        let keys: Vec<u64> = map.keys().copied().collect();
        assert_eq!(keys, vec![90, 91, 100]);
    }

    #[test]
    fn prune_pending_txs_saturating_sub_at_zero() {
        // current(5) - max_age(100) saturates to 0. No eviction.
        let mut map: BTreeMap<u64, PendingTxRecord> = BTreeMap::new();
        for k in [1u64, 2, 3] {
            map.insert(k, rec_at(0));
        }
        prune_pending_txs(&mut map, 5, 100);
        assert_eq!(map.len(), 3);
    }

    // ---- detect_dangerous_structure ----

    #[test]
    fn detect_no_concurrent_tx_returns_none() {
        // snapshot=100, commit=101 — concurrent range is (100, 101)
        // which is empty (lo_range=101, hi_range=101 ⇒ lo>=hi).
        let mut pending = BTreeMap::new();
        let res = detect_dangerous_structure(
            &mut pending,
            100,
            &[(1u32, [1u8; 16])],
            &[(2u32, [2u8; 16])],
            101,
        );
        assert!(res.is_none());
    }

    #[test]
    fn detect_empty_pending_returns_none() {
        let mut pending = BTreeMap::new();
        let res = detect_dangerous_structure(
            &mut pending,
            0,
            &[(1u32, [1u8; 16])],
            &[(2u32, [2u8; 16])],
            100,
        );
        assert!(res.is_none());
    }
}
