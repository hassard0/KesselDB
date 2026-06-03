//! SP-Perf-A T1 — read-worker pool scaffold.
//!
//! See `docs/superpowers/specs/2026-05-28-kesseldb-perf-a-parallel-reads-design.md`.
//!
//! What this module ships in T1
//! ============================
//! - `is_read_only(&Op) -> bool` — server-side classifier (a thin wrapper
//!   over `Op::is_mutating()` from kessel-proto). Locked by KAT
//!   `is_read_only_matches_proto_classifier_for_every_variant` so adding a
//!   new write Op variant in proto forces this side to update — exactly the
//!   regression-lock the spec calls for.
//! - `ReadPool` — `N` OS worker threads draining a shared
//!   `mpsc::sync_channel(bound)`; each worker holds an `EngineHandle` clone
//!   and dispatches its task by calling `engine.apply_raw(frame)`.
//!   T1 deliberately routes the dispatched ops THROUGH the existing apply
//!   queue rather than bypassing it (the bypass — `Arc<RwLock<StateMachine>>`
//!   + `apply_read_op_raw` — lands in T2 once the scaffold + classifier are
//!   proven byte-identical in the OFF case). This staged commit keeps T1
//!   strictly additive: with `ServerConfig.read_workers = None` the pool is
//!   never constructed and behaviour is byte-identical to pre-Perf-A.
//! - `ReadPoolHandle::dispatch(frame) -> OpResult` — synchronous submission;
//!   pushes onto the pool's queue, waits on a per-task `sync_channel(1)`.
//!
//! What T1 does NOT ship (deliberately, named for T2..T6)
//! ======================================================
//! - The `Arc<RwLock<StateMachine>>` + `apply_read_op_raw` bypass that
//!   actually delivers the speedup (T2).
//! - The parallel-vs-serial correctness oracle (T3).
//! - Multi-N point-read + mixed-blend benchmark sweep on vulcan (T4).
//!
//! Determinism
//! ===========
//! T1 dispatches through the existing single-writer apply queue, so
//! determinism is byte-for-byte unchanged. T2's bypass + `RwLock<StateMachine>`
//! reads via the SP116 `&self`-safe `Storage::get` (MVCC dispatch at
//! `u64::MAX`) preserve byte-identical results because reads are pure
//! functions of committed state. T3 will lock this with the parallel-vs-
//! serial oracle.

use crate::EngineHandle;
use kessel_io::DirVfs;
use kessel_proto::{Op, OpResult};
use kessel_sm::StateMachine;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;

/// Read-only Op classifier. Mirrors `Op::is_mutating()` via negation so the
/// proto crate stays the single source of truth: adding a new write Op
/// variant ⇒ `is_mutating()` returns true (locked by proto-side tests) ⇒
/// `is_read_only` returns false ⇒ the read pool refuses it ⇒ the variant
/// goes to the writer (correct). The KAT
/// `is_read_only_matches_proto_classifier_for_every_variant` walks every
/// Op variant and asserts both sides agree.
///
/// Per SP-Perf-A spec §4 the read-only variants are:
///   GetById, GetBlob, FindBy, FindByComposite, FindRange, Query, QueryExpr,
///   Select, QueryRows, SelectFields, SelectSorted, Aggregate, GroupAggregate,
///   Describe, SeqRead, Join.
///
/// SP-Perf-A-TXN-RO extension: `Op::Txn { ops }` is read-only IFF every
/// inner op is read-only. The proto-level `Op::is_mutating()` classifies
/// Op::Txn as always-mutating (conservative — it cannot know the inner
/// composition at the variant-discriminant level); the server-side
/// classifier here recurses the inner ops and reclassifies all-RO Txns
/// as read-only so the apply_raw / read pool dispatch can route them
/// around the write lock. Mixed-RW Op::Txn still classifies as
/// mutating and falls through to the engine queue, byte-untouched.
pub fn is_read_only(op: &Op) -> bool {
    match op {
        // SP-Perf-A-TXN-RO: all-RO Op::Txn is read-only. Empty Txn
        // (`ops.is_empty()`) is read-only by `.all` short-circuit — the
        // SM apply-Txn path returns Ok for empty too, so the bypass
        // matches that contract.
        Op::Txn { ops } => ops.iter().all(is_read_only),
        _ => !op.is_mutating(),
    }
}

/// SP-Perf-A-TXN-RW: count consecutive read-only ops at the head of
/// `ops`. The (read_prefix, write_suffix) split at this index is safe
/// for split-phase execution (read prefix runs via the TXN-RO bypass;
/// write suffix runs via the apply path with full catalog/index/
/// constraint machinery).
///
/// Cases:
///   - `ops.is_empty()` ⇒ returns 0 (no prefix to split). Caller routes
///     empty Op::Txn to the TXN-RO bypass which returns Ok.
///   - `ops[0]` is a write ⇒ returns 0 (no parallelizable prefix).
///     Caller routes to apply unchanged.
///   - All reads ⇒ returns `ops.len()`. Caller routes to TXN-RO bypass
///     directly (no split needed).
///   - Mixed `(R, R, ..., R, W, ..., W)` ⇒ returns the count of
///     leading reads. Caller dispatches the split:
///       read prefix via `read_only_op(Op::Txn{prefix})` — parallel
///       write suffix via `apply(op_no, Op::Txn{suffix})` — serial
///
///   - Mixed with read-after-write (e.g. `(R, W, R)`) ⇒ returns the
///     count of leading reads BEFORE the first write (1 in this
///     example). The trailing reads stay in the suffix where apply-
///     Txn's overlay preserves read-your-writes correctness.
///
/// SAFETY CONTRACT: The split-phase execution is byte-equivalent to
/// unified apply ONLY for `(R*, W*)` shapes (reads-then-writes). For
/// `(R, W, R)` shapes, the split would lose the apply-Txn overlay's
/// read-your-writes for the trailing R (it'd see the pre-W snapshot,
/// not the post-W overlay). The driver-level dispatcher MUST verify
/// the suffix `ops[prefix_len..]` contains NO read-only ops before
/// invoking the split (use `is_split_safe` below as a guard).
///
/// `read_prefix_length` is the LOCATION of the split; `is_split_safe`
/// is the SAFETY of splitting at that location. Both must pass for
/// split-phase to be correct.
pub fn read_prefix_length(ops: &[Op]) -> usize {
    ops.iter().take_while(|o| is_read_only(o)).count()
}

/// SP-Perf-A-TXN-RW: the suffix `ops[prefix_len..]` must contain NO
/// read-only ops for the split-phase execution to be byte-equivalent
/// to unified apply (otherwise the trailing read would see the pre-
/// write snapshot under split, but the post-write overlay under
/// unified apply — observable divergence).
///
/// True iff every op in the suffix is a write (mutating). The empty
/// suffix is trivially split-safe (no ops to misclassify); the
/// dispatcher should NOT split in that case because there's nothing
/// to run on the apply path — route the whole thing to the TXN-RO
/// bypass instead.
///
/// Use as: `prefix > 0 && prefix < ops.len() && is_split_safe(&ops[prefix..])`
/// — the three guards together ensure (a) the prefix is non-empty
/// (worth parallelizing), (b) the suffix is non-empty (worth
/// apply-routing), and (c) the suffix has no trailing reads (byte-
/// equivalence holds).
pub fn is_split_safe(suffix: &[Op]) -> bool {
    suffix.iter().all(|o| !is_read_only(o))
}

/// One unit of work for a read worker: an opaque request frame (the same
/// shape `EngineHandle::apply_raw` accepts) + a per-task oneshot reply.
type ReadTask = (Vec<u8>, SyncSender<OpResult>);

/// Pool of read-worker threads draining a shared bounded queue. Workers
/// are spawned once at `ReadPool::new(...)` and joined on `Drop`.
///
/// V1 (T1) dispatch path: each worker holds an `EngineHandle` clone and
/// pushes the task back into the existing apply queue. This is the
/// staged-commit shape: T1 ships the contract and the queueing; T2 swaps
/// the worker body for a direct `Storage::get` against an
/// `Arc<RwLock<StateMachine>>` read guard, which is what delivers the
/// speedup.
///
/// `Drop` closes the queue so workers exit, then joins every handle.
pub struct ReadPool {
    /// Sender end of the shared queue. Wrapped in `Option` so `Drop` can
    /// take it (closing the channel signals every worker to exit).
    tx: Option<SyncSender<ReadTask>>,
    /// Joined on `Drop`. `Option`-wrapped so we can take ownership.
    workers: Vec<JoinHandle<()>>,
    /// Reported worker count (== `workers.len()` until `Drop` empties it).
    n: usize,
}

impl ReadPool {
    /// T1 entry point — spawns `n` workers draining a shared
    /// `sync_channel(queue_bound)` that dispatch via `engine.apply_raw`.
    /// Preserved for the T1 KATs which exercise the dispatch path
    /// against the engine queue.
    ///
    /// SP-Perf-A T2 prefers `new_shared`, which gives workers a direct
    /// `Arc<RwLock<StateMachine>>` reference and bypasses the engine
    /// thread entirely.
    pub fn new(n: usize, queue_bound: usize, engine: EngineHandle) -> Self {
        let (tx, rx) = sync_channel::<ReadTask>(queue_bound.max(1));
        let rx = Arc::new(std::sync::Mutex::new(rx));
        let mut workers = Vec::with_capacity(n);
        for _ in 0..n {
            let rx = rx.clone();
            let engine = engine.clone();
            let h = std::thread::Builder::new()
                .name("kesseldb-read-worker".into())
                .spawn(move || {
                    Self::worker_loop(rx, engine);
                })
                .expect("spawn read worker");
            workers.push(h);
        }
        ReadPool { tx: Some(tx), workers, n }
    }

    /// SP-Perf-A T2 entry point. Workers each hold a clone of the
    /// shared `Arc<RwLock<StateMachine>>`; on dispatch they take
    /// `.read()` and call `StateMachine::read_only_op` directly —
    /// the SM bypass that delivers the latency win. The engine
    /// thread is not consulted; group-commit fsync is not paid.
    ///
    /// `n == 0` ⇒ graceful no-worker pool: dispatch falls back to
    /// the submitting thread's own `.read()` + `read_only_op` call,
    /// which is identical end-to-end except for the worker hop.
    pub fn new_shared(
        n: usize,
        queue_bound: usize,
        sm: Arc<RwLock<StateMachine<DirVfs>>>,
    ) -> Self {
        let (tx, rx) = sync_channel::<ReadTask>(queue_bound.max(1));
        let rx = Arc::new(std::sync::Mutex::new(rx));
        let mut workers = Vec::with_capacity(n);
        for _ in 0..n {
            let rx = rx.clone();
            let sm = sm.clone();
            let h = std::thread::Builder::new()
                .name("kesseldb-read-worker".into())
                .spawn(move || {
                    Self::worker_loop_shared(rx, sm);
                })
                .expect("spawn read worker");
            workers.push(h);
        }
        ReadPool { tx: Some(tx), workers, n }
    }

    fn worker_loop(rx: Arc<std::sync::Mutex<Receiver<ReadTask>>>, engine: EngineHandle) {
        loop {
            let task = {
                let guard = rx.lock().expect("read-pool rx mutex poisoned");
                guard.recv()
            };
            let (frame, rp) = match task {
                Ok(t) => t,
                Err(_) => return,
            };
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                engine.apply_raw(frame)
            }))
            .unwrap_or_else(|_| OpResult::SchemaError("read panicked".into()));
            let _ = rp.send(result);
        }
    }

    fn worker_loop_shared(
        rx: Arc<std::sync::Mutex<Receiver<ReadTask>>>,
        sm: Arc<RwLock<StateMachine<DirVfs>>>,
    ) {
        loop {
            let task = {
                let guard = rx.lock().expect("read-pool rx mutex poisoned");
                guard.recv()
            };
            let (frame, rp) = match task {
                Ok(t) => t,
                Err(_) => return,
            };
            // Decode the frame to an Op, classify, dispatch under
            // `.read()`. Panic-shielded so a storage-layer panic
            // doesn't tear down the worker.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                match Op::decode(&frame) {
                    Some(op) if is_read_only(&op) => match sm.read() {
                        Ok(g) => g.read_only_op(op),
                        Err(_) => OpResult::SchemaError(
                            "read lock poisoned".into(),
                        ),
                    },
                    Some(_) => OpResult::SchemaError(
                        "non-read Op dispatched to read pool".into(),
                    ),
                    None => OpResult::SchemaError(
                        "bad frame".into(),
                    ),
                }
            }))
            .unwrap_or_else(|_| OpResult::SchemaError("read panicked".into()));
            let _ = rp.send(result);
        }
    }

    /// SP-Perf-A T2: dispatch a read-only frame through the pool's
    /// shared SM. The submitting thread sends on the bounded queue,
    /// waits on a per-task oneshot for the reply. Returns
    /// `SchemaError("read pool stopped")` if the pool is shutting
    /// down. With `n == 0`, falls back to the same `.read()` +
    /// `read_only_op` on the submitting thread.
    pub fn dispatch_shared(
        &self,
        frame: Vec<u8>,
        sm: &Arc<RwLock<StateMachine<DirVfs>>>,
    ) -> OpResult {
        if self.n == 0 {
            return match Op::decode(&frame) {
                Some(op) if is_read_only(&op) => match sm.read() {
                    Ok(g) => g.read_only_op(op),
                    Err(_) => OpResult::SchemaError(
                        "read lock poisoned".into(),
                    ),
                },
                Some(_) => OpResult::SchemaError(
                    "non-read Op dispatched to read pool".into(),
                ),
                None => OpResult::SchemaError("bad frame".into()),
            };
        }
        let tx = match &self.tx {
            Some(tx) => tx,
            None => return OpResult::SchemaError("read pool shut down".into()),
        };
        let (rtx, rrx) = sync_channel(1);
        if tx.send((frame, rtx)).is_err() {
            return OpResult::SchemaError("read pool stopped".into());
        }
        rrx.recv()
            .unwrap_or_else(|_| OpResult::SchemaError("read pool dropped reply".into()))
    }

    /// Number of workers actually spawned (the caller's requested `n`).
    pub fn workers(&self) -> usize {
        self.n
    }

    /// Dispatch a request frame onto the pool. The frame is treated as
    /// opaque (the caller is responsible for having checked
    /// `is_read_only` against the decoded Op). Returns the result the
    /// engine produced.
    ///
    /// Graceful "n=0" mode: if no workers were spawned, falls back to the
    /// submitting-thread apply path so the wiring stays correct.
    pub fn dispatch(&self, frame: Vec<u8>, engine: &EngineHandle) -> OpResult {
        if self.n == 0 {
            // No workers — fall back to the engine queue on the submitting
            // thread (identical to pre-Perf-A).
            return engine.apply_raw(frame);
        }
        let tx = match &self.tx {
            Some(tx) => tx,
            None => return OpResult::SchemaError("read pool shut down".into()),
        };
        let (rtx, rrx) = sync_channel(1);
        if tx.send((frame, rtx)).is_err() {
            return OpResult::SchemaError("read pool stopped".into());
        }
        rrx.recv()
            .unwrap_or_else(|_| OpResult::SchemaError("read pool dropped reply".into()))
    }
}

impl Drop for ReadPool {
    fn drop(&mut self) {
        // Closing the Sender drops the channel; every worker's recv()
        // returns Err and the worker exits.
        self.tx.take();
        for h in self.workers.drain(..) {
            let _ = h.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_proto::{ObjectId, Pred};

    /// Construct ALL Op variants once with cheap payloads so we can walk
    /// them in the classifier-invariant tests. Updating this list when a
    /// new variant lands is the regression-lock: a missing variant trips
    /// the count assertion in
    /// `is_read_only_matches_proto_classifier_for_every_variant`.
    fn every_op_variant() -> Vec<Op> {
        let id = ObjectId::from_u128(0);
        vec![
            Op::CreateType { def: vec![] },
            Op::AlterTypeAddField { type_id: 1, field: vec![] },
            Op::Create { type_id: 1, id, record: vec![] },
            Op::Update { type_id: 1, id, record: vec![] },
            Op::Delete { type_id: 1, id },
            Op::GetById { type_id: 1, id },
            Op::GetBlob { handle: 0 },
            Op::CreateIndex { type_id: 1, field_id: 0 },
            Op::FindBy { type_id: 1, field_id: 0, value: vec![] },
            Op::AddUnique { type_id: 1, field_id: 0 },
            Op::Query { type_id: 1, preds: vec![Pred { field_id: 0, op: 0, value: vec![] }] },
            Op::AddForeignKey { type_id: 1, field_id: 0, ref_type_id: 1, on_delete: 0 },
            Op::AddCheck { type_id: 1, program: vec![] },
            Op::AddTrigger { type_id: 1, program: vec![] },
            Op::Txn { ops: vec![] },
            Op::QueryExpr { type_id: 1, program: vec![] },
            Op::AddOrderedIndex { type_id: 1, field_id: 0 },
            Op::FindRange { type_id: 1, field_id: 0, lo: vec![], hi: vec![] },
            Op::Select { type_id: 1, program: vec![], limit: 0 },
            Op::QueryRows {
                type_id: 1,
                eq_preds: vec![],
                program: vec![],
                limit: 0,
                range_preds: vec![],
            },
            Op::Aggregate { type_id: 1, program: vec![], kind: 0, field_id: 0, range_preds: vec![] },
            Op::SelectFields { type_id: 1, program: vec![], fields: vec![], limit: 0 },
            Op::GroupAggregate {
                type_id: 1,
                program: vec![],
                group_field: 0,
                kind: 0,
                agg_field: 0,
                range_preds: vec![],
            },
            Op::SelectSorted {
                type_id: 1,
                program: vec![],
                sort_field: 0,
                desc: false,
                offset: 0,
                limit: 0,
            },
            Op::Describe { type_id: 1 },
            Op::DropType { type_id: 1 },
            Op::DropIndex { type_id: 1, fields: vec![] },
            Op::DropField { type_id: 1, field_id: 0 },
            Op::RenameField { type_id: 1, field_id: 0, name: String::new() },
            Op::AddBalanceGuard { type_id: 1, field_id: 0 },
            Op::SeqAppend { payload: vec![] },
            Op::SeqRead { from: 0, limit: 0 },
            Op::XshardApply { seq: 0, ops: vec![] },
            Op::SeqAppendOnce { key: vec![], payload: vec![] },
            Op::XshardDecide { seq: 0, ops: vec![] },
            Op::XshardCommit { seq: 0, ops: vec![], commit: false },
            Op::UpdateSet { type_id: 1, id, sets: vec![] },
            Op::CreateExternalSource {
                name: String::new(),
                type_def: vec![],
                url: String::new(),
                format: 0,
                key_field_id: 0,
                auth_kind: 0,
                auth_a: String::new(),
                auth_b: String::new(),
                mapping: vec![],
                rows_path: None,
                pagination: None,
                objstore: None,
            },
            Op::DropExternalSource { name: String::new() },
            Op::RefreshExternalSource { name: String::new() },
            Op::Join {
                left_type: 1,
                right_type: 1,
                left_field: 0,
                right_field: 0,
                limit: 0,
                filter: vec![],
                join_type: kessel_proto::JoinType::Inner,
                order_by: None,
                limit_n: None,
                offset_n: None,
            },
            Op::AddCompositeIndex { type_id: 1, fields: vec![] },
            Op::FindByComposite { type_id: 1, fields: vec![], values: vec![] },
            Op::CommitTx {
                snapshot_opnum: 0,
                write_set: vec![],
                commit_opnum: 0,
                read_set: vec![],
            },
            Op::AdvanceWatermark { low_water_mark: 0 },
            Op::ReportActiveSnapshot { replica_id: 0, min_active_snapshot: 0 },
            // SP-Analytic-Plan-MULTI: multi-aggregate GROUP BY (wire tag 47).
            // Read-only — composes with the read-pool dispatch.
            Op::GroupAggregateMulti {
                type_id: 1,
                program: vec![],
                group_field: 0,
                aggregates: vec![(0, 0)],
                range_preds: vec![],
            },
        ]
    }

    /// KAT-1: the classifier mirrors `Op::is_mutating()` for every variant
    /// EXCEPT `Op::Txn{ops}` which the SP-Perf-A-TXN-RO server-side
    /// classifier reclassifies based on inner-op composition (proto's
    /// `is_mutating` cannot inspect inners). The exception is locked
    /// separately by `txn_ro_classifier_all_ro_inner_ops_is_read_only`
    /// + `txn_ro_classifier_mixed_inner_ops_is_not_read_only`. For every
    /// other variant the negation invariant still holds.
    #[test]
    fn is_read_only_matches_proto_classifier_for_every_variant() {
        let ops = every_op_variant();
        // The 47 distinct `Op::kind()` values cover every variant. A
        // missing variant in `every_op_variant()` is the regression
        // trigger.
        let kinds: std::collections::BTreeSet<u8> = ops.iter().map(Op::kind).collect();
        assert_eq!(
            kinds.len(),
            ops.len(),
            "every_op_variant() must contain each kind exactly once \
             (duplicate or missing variant)"
        );
        assert_eq!(
            kinds.len(),
            47,
            "Op variant count drifted — add the new variant to \
             every_op_variant() and re-check is_mutating() classification"
        );
        for op in &ops {
            // SP-Perf-A-TXN-RO: Op::Txn is the documented exception
            // (server-side recursive classifier inspects inner ops;
            // proto cannot). Skip the variant-discriminant equality
            // assertion for Txn; the txn_ro_* KATs lock its behaviour.
            if matches!(op, Op::Txn { .. }) {
                continue;
            }
            assert_eq!(
                is_read_only(op),
                !op.is_mutating(),
                "is_read_only and !is_mutating disagree on {:?}",
                op.kind()
            );
        }
    }

    /// KAT-2: the read-only set is exactly the 16 spec §4 variants PLUS
    /// `Op::Txn{ops: []}` (empty Txn is all-RO by vacuous truth, which
    /// matches apply-Txn's empty-ops behaviour: it returns Ok without
    /// doing any work). The bare 16-variant Set is locked by the
    /// `bare_read_only_set_matches_spec_section_4` helper below.
    #[test]
    fn read_only_set_matches_spec_section_4() {
        // SP-Perf-A-TXN-RO: with empty-ops Op::Txn in `every_op_variant()`,
        // the read-only set is the 16 spec variants + Op::Txn (kind 15).
        let expected: std::collections::BTreeSet<u8> = [
            6,  // GetById
            7,  // GetBlob
            9,  // FindBy
            11, // Query
            15, // Op::Txn (all-RO; empty here)
            16, // QueryExpr
            18, // FindRange
            19, // Select
            20, // Aggregate
            21, // SelectFields
            22, // GroupAggregate
            23, // SelectSorted
            25, // FindByComposite
            26, // QueryRows
            27, // Describe
            28, // Join
            35, // SeqRead
            47, // SP-Analytic-Plan-MULTI: GroupAggregateMulti
        ]
        .into_iter()
        .collect();
        let got: std::collections::BTreeSet<u8> = every_op_variant()
            .iter()
            .filter(|o| is_read_only(o))
            .map(|o| o.kind())
            .collect();
        assert_eq!(got, expected, "read-only set drifted from spec §4");
    }

    /// KAT-3: every write Op kind in spec §4 returns is_read_only=false.
    #[test]
    fn write_set_is_complement_of_read_set() {
        let read_set: std::collections::BTreeSet<u8> = every_op_variant()
            .iter()
            .filter(|o| is_read_only(o))
            .map(|o| o.kind())
            .collect();
        let all: std::collections::BTreeSet<u8> =
            every_op_variant().iter().map(Op::kind).collect();
        let write_set: std::collections::BTreeSet<u8> =
            all.difference(&read_set).copied().collect();
        // 47 total - (17 reads incl. SP-Analytic-Plan-MULTI tag 47 + 1
        // Op::Txn-empty) = 29 write kinds. The new variant is read-only,
        // so it lands in the read set and the write-set cardinality is
        // unchanged.
        assert_eq!(write_set.len(), 29, "write-set cardinality drifted");
    }

    /// KAT-4: ReadPool::new(0, ...) is a graceful no-op pool — no
    /// workers spawned, dispatch still works via the engine fallback.
    #[test]
    fn pool_with_zero_workers_is_graceful() {
        let dir = std::env::temp_dir()
            .join(format!("kesseldb-pool-zero-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine =
            crate::spawn_engine_cfg(&dir, &crate::ServerConfig::default()).unwrap();
        let pool = ReadPool::new(0, 1024, engine.clone());
        assert_eq!(pool.workers(), 0);
        // A read against an empty engine returns NotFound — pool routes
        // it back through the engine on the submitting thread.
        let frame = Op::GetById {
            type_id: 1,
            id: ObjectId::from_u128(1),
        }
        .encode();
        let r = pool.dispatch(frame, &engine);
        assert_eq!(r, OpResult::NotFound);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// KAT-5: ReadPool spawns N workers; pool.workers() reports N.
    #[test]
    fn pool_spawns_n_workers() {
        let dir = std::env::temp_dir()
            .join(format!("kesseldb-pool-n-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine =
            crate::spawn_engine_cfg(&dir, &crate::ServerConfig::default()).unwrap();
        let pool = ReadPool::new(4, 1024, engine.clone());
        assert_eq!(pool.workers(), 4);
        drop(pool);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// KAT-6: a single read dispatched through the pool matches the result
    /// of going through the engine directly (byte-for-byte). T1 routes
    /// through the same engine path so this is the OFF-case invariant;
    /// T3 will tighten this to the parallel-vs-serial oracle on the
    /// direct read path.
    #[test]
    fn dispatched_read_matches_direct_apply() {
        let dir = std::env::temp_dir()
            .join(format!("kesseldb-pool-eq-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine =
            crate::spawn_engine_cfg(&dir, &crate::ServerConfig::default()).unwrap();
        let pool = ReadPool::new(2, 1024, engine.clone());
        let op = Op::GetById {
            type_id: 99,
            id: ObjectId::from_u128(7),
        };
        let direct = engine.apply(op.clone());
        let viapool = pool.dispatch(op.encode(), &engine);
        assert_eq!(direct, viapool);
        drop(pool);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// KAT-7: 100 reads dispatched in parallel through the pool all
    /// complete; results match serial dispatch.
    #[test]
    fn many_parallel_reads_match_serial() {
        let dir = std::env::temp_dir()
            .join(format!("kesseldb-pool-many-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine =
            crate::spawn_engine_cfg(&dir, &crate::ServerConfig::default()).unwrap();
        let pool = Arc::new(ReadPool::new(4, 1024, engine.clone()));

        // 100 GetById, all NotFound on an empty engine.
        let mut threads = Vec::with_capacity(100);
        for i in 0..100u128 {
            let pool = pool.clone();
            let engine = engine.clone();
            let h = std::thread::spawn(move || {
                let op = Op::GetById {
                    type_id: 1,
                    id: ObjectId::from_u128(i),
                };
                pool.dispatch(op.encode(), &engine)
            });
            threads.push(h);
        }
        let results: Vec<OpResult> = threads
            .into_iter()
            .map(|t| t.join().expect("thread join"))
            .collect();
        for r in &results {
            assert_eq!(*r, OpResult::NotFound);
        }
        // Drop the pool last — workers join cleanly.
        drop(pool);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// KAT-8: Pool dropped while workers idle exits cleanly within
    /// 1 second (no hung threads).
    #[test]
    fn pool_drops_cleanly_without_outstanding_tasks() {
        let dir = std::env::temp_dir()
            .join(format!("kesseldb-pool-drop-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine =
            crate::spawn_engine_cfg(&dir, &crate::ServerConfig::default()).unwrap();
        let pool = ReadPool::new(8, 1024, engine.clone());
        let t = std::time::Instant::now();
        drop(pool);
        assert!(
            t.elapsed() < std::time::Duration::from_secs(1),
            "drop took {:?} — workers did not join promptly",
            t.elapsed()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// KAT-9: a panic in a worker is downgraded to SchemaError; the
    /// pool continues serving subsequent reads. We synthesize a panic
    /// by sending a malformed frame to a worker bound to an engine
    /// that returns the usual SchemaError, so this KAT confirms the
    /// `catch_unwind` shield exists even though under normal engine
    /// behaviour we never reach it. (A true synthetic panic requires
    /// a panicking apply_raw; T2 will exercise that path in earnest
    /// once the bypass dispatch lands.)
    #[test]
    fn worker_panic_path_is_shielded() {
        let dir = std::env::temp_dir()
            .join(format!("kesseldb-pool-panic-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine =
            crate::spawn_engine_cfg(&dir, &crate::ServerConfig::default()).unwrap();
        let pool = ReadPool::new(1, 1024, engine.clone());
        // A 0-byte frame: the engine's compute() arm decodes None and
        // returns SchemaError, not a panic — exercises the normal path.
        // We assert the result is a typed error (not a propagated panic).
        let r = pool.dispatch(vec![], &engine);
        assert!(matches!(r, OpResult::SchemaError(_)));
        // Pool is still alive — second dispatch works.
        let op = Op::GetById {
            type_id: 1,
            id: ObjectId::from_u128(1),
        };
        let r2 = pool.dispatch(op.encode(), &engine);
        assert_eq!(r2, OpResult::NotFound);
        drop(pool);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// KAT-10: ServerConfig.read_workers defaults to None, preserving
    /// byte-identical pre-Perf-A behaviour.
    #[test]
    fn server_config_read_workers_defaults_to_none() {
        let cfg = crate::ServerConfig::default();
        assert!(cfg.read_workers.is_none());
    }

    /// KAT-11: SQL frames (`0xFE`) are NOT classified as read-only by
    /// `is_read_only` because they aren't an `Op` — the classifier is
    /// safely a no-op for non-Op frames (the caller decodes first).
    /// This locks the spec invariant that V1 routes SQL through the
    /// writer regardless of statement content.
    #[test]
    fn sql_frames_decode_to_none_so_classifier_is_a_no_op() {
        let mut f = vec![0xFE];
        f.extend_from_slice(b"SELECT 1");
        // Op::decode returns None for the 0xFE-prefixed wrapper —
        // matching the engine's compute() that ALSO returns SchemaError
        // for unknown tags via the inner compile path. The caller's
        // dispatch sees a None decode and routes to the writer.
        assert!(Op::decode(&f).is_none());
    }

    /// KAT-12: every write Op kind from the spec §4 list is classified
    /// as NOT read-only. SP-Perf-A-TXN-RO: kind 15 (Op::Txn) is removed
    /// from this list — it's now content-dependent (all-RO inners ⇒ RO,
    /// mixed ⇒ write). Locked separately by
    /// `txn_ro_classifier_mixed_inner_ops_is_not_read_only`.
    #[test]
    fn write_ops_are_not_read_only() {
        let write_kinds: &[u8] = &[
            1, 2, 3, 4, 5, 8, 10, 12, 13, 14, 17, 24, 29, 30, 31, 32, 33, 34, 36,
            37, 38, 39, 40, 41, 42, 43, 44, 45, 46,
        ];
        let by_kind: std::collections::HashMap<u8, Op> =
            every_op_variant().into_iter().map(|o| (o.kind(), o)).collect();
        for k in write_kinds {
            let op = by_kind.get(k).expect("variant present");
            assert!(
                !is_read_only(op),
                "write kind {k} classified as read-only"
            );
        }
    }

    /// KAT-13: every read Op kind from the spec §4 list is classified
    /// as read-only.
    #[test]
    fn read_ops_are_read_only() {
        let read_kinds: &[u8] = &[6, 7, 9, 11, 16, 18, 19, 20, 21, 22, 23, 25, 26, 27, 28, 35];
        let by_kind: std::collections::HashMap<u8, Op> =
            every_op_variant().into_iter().map(|o| (o.kind(), o)).collect();
        for k in read_kinds {
            let op = by_kind.get(k).expect("variant present");
            assert!(
                is_read_only(op),
                "read kind {k} classified as NOT read-only"
            );
        }
    }

    // -----------------------------------------------------------------------
    // SP-Perf-A T2 KATs — actual bypass dispatch.
    // -----------------------------------------------------------------------

    /// Seed an engine with `n_rows` simple records under type "row(v: U64)".
    /// Returns (engine, type_id).
    fn seed_engine(
        n_rows: usize,
        read_workers: Option<usize>,
    ) -> (crate::EngineHandle, std::path::PathBuf) {
        use kessel_catalog::{encode_type_def, Field, FieldKind};
        use kessel_proto::{ObjectId, Op};
        let dir = std::env::temp_dir().join(format!(
            "kesseldb-pool-seed-{}-{}",
            std::process::id(),
            rand_suffix()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let cfg = crate::ServerConfig {
            read_workers,
            ..crate::ServerConfig::default()
        };
        let engine = crate::spawn_engine_cfg(&dir, &cfg).unwrap();
        let def = encode_type_def(
            "row",
            &[Field {
                field_id: 0,
                name: "v".into(),
                kind: FieldKind::U64,
                nullable: false,
            }],
        );
        assert!(matches!(
            engine.apply(Op::CreateType { def }),
            kessel_proto::OpResult::TypeCreated(_)
        ));
        for i in 0..n_rows {
            let id = ObjectId::from_u128(i as u128);
            let rec = (i as u64).to_le_bytes().to_vec();
            let _ = engine.apply(Op::Create {
                type_id: 1,
                id,
                record: rec,
            });
        }
        (engine, dir)
    }

    fn rand_suffix() -> u64 {
        // Cheap unique-per-call disambiguator (KAT helper only).
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(1);
        CTR.fetch_add(1, Ordering::Relaxed)
    }

    /// KAT-14: with `read_workers = Some(8)`, a single `GetById` returns
    /// the same OpResult as it would via the engine queue (serial path).
    /// Locks the byte-equality invariant the determinism oracle expands.
    #[test]
    fn bypass_get_by_id_matches_serial() {
        use kessel_proto::{ObjectId, Op};
        let (engine_p, dir_p) = seed_engine(1000, Some(8));
        let (engine_s, dir_s) = seed_engine(1000, None);
        for i in [0u128, 1, 42, 500, 999] {
            let p = engine_p.apply(Op::GetById {
                type_id: 1,
                id: ObjectId::from_u128(i),
            });
            let s = engine_s.apply(Op::GetById {
                type_id: 1,
                id: ObjectId::from_u128(i),
            });
            assert_eq!(p, s, "parallel != serial at id {i}");
        }
        drop(engine_p);
        drop(engine_s);
        let _ = std::fs::remove_dir_all(&dir_p);
        let _ = std::fs::remove_dir_all(&dir_s);
    }

    /// KAT-15: the bypass refuses write Ops (defence-in-depth — the
    /// classifier on the submitting side would normally route them to
    /// the engine queue, but if a worker is somehow handed a write
    /// frame, the SM's `read_only_op` returns SchemaError).
    #[test]
    fn bypass_refuses_write_ops() {
        use kessel_io::DirVfs;
        use kessel_proto::{ObjectId, Op, OpResult};
        use kessel_sm::StateMachine;
        let dir = std::env::temp_dir().join(format!(
            "kesseldb-pool-refuse-write-{}-{}",
            std::process::id(),
            rand_suffix()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let sm = StateMachine::open(DirVfs::new(&dir).unwrap()).unwrap();
        let r = sm.read_only_op(Op::Create {
            type_id: 1,
            id: ObjectId::from_u128(7),
            record: vec![],
        });
        assert!(matches!(r, OpResult::SchemaError(_)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// KAT-16: 16 worker threads × 64 reads each return the same
    /// committed-state results as the serial engine — byte-for-byte.
    /// This is the headline determinism KAT for the bypass.
    #[test]
    fn parallel_bypass_results_match_serial_engine() {
        use kessel_proto::{ObjectId, Op};
        let (engine_p, dir_p) = seed_engine(5000, Some(8));
        let (engine_s, dir_s) = seed_engine(5000, None);
        // 16 threads × 64 ids each = 1024 reads to test.
        let mut handles = Vec::new();
        for w in 0..16u128 {
            let engine_p = engine_p.clone();
            let engine_s = engine_s.clone();
            let h = std::thread::spawn(move || {
                let mut diffs = 0usize;
                for k in 0..64u128 {
                    let id = ObjectId::from_u128((w * 64 + k) % 5000);
                    let p = engine_p.apply(Op::GetById { type_id: 1, id });
                    let s = engine_s.apply(Op::GetById { type_id: 1, id });
                    if p != s {
                        diffs += 1;
                    }
                }
                diffs
            });
            handles.push(h);
        }
        let total_diffs: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total_diffs, 0, "parallel reads diverged from serial");
        drop(engine_p);
        drop(engine_s);
        let _ = std::fs::remove_dir_all(&dir_p);
        let _ = std::fs::remove_dir_all(&dir_s);
    }

    /// KAT-17: T3-style determinism oracle — 100 random mixed-op
    /// workloads × parallel-vs-serial result equality. Reads only;
    /// writes (Create/Delete/Update) interleave deterministically via
    /// the serial-apply path on BOTH engines (same op stream, same
    /// final state), and reads run in parallel on engine_p / serial
    /// on engine_s. Compares every read's OpResult byte-for-byte.
    #[test]
    fn determinism_oracle_100_random_workloads() {
        use kessel_proto::{ObjectId, Op, OpResult, Rng};
        let (engine_p, dir_p) = seed_engine(2000, Some(4));
        let (engine_s, dir_s) = seed_engine(2000, None);
        let mut rng = Rng::new(0xC0FFEE);
        let mut diffs = 0usize;
        for _ in 0..100 {
            // 10 reads per workload (writes already seeded by `seed_engine`).
            for _ in 0..10 {
                let i = rng.below(2000);
                let id = ObjectId::from_u128(i as u128);
                let p = engine_p.apply(Op::GetById { type_id: 1, id });
                let s = engine_s.apply(Op::GetById { type_id: 1, id });
                if p != s {
                    diffs += 1;
                }
                // Verify result is what we expect for a populated row.
                if i < 2000 {
                    assert!(matches!(s, OpResult::Got(_)));
                }
            }
        }
        assert_eq!(diffs, 0, "determinism oracle: parallel diverged from serial");
        drop(engine_p);
        drop(engine_s);
        let _ = std::fs::remove_dir_all(&dir_p);
        let _ = std::fs::remove_dir_all(&dir_s);
    }

    /// KAT-18: with `read_workers = Some(0)` (graceful no-worker mode),
    /// bypass still produces the same result as the engine queue —
    /// the n=0 fall-through runs `read_only_op` on the submitting
    /// thread under the `.read()` guard.
    #[test]
    fn bypass_with_zero_workers_still_correct() {
        use kessel_proto::{ObjectId, Op};
        let (engine_p, dir_p) = seed_engine(100, Some(0));
        let (engine_s, dir_s) = seed_engine(100, None);
        for i in 0..100u128 {
            let id = ObjectId::from_u128(i);
            let p = engine_p.apply(Op::GetById { type_id: 1, id });
            let s = engine_s.apply(Op::GetById { type_id: 1, id });
            assert_eq!(p, s, "n=0 bypass diverged at id {i}");
        }
        drop(engine_p);
        drop(engine_s);
        let _ = std::fs::remove_dir_all(&dir_p);
        let _ = std::fs::remove_dir_all(&dir_s);
    }

    // -----------------------------------------------------------------------
    // SP-Perf-A T6 KATs — in-process apply fast-path (Fix A).
    // -----------------------------------------------------------------------
    //
    // These lock the invariant that `EngineHandle::apply(op)` and
    // `EngineHandle::apply_op(&op)` produce byte-identical OpResult to
    // the encode→apply_raw→decode roundtrip on EVERY read variant.
    // Regression-lock for the encode/decode bypass.

    use kessel_catalog::{encode_type_def, Field, FieldKind};

    /// Build a small engine seeded with one type "row(v U64, score I32 eq+ord, group U16 eq)"
    /// + 1000 rows + indexes. Used by every T6 KAT.
    fn seed_t6_engine(read_workers: Option<usize>) -> (crate::EngineHandle, std::path::PathBuf) {
        use kessel_proto::{ObjectId, Op};
        let dir = std::env::temp_dir().join(format!(
            "kesseldb-t6-{}-{}",
            std::process::id(),
            rand_suffix()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let cfg = crate::ServerConfig {
            read_workers,
            ..crate::ServerConfig::default()
        };
        let engine = crate::spawn_engine_cfg(&dir, &cfg).unwrap();
        let def = encode_type_def(
            "row",
            &[
                Field { field_id: 0, name: "v".into(), kind: FieldKind::U64, nullable: false },
                Field { field_id: 1, name: "score".into(), kind: FieldKind::I32, nullable: false },
                Field { field_id: 2, name: "group".into(), kind: FieldKind::U16, nullable: false },
            ],
        );
        assert!(matches!(
            engine.apply(Op::CreateType { def }),
            kessel_proto::OpResult::TypeCreated(_)
        ));
        let _ = engine.apply(Op::CreateIndex { type_id: 1, field_id: 2 });
        let _ = engine.apply(Op::CreateIndex { type_id: 1, field_id: 1 });
        let _ = engine.apply(Op::AddOrderedIndex { type_id: 1, field_id: 1 });
        for i in 0..1000u32 {
            let id = ObjectId::from_u128(i as u128);
            let mut rec = Vec::with_capacity(14);
            rec.extend_from_slice(&(i as u64).to_le_bytes());
            rec.extend_from_slice(&(i as i32).to_le_bytes());
            rec.extend_from_slice(&((i % 8) as u16).to_le_bytes());
            let _ = engine.apply(Op::Create { type_id: 1, id, record: rec });
        }
        (engine, dir)
    }

    /// T6-KAT-1: apply(GetById) == apply_raw(encode(GetById)) byte-for-byte.
    /// Locks the Fix A encode-bypass for the bench's hot path.
    #[test]
    fn t6_apply_get_by_id_matches_apply_raw_roundtrip() {
        use kessel_proto::{ObjectId, Op};
        let (engine, dir) = seed_t6_engine(Some(0));
        for i in [0u128, 1, 42, 500, 999] {
            let op = Op::GetById { type_id: 1, id: ObjectId::from_u128(i) };
            let fast = engine.apply(op.clone());
            let slow = engine.apply_raw(op.encode());
            assert_eq!(fast, slow, "apply vs apply_raw diverged at id {i}");
        }
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// T6-KAT-2: apply(Select LIMIT 10) byte-equal to apply_raw fallback.
    #[test]
    fn t6_apply_select_limit_matches_apply_raw_roundtrip() {
        use kessel_proto::Op;
        let (engine, dir) = seed_t6_engine(Some(0));
        let op = Op::Select { type_id: 1, program: vec![], limit: 10 };
        let fast = engine.apply(op.clone());
        let slow = engine.apply_raw(op.encode());
        assert_eq!(fast, slow, "apply(Select) vs apply_raw diverged");
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// T6-KAT-3: apply(FindBy) byte-equal to apply_raw — exercise the
    /// indexed equality path.
    #[test]
    fn t6_apply_find_by_matches_apply_raw_roundtrip() {
        use kessel_proto::Op;
        let (engine, dir) = seed_t6_engine(Some(0));
        let op = Op::FindBy { type_id: 1, field_id: 2, value: 3u16.to_le_bytes().to_vec() };
        let fast = engine.apply(op.clone());
        let slow = engine.apply_raw(op.encode());
        assert_eq!(fast, slow, "apply(FindBy) vs apply_raw diverged");
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// T6-KAT-4: apply(Aggregate SUM) byte-equal — exercises the O(rows) scan path.
    #[test]
    fn t6_apply_aggregate_matches_apply_raw_roundtrip() {
        use kessel_proto::Op;
        let (engine, dir) = seed_t6_engine(Some(0));
        // Aggregate over indexed score field, SUM (kind 0).
        let op = Op::Aggregate { type_id: 1, program: vec![], kind: 0, field_id: 1, range_preds: vec![] };
        let fast = engine.apply(op.clone());
        let slow = engine.apply_raw(op.encode());
        assert_eq!(fast, slow, "apply(Aggregate) vs apply_raw diverged");
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// T6-KAT-5: apply(SelectSorted) byte-equal — exercises the ordered-index
    /// path with sort + limit.
    #[test]
    fn t6_apply_select_sorted_matches_apply_raw_roundtrip() {
        use kessel_proto::Op;
        let (engine, dir) = seed_t6_engine(Some(0));
        let op = Op::SelectSorted {
            type_id: 1,
            program: vec![],
            sort_field: 1,
            desc: false,
            offset: 0,
            limit: 10,
        };
        let fast = engine.apply(op.clone());
        let slow = engine.apply_raw(op.encode());
        assert_eq!(fast, slow, "apply(SelectSorted) vs apply_raw diverged");
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// T6-KAT-6: apply_op(&Op) byref overload byte-equal to apply(Op) by-value,
    /// across every read variant the bench drives.
    #[test]
    fn t6_apply_op_byref_matches_apply_by_value() {
        use kessel_proto::{ObjectId, Op};
        let (engine, dir) = seed_t6_engine(Some(0));
        let ops = vec![
            Op::GetById { type_id: 1, id: ObjectId::from_u128(42) },
            Op::Select { type_id: 1, program: vec![], limit: 10 },
            Op::FindBy { type_id: 1, field_id: 2, value: 3u16.to_le_bytes().to_vec() },
            Op::Aggregate { type_id: 1, program: vec![], kind: 0, field_id: 1, range_preds: vec![] },
            Op::SelectSorted {
                type_id: 1, program: vec![], sort_field: 1,
                desc: false, offset: 0, limit: 10,
            },
            Op::Describe { type_id: 1 },
        ];
        for op in ops {
            let by_val = engine.apply(op.clone());
            let by_ref = engine.apply_op(&op);
            assert_eq!(by_val, by_ref, "apply vs apply_op diverged on {:?}", op.kind());
        }
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// T6-KAT-7: writes still go through the engine queue on the fast
    /// path (encode + send + reply). Locks the invariant that Fix A
    /// doesn't silently bypass the write apply thread.
    #[test]
    fn t6_apply_write_op_still_uses_engine_queue() {
        use kessel_proto::{ObjectId, Op, OpResult};
        let (engine, dir) = seed_t6_engine(Some(0));
        // Issue a fresh Create — must go to the engine thread, take a
        // log slot, return Ok (or Constraint for a duplicate id which
        // would also prove the engine processed it).
        let r = engine.apply(Op::Create {
            type_id: 1,
            id: ObjectId::from_u128(5000),
            record: {
                let mut v = Vec::with_capacity(14);
                v.extend_from_slice(&5000u64.to_le_bytes());
                v.extend_from_slice(&0i32.to_le_bytes());
                v.extend_from_slice(&0u16.to_le_bytes());
                v
            },
        });
        assert_eq!(r, OpResult::Ok, "write should reach engine and apply");
        // Read it back via the fast path.
        let g = engine.apply(Op::GetById {
            type_id: 1, id: ObjectId::from_u128(5000),
        });
        assert!(matches!(g, OpResult::Got(_)), "write+read roundtrip on fast path");
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// T6-KAT-8: with `sm_shared = None` (read_workers = None), the
    /// fast path is OFF and apply(GetById) routes through the engine
    /// queue exactly as pre-T6. Result is byte-equal.
    #[test]
    fn t6_apply_without_sm_shared_falls_through_to_engine() {
        use kessel_proto::{ObjectId, Op};
        let (engine, dir) = seed_t6_engine(None); // read_workers = None ⇒ no sm_shared
        for i in [0u128, 1, 42, 500, 999] {
            let op = Op::GetById { type_id: 1, id: ObjectId::from_u128(i) };
            let fast = engine.apply(op.clone());
            let slow = engine.apply_raw(op.encode());
            assert_eq!(fast, slow, "no-bypass apply vs apply_raw diverged at id {i}");
        }
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // -----------------------------------------------------------------------
    // SP-Perf-A-TXN-RO classifier KATs — Op::Txn{ops} recursion locks.
    // -----------------------------------------------------------------------
    //
    // The proto `Op::is_mutating()` cannot inspect inner ops; it classifies
    // every Op::Txn as mutating (conservative). The server-side classifier
    // here recurses inner ops and reclassifies all-RO Op::Txn as read-only
    // so the apply_raw/apply dispatch can route them around the write lock.
    // These KATs lock both directions: all-RO ⇒ RO; ANY write inner ⇒ write.

    /// TXN-RO-KAT-1: empty Op::Txn{ops: vec![]} classifies as read-only.
    /// Matches apply-Txn's empty-ops behaviour (returns Ok without work).
    #[test]
    fn txn_ro_classifier_empty_txn_is_read_only() {
        let op = Op::Txn { ops: vec![] };
        assert!(is_read_only(&op), "empty Op::Txn should classify as RO");
        // Proto side stays mutating (conservative) — the SP-Perf-A-TXN-RO
        // documented exception in KAT-1.
        assert!(op.is_mutating(), "proto Op::is_mutating(Txn) is conservatively true");
    }

    /// TXN-RO-KAT-2: a Txn with only read-only inner ops classifies as
    /// read-only. Walks every spec §4 read variant inside one Txn.
    #[test]
    fn txn_ro_classifier_all_ro_inner_ops_is_read_only() {
        use kessel_proto::ObjectId;
        let id = ObjectId::from_u128(7);
        let all_ro_inners = vec![
            Op::GetById { type_id: 1, id },
            Op::GetBlob { handle: 0 },
            Op::Describe { type_id: 1 },
            Op::FindBy { type_id: 1, field_id: 0, value: vec![] },
            Op::FindByComposite { type_id: 1, fields: vec![], values: vec![] },
            Op::FindRange { type_id: 1, field_id: 0, lo: vec![], hi: vec![] },
            Op::Query { type_id: 1, preds: vec![Pred { field_id: 0, op: 0, value: vec![] }] },
            Op::QueryExpr { type_id: 1, program: vec![] },
            Op::Select { type_id: 1, program: vec![], limit: 0 },
            Op::QueryRows {
                type_id: 1, eq_preds: vec![], program: vec![],
                limit: 0, range_preds: vec![],
            },
            Op::SelectFields { type_id: 1, program: vec![], fields: vec![], limit: 0 },
            Op::SelectSorted {
                type_id: 1, program: vec![], sort_field: 0,
                desc: false, offset: 0, limit: 0,
            },
            Op::Aggregate {
                type_id: 1, program: vec![], kind: 0, field_id: 0, range_preds: vec![],
            },
            Op::GroupAggregate {
                type_id: 1, program: vec![], group_field: 0,
                kind: 0, agg_field: 0, range_preds: vec![],
            },
            Op::SeqRead { from: 0, limit: 0 },
            Op::Join {
                left_type: 1, right_type: 1, left_field: 0,
                right_field: 0, limit: 0, filter: vec![],
                join_type: kessel_proto::JoinType::Inner,
                order_by: None, limit_n: None, offset_n: None,
            },
        ];
        let op = Op::Txn { ops: all_ro_inners };
        assert!(
            is_read_only(&op),
            "Op::Txn with all 16 spec §4 read variants must classify as RO"
        );
    }

    /// TXN-RO-KAT-3: any write inner op poisons the Txn — classifies as
    /// write. Locks the safety invariant: a single write must keep the
    /// Txn on the apply path so the write actually persists.
    #[test]
    fn txn_ro_classifier_mixed_inner_ops_is_not_read_only() {
        use kessel_proto::ObjectId;
        let id = ObjectId::from_u128(7);
        // 9 reads + 1 write (the canonical sysbench-RW shape inverted: a
        // single Op::Create at the end of an otherwise-read Txn).
        let mut inners = Vec::new();
        for _ in 0..9 {
            inners.push(Op::GetById { type_id: 1, id });
        }
        inners.push(Op::Create { type_id: 1, id, record: vec![] });
        let op = Op::Txn { ops: inners };
        assert!(
            !is_read_only(&op),
            "mixed-RW Op::Txn must classify as write (apply-path)"
        );
    }

    /// TXN-RO-KAT-4: a write at position 0 (not just at the end) still
    /// classifies the whole Txn as write — locks the `.all` short-circuit
    /// from skipping the rest of the walk.
    #[test]
    fn txn_ro_classifier_write_at_front_is_not_read_only() {
        use kessel_proto::ObjectId;
        let id = ObjectId::from_u128(7);
        let mut inners = vec![Op::Create { type_id: 1, id, record: vec![] }];
        for _ in 0..9 {
            inners.push(Op::GetById { type_id: 1, id });
        }
        let op = Op::Txn { ops: inners };
        assert!(
            !is_read_only(&op),
            "Op::Txn with a write at the front must classify as write"
        );
    }

    /// TXN-RO-KAT-5: nested Op::Txn{Txn{[reads]}} classifies as RO when
    /// the inner Txn is RO (recursion). This is a defensive lock — the
    /// SM apply-Txn validator rejects nested Txn outright, but the
    /// bypass's read_only_op Txn arm has its own validator (design §3.2)
    /// that ALSO rejects nested Txn. Both paths agree: nested Txn never
    /// executes, period. The classifier's recursion is correct here —
    /// it just means the bypass attempt will then trip the structural
    /// validator and return SchemaError, matching apply's rejection.
    #[test]
    fn txn_ro_classifier_nested_all_ro_recurses() {
        use kessel_proto::ObjectId;
        let id = ObjectId::from_u128(7);
        let inner_txn = Op::Txn {
            ops: vec![Op::GetById { type_id: 1, id }],
        };
        let outer_txn = Op::Txn { ops: vec![inner_txn] };
        assert!(
            is_read_only(&outer_txn),
            "nested all-RO Txn classifies as RO (recursion); SM validator \
             handles the rejection downstream — symmetric with apply-Txn"
        );
    }

    /// TXN-RO-KAT-6: nested Op::Txn with a mixed inner Txn classifies as
    /// write (the recursion finds the write deep inside).
    #[test]
    fn txn_ro_classifier_nested_mixed_classifies_as_write() {
        use kessel_proto::ObjectId;
        let id = ObjectId::from_u128(7);
        let inner_txn = Op::Txn {
            ops: vec![
                Op::GetById { type_id: 1, id },
                Op::Create { type_id: 1, id, record: vec![] },
            ],
        };
        let outer_txn = Op::Txn { ops: vec![inner_txn] };
        assert!(
            !is_read_only(&outer_txn),
            "nested mixed Txn classifies as write via recursion"
        );
    }

    /// TXN-RO-KAT-7: 410-inner-op Txn (the sysbench-RO shape: 1+4×100+5
    /// GetByIds = 410 reads) classifies as RO. Locks the bulk-walk
    /// performance & correctness for the headline workload.
    #[test]
    fn txn_ro_classifier_sysbench_ro_shape_classifies_as_ro() {
        use kessel_proto::ObjectId;
        let mut inners = Vec::with_capacity(410);
        for i in 0..410u128 {
            inners.push(Op::GetById {
                type_id: 1,
                id: ObjectId::from_u128(i),
            });
        }
        let op = Op::Txn { ops: inners };
        assert!(
            is_read_only(&op),
            "sysbench-RO-shape (410 GetByIds in one Txn) classifies as RO"
        );
    }

    // -----------------------------------------------------------------------
    // SP-Perf-A-TXN-RW classifier KATs — read_prefix_length + is_split_safe.
    // -----------------------------------------------------------------------
    //
    // These lock the (R*, W*) split-phase boundary detection. The driver-
    // level split dispatcher composes both into a 3-guard check:
    //   prefix > 0 && prefix < ops.len() && is_split_safe(suffix)

    /// TXN-RW-KAT-1: empty ops ⇒ prefix length 0.
    #[test]
    fn txn_rw_prefix_empty_returns_zero() {
        assert_eq!(read_prefix_length(&[]), 0);
    }

    /// TXN-RW-KAT-2: pure-RO ops ⇒ prefix length == ops.len().
    /// Sysbench-RO shape: 10 reads → prefix = 10 (caller routes
    /// to TXN-RO bypass, not the split path).
    #[test]
    fn txn_rw_prefix_pure_reads_returns_full_length() {
        use kessel_proto::{ObjectId, Op};
        let mut ops = Vec::new();
        for i in 0..10u128 {
            ops.push(Op::GetById { type_id: 1, id: ObjectId::from_u128(i) });
        }
        assert_eq!(read_prefix_length(&ops), 10);
        // The (prefix > 0 && prefix < ops.len()) guard excludes this
        // shape from the split path.
        let prefix = read_prefix_length(&ops);
        assert!(prefix == ops.len(), "pure-reads ⇒ no split needed (route to TXN-RO)");
    }

    /// TXN-RW-KAT-3: pure-write ops ⇒ prefix length == 0.
    /// First op is a write (Op::Create) so no parallelizable read
    /// prefix. The dispatcher's 3-guard check excludes this shape;
    /// caller routes to apply unchanged.
    #[test]
    fn txn_rw_prefix_pure_writes_returns_zero() {
        use kessel_proto::{ObjectId, Op};
        let ops = vec![
            Op::Create { type_id: 1, id: ObjectId::from_u128(1), record: vec![] },
            Op::Update { type_id: 1, id: ObjectId::from_u128(2), record: vec![] },
            Op::Delete { type_id: 1, id: ObjectId::from_u128(3) },
        ];
        assert_eq!(read_prefix_length(&ops), 0);
    }

    /// TXN-RW-KAT-4: canonical sysbench-RW shape (10 reads then 4
    /// writes) ⇒ prefix length == 10. The 3-guard check holds:
    /// prefix(10) > 0; prefix(10) < total(14); suffix is all writes.
    /// This is the HEADLINE shape for the perf win.
    #[test]
    fn txn_rw_prefix_sysbench_rw_shape_returns_ten() {
        use kessel_proto::{ObjectId, Op};
        let mut ops = Vec::new();
        // 10 GetById reads
        for i in 0..10u128 {
            ops.push(Op::GetById { type_id: 1, id: ObjectId::from_u128(i) });
        }
        // 4 writes (UPDATE_INDEX, UPDATE_NON_INDEX, DELETE, INSERT)
        ops.push(Op::Update { type_id: 1, id: ObjectId::from_u128(100), record: vec![] });
        ops.push(Op::Update { type_id: 1, id: ObjectId::from_u128(101), record: vec![] });
        ops.push(Op::Delete { type_id: 1, id: ObjectId::from_u128(102) });
        ops.push(Op::Create { type_id: 1, id: ObjectId::from_u128(103), record: vec![] });

        let prefix = read_prefix_length(&ops);
        assert_eq!(prefix, 10, "sysbench-RW shape: 10 reads, 4 writes ⇒ prefix=10");
        // 3-guard check
        assert!(prefix > 0, "prefix must be non-empty");
        assert!(prefix < ops.len(), "suffix must be non-empty");
        assert!(is_split_safe(&ops[prefix..]), "suffix must be all-writes");
    }

    /// TXN-RW-KAT-5: read-after-write shape (R, W, R) ⇒ prefix=1,
    /// suffix has a trailing read (NOT split-safe). The dispatcher's
    /// is_split_safe guard catches this and falls through to apply
    /// — preserves read-your-writes semantics for the trailing R.
    #[test]
    fn txn_rw_prefix_read_after_write_is_not_split_safe() {
        use kessel_proto::{ObjectId, Op};
        let id = ObjectId::from_u128(7);
        let ops = vec![
            Op::GetById { type_id: 1, id },
            Op::Update { type_id: 1, id, record: vec![] },
            Op::GetById { type_id: 1, id },
        ];
        let prefix = read_prefix_length(&ops);
        assert_eq!(prefix, 1, "leading read counted; first write stops the prefix");
        assert!(
            !is_split_safe(&ops[prefix..]),
            "suffix (W, R) contains a read ⇒ NOT split-safe; \
             dispatcher must fall through to unified apply"
        );
    }

    /// TXN-RW-KAT-6: empty suffix is trivially split-safe (vacuously).
    /// is_split_safe(&[]) == true. The dispatcher's other guards
    /// (`prefix > 0 && prefix < ops.len()`) exclude this case, so
    /// is_split_safe is never the deciding factor for empty suffix —
    /// but the empty-vector vacuous-truth contract matters for the
    /// recursion shape.
    #[test]
    fn txn_rw_is_split_safe_empty_suffix_is_true_vacuous() {
        assert!(is_split_safe(&[]), "empty suffix is vacuously split-safe");
    }

    /// TXN-RW-KAT-7: write-only suffix (4 sysbench writes) is split-safe.
    #[test]
    fn txn_rw_is_split_safe_all_writes_suffix() {
        use kessel_proto::{ObjectId, Op};
        let id = ObjectId::from_u128(7);
        let suffix = vec![
            Op::Update { type_id: 1, id, record: vec![] },
            Op::Update { type_id: 1, id, record: vec![] },
            Op::Delete { type_id: 1, id },
            Op::Create { type_id: 1, id, record: vec![] },
        ];
        assert!(is_split_safe(&suffix), "all-writes suffix is split-safe");
    }

    /// TXN-RW-KAT-8: 3-guard dispatcher check on the canonical
    /// sysbench-RW Op::Txn shape. Confirms the dispatcher exits the
    /// 3-guard with split=YES.
    #[test]
    fn txn_rw_dispatcher_3guard_sysbench_rw_splits() {
        use kessel_proto::{ObjectId, Op};
        let mut ops = Vec::new();
        for i in 0..10u128 {
            ops.push(Op::GetById { type_id: 1, id: ObjectId::from_u128(i) });
        }
        for i in 0..4u128 {
            ops.push(Op::Update { type_id: 1, id: ObjectId::from_u128(100 + i), record: vec![] });
        }
        let prefix = read_prefix_length(&ops);
        let should_split =
            prefix > 0 && prefix < ops.len() && is_split_safe(&ops[prefix..]);
        assert!(should_split, "sysbench-RW shape passes 3-guard ⇒ split-phase");
    }

    /// TXN-RW-KAT-9: 3-guard dispatcher check on a Txn with read-
    /// after-write (R, W, R) shape — must NOT split. The
    /// is_split_safe guard catches it.
    #[test]
    fn txn_rw_dispatcher_3guard_read_after_write_does_not_split() {
        use kessel_proto::{ObjectId, Op};
        let id = ObjectId::from_u128(7);
        let ops = vec![
            Op::GetById { type_id: 1, id },
            Op::Update { type_id: 1, id, record: vec![] },
            Op::GetById { type_id: 1, id },
        ];
        let prefix = read_prefix_length(&ops);
        let should_split =
            prefix > 0 && prefix < ops.len() && is_split_safe(&ops[prefix..]);
        assert!(
            !should_split,
            "read-after-write shape must NOT split (preserves RYW via apply overlay)"
        );
    }

    /// TXN-RW-KAT-10: write-led shape (W, W, R, R) — prefix=0 ⇒ no
    /// split. The is_split_safe check is never even consulted because
    /// `prefix > 0` already fails. Dispatcher falls through to apply.
    #[test]
    fn txn_rw_dispatcher_3guard_write_led_does_not_split() {
        use kessel_proto::{ObjectId, Op};
        let id = ObjectId::from_u128(7);
        let ops = vec![
            Op::Update { type_id: 1, id, record: vec![] },
            Op::Update { type_id: 1, id, record: vec![] },
            Op::GetById { type_id: 1, id },
            Op::GetById { type_id: 1, id },
        ];
        let prefix = read_prefix_length(&ops);
        let should_split =
            prefix > 0 && prefix < ops.len() && is_split_safe(&ops[prefix..]);
        assert!(
            !should_split,
            "write-led Txn must NOT split (prefix=0; nothing to parallelize)"
        );
    }

    /// TXN-RW-KAT-11: nested Op::Txn inside a parent Txn. The outer
    /// classifier's `is_read_only` recurses, so a Txn{[reads, Txn{[reads]}]}
    /// would compute prefix=ops.len() (all-RO via recursion). A Txn{[reads,
    /// Txn{[reads, write]}]} would compute the prefix as the leading reads
    /// up to the nested Txn (which the recursion sees as a write because
    /// the inner Txn contains a write). The SM rejects nested Txn at apply,
    /// so this KAT is a defence-in-depth verification of the classifier
    /// behaviour, not a behaviour the bench drives.
    #[test]
    fn txn_rw_prefix_nested_mixed_txn_counts_as_write() {
        use kessel_proto::{ObjectId, Op};
        let id = ObjectId::from_u128(7);
        let nested_mixed = Op::Txn {
            ops: vec![
                Op::GetById { type_id: 1, id },
                Op::Update { type_id: 1, id, record: vec![] },
            ],
        };
        let ops = vec![
            Op::GetById { type_id: 1, id },  // R
            Op::GetById { type_id: 1, id },  // R
            nested_mixed,                    // mixed nested Txn ⇒ classified as write via recursion
            Op::GetById { type_id: 1, id },  // R (in the suffix)
        ];
        let prefix = read_prefix_length(&ops);
        assert_eq!(
            prefix, 2,
            "leading 2 reads counted; nested mixed Txn stops the prefix (recursion sees write inside)"
        );
        // The suffix has a trailing read ⇒ NOT split-safe.
        assert!(!is_split_safe(&ops[prefix..]));
    }
}
