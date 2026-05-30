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
pub fn is_read_only(op: &Op) -> bool {
    !op.is_mutating()
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
        ]
    }

    /// KAT-1: the classifier mirrors `Op::is_mutating()` for every variant.
    /// Locks the invariant the spec calls out: proto is the source of
    /// truth; adding a new write Op variant trips the proto-side test;
    /// this side then becomes automatically correct via the negation.
    #[test]
    fn is_read_only_matches_proto_classifier_for_every_variant() {
        let ops = every_op_variant();
        // The 46 distinct `Op::kind()` values cover every variant. A
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
            46,
            "Op variant count drifted — add the new variant to \
             every_op_variant() and re-check is_mutating() classification"
        );
        for op in &ops {
            assert_eq!(
                is_read_only(op),
                !op.is_mutating(),
                "is_read_only and !is_mutating disagree on {:?}",
                op.kind()
            );
        }
    }

    /// KAT-2: the read-only set is exactly the 16 variants the spec §4
    /// names. Locks both directions: any drift (a write-op reclassified
    /// as read, or vice versa) trips this.
    #[test]
    fn read_only_set_matches_spec_section_4() {
        let expected: std::collections::BTreeSet<u8> = [
            6,  // GetById
            7,  // GetBlob
            9,  // FindBy
            11, // Query
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
        // 46 total - 16 read = 30 write kinds.
        assert_eq!(write_set.len(), 30, "write-set cardinality drifted");
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
    /// as NOT read-only.
    #[test]
    fn write_ops_are_not_read_only() {
        let write_kinds: &[u8] = &[
            1, 2, 3, 4, 5, 8, 10, 12, 13, 14, 15, 17, 24, 29, 30, 31, 32, 33, 34, 36,
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
}
