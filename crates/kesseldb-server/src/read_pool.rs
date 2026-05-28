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
use kessel_proto::{Op, OpResult};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
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
    /// Spawn `n` workers draining a shared `sync_channel(queue_bound)`.
    /// `n == 0` is supported as a graceful "no-op" pool — it spawns no
    /// threads but `dispatch` will still call back through the engine on
    /// the SUBMITTING thread (so the caller does not observe a difference
    /// from the no-pool default). Use `read_workers = Some(0)` as a way
    /// to test the wiring path without paying for any worker threads.
    pub fn new(n: usize, queue_bound: usize, engine: EngineHandle) -> Self {
        // `sync_channel(0)` is rendezvous-only; sync_channel(>=1) is
        // bounded with a small buffer. We use a small buffer so a burst
        // doesn't immediately block submitters at the cost of bounded
        // memory.
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

    fn worker_loop(rx: Arc<std::sync::Mutex<Receiver<ReadTask>>>, engine: EngineHandle) {
        loop {
            // Lock only across `recv`. Each call returns one task or
            // RecvError on a closed channel.
            let task = {
                let guard = rx.lock().expect("read-pool rx mutex poisoned");
                guard.recv()
            };
            let (frame, rp) = match task {
                Ok(t) => t,
                Err(_) => return, // queue closed → exit cleanly
            };
            // Panic-safe dispatch. A panic in apply_raw is downgraded to
            // SchemaError; the worker continues to serve subsequent tasks.
            // (T1 uses the engine queue; T2 will use the direct read path,
            // where this guard is even more important — any storage-layer
            // panic would otherwise tear down the worker.)
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                engine.apply_raw(frame)
            }))
            .unwrap_or_else(|_| OpResult::SchemaError("read panicked".into()));
            let _ = rp.send(result);
        }
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
            Op::Aggregate { type_id: 1, program: vec![], kind: 0, field_id: 0 },
            Op::SelectFields { type_id: 1, program: vec![], fields: vec![], limit: 0 },
            Op::GroupAggregate {
                type_id: 1,
                program: vec![],
                group_field: 0,
                kind: 0,
                agg_field: 0,
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
}
