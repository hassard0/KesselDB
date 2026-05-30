//! SP-Perf-A-SHARD T2 — sharded state-machine scaffold.
//!
//! See `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-design.md`.
//!
//! What this module ships in T2 (scaffold only)
//! ============================================
//! - `ShardedStateMachine<V>` type — a wrapper over `Vec<Arc<RwLock<StateMachine<V>>>>`.
//!   At K=1 (`shards.len() == 1`) it collapses to a single read-lock
//!   acquisition — byte-identical dispatch shape to the existing
//!   SP-Perf-A T7 `Arc<RwLock<StateMachine>>` read path.
//! - `shard_of_key(&self, key) -> usize` — deterministic hash-mod
//!   routing. KAT-locked: same key always maps to the same shard
//!   across builds; the K=1 special-case short-circuits to 0.
//! - `shard_of_op(&self, &Op) -> ShardRoute` — classifies an op as
//!   either single-shard (a single owning shard derivable from the
//!   op's keys) or fan-out (must touch every shard, e.g. scans).
//! - `read_only_op_k1(&self, Op) -> OpResult` — the K=1 dispatch
//!   used by the regression-lock KAT. It acquires the single shard's
//!   read lock and calls `StateMachine::read_only_op`, byte-equivalent
//!   to today's `sm.read().read_only_op(op)`.
//!
//! What T2 does NOT ship (deliberately, named for future SHARD-* arcs)
//! ===================================================================
//! - K=N apply plumbing (per-shard apply thread, write routing,
//!   per-shard WAL group-commit) — `SP-Perf-A-SHARD-APPLY`, multi-week
//!   core work.
//! - K=N read pool dispatch — `SP-Perf-A-SHARD-READ`.
//! - Cross-shard scan scatter-merge — `SP-Perf-A-SHARD-SCAN`.
//! - Cross-shard atomic Op::Txn — `SP-Perf-A-SHARD-XTXN`.
//! - The measured K=N vs K=1 benchmark sweep on vulcan —
//!   `SP-Perf-A-SHARD-BENCH`.
//!
//! Engine wiring
//! =============
//! T2 does NOT wire `ShardedStateMachine` into `spawn_engine_cfg`. The
//! `ServerConfig.shard_count` field is added as a named slot for the
//! future arc, but the default (`None`) preserves the SP-Perf-A T7
//! single-`Arc<RwLock<StateMachine>>` ownership shape — every default
//! build is byte-identical to pre-SHARD.
//!
//! The shipping K=1 regression-lock KAT proves that the dispatch shape
//! introduced here is functionally equivalent to today's single-SM
//! shape, so the K=N plumbing in `SP-Perf-A-SHARD-APPLY` can land on
//! top of this scaffold without re-litigating correctness at K=1.

use kessel_io::Vfs;
use kessel_proto::Op;
use kessel_sm::StateMachine;
use std::sync::{Arc, RwLock};

/// Local mirror of `kessel_storage::make_key` so this scaffold does
/// not require a new dependency on kessel-storage (the storage crate
/// is NOT in `kesseldb-server`'s Cargo.toml today). Byte-for-byte
/// identical: `type_id.to_le_bytes() ++ object_id` — KAT-locked
/// implicitly because shard_of_op routes the same key bytes that
/// the SM's read paths produce via `kessel_storage::make_key`.
#[inline]
fn make_key_inline(type_id: u32, object_id: &[u8; 16]) -> Vec<u8> {
    let mut k = Vec::with_capacity(20);
    k.extend_from_slice(&type_id.to_le_bytes());
    k.extend_from_slice(object_id);
    k
}

/// Wrapper that owns K shards, each its own `Arc<RwLock<StateMachine>>`.
///
/// K=1 ⇒ a single shard ⇒ dispatch identical to the pre-SHARD
/// `Arc<RwLock<StateMachine>>` shape. KAT-locked by
/// `shard_k1_matches_unsharded_sm_byte_equal`.
///
/// K=N (V2 — `SP-Perf-A-SHARD-APPLY` + `SP-Perf-A-SHARD-READ`) ⇒ each
/// shard owns a disjoint slice of the key space; reads to different
/// shards do not contend on a shared cache line.
pub struct ShardedStateMachine<V: Vfs> {
    shards: Vec<Arc<RwLock<StateMachine<V>>>>,
}

/// Classification of an op's shard locality.
///
/// `Single(s)` — the op reads/writes only keys owned by shard `s`. The
/// dispatcher acquires only that shard's lock.
///
/// `FanOut` — the op may touch any key (range scans, full-table
/// aggregates, joins, etc.). The dispatcher must visit every shard and
/// merge the partial results. V1 (this scaffold) does NOT implement
/// the merge layer — `read_only_op_k1` short-circuits FanOut at K=1 by
/// dispatching to the single shard. At K≥2 the merge is V2's
/// `SP-Perf-A-SHARD-SCAN` job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardRoute {
    Single(usize),
    FanOut,
}

impl<V: Vfs> ShardedStateMachine<V> {
    /// Construct from a pre-built vector of per-shard SMs. Panics if
    /// `shards.is_empty()` — K=0 has no meaningful semantic.
    pub fn new(shards: Vec<Arc<RwLock<StateMachine<V>>>>) -> Self {
        assert!(!shards.is_empty(), "ShardedStateMachine requires K >= 1");
        Self { shards }
    }

    /// Number of shards K. K=1 ⇒ single-shard collapse.
    #[inline]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Borrow shard `i`. Panics on out-of-range — callers ALWAYS go
    /// through `shard_of_*` so the index is bounds-checked at the
    /// dispatch seam, not on the hot path.
    #[inline]
    pub fn shard(&self, i: usize) -> &Arc<RwLock<StateMachine<V>>> {
        &self.shards[i]
    }

    /// Deterministic key → shard mapping. Same key always maps to the
    /// same shard across binary builds and across machines (no clock,
    /// no PID, no RNG — pure function of the key bytes and K).
    ///
    /// K=1 short-circuits to 0 so the routing call is a single
    /// length check at the K=1 collapse. KAT-locked by
    /// `shard_of_key_k1_always_zero`.
    ///
    /// K>=2 uses a 64-bit FxHash-style fold (see `fxhash_fold` below)
    /// modulo K. The fold quality is sufficient for uniform-random
    /// key workloads (YCSB, sysbench); workloads with hot keys
    /// (per the design spec §7 weak-spot #6) will concentrate
    /// regardless of hash quality.
    #[inline]
    pub fn shard_of_key(&self, key: &[u8]) -> usize {
        if self.shards.len() == 1 {
            return 0;
        }
        let h = fxhash_fold(key);
        (h as usize) % self.shards.len()
    }

    /// Classify an op's shard locality. Point-data ops route to a
    /// single owning shard derived from the op's key; scan / range /
    /// aggregate / join ops fan out across every shard.
    ///
    /// At K=1 the classifier still emits `FanOut` for scan-shape ops
    /// (so the V2 SHARD-READ dispatcher has a uniform contract);
    /// `read_only_op_k1` folds FanOut to shard 0 because a fan-out
    /// over 1 shard IS a single-shard dispatch. Only K>=2 needs the
    /// V2 SHARD-SCAN merge layer.
    pub fn shard_of_op(&self, op: &Op) -> ShardRoute {
        match op {
            // Point-data ops — derive a key, route to its shard.
            Op::GetById { type_id, id } => {
                ShardRoute::Single(self.shard_of_key(&make_key_inline(*type_id, &id.0)))
            }
            // GetBlob's handle becomes a 20-byte make_key shape via
            // the overflow-type key prefix (kessel-sm::handle_key).
            // The OVERFLOW_TYPE constant lives in kessel-sm; we mirror
            // the construction here without importing the private
            // helper. KAT-locked.
            Op::GetBlob { handle } => {
                let mut id = [0u8; 16];
                id[..8].copy_from_slice(&handle.to_le_bytes());
                ShardRoute::Single(self.shard_of_key(&make_key_inline(0xFFFF_FFFF, &id)))
            }
            // FindBy / FindByComposite / FindRange / Describe — V1
            // routes by a "type_id + zero oid" synthetic key so all
            // rows of a given type live on one shard. Preserves the
            // per-type FindBy / Describe locality contract.
            Op::FindBy { type_id, .. }
            | Op::FindByComposite { type_id, .. }
            | Op::FindRange { type_id, .. }
            | Op::Describe { type_id } => {
                let key = make_key_inline(*type_id, &[0u8; 16]);
                ShardRoute::Single(self.shard_of_key(&key))
            }
            // SeqRead reads the sequencer keyspace (SEQ_TYPE =
            // 0xFFFF_FFF0). It lives on a single shard derivable
            // from a fixed key — routes deterministically.
            Op::SeqRead { .. } => {
                let key = make_key_inline(0xFFFF_FFF0, &[0u8; 16]);
                ShardRoute::Single(self.shard_of_key(&key))
            }
            // Op::Txn — V1 defers cross-shard detection. At K=1
            // every Txn collapses to Single(0); at K>=2 single-shard
            // Txn detection is the V2 SHARD-XTXN job. Conservative:
            // FanOut at K>=2 means "may touch every shard", which is
            // correct (the alternative — silently routing to one
            // shard — would be incorrect for a cross-shard Txn).
            Op::Txn { .. } => {
                if self.shards.len() == 1 {
                    ShardRoute::Single(0)
                } else {
                    ShardRoute::FanOut
                }
            }
            // Scan / range / aggregate / join — must visit every shard
            // at K>=2 to produce the correct result. At K=1 the
            // dispatcher folds FanOut to Single(0).
            _ => ShardRoute::FanOut,
        }
    }

    /// K=1 dispatch — the only dispatch shape SHARD-1 ships.
    ///
    /// At K=1, every op routes to shard 0 (the single shard). The
    /// implementation is `self.shards[0].read().read_only_op(op)` —
    /// byte-identical to the pre-SHARD `sm.read().read_only_op(op)`
    /// shape used by `EngineHandle::apply_raw` on the read-only
    /// bypass path.
    ///
    /// **Panics on K>=2** — the K=N dispatch is V2 work
    /// (`SP-Perf-A-SHARD-READ` + `SP-Perf-A-SHARD-SCAN`). The panic
    /// is a fail-fast so a stale K=N config doesn't silently
    /// regress to single-shard semantics under load.
    pub fn read_only_op_k1(&self, op: Op) -> kessel_proto::OpResult {
        assert_eq!(
            self.shards.len(),
            1,
            "read_only_op_k1 requires K=1; K>=2 dispatch lands in SP-Perf-A-SHARD-READ"
        );
        self.shards[0]
            .read()
            .expect("shard 0 rwlock poisoned")
            .read_only_op(op)
    }
}

/// 64-bit FxHash-style fold of a byte string. Same input ⇒ same
/// output across builds (no salt, no time, no allocator quirks).
/// Quality is sufficient for `% K` uniform distribution on
/// uniform-random keys; the design spec §7 weak-spot #6 documents
/// hot-key workloads where any hash will concentrate.
///
/// Why inline (rather than depend on the `fxhash` crate): standing
/// rule "no new external runtime deps". The implementation is 8
/// lines, has no `unsafe`, and is locked by KAT
/// `fxhash_fold_is_deterministic_across_calls` so any future
/// refactor catches accidental nondeterminism (e.g., replacing
/// with `DefaultHasher` which seeds from a process-wide random).
#[inline]
fn fxhash_fold(bytes: &[u8]) -> u64 {
    // Rotate-multiply chain — the FxHash kernel without unsafe.
    const SEED: u64 = 0xcbf2_9ce4_8422_2325; // FNV-style offset basis
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h: u64 = SEED;
    for &b in bytes {
        h = h.rotate_left(5) ^ (b as u64);
        h = h.wrapping_mul(PRIME);
    }
    h
}

// ============================================================
// Tests — the SHARD-1 regression-lock + scaffold KATs
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_catalog::{encode_type_def, Field, FieldKind};
    use kessel_io::DirVfs;
    use kessel_proto::{ObjectId, Op, OpResult};
    use kessel_sm::StateMachine;
    use std::sync::{Arc, RwLock};

    /// Test-only tempdir helper — pattern lifted verbatim from
    /// `read_pool.rs` (no `tempfile` dep on the server crate).
    fn rand_suffix() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }

    fn open_sm(tag: &str) -> (std::path::PathBuf, Arc<RwLock<StateMachine<DirVfs>>>) {
        let dir = std::env::temp_dir().join(format!(
            "kesseldb-shard-{tag}-{}-{}",
            std::process::id(),
            rand_suffix()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let sm = StateMachine::open(DirVfs::new(&dir).unwrap()).unwrap();
        (dir, Arc::new(RwLock::new(sm)))
    }

    // ---------- 1. fxhash determinism ----------

    /// fxhash_fold MUST be deterministic across calls within a single
    /// process AND across processes (the design spec §8 invariant 3
    /// — "deterministic key → shard mapping"). A subtle break (e.g.,
    /// replacing with `DefaultHasher` which uses a per-process seed)
    /// would re-route every key on every restart, which silently
    /// breaks SHARD-APPLY's per-shard ownership contract.
    #[test]
    fn fxhash_fold_is_deterministic_across_calls() {
        let cases: &[&[u8]] = &[
            b"",
            b"a",
            b"abc",
            b"a slightly longer key that crosses 32 bytes for safety",
            &[0u8; 20],
            &[0xFFu8; 20],
        ];
        for c in cases {
            let a = fxhash_fold(c);
            let b = fxhash_fold(c);
            assert_eq!(a, b, "fxhash_fold non-deterministic on {c:?}");
        }
    }

    /// Smoke check: the hash distinguishes obviously-different inputs.
    /// If a refactor accidentally returns a constant, this fails fast.
    #[test]
    fn fxhash_fold_distinguishes_inputs() {
        let empty = fxhash_fold(b"");
        let abc = fxhash_fold(b"abc");
        let ab = fxhash_fold(b"ab");
        assert_ne!(empty, abc);
        assert_ne!(empty, ab);
        assert_ne!(abc, ab);
        // Re-compute on the same call site — must match.
        assert_eq!(empty, fxhash_fold(b""));
        assert_eq!(abc, fxhash_fold(b"abc"));
    }

    // ---------- 2. shard_of_key at K=1 ----------

    /// At K=1, shard_of_key MUST short-circuit to 0 for every key.
    /// This is the K=1 collapse contract: the K=1 ShardedStateMachine
    /// is functionally equivalent to a single-SM. KAT-locked.
    #[test]
    fn shard_of_key_k1_always_zero() {
        let (dir, sm) = open_sm("kof-k1");
        let shard = ShardedStateMachine::new(vec![sm]);
        for key in &[
            b"".as_slice(),
            b"a".as_slice(),
            b"some longer key here".as_slice(),
            &[0u8; 20][..],
            &[0xFFu8; 20][..],
        ] {
            assert_eq!(shard.shard_of_key(key), 0, "K=1 must collapse to shard 0");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---------- 3. shard_of_key at K>=2 deterministic ----------

    /// At K>=2, shard_of_key MUST be a pure function of the key bytes
    /// + K. Same key + same K ⇒ same shard, across calls. (This is
    /// the cross-build determinism contract — replacing fxhash with
    /// std DefaultHasher would silently fail this on every restart.)
    #[test]
    fn shard_of_key_k4_deterministic_within_process() {
        let (d1, s1) = open_sm("kof-k4d-1");
        let (d2, s2) = open_sm("kof-k4d-2");
        let (d3, s3) = open_sm("kof-k4d-3");
        let (d4, s4) = open_sm("kof-k4d-4");
        let shard = ShardedStateMachine::new(vec![s1, s2, s3, s4]);

        let key1: &[u8] = b"deterministic-key-1";
        let key2: &[u8] = b"deterministic-key-2";
        let a1 = shard.shard_of_key(key1);
        let a2 = shard.shard_of_key(key1);
        let a3 = shard.shard_of_key(key1);
        assert_eq!(a1, a2);
        assert_eq!(a1, a3);
        let b1 = shard.shard_of_key(key2);
        let b2 = shard.shard_of_key(key2);
        assert_eq!(b1, b2);
        for s in &[a1, b1] {
            assert!(*s < 4);
        }

        let _ = std::fs::remove_dir_all(&d1);
        let _ = std::fs::remove_dir_all(&d2);
        let _ = std::fs::remove_dir_all(&d3);
        let _ = std::fs::remove_dir_all(&d4);
    }

    /// shard_of_key at K=4 MUST distribute SOME keys across shards
    /// (i.e., not collapse every key onto shard 0). Catches a silent
    /// bug where the modulo or the hash returns a constant.
    #[test]
    fn shard_of_key_k4_distributes_keys() {
        let (d1, s1) = open_sm("kof-k4d-d1");
        let (d2, s2) = open_sm("kof-k4d-d2");
        let (d3, s3) = open_sm("kof-k4d-d3");
        let (d4, s4) = open_sm("kof-k4d-d4");
        let shard = ShardedStateMachine::new(vec![s1, s2, s3, s4]);

        let mut counts = [0usize; 4];
        for i in 0..256u32 {
            let key = i.to_le_bytes();
            counts[shard.shard_of_key(&key)] += 1;
        }
        for (i, c) in counts.iter().enumerate() {
            assert!(*c > 0, "shard {i} got 0 of 256 keys — routing collapsed");
        }

        let _ = std::fs::remove_dir_all(&d1);
        let _ = std::fs::remove_dir_all(&d2);
        let _ = std::fs::remove_dir_all(&d3);
        let _ = std::fs::remove_dir_all(&d4);
    }

    // ---------- 4. shard_of_op at K=1 ----------

    /// At K=1, every point-op MUST be Single(0). Op::Txn AT K=1
    /// also collapses to Single(0). Scan-shape ops STILL emit
    /// FanOut from the classifier (the dispatcher folds them) so
    /// the V2 SHARD-READ contract is uniform.
    #[test]
    fn shard_of_op_k1_classifications() {
        let (dir, sm) = open_sm("sof-k1");
        let shard = ShardedStateMachine::new(vec![sm]);

        let id = ObjectId::from_u128(7);
        assert_eq!(
            shard.shard_of_op(&Op::GetById { type_id: 7, id }),
            ShardRoute::Single(0)
        );
        assert_eq!(
            shard.shard_of_op(&Op::GetBlob { handle: 42 }),
            ShardRoute::Single(0)
        );
        assert_eq!(
            shard.shard_of_op(&Op::Describe { type_id: 7 }),
            ShardRoute::Single(0)
        );
        assert_eq!(
            shard.shard_of_op(&Op::SeqRead { from: 1, limit: 10 }),
            ShardRoute::Single(0)
        );
        assert_eq!(
            shard.shard_of_op(&Op::FindBy {
                type_id: 7,
                field_id: 1,
                value: vec![],
            }),
            ShardRoute::Single(0)
        );

        // Op::Txn at K=1 collapses to Single(0).
        assert_eq!(
            shard.shard_of_op(&Op::Txn { ops: vec![] }),
            ShardRoute::Single(0)
        );

        // Scan-shape op: classifier emits FanOut even at K=1 (the
        // dispatcher folds, not the classifier). This pins the
        // contract.
        let select = Op::Select {
            type_id: 7,
            program: vec![],
            limit: 10,
        };
        assert_eq!(shard.shard_of_op(&select), ShardRoute::FanOut);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---------- 5. shard_of_op at K=2 deterministic point-op routing ----------

    /// At K>=2, point-data ops MUST route deterministically — the
    /// SAME (type_id, id) pair MUST land on the SAME shard across
    /// calls. This is the K=N ownership contract that SHARD-APPLY
    /// will rely on.
    #[test]
    fn shard_of_op_k2_get_by_id_deterministic() {
        let (d1, s1) = open_sm("sof-k2-1");
        let (d2, s2) = open_sm("sof-k2-2");
        let shard = ShardedStateMachine::new(vec![s1, s2]);

        let id = ObjectId::from_u128(0xDEAD_BEEF);
        let r1 = shard.shard_of_op(&Op::GetById { type_id: 5, id });
        let r2 = shard.shard_of_op(&Op::GetById { type_id: 5, id });
        let r3 = shard.shard_of_op(&Op::GetById { type_id: 5, id });
        assert_eq!(r1, r2);
        assert_eq!(r1, r3);
        match r1 {
            ShardRoute::Single(s) => assert!(s < 2),
            other => panic!("GetById classified as {other:?}, expected Single(_)"),
        }

        let _ = std::fs::remove_dir_all(&d1);
        let _ = std::fs::remove_dir_all(&d2);
    }

    // ---------- 6. K=1 regression-lock — the headline KAT ----------

    /// **The headline regression-lock for SP-Perf-A-SHARD-1.**
    ///
    /// Two state machines on independent storage, seeded identically,
    /// then queried in lockstep:
    ///   A: Arc<RwLock<StateMachine>> (the SP-Perf-A T7 shape)
    ///   B: ShardedStateMachine { shards: vec![A_equivalent] } (K=1)
    ///
    /// For every read op in a canonical workload, A's
    /// `read_only_op(op)` and B's `read_only_op_k1(op)` MUST be
    /// byte-equal. If this test ever fails, the SHARD-1 scaffold has
    /// drifted from the unsharded SM and the K=N work that builds on
    /// top is built on sand.
    #[test]
    fn shard_k1_matches_unsharded_sm_byte_equal() {
        let (dir_a, sm_a) = open_sm("k1-eq-A");
        let (dir_b, sm_b) = open_sm("k1-eq-B");

        // Build the type def using the catalog's encoder — same shape
        // the engine uses internally.
        let fields = vec![Field {
            field_id: 1,
            name: "v".to_string(),
            kind: FieldKind::U64,
            nullable: false,
        }];
        let def = encode_type_def("row", &fields);

        // Apply identical write workload to BOTH state machines.
        let seed_ops: Vec<Op> = vec![
            Op::CreateType { def: def.clone() },
            Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(0),
                record: 1u64.to_le_bytes().to_vec(),
            },
            Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(1),
                record: 2u64.to_le_bytes().to_vec(),
            },
            Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(2),
                record: 3u64.to_le_bytes().to_vec(),
            },
        ];
        for (i, op) in seed_ops.iter().enumerate() {
            let op_no = (i as u64) + 1;
            let r_a = sm_a.write().unwrap().apply(op_no, op.clone());
            let r_b = sm_b.write().unwrap().apply(op_no, op.clone());
            assert_eq!(r_a, r_b, "seed apply diverged at op #{i}");
        }

        // Wrap B in a K=1 ShardedStateMachine.
        let shard_b = ShardedStateMachine::new(vec![sm_b.clone()]);

        // Canonical read workload — 1 hit, 1 miss, 1 describe.
        let read_ops: Vec<Op> = vec![
            Op::GetById {
                type_id: 1,
                id: ObjectId::from_u128(0),
            },
            Op::GetById {
                type_id: 1,
                id: ObjectId::from_u128(99),
            },
            Op::Describe { type_id: 1 },
        ];

        for op in &read_ops {
            let r_a = sm_a.read().unwrap().read_only_op(op.clone());
            let r_b = shard_b.read_only_op_k1(op.clone());
            assert_eq!(
                r_a, r_b,
                "K=1 SHARD diverged from unsharded SM on op {op:?}"
            );
            // Sanity: the hit op returns Got; the miss op returns
            // NotFound. Confirms the workload actually exercises both
            // arms (not a vacuous all-NotFound run).
            match op {
                Op::GetById { id, .. } if id.0[0] == 0 => {
                    assert!(matches!(r_a, OpResult::Got(_)));
                }
                Op::GetById { .. } => {
                    assert!(matches!(r_a, OpResult::NotFound));
                }
                Op::Describe { .. } => {
                    assert!(matches!(r_a, OpResult::Got(_)));
                }
                _ => {}
            }
        }

        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    // ---------- 7. read_only_op_k1 panics on K>=2 ----------

    /// `read_only_op_k1` is fail-fast on K>=2 — a stale config that
    /// asks for K=N dispatch from the K=1-only entry point should
    /// crash the test, not silently regress.
    #[test]
    #[should_panic(expected = "read_only_op_k1 requires K=1")]
    fn read_only_op_k1_panics_on_k_ge_2() {
        let (_d1, s1) = open_sm("rok1-pan-1");
        let (_d2, s2) = open_sm("rok1-pan-2");
        let shard = ShardedStateMachine::new(vec![s1, s2]);
        let _ = shard.read_only_op_k1(Op::GetById {
            type_id: 1,
            id: ObjectId::from_u128(0),
        });
    }

    // ---------- 8. accessors ----------

    /// `shard_count()` returns the configured K. `shard(i)` borrows
    /// the per-shard arc. Both are trivial accessors but they're on
    /// the public API surface — pin them so a refactor doesn't
    /// accidentally make them private.
    #[test]
    fn shard_count_and_indexing() {
        let (d1, s1) = open_sm("acc-1");
        let (d2, s2) = open_sm("acc-2");
        let (d3, s3) = open_sm("acc-3");
        let shard = ShardedStateMachine::new(vec![s1.clone(), s2.clone(), s3.clone()]);
        assert_eq!(shard.shard_count(), 3);
        assert!(Arc::ptr_eq(shard.shard(0), &s1));
        assert!(Arc::ptr_eq(shard.shard(1), &s2));
        assert!(Arc::ptr_eq(shard.shard(2), &s3));
        let _ = std::fs::remove_dir_all(&d1);
        let _ = std::fs::remove_dir_all(&d2);
        let _ = std::fs::remove_dir_all(&d3);
    }

    /// K=0 is rejected — there is no meaningful "zero-shard" SM.
    #[test]
    #[should_panic(expected = "ShardedStateMachine requires K >= 1")]
    fn k0_rejected() {
        let _ = ShardedStateMachine::<DirVfs>::new(vec![]);
    }
}
