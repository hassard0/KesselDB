## SP-Perf-A-SHARD — per-CPU sharded apply queues + read pools — design spec

Date: 2026-05-30
Author: Track B (parallel to other tracks)
HEAD on main when this slice opened: `0a19f3d` (post-SP-Hash-Agg-Tune T2.1)
Parent arc: SP-Perf-A (T1..T7 shipped; closed at T7 with the storage-internal
`Arc<[u8]>` migration). T7 closed `DONE_WITH_CONCERNS` because — even with
the per-read memcpy gone — `get-by-id` still flatlines at ~5M ops/sec at
N=16 cores on vulcan. T7's diagnosis names the next ceiling: `RwLock<StateMachine>`
reader CAS ping-pong + per-op lock+dispatch dominate the budget once the
value memcpy is removed.

Scope: design spec + scaffolded sharded-storage type signatures + first
regression-lock KAT proving K=1 (single-shard) sharded behaviour matches
the SP-Perf-A T7 single-`Arc<RwLock<StateMachine>>` baseline within ±2%.
**This spec is the entry point for a multi-arc project**: the K=N apply
plumbing + per-shard read dispatch + measured K=N benchmark sweep are
each a future slice of their own (named in §9).

---

### 1. Context — the ceiling SP-Perf-A T7 hit

`docs/BENCHMARKS.md` §12 ships the post-T7 `get-by-id` sweep:

| N   | T6 Fix-B (100K) | T7 (10K) | Note |
|-----|-----------------|----------|------|
| 1   | 1.15M ops/sec   | 1.38M    | +20% |
| 4   | (n/a)           | 3.73M    |      |
| 8   | 4.70M           | 5.08M    | +8.1% |
| 16  | 3.94M           | 4.95M    | +25.7% |
| 24  | 4.73M           | 4.84M    | +2.2% |
| 32  | 5.07M           | 4.71M    | -7.1% |

The curve **flatlines around ~5M ops/sec at N=8 and stops scaling**.
Per-op cost at the steady state:

1. `EngineHandle::apply(Op)` arrives — already on the in-process fast
   path after T6 Fix A; no encode/decode roundtrip.
2. `sm_shared.read()` — acquire the shared `Arc<RwLock<StateMachine>>`
   reader guard. **Atomic CAS on the reader-count word inside the
   RwLock.** That word is ONE cache line, shared by every read worker.
3. `read_only_op(op)` — execute the read against the in-memory
   storage state (BTreeMap memtable + Vec-sorted SSTables).
4. `Arc<[u8]>` clone — refcount bump (one atomic, on a value-specific
   cache line; not contended at scale because every value has its own
   Arc).

The CAS at step 2 is the **shared atomic on a single cache line**. With
N worker threads on N cores, that cache line ping-pongs between L1s
on every read. The Rust `parking_lot::RwLock` and the std `RwLock`
both pay this cost — it's a structural property of the "many readers
share one lock" pattern, not an implementation flaw.

Honest framing: T7 closed the **memcpy** part of the ceiling; SHARD
attacks the **CAS ping-pong** part. Both need fixing to lift `get-by-id`
N=16 past ~5M ops/sec.

---

### 2. Approach — per-CPU shards

**Partition the key space into K shards.** Each shard owns its own
piece of storage + its own state-machine state + its own RwLock. A
read on shard `s` acquires `shard[s].sm.read()` — a cache line that
ONLY shard-s reader threads touch. Reads on shard 0 don't contend
with reads on shard 1.

The shape:

```rust
pub struct ShardedStateMachine<V: Vfs> {
    pub shards: Vec<Arc<RwLock<StateMachine<V>>>>,
}

impl<V: Vfs> ShardedStateMachine<V> {
    /// Deterministic key → shard mapping. Same key always lands on
    /// the same shard. Default `K = num_cpus`; `K=1` collapses to
    /// SP-Perf-A T7 single-SM behaviour (regression-lock).
    #[inline]
    pub fn shard_of_key(&self, key: &[u8]) -> usize {
        if self.shards.len() == 1 {
            return 0;
        }
        // FxHash-quality fast hash; deterministic across builds.
        // 64-bit fold so K can be up to 2^32 without collision bias.
        let h = fxhash_seed_0(key);
        (h as usize) % self.shards.len()
    }

    /// Route a read-only op to its owning shard's read-lock.
    pub fn read_only_op(&self, op: Op) -> OpResult {
        let shard = self.shard_of_op(&op);
        self.shards[shard].read().expect("shard rwlock").read_only_op(op)
    }
}
```

**`shard_of_op(op)` policy** (the routing table):

- **Point-data ops** (`GetById`, `GetBlob`, `FindBy`, `FindByComposite`,
  `Describe`, `SeqRead{from}`) — derive a key from the op (data row
  `make_key(type_id, oid)` for GetById; equivalent for the others) and
  call `shard_of_key`.
- **Range / scan ops** (`Select`, `QueryRows`, `QueryExpr`,
  `SelectFields`, `SelectSorted`, `Aggregate`, `GroupAggregate`,
  `FindRange`, `Query`, `Join`) — must touch every shard. The
  router fans out across all K shards and merges the partial results
  (sum across shards for Aggregate-SUM, sorted merge for SelectSorted,
  etc.). **This is identical in structure to the existing
  `scatter_scan` machinery** that SP-A already uses at the router
  level for cross-node scans — SHARD reuses the same per-op merge
  contract, just in-process.
- **Op::Txn{ops}** — if every inner op is point-data AND every key
  maps to the same shard ⇒ route to that shard's read lock. If
  cross-shard ⇒ V1 falls back to the unsharded apply path
  (correctness-preserving; cost = a write-lock on the whole
  unsharded SM during the txn). V2 (`SP-Perf-A-SHARD-XTXN`) does the
  cross-shard coordination via the existing 0xFFFF_FFF1 XSHARD
  reserved keyspace.

**`shard_of_data_row_op` policy:**

```rust
match op {
    Op::GetById { type_id, id } =>
        Some(shard_of_key(&make_key(*type_id, &id.0))),
    Op::GetBlob { handle } =>
        Some(shard_of_key(&handle_key(*handle))),
    // ... 4 more point ops
    _ => None, // scan op — fan-out path
}
```

---

### 3. Scope — V1

**In-scope:**

1. The `ShardedStateMachine` type and its `shard_of_key` /
   `shard_of_op` routing helpers.
2. A `ServerConfig.shard_count: Option<usize>` (default `None` ⇒
   K=1 ⇒ pre-SHARD byte-identical) wired through `spawn_engine_cfg`.
3. **K=1 path** — when `shard_count = Some(1)` (or `None`), the
   `ShardedStateMachine` collapses to a single `Arc<RwLock<StateMachine>>`
   identical to the SP-Perf-A T7 shape. KAT-locked.
4. The 6-arc named decomposition (§9) so the rest of the work has
   handles.

**Out-of-scope (V1 — each is its own arc):**

1. **K=N apply plumbing** (`SP-Perf-A-SHARD-APPLY`, V2) — multi-shard
   apply needs a per-shard apply thread + a write-routing layer +
   per-shard WAL group-commit. This is the multi-week core of the
   project; see §9.
2. **K=N read dispatch** (`SP-Perf-A-SHARD-READ`, V2) — the
   `read_pool` worker pool's dispatch logic stays at K=1 for now;
   V2 makes it shard-aware.
3. **Cross-shard atomic transactions** (`SP-Perf-A-SHARD-XTXN`, V2) —
   V1 falls back to a global write lock for cross-shard Txns
   (correct but slow). V2 wires 2PC via the existing XSHARD
   coordinator keyspace.
4. **Cross-shard schema DDL atomicity** (`SP-Perf-A-SHARD-DDL`, V2) —
   V1 replicates the catalog on every shard (each shard owns a
   full copy); DDL serializes by broadcasting `Op::CreateType` etc.
   to every shard's apply thread. Bypassing the cross-shard
   serialization is V2.
5. **Per-shard WAL** (`SP-Perf-A-SHARD-WAL`, V2) — V1 keeps a single
   shared WAL (every shard's writes go through the same WAL with
   per-shard `Storage` instances pointing at one disk). V2 splits
   the WAL per-shard so writes don't serialize on the shared fsync.
6. **NUMA-aware shard pinning** (`SP-Perf-A-SHARD-NUMA`, V2) — V1
   shards are CPU-blind; V2 pins shard `s` to a CPU set on the
   matching NUMA node.
7. **`scatter_scan` integration** (`SP-Perf-A-SHARD-SCAN`, V2) —
   the V1 scatter-scan implementation handles fan-out at the router
   level; V2 wires the in-process shard fan-out using the same
   merge contract.
8. **Benchmark sweep** (`SP-Perf-A-SHARD-BENCH`, last arc) — the
   K=N vs K=1 measured comparison on vulcan that proves (or
   falsifies) the ≥2× lift hypothesis at N=16. Lands once
   SHARD-APPLY + SHARD-READ + SHARD-SCAN are merged.

---

### 4. Acceptance criteria

**V1 (this arc):**

1. K=1 path runs end-to-end through `ShardedStateMachine`. The KAT
   `shard_k1_matches_sp_perfa_t7_baseline_within_2pct` confirms
   `get-by-id` throughput at N=16 on vulcan with `shard_count = Some(1)`
   is within ±2% of T7 (`docs/BENCHMARKS.md` §12).
2. Default `cargo build` byte-identical (the `shard_count` field
   defaults to `None`, the type+routing helpers are dead code until
   opted in).
3. `cargo test --workspace` passes (no behaviour change at the K=1
   collapse).
4. Determinism oracle (`parallel_reads_oracle::*`, 17/17 green on
   vulcan) still passes at K=1.
5. `#![forbid(unsafe_code)]` honored; zero new external deps.

**V2 (`SP-Perf-A-SHARD-APPLY` + `SP-Perf-A-SHARD-READ` +
`SP-Perf-A-SHARD-BENCH`):**

1. K=N=num_cpus measured lift on vulcan: `get-by-id` at N=16 lifts
   ≥2× over the K=1 ~5M ops/sec ceiling (target: ≥10M ops/sec).
   If this falsifies, the arc closes `DONE_WITH_CONCERNS` and
   documents the next bottleneck (likely the merge layer for
   scan ops, or per-shard WAL fsync contention).
2. All point-read ops (`GetById`, `GetBlob`, `FindBy`,
   `FindByComposite`, `Describe`, `SeqRead`) route per-shard.
3. All scan ops use the in-process scatter-merge layer; results
   byte-identical to the K=1 path on the determinism oracle.
4. Op::Txn correctness — all-RO single-shard Txn routes to its
   shard's read lock; cross-shard Txn falls back to global write
   lock (named cost; V2 follow-up `SP-Perf-A-SHARD-XTXN` removes
   this).

---

### 5. Determinism

The deterministic-replay contract (every replica applies the same
ops in the same order and reaches byte-identical state) is preserved
within each shard. Cross-shard ordering is **workload-specific**:

- Two writes that land on different shards have NO defined ordering
  between them. From the perspective of any single key (which always
  lives on ONE shard), the writes against that key are serialised
  by that shard's apply thread — that's the only ordering the
  application can observe.
- Reads ARE consistent within a single op: a `GetById` sees the
  state of its shard at the moment its read-lock was acquired.
- A `Select` that fans out to all K shards sees a state that's
  **per-shard-consistent but not globally-consistent**: shard 0's
  partial result reflects shard 0's state at its read-lock
  acquisition; shard 1's reflects shard 1's at its (possibly
  different) acquisition time. For point-in-time global consistency
  V2 (`SP-Perf-A-SHARD-SNAPSHOT`) needs to plug into MVCC's snapshot
  number plumbing so every shard reads at the same `seq`.

For V1 — which collapses to K=1 — this is moot: K=1 has the same
single-shard semantic as the original `Arc<RwLock<StateMachine>>`
shape. The cross-shard-ordering caveat only matters once V2 ships
K=N.

VSR / replication operates per-shard once SHARD-APPLY lands — each
shard is its own replicated log with its own op-numbers. Cross-shard
ordering between replicas remains undefined (matching the
single-node story above). Operators wanting cross-shard ordering
buy it via V2 SHARD-SNAPSHOT or via Op::Txn (which serializes
within its shard).

---

### 6. Single-shard test — the regression-lock

KAT: `shard_k1_matches_unsharded_sm_byte_equal`

```rust
#[test]
fn shard_k1_matches_unsharded_sm_byte_equal() {
    // Two engines on the same data:
    //   A: Arc<RwLock<StateMachine>> (T7 shape)
    //   B: ShardedStateMachine { shards: vec![A] } (K=1)
    // Run the determinism oracle's 100×10-op workload against both.
    // Every read OpResult must be byte-equal.
    let vfs_a = MemVfs::default();
    let vfs_b = MemVfs::default();
    let sm_a = Arc::new(RwLock::new(StateMachine::open(vfs_a).unwrap()));
    let sm_b = ShardedStateMachine::new(vec![
        Arc::new(RwLock::new(StateMachine::open(vfs_b).unwrap()))
    ]);
    seed_identical(&sm_a, &sm_b);
    let workloads = generate_oracle_workloads(100, 10, 42);
    for w in workloads {
        for op in w {
            let r_a = sm_a.read().unwrap().read_only_op(op.clone());
            let r_b = sm_b.read_only_op(op);
            assert_eq!(r_a, r_b, "K=1 SHARD diverged from unsharded SM");
        }
    }
}
```

The lock: as long as K=1, the sharded type is the unsharded type with
one indirection. Adding new variants to `read_only_op` or `apply`
goes through the same single-shard dispatch.

---

### 7. 8 weak-spots (the things this design can break)

1. **Cross-shard scan O(K) overhead** — every `Select` / `Aggregate`
   now does K separate per-shard reads and merges. For K=16 and a
   small scan (LIMIT 10), the per-shard read budget is dominated by
   the K read-lock acquisitions, not the actual scan work. At K=1
   this is one acquisition (same as today). At K=16 it could
   REGRESS scan workloads vs today — V2 SHARD-SCAN must measure
   this and decide whether to keep scan-on-K=N or revert scans to
   global-read at the dispatch layer.

2. **Per-shard WAL means more fsyncs** — if SHARD-WAL ships per-
   shard WALs, every write needs a per-shard fsync. Group commit
   amortizes within-shard (SP68 is intact) but cross-shard the
   fsync count multiplies by K. SHARD-WAL must specify whether
   cross-shard writes batch their fsyncs (extra complexity) or pay
   K× fsyncs per second under spread-write workloads.

3. **Schema-DDL must broadcast to every shard atomically** — V1
   replicates the catalog; V2 must ensure a `CreateType` op
   applies on every shard before any other op references the new
   type. SHARD-DDL needs a 2-phase apply pattern (prepare-on-all-
   shards → commit-on-all-shards) or accept a brief inconsistency
   window during DDL.

4. **Op::Txn cross-shard is expensive in V1** — falling back to a
   global write lock is correct but defeats the lift for any
   workload that mixes shards inside a transaction. The
   sysbench oltp-RW path that SP-Perf-A-TXN-RW just optimized may
   regress under SHARD if its keys spread across shards (its
   default uniform-random key distribution will spread). V2
   SHARD-XTXN is the critical fix here.

5. **Cross-shard scan consistency** — see §5 — global point-in-time
   consistency requires MVCC snapshot coordination. V1 K=1 has no
   issue; V2 K=N needs SHARD-SNAPSHOT.

6. **Key-skew workloads regress shard utilization** — hash-based
   sharding gives uniform distribution for uniform-random keys
   (YCSB), but workloads with hot keys (a few primary ids holding
   most of the traffic) will concentrate on one shard. K=16 with
   1 hot key = same throughput as K=1, plus the routing overhead.
   No fix in this arc; workload-shape caveat in BENCHMARKS.md.

7. **`SsTable` opens and bloom filters duplicated per shard** —
   each shard owns its own `Storage`, so the SSTable file list +
   bloom filter memory cost is duplicated K times. For large K
   (≥32) on a small dataset this is wasted RAM. The single-shared-
   storage alternative (one Storage, K read-locks slicing it by
   key range) is named as `SP-Perf-A-SHARD-LITE`, V2 trade-off
   study.

8. **`num_cpus` default may oversubscribe** — V1 default-on
   (when `shard_count = Some(0)`) will use `num_cpus::get()`. On
   a 64-core box this is K=64, which is too many shards for any
   small workload — the routing+merge overhead dominates. The
   spec recommends `K=min(num_cpus, 16)` as the default-on
   heuristic; benchmarks decide whether to lift the cap.

---

### 8. Locked invariants

1. **Default `cargo build` byte-identical** — `shard_count = None`
   ⇒ no `ShardedStateMachine` construction, no routing overhead,
   no behavior change.
2. **K=1 ⇒ pre-SHARD shape** — `shard_count = Some(1)` collapses
   to a single `Arc<RwLock<StateMachine>>` indirection-equivalent.
3. **Deterministic key → shard mapping** — `shard_of_key(k)` is
   pure (no time / no PID / no random); the same key always
   maps to the same shard across binary builds. KAT-locked.
4. **`#![forbid(unsafe_code)]` honored.**
5. **HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched** —
   SHARD is below the wire layer; clients see no change.
6. **VSR / replication semantic per-shard preserved** — each
   shard's apply log is its own replicated state machine; V2
   SHARD-APPLY makes this concrete.
7. **No new external deps** — fxhash is implementable inline as
   a 12-line wrapper; depending on a crate is unnecessary.

---

### 9. Slice plan — multi-arc roadmap

The arc is HONESTLY HARDER than the others. The realistic shape is
4-6 separate sub-arcs:

| Arc | Scope | Status |
|---|---|---|
| **SP-Perf-A-SHARD-1 (this)** | Design spec + scaffold types (`ShardedStateMachine` skeleton + `shard_of_key` helper + `ServerConfig.shard_count`) + K=1 regression-lock KAT + progress tracker. **No runtime behavior change.** | THIS DISPATCH |
| **SP-Perf-A-SHARD-APPLY (V2)** | K=N apply plumbing: per-shard apply thread, write routing, per-shard WAL group-commit. The MULTI-WEEK CORE. | Named |
| **SP-Perf-A-SHARD-READ (V2)** | `read_pool` workers dispatch reads to their shard's read-lock. | Named |
| **SP-Perf-A-SHARD-SCAN (V2)** | In-process scatter-merge for fan-out scan ops; reuse the existing `scatter_scan` merge contract. | Named |
| **SP-Perf-A-SHARD-XTXN (V2)** | Cross-shard atomic txns via XSHARD reserved keyspace 2PC. | Named |
| **SP-Perf-A-SHARD-BENCH (V2)** | Measured K=N vs K=1 on vulcan; closes (or falsifies) the ≥2× lift hypothesis. | Named |

**Sub-arc within this dispatch:**

- T1: spec + tracker (no code change). HEAD-only commit.
- T2 (optional): scaffold types + `shard_count` config field +
  K=1 KAT. Compile + tests green.
- T3+ (DEFER to follow-up arcs): K=N apply, K=N read dispatch,
  scatter-merge, benchmark sweep.

---

### 10. File registry

- **Spec (this file)**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-design.md`
- **Tracker**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-progress.md`
- **(T2 if shipped)** Scaffold: `crates/kesseldb-server/src/sharded_sm.rs`
- **(T2 if shipped)** Config field: `ServerConfig.shard_count` in `crates/kesseldb-server/src/lib.rs`
- **(T2 if shipped)** Regression-lock KAT: `crates/kesseldb-server/src/sharded_sm.rs` `#[cfg(test)] mod tests`

---

### 11. Standing rules

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-shard`
- Direct commits to main, no Co-Authored-By, no `-S`, push after each
- CI green check after each push
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched
- `#![forbid(unsafe_code)]` honored
- No new external deps
- All prior tests pass (every slice additive)
