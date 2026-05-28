## SP-Perf-A — parallel read execution off the single writer — design spec

Date: 2026-05-28
Author: Track B (parallel to Track A's SP-PG-EXTQ)
HEAD on main when this slice opened: `e8e98e3`
Scope: design spec + scaffold + first benchmark for Perf-A T1; T2..T6 named.

---

### 1. Context

KesselDB's deterministic apply path is a **single owning thread** (`spawn_engine_cfg`
in `crates/kesseldb-server/src/lib.rs`). Every wire frame — binary, HTTP, WS,
PG — funnels through `EngineHandle::apply_raw` into one `Sender<EngineMsg>`
the engine thread drains, applies, fsyncs (group-commit, SP68), and replies on a
per-task oneshot. This is the deterministic seam: one log-position per op, no
interleaving, no locks in the hot path.

That seam is also the **throughput ceiling**. The kessel-bench `profile` mode
on this hardware measures `sm.apply(GetById)` at sub-microsecond per-op (cached),
and `Storage::put` (autosync off) similar; but the engine thread executes them
SERIALLY, even when the work is a pure read. Real workloads are read-heavy
(80–95% reads is the canonical OLTP mix); a read-mostly client today bottlenecks
on the same thread as the writer.

The pieces to remove that ceiling are already on the floor:

- **SP116 / S2.7 MVCC dispatch** — every user-table point read in
  `Storage::get` already routes through `mvcc::get_at_snapshot(self, type_id,
  &oid, u64::MAX)`, a `&self` (immutable) method that observes only committed
  data. Reads do not see uncommitted writes; they do not block writers.
- **SP47 / SP51 compile cache** — engine-thread-local, plus the
  catalog_epoch-keyed cluster cache. Catalog reads are `&self` on the
  `Catalog` value, so multiple readers can share it via `Arc`.
- **`Op::is_mutating()`** (already in `kessel-proto`) — the read/write
  classifier already exists. Read-only ops return `false`. The set is locked
  by the proto crate's `is_mutating` definition.

Perf-A wires the read pool. No new abstraction; existing seams, additive.

---

### 2. Scope — V1

**In-scope:**

1. A **read-worker pool** of `N` OS threads (configurable via
   `ServerConfig.read_workers: Option<usize>`, default `None` = unchanged
   serial behavior) that dispatches read-only ops *without traversing the
   apply thread's queue.*
2. A **read-only classifier** `is_read_only(&Op) -> bool` on the server
   side, exported, mirroring `Op::is_mutating()` (negation) but locked in
   server scope so adding a new write op forces a server-side test update
   (regression-lock).
3. The pool only handles **bare-Op frames** that decode to a read-only op
   (Op::Select / QueryRows / SelectFields / SelectSorted / Aggregate /
   GroupAggregate / FindBy / FindByComposite / FindRange / GetById / Describe
   / Query / QueryExpr / GetBlob / SeqRead / Join). SQL frames (`0xFE`),
   session frames (`0xFD`), stats / snapshot / list-tables / list-indexes /
   list-constraints / describe-by-name admin tags, and write Ops are routed
   through the existing single writer.
4. **Correctness invariant:** every parallel read returns the SAME bytes
   the serial apply path would have returned at the same committed log
   position. Determinism preserved; the seed-7 corpus + Jepsen oracles
   (write-path tests) untouched.

**Out-of-scope (V1 — each a named V2 candidate):**

- NUMA-aware worker pinning (Perf-A-NUMA, V2).
- Per-shard read pools (Perf-A-SHARD, V2).
- Speculative-read with abort-on-snapshot-mismatch (Perf-A-SPEC, V2).
- Per-table read locks (V1 has no locks).
- io_uring submission queue for sync read (Perf-A-IORING, V2).
- SQL read frames (`0xFE`+SELECT) routed through the pool — V1 routes only
  bare-Op read frames; SQL compilation needs catalog access on the engine
  thread today (the compile cache is engine-thread-local). A V2 slice
  (Perf-A-SQL-READ) makes the compile cache shareable via `Arc<RwLock<>>`.

---

### 3. Architecture choice — Option B (recommended)

**Option A — pure-functional dispatch via `Arc<StateMachine>`.** Wrap the
state machine in an `Arc` after every apply, hand a clone to read workers,
they call `sm.read_only_op(op)` against their snapshot. *Pro:* zero
coordination. *Con:* `StateMachine::apply` takes `&mut self` (see
`crates/kessel-sm/src/lib.rs:1368`); the read cache (`kessel-cache::ReadCache`)
mutates LRU bookkeeping on every `get`; the per-op-number counter also mutates.
A no-allocation no-mutex Arc swap demands rewriting the read paths to a
`&self`-only API — bigger surface than V1 should commit to.

**Option B — read workers get an `EngineApply` reference + an additional
`apply_read_only_op(op)` arm.** Workers each spawn ONCE, hold a clone of
`EngineHandle`, and dispatch via a new low-overhead `apply_read_op_raw`
that bypasses the inflight queue and the engine `mpsc::Sender`, calling
into a shared `Arc<RwLock<StateMachine>>` (or equivalent shared owner)
acquired with `.read()`. The writer holds `.write()`; readers parallelize
under `.read()`. Pro: leverages existing MVCC dispatch in `Storage::get`,
which is already `&self`; no per-apply Arc clone overhead; the writer's
critical section is one apply at a time as today, but readers no longer
serialize behind it. Con: the read cache is `&mut`; either drop it for
parallel reads (acceptable — cache hit rate < total read throughput on
parallel hardware) OR shard the cache by key-hash to remove contention.

**Recommendation: Option B with the read cache DISABLED on the parallel
path.** Single-writer holds the lock long enough to mutate; readers do not
write to shared state at all (so no `&mut self` is needed by the read-only
code paths). The read cache stays on the writer's hot path where it
demonstrably wins (SP50); for parallel reads we trade the cache for the
parallelism — both are throughput wins, in different regimes. A V2 slice
(Perf-A-CACHE) measures whether sharding the cache adds meaningful headroom
once Perf-A baselines exist.

**Implementation note — V1 shape:** the V1 scaffold (T1) does NOT yet move
the `StateMachine` into an `Arc<RwLock<>>`. It ships the `ReadPool` thread
machinery + the `is_read_only` classifier + the `serve_cfg` opt-in plumbing
(`read_workers: Option<usize>`), with the pool dispatching every task back
through the existing `EngineHandle::apply_raw` queue. T1's benchmark therefore
measures: does the pool add coordination overhead in the OFF case? (must
be invisible). T2 wires the actual bypass via the `Arc<RwLock<StateMachine>>`
seam + `apply_read_op_raw`. This staged commit shape keeps T1 byte-identical
in default behavior while landing the contract.

---

### 4. Read-only classification

Read-only Op variants (no committed-state mutation, no catalog mutation):

| Variant | Wire tag | Notes |
|---|---|---|
| `Select` | 19 | filter scan |
| `QueryRows` | 26 | index-narrowed filter scan |
| `SelectFields` | 21 | projection scan |
| `SelectSorted` | 23 | sort + page |
| `Aggregate` | 20 | COUNT/SUM/MIN/MAX/AVG |
| `GroupAggregate` | 22 | GROUP BY agg |
| `FindBy` | 9 | equality index lookup |
| `FindByComposite` | 25 | composite-index lookup |
| `FindRange` | 18 | range-index lookup |
| `GetById` | 6 | primary-key point read |
| `Describe` | 27 | schema introspection |
| `Query` | 11 | AND-of-(Eq/Ge/Le) planner |
| `QueryExpr` | 16 | OR/NOT boolean scan |
| `GetBlob` | 7 | overflow-blob read |
| `SeqRead` | 35 | sequencer log scan |
| `Join` | 28 | inner equi-join |

Write Op variants (must go to apply thread):

`CreateType`(1), `AlterTypeAddField`(2), `Create`(3), `Update`(4),
`Delete`(5), `CreateIndex`(8), `AddUnique`(10), `AddForeignKey`(12),
`AddCheck`(13), `AddTrigger`(14), `Txn`(15), `AddOrderedIndex`(17),
`AddCompositeIndex`(24), `DropType`(29), `DropIndex`(30), `DropField`(31),
`RenameField`(32), `AddBalanceGuard`(33), `SeqAppend`(34), `XshardApply`(36),
`SeqAppendOnce`(37), `XshardDecide`(38), `XshardCommit`(39), `UpdateSet`(40),
`CreateExternalSource`(41), `DropExternalSource`(42),
`RefreshExternalSource`(43), `CommitTx`(44), `AdvanceWatermark`(45),
`ReportActiveSnapshot`(46).

Classification function (server-side, T1):

```rust
pub fn is_read_only(op: &Op) -> bool {
    !op.is_mutating()
}
```

This is a thin wrapper over the existing proto-side classifier (the same one
SP94's crash-recovery replay guard uses). Reusing it locks in the invariant:
adding a new write Op variant ⇒ `is_mutating()` returns true (the proto-side
test enforces it) ⇒ `is_read_only` returns false ⇒ the read pool refuses it
⇒ the new variant goes to the apply thread (correct). A server-side test
(`is_read_only_matches_proto_classifier_for_every_variant`) walks every Op
construction and asserts both sides agree.

Special frame tags (the read pool refuses these — they go to the apply
thread regardless of classification):
- `0xFE` SQL frames — compile-cache lives on the engine thread (V1).
- `0xFD` session frames — exactly-once dedup is engine-thread-local.
- `0xFC` AUTH, `0xFB` STATS, `0xFA` SNAPSHOT, `0xF9` TXN, `0xF8` PIPELINE,
  `0xF7` DESCRIBE_BY_NAME, `0xF6` LIST_TABLES, `0xF5` LIST_INDEXES,
  `0xF4` LIST_CONSTRAINTS — admin / control tags; all touch engine-thread
  state directly.

---

### 5. Concurrency safety

1. **Storage reads are already `&self`.** `Storage::get` (line 722 of
   `kessel-storage/src/lib.rs`) is immutable; user-data reads route through
   `mvcc::get_at_snapshot(.., u64::MAX)` which is also `&self`. Parallel
   workers can call it concurrently without locking the storage state itself
   — only the outer `RwLock<StateMachine>` needs to be held in read mode
   to keep the writer from mutating the storage during a read.

2. **Read cache is `&mut`.** `ReadCache::get` (line 47 of
   `kessel-cache/src/lib.rs`) updates LRU bookkeeping. V1 sidesteps by NOT
   consulting the cache from the parallel path — workers go straight to
   storage. The writer's hot path keeps the cache (SP50 win preserved).
   V2 (Perf-A-CACHE) measures whether per-shard cache contention beats the
   no-cache baseline.

3. **Compile cache is engine-thread-local.** SQL frames stay on the engine
   thread in V1 (the simplest invariant). V2 (Perf-A-SQL-READ) makes the
   cache shareable via `Arc<RwLock<>>`.

4. **Atomic counters** (`applied_ops_atomic`, `op_kind_counts`, `inflight`)
   are already atomics. The read pool bumps `op_kind_counts` on dispatch
   for observability symmetry — the metrics surface sees parallel reads.
   `applied_ops_atomic` is NOT bumped by reads (it tracks committed write
   ops; this preserves the SP142 semantic).

5. **Catalog access** — `StateMachine::catalog()` returns `&Catalog`,
   which is read-only. Parallel reads borrow it via the `RwLock`'s read
   guard; the writer takes write to mutate (DDL ops).

6. **MVCC active_snapshots** — readers running in parallel against
   `u64::MAX` (latest committed) don't register active snapshots, so the
   GC watermark (`AdvanceWatermark`, SP114) is unaffected. A future
   Perf-A-MVCCREAD slice could open a real snapshot at the read-pool
   level for read-after-write consistency within a connection; V1 ships
   READ COMMITTED (the same semantic the SQL auto-commit wrapper uses).

---

### 6. Determinism preserved

The deterministic state machine's apply path is **not modified**. Every
write goes through the single owning engine thread, in monotonic op-number
order, exactly as today. Reads are pure functions of committed state —
running a read in parallel against snapshot N returns the same answer as
running it serially after op N, *regardless of which worker dispatches
it.* The seed-7 corpus + Jepsen oracles + TLA+ Replication.tla + MVCC-SSI
property tests all exercise the WRITE path; reads are uniformly idempotent.

A T3 oracle test (Perf-A T3, planned) compares serial vs parallel result
on 1000 random op-mixes × 100 seeds. Any divergence is a bug.

---

### 7. Throughput model

Baseline (today, kessel-bench `profile`): `sm.apply(GetById cached)` ≈
SP10's ~245K/s memory point reads end-to-end through the server's binary
protocol on this class of hardware.

With N=8 read workers + 1 writer (24-core vulcan):
- **Read-only QPS** — expect ~`min(N, num_cpus)` × the per-thread peak,
  minus the RwLock-acquire amortized cost. Sub-linear scaling expected
  due to the shared RwLock cache line, the storage's internal SSTable
  + memtable BTreeMap traversal contention (none expected because
  `Storage::get` is `&self`), and OS scheduling. Realistic target: **≥4×**
  total throughput at N=8 vs N=1, dropping to **≥6×** at N=16.
- **Mixed workload (90% read / 10% write)** — writer no longer queues
  behind 9 reads per op; expected gain ≥3× total throughput.

These are projections from the design; T1's benchmark measures the
**baseline** (no read pool wired yet) to lock the absolute pre-state.
T2's benchmark measures **post-wiring** for the headline number.

---

### 8. Memory model

Each worker is one OS thread + ~2 MiB stack (Rust default) + a per-thread
small Vec scratch buffer for frame decode. No per-worker copy of state —
the `Arc<RwLock<StateMachine>>` shares one StateMachine across writer +
workers. The compile cache stays on the writer (no per-worker compile
cache in V1). Memory footprint adds ~2 MiB × N for N workers; default
N=`num_cpus()`/2 caps at ~24 MiB on a 24-core box.

---

### 9. Task decomposition

| Task | Scope | Status |
|---|---|---|
| **T1** | Design spec (this file) + `read_pool.rs` scaffold + `is_read_only` classifier + `ServerConfig.read_workers` plumbing + 10–15 KATs + kessel-bench `parallel-reads` mode + first vulcan numbers (PRE-WIRING baseline). | this slice |
| **T2** | `Arc<RwLock<StateMachine>>` migration + read workers dispatch through `.read()` guard + writer through `.write()`. Pool's `parallel-reads` benchmark on vulcan: PRE vs POST throughput. Headline number lands here. | next |
| **T3** | Parallel-read correctness oracle: 1000 random mixed-op workloads × 100 seeds, assert parallel result == serial result byte-for-byte. | T2 follow-up |
| **T4** | Real benchmark on vulcan — point-read QPS pre/post — extended to N=1,2,4,8,16; mixed 90/10 and 50/50 read/write blends. | T3 follow-up |
| **T5** | Perf tuning if T2 throughput is sub-linear: profile RwLock contention, shard the read cache, or rewrite the storage read API for `&self`-only fast path. | conditional on T2/T4 numbers |
| **T6** | Docs + arc closure: STATUS row update + README perf-row update + arc-progress tracker → CLOSED. | final |

Optional V2 follow-ups (each a separate arc): Perf-A-SQL-READ, Perf-A-CACHE,
Perf-A-NUMA, Perf-A-SHARD, Perf-A-MVCCREAD, Perf-A-IORING.

---

### 10. Acceptance criteria

1. Read-only QPS scales sub-linearly but **≥4×** at N=8 workers vs N=1
   (8-core vulcan slice, point reads).
2. Mixed workload (90% read / 10% write) shows **≥3×** total throughput
   improvement.
3. All existing tests pass — the read pool is opt-in via
   `read_workers: Option<usize>` (default None preserves byte-identical
   behavior).
4. Determinism oracle passes — parallel-result == serial-result on 1000
   random workloads × 100 seeds.
5. Default `cargo build -p kesseldb-server` byte-identical (no new deps).
6. seed-7 + full CI corpus stays green.

---

### 11. Weak-spot self-review (6+)

1. **Read cache contention.** Disabling the cache for parallel reads
   trades one win (in-memory hit) for another (parallelism). Net: positive
   if the parallel speedup > the cache hit-rate × cache-hit-vs-storage-cost.
   Worth measuring; T4 quantifies it.
2. **Thread startup overhead.** Pool spawned at engine start, not per-task;
   amortized over the process lifetime. Pool size capped at
   `num_cpus().max(1)` so we don't oversubscribe.
3. **Queuing imbalance under bursty reads.** V1 uses a single shared
   `mpsc::sync_channel(bound)`; under burst, slow workers don't starve
   fast workers. A V2 work-stealing deque is named as Perf-A-WORKSTEAL
   if the bench shows uneven distribution.
4. **Read-after-write within one connection.** A connection that does
   write→read sees them serialized today (single engine thread); with
   the pool, the read could land before the write commits if the
   writer's mpsc still holds the write. V1 ROUTES the write to the
   engine thread BEFORE returning to the client, so the client's NEXT
   read (after the write reply) sees the write — the read pool only
   dispatches AFTER the prior reply landed. So per-connection-FIFO is
   preserved. (The READ pool does NOT pre-dispatch reads ahead of pending
   writes from the same connection.)
5. **Engine shutdown coordination.** Pool workers must drain before the
   writer drops. V1 ships a `Drop` impl on `ReadPool` that closes the
   `Sender` and joins every `JoinHandle`; the writer thread continues
   to own its own shutdown. KAT: pool drop joins cleanly with N
   in-flight reads.
6. **Error-path attribution if a read panics on a worker.** Worker
   threads catch the panic via `std::panic::catch_unwind` (already
   `#![forbid(unsafe_code)]`-compatible) and reply
   `OpResult::SchemaError("read panicked: ..")`. The pool DOES NOT
   crash. KAT: a synthetic-panic read returns SchemaError.
7. **Counter symmetry.** `applied_ops_atomic` tracks committed writes
   only; reads do NOT bump it (preserves SP142 semantic that
   `applied_ops` counts log positions). `op_kind_counts` DOES bump for
   reads (preserves observability — Prometheus dashboards see read
   throughput).
8. **Bench-target dir contention.** All three parallel Track agents on
   vulcan share `~/KesselDB`; each uses its own `CARGO_TARGET_DIR`
   (Track A = `/tmp/kdb-target-a`, Track B = `/tmp/kdb-target-perf`,
   Track C = `/tmp/kdb-target-c`) so cargo lockfiles don't collide
   (Mighty v0.28 lesson).

---

### 12. Honest gaps

- T1 ships the contract + scaffold + baseline benchmark; the WIRING that
  delivers the parallel speedup lands in T2. T1's bench therefore shows
  baseline numbers, not the speedup. This is deliberate — the staged
  commit shape (T1 scaffolds, T2 wires, T3 oracles, T4 benchmarks the
  final number) is the highest-confidence path. The HEADLINE number
  arrives in T2's report.
- The bench mode is INTERNAL (in-process engine, no TCP), so it isolates
  the engine-thread bottleneck from the socket/codec overhead. Production
  workloads add socket cost; an external benchmark (kessel-client → real
  TCP → server with the pool) is T4 scope.

---

### 13. Locked invariants

- `is_read_only(op) == !op.is_mutating()` — proto-side classifier is the
  source of truth; adding a new write Op variant ⇒ the proto-side test
  catches it ⇒ this side is automatically right.
- `ServerConfig.read_workers == None` ⇒ identical behavior to pre-Perf-A.
- `serve_cfg(..)` with `read_workers = None` does NOT spawn the pool.
- Default `cargo build -p kesseldb-server` adds no deps (`std::thread`,
  `std::sync::mpsc` are stdlib).
- `#![forbid(unsafe_code)]` honored.
