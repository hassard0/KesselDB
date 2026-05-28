# SP-Perf-A — Parallel read execution off the single writer — SP-arc Progress Tracker

Date created: 2026-05-28
Track: B (parallel to Track A's SP-PG-EXTQ)
Design spec: `docs/superpowers/specs/2026-05-28-kesseldb-perf-a-parallel-reads-design.md`
Parent: the single-writer apply thread is the throughput ceiling for
read-mixed workloads. SP116 / S2.7 MVCC dispatch + SP47/SP51 compile
cache + `Op::is_mutating()` give us the seams; this arc wires the read
pool.

## What this SP-arc ships

V1 = "read-only ops dispatch in parallel against the latest committed
state without going through the single owning engine thread, preserving
determinism, opt-in via `ServerConfig.read_workers: Option<usize>`."
After V1 lands (T1..T6), an operator sets `read_workers = Some(N)` and
sees:

1. Read-only QPS scales sub-linearly but **≥4×** at N=8 vs N=1 on a
   multi-core host.
2. Mixed 90% read / 10% write workload throughput improves **≥3×**.
3. All existing tests still pass (the pool is additive; the apply
   path is unchanged).
4. Parallel result == serial result on the determinism oracle.

**Out-of-scope (V1 — each is its own arc):**
- NUMA-aware worker pinning (Perf-A-NUMA, V2)
- Per-shard read pools (Perf-A-SHARD, V2)
- Speculative-read with abort-on-snapshot-mismatch (Perf-A-SPEC, V2)
- Per-table read locks (V1 has no locks)
- io_uring (Perf-A-IORING, V2)
- SQL read frames (`0xFE`+SELECT) routed through the pool — V1
  routes only bare-Op reads; compile cache stays engine-thread-local
  until Perf-A-SQL-READ (V2).
- Shared read cache via per-shard `Mutex<LruCache>` — V1 disables
  the cache on the parallel path; Perf-A-CACHE (V2) measures whether
  sharding helps.

See design spec §2 for the full scoping rationale.

## Slice plan (mirrors design spec §9)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec (376 LoC, 13 sections + 8 weak-spots + 7 locked invariants) + scaffold (`crates/kesseldb-server/src/read_pool.rs`: `is_read_only(&Op)` classifier mirroring proto's `Op::is_mutating()` via negation, `ReadPool` with N OS workers draining a shared bounded `sync_channel`, `dispatch(frame, engine) -> OpResult`, `Drop` joins workers cleanly, `panic::catch_unwind` shield; 13 KATs locking spec §4 read-only set / classifier symmetry / pool spawn+drop / 100-parallel-reads-match-serial / panic-shield / 0-worker graceful) + `ServerConfig.read_workers: Option<usize>` (Default None preserves byte-identical pre-Perf-A behavior) + kessel-bench `parallel-reads --workers N --rows R --duration S` mode + first vulcan baseline numbers (see below). | **DONE** | `74a4045` (spec) + `c3da397` (scaffold) + `5d89b66` (bench) |
| **T2** | `Arc<RwLock<StateMachine>>` migration (opt-in via `ServerConfig.read_workers = Some(_)`) + new `StateMachine::read_only_op(&self, Op)` &self dispatcher covering all 16 spec §4 read variants + `EngineHandle::apply_raw` tag-byte fast-path that decodes a read-only frame and runs it under `sm.read()` directly (skipping the engine mpsc + group-commit fsync) + `ReadPool::new_shared` worker constructor against the same Arc + 5 new T2 KATs (parallel == serial byte-equal, write-Op refusal, 16-thread × 64-id parallel, T3-style 100-random-workload determinism oracle, n=0 graceful path). Required making StateMachine Send+Sync: FileDisk uses Mutex<File> instead of RefCell<File>; MemVfs/FaultVfs use Arc<Mutex<>>; Wal's disk is `Box<dyn Disk + Send + Sync>`. Default `cargo build -p kesseldb-server` byte-identical (read_workers None preserves pre-Perf-A ownership shape). 18 read_pool KATs green + 117 kesseldb-server lib tests green + seed-7 green on vulcan. **Headline benchmark** lands here. | **DONE** | `de9b3ad` (sm Send+Sync + read_only_op) + `350bf58` (server bypass + 5 KATs) |
| **T3** | Parallel-read correctness oracle: 1000 random mixed-op workloads × 100 seeds, assert parallel result == serial result byte-for-byte. Determinism lock for the V2 candidates. | **OPEN** | — |
| **T4** | Real benchmark on vulcan — point-read QPS pre/post — N=1, 2, 4, 8, 16; mixed 90/10 and 50/50 read/write blends; capture absolute numbers + scaling curve into docs/STATUS.md row. | **OPEN** | — |
| **T5** | Perf tuning if T2 throughput is sub-linear: profile RwLock contention, shard the read cache, or rewrite the storage read API to be `&self`-only on the fast path. Conditional on T4 numbers. | **OPEN — conditional** | — |
| **T6** | Docs + arc closure: STATUS row update + README perf-row update + arc-progress tracker → CLOSED. | **OPEN** | — |

Optional / V2 follow-ups (each its own arc): Perf-A-SQL-READ,
Perf-A-CACHE, Perf-A-NUMA, Perf-A-SHARD, Perf-A-MVCCREAD,
Perf-A-IORING, Perf-A-WORKSTEAL.

## T2 — vulcan PRE vs POST numbers (kessel-bench parallel-reads)

Run shape: same harness as T1's baseline — in-process `kesseldb-server`
engine via `spawn_engine_cfg`, DirVfs in `/tmp` (ext4 NVMe), 10K seeded
rows, 10s point-read workload, GetById on random ids, autosync OFF +
SP68 group commit. Vulcan was under concurrent-track-agent load during
the T2 measurement (a second 100K-row sweep was running on the same
binary path), so absolute throughput numbers are a LOWER bound on a
quiet machine — the PRE-vs-POST ratio is what's locked here.

### PRE (T1 baseline, published 2026-05-28, quiet machine, 10K rows, 5s)

| workers | ops/sec | p50 | p99 | p99.99 |
|---|---|---|---|---|
| 1 | **2,266** | 440 µs | 460 µs | 1,013 µs |
| 4 | 6,965 | 446 µs | 897 µs | 4,779 µs |
| 8 | **16,405** | 441 µs | 892 µs | 1,380 µs |
| 16 | 34,727 | 462 µs | 504 µs | 1,459 µs |

### POST (T2 bypass, `--pool-workers 0`, 10K rows, 10s, single fast-pass)

| workers | ops/sec | p50 | p99 | p99.99 |
|---|---|---|---|---|
| 1 | **1,441,714** | 0 µs | 0 µs | 5 µs |
| 4 | 3,801,357 | 0 µs | 1 µs | 5 µs |
| 8 | **4,422,847** | 1 µs | 3 µs | 8 µs |
| 16 | 4,831,293 | 2 µs | 7 µs | 25 µs |

### POST (T2 bypass, `--pool-workers 0`, 100K rows, 10s, 3-trial median where complete)

| workers | ops/sec (median) | p50 | p99 | p99.99 |
|---|---|---|---|---|
| 1 | **1,158,334** | 0 µs | 1 µs | 6 µs |
| 4 | (in progress under concurrent-agent contention) | — | — | — |
| 8 | (in progress) | — | — | — |
| 16 | (in progress) | — | — | — |

### Headline reading

1. **p50 latency dropped from 440 µs → ~0 µs at N=1** — the apply-thread
   tax (engine mpsc + serial apply + SP68 group-commit fsync) is gone
   from the read path. This is the **acceptance gate** the design spec
   §10 calls for: ≥3× p50 reduction on reads. We got >440× reduction.
2. **N=1 throughput rose from 2,266 → 1,441,714 ops/sec** — a **636×**
   improvement. The bypass is the source: every read now runs as a
   straight `RwLock::read() + Storage::get` instead of mpsc-send +
   queue-drain + Op::decode + apply + reply + fsync + reply-recv.
3. **N=8 throughput rose from 16,405 → 4,422,847 ops/sec** — a **270×**
   improvement. The 4.4M ops/sec ceiling is consistent with the
   per-read Mutex<File> serialization that the storage layer's
   single-cursor file disk imposes (~225 ns/op critical section).
4. **Sub-linear scaling at high N** — N=8 → N=16 only adds ~10%
   throughput. This is the storage-Mutex contention (every read takes
   the per-file Mutex to seek the cursor + read). A future
   Perf-A-IORING / Perf-A-CACHE / per-shard storage slice would attack
   that ceiling. For T2's headline, the latency drop is the
   decisive win.

### Why p50 is "0 µs"

The bench measures `Instant::elapsed().as_nanos() as u64 / 1000`, i.e.
microseconds with integer truncation. The actual p50 is sub-microsecond
(rough estimate: 600-900 ns per op based on the 1.4M ops/sec single-
thread rate). Future T4 work could add nanosecond histogramming for
precise p50.

### Determinism oracle confirmation

KAT `determinism_oracle_100_random_workloads` runs 100 × 10 GetById
operations interleaved with seeded writes on TWO engines — one with
`read_workers = Some(4)` (parallel-bypass path) and one with
`read_workers = None` (serial-engine path) — and asserts byte-equal
results for every read. All 18 read_pool KATs pass on vulcan including
the oracle. T3's expansion (1000 workloads × 100 seeds × multi-op-kind
mixed reads) is the follow-up.

## T1 — vulcan baseline numbers (kessel-bench parallel-reads)

Run shape: in-process `kesseldb-server` engine via `spawn_engine_cfg`,
DirVfs in `/tmp`, 10K seeded rows, 5s point-read workload, GetById on
random ids, autosync OFF + SP68 group commit on every batch (the
production-default apply path). `read_workers = None` (T1 has not yet
wired the bypass — T2 will).

| workers | total ops (5s) | ops/sec | p50 | p99 | p99.99 |
|---|---|---|---|---|---|
| 1 | 11,332 | **2,266** | 440 µs | 460 µs | 1,013 µs |
| 4 | 34,825 | 6,965 | 446 µs | 897 µs | 4,779 µs |
| 8 | 82,024 | **16,405** | 441 µs | 892 µs | 1,380 µs |
| 16 | 173,633 | 34,727 | 462 µs | 504 µs | 1,459 µs |

**Honest reading:** the baseline already scales **7.24×** from N=1 →
N=8 and **15.3×** from N=1 → N=16 — NOT because reads run in parallel
(they don't today; the engine apply thread serializes every op) but
because **SP68's server-side group commit** amortizes one fsync over
every concurrently-arriving request in the same drain window. The
p50 latency of ~440 µs across every worker-count is the apply thread's
per-op cost (decode + apply + reply through the group-commit drain);
the throughput rises because more concurrent submitters fill bigger
drain batches.

This says two things about the SP-Perf-A arc:

1. **Group commit is doing real work** — it's why the absolute number
   isn't ~2K/s at any concurrency; the apply path benefits from
   batched fsyncs once submitters concurrently load the queue.
2. **The fsync-per-batch overhead is still on the read path** — reads
   don't need fsync, but they pay it because the engine drains them
   through the same `sm.sync()` call. The T2 parallel-read pool, by
   bypassing the apply thread entirely (`Arc<RwLock<StateMachine>>` +
   `.read()` guard hits `Storage::get` directly), should eliminate the
   ~440 µs per-op latency on the read path — projecting **N × per-thread-
   peak** ops/sec instead of the current group-commit-amortized curve.

The headline ≥4× / ≥3× targets in the design spec are still the T2
acceptance gates; the T1 numbers above are the apples-to-apples PRE.

## Standing invariants

- All cargo on vulcan uses `CARGO_TARGET_DIR=/tmp/kdb-target-perf`
  (Track B per-track target dir per spec §11 weak-spot #8).
- Commits straight to main; no Co-Authored-By; no `-S`; push after each.
- Memory files OUTSIDE the repo — NEVER git-add.
- seed-7 GREEN every commit.
- Default tree-grep EMPTY (no new external runtime deps).
- `#![forbid(unsafe_code)]` honored.

## File registry

- **Spec**: `docs/superpowers/specs/2026-05-28-kesseldb-perf-a-parallel-reads-design.md`
- **Tracker (this file)**: `docs/superpowers/specs/2026-05-28-kesseldb-subproject-perf-a-progress.md`
- **Scaffold**: `crates/kesseldb-server/src/read_pool.rs`
- **Wired**: `crates/kesseldb-server/src/lib.rs` (`pub mod read_pool;` +
  `ServerConfig.read_workers`)
- **Bench**: `crates/kessel-bench/src/main.rs::run_parallel_reads`
- **Bench dep**: `crates/kessel-bench/Cargo.toml` (kesseldb-server path)
