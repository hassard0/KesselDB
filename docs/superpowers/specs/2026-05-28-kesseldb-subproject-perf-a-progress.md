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
| **T2** | `Arc<RwLock<StateMachine>>` migration + read workers dispatch through `.read()` guard (writer through `.write()`) — the actual bypass that delivers the speedup. Pool's `parallel-reads` benchmark on vulcan: PRE vs POST headline number lands here. | **OPEN** | — |
| **T3** | Parallel-read correctness oracle: 1000 random mixed-op workloads × 100 seeds, assert parallel result == serial result byte-for-byte. Determinism lock for the V2 candidates. | **OPEN** | — |
| **T4** | Real benchmark on vulcan — point-read QPS pre/post — N=1, 2, 4, 8, 16; mixed 90/10 and 50/50 read/write blends; capture absolute numbers + scaling curve into docs/STATUS.md row. | **OPEN** | — |
| **T5** | Perf tuning if T2 throughput is sub-linear: profile RwLock contention, shard the read cache, or rewrite the storage read API to be `&self`-only on the fast path. Conditional on T4 numbers. | **OPEN — conditional** | — |
| **T6** | Docs + arc closure: STATUS row update + README perf-row update + arc-progress tracker → CLOSED. | **OPEN** | — |

Optional / V2 follow-ups (each its own arc): Perf-A-SQL-READ,
Perf-A-CACHE, Perf-A-NUMA, Perf-A-SHARD, Perf-A-MVCCREAD,
Perf-A-IORING, Perf-A-WORKSTEAL.

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
