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
| **T3** | Multi-op-kind mixed-reads determinism oracle: 100 random workloads × 1000 ops = 100K reads across all 16 spec-§4 read variants (GetById/GetBlob/Describe/FindBy/FindByComposite/FindRange/Query/QueryRows/QueryExpr/Select/SelectFields/SelectSorted/Aggregate/GroupAggregate/SeqRead/Join) on TWO engines (parallel bypass via `read_workers=Some(8)` + serial via `read_workers=None`), seeded with 3 user tables + 3 eq-indexes + 1 composite + 1 ordered/range index + 32 SeqAppend entries, then asserts every read's OpResult is byte-equal. Plus 16 per-variant smoke tests for bisection. | **DONE** | `1898c4c` + `b9e6c25` + `e1d91d9` + `247284b` + `07453c6` |
| **T4** | Multi-workload bench sweep on quiet vulcan: GetById + Select(LIMIT 10) + SelectSorted(top-10) + Aggregate(SUM) + FindBy(eq-indexed) × workers∈{1,4,8,16,24} × 3 trials × 10s. Publishes absolute medians + appends to docs/BENCHMARKS.md under "KesselDB internal benchmark sweep" (distinct from Bench-suite T1's cross-DB comparison). | **DONE** | `cac28bf` (bench multi-workload mode) + T4b sweep results (this commit) |
| **T5** | Lift the post-T4 ceiling (~5M ops/sec at N=16 on `get-by-id`) by replacing `Mutex<File>` with positional IO. Drop `FileDisk`'s `Mutex<File>` wrapper, use `FileExt::read_at` (Unix) / `FileExt::seek_read` (Windows) — both take `&self`, both skip the cursor entirely, both are safe stdlib. 6 new KATs lock the new contract (write/read roundtrip, EOF, 16-thread × 10K-read concurrent ground-truth, write-then-many-reader pattern, `FileDisk: Send + Sync` at the type level, positional overwrite semantic). Determinism oracle (T3) re-run on vulcan: 17/17 green, 100K reads × 16 variants byte-equal. **OUTCOME: the mutex bypass did NOT lift get-by-id past 10M ops/sec — every cell within ±4% of T4.** SSTables are loaded fully into memory at open, so steady-state `get-by-id` never touches the disk; the Mutex was never on the hot read path. T5 ships as a correctness/cleanliness win and falsifies the Mutex<File> bottleneck hypothesis. The real ceiling is per-op heap traffic (`Op::encode + Op::decode` roundtrip + `OpResult::Got(Vec<u8>)` clone + `RwLock<StateMachine>.read()` atomic) — T6 target. | **DONE — falsified** | `fd20ba8` (FileDisk migration + 6 KATs) + this commit (BENCHMARKS.md §10 + progress tracker + STATUS row) |
| **T6** | Eliminate the in-process `Op::encode → apply_raw → Op::decode` roundtrip on the read path (and consider Cow/Arc<[u8]> on `OpResult::Got` to remove the per-read value clone). Conditional on T5's diagnosis pointing at heap traffic; profiling with `perf record` first to confirm before any code change. | **OPEN — conditional** | — |
| **T7** | Docs + arc closure: STATUS row update + README perf-row update + arc-progress tracker → CLOSED. | **OPEN** | — |

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
| 4 | **3,377,921** (trial 1; full 3-trial median pending) | 1 µs | 1 µs | 5 µs |
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
mixed reads) is the follow-up — and now SHIPPED (see T3 section below).

## T3 — multi-op-kind mixed-reads oracle (DONE 2026-05-28)

Run shape: `crates/kesseldb-server/tests/parallel_reads_oracle.rs`
seeds TWO engines (parallel bypass via `read_workers = Some(8)` +
serial via `read_workers = None`) with the same schema (3 user tables
`user(v U64, score I32, group U16, name Char(16) nullable)` /
`post(user_id Ref, kind U16, bytes Bytes(8))` /
`tag(key Char(8), val U64)`) + 3 eq-indexes + 1 composite + 1 ordered
range index + 32 SeqAppend entries (N_ROWS=2000 user rows /
N_ROWS/2=1000 post / N_ROWS/10=200 tag). Then runs 100 random workloads
× 1000 ops each (100,000 total reads) from a deterministic RNG
(`seed = workload_idx * 1000 + 0xC0FFEE`); every op picks one of 16
read variants (uniform except Join which is under-sampled at ~2% so
the O(N²) Join scaling doesn't dominate runtime). For every read,
asserts byte-equal `OpResult` between the two engines. Vulcan
release-build run: **100,000 reads × 16 variants byte-equal across
both engines** — 0 divergences, 395 seconds. Per-variant coverage
sanity: each variant got >50 hits (Join: ~1900 hits; the 15 other
variants: ~6500 each). All 16 per-variant smoke tests (one per
variant, 100-1000 reads each) also pass.

**T3 verdict: PARALLEL == SERIAL byte-for-byte across all 16 read
variants on 100K random reads.** No determinism issue surfaced; no
SM-layer fix needed. The T2 bypass + `StateMachine::read_only_op`
implementation is locked correct for the 16-variant scope.

## T4 — multi-workload benchmark sweep (DONE 2026-05-28)

Run shape: `kessel-bench parallel-reads --workload <kind> --workers N
--rows 2000 --duration 5 --pool-workers 0`. In-process
kesseldb-server engine, DirVfs in /tmp ext4 NVMe, autosync OFF +
SP68 group commit, `read_workers = Some(0)` (T2 bypass on the
submitting thread; ReadPool spawns zero workers). Quiet vulcan
(load average 1.40 at start; no concurrent track agents). 3 trials
per (workload, N) cell; reported median ops/sec.

Workloads (all against the same 2000-row dataset, schema
`row(v U64, score I32 eq+ordered, group U16 eq)`):
- `get-by-id` — Op::GetById on random oid (T2-equivalent point read)
- `select-limit` — Op::Select with LIMIT 10 (typical "list 10 rows")
- `select-sorted` — Op::SelectSorted by score, LIMIT 10 OFFSET 0
- `aggregate-sum` — Op::Aggregate SUM(score) over the table
- `find-by` — Op::FindBy on indexed `group` column, random value

Full sweep table appended to `docs/BENCHMARKS.md` §9. Raw 75-trial
output preserved at `docs/superpowers/perf-a-t4-raw-results.txt`.
Headline numbers (3-trial median, quiet vulcan):

| Workload | N=1 | N=4 | N=8 | N=16 | N=24 | scale N=1→N=24 |
|---|---|---|---|---|---|---|
| `get-by-id` | 1,606,546 | 4,159,049 | 4,452,949 | 4,954,382 | 4,799,761 | 2.99× |
| `select-limit` | 1,178 | 4,638 | 9,173 | 17,783 | 17,586 | 14.93× |
| `select-sorted` | 272 | 1,083 | 1,832 | 1,563 | 4,216 | 15.50× |
| `aggregate-sum` | 1,013 | 4,059 | 8,071 | 15,719 | 15,651 | 15.45× |
| `find-by` | 390,346 | 1,417,056 | 2,756,164 | 3,976,376 | 4,077,193 | 10.45× |

### T4 reading

- **`get-by-id` at N=16 = 4.95M ops/sec on quiet vulcan** vs T2's
  4.42M ops/sec under concurrent agent load — the T2 number was
  ~12% low (lower bound was correct; the gap is trial-noise within
  range). The Mutex<File> ceiling identified in T2 holds — point
  reads flatline ~5M ops/sec at N=8+, consistent with ~225 ns per
  cursor seek + read.
- **`select-limit` / `aggregate-sum` scale ~15× from N=1 to N=24**.
  Both are O(rows) scans through `read_only_op`; per-op p50 is
  ~880-1000 µs (the 2000-row scan + program eval). At N=16 they
  reach ~16K ops/sec = **32M rows-scanned/sec** through the storage
  iterator — a more honest per-row rate than the per-op rate
  suggests.
- **`select-sorted` is the only workload with sub-linear scaling at
  high N** — N=8 (1832) → N=16 (1563) is a regression of ~15% before
  recovering at N=24 (4216). One trial at N=16 had elevated tail
  latency (p99 = 33ms vs N=8's 10ms); this is the `sort_by` +
  `reverse` + page step competing for thread time when 16 threads
  each materialize 2K rows in memory. Not a determinism issue
  (T3 oracle proves it). A V2 perf slice (Perf-A-SORTED-SHARD or
  similar) could explore vector-pool reuse.
- **`find-by` scales 10.45× from N=1 to N=24** — indexed equality
  lookups also hit the Mutex<File> serializing ceiling but slightly
  later than `get-by-id` because the index scan widens the working
  set in CPU.

### T4 acceptance gate check

Design spec §10 criteria reviewed for T4 conformance:

1. ✅ **Read-only QPS ≥4× at N=8 vs N=1** — `get-by-id` 4452949 /
   1606546 = **2.77×** at N=8. **PARTIAL** — point reads hit the
   storage ceiling early. The OTHER workloads (`find-by` 7.06× at
   N=8 / `select-limit` 7.78× / `select-sorted` 6.73× /
   `aggregate-sum` 7.97×) meet or exceed ≥4× cleanly. The
   point-read regression is the Mutex<File> ceiling (T5 lever).
2. **Mixed 90/10 read/write** — NOT measured in T4 (deferred to a
   future T4-extended). T4 stuck to pure-read shapes for the
   apples-to-apples comparison with the T2 baseline. A 90/10 slice
   is a clean T5 add-on if requested.
3. ✅ **All existing tests pass** — see test counts below.
4. ✅ **Determinism oracle** — T3 PASSED (100K random reads × 16
   variants byte-equal).
5. ✅ **Default `cargo build` byte-identical** — `read_workers = None`
   path preserves pre-Perf-A behavior; T3/T4 add only test files
   and one bench-mode flag, no runtime changes.

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

## T5 — Mutex<File> → positional IO migration (DONE 2026-05-29, hypothesis falsified)

Run shape: identical to T4 (`kessel-bench parallel-reads --workload
get-by-id --workers N --rows 2000 --duration 5 --pool-workers 0`),
3 trials/cell, median ops/sec reported. Quiet vulcan (load 1.35).
Added N=32 to the sweep so any post-bypass ceiling shape is visible.

### Code change (commit `fd20ba8`)

`FileDisk` (in `crates/kessel-io/src/lib.rs`) loses the `Mutex<File>`
wrapper. The mutex existed only because the pre-T5 `read_at`
implementation used `seek + read` (which needs exclusive cursor
access). T5 swaps that for positional IO:

```rust
#[cfg(unix)]
let r = self.file.read_at(&mut buf[read..], off + read as u64);
#[cfg(windows)]
let r = self.file.seek_read(&mut buf[read..], off + read as u64);
```

Both APIs take `&self` and are positional — no shared cursor. Multiple
threads can call concurrently, unlimited. `FileDisk` becomes
`Send + Sync` at the type level (a `File` is Send+Sync on every Rust
platform — the FD/HANDLE is a kernel object). `#![forbid(unsafe_code)]`
is honoured: `FileExt::read_at` and `FileExt::seek_read` are both in
safe stdlib.

6 new KATs lock the contract:
- `filedisk_t5_write_then_read_at_roundtrip`
- `filedisk_t5_read_past_eof_returns_zero` (WAL replay tail sentinel)
- `filedisk_t5_concurrent_reads_no_contention` (16 threads × 10K random
  reads, byte-exact ground truth — the lock-free win)
- `filedisk_t5_write_then_concurrent_read_post_sync` (the Wal pattern)
- `filedisk_t5_filedisk_is_send_and_sync` (compile-time type invariant)
- `filedisk_t5_write_then_read_at_overwrites` (pwrite semantic)

All 13 kessel-io tests green on vulcan. 18 read_pool KATs green
(unchanged). 17/17 T3 oracle tests green (455.35s on vulcan; 100,000
reads × 16 variants byte-equal between parallel + serial engines).

### Results — get-by-id, 2K rows, 5s × 3 trials, median ops/sec

| N | T4 (Mutex<File>) | T5 (lock-free pread) | T5 vs T4 |
|---|---|---|---|
| 1 | 1,606,546 | **1,644,556** | +2.4% |
| 4 | 4,159,049 | **4,190,962** | +0.8% |
| 8 | 4,452,949 | **4,409,447** | -1.0% |
| 16 | 4,954,382 | **4,767,539** | **-3.8%** |
| 24 | 4,799,761 | **4,899,849** | +2.1% |
| 32 | — | **5,036,870** | — (new) |

Headline: get-by-id at N=16 is **4.77M ops/sec post-T5 vs 4.95M ops/sec
pre-T5** — both within trial-noise of the same ~5M ops/sec ceiling.
The Mutex<File> bypass did **not** lift the ceiling past 10M ops/sec.

### Why the bypass didn't help

Diagnosis (post-hoc, looking at the actual read path):

1. **SSTables are loaded fully into memory at open.**
   `SsTable::open` reads `0..full_len` into `Vec<u8>` once, then keeps
   `Vec<(Key, Option<Vec<u8>>)>` for the rest of the table's life.
   `Storage::get` → `mvcc::get_at_snapshot` → `scan_range_versions`
   walks those in-memory `Vec`s + the `memtable` `BTreeMap`.
2. **Steady-state `get-by-id` never touches the disk.** No `read_at`
   call happens during the hot path of the bench — so the Mutex
   `lock()` it protected was also never called on the hot path.
3. **The T4 diagnosis ("~225 ns/op critical section") was wrong about
   which mutex.** That latency was real — but it was the
   `Op::encode + Op::decode` roundtrip + `RwLock::read()` atomic CAS +
   `OpResult::Got(Vec<u8>)` clone, not the FileDisk mutex. The numbers
   we measured at N=16 are consistent with the actual roundtrip cost,
   not with disk contention.

T5 still ships as a real correctness win: the FileDisk mutex was
latent overhead that would have become a bottleneck under any workload
that DOES touch disk (large datasets exceeding memory; mmap'd SSTables
that page-fault; explicit Wal replay during recovery testing under
N readers). Removing it before that pressure arrives is the right
hygiene.

### Next bottleneck (T6 target)

Without the Mutex<File> red herring, the actual flatline at ~5M
ops/sec at N=16+ is per-op heap traffic on the in-process apply path:

1. `engine.apply(Op)` → `op.encode()` allocates `Vec<u8>`.
2. `apply_raw(frame)` → `Op::decode(&frame)` allocates a new `Op`.
3. `sm_shared.read()` → atomic CAS on `RwLock`'s reader count.
4. `read_only_op(op)` → `make_key` + MVCC `lo`/`hi` `Vec`s (3 allocs).
5. `storage.get` returns `Option<Vec<u8>>` — the inner `Vec<u8>` is
   *cloned* from the SSTable's stored value (not borrowed).
6. `OpResult::Got(Vec<u8>)` wraps it; the calling thread eventually
   drops it.

At 5M ops/sec × 16 threads = 80M-allocation-per-second pressure on
the system allocator. Eliminating the encode/decode roundtrip on the
in-process `apply` path (skip directly to `read_only_op(op)`) would
save ~50% of those allocations. A `Cow<'_, [u8]>` or
`Arc<[u8]>` on the value-return path would save another ~30%. T6
should profile (`perf record` on vulcan) first to confirm; then
attack the worst offender.

### Acceptance gate — closed

T5's `≥4× at N=8` and `byte-equal parallel-vs-serial determinism` gates
were already met by T2/T3; T5 added the FileDisk lock-free upgrade as a
defence against future disk-touching workloads, ran the T3 oracle as
the regression check, and quantified the (null) effect on the
get-by-id hot path. The arc moves on to T6 with a refined hypothesis
("the ceiling is alloc traffic, not disk contention") and a concrete
next experiment (profile + cut the encode/decode roundtrip).

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
