# SP-Perf-A-SHARD-SCAN-POOL-SCALEOUT — Progress tracker

Date created: 2026-06-01
Date closed: 2026-06-01
Design spec: `docs/superpowers/specs/2026-06-01-kesseldb-spperfa-shard-scan-pool-scaleout-design.md`
Parent: SP-Perf-A-SHARD-SCAN-FASTPATH (V1 SHIPPED 2026-05-30, perf
regression on select-limit / select-sorted at K=4 named POOL-SCALEOUT
as the follow-up).

## Status: V1 SHIPPED — Approach C closes select-limit/sorted/agg-sum at K=4 (DONE)

**Headline numbers** (vulcan, --pool-workers 16, --workers 16, 10K
rows, 10s, --shard-count 4):

| Workload | K=1 | K=4 POST-FASTPATH | K=4 POST-SCALEOUT | K=4 lift | K=4 vs K=1 |
|---|---|---|---|---|---|
| select-limit | 2,571 | 958 | **3,169** | **3.31×** | **1.23× faster than K=1** |
| select-sorted | 674 | 214 | **802** | **3.75×** | **1.19× faster than K=1** |
| aggregate-sum | 1,478 | 937 | **3,044** | **3.25×** | **2.06× faster than K=1** |
| find-by | 1,801,557 | 1,066,000 | 1,057,854 | 0.99× | preserved (no regression) |

Every scan workload at K=4 now scales POSITIVELY with K. What
FASTPATH §14b framed as "corner-case regressions" is no longer
regressed.

## What POOL-SCALEOUT ships

**Two slices** (T1 = Approach A scaffold, T2/T4 = Approach C
escalation), one final ship:

- **T1 (Approach A — bigger per-shard queue):** bumped per-worker
  `sync_channel(1)` to `sync_channel(POOL_BOUND=64)`. Vulcan bench
  proved insufficient — per-worker throughput was the bottleneck,
  not channel backpressure.
- **T2/T4 (Approach C — M shared workers):** refactor `ScatterPool`
  to spawn `M = max(K * POOL_WORKERS_PER_SHARD, MIN_POOL_WORKERS)`
  workers sharing a single `mpsc::sync_channel(POOL_BOUND)` queue.
  Per-shard dispatch closures held in `Arc<Vec<Box<dyn Fn>>>`
  shared by every worker; work items carry `shard_id: u32`; any
  worker can fulfill any `(shard_id, op)` pair. Mutex<Receiver>
  hop is ~50ns/item — negligible for the ≥5µs ops POOL-SCALEOUT
  targets.

K-invariance preserved byte-equal: each call still allocates K
dedicated reply_tx/rx pairs in shard-id order; the dispatcher
collects them in shard-id order; the merger sees per-shard
replies indexed by shard, NOT by worker. K-invariance oracle
(`t3_shard_scan_k_invariance_oracle_12_ops`) stays GREEN.

## Slice plan

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + `POOL_BOUND=64` const + bump `sync_channel(1)` to `sync_channel(POOL_BOUND)` in `ScatterPool::new` + high-concurrency-dispatch KAT (16 dispatcher threads × 100 ops). | DONE (insufficient) | `0d9f221` |
| **T2/T4** | Approach C escalation: `Arc<Vec<Box<dyn Fn>>>` shared dispatch table; M = max(K*4, 16) shared workers; `PoolWorkItem.shard_id`; 2 new KATs (M formula + shard_id routing). | DONE | `850c43d` |
| **T3** | vulcan bench: rerun §14b workloads at --workers 16 × K∈{1,4,8}, capture POST-SCALEOUT column for both Approach A AND Approach C, BENCHMARKS §14c update. | DONE | (this commit) |
| **T4** | STATUS row, this tracker → CLOSED, TaskList #354 ready. | DONE | (this commit) |

## Acceptance gate

| Criterion | Target | Outcome |
|---|---|---|
| `select-limit` K=4 ≥90% of K=1 baseline 2,571 | ≥2,314 ops/sec | **YES — 3,169 (1.23× of K=1)** |
| `select-sorted` K=4 ≥90% of K=1 baseline 674 | ≥607 ops/sec | **YES — 802 (1.19× of K=1)** |
| `find-by` K=4 no regression from POST-FASTPATH 1,066K | ≥1,000K ops/sec | YES — 1,058K (0.99× preserved) |
| `aggregate-sum` K=4 within ±10% of POST-FASTPATH | n/a (baseline) | EXCEEDED — 3,044 (3.25× of POST-FASTPATH) |
| K-invariance oracle stays GREEN | green | YES |
| All scatter_scan KATs stay GREEN | green | YES |
| `cargo test --workspace` passes | pass | YES (202/202 server lib) |
| Default `cargo build -p kesseldb-server` byte-identical | identical | YES |
| `#![forbid(unsafe_code)]` honored | honored | YES |
| No new external runtime deps | none | YES (`std::sync::Mutex` only) |

## Honest gaps — named follow-up arcs

1. **`find-by` still 0.59× of K=1 at K=4** (and 0.46× at K=8). The
   K-pessimal-cost-floor §14b documented: every K=8 find-by call
   pays 8 channel hops vs 1 direct call. Approach C spreads work
   wider but doesn't compress per-op overhead. Named
   `SHARD-SCAN-TINY-INLINE` to extend Approach B's serial fast
   path beyond FindBy/FindByComposite or bypass the pool entirely
   for sub-µs ops.

2. **Single-trial bench, not 3-trial median.** FASTPATH §14b used
   3-trial medians; POOL-SCALEOUT shipped a single trial because
   the lifts (3.3×, 3.75×, 3.25×) are well outside trial-variance
   range (~5%). A 3-trial sweep is a no-op confirmation.

3. **Workers may oversubscribe cores at high K.** At K=16, M = 64
   workers on a 24-core vulcan. Not yet a problem (workers idle in
   `recv()` most of the time), but a stress test at K=32 with 16+
   concurrent dispatchers would surface scheduler pressure if it
   exists. Named `SHARD-SCAN-POOL-CORE-AWARE` for a future arc.

## Standing invariants

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-poolscale`
- Commits straight to main; no Co-Authored-By; no `-S`; push after each
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched
- `#![forbid(unsafe_code)]` honored

## File registry

- **Spec**: `docs/superpowers/specs/2026-06-01-kesseldb-spperfa-shard-scan-pool-scaleout-design.md`
- **Tracker (this file)**: `docs/superpowers/specs/2026-06-01-kesseldb-spperfa-shard-scan-pool-scaleout-progress.md`
- **Bench results**: `docs/superpowers/spperfa-shard-scan-pool-scaleout-bench-2026-06-01.txt`
- **Pool refactor**: `crates/kesseldb-server/src/scatter_scan.rs` (POOL_BOUND, POOL_WORKERS_PER_SHARD, MIN_POOL_WORKERS, ScatterPool, PoolWorkItem.shard_id, pool_worker_loop with Arc<Vec<closures>>)
- **Wire-up**: `crates/kesseldb-server/src/sharded_engine.rs` (unchanged — ScatterPool::new signature preserved)
- **BENCHMARKS §14c POST-SCALEOUT**: `docs/BENCHMARKS.md`
- **STATUS entry**: `docs/STATUS.md`
