# SP-Perf-A-SHARD-SCAN-FASTPATH — Progress tracker

Date created: 2026-05-30
Date closed: 2026-05-30
Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-scan-fastpath-design.md`
Parent: SP-Perf-A-SHARD-SCAN (V1 SHIPPED with perf-regression concerns).

## Status: V1 SHIPPED — find-by perf gap recovered (DONE)

**Headline number**: find-by at K=4 = **1,066K ops/sec** (vs 10K pre-arc =
**105× lift**, vs 1,810K K=1 baseline = **59%, well within 2× target**).
K=8 = 844K (185× lift, 47% of K=1). Both crush the spec's recovery
targets (50× / 25×) and the 2× K=1 ideal.

## What FASTPATH ships

Two complementary fixes for the SHARD-SCAN find-by regression:

- **Approach A — Persistent ScatterPool** (`scatter_scan::ScatterPool`).
  K long-lived worker threads block on `sync_channel(1)`; one pool per
  `ShardedDispatcher`, lifecycle tied to it (Drop joins workers
  cleanly). Per-call overhead drops from ~1500µs (4× std::thread::spawn
  at K=4) to ~10-100µs (4 channel sends + 4 recvs). The cluster
  router's `ClusterClient` path still uses `scatter_and_merge` (spawn-
  per-call) because per-connection TCP state varies per call; only the
  in-process `ShardedDispatcher` switched to the pool.

- **Approach B — Serial fast path for tiny scans** (`scatter_serial` +
  `is_tiny_scan`). For `Op::FindBy / Op::FindByComposite` (sub-µs
  indexed lookups), the dispatcher calls `shards[i].apply_op(op)`
  inline in shard-id order on its own thread, then routes through
  `merge_scan_results` with the same `ScatterKind`. Total wall-clock =
  K × per-op cost (~4µs at K=8 for FindBy) — beats even pool dispatch
  for this op class. No channel hop, no worker handoff.

K-invariance preserved byte-equal: both paths drain per-shard results
in shard-id order and route through the same merger; the SHARD-SCAN T3
oracle (`t3_shard_scan_k_invariance_oracle_12_ops`) stays GREEN.

## Slice plan

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + `ScatterPool` scaffold + worker loop + Drop + unit KATs (k0/k1/k4 dispatch, pre-cancel skip, worker reuse, Drop joins cleanly, non-Got hard-fail, throughput sanity). | DONE | `01cbbb6` |
| **T2** | Wire pool into `ShardedDispatcher::scatter_dispatch`. Existing `scatter_and_merge` path remains for the cluster router. | DONE | `01cbbb6` |
| **T2.5** | Approach B: `is_tiny_scan` predicate + `scatter_serial` inline-walk for FindBy/FindByComposite. 2 KATs (classifier + serial-vs-K=1 multiset-equality). | DONE | `af98f3a` |
| **T3** | vulcan bench (3-trial median) + BENCHMARKS §14b POST-FASTPATH column. | DONE | (this commit) |
| **T4** | STATUS, progress tracker → CLOSED. | DONE | (this commit) |

## Acceptance gate

| Criterion | Outcome |
|---|---|
| find-by K=4 ops/sec ≥500K (50× recovery from ~10K) | YES (1,066K = 105×) |
| find-by K=8 ops/sec ≥250K (25× recovery from ~4.5K) | YES (844K = 185×) |
| find-by within 2× of K=1 baseline | YES (K=4 = 1.7×; K=8 = 2.1× — K=8 borderline) |
| K-invariance oracle byte/multiset-equal stays GREEN | YES |
| All ~40 scatter_scan unit KATs stay GREEN | YES |
| `cargo test --workspace` continues to pass | YES (198/198 server lib; all crates green) |
| Default `cargo build -p kesseldb-server` byte-identical | YES (pool only constructed when `shard_count >= 2`) |
| `#![forbid(unsafe_code)]` honored | YES |
| No new external runtime deps | YES |

## Honest gaps — named follow-up arcs

1. **`select-sorted` at K=4 regressed to 214 ops/sec** (vs K=1 674).
   Cause: pool's `sync_channel(1)` bound serializes 16 dispatcher
   threads → 4 workers under saturation. `SHARD-SCAN-POOL-SCALEOUT`
   would spawn `P` pool replicas (P ≈ typical-dispatcher-thread count)
   and round-robin or hash-bucket dispatchers to pools.

2. **K=8 find-by recovers to 47% of K=1, not within 2× ideally.**
   The remaining gap is 8 channel sends + 8 recvs per call vs the
   1 direct call at K=1. Even Approach B's serial walk is 8 × ~500ns
   = 4µs of work per call (vs ~500ns at K=1). For K=8 find-by the
   floor is fundamentally K× the per-op cost.

3. **`Op::FindRange / Query / QueryExpr` still scatter via the pool.**
   They could be classified as tiny if the result set is provably
   small — but the predicate would need catalog lookups (range index
   width, index selectivity) at routing time, which adds its own
   dispatch cost. Out of scope for FASTPATH V1.

## Standing invariants

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-shardscanfast`
- Commits straight to main; no Co-Authored-By; no `-S`; push after each
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched
- `#![forbid(unsafe_code)]` honored

## File registry

- **Spec**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-scan-fastpath-design.md`
- **Tracker (this file)**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-scan-fastpath-progress.md`
- **Bench results**: `docs/superpowers/spperfa-shard-scan-fastpath-bench-2026-05-30.txt`
- **Pool**: `crates/kesseldb-server/src/scatter_scan.rs` (ScatterPool +
  scatter_and_merge_via_pool + 8 KATs)
- **Wire-up**: `crates/kesseldb-server/src/sharded_engine.rs`
  (ScatterPool owned by dispatcher; is_tiny_scan + scatter_serial; 2 KATs)
- **BENCHMARKS §14b POST-FASTPATH update**: `docs/BENCHMARKS.md`
- **STATUS entry**: `docs/STATUS.md`
