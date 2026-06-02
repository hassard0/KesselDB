# SP-Perf-A-SHARD-SCAN-LOCAL-INDEX-FUSION — Progress tracker

Date created: 2026-06-02
Date closed: 2026-06-02
Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-spperfa-shard-scan-local-index-fusion-design.md`
Parent: SP-Perf-A-SHARD-SCAN-POOL-SCALEOUT (V1 SHIPPED 2026-06-01)
TaskList: #363

## Status: V1 SHIPPED — direct-borrow scatter_serial (DONE_WITH_CONCERNS)

**Headline (3-trial median, vulcan, --workers 16, 10K rows, 10s,
--workload find-by)**:

| Config | K=1 | K=4 | K=8 | K=16 |
|---|---|---|---|---|
| WITH `--pool-workers 16` | 1,806K | **1,072K** (+1.4% vs §14c 1.058M) | 849K (+1.5%) | 614K (new) |
| WITHOUT `--pool-workers`  | 19K | **1,084K** (matches WITH-POOL) | 848K | — |

**WITH-POOL: FUSION lift is in trial noise. Spec target of 10-20% lift
on the 1.07M K=4 figure NOT met** — the apply_op path was already
taking the SP-Perf-A T6 fast path under the read guard (sub-engines
had sm_shared populated because the caller passed --pool-workers 16
which propagated to sub_cfg.read_workers).

**NO-POOL: FUSION's structural fix is the honest delivery.** Pre-FUSION,
sub-engines without read_workers had `sm_shared = None` → every read
fell through to apply_raw → mpsc → apply thread with write-guard
contention. Post-FUSION, sub-engines force `sm_shared` regardless of
caller cfg, so `scatter_serial`'s direct-borrow path always fires.
`--pool-workers` becomes a no-op for find-by at K>=2.

**K=4 K=1 gap (41%) unchanged.** TINY-INLINE-named structural floor:
FindBy on a secondary index has no primary-key routing; every shard
must be queried; per-call cost is K × per-shard-read. Not addressed
by FUSION.

## What FUSION ships

Two tightly-coupled fixes:

1. **`spawn_sharded_engine_cfg` forces sub-engine `sm_shared`** —
   `sub_cfg.read_workers = Some(0)` when caller didn't specify. With
   `Some(0)`, `perfa_enabled = true` so the SM is wrapped in
   `Arc<RwLock<>>`; the read pool is constructed with 0 workers
   (graceful submitting-thread fall-through, no real worker threads).

2. **`ShardedDispatcher::shard_sms`** snapshots each sub-engine's
   `sm_shared()` at construction. `scatter_serial` walks this Vec
   directly when every slot is Some, bypassing `apply_op`'s
   `self.sharded.is_some()` branch + `is_read_only` classifier
   recursion + op_kind_counts atomic bump.

K-invariance preserved byte-equal: both paths collect results in
shard-id order and route through the same `merge_scan_results`
with the same `ScatterKind`.

## Slice plan

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1+T2** | Design spec + progress tracker + ShardedDispatcher.shard_sms scaffold + force sub-engine sm_shared in spawn path + scatter_serial direct-borrow path + 4 KATs. | DONE | `c6c50c6` |
| **T3** | Vulcan 3-trial median bench (WITH-POOL + NO-POOL sweeps, K∈{1,4,8,16}) + BENCHMARKS §14d. | DONE | `e568596` |
| **T4** | STATUS row + this tracker → CLOSED + TaskList #363. | DONE | (this commit) |

## Acceptance gate

| Criterion | Outcome |
|---|---|
| find-by K=4 ≥1.2M ops/sec (≥12% lift over §14c 1.07M) WITH-POOL | NO (1.072M = +1.4%; in noise) |
| find-by K=4 ≥1.4M (stretch, 77% of K=1) WITH-POOL | NO |
| find-by K=4 no-regression vs §14c 1.058M WITH-POOL | YES (1.072M = +1.4%) |
| find-by K=8 no-regression vs §14c 836K WITH-POOL | YES (849K = +1.5%) |
| find-by K=4 ≥K=1 NO-POOL (structural fix) | YES (1.084M >> 19K K=1 no-pool) |
| K-invariance oracle byte/multiset-equal stays GREEN | YES |
| All scatter_scan unit KATs stay GREEN | YES |
| `cargo test --workspace` continues to pass | YES (206/206 kesseldb-server lib; 217/217 with gateway features) |
| Default `cargo build -p kesseldb-server` byte-identical | YES |
| `#![forbid(unsafe_code)]` honored | YES |
| No new external runtime deps | YES |

## Test surface

- kesseldb-server lib: 202 → 206 (+4 FUSION KATs):
  - `fusion_t1_shard_sms_populated_when_read_workers_unset` (K∈{2,4,8})
  - `fusion_t2_serial_fast_equals_channel_byte_for_byte` (5 FindBy values)
  - `fusion_t2_k_invariance_findby_default_cfg` (6 values × K∈{1,4,8})
  - `fusion_t2_fallback_to_channel_when_slot_missing`
- K-invariance oracle `t3_shard_scan_k_invariance_oracle_12_ops` GREEN.
- Workspace `cargo test --workspace --release --lib` GREEN.

## Honest gaps — named follow-up arcs

1. **K=4 K=1 gap still 41% (1.07M vs 1.81M).** The structural floor
   SHARD-SCAN-TINY-INLINE named: FindBy on a secondary index has no
   primary-key routing; every shard must be queried; per-call cost
   is K × per-shard-read. Closing this requires either (a) replicating
   secondary indexes to enable single-shard routing for some FindBy
   shapes, or (b) parallelizing the scatter walk for tiny ops (currently
   serial). Both are larger arcs.
2. **`--pool-workers` becomes a no-op for find-by at K>=2 post-FUSION.**
   Intentional outcome: FUSION wiring makes the dispatcher's tiny-scan
   path always take direct-borrow regardless of `--pool-workers`. The
   flag still matters for non-tiny ops (select-*, aggregate-*) which
   scatter via the pool.
3. **WITH-POOL config got effectively zero lift (in trial noise).** The
   spec hypothesized the apply_op path had ~250ns of overhead; the
   actual measured per-call cost in WITH-POOL was already ~14µs
   dominated by the read_only_op work itself (each shard does a real
   secondary-index lookup + value decode + oid-payload emit). Channel-
   bypass savings of ~10ns/shard are invisible at that scale.
