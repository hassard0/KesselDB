# SP-Perf-A-SHARD-SCAN — Progress tracker

Date created: 2026-05-30
Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-scan-design.md`
Parent: SP-Perf-A-SHARD-APPLY (closed; shipped K=N apply path at
14.93M ops/sec at K=8 on vulcan, 3.19× over T7 ~5M ceiling). That
arc explicitly left the scan-correctness gap open: scan ops at K>=2
route to **shard 0 only**, returning ~1/K of the data.

## Status: IN PROGRESS

## What SHARD-SCAN ships

Wires the SP-A scatter-merge machinery (`scatter_scan.rs`,
~4300 lines, already used by the cluster router for network-attached
shards) into the in-process sharded engine. Same machinery, same
merge contract — just a different `ShardCaller` implementation
(`InProcShardCaller` that calls `EngineHandle::apply_op` directly
instead of `ClusterClient` doing TCP).

After this arc:

1. All 12 scan ops (Select, QueryRows, SelectFields, SelectSorted,
   Aggregate, GroupAggregate, GroupAggregateMulti, FindBy,
   FindByComposite, FindRange, Query, QueryExpr) fan out across every
   shard.
2. Per-shard partial results are merged byte-identically (Sorted,
   Aggregate kinds 0..3) or multiset-equivalently
   (Unordered/OidConcat) to the K=1 baseline.
3. Op::Aggregate kind=4 (AVG) returns SchemaError at K>=2 (the wire
   reply lacks count; SHARD-SCAN-AVG follow-up changes the wire
   shape). K=1 AVG unchanged.
4. Op::Join stays at `ShardZero` — non-goal for V1 (SHARD-JOIN arc).
5. Op::Txn stays at `ShardZero` — SHARD-XTXN's job.

## Slice plan

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + `InProcShardCaller` scaffold + new `ScatterKind::AggregateMerge / GroupAggregateMerge / GroupAggregateMultiMerge` variants + their merge functions + unit KATs. | DONE | (this commit) |
| **T2** | Sharded engine routes scan ops via `scatter_and_merge`. New `ShardRoute::Scatter(ScatterKind)` variant. Integration KATs at K=4/K=8 for all 12 scan ops. | — | — |
| **T3** | K-invariance oracle: 100-row workload × 12 scan variants × K∈{1,4,8} byte/multiset-equal assertion. | — | — |
| **T4** | vulcan bench: YCSB-A/B + TPC-H Q6 + BENCHMARKS update. | — | — |
| **T5** | Arc closure: STATUS, BENCHMARKS, progress tracker, TaskList ready. | — | — |

## Acceptance gate

| Criterion | Outcome |
|---|---|
| All 12 scan op KATs pass at K=4 + K=8 | — |
| K-invariance oracle byte/multiset-equal K=1 vs K=4 vs K=8 | — |
| `cargo test --workspace` continues to pass | — |
| YCSB-A scan throughput lifts at K>1 on vulcan | — |
| TPC-H Q6 returns correct result (was 1/K) at K>1 | — |
| Default `cargo build -p kesseldb-server` byte-identical | — |
| `#![forbid(unsafe_code)]` honored | — |
| No new external runtime deps | — |

## Standing invariants

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-shardscan`
- Commits straight to main; no Co-Authored-By; no `-S`; push after each
- Memory files OUTSIDE the repo
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched
- `#![forbid(unsafe_code)]` honored

## File registry

- **Spec**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-scan-design.md`
- **Tracker (this file)**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-scan-progress.md`
- **Scaffold + routing**: `crates/kesseldb-server/src/sharded_engine.rs`
- **New ScatterKind merges**: `crates/kesseldb-server/src/scatter_scan.rs`
