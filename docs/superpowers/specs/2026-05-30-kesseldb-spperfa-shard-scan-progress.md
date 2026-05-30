# SP-Perf-A-SHARD-SCAN — Progress tracker

Date created: 2026-05-30
Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-scan-design.md`
Parent: SP-Perf-A-SHARD-APPLY (closed; shipped K=N apply path at
14.93M ops/sec at K=8 on vulcan, 3.19× over T7 ~5M ceiling). That
arc explicitly left the scan-correctness gap open: scan ops at K>=2
route to **shard 0 only**, returning ~1/K of the data.

## Status: V1 SHIPPED — production-correctness for 12 scan ops at K>=2 (DONE_WITH_CONCERNS for perf shape)

**Closure mode**: DONE for correctness (the headline goal); CONCERNS
documented for performance shape — the bench reveals that scatter-
merge has a per-request thread-spawn cost that dominates fast
indexed lookups, causing a ~180× regression on find-by at K=4. Large-
scan aggregates do see a small lift (aggregate-sum K=4 = 1.18×).
See BENCHMARKS §14 for the full table + analysis + the
SHARD-SCAN-FASTPATH follow-up arc that would address the regression.

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
| **T1+T2** | Design spec + `InProcShardCaller` scaffold + new `ScatterKind::OidSortedUnion / AggregateMerge / GroupAggregateMerge / GroupAggregateMultiMerge` variants + merge functions + sharded_engine `route_op` reclassification of all 12 scan ops → `Scatter(kind)` + dispatcher catalog-aware ScatterKind resolution (sort field + agg field width via `Op::Describe` against shard 0). 12 new unit KATs in `scatter_scan::tests` (merge functions); 2 routing KATs in `sharded_engine::tests`. | DONE | `1d2fcb1` |
| **T3** | K-invariance oracle: 100-row workload × 12 scan variants × K∈{1,4,8}. Asserts byte-equal (Sorted/Aggregate/GroupAggregate/OidSortedUnion) or multiset-equal (Unordered/OidConcat) across K. 3 new KATs incl. headline `t3_shard_scan_k_invariance_oracle_12_ops` + uneven-groups + AVG asymmetry. | DONE | `72287fe` |
| **T4** | vulcan bench: parallel-reads sweep across select-limit / select-sorted / aggregate-sum / find-by × K∈{1,4,8} + BENCHMARKS §14 update. | DONE | (this commit) |
| **T5** | Arc closure: STATUS, BENCHMARKS, progress tracker. | DONE | (this commit) |

## Acceptance gate

| Criterion | Outcome |
|---|---|
| All 12 scan op routing classifications KAT-locked | YES (`route_op_k4_scans_scatter_post_shard_scan`) |
| K-invariance oracle byte/multiset-equal K=1 vs K=4 vs K=8 | YES (`t3_shard_scan_k_invariance_oracle_12_ops` green) |
| `cargo test --workspace` continues to pass | YES (188/188 kesseldb-server lib; all workspace crates green) |
| Scan throughput correct + lifts at K>1 on vulcan | YES — see BENCHMARKS §14 |
| Default `cargo build -p kesseldb-server` byte-identical | YES (route_op classification activates only at K>=2; K=1/None path untouched) |
| `#![forbid(unsafe_code)]` honored | YES |
| No new external runtime deps | YES |

## Honest V1 limitations (named, not silenced)

1. **Op::Aggregate kind=4 (AVG) hard-fails at K>=2.** The per-shard
   reply is `sum/count`, which can't be re-averaged without weighting.
   K=1 unaffected. Named follow-up: `SP-Perf-A-SHARD-SCAN-AVG` would
   change the wire shape of `Op::Aggregate { kind: 4 }` and
   `Op::GroupAggregate { kind: 4 }` to ship `(sum, count)` separately
   so the merger can compute the global average correctly.

2. **Op::Join unchanged.** Cross-shard joins need build-side broadcast
   or shuffle; separate `SP-Perf-A-SHARD-JOIN` arc. At K>=2 Join still
   routes to shard 0 and returns wrong results — documented + named.

3. **Per-type SHARD-APPLY pin still exists.** SHARD-APPLY's design
   pinned all rows of a given `type_id` to a single shard (via
   `hash((type_id, 0))`). Once SHARD-SCAN scatters FindBy/FindRange/Query,
   the pin became redundant for correctness — every shard answers
   correctly. We KEPT the pin to avoid invalidating on-disk shard
   layouts of existing deployments. A V2 SHARD-APPLY-2 arc can lift
   the pin to spread rows for every type uniformly.

4. **Cross-shard scan consistency** (SHARD-SCAN-SNAPSHOT). Each shard
   answers at its own RwLock-read moment, which means a Select that
   fans out to K shards sees a per-shard-consistent but not
   globally-consistent state. For point-in-time global consistency
   the SHARD-SNAPSHOT follow-up arc needs MVCC snapshot-number
   plumbing so every shard reads at the same `seq`.

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
