# SP-Perf-A-SHARD-SCAN-LOCAL-INDEX-FUSION — Progress tracker

Date created: 2026-06-02
Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-spperfa-shard-scan-local-index-fusion-design.md`
Parent: SP-Perf-A-SHARD-SCAN-POOL-SCALEOUT (V1 SHIPPED 2026-06-01)
TaskList: #363

## Status: IN PROGRESS — T1 (design + scaffold)

## Slice plan

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + `shard_sms` field on `ShardedDispatcher` populated from sub-engine `sm_shared()` + KAT proving every shard has Some. | DONE | (this commit) |
| **T2** | `scatter_serial` direct-borrow path (skip `apply_op` channel hop, call `read_only_op` directly). Equivalence KAT proves byte-equal to channel path. | PENDING | — |
| **T3** | vulcan bench 3 trials × K∈{1,4,8} for find-by + BENCHMARKS §14d POST-FUSION column. | PENDING | — |
| **T4** | STATUS row, progress tracker → CLOSED, TaskList #363. | PENDING | — |

## Acceptance gate

| Criterion | Target | Outcome |
|---|---|---|
| find-by K=4 ≥ 1.2M (≥12% lift) | YES | TBD |
| find-by K=4 stretch ≥ 1.4M (77% of K=1) | optional | TBD |
| find-by K=8 no regression vs 0.84M | YES | TBD |
| K-invariance oracle stays GREEN | YES | TBD |
| `cargo test --workspace` GREEN | YES | TBD |
| Default `cargo build` byte-identical | YES | TBD |
| Wire surfaces byte-untouched | YES | YES (by construction — sharded_engine.rs only) |
| K-invariance preserved | YES | YES (same merge contract) |
