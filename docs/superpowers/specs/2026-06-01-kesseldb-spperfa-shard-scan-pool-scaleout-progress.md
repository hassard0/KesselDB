# SP-Perf-A-SHARD-SCAN-POOL-SCALEOUT — Progress tracker

Date created: 2026-06-01
Design spec: `docs/superpowers/specs/2026-06-01-kesseldb-spperfa-shard-scan-pool-scaleout-design.md`
Parent: SP-Perf-A-SHARD-SCAN-FASTPATH (V1 SHIPPED 2026-05-30, perf
regression on select-limit / select-sorted at K=4 named POOL-SCALEOUT
as the follow-up).

## Status: IN PROGRESS

## What POOL-SCALEOUT ships

Approach A from the design spec: bump the per-worker channel bound
from `sync_channel(1)` to `sync_channel(64)` so 16 concurrent
dispatcher threads stop serializing behind the channel-full backpressure
when they all dispatch into the same K worker channels.

The change is one constant + one KAT. No behavior change for the
single-dispatcher case (the bound is purely a backpressure ceiling;
all dispatch ordering, reply ordering, merge semantics, and
K-invariance contracts are unchanged).

If Approach A is insufficient, the spec names B (per-dispatcher pool
replicas) and C (shared pool with shard routing) as the escalation
path.

## Slice plan

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + `POOL_BOUND=64` const + bump `sync_channel(1)` to `sync_channel(POOL_BOUND)` in `ScatterPool::new` + high-concurrency-dispatch KAT (16 dispatcher threads × 100 ops). | DONE | (T1 commit) |
| **T3** | vulcan bench: rerun §14b workloads (select-limit, select-sorted, find-by, aggregate-sum) at --workers 16 × K∈{1,4,8}, capture POST-SCALEOUT column. | DONE | (T3 commit) |
| **T4** | BENCHMARKS §14b POST-SCALEOUT column, STATUS row, this tracker → CLOSED, TaskList #354 ready. | DONE | (T4 commit) |

(T2 collapsed into T1 — the implementation is a one-line const change.)

## Acceptance gate

| Criterion | Target | Outcome |
|---|---|---|
| `select-limit` K=4 ≥90% of K=1 baseline | ≥2,318 ops/sec | (T3 result) |
| `select-sorted` K=4 ≥90% of K=1 baseline | ≥607 ops/sec | (T3 result) |
| `find-by` K=4 no regression from POST-FASTPATH 1,066K | ≥1,000K ops/sec | (T3 result) |
| `aggregate-sum` K=4/K=8 within ±10% of POST-FASTPATH | n/a | (T3 result) |
| K-invariance oracle stays GREEN | green | (T1 build) |
| All scatter_scan KATs stay GREEN | green | (T1 build) |
| `cargo test --workspace` passes | pass | (T1 build) |
| Default `cargo build -p kesseldb-server` byte-identical | identical | (T1 build) |
| `#![forbid(unsafe_code)]` honored | honored | YES |
| No new external runtime deps | none | YES |

## Standing invariants

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-poolscale`
- Commits straight to main; no Co-Authored-By; no `-S`; push after each
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched
- `#![forbid(unsafe_code)]` honored

## File registry

- **Spec**: `docs/superpowers/specs/2026-06-01-kesseldb-spperfa-shard-scan-pool-scaleout-design.md`
- **Tracker (this file)**: `docs/superpowers/specs/2026-06-01-kesseldb-spperfa-shard-scan-pool-scaleout-progress.md`
- **Bench results**: `docs/superpowers/spperfa-shard-scan-pool-scaleout-bench-2026-06-01.txt`
- **Pool bound + KAT**: `crates/kesseldb-server/src/scatter_scan.rs`
- **BENCHMARKS §14b POST-SCALEOUT update**: `docs/BENCHMARKS.md`
- **STATUS entry**: `docs/STATUS.md`
