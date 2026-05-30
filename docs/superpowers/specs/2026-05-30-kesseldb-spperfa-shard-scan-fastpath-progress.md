# SP-Perf-A-SHARD-SCAN-FASTPATH — Progress tracker

Date created: 2026-05-30
Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-scan-fastpath-design.md`
Parent: SP-Perf-A-SHARD-SCAN (V1 SHIPPED with perf-regression concerns).

## Status: IN PROGRESS

Recovers the find-by perf gap V1 SHARD-SCAN left open. Headline number
to beat: K=4 find-by ≥500K ops/sec (lifts from ~10K to ≥500K =
50× minimum). Approach A = persistent worker pool replacing per-call
`std::thread::spawn`.

## Slice plan

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + `ScatterPool` scaffold + worker loop + Drop + unit KATs. | | |
| **T2** | Wire pool into `ShardedDispatcher::scatter_dispatch`. Existing `scatter_and_merge` path remains for the cluster router. | | |
| **T3** | vulcan bench + BENCHMARKS §14 POST-FASTPATH column. | | |
| **T4** | STATUS, progress tracker → CLOSED. | | |

## Acceptance gate

| Criterion | Outcome |
|---|---|
| find-by K=4 ops/sec ≥500K (50× recovery from ~10K) | |
| find-by K=8 ops/sec ≥250K (25× recovery from ~4.5K) | |
| K-invariance oracle byte/multiset-equal stays GREEN | |
| All ~40+ scatter_scan unit KATs stay GREEN | |
| `cargo test --workspace` continues to pass | |
| Default `cargo build -p kesseldb-server` byte-identical | |
| `#![forbid(unsafe_code)]` honored | |
| No new external runtime deps | |

## Standing invariants

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-shardscanfast`
- Commits straight to main; no Co-Authored-By; no `-S`; push after each
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched
- `#![forbid(unsafe_code)]` honored

## File registry

- **Spec**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-scan-fastpath-design.md`
- **Tracker (this file)**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-scan-fastpath-progress.md`
- **Pool**: `crates/kesseldb-server/src/scatter_scan.rs`
- **Wire-up**: `crates/kesseldb-server/src/sharded_engine.rs`
