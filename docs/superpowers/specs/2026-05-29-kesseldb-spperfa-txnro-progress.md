# SP-Perf-A-TXN-RO — Progress tracker

Date created: 2026-05-29
Date closed: 2026-05-29 (V1 SHIPPED)
Design spec: `docs/superpowers/specs/2026-05-29-kesseldb-spperfa-txnro-design.md`
Parent: SP-Perf-A (parallel reads off the apply thread for 16 bare-Op
read variants; bench-suite revealed Op::Txn{ops} losses).

## Status: CLOSED — V1 SHIPPED

## What this SP-arc shipped

V1 = "all-RO `Op::Txn{ops}` is statically detected and dispatched through
the Perf-A read pool, bypassing the apply write lock. Mixed-RW Op::Txn
is UNCHANGED (next arc, SP-Perf-A-TXN-RW)."

## Acceptance gate — MET

Original gate: sysbench oltp-read-only at N=16 on vulcan lifts from
~680 tx/s (HEAD pre-arc) to ≥3000 tx/s (closing the Postgres-loss gap
from 7.5× to 1.5×).

**Actual result (vulcan, 3-trial median, 10s steady, 10×100K rows):**

| N | pre-arc tx/s | post-arc tx/s | Lift | vs Postgres |
|---:|---:|---:|---:|---|
| 1  | 1,241 | 2,299  |  1.85× | 7.3× faster (was 3.9× faster) |
| 8  | 641   | 16,213 | 25.3× | 4.0× faster (was 6.3× LOSING) |
| 16 | 680   | 28,977 | 42.6× | 5.7× faster (was 7.5× LOSING) |

Gate was ≥3000 tx/s at N=16; **actual = 28,977** — gate beat 9.7×.
Postgres-loss gap of 7.5× is now a 5.7× WIN.

oltp-read-write unchanged (mixed-RW still goes through apply path,
explicit V1 limit — SP-Perf-A-TXN-RW next):
- N=1: 1,472 (was 1,378), N=8: 715 (was 718), N=16: 712 (was 711).

## Slice plan (mirrors design spec §7)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + progress tracker | **DONE** | `fc8baff` |
| **T2** | Classifier extension (`is_read_only` recurses into Op::Txn) + 4 classifier KATs | **DONE** | `e2479ec` |
| **T3** | SM `read_only_op` Op::Txn arm + per-arm KATs + dispatch wiring + determinism oracle | **DONE** | `3dbe8fe` `75001e5` `fcff211` `4ebb338` |
| **T4** | Bench-compare RO routing + vulcan sysbench sweep + BENCHMARKS.md update | **DONE** | this commit |
| **T5** | STATUS row + arc closure | **DONE** | this commit |

## Standing invariants — honored

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-txnro` — YES
- Commits straight to main; no Co-Authored-By; no `-S`; push after each — YES
- Memory files OUTSIDE the repo — NEVER git-add — YES
- seed-7 GREEN every commit — YES
- Default tree-grep EMPTY (no new external runtime deps) — YES
- `#![forbid(unsafe_code)]` honored — YES

## Next arcs named

- **SP-Perf-A-TXN-RW** — mixed-RW Op::Txn bypass. Requires snapshot
  isolation on the read pool + commit-time conflict detection. Closes
  the oltp-read-write loss too.
- **SP-Perf-A-SHARD** — sharded apply queues + per-shard read pools.
  The K-shard router already exists; just need to wire per-shard pools.
