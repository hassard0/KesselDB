# SP-Perf-A-TXN-RO — Progress tracker

Date created: 2026-05-29
Design spec: `docs/superpowers/specs/2026-05-29-kesseldb-spperfa-txnro-design.md`
Parent: SP-Perf-A (parallel reads off the apply thread for 16 bare-Op
read variants; bench-suite revealed Op::Txn{ops} losses).

## What this SP-arc ships

V1 = "all-RO `Op::Txn{ops}` is statically detected and dispatched through
the Perf-A read pool, bypassing the apply write lock. Mixed-RW Op::Txn
is UNCHANGED (next arc, SP-Perf-A-TXN-RW)."

Acceptance gate: sysbench oltp-read-only at N=16 on vulcan lifts from
~680 tx/s (HEAD pre-arc) to ≥3000 tx/s (closing the Postgres-loss gap
from 7.5× to 1.5×).

## Slice plan (mirrors design spec §7)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + progress tracker (this file) | **DONE** | this commit |
| **T2** | Classifier extension (`is_read_only` recurses into Op::Txn) + 4 classifier KATs (all-RO Txn classifies true; mixed Txn false; empty Txn true; nested Txn handled). | pending | — |
| **T3** | SM `read_only_op` Op::Txn arm + per-arm KATs + dispatch wiring (`apply_raw` tag-15 + `apply` classifier swap) + determinism oracle extension. | pending | — |
| **T4** | Bench-compare RO routing + vulcan sysbench sweep + BENCHMARKS.md update. | pending | — |
| **T5** | STATUS row + arc closure. | pending | — |

## Standing invariants

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-txnro`
- Commits straight to main; no Co-Authored-By; no `-S`; push after each.
- Memory files OUTSIDE the repo — NEVER git-add.
- seed-7 GREEN every commit.
- Default tree-grep EMPTY (no new external runtime deps).
- `#![forbid(unsafe_code)]` honored.
