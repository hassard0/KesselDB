# SP-Perf-A-TXN-RW — Progress tracker

Date created: 2026-05-30
Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-txnrw-design.md`
Parent: SP-Perf-A-TXN-RO (SHIPPED 2026-05-29; all-RO Op::Txn bypass).

## Status: IN PROGRESS (T1 commit)

## What this SP-arc ships

V1 = "mixed-RW Op::Txn with reads-before-writes shape (the canonical
sysbench OLTP-RW shape) is split at the read/write boundary:
- read prefix runs via `sm.read().read_only_op(Op::Txn{prefix})` —
  parallel, no write lock (uses the SP-Perf-A-TXN-RO bypass)
- write suffix runs via `sm.write().apply(op_no, Op::Txn{suffix})` —
  serial, full apply with catalog/index/constraint/trigger machinery
Read-after-write Txn shapes fall through to unified apply (V1 limit;
SP-Perf-A-TXN-RW-RYW would attack this with SM-level write overlay)."

## Architectural pivot from original spec

The original spec called for full SI on mixed-RW via SP112's
`Tx::begin_rw` + `commit_with_si_check`. **Pivot rationale**: SP112
`Tx::write` operates at the raw MVCC layer (bytes-in/bytes-out, no
catalog/index/constraint machinery). The SM's Op::Create/Update/Delete
arms ARE the catalog/index machinery — porting them to the Tx overlay
is a multi-week design. Out of V1 scope. See design §2.

V1 ships a driver-level split (in `tools/bench-compare`) that
exploits the canonical "reads-then-writes" shape, preserves
byte-equivalent verdicts to unified apply, and delivers the perf win.

## Acceptance gate

oltp-read-write at N=8/N=16 lifts from ~715 tx/s to ≥3000 tx/s
(closes Postgres-loss gap from 4.2× to ≤1.5×).

## Slice plan

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec (with honest pivot) + progress tracker | this slice | (TBD this commit) |
| **T2** | `read_prefix_length` classifier + 6+ KATs | — | — |
| **T3** | Driver split-phase dispatch + determinism oracle KAT | — | — |
| **T4** | vulcan sysbench OLTP RO+RW+WO sweep + BENCHMARKS.md §3e | — | — |
| **T5** | STATUS row + arc closure + tracker close-out | — | — |

## Standing invariants — honored

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-txnrw`
- Commits straight to main; no Co-Authored-By; no `-S`; push after each
- Memory files OUTSIDE the repo — NEVER git-add
- seed-7 GREEN every commit
- Default tree-grep EMPTY (no new external runtime deps)
- `#![forbid(unsafe_code)]` honored

## Next arcs named (V2+)

- **SP-Perf-A-TXN-RW-SI** — full SI on mixed-RW Op::Txn via SM-level
  write overlay (porting Op::Create/Update/Delete to the Tx overlay
  with index-aware conflict detection). Multi-week effort.
- **SP-Perf-A-TXN-RW-RYW** — read-after-write Txn shapes (general
  read-your-writes inside split). Requires the SI overlay; depends
  on TXN-RW-SI.
- **SP-Perf-A-TXN-RW-SERVER** — server-side split classification
  (apply_raw decodes Op::Txn and splits in the engine, returning
  the combined verdict to the wire client). Currently V1 ships
  driver-side only; PG-wire/HTTP SQL clients submitting BEGIN/COMMIT
  brackets don't yet exploit the split.
- **SP-Optimistic-CC** — abort-and-retry with Cahill SSI on the
  write path. Distinct from SI overlay; addresses high-contention
  workloads where blocking serializes worse than aborts.
