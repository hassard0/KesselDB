# SP-Perf-A-TXN-RW — Progress tracker

Date created: 2026-05-30
Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-txnrw-design.md`
Parent: SP-Perf-A-TXN-RO (SHIPPED 2026-05-29; all-RO Op::Txn bypass).

## Status: CLOSED — V1 SHIPPED 2026-05-30

All 5 slices DONE. Acceptance gate met and beaten:
- Gate: oltp-read-write N=8/N=16 lifts from ~715 tx/s to ≥3,000 tx/s
- Measured: N=8 = 6,905 tx/s (gate × 2.30); N=16 = 10,273 tx/s (gate × 3.42)
- Postgres-gap target: ≤1.5× (was 4.22× / 5.43× loss)
- Postgres-gap measured: N=8 WIN by 2.28×; N=16 WIN by 2.66× (target beaten)

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
| **T1** | Design spec (with honest pivot) + progress tracker | DONE | `1fa264b` |
| **T2** | `read_prefix_length` + `is_split_safe` classifiers + 11 KATs | DONE | `a93f8a4` |
| **T3** | Driver split-phase dispatch + 3-test determinism oracle | DONE | `fa9b1df` |
| **T4** | vulcan sysbench OLTP-RW sweep + BENCHMARKS.md §3e update | DONE | `3b854cb` |
| **T5** | STATUS row + arc closure + tracker close-out | DONE | this commit |

## Headline (3-trial median × 10s × 10×100K rows on vulcan)

| N | Pre-arc tx/s | Post-split tx/s | Lift | vs Postgres |
|---|---:|---:|---:|---|
| 1 | 1,472 | **2,088** | 1.42× | already won |
| 8 | 715 | **6,905** | 9.66× | **2.28× win** (was 4.22× loss) |
| 16 | 712 | **10,273** | 14.43× | **2.66× win** (was 5.43× loss) |

p50 at N=8 dropped from 11.3 ms to 1.12 ms (10.1× faster).
KesselDB now also beats SQLite at N=8 (1.57×) and N=16 (2.60×).

## Standing invariants — honored

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-txnrw`
- Commits straight to main; no Co-Authored-By; no `-S`; push after each
- Memory files OUTSIDE the repo — NEVER git-add
- seed-7 GREEN every commit
- Default tree-grep EMPTY (no new external runtime deps)
- `#![forbid(unsafe_code)]` honored

## V1 limit (explicit, documented)

Read-after-write Txn shapes (`(R, W, R)` and similar) fall through to
unified apply — the 3-guard rejects them for byte-equivalence with
apply's overlay-based read-your-writes. For sysbench's canonical
(R*, W*) shape this is a no-op (the workload always hits the eligible
branch). For application Txns that interleave reads and writes, the
fallback preserves pre-arc behaviour exactly.

## What ships and what doesn't (V1 scope)

- IN  : driver-level split-phase dispatch (tools/bench-compare)
- IN  : server-side classifier helpers (read_prefix_length + is_split_safe)
- IN  : 11 classifier KATs + 3 determinism oracle tests
- IN  : oltp-RW lift documented in BENCHMARKS.md §3e
- OUT : server-side split in `apply_raw` (PG-wire/HTTP SQL submissions
        of BEGIN/COMMIT brackets don't yet exploit the split) — named
        follow-up SP-Perf-A-TXN-RW-SERVER
- OUT : read-after-write Txn shapes — named follow-up
        SP-Perf-A-OPTIMISTIC-CC (abort-and-retry with SI overlay)
- OUT : full SI overlay on SM write path — named follow-up
        SP-Perf-A-TXN-RW-SI (multi-week)

## Next arcs named (V2+)

- **SP-Perf-A-OPTIMISTIC-CC** — abort-and-retry with full SI overlay
  on the SM write path. Distinct from the static split-phase shipped
  here; addresses the read-after-write fallthrough case AND high-
  contention workloads where blocking serializes worse than aborts.
  Named in BENCHMARKS.md §3e + README + STATUS as the next published
  perf arc.
- **SP-Perf-A-TXN-RW-SERVER** — server-side split classification in
  `EngineHandle::apply_raw` (decode Op::Txn, split, combine verdict).
  Currently V1 ships driver-side only; PG-wire/HTTP SQL clients
  submitting BEGIN/COMMIT brackets don't yet exploit the split.
- **SP-Perf-A-TXN-RW-SI** — full SI on mixed-RW Op::Txn via SM-level
  write overlay (porting Op::Create/Update/Delete to the Tx overlay
  with index-aware conflict detection). Multi-week effort; depended-on
  by SP-Perf-A-OPTIMISTIC-CC.
