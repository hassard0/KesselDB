# SP-Analytic-Plan-MULTI — progress tracker

Closes the SP-Analytic-Plan T4 residual TPC-H Q1 gap by introducing
`Op::GroupAggregateMulti` — a single op that folds N aggregates in
ONE scan instead of N separate `Op::GroupAggregate` calls.

Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-spanalyticplanmulti-design.md`.

**Arc status: IN PROGRESS — T1 SCAFFOLDED.**

---

## T1 — design + scaffold [IN PROGRESS]

Commits:
- `<this commit>` docs(spec): design + progress tracker
- `<next commit>` feat(proto): Op::GroupAggregateMulti variant + wire encode/decode + KAT

Proof: new KAT `sp_analytic_plan_multi_group_aggregate_multi_wire_round_trip`
— round-trip 2 vectors (empty range_preds, non-empty range_preds, ≥2
aggregates each).

## T2 — kessel-sm multi-aggregate apply [PLANNED]

Both `read_only_op` and `apply` arms (and the SP116 MVCC arm if
separate) gain a `Op::GroupAggregateMulti` branch. Equivalence KAT:
byte-equal vs N sequential `Op::GroupAggregate` calls (one per
aggregate) on the same data.

## T3 — kessel-sql planner emits Multi for multi-aggregate SELECT [PLANNED]

`compile_select` projection parser handles `≥2 aggregates` or
`leading_cols + ≥1 agg` → emits `Op::GroupAggregateMulti`. Single-
aggregate path stays unchanged for back-compat with hand-rolled
callers + byte-identical encode.

## T4 — bench-compare TPC-H Q1 driver uses Multi + vulcan sweep [PLANNED]

Replace the 4× separate `Op::GroupAggregate` calls in
`kesseldb_tpch.rs` with one `Op::GroupAggregateMulti` carrying
`aggregates: [(COUNT, 0), (SUM, L_QUANTITY), (SUM, L_EXTENDEDPRICE),
(SUM, L_DISCOUNT)]` + same `range_preds`. 3-trial × 30s × SF=0.01 ×
N=1,4 sweep on vulcan with `CARGO_TARGET_DIR=/tmp/kdb-target-multi`.
BENCHMARKS.md §3f updated with PRE-MULTI / POST-MULTI columns.

Expected: Q1 N=4 ≥ 30 q/s (≥ 3× lift from collapsing scans).

## T5 — arc closure [PLANNED]

- STATUS.md row (Track F or next letter)
- This tracker → DONE
- BENCHMARKS.md §3f PRE-MULTI / POST-MULTI comparison
- SP-Hash-Agg named as the next arc (parallel hash aggregate for the
  remaining ~5× gap)
- TaskList #342 ready for completion
