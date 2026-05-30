# SP-Analytic-Plan — progress tracker

Closes the SP-Bench-Suite T4 TPC-H Q1+Q6 losses by teaching
`Op::Aggregate` + `Op::GroupAggregate` to consume the `range_preds`
interface that already ships in `Op::QueryRows` (SP70).

Design spec: `docs/superpowers/specs/2026-05-29-kesseldb-spanalyticplan-aggregate-index-narrowing-design.md`.

---

## T1 — design + scaffold [PLANNED]

- Spec drafted.
- Proto: `Op::Aggregate` + `Op::GroupAggregate` gain `range_preds`
  field with empty-Vec default; encode appends only when non-empty
  (back-compat wire-prefix); decode tolerates absence.
- Every callsite updated to pass `range_preds: vec![]`.
- New wire-round-trip KAT covers `range_preds: vec![]` (existing
  bytes) + `range_preds: vec![(f, op, v)]` (new bytes).

## T2 — kessel-sm narrowing apply [PLANNED]

- `read_only_op` arms for `Op::Aggregate` + `Op::GroupAggregate`
  consume `range_preds`: intersect candidate ids via existing
  ordered-index `scan_range` machinery, then loop the candidate set
  applying the verifying program; fold into aggregate result.
- `apply` arms get the same logic.
- Equivalence KAT: same data, two runs — one with full-scan, one with
  range_preds-narrowed — produce byte-identical aggregate output.

## T3 — kessel-sql planner emits range_preds for aggregate SELECT [PLANNED]

- Refactor `try_query_rows`'s range-extraction body into a shared
  helper `extract_range_preds(ot, span)`.
- Call it from `compile_select`'s `Proj::Agg` branch (same
  conjunct-safety gate as `try_query_rows`).
- Integration KAT: `SELECT SUM(x) FROM t WHERE d >= LO AND d < HI`
  compiles to `Op::Aggregate { range_preds: [(d, 1, LO), (d, 2,
  HI)], … }` when an order index on `d` exists.

## T4 — vulcan TPC-H sweep + BENCHMARKS.md update [PLANNED]

- `tools/bench-compare/src/drivers/kesseldb_tpch.rs` issues
  `Op::AddOrderedIndex { type_id, field_id: L_SHIPDATE }` at load
  time + populates `range_preds` on the Q6 `Op::Aggregate` and the
  Q1 `Op::GroupAggregate` calls.
- 3 trials × 30s × SF=0.01 × N=1,4 on vulcan; median.
- `docs/BENCHMARKS.md` §3f + §3g get a "PRE-Analytic-Plan vs
  POST-Analytic-Plan" comparison.

## T5 — arc closure + V2 follow-up named [PLANNED]

- STATUS.md row.
- This tracker → DONE.
- SP-Analytic-Plan-MULTI named in BENCHMARKS.md as the second prong
  for Q1 (folds 4 separate `Op::GroupAggregate` scans into one
  `Op::GroupAggregateMulti` scan).
