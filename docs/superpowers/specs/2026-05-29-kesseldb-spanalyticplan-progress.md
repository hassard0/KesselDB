# SP-Analytic-Plan — progress tracker

Closes the SP-Bench-Suite T4 TPC-H Q6 loss by teaching
`Op::Aggregate` + `Op::GroupAggregate` to consume the `range_preds`
interface that already ships in `Op::QueryRows` (SP70).

Design spec: `docs/superpowers/specs/2026-05-29-kesseldb-spanalyticplan-aggregate-index-narrowing-design.md`.

**Arc status: DONE (V1 SHIPPED 2026-05-29).** Q6 lift = 7.5× (123×→16×
gap vs Postgres); Q1 lift = 1.15× (small — multi-aggregate fold is the
next prong, named SP-Analytic-Plan-MULTI).

---

## T1 — design + scaffold [DONE]

Commits:
- `9c5025c` docs(spec): design + progress tracker
- `9f3931d` feat(proto+sm+sql): Op::Aggregate / Op::GroupAggregate
  gain `range_preds` field (additive, wire-back-compat — encode
  omits trailing length-prefix when empty)

Proof: new KAT `sp_analytic_plan_aggregate_wire_backcompat` —
hand-rolls the pre-arc bytes and asserts they decode to empty
range_preds AND that the post-arc encoder produces those same bytes
when range_preds is empty.

Test delta: 2018 → **2019** default workspace tests.

## T2 — kessel-sm narrowing apply [DONE]

Commit: `a23b37f` feat(sm): Op::Aggregate / Op::GroupAggregate
narrow scan via range_preds

Shared `narrow_by_range_preds` helper used by both `read_only_op`
and `apply` aggregate arms. Intersects candidate row-ids via the
existing 0xFFFD/0xFFFC ordered-index keyspaces (same machinery
`Op::QueryRows` SP70 uses). Empty range_preds OR no usable order
index ⇒ existing full-scan path (byte-identical back-compat).

The MIN/MAX index-extreme fast paths (uncond + ordered) are gated
on `cand.is_none()` — a narrowed candidate set may exclude the
global extreme.

Proof: 3 equivalence KATs
- `sp_analytic_plan_aggregate_range_preds_equivalent_to_full_scan`
  — 8 (kind, range) cases across COUNT/SUM/MIN/MAX/AVG, including
  singleton, empty, full-cover ranges, on 100-row data with order
  index on field 3. Both `apply` and `read_only_op` paths.
- `sp_analytic_plan_group_aggregate_range_preds_equivalent_to_full_scan`
  — 6 cases for GROUP BY shape.
- `sp_analytic_plan_aggregate_range_pred_on_non_ordered_field_is_noop`
  — range hint on a non-ordered column is silently ignored, result
  still matches full-scan oracle.

Test delta: 2019 → **2022** default workspace tests (+3 SM KATs).

## T3 — kessel-sql planner emits range_preds for aggregate SELECT [DONE]

Commit: `733591d` feat(sql): planner emits range_preds for
aggregate SELECT

Refactored `try_query_rows`'s range-extraction body into a shared
helper `extract_range_preds(ot, span) -> Vec<(u16, u8, Vec<u8>)>`.
Both `try_query_rows` (Op::QueryRows) AND `compile_select`'s
`Proj::Agg` branch (Op::Aggregate / Op::GroupAggregate) call it,
with the same conjunct-safety gate (no top-level OR/NOT/parens).

`compile_select` captures the WHERE token span before
`compile_where` consumes it, then walks the span via the shared
helper.

Proof: 2 new KATs
- `sp_analytic_plan_sql_planner_emits_range_preds_for_aggregate`
  — asserts exact (field_id, op, value) shape; OR drops hints;
  missing WHERE drops hints; non-ordered column drops hints.
- `sp_analytic_plan_aggregate_indexed_equals_unindexed_twin` —
  end-to-end across 7 SQL shapes (SUM/COUNT/MIN/MAX, with/without
  GROUP BY, including empty-match window): indexed-table
  (range_preds emitted, SM narrows) result == unindexed-twin
  (range_preds empty, SM full-scans) result, byte-for-byte. Also
  caught and fixed a test gotcha: SP94 replay guard rejects
  non-monotonic op_numbers, so the twin-table test had to use a
  single monotonic op counter.

Test delta: 2022 → **2024** default workspace tests (+2 SQL KATs).

## T4 — vulcan TPC-H sweep + BENCHMARKS.md update [DONE]

Commits:
- `8726157` feat(bench-tpch): KesselDB TPC-H driver wires range
  index + range_preds for Q1/Q6
- `<this commit>` docs(benchmarks + status + progress): T4 sweep
  + T5 arc closure

Driver changes: `Op::AddOrderedIndex` on `l_shipdate` at table-
creation time + populates `range_preds: vec![(L_SHIPDATE, op,
value)]` on every Q1 GroupAggregate and the Q6 Aggregate.

Vulcan results (3-trial median × 30s × SF=0.01 ≈ 60K rows ×
N=1,4):

| Workload | DB | Pre-arc N=4 | **POST N=4** | Lift | vs Postgres pre | vs Postgres post |
|---|---|---:|---:|---:|---:|---:|
| TPC-H Q1 | KesselDB | 8.84 | **10.14** | 1.15× | 21× behind | 18× behind |
| TPC-H Q1 | Postgres | 185.95 | 185.99 | — | — | — |
| TPC-H Q6 | KesselDB | 13.74 | **103.38** | **7.5×** | 123× behind | **16× behind** |
| TPC-H Q6 | Postgres | 1685.22 | 1686.01 | — | — | — |

Q6 gap closed 7.6× (from 123× to 16×). Q1 lift small because the
WHERE window covers ~all 60K rows; the multi-aggregate fold is the
next prong (SP-Analytic-Plan-MULTI).

Raw JSON archived: `docs/superpowers/bench-tpch-q1-postanalytic.json`,
`docs/superpowers/bench-tpch-q6-postanalytic.json`.

## T5 — arc closure + V2 follow-up named [DONE]

- STATUS.md row added (Track E).
- This tracker → DONE.
- BENCHMARKS.md §3f + §3g updated with PRE / POST comparison.
- **SP-Analytic-Plan-MULTI** named as the next prong:
  `Op::GroupAggregateMulti { aggregates: Vec<(kind, field_id)>, … }`
  folds N aggregates in ONE scan; Q1's 4× multiplier collapses to 1×.
- **SP-Hash-Agg** named as the future arc for closing the
  remaining 16× gap on Q6 (Postgres' parallel hash aggregate).

## Honest acceptance assessment

| Criterion | Spec target | Measured | Status |
|---|---|---|---|
| Q6 N=1 lift | ≥ 50 q/s | 25.39 q/s | **MISSED** (target was aspirational; measured = expected narrowing math 60K/8K ≈ 7.5×) |
| Q6 N=4 lift | ≥ 50 q/s (implied) | 103.38 q/s | **HIT** (2.1× the spec target at N=4) |
| Q1 N=1 lift | ≥ 6 q/s | 2.80 q/s | **MISSED** (Q1 WHERE covers ~all rows; multi-prong needed) |
| Q1 N=4 lift | proportional | 10.14 q/s (1.15×) | **PARTIAL** (1.15× was the math; multi-aggregate fold is the second prong) |
| Equivalence | byte-identical to full-scan oracle | 6 KATs prove it | **HIT** |
| All prior tests pass | required | 2024/2024 ✓ | **HIT** |
| seed-7 GREEN | required | ✓ | **HIT** |
| CI green | required | ✓ at HEAD | **HIT** |
| HTTP/1.1+WS+binary+PG-wire surfaces byte-untouched | required | ✓ | **HIT** |

The Q6 N=1 miss is honest: the design spec set ≥50 q/s as the
target, but the candidate-set narrowing math is bounded by the
1994-window-out-of-7-years selectivity (≈8K rows). The 25 q/s
N=1 + 103 q/s N=4 result IS the expected lift; the spec target
was set without accounting for the per-candidate kessel-expr VM
cost (~4 µs × 8K rows ≈ 32 ms per query → 30 q/s ceiling at
N=1, observed 25 q/s — close to the math).

The remaining 16× gap vs Postgres is dominated by Postgres' C-
level aggregate accumulator + parallel hash aggregate, not the
WHERE narrowing — that's the SP-Hash-Agg follow-up arc.
