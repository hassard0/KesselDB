# SP-Analytic-Plan-MULTI — progress tracker

Closes the SP-Analytic-Plan T4 residual TPC-H Q1 gap by introducing
`Op::GroupAggregateMulti` — a single op that folds N aggregates in
ONE scan instead of N separate `Op::GroupAggregate` calls.

Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-spanalyticplanmulti-design.md`.

**Arc status: CLOSED — V1 SHIPPED 2026-05-30.**

---

## T1 — design + scaffold [DONE]

Commits:
- `d0aa4e4` docs(spec): design + progress tracker
- `eb1a417` feat(proto + sm): Op::GroupAggregateMulti scaffold + SM apply paths

Proof: new KAT `sp_analytic_plan_multi_group_aggregate_multi_wire_round_trip`
— round-trip 3 vectors (2-agg empty range_preds, Q1-shape 4-agg + one
range_pred, 5-agg w/ AVG + 2 range_preds) + back-compat byte-equal
lock for Op::Aggregate (tag 20) + Op::GroupAggregate (tag 22); n_aggs=0
rejected at decode.

## T2 — kessel-sm multi-aggregate apply [DONE]

Commits:
- `eb1a417` (T1+T2 combined commit — shared group_aggregate_multi()
  helper used by BOTH apply + read_only_op arms)
- `c74e74a` test(sm): three equivalence KATs

Proof: 3 KATs lock:
1. `sp_analytic_plan_multi_equivalence_vs_n_group_aggregate` — for a
   Q1-shape (COUNT + SUM + MIN + MAX + AVG of `v`) the per-slot Multi
   value byte-equals what an Op::GroupAggregate would return for that
   (kind, agg_field) pair on the same group; groups in ascending key
   order.
2. `sp_analytic_plan_multi_apply_eq_read_only_op` — apply path and
   read_only_op path produce byte-identical results (determinism).
3. `sp_analytic_plan_multi_range_preds_equivalence` — full-cover
   range_preds yield byte-identical result vs no range_preds
   (narrowing only accelerates).

## T3 — kessel-sql planner emits Multi for multi-aggregate SELECT [DONE]

Commits:
- `60345a3` feat(sql): compile_select projection parser refactor + emits
  GroupAggregateMulti for ≥2 aggregates / leading-col + ≥1 agg

Proof: 2 KATs lock:
1. `sp_analytic_plan_multi_sql_planner_emits_group_aggregate_multi`
   — shape correctness (≥2 aggs, leading-col + 1 agg, Q1 shape,
   back-compat single-agg, error cases for plain-col-after-agg +
   no-GROUP-BY).
2. `sp_analytic_plan_multi_sql_indexed_equals_n_single_aggregate`
   — end-to-end oracle: 5-slot Multi result byte-equals 5×GroupAggregate
   per (kind, agg_field) pair through the planner + SM.

## T4 — bench-compare TPC-H Q1 driver uses Multi + vulcan sweep [DONE]

Commits:
- `d48d3c4` feat(bench): Q1 driver uses Op::GroupAggregateMulti (4
  Op::GroupAggregate calls → 1 Op::GroupAggregateMulti call)
- `ff35ed9` fix(server): read_pool every_op_variant() covers tag 47

Vulcan sweep (3 trials × 30s × SF=0.01 × N=1,4 ×
KesselDB+Postgres+SQLite, `/tmp/bench-tpch-q1-postmulti.json`):

| DB | N=1 q/s | N=4 q/s | vs pre-MULTI |
|---|---:|---:|---:|
| KesselDB | **10.90** (was 2.80) | **41.11** (was 10.14) | **+3.89× / +4.05×** |
| Postgres | 46.53 | 186.02 | unchanged |
| SQLite | 22.74 | 23.75 | unchanged |

**Headline:** Q1 N=4 10.14 → **41.11 q/s (+4.05×)**; gap vs Postgres
closed from 18× to **4.5×**; KesselDB N=4 now BEATS SQLite N=4
(1.73× win); design predicted 3-4× lift band — measured 3.9-4.0× lift
is exactly on prediction.

## T5 — arc closure [DONE]

- STATUS.md row (Track G) — added
- BENCHMARKS.md §3f POST-MULTI column — added with full 3-DB sweep
- This tracker → CLOSED
- SP-Hash-Agg named as the next arc (parallel hash aggregate for the
  remaining ~4.5× Q1 gap + 16× Q6 gap)
- TaskList #342 ready for completion
