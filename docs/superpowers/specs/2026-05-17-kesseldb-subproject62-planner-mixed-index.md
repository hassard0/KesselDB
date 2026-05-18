# KesselDB Sub-project 62 — planner: index-accelerate mixed WHEREs

**Date:** 2026-05-17  **Status:** shipped, oracle-tested. 160 green.
Production-feature-gap pass, slice 9. The "much faster" query slice —
done with a correctness oracle, not hope.

## The gap

The planner only used an index when the *entire* `WHERE` was a pure
`col = v AND col = v …` chain. The moment a query mixed equality with a
range/`LIKE`/`IN` (e.g. `WHERE owner = 100 AND bal > 60`) it fell back to
a **full table scan**. That is the common real-world query shape — and it
was O(n).

## The safe model (why this can't return wrong rows)

`Op::QueryRows` already re-verifies **every** candidate row against the
full compiled `WHERE` program (SP32 invariant). So the candidate set only
affects *speed*, never the answer — provided it is a **superset** of true
matches. An equality on an indexed column is a valid narrowing hint **iff
it is a mandatory conjunct**: a top-level `AND` term with no enclosing
`OR`/`NOT`/parentheses. Under `OR`/`NOT`, a `col = v` is *not* mandatory,
so it is **not** used as a hint (the query still runs correctly as a
verified scan).

## Delivered

`try_query_rows` now, for `SELECT * FROM t [WHERE …] [LIMIT n]`:

- compiles the **full** `WHERE` via `compile_where` (every predicate
  kind: range, `IN`/`BETWEEN`/`LIKE`/`IS NULL`, `AND`/`OR`/`NOT`) as the
  verifying program;
- extracts equality hints **only** from a WHERE span that contains no
  `OR`/`NOT`/`(` , and only for `indexed_col = literal` terms;
- emits `Op::QueryRows { program, eq_preds }`. The engine intersects the
  indexed equality hints to narrow candidates, then verifies each with
  the full program — identical result to a scan, now sub-linear when any
  mandatory equality is on an indexed column.

`ORDER BY`/`GROUP BY`/`OFFSET`/projections still fall back to the general
planner (unchanged, correct).

## The oracle (the point of this slice)

`planner_equivalence_oracle`: 120 randomized rows; **360 randomized
queries** mixing `k = K`, `k = K AND v > M`, double-range, `k = K OR
v = M`, range-first, and `NOT (k = K)`. For every one, the planned
`SELECT *` result must **exactly equal** an independent in-test
brute-force filter. It passed on the *pre-change* code (proving the
harness) and still passes after — so the index path provably never drops
or adds a row. Kept permanently as a regression guard.

One pre-existing test asserted the *old policy* ("`OR` → `Op::Select`
fallback"); SP62 deliberately changes that to `QueryRows` with **no**
hints (same result, oracle-proven). The test was updated to assert the
new safety invariant (`eq_preds` empty under `OR`) — a policy change with
behaviour preserved, not a masked regression.

## Result

`WHERE owner = 100 AND bal > 60` (indexed `owner`) is now index-narrowed
instead of a full scan, with byte-identical results — verified live and
by the randomized oracle. Full workspace green (160); determinism/VSR
corpus unaffected (no engine change — `Op::QueryRows` semantics are
untouched; only the planner emits it for more shapes).

## Honest scope boundary

Only **equality on a single-column index** narrows. Range-index
(`FindRange`) and composite-index (`FindByComposite`) candidate narrowing
are *not* wired (a `> / <` predicate is still verified by the program
over the equality-narrowed or full candidate set). That is the next perf
follow-up; correctness is already total (the program is the source of
truth). Named, not hidden.
