# KesselDB Sub-project 64 — SQL `EXPLAIN` (plan visibility)

**Date:** 2026-05-17  **Status:** shipped, tested, e2e-verified. 161 green.
A "wow" differentiator: the planner intelligence is now *visible*.

## Why

SP62/SP63 made queries index-aware, but a user couldn't *see* it.
`EXPLAIN` makes the chosen plan explicit — like Postgres `EXPLAIN`, but
clearer — and is the cleanest possible slice: **pure planner output, no
execution, zero engine/determinism risk**.

## Delivered

- `kessel-sql`: `EXPLAIN <stmt>` (case-insensitive, leading) compiles the
  inner statement and returns `Stmt::Explain(plan)` — the inner op is
  **never executed**. `plan_string` describes the real plan:
  - `Composite Index Scan on t using (a, b) → verify`
  - `Index Scan on t narrowed by [a] → verify full WHERE`
  - `Seq Scan on t → filter (no usable index)` (e.g. under `OR`)
  - `Primary-Key Lookup on t (O(1))`, `Hash Join a ⋈ b`,
    `Atomic Txn (N ops)`, `Alter … Add Column (online, no lock)`, etc.
- Server (single-node engine + cluster engine): `Stmt::Explain` replies
  `Got(plan_text)` and applies nothing; `EXPLAIN` inside a transaction
  batch is a clean error.
- `kessel` CLI prints `EXPLAIN` output as text (it is plain text, not a
  row blob).

## Tests (1 new, 161 total)

`kessel-sql::explain_shows_the_plan`: single-col index narrowing →
"Index Scan"; composite → "Composite"; `OR` → "Seq Scan";
`SELECT … ID n` → "Primary-Key Lookup"; DDL/`INSERT` plans;
case-insensitive `explain`; bare `EXPLAIN` → error. E2E (CLI, live
server): the three scan modes print exactly as above. Full workspace
regression green (161); nothing is executed by `EXPLAIN`, so the
determinism/VSR corpus is untouched.

## Honest scope boundary

The plan is a single descriptive line (no cost model, no row estimates,
no nested plan tree). It accurately reflects *which* access path the
engine will take (the decision that matters here) — estimates/`ANALYZE`
are a named follow-up, not implied.
