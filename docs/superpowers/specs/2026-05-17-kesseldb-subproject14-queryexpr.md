# KesselDB Sub-project 14 — Boolean queries (OR/NOT) via the expr VM

**Date:** 2026-05-17  **Status:** spec + build (autonomous continuation).
Directly serves the goal: Postgres-grade query flexibility.

## Goal

Arbitrary boolean predicates (AND/OR/NOT, comparisons, arithmetic) over a
type, not just the conjunctive indexed `Op::Query` from SP5.

## Design

Reuse the existing deterministic `kessel-expr` VM (SP7) as a row filter:
`Op::QueryExpr { type_id, program }` filter-scans the type's contiguous key
range and returns the sorted 16-byte ids of rows where the program evaluates
true. This unifies query-filter semantics with CHECK, adds zero new
evaluation surface, gives full AND/OR/NOT for free, and is a non-breaking
addition (the SP5 indexed-conjunctive `Op::Query` fast path is untouched).
Read-only and deterministic ⇒ identical on every replica; allowed inside
`Op::Txn`.

## Scope / non-goals (honest)

- `QueryExpr` is a full scan + per-row VM eval: O(n), correct, bounded.
  Index-accelerated boolean planning (pushing indexable disjuncts to
  `idx_lookup`, union/intersect) is a future optimization — `Op::Query`
  remains the indexed fast path for the common conjunctive case.
- No projection/ordering/limit yet (returns matching ids; callers fetch).

## Tests

`kessel-sm`: `query_expr_or_not_and_combined` (OR, NOT, mixed AND/OR, empty
result), `query_expr_is_readonly_and_deterministic` (digest unchanged +
stable). `kessel-proto`: `QueryExpr` round-trips in op codec test.
99 tests total green.
