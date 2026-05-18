# KesselDB Sub-project 56 — `IN` / `BETWEEN` predicates

**Date:** 2026-05-17  **Status:** shipped, tested. 152 green.
Production-feature-gap pass, slice 3 (query-language generality).

## The gap

`WHERE` supported `=, !=, <, <=, >, >=`, `AND/OR/NOT` and parentheses,
but not `IN (...)` or `BETWEEN … AND …` — table-stakes SQL.

## The approach: pure parser desugaring (zero engine risk)

Implemented entirely in `kessel-sql`'s `cmp_expr` as rewrites into the
**already-tested** kessel-expr opcodes (no storage/engine/determinism
change whatsoever):

- `col IN (a, b, c)` → `(col=a) OR (col=b) OR (col=c)`
- `col NOT IN (...)` → `NOT (…OR…)`
- `col BETWEEN lo AND hi` → `(col>=lo) AND (col<=hi)`
- `col NOT BETWEEN lo AND hi` → `NOT ((col>=lo) AND (col<=hi))`

The post-column `NOT` (`col NOT IN …`) is parsed in addition to the
existing prefix `NOT`; both compose. Values reuse the existing `term`
parser, so ints, strings and even columns work. The resulting program is
the same `OR`/`AND`/`NOT`/compare bytecode the SP14 boolean-query path
already evaluates and that the planner falls back to (the equality
fast-path simply doesn't match, so it cleanly uses the expr VM).

## Test (1 new, 152 total)

`in_and_between_predicates`: 4-row table; `IN (10,30,99)` → 2;
`NOT IN (10,30)` → 2; `bal BETWEEN 15 AND 35` → 3; `NOT BETWEEN` → 1;
and `owner IN (10,20) AND bal BETWEEN 0 AND 10` → 1 (composition with
`AND`). Full workspace regression green (152), determinism untouched —
this slice adds no new opcodes or ops.

## Honest scope boundary

`IN (subquery)` is **not** supported (no subqueries yet — a separate,
named follow-up). `LIKE` and `IS NULL` / `IS NOT NULL` are also still
open (SP57 candidates: `IS NULL` needs an expr-VM null test). `IN`/
`BETWEEN` value lists are literals/columns, not expressions beyond what
`term` parses. These are tracked, not hidden.
