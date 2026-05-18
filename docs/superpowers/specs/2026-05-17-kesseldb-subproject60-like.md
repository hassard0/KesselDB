# KesselDB Sub-project 60 — `LIKE` pattern matching

**Date:** 2026-05-17  **Status:** shipped, tested, e2e-verified. 158 green.
Production-feature-gap pass, slice 7.

## The gap

No text pattern matching — a very common SQL predicate. SP60 adds
`col LIKE 'pat'` / `col NOT LIKE 'pat'`.

## Delivered

- **Expr-VM opcode `LIKE` (20)** — pops pattern + value (both `Bytes`),
  pushes `Int 0/1`. Trailing NULs are trimmed from the value first so
  zero-padded fixed-width `CHAR(n)` text matches naturally; non-text
  operands deterministically yield false.
- **`like_match`** — a deterministic, allocation-free, non-recursive
  wildcard matcher: `%` = any (incl. empty) byte run, `_` = exactly one
  byte. Classic single-backtrack two-pointer, O(|s|·|p|) worst case,
  bounded by the VM gas limit. Determinism is preserved (same inputs →
  same result on every replica) — verified by the full VSR/determinism
  corpus staying green with the new opcode.
- **SQL**: `kessel-sql` `cmp_expr` gains a `LIKE` branch (and `NOT LIKE`
  via the existing post-column `NOT`), composing with `AND`/`OR`/`NOT`
  and the rest of the predicate grammar. `Program::like()` builder added.

## Tests (2 new, 158 total)

- `kessel-expr::like_match_semantics` — 15 cases: literal,
  case-sensitivity, `%` prefix/suffix/contains, `_` exact-length, mixed,
  `%`-only, empty/empty, empty-pattern, no-match, backtracking,
  multi-`%`.
- `kessel-sql::like_predicate` — `CHAR(16)` names; `LIKE 'Al%'` → 3,
  `LIKE 'Alic_'` → 1, `LIKE '%b%'` → 2, `NOT LIKE 'Al%'` → 1, and
  `LIKE 'A%' AND LIKE '%e'` → 1 (composition).
- E2E (CLI, live server): `LIKE 'Al%'` and a projected `LIKE '%b%'`
  render correctly. Full workspace regression green (158).

## Honest scope boundary

`%` and `_` only (the SQL-92 core); no `ESCAPE` clause and no
`ILIKE`/regex. Matching is byte-wise and case-sensitive (consistent with
the rest of KesselDB's value comparisons). These are named, non-gating
follow-ups, not hidden.
