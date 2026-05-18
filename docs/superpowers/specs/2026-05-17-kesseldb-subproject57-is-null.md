# KesselDB Sub-project 57 — `IS NULL` / `IS NOT NULL`

**Date:** 2026-05-17  **Status:** shipped, tested. 153 green.
Production-feature-gap pass, slice 4.

## The gap

`WHERE` could not test nullability — no `col IS NULL` / `col IS NOT NULL`,
the single most common remaining predicate gap.

## The work

The expr VM already had an `IS_NULL` opcode (2) and a `Program::is_null`
builder — it was simply never wired to SQL. SP57 adds the grammar in
`kessel-sql`'s `cmp_expr`: after the left term, `IS [NOT] NULL` emits
`[IS_NULL][field_id]` (+ `NOT` for `IS NOT NULL`, + the outer prefix
`NOT` if present). The left side must be a bare column load
(`[LOAD_FIELD][fid]`) — anything else (`5 IS NULL`) is a clean
`SqlError`, not a panic. No engine/opcode/determinism change — the
opcode already existed and is exercised by the expr corpus.

## Test (1 new, 153 total)

`is_null_predicate`: a table with a nullable `note` column; one row
inserted without `note` (→ `Value::Null`, the documented INSERT
behaviour), one with `note = 7`. `note IS NULL` → 1; `note IS NOT NULL`
→ 1; `a >= 0 AND note IS NULL` → 1; `note IS NULL OR note IS NOT NULL`
→ 2 (composition); `5 IS NULL` → compile error. Full workspace
regression green (153), determinism unaffected.

## Honest scope boundary

This is SQL `IS NULL` only. Full three-valued logic for the *other*
comparators on NULL operands (e.g. `NULL = NULL` → unknown) is not
modelled — KesselDB columns are NOT NULL by default and the common need
is the explicit null test, which is now covered. `LIKE` and subqueries
remain separate named follow-ups.
