# SP-PG-SQL-SUBQUERY-WHERE — non-correlated subqueries in a WHERE clause

**Date:** 2026-06-04
**Status:** CLOSED — shipped on `origin/main`.

## Goal

Support non-correlated subqueries in a `WHERE` clause over the PostgreSQL wire:

```sql
SELECT name FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100);
SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM banned);
SELECT name FROM products WHERE price = (SELECT MAX(price) FROM products);  -- scalar
```

- `col IN (subquery)` / `col NOT IN (subquery)` over a one-column inner SELECT.
- Scalar `col <op> (subquery)` for `= <> != < <= > >=` where the inner yields
  exactly one row / one column.

NON-correlated only: the inner SELECT does NOT reference the outer row.

## Design — two-phase at the gateway (engine-light, zero wire change)

The feature lives ENTIRELY at the dispatch layer (`kessel-pg-gateway`), with a
detection helper in `kessel-sql`. There is **no engine, `Op`, or wire-format
change**, so the determinism oracles over the apply path are byte-untouched.

### Phase 1 — detect (`kessel_sql::find_where_subquery`)

A raw byte scan over the SQL text finds the FIRST `(` that immediately follows
`IN`, `NOT IN`, or a comparison operator (`= <> != < <= > >=`), whose
parenthesised contents start with `SELECT`. The scan:

- **skips single-quoted string literals** (honoring the `''` escape) and
  **double-quoted identifiers**, so a `(SELECT …)` inside `'…'` is never a
  subquery;
- **balances nested parens** (`balanced_close_paren`), so an inner SELECT that
  itself contains parenthesised expressions returns the correct close paren;
- requires the operator keyword (`IN`) to be a whole token (boundary-checked),
  so an identifier like `coin` does not false-match.

It returns `WhereSubquery { op, inner_sql, paren_open, paren_close }`. `None`
for any SQL without the shape → the gateway dispatches it unchanged (every
prior path is byte-identical).

### Phase 2 — run the inner SELECT first

The inner SELECT text runs through the gateway's OWN `dispatch_query` — the
IDENTICAL render path the outer query would use. This means **any SELECT shape
that already renders is a valid inner query** (projection, `MAX(price)`
aggregate, WHERE, …) for free. The gateway then parses the inner's PG-wire
output:

- `parse_row_description` reads the inner `RowDescription`: column count + the
  first column's type OID.
- inner projects ≠ 1 column → clean `42601` error.
- `collect_first_column` walks every inner `DataRow`, collecting the first
  column's value, typed by the OID: int OIDs (int2/int4/int8) → `Number` (bare
  splice), everything else → `Text` (single-quoted, `'` doubled). DataRow
  length-`-1` cells → `Null`.
- if the inner produced an `ErrorResponse`, its message is surfaced as the
  subquery failure (correlated inner referencing an outer column lands here as
  a clean `unknown column` error — never silently-wrong rows).

### Phase 3 — splice + re-dispatch

The collected values splice into the outer query in place of `(SELECT …)`:

| outer shape | rewritten |
|---|---|
| `col IN (SELECT …)`     | `col IN (v1, v2, …)` |
| `col NOT IN (SELECT …)` | `col NOT IN (v1, v2, …)` |
| `col <op> (SELECT …)`   | `col <op> <value>` (scalar) |

The rewritten outer re-dispatches through the normal path. `IN`/`NOT IN` reuse
the existing IN-list desugar; the scalar form reuses the existing comparison
path. NULL inner cells are dropped from an IN-list (NULL never equals
anything).

### Edge cases

- **Scalar inner > 1 row** → clean `21000` cardinality error.
- **Scalar inner 0 rows / NULL scalar** → the scalar is NULL → the comparison is
  NULL/false → the outer returns no rows. Spliced as the per-row contradiction
  `col <> col`.
- **IN with empty inner** → `col IN (∅)` is false → no rows. Spliced as
  `col <> col`.
- **NOT IN with empty inner** → spliced as `col = col` (matches every non-NULL
  `col`). PostgreSQL would also return NULL-valued `col` rows here; KesselDB's
  non-NULL rows are returned — the NULL-row case is a documented V1 edge.

## Determinism

Two-phase execution rides the deterministic apply path: the inner runs (a
deterministic SELECT), then the outer runs (a deterministic SELECT). No `Op`,
wire, or storage format changes. The spliced IN-list order is the inner scan
order (deterministic). The oracles (`large_seed_corpus_is_deterministic_and_
converges`, `jepsen_3replica_partition_converges_byte_identical`, the sharded /
read-pool oracles) are untouched.

## V1 scope / named follow-ups

- NON-correlated only (`SP-PG-SQL-CORRELATED-SUBQUERY`).
- ONE subquery per WHERE (`SP-PG-SQL-MULTI-SUBQUERY`).
- `EXISTS` / `NOT EXISTS` (`SP-PG-SQL-EXISTS`).
- Subqueries in `FROM` / derived tables (`SP-PG-SQL-FROM-SUBQUERY`).
- Subqueries in the SELECT list (`SP-PG-SQL-SELECT-SUBQUERY`).
- A scalar/IN subquery whose inner column is a `NUMERIC`/float spliced into an
  IN-list (the existing IN-list term parser pushes quoted decimals as bytes, not
  coerced numbers) — `SP-PG-SQL-SUBQUERY-NUMERIC-INLIST`. Integer + text inner
  columns are fully supported.

## Files

- `crates/kessel-sql/src/lib.rs` — `find_where_subquery`, `detect_subquery_op`,
  `balanced_close_paren`, `WhereSubquery`, `SubqueryOp` + detection KATs.
- `crates/kessel-pg-gateway/src/subquery.rs` — `rewrite_where_subquery`
  (two-phase orchestration), inner-result parsing, value quoting + KATs.
- `crates/kessel-pg-gateway/src/dispatch.rs` — hooks in `dispatch_query` +
  `dispatch_query_with_params` (before the engine call).
- `scripts/sppgsqlsubquerywhere-smoke.py` — 10-stage psycopg2 smoke.
