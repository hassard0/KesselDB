# KesselDB Sub-project 28 — SQL text layer (kessel-sql)

**Date:** 2026-05-17  **Status:** spec + build. The headline "complete
database" capability: KesselDB now accepts SQL text.

## What it does

`kessel_sql::compile(sql, &Catalog) -> Result<Op, SqlError>` — a tokenizer +
recursive-descent parser compiling one statement to one existing `Op`:

- `CREATE TABLE t (col TYPE [NOT NULL], ...)` → `CreateType`
  (types: U8..U128, I8..I128, BOOL, TS, CHAR(n), BYTES(n), REF)
- `INSERT INTO t ID <n> (cols) VALUES (...)` → `Create` (explicit object id —
  the engine never invents ids; values codec-encoded by field kind)
- `DELETE FROM t ID <n>` → `Delete`
- `SELECT <* | cols | COUNT(*)|SUM|MIN|MAX(col)> FROM t [WHERE expr]
  [GROUP BY col] [ORDER BY col [DESC]] [LIMIT n] [OFFSET m]` →
  `Select` / `SelectFields` / `Aggregate` / `GroupAggregate` / `SelectSorted`
- `WHERE` compiles to a deterministic **kessel-expr** program: `= != < <= >
  >=`, `AND`/`OR`/`NOT`, parentheses, columns resolved by name to field ids.

Catalog-aware (table/column name resolution, value encoding). Zero new
execution surface — SQL is pure front-end sugar over the proven Ops, so it
inherits determinism, replication, constraints, indexes for free.

## Scope / non-goals (honest)

Constrained, well-defined subset: single-table, explicit object ids (no
auto-increment — determinism), no JOINs/subqueries/HAVING/DISTINCT, ORDER BY
returns whole rows (projection+sort not combined in one Op). Every supported
form maps cleanly to an existing Op; nothing faked. Wiring SQL into the TCP
server as a text endpoint is the next slice.

## Tests

`end_to_end_sql` (CREATE/INSERT/SELECT COUNT/SUM/`*`+WHERE-AND/ORDER BY
DESC LIMIT/DELETE through a real StateMachine), `where_or_not_paren`
(OR/NOT/parentheses), `parse_errors_are_clean`. 121 tests total green.
