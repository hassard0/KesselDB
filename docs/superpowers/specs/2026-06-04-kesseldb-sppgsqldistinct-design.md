# SP-PG-SQL-DISTINCT — design

**Arc:** `SELECT DISTINCT` row deduplication over the PostgreSQL wire.
**Date:** 2026-06-04
**Status:** shipped (see the companion progress doc).

## Goal

```sql
SELECT DISTINCT region FROM events;            -- the unique regions
SELECT DISTINCT region, category FROM events;  -- the unique (region,category) pairs
SELECT DISTINCT * FROM events;                  -- the unique whole rows
SELECT DISTINCT region FROM events WHERE x = 1; -- DISTINCT after a WHERE
```

`SELECT DISTINCT` (get the unique values of a column; dedup result rows) is
extremely common SQL. Before this arc the keyword was unsupported — only comment
mentions of "distinct" existed — so a DISTINCT query either errored or silently
returned ALL rows (duplicates included).

## The gap (confirmed)

`crates/kessel-sql/src/lib.rs` had NO handling of the `DISTINCT` keyword. The
projection/star recognizers (`select_columns`, `select_star_table`) and the
compiler's `SELECT` arm choked on the `DISTINCT` token right after `SELECT`, so
`SELECT DISTINCT region FROM t` did not even parse as a base-table SELECT. The
gateway `render_select_got` had no dedup step.

## Design — RENDER-LAYER dedup (zero engine/wire/determinism risk)

The engine returns ALL rows; the gateway dedups at render time. NO `Op` change,
NO wire change, NO storage change, NO determinism-oracle risk.

### kessel-sql

- **`select_is_distinct(sql) -> bool`** — a lenient, lexer-backed render-time
  signal: first token `SELECT`, second token the `DISTINCT` keyword. Returns
  `false` for the Postgres `DISTINCT ON (…)` extension (the `ON` keyword follows
  DISTINCT) so the gateway never mis-renders `DISTINCT ON` as plain DISTINCT.
- **`select_columns` / `select_star_table` / `select_projection_to_star`** now
  skip an optional `DISTINCT` immediately after `SELECT`, so
  `SELECT DISTINCT a, b FROM t` parses its projection as `[a, b]` and table as
  `t` exactly like the non-distinct form. `DISTINCT ON (…)` → `None` (follow-up).
- The compiler's `SELECT` arm consumes an optional `DISTINCT` (via the new
  `P::kw_peek_distinct`, which leaves `DISTINCT ON` for the parser to reject)
  BEFORE dispatching to `try_query_rows` / `compile_select`, so
  `SELECT DISTINCT … FROM t` compiles to the **same `Op`** as the non-distinct
  form (the engine returns every row). Proven by
  `select_distinct_compiles_identically_to_nondistinct`.

### kessel-pg-gateway

- **`dedup_data_rows(rows_buf) -> Option<(Vec<u8>, u64)>`** — dedups a run of
  already-encoded `DataRow` ('D') messages by the FULL message body (the encoded
  projected cells). Two rows are equal iff their projected cell tuple is
  byte-identical; a NULL cell is the same `-1` length sentinel for every NULL, so
  this gives exact SQL DISTINCT semantics including **NULL is not distinct from
  NULL**. Keeps the FIRST occurrence in scan order (deterministic).
- The **projection-list** path and the **`SELECT *`** path in `render_select_got`
  call `dedup_data_rows` on the just-emitted DataRows when `select_is_distinct`
  is true, then report the **deduped** count in the `SELECT N` CommandComplete
  tag. RowDescription order is unchanged (dedup operates on the row stream only).
  The dedup key is the PROJECTED columns (what's in the SELECT list), since the
  emit step already projected: `SELECT DISTINCT region` dedups by region only.

## DISTINCT … ORDER BY

A projection-list `SELECT DISTINCT col FROM t ORDER BY col` lowers to
`Op::SelectSorted` (the sorted-projection render branch), which emits the FULL
record stream already in sorted scan order. `dedup_data_rows` keeps the first
occurrence in scan order, so the post-dedup output is still correctly ordered.
DISTINCT + ORDER BY on a base-table projection therefore works (the dedup runs
on the sorted DataRows). No special-casing required.

## Named follow-ups (cleanly scoped out, NOT silently accepted)

- **`DISTINCT ON (…)`** (Postgres extension) — `select_is_distinct` returns
  false and the recognizers/compiler reject it, so it errors cleanly rather than
  rendering as plain DISTINCT.
- **DISTINCT over JOIN** — a `SELECT DISTINCT … JOIN` parses to neither
  `select_columns` nor `join_projection` (the latter is left DISTINCT-unaware on
  purpose), so it falls through to the existing clean `0A000` "unsupported"
  error instead of returning duplicates.
- **DISTINCT over aggregate / GROUP BY** (`SELECT DISTINCT category, COUNT(*) …`)
  — the aggregate recognizers reject the leading DISTINCT token, so these shapes
  are not mis-rendered.

These are NAMED here rather than silently accept-and-ignored: no shape returns
duplicates and looks broken — it is either deduped or cleanly errored.

## Determinism

RENDER-ONLY arc. No `Op` / wire / storage change: `SELECT DISTINCT …` compiles
to the identical `Op` as the non-distinct form (asserted by a compile-equivalence
test). The corpus / partition / 3-replica byte-identity oracles are unaffected
(no new write path, no oracle literal construction sites changed).

## Tests

- `kessel-sql`: `select_distinct_recognizers` (the signal + DISTINCT-aware
  recognizers incl. `DISTINCT ON` rejection) and
  `select_distinct_compiles_identically_to_nondistinct` (DISTINCT ≡ non-distinct
  Op for `*` and projection lists; `DISTINCT ON` errors).
- `kessel-pg-gateway`: `dedup_data_rows_keeps_first_and_dedups_nulls`,
  `dedup_data_rows_multi_cell_tuples`, `dedup_data_rows_rejects_non_datarow`.
- psql smoke `scripts/sppgsqldistinct-smoke.py` — DDL + seed (dups + NULLs) +
  distinct_region (HEADLINE: count < total) + nondistinct back-compat (all rows)
  + distinct_pair + distinct_null + distinct_star + a real whole-row-dup collapse
  under `DISTINCT *`, over psycopg2.
