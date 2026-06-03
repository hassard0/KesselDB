# SP-PG-SQL-PLAIN-GROUP-RENDER — design

**Arc:** render a PLAIN (non-JOIN) `GROUP BY` group-aggregate SELECT over the
PostgreSQL wire.
**Date:** 2026-06-03
**Status:** shipped (see the companion progress doc).

## Goal

```sql
SELECT category, COUNT(*) FROM products GROUP BY category;
SELECT category, COUNT(*) AS n, SUM(price), AVG(price), MIN(price), MAX(price)
  FROM products GROUP BY category HAVING COUNT(*) > 1 ORDER BY n DESC LIMIT 5;
```

Plain `GROUP BY` is everyday analytics / ORM SQL. The SQL planner + state
machine already **compile and execute** it (`Op::GroupAggregate` for a single
aggregate, `Op::GroupAggregateMulti` for ≥2 aggregates or a leading group
column), and the just-shipped SP-PG-SQL-HAVING already filters groups at the SM
layer. The missing piece was the **gateway RENDER path**: `render_select_got`
routed to `render_join_group_aggregate` only when `kessel_sql::join_group_aggregate`
matched (which REQUIRES a JOIN), so a plain group-aggregate fell through to the
bottom render error.

## The gap (confirmed)

`crates/kessel-pg-gateway/src/dispatch.rs::render_select_got` had branches for:

1. JOIN group-aggregate (`join_group_aggregate` → `render_join_group_aggregate`)
2. single scalar aggregate, no GROUP BY (`select_aggregate`)
3. `KTR1` JOIN row stream
4. explicit projection list
5. whole-row `SELECT *`

A plain `SELECT g, COUNT(*) … GROUP BY g` matched NONE of these — the engine
returned the value-only group stream `[u32 ngroups]([u32 keylen][key][16B i128
× n_aggs])*`, which no branch decoded, so the client hit the bottom
`0A000 … only renders SELECT *` error (or a width mismatch). Confirmed by a psql
smoke against pre-fix origin/main (see progress doc, `single_count` before/after).

## V1 scope

- **Recognizer** `kessel_sql::plain_group_aggregate(sql) -> Option<PlainGroupAggProj>`
  (mirrors `join_group_aggregate`): accepts
  `SELECT <group col>, <AGG>(<*|[t.]col>) [AS a] [, …]* FROM <table> [WHERE …]
  GROUP BY <group col> [HAVING …] [ORDER BY …] [LIMIT n] [OFFSET n]`.
  - Group column may be bare (`category`) or qualified (`products.category`) —
    qualifier stripped.
  - 1+ aggregates; COUNT / SUM / MIN / MAX / AVG; optional `AS` aliases (else the
    PG default name `count`/`sum`/`avg`/`min`/`max`).
  - Returns `None` for a JOIN group-aggregate (a `JOIN` keyword anywhere ⇒ the
    `join_group_aggregate` path owns it), for a single scalar aggregate with no
    GROUP BY (`select_aggregate` owns it), for a non-aggregate projection, and
    for no-GROUP-BY queries. Every existing render path stays byte-untouched.
- **Render** `render_plain_group_aggregate` (mirrors `render_join_group_aggregate`):
  decodes the SAME value-only group stream, but
  - types + names the group key column from the **FROM table** schema
    (`engine.describe_table`), not a join qualifier;
  - types the aggregate OIDs per aggregate: COUNT / SUM → `int8`, AVG →
    `numeric`, MIN / MAX → the **source column's OID** (an `int4` price column's
    MIN renders as `int4`). COUNT(\*) and unresolved sources fall back to `int8`.

### Ordering inside `render_select_got`

`join_group_aggregate` (JOIN) → **`plain_group_aggregate` (plain)** →
`select_aggregate` (single scalar) → `KTR1` JOIN stream → projection list →
whole-row `*`. The plain branch sits AFTER the JOIN branch (a JOIN must never
reach it — and the recognizer also rejects a `JOIN` keyword) and BEFORE the
single-scalar branch (`select_aggregate` returns `None` for a leading group
column, so they cannot shadow each other).

## Determinism

RENDER-ONLY arc. No `Op` / wire-format changes: `Op::GroupAggregate` and
`Op::GroupAggregateMulti` (and their result stream) are untouched. The engine's
group stream is already deterministic (BTreeMap ascending raw-key order). The
corpus / partition / 3-replica byte-identity invariants are unaffected (no new
write path, no oracle literal construction sites changed).

## Honest caveat — ORDER BY / LIMIT / OFFSET

The V1 planner parses `ORDER BY` / `LIMIT` / `OFFSET` on a plain group-aggregate
but does **not** thread them into `Op::GroupAggregate` / `Op::GroupAggregateMulti`
(those ops carry no sort/limit fields). So the engine returns ALL groups in
ascending group-key order regardless of a trailing `ORDER BY … LIMIT`. The
render faithfully emits whatever group set the engine returns, in key order — it
does NOT silently drop or reorder rows. Sort/limit pushdown for plain group-agg
is a follow-on arc (SP-PG-SQL-GROUP-SORT-LIMIT). The smoke's `order_limit` stage
documents this current behaviour explicitly (asserts the rendered group SET,
not a truncated/ordered list).

## Tests

- `kessel-sql`: `plain_group_aggregate_recognizer` — single COUNT(\*), multi-agg
  with aliases + HAVING + ORDER BY + LIMIT + OFFSET, qualified columns, and all
  the `None` shapes (JOIN agg, scalar agg, plain projection, no GROUP BY, `*`).
- `kessel-pg-gateway`: `plain_group_agg_render_single_count` (CHAR group key,
  default `count` name) + `plain_group_agg_render_multi_int_key` (INT group key,
  5 aggregates with an alias).
- psql smoke `scripts/sppgsqlplaingrouprender-smoke.py` — DDL + seed + single
  COUNT (HEADLINE) + multi-agg + HAVING + order_limit over psycopg2.
