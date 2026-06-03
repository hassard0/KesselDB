# SP-PG-SQL-MULTI-JOIN — chained N-way (3+ table) INNER equi-joins

**Date:** 2026-06-03
**Status:** SHIPPED
**Arc:** SP-PG-SQL-MULTI-JOIN

## The gap

`Op::Join` was BINARY — exactly two tables. Real apps and analytics constantly
chain joins:

```sql
SELECT u.name, p.title, c.body
  FROM users u JOIN posts p ON u.id = p.user_id
               JOIN comments c ON p.id = c.post_id
  WHERE u.id = 1;
```

The planner handled exactly ONE `JOIN`; a second `JOIN` segment failed to
compile. This arc makes 3+ table chained INNER equi-joins work end-to-end over
the PG wire.

## Design

### Wire (kessel-proto)

`Op::Join` gains an ADDITIVE, marker-guarded field:

```rust
pub struct JoinStep {
    pub right_type: TypeId,            // the next table
    pub left_combined_field: u16,      // ON-left: a field id in the RUNNING
                                       //   combined schema (any joined table)
    pub right_field: u16,              // ON-right: the next table's field id
}
// Op::Join { …, extra_joins: Vec<JoinStep> }
```

**Byte-identity (non-negotiable).** Empty `extra_joins` ⇒ a normal binary join
⇒ BYTE-IDENTICAL `Op` frame to before this arc. The extra-joins block is
positioned AFTER the page block, sharing the post-page position with the
group-aggregate (ga) block. To keep a ga-only frame byte-identical we give the
extra-joins block a DISTINCT marker byte (`EXTRA_JOINS_MARKER = 2`) vs. the ga
marker (`1`); the decoder PEEKS the next byte (`Cursor::peek_u8`) to choose the
block WITHOUT any presence anchor. So:

| frame                       | post-page bytes |
|-----------------------------|-----------------|
| 2-table, no page/ga/mj      | (nothing)       |
| 2-table + ga                | `[1]…` (unchanged) |
| N-table chain (mj)          | `[2][count][steps…]` |

A non-`0/1/2` marker, or `count == 0`, is a forward-incompatible / malformed op
⇒ decode failure (surfaced, never silently mis-applied). V1 never emits both the
ga and mj blocks together (multi-join + GROUP BY is a named follow-up).

Every `Op::Join { … }` literal construction site across the workspace (proto,
sm, sql, read_pool, sharded_engine, parallel_reads_oracle, oracle tests) was
updated with `extra_joins: vec![]`.

### Engine (kessel-sm `apply_multi_join`)

When `extra_joins` is non-empty, `apply_join` delegates to `apply_multi_join`:

1. Build the base `(a ++ b)` combined **decoded-Value** row set by INNER
   equi-joining `left_type` × `right_type` on the base ON columns (right table
   hashed by join-key bytes; left scanned in object-id order, each matched
   against its bucket in object-id order).
2. Fold each `JoinStep`: hash the step table by its `right_field` bytes, then
   probe each running combined row's `left_combined_field` Value (as raw
   fixed-width bytes) against it, extending each matched row with the step
   table's Values. Widen the combined schema by the step table's fields
   (renamed `<table>.<col>`, fresh sequential combined field ids).
3. Encode the final combined rows against the widened combined `ObjectType`,
   emit the SAME self-describing `KTR1` stream the binary join emits (just
   wider), then apply the optional combined-schema `filter`, then either stream
   in deterministic scan order (capped by the pre-sort `limit`) or stable-sort
   by the combined `order_by` field + paginate (`offset_n`/`limit_n`).

**Determinism.** The result is a PURE deterministic function of the input
tables: left-key / right-scan (object-id) order is preserved at every step, the
hash maps only bucket rows that were already inserted in scan order, and the
sort (when present) is a STABLE sort over the deterministic scan order. Empty
`extra_joins` runs the unchanged binary-join body, so every determinism oracle
stays byte-identical.

### SQL (kessel-sql)

- The join compile now consumes additional `[INNER] JOIN <table> ON <a.x> =
  <b.y>` segments after the base join. Each segment's ON must reference the NEW
  table on one side and an already-joined table on the other; the already-joined
  side resolves to a combined field id over the running schema, the new side to
  the new table's own field id ⇒ one `JoinStep`.
- `WHERE` / `ORDER BY` resolve over the FULL N-table combined schema
  (`combined_join_type_multi`); `compile_join_where_multi` generalizes the
  2-table join-WHERE rewriter to N tables (the binary path is the 2-table case,
  unchanged).
- `join_projection` already returns after the first `JOIN` keyword, so the
  gateway recovers `SELECT u.name, p.title, c.body` / `SELECT *` projections for
  3+ tables with no change; `render_join_result` maps them onto the wider
  combined `KTR1` schema by `<table>.<col>` name — the data path needed NO
  change.

## V1 scope + named follow-ups

- **In:** INNER equi-join chains (`JOIN` / `INNER JOIN`), 3+ tables; `SELECT
  a.c, b.c, c.c` and `SELECT *`; `WHERE` over any joined table's columns;
  `ORDER BY` / `LIMIT` / `OFFSET` over the combined schema.
- **Deferred (explicit errors):**
  - multi-join + `GROUP BY` (engine rejects `extra_joins` + `group_aggregate`;
    SQL rejects `GROUP BY` over a chain).
  - mixing `LEFT`/`RIGHT`/`FULL` into a chain (SQL rejects).
  - table aliases beyond what the base binary join already handles, and
    self-joins in a chain (rejected to avoid same-name ambiguity).
