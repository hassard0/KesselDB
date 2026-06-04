# SP-PG-SQL-GROUP-MULTI-COL — composite (multi-column) GROUP BY

**Date:** 2026-06-04
**Status:** SHIPPED (green on `origin/main`)

## Problem

`GROUP BY` was single-column: `Op::GroupAggregate.group_field: u16` (one field),
and the SQL / gateway recovered exactly ONE group column. Real analytics groups
by several columns:

```sql
SELECT region, category, COUNT(*), SUM(amount) FROM sales GROUP BY region, category;
```

This arc makes composite GROUP BY work end-to-end (plain single-table AND over a
binary join), composing with HAVING + ORDER BY / LIMIT / OFFSET, while keeping a
single-column GROUP BY BYTE-IDENTICAL on the wire and in the result stream.

## Design (additive + marker-guarded — preserves single-column byte-identity)

### 1. proto (`crates/kessel-proto/src/lib.rs`)

KEEP `group_field: u16` as the PRIMARY group column. ADD a marker-guarded,
additive `extra_group_fields: Vec<u16>` to `Op::GroupAggregate`,
`Op::GroupAggregateMulti`, and `JoinGroupAgg`. Empty vec ⇒ emit NO extra bytes ⇒
a single-column GROUP BY produces BYTE-IDENTICAL `Op` frames to before.

- New trailer block with marker byte `2` (distinct from HAVING's `0`/`1` and the
  group-sort marker `1`). `encode_extra_group` writes `[2][u16 count][u16 fid]*`
  only when non-empty; `decode_extra_group` peeks for marker `2` (and leaves the
  cursor untouched for HAVING/sort decode otherwise).
- For tag 22 / 47 the block leads the group trailer (`encode_group_trailer`
  writes extra → HAVING → sort). The range-preds length prefix is forced when
  `!extra_group_fields.is_empty()` so the trailer has a fixed offset.
- For `JoinGroupAgg` (inside `Op::Join`'s ga-block) the extra block sits after
  the aggregate list and before HAVING.

### 2. result stream

Per group was `[u32 keylen][primary key][16B i128 × n_aggs]`. Now each EXTRA
group column value follows the primary key as `[u32 len][value]`, BEFORE the
aggregates:

```
[u32 keylen][primary]([u32 len][extra])*[16B i128 × n_aggs]
```

With `n_extra == 0` NOTHING is appended ⇒ byte-identical to the single-column
stream. The gateway knows `n_extra` from the SQL, so it decodes unambiguously.

### 3. sm (`crates/kessel-sm/src/lib.rs`)

Group rows by the TUPLE `(primary_field, *extra_fields)`. The BTreeMap key is the
COMPOSITE blob = `primary_raw ++ extra0_raw ++ extra1_raw ++ …` (each fixed-width
raw field bytes). Fixed widths ⇒ concatenation order = tuple lexicographic order
= a deterministic total order. On emit, `split_composite_key` slices the blob
back into `(primary, extras)` and `emit_group_results_composite` writes the
stream per (2). With no extras the blob is exactly the primary bytes ⇒
byte-identical BTreeMap key + stream. Applied in all three paths:
`Op::GroupAggregate` (both apply + read-only arms), `group_aggregate_multi`, and
the join group-aggregate fold in `apply_join` (which builds the composite from
`Value`s via `raw_from_value`). All on the single deterministic apply thread.

### 4. sql (`crates/kessel-sql/src/lib.rs`)

Parse `GROUP BY <c1>, <c2>[, …]` (bare / qualified `t.c` / aliased `u.c`) →
primary `group_field` + `extra_group_fields`. The group columns must also appear
in the SELECT projection (leading non-aggregate columns); they must match the
GROUP BY columns in order (PostgreSQL requires non-aggregate SELECT columns to be
in GROUP BY). A GROUP BY column not in the catalog/combined schema is rejected
via `fid` / `resolve_combined`. The single-aggregate byte-identical path only
fires for a single group column (`extra_group.is_empty()`); a multi-column GROUP
BY routes to `Op::GroupAggregateMulti`.

### 5. gateway (`crates/kessel-pg-gateway/src/dispatch.rs`)

`PlainGroupAggProj` gained `extra_group_columns: Vec<String>`; `JoinGroupAggProj`
gained `extra_group_columns: Vec<(qualifier, column)>`. The `plain_group_aggregate`
/ `join_group_aggregate` recognizers parse N leading group columns (stopping at
the first aggregate call). `render_plain_group_aggregate` /
`render_join_group_aggregate` emit N group-key columns (each TYPED from the table
/ combined schema) followed by the aggregate columns, in SQL projection order,
decoding each extra column value from the stream per (2). NULL group values
render as NULL via the existing `value_from_raw` / `render_pg_text` path.

### Sharded engine (`crates/kesseldb-server/src/scatter_scan.rs` + `sharded_engine.rs`)

`ScatterKind::GroupAggregateMerge` / `GroupAggregateMultiMerge` gained
`n_extra: u16` (routed from `extra_group_fields.len()`). The merge treats the
COMPOSITE blob (primary keylen prefix + extra `[len][value]` blocks) as the merge
key and re-emits it verbatim, so K>=2 sharded clusters merge composite groups
correctly. `n_extra == 0` is byte-identical to the pre-multi-col merge.

## Scope (V1)

- 2+ group columns on plain GROUP BY AND over a binary join. Multi-column GROUP
  BY over a 3+ table chain is a NAMED FOLLOW-UP (the engine + SQL already reject
  GROUP BY over a chained multi-join).
- Composes with HAVING, ORDER BY (group column or aggregate), LIMIT / OFFSET.
- Single-column GROUP BY stays byte-identical and green.

## Determinism

Additive + marker-guarded: a single-column GROUP BY emits byte-identical `Op`
frames AND a byte-identical result stream. The composite key ordering
(length/width-fixed concatenation) is a deterministic total order over the tuple.
The determinism oracles (`large_seed_corpus_is_deterministic_and_converges`,
`partition_corpus_is_deterministic`, `jepsen_3replica_partition_converges_byte_identical`,
sharded `t2_determinism_oracle_k1_k4_k8_byte_equal`, read_pool
`determinism_oracle_100_random_workloads`) stay green.
