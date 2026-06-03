# SP-PG-SQL-HAVING — design

**Arc:** support a `HAVING` clause that filters aggregate groups after grouping.
**Date:** 2026-06-03
**Status:** shipped (see the companion progress doc).

## Goal

```sql
SELECT a.name, COUNT(b.id) AS c
FROM a JOIN b ON a.id = b.aid
GROUP BY a.name
HAVING COUNT(b.id) > 2 [ORDER BY …] [LIMIT …]
```

and the plain (non-join) form:

```sql
SELECT col, COUNT(*) FROM t GROUP BY col HAVING COUNT(*) >= 3
```

The `HAVING` predicate filters GROUPS *after* aggregation, comparing an
aggregate (already in the SELECT projection) against a constant.

## V1 scope

- `HAVING <agg-expr> <cmp-op> <int-or-numeric-literal>`.
- `<agg-expr>` is one of the aggregates already in the SELECT projection,
  matched by the same `(agg-function, arg-column)`.
- `<cmp-op> ∈ { >, >=, <, <=, =, <>, != }`.
- Covers the join-group-aggregate (`Op::Join { group_aggregate }`), the
  single-aggregate `GROUP BY` (`Op::GroupAggregate`), and the multi-aggregate
  `GROUP BY` (`Op::GroupAggregateMulti`).

### Deferred / rejected (with reason)

- **HAVING aggregate NOT in the projection** (e.g.
  `SELECT g, COUNT(*) … HAVING SUM(x) > 1`): chose option **(b) clean reject**
  over (a) compute-extra. Computing an extra aggregate would mean a second
  result column the gateway's RowDescription does not describe (it derives the
  column shape from the SELECT text), and threading a "hidden aggregate" through
  the result encoding is exactly the kind of low-reward wire churn that risks
  the determinism oracles. The HAVING aggregate must match one of the projected
  aggregates; otherwise a clear error is returned. This is what
  SQLAlchemy/Django/analytics emit in practice (they put the HAVING aggregate in
  the SELECT list).
- **HAVING with a non-aggregate projection** (`SELECT g … HAVING …`): rejected;
  HAVING requires an aggregate projection in V1.
- **HAVING on a scalar aggregate (no GROUP BY)**: rejected; there are no groups
  to filter.
- **HAVING over the group KEY** (e.g. `HAVING g > 5`): out of V1 scope — that is
  a WHERE-on-the-key rewrite and is a named follow-up. V1 compares an aggregate.

## Wire model (additive + marker-guarded — determinism-critical)

A new `HavingPred` carries the predicate as a pure function of the per-group
aggregate output:

```rust
pub struct HavingPred {
    pub agg_index: u16, // which aggregate in the op's output sequence
    pub op: u8,         // 0 > / 1 >= / 2 < / 3 <= / 4 = / 5 <>
    pub value: i128,    // RHS literal, same i128 the aggregate is computed as
}
// keep(results) == results[agg_index] <op> value
```

`agg_index` indexes the op's aggregate output sequence (always `0` for the
single-aggregate `Op::GroupAggregate`; into `aggregates` for the multi / join
shapes). `op_code("<cmp>")` maps the SQL operator string to the wire code; `<>`
and `!=` both map to `5`.

`having: Option<HavingPred>` is added to `Op::GroupAggregate`,
`Op::GroupAggregateMulti`, and the `JoinGroupAgg` struct inside `Op::Join`.

**Byte-identity guarantee.** The HAVING block is emitted *only* when
`Some`, via a `[u8 1][u16 agg_index][u8 op][16B i128 LE]` marker block:

- `Op::GroupAggregate` (tag 22): the range-preds length prefix was previously
  omitted when empty. With HAVING present we force it to be written (a `0u32`)
  so the trailing HAVING block has a fixed offset; a query with **no** range
  hints **and no** HAVING still omits both → byte-identical to the pre-arc frame.
- `Op::GroupAggregateMulti` (tag 47): already wrote an explicit range-preds
  length, so the HAVING block simply follows; absent when `None`.
- `Op::Join`'s `JoinGroupAgg`: the HAVING block lives *inside* the existing
  ga-block (only reachable when `group_aggregate` is `Some`); absent when `None`.

A non-`1` HAVING marker byte at decode is a forward-incompatible op and is
rejected (`Op::decode` → `None`), mirroring the other marker-guarded blocks.

This means: **a query with NO HAVING produces byte-identical `Op` frames to
before this arc**, which is what the determinism oracles require.

## Compile (kessel-sql)

`HAVING` is parsed after `GROUP BY` and before `ORDER BY` in both the
single-table aggregate path and the JOIN path. The lexer gains the SQL-standard
`<>` inequality token (additive — `<>` previously failed to lex; both `<>` and
`!=` map to the same inequality everywhere). The parser:

1. parses `<AGG>(arg) <cmp> <int-literal>` (negative RHS via the `Minus` token),
2. resolves the HAVING aggregate's arg to a field id the **same way** the
   projection aggregates are resolved (single-table field id, or combined
   `(a ++ b)` field id for the join path; `COUNT(*)` → field id `0` /
   `COUNT_STAR_FIELD`),
3. finds the matching aggregate index in the resolved aggregate list (by
   `(kind, field_id)`),
4. emits `HavingPred { agg_index, op, value }`; if no projected aggregate
   matches, returns a clear error.

## Engine (kessel-sm)

HAVING is applied on the **single deterministic apply thread**, on the
already-deterministic per-group aggregate output, *before* any
order-by/limit/offset paging:

- `Op::GroupAggregate`: after computing each group's scalar `res`, keep the
  group iff `having.keep(&[res])`.
- `Op::GroupAggregateMulti`: after computing each group's `Vec<i128>` result
  sequence, keep iff `having.keep(&results)`.
- `Op::Join` group-aggregate: same, over the combined-row group-aggregate
  results.

In all three the result encoding is identical to before but with the failing
groups dropped (and `ngroups` decremented), so it stays a pure function of the
input rows.

## Gateway (kessel-pg-gateway)

**No change needed** (verified). `render_join_group_aggregate` decodes
`[u32 ngroups] …` from the engine output and renders one DataRow per group —
fewer surviving groups render fewer rows automatically. The shape-recovery
helper `kessel_sql::join_group_aggregate(sql)` stops scanning at `GROUP BY`, so
a trailing `HAVING` does not perturb the recovered column shape.

The plain (non-join) `SELECT col, COUNT(*) … GROUP BY col` shape has no
dedicated gateway render path today (the gateway renders the JOIN group-
aggregate shape); the plain-path HAVING is validated at the SQL + SM layers by
unit tests. The PG-surface smoke exercises HAVING over the JOIN group-aggregate
shape end to end.
