# SP-PG-SQL-GROUP-SORT-LIMIT — design

**Arc:** make `ORDER BY / LIMIT / OFFSET` on a PLAIN (non-JOIN) `GROUP BY`
actually take effect in the engine.
**Date:** 2026-06-03
**Follows:** SP-PG-SQL-PLAIN-GROUP-RENDER (which surfaced the gap), SP-PG-SQL-HAVING
(the marker-guard template), SP-PG-SQL-JOIN-QUERY / -JOIN-AGG (the join sort
template).

## The gap

A plain `SELECT g, COUNT(*) AS n FROM t GROUP BY g ORDER BY n DESC LIMIT 5
OFFSET 1` PARSED the `ORDER BY` / `LIMIT` / `OFFSET` but the SQL layer DROPPED
them: `Op::GroupAggregate` / `Op::GroupAggregateMulti` carried no sort/page
fields, so the engine always returned ALL groups in ascending group-key order.
Top-N-per-group analytics ("the K categories with the most rows", "top sellers
by revenue") returned everything, in the wrong order.

The JOIN group-aggregate path already had the pieces (`Op::Join` carries
`order_by` / `limit_n` / `offset_n`, applied to join results), and the HAVING
arc established the additive, marker-guarded wire discipline. This arc mirrors
both for the plain group ops.

## Scope (V1)

`ORDER BY <target> [ASC|DESC] [LIMIT n] [OFFSET m]` where `<target>` is:

- an aggregate in the projection — by alias (`ORDER BY n`), by 1-based position
  (`ORDER BY 2`), or by the aggregate expression (`ORDER BY COUNT(*)`); OR
- the group key column (`ORDER BY g` / `ORDER BY 1`).

Sort is by the i128 aggregate value (or the raw group-key bytes for the key);
`DESC` reverses. Ties are broken deterministically by ascending group key
(stable). `LIMIT` / `OFFSET` apply AFTER the sort. `HAVING` (already shipped)
filters BEFORE the sort/limit, so the full pipeline is **filter → sort → offset
→ limit**, all on the single deterministic apply thread over the already-
deterministic per-group result.

## Decisions

### D1 — `GroupSort` carrier on the ops (additive, marker-guarded)

`kessel-proto` gains:

```rust
pub enum GroupSortTarget { Key, Agg(u16) }
pub struct GroupSort { target: GroupSortTarget, desc: bool,
                       limit: Option<u64>, offset: Option<u64> }
```

`Op::GroupAggregate` and `Op::GroupAggregateMulti` each gain
`sort: Option<GroupSort>`. `None` ⇒ pre-arc behaviour (ascending key order,
unbounded). The wire encode emits the block ONLY when `Some`, so a no-sort
query is BYTE-IDENTICAL to a pre-arc frame.

### D2 — composing HAVING + sort without aliasing

Both HAVING and the sort block lead with a marker byte `1`. To let the two
coexist after the range-preds region, `encode_group_trailer(having, sort)`:

- `(None, None)` ⇒ writes nothing (pre-arc identical).
- `(Some, None)` ⇒ the existing `encode_having` block only (a pre-sort
  HAVING-only frame is byte-identical).
- `sort.is_some()` ⇒ a HAVING presence region (`[1][..]` when `Some`, else a
  single `0` "no-HAVING anchor") FOLLOWED by the group-sort block.

`decode_having` is extended to treat a leading `0` as the consumed no-HAVING
anchor (returns `None`); `decode_group_sort` then reads its own marker. A non-1
sort marker, or a target tag other than `0`/`1`, is a forward-incompatible op
⇒ decode `Err` (never silently mis-applied) — mirroring the HAVING marker
rejection. For `Op::GroupAggregate` the range-preds length prefix is force-
written (possibly `0u32`) whenever HAVING **or** sort is present, giving the
trailer a fixed offset.

### D3 — shared engine emit

A single `emit_group_results(kept: Vec<(key, Vec<i128>)>, sort)` helper does
the sort + page + encode. `kept` arrives in ascending group-key order (the
deterministic pre-arc order); the sort uses an explicit ascending-key
tie-break so determinism does not depend on the caller's order. `Key` sorts by
raw key bytes; `Agg(i)` sorts by the i128 at slot `i` (defensive `0` if out of
range). `DESC` reverses; then `OFFSET` (drain) then `LIMIT` (truncate). Both
`Op::GroupAggregate` apply arms (apply + read_only_op) and the
`group_aggregate_multi` helper route through it. The single-aggregate path
wraps its one result in a 1-element `Vec` so the helper is uniform.

### D4 — SQL ORDER BY resolution

The ORDER BY target is captured richly enough to support all four forms:
`RawOrderTarget::{Ident, Position, Agg}`. In the aggregate projection branch
`resolve_group_sort(group_name, resolved_aggs, agg_aliases)` maps it to a
`GroupSortTarget`: position 1 = key, 2.. = aggregate slot; an aggregate
expression matches `(kind, arg field)` against the projected aggregates; an
ident matches the group-key column (⇒ key) or a projected aggregate alias.
Out-of-range positions and non-projected aggregates are cleanly rejected (V1
does not silently compute an extra aggregate).

### D5 — render is untouched

The gateway's `render_plain_group_aggregate` already emits DataRows in the exact
order of the engine's `[u32 ngroups]…` stream. Because the engine now applies
the sort/limit, the rendered order/window are correct automatically — no
gateway change.

## Determinism

The new fields are additive + marker-guarded; a no-ORDER-BY/no-LIMIT/no-OFFSET
query produces byte-identical `Op` frames to before. The sort/offset/limit run
on the single deterministic apply thread over the already-deterministic
per-group result, and the sort is total (value, then ascending key). The
determinism oracles (`large_seed_corpus_is_deterministic_and_converges`,
`partition_corpus_is_deterministic`,
`jepsen_3replica_partition_converges_byte_identical`, 3-replica byte-identity)
stay green; every `Op::GroupAggregate{,Multi}` construction site across
proto / sm / sql / read_pool / sharded_engine / parallel_reads_oracle / bench
was updated with `sort: None`.

## Tests

- proto: `sp_pg_sql_group_sort_limit_wire_round_trip_and_byte_identity` — wire
  layout KAT, HAVING+sort composition, sort-only anchor path, byte-identity
  lock, non-1 marker + bad-target-tag rejection.
- sm: `sp_pg_sql_group_sort_limit_reorders_and_truncates` — DESC count order,
  LIMIT, LIMIT+OFFSET, key ASC/DESC, HAVING→sort→LIMIT composition, multi-agg
  sort, apply≡read_only_op, no-sort = ascending key.
- sql: `sp_pg_sql_group_sort_limit_planner_attaches_sort` — ORDER BY by alias /
  position / aggregate expr / group key / position-1-key, no-ORDER-BY ⇒ None,
  and the two rejection cases.
- vulcan psql smoke: `scripts/sppgsqlgroupsortlimit-smoke.py`.

## Deferred

- ORDER BY over a JOIN group-aggregate is still the separately-named follow-up
  `SP-PG-SQL-JOIN-AGG-ORDERBY-AGG` (this arc is the PLAIN path only).
- Multi-column GROUP BY and multi-key ORDER BY remain V1-out-of-scope (single
  group column, single ORDER BY target), consistent with the existing plain
  group-by constraints.
