# SP-PG-SQL-GROUP-MULTI-COL — progress tracker

**Date:** 2026-06-04
**Status:** CLOSED

Composite (multi-column) `GROUP BY` end-to-end (plain + binary-join), composing
with HAVING + ORDER BY / LIMIT / OFFSET, single-column GROUP BY byte-identical.

## Task list

- [x] proto: marker-guarded additive `extra_group_fields` on `Op::GroupAggregate`,
      `Op::GroupAggregateMulti`, `JoinGroupAgg` (block marker `2`, leads the group
      trailer; range-preds length forced when non-empty). Single-column =
      byte-identical Op frame.
- [x] result stream: each extra column value as `[u32 len][value]` after the
      primary key, before the aggregates; `n_extra=0` byte-identical.
- [x] sm: composite BTreeMap key (primary ++ each extra's fixed-width bytes — a
      deterministic total order), `split_composite_key` + `emit_group_results_
      composite`, in all 3 paths (GroupAggregate apply+RO arms, group_aggregate_
      multi, join group-agg fold).
- [x] sql: parse `GROUP BY g1, g2, …` (plain + join), bare / qualified / aliased;
      validate leading projection cols match the group cols; reject unknown cols.
- [x] gateway: `extra_group_columns` on both proj structs; recognizers parse N
      leading group cols; renderers emit N typed group-key cols + decode extras.
- [x] scatter merge: `n_extra` threaded into `GroupAggregateMerge` /
      `GroupAggregateMultiMerge`; merge on the composite blob, re-emit verbatim.
- [x] docs: design, this progress, USAGE §3, STATUS, CHANGELOG.
- [x] new smoke `scripts/sppgsqlgroupmulticol-smoke.py` (port 5557).
- [x] vulcan: `cargo test --workspace --release` exit 0.
- [x] vulcan: regression smokes (plain-group-render + group-sort-limit) green.
- [x] vulcan: new multi-col smoke green.

## Deferred (named follow-up)

- Multi-column GROUP BY over a 3+ table chain (the engine + SQL already reject
  GROUP BY over a chained multi-join — INNER chains without GROUP BY keep
  working). Binary-join multi-col GROUP BY is in scope and shipped.

## Transcripts

### Workspace test (vulcan, `cargo test --workspace --release`)

```
exit 0 — all green (fresh worktree off origin/main). Determinism oracles pass:
  jepsen_3replica_partition_converges_byte_identical ... ok
  jepsen_mvcc_keyspace_3replica_byte_identical_under_partition ... ok
  large_seed_corpus_is_deterministic_and_converges ... ok
  scatter_scan merge_*/pentest_9/pentest_10 byte_identical ... ok
  sharded_engine t2_determinism_oracle_k1_k4_k8_byte_equal ... ok
  read_pool determinism_oracle_100_random_workloads ... ok
extra_group_fields is additive + marker-guarded (empty ⇒ no extra op/stream bytes), so single-column
GROUP BY frames + result streams are byte-identical and the oracles are untouched.
```

### Regression: single-column plain-group-render smoke (`scripts/sppgsqlplaingrouprender-smoke.py`)

```
# sppgsqlplaingrouprender-smoke.py --no-server (single-column GROUP BY, against the multi-col build)
  PASS  ddl / seed / single_count / multi_agg / having / order_limit
--- 6/6 stages PASS ---
# single-column plain GROUP BY render unaffected by composite-key support.
```

### Regression: single-column group-sort-limit smoke (`scripts/sppgsqlgroupsortlimit-smoke.py`)

```
# sppgsqlgroupsortlimit-smoke.py --no-server (single-column GROUP BY ORDER BY/LIMIT, against the multi-col build)
  PASS  ddl / seed / order_count_desc / order_limit2 / order_limit_offset / order_key_asc / having_order_limit
--- 7/7 stages PASS ---
# single-column GROUP BY sort/limit/offset + HAVING all unaffected.
```

### New: multi-column GROUP BY smoke (`scripts/sppgsqlgroupmulticol-smoke.py`)

```
# sppgsqlgroupmulticol-smoke.py --no-server (psycopg2, PG 127.0.0.1:5557)
STAGE composite_count: PASS GROUP BY region, category → one row per combo (5 composite groups)
STAGE composite_multi: PASS COUNT + SUM per composite (region, category) group
STAGE composite_having: PASS HAVING COUNT(*)>1 over composite groups: [('east', 'books', 3), ('east', 'gadgets', 2), ('west', 'gadgets', 4)]
STAGE composite_topn: PASS top-2 composite groups by count DESC: [('west', 'gadgets', 4), ('east', 'books', 3)]
STAGE single_col_back: PASS single-col GROUP BY region rollup unchanged: [('east', 5), ('north', 1), ('west', 5)]
--- 7/7 stages PASS ---
GROUP-MULTI-COL SMOKE COMPLETE

Every stage uses a HARD assert. `composite_count` asserts exactly one row per distinct (region,
category) pair with correct counts (5 groups). `composite_having` + `composite_topn` prove HAVING and
ORDER BY COUNT(*) DESC LIMIT compose over the COMPOSITE groups. `single_col_back` proves a plain
single-column GROUP BY still rolls up correctly (back-compat).
```
