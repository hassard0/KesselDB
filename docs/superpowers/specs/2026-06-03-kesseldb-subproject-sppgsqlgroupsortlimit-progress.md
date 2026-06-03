# SP-PG-SQL-GROUP-SORT-LIMIT — progress tracker

**Arc:** make `ORDER BY / LIMIT / OFFSET` on a PLAIN (non-JOIN) `GROUP BY`
actually take effect in the engine.
**Date:** 2026-06-03
**Design:** `docs/superpowers/specs/2026-06-03-kesseldb-sppgsqlgroupsortlimit-design.md`
**Status:** CLOSED.

## Task list

- [x] proto: `GroupSortTarget { Key, Agg(u16) }` + `GroupSort { target, desc,
      limit: Option<u64>, offset: Option<u64> }`; additive
      `sort: Option<GroupSort>` field on `Op::GroupAggregate` and
      `Op::GroupAggregateMulti`.
- [x] proto: `encode_group_sort` / `decode_group_sort` marker-guarded block
      (marker 1; target tag 0=Key / 1=Agg; `[u8 has_limit][?u64]` +
      `[u8 has_offset][?u64]`); `encode_group_trailer(having, sort)` composes the
      HAVING block + sort block with a no-HAVING anchor byte; `decode_having`
      extended to consume the `0` anchor. Non-1 marker / bad target tag rejected.
- [x] proto: tag-22 forces the range-preds length prefix when HAVING **or** sort
      is present; tag-47 already wrote it. No-HAVING/no-sort frames stay
      BYTE-IDENTICAL to pre-arc.
- [x] sm: shared `emit_group_results(kept, sort)` — sort by agg value or raw key
      bytes, `DESC` reverse with ascending-key tie-break, `OFFSET` then `LIMIT`,
      then encode. Wired into both `Op::GroupAggregate` arms (apply +
      read_only_op) and the `group_aggregate_multi` helper (which gained a `sort`
      param). Single-aggregate result wrapped in a 1-elem `Vec` for uniformity.
- [x] sql: ORDER BY captured as `RawOrderTarget::{Ident, Position, Agg}`;
      `resolve_group_sort` maps it to `GroupSortTarget` (position 1 = key, 2.. =
      agg slot; agg expr matched by (kind, arg field); ident = group col ⇒ key or
      projected alias ⇒ agg). Threaded into both Op constructions. Rejects
      out-of-range position + non-projected aggregate.
- [x] determinism: every `Op::GroupAggregate{,Multi}` construction site
      (proto/sm/sql/read_pool/sharded_engine/parallel_reads_oracle/bench) updated
      with `sort: None`. No-sort frames byte-identical ⇒ oracles untouched.
- [x] KATs: proto wire round-trip + byte-identity + marker rejection
      (`sp_pg_sql_group_sort_limit_wire_round_trip_and_byte_identity`); sm
      semantics (`sp_pg_sql_group_sort_limit_reorders_and_truncates`); sql planner
      (`sp_pg_sql_group_sort_limit_planner_attaches_sort`).
- [x] gateway: no change required — `render_plain_group_aggregate` emits DataRows
      in the engine's stream order.
- [x] docs: design, this progress tracker, USAGE §3 (caveat → works), STATUS,
      CHANGELOG.
- [x] vulcan: `cargo test --workspace --release` green.
- [x] vulcan: psql smoke (`scripts/sppgsqlgroupsortlimit-smoke.py`).
- [x] commit + push to origin/main; cleanup vulcan worktree + target dir.

## Determinism

The `sort` field is additive + marker-guarded: a query with no `ORDER BY` /
`LIMIT` / `OFFSET` emits BYTE-IDENTICAL `Op` frames to before this arc (the sort
block is written ONLY when `Some`; the HAVING anchor is written ONLY when a sort
block follows a no-HAVING op). The sort/offset/limit run on the single
deterministic apply thread over the already-deterministic per-group result, and
the sort is TOTAL (aggregate value, then ascending group key). The corpus /
partition / 3-replica byte-identity oracles stay green.

## vulcan smoke transcript

`cargo test --workspace --release` on vulcan: **exit 0 (all green)** — see the
workspace-test result below; this includes the corpus / partition / 3-replica
byte-identity determinism oracles.

**Before-state** (pre-fix, established by SP-PG-SQL-PLAIN-GROUP-RENDER's own
caveat + code inspection): the SQL layer parsed `ORDER BY`/`LIMIT`/`OFFSET` on a
plain GROUP BY but `Op::GroupAggregate` / `Op::GroupAggregateMulti` carried no
sort/page fields, so the engine returned ALL groups in ascending group-key
order regardless — `ORDER BY COUNT(*) DESC` came back in key order and `LIMIT`
truncated nothing.

**After-state** (live psql/psycopg2 smoke against the gateway-built server,
`KESSELDB_PG_ADDR=127.0.0.1:5551`):

```
# psycopg2 2.9.12 -> postgresql://test:admin@127.0.0.1:5551/kesseldb
STAGE ddl: PASS products created
STAGE seed: PASS 10 products in 4 categories (counts 4/3/2/1)
STAGE order_count_desc: PASS DESC count order (not key order): [('books', 4), ('gadgets', 3), ('toys', 2), ('misc', 1)]
STAGE order_limit2: PASS top 2 of 4 only: [('books', 4), ('gadgets', 3)]
STAGE order_limit_offset: PASS window LIMIT 2 OFFSET 1: [('gadgets', 3), ('toys', 2)]
STAGE order_key_asc: PASS ORDER BY key ASC: [('books', 4), ('gadgets', 3), ('misc', 1), ('toys', 2)]
STAGE having_order_limit: PASS HAVING→sort(SUM desc)→LIMIT 2: [('gadgets', 3, 180), ('books', 4, 100)]

=== SP-PG-SQL-GROUP-SORT-LIMIT SMOKE SUMMARY ===
  PASS  ddl / seed / order_count_desc / order_limit2 / order_limit_offset / order_key_asc / having_order_limit
--- 7/7 stages PASS ---
```

Every stage uses a HARD `assert` on the exact expected rows, so 7/7 is a verified
result (not just a non-crash). `order_count_desc` additionally asserts the result
is NOT the pre-fix ascending-key order.

HEADLINE `... GROUP BY category ORDER BY COUNT(*) DESC` →
**[books(4), gadgets(3), toys(2), misc(1)]** — descending-count order, NOT the
pre-fix ascending-key order `[books, gadgets, misc, toys]`. `LIMIT 2` returns
ONLY the top 2; `LIMIT 2 OFFSET 1` returns the right window; `ORDER BY category
ASC` sorts by key; `HAVING COUNT(*) > 1 ORDER BY SUM(price) DESC LIMIT 2`
composes correctly.
