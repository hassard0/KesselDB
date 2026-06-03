# SP-PG-SQL-MULTI-JOIN — progress tracker

**Date:** 2026-06-03
**Status:** CLOSED
**Design:** `docs/superpowers/specs/2026-06-03-kesseldb-sppgsqlmultijoin-design.md`

## Goal

Make 3+ table chained INNER equi-joins (`users JOIN posts JOIN comments`) work
end-to-end over the PG wire. `Op::Join` was binary (exactly two tables); the
planner handled exactly ONE `JOIN`.

## Task list

- [x] **proto** — `JoinStep` struct + additive, marker-guarded `extra_joins:
      Vec<JoinStep>` on `Op::Join`. Distinct `EXTRA_JOINS_MARKER = 2` (vs. ga
      marker `1`) + `Cursor::peek_u8` so a 2-table / group-aggregate frame stays
      BYTE-IDENTICAL. Round-trip + byte-identity + malformed-count tests.
- [x] **sm (engine)** — `apply_multi_join`: build the base `(a ++ b)` combined
      decoded-Value rows, fold each `JoinStep` (INNER hash equi-join on the ON
      columns), widen the combined schema each step, emit the same `KTR1` stream
      + apply combined `filter` / `order_by` / `limit_n` / `offset_n`. Empty
      `extra_joins` ⇒ the binary-join body runs unchanged (byte-identical).
      Engine 3-way + sorted-chain tests.
- [x] **sql** — parse chained `[INNER] JOIN <t> ON <a.x> = <b.y>` segments;
      resolve `WHERE` / `ORDER BY` over the full N-table combined schema
      (`combined_join_type_multi` + `compile_join_where_multi`); reject
      GROUP-BY-over-chain + LEFT-in-chain. Compile test.
- [x] **Updated EVERY `Op::Join { … }` literal construction site** across the
      workspace (proto, sm, sql, read_pool, sharded_engine,
      parallel_reads_oracle) with `extra_joins: vec![]`.
- [x] **gateway** — NO change needed: `render_join_result` + `join_projection`
      already handle 3+ tables (the combined `KTR1` schema just grows).
- [x] **Determinism oracles green** (corpus / partition / 3-replica byte-identity).
- [x] **psql 3-table smoke** `scripts/sppgsqlmultijoin-smoke.py` (real psycopg2).
- [x] **Docs** — design doc, this progress tracker, USAGE §3 JOIN ref, STATUS,
      CHANGELOG, README.

## Deferred (named follow-ups, V1 out-of-scope)

- `GROUP BY` over a chained multi-join (engine rejects `extra_joins` +
  `group_aggregate`; SQL rejects `GROUP BY` over a chain). Reason: the
  group-aggregate result encoding differs from the row stream; combining the two
  adds risk with no V1 demand.
- Mixing `LEFT`/`RIGHT`/`FULL` into a chain (SQL rejects). V1 ships the INNER
  chain solidly; outer chains are their own arc.
- Table aliases in the FROM/JOIN clause (`SP-PG-SQL-JOIN-ALIAS`) — columns must
  be qualified by the full table name, SAME as the existing binary join.

## Validation

### 1. Workspace test (vulcan, `cargo test --workspace --release`)

`cargo test --workspace --release` exited **0** (all green) at HEAD
`f75a23d` on vulcan. Multi-join unit tests (re-run, focused):

```
test tests::multi_join_round_trips ... ok          # kessel-proto wire round-trip
test result: ok. 31 passed; 0 failed; ...          # kessel-proto
test tests::mj_three_way_filtered ... ok           # kessel-sm engine 3-way + sort/limit
test tests::mj_three_way_inner_chain ... ok        # kessel-sm engine 3-way INNER
test result: ok. 191 passed; 0 failed; ...         # kessel-sm
test tests::multi_join_compiles_to_extra_joins ... ok  # kessel-sql compile
test result: ok. 139 passed; 0 failed; ...         # kessel-sql
```

The determinism oracles (`large_seed_corpus_is_deterministic_and_converges`,
`partition_corpus_is_deterministic`,
`jepsen_3replica_partition_converges_byte_identical`, 3-replica byte-identity)
are part of the workspace suite that exited 0.

### 2. psql smoke transcript (vulcan, real psycopg2 2.9.12)

Real `scripts/sppgsqlmultijoin-smoke.py` against a live `kesseldb` built
`--features pg-gateway` on vulcan (PG `127.0.0.1:5552`):

```
# psycopg2 2.9.12 (dt dec pq3 ext lo64) -> postgresql://test:admin@127.0.0.1:5552/kesseldb
STAGE ddl: PASS users, posts, comments created
STAGE seed: PASS 2 users, 3 posts, 3 comments
STAGE three_way: PASS 3-way combined rows: [('alice', 'hello', 'nice'), ('alice', 'hello', 'ok'), ('alice', 'world', 'wow')]
STAGE three_way_star: PASS SELECT * → 3 rows × 8 cols; wow row = (1, 'alice', 11, 1, 'world', 102, 11, 'wow')
STAGE three_way_where: PASS WHERE users.id=1 → [('alice', 'hello', 'nice'), ('alice', 'hello', 'ok'), ('alice', 'world', 'wow')]; WHERE users.id=2 → []

=== SP-PG-SQL-MULTI-JOIN SMOKE SUMMARY ===
  PASS  ddl
  PASS  seed
  PASS  three_way
  PASS  three_way_star
  PASS  three_way_where
--- 5/5 stages PASS ---
MULTI-JOIN SMOKE COMPLETE
```

HEADLINE proven: `SELECT users.name, posts.title, comments.body FROM users JOIN
posts ON users.id = posts.user_id JOIN comments ON posts.id = comments.post_id`
returns exactly the 3 combined rows where a user has a post AND that post has a
comment (bob/solo's post has no comment ⇒ dropped by the INNER chain).
`SELECT *` returns 8 columns (all of users + posts + comments); the filtered
3-way join works over the full combined schema.

## CLOSED
