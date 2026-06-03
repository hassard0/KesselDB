# SP-PG-SQL-HAVING — progress tracker

**Arc:** `HAVING` clause filtering aggregate groups after grouping.
**Date:** 2026-06-03
**Design:** `docs/superpowers/specs/2026-06-03-kesseldb-sppgsqlhaving-design.md`

## Task list

- [x] proto: `HavingPred { agg_index, op, value: i128 }` + `keep()` + `op_code()`.
- [x] proto: additive `Option<HavingPred>` on `Op::GroupAggregate`,
      `Op::GroupAggregateMulti`, and `JoinGroupAgg`; marker-guarded wire
      encode/decode (`encode_having` / `decode_having`).
- [x] proto: byte-identity preserved (tag-22 forces rp-len prefix only when
      HAVING present; no-HAVING frames byte-identical); non-1 marker rejected.
- [x] proto: wire round-trip + byte-identity KATs for all three shapes.
- [x] sql: lexer recognizes SQL-standard `<>` (both `<>` / `!=` → one opcode).
- [x] sql: parse `HAVING <AGG>(arg) <cmp> <int>` after GROUP BY (plain + JOIN
      paths); resolve + match the aggregate to a projected aggregate → agg_index;
      reject a HAVING aggregate not in the projection / non-aggregate projection /
      scalar (no GROUP BY).
- [x] sql: planner KATs (single / multi / all cmp ops / negative RHS / rejects).
- [x] sm: apply HAVING on the deterministic apply thread over the per-group
      result BEFORE order/limit paging, for `Op::GroupAggregate`,
      `Op::GroupAggregateMulti`, and the `Op::Join` group-aggregate.
- [x] sm: functional KAT — HAVING drops the right groups (incl. empty result).
- [x] gateway: verified NO change needed (fewer groups → fewer rendered rows).
- [x] fix every `Op` / `JoinGroupAgg` literal construction across the workspace
      (proto, sm, sql, read_pool, sharded_engine, parallel_reads_oracle, bench).
- [x] vulcan: `cargo test --workspace --release` green.
- [x] vulcan: psql smoke (`scripts/sppgsqlhaving-smoke.py`) — HAVING over JOIN
      filters groups correctly (before/after group counts).
- [x] docs: design, this progress tracker, USAGE §3, STATUS, CHANGELOG, README.
- [x] commit + push to origin/main; cleanup vulcan worktree + target dir.

## Determinism

Wire changes are additive + marker-guarded: a query with NO HAVING produces
byte-identical `Op` frames to before. HAVING filtering is a pure function of the
already-deterministic per-group aggregate output and runs on the single
deterministic apply thread, so it does not perturb the determinism oracles.

## vulcan workspace test result

`CARGO_TARGET_DIR=/tmp/kdb-t-having cargo test --workspace --release` on vulcan
(commit a6ced40, rustc 1.95.0):

```
TOTAL: 2699 passed / 0 failed
```

Includes the determinism oracles — `large_seed_corpus_is_deterministic_and_converges`,
`partition_corpus_is_deterministic`, `jepsen_3replica_partition_converges_byte_identical`,
and the 3-replica byte-identity suite (kessel-vsr: 28 passed in 1966s;
simulator: 226 passed in 53s) — all green, confirming the additive,
marker-guarded HAVING wire bytes did not perturb the no-HAVING frames.

New HAVING tests:
- `kessel-proto::tests::sp_pg_sql_having_wire_round_trip_and_byte_identity`
- `kessel-sql::tests::sp_pg_sql_having_planner_attaches_predicate`
- `kessel-sm::tests::sp_pg_sql_having_filters_groups`

## vulcan psql smoke transcript

Full transcript: `docs/superpowers/sppgsqlhaving-smoke-2026-06-03.txt`.
Server: `/tmp/kdb-t-having/release/kesseldb` (`--features pg-gateway`,
`KESSELDB_PG_ADDR=127.0.0.1:5550`, `KESSELDB_TOKEN=admin`); client psycopg2 2.9.12.

```
# psycopg2 2.9.12 -> postgresql://test:admin@127.0.0.1:5550/kesseldb
STAGE ddl: PASS author + book created
STAGE seed: PASS 3 authors, 6 books
STAGE baseline_groups: PASS 3 groups (no HAVING): [('asimov', 2), ('lonely', 1), ('tolkien', 3)]
STAGE having_gt2: PASS before=3 after=1 -> [('tolkien', 3)]
STAGE having_ge2: PASS before=3 after=2 -> [('asimov', 2), ('tolkien', 3)]
STAGE having_eq1: PASS before=3 after=1 -> [('lonely', 1)]
STAGE having_ne3: PASS before=3 after=2 -> [('asimov', 2), ('lonely', 1)]
STAGE having_none: PASS before=3 after=0 -> []
--- 8/8 stages PASS ---
HAVING SMOKE COMPLETE
```

HEADLINE: the same `GROUP BY` query, baseline = **3 groups**, then with each
HAVING predicate filters to exactly the surviving groups (`> 2` → 1, `>= 2` → 2,
`= 1` → 1, `<> 3` → 2, `> 99` → 0). HAVING filters groups correctly over the PG
wire, end to end.

## CLOSED

**CLOSED 2026-06-03.** Shipped green on origin/main: 2699/0 workspace tests on
vulcan (determinism oracles included), 8/8 psql smoke stages with correct group
filtering. Deferred (named follow-ups, by design): HAVING over an aggregate not
in the SELECT projection (`SP-PG-SQL-HAVING-EXTRA-AGG`) — cleanly rejected in V1
to avoid undescribed result columns / wire churn; HAVING over the group key
(`SP-PG-SQL-HAVING-KEY`); the plain (non-JOIN) `GROUP BY … HAVING` has no
dedicated PG-gateway render path yet (validated at the SQL + SM layers), shared
with the broader plain-group-aggregate gateway-render follow-up.
