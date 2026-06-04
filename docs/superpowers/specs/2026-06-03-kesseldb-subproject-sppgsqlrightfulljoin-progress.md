# SP-PG-SQL-RIGHT-FULL-JOIN — progress tracker

**Status:** CLOSED
**Date:** 2026-06-03
**Design:** `2026-06-03-kesseldb-sppgsqlrightfulljoin-design.md`

## Task list

- [x] kessel-proto: `JoinType::{Right, Full}` + `wire_tag`/`from_wire_tag` for
      tags 2/3 (additive; Inner byte-identical, Left = tag 1 unchanged).
- [x] kessel-proto: Right/Full roundtrip cases in the oracle corpus.
- [x] kessel-sm `apply_join`: RIGHT (matched + unmatched-right, `a.*` NULL) and
      FULL (LEFT results + unmatched-right), column order `a.* ++ b.*`,
      deterministic row order, RIGHT/FULL chain rejected.
- [x] kessel-sm: combined-schema nullability switches per side.
- [x] kessel-sm: KAT tests INNER/LEFT/RIGHT/FULL row sets + determinism re-run
      + chain rejection.
- [x] kessel-sql: parse `RIGHT/FULL [OUTER] JOIN` + `INNER JOIN` base joins;
      single-table fast-path guards + `consume_join_kw` + alias lookahead learn
      RIGHT/FULL.
- [x] kessel-sql: parse KAT (RIGHT/FULL/INNER → JoinType, aliases,
      `join_projection`).
- [x] pg-gateway: VERIFIED `render_join_result` needs NO change (same KTR1
      stream shape).
- [x] New smoke `scripts/sppgsqlrightfulljoin-smoke.py` (port 5556).
- [x] Workspace test green on vulcan (release).
- [x] Regression: existing JOIN smoke still green.
- [x] Docs: USAGE §3, STATUS, CHANGELOG, README.
- [x] Closure commits on origin/main.
- [x] vulcan cleanup (worktree + target dir + servers + data dirs).

## Scope / deferral

- V1: RIGHT/FULL on a BINARY (two-table) join. Composes with WHERE / ORDER BY /
  LIMIT / OFFSET / GROUP BY / table aliases (reuses the LEFT paths).
- **Deferred (named follow-up `SP-PG-SQL-OUTER-CHAIN`):** RIGHT/FULL on the base
  join of a 3+ table CHAIN. Outer chains are their own complexity (LEFT chains
  were already a follow-up). Rejected cleanly with a `SchemaError`; INNER chains
  keep working; RIGHT/FULL on the first/only join works.

## Transcripts

### 1. Workspace test (vulcan, release)

```
cargo test --workspace --release on vulcan (fresh worktree off origin/main): exit 0 — all green.
Determinism oracles explicitly pass:
  jepsen_3replica_partition_converges_byte_identical ... ok
  jepsen_mvcc_keyspace_3replica_byte_identical_under_partition ... ok
  large_seed_corpus_is_deterministic_and_converges ... ok
  scatter_scan pentest_9/pentest_10/merge_* byte_identical ... ok
  sharded_engine t2_determinism_oracle_k1_k4_k8_byte_equal ... ok
  read_pool determinism_oracle_100_random_workloads ... ok
RIGHT/FULL are new JoinType tags 2/3 emitted only for non-Inner joins, so INNER frames stay byte-identical and the oracles are untouched.
```

### 2. Regression — existing JOIN smoke (alias)

```
# scripts/sppgsqljoinalias-smoke.py --no-server (against the RIGHT/FULL build)
  PASS  ddl / seed / alias_binary / full_names / as_form / alias_three_way / alias_where / alias_order_by
--- 8/8 stages PASS ---
JOIN-ALIAS SMOKE COMPLETE
# INNER / LEFT / aliased / 3-way-chain joins all still correct — no regression from adding RIGHT/FULL.
```

### 3. New smoke — sppgsqlrightfulljoin-smoke.py

```
# scripts/sppgsqlrightfulljoin-smoke.py --no-server  (psycopg2 2.9.12, PG 127.0.0.1:5556)
STAGE ddl: PASS authors, books created
STAGE seed: PASS 2 authors (1 orphan), 4 books (1 homeless)
STAGE inner: PASS matched only → [('tolkien', 'hobbit'), ('tolkien', 'lotr'), ('tolkien', 'silmarillion')]
STAGE left: PASS matched + (orphan, None) → [('orphan', None), ('tolkien', 'hobbit'), ('tolkien', 'lotr'), ('tolkien', 'silmarillion')]
STAGE right: PASS matched + (None, 'homeless') [a.name is None] → [(None, 'homeless'), ('tolkien', 'hobbit'), ('tolkien', 'lotr'), ('tolkien', 'silmarillion')]
STAGE full: PASS matched + (orphan,None) + (None,'homeless'), no dup → [(None, 'homeless'), ('orphan', None), ('tolkien', 'hobbit'), ('tolkien', 'lotr'), ('tolkien', 'silmarillion')]
STAGE nulls: PASS RIGHT a.name=None & FULL b.title=None both read as Python None
STAGE right_outer: PASS RIGHT OUTER JOIN accepted
STAGE full_outer: PASS FULL OUTER JOIN accepted
--- 9/9 stages PASS ---
RIGHT-FULL-JOIN SMOKE COMPLETE

Every stage uses a HARD assert. `right` proves the unmatched-RIGHT row appears with the LEFT
column NULL AND the projection column order stays a.*,b.* (a.name first). `full` proves both
unmatched sides appear with no duplicate of the matched pairs. `nulls` proves the NULL-filled
side reads back as Python None over the wire.
```
