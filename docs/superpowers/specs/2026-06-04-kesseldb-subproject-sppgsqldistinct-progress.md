# SP-PG-SQL-DISTINCT — progress tracker

**Arc:** `SELECT DISTINCT` row deduplication over the PostgreSQL wire.
**Date:** 2026-06-04
**Design:** `docs/superpowers/specs/2026-06-04-kesseldb-sppgsqldistinct-design.md`
**Status:** CLOSED.

## Task list

- [x] sql: `select_is_distinct(sql) -> bool` — lexer-backed render-time signal
      (true for plain `DISTINCT` after SELECT; false for `DISTINCT ON (…)`, the
      `ON` keyword being the discriminant).
- [x] sql: `select_columns`, `select_star_table`, `select_projection_to_star`
      skip an optional `DISTINCT` right after SELECT so the projection + table
      parse exactly like the non-distinct form; `DISTINCT ON (…)` → `None`.
- [x] sql: the compiler's `SELECT` arm consumes an optional `DISTINCT` (via
      `P::kw_peek_distinct`, leaving `DISTINCT ON` for the parser to reject) so
      `SELECT DISTINCT … FROM t` compiles to the SAME `Op` as the non-distinct
      form (engine returns all rows).
- [x] gateway: `dedup_data_rows(rows_buf) -> Option<(Vec<u8>, u64)>` dedups a
      run of encoded `DataRow` messages by full message body (exact DISTINCT
      semantics incl. NULL-not-distinct-from-NULL), keeping first occurrence in
      scan order.
- [x] gateway: the projection-list path and the `SELECT *` path in
      `render_select_got` dedup the emitted DataRows when `select_is_distinct`
      is true and report the DEDUPED count in the `SELECT N` tag.
- [x] scope: DISTINCT + WHERE and DISTINCT + ORDER BY (sorted scan order
      preserved post-dedup) work. `DISTINCT ON (…)`, DISTINCT over JOIN, and
      DISTINCT over aggregate/GROUP BY are NAMED FOLLOW-UPS — cleanly errored,
      never returned with duplicates.
- [x] determinism: RENDER-ONLY — `SELECT DISTINCT …` compiles to the identical
      `Op` (compile-equivalence test); no `Op`/wire/storage change; no oracle
      construction site touched.
- [x] sql tests: `select_distinct_recognizers`,
      `select_distinct_compiles_identically_to_nondistinct`.
- [x] gateway tests: `dedup_data_rows_keeps_first_and_dedups_nulls`,
      `dedup_data_rows_multi_cell_tuples`, `dedup_data_rows_rejects_non_datarow`.
- [x] vulcan: `cargo test --workspace --release` green (exit 0).
- [x] vulcan: psql smoke `scripts/sppgsqldistinct-smoke.py` — 8/8 stages PASS.
- [x] docs: design, this progress tracker, USAGE §3, STATUS, CHANGELOG.
- [x] commit + push to origin/main; cleanup vulcan worktree + target dir + data.

## Determinism

RENDER-ONLY arc. `SELECT DISTINCT … FROM t` compiles to the SAME `Op` as the
non-distinct `SELECT … FROM t` (proven by `select_distinct_compiles_identically
_to_nondistinct`); the dedup is a pure gateway render step over the emitted
DataRows. No `Op` / wire / storage change, no new write path, no oracle literal
construction site touched, so the corpus / partition / 3-replica byte-identity
oracles are unaffected — confirmed by the full-workspace test run below.

## Named follow-ups

- `DISTINCT ON (…)` (Postgres extension) — cleanly rejected (the `ON` keyword
  discriminates it from plain DISTINCT in `select_is_distinct`, the recognizers,
  and the compiler).
- DISTINCT over JOIN — falls through to the existing clean `0A000` "unsupported"
  error rather than returning duplicates (`join_projection` is left
  DISTINCT-unaware on purpose).
- DISTINCT over aggregate / GROUP BY (`SELECT DISTINCT category, COUNT(*) …`) —
  the aggregate recognizers reject the leading DISTINCT token, not mis-rendered.

## vulcan transcript — `cargo test --workspace --release`

Full workspace test on vulcan in an isolated worktree
(`/tmp/kdb-di-wt`, `CARGO_TARGET_DIR=/tmp/kdb-t-di`): **exit code 0 (all
green)**, including the determinism oracles (`large_seed_corpus_is_
deterministic_and_converges`, `jepsen_3replica_partition_converges_byte_
identical`, sharded-engine / read-pool / parallel-reads oracles all run).

The 5 new tests, captured by name (filtered run):

```
test dispatch::tests::dedup_data_rows_keeps_first_and_dedups_nulls ... ok
test dispatch::tests::dedup_data_rows_rejects_non_datarow ... ok
test dispatch::tests::dedup_data_rows_multi_cell_tuples ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 1007 filtered out; finished in 0.00s
...
test tests::select_distinct_compiles_identically_to_nondistinct ... ok
test tests::select_distinct_recognizers ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 148 filtered out; finished in 0.00s
EXIT=0
```

## vulcan smoke transcript — `scripts/sppgsqldistinct-smoke.py`

Live psycopg2 smoke against a gateway-built server
(`KESSELDB_PG_ADDR=127.0.0.1:5558`, positional `<client_addr> <data_dir>`):

```
# psycopg2 2.9.12 (dt dec pq3 ext lo64) -> postgresql://test:admin@127.0.0.1:5558/kesseldb
STAGE ddl: PASS events created
STAGE seed: PASS 7 events (with dups + 2 NULL-region rows)
STAGE distinct_region: PASS 3 unique regions ['<NULL>', 'eu', 'us'] (< 7 total rows)
STAGE nondistinct_back: PASS non-distinct SELECT region → all 7 rows (dups kept)
STAGE distinct_pair: PASS 4 unique (region,category) pairs: [('<NULL>', 'view'), ('eu', 'click'), ('us', 'click'), ('us', 'view')]
STAGE distinct_null: PASS NULL region appears exactly once under DISTINCT
STAGE distinct_star: PASS SELECT DISTINCT * → 7 unique whole rows
STAGE distinct_collapse: PASS non-distinct=7 distinct=2 ['click', 'view'] (dups collapsed)
=== SP-PG-SQL-DISTINCT SMOKE SUMMARY ===
  PASS  ddl
  PASS  seed
  PASS  distinct_region
  PASS  nondistinct_back
  PASS  distinct_pair
  PASS  distinct_null
  PASS  distinct_star
  PASS  distinct_collapse
--- 8/8 stages PASS ---
SP-PG-SQL-DISTINCT SMOKE COMPLETE
SMOKE_EXIT=0
```

**HEADLINE:** `SELECT DISTINCT region FROM events` → the **3 UNIQUE** regions
`{us, eu, NULL}` (count 3 **< 7** total rows), while the non-distinct
`SELECT region FROM events` still returns **all 7** rows (`nondistinct_back`).
`distinct_pair` → 4 unique `(region, category)` tuples. `distinct_null` proves
NULL is not distinct from NULL (the two NULL-region rows collapse to one).
`distinct_star` → 7 unique whole rows. `distinct_collapse` shows
`SELECT DISTINCT category` collapsing 7 rows (3 click + 4 view) to 2 values
while the non-distinct projection still returns all 7.
