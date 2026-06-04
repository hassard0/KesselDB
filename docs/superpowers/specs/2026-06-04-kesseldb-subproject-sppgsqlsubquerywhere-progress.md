# SP-PG-SQL-SUBQUERY-WHERE — progress tracker

**Status:** CLOSED (2026-06-04)
**Design:** `2026-06-04-kesseldb-sppgsqlsubquerywhere-design.md`

## Task list

- [x] `kessel_sql::find_where_subquery` — quote-skipping, paren-balancing scan
      detecting `col IN / NOT IN / <cmp> (SELECT …)` (+ `SubqueryOp`,
      `WhereSubquery`, `detect_subquery_op`, `balanced_close_paren`).
- [x] detection KATs (IN / NOT IN / all scalar ops / nested parens / string-
      literal skip / plain-IN-list None / ident-boundary).
- [x] `kessel-pg-gateway::subquery::rewrite_where_subquery` — two-phase: run the
      inner via `dispatch_query`, parse RowDescription + DataRows, splice typed
      values, return rewritten outer SQL (or a clean error frame).
- [x] hook into `dispatch_query` + `dispatch_query_with_params` (before the
      engine call; recurse on the rewritten outer).
- [x] value quoting by type (int OIDs bare, text single-quoted + `'` doubled),
      NULL handling, empty-IN / empty-NOT-IN / scalar-0-row contradictions.
- [x] error paths: inner ≠ 1 column (42601), scalar > 1 row (21000), inner
      error surfaced.
- [x] gateway KATs (literal quoting, RowDescription parse, column collect, LHS
      column extraction).
- [x] new psql smoke `scripts/sppgsqlsubquerywhere-smoke.py` (10 stages).
- [x] docs: design, this progress, USAGE §Queries, STATUS, CHANGELOG.
- [x] workspace tests green on vulcan (`--release`).
- [x] commit + push feat / test / docs to `origin/main`.

## Determinism

No `Op`/wire/storage change — pure gateway orchestration + render. The inner
and outer both run on the deterministic apply path; spliced IN-list order is the
inner scan order. Oracles untouched.

## VALIDATION — workspace test (vulcan, CARGO_TARGET_DIR=/tmp/kdb-t-sq)

`cargo test --workspace --release` → **exit 0** (full workspace green at HEAD
`4f69b08`; closure docs are docs-only on top). Doc-test tail:

```
   Doc-tests kessel_sql
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
   Doc-tests kesseldb_server
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

The 12 SP-PG-SQL-SUBQUERY-WHERE unit tests (8 in kessel-sql detection + 4 in the
gateway orchestration), all green:

```
running 4 tests
test subquery::tests::collect_first_column_numbers_and_text ... ok
test subquery::tests::inner_value_literal_quoting ... ok
test subquery::tests::lhs_column_text_for_each_op ... ok
test subquery::tests::parse_row_description_extracts_count_and_oid ... ok
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 1010 filtered out; finished in 0.00s
running 8 tests
test tests::subquery_detect_in ... ok
test tests::subquery_detect_in_not_matched_inside_ident ... ok
test tests::subquery_detect_nested_parens_balanced ... ok
test tests::subquery_detect_not_in ... ok
test tests::subquery_detect_plain_in_list_is_none ... ok
test tests::subquery_detect_scalar_all_ops ... ok
test tests::subquery_detect_scalar_eq ... ok
test tests::subquery_detect_skips_string_literal_paren ... ok
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 150 filtered out; finished in 0.00s
```

## VALIDATION — new smoke (vulcan, psycopg2)

`scripts/sppgsqlsubquerywhere-smoke.py` — REAL psycopg2 2.9.12 transcript,
**10/10 stages PASS**:

```
# psycopg2 2.9.12 (dt dec pq3 ext lo64) -> postgresql://test:admin@127.0.0.1:5559/kesseldb
STAGE ddl: PASS 5 tables created
STAGE seed: PASS 4 users, 3 orders, 1 banned, 4 products, 1 featured
STAGE in_subquery: PASS id IN (SELECT user_id WHERE total>100) → ['alice', 'carol']
STAGE not_in_subquery: PASS id NOT IN (SELECT user_id FROM banned) → ['alice', 'carol', 'dave']
STAGE scalar_max: PASS price = (SELECT MAX(price)) → ['gadget', 'gizmo']
STAGE empty_in: PASS id IN (empty) → 0 rows
STAGE empty_not_in: PASS id NOT IN (empty) → all users ['alice', 'bob', 'carol', 'dave'] (NULL-row edge documented)
STAGE string_subquery: PASS category IN (SELECT cat FROM featured) → ['gadget', 'sprocket', 'widget']
STAGE wrong_col_count: PASS 2-col subquery rejected: subquery must project exactly ONE column (projects 2)
STAGE scalar_multi_row: PASS multi-row scalar rejected: scalar subquery returned 4 rows (expected at most 1)

=== SP-PG-SQL-SUBQUERY-WHERE SMOKE SUMMARY ===
  PASS  ddl
  PASS  seed
  PASS  in_subquery
  PASS  not_in_subquery
  PASS  scalar_max
  PASS  empty_in
  PASS  empty_not_in
  PASS  string_subquery
  PASS  wrong_col_count
  PASS  scalar_multi_row
--- 10/10 stages PASS ---
SUBQUERY-WHERE SMOKE COMPLETE
```

The HEADLINE cases land: `WHERE id IN (SELECT user_id FROM orders WHERE total >
100)` → only the qualifying users (alice, carol); scalar `WHERE price = (SELECT
MAX(price) FROM products)` → the max-price products (gadget, gizmo).

## Deferred sub-cases (named follow-ups)

- `SP-PG-SQL-CORRELATED-SUBQUERY` — correlated inner (references an outer
  column). Detected best-effort: runs non-correlated; a genuine outer-column
  reference surfaces as a clean engine `unknown column` error.
- `SP-PG-SQL-MULTI-SUBQUERY` — more than one subquery per WHERE.
- `SP-PG-SQL-EXISTS` — `EXISTS` / `NOT EXISTS`.
- `SP-PG-SQL-FROM-SUBQUERY` — derived tables in FROM.
- `SP-PG-SQL-SELECT-SUBQUERY` — subqueries in the SELECT list.
- `SP-PG-SQL-SUBQUERY-NUMERIC-INLIST` — NUMERIC/float inner column spliced into
  an IN-list (the IN-list term parser pushes a quoted decimal as bytes, not a
  coerced number). Integer + text inner columns fully supported.
```
