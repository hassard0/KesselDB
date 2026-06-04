# SP-PG-NULL-INT-RENDER — progress tracker

**Status:** CLOSED
**Date:** 2026-06-03

## Tasks
- [x] Diagnose the exact failure layer (render layer — non-sorted projection)
- [x] Confirm INSERT lowering sets `Value::Null` for omitted nullable (CORRECT)
- [x] Confirm codec `encode`/`decode` honor the null bitmap (CORRECT)
- [x] Confirm `SELECT *` (`emit_data_rows`/`decode_record`) renders NULL (CORRECT)
- [x] Fix: re-project non-sorted projection via `SELECT *` full records (root, render-only)
- [x] Add `kessel_sql::select_projection_to_star` + token-boundary FROM finder
- [x] Make `emit_projected_from_full_records` robust to bare-record (GetById) shape
- [x] Add explicit `NULL` literal support to INSERT VALUES (`Lit::Null`)
- [x] Generic across kinds (int + TEXT/CHAR), NOT-NULL/PK back-compat preserved
- [x] Unit tests (kessel-sql rewrite helper + boundary cases)
- [x] New psql smoke `scripts/sppgnullintrender-smoke.py`
- [x] Workspace test green on vulcan (exit 0)
- [x] Regression: relationships + realapp + fk-enforce smokes green
- [x] Docs: USAGE / STATUS / CHANGELOG
- [x] Closure commit on origin/main + final git-log proof
- [x] vulcan cleanup (worktree + target dir + data dirs)

## Transcripts

### Workspace test (vulcan, `CARGO_TARGET_DIR=/tmp/kdb-t-nz cargo test --workspace --release`)
`cargo test --workspace --release` on origin/main @ `6889f04` — **exit code 0**,
zero failures / zero panics across the whole workspace (grep for FAILED|panicked
= 0). Per-crate counts for the directly-touched crates (re-run for the record):
```
     Running unittests src/lib.rs (kessel_codec)
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
     Running unittests src/lib.rs (kessel_pg_gateway)
test result: ok. 1007 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
     Running unittests src/lib.rs (kessel_sql)
test result: ok. 147 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```
(pg-gateway went 1005+2-failed → 1007/0 after the projection KATs were moved to
full-record streams; kessel-sql +6 new rewrite-helper tests.)

### Regression smoke — sppgormrelationships (`--no-server`, self-built pg-gateway)
```
STAGE create_all_fk_ddl: PASS 2 CREATE TABLE (2nd w/ FK)
STAGE cascade_insert: PASS author id=1 book ids=[1, 2]
STAGE join_query: PASS joined -> [('tolkien', 'hobbit'), ('tolkien', 'lotr')]
STAGE lazy_load_nav: PASS author.books (lazy) -> ['hobbit', 'lotr']
--- 4/4 stages PASS ---
```

### Regression smoke — sppgormrealapp (`--no-server`, self-built pg-gateway)
```
STAGE schema: PASS 3 CREATE TABLE (2 w/ FK)
STAGE cascade_seed: PASS 2 users, 3 posts, 2 comments
STAGE Q1_join: PASS posts+author -> [("bob's first post", 'bob'), ('hello world', 'alice'), ('kesseldb rocks', 'alice')]
STAGE Q2_filtered_join: PASS alice's posts -> ['hello world', 'kesseldb rocks']
STAGE Q3_group_agg: PASS comment counts -> [('hello world', 2)]
STAGE Q4_paginate: PASS paginated -> ["bob's first post", 'hello world']
STAGE Q5_nav: PASS alice.posts -> ['hello world', 'kesseldb rocks']
STAGE Q6_update_delete: PASS after update+delete, comment count -> 1
--- 8/8 stages PASS ---
```

### Regression smoke — sppgddlfkenforce (self-built pg-gateway)
```
STAGE ddl: PASS  | STAGE seed_parent: PASS | STAGE good_insert: PASS
STAGE bad_insert: PASS (SQLSTATE 23503) | STAGE null_fk: PASS
STAGE restrict_block: PASS (23503; parent survives) | STAGE restrict_clear: PASS
--- 7/7 stages PASS ---
```

### NEW smoke — sppgnullintrender (real psycopg2 2.9.12 transcript)
```
# psycopg2 2.9.12 (dt dec pq3 ext lo64) -> postgresql://test:admin@127.0.0.1:5555/kesseldb
STAGE ddl: PASS t(id BIGINT pk, n BIGINT NULL, note CHAR(16) NULL)
STAGE star_omitted_null: PASS SELECT * -> (1, None, 'hi')  (n is None)
STAGE proj_omitted_null: PASS SELECT n -> (None,)
STAGE explicit_value: PASS n=42 round-trips via projection AND *
STAGE explicit_null: PASS INSERT … VALUES(3, NULL, 'y') -> n is None
STAGE text_omitted_null: PASS SELECT note (omitted) -> (None,)  (generic across kinds)
STAGE notnull_backcompat: PASS PK id=1 + note='hi' read their real values
--- 7/7 stages PASS ---
```

## CLOSED
All asserts green; fix is on origin/main; vulcan cleaned up.
