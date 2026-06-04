# SP-PG-DDL-FK-ENFORCE — progress tracker

**Status: CLOSED**

Make a `FOREIGN KEY` declared in `CREATE TABLE` DDL actually ENFORCE referential
integrity by WIRING the DDL parser to the pre-existing engine FK machinery.

## Tasks

| # | Task | Status |
|---|------|--------|
| T1 | `kessel-catalog`: `FkSpec` + `encode_type_def_full_fk` / `decode_type_fks` (marker-guarded additive FK trailer; empty ⇒ byte-identical) | DONE |
| T2 | `kessel-sql`: `parse_referential_actions` returns the `on_delete` code; capture table-level + inline FK descriptors into `fk_specs`; route `CreateType` through `encode_type_def_full_fk` | DONE |
| T3 | `kessel-sm`: factor `add_foreign_key` helper; `Op::CreateType` pre-validates FK names → ids (atomic, no half-created type on forward ref) then registers via the shared path | DONE |
| T4 | `kessel-pg-gateway`: widen `constraint_to_sqlstate` so the RESTRICT / dangling-ref messages also map to `23503` (INSERT-enforce path already mapped) | DONE |
| T5 | Unit tests: catalog FK trailer round-trip + byte-identity; SQL FK-capture + ON DELETE keyword mapping; SM DDL-FK registered+enforced + forward-ref clean error; gateway 23503 mapping | DONE |
| T6 | New psql smoke `scripts/sppgddlfkenforce-smoke.py` (good/bad/NULL insert + RESTRICT) | DONE |
| T7 | Regression: ORM relationships + realapp smokes still green under enforcement | DONE |
| T8 | Closure docs (this file, design, USAGE/STATUS/CHANGELOG/README) + push | DONE |

## Determinism

- The FK trailer is additive + marker-guarded (`0xFE`): a no-FK CREATE TABLE emits
  a BYTE-IDENTICAL `def` to before this arc. The `Op` enum is UNCHANGED, so every
  `Op::CreateType { def }` construction site (proto/sm/sql/read_pool/sharded_engine/
  oracles/benches) is unaffected — no "missing field in oracle literal".
- FK registration runs on the single deterministic apply thread; name→id
  resolution is a pure function of catalog state. Oracles stay green (see
  workspace transcript).

## REAL transcripts

### Workspace test (vulcan, `cargo test --workspace --release`)

`CARGO_TARGET_DIR=/tmp/kdb-t-fk cargo test --workspace --release` exited **0**
(full sweep on vulcan from the isolated `/tmp/kdb-fk-wt` worktree at
`origin/main`). The four changed crates (re-run, already built):

```
kessel-catalog    test result: ok. 10 passed; 0 failed
kessel-sql        test result: ok. 1007 passed; 0 failed  (lib)
kessel-sm         test result: ok. 193 passed; 0 failed   (lib)  + 10 + 6 + 141 (oracle/integration bins) all ok
kessel-pg-gateway test result: ok. 141 passed; 0 failed
```

The 6 new tests, confirmed by name:

```
test tests::fk_trailer_round_trips_and_is_byte_identical_when_empty ... ok   (kessel-catalog)
test tests::fk_table_constraint_ddl_parses ... ok                            (kessel-sql)
test tests::ddl_declared_fk_is_registered_and_enforced_at_apply ... ok       (kessel-sm)
test tests::ddl_fk_forward_reference_is_a_clean_error ... ok                 (kessel-sm)
test error::tests::sppgddlfkenforce_insert_violation_maps_to_23503 ... ok    (kessel-pg-gateway)
test error::tests::sppgddlfkenforce_restrict_block_maps_to_23503 ... ok      (kessel-pg-gateway)
```

Determinism oracles (`large_seed_corpus_is_deterministic_and_converges`,
`partition_corpus_is_deterministic`, `jepsen_3replica_partition_converges_byte_identical`,
`sharded_engine t2_determinism_oracle_*`, `read_pool determinism_oracle_*`)
all included in the exit-0 workspace sweep — green.

### Regression — `scripts/sppgormrelationships-smoke.py` (server on 5549, self-built pg-gateway)

```
# SQLAlchemy 2.0.45 -> postgresql+psycopg2://test:admin@127.0.0.1:5549/kesseldb
STAGE create_all_fk_ddl: PASS 2 CREATE TABLE (2nd w/ FK)
STAGE cascade_insert: PASS author id=1 book ids=[1, 2]
STAGE join_query: PASS joined -> [('tolkien', 'hobbit'), ('tolkien', 'lotr')]
STAGE lazy_load_nav: PASS author.books (lazy) -> ['hobbit', 'lotr']
--- 4/4 stages PASS ---
RELATIONSHIPS SMOKE COMPLETE
```

### Regression — `scripts/sppgormrealapp-smoke.py` (server on 5556)

```
# SQLAlchemy 2.0.45 -> postgresql+psycopg2://test:admin@127.0.0.1:5556/kesseldb
STAGE schema: PASS 3 CREATE TABLE (2 w/ FK)
STAGE cascade_seed: PASS 2 users, 3 posts, 2 comments
STAGE Q1_join: PASS posts+author -> [("bob's first post", 'bob'), ('hello world', 'alice'), ('kesseldb rocks', 'alice')]
STAGE Q2_filtered_join: PASS alice's posts -> ['hello world', 'kesseldb rocks']
STAGE Q3_group_agg: PASS comment counts -> [('hello world', 2)]
STAGE Q4_paginate: PASS paginated -> ["bob's first post", 'hello world']
STAGE Q5_nav: PASS alice.posts -> ['hello world', 'kesseldb rocks']
STAGE Q6_update_delete: PASS after update+delete, comment count -> 1
--- 8/8 stages PASS ---
REALAPP SMOKE COMPLETE
```

Both regression smokes stay ALL-GREEN under enforcement: the ORM inserts
parents before children and uses the parent's assigned id as the child FK, so
enforcement is satisfied by the dependency-ordered seed.

### New smoke — `scripts/sppgddlfkenforce-smoke.py` (server on 5554, self-built pg-gateway)

```
# psycopg2 2.9.12 -> postgresql://test:admin@127.0.0.1:5554/kesseldb
STAGE ddl: PASS parent + child(FK parent_id->parent.id ON DELETE RESTRICT)
STAGE seed_parent: PASS parent id=1
STAGE good_insert: PASS child(10 -> parent 1) inserted -> (10, 1)
STAGE bad_insert: PASS rejected with SQLSTATE 23503 (foreign_key_violation): FOREIGN KEY violated on field 'parent_id' -> type 1
STAGE null_fk: PASS child(12, NULL fk) inserted — enforcement skipped the NULL FK
STAGE restrict_block: PASS RESTRICT blocked parent delete with SQLSTATE 23503; parent survives
STAGE restrict_clear: PASS child 10 removed -> parent 1 delete succeeds
--- 7/7 stages PASS ---
DDL-FK-ENFORCE SMOKE COMPLETE
```

HEADLINE proven over the real PG wire: a bad child INSERT returns SQLSTATE
**23503** (`e.pgcode == '23503'` asserted via psycopg2), a good child INSERT
succeeds, a NULL FK is allowed, and ON DELETE RESTRICT blocks deleting a
referenced parent (23503).

## Deferred

- `SP-PG-DDL-COMPOSITE-FK` — composite FKs (V1 captures the FIRST column only).
- `SP-PG-DDL-FK-ON-UPDATE` — `ON UPDATE` actions parsed but not enforced.
- True inline circular FKs require the parent to exist first (`ALTER TABLE ADD
  CONSTRAINT` via `Op::AddForeignKey` is the cycle escape hatch).
