# KesselDB Sub-project 51 — cluster-side prepared-statement cache

**Date:** 2026-05-17  **Status:** shipped, tested. 145 green.

## What this closes

SP47 cached compiled SQL on the **single-node** engine for a 26× compile
speedup, but explicitly left the **cluster** SQL path recompiling every
request — the documented follow-up was "needs DDL-linearization
reasoning." SP51 delivers it correctly.

## The mechanism: a deterministic catalog epoch

The hard part is invalidation: a cached plan must never be served against
a changed schema, and in the cluster a DDL changes the catalog only when
it *commits* (applied inside `Replica::handle`/`apply_through`), not when
it is submitted.

Solution — one choke point. Every catalog mutation in the state machine
flows through `persist_catalog`. SP51 bumps a `catalog_epoch: u64` there:

- **Single point, total coverage**: CreateType, AlterTypeAddField, every
  index/constraint/trigger DDL — all call `persist_catalog`, so all bump
  the epoch.
- **Deterministic**: same applied-op stream ⇒ same epoch on every replica.
- **Not replicated state**: the epoch is engine-local metadata, never part
  of the digest, so it cannot affect consensus or convergence — proven by
  the full determinism/VSR corpus staying green.

`StateMachine::catalog_epoch()` → `Replica::catalog_epoch()` exposes it.

## The cache

The cluster engine keeps `HashMap<String, (u64 epoch, Stmt)>`. On a SQL
request it serves the cached `Stmt` **iff the stored epoch equals the
current `replica.catalog_epoch()`**; otherwise it recompiles and stores
`(current_epoch, stmt)`. No explicit invalidation pass is needed — a
stale-epoch entry is simply recompiled and overwritten. Bounded
(`cap = 4096`, deterministic clear on overflow). Engine-thread-local, so
determinism and exactly-once are untouched.

## Test (1 new, 145 total)

`cluster_sql_cache_correct_across_ddl` (3 real-TCP nodes, client front):
identical result on a same-epoch cache hit; a `CREATE TABLE` (catalog
change → epoch bump) followed by the previously-cached query still
returns the correct result (recompiled, not a stale-epoch entry); a new
table is queryable; the `UPDATE` RMW path works through the cache. The
existing `sql_over_cluster_full_crud_and_rmw` now exercises the cached
path unchanged (regression guard), and the entire determinism/VSR/
partition corpus stays green.

## Honest framing

This is the correctness-careful extension SP47 named, not a new speedup
claim: the per-statement compile saving is the same ~µs→~ns SP47 measured;
SP51's contribution is making it **safe on the replicated cluster** via a
deterministic, digest-invisible epoch — verified by the full suite, not
assumed.
