# KesselDB Sub-project 47 — prepared-statement compile cache

**Date:** 2026-05-17  **Status:** shipped, tested, benchmarked. 140 green.
**Theme:** innovate so PG-flexibility costs nothing in TB-speed.

## The bottleneck

The deterministic core is single-threaded by design — every cycle on the
engine thread is the throughput ceiling. Every `Client::sql(...)` request
re-ran the full `kessel_sql::compile_stmt` (tokenise → recursive-descent
parse → plan) against the catalog *before* the op was even applied. For
the overwhelmingly common case (the same statement shape issued over and
over) that is pure repeated CPU on the hottest, least-parallel path.

## The innovation

`CompileCache`: an engine-thread-local `sql String -> compiled Stmt` map.
A hit clones the already-compiled `Stmt` (~tens of ns) instead of
recompiling (~µs). Correctness is *identical to always recompiling*:

- The cache is engine-local and never touches replicated state, so the
  state machine, digests and determinism are unchanged.
- It is **invalidated whenever an applied op can change the catalog**
  (`mutates_schema`: CreateType, AlterTypeAddField, CreateIndex, AddUnique,
  AddForeignKey, AddCheck, AddTrigger, AddOrderedIndex, AddCompositeIndex,
  and conservatively Txn). A cached plan can therefore never be served
  against a changed schema.
- Bounded (`cap = 4096`, deterministic clear on overflow).
- Functionality is **100% unchanged** — same `Stmt`, same `Op`, same
  result bytes; the SQL `UPDATE` RMW path flows through it untouched.

## Measured (kessel-bench `sqlcache`, release, localhost)

```
SQL compile (cold)  :       573,960 stmt/s   (recompile every request)
SQL compile (cached):    15,035,785 stmt/s   (compile once, clone)
speedup             :          26.2x
```

~1.7 µs of tokenise+parse+plan removed from the single-threaded hot path
per repeated statement, replaced by a ~66 ns clone.

## Test (1 new, 140 total)

`compile_cache_is_correct_across_schema_change`: identical result on a
cache hit; after a DDL (catalog change) the previously-cached query still
returns the correct result (recompiled cleanly — invalidation is safe, not
stale) and a brand-new statement against the new table compiles; the
`UPDATE` RMW path is exercised through the cache. The existing ~10 SQL
e2e/cluster tests are unchanged (regression guard).

## Honest scope boundary

Implemented on the single-node engine (the throughput surface the
benchmark measures and the common deployment). The cluster engine's SQL
path still compiles per request: caching there must also reason about the
*linearization point* of a concurrently-submitted DDL, which deserves its
own careful slice — named here, not silently skipped. Not a gate (failover
SQL correctness is unaffected); a pure further-speed follow-up.
