# KesselDB Sub-project 50 — read cache on by default

**Date:** 2026-05-17  **Status:** shipped, tested. 144 green.

## The gap

A bounded LRU `ReadCache` already existed, was wired into the state
machine (insert on create/update, invalidate on update/delete, consulted
by `GetById`) and tested — but `StateMachine::open` left it `None`, so the
**product (and every cluster node) ran with hot point reads uncached**.
Only an explicit `with_cache(cap)` builder turned it on.

## The fix (one line, like SP49)

`StateMachine::open` now constructs the cache with a default capacity
(`DEFAULT_READ_CACHE = 8192`). `with_cache` still overrides the capacity;
the raw builder path can still run cache-off.

Safety/correctness — why this is zero-risk:

- The cache is **digest-invisible**: it is never consulted to compute
  committed state or the content digest (documented invariant on the
  field), only to short-circuit a `GetById`.
- It is **deterministically invalidated** on every write/delete of the
  key (pre-existing, tested logic).
- Therefore replicas remain bit-identical and the seeded
  determinism / VSR-convergence corpus is the proof: the **entire
  144-test workspace stays green** with the cache on by default, including
  every multi-node and partition-simulation test.

The cluster benefits automatically (it opens nodes via
`StateMachine::open`).

## Test (1 new, 144 total)

`read_cache_on_by_default_and_correct_under_mutation`: cache is `Some` on
a fresh `open`; repeated `GetById` registers hits (`hit_rate > 0`); a
read after `Update` returns the **new** value (not a stale entry); a read
after `Delete` returns `NotFound`. Correctness under mutation is proven,
not assumed.

## Honest framing

This is an enablement + safe-default slice, not a new algorithm — the
caching machinery was already built and tested; SP50 makes the product
actually use it. No performance number is fabricated: the win is
"hot-key `GetById` served from an in-memory LRU instead of the LSM," and
the guarantee that matters (no observable/replicated change) is proven by
the full corpus.
