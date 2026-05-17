# KesselDB Sub-project 17 — Equality-index sharding (ATTEMPTED, REVERTED)

**Date:** 2026-05-17  **Status:** attempted, measured, **reverted**. Recorded
as an honest negative result (same discipline as the seed-7 decision).

## Hypothesis

SP16's flex benchmark flagged equality-index writes (~6.5× vs plain) as the
#1 perf debt — the per-insert whole-bucket read-modify-write. Hypothesis:
shard each (field,value) id-list across N sub-keys (by an object-id byte)
plus a per-value shard bitmap, so a hot value's per-insert RMW touches only
~1/N of the ids.

## What was built & measured

Full implementation: 64-way shard keys, a per-(field,value) 64-bit shard
bitmap (extra small key so reads skip empty shards), `idx_add`/`idx_remove`/
`idx_lookup`/`unique_conflict`/`AddUnique` reworked. All 39 sm + 15 vsr tests
stayed green (semantics preserved, deterministic).

Re-ran the flex benchmark (same machine session, ratios vs `plain CREATE`):

| | SP16 (simple bucket) | SP17 (sharded+bitmap) |
|---|---|---|
| CREATE +eq-index vs plain | ~6.5× slower | ~6.0× slower (no real gain) |
| FindBy | ~1.2M ops/s | ~0.58M ops/s (**~2× regression**) |

## Why it didn't work / decision

At the benchmark's cardinality (~1000 distinct values, ~100 ids/bucket) the
original buckets were already small, so the cost was **per-key storage
overhead (LSM/WAL/clone), not bucket size**. Sharding *added* keys (shard
bucket + bitmap RMW) → roughly neutral on writes, and the bitmap + shard
fan-out **regressed point reads ~2×**. Sharding only helps pathological
extreme skew (very few distinct values → huge buckets), which is not the
measured debt and is still quadratic there anyway.

Shipping this as a "perf fix" would be overclaiming (it does not improve the
measured debt and regresses reads). **Reverted to the SP16 implementation.**

## The actual fix (future spec)

The right design is **one index entry per (value, object) as its own LSM
key** (O(1) put, no read-modify-write, prefix-scan reads) — blocked only by
the 20-byte storage key not fitting `field_id + value-digest + full
object_id`. The proper future spec widens the storage key (or adds a
secondary key encoding) so the index needs no bucket RMW at all. Recorded in
STATUS as the prioritized, correctly-scoped optimization.

102 tests green (unchanged — revert restored SP16 state).
