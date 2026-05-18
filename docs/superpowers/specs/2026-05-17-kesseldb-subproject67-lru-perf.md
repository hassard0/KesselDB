# KesselDB Sub-project 67 — profile-driven write-path fix (O(log n) LRU)

**Date:** 2026-05-17  **Status:** shipped, measured on vulcan. 166 green.
The single highest-impact perf change in the project — and a case study in
*profiling before optimising*.

## #6 — profile first (it changed the plan)

`perf` is locked down on the target (`perf_event_paranoid=4`, no sudo), so
a deterministic sub-phase profiler was added to `kessel-bench`
(`… profile`). It isolated every write-path phase:

```
Vec<u8> record clone            32 ns
codec::encode (9 fields)       118 ns
codec::decode (9 fields)       211 ns
sm.apply GetById (cached)      114 ns
Storage::put (WAL+sync+mt)    1593 ns
sm.apply Create (no index)  116738 ns   <-- the entire cost is HERE
```

`Storage::put` was 1.6 µs but `sm.apply(Create)` was **117 µs**. Varying
N (2k→2µs, 5k→2µs, 50k→117µs) proved it was *not* per-op allocations
(#2's hypothesis) but an O(cap)-once-full cost. The plan changed because
the measurement said so.

## Root cause

`ReadCache::insert` evicted the LRU victim with
`self.map.iter().min_by(...)` — an **O(cap) linear scan of all 8192
entries on every insert once the cache is full**. SP50 turned the read
cache on by default, so every write past 8192 rows paid an ~115 µs
eviction-scan tax. (N<8192 was fast → why the dev figure once looked
fine; the bug was *latent* until SP50.)

## The fix

`ReadCache` keeps a secondary `BTreeSet<(tick, Key)>` LRU index mirroring
the map. Eviction is now `order.iter().next()` — **O(log n)**, not
O(cap). Ordering is `(tick, key)`, **byte-identical** to the previous
deterministic tiebreak (oldest tick, then smallest key), so eviction
behaviour is unchanged. `get`/`insert`/`invalidate`/`clear` keep the
index consistent (all O(log n)). The cache remains digest-invisible — the
full VSR/determinism corpus stays green (166), proving zero behavioural
change to replicated state.

## Measured impact (vulcan, 16-core Xeon — cloud-representative)

| `kessel-bench mem` | before | after |
|---|---|---|
| CREATE throughput | 7,730 ops/s | **215,740 ops/s** (~28×) |
| CREATE p50 latency | 131 µs | **2 µs** (~65×) |
| GET | — | 824,000 ops/s, p50 1 µs |
| `profile` sm.apply Create | 116,738 ns | **2,393 ns** (~49×) |

`Storage::put` unchanged (~1.6 µs) — confirming the win was exactly the
LRU, nothing else. The result now matches/exceeds the historical
pre-SP50 dev figure: the bug had been masking real throughput.

## Tests

`kessel-cache`: existing LRU/stale/metrics tests unchanged + new
`eviction_order_matches_lru_across_many_ops` (exact victim sequence,
overwrite-no-evict, `order.len()==map.len()` invariant). Full workspace:
**166 green, 0 failed**, determinism/VSR corpus unaffected.

## Honest note

This restores throughput that a prior slice (SP50) had silently
regressed via a latent O(cap) path. Calling that out — found by
profiling, fixed with proof, no overclaim — is the point. The other
named perf levers (server-side group-commit default for EBS-class fsync,
range-index narrowing, NUMA/allocator) remain open follow-ups.
