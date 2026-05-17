# KesselDB Sub-project 48 — per-SSTable bloom filter

**Date:** 2026-05-17  **Status:** shipped, tested, benchmarked. 142 green.
**Honest result — no overclaim (see "What this is NOT").**

## What this is

The M1 perf log flagged, since the very beginning, that point reads are
O(#sstables) with a binary search + value-clone per table and **no bloom
filter** — the longest-standing documented read-path debt. SP48 adds a
zero-dep bloom filter, built once per SSTable from its keys (in `open`, so
flush/compact/reload all get it; on-disk format unchanged):

- FNV-1a 64 + double hashing, ~10 bits/key, k=7 (~1% false positive).
- `SsTable::get` does an O(1) `maybe_contains` bit-test reject *before*
  the binary search. **No false negatives**, so a real key is never
  skipped — tombstone/shadow correctness (newest-first caller) is
  preserved.

## What this is NOT (the honest part)

This does **not** make point reads O(1). `Storage::get` is a flat
newest-first scan over all SSTables, so an absent-key read still *visits*
every segment — it stays **O(#sstables)**. SP48 only shrinks the
*per-segment* cost: a ~28 ns bloom bit-test instead of a binary search
over the segment's sorted keys. The earlier instinct to call this an
"O(1) point read" was wrong and is not claimed (cf. the SP17/SP25
self-corrections — measure, then state only what the number supports).

True sub-linear point reads require **leveled/tiered compaction** so a key
maps to O(1) candidate segments. That is now the named next innovation
(SP49 candidate), with the bloom as its prerequisite building block.

## Measured (`kessel-bench bloomget`, release, MemVfs)

```
absent-key GET, 1  segment :   16,784,250 ops/s
absent-key GET, 64 segments:      553,202 ops/s
per-segment miss cost      :  ~28 ns   (bloom bit-tests, not a binary search)
```

Still O(#sstables) (64-seg ≈ 1/30 of 1-seg) — but each segment now costs
~28 ns to reject instead of a full binary search; the bloom is the
constant-factor win and the structural prerequisite for leveled lookups.

## Tests (2 new, 142 total)

- `bloom_has_no_false_negatives`: every inserted key tests positive (the
  one correctness invariant); false-positive rate stays <5% on a disjoint
  probe set.
- `point_get_correct_with_bloom_across_many_sstables`: 62 SSTables with a
  shadowed key and a tombstone — every `get` returns the exact correct
  value/None through the bloom path; absent keys correct. Plus the
  existing `property_vs_btreemap_oracle` / `lsm_get_spans_*` /
  `scan_prunes_*` are unchanged regression guards.

Zero functional change; determinism intact (bloom is derived from keys,
identical on every replica).
