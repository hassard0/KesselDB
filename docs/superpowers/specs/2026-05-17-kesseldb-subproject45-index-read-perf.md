# KesselDB Sub-project 45 — index point-read perf (close the SP25 tradeoff)

**Date:** 2026-05-17  **Status:** shipped, tested. 139 green.

## Context (honest)

SP25 moved the equality index to a per-`(value,object)` LSM key for O(1)
scalable writes; the documented cost was that a point-value read became a
prefix scan. Two of the three layers were already optimal *before* this
slice: the memtable path uses `BTreeMap::range` (O(log n + m)) and each
SSTable is entered with `partition_point` (binary search). The remaining
real cost in a large LSM: `scan_prefix`/`scan_range` still **touched every
SSTable** (a `partition_point` + bookkeeping per table) even when a
table's key span could not possibly intersect the query.

## The fix

`SsTable::overlaps(lo, hi)` — O(1): `entries` is sorted, so min/max are
the first/last keys; skip the table unless `min <= hi && max >= lo`.
Applied in both `scan_prefix` and `scan_range`. A selective point-value
read over an *S*-segment LSM goes from O(*S* · log n) to
O(*S_overlapping* · log n) — for a point lookup that is typically 1–2
tables instead of all *S*. Empty tables are skipped too.

## Test (1 new, 139 total)

`scan_prunes_disjoint_sstables_without_changing_results`: 40 single-key
SSTables (a deliberately many-segment LSM); a single-key point
`scan_prefix`/`scan_range` returns *exactly* that key (39/40 tables pruned
in O(1) each); an absent key → clean empty; a full `[min,max]` scan still
returns the entire set — pruning skips disjoint tables but never drops a
key in range. Correctness is identical with the optimization; only work
done is reduced.

## Honest result

This closes the SP25 point-read concern: equality point-value reads are
now sub-linear and prune non-overlapping segments, with **zero** change to
write scalability (the per-`(value,object)` key design is untouched) and
zero correctness change (proven by the oracle test + the full suite). Not
overclaimed: this is a segment-pruning win on top of the already
binary-searched scan, not a new index structure.
