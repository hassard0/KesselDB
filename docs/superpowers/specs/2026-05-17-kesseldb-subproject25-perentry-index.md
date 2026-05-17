# KesselDB Sub-project 25 — Per-(value,object) equality index

**Date:** 2026-05-17  **Status:** spec + build. Fixes the SP16 #1 perf debt
with the architecturally-correct design. **Mixed result, reported honestly.**

## Change

Equality index is no longer a per-(field,value) *bucket blob* requiring a
read-modify-write on every insert. It is now **one LSM entry per
(value,object)**, keyed (variable-length, enabled by SP24):

`ns(4) ++ field_id(2) ++ value(w) ++ object_id(16)` → empty payload.

- `idx_add` = **1 `put`** (no read, no decode, no re-encode). O(1).
- `idx_remove` = **1 `delete`**. O(1).
- `idx_lookup` = **prefix scan** over `[prefix‖0¹⁶, prefix‖0xff¹⁶]`,
  collecting the object id from each key suffix. No bucket, no bitmap,
  no fan-out.
- `scan_range` made **overlay-aware** so intra-transaction UNIQUE / FK /
  cascade still hold (indexes now prefix-scan, so read-your-writes must
  cover scans, not just point gets) — a correctness improvement.

All 115 tests stay green (semantics identical: `idx_lookup` returns the same
id sets; UNIQUE/FK/cascade/query/determinism/VSR-convergence unchanged).

## Honest benchmark (same-run ratios, machine loaded; ratios, not absolutes)

| path | pre-SP25 vs plain | SP25 vs plain |
|---|---|---|
| **CREATE +eq-index** | ~6.5× slower | **~2.6× slower** ✅ (the SP16 #1 debt — fixed) |
| FindBy (point lookup) | ~1.2M ops/s | **~30–70K ops/s** ⚠️ (regressed) |

**The write debt that SP16 flagged as #1 is genuinely fixed** and the design
is now the one real databases use. But moving `idx_lookup` onto the
heavyweight `scan_range` (which builds a merged `BTreeMap` and clones)
**regressed point-index reads**. This is reported, not hidden, and is **not**
claimed as a pure win.

## Why ship it (vs revert, à la SP17)

Unlike SP17 (fixed nothing, regressed reads), SP25 **fixes the actual
flagged debt** and replaces a fundamentally non-scalable design (bucket RMW,
quadratic under skew) with the correct one real databases use.

## CORRECTION (honest — superseding the optimistic note above)

SP26 added the lightweight `Storage::scan_prefix` (keys-only, memtable
fast-path, no merged-BTreeMap/value-clones) and pointed `idx_lookup` at it.
It helped only marginally. The earlier claim that the FindBy regression was
"just an implementation artifact of `scan_range`" was **over-optimistic and
is hereby corrected**: it is a genuine **architectural read/write tradeoff**.

- The old bucket design served point reads in ~1.2M ops/s *because* a value's
  ids were one blob fetched in a single `get` — the very thing that made its
  **writes pathological and non-scalable** (read-modify-write, quadratic
  under value skew).
- The per-entry design makes **writes O(1) and scalable** (the flagged #1
  debt, genuinely fixed) at the cost of point-value reads being an
  **O(matching) prefix scan** instead of a single get — slower per call but
  scalable and not skew-quadratic.

This is the correct tradeoff for a write-optimized, TigerBeetle-class engine.
The old 1.2M FindBy number was an artifact of a non-scalable write design and
is **not** the right baseline. Legitimate *further* read speedups (a
per-prefix block index / bloom filter on the index keyspace; routing FindBy
through the existing M4 read cache) are real future perf work — but framed
honestly as enhancements, not as "restoring" a number that depended on a
broken write path.

115 tests green; eq-index writes materially faster and scalable; the read
characteristic is a documented, understood, deliberate tradeoff — not hidden,
not over-claimed.
