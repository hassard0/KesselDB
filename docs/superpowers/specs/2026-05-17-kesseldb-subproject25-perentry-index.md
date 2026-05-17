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
quadratic under skew) with the correct one. The read regression is an
*implementation* artifact of `scan_range`, not the data model, and is a
clean, well-scoped follow-up:

> **Documented follow-up (next perf SP):** a lightweight prefix-key iterator
> (`Storage::scan_prefix`) that streams keys in `[lo,hi]` without building a
> `BTreeMap` or cloning values, restoring O(matching) point/index reads while
> keeping the O(1) write win.

115 tests green; eq-index writes ~2.5× faster; read-path optimization
explicitly queued.
