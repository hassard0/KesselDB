# KesselDB Sub-project 5 — Query planner (filtered scan + multi-index AND)

**Date:** 2026-05-17  **Status:** spec + build (autonomous continuation)
**Builds on:** SP1–SP4. Completes the North Star access-path tier
"filtered scans → multi-index intersection" (equality indexes were SP3).

## Goal

`Op::Query { type_id, preds }` — return the 16-byte object ids of rows
matching the conjunction (AND) of predicates. Each `Pred { field_id, op,
value }` with `op` ∈ { Eq(0), Ge(1), Le(2) }.

## Design

- **Per-kind comparison (`cmp_field`):** width-normalize both operands, then
  compare numerically for integer/bool/timestamp kinds (LE→u128 / sign-
  extended i128 / scaled), lexicographically for Char/Bytes/Ref. This makes
  range predicates correct for little-endian integer fields (naïve byte
  compare would be wrong).
- **Planner:**
  1. For every **indexed equality** predicate, fetch its object-id set from
     the SP3 index and **intersect** them (multi-index AND).
  2. If ≥1 such predicate: read only those candidate rows and verify ALL
     predicates (so non-indexed and range predicates are applied as a cheap
     post-filter, and digest-collision safety is preserved).
  3. Otherwise: **filtered scan** of the type's contiguous key range via
     `Storage::scan_range`, applying all predicates per row.
- **Read-only & deterministic:** `Query` never mutates state (verified by a
  digest-unchanged test) and is a pure function of committed state, so it is
  identical on every replica without needing to be in the VSR log.
- Result ids are sorted for deterministic output.

## Scope / non-goals (honest)

- Conjunctive (AND) only; OR / NOT / arbitrary boolean trees are future work.
- No range *index* — range predicates are evaluated by scan/post-filter
  (the SP3 index is hash-bucketed, not order-preserving). An order-preserving
  index for sub-linear range is a later spec.
- No cost-based ordering of index intersections (intersect-all is correct;
  picking the most selective first is a future optimization).
- Equality fast-path only for fields that actually have an SP3 index.

## Tests

`query_multi_index_intersection` (two indexed fields AND'd),
`query_range_filtered_scan_no_index` (numeric LE range via scan),
`query_indexed_eq_plus_unindexed_range` (index fast-path + post-filter +
empty result), `query_is_readonly_and_deterministic` (digest unchanged +
stable output). 64 tests total green.
