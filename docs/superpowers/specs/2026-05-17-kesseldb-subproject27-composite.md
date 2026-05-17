# KesselDB Sub-project 27 — Composite (multi-field) indexes

**Date:** 2026-05-17  **Status:** spec + build. Core Postgres feature; made
low-risk by the SP25 per-entry index design.

`ObjectType.composite: Vec<Vec<u16>>` (each = an ordered field-id list).
`Op::AddCompositeIndex { type_id, fields }` (validates, idempotent,
backfills). `Op::FindByComposite { type_id, fields, values }` returns object
ids whose member fields all equal the given values.

## Design

A composite index reuses the SP25 per-(value,object) machinery with a
synthetic field-id `0xC000 | index_no` and value = the member fields'
bytes **concatenated in declared order**. So `idx_add`/`idx_remove`/
`idx_lookup` are unchanged — composite is "just another value". O(1)
maintenance, prefix-scan lookup. Maintained in `idx_maintain` alongside
single-field equality/ordered; `need_idx` gate extended. Deterministic;
3-node VSR convergence tested.

## Non-goals (honest)

Equality on the full tuple only (no prefix/partial-tuple lookup, no
composite *range*). Members must be fixed-width non-overflow fields. These
are natural future extensions.

## Tests

`kessel-sm`: `composite_index_find_and_maintenance` (backfill, idempotent,
multi-value lookup, update moves tuple, delete drops),
`composite_index_is_deterministic`. `kessel-vsr`:
`composite_index_replicates_and_converges`. `kessel-proto` round-trips.
118 tests total green.
