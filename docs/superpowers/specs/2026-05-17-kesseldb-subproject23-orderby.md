# KesselDB Sub-project 23 — ORDER BY + OFFSET/LIMIT

**Date:** 2026-05-17  **Status:** spec + build. Core Postgres ergonomic.

`Op::SelectSorted { type_id, program, sort_field, desc, offset, limit }` —
rows matching the VM `program`, ordered by `sort_field` (per-kind compare,
ties broken by object id → total deterministic order), `desc` to reverse,
then `offset` skipped and ≤ `limit` returned (0 = unlimited). Result =
`[u32 rowlen][record]*`.

## Design

Filtered `scan_range` → buffer `(sortkey, objid, rec)` → `sort_by`
`cmp_field` (reused from SP5) then objid tiebreak → optional reverse →
skip/take. Read-only, deterministic, txn-allowed, no catalog change.

## Non-goals (honest)

Single sort key; in-memory sort of the matched set (O(m log m), m =
matches) — index-ordered streaming sort is a future opt. No multi-key
ORDER BY / NULLS FIRST.

## Tests

`kessel-sm`: `select_sorted_orders_and_paginates` (asc full, desc +
offset+limit window), `select_sorted_is_deterministic`. `kessel-proto`
round-trips. 115 tests total green.
