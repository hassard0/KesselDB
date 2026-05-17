# KesselDB Sub-project 21 — Projection (SelectFields)

**Date:** 2026-05-17  **Status:** spec + build. Core Postgres ergonomic.

`Op::SelectFields { type_id, program, fields, limit }` — like `Select` but
each returned row is only the concatenated bytes of the requested `fields`
(in order), not the whole record. Result = `[u32 rowlen][row]*`.

## Design

Filtered `scan_range` (VM filter, SP7); per matching row, copy each
projected field's bytes by its layout offset/width; emit length-prefixed up
to `limit`. Unknown field → `SchemaError`. Read-only, deterministic,
txn-allowed, non-breaking. No catalog change.

## Non-goals (honest)

Whole-field projection only (no expressions/computed columns in the
projection list), no ORDER BY/OFFSET. Full scan O(n).

## Tests

`kessel-sm`: `select_fields_projects_chosen_columns` (multi-field project,
row width, values, unknown-field rejected),
`select_fields_is_readonly_and_deterministic`. `kessel-proto` round-trips.
111 tests total green.
