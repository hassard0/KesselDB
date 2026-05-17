# KesselDB Sub-project 20 — Aggregates (COUNT/SUM/MIN/MAX)

**Date:** 2026-05-17  **Status:** spec + build. Core Postgres capability.

## Goal

`Op::Aggregate { type_id, program, kind, field_id }` over rows where the
kessel-expr `program` is true: `kind` 0=COUNT, 1=SUM, 2=MIN, 3=MAX of a
numeric field. Result returned as a 16-byte little-endian `i128`.

## Design

Filtered `scan_range` + accumulator. COUNT needs no field; SUM/MIN/MAX
require a numeric ≤8B field (reuses `ord_field_pos`; signed sign-extended to
i128, SUM wrapping). Empty match ⇒ 0. Read-only, deterministic,
txn-allowed, non-breaking.

## Scope / non-goals (honest)

- Numeric ≤8B fields for SUM/MIN/MAX (u128/i128 unsupported, documented).
- No GROUP BY / AVG / DISTINCT yet (AVG = SUM/COUNT client-side; GROUP BY is
  future). Full scan O(n) — index-accelerated aggregates are a future opt.

## Tests

`kessel-sm`: `aggregate_count_sum_min_max` (all four over filtered/all sets,
empty), `aggregate_is_readonly_and_deterministic`. `kessel-proto`:
round-trips. 109 tests total green.
