# KesselDB Sub-project 18 — Select (filtered rows + LIMIT)

**Date:** 2026-05-17  **Status:** spec + build. Serves the goal: usable
Postgres-like row queries end-to-end.

## Goal

`Op::Select { type_id, program, limit }` — return up to `limit` whole rows
(not just ids) for which the kessel-expr boolean `program` is true.
`limit == 0` means unlimited. The first query op that returns *records*,
making the TCP server a usable query endpoint.

## Design

Filtered `scan_range` over the type's contiguous key range; per row, eval
the deterministic VM filter (reused from SP7/SP14); emit matching records as
`[u32 len][record bytes]*` until `limit` is reached. Key-ordered ⇒
deterministic output. Read-only, txn-allowed, non-breaking.

## Scope / non-goals (honest)

- Full scan + per-row VM eval (O(n)); index-accelerated Select is a future
  optimization (the SP3/SP15 indexes + a planner). `Select` is the
  expressive/usable path, not the fast path for huge tables.
- No projection (returns whole records), no ORDER BY beyond physical key
  order, no OFFSET — future ergonomics.

## Tests

`kessel-sm`: `select_returns_filtered_rows_with_limit` (filter, LIMIT cap,
empty), `select_is_readonly_and_deterministic` (digest unchanged + stable).
`kesseldb-server`: end-to-end `Select` over real TCP returns row blobs.
`kessel-proto`: round-trips. 104 tests total green.
