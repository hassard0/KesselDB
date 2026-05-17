# KesselDB Sub-project 15 — Order-preserving range index

**Date:** 2026-05-17  **Status:** spec + build (autonomous continuation).
Squarely the goal: Postgres flexibility *and* TigerBeetle speed — turns O(n)
range scans into sub-linear ordered-index lookups.

## Goal

`Op::AddOrderedIndex { type_id, field_id }` builds an order-preserving index
(backfilling existing rows). `Op::FindRange { type_id, field_id, lo, hi }`
returns the 16-byte ids of rows with `lo <= field <= hi` by scanning only
the matching slice of the index keyspace — sub-linear, not a full table scan.

## Design

- **Order-preserving key.** `order_key(kind, raw)` → 8-byte big-endian:
  unsigned as-is (BE preserves numeric order); signed/Fixed with the sign
  bit flipped so lexicographic byte order == numeric order. Supported kinds:
  fixed-width ≤ 8 B numeric / Bool / Timestamp / Fixed. U128/I128/Char/Bytes/
  Ref are rejected by `AddOrderedIndex` (documented).
- **Keyspace** `0xFFFD0000 | (type & 0xFFFF)`, key id = `field_id(2) ++
  order_key(8) ++ pad(6)`, value = sorted set of 16-byte object ids. Because
  the LSM stores keys sorted, entries are physically ordered by `order_key`,
  so a range query is a contiguous `scan_range` slice.
- **Maintenance** in `idx_maintain` alongside equality indexes: on
  Create/Update/Delete the old order-key bucket loses the id and the new one
  gains it. (`ObjectType.ordered` in the replicated catalog.)
- Bug fixed in passing: the `need_idx` gate that decides whether to run
  index maintenance only checked equality `indexes`; it now also checks
  `ordered` (otherwise ordered-only types skipped Create/Update maintenance).

## Scope / non-goals (honest)

- Fields ≤ 8 bytes only (covers all common numeric/time/bool keys); wide
  (u128/i128) and byte-string range indexes are future work.
- Inclusive `[lo, hi]` only; open/half-open and `ORDER BY ... LIMIT` are
  future ergonomics.
- Read path is sub-linear in the matching set; write path is the same
  read-modify-write per-bucket as the equality index (documented perf item).

## Tests

`kessel-sm`: `range_index_signed_ordering_and_maintenance` (negative/positive
across the sign boundary, update moves a row between ranges, delete removes,
idempotent add, unsupported-kind rejected), `range_index_is_deterministic`
(400 random create/update/delete ops). `kessel-vsr`:
`ordered_index_replicates_and_converges`. `kessel-proto`: round-trips the new
ops. 102 tests total green.
