# KesselDB Sub-project 91 — `U128` / `I128` ordered (range) indexes

**Date:** 2026-05-18  **Status:** shipped. Closes the
"`U128`/`I128` ordered indexes" out-of-scope item: a `RANGE INDEX`
on a 16-byte integer column now works (engine + SQL planner), with
correct signed ordering.

## Why it needed a slice

The numeric ordered path (`0xFFFD`) packs an 8-byte sign-flipped
big-endian order key — `U128`/`I128` are 16 bytes and don't fit.
`ord_field_pos` therefore returned `None` for them and
`AddOrderedIndex` rejected the column. The SP87 `0xFFFC`
variable-length keyspace already carries arbitrary-width memcmp
order keys for CHAR/BYTES; SP91 reuses it for 16-byte integers via
an order-preserving transform.

## What

- **`vorder_key(kind, raw, w)`** — the order-preserving key for the
  `0xFFFC` keyspace:
  - **CHAR/BYTES:** the raw width-`w` bytes, *unchanged*. Every
    pre-SP91 string index is therefore byte-identical — zero
    migration / digest risk.
  - **U128:** 16-byte big-endian (memcmp == numeric).
  - **I128:** 16-byte big-endian with the sign bit flipped, so
    negatives sort below positives (short negative bounds are
    sign-extended, mirroring the codec load path).
- `vord_field_pos` now also accepts `U128 | I128`.
- Every `0xFFFC` producer/consumer routes through `vorder_key`:
  `idx_maintain` (insert/remove), `AddOrderedIndex` backfill,
  `Op::FindRange`, and the SP70 SQL-planner range-narrowing. The
  numeric `0xFFFD` path is **untouched** (byte-identical, zero
  digest risk).
- The SQL surface needed no change: `CREATE RANGE INDEX` has no kind
  gate, and the SP70 integer range-hint already emits 16 LE value
  bytes for any ordered column — with the SM transform in place,
  `WHERE v BETWEEN …` on a `U128`/`I128` index narrows automatically.

## Verified

- **kessel-sm `u128_i128_range_index_equals_brute_force_and_is_maintained`**:
  `Op::FindRange` == an independent brute-force numeric filter for
  **U128** (whole-range values) **and I128 including negatives**
  (a window straddling zero returns both signs; full `[i128::MIN,
  i128::MAX]` returns every row); correct under UPDATE/DELETE
  maintenance; `digest()`-deterministic across rebuilds.
- **kessel-sql `u128_i128_range_planner_narrows_and_equals_scan`**:
  the planner emits a range pred on the 16-byte column, and
  `SELECT … WHERE v >= a AND v <= b` is **byte-identical** to the
  same `WHERE` over an unindexed twin table — U128, I128, and a
  zero-straddling I128 window — across 30 random ranges.

Full workspace regression **197 green** (195 → +1 engine oracle, +1
SQL twin), determinism corpus / seed-7 intact, CHAR/BYTES indexes
byte-identical.

## Honest boundary

Delivered: `U128`/`I128` `RANGE INDEX` — `AddOrderedIndex`,
`FindRange`, maintenance, and SQL-planner range-narrowing. *Not* in
this slice: the SP73-style `bound_in` `MIN`/`MAX` aggregate
fast-path on `0xFFFC` columns (string **and** U128/I128) — those
`MIN`/`MAX` remain correct via the verified scan, just not
index-accelerated. Tracked as a single follow-up (extend `bound_in`
to the `0xFFFC` keyspace), not faked.
