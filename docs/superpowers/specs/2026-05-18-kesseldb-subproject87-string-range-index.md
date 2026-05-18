# KesselDB Sub-project 87 — wide / byte-string range indexes

**Date:** 2026-05-18  **Status:** shipped. Closes the last genuine
deferred *feature* ("wide/byte-string range indexes").

## Design — isolated dual path (zero risk to the numeric path)

The numeric ≤8B ordered index (tag `0xFFFD`, fixed 8-byte
sign-flipped big-endian key, 20-byte LSM key with a 6-byte pad slot)
is **left completely untouched** — byte-identical on disk, same
digest, same code. `CHAR(n)`/`BYTES(n)` get a **separate keyspace**
(tag `0xFFFC`):

- `vord_field_pos` — accepts only `Char`/`Bytes` (the numeric
  `ord_field_pos` still rejects them, so SP70 planner narrowing and the
  SP73 `MIN`/`MAX` fast-path keep ignoring string ordered fields and
  fall back to the verified scan — correct, just not accelerated:
  a documented boundary, not a regression).
- Order key = the raw width-`w` bytes as stored (CHAR/BYTES are
  fixed-width, zero-padded, memcmp-ordered) — no transform needed.
- `voidx_key = tag(4) ++ field_id(2) ++ ok(w)`; bucket value is the
  sorted set of 16-byte object ids (same shape as `oidx`). Because
  `ok` is a constant width per (type,field) and the keyspace tag is
  distinct, the inclusive `[lo,hi]` scan needs no padding slot.
- `voidx_add`/`voidx_remove` mirror `oidx_*`.

Wired into: `idx_maintain` (a second ordered loop for CHAR/BYTES —
the numeric loop already skipped them), `Op::AddOrderedIndex`
(accepts CHAR/BYTES, picks the path by kind, backfills the right
keyspace), and `Op::FindRange` (branches: numeric → existing path;
CHAR/BYTES → `voidx` bucket scan over the lexicographic range).
SQL `CREATE RANGE INDEX ON t (c)` for a string column now works
(it lowers to `AddOrderedIndex`, no longer rejected).

## Verified

`kessel-sm::string_range_index_equals_brute_force_and_is_maintained`:
a `CHAR(8)` `RANGE INDEX`, ~40 randomized `[lo,hi]` ranges where
`FindRange` must exactly equal an independent brute-force
lexicographic filter; correctness preserved under `UPDATE` (row moves
to its new value) and `DELETE` (row leaves the index); deterministic
(two builds ⇒ identical digest). Full workspace regression green;
the numeric ordered-index tests and the determinism / VSR partition
corpus (incl. seed 7) are unchanged — the numeric path was not
touched.

## Honest boundary

- String range indexes accelerate `Op::FindRange` (and keep the index
  maintained on writes). The **SQL planner's range-narrowing** (SP70)
  and the **`MIN`/`MAX` fast-path** (SP73) remain numeric-only;
  `BETWEEN`/`<`/`>` on a string column is still *correct* (verified
  full scan) but not index-accelerated through the planner — a
  documented follow-up, not a wrong answer.
- Scope is `CHAR(n)`/`BYTES(n)`. `U128`/`I128` ordered indexes remain
  out of scope (a separate niche; the documented item was
  string/byte-string).
