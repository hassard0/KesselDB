# KesselDB Sub-project 90 — string `RANGE INDEX` wired into the SQL planner

**Date:** 2026-05-18  **Status:** shipped. Closes the
SQL-planner-range-narrowing half of the SP87 boundary: a CHAR/BYTES
`RANGE INDEX` now actually *accelerates* `SELECT … WHERE` instead of
only being correct via a full verified scan.

## What

- **kessel-sql** `try_query_rows`: a `Tok::Str` range-hint branch
  mirrors the existing numeric one — `<col> <cmp> '<str>'` on an
  `ordered` CHAR/BYTES column emits a planner `range_preds` entry
  (rop `>`=0 `>=`=1 `<`=2 `<=`=3) instead of being left to the
  generic `WHERE` program.
- **kessel-sm** SP70 narrowing loop: when the field is not numeric
  (`ord_field_pos`) but is variable-length ordered
  (`vord_field_pos`), it combines the range hints lexicographically
  into one tight `[lo, hi]` over the SP87 `0xFFFC` keyspace
  (`voidx_key`), scans that bucket for candidate object ids, then —
  the unchanged SP62/63/70 invariant — re-verifies every candidate
  against the compiled `WHERE`. `>`/`<` strictness and the
  fixed-width-padding CHAR semantics are decided entirely by that
  re-verification, so the index slice only ever *narrows* a correct
  superset.
- **SP87 cleanup completion:** `DropIndex` and `DropField` now also
  sweep the `0xFFFC` entries (they previously only swept the numeric
  `0xFFFD` ones) — a genuine SP87 correctness fix, independent of the
  planner work.

## Robustness fix (real bug, not faked)

A planner can legitimately narrow `WHERE s >= 'd' AND s <= 'b'` into
*inverted* index bounds (`lo > hi`). `Storage::scan_range` /
`scan_prefix` fed that to `BTreeMap::range(lo..=hi)`, which **panics**.
An inverted inclusive range contains nothing, so both now return
empty for `lo > hi`. This protects all ~30 `scan_range` callers, not
just SP90 — it was a pre-existing latent abort that SP90 made
reachable from SQL.

## Verified

`kessel-sql::string_range_planner_narrows_and_equals_scan`:

- a range-indexed `t` and an **identical unindexed twin `u`** get the
  same 140 rows;
- the planner is asserted to emit a range pred on the string column
  for `t` and a *pure Seq Scan* for `u` (so the check is
  index-vs-fullscan, not index-vs-index);
- across 30 deterministic random `[lo,hi]` ranges **plus** open
  bounds (`s > 'm'`, `s >= 'c'`, `s < 'e'`, `s <= 'bb'`, including
  inverted ones), the index-narrowed result is **byte-identical** to
  the same `WHERE` run as a full scan over `u`. This oracle makes
  **no assumption about CHAR comparison semantics** (fixed-width
  padded LHS vs raw literal RHS): whatever the engine's `WHERE`
  means, the index path must mean exactly the same — which is the
  real superset-verify invariant;
- `EXPLAIN` (via `compile_stmt`, the planner-only path) names the
  range accelerator.

Full workspace regression **195 green** (was 194 at SP89; +1 this
oracle), determinism corpus / seed-7 intact.

## Honest boundary

Delivered: the **SQL-planner range-narrowing** half for string
`RANGE INDEX`. *Not* in this slice: the SP73-style `MIN`/`MAX`
aggregate fast-path (`bound_in`) on string-ordered columns — it stays
numeric-only; string `MIN`/`MAX` remains correct via the verified
scan. That is its own concern (extend `bound_in` to the `0xFFFC`
keyspace) and is tracked, not faked. `U128`/`I128` ordered indexes
remain a separate follow-up.
