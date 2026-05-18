# KesselDB Sub-project 93 — `MIN`/`MAX` over the `0xFFFC` keyspace

**Date:** 2026-05-18  **Status:** shipped. Closes the last
documented "scan-correct only" boundary from the SP87→SP91 arc:
`MIN`/`MAX` now works over CHAR/BYTES **and** U128/I128 columns,
accelerated by the SP87/SP91 `0xFFFC` ordered index.

## Why it needed a real slice (not just a fast-path)

`Op::Aggregate` did not merely run `MIN`/`MAX`-over-strings
unaccelerated — it **rejected** any non-numeric-≤8B field outright
(`"Aggregate field must be numeric ≤8B"`) and folded every result
into an `i128`. So enabling it meant generalising the aggregate
result/compare path, not adding a `bound_in` shortcut to an existing
working path.

## What

A self-contained early-return block in `Op::Aggregate`, taken only
for `MIN`/`MAX` (kinds 2/3) when `ord_field_pos` is `None` (i.e. the
field is *not* the legacy numeric ≤8B kind):

- field resolved via `vord_field_pos` (CHAR/BYTES/U128/I128);
- **fast path** — no `WHERE` + an ordered index → new
  `agg_extreme_var` reads the extreme of the `0xFFFC` bucket range
  with the existing early-stopping `Storage::bound_in` (the order
  key is `vorder_key`, order-preserving, so first/last bucket is the
  global MIN/MAX);
- **slow path** (the oracle) — filtered or unindexed: full scan,
  track the extreme raw bytes via `cmp_field` (kind-correct:
  lexicographic for byte kinds, unsigned/signed for U128/I128);
- result = the extreme row's **raw width-`w` field bytes** (U128/I128
  = 16 LE ⇒ fits the existing 16-byte scalar contract the client
  already decodes; CHAR/BYTES = `w` bytes; empty input = `Got([])`).

**Zero regression:** the numeric ≤8B path is byte-for-byte unchanged
— the new block returns early *only* when `ord_field_pos` is `None`,
which is exactly the set of fields the old code rejected. The
generic client's scalar pretty-printer was already length-guarded
(`if b.len() == 16`), so a CHAR `MIN` (≠16 bytes) falls through to
its generic branch — no panic, no client change.

## Honest boundary

`SUM`/`AVG` over CHAR/BYTES or U128/I128 remain a **deliberate
non-goal** and still return a `SchemaError` (summing strings is
meaningless; wide-integer `SUM` overflow semantics are out of scope
here). Only `MIN`/`MAX`/`COUNT` are meaningful for these kinds and
all three now work.

## Verified

- **kessel-sm `agg_minmax_over_0xfffc_equals_bruteforce`**: for
  CHAR, U128 (spanning past `i128::MAX`) and I128 (negatives),
  `MIN`/`MAX` == an independent brute-force model — **fast path**
  (indexed, no filter), **slow path** (an unindexed I128 column;
  and a filtered predicate), and the **empty** case — plus
  `SUM`-over-CHAR is asserted to stay a `SchemaError`, and the whole
  build is `digest()`-deterministic run-to-run.
- **kessel-sql `sql_min_max_over_string_and_u128`**: `SELECT
  MIN(s)/MAX(s)` (CHAR) and `MIN(u)/MAX(u)` (U128) compile and
  return the brute-force extreme — these were a hard error before.

Full workspace regression **200 green** (198 → +2: the engine and
SQL oracles), numeric ≤8B aggregate tests unchanged, determinism
corpus / seed-7 intact.
