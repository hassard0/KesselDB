# KesselDB Sub-project 70 — range-index narrowing

**Date:** 2026-05-17  **Status:** shipped, oracle-proven, measured on
vulcan. 169 green. This is the last open *performance* item — the
deliberately-deferred "careful Op-shape" slice.

## What it does

Before SP70 a `SELECT * … WHERE v > X` (or a `BETWEEN` band) on an
order-indexed column still did a **full scan** + program verify: only
equality (SP62) and composite-equality (SP63) narrowed. SP70 makes the
planner emit half-range hints on order-indexed columns and the engine
narrow candidates through the existing order index (SP15/25).

## Wire change — backward-compatible by construction

`Op::QueryRows` gained `range_preds: Vec<(u16, u8, Vec<u8>)>`
(`op` 0=`>` 1=`>=` 2=`<` 3=`<=`), encoded **after** `limit` and only
when non-empty. An older frame (no range hints) is a valid prefix that
decodes via `Cursor::remaining()==0` to an empty list and behaves
**exactly** as before. So the replicated op shape evolved without
breaking replay or determinism — the concern that made prior sessions
defer this is handled, not hand-waved.

## Correctness — the SP62/63 invariant, unchanged

The planner only emits a range hint when it is a *mandatory conjunct*
(same `no top-level OR/NOT/(` gate as eq hints). The engine takes the
order-index slice **inclusively** (`>`/`<` strictness is left to
`program`), so the slice is always a **superset** of the predicate's
matches; intersecting it into the candidate set can only narrow, and the
full compiled WHERE still verifies every candidate. Result is therefore
identical to a scan regardless of the candidate set — proven, not
asserted: `planner_equivalence_oracle` now also builds a RANGE index on
`v` and adds pure-range and band queries (~660 randomized queries:
eq / composite / range / band / OR / NOT) and checks the planned result
equals an independent brute-force filter, every time.

## The band optimisation that actually mattered

First cut scanned each half-range separately and intersected — a band
`v >= a AND v <= b` became two *huge* half-open slices (e.g.
`[a, +∞)` ≈ 90% of the table) intersected: only ~2×. Fixed by combining
all hints on one field into a single tight interval `[max lower, min
upper]` scanned once. order_key is monotone so byte-wise `max`/`min` of
the 8-byte keys is numerically correct.

## Measured (`range_index_is_sublinear_and_correct`)

40 000 rows, narrow band (~0.2% of domain, 81 rows matched), identical
result asserted vs the full scan:

| band query | full scan | range-index | speed-up |
|---|---|---|---|
| dev box (Windows) | 54,186 µs | 251 µs | **~216×** |
| **vulcan (Linux)** | 35,007 µs | 313 µs | **~112×** |

## Honest boundaries

- Sub-linear needs the column to actually have a `RANGE INDEX` and the
  hint to be a mandatory conjunct; otherwise it cleanly falls back to the
  verified full scan (still correct, just not faster) — same contract as
  SP62/63.
- A lone unselective half-range (`v > tiny`) still scans most of the
  index — inherent to a half-range; bands and selective bounds are where
  the win is, and that is what the test measures.
- No new config knob; on by default; existing `sql()`/`call()` and the
  determinism/VSR partition corpus (incl. seed 7) unchanged.
