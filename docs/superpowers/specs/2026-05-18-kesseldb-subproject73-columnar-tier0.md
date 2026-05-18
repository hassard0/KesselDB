# KesselDB Sub-project 73 — columnar aggregate fast-path (Tier 0)

**Date:** 2026-05-18  **Status:** shipped, oracle-proven, measured.
174 green. The low-risk, high-value slice of "columnar behaviour as an
option": automatic, transparent, never a knob, never a different answer.

## What was already columnar

Both `Op::Aggregate` and `Op::GroupAggregate` already read **only** the
target/group column by fixed offset (`ord_field_pos` + a width slice) —
they never `decode` the whole record. So column projection was not the
gap. The real costs were: (1) the per-row WHERE expr-VM runs even when
there is no filter, and (2) `MIN`/`MAX` still scans every row.

## Two accelerators (both pure, both oracle-guarded)

1. **No-filter VM skip.** When `program` is the planner's canonical
   always-true constant (`Program::new().push_int(1).bytes()` — emitted
   for a `WHERE`-less aggregate), the per-row expr-VM is pure overhead;
   skip it and fold only the column. Applies to scalar `Aggregate` and
   `GroupAggregate`. A non-canonical-but-true program simply doesn't
   match and takes the normal path — correct, just not accelerated.

2. **`MIN`/`MAX` from the index extreme (scan elimination).** For an
   unfiltered `MIN`/`MAX` on a column with a `RANGE INDEX`, the answer is
   the smallest/largest order-index key. New
   `Storage::bound_in(lo, hi, want_max)` returns the single
   smallest/largest **live** entry in a key range *without materialising
   the range*: each SSTable is binary-searched for its boundary
   candidate, the memtable/overlay use ordered cursors, and the global
   candidate is resolved with a tombstone-aware point `get`, stepping
   past a (rare) tombstoned boundary with a hard iteration cap that
   falls back to `None` (caller full-scans) rather than ever loop.
   `agg_extreme` uses it to fetch one row under the extreme entry and
   read the column. Sub-linear; correctness unchanged.

The first cut used the existing `scan_range` over the order index — that
materialises every entry (O(n)) and gave **no** speed-up (~1×).
`bound_in` is the difference between ~1× and ~4,600×; the speed only
appeared once the whole range stopped being materialised.

## Correctness — the same invariant as the other accelerators

`aggregate_columnar_fastpath_equals_scan_oracle`: randomized data + a
`RANGE INDEX`, every kind (COUNT/SUM/MIN/MAX/AVG), with no filter (fast
path incl. index extreme) and with a filter (slow path), including the
empty case — the result must exactly equal an independent brute-force
model. So the fast path provably never changes the answer; the scan is
the oracle. Determinism / VSR partition corpus (incl. seed 7) unchanged
(read-op only, not in the state digest).

## Measured

`min_max_via_index_skips_the_scan` — 40 000 rows, `MIN`/`MAX` of a
range-indexed column, value asserted identical to the forced full scan:

| `MIN` over 40 K rows | full scan | index extreme | speed-up |
|---|---|---|---|
| Linux reference server | ~22,997 µs | **~5 µs** | **~4,600×** |
| reference laptop | ~24,723 µs | ~14 µs | ~1,766× |

Absolute µs tracks single-core speed; the *shape* (scan eliminated,
table-size-independent) is platform-independent.

## Honest boundaries

- Accelerated only for an **unfiltered** `MIN`/`MAX` on a column that
  actually has a `RANGE INDEX`; anything else takes the (correct) scan.
- A filtered aggregate still scans + verifies (the WHERE can reference
  any column) — the only saving there is skipping the VM when there is
  no WHERE.
- This is Tier 0: a column-aware fast-path on the row store, not a
  columnar storage format. A real per-table columnar segment (RLE/zone
  maps for filtered scans) remains a separate, larger, opt-in follow-up
  — deliberately not attempted here.
- No new config knob; on by default; strictly an accelerator.

Docs: `README` performance section genericised (no host names / internal
slice labels), new `docs/PERFORMANCE.md` with the scaling model and
order-of-magnitude cloud projections (clearly marked projected, not
measured), `docs/STATUS.md` updated and scrubbed of internal server
names.
