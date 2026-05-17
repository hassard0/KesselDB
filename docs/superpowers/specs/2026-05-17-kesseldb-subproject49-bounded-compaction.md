# KesselDB Sub-project 49 — bounded-segment compaction (sub-linear reads)

**Date:** 2026-05-17  **Status:** shipped, tested. 143 green. This is the
piece SP48 named: it makes product point reads genuinely sub-linear.

## The problem SP48 left open

SP48's bloom shrank the *per-segment* miss cost to ~28 ns, but
`Storage::get` still visits every segment, so reads stayed O(#sstables) —
and #sstables grows with every flush. The honest next step (named in the
SP48 spec) was to bound the segment count.

## The innovation

`Storage::set_compact_threshold(k)`: when `flush` produces the k-th live
segment it auto-`compact`s back to one. Point-read fan-out is then **≤ k
regardless of total data** — combined with the SP48 bloom (~28 ns/segment)
that is a bounded, data-size-independent point read: **O(1) in the data,
O(k) constant**.

- **Deterministic.** Compaction fires purely off the op/flush stream, so
  every replica compacts at the same points and ends in identical state.
  Compaction preserves live keys and drops shadowed/tombstoned entries
  (already covered by `compaction_drops_tombstones_and_shadowed`), so the
  replicated **digest is unchanged** — verified by the full determinism /
  VSR-convergence corpus still passing (143 green, 0 failed).
- **Opt-in, zero behavioural change to the primitive.** Default is `0`
  (off); raw `Storage` and every existing storage test are untouched. The
  product enables it: `StateMachine::open` sets threshold **8** (reads
  ≤ 8 bloom-probed segments while amortising write/compaction cost).

## Tests (1 new, 143 total)

`bounded_compaction_caps_segments_and_stays_correct`: 30 flushes with
threshold 4 — `sstable_count()` asserted ≤ 4 after *every* flush (would be
30 unbounded); then a shadow + a tombstone; every live key reads back
correct, the tombstoned key is `None`, the shadowed key is the newest
value. The whole SM/VSR/cluster suite (now running with the SM's
threshold-8 auto-compaction) stays green — the strongest evidence
determinism and recovery are unaffected.

## Honest framing

This is the real sub-linear-read result, stated precisely: reads are
O(k=8) bloom-probed segments, *independent of total data size* — not a
literal single-seek O(1), and write cost now includes amortised
compaction (the classic LSM read/write trade, chosen deliberately and
bounded). No overclaim: the number that matters — segment fan-out — is
asserted to stay capped, and correctness/determinism are proven by the
full suite, not assumed.
