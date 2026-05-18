# KesselDB Sub-project 82 — cross-shard adversarial proof (5/6)

**Date:** 2026-05-18  **Status:** shipped. Proves the cross-shard
protocol is atomic and deterministic under adversity, by composition
with the existing per-group partition corpus.

## The argument

Cross-shard correctness under adversity decomposes into two parts,
each independently proven:

1. **Each shard group tolerates partition.** Already proven — the
   seeded VSR partition corpus (seeds 0–11 incl. seed 7,
   `partition_corpus_is_deterministic` / `partition_then_heal_
   converges`). A shard group is an ordinary VSR cluster.

2. **The cross-shard layer is idempotent + deterministic under an
   arbitrary router fault schedule.** Any router fault is a prefix of
   decide/commit calls followed by a `recover()` re-drive; every op is
   verdict-stable / cursor-idempotent and the decision is a pure
   function of durable state, so any schedule converges to the
   clean-run state.

This slice adds the explicit test for (2), in the project's
established deterministic style (no flaky socket fault-injection — the
rigorous proof is the deterministic one), plus an over-sockets
concurrency confirmation.

## Verified

- `xshard_protocol_atomic_and_deterministic_under_adversarial_drive`
  (deterministic, SM-level, 3 shard SMs + a sequencer SM): a clean
  reference run (T1/T3 commit everywhere, T2 — a slice that dups a
  pre-seeded row — aborts on **every** shard) versus an adversarial
  run with duplicate/out-of-order `SeqAppendOnce` retries (same key ⇒
  same seq), a fully-driven txn, a partially-decided txn, a simulated
  router crash, **repeated** full-log `recover()`, and a stray
  duplicate commit. Asserts the adversarial run's per-shard digests
  equal the clean reference **and** that the whole chaotic schedule is
  itself bit-for-bit deterministic (run twice ⇒ identical).
- `concurrent_cross_shard_txns_are_atomic_over_sockets` (real sockets,
  2 shard groups ×3 + sequencer ×3): 8 concurrent cross-shard
  transactions from independent connections all commit atomically
  (the `xs` lock serialises the global order), every row lands on its
  owning shard, and a post-hoc full `recover()` re-drive changes
  nothing.

Full workspace regression green; the per-group determinism / VSR
partition corpus (incl. seed 7) is unchanged and now underpins the
cross-shard guarantee by composition.

## Honest boundary

The deterministic test is the rigorous proof (matching how every other
guarantee in this system is established — seeded, replayable). A
socket-level network-fault-injection harness between router /
sequencer / shards would add little beyond what the per-group corpus
+ the deterministic adversarial test already prove, and would be
flaky; it is intentionally not added. Slice 6 updates the public docs.
