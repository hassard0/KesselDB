# KesselDB Sub-project 88 — large seed-corpus sweep (M3 hardening)

**Date:** 2026-05-18  **Status:** shipped (sweep). The remaining M3
"hardening backlog" had two items; this delivers one in full and
states the other precisely instead of faking it.

## Delivered: large randomized seed-corpus sweep

`kessel-vsr::large_seed_corpus_is_deterministic_and_converges` — pure
test addition over the existing seeded partition/fault model, at scale:

- **Determinism at scale:** seeds `0..120`, each run twice, asserted
  bit-for-bit identical (`live_digests`). Far wider than the focused
  `partition_corpus_is_deterministic` (`0..6`) — flushes out any
  hidden nondeterminism across the adversarial schedule space.
- **Post-heal convergence at scale:** seeds `0..40` (vs the focused
  `0..12`), each: run under partition (may stall), `heal()`, then it
  **must** finish and **all replicas must reconverge** (no divergence
  in `live_digests`).

No engine change; this exercises the proven fault/partition model
across a much larger corpus. Determinism / VSR partition corpus (incl.
seed 7) unchanged and now corroborated at breadth.

## Honest classification: disk-fault-*during-view-change*

Not faked. What already exists, precisely:

- **Storage-level torn-write / crash recovery** is tested (M1: WAL
  replay with torn-tail handling).
- **Partition + message-loss during/around view changes** and
  **post-heal reconvergence** are tested (the partition corpus,
  incl. seed 7, now at breadth via this sweep).

The uncovered sliver is *byte corruption injected precisely at the
view-change moment*. The VSR `Cluster` test harness builds each
replica on `StateMachine::open(MemVfs::new())`; `MemVfs` exposes **no
corruption seam** wired into the harness (the `MemDisk` fault hooks
exist but are not connected to the VSR cluster driver). A real test
needs a corruptible Vfs threaded into `Cluster` replicas plus a
trigger that flips corruption on at the view-change boundary and
asserts recovery / no-divergence — that is harness infrastructure, a
legitimately scoped follow-up, **not** a 10-line test. Implementing a
hollow version that doesn't actually inject mid-view-change would be
the dishonest path and is deliberately avoided.

`docs/STATUS.md` is updated accordingly: the seed-corpus-sweep item is
closed; disk-fault-during-view-change is restated as a precise,
narrowly-scoped remaining harness item with its adjacent coverage
named.
