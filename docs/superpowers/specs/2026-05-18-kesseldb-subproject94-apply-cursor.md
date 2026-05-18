# KesselDB Sub-project 94 — crash-recovery apply-cursor + replay guard

**Date:** 2026-05-18  **Status:** shipped. The engine plumbing
SP92 named as the blocker for a faithful multi-node
disk-fault-*during-view-change* harness: a state machine reopened
from disk must be able to be re-fed its already-durable committed
prefix by the VSR primary **without double-applying**.

## The problem (named in SP92)

`StateMachine::apply` had no op-number replay guard. The cluster
sim never reopened a `StateMachine` from disk; a recovered replica
(SM at durable op K, VSR log empty) re-fed the committed log from 0
by the primary would re-run every op — and a non-idempotent op like
`SeqAppend` (counter +1, no dedup) would advance twice and diverge
from the quorum.

## What

- **`Storage::high_op() -> Option<u64>`** — the highest op-number
  ever durably WAL-framed. Recovered on `open` from the WAL replay
  max **and** a new `Manifest.high_op` watermark (written on
  `flush`/`compact`, which truncate the WAL). The manifest field is
  appended before the trailing CRC and read back length-tolerantly,
  so a pre-SP94 manifest reads as `0` (unknown) — backward
  compatible. It is **not** in the digest (it is derived from the
  WAL, not stored state) ⇒ zero digest perturbation, every existing
  digest byte-identical.
- **`Op::is_mutating()`** — reads (`Get*`/`Find*`/`Query*`/
  `Select*`/`Aggregate*`/`Describe`/`SeqRead`/`Join`) are never
  guarded: re-running them is side-effect-free and they must always
  return real data.
- **`StateMachine::apply` guard** — a *mutating* op whose
  `op_number ≤ high_op` short-circuits to `OpResult::Ok` with no
  side effects (its WAL frames were already replayed on `open`).
  **Inert in normal operation**: VSR assigns strictly-increasing
  op-numbers, so a fresh op is always `> high_op`; the guard fires
  only on the recovery-replay path.
- **`StateMachine::applied()`** exposes the cursor for the VSR
  layer.

## Verified

`kessel-sm::reopen_then_vsr_replay_of_durable_prefix_is_idempotent`:
apply `CreateType` + `Create` + two `SeqAppend` + `Create`, `flush`
+ `sync`; reopen from the same disk → digest **and** cursor
recovered (cursor survives the WAL-truncating flush via the
manifest); re-feed the *entire* durable prefix (every op ≤ cursor,
incl. the non-idempotent `SeqAppend`) → digest **byte-identical**;
a fresh op past the cursor still applies and advances it.

Full workspace regression **201 green**, determinism corpus /
seed-7 intact. Two SP90/SP91 SQL oracles were corrected to use
**monotonic** op-numbers — they had used unrealistic disjoint
ranges (`t` at `10+id`, `u` at `10_000+id`, interleaved) that the
(correct) recovery guard rightly short-circuits; real VSR never
decreases an op-number.

## Boundary

This delivers the *engine* prerequisite only. The multi-node
disk-fault-*during-view-change* cluster harness (a recovered
replica rejoining VSR and converging with the quorum under an
injected fault) builds on this plus `kessel_io::FaultVfs` (SP92)
and is the next slice (#74).
