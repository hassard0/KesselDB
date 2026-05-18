# KesselDB Sub-project 81 — cross-shard atomicity, exactly-once, recovery (4/6)

**Date:** 2026-05-18  **Status:** shipped. The correctness core of
deterministic cross-shard transactions.

## What this slice closes

Slice 3 committed non-conflicting cross-shard transactions but a slice
that would deterministically fail on one shard while another succeeds
was the documented gap. This slice makes a cross-shard transaction
**all-or-none across shards**, **exactly-once** under client/router
retry, and **recoverable** after a router restart — with no
coordinator and no 2PC blocking, because every decision is a pure
function of durable state.

## Deterministic two-phase (no coordinator-failure hole)

Replaces the slice-3 one-phase drive with decide → commit:

- `Op::XshardDecide { seq, ops }` — the shard **dry-runs** its slice
  against committed state (overlay applied then *always aborted* — no
  state change) and persists a **stable verdict** for `seq` in a
  reserved keyspace (in the digest). Idempotent: a re-decide returns
  the recorded verdict even if state later changed, so the verdict is
  fixed at its serialization point.
- The global decision is the **AND** of participant verdicts. Because
  each verdict is a pure function of that shard group's durable state,
  *any* router (including a freshly restarted one) recomputes the same
  decision — there is no coordinator whose crash loses the outcome.
- `Op::XshardCommit { seq, ops, commit }` — same in-order, cursor-
  idempotent rule as slice 3, but gated: `commit=true` applies the
  slice atomically and advances the cursor; `commit=false` is a
  deterministic atomic **skip** (advance, apply nothing). Either way
  every shard advances lockstep, so the txn is all-or-none.

## Exactly-once

`Op::SeqAppendOnce { key, payload }` — a dedup map (in the digest)
returns the *same* seq for a repeated `key`, so a client/router retry
appends the descriptor once and re-drives the same seq idempotently.
The router derives `key` from a session frame's stable `(client, req)`
(true exactly-once); a bare-Op client gets a unique per-call key
(at-least-once — never a *false* dedup; the dedup map verifies the
full key so a 128-bit-hash collision can only ever MISS, never
falsely match). Documented, consistent with the rest of the system
(exactly-once needs session frames).

## Recovery

`router::recover(&router)` re-reads the entire ordered sequencer log
and re-drives every seq's decide+commit. Safe to call anytime: decide
is verdict-stable, commit is cursor-idempotent — a transaction durably
appended but not fully driven (router died mid-protocol) is completed,
and one already applied is a no-op. The commit point is "descriptor
durably in the ordered log + deterministic decision", computable by
anyone from durable state — nothing is lost on router crash.

## Verified

- `xshard_two_phase_and_exactly_once_primitives` (fast, SM-level):
  same-key append ⇒ same seq, new key ⇒ next seq; decide applies
  nothing and is verdict-stable; a would-fail slice ⇒ verdict 0;
  commit=false skips, commit=true applies, re-drive past the cursor is
  a no-op; deterministic digest.
- `cross_shard_aborts_atomically_is_exactly_once_and_recovers` (real
  sockets, 2 shard groups ×3 + sequencer ×3): a cross-shard txn whose
  shard-0 slice is a dup **aborts on shard 1 too** (no partial write);
  replaying the same session `(client,req)` twice applies exactly
  once; `recover` re-drives the whole log idempotently and the aborted
  txn stays aborted (stable verdict).

Full workspace regression green; determinism / VSR partition corpus
(incl. seed 7) unchanged (additive ops + router front-end).

## Honest boundary (→ slice 5/6)

The router serializes cross-shard commits (the `xs` lock) to drive
seqs in order; an async pull-drive (each shard tails the log itself)
is a later efficiency change, not a correctness one. Slice 5 adds the
adversarial-partition proof (loss/partition between router, sequencer,
shards); slice 6 updates the public docs to describe the delivered
design.
