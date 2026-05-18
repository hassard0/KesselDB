# KesselDB Sub-project 80 — deterministic cross-shard execution (3/6)

**Date:** 2026-05-18  **Status:** shipped. The cross-shard transaction
now actually commits, deterministically (Calvin-style), over real
sockets.

## Mechanism

`Op::XshardApply { seq, ops }` on the shard state machine. A shard
processes **every** global sequence number in order, exactly once,
tracking a cursor in a reserved keyspace (`XSHARD_TYPE = 0xFFFF_FFF1`,
part of the digest ⇒ every replica of the shard group advances
identically):

- `seq <= cursor` → idempotent no-op (a safe re-drive);
- `seq != cursor + 1` → refused (no gaps — the driver feeds in order);
- else apply `ops` atomically via the existing `Op::Txn` overlay and
  advance the cursor **in the same transaction** (slice + cursor commit
  or roll back together). Empty `ops` = a non-participant shard just
  advancing its cursor, keeping it lockstep with the global order.

Router (`commit_cross_shard`), holding a serialization lock so global
seqs are driven in order:

1. Decompose the `Op::Txn` into per-shard slices by routing each member
   (Create/Update/Delete) by its key.
2. Encode a descriptor and `Op::SeqAppend` it to the sequencer group —
   **this is the commit point**: once it returns, the transaction is
   durably ordered in the replicated log and is committed.
3. Drive every shard through that seq in order — participants get their
   slice, others an empty advance.

No 2PC, no locks held across shards, no coordinator decision: the
ordered log is the single source of truth and per-shard execution is
deterministic, so all shards converge to the same outcome.

## Verified

- `xshard_apply_is_in_order_idempotent_and_atomic` (fast, SM-level):
  in-order only (gap refused), idempotent re-drive does not re-apply,
  a failing slice rolls back fully and does **not** advance the cursor
  (so it can be retried), empty slice advances, identical slice stream
  ⇒ identical digest.
- `cross_shard_txn_commits_atomically_via_sequencer` (real sockets,
  2 shard groups ×3 + a sequencer group ×3): a cross-shard `Op::Txn`
  with one row per shard commits `Ok`; each row lands on **exactly**
  its owning shard (verified by querying each shard directly —
  `NotFound` on the non-owner); a second cross-shard txn at the next
  global seq also commits.

Full workspace regression green; determinism / VSR partition corpus
(incl. seed 7) unchanged (additive op + router front-end).

## Honest boundaries (carried into slice 4)

- **Abort agreement**: a slice op that deterministically fails on one
  shard rolls back *that shard* and does not advance its cursor, but
  cross-shard *agreement* (all shards abort iff any would) is slice 4;
  this slice's tests use non-conflicting slices. The cursor design
  makes a clean retry possible, which slice 4 builds on.
- **Router-level cross-shard exactly-once** and **recovery** (re-drive
  from the durable log if the router dies after `SeqAppend`) are
  slice 4. Per-`XshardApply` is already idempotent, so recovery is a
  re-drive, not a correctness hazard.
- The router serializes cross-shard commits to drive seqs in order;
  an async pull-drive (each shard tails the sequencer log itself) is a
  later optimisation, not a correctness change.
