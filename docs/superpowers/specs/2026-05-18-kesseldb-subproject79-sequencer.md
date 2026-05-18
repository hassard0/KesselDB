# KesselDB Sub-project 79 — global sequencer (cross-shard slice 2/6)

**Date:** 2026-05-18  **Status:** shipped. 180 green. The total-order
primitive the Calvin-style cross-shard protocol is built on.

## Why

Deterministic (Calvin-style) cross-shard transactions need every
participant shard to agree, without locks or 2PC, on **one global
order** of cross-shard transactions. This slice provides that order as
a primitive; later slices decompose a cross-shard `Op::Txn` into
per-shard slices and have each shard apply them in this order.

## What

Two ops on the ordinary state machine, so the sequencer is just a
**normal VSR group** (linearizable, failover-safe, already proven) —
no new runtime, no new consensus:

- `Op::SeqAppend { payload }` — in **one replicated op**: read the
  reserved counter, assign `seq = counter + 1`, store `payload` at the
  seq's log key, write the counter back, reply `Got(seq u64 LE)`.
- `Op::SeqRead { from, limit }` — the ordered descriptor log from
  `from` (inclusive), `Got([u64 seq][u32 len][payload])*`.

Reserved keyspace `SEQ_TYPE = 0xFFFF_FFF0` (distinct from the overflow
`0xFFFF_FFFF` and index `0xFFFE/0xFFFD` tags): object id 0 = the
counter, ids ≥ 1 = log entries keyed by **big-endian** seq so a range
scan is already in order. Because the counter lives in storage, it is
part of the replicated **digest** and is WAL-recovered — not engine-
local state.

## Why it's deterministic (the whole point)

`SeqAppend` is a single op: the counter advances strictly in
VSR-replicated op order, so every replica of the sequencer group
assigns the identical gap-free seq and converges bit-for-bit.
Concurrency is linearized by the group's own consensus — there is no
read-modify-write race because assign+store+bump is one op, not two.

## Verified

`sequencer_is_gap_free_monotonic_and_deterministic`: appends return
1, 2, 3 (gap-free, monotonic, 1-based); the full log reads back
exactly; `from`/`limit` windows correctly; reading past the end is
empty (not an error); an identical op stream yields an identical
digest (⇒ every sequencer replica converges). Full workspace
regression green; determinism / VSR partition corpus (incl. seed 7)
unchanged (additive ops, no change to existing paths).

## Boundary

This slice is the ordering primitive only. Decomposition of a
cross-shard transaction into per-shard slices, submitting the
descriptor here, and deterministic per-shard application in seq order
are the next slices. Sequencer-group placement/operation (it is run as
an ordinary cluster) and its liveness are covered by the existing VSR
guarantees.
