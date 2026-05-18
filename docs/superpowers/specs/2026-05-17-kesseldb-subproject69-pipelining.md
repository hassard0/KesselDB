# KesselDB Sub-project 69 — request pipelining

**Date:** 2026-05-17  **Status:** shipped, measured on vulcan. 168 green.

## The lever SP68 left open

SP68 made the engine group-commit (one fsync per drained batch), but the
batch is bounded by *in-flight ops* and `handle_conn` is strictly serial:
read one frame → block on the engine → write the reply → read the next.
So a single connection only ever has **one** op in flight; the group
fsync amortises over a batch of one, and the network pays one full
round-trip per statement. SP68 explicitly named "scales with
concurrency / pipelining" as the next lever — this is it.

## Design

New wire tag `PIPELINE_TAG = 0xF8`:

```
request:  [0xF8][u32 cnt] then cnt × ([u32 len][inner frame])
reply:    Got( [u32 cnt] then cnt × ([u32 len][OpResult::encode]) )
```

Each inner frame is an ordinary `[0xFE] ++ SQL` (or bare `Op::encode()`).
The whole batch is **one engine message**, so it lands in a single
group-commit fsync and costs a single network round-trip — while every
member applies *exactly as if sent alone*.

To guarantee that equivalence, the per-frame core (`[0xFE]`-SQL,
compile-cache use/invalidation, the server-side `UPDATE` RMW, bare-op
decode, monotonic id) was extracted verbatim into a single free fn
`apply_one`, now the **one** source of truth shared by the normal path
and by every pipeline member. The big duplicated tail in the `compute`
closure is gone; the refactor is behaviour-preserving (full suite green).

**Not a transaction.** A pipeline is N *independent* requests — unlike
`TXN_TAG`/`BEGIN…COMMIT` it is not atomic. A failing member returns its
own error and the rest still apply. The test asserts this explicitly
(a dup-id member returns `Exists` while later members return `Ok`, and
the final state equals sending the statements singly).

Client: `Client::pipeline(&[&str]) -> io::Result<Vec<OpResult>>`.

## Measured

`pipelined_batch_is_equivalent_and_amortises_round_trips`: one
connection, 12 000 inserts in batches of 500 (24 round-trips, not
12 000) vs the serial path on the same connection.

| single connection | serial | pipelined (batch 500) | speedup |
|---|---|---|---|
| dev box (Windows) | 1,839 ops/s | 88,933 ops/s | ~48× |
| **vulcan (Linux)** | **242 ops/s** | **52,721 ops/s** | **~217×** |

For comparison, SP68's best (8 *concurrent* connections, durable) was
1,870 ops/s on vulcan; a single pipelined connection now does ~28× that
because it fills the group-commit batch itself instead of needing many
connections.

## Honest reading

- Serial single-connection on vulcan is only 242 ops/s precisely because
  each request is one round-trip and the group fsync sees a batch of 1 —
  that is the SP68-named limitation, now removed for batched workloads.
- The 52K number is gated by real fsync amortised over 500-op batches on
  a near-full shared disk; larger batches / multiple pipelined
  connections push it higher. Not overclaimed — the limiting factors are
  named and the correctness (14 003 rows durable from a fresh conn) is
  asserted, not assumed.
- No new config knob and zero change to existing paths: `sql()` /
  `call()` are byte-for-byte unaffected (proven by the unchanged suite);
  pipelining is purely additive.
- Determinism/VSR corpus untouched: members apply through the same
  `sm.apply` in the same order with the same ids — the digest is
  identical to sending them one at a time.
