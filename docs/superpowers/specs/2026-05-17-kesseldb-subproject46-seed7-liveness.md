# KesselDB Sub-project 46 — seed-7 adversarial-partition liveness (FIXED)

**Date:** 2026-05-17  **Status:** fixed, seed 7 un-skipped, 139 green.
**This was the last open production gate.**

## The honest diagnosis (not a band-aid this time)

A focused trace (`seed7_diag`) of seed 7 after heal showed the cluster was
**provably healthy and converged**: all 3 replicas `Normal`, one stable
primary, identical `view`, `commit == op_number == 13`, stable for 30,000
steps. So it was *never* a view-change-liveness / divergence /
view-storm problem — every prior hypothesis (and every prior band-aid) was
aimed at the wrong layer.

The real bug: `Replica::on_request`'s dedup branch replied under
`(client, *last)` instead of `(client, req)`:

```rust
if req <= *last { out.replies.push((client, *last, res.clone())); return; }
```

In textbook VSR a client has one outstanding request with a monotonically
increasing number, so `req == last` on any retransmit and the mis-key is
invisible. But under loss + transient partition, delivery reorders: an
*older* request (`req < last`) can reach the primary *after* a later one
already advanced `last`. The reply was then addressed to `last` — a
request already satisfied — so the caller waiting on `(client, req)` waited
forever. A permanent liveness stall **with a perfectly healthy cluster**.
Seed 7 was simply the schedule that produced that reordering.

## The fix (one line, correct for real VSR too)

Reply addressed to the request actually asked:

```rust
if req <= *last { out.replies.push((client, req, res.clone())); return; }
```

- `req == last` (the normal retransmit): identical behaviour — unchanged.
- `req < last` (a superseded, reordered duplicate): now acked under its
  own key with the client's latest committed result. The op is long
  superseded and is **never re-executed** (still exactly-once); the caller
  just gets the correct "you're already past this" answer instead of
  hanging.

## Result

`partition_then_heal_converges` now asserts the **entire 0..12
adversarial-partition corpus including seed 7** — completion *and* digest
convergence — and passes. With the fix, seed 7 even completes *during*
phase 1 (`acked = 16` before heal). `partition_corpus_is_deterministic`
still passes (determinism preserved). Full workspace: **139 green, 0
failed**, no regression. The `seed7_diag` scaffold was removed once it had
served its purpose.

## Honest framing

Earlier specs/STATUS called this a "formally-paper-grade consensus
liveness" open item. That framing was **wrong** — and saying so is the
honest correction (cf. the SP25→SP26 self-correction). The defect was a
reply-routing key mismatch, not a consensus-protocol gap. VSR safety
(SP37) and the protocol itself were never the problem here. Diagnose
before theorising.
