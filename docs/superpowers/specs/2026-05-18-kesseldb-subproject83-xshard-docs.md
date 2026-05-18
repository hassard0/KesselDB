# KesselDB Sub-project 83 — cross-shard docs (6/6)

**Date:** 2026-05-18  **Status:** shipped. Closes the cross-shard
build: the public docs now describe the delivered design instead of a
deferral.

## Changes (docs only — no code)

- **README**: the "documented single-shard boundary / 2PC
  intentionally not implemented" paragraph is replaced with an accurate
  description of the delivered **deterministic (Calvin-style)**
  cross-shard transactions — router + sequencer + two-phase
  decide/commit; atomic, exactly-once, recoverable; how it is proven —
  with the honest remaining boundaries (router serializes the global
  order; cross-shard *reads*/SQL routing is a separate later concern).
- **ARCHITECTURE.md**: the "Sharding (groundwork, deferred)" section is
  rewritten as "Sharding & cross-shard transactions" describing the
  K-shard-groups + router + sequencer model, the decide→commit
  protocol, why there is no coordinator-failure hole, and the
  composition proof (per-group partition corpus + adversarial drive).
- **USAGE.md**: new "§7b Sharded deployment & cross-shard transactions"
  with the `Router::new(...).with_sequencer(...)` / `serve_router` /
  `recover` wiring and the operational properties; TOC updated.
- **PERFORMANCE.md**: scaling-model point added — sharding scales
  single-shard work horizontally; a cross-shard txn pays one sequencer
  round-trip plus a decide+commit per participating shard and is the
  deliberate serialized slow path.
- **STATUS.md**: stale "cross-shard deferred / single-shard today /
  out of scope" prose corrected to "delivered"; the former non-gating
  roadmap is recorded as completed.

All public docs verified free of internal host names and internal
slice codenames.

## Result

Cross-shard transactions are complete across six tested, committed
slices (router substrate → sequencer → deterministic execution →
atomicity/exactly-once/recovery → adversarial proof → docs), with the
honest boundaries stated, not hidden. The earlier README claim that a
deterministic cross-shard commit was "intentionally not implemented"
is no longer true and has been corrected — the system now does exactly
that, proven the same way every other guarantee in KesselDB is:
seeded, replayable, composed on the partition corpus.
