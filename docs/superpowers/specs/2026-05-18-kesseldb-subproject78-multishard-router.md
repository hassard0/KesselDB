# KesselDB Sub-project 78 — multi-shard router (cross-shard slice 1/6)

**Date:** 2026-05-18  **Status:** shipped. The substrate for
deterministic (Calvin-style) cross-shard transactions, over real
sockets, per the chosen design.

## Context

`kessel-shard` (rendezvous key→shard map) had been groundwork since M4
but was **never instantiated by any runtime** — one `Replica` = one
VSR group = one state machine. This slice wires it into a real
front-end so a deployment can run **K independent shard groups** (each
an existing multi-node VSR cluster) with requests routed to the shard
that owns their key.

## What this slice delivers

`kesseldb_server::router`:

- **Point ops** (`Create`/`Update`/`Delete`/`GetById`) → the single
  owning shard (rendezvous over the 20-byte row key, identical to
  `make_key`).
- **Schema/DDL** → **broadcast to every shard**. Shards must keep
  byte-identical catalogs so per-shard execution stays deterministic
  (essential for the Calvin-style protocol in later slices); the
  router verifies every shard returns the same result or reports
  divergence.
- **`Op::Txn` within one shard** → that shard (per-shard atomic, exactly
  as a single cluster already is).
- **Cross-shard `Op::Txn`** → **detected and cleanly rejected** with a
  precise error. This slice makes a multi-shard deployment *correct*,
  not silently wrong — the atomic cross-shard commit is the next
  slices (sequencer → deterministic execution).
- Scatter-gather reads / SQL text → a clear "later slice" error, never
  a wrong answer.

Per-shard hops use `kessel_client::ClusterClient` (finds the primary,
exactly-once). Router-level client exactly-once *across* shards is a
later slice (each hop is already exactly-once).

## Honest boundaries (explicit, not hidden)

- Cross-shard transactions are not yet atomic — rejected, not faked.
- The router is operation-level; SQL text and multi-shard
  scatter-gather reads/aggregates/joins are deferred (separate concern
  from cross-shard *transactions*, which is the deliverable).
- `GetBlob`/overflow routing under sharding is deferred.
- Router-level cross-shard exactly-once and recovery are later slices.

## Verified

- `route_decisions_are_correct` (pure, no sockets): DDL→All,
  Describe→one, point→owning shard, txn split→Cross, txn on one
  shard→One, scan→Unsupported.
- `router_routes_points_broadcasts_ddl_and_rejects_cross_shard` (real
  sockets, 2 shard groups × 3 nodes): DDL broadcast yields one
  identical reply; two ids that hash to different shards each land on
  exactly their owning shard (verified by talking to each shard
  directly — present on the owner, `NotFound` on the other); a routed
  read returns the owning shard's row; a single-shard txn commits
  atomically; a cross-shard txn is rejected with a `cross-shard`
  error **and leaves no partial write** (the pre-existing value is
  intact).

Full workspace regression green; determinism / VSR partition corpus
(incl. seed 7) unchanged (router is a front-end; no state-machine or
consensus change).

## Next slices

2. Global deterministic sequencer group (total order for cross-shard
   txn descriptors).
3. Deterministic per-shard slice execution in sequence order.
4. Atomicity/abort hardening + cross-shard exactly-once + recovery.
5. Over-sockets + adversarial-partition atomicity/determinism proof.
6. Docs: replace the "documented boundary" with the delivered design.
