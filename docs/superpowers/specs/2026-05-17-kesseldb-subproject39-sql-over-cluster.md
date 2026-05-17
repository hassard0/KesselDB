# KesselDB Sub-project 39 — SQL over the multi-node cluster

**Date:** 2026-05-17  **Status:** shipped, tested over real sockets.

## What this closes

The SP38 cluster only accepted `Op` frames via `Node::submit`. Production
clients speak the full `kessel-client` protocol incl. `Client::sql(...)`.
SP39 makes the **real multi-node TCP cluster speak full SQL**, including
the `UPDATE` read-modify-write — every statement linearized through VSR
consensus.

## Delivered

1. **`Replica::catalog()`** — read-only accessor so SQL compiles against
   the live schema on the engine thread (the catalog is owned by the
   wrapped `StateMachine`; kessel-vsr now depends on kessel-catalog).

2. **`Ev::ClientRaw` + continuation engine.** A raw client frame
   (`Op::encode()` or `[0xFE] ++ SQL`) is compiled on the engine thread.
   `Stmt::Op` is submitted as one replicated op. `Stmt::Update` becomes a
   **two-round RMW over consensus**: a linearized `GetById`, then — via a
   `Cont::Update` continuation that fires when that commits — a patched
   single `Op::Update` is submitted, and *its* result returns to the
   client. The engine stays a single non-blocking loop (no engine-thread
   stalls waiting on consensus): continuations are chased through a work
   queue inside `process`, never by blocking. Internal consensus ops use a
   client-id range (`1<<100 +`) disjoint from external `Node::submit` ids.

3. **`serve_clients`** — the ordinary client TCP front for a cluster node,
   one thread per connection, so existing `kessel-client` (incl. `sql()`)
   works unmodified against the cluster.

4. **Integration test `sql_over_cluster_full_crud_and_rmw`** — 3 real-TCP
   nodes; primary fronted with `serve_clients`; a real `Client` does
   `CREATE TABLE` / `INSERT` / `SELECT SUM … WHERE` / `UPDATE … SET`
   (RMW) / `UPDATE` of a missing row → `NotFound` / `SELECT * … ID`; then
   asserts both followers converge to the **primary's exact state digest**.
   Full suite: **130 green**.

## Honest scope boundary

- Clients connect to the primary (replies are emitted there). Failover
  client-reply re-routing (reconnect/retry to the new primary) remains the
  named follow-up — unchanged from SP38, not regressed, not hidden.
- Adversarial-partition liveness (seed 7, SP37) is still the one open
  consensus-research item; orthogonal to this transport/SQL slice.
