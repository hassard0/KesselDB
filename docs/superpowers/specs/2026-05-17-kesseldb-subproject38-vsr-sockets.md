# KesselDB Sub-project 38 — VSR over real TCP sockets (multi-node)

**Date:** 2026-05-17  **Status:** shipped, tested over real sockets.

## What this closes

Production-gate item #2 ("multi-node replication over real sockets"). Until
now `kessel-vsr` only ran on the in-process deterministic sim bus; the
server was strictly single-node. KesselDB now runs as a **real multi-node
replicated cluster over TCP**.

## Delivered

1. **`kessel_vsr::wire`** — a zero-dep, length-prefixed codec for all 9
   `Msg` variants (`encode`/`decode`); truncated/garbage frames decode to
   `None`, never panic. Roundtrip-tested for every variant. The protocol
   was always transport-agnostic; this makes the swap real.

2. **`kesseldb_server::cluster`** — one engine thread is the sole owner of
   the non-`Send` `Replica<DirVfs>`. Everything reaches it as an `Ev` on a
   single channel (client op / inbound peer `Msg` / timer tick / probe), so
   apply stays serial and deterministic even with real concurrency at the
   edge. Per-peer writer threads lazily (re)dial and drop on failure (VSR
   re-drives); an accept loop tags inbound frames with the sender idx.
   `Node::submit` linearizes an op through consensus and blocks for the
   committed reply; `Node::probe` exposes `(digest, op_number, commit)`.

3. **Integration test `three_nodes_replicate_over_real_tcp`** — 3 nodes on
   real ephemeral TCP ports; client→primary `CreateType` + `Create` +
   linearized `GetById` + atomic `Txn`; asserts all 3 nodes converge to the
   **same state digest** with ≥4 ops committed everywhere, over the socket
   transport. Full suite: **129 green**.

## Honest scope boundary (not hedging — precise)

- Clients submit `Op`s; replies are emitted on the primary, so a client
  connects to the primary. **SQL-over-cluster** (catalog-side compile +
  UPDATE RMW under replication) and **client-reply re-routing on failover**
  (client reconnect/retry to the new primary) are named follow-ups, not
  silently missing — the single-node server still serves full SQL.
- The adversarial-partition *liveness* item (seed 7, SP37) is unchanged and
  still open; this slice is the transport/replication milestone, orthogonal
  to that consensus-liveness research item. Safety (SP37) holds here too.
