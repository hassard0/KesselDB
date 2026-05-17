# KesselDB Sub-project 42 — client-side new-primary auto-discovery

**Date:** 2026-05-17  **Status:** shipped, tested. Failover gate now
**fully closed** (server side SP41 + client side SP42).

## What this closes

A client connected to a backup that submits a *new* request would hang
(the reply is produced on the primary). SP42 makes the client discover and
fail over to the primary automatically — **exactly-once**, not best-effort.

## Delivered

1. **`OpResult::Unavailable`** (wire tag 7, roundtrip-tested) — a
   transport-level "not the active primary / mid view-change, and I held
   no cached result for you; try another node" signal. Explicitly *not* a
   committed result.

2. **`Replica::is_active_primary()`** — `is_primary() && status==Normal`.
   The cluster engine, after handling a client request, calls `redirect`:
   if the request is still unanswered **and** this node is not the active
   primary, it replies `Unavailable` immediately instead of letting the
   caller hang. (On the primary it leaves the request pending — the reply
   arrives async on commit. A cached hit, SP41, is delivered normally.)

3. **Session-request wire frame `0xFD`**:
   `[0xFD][client:u128][req:u64][Op::encode()]`. Carries a **stable
   `(client, req)`** so a cross-node retry is deduped by the replicated
   client table (exactly-once). `kessel_client::{session_frame,
   parse_session_frame}`; the server front routes `0xFD` through
   `Node::submit_as` (the dedup-aware path).

4. **`ClusterClient`** — holds the node address list and a stable session
   (unique-enough zero-dep client id: wall-clock nanos folded with a
   process/thread salt). `call(op)` increments `req`, and on `Unavailable`
   or a connection error rotates to the next node and **retries the same
   `(client, req)`** (bounded attempts). Safe because the server is
   exactly-once.

5. **Test `cluster_client_finds_primary_and_is_exactly_once`** — 3
   real-TCP nodes each with a client front; address list with the primary
   **last**, so the client must rotate past two followers (each answering
   `Unavailable`) to reach it; `CreateType` + `Create` succeed. Then the
   *identical* committed session frame for the latest `(client, req)` is
   replayed straight to a follower: it returns the **cached `Ok`** and the
   follower's digest is **unchanged** (no re-apply). Full suite: **133**.

## Honest scope boundary

`ClusterClient` is the failover-aware path for `Op`s. SQL over the cluster
client still connects to the primary (SQL `UPDATE`'s 2-round RMW is not
keyed for cross-node mid-RMW replay) — the documented SP39/41 boundary,
unchanged. Adversarial-partition liveness (seed 7) remains the one open
consensus item, orthogonal to this.
