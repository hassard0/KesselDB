# KesselDB Sub-project 40 — client sessions (exactly-once retries)

**Date:** 2026-05-17  **Status:** shipped, tested on the real cluster.

## Why

`Replica::on_request` already had correct exactly-once dedup (`req <= last`
for a client → return the **cached** reply, do not re-apply). But the
cluster's `Node::submit`/SQL path used a *fresh client id every call*, so
that dedup never engaged: a client that timed out and retried would have
its op applied **twice** — a real production correctness hole and the
foundational half of failover safety.

## Delivered

- **`Node::session() -> Session`** — a stable VSR `ClientId` (tagged into a
  range disjoint from bare `submit` and internal SQL ops) plus a monotonic
  request counter.
- **`Session::submit` / `submit_with_req`** — `submit` auto-increments the
  request number; `submit_with_req` lets a caller (or a retrying client)
  re-send an explicit number. Re-sending a number that already committed is
  a retry: the replica returns the cached reply, the op does **not**
  re-execute.
- **Test `session_retry_is_exactly_once`** (3 real-TCP nodes): create row
  under `(session, req=1)` → `Ok`; record digest; **resend the exact same
  `(client, req=1)`** → still `Ok` (cached) **and the state digest is
  unchanged** (proves no second apply); a *different* client creating the
  same id then collides (`Exists`, proves the row exists exactly once); a
  fresh `req=2` on the session still works; all nodes converge. Full
  suite: **131 green**.

## Honest scope boundary

Exactly-once-**on-retry** is done. The remaining failover piece —
**client-reply re-routing across a primary change** (client discovers and
reconnects/retries to the new primary, and the new primary serves the
cached reply from its replicated client table) — is the named follow-up.
The replicated client table (`apply_through` updates it on every node) and
stable sessions are exactly the substrate that follow-up needs; this slice
makes it tractable rather than research. Adversarial-partition liveness
(seed 7, SP37) is still the one open consensus item, orthogonal to this.
