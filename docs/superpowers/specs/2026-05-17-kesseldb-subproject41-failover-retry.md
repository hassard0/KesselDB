# KesselDB Sub-project 41 — failover-safe retries (any-node cached reply)

**Date:** 2026-05-17  **Status:** shipped, tested on the real cluster.

## What this closes

The server half of "failover client-reply routing". Before SP41 only the
*primary* answered a duplicate `(client, req)` from its client table; a
client whose primary died and who retried elsewhere would have the op
**re-executed** on the new primary (exactly-once broken across failover).

## The change (one focused consensus edit)

`Replica::on_request`: the cached-reply check now runs **before** the
backup→primary relay, so *any* replica that already holds the committed
result (every node's client table is populated by `apply_through` on the
commit path) answers a retransmission directly — no relay, no
re-execution. Falls through to the existing relay only when this node does
*not* yet have the result. Full workspace (incl. the 16 VSR sim scenarios
and the determinism corpus) stays green: **132**.

## API + test

- `Node::submit_as(client, req, op)` and `Session::client_id()` — what a
  failover-aware client uses to retry the same `(client, req)` against a
  surviving node.
- `failover_retry_against_follower_returns_cached_reply` (3 real-TCP
  nodes): commit `(session, req=1)` via the primary; wait for the follower
  to apply it; **retry the exact same `(client, req=1)` against the
  follower** → returns the original `Ok` from the follower's replicated
  client table, **and the follower's digest is unchanged** (no second
  apply); a fresh client creating the same id then collides (`Exists`,
  proving exactly-once held).

## Honest scope boundary

Server-side failover-safe retry is **done**. What remains is purely a
*client-side* concern: `kessel-client` does not yet auto-discover the new
primary / auto-reconnect+retry — an application currently drives that with
`submit_as` against another node. That client convenience is the named,
small follow-up; the consensus/server substrate it needs is complete.
Adversarial-partition liveness (seed 7, SP37) is still the one open
consensus-research item, orthogonal to this.
