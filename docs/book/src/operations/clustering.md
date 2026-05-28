# Running a cluster

A cluster is composed from the `kesseldb-server` library — each node
runs the deterministic engine wrapped in a Viewstamped Replication
replica; nodes talk over real TCP sockets. `ClusterClient` rotates the
address list and retries on `Unavailable` with stable `(client, req)`
exactly-once semantics.

For horizontal scale, run **K independent VSR shard groups** behind a
**router** — a single-shard transaction stays on its shard's own group
(serializable, fast path); cross-shard transactions are deterministic
(Calvin-style, no 2PC) via a replicated sequencer group.

Full reference (code snippets, peer-address layout, recovery, the
deterministic Calvin-style cross-shard transaction model):
[Usage guide (full) §7 + §7b](../usage/full-usage.md#7-running-a-cluster).

Architectural background:
[Architecture → MVCC & VSR](../architecture/mvcc-and-vsr.md) and
[Architecture → Sharding](../architecture/sharding.md).
