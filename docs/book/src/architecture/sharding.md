# Sharding & scatter scan

A deployment runs **K independent VSR shard groups** behind a
**router** with a rendezvous key→shard mapping. A single-shard
transaction stays on its shard's own VSR group (serializable, fast
path). Cross-shard `Op::Txn` is deterministic (Calvin-style) — slices
durably totally ordered by a sequencer group, then each shard applies
its slice in that order via a *decide → commit* — **no 2PC, no
coordinator-failure hole**.

**Cross-shard reads (SP-A)** — `Select` / `QueryRows` / `SelectFields`
/ `SelectSorted` automatically scatter across every shard via
`scatter_scan`. Unordered scatter = shard-id-deterministic
concatenation. Sorted scatter = `BinaryHeap` k-way merge of
already-sorted per-shard streams. **K-invariance** is locked across
K ∈ {1, 2, 4, 8, 16} by an 85-seed property sweep — with unique sort
values, merged output is byte-identical to the K=1 baseline.

Full reference:
[Architecture → Sharding & cross-shard transactions](overview.md#sharding--cross-shard-transactions)
and [Architecture → Cross-shard reads (SP-A)](overview.md#cross-shard-reads-sp-a).
