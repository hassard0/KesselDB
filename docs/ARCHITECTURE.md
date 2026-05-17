# KesselDB Architecture

## The determinism seam (foundational)

Everything above storage is a pure function over an **injected** clock, disk, and network
(`kessel-io`). Production injects real I/O; `kessel-sim` injects a seeded, fault-injecting fake.
The whole database runs deterministically from one `u64` seed — this is what makes a from-scratch
VSR reimplementation verifiable rather than hopeful.

`kessel-sm`, `kessel-catalog`, `kessel-codec` contain **zero** I/O / clock / RNG.

## Crates

`kessel-proto` (wire types) · `kessel-io` (clock/disk/net traits + real & sim impls) ·
`kessel-storage` (LSM+WAL+recovery) · `kessel-catalog` (schema as object type 0) ·
`kessel-codec` (record encode/decode) · `kessel-sm` (deterministic apply) ·
`kessel-vsr` (replication) · `kessel-cache` (read cache) · `kessel-sim` (fault simulator) ·
`kessel-bench` (perf harness) · `kesseldb` (node binary).

## Replication (VSR)

Viewstamped Replication ported from TigerBeetle's design: primary assigns op-number + a
deterministic timestamp; Prepare → f+1 PrepareOk → Commit; backups apply in op-number order;
view-change on primary timeout; state transfer for lagging replicas; client table for
exactly-once retried client batches. Fixed cluster size (3 or 5); membership reconfiguration
is out of scope for Sub-project 1.

## Sharding (groundwork in M4)

Deterministic key→shard mapping (hash/rendezvous over `type_id‖primary_id`). Sub-project 1
ships a single shard with the routing interface in place. Multi-shard introduces cross-shard
transactions — explicitly deferred; the limitation is documented rather than hidden. Each shard
would be its own VSR group; a shard router sits in front.

## Caching (M4)

Bounded LRU read cache keyed by `type_id‖primary_id`, invalidated by the state machine on
Update/Delete (write path stays the source of truth and stays deterministic — the cache is a
side index off the committed state, never consulted during `apply`). Feature-flagged so the
deterministic core path is unaffected when off.

## Storage layout

LSM key = `type_id(4B) ‖ primary_id(16B)`, value = codec-encoded fixed-width record with a
per-record `schema_ver` header and null bitmap. A type is a contiguous key range (sets up
future range scans). WAL frame: `(op_number, kind, type_id, payload, crc32c)`.
