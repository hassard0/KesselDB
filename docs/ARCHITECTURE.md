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

## Variable-length overflow store (Sub-project 2)

`OverflowRef` fields hold arbitrary-length bytes without breaking the
fixed-width record. The blob travels inside the replicated `Create`/`Update`
record as a trailer; the state machine splits it out, writes it to a reserved
LSM keyspace (`type_id = 0xFFFF_FFFF`) under a **deterministic op-derived
handle** `(op_number << 20) | field_idx`, and patches the 8-byte handle into
the record's `OverflowRef` slot. Determinism holds because `op_number` is
assigned by the VSR primary and replicated, so every replica computes the
same handle and stores identical bytes. Reads use `GetBlob { handle }`.
Orphaned-blob GC (after an overflow-field `Update`) is deferred and documented.

## Equality secondary indexes (Sub-project 3)

`ObjectType.indexes` lists indexed `field_id`s (replicated catalog). Index
entries live in a reserved storage type-slot `0xFFFE0000 | (user_type&0xFFFF)`,
key id = `field_id ++ value_digest8 ++ pad`, entry value = digest-collision-
safe buckets (per full value, a sorted set of object ids). Keys/bytes are
content-derived and id sets sorted, so replicas build a byte-identical index
keyspace (digest-covered). `CreateIndex` backfills via `Storage::scan_range`
over the type's contiguous key range; Create/Update/Delete maintain indexes.
Equality only; range scans + a multi-index intersection planner are a later
spec.

## Built-in constraints (Sub-project 4)

`OpResult::Constraint` is a deterministic op result. NOT NULL derives from
`Field.nullable` and is checked against the codec null-bitmap, but only for
well-formed codec records (`len == record_size` and `field_count == #fields`)
— raw/opaque writers opt out by construction. UNIQUE (`ObjectType.unique`,
always ⊆ `indexes`) consults the SP3 equality-index bucket on every
Create/Update, excluding self. `Op::AddUnique` builds the backing index if
needed, validates that current data has no duplicate (rejecting without
half-applying), then records the constraint in the replicated catalog. All
deterministic; convergence is digest-covered and VSR-tested. FK-ref, CHECK,
balance-guard, and the WASM trigger sandbox are later specs.

## Storage layout

LSM key = `type_id(4B) ‖ primary_id(16B)`, value = codec-encoded fixed-width record with a
per-record `schema_ver` header and null bitmap. A type is a contiguous key range (sets up
future range scans). WAL frame: `(op_number, kind, type_id, payload, crc32c)`.
