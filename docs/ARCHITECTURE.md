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

## Sharding & cross-shard transactions

Deterministic key→shard mapping (rendezvous hashing over the 20-byte
row key). A deployment runs **K independent VSR shard groups** behind a
**router**: a request goes to the shard that owns its key; schema/DDL
is broadcast to every shard (identical catalogs ⇒ deterministic
per-shard execution); a single-shard transaction stays on that shard's
own VSR group (serializable, fast path).

**Cross-shard transactions are deterministic (Calvin-style), not 2PC.**
A cross-shard `Op::Txn` is decomposed into per-shard slices and a
descriptor is durably **totally ordered** by a dedicated replicated
**sequencer group** (an ordinary VSR cluster; one append op assigns a
gap-free seq, the counter lives in the digest). Each shard then
processes every global seq in order via a deterministic two phases:

- *decide* — dry-run the slice against committed state and persist a
  **stable** verdict (applies nothing);
- *commit* — apply the slice iff the **global decision** (the AND of
  participant verdicts) is commit, else a deterministic atomic skip;
  the per-shard cursor advances lockstep with the global order.

Because every verdict is a pure function of that group's durable state,
the global decision is recomputable by **any** router (no coordinator
whose crash loses the outcome) and no locks are held across shards.
Properties: atomic (all-or-none across shards), exactly-once under
client retry (stable `(client,req)` keying with a digest-resident dedup
map), and recoverable (a full ordered re-drive after a router restart
is idempotent — verdicts are stable, commits cursor-idempotent).

Correctness is proven by composition: each shard group's partition
tolerance is the seeded VSR corpus (incl. the historically-hard
seed 7); the cross-shard layer adds a deterministic adversarial-drive
test (partial decide, simulated router crash, duplicate retries,
repeated recovery, reordering ⇒ identical state, itself deterministic)
plus over-sockets atomicity/exactly-once/recovery/concurrency tests.

Documented boundary: the router serializes cross-shard commits to drive
the global order (an async per-shard pull-drive is an efficiency
follow-up, not a correctness change); cross-shard transactions are
point-op batches, and cross-shard scatter-gather *reads*/SQL routing is
a separate later concern from cross-shard *transactions*.

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

## Query planner (Sub-project 5)

`Op::Query` takes a conjunction of `Pred{field_id, op∈{Eq,Ge,Le}, value}`.
The planner fetches and **intersects** the SP3 id-sets of all indexed
equality predicates; if any exist it verifies every predicate on just those
candidate rows, else it does a filtered `scan_range` over the type's key
range. `cmp_field` compares per kind (numeric for ints/bool/timestamp,
sign-extended for signed/Fixed, lexicographic for byte kinds) so range
predicates are correct on little-endian integer storage. `Query` is
read-only and a pure function of committed state, so it is not logged and is
trivially identical across replicas.

## Foreign keys (Sub-project 6)

`ObjectType.fks` = `(field_id, ref_type_id)` pairs. On Create/Update (after
UNIQUE) each FK field's value, padded to a 16-byte id, must resolve via
`storage.get(make_key(ref_type, id))`. Read-only against committed state ⇒
deterministic/replication-safe. Codec-record scoped; NULL skipped.
`AddForeignKey` validates all existing rows and refuses (without enabling) on
any dangling reference. No referential actions (`ON DELETE`/`ON UPDATE`) —
documented limitation.

## Deterministic expression VM + CHECK (Sub-project 7)

`kessel-expr` is a zero-dependency stack bytecode VM that is **pure,
gas-bounded, and terminating** (no backward jumps). It is the mechanism that
lets KesselDB carry Postgres-style programmable constraints while staying a
deterministic replicated state machine: a CHECK program is part of the
replicated catalog (`ObjectType.checks`), so every replica runs byte-identical
logic and reaches the same accept/reject. `Op::AddCheck` validates the
program structurally and against all existing rows before enabling. The same
VM is the substrate for SP8 deterministic triggers.

## Atomic transactions (Sub-project 9)

`Storage` has a transaction overlay: `begin_txn` buffers writes in-memory
(reads see them — read-your-writes), `commit_txn` flushes the whole batch to
the WAL with a single fsync then makes it visible, `abort_txn` drops the
overlay (nothing reached WAL/memtable ⇒ nothing to undo). `Op::Txn` runs its
inner data ops through the normal `apply` path so constraints/indexes/
triggers/overflow all compose and roll back together; the read cache is
cleared on abort. A transaction is one replicated op, so the serial state
machine makes it serializable and replica-identical. DDL/nested txns are
rejected (the overlay does not cover the catalog or range scans).

## Storage layout

LSM key = `type_id(4B) ‖ primary_id(16B)`, value = codec-encoded fixed-width record with a
per-record `schema_ver` header and null bitmap. A type is a contiguous key range (sets up
future range scans). WAL frame: `(op_number, kind, type_id, payload, crc32c)`.
