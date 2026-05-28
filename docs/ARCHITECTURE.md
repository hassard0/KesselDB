# KesselDB Architecture

## How to read this doc

This document is the internals reference for KesselDB. It assumes you've
read [`README.md`](../README.md) (the front door) and at least skimmed
[`docs/USAGE.md`](USAGE.md) (operator + SQL reference).

The structure goes inside-out:

1. **Foundational seams** — determinism, crate layout, the IO injection
   pattern that makes seeded simulation possible.
2. **Replication & sharding** — Viewstamped Replication, scatter scan,
   cross-shard transactions.
3. **Storage & MVCC** — the LSM keyspace, versioned MVCC dispatch,
   Cahill SSI, GC.
4. **SQL + constraints + planner** — query planner, constraints, the
   deterministic expression VM, the deterministic WASM-MVP UDF interpreter.
5. **Wire surfaces** — binary protocol (the deterministic fast path),
   HTTP/1.1 gateway, WebSocket arm, PostgreSQL Frontend/Backend v3.0
   wire + pg_catalog stubs.
6. **Rigor artifacts** — TLA+ specs and TLC baselines.

Every named subsystem has a separate progress / design spec under
`docs/superpowers/specs/`; this doc summarizes and links — it does not
substitute for the per-slice spec when you're modifying a subsystem.

## The determinism seam (foundational)

Everything above storage is a pure function over an **injected** clock, disk, and network
(`kessel-io`). Production injects real I/O; `kessel-sim` injects a seeded, fault-injecting fake.
The whole database runs deterministically from one `u64` seed — this is what makes a from-scratch
VSR reimplementation verifiable rather than hopeful.

`kessel-sm`, `kessel-catalog`, `kessel-codec` contain **zero** I/O / clock / RNG.

## Crates

**Kernel (default `cargo build`, zero external dependencies):**
`kessel-proto` (wire types) · `kessel-io` (clock/disk/net traits + real & sim impls) ·
`kessel-storage` (LSM+WAL+recovery + **MVCC versioned keyspace** + **MVCC Tx/SI/SSI**) ·
`kessel-catalog` (schema as object type 0) ·
`kessel-codec` (record encode/decode) · `kessel-sm` (deterministic apply + heartbeat watermark) ·
`kessel-vsr` (replication + 5 Jepsen-style multi-replica linearizability tests) ·
`kessel-cache` (read cache) · `kessel-shard` (rendezvous key→shard hashing) ·
`kessel-sim` (fault simulator) ·
`kessel-expr` (zero-dep gas-bounded expression VM for CHECK/triggers) ·
`kessel-crypto` (zero-dep SHA-256 + HMAC-SHA256 + PBKDF2 + SHA-1 (RFC 6455 handshake only)) ·
`kessel-wasm` (zero-dep deterministic WASM-MVP interpreter for UDFs — S4) ·
`kessel-sql` (SQL parser + planner) ·
`kessel-bench` (perf harness) · `kessel-client` (CLI + cluster client) ·
`kesseldb-server` (node library + `scatter_scan` cross-shard fan-out) · `kesseldb` (node binary).

**Optional (feature-gated):**
`kessel-fetch` (HTTP/HTTPS/object-store reader, behind `--features external-sources*`) ·
`kessel-objstore` (S3 SigV4 + Azure Shared-Key signers,
behind `--features external-sources-objstore`) ·
`kessel-http-gateway` (HTTP/1.1 + WebSocket surface,
behind `--features http-gateway`; zero external (non-workspace) deps) ·
`kessel-pg-gateway` (PostgreSQL Frontend/Backend v3.0 wire + `pg_catalog`
synthesis, behind `--features pg-gateway`; zero external (non-workspace) deps) ·
`kessel-parquet` (zero-dep Parquet reader — Snappy/GZIP/zstd/LZ4_RAW/Brotli +
V1/V2 + PLAIN/dict + REQUIRED/OPTIONAL + INT96/DECIMAL/FLBA +
**`LIST<primitive>` (SP143)** + **`MAP<K, V>` + struct of primitives
(SP144)** + **`List<List<T>>` + `List<struct<...>>` + `Map<K, struct<...>>` +
`Map<K, List<T>>` + `struct<List/Map/struct>` (SP145)** +
**`List<List<List<T>>>` 3-deep + `List<Map<K,V>>` + `Map<K1, Map<K2,V>>`
(SP146 — OBJ-2c-5 FULLY CLOSED)** + sub-modules `snappy.rs` /
`gzip.rs` / `zstd*.rs` / `lz4.rs` / `assembly.rs` (Dremel record assemblers:
`assemble_list_primitive` / `assemble_map_kv` / `assemble_struct` /
`assemble_list_of_list_primitive` / `assemble_list_of_struct` /
`assemble_map_of_struct` / `assemble_map_of_list` /
`assemble_list_of_list_of_list_primitive` / `assemble_list_of_map_kv` /
`assemble_map_of_map_kv`)).

SP143 extends kessel-parquet with `SchemaTree` (recursive nested schema
model alongside the flat `leaves` list), multi-bit rep/def level decoders
(`decode_page_v1_nested`, `decode_data_page_v2_nested`), and the
Dremel-style `assemble_list_primitive` record assembler. The `extract`
entry-point dispatches flat vs nested via `FileMetaData.flat_schema` —
flat-schema files take the byte-identical pre-SP143 path; nested files
route through `extract_nested`.

SP144 adds `Map<K, V>` decode via `assemble_map_kv` (Dremel-style:
consumes from parallel key + value streams at every `def == max_def`
slot; classifies into 4 cases per outer/value optionality with
REQUIRED-key enforcement) and struct decode via `assemble_struct` (zip
of N flat-decoded field columns into `PqValue::Struct(Vec<(String,
PqValue)>)`, with an all-fields-Null heuristic that surfaces OPT struct
nulls as `PqValue::Null` rather than a struct of all-Null fields). The
`classify_column_plan` dispatcher recognises the canonical 3-node MAP
encoding (via either `converted_type=MAP(1)` / `MAP_KEY_VALUE(2)`
annotation or the structural pattern `REPEATED middle with 2 children,
first REQUIRED`) and the bare struct-of-primitives pattern. The
`read_chunk_levels_and_values` page-loop helper was factored out of the
SP143 List path so List + Map share the V1/V2 + dict/codec dispatch.

SP145 + SP146 together close the OBJ-2c-5 arc via BOLD per-shape
composition (no full Dremel automaton — see specs
`docs/superpowers/specs/2026-05-26-kesseldb-parquet-deep-nesting-design.md` §3.3
and `docs/superpowers/specs/2026-05-26-kesseldb-parquet-deep-nesting-followups-design.md`).
SP145 shipped four new `ColumnKind` variants (`NestedListOfListPrimitive`,
`NestedListOfStruct`, `NestedMapOfStruct`, `NestedMapOfList`) plus
`StructField.nested: Option<Box<ColumnKind>>` for recursive composition.
SP146 closes the 3 cross-products SP145 V1 deferred by adding three more
ColumnKind variants and assemblers: `NestedListOfListOfListPrimitive` +
`assemble_list_of_list_of_list_primitive` (max_rep_level=3 via
8-case classifier + 3-level stack outer/middle/inner accumulators);
`NestedListOfMap` + `assemble_list_of_map_kv` (outer LIST of inner Maps
driven off K/V shared rep stream at max_rep=2); `NestedMapOfMap` +
`assemble_map_of_map_kv` (outer Map of inner Maps with outer K at
max_rep=1 + inner K/V at max_rep=2). The classifier path adds a recursive
`classify_list_of_list_of_group` helper for the 3-deep List case, and
lifts the `LogicalType::Map` arms inside both `classify_list_of_group`
and `classify_map_of_group` to emit the new ColumnKind variants instead
of rejecting. **Every nested Parquet shape pyarrow writes — including
all cross-products up to 3-deep nesting — now decodes through
`extract()`. OBJ-2c-5 arc FULLY CLOSED with NO follow-ups remaining.**
4+ deep nesting (`List<List<List<List<T>>>>` etc.) rejects with
typed `Unsupported("...: SP147 follow-up")` errors awaiting a real
pyarrow fixture that exercises that depth.

**Mechanically-checked artifacts:**
`kesseldb-tla/` — seven layered TLA+ specs
(Replication.tla / MVCCStorage.tla / MVCCTx.tla / MVCCSi.tla / MVCCSsi.tla /
MVCCGc.tla / MVCCCutover.tla) + TLC baselines under `results/`. Replication.tla
TLC: 528M distinct states / depth 21 / 0 violations.

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
point-op batches; SQL-text routing is a separate later concern from
cross-shard *transactions*.

### Cross-shard reads (SP-A)

`Op::Select` / `Op::QueryRows` / `Op::SelectFields` / `Op::SelectSorted`
fan out across every shard via the router-side
`crate::scatter_scan` helper (`crates/kesseldb-server/src/scatter_scan.rs`).
The client wire stays unchanged — clients keep sending the same `Op`;
the router translates each scan into K parallel per-shard calls and
merges the per-shard `OpResult::Got([u32 rowlen][record]*)` payloads
into a single byte-shaped result.

**Fan-out (zero-dep, std-thread only):** one `std::thread` per shard,
each driving a per-shard `ClusterClient` (the `ShardCaller` trait) at
SP-A's only entry point `scatter_and_merge`. Per-shard reply channels
are bounded `sync_channel(SHARD_BACKPRESSURE_BOUND=4)` for skew
defense (one shard returning millions of rows cannot OOM the router
while another times out). NO tokio / NO rayon / NO new external
dependencies.

**Merge semantics, two strategies** — discriminated by the router's
internal `ScatterKind`:

  - **Unordered** (`Select` / `QueryRows` / `SelectFields`): the merged
    output is the shard-id-ordered concatenation of every shard's
    `[u32 rowlen][record]*` payload, truncated to `LIMIT` rows
    (`LIMIT 0` = no cap). Order is shard-id deterministic, NOT
    arrival-order, so the merged bytes are replay-safe.
  - **Sorted** (`SelectSorted`): a `BinaryHeap` k-way merge of the
    per-shard already-sorted streams. The catalog-derived
    `(FieldKind, byte_offset, byte_width)` for the sort field is
    resolved once via shard 0's `Op::Describe` reply (per-shard
    catalogs are identical because DDL is broadcast). `OFFSET` +
    `LIMIT` are applied in the merge loop, NOT shard-side (per-shard
    `OFFSET m` is wrong for a cross-shard sort because rows interleave).

**LIMIT cancellation:** the Unordered merge fires a shared
`Arc<AtomicBool>` cancel flag the instant `output.len() == LIMIT` so
late shards see it pre-call and don't keep the router pinned waiting.
Worker thread join discipline is preserved: every spawned worker is
joined before `scatter_and_merge` returns; cancel-path interactions
with the bounded channel (worker blocked on `send()` after the rx is
dropped) surface as `SendError` clean-exit. Honest gap: an in-flight
`ShardCaller::call` cannot be interrupted (`std::net::TcpStream` has
no cancellable read), so a shard mid-reply continues until its own
`call` returns; per-shard `read_timeout` (default 30s) is the upper
bound.

**Partial-result vs hard-fail mode (T9):** the V1 default is hard-fail
— a single per-shard non-`Got` slot (Unavailable / SchemaError / ...)
poisons the merged result with that slot's typed error. An opt-in
`ScatterContext::partial_on_timeout: bool` lets callers select
best-effort mode via `scatter_and_merge_ctx`: failed shards are
omitted, surviving shards merge normally, the caller receives a
`Vec<u32>` of failed shard ids and is responsible for surfacing
"partial result" to the user. The router's public scan path defaults
to hard-fail for safety; a future T-slice or SQL hint surfaces the
opt-in to clients. Malformed-Got framing bugs (a `Got(garbage)` that
the merger's `iter_rows` rejects) STILL surface clean — partial mode
does not silently drop garbage bytes from a shard.

**K-invariance** (the killer correctness property, SP155 §5.4 / T3):
for `SelectSorted` with unique sort values, the merged output is
**byte-identical to a K=1 baseline** for K ∈ {1, 2, 4, 8, 16}. The
T3 property sweep locks this across 85 seeds × 5 K values = 425
fixture runs at the merge layer, plus a real-socket K=1↔K=4
byte-identical integration test. Unordered scatter is multiset-equal
to K=1 (byte order naturally differs because rows distribute
differently across shards).

**Sort-key tie-break (V1 limitation):** ties in the sort field are
broken by `(value, shard_id)`, NOT `(value, object_id)`. Deterministic
for fixed K + reproducible, but cross-K with sort-value ties may
order tied rows differently. Per-shard records don't carry the
object_id (it's the storage key, not the record), so an oid-based
tie-break would require a new `Op::SelectSortedWithKey` (spec OQ8) —
deferred until a workload needs it. The 85-seed K-invariance sweep
confirmed this is acceptable for V1 because unique sort values are
the common user-facing case.

**Cross-shard snapshot is NOT consistent:** a scatter read can see
rows committed on shard A at opnum_a=100 and rows on shard B at
opnum_b=200 (independent counters). The per-shard MVCC SI work (S2
arc) is per-shard; SP-A inherits per-shard MVCC and reports the
row-set as the union of per-shard snapshots taken at request-arrival
on each shard. A cross-shard consistent snapshot is an explicit
non-goal.

**Out of arc** (named, deferred): SP-B aggregate combine
(`Aggregate { COUNT/SUM/MIN/MAX }` partial-then-final), SP-C streamed
sorted merge over indexes, SP-D `GroupAggregate` cross-shard combine,
SP-E SQL-text routing, cross-shard `Join` (non-goal), cross-shard
consistent snapshot (non-goal). `FindBy` / `FindByComposite` still
route via the existing per-shard path (an indexed-equality lookup is
per-shard but the per-shard secondary index doesn't carry the
matching rows from other shards; a future T-slice can extend the
scatter machinery to FindBy if/when a workload needs it).

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
Orphaned-blob GC is implemented: an overflow-field `Update` frees the
superseded blob and a `Delete` frees the row's blobs, precisely at the
mutating op (deterministic, replication-safe — handles are
op-number-derived).

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
any dangling reference. `ON DELETE` referential actions are implemented:
`RESTRICT`, `CASCADE` (budget-bounded transitive closure), and `SET NULL`
(atomic with the delete). `ON DELETE SET DEFAULT` is not implemented because
there are no per-column defaults yet (a genuine, separate follow-up).
`ON UPDATE` actions are **inapplicable by model, not a missing feature**: a
foreign key references a parent's *object id*, which is immutable (an
`Update` never changes a row's id), so the SQL `ON UPDATE` trigger — "the
referenced key changed" — has no condition under which it could ever fire.

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

## MVCC (Strategic-tier S2, SP110–SP116)

Every SQL statement that touches a user-type row is, **by construction**, a
deterministic MVCC transaction. The MVCC keyspace is a 28-byte
`type_id(4) ‖ object_id(16) ‖ inverted_commit_opnum(8 BE)` layout living in
the same LSM as the 20-byte legacy keyspace; the inverted op_number puts the
newest version first under `scan_range`. The `data_row_dispatch(key)`
discriminator at the storage layer routes 20-byte user-type data-row keys
(type_id in `(0, 0xFF00_0000)`) through MVCC primitives at `u64::MAX`
snapshot (reads) and `op_number` commit (writes) — **no apply-arm or
schema-op rewrites needed**. The dispatch is a one-helper-function +
4-call-site change in `Storage::{get,put,delete,scan_range}` covering
~25-35 data-row I/O sites silently. Replicas reach byte-identical state
at every committed log position (3-replica byte-identity tests gate this).

Isolation: snapshot reads (`Tx::begin`), SI write-side (`Tx::begin_rw`,
SP112), Cahill serializable SSI (`Tx::begin_ssi`, SP113 — write-skew
impossible by construction). GC: `Op::AdvanceWatermark` is a deterministic
op in the apply path (SP114); a heartbeat closure (SP115) submits it.
The whole stack is mechanically verified by TLC across 7 layered TLA+
modules (`kesseldb-tla/MVCC*.tla`).

## Deterministic WASM UDFs (Strategic-tier S4, SP118 + extensions)

`kessel-wasm` is a from-scratch zero-dependency WASM-MVP-subset
interpreter for `CHECK` constraints and triggers. Supported: i32/i64/f32/f64
values + arithmetic + comparison + control flow + locals + in-module call +
**linear memory** (load/store/size/grow) + **tables + call_indirect** with
runtime type_idx equality check + **bit-manipulation** (clz/ctz/popcnt) +
**sign-extension** + **canonical NaN** (0x7FC00000 / 0x7FF8000000000000)
per WASM determinism rules. Gas-bounded: 1 unit per executed instruction;
trap `WasmError::OutOfGas` on limit. Bounds-checked decoder + opcode allow-
list distinguishes "valid WASM-MVP unsupported" from "invalid garbage". A
UDF is part of the replicated catalog; every replica runs byte-identical
logic; UDF behavior is replayable from the log. 113+ hand-derived KATs
against the official WASM-MVP spec.

## Strategic-tier rigor artifacts (S1, S3)

**`kesseldb-tla/`** — seven layered TLA+ modules with TLC baselines:
Replication.tla (S1, SP109 — 528M states / depth 21 / 0 violations) →
MVCCStorage.tla (SP110) → MVCCTx.tla (SP111, 7.36M / 8) →
MVCCSi.tla (SP112, 3.73M / 13) → MVCCSsi.tla (SP113, 348K / 9) →
MVCCGc.tla (SP114, 1.59M / 12) → MVCCCutover.tla (SP115/SP116, 15.08M / 17).
Every module preserves prior invariants; SP109-SP114 discipline is
"never weaken a test" — refinements TIGHTEN or RESTATE.

**Jepsen-style multi-replica linearizability (S3, SP117)** — 5 hand-derived
Jepsen tests in `kessel-vsr::sim::tests` validate the SP116 storage-layer
transparent MVCC dispatch preserves linearizability across the full VSR +
MVCC stack under partition + message loss. `Cluster::drive_until_digests_converge`
extends the simulation past replies-complete so isolated minority replicas
finish state-transfer + catch up.

## Wire protocol gateways

KesselDB exposes the same `Op` apply path through four wire surfaces. The
binary protocol is the default + the deterministic fast path; every other
listener is opt-in via a cargo feature, runs on a sibling TCP socket, and
is byte-untouched by the binary protocol. See
[`docs/USAGE.md`](USAGE.md) §9 (PostgreSQL) and §10 (HTTP + WebSocket) for
the operator-side configuration; this section covers the engine-side
plumbing.

### HTTP listener (with `--features http-gateway`)

When `kesseldb-server` is built with the opt-in `http-gateway` feature, it
runs TWO sibling listener threads (or three with `--features http-gateway,tls`):

1. **Binary wire** on the primary port — the deterministic hot path; this
   is what the SP69 pipelined-batch perf number measures and what every
   replication / VSR / Jepsen oracle exercises.
2. **HTTP gateway** on `ServerConfig.http_addr` — translates HTTP/1.1
   requests into the same engine apply path via the
   `kessel_http_gateway::EngineApply` trait that `EngineHandle` impls.
3. **HTTPS gateway** on `ServerConfig.http_tls_addr` (with `tls` feature)
   — same gateway, TLS-terminated via the existing rustls config used by
   the binary listener.

Each listener has its own `max_conns` cap (per-listener, not joint — so a
saturated HTTP gateway can never starve the binary protocol). The shared
engine `max_inflight` cap bounds total in-flight ops across all listeners
honestly.

The gateway crate `kessel-http-gateway` has zero external (non-workspace)
runtime dependencies. The default `cargo build -p kesseldb-server` (without
`--features http-gateway`) does not link the gateway crate — `cargo tree`
verifies the binary stays untouched.

### WebSocket listener (with `--features http-gateway`)

The WebSocket arm of the HTTP gateway exposes a long-lived `/v1/ws` upgrade
that frames raw `Op::encode()` payloads under the `kessel-op-v1`
subprotocol. RFC 6455 strict handshake + binary frames only + bounded send
queue (16 messages) + 30s ping/pong heartbeat. Shipped under the SP-WS
arc inside the same `kessel-http-gateway` crate (there is no separate
`ws-gateway` feature flag); the session model is fundamentally different
from `/v1/sql` and `/v1/op` (long-lived reader/writer-thread split vs
request/response) but the crate, the Bearer auth surface, and the
`EngineApply` trait are shared.

### PostgreSQL wire listener (with `--features pg-gateway`)

When `kesseldb-server` is built with the opt-in `pg-gateway` feature, it
spawns an additional listener that speaks the **PostgreSQL Frontend/Backend
Protocol v3.0** — the same wire libpq / `psql` / pgcli / JDBC / psycopg /
`pgx` / `tokio-postgres` / sqlx-pg all speak. Shipped under the SP-PG arc
(`docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`,
936 lines).

**V1 scope (T1-T18):**

- **Simple Query protocol** (`Q` message): single statement per `Q`,
  multi-statement rejected with `42601` syntax_error. Streaming
  `RowDescription` → `DataRow`* → `CommandComplete` → `ReadyForQuery`
  per query.
- **SCRAM-SHA-256 auth** (RFC 5802 + RFC 7677), 4096 PBKDF2 iterations,
  with the **Bearer ↔ SCRAM bridge** — the operator's `ServerConfig.token`
  IS the SCRAM password input. One credential surface; rotating the
  Bearer token rotates BOTH HTTP-Bearer and PG-SCRAM atomically. SCRAM
  channel binding (`SCRAM-SHA-256-PLUS`) is V2.
- **Type-OID mapping** for KesselDB `FieldKind` → PG type catalog (Bool=16,
  int2/int4/int8 for signed/unsigned ints, text=25 for Char(n), bytea=17
  for Bytes/Ref/OverflowRef, timestamptz=1184 for Timestamp, numeric=1700
  for U128/I128/Fixed). Text-format wire encoding only in V1; binary
  format is V2.
- **Listener integration** behind the `pg-gateway` feature flag. Bind on
  `ServerConfig.pg_addr` (default port 5432; env `KESSELDB_PG_ADDR`).
  Per-connection `std::thread`. Connection cap defaults to
  `DEFAULT_MAX_PG_CONNS=256` (smaller than HTTP's 1024 — PG clients hold
  connections longer). The PG and HTTP caps are **INDEPENDENT** — a
  misbehaving pgcli cannot starve HTTP clients.
- **Cap-overflow rejection**: `pg_max_conns` exceeded → wire-level
  `ErrorResponse('S=FATAL','C=53300','M=sorry, too many clients already')`
  emitted BEFORE the close, so libpq's `PQerrorMessage()` surfaces the
  structured rejection instead of seeing a bare TCP close.
- **Idle timeout**: `pg_idle_timeout` (default 600s) plumbs through
  `TcpStream::set_read_timeout`. On fire → wire-level `ErrorResponse(
  'S=FATAL','C=57014','M=terminating connection due to idle timeout')`
  emitted BEFORE the close. Distinguished from peer-EOF (silent return)
  and peer-RST (no emit — the write would fail anyway).
- **OpResult → SQLSTATE** mapping: `Exists`→23505, `Unauthorized`→FATAL
  28000, `Unavailable`→FATAL 57P03, `SchemaError(msg)`→42P01/42703/
  42804/42601/42000 (string-match heuristic per spec §11 weak-spot #2;
  V2 SP-PG-SQL-ERRORS adds a structured `kessel-sql::SchemaErrorKind`),
  `Constraint`→23502/23505/23503/23514/23000, `TxAborted`→40001/25006/
  58030.
- **Scatter-scan transparency**: PG-wire dispatches every SQL through
  `EngineApply::apply_sql`; the underlying engine routes scan-shaped
  ops via `Route::Scatter` (SP-A T2) and merges per-shard
  `OpResult::Got(bytes)` slots. Merged bytes have the SAME `[u32 LE
  len][record]*` shape a single-shard `Op::Select` produces — PG-wire
  is byte-identical between K=1 and K=N.

**V2 follow-ups (each its own arc):** Extended Query (`P`/`B`/`D`/`E`/
`S`/`C`/`H` — mandatory for ORMs and prepared statements; SP-PG-EXTQ);
binary-format wire encoding (per-column negotiated in `Bind`);
`RETURNING`; `CancelRequest` (V1 generates BackendKeyData but takes no
action); GUC plumbing for `SET timezone`; COPY FROM STDIN / COPY TO
STDOUT; TLS via SSLRequest 'S' reply + rustls (V1 plaintext only);
MD5 auth fallback for legacy clients (PG 14+ deprecated). See SP-PG
design spec §2.2 for the full deferred list.

#### pg_catalog stubs (SP-PG-CAT — V1 closed)

When pgAdmin / DBeaver / DataGrip / Metabase / Tableau / Looker
open a connection, they don't just run `SELECT 1` — they issue
~5-50 introspection queries against `pg_catalog.*` and
`information_schema.*` to populate their UI tree (databases →
schemas → tables → columns → indexes → constraints). V1 of the
SP-PG arc returned `42P01 undefined_table` for every such query,
so GUI tools refused to display the connection. The SP-PG-CAT
arc closes that boundary by intercepting the query at the
dispatch layer (`kessel_pg_gateway::pg_catalog::catalog_query_hook`)
BEFORE the engine apply path and synthesizing a wire-coherent
response from the live KesselDB catalog.

Synthesized catalogs (each one row per the matching KesselDB
entity, with PG-canonical column shapes locked vs the upstream
`src/include/catalog/pg_*.dat` + `pg_*.h` files):

- `pg_namespace` — 3 canned schemas (pg_catalog OID 11, public
  OID 2200, information_schema OID 2202)
- `pg_class` — one row per KesselDB user table; relkind='r'
- `pg_attribute` — one row per (table × column) with the V1
  type-OID map
- `pg_type` — 13 canned rows for the OIDs V1 actually emits
- `pg_index` — one row per KesselDB index (Equality / Range /
  Composite); `indisunique` per the index kind
- `pg_constraint` — one row per UNIQUE / FK / CHECK with
  synthetic constraint names (`<table>_<col>_key` /
  `_fkey` / `_check_N`)
- `information_schema.tables` / `.columns` / `.schemata` /
  `.key_column_usage` / `.table_constraints` — the SQL-standard
  catalog mirror with SQL-standard type names (`bigint`,
  `boolean`, `timestamp with time zone`, ...) — preferred by
  Metabase / Tableau / Looker / dbt over pg_catalog
- `information_schema.views` / `.routines` — well-framed empty
  (KesselDB V1 has no views / stored procedures)
- SQL helper functions: `version()` → `'PostgreSQL 14.0
  (KesselDB 1.0)'`, `current_database()` → `'kesseldb'`,
  `current_schema()` → `'public'`, `current_user` /
  `session_user` → `'kesseldb'`, `pg_table_is_visible(oid)`
  → `true`, `pg_get_userbyid(oid)` → `'kesseldb'`,
  `format_type(oid, typmod)` → canonical type name,
  `current_setting('<guc>')` / `SHOW <guc>` → canned values
  matching the V1 ParameterStatus emit

The intercept is purely additive: every SP-PG V1 KAT continues
to pass because the hook returns `None` for non-pg_catalog SQL
and the existing `engine.apply_sql` path runs unchanged. Indexes
+ constraints round-trip through new admin frames
(`LIST_INDEXES_TAG`=0xF5, `LIST_CONSTRAINTS_TAG`=0xF4) that read
`StateMachine::catalog()` engine-thread-local with no SM mutation
(mirrors the existing `DESCRIBE_BY_NAME_TAG`=0xF7 /
`LIST_TABLES_TAG`=0xF6 admin pattern).

V2-deferred (each named): `pg_proc` real function listing
(SP-PG-CAT-PROC); `pg_stat_*` runtime stats (SP-PG-CAT-STATS);
arbitrary pg_catalog SQL via AST walker (SP-PG-CAT-AST);
psql `\d+` extended output; multi-database `pg_database`
(blocks on KesselDB multi-database support); per-query catalog
cache invalidated on DDL (SP-PG-CAT-CACHE — matters at ≥1000
tables); cross-schema queries (blocks on SP-NS).

See `docs/superpowers/specs/2026-05-27-kesseldb-sppgcat-pg-catalog-design.md`
+ `docs/superpowers/specs/2026-05-27-kesseldb-subproject-sppgcat-progress.md`.

The gateway crate `kessel-pg-gateway` has zero external (non-workspace)
runtime dependencies (only `kessel-proto`, `kessel-client`, `kessel-
crypto`, `kessel-codec`, `kessel-sql`, `kessel-catalog`). The default
`cargo build -p kesseldb-server` (without `--features pg-gateway`) does
not link the gateway crate — `cargo tree -p kesseldb-server --no-default
-features` shows no `kessel-pg-gateway` entry.
