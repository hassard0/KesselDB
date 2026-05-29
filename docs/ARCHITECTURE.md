# KesselDB Architecture

This document is the internals reference. It assumes you've read
[`README.md`](../README.md) (the front door) and at least skimmed
[`docs/USAGE.md`](USAGE.md) (operator + SQL reference).

The structure goes inside-out: foundational seams → replication +
sharding → storage + MVCC → SQL surface → wire protocols → rigor
artifacts → limitations. Every named subsystem has a separate
progress / design spec under `docs/superpowers/specs/`; this doc
summarizes and links — it does not substitute for the per-slice spec
when you're modifying a subsystem.

## The determinism seam

Everything above storage is a pure function over an **injected**
clock, disk, and network (`kessel-io`). Production injects real I/O;
`kessel-sim` injects a seeded, fault-injecting fake. The whole database
runs deterministically from one `u64` seed — this is what makes a
from-scratch VSR reimplementation verifiable rather than hopeful.

`kessel-sm`, `kessel-catalog`, `kessel-codec` contain **zero** I/O /
clock / RNG.

## Crates

**Kernel** (default `cargo build`, zero external dependencies):

| Crate | Role |
|---|---|
| `kessel-proto` | Wire types (Op, OpResult, Field, etc.) |
| `kessel-io` | Clock / disk / net traits + real & sim impls |
| `kessel-storage` | LSM + WAL + recovery + MVCC versioned keyspace + Tx/SI/SSI |
| `kessel-catalog` | Schema as object type 0 |
| `kessel-codec` | Record encode/decode |
| `kessel-sm` | Deterministic apply path + heartbeat watermark |
| `kessel-vsr` | Replication + Jepsen-style linearizability tests |
| `kessel-cache` | Read cache |
| `kessel-shard` | Rendezvous key→shard hashing |
| `kessel-sim` | Fault simulator |
| `kessel-expr` | Gas-bounded expression VM (CHECK / triggers) |
| `kessel-crypto` | SHA-256 + HMAC + PBKDF2 + SHA-1 (RFC 6455 only) |
| `kessel-wasm` | Deterministic WASM-MVP interpreter (UDFs) |
| `kessel-sql` | SQL parser + planner |
| `kessel-bench` | Perf harness |
| `kessel-client` | CLI binary + cluster client library |
| `kesseldb-server` | Node library (+ `scatter_scan` cross-shard fan-out) |
| `kesseldb` | Node binary |

**Optional** (feature-gated, still zero external runtime deps):

| Crate | Feature flag | Role |
|---|---|---|
| `kessel-fetch` | `external-sources*` | HTTP/HTTPS/object-store reader |
| `kessel-objstore` | `external-sources-objstore` | S3 SigV4 + Azure Shared-Key signers |
| `kessel-parquet` | (always; gated by fetch features at use site) | Zero-dep Parquet reader |
| `kessel-http-gateway` | `http-gateway` | HTTP/1.1 + WebSocket surface |
| `kessel-pg-gateway` | `pg-gateway` | PostgreSQL Frontend/Backend v3.0 + pg_catalog stubs |

`kessel-parquet` decodes every nested shape pyarrow writes up to 3-deep
nesting (List, Map, struct, and all cross-products). Supported codecs:
Uncompressed, Snappy, GZIP, zstd, LZ4_RAW, Brotli (6 of 7; legacy
LZ4 framing remaining). Page versions V1 + V2; encodings PLAIN +
RLE_DICTIONARY. The `assembly` module hosts the Dremel record
assemblers. See the OBJ-2c-* design specs for shape coverage.

## Replication (VSR)

Viewstamped Replication ported from TigerBeetle's design:

- Primary assigns op-number + a deterministic timestamp
- Prepare → f+1 PrepareOk → Commit
- Backups apply in op-number order
- View-change on primary timeout; state transfer for lagging replicas
- Client table for exactly-once retried client batches

Fixed cluster size (3 or 5); membership reconfiguration is out of scope
for v1. Correctness is mechanically verified via TLA+ (`Replication.tla`,
528M distinct states, depth 21, 0 violations) and exercised by the
seeded VSR corpus including the historically-hard seed 7
(see SP46 for the diagnosis + fix that closed seed 7 liveness).

## Sharding & cross-shard transactions

Deterministic key→shard mapping (rendezvous hashing over the 20-byte
row key). A deployment runs **K independent VSR shard groups** behind
a **router**: a request goes to the shard that owns its key; schema /
DDL is broadcast to every shard (identical catalogs ⇒ deterministic
per-shard execution); a single-shard transaction stays on that shard's
own VSR group (serializable, fast path).

**Cross-shard transactions are deterministic (Calvin-style), not 2PC.**
A cross-shard `Op::Txn` is decomposed into per-shard slices and a
descriptor is durably **totally ordered** by a dedicated replicated
**sequencer group** (an ordinary VSR cluster; one append op assigns a
gap-free seq, the counter lives in the digest). Each shard then
processes every global seq in order via two phases:

- **decide** — dry-run the slice against committed state and persist a
  stable verdict (applies nothing)
- **commit** — apply the slice iff the global decision (the AND of
  participant verdicts) is commit, else a deterministic atomic skip;
  the per-shard cursor advances lockstep with the global order

Because every verdict is a pure function of that group's durable state,
the global decision is recomputable by any router (no coordinator whose
crash loses the outcome) and no locks are held across shards. Properties:
atomic (all-or-none across shards), exactly-once under client retry
(stable `(client, req)` keying with a digest-resident dedup map), and
recoverable (a full ordered re-drive after a router restart is
idempotent — verdicts are stable, commits cursor-idempotent).

### Cross-shard reads (SP-A scatter scan)

`Op::Select` / `Op::QueryRows` / `Op::SelectFields` / `Op::SelectSorted`
fan out across every shard via the router-side `scatter_scan` helper
in `crates/kesseldb-server/src/scatter_scan.rs`. The client wire stays
unchanged — the router translates each scan into K parallel per-shard
calls and merges the per-shard `OpResult::Got(...)` payloads into a
single byte-shaped result.

**Fan-out** is zero-dep std-thread: one `std::thread` per shard, each
driving a per-shard `ClusterClient` (the `ShardCaller` trait); per-shard
reply channels are bounded `sync_channel(SHARD_BACKPRESSURE_BOUND=4)`
for skew defense.

**Merge** has two strategies discriminated by `ScatterKind`:

- **Unordered** (`Select` / `QueryRows` / `SelectFields`): shard-id-ordered
  concatenation of every shard's `[u32 rowlen][record]*` payload,
  truncated to `LIMIT` rows. Order is deterministic, not arrival-order,
  so the merged bytes are replay-safe.
- **Sorted** (`SelectSorted`): k-way `BinaryHeap` merge of per-shard
  already-sorted streams; `OFFSET` + `LIMIT` are applied in the merge
  loop, not shard-side.

**LIMIT cancellation** uses a shared `Arc<AtomicBool>` flag fired the
instant `output.len() == LIMIT`; late shards see it pre-call so the
router isn't pinned waiting.

**K-invariance** (the headline correctness property): for `SelectSorted`
with unique sort values, the merged output is byte-identical to a K=1
baseline for K ∈ {1, 2, 4, 8, 16}. Locked by SP-A T3's 425-fixture
property sweep at the merge layer plus a real-socket K=1↔K=4
integration test.

See "Limitations & V2 follow-ups" below for the V1 sort-key tie-break
boundary and the cross-shard snapshot non-property.

## Caching

Bounded LRU read cache keyed by `type_id ‖ primary_id`, invalidated by
the state machine on Update / Delete. The write path is the source of
truth and stays deterministic — the cache is a side index off the
committed state, never consulted during `apply`. Default-on under
`StateMachine::open`; digest-invisible.

## Variable-length overflow store

`OverflowRef` fields hold arbitrary-length bytes without breaking the
fixed-width record. The blob travels inside the replicated Create /
Update record as a trailer; the state machine splits it out, writes it
to a reserved LSM keyspace (`type_id = 0xFFFF_FFFF`) under a
deterministic op-derived handle `(op_number << 20) | field_idx`, and
patches the 8-byte handle into the record's `OverflowRef` slot.
Determinism holds because `op_number` is assigned by the VSR primary
and replicated, so every replica computes the same handle and stores
identical bytes. Reads use `GetBlob { handle }`. Orphaned-blob GC
fires precisely at the mutating op: an overflow-field Update frees
the superseded blob; a Delete frees the row's blobs.

## Equality secondary indexes

`ObjectType.indexes` lists indexed field ids (replicated catalog).
Index entries live in a reserved storage type-slot
`0xFFFE0000 | (user_type & 0xFFFF)`; key = `field_id ‖ value_digest8 ‖ pad`;
value = a per-full-value sorted set of object ids. Keys/bytes are
content-derived and id sets sorted, so replicas build a byte-identical
index keyspace (digest-covered). `CreateIndex` backfills via
`Storage::scan_range` over the type's contiguous key range; Create /
Update / Delete maintain indexes inline.

Range-and-composite extensions: `ObjectType.composite` indexes the
concatenation of N field values for multi-column equality (SP27);
`ObjectType.ordered` provides sign-correct 8-byte key ordering for
range scans via `Op::FindRange` (SP15).

## Built-in constraints

`OpResult::Constraint` is a deterministic op result.

- **NOT NULL** derives from `Field.nullable` and is checked against
  the codec null-bitmap, but only for well-formed codec records
  (`len == record_size` and `field_count == #fields`) — raw / opaque
  writers opt out by construction.
- **UNIQUE** (`ObjectType.unique`, always ⊆ `indexes`) consults the
  equality-index bucket on every Create / Update, excluding self.
  `Op::AddUnique` builds the backing index if needed, validates that
  current data has no duplicate (rejecting without half-applying),
  then records the constraint.
- **FOREIGN KEY** (`ObjectType.fks`): on Create / Update each FK field's
  value (padded to a 16-byte id) must resolve via
  `storage.get(make_key(ref_type, id))`. Read-only against committed
  state ⇒ deterministic. `ON DELETE`: `RESTRICT`, `CASCADE`
  (budget-bounded transitive closure), and `SET NULL` (atomic with
  the delete). `ON UPDATE` is inapplicable by model — KesselDB row ids
  are immutable.
- **CHECK** programs (`ObjectType.checks`) run on the deterministic
  expression VM (`kessel-expr`) — pure, gas-bounded, terminating.

## Query planner

`Op::Query` takes a conjunction of `Pred { field_id, op∈{Eq,Ge,Le}, value }`.
The planner fetches and intersects the id-sets of all indexed equality
predicates; if any exist, it verifies every predicate on just those
candidate rows, else it does a filtered `scan_range` over the type's
key range. `cmp_field` compares per kind (numeric for ints / bool /
timestamp, sign-extended for signed / Fixed, lexicographic for byte
kinds). `Query` is read-only and a pure function of committed state,
so it is not logged and is trivially identical across replicas.

SP32 / SP62 extend the planner to SQL `SELECT * WHERE` with indexed
fast paths; SP63 wires composite-index narrowing for queries whose
equality predicates cover an indexed tuple exactly.

## Atomic transactions

`Storage` has a transaction overlay: `begin_txn` buffers writes
in-memory (reads see them — read-your-writes), `commit_txn` flushes
the whole batch to the WAL with a single fsync then makes it visible,
`abort_txn` drops the overlay (nothing reached WAL / memtable ⇒
nothing to undo). `Op::Txn` runs its inner data ops through the normal
`apply` path so constraints / indexes / triggers / overflow all compose
and roll back together; the read cache is cleared on abort. A
transaction is one replicated op, so the serial state machine makes
it serializable and replica-identical. DDL / nested txns are rejected.

SQL `BEGIN / COMMIT / ROLLBACK` (SP55) buffers statements connection-side
and emits one `Op::Txn` at COMMIT.

**Honest perf boundary (SP-Bench-Suite T3, 2026-05-28).** `Op::Txn`
goes through `StateMachine::apply()` and takes the write lock for the
whole transaction — *even when every inner op is read-only*. The
Perf-A T2 parallel-read bypass (`read_only_op` dispatch) is `GetById`-only
and does NOT compose with `Op::Txn`. Under sysbench OLTP read-only
this surfaces as a regression N=1 → N=8 (1,241 → 641 tx/s) because
N workers serialize on the apply lock instead of running their
10-SELECT brackets in parallel. The KesselDB win on sysbench OLTP
write-only is the symmetric story — apply-path is fast at the inner-op
level (53K tx/s at N=8, 5.2× Postgres). Closing the RO/RW gap is the
named follow-up arc **SP-Perf-A-SHARD** (sharded apply queues +
per-shard read pools, OR routing read-only `Op::Txn` through the
read-pool bypass when every inner op is statically detectable as
read-only). See [`docs/BENCHMARKS.md`](BENCHMARKS.md) §3c–§3e for
the full transaction-bracket table.

## Storage + MVCC

LSM key layout has two shapes:

- **Legacy 20-byte** keys: `type_id(4) ‖ primary_id(16)`. Used for
  catalog, indexes, overflow, and internal reserved type-slots.
- **MVCC 28-byte** keys: `type_id(4) ‖ object_id(16) ‖ inverted_commit_opnum(8 BE)`.
  Used for every user-type data row. The inverted op_number puts the
  newest version first under `scan_range`.

WAL frame: `(op_number, kind, type_id, payload, crc32c)`. A type is a
contiguous key range (sets up range scans).

`data_row_dispatch(key)` at the storage layer routes 20-byte
user-type data-row keys (type_id in `(0, 0xFF00_0000)`) through MVCC
primitives at `u64::MAX` snapshot (reads) and `op_number` commit
(writes). The dispatch is a one-helper-function + 4-call-site change
in `Storage::{get, put, delete, scan_range}` covering ~25-35 data-row
I/O sites silently. Replicas reach byte-identical state at every
committed log position (3-replica byte-identity tests gate this).

**Read-fast-path zero-memcpy (SP-Perf-A T7, 2026-05-29).** The
memtable + SSTable cached blocks + transaction overlay all store
values as `Arc<[u8]>` rather than `Vec<u8>`; `Storage::get` returns
the `Arc` clone directly so the engine's `read_only_op` bypass walks
the byte slice without a heap copy. Combined with T2's parallel-read
dispatch (`Arc<RwLock<StateMachine>>` reader bypass), this lifts
point-read throughput to **~4.75M ops/sec at N=16 cores** on the
vulcan reference server with p50 < 1 µs. The honest ceiling at
~5M ops/sec is the `RwLock<StateMachine>` reader CAS ping-pong; the
named follow-up **SP-Perf-A-SHARD** sharded apply queues + per-shard
read pools is what unlocks the next order of magnitude.

**Isolation**:

- Snapshot reads via `Tx::begin`
- Snapshot Isolation write-side via `Tx::begin_rw` (SP112)
- Cahill serializable SSI via `Tx::begin_ssi` (SP113 — write-skew
  impossible by construction)

**GC**: `Op::AdvanceWatermark` is a deterministic op in the apply
path (SP114); a heartbeat closure (SP115) submits it. The whole stack
is mechanically verified across 7 layered TLA+ modules.

## Deterministic WASM UDFs

`kessel-wasm` is a from-scratch zero-dep WASM-MVP-subset interpreter
for `CHECK` constraints and triggers.

Supported instruction surface: i32 / i64 / f32 / f64 values + arithmetic
+ comparison + control flow + locals + in-module call + linear memory
(load / store / size / grow) + tables + call_indirect with runtime
type_idx equality check + bit-manipulation (clz / ctz / popcnt) +
sign-extension + canonical NaN (`0x7FC0_0000` / `0x7FF8_0000_0000_0000`)
per WASM determinism rules.

Gas-bounded at 1 unit per executed instruction; trap `WasmError::OutOfGas`
on limit. A bounds-checked decoder + opcode allow-list distinguishes
"valid WASM-MVP unsupported" from "invalid garbage". A UDF is part of
the replicated catalog; every replica runs byte-identical logic; UDF
behavior is replayable from the log. 113+ hand-derived KATs against
the official WASM-MVP spec.

## Wire protocol gateways

KesselDB exposes the same `Op` apply path through four wire surfaces.
The binary protocol is the default + the deterministic fast path;
every other listener is opt-in via a cargo feature, runs on a sibling
TCP socket, and is byte-untouched by the binary protocol. See
[`docs/USAGE.md`](USAGE.md) §9 (PostgreSQL) and §10 (HTTP + WebSocket)
for operator-side configuration; this section covers the engine-side
plumbing.

Each listener has its own `max_conns` cap (per-listener, not joint —
so a saturated HTTP gateway can never starve the binary protocol).
The shared engine `max_inflight` cap bounds total in-flight ops
across all listeners honestly.

### Binary protocol

The deterministic hot path on the primary port. Length-prefixed
`Op::encode` payloads framed by a 1-byte kind tag; replies are
`OpResult::encode`. This is what the SP69 pipelined-batch perf
number measures and what every replication / VSR / Jepsen oracle
exercises. Bearer-token authed via the `0xFC` handshake frame
(SP43).

### HTTP gateway (`--features http-gateway`)

Translates HTTP/1.1 requests into the same engine apply path via the
`kessel_http_gateway::EngineApply` trait that `EngineHandle` impls.
Routes: `/v1/sql`, `/v1/op`, `/v1/health`, `/v1/metrics`, `/v1/ws`.
Bearer auth shared with the binary protocol. HTTPS variant runs on a
third listener via the existing rustls config used by the binary
listener (with the `tls` feature).

The gateway crate `kessel-http-gateway` has zero external (non-workspace)
runtime dependencies. Default `cargo build -p kesseldb-server` (without
`--features http-gateway`) does not link the gateway crate — `cargo tree`
verifies the binary stays untouched.

### WebSocket arm (under `--features http-gateway`)

The WebSocket arm of the HTTP gateway exposes a long-lived `/v1/ws`
upgrade that frames raw `Op::encode()` payloads under the
`kessel-op-v1` subprotocol. RFC 6455 strict handshake + binary frames
only + bounded send queue (16 messages) + 30s ping/pong heartbeat.
There is no separate `ws-gateway` feature — the WebSocket session
model lives inside `kessel-http-gateway` alongside the HTTP routes;
the crate, the Bearer auth surface, and the `EngineApply` trait are
shared.

### PostgreSQL wire (`--features pg-gateway`)

Speaks the **PostgreSQL Frontend/Backend Protocol v3.0** — the same
wire that libpq / psql / pgcli / JDBC / psycopg / pgx / tokio-postgres /
sqlx-pg all speak. Per-connection `std::thread`. Connection cap
defaults to `DEFAULT_MAX_PG_CONNS = 256`; the PG and HTTP caps are
independent.

V1 scope:

- **Simple Query** (`Q` message): single statement per `Q`,
  multi-statement rejected with `42601`. Streaming
  `RowDescription` → `DataRow`* → `CommandComplete` → `ReadyForQuery`
  per query.
- **Extended Query (SP-PG-EXTQ V1, 2026-05-29)** — full V1 message set
  `P` (Parse) / `B` (Bind) / `D` (Describe) / `E` (Execute) / `S` (Sync) /
  `C` (Close) / `H` (Flush). Per-connection `SessionState` holds named +
  unnamed prepared statements + portals up to
  `MAX_PREPARED_STATEMENTS_PER_CONN = MAX_PORTALS_PER_CONN = 4096`.
  Parse stores SQL VERBATIM (no parse, no AST cache — SQL parse errors
  surface at Execute time so the engine catalog state governs the
  message). Bind validates parameter format codes (V1 rejects binary
  with `0A000` — V2 SP-PG-EXTQ-BIN), enforces parameter count vs Parse's
  OID hints (mismatch → `08P02`), and stores text-format parameter
  values into the portal. Describe 'S' emits ParameterDescription +
  RowDescription/NoData; Describe 'P' emits RowDescription/NoData
  (parameters frozen at Bind time per PG §55.2.3). Execute substitutes
  `$N` text-format parameters into the SQL and dispatches through
  `EngineApply::apply_sql`; `max_rows > 0` emits `PortalSuspended`
  with buffered cursor state so a re-Execute resumes pagination.
  Sync emits `ReadyForQuery('I')`, clears the per-connection
  `error_state` (set on any prior dispatch error), and drops the
  unnamed portal. Close drops the named statement or portal; CloseComplete
  emitted on success even for missing-name no-ops per PG §55.2.3.
  Flush triggers an outbound stream flush (no bytes, no state change).
  **End-to-end verification**: a real `psycopg2.connect(...)` +
  `cur.execute("SELECT * FROM pgtest WHERE id = %s", (42,))` returns
  real rows on vulcan (SP-PG-EXTQ T5 / commit `cec17c4`). Full ORM-suite
  smoke against SQLAlchemy + JDBC + Drizzle + Prisma is post-V1.1
  (SP-PG-EXTQ T8 / T11 / T12 — still OPEN at the time of writing).
- **SCRAM-SHA-256 auth** (RFC 5802 + RFC 7677, 4096 iterations) via the
  **Bearer ↔ SCRAM bridge**: the operator's `ServerConfig.token` IS the
  SCRAM password input. One credential surface; rotating the Bearer
  token rotates both HTTP-Bearer and PG-SCRAM atomically.
- **Type-OID mapping** for KesselDB `FieldKind` → PG type catalog
  (Bool, int2/4/8, text, bytea, timestamptz, numeric). Text-format
  wire encoding only — binary-format wire is V2 SP-PG-EXTQ-BIN.
- **Cap-overflow rejection** as wire-level `ErrorResponse('S=FATAL',
  'C=53300', 'M=sorry, too many clients already')` emitted before the
  close, so libpq surfaces the structured rejection.
- **Idle timeout** (`pg_idle_timeout`, default 600s) emits FATAL
  `57014 terminating connection due to idle timeout` before close.
- **OpResult → SQLSTATE** mapping: `Exists` → `23505`, `Unauthorized` →
  FATAL `28000`, `Unavailable` → FATAL `57P03`, `SchemaError(msg)` →
  `42P01` / `42703` / `42804` / `42601` / `42000` (string-match
  heuristic), `Constraint` → `23502` / `23505` / `23503` / `23514` /
  `23000`, `TxAborted` → `40001` / `25006` / `58030`.
- **Scatter-scan transparency**: PG-wire dispatches every SQL through
  `EngineApply::apply_sql`; the underlying engine routes scan-shaped
  ops via `Route::Scatter` and merges per-shard `OpResult::Got` slots.
  PG-wire is byte-identical between K=1 and K=N.

#### pg_catalog stubs (SP-PG-CAT)

GUI tools (pgAdmin / DBeaver / DataGrip / Metabase / Tableau / Looker)
issue 5-50 introspection queries against `pg_catalog.*` and
`information_schema.*` to populate their UI tree on connect. SP-PG V1
returned `42P01 undefined_table` for every such query, so GUI tools
refused to display the connection. SP-PG-CAT closes that boundary by
intercepting the query at the dispatch layer
(`kessel_pg_gateway::pg_catalog::catalog_query_hook`) BEFORE the
engine apply path and synthesizing a wire-coherent response from the
live KesselDB catalog.

Synthesized catalog tables (PG-canonical column shapes locked vs the
upstream `src/include/catalog/pg_*.dat` + `pg_*.h` files):

- `pg_namespace` — 3 canned schemas (pg_catalog, public,
  information_schema)
- `pg_class` — one row per KesselDB user table
- `pg_attribute` — one row per (table × column) with the V1 type-OID
  map
- `pg_type` — 13 canned rows for the OIDs V1 actually emits
- `pg_index` — one row per KesselDB index
- `pg_constraint` — one row per UNIQUE / FK / CHECK
- `information_schema.tables` / `.columns` / `.schemata` /
  `.key_column_usage` / `.table_constraints` — the SQL-standard
  catalog mirror with SQL-standard type names (preferred by Metabase /
  Tableau / Looker / dbt over `pg_catalog`)
- `information_schema.views` / `.routines` — well-framed empty

SQL helper functions: `version()`, `current_database()`,
`current_schema()`, `current_user`, `session_user`,
`pg_table_is_visible(oid)`, `pg_get_userbyid(oid)`,
`format_type(oid, typmod)`, `current_setting('<guc>')`, `SHOW <guc>`.

Indexes + constraints round-trip through admin frames
`LIST_INDEXES_TAG=0xF5` and `LIST_CONSTRAINTS_TAG=0xF4` that read
`StateMachine::catalog()` engine-thread-local with no SM mutation
(mirrors the existing `DESCRIBE_BY_NAME_TAG=0xF7` /
`LIST_TABLES_TAG=0xF6` admin pattern). The intercept is purely
additive: every SP-PG V1 KAT continues to pass because the hook
returns `None` for non-pg_catalog SQL and the existing `engine.apply_sql`
path runs unchanged.

The gateway crate `kessel-pg-gateway` has zero external (non-workspace)
runtime dependencies. Default `cargo build -p kesseldb-server` (without
`--features pg-gateway`) does not link the gateway crate.

## Mechanically-checked rigor artifacts (S1, S3)

**`kesseldb-tla/`** — seven layered TLA+ modules with TLC baselines:

| Module | Slice | TLC baseline |
|---|---|---|
| `Replication.tla` | S1 / SP109 | 528M states, depth 21, 0 violations |
| `MVCCStorage.tla` | SP110 | (see results/) |
| `MVCCTx.tla` | SP111 | 7.36M states, depth 8 |
| `MVCCSi.tla` | SP112 | 3.73M states, depth 13 |
| `MVCCSsi.tla` | SP113 | 348K states, depth 9 |
| `MVCCGc.tla` | SP114 | 1.59M states, depth 12 |
| `MVCCCutover.tla` | SP115 / SP116 | 15.08M states, depth 17 |

Every module preserves prior invariants; the SP109–SP116 discipline
is "never weaken a test" — refinements tighten or restate.

**Jepsen-style multi-replica linearizability (S3, SP117)** — 5
hand-derived Jepsen tests in `kessel-vsr::sim::tests` validate that
the SP116 storage-layer transparent MVCC dispatch preserves
linearizability across the full VSR + MVCC stack under partition +
message loss. `Cluster::drive_until_digests_converge` extends the
simulation past replies-complete so isolated minority replicas finish
state-transfer and catch up.

## Limitations & v2 follow-ups

Consolidated list of named deferrals across the codebase. Each is a
deliberate boundary, not a hidden gap.

### Cross-shard reads (SP-A)

- **Sort-key tie-break by `(value, shard_id)`, not `(value, object_id)`**.
  Deterministic + reproducible for fixed K, but cross-K with sort-value
  ties may order tied rows differently. Per-shard records don't carry
  the object_id (it's the storage key, not the record), so an
  oid-based tie-break would require a new `Op::SelectSortedWithKey`.
  Deferred until a workload needs it; the 85-seed K-invariance sweep
  confirmed this is acceptable for V1 because unique sort values are
  the common case.
- **Cross-shard snapshot is not consistent**: a scatter read can see
  rows committed on shard A at opnum_a=100 and rows on shard B at
  opnum_b=200 (independent counters). Cross-shard consistent snapshot
  is an explicit non-goal.
- **`ShardCaller::call` cannot be interrupted** mid-reply (`std::net::TcpStream`
  has no cancellable read); per-shard `read_timeout` (default 30s) is
  the upper bound on cancel latency.
- **Hard-fail by default**: a single per-shard non-`Got` slot poisons
  the merged result. Opt-in best-effort mode via
  `ScatterContext::partial_on_timeout` lets callers receive the
  surviving shards' rows plus a `Vec<u32>` of failed shard ids.

**Out of arc** (each its own future arc): SP-B aggregate combine,
SP-C streamed sorted merge over indexes, SP-D `GroupAggregate`
cross-shard combine, SP-E SQL-text routing. Cross-shard `Join`
remains an explicit non-goal.

`FindBy` / `FindByComposite` extend SP-A via the `OidConcat`
`ScatterKind` — the per-shard secondary index doesn't carry rows
from other shards, so the router fans out an indexed-equality lookup
to every shard and unions the resulting oid sets.

### PostgreSQL wire

V2 follow-ups (each its own arc). **Extended Query has SHIPPED at V1.1
(SP-PG-EXTQ); it is no longer on this list.** What remains:

- **Binary-format wire encoding** (per-column negotiated in `Bind`) —
  SP-PG-EXTQ-BIN
- **`RETURNING`** clause
- **`CancelRequest`** action (V1 generates BackendKeyData but takes
  no action)
- **GUC plumbing** for `SET timezone` etc.
- **COPY FROM STDIN / COPY TO STDOUT** — SP-PG-COPY
- **TLS** via SSLRequest 'S' reply + rustls (V1 plaintext only)
- **MD5 auth fallback** for legacy clients (PG 14+ deprecated)
- **SCRAM channel binding** (`SCRAM-SHA-256-PLUS`)

See SP-PG design spec §2.2 for the full deferred list.

### pg_catalog stubs

- **`pg_proc`** real function listing (SP-PG-CAT-PROC)
- **`pg_stat_*`** runtime stats (SP-PG-CAT-STATS)
- **Arbitrary pg_catalog SQL** via AST walker (SP-PG-CAT-AST) — V1
  handles named queries only; ad-hoc JOINs against catalog tables
  fall through to the engine and error
- **psql `\d+`** extended output
- **Multi-database `pg_database`** (blocks on KesselDB multi-database
  support)
- **Per-query catalog cache** invalidated on DDL (SP-PG-CAT-CACHE —
  matters at ≥1000 tables)
- **Cross-schema queries** (blocks on SP-NS)

### Parquet

- **4+ deep nesting** (`List<List<List<List<T>>>>` etc.) rejects with
  a typed `Unsupported` error awaiting a real pyarrow fixture that
  exercises that depth — synthetic tests don't justify the
  classifier extension
- **Legacy LZ4 framing** (codec id 5) — pyarrow ≤ 8 default; modern
  pyarrow uses LZ4_RAW (codec id 7) which IS supported

### Storage / SQL

- **`ON DELETE SET DEFAULT`** — needs per-column defaults first
- **`ON UPDATE`** referential actions — inapplicable by model (row
  ids are immutable; the trigger has no condition under which it
  could ever fire)
- **`ALTER TABLE DROP / ALTER COLUMN`**, **`DROP INDEX`** — only
  `ADD COLUMN` and `DROP TABLE` are wired
- **Auto-id / sequences** — callers supply the 16-byte object id
  today
- **Range / composite index narrowing for `>` / `<`** — equality
  predicates narrow via the planner; range predicates still verify
  via the expression VM

### Operations

- **TLS for the binary protocol** is implemented (rustls); TLS for
  HTTP and PG wire is V2
- **No incremental backup / PITR** — `Op::Snapshot` produces a flat
  crash-consistent copy
- **No per-table or role-based authz** beyond shared Bearer token /
  SCRAM password
