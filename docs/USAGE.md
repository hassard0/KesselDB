# KesselDB — Usage Guide

Everything you need to install, run, query, cluster, secure, and operate
KesselDB. Every feature described here is covered by the test suite.

- [1. Install & build](#1-install--build)
- [2. Run a server](#2-run-a-server)
- [2b. The `kessel` command-line client](#2b-the-kessel-command-line-client)
- [3. The client library](#3-the-client-library)
- [4. SQL reference](#4-sql-reference)
- [5. The data model](#5-the-data-model)
- [6. Transactions](#6-transactions)
- [7. Running a cluster](#7-running-a-cluster)
- [7b. Sharded deployment & cross-shard transactions](#7b-sharded-deployment--cross-shard-transactions)
- [7c. External sources (JSON/CSV over HTTP)](#7c-external-sources-jsoncsv-over-http)
- [7d. Paginated & NDJSON sources](#7d-paginated--ndjson-sources)
- [7e. Object-store sources (S3 / Azure Blob)](#7e-object-store-sources-s3--azure-blob)
- [7f. FORMAT PARQUET for object-store sources](#7f-format-parquet-for-object-store-sources)
- [8. Authentication, quotas & backpressure](#8-authentication-quotas--backpressure)
- [9. PostgreSQL clients (psql, pgcli, JDBC, psycopg, pgx, …)](#9-postgresql-clients-psql-pgcli-jdbc-psycopg-pgx-)
- [10. HTTP gateway (and WebSocket)](#10-http-gateway)
- [11. Backup & monitoring](#11-backup--monitoring)
- [12. Wire protocol](#12-wire-protocol)
- [13. Troubleshooting](#13-troubleshooting)

---

## 1. Install & build

### Option A — download a prebuilt binary (Linux x86_64)

KesselDB ships prebuilt server (`kesseldb`) and CLI (`kessel`) binaries for
`x86_64-unknown-linux-gnu` on the [GitHub Releases page](https://github.com/hassard0/KesselDB/releases).
Each release is built from `cargo build --release --features
pg-gateway,http-gateway`, so the PostgreSQL, HTTP/1.1, and WebSocket
gateways are wired in.

```bash
VER=v1.0.0   # see the releases page for the latest
curl -L https://github.com/hassard0/KesselDB/releases/download/$VER/kesseldb-$VER-x86_64-unknown-linux-gnu \
  -o kesseldb && chmod +x kesseldb
curl -L https://github.com/hassard0/KesselDB/releases/download/$VER/kessel-$VER-x86_64-unknown-linux-gnu \
  -o kessel    && chmod +x kessel

# or grab the bundle (server + CLI + README + USAGE + LICENSE):
curl -L https://github.com/hassard0/KesselDB/releases/download/$VER/kesseldb-$VER-x86_64-unknown-linux-gnu.tar.gz \
  | tar xz

# SHA-256 checksums are published alongside the binaries:
curl -L https://github.com/hassard0/KesselDB/releases/download/$VER/SHA256SUMS -o SHA256SUMS
sha256sum -c SHA256SUMS --ignore-missing
```

### Option B — run from the official Docker image

A pre-published multi-arch image (`linux/amd64` + `linux/arm64`) is
pushed to GitHub Container Registry on every `v*` release. The image
is the existing `--features pg-gateway,http-gateway` server, runs as a
non-root `kessel:1100` UID, and exposes all three wire surfaces.

```bash
# Pull and run, mounting a host data dir + a one-token auth surface.
docker run --rm \
  -p 6532:6532 -p 6533:6533 -p 5432:5432 \
  -v $PWD/kesseldb-data:/data \
  -e KESSELDB_TOKEN=changeme \
  ghcr.io/hassard0/kesseldb:latest

# From another shell, the bare kessel CLI works exactly like local:
kessel --addr 127.0.0.1:6532 --token changeme \
  'CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)'
# or via the HTTP gateway on :6533, or psql on :5432.
```

Default ports inside the container: `6532` binary, `6533` HTTP+WS,
`5432` PostgreSQL. Persist the data dir with `-v <host>:/data`. The
image is rebuilt from `Dockerfile` at the repo root — see that file
for the two-stage layout (rust:1-slim builder → debian-bookworm-slim
runtime, ~77 MiB stripped).

### Option C — build from source

KesselDB is pure Rust with **no external dependencies** in the kernel and
no native build steps.

```bash
git clone https://github.com/hassard0/KesselDB && cd KesselDB
cargo build --release                                # default — binary protocol only, no gateway code linked
cargo build --release --features pg-gateway,http-gateway   # all wire surfaces
cargo test --workspace                               # 2442 default tests
cargo test --workspace --features pg-gateway         # 2470 (adds SP-PG + SP-PG-CAT + SP-PG-EXTQ V1 + V2 hardening + SP-PG-COPY V1)
cargo test --workspace --features pg-gateway,http-gateway,kessel-http-gateway/test-server   # 2503 — full matrix
```

Requires Rust stable **1.95+**.

Workspace crates (use the ones you need as path/library deps):

| Crate | Purpose |
|---|---|
| `kesseldb-server` | runnable node, engine, single‑node + cluster servers |
| `kessel-client` | blocking TCP client (`Client`, `ClusterClient`) |
| `kessel-sql` | SQL tokenizer + planner (`compile_stmt`) |
| `kessel-sm` | deterministic state machine |
| `kessel-storage` | LSM + WAL + bloom + bounded compaction |
| `kessel-vsr` | Viewstamped Replication + seeded simulator |
| `kessel-proto` / `kessel-catalog` / `kessel-codec` / `kessel-expr` | wire types, schema, record codec, expression VM |

## 2. Run a server

The `kesseldb` binary runs a **single, open node** (no auth) — the simplest way
to get going:

```bash
# kesseldb [LISTEN_ADDR] [DATA_DIR]
cargo run --release --bin kesseldb -- 127.0.0.1:7878 ./data
# defaults: 127.0.0.1:7878  ./kesseldb-data
```

The data directory holds the WAL, SSTables and manifest. Stop and restart the
process and it recovers from the WAL automatically (crash‑safe, torn‑tail
handled).

For authentication, quotas, or a multi‑node cluster you compose the
`kesseldb-server` **library** API — see §7 and §8.

## 2b. The `kessel` command-line client

Query KesselDB without writing any code — the fastest path for humans,
scripts, ops, and agents.

```bash
# one-shot (exit 0 = success, 1 = statement/connection error, 2 = bad usage)
cargo run -q -p kessel-client --bin kessel -- "CREATE TABLE t (v U64 NOT NULL)"
cargo run -q -p kessel-client --bin kessel -- "INSERT INTO t ID 1 (v) VALUES (42)"
cargo run -q -p kessel-client --bin kessel -- "SELECT SUM(v) FROM t"   # => = 42

# a whole-row SELECT prints a real aligned table (no DESCRIBE needed):
#   owner | bal
#   ------+----
#   100   | 50
#   (1 row)
kessel "SELECT * FROM t ID 1"
kessel "SELECT * FROM t WHERE owner = 100"
kessel "SELECT owner, bal FROM acct"      # projections render too
kessel "SELECT * FROM a JOIN b ON a.x = b.y"   # JOINs render too (self-describing)

# pipe a .sql file (lines starting with # or -- are comments; blanks ignored)
cat schema.sql | cargo run -q -p kessel-client --bin kessel

# machine-readable: one JSON object per statement (ideal for agents)
kessel --json "SELECT * FROM t"
#   {"status":"ok","rows":[{"v":42}]}
kessel --json "SELECT SUM(v) FROM t"      # {"status":"ok","value":42}
kessel --json "DESCRIBE t"                # {"status":"ok","table":"t","columns":[…]}
kessel --json "SELECT * FROM nope"        # {"status":"error","message":"…"}  (exit 1)

# DESCRIBE / \d render a readable schema (text mode):
#   table t
#   column | type | null
#   -------+------+-----
#   v      | U64  | NO
kessel "DESCRIBE t"

# interactive shell (TTY): a `kessel>` prompt
cargo run -q -p kessel-client --bin kessel
#   \?  \h  \help      list shell commands
#   \d <table>         describe a table
#   \timing            toggle per-statement timing
#   \q  quit  exit     leave

# remote / authenticated
kessel --addr 10.0.0.1:7878 --token s3cret "SELECT * FROM t ID 1"
```

`kessel [--addr HOST:PORT] [--token TOKEN] [--json] ["SQL"]` — default
address `127.0.0.1:7878`. With no SQL argument it reads statements from
stdin (one per line). The **exit code is reliable** (0 ok, 1
statement/connection error, 2 bad usage) and `--json` emits one stable
object per statement, so an agent or script can branch on success
without parsing prose. (After `cargo build --release` the binary is
`target/release/kessel`.)

## 3. The client library

`kessel-client` is a minimal blocking client. Add it as a path dependency, or
copy the wire protocol (§10) into any language.

**Python** — a dependency-free, stdlib-only reference client ships at
[`clients/python/kesseldb.py`](../clients/python/kesseldb.py):

```python
from kesseldb import connect
db = connect("127.0.0.1:7878")            # connect(addr, token=b"..") for auth
db.sql("CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)")
db.sql("INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)")
print(db.sql("SELECT SUM(bal) FROM acct WHERE owner = 100").value)  # 50
db.close()
```

Or one-shot: `python clients/python/kesseldb.py "SELECT …" [--addr H:P]
[--token T]` (exit 0 ok / 1 error / 2 usage). It is a faithful, tested
implementation of §10 — the template for an SDK in any language.

### Single node

```rust
use kessel_client::Client;
use kessel_proto::{Op, ObjectId, OpResult};

let mut db = Client::connect("127.0.0.1:7878")?;

// SQL (compiled server‑side against the live catalog):
db.sql("CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)")?;
db.sql("INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)")?;
let r = db.sql("SELECT SUM(bal) FROM acct WHERE owner = 100")?;

// Low‑level ops (no SQL parse), if you want them:
db.call(&Op::Create { type_id: 1, id: ObjectId::from_u128(2), record: vec![/* codec bytes */] })?;
db.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(2) })?;
```

`OpResult` variants you will see: `Ok`, `Got(bytes)`, `Exists`, `NotFound`,
`TypeCreated(id)`, `Constraint(msg)`, `SchemaError(msg)`, plus the transport
signals `Unavailable` (not the primary — try another node) and `Unauthorized`.

### Authenticated connection

```rust
let mut db = Client::connect_authed("127.0.0.1:7878", b"my-shared-secret")?;
```

### Cluster client (automatic failover, exactly‑once)

```rust
use kessel_client::ClusterClient;

let mut db = ClusterClient::new(vec![
    "10.0.0.1:7878".into(), "10.0.0.2:7878".into(), "10.0.0.3:7878".into(),
]);                                  // .with_token(b"secret".to_vec()) if authed

db.call(&op)?; // finds the primary, retries the *same* (client,req) on
               // Unavailable/connection loss — never double‑applies
```

`ClusterClient` holds a stable session id and a monotonic request number, so a
retry after a primary change returns the original committed reply rather than
re‑executing.

### Embedded — KesselDB inside your Rust process

Skip the network round-trip entirely: depend on `kesseldb-server` directly
and call the engine in-process. Read paths take the SP‑Perf‑A bypass under
an `RwLock::read()` (sub‑µs latency); writes still serialise through the
engine thread's deterministic apply.

```rust
use kesseldb_server::{spawn_engine_cfg, ServerConfig};
use kessel_proto::OpResult;

let cfg = ServerConfig { read_workers: Some(0), ..Default::default() };
let engine = spawn_engine_cfg("./kesseldb-data", &cfg)?;

// SQL fast path — same compile + apply as a wire `[0xFE] ++ sql` frame,
// minus the socket.
engine.sql("CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)");
engine.sql("INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)");
match engine.sql("SELECT SUM(bal) FROM acct WHERE owner = 100") {
    OpResult::Got(b) => println!("sum = {}", i128::from_le_bytes(b[..16].try_into().unwrap())),
    other            => panic!("{other:?}"),
}

// Hot consistent backup — copies the data dir while no apply is in flight.
engine.snapshot("./kesseldb-data.snap")?;
```

A complete walkthrough — typed `Op` fast path, `kessel_codec::encode`
round-trip, snapshot — lives at
[`crates/kesseldb-server/examples/embedded.rs`](../crates/kesseldb-server/examples/embedded.rs).
Run it from the repo root:

```bash
cargo run --release --example embedded -p kesseldb-server
```

## 4. SQL reference

Compiled server‑side against the live catalog. Supported surface (each item is
tested):

### DDL

```sql
CREATE TABLE <t> (<col> <TYPE> [NOT NULL], ...)
ALTER TABLE <t> ADD [COLUMN] <c> <TYPE> [NOT NULL]  -- online, no lock; old rows: NULL
DROP TABLE <t>                              -- removes rows, indexes & the type
                                            -- (refused if an FK still points at it)
CREATE INDEX        ON <t> (<col>)          -- equality index
CREATE UNIQUE INDEX ON <t> (<col>)          -- unique constraint + index
CREATE RANGE  INDEX ON <t> (<col>)          -- order‑preserving (range scans)
CREATE INDEX        ON <t> (<c1>, <c2>)     -- composite
DESCRIBE <t>                                -- returns the table definition
EXPLAIN <stmt>                              -- prints the plan, runs nothing
```

Column types: `U8 U16 U32 U64`, `I8 I16 I32 I64`, `BYTES`, `BOOL`.

### DML

```sql
INSERT INTO <t> ID <n> (<cols>) VALUES (<vals>)            -- legacy single-row
INSERT INTO <t> (id, <cols>) VALUES (<v>)[, (<v>)]*       -- Postgres-shaped;
                                                          -- multi-row = 1 atomic op
UPDATE <t> ID <n> SET <col> = <val> [, ...]      -- server‑side read‑modify‑write
DELETE FROM <t> WHERE <col> = <val>
```

### Queries

```sql
SELECT * FROM <t> ID <n>                         -- O(1) primary‑key fetch
SELECT * FROM <t> [WHERE <expr>]                 -- =, !=, <, <=, >, >=, AND/OR/NOT,
                                                 --   col IN (a,b,..), col BETWEEN lo AND hi
                                                 --   col IS [NOT] NULL, col [NOT] LIKE 'pat%' (NOT IN / NOT BETWEEN too)
SELECT <c1>, <c2> FROM <t> [WHERE ...]           -- projection
SELECT COUNT(*) | SUM(c) | MIN(c) | MAX(c) | AVG(c) FROM <t> [WHERE ...]
       [GROUP BY <col>]
SELECT * FROM <t> [WHERE ...] ORDER BY <col> [DESC] [OFFSET n] [LIMIT n]
SELECT * FROM <a> JOIN <b> ON <a.x> = <b.y> [LIMIT n]   -- inner equi‑join
```

`WHERE` supports `AND`/`OR`/`NOT`, all of `= != < <= > >=`, and `IN`/`BETWEEN` (incl. `NOT IN`/`NOT BETWEEN`). `SELECT *` returns
length‑prefixed record blobs; use `DESCRIBE <t>` to decode them against the
schema (the client decodes the wire schema for you).

> **Note:** rows carry an explicit caller‑supplied `ID` (a 128‑bit key). There
> is no auto‑increment — the engine never generates ids, because that would
> introduce non‑determinism into the replicated state machine. Generate ids in
> your application (UUID, snowflake, etc.).

## 5. The data model

- **Tables** are runtime‑defined (`CREATE TABLE`) and can be altered online
  (add field) without downtime.
- **Records** are fixed‑width per the schema; variable‑length values use an
  overflow store transparently.
- **Constraints**: `NOT NULL`, `UNIQUE`, foreign keys
  (`ON DELETE RESTRICT | CASCADE | SET NULL`), `CHECK` (a deterministic,
  gas‑bounded expression program).
- **Triggers**: before‑write programs that may mutate or reject a row — same
  zero‑dep deterministic VM as `CHECK`.
- **Indexes**: equality, unique, order‑preserving (range), and composite.

Everything is applied through one deterministic state machine, so a given
sequence of operations always produces the same state and the same content
digest on every replica.

## 6. Transactions

**SQL** (single-node server) — `BEGIN` buffers subsequent statements;
`COMMIT` applies them as one atomic unit; `ROLLBACK` discards them:

```sql
BEGIN;
INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50);
INSERT INTO acct ID 2 (owner, bal) VALUES (100, 999);
COMMIT;          -- both rows land atomically; any failure aborts ALL
```

```bash
printf 'BEGIN\nINSERT INTO acct ID 9 (owner,bal) VALUES (1,1)\nCOMMIT\n' | kessel
```

A failing statement (e.g. a duplicate id) makes `COMMIT` fail and rolls
back every statement in the transaction; the connection stays usable.
`COMMIT`/`ROLLBACK` without `BEGIN` is a clean error. `UPDATE` composes
inside a transaction (it lowers to the deterministic replicated
`Op::UpdateSet`), and read-your-writes holds for writes within the
batch (a later statement sees an earlier one's effect).

**Model boundary (by design, not a TODO):** a KesselDB transaction is
an *atomic, non-interactive write batch* — serializable by
construction. A `SELECT`/`DESCRIBE`/`EXPLAIN` *inside* `BEGIN`/`COMMIT`
is **rejected with a clear error**: returning interactive
read-your-writes mid-transaction would require holding the single
engine overlay across client round-trips, serializing the whole engine
— a deliberate non-goal. Run reads outside the transaction.
`UPDATE … SET col = NULL` *inside* a transaction is the one
unsupported write form (clear error; works outside a txn).
Transactions are per-connection and single-node (the cluster front
doesn't intercept the keywords — use op-level `Op::Txn` there).

**Op level** (works everywhere, incl. the cluster) — atomic,
all‑or‑nothing, replicated as a single operation:

```rust
use kessel_proto::Op;
db.call(&Op::Txn { ops: vec![
    Op::Create { type_id: 1, id: a, record: ra },
    Op::Create { type_id: 1, id: b, record: rb },
]})?; // both apply, or neither — any failure rolls the whole batch back
```

If any inner op fails a constraint, the entire transaction is rejected with no
visible side effects.

## 7. Running a cluster

A cluster is composed from the `kesseldb-server` library. Each node runs the
deterministic engine wrapped in a VSR replica; nodes talk over TCP.

```rust
use kesseldb_server::cluster::{spawn_node, serve_clients};
use std::net::TcpListener;
use std::sync::Arc;

// Peer addresses, indexed by node id; all nodes share the same list.
let peers = vec![/* SocketAddr per node */];
let peer_listener = TcpListener::bind(my_peer_addr)?;       // this node's VSR socket
let node = Arc::new(spawn_node(my_idx, peer_listener, peers, "./data".into())?);

// Expose the ordinary client protocol for apps:
let client_listener = TcpListener::bind(my_client_addr)?;
serve_clients(client_listener, node.clone());
```

Properties (all tested, including a seeded adversarial partition corpus):

- **Safety**: a committed, client‑acknowledged operation is never lost across a
  view change.
- **Liveness**: once a quorum can communicate again, the cluster completes
  outstanding work and every replica reconverges to an identical digest.
- **Exactly‑once**: any node serves a committed `(client, req)` from its
  replicated client table; `ClusterClient` retries the same `(client, req)` on
  failover without re‑executing.

Connect applications with `ClusterClient` (§3). It rotates the address list and
retries on `Unavailable` until it reaches the primary.

### 7b. Sharded deployment & cross-shard transactions

For horizontal scale, run **K independent shard groups** (each a
cluster as above) plus one small **sequencer group**, with a
**router** in front (`kesseldb_server::router`):

```rust
use kesseldb_server::router::{Router, serve_router, recover};

let router = std::sync::Arc::new(
    Router::new(vec![
        vec!["shard0a:7878".into(), "shard0b:7878".into(), "shard0c:7878".into()],
        vec!["shard1a:7878".into(), "shard1b:7878".into(), "shard1c:7878".into()],
    ])
    .with_sequencer(vec!["seqa:7878".into(), "seqb:7878".into(), "seqc:7878".into()]),
);
serve_router(listener, router.clone());      // speaks the ordinary client wire
// after a router restart, finish any in-flight cross-shard txns:
recover(&router).unwrap();
```

The router sends each request to the shard that owns its key
(rendezvous mapping); schema/DDL is broadcast to every shard. A
single‑shard transaction stays on its shard's own VSR group. A
**cross‑shard `Op::Txn`** is decomposed into per‑shard slices,
durably totally‑ordered by the sequencer, then applied by a
deterministic *decide → commit*: it is **atomic** (a slice that would
fail aborts the whole transaction on every shard), **exactly‑once**
under client retry (use session‑framed clients for true exactly‑once),
and **recoverable** (`recover()` re‑drives the ordered log
idempotently after a router restart). This is deterministic
(Calvin‑style), not blocking 2PC. Cross‑shard transactions are
point‑op batches (`Create`/`Update`/`Delete`); SQL‑text routing is a
separate later concern.

**Cross‑shard reads (SP‑A).** `Op::Select` / `Op::QueryRows` /
`Op::SelectFields` / `Op::SelectSorted` automatically scatter to every
shard and merge at the router. Clients send the same `Op` they would
send to a K=1 deployment — the router does the fan‑out, the wire
contract stays unchanged. When you scale shard count: parallel
scatter latency is ≈ `max(per‑shard scan latency) + merge overhead`,
so adding more shards keeps per‑query latency roughly flat while
throughput scales linearly with K. `SelectSorted` produces
**byte‑identical** output to a K=1 deployment for the same dataset
(K‑invariance is locked across K ∈ {1, 2, 4, 8, 16} by a 425‑run
property sweep at the merge layer + a real‑socket K=1↔K=4 byte‑
identical integration test). LIMIT cancellation propagates a shared
cancel flag the instant the output buffer fills, so late shards don't
keep the router pinned. **V1 limitations** (each a later spec):
cross‑shard `Aggregate` / `GroupAggregate` reject with a clear error
(SP‑B / SP‑D); SQL‑text routing for queries that COULD route to one
shard by a key‑equality WHERE still fans out (SP‑E); `FindBy` /
`FindByComposite` still route via per‑shard secondary indexes
(extension to scatter is a follow‑up); sort‑key tie‑break is by
`(value, shard_id)` not `(value, object_id)` (documented edge); a
scatter read sees per‑shard snapshots taken at request‑arrival, NOT
a cross‑shard consistent snapshot. The default failure mode is hard‑
fail (a single unavailable shard surfaces a clean error to the
caller, never a silently partial result); a `ScatterContext`
opt‑in for partial‑on‑timeout best‑effort mode exists at the
`scatter_and_merge_ctx` API level for embeddable use.

## 7c. External sources (JSON/CSV over HTTP)

An **external source** is a named table whose rows are populated by
fetching a remote JSON or CSV endpoint and materializing the result
into a normal KesselDB type. Once materialized, the rows are queried
with ordinary SQL — indexes, aggregates, joins, and constraints all
apply.

> **Requires the `external-sources` cargo feature.**
> Build and run the server with `--features external-sources`:
>
> ```bash
> cargo build --release --features external-sources
> cargo run --release --bin kesseldb --features external-sources -- 127.0.0.1:7878 ./data
> ```

### Register a source

```sql
CREATE EXTERNAL SOURCE prices (
    ticker  CHAR(8)   NOT NULL FROM 'symbol',
    price   I64       NOT NULL FROM 'quote.last',
    volume  U64       NOT NULL FROM 'vol'
) FROM 'http://data.example.com/quotes.json'
  FORMAT JSON
  KEY ticker
  AUTH BEARER ENV 'PRICES_API_TOKEN'
```

- `FROM '<path>'` after a column name is the **JSON dotted path** to
  the value in each array element (`FORMAT JSON`) or the **CSV header
  name** (`FORMAT CSV`).
- `FROM '<url>'` after the column list is the HTTP endpoint.
- `FORMAT JSON` — expects a top-level JSON array of objects.
  `FORMAT CSV` — expects RFC 4180 with a header row.
- `KEY <col>` — the column whose value is the stable row identity.
  The same upstream key always maps to the same row; `REFRESH` upserts
  (create-if-absent / update-if-changed) without duplicating rows.
- `AUTH BEARER ENV 'VAR'` — send `Authorization: Bearer $VAR` where
  `$VAR` is read from the server's environment. No auth: omit the
  clause. Custom header: `AUTH HEADER 'X-Api-Key' ENV 'VAR'`.

### Populate / refresh

```sql
REFRESH prices
```

`REFRESH` fetches the URL once, parses and type-checks every row, and
submits a single atomic upsert batch through the replicated log. If
any row fails type coercion the entire refresh is rejected and the
prior data is unchanged. Re-`REFRESH` with the same upstream data is
idempotent (same rows → same ids → same state, digest unchanged).

### Query

```sql
SELECT * FROM prices WHERE ticker = 'AAPL'
SELECT ticker, price FROM prices ORDER BY price DESC LIMIT 10
SELECT COUNT(*) FROM prices WHERE volume > 1000000
```

`prices` is an ordinary KesselDB table — all SQL, indexes, aggregates,
and joins work normally.

### Remove a source

```sql
DROP EXTERNAL SOURCE prices
```

Removes both the materialized rows and the registered source
definition.

### Security — secret handling

Only the env-var **name** (`'PRICES_API_TOKEN'`) is stored in the
catalog and replicated. The actual secret value is read from the
router's process environment at `REFRESH` time and never appears in
any operation, WAL entry, or log line.

Export the secret in the environment of the server process before
starting it:

```bash
export PRICES_API_TOKEN=sk-...
cargo run --release --bin kesseldb --features external-sources -- 127.0.0.1:7878 ./data
```

### Honest boundaries

- **Snapshot since last REFRESH.** A source reflects only its last
  successful `REFRESH`; queries read the materialized snapshot, never
  live upstream. Re-run `REFRESH` whenever you need fresh data.
- **HTTP and HTTPS.** The fetch client speaks plain HTTP/1.1 for
  `http://` sources. `https://` is supported when the server is built
  with `--features external-sources-tls` (rustls client, bundled Mozilla
  webpki-roots, full certificate + hostname verification, no bypass under
  any flag). The TLS-terminating sidecar is now optional.
- **Upsert only.** Rows deleted from the upstream source are NOT
  automatically removed; only creates and updates are applied.

## 7d. Paginated & NDJSON sources

External sources support two additional capabilities (requires the
`external-sources` feature, same as §7c):

- **`FORMAT NDJSON`** — one JSON object per line; otherwise identical to
  `FORMAT JSON`.
- **Multi-page `PAGE` clause** — a single `REFRESH` walks multiple pages
  and materializes the union. Three cursor forms are supported.

### NDJSON one-liner

```sql
CREATE EXTERNAL SOURCE events (
    id     U64    NOT NULL FROM 'id',
    kind   BYTES  NOT NULL FROM 'type'
) FROM 'http://ingest.example.com/events.ndjson'
  FORMAT NDJSON
  KEY id
```

### Paginated JSON with a next-URL in the response body

```sql
CREATE EXTERNAL SOURCE products (
    sku    BYTES  NOT NULL FROM 'sku',
    price  I64    NOT NULL FROM 'price_cents'
) FROM 'http://catalog.example.com/api/products'
  FORMAT JSON
  KEY sku
  ROWS 'data.items'
  PAGE NEXT JSON 'paging.next'
```

`ROWS 'data.items'` — the row array lives at that dotted path inside the
envelope object (required when `FORMAT JSON` is combined with a
body-cursor `PAGE` clause).

`PAGE NEXT JSON 'paging.next'` — after each page, extract the absolute
next-page URL from `paging.next` in the envelope; stop when the field is
absent, `null`, or an empty string.

### Cursor form — opaque token

```sql
CREATE EXTERNAL SOURCE orders (
    order_id  U64   NOT NULL FROM 'id',
    total     I64   NOT NULL FROM 'total'
) FROM 'http://shop.example.com/api/orders'
  FORMAT JSON
  KEY order_id
  ROWS 'results'
  PAGE CURSOR JSON 'meta.cursor' PARAM 'cursor'
```

`PAGE CURSOR JSON 'meta.cursor' PARAM 'cursor'` — extract the opaque
token from `meta.cursor` in the envelope; the next request is the
**original recipe URL** with `?cursor=<token>` appended (replacing any
pre-existing `cursor` query parameter).

### `PAGE NEXT LINK` — HTTP Link header

```sql
CREATE EXTERNAL SOURCE issues (
    id     U64    NOT NULL FROM 'id',
    title  BYTES  NOT NULL FROM 'title'
) FROM 'http://api.example.com/repos/acme/issues'
  FORMAT JSON
  KEY id
  PAGE NEXT LINK
```

Valid with any format (`FORMAT JSON`, `FORMAT NDJSON`, `FORMAT CSV`).
Uses the `Link: <url>; rel="next"` response header as the next-page URL.

### Compatibility rules (enforced at `CREATE`)

| Format | `PAGE` clause | Rule |
|---|---|---|
| `FORMAT JSON` | `PAGE NEXT JSON` or `PAGE CURSOR JSON` | `ROWS '<path>'` **required** |
| `FORMAT NDJSON` or `FORMAT CSV` | `PAGE NEXT JSON` or `PAGE CURSOR JSON` | **rejected** — no body envelope to read a cursor from; use `PAGE NEXT LINK` or omit `PAGE` |
| Any format | `PAGE NEXT LINK` | always valid |
| Any format | absent | single-page fetch (no pagination) |

### Bounded fetch — safety caps

All multi-page fetches are hard-bounded:

- **`MAX_PAGES = 1000`** — a `REFRESH` walks at most 1,000 pages.
- **`MAX_TOTAL_BODY = 8 × 64 MiB`** — aggregate decompressed response
  bytes across all pages.
- Per-page body cap (64 MiB) still applies to each individual response.
- **Loop detection** — if the extracted next-URL or cursor token exactly
  equals one already seen in the current walk, `REFRESH` returns an error.

If **any** of these caps is exceeded, or if any page returns an HTTP
error, parse error, or type-coercion failure, the entire `REFRESH` is
aborted and **nothing is materialized** — prior data remains intact
(all-or-nothing, same as a single-page refresh).

### Honest boundaries (same as §7c, unchanged)

- **Snapshot since last `REFRESH`.** A source reflects only its last
  successful `REFRESH`; queries read the materialized snapshot, never
  live upstream.
- **HTTP and HTTPS.** Plain HTTP/1.1 for `http://` sources. `https://`
  when built with `--features external-sources-tls` (bundled Mozilla
  roots, full certificate + hostname verification, no bypass). Sidecar
  now optional.
- **Upsert only.** Rows deleted from the upstream source are not
  automatically removed.

## 7e. Object-store sources (S3 / Azure Blob)

An external source can read its bytes directly from an S3-compatible or
Azure Blob object store — `CREATE EXTERNAL SOURCE … FROM 's3://…' |
'az://…'` — using the same fetch → decode → atomic-upsert pipeline as
§7c and §7d. The difference is transport: the router builds a signed
HTTPS GET (AWS SigV4 for S3; Azure Shared Key for Azure Blob), fetches
the object body, and feeds it through the existing decoder. Pagination
(`PAGE …`) is not applicable to object-store sources; a single object
is fetched per `REFRESH`.

> **Requires the `external-sources-objstore` cargo feature** (which
> implies `external-sources-tls`; the default build and plain
> `--features external-sources` remain `http(s)://`-only and pull no
> rustls/webpki/objstore):
>
> ```bash
> cargo build --release --features external-sources-objstore
> cargo run  --release --bin kesseldb --features external-sources-objstore -- 127.0.0.1:7878 ./data
> ```

### S3 / S3-compatible (MinIO, Cloudflare R2, Ceph)

```sql
-- AWS S3 — IAM-key auth, inferred path-style URL from region + bucket/key
CREATE EXTERNAL SOURCE prices (
    ticker  BYTES  NOT NULL FROM 'symbol',
    price   I64    NOT NULL FROM 'quote.last'
) FROM 's3://my-bucket/data/prices.json'
  FORMAT JSON
  KEY ticker
  REGION 'us-east-1'
  AUTH OBJSTORE S3 KEYID ENV 'AWS_ACCESS_KEY_ID' SECRET ENV 'AWS_SECRET_ACCESS_KEY'

-- S3-compatible (MinIO / R2 / Ceph) — ENDPOINT overrides the host; REGION optional
CREATE EXTERNAL SOURCE events (
    id    U64   NOT NULL FROM 'id',
    kind  BYTES NOT NULL FROM 'type'
) FROM 's3://warehouse/events.ndjson'
  FORMAT NDJSON
  KEY id
  ENDPOINT 'https://minio.internal:9000'
  AUTH OBJSTORE S3 KEYID ENV 'MINIO_KEY' SECRET ENV 'MINIO_SECRET'
```

Clause rules for `s3://`:
- `REGION '<r>'` — required for AWS S3 unless `ENDPOINT` is supplied.
  Ignored for presigning purposes when `ENDPOINT` is given.
- `ENDPOINT '<https-url>'` — overrides the request host for
  path-style access (MinIO / R2 / Ceph / any S3-compatible). The
  value **must** start with `https://` (rejected at `CREATE` if not).
- `AUTH OBJSTORE S3 KEYID ENV '<idvar>' SECRET ENV '<secretvar>'` —
  env-var **names** only; the actual key and secret are resolved from
  the router's process environment at each `REFRESH` and never appear
  in any op, WAL entry, or log line.

### Azure Blob Storage

```sql
-- Azure Blob — ACCOUNT declared explicitly in the AUTH clause (or use ENDPOINT instead; exactly one)
CREATE EXTERNAL SOURCE catalog (
    sku    BYTES  NOT NULL FROM 'sku',
    price  I64    NOT NULL FROM 'price_cents'
) FROM 'az://my-container/catalog.json'
  FORMAT JSON
  KEY sku
  AUTH OBJSTORE AZURE ACCOUNT 'mystorageaccount' KEY ENV 'AZURE_STORAGE_KEY'

-- Custom / sovereign endpoint — ENDPOINT replaces the default host
CREATE EXTERNAL SOURCE archive (
    id     U64   NOT NULL FROM 'id',
    label  BYTES NOT NULL FROM 'label'
) FROM 'az://archive-container/records.ndjson'
  FORMAT NDJSON
  KEY id
  ENDPOINT 'https://mystorageaccount.blob.core.windows.net'
  AUTH OBJSTORE AZURE KEY ENV 'AZURE_STORAGE_KEY'
```

Clause rules for `az://`:
- `ACCOUNT '<a>'` — the Azure storage account name. Exactly one of
  `ACCOUNT` or `ENDPOINT` is required; both present is also accepted
  when the `ENDPOINT` is the account's canonical blob URL.
- `ENDPOINT '<https-url>'` — overrides the default
  `https://<account>.blob.core.windows.net` host. Must start with
  `https://` (rejected at `CREATE` if not).
- `AUTH OBJSTORE AZURE [ACCOUNT '<a>'] KEY ENV '<keyvar>'` — the
  storage account shared key is resolved from the named env var at
  `REFRESH` time; never persisted.

### Refresh & query

```sql
REFRESH prices     -- fetches, decodes, upserts; prior data intact on any error
SELECT * FROM prices WHERE ticker = 'AAPL'
DROP EXTERNAL SOURCE prices
```

`REFRESH` is all-or-nothing: any fetch error, HTTP error status,
parse failure, or type-coercion failure aborts the entire operation
and leaves the prior materialized rows unchanged. Re-`REFRESH` with
the same upstream data is idempotent (same rows → same IDs → same
digest, same as §7c).

### Security

**Signing.** AWS SigV4 (HMAC-SHA256, no external crypto library — the
same zero-dep SHA-256/HMAC-SHA256 already in the kessel-sm kernel);
Azure Shared Key (HMAC-SHA256). Both are implemented entirely inside
the `kessel-objstore` crate.

**HTTPS-only, no bypass.** Every object-store request goes over TLS
(rustls + bundled Mozilla webpki roots, full certificate + hostname
verification). There is no flag, env var, or clause to disable
certificate verification.

**Injection-safe.** Container names, blob keys, and the S3 bucket/key
are RFC-3986 percent-encoded before being placed in the request URI
and the signing string. CRLF and query-parameter injection are
blocked.

**Secret references only.** Only the env-var **name** strings are
stored in the catalog trailer and replicated in the WAL. The actual
key/secret values are resolved from the router's environment at each
`REFRESH`, are never logged, never included in any operation or WAL
entry, never appear in digest output, and are never surfaced in error
messages.

### Honest boundaries

- **`FORMAT PARQUET`** is supported for `s3://` / `az://` sources with
  the `--features external-sources-objstore` build (OBJ-2a, §7f below).
  See §7f for the precise scope (PLAIN/UNCOMPRESSED/GZIP/flat REQUIRED or OPTIONAL/V1
  and V2 pages) and the supported-vs-deferred matrix.
- **Iceberg manifests, prefix/multi-object listing, and STS/SAS/IMDS
  credential providers** are explicit follow-ons (OBJ-3 through OBJ-5)
  and are **rejected at `CREATE`** with a clear error message.
- **Single object per source.** A `REFRESH` fetches exactly one
  object. Listing a prefix or walking a multi-object partition is OBJ-4.
- **Upsert only.** Same as §7c — rows deleted from the upstream object
  are not automatically pruned from the materialized table.
- **Snapshot since last `REFRESH`.** Queries read the last materialized
  snapshot; live object-store reads are never issued by a `SELECT`.

## 7f. FORMAT PARQUET for object-store sources

> **Current capability (SP125‑SP154, OBJ‑2c‑2 codec arc CLOSED at 6/7 codecs):**
> `FORMAT PARQUET` reads real `pyarrow 24.0.0` Parquet files end‑to‑end
> across the **flat REQUIRED + OPTIONAL × UNCOMPRESSED + Snappy + GZIP + zstd
> + LZ4_RAW + Brotli × PLAIN + dictionary × V1 + V2 data pages × INT32 +
> INT64 + INT96 + FLBA + BYTE_ARRAY + DECIMAL (precision ≤ 38)** matrix.
> Vanilla `pq.write_table(df)` works zero‑flags for everything in that
> matrix; pyarrow output for every supported codec decodes for all tested
> fixtures including a 2000‑row zstd stress fixture exercising FseCompressed
> mode for all three LL/OF/ML codes simultaneously and pyarrow
> `compression='brotli'` round-trips via the SP154 zero-dep RFC 7932
> decoder. Still typed‑Unsupported: legacy LZ4 framing (codec id 5;
> modern LZ4_RAW codec id 7 IS supported), 4+ deep nested groups (would
> be SP147), DECIMAL precision > 38, per‑page > 256 MiB (SP151 raised the
> historical 64 MiB cap to a 256 MiB default + added
> `kessel_parquet::extract_with_cap` for operators with known-trusted
> producers or memory-constrained ingest).
> **All Parquet nested types supported (LIST, MAP, struct + arbitrary
> nesting up to 3-deep — OBJ-2c-5 fully closed at SP146).**
>
> The slice‑by‑slice history below records how the capability grew —
> kept verbatim for traceability — but the matrix above is the
> authoritative current scope.

> **OBJ-2b in progress:** the RLE/bit-packing-hybrid primitive is
> implemented (SP102) but not yet wired. Until OBJ-2b-2/3/4 ship,
> `FORMAT PARQUET` still requires PLAIN-encoded, UNCOMPRESSED,
> REQUIRED columns (pyarrow `use_dictionary=False, compression=None`).

> **OBJ-2b-2 (SP103):** dictionary-encoded Parquet (pyarrow default
> `use_dictionary=True`) is now supported for flat REQUIRED,
> UNCOMPRESSED, V1 files. Compression still requires
> `compression=None` (Snappy → OBJ-2b-3); nullable/OPTIONAL columns
> still unsupported (→ OBJ-2b-4).

> **OBJ-2b-3 (SP104):** Snappy-compressed Parquet (pyarrow default
> `compression='snappy'`) is now supported for flat REQUIRED, V1
> files (dictionary or PLAIN). nullable/OPTIONAL columns still
> unsupported (→ OBJ-2b-4); gzip/zstd and Snappy pages >64 MiB →
> OBJ-2c.

> **OBJ-2b-4 (SP105):** vanilla `pq.write_table(df)` — flat REQUIRED
> or OPTIONAL columns, UNCOMPRESSED or Snappy, PLAIN or dictionary, V1
> — is now fully supported, including NULLs (OPTIONAL def-level 0 →
> `PqValue::Null`). The OBJ-2b arc is COMPLETE. REPEATED columns /
> repetition levels, nested/optional groups, gzip/zstd/lz4/brotli,
> INT96/DECIMAL, V2 data pages, and Snappy pages >64 MiB remain
> Unsupported (→ OBJ-2c).

> **OBJ-2c-1 (SP106):** GZIP-compressed Parquet (pyarrow
> `compression='gzip'`) is now supported for flat REQUIRED or OPTIONAL
> columns, PLAIN or dictionary encoding, V1 pages. The pure zero-dep
> RFC 1952 + RFC 1951 inflater composes with dictionary and
> OPTIONAL/def-levels via the existing page_payload seam; no other
> code path changed. Pages decompressed to more than 64 MiB are
> rejected (typed `Unsupported`). ZSTD/lz4/brotli, INT96/DECIMAL, V2
> data pages, REPEATED/nested, and GZIP pages >64 MiB remain
> Unsupported (→ OBJ-2c-2+).

> **OBJ-2c-3 (SP107):** `DATA_PAGE_V2` data pages (pyarrow
> `data_page_version='2.0'`) are now supported for the existing flat
> REQUIRED or OPTIONAL × UNCOMPRESSED|Snappy|GZIP × PLAIN|dict matrix.
> The V2 raw-level-split path reads the uncompressed def/rep level
> bytes directly, then decompresses only the value section; the shared
> `scatter_nulls` helper keeps the V1 OPTIONAL path byte-identical.
> OBJ-2c-2 (zstd) was resequenced/deferred to prioritise broader
> pyarrow compatibility. ZSTD/lz4/brotli, INT96/DECIMAL,
> REPEATED/nested (incl. V2 repetition levels), and pages >64 MiB
> remain Unsupported (→ OBJ-2c-2/4/5).

> **OBJ-2c-4 (SP108):** INT96 timestamps and DECIMAL logical-type
> values are now decoded for the existing flat REQUIRED or OPTIONAL ×
> UNCOMPRESSED|Snappy|GZIP × V1|V2 × PLAIN|dict matrix. `INT96`
> physical columns decode to `PqValue::Timestamp(i64 ns)` via checked
> Julian-day arithmetic (nanoseconds since Unix epoch). `DECIMAL`
> logical-type columns decode to `PqValue::Decimal { unscaled: i128,
> scale: i32 }` for physical types INT32, INT64, and
> FixedLenByteArray (BYTE_ARRAY DECIMAL is covered by hand-KATs only;
> pyarrow 24.0.0 does not write it). FLBA non-DECIMAL columns (e.g.,
> FLBA-UUID) decode to `PqValue::Bytes`. Today, `pq_to_cell` maps
> Timestamp → `Cell::Text` (Unix-ns string) and Decimal →
> `Cell::Text` (unscaled-integer string); mapping via `FieldKind::I64`
> or `FieldKind::I128` (unscaled) works end-to-end. Coercion to
> `FieldKind::Timestamp` (for Timestamp) and `FieldKind::Fixed{scale}`
> (for Decimal) are immediate follow-up items. DECIMAL precision must
> be 1..=38 (backed by i128); precision > 38 is rejected with
> `Unsupported`. ZSTD/lz4/brotli, REPEATED/nested (incl. V2
> rep-levels), and pages >64 MiB remain Unsupported (→ OBJ-2c-2/5).

> **OBJ-2c-4 follow-up (SP151):** the historical 64 MiB per-page cap
> is lifted to **256 MiB default** + a configurable operator knob.
> Pyarrow writers emit pages above 64 MiB on common shapes
> (high-cardinality dictionary pages, large value pages on many-row
> row groups); pre-SP151 those tripped a typed Unsupported with the
> 64 MiB cap value. Post-SP151:
>
> - `kessel_parquet::extract(bytes, wanted)` uses
>   `DEFAULT_MAX_PAGE_SIZE = 256 * 1024 * 1024` — covers every pyarrow
>   shape seen in the wild without operator intervention.
> - `kessel_parquet::extract_with_cap(bytes, wanted, max_page_size)`
>   is the operator knob. Raise above 256 MiB up to the per-codec
>   module ceiling (also 256 MiB) for known-trusted producers;
>   lower for memory-constrained ingest; `cap=0` is the kill-switch
>   that rejects every page (useful when sanitising hostile input).
>   The cap is enforced as a thread-local set on entry and restored
>   on return (RAII, including panic-unwind).
> - Pages above the cap return `Unsupported` naming `SP151`, the
>   `extract_with_cap` operator knob, and the cap value so an
>   operator hitting the cap in production has a direct path to
>   raise it.
> - Defense in depth: the four per-codec module ceilings
>   (`SNAPPY_MAX_DECOMP`, `GZIP_MAX_DECOMP`, `ZSTD_MAX_DECOMP`,
>   `LZ4_MAX_DECOMP`) all bumped from 64 MiB → 256 MiB in lockstep.
>   Even a caller passing `usize::MAX` to `extract_with_cap` can't
>   OOM the decoder — the per-codec ceiling still gates allocation.

`FORMAT PARQUET` is supported for `s3://` and `az://` sources when the
server is built with `--features external-sources-objstore`. Plain
`http://` / `https://` URLs are **rejected** with a clear message if
`FORMAT PARQUET` is specified — Parquet is object-store only. `PAGE`
and `ROWS` clauses are also **rejected** at `CREATE` with `FORMAT
PARQUET` (they are not applicable: a Parquet object is self-describing
and multi-row-group; row selection is column-map driven, not page-cursor
driven).

> **Requires `--features external-sources-objstore`** (same as §7e);
> the default build and plain `--features external-sources` do not
> compile Parquet support and do not link any parquet/objstore/rustls
> dependency.

### SQL syntax

```sql
CREATE EXTERNAL SOURCE readings (
    sensor_id  U64    NOT NULL FROM 'sensor_id',
    temp_c     I64    NOT NULL FROM 'temp_celsius',
    label      BYTES  NOT NULL FROM 'label'
) FROM 's3://my-bucket/data/readings.parquet'
  FORMAT PARQUET
  KEY sensor_id
  REGION 'us-east-1'
  AUTH OBJSTORE S3 KEYID ENV 'AWS_ACCESS_KEY_ID' SECRET ENV 'AWS_SECRET_ACCESS_KEY'
```

- `FROM '<col_name>'` after each column is the **flat Parquet leaf
  column name** (`ColumnMap.source`). It must be a leaf column present
  in the Parquet schema at the top level (no nested group path syntax
  in OBJ-2a).
- All other clauses (`REGION`, `ENDPOINT`, `AUTH OBJSTORE S3/AZURE`,
  `KEY`) are identical to §7e.
- `REFRESH` and `DROP EXTERNAL SOURCE` work identically to §7e.

### Parquet scope: what is currently supported (OBJ-2a → OBJ-2c-5 SP146 — arc FULLY CLOSED)

| Parquet property | OBJ-2a → OBJ-2c-5 SP146 |
|---|---|
| Encoding | `PLAIN` and dictionary (`PLAIN_DICTIONARY`/`RLE_DICTIONARY`); RLE/bit-packing hybrid for dictionary indices |
| Compression codec | `UNCOMPRESSED`, `SNAPPY` (raw block; pages ≤ 64 MiB decompressed), `GZIP` (RFC 1952; pages ≤ 64 MiB decompressed), `ZSTD` (RFC 8478), or `LZ4_RAW` (SP149; codec id 7 — the modern raw LZ4 block format pyarrow emits for `compression='lz4'` since v8). `BROTLI` (codec id 4) is recognized at meta-decode time as of SP150 but decompression is rejected with a named follow-up — a zero-dep RFC 7932 decoder is a dedicated multi-week SP-arc; **workaround**: re-encode the file with `compression='zstd'` (often better ratio) or `compression='lz4'` (very fast). Legacy LZ4 (codec id 5, deprecated Hadoop framing) is also rejected with a named pointer to SP149. |
| Column repetition | `REQUIRED` or `OPTIONAL` flat columns (nullable; V1 and V2 definition levels) |
| Schema shape | **All Parquet nested types supported** (LIST, MAP, struct + arbitrary nesting up to 3-deep). Flat (REQUIRED + OPTIONAL), `LIST<primitive>` (SP143), `MAP<K, V>` (SP144), `struct<...>` (SP144), `List<List<T>>` / `List<struct<...>>` / `Map<K, struct<...>>` / `Map<K, List<T>>` / `struct<List/Map/struct>` (SP145), `List<List<List<T>>>` / `List<Map<K,V>>` / `Map<K1, Map<K2,V>>` (SP146 — closes the 3 SP145-deferred cross-products) |
| Nested LIST (SP143/SP145/SP146) | `List<T>` for primitive T (SP143); `List<List<T>>` for primitive T (SP145; max_rep_level=2 generalized assembler); `List<struct<primitives>>` (SP145; field-zip per item slot); `List<List<List<T>>>` 3-deep (SP146; max_rep_level=3 3-level-stack assembler); `List<Map<K, V>>` (SP146; outer-list-of-inner-maps) |
| Nested MAP (SP144/SP145/SP146) | `Map<K, V>` for primitive K and V (SP144; canonical 3-node encoding `MAP { repeated key_value { REQUIRED key; REQ\|OPTIONAL value }}`; REQUIRED key enforced); `Map<K, struct<...>>` (SP145); `Map<K, List<T>>` (SP145 cross-product); `Map<K1, Map<K2, V>>` (SP146; outer-map-of-inner-maps) |
| Nested struct (SP144/SP145) | struct of primitives (SP144); struct of `List<T>` / `struct<...>` / `Map<K,V>` fields (SP145; recursive composition via `StructField.nested: Option<Box<ColumnKind>>`) |
| Nested depth | Up to 3 REPEATED ancestors (`max_rep_level ≤ 3`); 4+ deep (`List<List<List<List<T>>>>` etc.) defers to SP147 when a real fixture demands it |
| Data page version | V1 and V2 (`DATA_PAGE_V2`) |
| Row groups | Multi-row-group files are fully supported |
| Column subset | Only the recipe-mapped columns are decoded; unmapped columns are skipped |
| Physical types | `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY`, `INT96` (→ Timestamp), `FixedLenByteArray` (raw bytes or DECIMAL) |
| Logical types | `DECIMAL{precision ≤ 38, scale ≤ precision}` (typed `PqValue::Decimal{ unscaled: i128, scale }`); `LIST` (SP143; element values typed `PqValue::List(Vec<PqValue>)`) |
| Temporal | `INT96` → `PqValue::Timestamp` (Unix nanoseconds; ≥ 1970 end-to-end today via `FieldKind::Timestamp`; any sign via `FieldKind::I64`) |
| Null values | OPTIONAL def-level 0 rows → `PqValue::Null` (coerced via the same path as JSON `null`); LIST element nulls handled via def-level scatter per Dremel record assembly |

> **SP143 nested decode**: SP143 lifts the OBJ-2c flat-schema restriction
> for `List<primitive>` columns specifically. Each `List<primitive>` row's
> value is decoded as `PqValue::List(Vec<PqValue>)` per Dremel-style record
> assembly using the canonical 3-node LIST encoding pattern (outer group
> `LIST { repeated middle group { primitive element }}`). 5 pyarrow 24.0.0
> fixtures roundtrip-tested (`list_i64_required`, `list_i64_optional`,
> `list_string`, `optional_list_i64`, `list_with_null_items`).
>
> **SP144 nested decode**: SP144 lifts the OBJ-2c-5 nested rejection for
> canonical `Map<K, V>` and struct-of-primitives columns. Map values decode
> as `PqValue::Map(Vec<(PqValue, PqValue)>)` via `assemble_map_kv` over
> parallel key+value streams; struct values decode as
> `PqValue::Struct(Vec<(String, PqValue)>)` via `assemble_struct` zipping N
> field columns. Map keys MUST be REQUIRED per Parquet spec (rejected as
> `Bad` otherwise). 5 pyarrow 24.0.0 fixtures roundtrip-tested
> (`map_string_i64`, `optional_map_string_i64`, `map_string_string`,
> `struct_i64_string`, `optional_struct`).
>
> **SP145 deep nesting**: SP145 lifts the 4 SP145-named rejections in
> `classify_column_plan` via per-shape composition (BOLD V1 — no full
> Dremel automaton needed for the shapes Parquet writers actually
> produce). 4 new `ColumnKind` variants + a recursive
> `StructField.nested: Option<Box<ColumnKind>>` enable: (a) `List<List<T>>`
> via `assemble_list_of_list_primitive` (max_rep_level=2 generalized
> assembler); (b) `List<struct<...>>` via `assemble_list_of_struct`
> (field-zip per item slot using the shared REPEATED-ancestor rep stream);
> (c) `Map<K, struct<...>>` via `assemble_map_of_struct`; (d)
> `Map<K, List<T>>` (BOLD cross-product) via `assemble_map_of_list`;
> (e) `struct<List/Map/struct>` via recursive `classify_nested_group_child`
> + `decode_field_by_kind` dispatch. 7 pyarrow 24.0.0 fixtures
> roundtrip-tested.
>
> **SP146 deep-nesting follow-ups**: SP146 closes the 3 cross-products SP145
> V1 deferred (each named `SP146 follow-up` in the SP145-era source error
> messages). 3 new `ColumnKind` variants + 3 new assemblers + 1 new classify
> helper: (a) `List<List<List<T>>>` 3-deep (max_rep_level=3) via
> `assemble_list_of_list_of_list_primitive` (8-case classifier + 3-level
> stack outer/middle/inner accumulators); (b) `List<Map<K, V>>` via
> `assemble_list_of_map_kv` (outer-list-of-inner-maps driven off shared K/V
> rep stream); (c) `Map<K1, Map<K2, V>>` via `assemble_map_of_map_kv`
> (outer-map-of-inner-maps with outer K at max_rep=1 + inner K/V at
> max_rep=2). 3 pyarrow 24.0.0 fixtures roundtrip-tested
> (`list_of_list_of_list_i64`, `list_of_map_string_i64`,
> `map_string_map_string_i64`) — all GREEN on first try. **OBJ-2c-5 arc
> FULLY CLOSED with NO follow-ups remaining — every nested Parquet shape
> pyarrow writes is now decodable**.

### What is NOT supported (rejected at REFRESH with a precise error)

The following trigger a typed `PqError` (surfaced as a `REFRESH`
failure; prior materialized data is left intact — all-or-nothing, same
as every other format):

- **REPEATED columns / repetition levels** outside the canonical
  `LIST<primitive>` (SP143), `MAP<K, V>` (SP144), `List<List<T>>` /
  `List<struct>` / `Map<K, struct>` / `Map<K, List<T>>` /
  `struct<List/Map/struct>` (SP145), `List<List<List<T>>>` /
  `List<Map<K,V>>` / `Map<K1, Map<K2,V>>` (SP146) shapes — rejected
  with `Unsupported(...)`. **All Parquet nested types up to 3-deep are
  now supported (OBJ-2c-5 arc fully closed).**
- **4-layer-deep nesting** (`List<List<List<List<T>>>>` etc.) —
  rejected with `Unsupported("...: SP147 follow-up")`. The per-shape
  composition pattern from SP145/SP146 generalizes to one more level
  the same way; no pyarrow corpus exercises this depth yet.
- **Brotli compression (codec id 4)** — **fully supported** (SP154). A
  hand-rolled zero-dep RFC 7932 Brotli decoder ships across 12 layers
  (bit reader → stream/metablock framing → simple+complex prefix codes
  → NBLTYPES + NPOSTFIX/NDIRECT + context-map headers → 704-symbol
  insert-and-copy command alphabet → 64-symbol distance prefix code +
  recent-distance ring → 122,784-byte static dictionary blob + 121
  Appendix B transforms → compressed-metablock orchestration → flat
  output buffer with pre-stream-zero copy semantics), comparable in
  scope to the SP125-SP140 zstd arc. The decoder enforces V1 reductions
  matching the common pyarrow-emitted shape (NBLTYPES=1, NPOSTFIX=0+NDIRECT=0,
  NTREES=1 for both CMAPs, identity-only dictionary transforms); files
  that exceed those reductions surface typed
  `BrotliMetablockError::{UnsupportedBlockTypes, UnsupportedDistanceParams,
  Context, Dictionary, ...}` mapped to `Unsupported` with a named
  SP154-followup pointer. Pyarrow's `compression='brotli'` round-trips
  byte-identical for the standard flat-i64 + flat-BYTE_ARRAY shape
  (locked by the `pyarrow_brotli_flat` integration KAT). Closes
  **OBJ-2c-2** codec matrix at 6/7 codecs supported (UNCOMPRESSED,
  Snappy, GZIP, Zstd, LZ4_RAW, Brotli; LZO remains deprecated, legacy
  LZ4 codec id 5 rejected with named pointer).
- **Legacy LZ4 compression (codec id 5, deprecated Hadoop framing)** —
  rejected with `Unsupported("LZ4 (deprecated Hadoop framing) — use
  LZ4_RAW; SP149 follow-up if needed")`. Pyarrow stopped writing this
  variant in v8; the modern LZ4_RAW (codec id 7) is fully supported.
- **Pages above the per-call max_page_size cap** — rejected with
  `Unsupported("<page kind> size <N> exceeds max_page_size cap <cap>:
  SP151 (raise via kessel_parquet::extract_with_cap)")`. The default
  cap is 256 MiB (4× the historical 64 MiB limit; SP151). The
  per-codec module ceilings (`SNAPPY_MAX_DECOMP`, `GZIP_MAX_DECOMP`,
  `ZSTD_MAX_DECOMP`, `LZ4_MAX_DECOMP`) are also 256 MiB and act as
  the absolute defense-in-depth ceiling — `extract_with_cap` can
  lower the cap but cannot raise it above the per-codec ceiling.
- **DECIMAL precision > 38** — rejected with
  `Unsupported("DECIMAL precision … (must be 1..=38): OBJ-2c-4")`.
  DECIMAL backed by i128 (≤ 38 digits) is supported; wider types are not.
- **Pre-1970 INT96 through `FieldKind::Timestamp` coerce** — the decoder
  produces a correct negative-nanosecond `PqValue::Timestamp`; the
  `FieldKind::Timestamp` coerce path in `pq_to_cell` is typed
  `FetchError::Type` at coerce time for negative values. Map to
  `FieldKind::I64` for any sign (unscaled Unix ns); immediate follow-up:
  signed-Timestamp FieldKind extension.
- **DECIMAL → `FieldKind::Fixed` coerce** — `pq_to_cell` Decimal arm is
  typed `FetchError::Type` at coerce time when the target column is
  `FieldKind::Fixed` (Fixed is internal-only today); immediate follow-up:
  `to_field_bytes` Fixed arm. Mapping DECIMAL → `FieldKind::I128`/`I64`
  (unscaled integer) works today.
- **BYTE_ARRAY DECIMAL via pyarrow** — hand-KAT-only coverage; pyarrow
  24.0.0 does not write BYTE_ARRAY DECIMAL (it always chooses INT32, INT64,
  or FLBA based on precision). The decode arm is implemented and KAT-tested;
  real-fixture coverage is deferred until a writer that emits it is available.
- **A mapped column name absent from the Parquet schema** — rejected
  with `Bad("column \`<name>\` not found in Parquet schema")`.

None of the above are decoded silently or partially. Failure is
precise, typed, and fail-closed — the error message names the
OBJ-2c follow-on that will address it.

### Producing a compatible Parquet file

A file compatible with OBJ-2a can be written with pyarrow:

```python
import pyarrow as pa, pyarrow.parquet as pq

schema = pa.schema([
    pa.field("sensor_id", pa.int64(), nullable=False),
    pa.field("temp_celsius", pa.int64(), nullable=False),
    pa.field("label", pa.large_binary(), nullable=False),
])
table = pa.table({
    "sensor_id":   pa.array([1, 2, 3], type=pa.int64()),
    "temp_celsius": pa.array([22, 18, 25], type=pa.int64()),
    "label":        pa.array([b"A", b"B", b"C"], type=pa.large_binary()),
})
pq.write_table(table, "readings.parquet",
               version='1.0',
               use_dictionary=False,
               compression="none",
               data_page_version="1.0")
```

Key options: `use_dictionary=False` (forces PLAIN encoding),
`compression="none"` (UNCOMPRESSED), `data_page_version="1.0"` (V1
pages). Multi-row-group files are supported — all row groups are
iterated in order. `data_page_version="2.0"` (DATA_PAGE_V2) is also
supported as of OBJ-2c-3 (§7f) for the same flat REQUIRED|OPTIONAL ×
UNCOMPRESSED|Snappy|GZIP × PLAIN|dict matrix.

### Physical-type-to-KesselDB-column mapping

| Parquet physical type | Mapped as (`ColumnMap.source`) | Notes |
|---|---|---|
| `INT32` | `I64` or `U64` column | Value widened to i64 |
| `INT64` | `I64` or `U64` column | Value taken as i64 |
| `FLOAT` | Any numeric column | Rendered via canonical-f64 formatting |
| `DOUBLE` | Any numeric column | Rendered via canonical-f64 formatting |
| `BOOLEAN` | `Bool` column, or numeric column (as 1/0) | `PqValue::Bool(v) → Cell::Bool(v)` — same as a JSON boolean; coerces to a 1-byte `0x01`/`0x00` for a `Bool` column, or `1`/`0` for a numeric column |
| `BYTE_ARRAY` | `BYTES` or `CHAR` column | Decoded as UTF-8 (lossy) |

The mapping goes through the same `coerce::to_field_bytes` path the
JSON decoder uses, so the same logical value yields identical
`FieldKind` bytes regardless of whether it arrived as JSON or Parquet.

## 8. Authentication, quotas & backpressure

Configured via `ServerConfig` and the `*_cfg` entry points:

```rust
use kesseldb_server::{run_cfg, ServerConfig};

run_cfg("0.0.0.0:7878", "./data", ServerConfig {
    token: Some(b"my-shared-secret".to_vec()), // None = open (default)
    max_conns: 1024,        // refuse connections past this
    max_inflight: 4096,     // shed load to `Unavailable` past this
})?;
```

- **Auth**: when `token` is set, the first frame on every connection must be the
  token; it is compared in constant time (no byte‑timing the secret). Clients
  use `Client::connect_authed` / `ClusterClient::with_token`.
- **Connection quota**: connections past `max_conns` are refused immediately.
- **Backpressure**: when `max_inflight` requests are queued, new ones get
  `OpResult::Unavailable` instead of growing the queue unbounded.

**Transport encryption**: KesselDB does *not* implement TLS in‑process (that
would require bundling cryptography and break the zero‑dependency design). Run it
behind a TLS‑terminating reverse proxy, or on a private/encrypted network
(WireGuard, tailnet, VPC). The wire is plaintext but token‑authenticated.
Or build with `--features http-gateway,tls` to terminate HTTPS in-process on
`ServerConfig.http_tls_addr` — see §HTTP gateway below.

## 9. PostgreSQL clients (psql, pgcli, JDBC, psycopg, pgx, …)

KesselDB speaks the PostgreSQL Frontend/Backend Protocol v3.0 — the same
wire libpq, `psql`, pgcli, JDBC, psycopg, `pgx`, `tokio-postgres`, sqlx-pg,
Diesel-pg, GORM-pg, Drizzle-pg, Prisma-pg, … all speak. Built behind the
opt-in `pg-gateway` feature flag so the default binary stays lean.

**Both Simple Query AND Extended Query are supported (V1.1, 2026-05-29).**
SP-PG V1 shipped the Simple Query path (`Q` message) so `psql` and any
client that does its own SQL formatting works. SP-PG-EXTQ V1 (2026-05-29)
adds the full Extended Query message set (`P` / `B` / `D` / `E` / `S` /
`C` / `H`), which is what every modern ORM (psycopg2/3, asyncpg,
SQLAlchemy, Drizzle, Prisma, JDBC default, sqlx, pgx, Diesel) uses on
connect — they probe via Parse + Bind + Sync before falling back to
Simple Query. KesselDB now satisfies that probe end-to-end. A real
`psycopg2.connect(...)` + `cur.execute("SELECT * FROM pgtest WHERE id = %s", (42,))`
returns real rows on vulcan; full ORM-suite smoke for SQLAlchemy/JDBC/
Drizzle/Prisma is the SP-PG-EXTQ T8 / T11 / T12 follow-up (still open
at the time of writing — the wire surface IS lit, the formal compat
test fixtures are post-V1.1).

### Enable the PG listener

```bash
# Build (or download the release binary, which already includes pg-gateway):
cargo build --release -p kesseldb-server --features pg-gateway

# Start kesseldb with the PG listener bound:
KESSELDB_TOKEN=secret \
KESSELDB_PG_ADDR=127.0.0.1:5432 \
  ./target/release/kesseldb 127.0.0.1:7878 /tmp/kessel.db
# => KesselDB listening on 127.0.0.1:7878, data dir /tmp/kessel.db, pg=127.0.0.1:5432
```

Two env vars matter:

- `KESSELDB_TOKEN` — the operator's shared-secret Bearer token (the same one
  the HTTP gateway uses). The PG listener REQUIRES a token to be set;
  closed-mode-without-token rejects the connection with `28000`
  invalid_authorization_specification. The PG-wire SCRAM exchange uses
  this token as the SCRAM password input (one credential surface; rotating
  the token rotates HTTP-Bearer, WS, and PG-SCRAM atomically).
- `KESSELDB_PG_ADDR` — `host:port` to bind the PG listener on. Standard
  default is `:5432`; bind to `127.0.0.1:5432` for localhost-only, or
  `0.0.0.0:5432` to accept remote connections. PG and HTTP have
  independent connection caps so a misbehaving pgcli cannot starve HTTP
  clients.

When all three listeners are active (binary + HTTP + PG), the startup line
surfaces every bound address:

```text
KesselDB listening on 127.0.0.1:7878, data dir ./data, http=127.0.0.1:8080, pg=127.0.0.1:5432
```

### Connect with `psql`

```bash
PGPASSWORD=$KESSELDB_TOKEN psql -h localhost -p 5432 -U test "SELECT 1"
```

The `-U test` username can be anything — V1 is multi-user-deferred (the
SCRAM exchange authenticates against the Bearer token regardless of the
PG `user` field). Interactive sessions work too:

```bash
PGPASSWORD=$KESSELDB_TOKEN psql -h localhost -p 5432 -U test
kessel=> CREATE TABLE users (id i64 PK, name char(64));
CREATE TABLE
kessel=> INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
INSERT 0 2
kessel=> SELECT * FROM users;
 id |  name
----+-------
  1 | Alice
  2 | Bob
(2 rows)
kessel=> \q
```

### Connect from JDBC

Standard `org.postgresql:postgresql` driver:

```java
String url = "jdbc:postgresql://localhost:5432/kessel";
Properties props = new Properties();
props.setProperty("user", "test");
props.setProperty("password", System.getenv("KESSELDB_TOKEN"));
Connection conn = DriverManager.getConnection(url, props);
PreparedStatement stmt = conn.prepareStatement("SELECT * FROM users");
ResultSet rs = stmt.executeQuery();
while (rs.next()) {
    System.out.println(rs.getLong("id") + " " + rs.getString("name"));
}
```

### Connect from Python (psycopg2/psycopg3)

After SP-PG-EXTQ V1 (2026-05-29) parameterized queries through `%s`
placeholders work end-to-end — psycopg2 sends them via Extended Query
(Parse / Bind / Execute / Sync) and KesselDB dispatches every frame
through `EngineApply::apply_sql` after substituting the bind values.

```python
import os
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="test",
    password=os.environ["KESSELDB_TOKEN"],
    dbname="kessel",
)
cur = conn.cursor()

# 1. Simple Query path (no placeholders) — works since SP-PG V1:
cur.execute("CREATE TABLE pgtest (id BIGINT, name CHAR(64))")
cur.execute("INSERT INTO pgtest (id, name) VALUES (42, 'Alice')")
cur.execute("SELECT * FROM pgtest")
print(cur.fetchall())           # → [(42, 'Alice')]

# 2. Extended Query path (parameterized) — works since SP-PG-EXTQ V1:
cur.execute("SELECT * FROM pgtest WHERE id = %s", (42,))
print(cur.fetchall())           # → [(42, 'Alice')] — real round-trip
```

### Connect from SQLAlchemy

SQLAlchemy uses the psycopg2 or psycopg3 driver under the hood and probes
via Extended Query on `engine.connect()`. With SP-PG-EXTQ V1 those probes
return without `08P01 protocol_violation` so `engine.connect()` succeeds:

```python
import os
from sqlalchemy import create_engine, text

url = (
    f"postgresql+psycopg2://test:{os.environ['KESSELDB_TOKEN']}"
    f"@localhost:5432/kessel"
)
engine = create_engine(url)

with engine.connect() as conn:
    rows = conn.execute(
        text("SELECT * FROM pgtest WHERE id = :id"),
        {"id": 42},
    ).fetchall()
    print(rows)                 # → [(42, 'Alice')]
```

The ORM-layer scope (declarative models, autoflush, the full SQLAlchemy
expression language) depends on which subset of catalog SQL SQLAlchemy
emits — synthetic-peer KATs verify the connect + probe + simple
parameterized SELECT shape. Full ORM smoke (model creation +
autogenerated DDL + relationship loading) is the SP-PG-EXTQ T11
follow-up.

### Supported GUI / admin tools

After SP-PG-CAT (V1 of the pg_catalog stubs arc), GUI admin / BI
tools that issue catalog-introspection queries on connect now see
synthesized responses instead of `42P01 undefined_table`. The
following tools have been verified via synthetic-peer KATs driving
each tool's verbatim connect / introspection SQL through the
catalog hook:

| Tool | Connect / introspect | Notes |
|---|---|---|
| `psql` | full | `\dt`, `\d <t>`, `\dn`, `\di`, `\d+ <t>` (partial — no comments) all work; `\dt+` shows table list with row-count column = `-1` (V1 doesn't track row counts) |
| `pgcli` | full | tab-completion populates from `pg_class` enumeration; autocomplete works against created tables |
| pgAdmin 4 | connect + browse | "Add Server" wizard completes; tables visible under public schema; column/index/constraint panels populated. Functions / triggers / extensions / event-triggers panels show empty (V1-out-of-scope) |
| DBeaver | connect + browse | "Connect to PostgreSQL" wizard completes; navigator tree shows tables + columns + indexes + UNIQUE constraints |
| DataGrip / IntelliJ | connect + browse | works; `information_schema.routines` returns empty so the Functions panel is empty (V1) |
| Metabase | connect + introspect | "Add Database" → PostgreSQL wizard completes; tables/columns discoverable via `information_schema.{tables,columns,schemata}` |
| Tableau / Looker / Hex / Superset | connect + introspect | ODBC-driver-based connect wizards complete; schema is discoverable |
| pgJDBC `getTables` / `getColumns` / `getIndexInfo` | full | The standard `org.postgresql:postgresql` driver's database-metadata API surfaces KesselDB tables + columns + indexes correctly |

Sample interactive session through `psql`:

```text
$ PGPASSWORD=$KESSELDB_TOKEN psql -h localhost -p 5432 -U test kessel
psql (14.10, server PostgreSQL 14.0 (KesselDB 1.0))
kessel=> CREATE TABLE users (id I64 NOT NULL, email CHAR(64) NOT NULL);
CREATE TABLE
kessel=> CREATE UNIQUE INDEX ON users (email);
CREATE INDEX
kessel=> \dt
        List of relations
 Schema | Name  | Type  | Owner
--------+-------+-------+----------
 public | users | table | kesseldb
(1 row)

kessel=> \d users
                Table "public.users"
 Column | Type | Collation | Nullable | Default
--------+------+-----------+----------+---------
 id     | int8 |           | not null |
 email  | text |           | not null |
Indexes:
    "users_email_idx" UNIQUE, btree (email)

kessel=> SELECT version();
                   version
---------------------------------------------
 PostgreSQL 14.0 (KesselDB 1.0)
(1 row)

kessel=> SELECT * FROM information_schema.tables
         WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
 table_catalog | table_schema | table_name | table_type
---------------+--------------+------------+------------
 kesseldb      | public       | users      | BASE TABLE
(1 row)
```

### Real psql session (verified 2026-05-28)

Captured from a real `psql 16.14` libpq client driving the
`kesseldb-server` binary (built with `--features pg-gateway,http-gateway`)
on a Linux reference server. The server was started with:

```bash
KESSELDB_TOKEN=admin KESSELDB_PG_ADDR=127.0.0.1:5532 \
  ./target/release/kesseldb 127.0.0.1:6532 /tmp/kdb-data
# => KesselDB listening on 127.0.0.1:6532, data dir /tmp/kdb-data, pg=127.0.0.1:5532
```

Every command and its actual response are shown below. The session
exercises authentication, the `version()` helper, `\dt` empty +
populated, `CREATE TABLE` with the canonical PG `BIGINT` type
(NOT KesselDB's `I64` spelling), `INSERT` (single + multi-row),
`SELECT *`, `\d <table>`, and `\dn` schema-list.

```text
$ PGPASSWORD=admin psql -h 127.0.0.1 -p 5532 -U test -d kesseldb -c "SELECT version();"
            version
--------------------------------
 PostgreSQL 14.0 (KesselDB 1.0)
(1 row)

$ PGPASSWORD=admin psql -h 127.0.0.1 -p 5532 -U test -d kesseldb -c "\dt"
Did not find any relations.

$ PGPASSWORD=admin psql -h 127.0.0.1 -p 5532 -U test -d kesseldb -c \
    "CREATE TABLE smoke (id BIGINT, n CHAR(16));"
CREATE TABLE

$ PGPASSWORD=admin psql -h 127.0.0.1 -p 5532 -U test -d kesseldb -c \
    "INSERT INTO smoke (id, n) VALUES (1, 'hello');"
INSERT 0 1

$ PGPASSWORD=admin psql -h 127.0.0.1 -p 5532 -U test -d kesseldb -c "SELECT * FROM smoke;"
 id |   n
----+-------
  1 | hello
(1 row)

$ PGPASSWORD=admin psql -h 127.0.0.1 -p 5532 -U test -d kesseldb -c "\dt"
         List of relations
 Schema | Name  | Type  |  Owner
--------+-------+-------+----------
 public | smoke | table | kesseldb
(1 row)

$ PGPASSWORD=admin psql -h 127.0.0.1 -p 5532 -U test -d kesseldb -c "\d smoke"
              Table "public.smoke"
 Column | Type | Collation | Nullable | Default
--------+------+-----------+----------+---------
 id     | int8 |           |          |
 n      | text |           |          |

$ PGPASSWORD=admin psql -h 127.0.0.1 -p 5532 -U test -d kesseldb -c \
    "INSERT INTO smoke (id, n) VALUES (2, 'world'), (3, 'kessel');"
INSERT 0 2

$ PGPASSWORD=admin psql -h 127.0.0.1 -p 5532 -U test -d kesseldb -c "\dn"
  List of schemas
  Name  |  Owner
--------+----------
 public | kesseldb
(1 row)
```

What the real-client smoke caught (and SP-PG-CAT-T8 fixed inline):

- **`BIGINT` / `INTEGER` / `SMALLINT` / `BOOLEAN` are now accepted as
  pure aliases** for `I64` / `I32` / `I16` / `Bool` in `CREATE TABLE`.
  Previously a real psql `CREATE TABLE foo (id BIGINT)` would error
  with `sql: unknown type "BIGINT"`. `INT8` / `INT4` / `INT2` are NOT
  aliased because KesselDB's own `I8` / `I16` / `I32` already use
  those spellings for narrow widths.
- **`\d <name>` is fully supported.** psql ships a 5-query catalog
  walk (OID lookup with `OPERATOR(pg_catalog.~)` + `pg_class`
  relsummary + `pg_attribute` column list + `pg_policy` /
  `pg_inherits` / `pg_trigger` / `pg_statistic_ext` / `pg_publication`
  / `pg_foreign_table` polls); the catalog hook now recognizes every
  shape, synthesizing the live table description for the column-list
  and well-framed empties for the V1-absent surfaces (RLS,
  partitioning, triggers, extended statistics, logical replication,
  foreign data).
- **`\dn` schema-list is fully supported.** Returns the canonical
  single-row `public/kesseldb` table.

What the real-client smoke flagged as a known V1 limitation (NOT a
catalog bug — these are documented PG-wire query-shape boundaries):

- **`SELECT n FROM smoke WHERE id = 1;`** → `V1 PG-wire only renders
  SELECT * FROM <table>`. Same for `SELECT COUNT(*) FROM smoke`.
  The V1 SELECT-rendering path supports `SELECT *` from a single
  table; projected columns + `WHERE` + aggregates go through the
  engine SQL layer in V2 SP-PG-EXEC.

### Limitations (V1)

Honest scope boundary — V1 PG-wire supports CLI clients (psql,
pgcli), programmatic-driver clients (JDBC, psycopg, pgx,
tokio-postgres, sqlx-pg), AND GUI admin / BI tools (per the table
above). Some advanced introspection paths remain V2-deferred:

- **`pg_proc` real function listing** → V1 returns an empty
  `pg_proc` so pgAdmin's "Functions" panel is empty + DataGrip's
  routine browser is empty. V2 SP-PG-CAT-PROC.
- **`pg_database` multi-database** → V1 returns one row
  (`kesseldb`). A tool that lists databases sees only this one;
  KesselDB itself has one logical database today. V2 expands when
  KesselDB grows multi-database (no current plan).
- **`pg_stat_*` runtime statistics** → V1 returns zero rows for
  every pg_stat_* query so prometheus-postgres-exporter reports
  zero metrics + pgAdmin's "Statistics" tab is empty. V2
  SP-PG-CAT-STATS.
- **Arbitrary pg_catalog JOIN/GROUP BY/sub-SELECT** → V1 recognizes
  ~35 canonical query patterns the common tools issue. A tool
  issuing a novel JOIN that doesn't match any pattern still gets
  `42P01`. V2 SP-PG-CAT-AST switches to AST-walking via kessel-sql.
- **psql `\d+` extended output** → V1 covers `\d` (basic table
  description); `\d+` (with comments + size + stats) is partial
  (comments + size columns are NULL). V2.
- **Cross-schema queries** → V1 only knows about `public`. When
  KesselDB grows multi-schema (SP-NS), V1 of this arc auto-extends.
- **Extended Query SHIPPED at V1.1** (SP-PG-EXTQ, 2026-05-29). Parse /
  Bind / Describe / Execute / Sync / Close / Flush all dispatched
  end-to-end. psycopg2 / asyncpg / pgx / tokio-postgres / SQLAlchemy /
  Drizzle / Prisma / JDBC default-EXTQ paths connect at the wire
  level. Full ORM-suite formal verification is SP-PG-EXTQ T8 / T11 /
  T12 (post-V1.1).
- **Typed-parameter compile path** (SP-PG-EXTQ-PARSED, 2026-06-02).
  kessel-sql's lexer now recognizes `$N` (1..99) as a `Tok::Param`
  variant; the new `compile_with_params(sql, cat, params: &[Option<Value>])`
  entry point threads typed `Value`s through the parser WITHOUT ever
  concatenating them into SQL text. Closes the SP-PG-EXTQ V1 §11
  weak-spot #1 attack surface (SQL-text-substitution `'`→`''`
  escaping) for every typed-path-eligible parameter — the bound
  bytes enter as a typed `Value`, get carried verbatim through the
  AST, and emerge in the program as the same `Value`. Adversarial
  KAT locked: a quote-injection payload like `'; DROP TABLE t; --`
  in a bound parameter survives as a `Value::Blob` operand at the
  EQ comparison; the engine never sees the injected SQL.
  Gateway-side classifier (`preprocess_typed_params`) selects the
  typed path for int/text/bytea/bool params and falls back to the
  existing text-substitution path for FLOAT/TIMESTAMPTZ/NUMERIC
  (which still need the cast-wrapper shape `'ISO'::timestamptz`).
  V1 disposition: typed path is opt-in (KAT-only); default remains
  text-substitution to avoid a silent compat regression. Follow-up
  `SP-PG-EXTQ-PARSED-DEFAULT` flips the default after soak.
- **Typed-parameter path is now the DEFAULT** (SP-PG-EXTQ-PARSED-
  DEFAULT, 2026-06-02). `dispatch_execute` routes through
  `EngineApply::apply_sql_with_params(sql, params)` whenever the
  classifier (`preprocess_typed_params`) returns `Some` — every bound
  parameter is then carried as a typed `kessel_codec::Value` over a
  new `PARAMETERIZED_SQL_TAG = 0xF3` admin frame; the engine thread
  decodes + runs `compile_stmt_with_params` against the live
  catalog. **No SQL text concatenation; no `'`→`''` escape rules.**
  Closes the SP-PG-EXTQ V1 §11 weak-spot #1 attack surface at the
  DISPATCH layer (V1 closed it at the kessel-sql + classifier layer
  only). Vulcan-verified with psycopg2 + asyncpg + psycopg3 round-
  trip + quote-injection wire test (`"; DROP TABLE inj_smoke; --`
  stored verbatim, table NOT dropped). Fallback to text-substitute
  path remains for FLOAT/TIMESTAMPTZ/NUMERIC (parameter shapes the
  typed path cannot represent cleanly without widening `Value`).
- **BYTEA-binary preserves arbitrary bytes through the typed path**
  (SP-PG-EXTQ-PARSED-BYTEA-TYPED, 2026-06-02). kessel-sql's
  `Tok::Bytes(Vec<u8>)` + `Lit::Bytes(Vec<u8>)` variants thread raw
  bytes (including non-UTF8 sequences like 0xFF, 0xFE, isolated
  continuation bytes 0x80-0xBF) from a bound `Value::Blob`
  parameter through to the storage layer byte-equal. Drops the
  prior `String::from_utf8_lossy(b).into_owned()` call in
  `rewrite_param_tokens` that corrupted non-UTF8 bytes before they
  reached storage. Vulcan-verified with psycopg3 binary-format
  BYTEA round-trip of payloads `fffefd8090a0b0c0`, `00...00`, and
  `deadbeefcafebabe` — all bytes survive verbatim.
- **One statement per `Q`** → `psql \copy ...; SELECT ...` rejected
  with `42601` syntax_error. Send statements one at a time. V2.
- **Text format only** → every column rendered as PG text;
  binary-format preference (advertised in `Bind`) is rejected with
  `0A000 feature_not_supported` at Bind time. V2 SP-PG-EXTQ-BIN.
- **No `RETURNING`** → `INSERT ... RETURNING id` returns `0A000`
  feature_not_supported. V2.
- **No COPY** → `\copy users FROM 'data.csv'` rejected with `0A000`.
  V2 SP-PG-COPY.
- **No `LISTEN/NOTIFY`** → KesselDB has no changefeeds yet. Skip
  until it does.
- **No `CancelRequest`** → V1 emits BackendKeyData (so clients
  don't refuse to enter the query loop) but ignores incoming
  `CancelRequest` on a separate connection. V2 SP-PG T24.
- **No TLS** → V1 PG-wire is plaintext only. SSLRequest gets the
  'N' reply (continue with cleartext). V2 wires `rustls` behind
  the existing `tls` feature gate.
- **SCRAM-SHA-256 only** → no MD5, no cleartext password, no
  GSSAPI, no LDAP. Every libpq / JDBC / pgx / psycopg since
  2017-2018 supports SCRAM-SHA-256 (PG 10 default), so this is
  rarely a real-world blocker.
- **One credential surface** → V1 has ONE shared-secret Bearer
  token; the PG `user` field is logged but not authorized against
  (V2 SP-PG-USERS adds a real user table + per-user privileges).
- **`SET timezone = …` is a no-op** → V1 accepts the SET statement
  (returns `CommandComplete: SET`) but does not actually rewrite
  subsequent timestamp formatting. `SHOW timezone` always returns
  UTC. V2 wires per-session GUC state.

### Troubleshooting

- **`server closed the connection unexpectedly` from psql** → KesselDB
  binary not built with `--features pg-gateway`, or `KESSELDB_PG_ADDR`
  not set, or `KESSELDB_TOKEN` not set (closed-mode rejects without a
  token).
- **`FATAL: invalid_authorization_specification`** → the Bearer token
  passed via `PGPASSWORD` doesn't match `KESSELDB_TOKEN`. Note: this looks
  identical to "no token set" on the wire (the no-oracle rule — SCRAM
  failure modes don't tell the attacker which input was wrong).
- **`FATAL: sorry, too many clients already`** (SQLSTATE 53300) →
  `pg_max_conns` (default 256) hit. Either close idle clients or raise
  the cap via `ServerConfig.pg_max_conns`.
- **`FATAL: terminating connection due to idle timeout`** (SQLSTATE
  57014) → the connection sent no client message for
  `pg_idle_timeout` (default 600s = 10 min). Either reduce session idle
  time, send a periodic keepalive `SELECT 1`, or raise
  `pg_idle_timeout` for long-lived analytical sessions.
- **`relation "pg_catalog.pg_proc" does not exist`** (SQLSTATE
  42P01) → V1 of the pg_catalog stubs covers `pg_namespace`,
  `pg_class`, `pg_attribute`, `pg_type`, `pg_index`,
  `pg_constraint` + the 5 most-queried `information_schema` views.
  `pg_proc` / `pg_stat_*` / `pg_locks` / `pg_extension` are V2-deferred
  and remain `42P01` — tools that probe these gracefully degrade
  (the affected panel is empty but the connection works). See
  "Limitations (V1)" above for the per-catalog V2 follow-up names.

### Real ORM session (verified 2026-05-29 — SP-PG-EXTQ T7 + T8)

Captured from a real Python session driving the `kesseldb-server`
binary (built with `--features pg-gateway`) on vulcan. Both
`psycopg2` (libpq Extended Query directly) AND SQLAlchemy 2.0
(higher-level ORM atop psycopg2) round-trip end-to-end. T8 (2026-05-29
T8 commit) closes the T7 SQLAlchemy `use_native_hstore=False` caveat
and broadens the matrix to psycopg3 / asyncpg / JDBC. The server
was started with:

```bash
KESSELDB_TOKEN=admin KESSELDB_PG_ADDR=127.0.0.1:5532 \
  ./target/release/kesseldb 127.0.0.1:6532 /tmp/kdb-data
# => KesselDB listening on 127.0.0.1:6532, data dir /tmp/kdb-data, pg=127.0.0.1:5532
```

`Versions: psycopg2 2.9.12 + sqlalchemy 2.0.45 + Python 3.12.3.`
Total `19 / 19 steps pass` on a clean server.

#### Section 1 — psycopg2 (libpq Extended Query)

```python
import psycopg2
conn = psycopg2.connect(host="127.0.0.1", port=5532,
                        user="test", password="admin", dbname="kesseldb")
conn.autocommit = True
cur = conn.cursor()

# CREATE TABLE + INSERT (parameterized via %s).
cur.execute("CREATE TABLE orm_smoke_t7 (id BIGINT, name CHAR(32))")
cur.execute("INSERT INTO orm_smoke_t7 (id, name) VALUES (%s, %s)",
            (1, "hello"))
cur.execute("INSERT INTO orm_smoke_t7 (id, name) VALUES (%s, %s)",
            (2, "world"))

# SELECT * (no params) + parameterized SELECT WHERE.
cur.execute("SELECT * FROM orm_smoke_t7")
print(cur.fetchall())                # → [(1, 'hello'), (2, 'world')]
cur.execute("SELECT * FROM orm_smoke_t7 WHERE id = %s", (1,))
print(cur.fetchall())                # → [(1, 'hello')]

# DISCARD ALL / STATEMENTS / PORTALS — gateway-intercepted (T7).
cur.execute("DISCARD ALL")
print(cur.statusmessage)             # → 'DISCARD ALL'
cur.execute("DISCARD STATEMENTS")
cur.execute("DISCARD PORTALS")

# BEGIN / COMMIT / ROLLBACK / SET TRANSACTION — gateway-intercepted (T7).
cur.execute("BEGIN")
print(cur.statusmessage)             # → 'BEGIN'
cur.execute("COMMIT")
print(cur.statusmessage)             # → 'COMMIT'
cur.execute("ROLLBACK")
cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
print(cur.statusmessage)             # → 'SET'

# SELECT 1 — SQLAlchemy do_ping() probe (T7 pg_catalog hook).
cur.execute("SELECT 1")
print(cur.fetchall())                # → [(1,)]

cur.close()
conn.close()
```

#### Section 2 — SQLAlchemy 2.0

```python
import sqlalchemy as sa

# T8 (2026-05-29) — `use_native_hstore=False` is no longer needed.
# The pg_catalog hook intercepts the canonical psycopg2 hstore-OID
# JOIN probe (`SELECT t.oid, typarray FROM pg_type t JOIN pg_namespace
# ns ON typnamespace = ns.oid WHERE typname = 'hstore'`) and returns a
# 0-row well-framed response, which is the truth — KesselDB has no
# hstore extension.
engine = sa.create_engine(
    "postgresql+psycopg2://test:admin@127.0.0.1:5532/kesseldb",
)

# Full engine.connect() probe sequence + SELECT *.
with engine.connect() as conn:
    rs = conn.execute(sa.text("SELECT * FROM orm_smoke_t7"))
    print(list(rs))                  # → [(1, 'hello'), (2, 'world')]

# Parameterized SELECT via bind-param.
with engine.connect() as conn:
    rs = conn.execute(
        sa.text("SELECT * FROM orm_smoke_t7 WHERE id = :id"),
        {"id": 1},
    )
    print(list(rs))                  # → [(1, 'hello')]

# DISCARD ALL via engine.
with engine.connect() as conn:
    conn.execute(sa.text("DISCARD ALL"))

# Connection-pool checkout/checkin x3 (pool reset triggers DISCARD).
for _ in range(3):
    with engine.connect() as conn:
        list(conn.execute(sa.text("SELECT * FROM orm_smoke_t7")))
```

#### T8 — hstore probe now intercepted (no caveat needed)

SQLAlchemy 2.0 + psycopg2 by default queries pg_type for the `hstore`
type OID at connect:

```sql
SELECT t.oid, typarray
FROM pg_type t JOIN pg_namespace ns ON typnamespace = ns.oid
WHERE typname = 'hstore'
```

T8 ships a matcher in `pg_catalog::catalog_query_hook` that recognizes
this canonical psycopg2/SQLAlchemy probe shape (qualified +
unqualified forms, mixed qualification, case-insensitive, generic
extension typname) and emits a well-framed 0-row response with two
OID columns. psycopg2 then concludes "no hstore extension installed"
— which is the truth, since KesselDB has no extension catalog — and
SQLAlchemy proceeds normally. `use_native_hstore=False` is no longer
required for any modern PG client.

#### What the smoke test covers — 19/19 PASS

| # | Step | Status |
|---|---|---|
| 1 | psycopg2 CREATE TABLE | PASS |
| 2-3 | psycopg2 INSERT (parameterized, 2 rows) | PASS |
| 4 | psycopg2 SELECT * (no params) | PASS |
| 5 | psycopg2 SELECT WHERE id = %s (parameterized) | PASS |
| 6-8 | psycopg2 DISCARD ALL / STATEMENTS / PORTALS — gateway-intercepted | PASS |
| 9-11 | psycopg2 BEGIN / COMMIT / ROLLBACK — tx-control gateway-intercepted | PASS |
| 12 | psycopg2 SET TRANSACTION ISOLATION LEVEL — gateway-intercepted | PASS |
| 13 | psycopg2 SELECT 1 — SQLAlchemy do_ping() probe | PASS |
| 14-15 | psycopg2 cursor + connection close | PASS |
| 16 | SQLAlchemy `engine.connect()` — full probe sequence + SELECT * | PASS |
| 17 | SQLAlchemy parameterized SELECT (BindParam) | PASS |
| 18 | SQLAlchemy DISCARD ALL via engine | PASS |
| 19 | SQLAlchemy connection pool checkout/checkin x3 | PASS |

#### Broader ORM compat matrix (T3, 2026-06-01 — SP-PG-EXTQ-BIN unlock)

T8 ran a deeper compat smoke against the drivers psycopg2 + SQLAlchemy
already covered. SP-PG-EXTQ-BIN T3 then lifted the binary-format
parameter gap for the V1 supported PG types (INT2/INT4/INT8/FLOAT4/
FLOAT8/BOOL/TEXT/VARCHAR/BYTEA/TIMESTAMPTZ). SP-PG-EXTQ-BIN-RESULTS T3
then closed the binary-RESULTS gap (the asterisk on asyncpg) by adding
the symmetric DataRow + RowDescription post-processor. Each row is
the actual driver session — see
`docs/superpowers/sppgextqbinr-t3-smoke-2026-06-01.txt` for the
SP-PG-EXTQ-BIN-RESULTS T3 transcript (asyncpg fetch round-trip),
`docs/superpowers/sppgextqbin-t3-smoke-2026-06-01.txt` for the earlier
SP-PG-EXTQ-BIN T3 transcript, and `docs/superpowers/sppgextq-t8-orm-
smoke-2026-05-29.txt` for the original T8 baseline.

| Driver          | Status   | Notes                                              |
|-----------------|----------|----------------------------------------------------|
| psycopg2 2.9.12 | PASS     | T7 baseline (19/19 steps)                          |
| SQLAlchemy 2.0  | PASS     | T8 closes the `use_native_hstore=False` caveat     |
| psycopg3 3.3.4  | PASS     | SP-PG-EXTQ-BIN T3 — DEFAULT cursor (NOT ClientCursor) works end-to-end |
| asyncpg 0.31.0  | PASS     | SP-PG-EXTQ-BIN-RESULTS T3 — fetch() round-trip works end-to-end (binary params + binary results) |
| JDBC 42.7       | PASS    | SP-PG-SQL-PAREN-VALUES T3 (2026-06-02) — real pgJDBC 42.7.4 + OpenJDK 21 against KesselDB on vulcan. **Full CRUD PASS in both simple AND extended modes**: CREATE TABLE, `PreparedStatement` INSERT (`setLong` + `setString`), SELECT \*, `PreparedStatement` SELECT `WHERE id = ?`, `SELECT version()`. In extended mode pgJDBC uses binary Bind (SP-PG-EXTQ-BIN) + binary result columns (SP-PG-EXTQ-BIN-RESULTS); in simple mode pgJDBC substitutes the param client-side and emits the post-strip shape `VALUES (('42'), ('hello-jdbc'))` / `WHERE id = ('42')` which the kessel-sql VALUES tuple parser + WHERE term parser now accept (paren-wrapped literals up to depth 8 + `Str → numeric` coercion on numeric column LHS). Smoke: `docs/superpowers/sppgsqlparenvalues-t3-smoke-2026-06-02.txt`. |
| pgx (Go)        | n/a      | Go runtime not on vulcan (V2 `SP-PG-GO-SMOKE`)     |
| Drizzle (Node)  | n/a      | Node runtime not on vulcan (V2 `SP-PG-NODE-SMOKE`) |
| Prisma (Node)   | n/a      | Node runtime not on vulcan (V2 `SP-PG-NODE-SMOKE`) |
| sqlx (Rust)     | n/a      | Same binary-Bind + binary-RESULTS unlock; not yet smoke-tested on vulcan |

SP-PG-EXTQ-BIN T3 wired the binary-format decoder into the Bind path:
each parameter with `format_code=1` (binary) at position `i` is
admitted iff `param_oids[i]` is one of the V1 supported PG types
(INT2/INT4/INT8/FLOAT4/FLOAT8/BOOL/TEXT/VARCHAR/BYTEA/TIMESTAMPTZ),
then decoded at Execute time into a SQL literal that flows through
the existing substitute layer (bare-int literal for integers, single-
quoted + `'`→`''`-escaped for text, `'\xHEX'::bytea` for bytea, etc).
Describe('S') synthesizes ParameterDescription from the SQL's `$N`
count when Parse omitted OID hints (V1 emits PG_TYPE_TEXT for each
position; clients encode text-as-binary which routes through the
existing text path).

SP-PG-EXTQ-BIN-RESULTS T2 then wired the symmetric result-side
post-processor: when the portal's Bind requested `result_formats=[1]`
(asyncpg / JDBC default extended mode / sqlx), `dispatch_execute`
re-encodes each buffered DataRow per-column into PG binary format +
flips the per-field `format_code` slot in RowDescription in lockstep
so libpq's per-field decoder switches to its binary read path. NULL
columns and text-format columns pass through unchanged; the rewrite
is zero-cost for the existing text-only path.

**SP-PG-EXTQ-BIN-NUMERIC (2026-06-02) — Decimal/BigDecimal round-trip
unlocked.** The V1 BIN + BIN-RESULTS arcs deferred NUMERIC binary
because the PG wire shape is base-10000 variable-length-digit (sign +
dscale + weight + N i16 digits) and bug-prone. This follow-up arc
ships a pure-Rust NUMERIC codec in `crates/kessel-pg-gateway/src/extq/
binary_numeric.rs` (`decode_numeric_binary` + `encode_numeric_binary`
+ `BinaryNumericError`) covering `|value| < 10^18` with ≤18 fractional
digits — the typical ORM Decimal/BigDecimal range. Wired into both
`extq::substitute::decode_binary_param` (Bind path) and
`extq::binary_results::encode_binary_value` (Execute result path). The
`binary_format_supported_for_oid` / `binary_result_supported_for_oid`
predicates now include PG_TYPE_NUMERIC (OID 1700). Wider values reject
with the precise `SP-PG-EXTQ-BIN-NUMERIC-BIGNUM` follow-up. NaN /
+Infinity / -Infinity (`sign=0xC000` / `0xD000` / `0xF000`) decode to
the canonical PG strings `"NaN"` / `"Infinity"` / `"-Infinity"` and
encode from case-insensitive variants of the same strings (including
short `"inf"` / `"+inf"` / `"-inf"` aliases) — closed by
**SP-PG-EXTQ-BIN-NUMERIC-NAN-INF (2026-06-02)** at the codec layer;
the engine-level NUMERIC storage of these specials remains a separate
follow-up (FieldKind::I128 has no native NaN/Inf representation).
Vulcan smoke transcript:
`docs/superpowers/sppgextqbinnumeric-t4-smoke-2026-06-02.txt`
(psycopg2 + asyncpg both decode `Decimal('42')` / `Decimal('-7')` /
`Decimal('999999999')` from kesseldb-emitted NUMERIC binary DataRow).
COPY binary NUMERIC also works end-to-end after
**SP-PG-COPY-BIN-NUMERIC V1 (2026-06-02)** — the same codec routes
through the COPY-BIN admission + per-row encode/decode paths
(`docs/superpowers/sppgcopybinnumeric-t3-smoke-2026-06-02.txt`).

The remaining residual ORM gaps are:
- ~~JDBC simple-query mode hits a kessel-sql parser gap on `::int8`
  casts~~ → **CLOSED 2026-06-02 by SP-PG-EXTQ-CAST T2** —
  `cast_stripper::strip_pg_casts` removes `::TYPE[(args)]` from SQL
  text at `dispatch_query` entry (preserving string/comment context).
  psql proxy round-trip for `SELECT 1::int8` / `WHERE id = $1::int8` /
  `INSERT ... VALUES (3::int8, 'x'::text)` verified on vulcan
  (`docs/superpowers/sppgextqcast-t3-smoke-2026-06-02.txt`). Real
  pgJDBC round-trip then verified by **SP-PG-JDBC-SMOKE T2**
  (`docs/superpowers/sppgjdbcsmoke-t2-smoke-2026-06-02.txt`) — JDBC
  simple-mode `WHERE id = 42::int8` round-trips end-to-end through
  the actual pgJDBC 42.7.4 driver against KesselDB. The cast-stripper
  is closed end-to-end. ~~Simple-mode `PreparedStatement` paren-
  wrapped VALUES~~ → **CLOSED 2026-06-02 by SP-PG-SQL-PAREN-VALUES
  V1** — `kessel-sql`'s VALUES tuple parser now accepts
  `(LITERAL)` paren-wrapped literals up to depth 8 (anti-stack-bomb
  cap at 9 levels), and the same arc adds `Str → numeric` coercion
  in the WHERE term parser when the LHS is a numeric column (PG's
  `'42'::int8` semantic preserved across the cast strip). Real
  pgJDBC simple-mode `PreparedStatement` INSERT + SELECT `WHERE id =
  ?` round-trip end-to-end on vulcan
  (`docs/superpowers/sppgsqlparenvalues-t3-smoke-2026-06-02.txt`).
  ~~Extended-mode `SELECT version()` Describe/NoData ordering~~ →
  **CLOSED 2026-06-02 by SP-PG-EXTQ-DESCRIBE-VERSION V1** — the
  gateway's `extq::row_description_or_no_data_for_sql` helper now
  recognizes the closed set of scalar SELECTs that SP-PG-EXTQ T7
  added Simple-Query handlers for (`SELECT version()`,
  `SELECT current_user`, `SELECT 1`, etc.) and emits the matching
  `RowDescription` at Describe time instead of `NoData`. pgJDBC
  extended-mode `SELECT version()` round-trips end-to-end via real
  pgJDBC 42.7.4 on vulcan
  (`docs/superpowers/sppgextqdescribeversion-t3-smoke-2026-06-02.txt`).
- ~~Parameterized SELECT with a CHAR(N) WHERE clause may match zero rows
  because the engine's EQ-on-Char doesn't ignore trailing NUL padding
  on the storage side; lifts in `SP-CHAR-PAD-COMPARE` (engine-side).~~
  → **CLOSED 2026-06-02 by SP-CHAR-PAD-COMPARE V1** — the engine's
  `kessel-expr` EQ / NE / LT / LE / GT / GE opcodes (and the engine-wide
  `kessel-sm::cmp_field` helper) now treat trailing NUL (0x00) and
  space (0x20) as insignificant on `Char(_)` / `Bytes(_)` byte
  comparisons (PG SQL §9.20 semantic, with the storage-aware NUL
  widening — engine stores fixed-width values NUL-padded). asyncpg
  `WHERE name = $1` against `CHAR(32)` now returns the matching row
  on vulcan; BETWEEN / NE also work; the Describe-on-`$N` enabler
  (substitute `$N` with NULL for the table-name probe) closes the
  asyncpg ProtocolError that the engine fix unmasked. Storage /
  indexes / hashing UNCHANGED — only the comparison layer trims. +15
  KATs across kessel-expr / kessel-sm / kessel-pg-gateway. Smoke
  transcript: `docs/superpowers/spcharpadcompare-t3-smoke-2026-06-02.txt`.
- Binary NUMERIC / JSONB / UUID / ARRAY remain V2 (`SP-PG-EXTQ-BIN-
  NUMERIC` / `SP-PG-EXTQ-BIN-EXTRA`).
- ~~SP-PG-EXTQ-CAST V1 is "strip + hope" — a Bind whose declared
  param OID disagrees with the SQL's `$N::TYPE` cast silently
  coerces.~~ → **CLOSED 2026-06-02 by SP-PG-EXTQ-CAST-VALIDATE V1** —
  `cast_stripper::strip_pg_casts_tracked` returns `(stripped_sql,
  Vec<($N_index, declared_oid)>)`; `PreparedStmt.param_casts` stores
  the pairs at Parse time; `dispatch_bind` rejects any mismatch
  between the bound parameter OID and the declared cast OID with
  `42846 cannot_coerce`. Closes the silent-coercion attack vector
  the parent arc's "V1 scope is strip + hope" note explicitly
  flagged. Literal casts (no `$N`) bypass the validator so the
  parent arc's psql shapes still PASS. Smoke:
  `docs/superpowers/sppgextqcastvalidate-t3-smoke-2026-06-02.txt`.
- ~~SP-PG-EXTQ-CAST-VALIDATE V1 enforces STRICT OID equality — pgJDBC's
  default Java-`int` against `::int8` cast (and psycopg3's
  Python-`int` against `::int8`) false-rejected with 42846 because
  the wire-supplied INT4 OID didn't equal the declared INT8 OID.~~ →
  **CLOSED 2026-06-02 by SP-PG-EXTQ-CAST-VALIDATE-COMPAT V1** — V1
  strict equality relaxes to PG's `pg_type.dat::typcategory` table.
  `types::oid_category(oid)` returns the category byte ('N' numeric,
  'S' string, 'B' bool, 'D' date/time, 'U' unknown/bytea);
  `types::oid_castable(param_oid, cast_oid)` accepts the pair iff
  strict equality OR `param_oid == 0` (omitted hint skip) OR
  same-category widening. `dispatch_bind`'s validator swaps the
  strict `!=` check for `!oid_castable(...)`. Intra-category
  widenings now accept (INT4↔INT8, INT8↔FLOAT8, INT4↔NUMERIC,
  TEXT↔VARCHAR, etc.); cross-category mismatches (TEXT vs INT8,
  BOOL vs INT8, BYTEA vs TEXT) STILL reject with the same
  `ExtqError::CastOidMismatch` → `42846 cannot_coerce` wire frame
  so the V1 silent-coercion vector stays closed. **vulcan-verified**
  via psycopg3 PQ-layer 5-case smoke
  (`docs/superpowers/sppgextqcastvalidatecompat-t3-smoke-2026-06-02.txt`):
  INT4+INT8 / INT8+INT4 / TEXT+VARCHAR all accept; cross-category
  TEXT+INT8 still rejects with the exact 42846 message; strict-
  equality INT8+INT8 still works. +14 KATs across types::tests +
  extq::tests. V2 follow-ups named:
  `SP-PG-EXTQ-CAST-VALIDATE-COMPAT-RANGE` (overflow-check the
  param value vs cast-type range),
  `SP-PG-EXTQ-CAST-VALIDATE-LITERAL` (validate literal casts too),
  `SP-PG-EXTQ-CAST-VALIDATE-CATEGORY-CROSS` (accept the cross-
  category casts PG itself accepts, e.g. TEXT '42' → INT8).

#### Pipelining throughput (T8, 2026-05-29)

Single-statement round-trip throughput measured on vulcan with
psycopg2 (no libpq pipeline mode):

| Workload                        | N    | Elapsed | Throughput      |
|---------------------------------|------|---------|-----------------|
| INSERT (parameterized)          | 1000 | 3.97 s  | 252 stmt/s      |
| SELECT WHERE id=%s + fetchall   | 1000 | 2.47 s  | 404 stmt/s      |
| SELECT WHERE id=%s (loop only)  | 1000 | 2.45 s  | 409 stmt/s      |

Latency-bound (SOCK_STREAM + Parse/Bind/Execute/Sync flush cost per
statement). A libpq-pipeline-mode test would batch up to 8 messages
and post higher numbers; that's V2 `SP-PG-EXTQ-PIPELINE-BATCH`.

### Spec + design

- SP-PG wire spec: `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`
- SP-PG progress (closed): `docs/superpowers/specs/2026-05-27-kesseldb-subproject-sppg-progress.md`
- SP-PG-CAT pg_catalog stubs spec: `docs/superpowers/specs/2026-05-27-kesseldb-sppgcat-pg-catalog-design.md`
- SP-PG-CAT progress (closed at T8): `docs/superpowers/specs/2026-05-27-kesseldb-subproject-sppgcat-progress.md`
- SP-PG-EXTQ design spec: `docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`
- SP-PG-EXTQ progress (T7 — hardening + real ORM smoke): `docs/superpowers/specs/2026-05-28-kesseldb-subproject-sppgextq-progress.md`
- SP-PG-EXTQ-BIN design spec: `docs/superpowers/specs/2026-06-01-kesseldb-sppgextq-bin-design.md`
- SP-PG-EXTQ-BIN progress (V1 SHIPPED at T3 — binary-format params unlock): `docs/superpowers/specs/2026-06-01-kesseldb-subproject-sppgextqbin-progress.md`
- SP-PG-COPY design spec: `docs/superpowers/specs/2026-05-30-kesseldb-sppgcopy-design.md`
- SP-PG-COPY progress (V1 SHIPPED at T4 — pg_dump / sysbench / `\copy` bulk-load): `docs/superpowers/specs/2026-05-30-kesseldb-subproject-sppgcopy-progress.md`
- SP-PG-COPY-BULKAPPLY design spec: `docs/superpowers/specs/2026-05-30-kesseldb-sppgcopybulkapply-design.md`
- SP-PG-COPY-BULKAPPLY progress (V1 SHIPPED — 181.9× COPY throughput lift via per-batch Op::Txn fold): `docs/superpowers/specs/2026-05-30-kesseldb-subproject-sppgcopybulkapply-progress.md`
- SP-PG-COPY-CSV design spec: `docs/superpowers/specs/2026-06-01-kesseldb-sppgcopycsv-design.md`
- SP-PG-COPY-CSV progress (V1 SHIPPED at T2 — CSV format with quoting / HEADER / custom DELIMITER+QUOTE+ESCAPE+NULL): `docs/superpowers/specs/2026-06-01-kesseldb-subproject-sppgcopycsv-progress.md`
- SP-PG-COPY-CSV-NUMERIC design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgcopycsvnumeric-design.md`
- SP-PG-COPY-CSV-NUMERIC progress (V1 SHIPPED at T3 — text+CSV NUMERIC validator with canonical/sign normalisation + case-insensitive NaN/Inf acceptance + precise 22P02 rejections): `docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgcopycsvnumeric-progress.md`
- SP-PG-COPY-CSV-NUMERIC-SCI design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgcopycsvnumericsci-design.md`
- SP-PG-COPY-CSV-NUMERIC-SCI progress (V1 SHIPPED at T3 — scientific notation in text/CSV NUMERIC validator, mantissa+exponent expansion to canonical decimal text, |exp|<=100 cap): `docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgcopycsvnumericsci-progress.md`
- SP-PG-COPY-CSV-NUMERIC-SCI smoke transcript (V1 SHIPPED — HEADLINE: scientific notation round-trips end-to-end on vulcan; 1e10/6e3/-3.14e2/1.5e3 expand cleanly; out-of-range and missing-exponent reject with precise 22P02): `docs/superpowers/sppgcopycsvnumericsci-t2-smoke-2026-06-02.txt`
- SP-PG-EXTQ-CAST design spec: `docs/superpowers/specs/2026-06-01-kesseldb-sppgextqcast-design.md`
- SP-PG-EXTQ-CAST smoke transcript (V1 SHIPPED — psql `SELECT 1::int8` round-trip PASS): `docs/superpowers/sppgextqcast-t3-smoke-2026-06-02.txt`
- SP-PG-EXTQ-CAST-VALIDATE design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqcastvalidate-design.md`
- SP-PG-EXTQ-CAST-VALIDATE smoke transcript (V1 SHIPPED — HEADLINE: $N cast OID mismatch returns 42846 cannot_coerce via psycopg3 PQ-layer): `docs/superpowers/sppgextqcastvalidate-t3-smoke-2026-06-02.txt`
- SP-PG-EXTQ-CAST-VALIDATE-COMPAT design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqcastvalidatecompat-design.md`
- SP-PG-EXTQ-CAST-VALIDATE-COMPAT smoke transcript (V1 SHIPPED — HEADLINE: pgJDBC INT4 param + INT8 cast accepted; cross-category TEXT + INT8 still rejects with 42846): `docs/superpowers/sppgextqcastvalidatecompat-t3-smoke-2026-06-02.txt`

### SP-PG-COPY — `COPY FROM STDIN` / `COPY TO STDOUT` bulk load (V1 SHIPPED 2026-05-30)

PG's `COPY` command is the bulk-load lever every modern pg_dump
restore, sysbench `prepare` phase, and analyst-friendly
`psql \copy ... CSV` workflow uses — the same wire shape every
PostgreSQL-aware ETL tool defaults to. V1 ships text format
end-to-end for both directions; CSV + binary deferred to V2 arcs
(`SP-PG-COPY-CSV`, `SP-PG-COPY-BIN`).

```bash
# COPY FROM STDIN — text format, the pg_dump default
psql -h kesseldb -p 5532 -U test -d kesseldb -c \
  "COPY users FROM STDIN" < users.tsv
# → COPY 1000

# psql \copy is the client-side wrapper around the same wire shape
psql -h kesseldb -p 5532 -U test -d kesseldb -c \
  '\copy users FROM /path/to/users.tsv'

# COPY TO STDOUT — text format
psql -h kesseldb -p 5532 -U test -d kesseldb -c \
  "COPY users TO STDOUT" > users.tsv

# Round-trip: export then re-import produces an identical row set.

# Optional column list works in both directions.
psql -h kesseldb -p 5532 -U test -d kesseldb -c \
  "COPY users (id, name) FROM STDIN" < partial.tsv
psql -h kesseldb -p 5532 -U test -d kesseldb -c \
  "COPY users (id, name) TO STDOUT" > partial.tsv

# NULL columns use the PG-canonical `\N` sentinel.
printf '10\tfoo\n20\t\\N\n30\tbar\n' | \
  psql -h kesseldb -p 5532 -U test -d kesseldb -c \
    "COPY t (id, label) FROM STDIN"
# → rows with id=20 have label=NULL
```

V1 text-format escapes per PG §COPY-FORMATS: rows separated by
`\n`, fields by `\t`, NULL is `\N`, the 7 PG-canonical backslash
escapes (`\b \f \n \r \t \v \\`) are recognized on input + emitted
on output, and the legacy v2 end-of-data marker `\.` is tolerated
on input.

Connection state model: a `COPY ... FROM STDIN` Query transitions
the connection to `CopyIn` state — the server then accepts only
`CopyData`, `CopyDone`, `CopyFail`, or `Terminate` until the COPY
exchange ends. Any other frontend tag in CopyIn = `08P01
protocol_violation` + clean error + state cleared. A subsequent Q
or extended-query message works normally — the connection STAYS
ALIVE across COPY errors (matching the SP-PG-EXTQ tolerant
probe-then-fall-back contract).

**Abort-tail drain (SP-PG-COPY-ABORT-DONE-TAIL V1, 2026-06-02):**
when a per-row error mid-CopyData emits `ErrorResponse + RFQ` and
transitions back to Idle, the client may still be flushing trailing
`CopyData` / `CopyDone` / `CopyFail` frames it had already queued
before observing the error. Per PG §55.2.7 the server silently
drains those tail frames without emitting any additional response.
KesselDB does the same: the connection stays alive, the tail bytes
are absorbed without a spurious `unsupported message tag` `08P01`,
and the next Query (`SELECT` / `COPY` / extq `Parse` / etc.)
succeeds on the SAME TCP connection. A stray `c` / `f` in pristine
Idle with no preceding abort still rejects with `08P01` (defensive
shape against a truly broken client). Vulcan psql 16 smoke
transcript:
`docs/superpowers/sppgcopyaborttail-t3-smoke-2026-06-02.txt`.

**Throughput** (on vulcan, 100K rows of `(BIGINT, CHAR(64))`, 2026-05-30):
**~51,840 rows/sec** with SP-PG-COPY-BULKAPPLY (default
`KESSELDB_COPY_BATCH_SIZE=1024`). The V1 per-row baseline was
~285 rows/sec; BULKAPPLY V1 lifts **181.9×** by folding N rows into a
single multi-row `INSERT INTO t (cols) VALUES (...), (...), ...`
which kessel-sql compiles to `Op::Txn { ops: Vec<Op::Create> }` —
one apply round-trip + one WAL fsync per batch instead of one per
row. Tunable via `KESSELDB_COPY_BATCH_SIZE` env at server start
(clamped to `[1, 65536]`); set to `1` to restore V1-baseline shape.
Postgres 16 reference on the same workload: ~578K rows/sec — KesselDB
is now within ~11× of Postgres COPY throughput (was ~2000× behind).
Bench transcript: `docs/superpowers/sppgcopybulkapply-t3-bench-2026-05-30.txt`.

**Atomicity** vs PG: SP-PG-COPY-BULKAPPLY V1 is **per-batch
atomic** — each batch (default 1024 rows) is wrapped in an `Op::Txn`,
so any inner-op failure rolls back the whole batch. Real PG is
**whole-COPY atomic** (an implicit transaction wraps every row in
the COPY). A constraint failure at row 1500 of 10000 with the
default batch size: rows 1-1024 stay committed; rows 1025-1500's
batch rolls back; COPY aborts. The named follow-up arc
`SP-PG-COPY-BULKAPPLY-WHOLECOPY` would close the rest of the gap
(gated on an engine-side streaming-Txn shape landing first).

**NULL-row fallback**: a batch containing any `\N` NULL field falls
back to per-row dispatch (the column-omit trick V1 relies on for
NULL handling requires per-row column lists, which multi-row INSERT
can't carry). Throughput on NULL-heavy tables is therefore similar to
the V1 baseline; throughput on all-non-NULL tables (sysbench /
pg_dump common case) lands the headline lift.

**V1 NULL handling caveat**: kessel-sql's `INSERT VALUES` parser
has no `NULL` keyword. SP-PG-COPY V1 works around this by OMITTING
NULL columns from the synthesized `INSERT (col, col, ...) VALUES
(...)` — kessel-sql's SP86 default-fill semantics for omitted
nullable columns then applies. This means a NOT NULL column
receiving `\N` surfaces as a clean `23502 not_null_violation`
error at ingest time (matching PG).

### SP-PG-COPY-CSV — CSV format (V1 SHIPPED 2026-06-01)

CSV format unlocks `pg_dump --csv` + every spreadsheet/analyst
on-ramp (Excel, Sheets, R, `pandas.read_csv`). RFC 4180 grammar
with the PG superset (HEADER + custom DELIMITER / QUOTE / ESCAPE /
NULL).

```bash
# COPY FROM CSV with HEADER (the pg_dump --csv default shape)
psql -h kesseldb -p 5532 -U test -d kesseldb -c \
  "COPY users FROM STDIN WITH (FORMAT csv, HEADER)" < users.csv
# → COPY 1000  (the header row is skipped)

# COPY TO CSV with HEADER — exports a spreadsheet-openable file
psql -h kesseldb -p 5532 -U test -d kesseldb -c \
  "COPY users TO STDOUT WITH (FORMAT csv, HEADER)" > users.csv

# Embedded comma / embedded quote / NULL all round-trip byte-equal:
#   1,"Alice, the brave"        ← quoted because of the embedded comma
#   2,"Bob ""the builder"""     ← doubled-quote escape inside the value
#   3,Charlie                   ← bare unquoted
#   4,                          ← empty unquoted = NULL (default)

# Custom delimiter + NULL marker for CSVs exported from Sheets etc.
psql -h kesseldb -p 5532 -U test -d kesseldb -c \
  "COPY t FROM STDIN WITH (FORMAT csv, HEADER, DELIMITER ';', NULL '<NA>')" \
  < euro-style.csv
# → ';' splits fields; '<NA>' decodes as NULL
```

V1 CSV codec honors per PG §SQL-COPY CSV defaults: delimiter `,`,
quote `"`, escape = quote (so `""` is the doubled-quote escape;
configure a distinct `ESCAPE 'X'` to use the alternate single-char
escape shape), NULL = empty unquoted (a quoted empty `""` stays
empty-string, distinct from NULL). HEADER on input consumes the
first record; on output emits a first record containing the resolved
column names. The CSV record parser is multi-line aware — a quoted
field containing literal newlines reassembles correctly across
CopyData frame boundaries via the carry buffer.

Rejected CSV options surface precise V2-pointing errors:

```text
ERROR:  COPY csv option FORCE_QUOTE not supported in V1 (SP-PG-COPY-CSV-FORCEQUOTE / SP-PG-COPY-CSV-ENCODING)
ERROR:  COPY csv DELIMITER must be a single character (got '||')
```

CSV format inherits the SP-PG-COPY-BULKAPPLY V1 batching throughput
+ NULL-row fallback semantics — the codec is a payload concern only.
Smoke transcript: `docs/superpowers/sppgcopycsv-t2-smoke-2026-06-01.txt`.

#### SP-PG-COPY-CSV-NUMERIC — canonical NUMERIC validator (V1 SHIPPED 2026-06-02)

Both text + CSV COPY now validate the canonical PG NUMERIC text
grammar at the gateway BEFORE handing the row to the engine, with
sign normalisation + case-insensitive NaN/Infinity acceptance:

```bash
# Sign normalisation — +999 stored as 999:
echo 'id,amount
1,42
2,12345.6789
3,-3
4,+999' | psql -c "COPY t FROM STDIN WITH (FORMAT csv, HEADER)"

# Case-insensitive NaN / Infinity / -Infinity (validator pass —
# canonicalised to mixed-case PG form before reaching the engine):
#   nan → NaN
#   infinity / +infinity / inf / +inf → Infinity
#   -infinity / -inf → -Infinity

# Malformed input rejects with precise 22P02 + row + column + reason:
psql -c "COPY t FROM STDIN WITH (FORMAT csv)" <<< $'id,amount\n1,1.2.3\n'
# ERROR:  COPY csv row 1 column "amount" NUMERIC: malformed (multiple decimal points)
psql -c "COPY t FROM STDIN WITH (FORMAT csv)" <<< $'id,amount\n1,hello\n'
# ERROR:  COPY csv row 1 column "amount" NUMERIC: bad byte 0x68 at position 0
psql -c "COPY t FROM STDIN WITH (FORMAT csv)" <<< $'id,amount\n1,1e10\n'
# ERROR:  COPY csv row 1 column "amount" NUMERIC: scientific notation not supported in V1 (SP-PG-COPY-CSV-NUMERIC-SCI)
```

The validator runs on every column whose PG type OID resolves to
`PG_TYPE_NUMERIC` (1700 — kessel-sql `I128`, `U128`, `Fixed`); other
column types pass through unchanged. NULL fields are forwarded
verbatim (text `\N`; CSV empty unquoted) so the kessel-sql
column-omit auto-NULL-fill semantics keep working.

V1 limitations (each with its own follow-up arc):

- **`SP-PG-COPY-CSV-NUMERIC-SCI`** — scientific notation lifted in
  V1 (see next subsection — `1e10`, `1.5e-3`, `6.022e23`, `-3.14e2`
  now expand cleanly to canonical decimal text in the validator).
- **`SP-PG-COPY-NUMERIC-BIGNUM`** — values beyond the kessel-sql
  i128 (\|value\| < 10^18) cap surface at INSERT time, not at the
  validator.
- **NaN/Infinity engine storage** — the validator accepts and
  canonicalises NaN/Infinity, but the engine-side I128 literal
  parser cannot store them yet (engine surfaces `sql: expected
  value`). A separate arc lifts the engine-storage gap.

Smoke transcript: `docs/superpowers/sppgcopycsvnumeric-t2-smoke-2026-06-02.txt`.

#### SP-PG-COPY-CSV-NUMERIC-SCI — scientific notation (V1 SHIPPED 2026-06-02)

Both text + CSV COPY now also accept scientific-notation NUMERIC
fields and expand the exponent into the canonical PG decimal text
BEFORE the row reaches the engine:

```bash
# Integer-yielding scientific notation expands cleanly end-to-end:
echo 'id,val
1,1e10
2,6e3
3,-3.14e2
4,1.5e3' | psql -c "COPY t FROM STDIN WITH (FORMAT csv, HEADER)"

# Stored canonical values:
#   1e10      → 10000000000
#   6e3       → 6000
#   -3.14e2   → -314
#   1.5e3     → 1500

# Avogadro-style large exponents in the |exp|<=100 band:
#   6.022e23  → 602200000000000000000000

# Out-of-range exponent (|exp|>100) rejects with precise 22P02:
psql -c "COPY t FROM STDIN WITH (FORMAT csv)" <<< $'id,val\n1,1e1000\n'
# ERROR:  COPY csv row 1 column "val" NUMERIC: malformed (exponent out of range)

# Missing exponent / malformed exponent reject with precise 22P02:
psql -c "COPY t FROM STDIN WITH (FORMAT csv)" <<< $'id,val\n1,1e\n'
# ERROR:  COPY csv row 1 column "val" NUMERIC: malformed (missing exponent)
psql -c "COPY t FROM STDIN WITH (FORMAT csv)" <<< $'id,val\n1,1e1.5\n'
# ERROR:  COPY csv row 1 column "val" NUMERIC: malformed (non-integer exponent)
```

Grammar accepted: `[+-]?(\d+(\.\d+)?|\.\d+)[eE][+-]?\d+` — mantissa
(integer or integer+fractional or leading-dot-fractional) + `e`/`E`
(case-insensitive) + signed integer exponent. Trailing-dot mantissa
(`5.e2`) is the named follow-up arc
`SP-PG-COPY-CSV-NUMERIC-SCI-TRAILDOT` (no ORM/spreadsheet emits it
in practice; rejected with the arc name in the 22P02 reason).

The expansion uses a decimal-point-shift algorithm (no bigint dep):
the mantissa's digit string is shifted by `exp - frac_digit_count`
places. Leading-zero padding handles `1e-3` → `0.001`. Negative-
zero canonicalises to `0` (matches V1 `-0` → `0` rule).

V2 follow-ups:

- **`SP-PG-COPY-CSV-NUMERIC-SCI-TRAILDOT`** — trailing-dot mantissa.
- **`SP-PG-COPY-NUMERIC-BIGNUM`** — fractional results from
  negative-exponent scientific (e.g. `1.5e-3` → `0.0015`) pass the
  validator but the engine-side I128 literal parser only stores
  integer values; the engine surfaces `sql: expected value`. Same
  pre-existing gap V1 documented for NaN/Infinity.

Smoke transcript: `docs/superpowers/sppgcopycsvnumericsci-t2-smoke-2026-06-02.txt`.

### SP-PG-COPY-BIN — binary format (V1 SHIPPED 2026-06-02)

PG binary COPY per §55.2.7 — `WITH (FORMAT binary)`. The wire format
every `pg_dump --format=custom` restore + every JDBC
`CopyManager.copyIn(PGCopyOutputStream...)` + every modern ETL
binary-bulk-loader (`pg_bulkload`, `pgloader`, Stitch, Fivetran,
Airbyte) hard-requires. After this arc shipped, those workflows succeed
against KesselDB end-to-end.

```bash
# COPY TO STDOUT binary — emits the canonical PGCOPY\n\xff\r\n\0
# signature header + length-prefixed binary values + 0xff 0xff EOD.
psql -h kesseldb -p 5532 -U test -d kesseldb -c \
  "COPY users TO STDOUT WITH (FORMAT binary)" > users.bin
hexdump -C users.bin | head -2
# 00000000  50 47 43 4f 50 59 0a ff  0d 0a 00 00 00 00 00 00  |PGCOPY..........|
# 00000010  00 00 00 00 02 00 00 00  08 ...

# COPY FROM STDIN binary — round-trips into a fresh table.
psql -h kesseldb -p 5532 -U test -d kesseldb -c \
  "COPY users2 FROM STDIN WITH (FORMAT binary)" < users.bin
# → COPY 3
```

V1 supports the same 11 column types as SP-PG-EXTQ-BIN + SP-PG-EXTQ-BIN-
NUMERIC (param/result binary): BOOL, INT2/INT4/INT8, FLOAT4/FLOAT8,
TEXT/VARCHAR, BYTEA, TIMESTAMPTZ, and **NUMERIC** (the latter shipped at
SP-PG-COPY-BIN-NUMERIC V1, 2026-06-02 — reuses the
`extq::binary_numeric` codec the EXTQ-BIN-NUMERIC arc shipped, for
`|value| < 10^18` with ≤18 fractional digits). Tables with UUID /
JSONB / ARRAY columns are pre-rejected at COPY-start with a precise
V2-arc-pointing message:

```text
ERROR:  COPY binary: column "data" type OID 3802 not supported in V1 (SP-PG-COPY-BIN-EXTRA)
```

The binary codec reuses the existing SP-PG-EXTQ-BIN-RESULTS encoder
(`encode_binary_value`) and SP-PG-EXTQ-BIN param decoder
(`decode_binary_param`) verbatim — only the framing layer
(`copy::binary` — 19-byte signature header, per-row length-prefixed
field values, 2-byte i16 -1 end-of-data marker) is new. Inherits the
SP-PG-COPY-BULKAPPLY V1 batching throughput.

Smoke transcripts:
- `docs/superpowers/sppgcopybin-t3-smoke-2026-06-02.txt` (V1 — 10 types)
- `docs/superpowers/sppgcopybinnumeric-t3-smoke-2026-06-02.txt`
  (SP-PG-COPY-BIN-NUMERIC — NUMERIC round-trip incl. negative + byte-equal
  re-export md5 match).

Rejected variants surface precise V2-pointing error messages:

```text
ERROR:  COPY FROM/TO file path not supported in V1; use STDIN/STDOUT (SP-PG-COPY-FILE)
ERROR:  COPY FROM/TO PROGRAM not supported (permanent security restriction)
```

V2 follow-ups (each its own SP-arc):

- ~~`SP-PG-COPY-BIN-NUMERIC`~~ — **CLOSED at V1 (2026-06-02)** — binary
  NUMERIC routes through the SP-PG-EXTQ-BIN-NUMERIC codec.
- `SP-PG-COPY-BIN-OID` — the optional OID-column flag bit (legacy PG
  ≤11 `WITH OIDS` tables).
- `SP-PG-COPY-BIN-EXTRA` — binary UUID / JSONB / ARRAY encoding.
- `SP-PG-COPY-BIN-DIRECT` — bypass the per-value binary→text round
  trip with typed parameter binding (5-10× throughput win for binary-
  heavy workloads).
- `SP-PG-COPY-CSV-FORCEQUOTE` — `FORCE_QUOTE (cols)` / `FORCE_NOT_NULL` /
  `FORCE_NULL` column-scoped CSV modifiers.
- `SP-PG-COPY-CSV-ENCODING` — non-UTF-8 CSV input/output encodings.
- `SP-PG-COPY-CSV-HEADER-MATCH` — PG-15+ `HEADER MATCH` (validate input
  header against table schema).
- `SP-PG-COPY-BULKAPPLY-WHOLECOPY` — whole-COPY atomicity (one
  Op::Txn covers every row) for full PG-compatible
  all-or-nothing semantics. Gated on an engine-side streaming-Txn
  shape (`Op::TxnBegin / TxnAppend / TxnCommit`) landing first;
  otherwise a 100M-row COPY would buffer 100M rows in RSS.
- `SP-PG-COPY-BULKAPPLY-NULLBATCH` — restore the BULKAPPLY win for
  batches containing NULL fields (today they fall back to per-row
  dispatch).
- `SP-PG-COPY-FILE` — `COPY ... FROM '/path'` (operator-opt-in
  only, security).
- `SP-PG-COPY-PROGRAM` — `COPY ... FROM PROGRAM '...'` (permanent
  hard pass).

## 10. HTTP gateway

Opt-in HTTP/1.1 surface (plus a WebSocket upgrade — see §10.5 below) for
operators, browsers, and tools that prefer HTTP/JSON over the binary wire
protocol. Built with
`cargo build --release -p kesseldb-server --features http-gateway` (add
`,tls` for HTTPS). The binary wire protocol is byte-untouched and remains
the default + fast path; the gateway runs on a sibling TCP listener.

### Configuration

```rust
let cfg = kesseldb_server::ServerConfig {
    http_addr: Some("127.0.0.1:6789".parse().unwrap()),
    http_tls_addr: Some("127.0.0.1:6790".parse().unwrap()), // requires `tls`
    tls: Some((cert_pem.into(), key_pem.into())),           // requires `tls`
    token: Some(b"my-token".to_vec()),                      // optional Bearer
    ..Default::default()
};
```

### Routes

| Method | Path | Body | Response |
|---|---|---|---|
| POST | `/v1/sql` | `text/plain` SQL | JSON `OpResult` |
| POST | `/v1/op` | `application/x-kessel-op` binary `Op::encode()` | JSON `OpResult` |
| GET | `/v1/health` | — | JSON liveness |
| GET | `/v1/metrics` | — | Prometheus text v0.0.4 |

### Auth

In token mode (`ServerConfig.token == Some(...)`), every request must carry
`Authorization: Bearer <token>` (constant-time compared, RFC 6750 §2.1
case-insensitive scheme). In open mode the header is ignored. Mismatched
or missing in token mode → HTTP `401` with `{"status":"unauthorized"}`.

### Exactly-once (optional)

Add the headers `X-Kessel-Client-Id: <32-char lowercase hex u128>` and
`X-Kessel-Req-Seq: <decimal u64>` together to bind the request to the
engine's per-client dedup map — retrying the same `(client_id, req_seq)`
returns the cached `OpResult`. Both-or-neither (one alone → `400`).
Duplicate `Authorization` / `X-Kessel-Client-Id` / `X-Kessel-Req-Seq`
headers are rejected at parse-time per the exactly-once contract.

### curl examples

```bash
# Health
curl -s http://127.0.0.1:6789/v1/health
# → {"status":"ok","primary":true,"view":0,"op_number":42,"role":"primary"}

# SQL
curl -s -X POST --data-binary 'CREATE TABLE t (v U64 NOT NULL)' \
  -H 'Content-Type: text/plain' \
  http://127.0.0.1:6789/v1/sql
# → {"status":"ok"}

# Metrics (for Prometheus scrape)
curl -s http://127.0.0.1:6789/v1/metrics
# → # HELP kesseldb_ops_total Number of Ops applied since process start.
#   # TYPE kesseldb_ops_total counter
#   kesseldb_ops_total{kind="applied"} 1234
#   ...

# Token mode
curl -s -H 'Authorization: Bearer my-token' \
  http://127.0.0.1:6789/v1/health
```

### Error mapping (excerpt — full table in spec §4.4)

| Body / situation | HTTP status |
|---|---|
| `OpResult::Ok` and most variants | 200 |
| `OpResult::Unauthorized` (engine denied) | 401 |
| `OpResult::Unavailable` (engine in-flight cap) | 429 |
| `OpResult::Unavailable` (cluster — no primary) | 503 |
| Body > 8 MiB (default cap; configurable via `http_max_body`) | 413 |
| Request line / headers > 64 KiB | 414 |
| Missing `Content-Length` on POST | 411 |
| Wrong `Content-Type` | 415 |
| `Expect: 100-continue` with body (V1 unsupported) | 417 |
| Conflicting `Content-Length` + `Transfer-Encoding` | 400 |
| Duplicate `Host` header | 400 |
| Differing `Content-Length` headers | 400 |
| Malformed chunked encoding | 400 |
| Unsupported `Transfer-Encoding` (V1 supports only `chunked`) | 400 |

Full mapping: `docs/superpowers/specs/2026-05-24-kesseldb-http-gateway-design.md` §4.4.

### Prometheus metrics (bounded cardinality)

- `kesseldb_ops_total{kind="applied"}` — counter
- `kesseldb_inflight` — gauge
- `kesseldb_last_op_number` — gauge
- `kesseldb_view_number` — gauge
- `kesseldb_is_primary` — gauge (0 or 1)
- `kesseldb_http_requests_total{path,status}` — counter (V1: empty; wiring in follow-up)

### WebSocket gateway (SP-WS)

The HTTP gateway includes a WebSocket arm at `GET /v1/ws` for long-lived
push/streaming clients that don't want a per-request HTTP round trip.
Shipped under SP-WS, lives in the same `kessel-http-gateway` crate, enabled
automatically by `--features http-gateway`. There is no separate
`ws-gateway` feature flag.

**Wire shape**

- RFC 6455 strict handshake. Required headers: `Upgrade: websocket`,
  `Connection: Upgrade`, `Sec-WebSocket-Version: 13`, `Sec-WebSocket-Key`,
  and `Sec-WebSocket-Protocol: kessel-op-v1`. Server replies with the
  matching `Sec-WebSocket-Accept` (RFC 6455 §4.2.2 SHA-1 / base64) and
  echoes `Sec-WebSocket-Protocol: kessel-op-v1`.
- Binary frames only. Each frame payload is one `Op::encode()` request;
  the server replies with one `OpResult::encode()` per request.
- Bounded send queue (16 messages). A slow client cannot grow the server
  send buffer unbounded — the session closes if the queue is full at
  enqueue time.
- 30 s ping/pong heartbeat. If the peer fails to respond to a `Ping`
  within the deadline the session closes with a `1011 internal error`.
- Idle timeout (default 30 s with no inbound message) → graceful
  close handshake (`Close 1000`).
- Subprotocol `kessel-op-v1` is required; clients that omit it are
  rejected with `HTTP 426 upgrade_required`. JSON-over-WS is a V2
  follow-up.

**Auth**

Same Bearer token as HTTP, checked **once at handshake** via the standard
`Authorization: Bearer <token>` header. After the upgrade succeeds the
session is trusted for its lifetime — there is no per-frame auth replay.
Rotating `ServerConfig.token` invalidates every future handshake (existing
WS sessions keep running until they close).

**Backpressure & limits**

WS sessions share the engine's `max_inflight` cap with HTTP and the binary
protocol. Per-session bounds: 16-message send queue, max frame payload =
`http_max_body` (default 8 MiB), strict RFC 6455 framing (rejects RSV1-3
bits, masked server-to-client frames, fragmented control frames).

**Browser example**

```js
const ws = new WebSocket('ws://127.0.0.1:8080/v1/ws', 'kessel-op-v1');
ws.binaryType = 'arraybuffer';
ws.onopen = () => {
  // Send a binary Op::encode() frame
  ws.send(encodeOp({ kind: 'Select', table: 't' }));
};
ws.onmessage = ev => {
  const result = decodeOpResult(new Uint8Array(ev.data));
  console.log(result);
};
```

### HTTP + WS spec + design

- HTTP gateway spec: `docs/superpowers/specs/2026-05-24-kesseldb-http-gateway-design.md`
- SP-WS WebSocket spec: `docs/superpowers/specs/2026-05-26-kesseldb-spws-websocket-design.md`
- Internal record: `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md`

## 11. Deploying to the cloud

KesselDB ships four supported deploy shapes — pick the one that
matches the runtime you already operate. The V1 cloud-deploy story
is **single-pod / single-VM** (matches the engine's single-writer
posture); replicated VSR clustering on k8s / Fly.io is tracked as
the named follow-up arc **SP-Cloud-Cluster**.

### 11.1 Docker (single-host)

The fastest path from zero to a running node:

```bash
docker run --rm \
  -p 6532:6532 -p 6533:6533 -p 5432:5432 \
  -v $PWD/kesseldb-data:/data \
  -e KESSELDB_TOKEN=$(openssl rand -hex 32) \
  ghcr.io/hassard0/kesseldb:latest
```

The image is multi-arch (`linux/amd64` + `linux/arm64`), runs as a
dedicated non-root `kessel:1100` UID, exposes all three wire surfaces,
and is ~77 MiB stripped. See [`Dockerfile`](../Dockerfile) for the
layout + env var matrix.

### 11.2 Kubernetes (Helm chart)

A Helm chart lives at [`deploy/helm/kesseldb/`](../deploy/helm/kesseldb).
Single-pod, ReadWriteOnce PVC, ClusterIP service.

```bash
# 1. Pre-create the token Secret (Helm chart references it by name).
kubectl create secret generic kesseldb-token \
  --from-literal=token=$(openssl rand -hex 32)

# 2. Install.
helm install kesseldb ./deploy/helm/kesseldb

# 3. Verify.
kubectl wait --for=condition=ready pod -l app=kesseldb --timeout=120s
kubectl exec deploy/kesseldb -- \
  kessel --addr 127.0.0.1:6532 --token "$KESSELDB_TOKEN" 'SELECT 1'
```

Overridable values (full list in
[`deploy/helm/kesseldb/values.yaml`](../deploy/helm/kesseldb/values.yaml)):
`image.tag`, `persistence.size`, `persistence.storageClassName`,
`resources.{requests,limits}`, `service.type`, `auth.secretName`
(set to `""` for open mode).

Verified end-to-end on vulcan (kind v0.24.0 + Kubernetes v1.31.0 +
helm v3.16.3) — transcript at
[`docs/superpowers/spclouddeploy-t3-kind-verify-2026-05-30.txt`](superpowers/spclouddeploy-t3-kind-verify-2026-05-30.txt).

### 11.3 Fly.io (`fly.toml`)

A ready-to-deploy `fly.toml` lives at
[`deploy/fly/fly.toml`](../deploy/fly/fly.toml).

```bash
cd deploy/fly
fly launch --no-deploy --copy-config --name <your-app>
fly secrets set KESSELDB_TOKEN=$(openssl rand -hex 32)
fly volumes create kesseldb_data --size 10 --region iad
fly deploy
```

Full walkthrough + connect-from-outside section + backup commands
in [`deploy/fly/README.md`](../deploy/fly/README.md).

### 11.4 Custom (any container runtime)

The Docker image is a plain OCI image — anywhere that runs OCI
containers (Nomad, ECS, Cloud Run, Azure Container Apps, your own
systemd-nspawn unit) works the same way:

```
image:      ghcr.io/hassard0/kesseldb:latest
entrypoint: /usr/local/bin/kesseldb
args:       ["0.0.0.0:6532", "/data"]
env:        KESSELDB_TOKEN  (required for auth; omit for open mode)
            KESSELDB_HTTP_ADDR=0.0.0.0:6533  (opt-in HTTP gateway)
            KESSELDB_PG_ADDR=0.0.0.0:5432    (opt-in PostgreSQL gateway)
volume:     /data  (mount a persistent volume here; the engine writes
                    its WAL + LSM + manifest under this dir)
ports:      6532/tcp  (binary protocol)
            6533/tcp  (HTTP/1.1 + WebSocket — opt-in)
            5432/tcp  (PostgreSQL Frontend/Backend v3.0 — opt-in)
```

Health check: TCP probe on `:6532` is sufficient (engine being up
implies the surface is accepting connections). If you have the HTTP
gateway enabled, prefer `GET /v1/health` (returns
`{"status":"ok","primary":true,...}` from the active engine).

### 11.5 Kubernetes cluster mode (SP-Cloud-Cluster)

Replicated VSR consensus (`kessel-vsr`, 3 or 5 replicas) under a
single Helm install — survives primary-pod kill + view-change +
elects a new primary without operator intervention.

Opt-in via `--set cluster.enabled=true`:

```bash
# 1. (open mode here — set auth.secretName for token auth)
helm install kesseldb-cluster ./deploy/helm/kesseldb \
  --set cluster.enabled=true \
  --set cluster.replicas=3 \
  --set auth.secretName=

# 2. Wait for every replica to be Ready.
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=kesseldb \
  --timeout=120s
# kesseldb-cluster-0 / -1 / -2 all Running.

# 3. The cluster runs as a StatefulSet with stable pod DNS:
#      kesseldb-cluster-<idx>.kesseldb-cluster-headless.<ns>.svc.cluster.local
#    Peer traffic uses port 6534 (the headless Service publishes
#    only that port); client traffic uses 6532 on the regular
#    ClusterIP Service.

# 4. Talk to the cluster via the failover-aware kessel CLI.
#    --addrs takes a comma-separated address list; the CLI rotates
#    past any node that answers UNAVAILABLE and lands the SQL on
#    the active primary.
ADDRS=kesseldb-cluster-0.kesseldb-cluster-headless.default.svc.cluster.local:6532,\
      kesseldb-cluster-1.kesseldb-cluster-headless.default.svc.cluster.local:6532,\
      kesseldb-cluster-2.kesseldb-cluster-headless.default.svc.cluster.local:6532

kubectl exec kesseldb-cluster-0 -- kessel --addrs "$ADDRS" \
  'CREATE TABLE acct (id BIGINT NOT NULL, bal BIGINT NOT NULL)'
kubectl exec kesseldb-cluster-0 -- kessel --addrs "$ADDRS" \
  'INSERT INTO acct ID 1 (id, bal) VALUES (1, 100)'
kubectl exec kesseldb-cluster-0 -- kessel --addrs "$ADDRS" \
  'SELECT SUM(bal) FROM acct'
# = 100  (16 bytes)
```

Primary-kill failover (kind-verified end-to-end on vulcan):

```bash
# Identify the current primary from logs.
for p in 0 1 2; do
  echo "--- kesseldb-cluster-$p ---"
  kubectl logs kesseldb-cluster-$p | grep -i "elected primary" | tail -1
done
# kesseldb-cluster-0: kesseldb cluster: replica 0 elected primary (view=0)

# Kill it.
kubectl delete pod kesseldb-cluster-0 --grace-period=1

# Within view-change timeout (~5s default) a surviving replica is elected.
sleep 8
kubectl logs kesseldb-cluster-1 | grep -i "elected primary" | tail -1
# kesseldb-cluster-1: kesseldb cluster: replica 1 elected primary (view=1)

# Issue another write via --addrs — the CLI rotates past the deleted
# primary's address and lands on the new primary.
kubectl exec kesseldb-cluster-1 -- kessel --addrs "$ADDRS" \
  'INSERT INTO acct ID 2 (id, bal) VALUES (2, 200)'
# OK

kubectl exec kesseldb-cluster-1 -- kessel --addrs "$ADDRS" \
  'SELECT SUM(bal) FROM acct'
# = 300  (16 bytes)   ← 100 + 200, the committed total
```

End-to-end kind transcript:
[`docs/superpowers/spcloudcluster-t3-t5-failover-2026-06-02.txt`](superpowers/spcloudcluster-t3-t5-failover-2026-06-02.txt).

Overridable values for cluster mode (full list in
[`deploy/helm/kesseldb/values.yaml`](../deploy/helm/kesseldb/values.yaml)):
`cluster.enabled`, `cluster.replicas` (3 or 5),
`cluster.peerAddressTemplate`, `cluster.viewChangeTimeout`,
`cluster.peerPort` (default 6534),
`cluster.podManagementPolicy` (default `Parallel`).

#### Prometheus monitoring (SP-Cloud-Cluster T7)

The chart can emit prometheus-operator CRDs
(`monitoring.coreos.com/v1` `ServiceMonitor` + `PrometheusRule`)
that point your Prometheus install at KesselDB's `/v1/metrics`
endpoint and ship three canned alerts on the cluster failure modes
that matter. The CRDs are OFF by default — the chart installs
cleanly in clusters without prometheus-operator — opt in with
`--set monitoring.prometheus.enabled=true`:

```bash
helm upgrade kesseldb-cluster ./deploy/helm/kesseldb \
  --set cluster.enabled=true \
  --set cluster.replicas=3 \
  --set monitoring.prometheus.enabled=true \
  --set monitoring.prometheus.additionalLabels.release=prometheus
# The `release=prometheus` label matches the default kube-prometheus-stack
# ServiceMonitor selector. Skip it if your operator selects on
# something else.

# Verify the CRDs landed:
kubectl get servicemonitor,prometheusrule -l app.kubernetes.io/instance=kesseldb-cluster
#   servicemonitor.monitoring.coreos.com/kesseldb-cluster   30s   <created>
#   prometheusrule.monitoring.coreos.com/kesseldb-cluster   30s   <created>
```

The ServiceMonitor scrapes the chart's client ClusterIP Service on
the named `http` port (6533) at `/v1/metrics`. The
`PrometheusRule` ships four alerts:

| Alert | Expression | For | Severity |
|---|---|---|---|
| `KesselDBClusterReplicaDown` | `up{} == 0` | 30s | critical |
| `KesselDBNoPrimary` | `sum(kesseldb_is_primary) == 0` | 60s | critical |
| `KesselDBViewChangeStorm` | `rate(kesseldb_view_changes_total[5m]) > 1` | 5m | warning |
| `KesselDBReplicaLag` | `kesseldb_replica_lag_opnum > 100` | 60s | warning |

Emitted metrics (from
[`crates/kessel-http-gateway/src/metrics_writer.rs`](../crates/kessel-http-gateway/src/metrics_writer.rs)
in single-node mode, and from
[`crates/kesseldb-server/src/cluster.rs`](../crates/kesseldb-server/src/cluster.rs)
`render_cluster_metrics_text` in cluster mode):

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `kesseldb_ops_total` | counter | `kind` | Ops applied since process start (single-node) |
| `kesseldb_inflight` | gauge | — | Ops currently in flight to the engine (single-node) |
| `kesseldb_last_op_number` | gauge | — | Highest applied op_number on this replica |
| `kesseldb_view_number` | gauge | — | Current VSR view number |
| `kesseldb_is_primary` | gauge | — | 1 if this replica is primary, 0 otherwise |
| `kesseldb_view_changes_total` | counter | — | Monotonic per-process count of view advances (SP-Cloud-Cluster-METRICS-EXPAND) |
| `kesseldb_replica_lag_opnum` | gauge | — | Op-number lag from primary (0 on primary; >=0 on backups, SP-Cloud-Cluster-METRICS-EXPAND) |
| `kesseldb_http_requests_total` | counter | `path`, `status` | HTTP gateway requests (single-node only) |

Plus the Prometheus-injected `up{}` per scrape target.
`view_changes_total` is per-process and resets on replica restart
— Prometheus's `rate()` handles counter resets explicitly via its
reset-detection algorithm, so the `ViewChangeStorm` alert remains
correct across restart windows. `replica_lag_opnum` accuracy is
bounded by Prepare-message cadence — a quiet primary leaves the
gauge stale at the last Prepare's op_number (accurate within one
12 ms tick under load; stale during quiet).

Knobs (all under `monitoring.prometheus.*`): `enabled`, `interval`
(default `30s`), `scrapeTimeout` (default `10s`),
`additionalLabels` (ServiceMonitor object labels — used by some
operator releases to select which ServiceMonitors to honour),
`rules.enabled` (default `true` — set false to scrape WITHOUT the
canned alerts), `rules.additionalLabels`.

Cluster-mode V1 limits (named, not vague):

- **HTTP/WS/PG-wire SQL/Op gateways NOT served in cluster mode V1.**
  The cluster path serves the binary client protocol on the client
  port, plus a dedicated metrics-only HTTP endpoint
  (`/v1/metrics` + `/v1/health`, no SQL or Op surfaces) bound to
  `KESSELDB_HTTP_ADDR` for Prometheus scrape and liveness checks.
  Full HTTP/WS/PG SQL/Op gateway surfaces in cluster mode remain a
  documented V2 follow-up.
- **Cross-node exactly-once on SQL writes is NOT guaranteed.** The
  CLI's `--addrs` retry uses `[0xFE] ++ sql` (the same wire as
  single-target SQL) which the cluster server's `apply_raw` path
  accepts on every node. For STRICT cross-node exactly-once on
  writes (replay a committed SQL after a primary kill returns the
  cached reply rather than re-executing), embed `ClusterClient`
  directly and call the `Op`-level session-framed `call(&Op)`
  surface — the same shape the cluster KATs use.
- **Fly.io multi-region cluster deploy NOT in V1.** Fly Machines
  don't have stable headless-Service-style DNS; per-region cluster
  deploy uses a different transport (`<machine-id>.vm.<app>.internal`).
  Named V2 follow-up: **SP-Cloud-Cluster-GEO** (multi-region) and
  the upstream **SP-Cloud-Cluster T6** Fly slice (single-region,
  6PN address mesh).
- **Online cluster reconfiguration (add/remove replicas without
  restart) NOT in V1.** Static N is the V1 contract. Named V2
  follow-up: **SP-Cloud-Cluster-RECONFIG** (requires upstream
  `kessel-vsr` membership-change support).
- **Coordinated cluster-wide backup NOT in V1.** Per-pod PVC
  snapshots are uncoordinated (every replica has every byte, so
  any one snapshot is recoverable; a quiesce-at-op-number
  cluster-wide snapshot is a separate design). Named V2 follow-up:
  **SP-Cloud-Cluster-BACKUP**.

### V1 cloud-deploy caveats (named, not vague)

- **~~Single-pod / single-VM by design~~ Cluster mode shipped (V1).**
  SP-Cloud-Cluster T3+T5 lands a 3 or 5 replica StatefulSet + headless
  Service + per-replica PVC + failover-aware `kessel --addrs ...` CLI.
  Multi-region (cross-zone WAN-tolerant view-change) is the named
  **SP-Cloud-Cluster-GEO** follow-up; sharding × clustering is
  **SP-Cloud-Cluster-SHARD**.
- **No public TLS in the v1 ghcr.io image.** The image is built with
  `--features pg-gateway,http-gateway` only; `--features tls` is
  opt-in (rustls). Pair with your platform's ingress (k8s Ingress +
  cert-manager, `fly certs`, etc.) or a fronting reverse proxy if
  you need HTTPS in front of `:6533` from the public internet.
- **GHCR package visibility.** The `ghcr.io/hassard0/kesseldb`
  package is currently private (default for new GHCR packages); to
  pull from a fresh cluster without `imagePullSecrets`, flip the
  package to Public in the GitHub UI (repo Packages -> kesseldb ->
  Settings -> Change visibility -> Public).

## 12. Backup & monitoring

Both are handled on the engine thread, so a snapshot is crash‑consistent and
metrics are exact. Using the embedded engine handle:

```rust
let engine = kesseldb_server::spawn_engine("./data")?;

// Hot, consistent snapshot — recovers to the exact live state digest:
engine.snapshot("./backup-2026-05-17")?;

// Live metrics:
let s = engine.stats();   // ServerStats { applied_ops, digest, uptime_secs }
```

`StateMachine::open("./backup-...")` recovers an identical state. The `digest`
field matches `Replica::digest`, so comparing stats across a cluster detects
replica divergence. In a cluster, `Node::probe()` returns
`(digest, op_number, commit)` for the same purpose.

Restore = point a fresh node at a snapshot directory and start it.

## 13. Wire protocol

Each message is length‑prefixed: `[u32 little‑endian length][payload]`.

| First byte | Meaning |
|---|---|
| (none / op bytes) | `Op::encode()` request → `OpResult::encode()` reply |
| `0xFE` | `0xFE ++ utf8 SQL` → compiled server‑side, `OpResult` reply |
| `0xFD` | session frame: `0xFD ++ client(u128 LE) ++ req(u64 LE) ++ Op::encode()` (exactly‑once) |
| `0xFC` | auth handshake: `0xFC ++ token` → `Ok` / `Unauthorized` |
| `0xFB` | admin: request `ServerStats` |
| `0xFA` | admin: `0xFA ++ dest_dir` → snapshot |

This is intentionally tiny — any language can speak it with a socket and the
length framing. `kessel-client` implements all of it.

## 14. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `OpResult::Unavailable` | The node is not the active primary, or it is shedding load. Use `ClusterClient` (auto‑rotates), or retry. |
| `OpResult::Unauthorized` | Missing/incorrect token. Use `connect_authed` / `with_token` with the server's `ServerConfig.token`. |
| `OpResult::Constraint(msg)` | A `NOT NULL` / `UNIQUE` / FK / `CHECK` rejected the write. This *is* a committed, deterministic result. |
| `OpResult::SchemaError(msg)` | Bad SQL, unknown table/column, or malformed frame. The message says which. |
| Client hangs on a fresh request to a backup | Connect to the primary, or use `ClusterClient` — backups answer cached results but relay new work to the primary. |
| Slow point reads as data grows | Expected only on the raw `Storage` primitive; the product (`StateMachine`) caps segment fan‑out (bounded compaction). |

For internals see [`docs/ARCHITECTURE.md`](ARCHITECTURE.md); for exactly what is
proven vs. roadmap and the performance log see [`docs/STATUS.md`](STATUS.md).

