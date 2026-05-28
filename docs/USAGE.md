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
- [9. Backup & monitoring](#9-backup--monitoring)
- [10. Wire protocol](#10-wire-protocol)
- [11. Troubleshooting](#11-troubleshooting)

---

## 1. Install & build

KesselDB is pure Rust with **no external dependencies** and no native build
steps.

```bash
git clone https://github.com/hassard0/KesselDB && cd KesselDB
cargo build --release        # builds every crate
cargo test --workspace       # full suite, incl. seeded partition simulation
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

> **Current capability (SP125‑SP140, OBJ‑2c‑2 zstd arc CLOSED):**
> `FORMAT PARQUET` reads real `pyarrow 24.0.0` Parquet files end‑to‑end
> across the **flat REQUIRED + OPTIONAL × UNCOMPRESSED + Snappy + GZIP + zstd
> × PLAIN + dictionary × V1 + V2 data pages × INT32 + INT64 + INT96 +
> FLBA + BYTE_ARRAY + DECIMAL (precision ≤ 38)** matrix.
> Vanilla `pq.write_table(df)` works zero‑flags for everything in that
> matrix; pyarrow zstd output decodes for all tested fixtures including
> a 2000‑row stress fixture exercising the FseCompressed mode for all
> three LL/OF/ML codes simultaneously. Still typed‑Unsupported:
> Brotli (recognized at meta-decode time per SP150 but decompression is a
> dedicated multi-week SP-arc — workaround: re-encode with zstd or lz4),
> 4+ deep nested
> groups (would be SP147), DECIMAL precision > 38, per‑page > 256 MiB
> (SP151 raised the historical 64 MiB cap to a 256 MiB default + added
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

## 9. Backup & monitoring

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

## 10. Wire protocol

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

## 11. Troubleshooting

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

## HTTP gateway

Opt-in HTTP/1.1 surface for operators, browsers, and tools that prefer
HTTP/JSON over the binary wire protocol. Built with
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

### Spec + design

- Spec: `docs/superpowers/specs/2026-05-24-kesseldb-http-gateway-design.md`
- Internal record: `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md`

## 9. PostgreSQL clients (psql, pgcli, JDBC, psycopg, pgx, …)

KesselDB speaks the PostgreSQL Frontend/Backend Protocol v3.0 — the same
wire libpq, `psql`, pgcli, JDBC, psycopg, `pgx`, `tokio-postgres`, sqlx-pg,
Diesel-pg, GORM-pg, Drizzle-pg, Prisma-pg, … all speak. Built behind the
opt-in `pg-gateway` feature flag so the default binary stays lean.

### Enable the PG listener

```bash
cargo build -p kesseldb-server --features pg-gateway
KESSEL_TOKEN=secret \
KESSELDB_PG_ADDR=127.0.0.1:5432 \
  ./target/debug/kesseldb-server --data /tmp/kessel.db --bind 127.0.0.1:7777
```

Two env vars matter:

- `KESSEL_TOKEN` — the operator's shared-secret Bearer token (the same one
  the HTTP gateway uses). The PG listener REQUIRES a token to be set;
  closed-mode-without-token rejects the connection with `28000`
  invalid_authorization_specification. The PG-wire SCRAM exchange uses
  this token as the SCRAM password input (spec §3.4 Bearer ↔ SCRAM
  bridge — one credential surface; rotating the token rotates BOTH
  HTTP-Bearer and PG-SCRAM atomically).
- `KESSELDB_PG_ADDR` — `host:port` to bind the PG listener on. Standard
  default is `:5432`; bind to `127.0.0.1:5432` for localhost-only, or
  `0.0.0.0:5432` to accept remote connections. Bind separately from the
  HTTP listener — PG and HTTP have independent connection caps so a
  misbehaving pgcli cannot starve HTTP clients.

When the listener is active, the binary's startup line widens to surface
the PG address:

```text
kesseldb: bound 127.0.0.1:7777, http=127.0.0.1:8080, pg=127.0.0.1:5432
```

### Connect with `psql`

```bash
PGPASSWORD=$KESSEL_TOKEN psql -h localhost -p 5432 -U test "SELECT 1"
```

The `-U test` username can be anything — V1 is multi-user-deferred (the
SCRAM exchange authenticates against the Bearer token regardless of the
PG `user` field). Interactive sessions work too:

```bash
PGPASSWORD=$KESSEL_TOKEN psql -h localhost -p 5432 -U test
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
props.setProperty("password", System.getenv("KESSEL_TOKEN"));
Connection conn = DriverManager.getConnection(url, props);
PreparedStatement stmt = conn.prepareStatement("SELECT * FROM users");
ResultSet rs = stmt.executeQuery();
while (rs.next()) {
    System.out.println(rs.getLong("id") + " " + rs.getString("name"));
}
```

### Connect from Python (psycopg2/psycopg3)

```python
import os
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="test",
    password=os.environ["KESSEL_TOKEN"],
    dbname="kessel",
)
cur = conn.cursor()
cur.execute("SELECT * FROM users")
for row in cur.fetchall():
    print(row)
```

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
$ PGPASSWORD=$KESSEL_TOKEN psql -h localhost -p 5432 -U test kessel
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
- **Simple Query only** → no Extended Query (Parse/Bind/Execute).
  ORMs that REQUIRE prepared statements may fall back to
  simple-query mode or fail. Most ORMs (Drizzle, Prisma, sqlx) work
  in simple-query mode out of the box. V2 SP-PG-EXTQ.
- **One statement per `Q`** → `psql \copy ...; SELECT ...` rejected
  with `42601` syntax_error. Send statements one at a time. V2.
- **Text format only** → every column rendered as PG text;
  binary-format preference (advertised in `Bind`) is ignored. V2.
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
  not set, or `KESSEL_TOKEN` not set (closed-mode rejects without a
  token).
- **`FATAL: invalid_authorization_specification`** → the Bearer token
  passed via `PGPASSWORD` doesn't match `KESSEL_TOKEN`. Note: this looks
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

### Spec + design

- SP-PG wire spec: `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`
- SP-PG progress (closed): `docs/superpowers/specs/2026-05-27-kesseldb-subproject-sppg-progress.md`
- SP-PG-CAT pg_catalog stubs spec: `docs/superpowers/specs/2026-05-27-kesseldb-sppgcat-pg-catalog-design.md`
- SP-PG-CAT progress (closed at T8): `docs/superpowers/specs/2026-05-27-kesseldb-subproject-sppgcat-progress.md`
