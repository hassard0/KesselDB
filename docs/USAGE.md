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
- [7f. FORMAT PARQUET for object-store sources (OBJ-2a)](#7f-format-parquet-for-object-store-sources-obj-2a)
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
point‑op batches (`Create`/`Update`/`Delete`); routing cross‑shard
*scatter‑gather reads*/SQL text is a separate later concern.

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
  See §7f for the precise scope (PLAIN/UNCOMPRESSED/flat REQUIRED/V1
  pages) and the supported-vs-deferred matrix.
- **Iceberg manifests, prefix/multi-object listing, and STS/SAS/IMDS
  credential providers** are explicit follow-ons (OBJ-3 through OBJ-5)
  and are **rejected at `CREATE`** with a clear error message.
- **Single object per source.** A `REFRESH` fetches exactly one
  object. Listing a prefix or walking a multi-object partition is OBJ-4.
- **Upsert only.** Same as §7c — rows deleted from the upstream object
  are not automatically pruned from the materialized table.
- **Snapshot since last `REFRESH`.** Queries read the last materialized
  snapshot; live object-store reads are never issued by a `SELECT`.

## 7f. FORMAT PARQUET for object-store sources (OBJ-2a)

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

### OBJ-2a scope: what is supported

| Parquet property | OBJ-2a |
|---|---|
| Encoding | `PLAIN` only |
| Compression codec | `UNCOMPRESSED` only |
| Column repetition | `REQUIRED` flat columns only (no `OPTIONAL`, no `REPEATED`, no nested groups) |
| Data page version | V1 (`DATA_PAGE`) only |
| Row groups | Multi-row-group files are fully supported |
| Column subset | Only the recipe-mapped columns are decoded; unmapped columns are skipped |
| Physical types | `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY` |

### What is NOT supported (rejected at REFRESH with a precise error)

The following trigger a typed `PqError` (surfaced as a `REFRESH`
failure; prior materialized data is left intact — all-or-nothing, same
as every other format):

- **Dictionary / RLE-data encoding** — rejected with
  `Unsupported("<enc> encoding: OBJ-2b")`.
- **Snappy compression** — rejected with
  `Unsupported("compression SNAPPY: OBJ-2b")`.
- **Gzip / Zstd compression** — rejected with
  `Unsupported("compression GZIP/ZSTD: OBJ-2c")`.
- **OPTIONAL or REPEATED columns** (definition/repetition levels) —
  rejected with `Unsupported("OPTIONAL/REPEATED/nested columns: OBJ-2b")`.
- **Nested group columns** — same `Unsupported` as OPTIONAL/REPEATED.
- **V2 data pages** (`DATA_PAGE_V2`) — rejected with
  `Unsupported("Parquet V2 data pages: OBJ-2b")`.
- **`INT96` / `FIXED_LEN_BYTE_ARRAY` / `DECIMAL`** physical types —
  rejected with `Unsupported("INT96/FIXED_LEN_BYTE_ARRAY: OBJ-2c")`.
- **A mapped column name absent from the Parquet schema** — rejected
  with `Bad("column \`<name>\` not found in Parquet schema")`.

None of the above are decoded silently or partially. Failure is
precise, typed, and fail-closed — the error message names the
OBJ-2b/2c follow-on that will address it.

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
iterated in order.

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
