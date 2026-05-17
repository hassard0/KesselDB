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
cargo test --workspace       # 143 tests, incl. seeded partition simulation
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
# (projections `SELECT c1,c2` and JOINs print opaque bytes — see USAGE §4)

# pipe a .sql file (lines starting with # or -- are comments; blanks ignored)
cat schema.sql | cargo run -q -p kessel-client --bin kessel

# interactive shell (TTY): a `kessel>` prompt; `quit` / `exit` / `\q` to leave
cargo run -q -p kessel-client --bin kessel

# remote / authenticated
kessel --addr 10.0.0.1:7878 --token s3cret "SELECT * FROM t ID 1"
```

`kessel [--addr HOST:PORT] [--token TOKEN] ["SQL"]` — default address
`127.0.0.1:7878`. With no SQL argument it reads statements from stdin (one
per line). The **exit code is reliable**, so an agent or script can branch
on success without parsing output. (After `cargo build --release` the
binary is `target/release/kessel`.)

## 3. The client library

`kessel-client` is a minimal blocking client. Add it as a path dependency, or
copy the wire protocol (§10) into any language.

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
CREATE INDEX        ON <t> (<col>)          -- equality index
CREATE UNIQUE INDEX ON <t> (<col>)          -- unique constraint + index
CREATE RANGE  INDEX ON <t> (<col>)          -- order‑preserving (range scans)
CREATE INDEX        ON <t> (<c1>, <c2>)     -- composite
DESCRIBE <t>                                -- returns the table definition
```

Column types: `U8 U16 U32 U64`, `I8 I16 I32 I64`, `BYTES`, `BOOL`.

### DML

```sql
INSERT INTO <t> ID <n> (<cols>) VALUES (<vals>)
UPDATE <t> ID <n> SET <col> = <val> [, ...]      -- server‑side read‑modify‑write
DELETE FROM <t> WHERE <col> = <val>
```

### Queries

```sql
SELECT * FROM <t> ID <n>                         -- O(1) primary‑key fetch
SELECT * FROM <t> [WHERE <col> = <v> [AND ...]]  -- index‑accelerated when possible
SELECT <c1>, <c2> FROM <t> [WHERE ...]           -- projection
SELECT COUNT(*) | SUM(c) | MIN(c) | MAX(c) | AVG(c) FROM <t> [WHERE ...]
       [GROUP BY <col>]
SELECT * FROM <t> [WHERE ...] ORDER BY <col> [DESC] [OFFSET n] [LIMIT n]
SELECT * FROM <a> JOIN <b> ON <a.x> = <b.y> [LIMIT n]   -- inner equi‑join
```

`WHERE` supports `AND`/`OR`/`NOT` and `=`, `>=`, `<=`. `SELECT *` returns
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

Atomic, all‑or‑nothing, replicated as a single operation. At the op level:

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
