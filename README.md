<div align="center">

# KesselDB

**A deterministic, replicated SQL database. Speaks PostgreSQL, HTTP, WebSocket, and a fast binary wire. Zero‑dependency Rust kernel.**

*"It's the database that made the Kessel Run in 12 parsecs."*

`~2,700+ tests green` (default; more with `--features pg-gateway` and the full gateway matrix) · `0 external dependencies in the kernel` · `Rust 1.95+` · single‑binary

**Highlights:**
- **14.71M ops/sec point reads at K=8 sharded (sub‑µs p50).** K independent per‑shard sub‑engines break the ~5M single‑shard `RwLock`‑reader ceiling. Sharded get‑by‑id at K=8 reaches **14.71M ops/sec (3.00× the 4.91M K=1 baseline)**; K=16 climbs to 16.24M. Scan‑side companions scale every scan workload at K=4 positively, with find‑by parity restored end‑to‑end without the `--pool-workers` flag. See [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) §13 + §14 + §14b–14d.
- **Real PostgreSQL ORM compatibility — psycopg2 ✓ SQLAlchemy ✓ asyncpg ✓ pgJDBC ✓.** Binary‑format Extended Query parameters and results (for the 10 supported PG scalar types) unblock asyncpg's default mode and pgJDBC's binary Bind path; JDBC simple‑mode `::int8` casts are stripped at the dispatcher; RowDescription is synthesized for the scalar SELECTs pgJDBC probes at connect; the parser handles pgJDBC's `VALUES (('42'), ('hello'))` paren‑wrapped substitution shape; and `WHERE name = $1` against `CHAR(N)` returns correct rows. Real pgJDBC 42.7.4 passes full CRUD in **both simple AND extended modes**. See [`docs/USAGE.md`](docs/USAGE.md) §9.
- **Beats Postgres on 6 of 8 cross‑DB workloads.** OLTP read‑only at N=16 is **6.02× faster than Postgres**; OLTP read‑write at N=16 is **2.30× faster than Postgres**. For TPC‑H Q6, the gap vs Postgres was closed from ~123× to **3.09×** (N=4, 544.59 q/s); the Q1 gap from ~18× to **2.16×**. The Q6 design floor (≥400 q/s) and stretch (≥500 q/s) are both met. See [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) §3 + §3f + §3g.
- **PG COPY FROM STDIN — text + CSV + binary, ~51,840 rows/sec (181.9× lift).** The text wire surface ships end‑to‑end; CSV adds RFC 4180 + the PG superset (HEADER + DELIMITER/QUOTE/ESCAPE/NULL); binary COPY (§55.2.7) covers the 10 supported PG scalar types — `pg_dump --format=custom` restore + JDBC `CopyManager` + `pg_bulkload` + `pgloader` + Stitch + Fivetran + Airbyte binary‑bulk‑loaders all unlock. Bulk‑apply lifted ingest throughput **181.9×** (~285 → 51,840 rows/sec). See [`docs/USAGE.md`](docs/USAGE.md) §9.
- **Cloud deploy story — Docker (ghcr.io/hassard0/kesseldb), Helm, Fly.io.** A multi‑arch ~77 MiB image, did‑you‑mean SQL errors, CLI error‑class hints, an embedded Rust example, plus a Helm chart and `fly.toml` (tested end‑to‑end in CI). See [`docs/USAGE.md`](docs/USAGE.md#11-deploying-to-the-cloud) §11.

</div>

---

## What is KesselDB?

KesselDB is a from‑scratch Rust database with the engineering rigor of
[TigerBeetle](https://github.com/tigerbeetle/tigerbeetle) — deterministic state
machine, LSM storage, write‑ahead log, Viewstamped Replication,
simulation‑driven testing — applied to a **general, schema‑flexible SQL
database** instead of a single hard‑coded domain.

You get runtime‑defined tables and online DDL, real SQL (joins, aggregates,
indexes, constraints, triggers, transactions), exactly‑once client semantics
across a replicated multi‑node cluster, and **four wire protocols on the same
engine**:

- **Binary** — the deterministic fast path (`Op::encode()` length‑prefixed frames)
- **HTTP/1.1 + JSON** — `/v1/sql`, `/v1/op`, `/v1/health`, `/v1/metrics` (Prometheus)
- **WebSocket** — long‑lived `/v1/ws` upgrade, framed `Op::encode()` payloads
- **PostgreSQL Frontend/Backend v3.0** — `psql`, `pgcli`, JDBC, psycopg, `pgx`,
  `tokio-postgres`, sqlx-pg + GUI tools (pgAdmin, DBeaver, DataGrip, Metabase,
  Tableau) connect straight in

The kernel is **pure Rust with zero external dependencies**. Every wire surface
is opt‑in via cargo features — `cargo build --release` links no gateway code at
all. Determinism is a feature, not an aspiration.

## Highlights

- **Real SQL** — `CREATE TABLE`, `ALTER TABLE … ADD COLUMN` (online, no lock), `DROP TABLE`, `INSERT`, `SELECT` (filters
  incl. `IN` / `BETWEEN` / `LIKE` / `IS [NOT] NULL` / `AND`/`OR`/`NOT`, `JOIN` (INNER/LEFT/RIGHT/FULL on a binary join, chained 3+ table INNER, table aliases `users u` / `users AS u`), `GROUP BY`, `HAVING`,
  `ORDER BY`, `LIMIT/OFFSET`), `UPDATE`, `DELETE`,
  `COUNT/SUM/MIN/MAX/AVG`, `CREATE [UNIQUE|RANGE] INDEX`, `DESCRIBE`, `EXPLAIN`.
- **Constraints & logic** — `NOT NULL`, `UNIQUE`, foreign keys ENFORCED from
  `CREATE TABLE … FOREIGN KEY` DDL (bad child INSERT → SQLSTATE 23503, NULL FK
  allowed) with `ON DELETE NO ACTION/RESTRICT/CASCADE/SET NULL/SET DEFAULT`,
  `CHECK`, and deterministic triggers
  (a gas‑bounded zero‑dep expression VM) — incl. zero‑dep, test‑vector‑verified
  **SHA‑256 / HMAC‑SHA256** usable in `CHECK`/triggers (pgcrypto‑subset).
- **Atomic transactions** — SQL `BEGIN`/`COMMIT`/`ROLLBACK` (and op‑level
  `Op::Txn`): all‑or‑nothing, replicated as a single operation. Multi‑row
  `INSERT … VALUES (…),(…)` is one atomic op in **one round‑trip** — a
  naive client pays N round‑trips and N consensus decisions; KesselDB pays
  one.
- **Replicated & highly available** — Viewstamped Replication over real TCP
  sockets; safety‑hardened (no committed‑op loss across view change) and
  liveness‑tested under an adversarial partition corpus.
- **Exactly‑once clients with automatic failover** — stable client sessions; a
  `ClusterClient` finds the primary, retries safely, and never double‑applies.
- **Crash‑safe** — WAL replay with torn‑tail handling; tested.
- **Operable** — hot consistent snapshots/backup, live metrics, shared‑secret
  auth, connection quotas and backpressure.
- **Fast where it counts** — prepared‑statement cache (≈26× faster SQL compile),
  per‑SSTable bloom filters, bounded‑segment compaction for data‑size‑independent
  point reads, range/band index narrowing, a columnar fast‑path that answers
  `MIN`/`MAX` from the index extreme without scanning, and an in‑memory read
  cache for hot keys — all on by default, each proven equivalent to a full
  scan by a randomized oracle.
- **Mechanically verified by TLA+ (S1)** — the Viewstamped Replication safety
  invariants are model‑checked by TLC across 528 million distinct states /
  depth 21 (zero counterexamples). Seven layered TLA+ modules cover the full
  Replication → MVCC backbone (Replication / MVCCStorage / MVCCTx / MVCCSi /
  MVCCSsi / MVCCGc / MVCCCutover). See `kesseldb-tla/`.
- **Serializable MVCC (S2)** — every SQL statement that touches a user-type row
  is, by construction, a deterministic MVCC transaction with snapshot-isolation
  + Cahill SSI (write‑skew impossible) + GC under a dynamic watermark protocol.
  Replicas reach byte‑identical state at every committed log position.
- **Jepsen-style linearizability under partition (S3)** — 5 hand-derived Jepsen
  tests against the in-process VSR + MVCC stack; multi-replica byte-identity
  digests post-partition + post-recovery.
- **Deterministic WASM UDFs (S4)** — zero‑dep WASM-MVP interpreter
  (`kessel-wasm`) for `CHECK`/trigger user functions: i32/i64/f32/f64 +
  memory + tables/call_indirect + canonical NaN, gas‑bounded, no host calls
  / no clocks — every replica runs byte‑identical UDF logic; UDF behavior is
  replayable from the log.
- **External sources & Parquet** — register and `REFRESH`
  JSON/NDJSON/CSV/Parquet from HTTP/HTTPS endpoints or directly from
  S3‑compatible and Azure Blob object storage. The pure‑Rust zero‑dep
  Parquet reader (`kessel-parquet`) supports **flat REQUIRED + OPTIONAL +
  `LIST<primitive>` + `MAP<K, V>` + `struct` (+ 3‑deep cross‑products)
  × UNCOMPRESSED + Snappy + GZIP + zstd
  + LZ4_RAW + Brotli (the full 6‑codec matrix) × PLAIN +
  dictionary × V1 + V2 data pages × INT64 + INT32 + INT96 (timestamps) +
  DECIMAL (INT32 / INT64 / FLBA, precision ≤ 38) + FLBA + BYTE_ARRAY**
  out of the box. Every nested Parquet shape pyarrow writes up to 3‑deep
  nesting decodes. See [Parquet capability matrix](#parquet-capability-matrix)
  below. (`--features external-sources`, default off; `--features
  external-sources-objstore` for S3/Azure + Parquet; deterministic kernel
  unaffected when off.)
- **Cross‑shard scatter scan** — `SELECT` / `SELECT … ORDER BY` /
  projection / row‑filter ops fan out across K independent VSR shard groups
  via a zero‑dep std‑thread scatter‑gather with bounded per‑shard channels.
  Unordered scan is shard‑id deterministic; sorted scan is a `BinaryHeap`
  k‑way merge of per‑shard already‑sorted streams. **K‑invariance** locked
  by an 85‑seed × 5‑K property sweep: with unique sort values, merged
  output is byte‑identical to the K=1 baseline for K ∈ {1, 2, 4, 8, 16}.
  Per‑shard MVCC snapshot per request; opt‑in best‑effort `partial_on_timeout`
  mode beside the safe hard‑fail default.
- **HTTP/1.1 gateway (opt‑in `--features http-gateway`)** — full Op surface
  + SQL + `/v1/health` + `/v1/metrics` (Prometheus text v0.0.4) on a
  sibling TCP listener (`ServerConfig.http_addr`; HTTPS on `http_tls_addr`
  with the `tls` feature). `Authorization: Bearer` constant‑time, optional
  `X-Kessel-Client-Id` + `X-Kessel-Req-Seq` exactly‑once headers. JSON
  responses via the existing `kessel_client::format_result_json` contract.
  Binary protocol byte‑untouched; zero external (non‑workspace) deps on the
  gateway crate. See `docs/USAGE.md` §HTTP gateway.
- **WebSocket gateway (shipped under the HTTP gateway crate)** —
  long‑lived `/v1/ws` upgrade carrying raw `Op::encode()` payloads under the
  `kessel-op-v1` subprotocol. RFC 6455 strict handshake, binary frames only,
  bounded send queue (16 messages), 30 s ping/pong heartbeat. Same Bearer
  auth as HTTP, checked once at handshake. Useful for browser‑direct
  push/streaming clients that don't want a per‑request HTTP round trip.
  Enabled automatically with `--features http-gateway`. See `docs/USAGE.md`
  §HTTP gateway → WebSocket.
- **PostgreSQL wire protocol (opt‑in `--features pg-gateway`)** —
  Frontend/Backend Protocol v3.0 **Simple Query AND Extended Query** paths
  with SCRAM‑SHA‑256 authentication on a sibling TCP listener
  (`ServerConfig.pg_addr`, default port 5432). Operator's Bearer token IS
  the SCRAM password input — one credential surface; rotating the token
  rotates HTTP, WS and PG together. SELECT / INSERT / UPDATE / DELETE /
  CREATE TABLE work end‑to‑end against `psql`, `pgcli`, JDBC, psycopg,
  `pgx`, `tokio-postgres`, sqlx-pg, Diesel‑pg, GORM‑pg, Drizzle‑pg,
  Prisma‑pg, and every libpq‑derived client. The **full Extended Query
  message set** (Parse / Bind / Describe / Execute / Sync / Close / Flush)
  ships — psycopg2's `cursor.execute("…WHERE id = %s", (42,))`
  round‑trips end‑to‑end, and ORMs that REQUIRE prepared statements
  (SQLAlchemy, Drizzle, Prisma, JDBC default) connect. **`pg_catalog` +
  `information_schema` stubs** let pgAdmin 4,
  DBeaver, DataGrip, Metabase, Tableau, Looker, dbt and pgJDBC
  `getTables` all connect + browse out of the box. Cap‑overflow (`53300`) and idle‑timeout (`57014`) emit
  wire‑level `ErrorResponse` with canonical PG message text before closing.
  Independent connection cap from HTTP (default `pg_max_conns=256` vs
  HTTP's 1024) — a misbehaving pgcli cannot starve HTTP clients. Binary
  protocol byte‑untouched; zero external (non‑workspace) deps on the
  gateway crate. See `docs/USAGE.md` §9 PostgreSQL clients.
- **Deterministic & verifiable** — the whole engine is a seedable state
  machine; the test suite (~2,700+ tests by default, more with the
  PostgreSQL and HTTP gateway features) includes seeded partition/fault
  simulation, multi‑replica Jepsen, hand‑derived KATs against published
  spec text for every codec, the 85‑seed cross‑shard K‑invariance sweep, a
  synthetic‑peer suite verifying each GUI tool's verbatim
  introspection SQL, and adversarial pentests for every public input
  surface.

## Quick start

### Download a prebuilt Linux binary

```bash
# x86_64 Linux (glibc):
VER=v2.0.0       # see https://github.com/hassard0/KesselDB/releases for the latest
curl -L https://github.com/hassard0/KesselDB/releases/download/$VER/kesseldb-$VER-x86_64-unknown-linux-gnu \
  -o kesseldb && chmod +x kesseldb
curl -L https://github.com/hassard0/KesselDB/releases/download/$VER/kessel-$VER-x86_64-unknown-linux-gnu \
  -o kessel    && chmod +x kessel

# Or grab the bundle (server + CLI + README + USAGE + LICENSE):
curl -L https://github.com/hassard0/KesselDB/releases/download/$VER/kesseldb-$VER-x86_64-unknown-linux-gnu.tar.gz \
  | tar xz
```

The release workflow builds these as part of `cargo build --release
--features pg-gateway,http-gateway` so the binaries you download include the
PostgreSQL and HTTP gateways out of the box; the binary protocol is the
default + fast path either way.

### Or run the official Docker image

A pre-published multi-arch (`linux/amd64` + `linux/arm64`) image is
pushed to GitHub Container Registry on every `v*` release. The image
bundles the same `--features pg-gateway,http-gateway` server you would
build from source, runs as a non-root UID, and exposes all three wire
surfaces (binary 6532, HTTP+WS 6533, PostgreSQL 5432).

```bash
docker run --rm \
  -p 6532:6532 -p 6533:6533 -p 5432:5432 \
  -v $PWD/kesseldb-data:/data \
  -e KESSELDB_TOKEN=changeme \
  ghcr.io/hassard0/kesseldb:latest
```

Stripped image size: ~77 MiB. See [`Dockerfile`](Dockerfile) and
[`docs/USAGE.md`](docs/USAGE.md#option-b--run-from-the-official-docker-image)
for the layout + the matrix of supported env vars.

### Or build from source

```bash
git clone https://github.com/hassard0/KesselDB && cd KesselDB
cargo build --release                                # default — binary protocol only
cargo build --release --features pg-gateway,http-gateway   # all wire surfaces
cargo test  --workspace --release                    # workspace gate: ~2,700+ default tests
```

### Start a node

```bash
# kesseldb [LISTEN_ADDR] [DATA_DIR]
./kesseldb 127.0.0.1:7878 ./data

# Or enable the HTTP + PG listeners alongside the binary protocol:
KESSELDB_TOKEN=mysecret \
KESSELDB_HTTP_ADDR=127.0.0.1:8080 \
KESSELDB_PG_ADDR=127.0.0.1:5432 \
  ./kesseldb 127.0.0.1:7878 ./data
# => KesselDB listening on 127.0.0.1:7878, data dir ./data, http=127.0.0.1:8080, pg=127.0.0.1:5432
```

### Connect

```bash
# Binary protocol via the kessel CLI (one-shot, pipe, interactive):
./kessel "CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)"
./kessel "INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)"
./kessel "SELECT SUM(bal) FROM acct WHERE owner = 100"     # => = 50
./kessel "SELECT * FROM acct"                              # aligned table
./kessel --json "SELECT * FROM acct"                       # {"status":"ok","rows":[…]}
echo "SELECT * FROM acct ID 1" | ./kessel                  # pipe a .sql file
./kessel                                                   # interactive shell (\? for commands)

# HTTP/1.1 + JSON:
curl -s -X POST --data-binary 'SELECT * FROM acct' \
  -H 'Content-Type: text/plain' \
  -H 'Authorization: Bearer mysecret' \
  http://127.0.0.1:8080/v1/sql

# PostgreSQL wire — any libpq-derived client (Simple Query + Extended Query both supported):
PGPASSWORD=mysecret psql -h 127.0.0.1 -p 5432 -U test "SELECT SUM(bal) FROM acct"
PGPASSWORD=mysecret pgcli -h 127.0.0.1 -p 5432 -u test
```

```python
# psycopg2 — parameterized queries through the Extended Query protocol:
import os, psycopg2
conn = psycopg2.connect(host="127.0.0.1", port=5432, user="test",
                        password=os.environ["KESSELDB_TOKEN"], dbname="kessel")
cur  = conn.cursor()
cur.execute("SELECT * FROM acct WHERE owner = %s", (100,))
print(cur.fetchall())     # → real rows, real round‑trip
```

The `kessel` CLI is one-shot, pipe, and interactive, with reliable exit codes
and a `--json` mode — ideal for scripts, ops, and agents. In the shell, `\?`
lists commands, `\d <table>` describes a table, `\timing` toggles query timing.

### Or from Rust

```rust
use kessel_client::Client;

let mut db = Client::connect("127.0.0.1:7878")?;

db.sql("CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)")?;
db.sql("INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)")?;
db.sql("INSERT INTO acct ID 2 (owner, bal) VALUES (100, 999)")?;

let total = db.sql("SELECT SUM(bal) FROM acct WHERE owner = 100")?; // => 1049
db.sql("UPDATE acct ID 1 SET bal = 500")?;
let row   = db.sql("SELECT * FROM acct ID 2")?;
```

→ Full instructions, SQL reference, cluster setup, auth and operations are in
**[`docs/USAGE.md`](docs/USAGE.md)**.

## Deploy

Single-pod (default) PLUS replicated VSR cluster mode (`--set cluster.enabled=true`).
Multi-region (cross-zone WAN-tolerant view-change) and sharding × clustering
are roadmap follow-ups.

| Shape | One-liner | Reference |
|---|---|---|
| **Docker** (any host) | `docker run -p 6532:6532 -p 6533:6533 -p 5432:5432 -e KESSELDB_TOKEN=admin -v /tmp/kdb-data:/data ghcr.io/hassard0/kesseldb:latest` | [`Dockerfile`](Dockerfile) |
| **Kubernetes (single-pod)** | `helm install kesseldb ./deploy/helm/kesseldb` (pre-create the `kesseldb-token` Secret first) | [`deploy/helm/kesseldb/`](deploy/helm/kesseldb) |
| **Kubernetes cluster (3 or 5 VSR replicas)** | `helm install kesseldb-cluster ./deploy/helm/kesseldb --set cluster.enabled=true --set cluster.replicas=3` (failover-aware `kessel --addrs ...` CLI; opt-in `--set monitoring.prometheus.enabled=true` ships ServiceMonitor + PrometheusRule) | [`docs/USAGE.md`](docs/USAGE.md) §11.5 |
| **Fly.io** | `fly launch --copy-config --no-deploy && fly secrets set KESSELDB_TOKEN=… && fly volumes create kesseldb_data --size 10 && fly deploy` | [`deploy/fly/`](deploy/fly) |
| **Custom** (Nomad / ECS / Cloud Run / systemd-nspawn / …) | Same OCI image; mount `/data`, set `KESSELDB_TOKEN`, expose 6532/6533/5432 | [`docs/USAGE.md`](docs/USAGE.md) §11.4 |

Full walkthrough + caveats (TLS, single‑attach volume, GHCR
visibility) in [`docs/USAGE.md`](docs/USAGE.md) §11; cluster mode +
primary-kill failover + Prometheus monitoring in §11.5. The Helm chart
(single-pod and cluster mode, including primary-kill failover) is tested
end‑to‑end in CI.

## PostgreSQL client compatibility

KesselDB speaks the PostgreSQL Frontend/Backend Protocol v3.0 **Simple
Query AND Extended Query** paths with SCRAM‑SHA‑256 auth. With the
`pg_catalog` / `information_schema` stubs and the Extended Query message
set, the following PG ecosystem tools connect and browse out of the box
(verified by synthetic‑peer KATs driving each tool's verbatim connect /
introspection SQL, and — for the real drivers below — by end‑to‑end driver
round‑trips):

| Tool | Connect | Run queries | Notes |
|---|---|---|---|
| `psql` | ✓ | ✓ Simple Query + EXTQ + COPY | `\dt`, `\d <t>`, `\dn`, `\di`; `\dt+` row count = `-1` (V1 doesn't track) |
| `pgcli` | ✓ | ✓ Simple Query + EXTQ | tab‑completion populated from `pg_class` enumeration |
| pgAdmin 4 | ✓ | ✓ browse tables/columns/indexes/constraints | Functions / triggers / extensions panels empty (V2) |
| DBeaver | ✓ | ✓ navigator tree | tables + columns + indexes + UNIQUE constraints |
| DataGrip / IntelliJ | ✓ | ✓ tables/columns | Functions panel empty (V1 returns empty `routines`) |
| Metabase | ✓ | ✓ schema discovery via `information_schema.{tables,columns,schemata}` | |
| Tableau / Looker / Hex / Superset | ✓ | ✓ ODBC wizards complete | schema discoverable |
| **JDBC `org.postgresql:postgresql` 42.7.4** | ✓ | ✓ **PASS — full CRUD in both simple AND extended modes** | Real pgJDBC verified end‑to‑end: CREATE TABLE, `PreparedStatement` INSERT (`setLong`+`setString`), SELECT \*, `PreparedStatement` SELECT WHERE id=?, `SELECT version()`. Extended mode uses binary Bind + binary result columns. Simple mode (`preferQueryMode=simple`) goes through the `::cast` stripper + paren‑VALUES parser + scalar‑SELECT Describe synthesizer |
| **psycopg2 2.9.12** | ✓ | ✓ **19/19 ORM smoke steps PASS** | SCRAM auth + `cur.execute("…WHERE id = %s", (42,))` round‑trips through Extended Query |
| **SQLAlchemy 2.0** | ✓ | ✓ **PASS — full session round-trip with DEFAULT settings** | `sa.create_engine(...)` + `engine.connect()` + parameterized queries + pool checkout/checkin all green |
| **psycopg3 3.3.4** | ✓ | ✓ **PASS — DEFAULT cursor works (no ClientCursor needed)** | binary‑Bind path closed; no ClientCursor workaround needed |
| **asyncpg 0.31.0** | ✓ | ✓ **PASS — fetch() round-trip works end-to-end** | binary RowDescription/DataRow path closed; `WHERE name = $1` against CHAR(N) returns correct rows |
| `pgx` (Go) / `tokio-postgres` (Rust) / sqlx‑pg (Rust) | n/a | n/a | not yet smoke-tested; same binary‑Bind + binary‑RESULTS unlock as asyncpg / JDBC |
| Drizzle / Prisma (Node) | n/a | n/a | not yet smoke-tested |
| GORM (Go) / Diesel (Rust) | n/a | n/a | not yet smoke-tested; same binary‑format unlock as asyncpg / JDBC |

**This release ships** Extended Query
(`Parse/Bind/Describe/Execute/Sync/Close/Flush`) with binary‑format
parameters AND binary‑format results
for the 10 supported PG scalar types (INT2/INT4/INT8/FLOAT4/FLOAT8/BOOL/
TEXT/VARCHAR/BYTEA/TIMESTAMPTZ); JDBC simple‑mode `::cast` rewrite + paren‑VALUES
parse + scalar‑SELECT Describe synthesizer; CHAR(N) padding‑aware comparison;
COPY FROM/TO STDIN in text, CSV, and binary formats with 181.9× ingest lift.

**Follow‑ups on the roadmap:**
binary NUMERIC, JSONB/UUID/ARRAY binary,
pgJDBC simple‑mode nested casts, Describe for multi‑projection
SELECTs, Go pgx / Node Drizzle+Prisma smoke harnesses,
libpq pipeline mode, `RETURNING`,
`CancelRequest` action, GUC plumbing, `pg_proc` real function listing,
`pg_stat_*` runtime stats, TLS via SSLRequest, MD5 auth fallback, SCRAM
channel binding, per‑user privileges. Full list in
[`docs/USAGE.md`](docs/USAGE.md) §9 → Limitations.

## Running a cluster

A replicated cluster is composed with the `kesseldb-server` library
(`spawn_node` + `serve_clients`); clients use `ClusterClient`, which discovers
the primary and fails over automatically with exactly‑once semantics:

```rust
use kessel_client::ClusterClient;

let mut db = ClusterClient::new(vec![
    "10.0.0.1:7878".into(),
    "10.0.0.2:7878".into(),
    "10.0.0.3:7878".into(),
]);
db.call(&op)?; // routed to the primary; retried safely on failover
```

See [`docs/USAGE.md`](docs/USAGE.md) → *Running a cluster*.

## Performance

Single deterministic writer, default zero‑dependency build. Measured on
a 16‑core x86‑64 Linux reference server (numbers move with hardware; the
*relationships* hold across platforms — see
**[`docs/PERFORMANCE.md`](docs/PERFORMANCE.md)** for the scaling model
and cloud projections).

| Path | Result |
|---|---|
| State‑machine create (in‑mem, 128 B) | ~215 K ops/s @ p50 ~2 µs |
| **Sharded point‑read, K=8, in‑process, N=16 cores** | **~14.71 M ops/s, p50 sub‑µs** — 3.00× the K=1 4.91M baseline; K=16 → 16.24M ops/s |
| **Parallel point‑read (single shard), in‑process, N=16 cores** | **~4.91 M ops/s, p50 < 1 µs, p99 ~7 µs** — read‑pool bypass + storage `Arc<[u8]>` (zero‑memcpy reads); the ~5M `RwLock`‑reader ceiling is broken by sharding |
| **YCSB‑C uniform‑random reads, N=16** | **~5.27 M ops/s — ≈ 63.75× Postgres** (cross‑DB headline; see [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) §3) |
| **sysbench OLTP read‑only, N=16** | **~30,646 tx/s — 6.02× Postgres** |
| **sysbench OLTP read‑write, N=16** | **~8,852 tx/s — 2.30× Postgres** |
| **sysbench OLTP write‑only, N=8** | **~50,687 tx/s — 4.91× Postgres** |
| **TPC‑H Q6 (SUM with WHERE), N=4** | **~544.59 q/s** (gap vs Postgres 3.09×; Q6 design floor ≥400 q/s + stretch ≥500 q/s both met) |
| **TPC‑H Q1 (multi‑aggregate GROUP BY), N=4** | **~86.17 q/s** (gap vs Postgres 2.16×) |
| **PG COPY FROM STDIN, 100K rows, single conn** | **~51,840 rows/sec** — 181.9× lift over the bulk‑apply baseline of 285 rows/sec; within ~11× of Postgres 16 (~578K rows/sec) |
| Durable create, group commit (~1 K batch) | ~87 K ops/s (local NVMe) |
| Concurrent durable, 8 clients | **~1,870 ops/s** — group commit + `TCP_NODELAY` (conservative; rises with concurrency) |
| Pipelined batch, 1 connection | **~52,700 ops/s** — N statements per round‑trip |
| SQL compile, prepared‑statement cache | **~574 K → ~15 M stmt/s** (cold → cached) |
| Equality / composite `WHERE` | index‑narrowed, not full scan (equivalence‑oracle verified) |
| Range/band `WHERE v BETWEEN a AND b` (range index) | **~35 ms → ~0.31 ms (~112×)**, oracle‑verified |
| `MIN`/`MAX` on a range‑indexed column | **~23 ms → ~5 µs (~4,600×)** — columnar fast‑path, answered from the index extreme (no scan), oracle‑verified |
| Point read | ≤8 bloom‑probed segments (~28 ns/segment), bounded by design |
| 3‑node replicated | ~161 K ops/s |

**Cross‑DB benchmark suite.** Full tables —
including the *losses* where KesselDB does NOT win — are in
[`docs/BENCHMARKS.md`](docs/BENCHMARKS.md). Honest summary:

| Workload | Winner | KesselDB place | One‑line cause |
|---|---|---|---|
| YCSB‑C (100% reads, uniform, ~1 KiB rows) | **KesselDB** | 1st at every N | in‑process + parallel read‑pool |
| YCSB‑B (95% reads / 5% updates) | **KesselDB** | 1st at every N | same — read‑mostly workload |
| YCSB‑A (50/50) | **KesselDB at N=1 + N=16** | 1st N=1, ≈ tied N=8 vs Postgres, 1st N=16 | write‑side apply lock pays cost at N=8 then amortizes |
| sysbench OLTP write‑only | **KesselDB** | **1st at every N (4.91× Postgres at N=8)** | apply‑path is fast at the inner‑op level |
| sysbench OLTP read‑only | **KesselDB at N=8 / N=16** | **1st at every N≥8 (6.02× Postgres at N=16)** | all‑RO `Op::Txn{ops}` routes through the read pool — see [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) §3c |
| sysbench OLTP read‑write | **KesselDB at N=8 / N=16** | **1st at every N≥8 (2.30× Postgres at N=16)** | driver‑level split‑phase dispatch — (R*, W*)‑shape Txns split at the read/write boundary; read prefix routes via the read‑pool bypass (parallel), write suffix via `sm.write().apply` (serial) — see [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) §3e |
| TPC‑H Q1 (multi‑aggregate GROUP BY) | Postgres at every N | **2nd at every N** | N=4 86.17 q/s; gap vs Postgres **2.16×**; a closure‑built‑once‑per‑query WHERE evaluator cut per‑row VM dispatch — see [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) §3f |
| TPC‑H Q6 (SUM with WHERE) | Postgres at every N | **2nd at N=4** | N=4 544.59 q/s; gap vs Postgres **3.09×**; Q6 design floor ≥400 q/s + stretch ≥500 q/s both met — see [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) §3g |

**How the headline wins were built:**

- **Transaction‑bracket read paths.** Static all‑RO `Op::Txn`
  classification recurses into the inner‑op vector and routes through
  the read‑pool bypass when every inner op is read‑only — lifting
  OLTP read‑only at N=16 42.6× (680 → 28,977 tx/s) to 5.7× Postgres,
  and now 6.02×.
- **Mixed‑transaction split‑phase dispatch.** (R*, W*)‑shape mixed
  Txns are split at the read/write boundary — the read prefix runs in
  parallel via the read‑pool bypass, the write suffix serially via
  `sm.write().apply`; read‑after‑write Txns fall through to unified
  apply (byte‑equivalent preserved via apply's overlay). This lifts
  OLTP read‑write at N=16 14.4× (712 → 10,273 tx/s) to 2.30× Postgres.
- **Sharding.** K independent per‑shard sub‑engines (each its own
  `Arc<RwLock<StateMachine>>` + apply thread + WAL + SSTables, rooted
  at `data_dir/shard‑<i>/`) route every Op via
  `hash(make_key(type_id, oid)) % K`. Opt‑in via
  `ServerConfig.shard_count = Some(K)` (default `None` is byte‑identical
  to the unsharded engine). Get‑by‑id scales K=1 ~4.9M → K=4 ~11.4M
  (2.3×) → **K=8 ~14.7M (3.0×, breaks the 10M ceiling)** → K=16 ~16.2M
  (3.3×); p50 latency drops 3 µs → <1 µs. Scan‑side companions prove
  K‑invariance for scatter‑gather scans, recover find‑by perf at K≥2
  (105×), make every scan workload at K=4 scale positively, and deliver
  sharded find‑by parity without the `--pool-workers` flag. See
  [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) §14 + §14b + §14c + §14d.
- **Analytical aggregates (TPC‑H Q1/Q6).** Range‑pred narrowing on
  aggregate scans, single‑scan multi‑aggregate folding, a parallel
  hash aggregate (per‑worker `HashMap` partials + sorted‑`BTreeMap`
  merge) for large row counts, batched streaming overlap so workers
  start folding on row 1, and a WHERE filter compiled once per query
  into a closure that captures pre‑resolved field offsets + comparison
  ops + the AND/OR short‑circuit tree. Together these closed the gap vs
  Postgres from ~18× to **2.16×** (Q1) and from ~123× to **3.09×** (Q6).
  Q1 and Q6 remain the two workloads where Postgres still wins; closing
  the residual gap (now in the decode→update fold work, not WHERE
  evaluation) is a JIT‑aggregate follow‑up. See
  [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) §3f + §3g.

**Headline numbers worth quoting** (see `docs/BENCHMARKS.md` §1):
- **Sharded point‑read get‑by‑id, K=8, N=16 workers**: KesselDB **14.71M ops/sec** (3.00× the 4.91M K=1 baseline; sub‑µs p50; K=16 → 16.24M)
- **YCSB‑C reads, N=16**: KesselDB 5.27M ops/s — **63.75× Postgres**
- **YCSB‑B mixed (95/5), N=16**: KesselDB 573.6K ops/s — **7.26× Postgres**
- **sysbench OLTP write‑only, N=8**: KesselDB 50.7K tx/s — **4.91× Postgres**
- **sysbench OLTP read‑only, N=16**: KesselDB 30.6K tx/s — **6.02× Postgres**
- **sysbench OLTP read‑write, N=16**: KesselDB 8.85K tx/s — **2.30× Postgres**
- **TPC‑H Q6, N=4**: KesselDB 544.59 q/s — gap vs Postgres 3.09× (design floor ≥400 + stretch ≥500 both met)
- **KesselDB wins 6 of 8 cross‑DB workloads vs Postgres** (only TPC‑H Q1+Q6 remain losses)
- **PG COPY FROM STDIN, 100K rows, single conn**: KesselDB 51,840 rows/sec — 181.9× lift over the bulk‑apply baseline

Every figure is reproducible from the test suite / `kessel-bench`, and
each query accelerator is guarded by a randomized equivalence oracle
(the accelerated result is proven identical to a brute‑force scan). Full
methodology, the single‑core/fsync/RTT scaling model, and
order‑of‑magnitude projections for common cloud instance + storage
configurations are in **[`docs/PERFORMANCE.md`](docs/PERFORMANCE.md)**.

## Parquet capability matrix

The `kessel-parquet` crate is a from‑scratch, zero‑external‑dependency
Parquet reader. Its capability surface, **proven by hand‑derived
KATs against published Apache spec text + by real `pyarrow 24.0.0`
round‑trip fixtures**:

| Axis | Supported | Notes |
|---|---|---|
| **Page version** | V1 + **V2** | V2 raw‑level‑split path (def/rep levels uncompressed, values section compressed) |
| **Compression** | UNCOMPRESSED, **Snappy**, **GZIP**, **zstd**, **LZ4_RAW (SP149)**, **Brotli (SP154)** | All decompressors are zero‑dep hand‑written: `snappy.rs` (338 LOC); `gzip.rs` (RFC 1951 inflate); `zstd*.rs` (full RFC 8478 pipeline — frame + block + literals (Raw/RLE/Compressed/Treeless) + Huffman (direct + FSE‑weight × 1‑stream + 4‑stream) + sequences (Predefined/RLE/FseCompressed × LL/OF/ML) + 3‑slot repeat‑offset LZ77 execution); `lz4.rs` (raw LZ4 block format — literal + match sequences, minmatch=4, 2-byte LE offset, LZ77 overlapping-copy); `brotli*.rs` (RFC 7932 — 12 layers: bit reader → stream/metablock framing → simple+complex prefix codes → NBLTYPES + NPOSTFIX/NDIRECT + context-map headers → 704-symbol insert-and-copy command alphabet → 64-symbol distance prefix code + recent-distance ring → 122,784-byte static dictionary blob + 121 Appendix B transforms → flat output buffer). All real pyarrow fixtures pass end‑to‑end through `extract()` incl. a 2000‑row zstd stress fixture exercising FseCompressed mode for all three LL/OF/ML codes simultaneously and pyarrow `compression='brotli'` round-trips for the standard flat-i64 + flat-BYTE_ARRAY shape. |
| **Encoding** | PLAIN, **PLAIN_DICTIONARY / RLE_DICTIONARY** | Dictionary page + data‑page index resolve |
| **Repetition** | flat REQUIRED + **flat OPTIONAL (nullable)** + **`LIST<primitive>` (SP143)** + **`MAP<K, V>` and `struct` (SP144)** + **`List<List<T>>`, `List<struct>`, `Map<K, struct>`, `Map<K, List<T>>`, `struct<List/Map/struct>` (SP145)** + **`List<List<List<T>>>` 3‑deep, `List<Map<K,V>>`, `Map<K1, Map<K2,V>>` (SP146 — OBJ-2c-5 FULLY CLOSED)** | OPTIONAL via RLE‑hybrid def‑level decode + null‑scatter; SP143 adds Dremel‑style record assembly for canonical 3‑node `LIST<primitive>` (4‑shape matrix); SP144 adds `Map<K, V>` via `assemble_map_kv` (REQUIRED key enforced) and `struct` via `assemble_struct`; SP145 adds 4 new variants via per‑shape composition; SP146 adds 3 more (`assemble_list_of_list_of_list_primitive` 3-level stack, `assemble_list_of_map_kv` outer-list-of-inner-maps, `assemble_map_of_map_kv` outer-map-of-inner-maps) — every nested Parquet shape pyarrow writes now decodes |
| **Physical types** | INT32, **INT64**, **INT96 (timestamp)**, **FLBA**, **BYTE_ARRAY** | INT96 → `PqValue::Timestamp(i64 ns)` via checked Julian‑day arithmetic |
| **Logical types** | **DECIMAL (INT32/INT64/FLBA, precision 1..=38)**, **FLBA‑UUID** | DECIMAL → `PqValue::Decimal { unscaled: i128, scale: i32 }` |
| **Multi‑row‑group** | yes | Cross‑row‑group column concatenation |
| **Bounds + safety** | `#![forbid(unsafe_code)]`, **256 MiB per‑page cap** (configurable via `extract_with_cap`, SP151), every offset bounds‑checked, typed `PqError` on every failure mode, no panics on attacker bytes | + dedicated pentest module per codec (`pentest_optional` / `pentest_int96_decimal` / `pentest_v2` / etc.) |

**Still deferred** (typed `Unsupported` at `REFRESH` with a precise
error naming the follow‑on slice):
- Legacy LZ4 framing (codec id 5, deprecated Hadoop variant — modern LZ4_RAW codec id 7 is fully supported via SP149)
- 4‑deep nesting (`List<List<List<List<T>>>>` etc.) — would be SP147 if a real fixture demands it; **all 3‑deep and below now supported (OBJ-2c-5 fully closed at SP146)**
- DECIMAL precision > 38 (would need i256)
- Per‑page decompressed size > 256 MiB (SP151 lifted the 64 MiB historical cap; operators with known-trusted producers can lower or raise the cap via `extract_with_cap` up to the per-codec module ceiling)

The reader is feature‑gated through `kessel-fetch`'s `object-store`
feature; the default `cargo build` links **no Parquet code at all**
and the kernel's deterministic state machine is unaffected.

## Project status & maturity

KesselDB is a **complete, functionally‑correct relational SQL database** on a
VSR‑safe, liveness‑tested, real multi‑node consensus core. Every named
production‑readiness gate is met (functional completeness, crash recovery, VSR
safety + adversarial‑partition liveness, multi‑node over sockets, full SQL over
the cluster, exactly‑once + failover, auth/quotas/backpressure, hot
backup + metrics). See the gate table in [`docs/STATUS.md`](docs/STATUS.md).

Honest boundaries (documented, not hidden):

- **Transport encryption (TLS)** is an **opt‑in cargo feature**
  (`--features tls`, rustls) so the default build stays strictly
  zero‑dependency. Without it the wire is plaintext but token‑authenticated
  with a timing‑safe comparison (deploy behind a TLS proxy / private
  network). Hand‑rolling TLS would be irresponsible, hence the feature.
- **HTTPS external sources** are an **opt‑in cargo feature**
  (`--features external-sources-tls`, rustls + webpki‑roots) so the default
  build and plain `--features external-sources` remain zero‑new‑dependency
  and `http://`‑only. Enable this feature to register `https://` endpoints;
  without it only plaintext HTTP is accepted.
- **Object-store external sources (S3 / Azure Blob)** are an **opt‑in
  cargo feature** (`--features external-sources-objstore`, which implies
  `external-sources-tls` and pulls rustls + webpki‑roots + the
  `kessel-objstore` crate). The default build and plain
  `--features external-sources` remain unaffected. Enable this feature to
  register `s3://` or `az://` endpoints; without it those URL schemes are
  rejected at `CREATE` with a clear message. Object-store requests are
  HTTPS-only with full webpki certificate verification and no bypass.
  `FORMAT PARQUET` for `s3://`/`az://` is supported under this same
  feature; the kessel‑parquet crate has its own empty `[dependencies]`
  (the entire reader is hand‑written zero‑dep Rust). See the [Parquet
  capability matrix](#parquet-capability-matrix) below for the exact
  matrix of supported encodings / compressions / types / page versions.
- **Cross‑shard transactions** are implemented, **deterministically**
  (Calvin‑style), over real sockets — *not* blocking two‑phase commit.
  A deployment runs K independent VSR shard groups behind a router
  (rendezvous key→shard mapping). A cross‑shard `Op::Txn` is
  decomposed into per‑shard slices, durably **totally ordered** by a
  replicated sequencer group, then each shard applies its slice in that
  order: a deterministic *decide → commit* in which every shard’s
  verdict is a pure function of its durable state, so the global AND
  decision is recomputable by any router with **no coordinator‑failure
  hole** and no locks held across shards. It is **atomic** (a slice
  that would fail aborts the transaction on every shard),
  **exactly‑once** under client retry (stable `(client,req)` keying),
  and **recoverable** (a full ordered re‑drive after a router restart
  is idempotent). Single‑shard transactions stay on their shard’s own
  VSR group (serializable, fast path). Proven by a deterministic
  adversarial‑drive test composed on the seeded per‑group partition
  corpus, plus over‑sockets atomicity/abort/exactly‑once/recovery and
  concurrency tests. Balance‑guard helpers, destructive `ALTER TABLE`
  (`DROP`/`RENAME COLUMN`), `DROP INDEX`, `DROP TABLE`, and
  overflow‑blob GC are all implemented.

  *Boundary (documented, not hidden):* the router serializes
  cross‑shard commits to drive the global order; an async per‑shard
  pull‑drive is an efficiency follow‑up, not a correctness change.
  Cross‑shard transactions are point‑op batches (`Create`/`Update`/
  `Delete`); cross‑shard scatter‑gather *reads*/SQL text routing is a
  separate, later concern from cross‑shard *transactions*.

Every claim in this repository is backed by the test suite (~2,700+ tests
by default, more with the PostgreSQL and HTTP gateway features); the docs
call out exactly what is proven versus roadmap. The four **strategic‑tier
items S1–S4** (TLA+/model‑checked safety, serializable MVCC/SI, Jepsen
linearizability under partition, deterministic WASM UDFs) are all **shipped**
— see [`docs/THESIS.md`](docs/THESIS.md) for the framing, and
[`docs/STATUS.md`](docs/STATUS.md) for per‑slice records.

## Documentation

| Doc | Contents |
|---|---|
| [`AGENTS.md`](AGENTS.md) | Machine-first operating guide — build/test/run/CLI, wire protocol, repo map, working rules (read this first if you're an agent) |
| [`docs/THESIS.md`](docs/THESIS.md) | The 5 thesis pillars (deterministic / verifiable / replayable / zero‑dep / honest‑docs) + strategic‑tier backlog S1–S4 (all shipped) |
| [`docs/USAGE.md`](docs/USAGE.md) | Install, run, **CLI**, client API, **SQL reference**, clustering, auth, backup & monitoring, external sources + Parquet matrix |
| [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md) | Methodology, measured numbers, scaling model, cloud projections |
| [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) | Cross-DB comparison (KesselDB vs Postgres / SQLite / TigerBeetle) — YCSB-A/B/C + sysbench OLTP RO/WO/RW, wins AND losses, full disclosure |
| [`docs/STATUS.md`](docs/STATUS.md) | Current capabilities summary + production‑readiness gate + per‑slice status (incl. SP109‑SP140 strategic‑tier + the Parquet codec arc through SP154 / OBJ‑2c‑2 closed), performance log |
| [`CHANGELOG.md`](CHANGELOG.md) | Keep-a-Changelog release notes, starting at v1.0.0 |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Storage, replication, sharding, caching, MVCC + WASM + Parquet internals |
| [`kesseldb-tla/`](kesseldb-tla/) | Seven layered TLA+ specs (Replication / MVCCStorage / MVCCTx / MVCCSi / MVCCSsi / MVCCGc / MVCCCutover) + TLC baselines |
| [`clients/python/kesseldb.py`](clients/python/kesseldb.py) | Dependency‑free Python reference client (stdlib‑only, single file) |
| [`docs/superpowers/specs/`](docs/superpowers/specs/) | One design spec per sub‑project |
| [`docs/USAGE.md` → §7c–7f](docs/USAGE.md#7c-external-sources-jsoncsv-over-http) | External sources — register & `REFRESH` paginated JSON/NDJSON/CSV‑over‑HTTP + Parquet over S3/Azure into a table |

## Building & testing

```bash
cargo build                 # all kernel crates, zero external deps
cargo test --workspace      # ~2,700+ default tests (seeded partition/fault sim,
                            # Jepsen linearizability, MVCC TLA+ refinement,
                            # pyarrow Parquet round-trips, WASM-MVP KATs,
                            # 85-seed cross-shard K-invariance sweep)
cargo test --workspace --features pg-gateway                # adds the PostgreSQL gateway suite
cargo test --workspace --features pg-gateway,http-gateway,kessel-http-gateway/test-server   # full matrix
cargo run -p kessel-bench --release -- --help               # benchmarks

# Strategic-tier rigor artifacts:
cd kesseldb-tla/ && tlc -workers auto Replication.tla       # ≥528M states / depth 21 / 0 violations
```

Requires Rust stable 1.95+. No system libraries, no native build steps.

## Contributing

Issues and PRs welcome. The project rule is simple and strict: **every change is
test‑driven, the full suite stays green, and documentation/claims never exceed
what the tests prove.** Each unit of work ships as one reviewed slice with its
own spec under `docs/superpowers/specs/`.

## License

MIT License — see [`LICENSE`](LICENSE). © 2026 Ian Hassard.
