<div align="center">

# KesselDB

**A deterministic, replicated SQL database with PostgreSQL-style flexibility on a TigerBeetle-style core.**

*"It's the database that made the Kessel Run in 12 parsecs."*

`222 tests green` · `0 external dependencies` · `Rust 1.95+` · single‑binary

</div>

---

## What is KesselDB?

KesselDB is a from‑scratch Rust database that takes the engineering ideas behind
[TigerBeetle](https://github.com/tigerbeetle/tigerbeetle) — a deterministic state
machine, an LSM storage engine, a write‑ahead log, Viewstamped Replication, and
simulation‑driven testing — and lifts them to a **general, schema‑flexible SQL
database** instead of a single hard‑coded domain.

You get runtime‑defined tables and online DDL, real SQL (joins, aggregates,
indexes, constraints, triggers, transactions), and a replicated multi‑node
cluster with exactly‑once client semantics — while the core stays a single
deterministic state machine that can be replayed bit‑for‑bit from a seed.

It is written in **pure Rust with zero external dependencies** — determinism is a
feature, not an aspiration.

## Highlights

- **Real SQL** — `CREATE TABLE`, `ALTER TABLE … ADD COLUMN` (online, no lock), `DROP TABLE`, `INSERT`, `SELECT` (filters
  incl. `IN` / `BETWEEN` / `LIKE` / `IS [NOT] NULL` / `AND`/`OR`/`NOT`, `JOIN`, `GROUP BY`,
  `ORDER BY`, `LIMIT/OFFSET`), `UPDATE`, `DELETE`,
  `COUNT/SUM/MIN/MAX/AVG`, `CREATE [UNIQUE|RANGE] INDEX`, `DESCRIBE`, `EXPLAIN`.
- **Constraints & logic** — `NOT NULL`, `UNIQUE`, foreign keys with
  `ON DELETE RESTRICT/CASCADE/SET NULL`, `CHECK`, and deterministic triggers
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
- **External sources** — register a JSON or CSV-over-HTTP endpoint as a table
  (`CREATE EXTERNAL SOURCE … FORMAT JSON|CSV`), populate it with `REFRESH`,
  and query with ordinary SQL (`--features external-sources`, default off; the
  deterministic kernel is unaffected when off).
- **Deterministic & verifiable** — the whole engine is a seedable state machine;
  the test suite includes a seeded partition/fault simulation corpus.

## Quick start

### Build & run a server

```bash
git clone https://github.com/hassard0/KesselDB && cd KesselDB
cargo build --release

# start a node:  kesseldb [LISTEN_ADDR] [DATA_DIR]
cargo run --release --bin kesseldb -- 127.0.0.1:7878 ./data
```

### Query it in one command — no code required

```bash
kessel "CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)"
kessel "INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)"
kessel "SELECT SUM(bal) FROM acct WHERE owner = 100"     # => = 50
kessel "SELECT * FROM acct"                              # aligned table
kessel "DESCRIBE acct"                                   # readable schema
kessel --json "SELECT * FROM acct"                       # {"status":"ok","rows":[…]}

echo "SELECT * FROM acct ID 1" | kessel                  # pipe a .sql file
kessel                                                   # interactive shell (\? for commands)
```

The `kessel` CLI (`cargo run -p kessel-client --bin kessel -- …`, or
`target/release/kessel` after a release build) is one-shot, pipe, and
interactive, with reliable exit codes and a `--json` mode — ideal for
scripts, ops, and agents. In the shell, `\?` lists commands, `\d <table>`
describes a table, `\timing` toggles query timing.

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

That's the whole loop: connect, run SQL, get results — over a single TCP socket,
no drivers, no dependencies.

→ Full instructions, SQL reference, cluster setup, auth and operations are in
**[`docs/USAGE.md`](docs/USAGE.md)**.

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
| Durable create, group commit (~1 K batch) | ~87 K ops/s (local NVMe) |
| Concurrent durable, 8 clients | **~1,870 ops/s** — group commit + `TCP_NODELAY` (conservative; rises with concurrency) |
| Pipelined batch, 1 connection | **~52,700 ops/s** — N statements per round‑trip |
| SQL compile, prepared‑statement cache | **~574 K → ~15 M stmt/s** (cold → cached) |
| Equality / composite `WHERE` | index‑narrowed, not full scan (equivalence‑oracle verified) |
| Range/band `WHERE v BETWEEN a AND b` (range index) | **~35 ms → ~0.31 ms (~112×)**, oracle‑verified |
| `MIN`/`MAX` on a range‑indexed column | **~23 ms → ~5 µs (~4,600×)** — columnar fast‑path, answered from the index extreme (no scan), oracle‑verified |
| Point read | ≤8 bloom‑probed segments (~28 ns/segment), bounded by design |
| 3‑node replicated | ~161 K ops/s |

Every figure is reproducible from the test suite / `kessel-bench`, and
each query accelerator is guarded by a randomized equivalence oracle
(the accelerated result is proven identical to a brute‑force scan). Full
methodology, the single‑core/fsync/RTT scaling model, and
order‑of‑magnitude projections for common cloud instance + storage
configurations are in **[`docs/PERFORMANCE.md`](docs/PERFORMANCE.md)**.

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

Every claim in this repository is backed by the test suite (`222 tests`); the
docs call out exactly what is proven versus roadmap.

## Documentation

| Doc | Contents |
|---|---|
| [`AGENTS.md`](AGENTS.md) | Machine-first operating guide — build/test/run/CLI, wire protocol, repo map, working rules (read this first if you're an agent) |
| [`docs/USAGE.md`](docs/USAGE.md) | Install, run, **CLI**, client API, **SQL reference**, clustering, auth, backup & monitoring |
| [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md) | Methodology, measured numbers, scaling model, cloud projections |
| [`docs/STATUS.md`](docs/STATUS.md) | Production‑readiness gate, per‑slice status, performance log |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Storage, replication, sharding, caching internals |
| [`clients/python/kesseldb.py`](clients/python/kesseldb.py) | Dependency‑free Python reference client (stdlib‑only, single file) |
| [`docs/superpowers/specs/`](docs/superpowers/specs/) | One design spec per sub‑project |
| [`docs/USAGE.md` → §7c](docs/USAGE.md#7c-external-sources-jsoncsv-over-http) | External sources — register & `REFRESH` JSON/CSV-over-HTTP into a table |

## Building & testing

```bash
cargo build                 # all crates, zero external deps
cargo test --workspace      # 222 tests (incl. seeded partition/fault simulation)
cargo run -p kessel-bench --release -- --help   # benchmarks
```

Requires Rust stable 1.95+. No system libraries, no native build steps.

## Contributing

Issues and PRs welcome. The project rule is simple and strict: **every change is
test‑driven, the full suite stays green, and documentation/claims never exceed
what the tests prove.** Each unit of work ships as one reviewed slice with its
own spec under `docs/superpowers/specs/`.

## License

Unlicensed / private. © 2026. (Re‑licensing for public open‑source release is
tracked separately.)
