<div align="center">

# KesselDB

**A deterministic, replicated SQL database with PostgreSQL-style flexibility on a TigerBeetle-style core.**

*"It's the database that made the Kessel Run in 12 parsecs."*

`154 tests green` · `0 external dependencies` · `Rust 1.95+` · single‑binary

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

- **Real SQL** — `CREATE TABLE`, `DROP TABLE`, `INSERT`, `SELECT` (filters
  incl. `IN` / `BETWEEN` / `IS [NOT] NULL` / `AND`/`OR`/`NOT`, `JOIN`, `GROUP BY`,
  `ORDER BY`, `LIMIT/OFFSET`), `UPDATE`, `DELETE`,
  `COUNT/SUM/MIN/MAX/AVG`, `CREATE [UNIQUE|RANGE] INDEX`, `DESCRIBE`.
- **Constraints & logic** — `NOT NULL`, `UNIQUE`, foreign keys with
  `ON DELETE RESTRICT/CASCADE/SET NULL`, `CHECK`, and deterministic triggers
  (a gas‑bounded zero‑dep expression VM).
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
  point reads, and an in‑memory read cache for hot keys (all on by default).
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
kessel "SELECT * FROM acct"                              # prints an aligned table

echo "SELECT * FROM acct ID 1" | kessel                  # pipe a .sql file
kessel                                                   # interactive shell
```

The `kessel` CLI (`cargo run -p kessel-client --bin kessel -- …`, or
`target/release/kessel` after a release build) is one-shot, pipe, and
interactive, with reliable exit codes — ideal for scripts, ops, and agents.

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

## Performance (localhost, single thread, honest)

| Path | Result |
|---|---|
| State‑machine create (in‑mem, 128B record) | ~245K ops/s |
| Durable create, group commit (batch 1000) | ~87K ops/s |
| SQL compile, prepared‑statement cache | **574K → 15.0M stmt/s (26.2×)** |
| Point read | ≤8 bloom‑probed segments (~28 ns/segment), bounded by design |
| 3‑node replicated | ~161K ops/s |

Numbers are reproducible (`cargo run -p kessel-bench --release -- --help`) and
every figure in the docs is backed by a benchmark or a test. See
[`docs/STATUS.md`](docs/STATUS.md) for the full performance log and the precise
production‑readiness gate.

## Project status & maturity

KesselDB is a **complete, functionally‑correct relational SQL database** on a
VSR‑safe, liveness‑tested, real multi‑node consensus core. Every named
production‑readiness gate is met (functional completeness, crash recovery, VSR
safety + adversarial‑partition liveness, multi‑node over sockets, full SQL over
the cluster, exactly‑once + failover, auth/quotas/backpressure, hot
backup + metrics). See the gate table in [`docs/STATUS.md`](docs/STATUS.md).

Honest boundaries (documented, not hidden):

- **Transport encryption (TLS)** is intentionally *not* in‑process — hand‑rolled
  crypto would violate the zero‑dependency design. Deploy behind a TLS‑terminating
  proxy or on a private/encrypted network; the wire is plaintext but
  token‑authenticated with a timing‑safe comparison.
- **Non‑gating roadmap** (tracked, not blocking): balance‑guard helpers,
  cross‑shard transactions, destructive `ALTER TABLE` & `DROP INDEX` (`DROP TABLE` done, SP54), overflow GC.

Every claim in this repository is backed by the test suite (`154 tests`); the
docs call out exactly what is proven versus roadmap.

## Documentation

| Doc | Contents |
|---|---|
| [`AGENTS.md`](AGENTS.md) | Machine-first operating guide — build/test/run/CLI, wire protocol, repo map, working rules (read this first if you're an agent) |
| [`docs/USAGE.md`](docs/USAGE.md) | Install, run, **CLI**, client API, **SQL reference**, clustering, auth, backup & monitoring |
| [`docs/STATUS.md`](docs/STATUS.md) | Production‑readiness gate, per‑slice status, performance log |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Storage, replication, sharding, caching internals |
| [`docs/superpowers/specs/`](docs/superpowers/specs/) | One design spec per sub‑project |

## Building & testing

```bash
cargo build                 # all crates, zero external deps
cargo test --workspace      # 154 tests (incl. seeded partition/fault simulation)
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
