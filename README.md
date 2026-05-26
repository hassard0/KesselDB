<div align="center">

# KesselDB

**A deterministic, replicated SQL database with PostgreSQL-style flexibility on a TigerBeetle-style core.**

*"It's the database that made the Kessel Run in 12 parsecs."*

`1023 default tests green / 1052 with --features kessel-http-gateway/test-server` · `0 external dependencies in the kernel` · `Rust 1.95+` · single‑binary

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
  `LIST<primitive>` + `MAP<K, V>` + `struct` × UNCOMPRESSED + Snappy +
  GZIP + zstd × PLAIN + dictionary × V1 + V2 data pages × INT64 + INT32 +
  INT96 (timestamps) + DECIMAL (INT32 / INT64 / FLBA, precision ≤ 38) +
  FLBA + BYTE_ARRAY** out of the box. Map keys MUST be REQUIRED per
  Parquet spec. **SP146 FULLY closes OBJ‑2c‑5**: SP145 added 2‑deep
  cross‑products (`List<List<T>>`, `List<struct>`, `Map<K, struct>`,
  `Map<K, List<T>>`, `struct<List/Map/struct>`); SP146 closes the 3
  cross‑products SP145 V1 deferred (`List<List<List<T>>>` 3‑deep,
  `List<Map<K,V>>`, `Map<K1, Map<K2,V>>`) via the same per‑shape
  composition pattern extended one more recursion layer. **All Parquet
  nested types up to 3‑deep nesting now supported** — every shape
  pyarrow writes decodes. See [Parquet capability matrix](#parquet-capability-matrix) below.
  (`--features external-sources`, default off; `--features
  external-sources-objstore` for S3/Azure + Parquet; deterministic kernel
  unaffected when off.)
- **HTTP/1.1 gateway (opt‑in `--features http-gateway`)** — full Op
  surface + SQL + `/v1/health` + `/v1/metrics` (Prometheus text v0.0.4)
  on a sibling TCP listener (`ServerConfig.http_addr`; HTTPS on
  `http_tls_addr` with the `tls` feature). `Authorization: Bearer`
  constant‑time, optional `X-Kessel-Client-Id` + `X-Kessel-Req-Seq`
  exactly‑once headers. JSON responses via the existing
  `kessel_client::format_result_json` contract. Binary protocol
  byte‑untouched; zero external (non‑workspace) deps on the gateway
  crate. See `docs/USAGE.md` §HTTP gateway.
- **Deterministic & verifiable** — the whole engine is a seedable state machine;
  the test suite (1023 default tests / 1052 with `--features kessel-http-gateway/test-server`, 0 ignored) includes seeded partition/fault
  simulation, multi‑replica Jepsen, hand‑derived KATs against published
  spec text for every codec, and adversarial pentests for every public input
  surface.

## Quick start

### Build & run a server

```bash
git clone https://github.com/hassard0/KesselDB && cd KesselDB
cargo build --release

# start a node:  kesseldb [LISTEN_ADDR] [DATA_DIR]
cargo run --release --bin kesseldb -- 127.0.0.1:7878 ./data
# Workspace gate: 1023 default tests, 0 ignored (1052 with --features kessel-http-gateway/test-server)
cargo test --workspace --release
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

## Parquet capability matrix

The `kessel-parquet` crate is a from‑scratch, zero‑external‑dependency
Parquet reader. Its capability surface, **proven by hand‑derived
KATs against published Apache spec text + by real `pyarrow 24.0.0`
round‑trip fixtures**:

| Axis | Supported | Notes |
|---|---|---|
| **Page version** | V1 + **V2** | V2 raw‑level‑split path (def/rep levels uncompressed, values section compressed) |
| **Compression** | UNCOMPRESSED, **Snappy**, **GZIP**, **zstd** | All decompressors are zero‑dep hand‑written (`snappy.rs` 338 LOC / `gzip.rs` RFC 1951 inflate / `zstd*.rs` full RFC 8478 pipeline: frame + block + literals (Raw/RLE/Compressed/Treeless) + Huffman (direct + FSE‑weight × 1‑stream + 4‑stream) + sequences (Predefined/RLE/FseCompressed × LL/OF/ML) + 3‑slot repeat‑offset LZ77 execution). All real pyarrow zstd fixtures pass end‑to‑end through `extract()` incl. a 2000‑row stress fixture exercising FseCompressed mode for all three LL/OF/ML codes simultaneously. |
| **Encoding** | PLAIN, **PLAIN_DICTIONARY / RLE_DICTIONARY** | Dictionary page + data‑page index resolve |
| **Repetition** | flat REQUIRED + **flat OPTIONAL (nullable)** + **`LIST<primitive>` (SP143)** + **`MAP<K, V>` and `struct` (SP144)** + **`List<List<T>>`, `List<struct>`, `Map<K, struct>`, `Map<K, List<T>>`, `struct<List/Map/struct>` (SP145)** + **`List<List<List<T>>>` 3‑deep, `List<Map<K,V>>`, `Map<K1, Map<K2,V>>` (SP146 — OBJ-2c-5 FULLY CLOSED)** | OPTIONAL via RLE‑hybrid def‑level decode + null‑scatter; SP143 adds Dremel‑style record assembly for canonical 3‑node `LIST<primitive>` (4‑shape matrix); SP144 adds `Map<K, V>` via `assemble_map_kv` (REQUIRED key enforced) and `struct` via `assemble_struct`; SP145 adds 4 new variants via per‑shape composition; SP146 adds 3 more (`assemble_list_of_list_of_list_primitive` 3-level stack, `assemble_list_of_map_kv` outer-list-of-inner-maps, `assemble_map_of_map_kv` outer-map-of-inner-maps) — every nested Parquet shape pyarrow writes now decodes |
| **Physical types** | INT32, **INT64**, **INT96 (timestamp)**, **FLBA**, **BYTE_ARRAY** | INT96 → `PqValue::Timestamp(i64 ns)` via checked Julian‑day arithmetic |
| **Logical types** | **DECIMAL (INT32/INT64/FLBA, precision 1..=38)**, **FLBA‑UUID** | DECIMAL → `PqValue::Decimal { unscaled: i128, scale: i32 }` |
| **Multi‑row‑group** | yes | Cross‑row‑group column concatenation |
| **Bounds + safety** | `#![forbid(unsafe_code)]`, 64 MiB per‑page cap, every offset bounds‑checked, typed `PqError` on every failure mode, no panics on attacker bytes | + dedicated pentest module per codec (`pentest_optional` / `pentest_int96_decimal` / `pentest_v2` / etc.) |

**Still deferred** (typed `Unsupported` at `REFRESH` with a precise
error naming the follow‑on slice):
- Brotli compression (OBJ‑2c‑2 follow‑on; LZ4_RAW shipped in SP149)
- 4‑deep nesting (`List<List<List<List<T>>>>` etc.) — would be SP147 if a real fixture demands it; **all 3‑deep and below now supported (OBJ-2c-5 fully closed at SP146)**
- DECIMAL precision > 38 (would need i256)
- Per‑page decompressed size > 64 MiB

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

Every claim in this repository is backed by the test suite (`1023 default tests / 1052 with --features kessel-http-gateway/test-server, 0 ignored`); the
docs call out exactly what is proven versus roadmap. The four
**strategic‑tier items S1–S4** (TLA+/model‑checked safety, serializable
MVCC/SI, Jepsen linearizability under partition, deterministic WASM
UDFs) are all **shipped** — see [`docs/THESIS.md`](docs/THESIS.md) for
the framing, and [`docs/STATUS.md`](docs/STATUS.md) for per‑slice
records (SP109 / SP110‑SP116 / SP117 / SP118).

## Documentation

| Doc | Contents |
|---|---|
| [`AGENTS.md`](AGENTS.md) | Machine-first operating guide — build/test/run/CLI, wire protocol, repo map, working rules (read this first if you're an agent) |
| [`docs/THESIS.md`](docs/THESIS.md) | The 5 thesis pillars (deterministic / verifiable / replayable / zero‑dep / honest‑docs) + strategic‑tier backlog S1–S4 (all shipped) |
| [`docs/USAGE.md`](docs/USAGE.md) | Install, run, **CLI**, client API, **SQL reference**, clustering, auth, backup & monitoring, external sources + Parquet matrix |
| [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md) | Methodology, measured numbers, scaling model, cloud projections |
| [`docs/STATUS.md`](docs/STATUS.md) | Production‑readiness gate, per‑slice status (incl. SP109‑SP139 strategic‑tier + Parquet codec arc), performance log |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Storage, replication, sharding, caching, MVCC + WASM + Parquet internals |
| [`kesseldb-tla/`](kesseldb-tla/) | Seven layered TLA+ specs (Replication / MVCCStorage / MVCCTx / MVCCSi / MVCCSsi / MVCCGc / MVCCCutover) + TLC baselines |
| [`clients/python/kesseldb.py`](clients/python/kesseldb.py) | Dependency‑free Python reference client (stdlib‑only, single file) |
| [`docs/superpowers/specs/`](docs/superpowers/specs/) | One design spec per sub‑project |
| [`docs/USAGE.md` → §7c–7f](docs/USAGE.md#7c-external-sources-jsoncsv-over-http) | External sources — register & `REFRESH` paginated JSON/NDJSON/CSV‑over‑HTTP + Parquet over S3/Azure into a table |

## Building & testing

```bash
cargo build                 # all kernel crates, zero external deps
cargo test --workspace      # 1023 default tests / 1052 with --features kessel-http-gateway/test-server (incl. seeded partition/fault sim,
                            # Jepsen linearizability, MVCC TLA+ refinement,
                            # pyarrow Parquet round-trips, WASM-MVP KATs)
cargo run -p kessel-bench --release -- --help   # benchmarks

# Strategic-tier rigor artifacts:
cd kesseldb-tla/ && tlc -workers auto Replication.tla   # ≥528M states / depth 21 / 0 violations
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
