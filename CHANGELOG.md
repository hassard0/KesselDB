# Changelog

All notable changes to KesselDB will be documented in this file.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), versioning [SemVer](https://semver.org).

## [Unreleased]

### Added

- **PostgreSQL Extended Query protocol (SP-PG-EXTQ V1, 2026-05-29)** —
  full V1 message set `P` (Parse) / `B` (Bind) / `D` (Describe) /
  `E` (Execute) / `S` (Sync) / `C` (Close) / `H` (Flush). Per-connection
  `SessionState` with named + unnamed prepared statements + portals up
  to `MAX_PREPARED_STATEMENTS_PER_CONN = MAX_PORTALS_PER_CONN = 4096`.
  Parse stores SQL VERBATIM; Bind validates parameter formats (text only;
  binary rejected with `0A000` — V2 SP-PG-EXTQ-BIN) and parameter count
  vs Parse's OID hints; Describe 'S' emits ParameterDescription +
  RowDescription/NoData; Describe 'P' emits RowDescription/NoData;
  Execute substitutes `$N` text-format parameters and dispatches
  through `EngineApply::apply_sql`; `max_rows > 0` emits
  `PortalSuspended` with buffered cursor pagination; Sync emits
  `ReadyForQuery('I')` and clears the per-connection `error_state`;
  Close drops named statements/portals; Flush triggers an outbound
  flush. **End-to-end verification**: real `psycopg2.connect(...)` +
  `cur.execute("…WHERE id = %s", (42,))` returns rows on vulcan.
  SQLAlchemy / Drizzle / Prisma / JDBC default-EXTQ paths unblocked
  at the wire level; full ORM-suite formal smoke (T8/T11/T12) is the
  post-V1.1 follow-up.
- **Cross-DB benchmark suite (SP-Bench-Suite T1-T3)** — KesselDB vs
  Postgres + SQLite + TigerBeetle reproducible head-to-head harness at
  `tools/bench-compare/`. Workloads: YCSB-A (50/50 read/update), YCSB-B
  (95/5), YCSB-C (100% reads), sysbench OLTP read-only / write-only /
  read-write. Wins AND losses published verbatim in
  `docs/BENCHMARKS.md` — KesselDB wins YCSB-A/B/C and sysbench WO
  decisively; loses sysbench RO and RW to Postgres / SQLite because
  `Op::Txn` apply-lock serializes RO inner ops.
- **Perf-A read-pool bypass (SP-Perf-A T1-T7)** — parallel-read dispatch
  via `read_only_op(&self, ...)` through `Arc<RwLock<StateMachine>>`
  (T2); in-process apply skips encode/decode roundtrip (T6 Fix A);
  `OpResult::Got` carries `Arc<[u8]>` instead of `Vec<u8>` (T6 Fix B);
  storage memtable + SSTable cached blocks + transaction overlay lifted
  to `Arc<[u8]>` (T7). Measured: **~4.75M ops/sec at N=16 cores,
  p50 < 1 µs, p99 ~3 µs** on the vulcan reference server. Storage
  point-read ceiling honestly diagnosed at ~5M ops/sec (`RwLock`
  reader CAS ping-pong); next arc named SP-Perf-A-SHARD.

### Fixed

- **Cluster test flakes (SP-CLUSTER-FLAKE T2, root-cause fix)** —
  `Node::submit*` / `apply_raw` now retry transient `ViewChange` →
  `Unavailable` the same way production `ClusterClient` does. The fix
  lives in the production code path, not a test relaxation, so any
  call site that previously saw an intermittent transient
  `Unavailable` during a view-change is now automatically retried.
  Closes the long-standing CI intermittent surfaced by stress runs.

### Performance

- Sub-µs p50 read latency at N=16 (Perf-A T2 + T7).
- 4.75M ops/sec parallel-read ceiling (YCSB-C N=16 — ≈ 40× SQLite,
  ≈ 57× Postgres).
- sysbench OLTP write-only N=8 = 53,409 tx/s — 5.2× Postgres at
  the same N.

### Compatibility

- psycopg2 / psycopg3 / asyncpg connect via SCRAM-SHA-256 and run
  parameterized queries through Extended Query (verified on vulcan).
- SQLAlchemy `create_engine(...)` + `text(... :id ...)` parameterized
  SELECT works end-to-end (the formal ORM-suite shape is the T11
  follow-up).
- Drizzle / Prisma / JDBC default-EXTQ paths unblocked at the wire
  level (formal driver smoke is T8/T12).

### Documentation

- README rewritten above the fold with the three night-headlines
  (Postgres ORM compat / 4.8M ops/sec parallel reads / honest cross-DB
  benches); compatibility matrix promoted; performance table adds
  parallel-read row + cross-DB headline table; next-arc named.
- STATUS preamble bumped to 2026-05-29 with a 4-track recap of
  tonight's deliveries.
- ARCHITECTURE: PG-wire section gains the SP-PG-EXTQ V1 paragraph;
  storage section adds the T7 Arc<[u8]> read-fast-path note; atomic
  transactions gains the honest Op::Txn apply-lock perf boundary
  paragraph; V2 follow-ups list trimmed (Extended Query removed).
- USAGE §9 reflects EXTQ V1 — psycopg2 parameterized + SQLAlchemy
  samples; "Simple Query only" limitation dropped.
- BENCHMARKS already published §3-§3e (sysbench OLTP) + §11-§12
  (Perf-A T6/T7) as part of the night's commits.

### Tests

- 1974 default / 2002 with `--features pg-gateway` / 2035 with all
  gateway features (vulcan-measured at HEAD `546e79a`).

## [1.0.0] — 2026-05-28

Initial public release.

### Added

- **Binary protocol over TCP** — length-prefixed `Op::encode()` framing
  with mode tags `0xFE` (SQL), `0xFD` (session / exactly-once), `0xFC`
  (auth handshake), `0xFB` (admin stats), `0xFA` (snapshot). Zero
  external dependencies.
- **HTTP/1.1 gateway** (SP141, opt-in `--features http-gateway`) —
  `/v1/sql`, `/v1/op`, `/v1/health`, `/v1/metrics` (Prometheus text
  v0.0.4). `Authorization: Bearer` constant-time auth + optional
  `X-Kessel-Client-Id` + `X-Kessel-Req-Seq` exactly-once headers.
- **WebSocket gateway** (SP-WS, same `--features http-gateway`) —
  `/v1/ws` upgrade, `kessel-op-v1` subprotocol, binary frames carrying
  `Op::encode()`. RFC 6455 strict handshake, 16-message bounded send
  queue, 30 s ping/pong heartbeat.
- **PostgreSQL Frontend/Backend Protocol v3.0** (SP-PG, opt-in
  `--features pg-gateway`) — Simple Query path + SCRAM-SHA-256
  authentication with the Bearer↔SCRAM bridge (the operator's Bearer
  token IS the SCRAM password input — one credential surface, rotating
  the token rotates HTTP + WS + PG together).
- **`pg_catalog` + `information_schema` stubs** (SP-PG-CAT V1) — synth
  responses for `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`,
  `pg_index`, `pg_constraint`, plus 7 `information_schema` views
  (`tables`, `columns`, `schemata`, `key_column_usage`,
  `table_constraints`, `views`, `routines`). psql, pgcli, pgAdmin 4,
  DBeaver, DataGrip, Metabase, Tableau, Looker, Hex, Superset, and
  pgJDBC all connect + browse out of the box, verified by
  synthetic-peer KATs.
- **Cross-shard scatter scan (SP-A)** — `Select` / `QueryRows` /
  `SelectFields` / `SelectSorted` fan out across K shard groups via
  `scatter_scan` with std-thread workers + bounded per-shard channels.
  Unordered = shard-id-deterministic concat; sorted = `BinaryHeap`
  k-way merge. K-invariance locked across K ∈ {1, 2, 4, 8, 16} by an
  85-seed property sweep.
- **Parquet codec matrix** — 6 of 7 codecs supported
  (UNCOMPRESSED, Snappy, GZIP, zstd, LZ4_RAW, Brotli). SP154 closed
  OBJ-2c-2 with a hand-rolled zero-dep RFC 7932 Brotli decoder
  comparable in scope to the SP125-SP140 zstd arc. Legacy LZ4 framing
  (codec id 5) and LZO remain unsupported; modern LZ4_RAW (codec id 7)
  is fully supported via SP149.
- **Strategic-tier rigor artifacts** — mechanically-verified TLA+
  (S1, `Replication.tla` TLC: 528M states / depth 21 / 0 violations)
  over 7 layered modules (Replication → MVCCStorage → MVCCTx →
  MVCCSi → MVCCSsi → MVCCGc → MVCCCutover); serializable MVCC with
  Cahill SSI (S2); 5 hand-derived Jepsen-style linearizability tests
  under partition + message loss (S3); deterministic WASM-MVP UDF
  interpreter (S4).
- **External sources** — `REGISTER` + `REFRESH` JSON/NDJSON/CSV/Parquet
  from HTTP/HTTPS endpoints (`--features external-sources` /
  `external-sources-tls`) or S3-compatible / Azure Blob object storage
  (`--features external-sources-objstore`).
- **MIT License**.

### Changed

- Cluster test wait-for-primary now uses `submit_with_retry` (the
  test-side analog of the production SP42 `ClusterClient` retry
  contract). Fixes a long-standing intermittent CI flake that depended
  on the primary's commit-counter racing with the test's first op.

### Security

- One credential surface across binary + HTTP + WebSocket + PostgreSQL
  wire (Bearer token, constant-time compared; rotating it rotates
  every listener atomically).
- SCRAM-SHA-256 password derivation via PBKDF2-HMAC-SHA-256 (RFC 8018
  §5.2), zero-dep implementation in `kessel-crypto`.
- HTTPS for external sources via rustls + bundled Mozilla webpki roots
  with full certificate + hostname verification (no bypass, no flag
  to disable). Object-store transport is HTTPS-only.

### Tests

- 1792 default / 1820 with `--features pg-gateway` / 1875 with all
  gateway features. Includes seeded partition/fault simulation,
  multi-replica Jepsen linearizability, MVCC TLA+ refinement, pyarrow
  Parquet round-trips, WASM-MVP KATs, the SP-A 85-seed K-invariance
  sweep, and synthetic-peer suites verifying each GUI tool's verbatim
  introspection SQL.

### Documentation

- README + `docs/USAGE.md` + `docs/ARCHITECTURE.md` + `docs/STATUS.md`
  + `AGENTS.md` shipped polished to coherent terminology and
  consistent test counts.
- `docs/book/` mdBook GitHub Pages site (built + deployed by
  `.github/workflows/pages.yml`); single source of truth — each
  chapter either uses `{{#include}}` against the existing root doc or
  is a thin cross-link landing page.

[1.0.0]: https://github.com/hassard0/KesselDB/releases/tag/v1.0.0
