# Changelog

All notable changes to KesselDB will be documented in this file.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), versioning [SemVer](https://semver.org).

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
