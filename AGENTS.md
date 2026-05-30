# AGENTS.md — operating guide for KesselDB

The machine-first entry point. Read this first. Humans: see
[`README.md`](README.md); deep usage is [`docs/USAGE.md`](docs/USAGE.md).

## What this is

KesselDB: a deterministic, replicated SQL database in **pure Rust, zero
external dependencies**. PostgreSQL-style flexibility (runtime tables,
online DDL, SQL, constraints, triggers, transactions) on a
TigerBeetle-style core (deterministic state machine, LSM+WAL, Viewstamped
Replication, seeded simulation testing).

Status: every named production-readiness gate is met; see
[`docs/STATUS.md`](docs/STATUS.md) for the precise gate table and per-slice
history. Every claim is backed by the test suite.

## Build / test / run

```bash
cargo build --workspace                          # all crates, no external deps, no native steps
cargo test  --workspace                          # 2018 default tests
cargo test  --workspace --features pg-gateway    # 2046 (adds SP-PG + SP-PG-CAT + SP-PG-EXTQ V1)
cargo test  --workspace --features pg-gateway,http-gateway,kessel-http-gateway/test-server   # 2079 — full matrix

cargo run --release --bin kesseldb -- 127.0.0.1:7878 ./data   # single open node, binary protocol only

# All wire surfaces (binary + HTTP + WS + PG) on a single node:
cargo build --release --bin kesseldb -p kesseldb-server --features pg-gateway,http-gateway
KESSELDB_TOKEN=secret \
KESSELDB_HTTP_ADDR=127.0.0.1:8080 \
KESSELDB_PG_ADDR=127.0.0.1:5432 \
  ./target/release/kesseldb 127.0.0.1:7878 ./data
```

Rust stable 1.95+. The test suite is the source of truth — if it is green,
the documented behaviour holds.

## Talk to it without writing code (preferred for agents)

The `kessel` CLI is line-oriented with **meaningful exit codes** — do not
scrape text to detect success.

```bash
cargo run -q -p kessel-client --bin kessel -- "CREATE TABLE t (v U64 NOT NULL)"
cargo run -q -p kessel-client --bin kessel -- "INSERT INTO t ID 1 (v) VALUES (42)"
cargo run -q -p kessel-client --bin kessel -- "SELECT SUM(v) FROM t"     # => = 42
echo "SELECT * FROM t ID 1" | cargo run -q -p kessel-client --bin kessel  # pipe
```

`kessel [--addr HOST:PORT] [--token TOKEN] ["SQL"]`. Exit `0` = success,
`1` = statement error / connection failure, `2` = bad usage. With no SQL
arg it reads stdin (one statement per line; `#`/`--` lines are comments).
SQL reference: [`docs/USAGE.md`](docs/USAGE.md) §4.

From Rust, use `kessel_client::{Client, ClusterClient, format_result}`.

## Wire protocols (for non-Rust clients)

KesselDB exposes four wire surfaces; all run on the same engine and apply
the same `Op`. The binary protocol is the deterministic fast path and the
default.

- **Binary** — length-prefixed `[u32 LE len][payload]`. First payload byte
  selects mode: plain `Op::encode()`, `0xFE`+SQL, `0xFD`+session frame
  (exactly-once), `0xFC`+token (auth), `0xFB` (stats), `0xFA`+dir
  (snapshot). Full table in [`docs/USAGE.md`](docs/USAGE.md) §12.
- **HTTP/1.1** — `/v1/sql`, `/v1/op`, `/v1/health`, `/v1/metrics`. JSON
  responses. `--features http-gateway`. See `docs/USAGE.md` §10.
- **WebSocket** — `/v1/ws` upgrade, `kessel-op-v1` subprotocol, binary
  frames carrying `Op::encode()` payloads. Same `http-gateway` feature.
  See `docs/USAGE.md` §10 → WebSocket.
- **PostgreSQL Frontend/Backend v3.0** — Simple Query + SCRAM-SHA-256 +
  Bearer↔SCRAM bridge. `--features pg-gateway`. See `docs/USAGE.md` §9.

## Repo map

| Path | What |
|---|---|
| `crates/kesseldb-server` | node binary, engine, single-node + cluster servers, `scatter_scan` |
| `crates/kessel-client` | `Client`, `ClusterClient`, the `kessel` CLI |
| `crates/kessel-sql` | SQL tokenizer + planner |
| `crates/kessel-sm` | deterministic state machine |
| `crates/kessel-storage` | LSM + WAL + bloom + bounded compaction + MVCC dispatch |
| `crates/kessel-vsr` | Viewstamped Replication + seeded simulator + 5 Jepsen tests |
| `crates/kessel-shard` | rendezvous key→shard hashing |
| `crates/kessel-http-gateway` | HTTP/1.1 + WebSocket (`--features http-gateway`) |
| `crates/kessel-pg-gateway` | PostgreSQL FB v3.0 + `pg_catalog` (`--features pg-gateway`) |
| `crates/kessel-fetch` + `kessel-objstore` + `kessel-parquet` | external sources stack |
| `crates/kessel-wasm` | zero-dep WASM-MVP interpreter (S4) |
| `crates/kessel-{proto,catalog,codec,expr,cache,crypto}` | wire types, schema, codec, expr VM, read cache, crypto |
| `docs/STATUS.md` | current capabilities summary + gate table + per-slice status |
| `docs/USAGE.md` | install, CLI, client API, SQL reference, clustering, auth, ops, all wire surfaces |
| `docs/ARCHITECTURE.md` | internals |
| `docs/superpowers/specs/` | one design spec per sub-project |
| `kesseldb-tla/` | seven TLA+ modules + TLC baselines (S1 + the MVCC stack) |
| `.github/workflows/release.yml` | builds Linux/macOS/Windows binaries on `v*` tags |

## Working rules (apply if you modify this repo)

1. **Test-driven, one slice at a time.** Add/extend a test, implement,
   keep `cargo test --workspace` fully green, then commit. Never commit
   red or unverified code.
2. **Claims never exceed tests.** Docs (`README`, `STATUS`, specs) state
   only what the suite proves. If a benchmark contradicts an expected
   result, report the real number and reframe — do not overclaim (see the
   SP46 / SP48 self-corrections for the expected discipline).
3. **One spec per slice** under `docs/superpowers/specs/`, and update
   `docs/STATUS.md` (gate/table) + `README.md` test count.
4. **Zero external dependencies** is a hard design rule. Don't add crates.
5. **Determinism is sacred.** Anything affecting replicated state must be
   deterministic; engine-local accelerators (caches, blooms) must be
   digest-invisible and proven so by the full corpus.
6. Commit per slice (history has shown disk-full truncations; per-slice
   commits are the recovery mechanism). End commit messages with the
   project's `Co-Authored-By` line.
