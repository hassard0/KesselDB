# KesselDB

> *"It's the database that made the Kessel Run in 12 parsecs."*

A schema-flexible, deterministic OLTP object kernel — a **fresh Rust reimplementation that ports TigerBeetle's engineering designs** (determinism seam, LSM storage, WAL, Viewstamped Replication, simulation-driven testing) toward PostgreSQL-grade schema flexibility.

## Status

**This is a milestone-gated foundation, NOT a complete production database.** Implemented and tested (**77 tests green**): SP1 M0–M4 (determinism seam, LSM storage + crash recovery, schema catalog + record codec + online DDL, deterministic state machine, group commit, crash-stop VSR with view change, read cache, sharding groundwork), **SP2** variable-length overflow store, **SP3** equality secondary indexes, **SP4** UNIQUE + NOT NULL constraints, **SP5** query planner (AND of Eq/range, multi-index intersection + filtered scan), **SP6** foreign keys, **SP7** deterministic expression VM + CHECK constraints (zero-dep, pure, gas-bounded — user logic inside the replicated state machine) — all replication-correct/deterministic and tested (**77 tests**). Still *not* done (each a later spec): deterministic mutating triggers, ON DELETE/UPDATE actions, OR/NOT & order-preserving range index, balance-guard, destructive ALTER/DROP, overflow GC, M3 hardening (partition matrix, socket transport, membership). See [`docs/STATUS.md`](docs/STATUS.md) for exactly what is proven vs. roadmap and honest perf + cloud-scaling reasoning. Claims here never exceed what the test suite proves.

```bash
cargo test --workspace                                  # 47 tests
cargo run -p kessel-bench --release -- 200000 file 1000 # durable, group commit
cargo run -p kessel-bench --release -- 20000  repl      # 3-node replicated
cargo run -p kessel-storage --release --example bench_storage
```

## Why

TigerBeetle is fast *because* it is inflexible (hardcoded schema, single domain, immutable records, static allocation). PostgreSQL is flexible but general-purpose. KesselDB picks a deliberate point on that tradeoff curve: **runtime-defined object types and online DDL, kept deterministic and replicated, on a TB-style storage + consensus core.**

## Design

- [`docs/superpowers/specs/2026-05-17-kesseldb-design.md`](docs/superpowers/specs/2026-05-17-kesseldb-design.md) — full design spec (Sub-project 1)
- [`docs/superpowers/plans/2026-05-17-kesseldb-subproject1.md`](docs/superpowers/plans/2026-05-17-kesseldb-subproject1.md) — implementation plan
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — architecture, replication, sharding, caching

## Build

```bash
cargo build
cargo test            # all crates
cargo run -p kessel-bench -- --help
```

Requires Rust stable (1.95+).

## License

Unlicensed / private. © 2026.
