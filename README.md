# KesselDB

> *"It's the database that made the Kessel Run in 12 parsecs."*

A schema-flexible, deterministic OLTP object kernel — a **fresh Rust reimplementation that ports TigerBeetle's engineering designs** (determinism seam, LSM storage, WAL, Viewstamped Replication, simulation-driven testing) toward PostgreSQL-grade schema flexibility.

## Status

**This is an in-progress, milestone-gated build. It is NOT a complete database.** See [`docs/STATUS.md`](docs/STATUS.md) for exactly what is implemented and tested vs. roadmap. Claims here never exceed what the test suite proves.

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
