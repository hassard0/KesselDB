# KesselDB

> *"It's the database that made the Kessel Run in 12 parsecs."*

A schema-flexible, deterministic OLTP object kernel — a **fresh Rust reimplementation that ports TigerBeetle's engineering designs** (determinism seam, LSM storage, WAL, Viewstamped Replication, simulation-driven testing) toward PostgreSQL-grade schema flexibility.

## Status

**This is a milestone-gated foundation, NOT a complete production database.** Implemented and tested (**115 tests green**): SP1 M0–M4 (determinism seam, LSM storage + crash recovery, schema catalog + record codec + online DDL, deterministic state machine, group commit, crash-stop VSR with view change, read cache, sharding groundwork), **SP2** variable-length overflow store, **SP3** equality secondary indexes, **SP4** UNIQUE + NOT NULL constraints, **SP5** query planner (AND of Eq/range, multi-index intersection + filtered scan), **SP6** foreign keys, **SP7** deterministic expression VM + CHECK, **SP8** deterministic mutating triggers / generated columns (same zero-dep pure gas-bounded VM), **SP9** atomic all-or-nothing transactions (`Op::Txn`), **SP10** a runnable `kesseldb` TCP server + `kessel-client`, **SP11** ON DELETE RESTRICT/CASCADE (recursive, atomic), **SP12/13** VSR partition hardening (partition fault model, request-relay, max-view convergence; determinism & bounded post-heal convergence proven, one precisely-diagnosed open view-change-liveness repro), **SP14** arbitrary OR/NOT boolean queries, **SP15** order-preserving range index (`AddOrderedIndex`/`FindRange`, sub-linear), **SP16** flexibility-cost benchmark, **SP18** `Select` (filtered whole-row queries + LIMIT, end-to-end over the server), **SP19** `ON DELETE SET NULL` (referential-action set complete), **SP20** aggregates (COUNT/SUM/MIN/MAX), **SP21** projection (`SelectFields`), **SP22** `GROUP BY` aggregation, **SP23** `ORDER BY` + OFFSET/LIMIT, **SP24** variable-length storage key, **SP25** per-(value,object) equality index (O(1) writes — the SP16 #1 perf debt fixed ~6.5×→~2.6×; honest tradeoff: point-read path regressed, optimization queued) — tested (**115 tests**). (SP17 eq-index sharding was attempted, measured, and honestly reverted — see STATUS.) Still *not* done (each a later spec): SET NULL/ON UPDATE actions, OR/NOT & order-preserving range index, balance-guard, cross-shard atomicity, multi-node VSR over sockets, destructive ALTER/DROP, overflow GC, M3 hardening, auth/TLS. See [`docs/STATUS.md`](docs/STATUS.md) for exactly what is proven vs. roadmap and honest perf + cloud-scaling reasoning. Claims here never exceed what the test suite proves.

```bash
cargo test --workspace                                  # 91 tests
cargo run --release --bin kesseldb -- 127.0.0.1:7878 ./data   # run the server
cargo run -p kessel-bench --release -- 200000 file 1000 # durable, group commit
cargo run -p kessel-bench --release -- 20000  repl      # 3-node replicated
cargo run -p kessel-bench --release -- 100000 flex      # flexibility-cost
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
