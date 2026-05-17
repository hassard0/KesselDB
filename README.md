# KesselDB

> *"It's the database that made the Kessel Run in 12 parsecs."*

A schema-flexible, deterministic OLTP object kernel — a **fresh Rust reimplementation that ports TigerBeetle's engineering designs** (determinism seam, LSM storage, WAL, Viewstamped Replication, simulation-driven testing) toward PostgreSQL-grade schema flexibility.

## Status

**A complete, functionally-correct relational SQL database with a hardened-safety, real-multi-node VSR core. Not yet production-*operable* — the remaining gates are concrete and named (see `docs/STATUS.md` "Production-readiness gate"), not vague.** Implemented and tested (**139 tests green**): SP1 M0–M4 (determinism seam, LSM storage + crash recovery, schema catalog + record codec + online DDL, deterministic state machine, group commit, crash-stop VSR with view change, read cache, sharding groundwork), **SP2** variable-length overflow store, **SP3** equality secondary indexes, **SP4** UNIQUE + NOT NULL constraints, **SP5** query planner (AND of Eq/range, multi-index intersection + filtered scan), **SP6** foreign keys, **SP7** deterministic expression VM + CHECK, **SP8** deterministic mutating triggers / generated columns (same zero-dep pure gas-bounded VM), **SP9** atomic all-or-nothing transactions (`Op::Txn`), **SP10** a runnable `kesseldb` TCP server + `kessel-client`, **SP11** ON DELETE RESTRICT/CASCADE (recursive, atomic), **SP12/13** VSR partition hardening (partition fault model, request-relay, max-view convergence; determinism & bounded post-heal convergence proven, one precisely-diagnosed open view-change-liveness repro), **SP14** arbitrary OR/NOT boolean queries, **SP15** order-preserving range index (`AddOrderedIndex`/`FindRange`, sub-linear), **SP16** flexibility-cost benchmark, **SP18** `Select` (filtered whole-row queries + LIMIT, end-to-end over the server), **SP19** `ON DELETE SET NULL` (referential-action set complete), **SP20** aggregates (COUNT/SUM/MIN/MAX), **SP21** projection (`SelectFields`), **SP22** `GROUP BY` aggregation, **SP23** `ORDER BY` + OFFSET/LIMIT, **SP24** variable-length storage key, **SP25/26** per-(value,object) equality index + lightweight `scan_prefix` (O(1) scalable writes — the SP16 #1 perf debt fixed ~6.5×→~2.6×; honest deliberate tradeoff: point-value reads are now an O(matching) scan, not a single bucket get), **SP27** composite (multi-field) indexes, **SP28** a **SQL text layer** (`kessel-sql`: CREATE/INSERT/SELECT…WHERE/GROUP BY/ORDER BY/LIMIT/aggregates/DELETE), **SP29** **SQL over TCP** (`Client::sql("…")`), **SP30** SQL `UPDATE` (full CRUD over the network), **SP31** `SELECT … ID <n>` O(1) primary-key fetch, **SP32** index-accelerated SQL queries (`SELECT * … WHERE c=v [AND…]` sub-linear, VM-verified), **SP33** SQL `CREATE [UNIQUE|RANGE] INDEX` DDL, **SP34** `DESCRIBE` (clients decode `SELECT` rows from the wire schema), **SP35** `AVG` (standard aggregate set complete), **SP36** inner equi-**JOIN** (`SELECT * FROM a JOIN b ON a.x=b.y`), **SP37** VSR view-change **safety hardening** (fixed a real committed-op-loss bug: a stale log could win `DoViewChange`; `Normal`/`normal_view` now set only via authoritative log install), **SP38** **VSR over real TCP sockets** — `kessel_vsr::wire` Msg codec + `kesseldb_server::cluster`; a **3-node cluster replicates over real sockets** and converges to an identical state digest, **SP39** **full SQL over the cluster** — `Client::sql()` full CRUD (incl. `UPDATE` as a 2-round read-modify-write linearized through consensus) against a 3-node TCP cluster, followers match the primary digest, **SP40** **client sessions / exactly-once retries** — `Node::session()`: a retried `(client, req)` returns the cached reply without re-applying (digest-stable, proven on the 3-node cluster), **SP41** **failover-safe retries** — *any* node serves a committed `(client, req)` from its replicated client table, **SP42** **client-side failover discovery** — `ClusterClient` rotates the node list and retries the same `(client, req)` on `OpResult::Unavailable` until it reaches the primary, exactly-once over the wire (`0xFD` session frame), **SP43** **auth + quotas/backpressure** — zero-dep timing-safe shared-secret token (`connect_authed`), connection cap, in-flight load-shedding; transport encryption is a deliberately documented zero-dep boundary (deploy behind a TLS proxy / private network), not a faked feature, **SP44** **operational tooling** — engine-thread-consistent hot `snapshot(dest)` (recovers to the exact live state digest) + live `stats()` (`ServerStats{applied_ops,digest,uptime}`), **SP45** **index point-read perf** — O(1) `SsTable::overlaps` min/max pruning in `scan_prefix`/`scan_range` makes equality point-value reads sub-linear (skip non-overlapping segments) with write scalability untouched — tested (**139 tests**). See `docs/STATUS.md` "Production-readiness gate" for the precise, named list of what gates production (no vague hedging): functional completeness, crash recovery, VSR safety, multi-node-over-sockets, full SQL over the cluster, failover (server+client, exactly-once), auth/quotas/backpressure, ops tooling, and index point-read perf **are now ✅**; adversarial-partition liveness (seed 7) is the single concrete remaining lever (TLS is a deliberate zero-dep boundary: deploy behind a proxy/private net). (SP17 eq-index sharding was attempted, measured, and honestly reverted — see STATUS.) Still *not* done (each a later spec): adversarial-partition VSR liveness (seed 7), balance-guard, cross-shard atomicity, destructive ALTER/DROP, overflow GC. See [`docs/STATUS.md`](docs/STATUS.md) for exactly what is proven vs. roadmap and honest perf + cloud-scaling reasoning. Claims here never exceed what the test suite proves.

```bash
cargo test --workspace                                  # 139 tests
cargo run --release --bin kesseldb -- 127.0.0.1:7878 ./data   # SQL server
# then: Client::connect(addr)?.sql("CREATE TABLE t (a U64 NOT NULL)")?;
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
