# SP-Bench-Suite — progress tracker

Slice arc: install comparison DBs + bench-compare harness + publish
cross-DB workload results in `docs/BENCHMARKS.md`. Track C, runs parallel
to Track A (SP-PG-EXTQ) and Track B (SP-Perf-A).

Design spec: `docs/superpowers/specs/2026-05-28-kesseldb-bench-suite-design.md`.

---

## T1 — install + scaffold + YCSB-C [DONE]

Commits (in order):
- `c7c5e2f` docs(spec) — design spec
- `4895e0a` ops(bench) — comparison DBs verified on vulcan
- `b8fd344` feat(tools/bench-compare) — scaffold (4 driver shells)
- `953538e` fix(bench-compare) — rand small_rng feature
- `6487b26` fix(bench-compare/kesseldb) — catalog-encoded record
- (next) docs(bench) — BENCHMARKS.md + STATUS row + this tracker

Installs verified on vulcan (`192.168.4.178`):
- PostgreSQL 16.14 — running in docker (`bench-pg` on 127.0.0.1:5533).
  Bench DB: `bench` / user `bench` / pass `admin`.
- SQLite ≥ 3.45 — via rusqlite-bundled (hermetic).
- TigerBeetle 0.17.4 — `~/bench/bin/tigerbeetle`, version printout OK.
- KesselDB — in-process via the kessel-sm StateMachine.

Harness: `tools/bench-compare/` — **OUTSIDE the workspace**. Default
KesselDB `cargo build` does NOT pull rusqlite / postgres / etc. Zero
impact on the production binary.

YCSB-C results (3-trial median, 100K rows, 10s duration each):

| DB | N=1 | N=8 | N=16 | p50 (N=8) | p99 (N=8) |
|---|---:|---:|---:|---:|---:|
| KesselDB | 873,950 ops/s | 3,756,961 | 4,749,586 | 1 µs | 3 µs |
| SQLite (bundled) | 139,823 | 203,558 | 118,482 | 30 µs | 191 µs |
| PostgreSQL 16.14 | 5,396 | 67,478 | 82,628 | 114 µs | 188 µs |
| TigerBeetle | — | — | — | — | — |

KesselDB peak (N=16) is **40× SQLite** and **57× Postgres** on YCSB-C.

TigerBeetle: installed and verified, but driver stub in T1 (the
ledger-shaped → KV translation lands in T2 alongside YCSB-A/B). The harness
records a 0-ops row with the `note` field documenting the deferral so
downstream JSON tooling sees an explicit "tried + flagged" entry.

Raw JSON: vulcan:/tmp/bench-ycsb-c.json (36 trial-rows).
Markdown view: `docs/BENCHMARKS.md`.

CI: green (workspace default + pg-gateway + all-features unchanged; this
arc adds zero workspace deps).

---

## T2 — YCSB-A + YCSB-B + TigerBeetle real wiring [DONE]

Commits (in order):
- `b00fab7` feat(bench-compare) — YCSB-A + YCSB-B workloads + update paths
  (KesselDB Op::Update, Postgres UPDATE, SQLite UPDATE; TB honest stub for
  unmappable workloads)
- `6dae403` feat(bench-compare) — real TigerBeetle client behind
  `tigerbeetle-real` cargo feature
- `444dd5b` fix(bench-compare/tigerbeetle) — handle
  `Result<(), CreateAccountsError>` shape
- `4d92a45` fix(bench-compare/tigerbeetle) — reduce create_accounts batch
  to 1K to avoid TooMuchData
- (next) docs(bench) — BENCHMARKS.md update + STATUS + progress tracker

YCSB-A results (3-trial median, 100K rows, 10s duration each):

| DB | N=1 | N=8 | N=16 | p50 (N=8) | p99 (N=8) |
|---|---:|---:|---:|---:|---:|
| KesselDB | 116,390 | 66,660 | 79,830 | 109 µs | 337 µs |
| Postgres | 5,045 | 57,466 | 74,408 | 134 µs | 234 µs |
| SQLite | 74,136 | 12,978 | 6,906 | 28 µs | 86 µs |
| TigerBeetle | — | — | — | — | — |

YCSB-B results (3-trial median, 100K rows, 10s duration each):

| DB | N=1 | N=8 | N=16 | p50 (N=8) | p99 (N=8) |
|---|---:|---:|---:|---:|---:|
| KesselDB | 433,966 | 404,030 | 575,760 | 3 µs | 125 µs |
| SQLite | 127,545 | 15,740 | 9,552 | 22 µs | 8,340 µs |
| Postgres | 5,249 | 65,827 | 80,536 | 116 µs | 199 µs |
| TigerBeetle | — | — | — | — | — |

TigerBeetle YCSB-C real (3-trial median, 100K rows, 10s duration each):

| N=1 | N=8 | N=16 | p50 (N=8) | p99 (N=8) |
|---:|---:|---:|---:|---:|
| 159 | 642 | 1,281 | 12,394 µs | 13,481 µs |

Honest takeaways:
- **YCSB-A (50/50)**: KesselDB wins decisively at N=1 (116K vs SQLite 74K
  vs Postgres 5K). At N=16 KesselDB still wins (~80K) but only marginally
  vs Postgres (~74K). The write path serializes through the apply thread
  — Perf-A T2 read-pool helps reads only. SQLite collapses with concurrent
  writers (6.9K @ N=16) on the rollback-journal lock — canonical SQLite
  one-writer property, not a benchmark artifact.
- **YCSB-B (95/5)**: KesselDB wins decisively at every N. At N=16 KesselDB
  (576K) is 7.1× Postgres (81K) and 60× SQLite (9.5K). The read-heavy
  shape lets Perf-A T2 read-pool kick in for 95% of ops.
- **TigerBeetle YCSB-C**: 159 ops/s @ N=1, 1,281 ops/s @ N=16. LOW because
  TB is designed for batched ops (upstream example pushes 8K transfers per
  batch); single-record `lookup_accounts` measures RPC round-trip + queue
  overhead. The number is honest but not flattering for TB on this shape.
- **TigerBeetle YCSB-A/B**: refused. TB Accounts are append-only after
  creation; no row-UPDATE primitive maps honestly. Driver returns 0-ops
  with explanatory `note`.

TigerBeetle setup notes (captured in BENCHMARKS.md §5 + §7):
- Available crates.io Rust clients target TB 0.16.x; vulcan's headline
  binary is 0.17.4. Wire protocol differs. Downloaded 0.16.78 binary
  alongside (at `/tmp/tb016/tigerbeetle`) so the
  `tigerbeetle-unofficial 0.14.28+0.16.78` crate can talk to a matching
  server. T6 re-tests against 0.17.4 if/when an updated client ships.
- Build needs Zig toolchain (auto-downloaded) + bindgen + clang headers.
  Gated behind `tigerbeetle-real` cargo feature so default build stays
  hermetic. Build with `BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include'`.
- Account record is 128 B fixed (not 1 KiB like the YCSB rows the other
  drivers serve). Asymmetry documented in driver header + BENCHMARKS.md §5.

Raw JSON: vulcan:/tmp/bench-ycsb-a.json (36 rows),
vulcan:/tmp/bench-ycsb-b.json (36 rows), vulcan:/tmp/bench-ycsb-c-tb.json
(9 rows).

---

## T3 — sysbench OLTP [DONE_WITH_CONCERNS]

Commits (in order):
- `7826f75` feat(bench-compare) — workload definitions + CLI surface
  (`--tables`, `--rows-per-table` flags; OltpRO/OltpWO/OltpRW variants in
  `workloads::Workload`; per-shape helpers `is_sysbench()`,
  `sysbench_has_reads/writes()`; named constants in `workloads::sysbench`)
- `bb5d5f0` feat(bench-compare) — driver tx-bracket support across all 4
  drivers (KesselDB Op::Txn{ops}; Postgres Client::transaction(); SQLite
  BEGIN IMMEDIATE for writers / BEGIN for RO; TigerBeetle honest skip)
- `c5d9c9c` fix(bench-compare/postgres) — switch sysbench c+pad to BYTEA
  (Postgres CHAR rejects arbitrary binary bytes in COPY BINARY)
- `28c4b5a` fix(bench-compare/sqlite) — treat SQLITE_BUSY as abort (not
  crash); busy_timeout 10s → 60s; new (txns, inner, aborts, lat)
  return; abort% in note. Was needed to get SQLite N=8/N=16 WO+RW
  through 60s of high write contention.
- (next) docs(bench) — BENCHMARKS.md §3c/§3d/§3e + STATUS + this tracker

Run shape on vulcan (3 trials × 10s × 10 tables × 100K rows/table = 1M
total rows per DB per trial; load is NOT included in the measured 10s):

```
ssh admin@192.168.4.178
cd ~/KesselDB && git pull && cd tools/bench-compare
source ~/.cargo/env
CARGO_TARGET_DIR=/tmp/kdb-target-bench cargo build --release
/tmp/kdb-target-bench/release/bench-compare \
  --db kesseldb,postgres,sqlite \
  --workload oltp-read-only \
  --connections 1,8,16 --duration 10 --tables 10 \
  --rows-per-table 100000 --trials 3 \
  --output /tmp/bench-sysbench-ro.json
# Repeat with --workload oltp-write-only / oltp-read-write.
```

sysbench OLTP read-only results (3-trial median, tx/s):

| DB | N=1 | N=8 | N=16 | p50 (N=8) | p99 (N=8) |
|---|---:|---:|---:|---:|---:|
| KesselDB | 1,241 | 641 | 680 | 12,642 µs | 20,203 µs |
| Postgres | 316 | 4,068 | 5,073 | 1,931 µs | 2,553 µs |
| SQLite | 6,507 | 1,577 | 1,978 | 4,548 µs | 10,096 µs |
| TigerBeetle | — | — | — | — | — |

sysbench OLTP write-only results (3-trial median, tx/s):

| DB | N=1 | N=8 | N=16 | p50 (N=8) | p99 (N=8) |
|---|---:|---:|---:|---:|---:|
| KesselDB | 136,035 | 53,409 | 52,321 | 17 µs | 61 µs |
| Postgres | 940 | 10,254 | 12,883 | 766 µs | 1,044 µs |
| SQLite | 13,451 | 12,757 | 11,857 | 45 µs | 650 µs |
| TigerBeetle | — | — | — | — | — |

sysbench OLTP read-write results (3-trial median, tx/s):

| DB | N=1 | N=8 | N=16 | p50 (N=8) | p99 (N=8) |
|---|---:|---:|---:|---:|---:|
| KesselDB | 1,378 | 718 | 711 | 11,352 µs | 11,998 µs |
| Postgres | 248 | 3,024 | 3,862 | 2,608 µs | 3,349 µs |
| SQLite | 4,835 | 4,386 | 3,960 | 191 µs | 712 µs |
| TigerBeetle | — | — | — | — | — |

Honest takeaways:
- **oltp-read-only**: KesselDB LOSES at every N. SQLite wins N=1 (6.5K
  tx/s, in-process + cheap journal); Postgres wins N=8/N=16 (5K tx/s,
  true read-concurrency from per-backend MVCC snapshots). KesselDB's
  Op::Txn wrapper goes through the apply path which takes the write lock
  for the WHOLE transaction — even when every inner op is `GetById`. The
  Perf-A T2 read-pool bypass is `GetById`-only and does NOT compose with
  `Op::Txn`. Roadmap: route RO Op::Txn through the read-pool fast path
  (statically knowable from Op::Txn{ops}) OR per-shard apply parallelism
  via the K-shard router. Both are post-V1.
- **oltp-write-only**: KesselDB WINS decisively at every N. N=1 = 136K
  tx/s (= 10× SQLite + 145× Postgres). N=8/16 ~52K (= 4× SQLite + 5×
  Postgres). The MemVfs no-fsync write path plus the tight Op::Txn{ops}
  apply loop dominates. SQLite stays surprisingly flat ~12K across N
  (rollback-journal serialization is fast in MEMORY mode). Postgres
  is slow at N=1 (940) because BEGIN+4 ops+COMMIT = ~6 TCP RTTs.
- **oltp-read-write**: KesselDB LOSES at every N. SQLite wins everywhere
  (~4-5K tx/s — surprising; in-process model + MEMORY journal beats both
  Postgres and KesselDB on the 14-op RW shape). Postgres takes silver at
  N=8/N=16 (3-4K tx/s). KesselDB sits at ~700-1,400 tx/s, bottlenecked
  by the same apply-lock serialization that hurts oltp-read-only.
- **Headline**: KesselDB wins ONE of the 3 sysbench-OLTP variants
  (write-only). The other 2 expose Op::Txn's apply-serialization gap.
  Reported honestly; documented as a roadmap target for the next
  perf arc.
- **TigerBeetle**: refused all 3 sysbench variants. TB has no
  arbitrary-SQL transaction primitive; its account/transfer ledger
  model doesn't map onto row-shape SELECT/UPDATE/DELETE/INSERT brackets.

Transaction isolation per DB (recorded in each result's `note` field):
- KesselDB: snapshot isolation (SP112 / S2.3 — `Op::CommitTx` semantics;
  the `Op::Txn` wrapper inherits SI at the Txn boundary)
- Postgres: READ COMMITTED (Postgres 16 default)
- SQLite: SERIALIZABLE (SQLite's only level; single-writer-at-a-time
  via the rollback journal lock + BEGIN IMMEDIATE for write workloads)

Schema mapping per driver:
- KesselDB: 10 `sbtest{N}` types in the catalog with schema `(id U64,
  k I32, c Char(120), pad Char(60))`. RO range scans expand as
  `RANGE_WIDTH × Op::GetById` since the KesselDB apples-to-apples cost
  is the same row-by-row read shape. SUM_RANGE / ORDER_RANGE /
  DISTINCT_RANGE are also expanded as `RANGE_WIDTH × GetById` — the
  client doesn't fold the SUM/ORDER/DISTINCT (the cost we measure is
  the I/O cost of returning 100 records, which is what Postgres/SQLite
  do too). Inner-ops/txn ≈ 406 for RO, 4 for WO, 410 for RW.
- Postgres: 10 UNLOGGED tables × `(id BIGINT PK, k INTEGER, c BYTEA, pad BYTEA)`
  with secondary index on k. BYTEA chosen over upstream CHAR because
  Postgres CHAR rejects arbitrary binary bytes in COPY BINARY; BYTEA
  preserves row-width contract and ORDER BY semantics (lexicographic
  byte order).
- SQLite: 10 tables × `(id INTEGER PK, k INTEGER, c BLOB, pad BLOB)`
  with secondary index on k. journal_mode=MEMORY, synchronous=OFF.
- TigerBeetle: refused with explanatory note.

Raw JSON: vulcan:/tmp/bench-sysbench-{ro,wo,rw}.json (27 trial-rows each).

---

## T4 — TPC-H Q1 / Q6 [PLANNED]

- lineitem table only, SF=0.01 (≈60K rows).
- Q1: COUNT/SUM/AVG with WHERE + GROUP BY l_returnflag, l_linestatus.
- Q6: SUM with WHERE filter only.
- KesselDB target: Op::Aggregate / Op::GroupAggregate.

---

## T5 — JSON → markdown generator + arc-closure docs [PLANNED]

- `tools/bench-compare/scripts/render.{py,rs}` ingests the JSON output and
  regenerates the BENCHMARKS.md tables (medians, stdevs, ratios).
- Update README perf section + STATUS with the headline numbers.

---

## T6 — quiet-vulcan final sweep [PLANNED]

- Pause iddb containers (with consent) for the final clean-run.
- All workloads × all DBs × 3 trials at the same time.
- Headline numbers for the README; freeze BENCHMARKS.md v1.

---
