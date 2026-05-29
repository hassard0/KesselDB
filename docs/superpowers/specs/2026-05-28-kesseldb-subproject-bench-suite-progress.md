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

## T3 — sysbench OLTP [PLANNED]

- 10 tables × 100K rows × `(id, k, c, pad)` shape with secondary index on `k`.
- 3 sub-workloads: oltp-read-only (10 SELECT/tx), oltp-write-only (4 writes/tx),
  oltp-mixed (10+4/tx). 1 tx = the bench unit.
- Add a transaction-bracket API to each driver (KesselDB BeginTx / CommitTx,
  Postgres BEGIN/COMMIT, SQLite BEGIN/COMMIT).

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
