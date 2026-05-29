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

## T2 — YCSB-A + YCSB-B + TigerBeetle real wiring [PLANNED]

- Add Op::Update path + write workers to KesselDB / Postgres / SQLite drivers.
- YCSB-A = 50/50 read/update, uniform random key, ~1 KiB row.
- YCSB-B = 95/5 read/update, same shape.
- TigerBeetle driver: load via create_accounts (one account per YCSB row),
  read via lookup_accounts. YCSB-A/B writes don't map cleanly to TB's
  append-only ledger; document the asymmetry, run TB on YCSB-C only.
- Output JSON: same shape; one row per (db, workload, N, trial).
- Update BENCHMARKS.md with the new tables; preserve T1's YCSB-C table
  unchanged (T1 numbers are the lock).

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
