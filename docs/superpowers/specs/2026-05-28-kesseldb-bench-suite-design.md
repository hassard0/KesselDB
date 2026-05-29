## SP-Bench-Suite — KesselDB vs Postgres vs SQLite vs TigerBeetle — design spec

Date: 2026-05-28
Author: Track C (parallel to Track A: SP-PG-EXTQ; Track B: SP-Perf-A)
HEAD on main when this slice opened: `de27d46`
Scope: design spec + DB installs + bench-compare scaffold + first cross-DB workload (YCSB-C).
T2..T6 named.

---

### 1. Context

Perf-A T2 landed sub-microsecond p50 reads and 4.8M ops/sec at N=16 on KesselDB
(see `docs/superpowers/specs/2026-05-28-kesseldb-subproject-perf-a-progress.md`).
These numbers are CREDIBLE within KesselDB's own bench harness, but they mean
nothing to a reader who does not already trust the kessel-bench measurement
methodology.

The fix is a **comparison baseline** under identical hardware, workload, and
durability promise. This arc publishes:

- KesselDB vs Postgres vs SQLite vs TigerBeetle on the same vulcan host
- The same workloads (YCSB-A/B/C, sysbench OLTP, TPC-H Q1/Q6)
- A single JSON-emitting harness (one binary per DB driver, identical metric code)
- A public comparison table in `docs/BENCHMARKS.md`

These numbers are the foundation for any KesselDB blog post, README perf
section, or "why KesselDB" pitch. The arc commits to publishing **every**
number — wins AND losses.

---

### 2. Scope — V1 (this arc = T1..T6)

**In-scope:**

1. 5-7 workloads × 4 DBs × 3 trials, JSON output → markdown comparison table
2. Drivers for KesselDB (in-process), Postgres (tokio-postgres), SQLite
   (rusqlite, bundled), TigerBeetle (subprocess + tigerbeetle-rs client)
3. Same client concurrency across DBs (N=1, 8, 16)
4. Same data-volume per workload across DBs
5. Same durability promise per DB (Postgres: synchronous_commit=on; SQLite:
   synchronous=FULL; TigerBeetle: default; KesselDB: AutosyncMode::EveryCommit)
6. Honest reporting: every workload result published (`docs/BENCHMARKS.md`),
   including the ones where KesselDB loses

**Out-of-scope (V1):**

- Networked client-server bench (this V1 measures local DB engines —
  drivers connect over loopback for the non-in-process targets)
- Distributed / multi-node bench (Postgres replicas, KesselDB shard groups)
- Benchmarks that exercise V1 KesselDB gaps (cross-shard joins, recursive
  CTEs, window functions) — those land when the gap closes
- Production tuning sweep (default DB configs only, except where durability
  parity requires a flag)

---

### 3. Workloads to ship

Each workload defined SQL-agnostically (operation types, data shape, scale,
target QPS). The bench-compare harness translates to each DB's actual SQL
dialect or API call.

#### YCSB family
- **YCSB-C** (T1): 100% read, uniform random key — the "raw read throughput"
  headline workload
- **YCSB-A** (T2): 50% read / 50% update — realistic mixed OLTP
- **YCSB-B** (T2): 95% read / 5% update — typical web-app workload

#### sysbench OLTP family
- **OLTP-read-only** (T3): 10 SELECT queries per "transaction" over the
  indexed `k` column
- **OLTP-write-only** (T3): 4 INSERT/UPDATE/DELETE ops per transaction
- **OLTP-mixed** (T3): 10 SELECT + 4 writes per transaction (the default
  sysbench OLTP profile)

#### TPC-H subset
- **TPC-H Q1** (T4): single-table aggregate — COUNT/SUM/AVG over `lineitem`
  with WHERE + GROUP BY (this is "pricing summary report")
- **TPC-H Q6** (T4): SUM with WHERE filter — simplest possible analytical
  query, single table, no joins, no GROUP BY ("forecasting revenue change")

---

### 4. Schema

- **YCSB**: `(id BIGINT PRIMARY KEY, field0..field9 TEXT)` × 100K-1M rows.
  Each `field` is a random 100-byte ASCII string; record size ≈ 1KB.
- **sysbench OLTP**: 10 tables × 100K rows × `(id INT, k INT, c CHAR(120),
  pad CHAR(60))`, with `id` primary key + secondary index on `k`. Same
  shape as upstream sysbench oltp_common.lua.
- **TPC-H**: `lineitem` table only, scale-factor 0.01 (≈60K rows) for T4 V1.
  Future T7+ may add larger SF.

---

### 5. Methodology

- All DBs on the same hardware (vulcan: 24-core Xeon, 251 GiB RAM, NVMe)
- All DBs configured for the same write-durability promise (see §2.5)
- Each workload runs **3 trials**; **median** reported, **stdev** shown
- Same data-volume per workload across DBs
- Same client concurrency (N ∈ {1, 4, 8, 16})
- Output is JSON, one entry per (db, workload, N, trial):
  `{db, workload, N, trial, ops_per_sec, p50_us, p99_us, p99_99_us, runtime_secs}`
- Comparison table `docs/BENCHMARKS.md` generated from the JSON by a small
  shell/Rust helper

---

### 6. Honest reporting commitments

- **Publish every number, faster AND slower.** KesselDB will lose some
  workloads: TigerBeetle on its native ledger-accounting workload (when we
  re-translate YCSB-C to TigerBeetle's lookup_accounts), Postgres on TPC-H
  Q1/Q6 if its query planner picks a bad plan at SF=0.01, SQLite on
  multi-client write throughput. Show all of them.
- **Publish the workload definition + the SQL/op script used** so anyone can
  reproduce. The bench-compare crate is the source of truth.
- **Note configuration**: durability mode, write-batching, parallelism
  settings, JIT/cache settings.
- **Note the hardware**: vulcan = 24-core Intel Xeon, 251 GiB RAM, 4× V100
  (irrelevant for CPU/IO bench), NVMe storage, kernel 6.14.
- **Note DB versions** (Postgres 16.14, SQLite 3.45 or rusqlite-bundled
  3.45+, TigerBeetle 0.17.4, KesselDB git rev `<commit>`).

---

### 7. Eight weak-spots self-review

1. **Single-machine benchmarks lie about distributed-systems work.** Note
   prominently in BENCHMARKS.md. The Track-A/B/C comparison is about engine
   throughput, NOT replication topology.
2. **Each DB's default config is optimized for a different goal.** Show
   "default" config only in V1; "tuned" sweep deferred to T7+.
3. **SQLite is single-threaded by design** (one writer at a time, even with
   WAL). N>1 numbers for SQLite writes are constrained by this. Show N=1
   prominently and call out the SQLite write-concurrency note.
4. **TigerBeetle's API is ledger-specific.** Translating sysbench OLTP
   requires creating fake "ledger entries" that aren't its workload — note
   this. YCSB-C can map to lookup_accounts (each account = a "row"), but
   YCSB-A/B writes do not map cleanly; T2 ships TB with a documented caveat.
5. **Postgres with default fsync=on always loses pure-write throughput vs
   SQLite WAL_MEMORY mode.** Show fsync=on (default) AND fsync=off
   ("hostile durability") variants where SQLite shows journal_mode=WAL.
6. **Bench harness in same process as DB (KesselDB) vs separate process
   (Postgres/TigerBeetle in docker or via TCP).** Measure overhead — V1
   reports raw numbers; T6 (final sweep) adds an IPC-overhead column where
   applicable.
7. **YCSB's uniform random keys hit page cache disproportionately.** Note
   that real workloads have temporal locality; YCSB-D (read-latest) is a
   future addition, not in this V1.
8. **We're using cargo-built libraries for SQLite (rusqlite bundled) and
   Postgres (tokio-postgres client).** The comparison crate ABIs are not
   exactly the same as the server CLIs. SQLite-bundled is identical to the
   apt sqlite3 binary at version parity; Postgres client→TCP→server matches
   what an app would actually see in production.

---

### 8. Task decomposition

- **T1** (this slice): design spec + install Postgres + SQLite + TigerBeetle
  on vulcan + tools/bench-compare scaffold + YCSB-C workload + first cross-DB
  run + BENCHMARKS.md v0.
- **T2**: YCSB-A + YCSB-B; add the write-path to all three Rust drivers;
  TigerBeetle YCSB-C via lookup_accounts; document the YCSB-A/B
  TB-incompatibility.
- **T3**: sysbench OLTP read-only / write-only / mixed workloads.
- **T4**: TPC-H Q1 / Q6 (lineitem-only, SF=0.01).
- **T5**: JSON output → BENCHMARKS.md generator (small Rust helper, runs
  locally on Windows or vulcan); arc closure docs (README perf section);
  honest-loss callouts.
- **T6**: quiet-vulcan final sweep (run all workloads × all DBs × 3 trials
  with no other workload competing for CPU/IO); headline numbers for the
  README.

---

### 9. Architecture — the harness

**Crate location:** `tools/bench-compare/` — **OUTSIDE the workspace**.

The default `cargo build` for KesselDB does NOT see this crate. Workspace
`[members]` does not list it. The default `cargo tree -p kesseldb-server
--no-default-features` shows no rusqlite, no tokio-postgres, no
tigerbeetle-rs deps. Zero impact on the production binary.

Compile it explicitly:
```bash
cd tools/bench-compare && cargo build --release
```

**Driver shape** (one trait, four impls):
```rust
#[async_trait]
trait BenchDriver {
    async fn setup(&mut self, scale: Scale) -> Result<()>;
    async fn load(&mut self, scale: Scale) -> Result<()>;
    async fn workload_step(&self, op: BenchOp) -> Result<()>;
    async fn teardown(&mut self) -> Result<()>;
    fn name(&self) -> &'static str;
}
```

Drivers:
- `KesselDriver` — in-process via the kessel-sm StateMachine (same pattern as
  existing kessel-bench `mem` mode); links workspace crates by relative path
  `../../crates/kessel-sm`.
- `PostgresDriver` — tokio-postgres against `127.0.0.1:5533` (the docker
  bench-pg container).
- `SqliteDriver` — rusqlite with `bundled` feature; on-disk file in /tmp.
- `TigerBeetleDriver` — T1 ships a stub that reports `unsupported` for all
  workloads; T2 wires the real lookup_accounts impl for YCSB-C.

**CLI shape:**
```
bench-compare \
  --db kesseldb,postgres,sqlite,tigerbeetle \
  --workload ycsb-c \
  --connections 1,8,16 \
  --duration 10 \
  --rows 100000 \
  --output /tmp/bench-results.json
```

**Output JSON** (one line per (db, workload, N, trial)):
```json
{"db":"kesseldb","workload":"ycsb-c","N":8,"trial":1,
 "ops_per_sec":4823171,"p50_us":1,"p99_us":3,"p99_99_us":18,
 "runtime_secs":10.04,"rows":100000}
```

---

### 10. Critical invariants

- All prior workspace tests pass (seed-7 = `cargo test --release -- --test-threads=1 seed_7`)
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched
- Workspace default-build byte-identical (tools/ crate OUTSIDE the workspace)
- KesselDB still has zero external runtime deps in the production binary
- `#![forbid(unsafe_code)]` honored in tools/bench-compare/

---

### 11. Honest progress estimate

- T1 (this slice): install + scaffold + YCSB-C = ~1.5 sessions
- T2 (YCSB-A + YCSB-B + TB-real): ~1 session
- T3 (sysbench OLTP): ~1 session
- T4 (TPC-H Q1/Q6): ~1 session
- T5 (generator + arc-closure docs): ~0.5 session
- T6 (quiet-vulcan final sweep): ~0.5 session

Total Bench-suite arc: ~5-6 sessions to publishable BENCHMARKS.md v1.

---
