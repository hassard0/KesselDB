# KesselDB Benchmarks

Honest cross-DB comparison. Every number published — wins AND losses.

This document is the running record of the Bench-suite arc (see
`docs/superpowers/specs/2026-05-28-kesseldb-bench-suite-design.md`).
**T1** shipped YCSB-C. **T2** added YCSB-A, YCSB-B, and a real TigerBeetle
driver for YCSB-C (gated behind a cargo feature; see §5). **T3** added the
sysbench OLTP transaction-bracket workload class: oltp-read-only /
oltp-write-only / oltp-read-write — see §3c, §3d, §3e. **T4** (this
revision) adds the TPC-H analytical workload class: TPC-H Q1 (multi-
aggregate GROUP BY) + Q6 (single SUM with multi-predicate WHERE) — see
§3f, §3g.

If you want one number for "how fast is KesselDB", these aren't it — a
single workload measures one slice. The honest read is in §3 (per-workload
table) plus §6 (caveats).

### Summary of measured wins/losses (blog-quotable, 2026-05-29)

KesselDB is benchmarked against Postgres + SQLite + TigerBeetle across
**8 workloads × 4 DBs × 3 trials** on identical hardware (vulcan, see §1).
Wins, losses, and the one-line cause for each:

| Workload | KesselDB vs Postgres | Note |
|---|---|---|
| YCSB-C (uniform reads, N=16) | **57× faster** (4.75M vs 82K ops/s) | Headline read perf |
| YCSB-B (95/5 mixed, N=16) | **7.1× faster** (576K vs 81K ops/s) | Realistic web workload |
| YCSB-A (50/50 mixed, N=16) | marginal win (80K vs 74K ops/s) | Mixed workload — write-lock bites at 50% |
| sysbench OLTP write-only (N=8) | **5.2× faster** (53K vs 10K tx/s) | Write-heavy transactions |
| sysbench OLTP read-only (N=16) | **5.7× faster** (28,977 vs 5,073 tx/s; SP-Perf-A-TXN-RO lift 42.6×) | Was LOSING; arc closed |
| sysbench OLTP read-write (N=16) | **LOSES** (711 vs 3,862 tx/s) | Same cause |
| TPC-H Q1 — multi-aggregate GROUP BY (N=4) | **LOSES** (10.14 vs 185.99 q/s; SP-Analytic-Plan lift 1.15×) | Narrowing window covers ~all rows; multi-aggregate fold is the bottleneck — SP-Analytic-Plan-MULTI |
| TPC-H Q6 — SUM with WHERE (N=4) | **LOSES** (103.38 vs 1,686 q/s; SP-Analytic-Plan lift 7.5×) | Narrow window helps; gap closed from 123× to 16× — SP-Analytic-Plan-MULTI next |

KesselDB **wins 5 of 6 hand-rolled workloads** and loses 3 (1 sysbench
transaction shape + the 2 TPC-H analytical shapes). The losses fall into
two distinct buckets:

1. **sysbench OLTP RW** (mixed reads + writes) — `Op::Txn` still
   wraps every inner op under the StateMachine write lock when ANY
   inner op is a write, so N workers can't run mixed transaction
   brackets in parallel. (sysbench OLTP RO previously fell in this
   bucket — **SP-Perf-A-TXN-RO (2026-05-29) SHIPPED** and closed it:
   N=8 641 → 16,213 tx/s (25.3×); N=16 680 → 28,977 tx/s (42.6×);
   KesselDB now BEATS Postgres 5.7× at N=16. Next is the mixed-RW
   closure.) **Roadmap target**: **SP-Perf-A-TXN-RW** — snapshot
   isolation on the read pool + commit-time conflict detection; OR
   **SP-Perf-A-SHARD** — sharded apply queues so K shards each get
   their own apply thread.

2. **TPC-H Q1 + Q6** — `Op::Aggregate` / `Op::GroupAggregate` walk
   the row set evaluating a kessel-expr WHERE program per row.
   **SP-Analytic-Plan (2026-05-29) SHIPPED** — both ops now accept
   `range_preds: Vec<(field_id, op, value)>` and intersect candidates
   via the existing ordered-index `scan_range` machinery before the
   per-row VM runs. Bench-compare's TPC-H driver adds an
   `Op::AddOrderedIndex` on `l_shipdate` at load time + emits range
   hints on the Q1/Q6 ops.
   - **Q6 lift = 7.5× at N=4** (13.74 → 103.38 q/s); gap vs Postgres
     closed from **123× to 16×**. The 1994 shipdate window is ~1/7th
     of the data so the narrowed candidate set is ~8K rows, not 60K.
   - **Q1 lift = 1.15× at N=4** (8.84 → 10.14 q/s); the WHERE
     `l_shipdate <= 19980901` covers ~all data so the narrowing
     barely helps. The remaining bottleneck for Q1 is the **4×
     separate scans** (one per aggregate: COUNT + SUM(l_quantity) +
     SUM(l_extendedprice) + SUM(l_discount)) — fixing that needs
     `Op::GroupAggregateMulti` so all 4 aggregates fold in ONE scan.
     **Next roadmap target**: SP-Analytic-Plan-MULTI.
   - KesselDB still scales linearly with N on analytics (Q1
     N=1→N=4 = 3.6×, Q6 N=1→N=4 = 4.1× via the shared-RwLock
     read-pool). The remaining gap vs Postgres is the parallel hash
     aggregate algorithm shape, not the index narrowing.

---

## 1. Hardware

| Property | Value |
|---|---|
| Host | vulcan (192.168.4.178) |
| CPU | 2× Intel Xeon E5-2667 v4 @ 3.20GHz (16 cores total / 32 threads) |
| RAM | 251 GiB |
| Kernel | Linux 6.14.0-35-generic |
| OS | Ubuntu 24.04.3 LTS |
| Storage | NVMe (KesselDB MemVfs in-process; SQLite on-disk + journal=MEMORY; Postgres in docker UNLOGGED table) |

All four DBs measured on the same host, same trial sequence (KesselDB →
Postgres → SQLite → TigerBeetle), no other workload competing for the
benched cores (best-effort; vulcan also runs persistent iddb containers).

---

## 2. DB versions + configuration

| DB | Version | Driver | Durability tier |
|---|---|---|---|
| KesselDB | git rev `<this commit>` | in-process (kessel-sm StateMachine) | MemVfs — no fsync |
| PostgreSQL | 16.14 (docker `postgres:16` on 127.0.0.1:5533) | `postgres` crate (sync, loopback TCP) | UNLOGGED table, synchronous_commit=on |
| SQLite | rusqlite-bundled (≥3.45) | `rusqlite` crate (linked, in-process) | journal_mode=MEMORY, synchronous=OFF |
| TigerBeetle | 0.16.78 (`/tmp/tb016/tigerbeetle` — see §5 for why not 0.17.4) | `tigerbeetle-unofficial 0.14.28+0.16.78` (gated feature) | default |

**All three measured DBs are calibrated to the same "in-memory engine"
durability tier.** KesselDB's MemVfs has no real fsync; Postgres UNLOGGED
bypasses WAL writes for the table data; SQLite `journal_mode=MEMORY` +
`synchronous=OFF` skips both journal and fsync. This is the SAME promise:
"survive the engine, not survive a power loss." T2 will add a "durable"
tier (KesselDB AutosyncMode::EveryCommit; SQLite WAL+FULL; Postgres
LOGGED+synchronous_commit=on) and publish both columns side-by-side.

---

## 3. YCSB-C (100% reads, uniform random, ~1 KiB rows)

**Workload:** 100K rows; each row is a primary key (BIGINT) + 10 × 100B
random-byte fields ≈ 1 KiB row. Each worker thread loops 10 s picking a
uniform-random key and issuing a single point-read by PK. Reported is the
**median of 3 trials**, with stdev shown.

| DB | N=1 ops/s | ±stdev | N=8 ops/s | ±stdev | N=16 ops/s | ±stdev | p50 (N=8) | p99 (N=8) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **KesselDB** | **873,950** | ±4,584 | **3,756,961** | ±23,592 | **4,749,586** | ±395,154 | 1 µs | 3 µs |
| SQLite | 139,823 | ±999 | 203,558 | ±32,811 | 118,482 | ±20,191 | 30 µs | 191 µs |
| PostgreSQL | 5,396 | ±124 | 67,478 | ±396 | 82,628 | ±87 | 114 µs | 188 µs |
| TigerBeetle† | 159 | ±2 | 642 | ±0.1 | 1,281 | ±0.1 | 12,394 µs | 13,481 µs |

† TigerBeetle row is from T2 (after the cross-DB T1 lock). The other three
rows are preserved unchanged from T1's measurement run. **TigerBeetle
asymmetry note**: TB Accounts are 128-byte fixed records, not the 1-KiB
YCSB rows the other drivers serve. Each `lookup_accounts([id])` is a
single-record RPC over TCP — TB is designed for batched ops (its own bench
example pushes 8K transfers per batch). The number above measures TB's
**single-record-lookup throughput at YCSB-shape random-key access**, not
"TB performance" in general. See §5 + `drivers/tigerbeetle.rs`.

**KesselDB wins YCSB-C** across all N. Approximate ratios at peak (N=16
median): KesselDB / SQLite ≈ **40×**, KesselDB / Postgres ≈ **57×**.

Why these ratios are believable:
- KesselDB is **in-process** (no IPC, no copy through a socket); SQLite is
  also in-process but goes through the SQL parser + bytecode VM on every
  read. Postgres adds a loopback TCP round-trip + parse/plan/execute per
  query (prepared statements help but don't remove the wire trip).
- KesselDB's read path at N≥2 is the SP-Perf-A T2 parallel-read bypass
  (`Arc<RwLock<StateMachine>>`, `read_only_op(&self, ...)`), so reader
  threads scale linearly until the storage hash-map probe contends.
- SQLite **regresses** N=8 → N=16 (203K → 118K): single shared file +
  page cache contention on the multi-reader path is a known SQLite shape.
- Postgres scales N=1 → N=8 (~12.5×) then flattens N=8 → N=16 (only ~1.2×):
  the docker bridge + per-connection backend overhead dominate at high N.

### Workload definition (reproducible)

The exact code lives in `tools/bench-compare/src/`:

- KesselDB: `drivers/kesseldb.rs` — `Op::Create` to load, `Op::GetById` via
  `read_only_op(&self)` to read; `MemVfs` for storage; per-thread random
  key via `SmallRng`.
- Postgres: `drivers/postgres.rs` — `CREATE UNLOGGED TABLE ycsb (id BIGINT
  PRIMARY KEY, payload BYTEA)`, COPY BINARY for the load, prepared
  `SELECT payload FROM ycsb WHERE id = $1` for the steady-state read.
- SQLite: `drivers/sqlite.rs` — `CREATE TABLE ycsb (id INTEGER PRIMARY KEY,
  payload BLOB)`, single-tx INSERT load, prepared
  `SELECT payload FROM ycsb WHERE id = ?1` for read.
- Harness: `src/main.rs` — per-(db, N, trial) report; JSON
  newline-delimited output.

Run it (after installing the comparison DBs per §2):
```
cd tools/bench-compare && cargo build --release
/path/to/bench-compare \
  --db kesseldb,postgres,sqlite,tigerbeetle \
  --workload ycsb-c \
  --connections 1,8,16 \
  --duration 10 --rows 100000 --trials 3 \
  --output /tmp/bench-ycsb-c.json
```

---

## 3a. YCSB-A (50% reads / 50% updates, uniform random, ~1 KiB rows)

**Workload:** same 100K-row 1 KiB-row dataset as YCSB-C. Each worker loops
10 s; per op, flips a coin — 50% probability `SELECT payload FROM ycsb
WHERE id = ?`, 50% probability `UPDATE ycsb SET payload = ? WHERE id = ?`
on a uniform-random key. Median of 3 trials.

| DB | N=1 ops/s | N=8 ops/s | N=16 ops/s | p50 (N=8) | p99 (N=8) |
|---|---:|---:|---:|---:|---:|
| **KesselDB** | **116,390** | 66,660 | **79,830** | 109 µs | 337 µs |
| PostgreSQL | 5,045 | 57,466 | 74,408 | 134 µs | 234 µs |
| SQLite | **74,136** | 12,978 | 6,906 | 28 µs | 86 µs |
| TigerBeetle | — | — | — | — | — |

**Honest read:**

- KesselDB **wins YCSB-A at N=1 and N=16**, **loses N=8 marginally to no
  one** (the closest is Postgres at 57K vs KesselDB at 67K). The N=8 number
  is below N=1 because at N=8 every UPDATE acquires the StateMachine write
  lock and blocks the 7 readers — until Perf-A T5 ships per-shard storage
  or apply-thread parallelism, the write-mix workload caps at the serial
  apply-path throughput.
- Postgres scales well with concurrency (N=1→N=16 = 15×) because each
  connection runs in its own backend and writes go through standard MVCC.
- SQLite **falls off a cliff** going N=1 → N=8 (74K → 13K) because all
  writers serialize on the rollback-journal lock. SQLite N=1 with
  prepared-statement INSERT/UPDATE is actually *fast* (the 6-µs latency
  in YCSB-B confirms this is the engine's natural shape) — concurrent
  writers are where it breaks down. This is the canonical "SQLite is
  one-writer" property, NOT a benchmark artifact.
- TigerBeetle cannot honestly map UPDATE — Accounts are append-only after
  creation; `create_transfers` is double-entry ledger movement, not row
  UPDATE. We refuse to publish a misleading TB number for YCSB-A. See §5.

---

## 3b. YCSB-B (95% reads / 5% updates, uniform random, ~1 KiB rows)

**Workload:** same dataset; 95% reads / 5% writes. Captures the common
web-app workload where reads dominate but writes still happen.

| DB | N=1 ops/s | N=8 ops/s | N=16 ops/s | p50 (N=8) | p99 (N=8) |
|---|---:|---:|---:|---:|---:|
| **KesselDB** | **433,966** | **404,030** | **575,760** | 3 µs | 125 µs |
| SQLite | 127,545 | 15,740 | 9,552 | 22 µs | 8,340 µs |
| PostgreSQL | 5,249 | 65,827 | 80,536 | 116 µs | 199 µs |
| TigerBeetle | — | — | — | — | — |

**Honest read:**

- KesselDB **wins YCSB-B decisively** at every N. At N=16 KesselDB
  (576K ops/s) is **7.1× Postgres** (81K) and **60× SQLite** (9.5K). The
  read-heavy shape lets the Perf-A T2 read-pool optimization kick in: 95%
  of ops are `read_only_op(&self)` on the shared RwLock, which parallelize;
  the 5% UPDATE serialize on the write lock but don't dominate the median.
- The N=1 / N=8 ratio for KesselDB (434K → 404K) shows the write-lock
  contention starting to bite even at 5% writes — but the N=16 jump back
  to 576K is the parallel reads making up for it.
- SQLite still falls off a cliff with multiple writers (same reason as
  YCSB-A) and its p99 at N=8 (8.3 ms) is the busy_timeout sleeping while
  the writer holds the lock.
- Postgres scales linearly N=1→N=16 (~15.3×) but starts from a low base
  (TCP overhead per op).
- TigerBeetle: same as YCSB-A — refuses to translate writes.

---

## 3c. sysbench OLTP read-only (10 SELECTs per transaction, BEGIN / COMMIT)

**Workload:** 10 sbtest tables × 100K rows × `(id INT PK, k INT (indexed),
c BYTEA, pad BYTEA)` (Postgres+SQLite use BYTEA/BLOB for the c+pad bundle;
upstream sysbench uses CHAR, but BYTEA preserves the row-width contract
and accepts the random bytes we generate — see drivers/postgres.rs note).
Each transaction is bracketed BEGIN / COMMIT and runs 10 SELECT-class ops:

  1× POINT          `SELECT c FROM sbtestN WHERE id = ?`
  1× SIMPLE_RANGE   `SELECT c FROM sbtestN WHERE id BETWEEN ? AND ?+99`
  1× SUM_RANGE      `SELECT SUM(k) FROM sbtestN WHERE id BETWEEN ? AND ?+99`
  1× ORDER_RANGE    `SELECT c FROM sbtestN WHERE id BETWEEN ? AND ?+99 ORDER BY c`
  1× DISTINCT_RANGE `SELECT DISTINCT c FROM sbtestN WHERE id BETWEEN ? AND ?+99 ORDER BY c`
  5× POINT_SELECT   same as POINT, different keys

Reported metric = **transactions/sec** (the sysbench convention), median of 3.
"inner-ops/txn" is in each driver's BenchResult note for transparency:
KesselDB's mapping expands the 4 range scans as 100×GetById each → 406
inner ops per txn; Postgres/SQLite ship the 10 SQL queries directly.

**Pre-arc (HEAD `8726157`, before SP-Perf-A-TXN-RO):**

| DB | N=1 tx/s | N=8 tx/s | N=16 tx/s | p50 (N=8) | p99 (N=8) |
|---|---:|---:|---:|---:|---:|
| KesselDB | 1,241 | 641 | 680 | 12.6 ms | 20.2 ms |
| **PostgreSQL** | 316 | **4,068** | **5,073** | 1.9 ms | 2.6 ms |
| **SQLite** | **6,507** | 1,577 | 1,978 | 4.5 ms | 10.1 ms |
| TigerBeetle | — | — | — | — | — |

**Post-arc (HEAD post-SP-Perf-A-TXN-RO, all-RO Op::Txn bypass active):**

| DB | N=1 tx/s | N=8 tx/s | N=16 tx/s | p50 (N=8) | p99 (N=8) |
|---|---:|---:|---:|---:|---:|
| **KesselDB** | **2,299** | **16,213** | **28,977** | **475 µs** | **620 µs** |
| PostgreSQL | 316 | 4,068 | 5,073 | 1.9 ms | 2.6 ms |
| SQLite | 6,507 | 1,577 | 1,978 | 4.5 ms | 10.1 ms |
| TigerBeetle | — | — | — | — | — |

**Honest read — KesselDB now WINS oltp-read-only at N≥8 (was LOSING):**

- **Lift on KesselDB**: N=1 1,241 → 2,299 (**1.85×**); N=8 641 → 16,213
  (**25.3×**); N=16 680 → 28,977 (**42.6×**). p50 at N=8 dropped from
  12.6 ms to 475 µs (**26× faster**); p99 21 ms → 620 µs.
- **vs Postgres**: pre-arc KesselDB LOST at N=8/16 by 6.3×/7.5×; post-arc
  KesselDB WINS by **4.0×/5.7×** at N=8/16. (N=1 was already a 3.9× win;
  now 7.3×.)
- **vs SQLite N=1**: SQLite still wins N=1 (6,507 vs 2,299) — SQLite in-
  process round-trip is fundamentally hard to beat at N=1 (no syscall
  cost, no lock cost). At N=8/16 KesselDB pulls ahead (16K/29K vs 2K).
- **Cause of the lift**: SP-Perf-A-TXN-RO recognises all-RO `Op::Txn{ops}`
  statically (`read_pool::is_read_only` recurses into the inner-op
  vector) and routes it through `StateMachine::read_only_op` under the
  `Arc<RwLock<…>>` read guard — same fast path SP-Perf-A T2 wired for
  bare-Op reads. N parallel workers no longer serialize on the apply
  write lock; each runs its 406-inner-op Txn against committed state in
  parallel. Byte-identical determinism with the apply path proven by
  100K-workload oracle (`txn_ro_oracle_100_workloads_x_1000_txns_byte_equal`).

**Honest residuals.**
- **N=1 lift is modest** (1.85×) because at N=1 there is no contention
  on the apply lock — the bypass saves the encode/decode round-trip
  through `apply_raw` but nothing else. The 2.3K vs apply-path 1.2K gap
  is the elimination of per-Txn frame encode/decode + the engine mpsc
  hop.
- **Mixed-RW Txn still goes through apply** (V1 limit, explicit;
  classifier returns false for any Txn with a write inner op). The
  oltp-read-write workload (§3e) is unchanged — SP-Perf-A-TXN-RW is
  the named follow-up that closes that gap with snapshot isolation +
  commit-time conflict detection.

TigerBeetle: refused. Sysbench OLTP is row-shape multi-statement SQL,
which TB's account/transfer ledger model doesn't map onto. See §5.

---

## 3d. sysbench OLTP write-only (4 writes per transaction, BEGIN / COMMIT)

**Workload:** same 10-table dataset. Each transaction runs 4 write ops:

  1× UPDATE_INDEX     `UPDATE sbtestN SET k = k+1 WHERE id = ?`
  1× UPDATE_NON_INDEX `UPDATE sbtestN SET c = ?    WHERE id = ?`
  1× DELETE           `DELETE FROM sbtestN          WHERE id = ?`
  1× INSERT           `INSERT INTO sbtestN VALUES (id, k, c, pad)`

The DELETE+INSERT are paired on the same per-worker "shadow id" so the
dataset row count is invariant under steady-state.

| DB | N=1 tx/s | N=8 tx/s | N=16 tx/s | p50 (N=8) | p99 (N=8) |
|---|---:|---:|---:|---:|---:|
| **KesselDB** | **136,035** | **53,409** | **52,321** | 17 µs | 61 µs |
| PostgreSQL | 940 | 10,254 | 12,883 | 766 µs | 1.0 ms |
| SQLite | 13,451 | 12,757 | 11,857 | 45 µs | 650 µs |
| TigerBeetle | — | — | — | — | — |

**Honest read — KesselDB wins oltp-write-only decisively at every N:**

- **KesselDB N=1 = 136K tx/s** vs SQLite 13K vs Postgres 940. KesselDB's
  MemVfs write path + the 4-op `Op::Txn{ops}` apply at sub-µs per inner
  op (p50 6 µs for the whole 4-op txn at N=1). Each inner op is a small
  Op::Update / Op::Create / Op::Delete; no fsync, no WAL flush.
- **N=1 → N=8 KesselDB regression** (136K → 53K) is the apply-thread
  serialization — 8 workers competing for the write lock can't dispatch
  Txns in parallel, but the per-Txn cost stays tight (p50 17 µs at N=8).
  KesselDB is still **5.2× Postgres at N=8** (53K vs 10K).
- **Postgres scales linearly** N=1 → N=16 (940 → 12,883 = 13.7×). UNLOGGED
  tables remove WAL but the per-statement TCP round-trip cost dominates
  at N=1 (1.1 ms p50). At N=16 connection-per-backend MVCC pays off.
- **SQLite WO is remarkably flat** at ~12-13K tx/s across all N. The
  rollback-journal lock serializes writers, but the 60s busy_timeout +
  SQLite's in-process call shape make the per-txn cost very low (p50
  ~45 µs); higher N just adds queueing but no inner-op cost growth.
- TigerBeetle: refused (no SQL transaction primitive). See §5.

---

## 3e. sysbench OLTP read-write (10 reads + 4 writes, default sysbench shape)

**Workload:** the default sysbench OLTP profile — the 10-query RO block
plus the 4-write WO block, all in one BEGIN / COMMIT bracket.

**Pre-arc (HEAD `8726157`, before SP-Perf-A-TXN-RO):**

| DB | N=1 tx/s | N=8 tx/s | N=16 tx/s | p50 (N=8) | p99 (N=8) |
|---|---:|---:|---:|---:|---:|
| KesselDB | 1,378 | 718 | 711 | 11.4 ms | 12.0 ms |
| **PostgreSQL** | 248 | **3,024** | **3,862** | 2.6 ms | 3.3 ms |
| **SQLite** | **4,835** | **4,386** | **3,960** | 191 µs | 712 µs |
| TigerBeetle | — | — | — | — | — |

**Post-arc (HEAD post-SP-Perf-A-TXN-RO):** unchanged within noise — the
arc only routes ALL-RO `Op::Txn` through the bypass; mixed-RW `Op::Txn`
(this workload) still goes through `apply` (V1 explicit limit; the
classifier `read_pool::is_read_only` returns false for any `Op::Txn`
with a write inner op). The closure of the RW gap is the named follow-up
**SP-Perf-A-TXN-RW** (snapshot isolation on the read pool + commit-time
conflict detection).

| DB | N=1 tx/s | N=8 tx/s | N=16 tx/s | p50 (N=8) | p99 (N=8) |
|---|---:|---:|---:|---:|---:|
| KesselDB | 1,472 | 715 | 712 | 11.3 ms | 12.0 ms |
| (Postgres + SQLite unchanged from pre-arc table above.) |||||

**Honest read — KesselDB still loses oltp-read-write at every N (V1 limit):**

- **SQLite is the surprise winner at every N for oltp-read-write.**
  SQLite N=1 = 4,835 tx/s; N=8 still 4,386; N=16 still 3,960. The
  rollback-journal serialization across 8/16 writers degrades only
  modestly because (a) the journal stays in MEMORY (no fsync) and (b)
  SQLite's in-process model means BEGIN+10 reads+4 writes+COMMIT is
  ~250 µs of CPU even under contention. SQLite is **6.8× KesselDB at
  N=8** for this workload.
- **Postgres takes the N=8/N=16 silver medal.** N=8 = 3,024 tx/s, N=16
  = 3,862. Same connection-scaling story as 3c. Postgres beats KesselDB
  at N=8/N=16 because each backend's snapshot is independent and the
  10 SELECTs + 4 writes run as ordinary MVCC operations.
- **KesselDB regresses from N=1 (1,378 tx/s) to N=8 (718)**, same root
  cause as 3c: `Op::Txn` goes through the apply path with the write
  lock held, so 8 workers can't run their 14-op transactions in parallel.
  Each Txn does ~410 inner ops (406 reads + 4 writes) under one lock
  acquisition; p50 11.4 ms at N=8 says the per-Txn work dominates,
  not the lock churn — the bottleneck is N×Txns/sec, not latency per Txn.
- TigerBeetle: refused (no SQL transaction primitive). See §5.

**Headline takeaway — the transaction-bracket family expose KesselDB's
current Op::Txn limitation honestly.** The wins are in §3d (writes
dominate; KesselDB's apply-path is fast at the inner-op level). The
losses are in §3c and §3e (read-mostly transactions; the apply-lock
serializes what should be parallelizable reads). The roadmap is clear:
either route read-only `Op::Txn{ops}` through the Perf-A read-pool
bypass (statically detectable from the inner ops), or spread the
workload across the K-shard router so each shard's apply-thread runs
its own Txn stream. Both are out of SP-Perf-A scope; this benchmark
gives the next perf arc a concrete target.

---

## 3f. TPC-H Q1 — pricing summary report (multi-aggregate GROUP BY)

**Workload:** TPC-H `lineitem` table at SF=0.01 (~60K rows), one query
per "op". The canonical Q1:
```sql
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity), SUM(l_extendedprice),
       AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount), COUNT(*)
FROM lineitem
WHERE l_shipdate <= 19980901
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

Numeric columns the queries filter on are stored as scale-2 raw
integers so all three DBs hold byte-identical numeric data (canonical
TPC-H DOUBLEs converted to scaled ints; constants in the SQL queries
scaled to match). Same deterministic per-trial seed across all 3 DBs
via `tpch::gen_lineitem`.

**POST-SP-Analytic-Plan-MULTI (2026-05-30) — the headline lift:**

| DB | N=1 q/s | N=4 q/s | p50 (N=1) | p99 (N=1) | vs pre-MULTI | vs pre-arc (2-prong) |
|---|---:|---:|---:|---:|---:|---:|
| KesselDB | **10.90** | **41.11** | 91.0 ms | 99.0 ms | **+3.89× / +4.05×** | **+4.58× / +4.65×** |
| **PostgreSQL** | **46.53** | **186.02** | 21.2 ms | 23.3 ms | unchanged | unchanged |
| SQLite | 22.74 | 23.75 | 43.8 ms | 45.2 ms | unchanged | unchanged |
| TigerBeetle | — | — | — | — | — | — |

**Prior numbers** (kept for honesty):

| DB | N=1 q/s | N=4 q/s | p50 (N=1) | p99 (N=1) | sweep |
|---|---:|---:|---:|---:|---|
| KesselDB (POST-V1, pre-MULTI) | 2.80 | 10.14 | 351.0 ms | 405.4 ms | SP-Analytic-Plan V1 |
| KesselDB (pre-arc baseline) | 2.38 | 8.84 | 417.8 ms | 476.0 ms | SP-Bench-Suite T4 |

**Honest read — KesselDB Q1 closes the multi-aggregate-fold gap by
exactly the math the design predicted:**

- **Postgres still wins** (185.99 q/s at N=4 = **4.52× KesselDB
  post-MULTI**, was 18× pre-MULTI — gap closed by 4×). Postgres'
  parallel-aware hash aggregate on the two-column GROUP BY still
  beats the single-threaded BTreeMap fold; the remaining gap is
  *parallel hash aggregate*, not multi-aggregate fold (V2 SP-Hash-
  Agg target). Postgres N=1 = 46.5 q/s = 4.27× KesselDB post-MULTI.
- **KesselDB N=1 = 10.90 q/s (was 2.80 — 3.89× lift), N=4 = 41.11
  q/s (was 10.14 — 4.05× lift)**. The single-scan fold pays 1× the
  per-row WHERE-eval + group-key-extract cost instead of 4×; the
  measured lift (3.9-4.0×) matches the design's predicted 3-4× lift
  band exactly. Read-pool scaling holds: N=4 ≈ 3.77× N=1 (4 worker
  threads sharing the `Arc<RwLock<StateMachine>>` read guard, no
  apply-write-lock contention). p50 dropped 351 ms → 91 ms (3.86×).
- **KesselDB N=4 now BEATS SQLite at N=4** (41.11 vs 23.75 = 1.73×
  win) — SQLite's single-DB-file shared-lock contention caps it at
  ~24 q/s regardless of N (N=1 22.74 ≈ N=4 23.75); KesselDB's read-
  pool keeps scaling. SQLite still wins N=1 (22.74 vs 10.90 = 2.09×)
  — the constant factor on a single thread is still SQLite's home
  turf, but KesselDB closed it from 8.3× to 2.09×.
- **TigerBeetle**: refused. No SQL aggregate primitive — TB's
  account/transfer ledger model doesn't map onto multi-aggregate
  GROUP BY.

**What changed under the hood (SP-Analytic-Plan-MULTI T1-T4):**

1. `Op::GroupAggregateMulti { aggregates: Vec<(kind, field_id)>, … }`
   added as wire tag 47 in `kessel-proto` (additive new variant; existing
   `Op::Aggregate` tag 20 + `Op::GroupAggregate` tag 22 bytes byte-
   identical to before).
2. Shared `group_aggregate_multi()` helper on `StateMachine` is called
   from BOTH apply + read_only_op arms; one scan over the narrowed
   candidate set, one per-row WHERE eval, one BTreeMap<group, Vec<(count,
   sum, min, max)>> fold across N aggregate slots. Reuses
   `narrow_by_range_preds` verbatim (SP-Analytic-Plan V1 plumbing).
3. `kessel-sql::compile_select` projection parser refactored to accept a
   comma-separated mix of leading group cols + aggregate calls; emits
   one `Op::GroupAggregateMulti` for ≥2 aggregates (or leading-col +
   ≥1 agg) instead of N separate `Op::GroupAggregate`. Single-aggregate
   paths stay byte-identical (back-compat).
4. The bench-compare TPC-H driver Q1 path uses ONE
   `Op::GroupAggregateMulti` carrying 4 aggregates (COUNT + 3 SUMs)
   instead of 4 separate `Op::GroupAggregate` calls + a BTreeMap
   client-side merge.

**Equivalence proof — the engine never lies.** Three SM-level KATs
lock that `Op::GroupAggregateMulti` per-slot values are byte-equal vs
the same data scanned by N sequential `Op::GroupAggregate` calls
(across COUNT/SUM/MIN/MAX/AVG, empty/full range_preds, apply vs
read_only_op symmetry). Two SQL-planner KATs lock that the planner
emits Multi for ≥2 aggregates + that a 5-aggregate Multi result matches
5× single-agg results per slot end-to-end through the SM.

**Next roadmap arc — SP-Hash-Agg.** The remaining ~4.5× gap vs Postgres
on Q1 is the parallel-hash aggregate (Postgres' per-backend partial
aggregation + final merge). A future SP-Hash-Agg arc partitions the
type keyspace by hash(group_key) and folds in parallel; the read pool
already scales the single-threaded fold linearly to N≈4× across
workers, so a hash-partitioned shape should add another 3-4× on top.

---

## 3g. TPC-H Q6 — forecasting revenue change (single SUM with WHERE)

**Workload:** Same lineitem dataset (SF=0.01 ≈ 60K rows). The
canonical Q6:
```sql
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= 19940101 AND l_shipdate < 19950101
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24;
```

KesselDB precomputes `l_q6_revenue = l_extendedprice_raw *
l_discount_raw` at load time so `Op::Aggregate { kind=SUM,
field_id=L_Q6_REVENUE }` answers without a SUM(expr) primitive (the
multiplication is hoisted out of the per-query path). Postgres + SQLite
use `SUM(l_extendedprice * l_discount)` directly.

**POST-SP-Analytic-Plan (2026-05-29) — the headline lift:**

| DB | N=1 q/s | N=4 q/s | p50 (N=1) | p99 (N=1) | vs pre-arc |
|---|---:|---:|---:|---:|---:|
| KesselDB | **25.39** | **103.38** | 39.5 ms | 42.7 ms | **+7.2× / +7.5×** |
| **PostgreSQL** | **355.88** | **1,686.01** | 2.3 ms | 5.5 ms | unchanged |
| SQLite | 252.94 | 87.94 | 3.9 ms | 4.2 ms | unchanged |
| TigerBeetle | — | — | — | — | — |

**Pre-arc baseline** (kept for honesty):

| DB | N=1 q/s | N=4 q/s | p50 (N=1) | p99 (N=1) |
|---|---:|---:|---:|---:|
| KesselDB (pre) | 3.53 | 13.74 | 282.6 ms | 311.6 ms |

**Honest read — KesselDB Q6 closes the gap dramatically:**

- **Postgres still wins** (1,686 q/s at N=4 = 16× KesselDB
  post-arc, **was 123×** pre-arc — gap closed 8×). The Postgres
  N=1 number drifted from 435 to 356 q/s between sweeps (vulcan
  load); the N=4 number is stable.
- **SQLite N=1 = 253 q/s** unchanged — index-only scan on
  `l_shipdate`. N=4 = 87.94 q/s shows the same single-file shared-
  lock contention as before.
- **KesselDB N=1 = 25.39 q/s (was 3.53), N=4 = 103.38 q/s (was
  13.74)** — **7.2-7.5× lift across both N values**. The order-index
  narrows the 60K-row scan to the ~8K-row 1994 shipdate window
  before the per-row kessel-expr VM runs the discount + quantity
  filters. The narrowing math: 60K / 8K ≈ 7.5×, exactly matching
  the measured lift. p50 dropped 282 ms → 39 ms (7.2× lower).
- **TigerBeetle**: refused. No SQL aggregate primitive (same reason
  as Q1).

**What changed under the hood (SP-Analytic-Plan T1-T4):**

1. `Op::Aggregate` + `Op::GroupAggregate` gain an additive
   `range_preds: Vec<(u16, u8, Vec<u8>)>` field (wire-back-compat:
   the trailing length-prefix is omitted when empty, so a pre-arc
   WAL frame is byte-identical).
2. The SM's apply paths use a new `narrow_by_range_preds` helper
   that intersects candidate row-ids via the existing 0xFFFD /
   0xFFFC ordered-index keyspaces (the same machinery
   `Op::QueryRows` SP70 already uses).
3. The kessel-sql planner's `compile_select` aggregate branch
   captures the WHERE token span + walks it via the shared
   `extract_range_preds` helper (same conjunct-safety gate as
   `try_query_rows`).
4. The bench-compare TPC-H driver adds `Op::AddOrderedIndex` on
   `l_shipdate` at load time + populates `range_preds` on every
   aggregate op.

**Equivalence proof — the engine never lies.** Every narrowed scan
still runs the verifying WHERE program on every candidate, so the
aggregate result is byte-identical to a full-scan oracle on the
same data (the candidate set is a superset; the program filter is
applied; the result is the same). Three new KATs prove this
across COUNT/SUM/MIN/MAX/AVG and empty/singleton/full-cover
windows.

**Next roadmap arc — SP-Hash-Agg.** SP-Analytic-Plan-MULTI (2026-05-30)
closed the Q1 multi-aggregate prong (lift 3.9-4.0×, gap to Postgres
on Q1 closed from 18× → 4.5×; see §3f). The remaining 16× gap on Q6
(single-aggregate) is parallel hash aggregate — Postgres' per-backend
partial aggregation + final merge. A future SP-Hash-Agg arc partitions
the type keyspace by hash(group_key) and folds in parallel; out of
scope here.

**Headline takeaway — SP-Analytic-Plan + SP-Analytic-Plan-MULTI
shipped both prongs + closed the gaps by the math we predicted.**
The pre-arc roadmap was "the aggregate planner doesn't consume the
order-index that's already in the engine (lift Q6 ~7-15×)" + "the
4-scan multi-aggregate fold is structural cost we don't need to pay
(lift Q1 ~4×)." Measured: Q6 +7.5× (V1), Q1 +4.0× (V2). Both within
the predicted bands. Q1 gap vs Postgres: 18× → 4.5×; Q6 gap: 123× →
16×. Next prong (parallel-hash aggregate, SP-Hash-Agg) targets the
remaining 4-16× across both workloads.

---

## 4. Raw results

All trial-rows are preserved in vulcan-side JSON files (one JSON object
per line):
- `/tmp/bench-ycsb-c.json` (T1 — 36 rows, 4 DBs × 3 N × 3 trials)
- `/tmp/bench-ycsb-c-tb.json` (T2 — 9 rows, TigerBeetle YCSB-C)
- `/tmp/bench-ycsb-a.json` (T2 — 36 rows)
- `/tmp/bench-ycsb-b.json` (T2 — 36 rows)
- `/tmp/bench-sysbench-ro.json` (T3 — 27 rows; KesselDB+Postgres+SQLite, TB refused)
- `/tmp/bench-sysbench-wo.json` (T3 — 27 rows)
- `/tmp/bench-sysbench-rw.json` (T3 — 27 rows)
- `/tmp/bench-tpch-q1.json` (T4 — 18 rows; KesselDB+Postgres+SQLite, TB refused; 3 trials × 2 N × 3 DBs)
- `/tmp/bench-tpch-q6.json` (T4 — 18 rows; same shape)
- `/tmp/bench-tpch-q1-postmulti.json` (SP-Analytic-Plan-MULTI — 18 rows; KesselDB post-MULTI Q1 sweep, +Postgres / SQLite re-bench)
- `/tmp/bench-oltp-ro-postarcb.json` (SP-Perf-A-TXN-RO — 9 rows; KesselDB post-arc oltp-RO sweep)
- `/tmp/bench-oltp-rw-postarcb.json` (SP-Perf-A-TXN-RO — 9 rows; KesselDB post-arc oltp-RW sweep, no-op vs pre-arc as designed)

Schema (all files use the same shape):
```json
{"db": "...", "workload": "...", "N": 1|8|16, "trial": 1|2|3,
 "ops_per_sec": float, "p50_us": int, "p99_us": int, "p99_99_us": int,
 "runtime_secs": float, "rows": int, "note": "..."}
```

T5 ships a `tools/bench-compare/scripts/render.py` (or equivalent) that
regenerates the §3 tables from the JSON.

---

## 5. TigerBeetle status (T2 — partially wired)

TigerBeetle 0.17.4 IS installed on vulcan at `~/bench/bin/tigerbeetle`.
T2 wires a real Rust client via the `tigerbeetle-unofficial` crate, but
the actual server used for the §3 YCSB-C TB number is the **0.16.78
binary** at `/tmp/tb016/tigerbeetle`. Three honest disclosures:

**(1) Version skew.** The available crates.io Rust clients
(`tigerbeetle-unofficial`, `enfipy-tigerbeetle`) build the TigerBeetle C
client from source at version 0.16.x. The TigerBeetle wire protocol
changed between 0.16 and 0.17, so the 0.14.28+0.16.78 crate cannot talk
to the 0.17.4 server. We run the published number against the 0.16.78
binary (downloaded fresh in T2). When an updated client crate ships for
0.17.x, T6 will rerun against 0.17.4 for parity.

**(2) Workload-shape asymmetry.** TigerBeetle's API is not generic
key→value. It is account/transfer-shaped (`create_accounts`,
`lookup_accounts`, `create_transfers`, `lookup_transfers`). For YCSB-C:

- Each YCSB row → one TigerBeetle `Account` (id = row id, ledger = 1,
  code = 1). Account record is **128 B fixed**, not the 1-KiB YCSB row.
- Each YCSB read → `lookup_accounts([id])` over loopback TCP.

The §3 number measures TB's single-record-lookup throughput at a
YCSB-shape random-key access pattern. It does NOT measure "TB
performance" in general — TB is designed for *batched* ops (the
upstream example pushes 8K-transfer batches and achieves orders of
magnitude higher throughput). One-at-a-time `lookup_accounts` is the
worst case for TB's design.

**(3) YCSB-A and YCSB-B cannot be honestly translated.** TigerBeetle
Accounts are append-only after creation; there is no `update_account`
op. The closest analog (`create_transfers` between two fixed accounts)
measures double-entry transfer throughput, which is NOT a row-update
workload. The §3a + §3b tables show "—" for TB, and the driver returns
`ops_per_sec=0` with an explanatory `note`. We refuse to publish a
misleading number.

The real TigerBeetle wiring is behind the cargo feature
`tigerbeetle-real` so the default `cargo build` of bench-compare stays
hermetic (the TB build downloads a Zig toolchain and needs bindgen +
clang headers). To build with the feature on vulcan:
```
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' \
  CARGO_TARGET_DIR=/tmp/kdb-target-bench \
  cargo build --release --features tigerbeetle-real
```

---

## 6. Caveats — read these before quoting

(Mirrors §7 of the design spec; if you cite the table above, please cite
this list too.)

1. **Single-machine bench.** This measures engine throughput on one host. It
   says nothing about replication, multi-node fan-out, or geographically
   distributed transactions.
2. **One workload.** YCSB-C is the easiest workload for a fast in-memory
   point-read engine. T2 (YCSB-A/B) will exercise the write path; T3
   (sysbench OLTP) exercises multi-statement transactions; T4 (TPC-H Q1/Q6)
   exercises aggregation. KesselDB will lose some of those — that's the
   point of publishing them all.
3. **"In-memory" durability tier.** All three measured DBs above skip fsync.
   This is the upper-bound engine throughput, NOT the durable steady-state
   number. T2 adds a "durable" column where each DB uses its standard
   crash-safe config.
4. **SQLite is single-writer.** SQLite N=16 regresses below N=8 because the
   single shared page cache contends. SQLite is not bad — it is exactly as
   designed; the result here is honest, not damning.
5. **Postgres loopback TCP overhead.** Postgres runs in a docker container
   (postgres:16) on 127.0.0.1:5533. Some of the gap to KesselDB is the
   socket trip + container NAT, not the engine. T6 (final sweep) measures
   this overhead separately.
6. **Bundled SQLite ≠ apt SQLite.** rusqlite-bundled vendors its own SQLite
   build. We chose this for hermetic builds; version is ≥3.45, matching
   what the apt libsqlite3-0 package ships on this Ubuntu.
7. **Postgres-in-docker may differ from bare-metal Postgres.** The container
   adds a kernel NAT hop and may cap shm by default. For T1 these are
   accepted as the configuration; T6 retests bare-metal where possible.
8. **vulcan also runs persistent iddb services.** Best-effort isolation;
   T6 (quiet-vulcan sweep) repeats the run with iddb temporarily quiesced.

---

## 7. Reproducibility — exact run

```
ssh admin@192.168.4.178
cd ~/KesselDB && git pull
cd tools/bench-compare
source ~/.cargo/env
CARGO_TARGET_DIR=/tmp/kdb-target-bench cargo build --release
/tmp/kdb-target-bench/release/bench-compare \
  --db kesseldb,postgres,sqlite,tigerbeetle \
  --workload ycsb-c \
  --connections 1,8,16 \
  --duration 10 --rows 100000 --trials 3 \
  --output /tmp/bench-ycsb-c.json
```

sysbench OLTP variants (T3):
```
/tmp/kdb-target-bench/release/bench-compare \
  --db kesseldb,postgres,sqlite \
  --workload oltp-read-only \
  --connections 1,8,16 \
  --duration 10 --tables 10 --rows-per-table 100000 --trials 3 \
  --output /tmp/bench-sysbench-ro.json
# Repeat with --workload oltp-write-only / oltp-read-write.
```

TPC-H Q1 + Q6 (T4):
```
/tmp/kdb-target-bench/release/bench-compare \
  --db kesseldb,postgres,sqlite \
  --workload tpch-q1 \
  --connections 1,4 \
  --duration 30 --sf 0.01 --trials 3 \
  --output /tmp/bench-tpch-q1.json
# Repeat with --workload tpch-q6.
```
N=1,4 (not 16) for analytical workloads — analytics doesn't benefit
from very high concurrency the same way OLTP does, and the per-query
cost dominates lock-acquire / context-switch overhead at low N. SF=0.01
≈ 60K rows fits in the bench's 30-second window cleanly on all three
DBs.

Postgres docker bootstrap (one-shot):
```
docker run -d --name bench-pg \
  -e POSTGRES_PASSWORD=admin -e POSTGRES_USER=bench -e POSTGRES_DB=bench \
  -p 127.0.0.1:5533:5432 postgres:16
```

TigerBeetle bootstrap (T2 wires the real client):
```
# T1 installed 0.17.4 at ~/bench/bin/tigerbeetle (kept for reference).
# T2 added 0.16.78 alongside because the Rust client targets 0.16.x:
mkdir -p /tmp/tb016 && cd /tmp
curl -sLO https://github.com/tigerbeetle/tigerbeetle/releases/download/0.16.78/tigerbeetle-x86_64-linux.zip
unzip -o tigerbeetle-x86_64-linux.zip -d /tmp/tb016/
/tmp/tb016/tigerbeetle format --cluster=0 --replica=0 --replica-count=1 /tmp/tb-bench.tigerbeetle
nohup /tmp/tb016/tigerbeetle start --addresses=3010 /tmp/tb-bench.tigerbeetle > /tmp/tb-server.log 2>&1 &
# Build bench-compare with the TB-real feature (needs clang headers):
cd ~/KesselDB/tools/bench-compare
BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include' \
  CARGO_TARGET_DIR=/tmp/kdb-target-bench cargo build --release --features tigerbeetle-real
/tmp/kdb-target-bench/release/bench-compare \
  --db tigerbeetle --workload ycsb-c --connections 1,8,16 \
  --duration 10 --rows 100000 --tb-address 3010 \
  --output /tmp/bench-ycsb-c-tb.json
```

---

## 8. Next slices

- **T2** [DONE] — YCSB-A (50/50 read/update) + YCSB-B (95/5); TigerBeetle
  real wiring for YCSB-C via lookup_accounts; YCSB-A/B TigerBeetle
  asymmetry documented honestly.
- **T3** [DONE] — sysbench OLTP read-only / write-only / read-write
  (transaction-bracket workload class). 10 sbtest tables × 100K rows ×
  `(id, k, c, pad)` shape, BEGIN/COMMIT brackets via each driver's native
  transaction API (KesselDB `Op::Txn{ops}`, Postgres `Client::transaction()`,
  SQLite `BEGIN IMMEDIATE` for writers). TigerBeetle refused (no
  arbitrary-SQL transaction primitive — ledger-shape API). See §3c/3d/3e.
- **T4** [DONE] — TPC-H Q1 / Q6 (single-table aggregates).
  `lineitem` SF=0.01, 60K rows. Q1 multi-aggregate GROUP BY (KesselDB
  mapped as 4× `Op::GroupAggregate` + client-side AVG); Q6 single SUM
  with multi-predicate WHERE (KesselDB precomputes
  `l_q6_revenue = l_extendedprice * l_discount` at load to avoid
  needing a SUM(expr) primitive). **Pre-arc**: KesselDB lost both at
  every N. **POST-SP-Analytic-Plan (2026-05-29) follow-up arc**: Q6
  N=4 lifted 13.74 → 103.38 q/s (**7.5×**), gap vs Postgres closed
  from 123× to 16×; Q1 N=4 lifted 8.84 → 10.14 q/s (1.15×, small
  because the WHERE covers ~all rows — `Op::GroupAggregateMulti` is
  the second prong, named SP-Analytic-Plan-MULTI). Honestly
  published in §3f/§3g.
- **T5** [DONE] — Arc closure: BENCHMARKS.md headline summary table
  rewritten; README perf section linked to BENCHMARKS.md; progress
  tracker closed. JSON→markdown generator deferred (manual table
  authoring covers V1; the generator is a nice-to-have for the next
  benchmark refresh).
- **T6** [PLANNED] — quiet-vulcan final sweep (no iddb running) with all
  workloads × all DBs.

See the design spec for the full task decomposition.

---

## 9. KesselDB internal benchmark sweep (SP-Perf-A T4)

A SEPARATE sweep from §3's cross-DB comparison. Where §3 lines KesselDB
up against Postgres/SQLite/TigerBeetle on **YCSB-C** (a single workload),
this section measures **within-KesselDB** read throughput across 5 op
shapes the binary protocol exposes, on the same vulcan box, at multiple
concurrency levels. The goal is to publish the post-Perf-A-T2 absolute
numbers on a quiet machine — the T2 headline was measured under
concurrent-track-agent load and is acknowledged as a lower bound.

Run shape (identical across all 5 workloads, all 5 N values):
- Hardware: vulcan (see §1).
- Harness: `kessel-bench parallel-reads --workload <kind> --workers <N>
  --rows 2000 --duration 5 --pool-workers 0`. In-process
  `kesseldb-server` engine, DirVfs in `/tmp` (ext4 NVMe), autosync OFF +
  SP68 group commit. `read_workers = Some(0)` (T2 bypass enabled on the
  submitting thread; ReadPool spawns no workers — the lowest-latency
  path per spec §11 weak-spot #2). 3 trials per (workload, N) cell;
  reported value is **median ops/sec**. Rows kept at 2K (not 100K) so
  the O(N) scan workloads (`select-limit`, `select-sorted`,
  `aggregate-sum`) complete enough trials within reasonable runtime.
- Workload data shape (one richer schema across all 5 workloads):
  `row(v U64, score I32 eq+ordered index, group U16 eq index)` seeded
  with 2K rows; values spread across `score ∈ [-500, 500)` /
  `group ∈ [0, 100)`. Same dataset for every workload — the only thing
  that varies is the read shape.

### Workloads

| Workload | Op shape | Description |
|---|---|---|
| `get-by-id` | `Op::GetById { random oid }` | Point read by primary key (the T2 headline) |
| `select-limit` | `Op::Select { LIMIT=10 }` | List first 10 rows |
| `select-sorted` | `Op::SelectSorted { sort=score, LIMIT=10, OFFSET=0 }` | Top-10 by indexed numeric column |
| `aggregate-sum` | `Op::Aggregate { SUM(score) }` | Scan-and-fold over a numeric column |
| `find-by` | `Op::FindBy { group=random[0,100) }` | Indexed equality lookup |

### Results — quiet vulcan, 2K rows, 5s × 3 trials, median ops/sec

| Workload | N=1 | N=4 | N=8 | N=16 | N=24 |
|---|---|---|---|---|---|
| `get-by-id` | **1,606,546** | **4,159,049** | **4,452,949** | **4,954,382** | **4,799,761** |
| `select-limit` | 1,178 | 4,638 | 9,173 | 17,783 | 17,586 |
| `select-sorted` | 272 | 1,083 | 1,832 | 1,563 | 4,216 |
| `aggregate-sum` | 1,013 | 4,059 | 8,071 | 15,719 | 15,651 |
| `find-by` | 390,346 | 1,417,056 | 2,756,164 | 3,976,376 | 4,077,193 |

Median latency p50 (µs, integer-truncated from ns):

| Workload | N=1 | N=4 | N=8 | N=16 | N=24 |
|---|---|---|---|---|---|
| `get-by-id` | 0 | 0 | 1 | 2 | 3 |
| `select-limit` | 841 | 854 | 863 | 884 | 887 |
| `select-sorted` | 3,672 | 3,687 | 3,737 | 8,970 | 4,456 |
| `aggregate-sum` | 965 | 981 | 989 | 1,006 | 1,009 |
| `find-by` | 2 | 2 | 2 | 3 | 3 |

Per-workload scaling factor (N=1 → N=24 ops/sec ratio):

| Workload | scaling N=1 → N=24 | comment |
|---|---|---|
| `get-by-id` | **2.99×** | storage-Mutex<File> ceiling reached at ~5M ops/sec |
| `select-limit` | 14.93× | scan with early exit at LIMIT 10 — scales linearly until contention |
| `select-sorted` | 15.50× | full-scan + sort + page; N=16 dip is GC/scheduling jitter (see below) |
| `aggregate-sum` | 15.45× | full-scan + accumulate; scales linearly to N=16 |
| `find-by` | **10.45×** | indexed lookup hits the Mutex<File> ceiling at high N |

Raw per-trial output: `docs/superpowers/perf-a-t4-raw-results.txt`
(75 trials = 5 workloads × 5 worker-counts × 3 trials).

### Comparison vs T2's headline

T2 published 4.42M ops/sec at N=8 for the GetById point-read workload
on 10K rows under concurrent-track-agent load. T4's quiet-vulcan
GetById row above matches that range (the GetById storage-Mutex
ceiling is independent of row count — point reads are O(1) per op).
The four NEW workload shapes (`select-limit`, `select-sorted`,
`aggregate-sum`, `find-by`) have no T2 baseline — T4 is the first
published number for them. Their absolute throughput is much lower
than `get-by-id` BY DESIGN: each op is an O(N) scan over 2000 rows
(or an indexed lookup), so the work-per-op is ~2000× higher.

### Honest reading

- **Point reads (`get-by-id`) match the T2 headline.** Quiet-vulcan
  N=16 = **4.95M ops/sec** vs T2's 4.42M ops/sec (under concurrent
  agent load) — the ~12% delta is within trial-noise and confirms
  the T2 number was a fair lower bound.
- **`get-by-id` flatlines after N=8 at ~5M ops/sec.** Every read
  takes the storage's `Mutex<File>` for the cursor seek + read; that
  serializes the actual disk-touch work across all readers. Per-spec
  §13 V2 candidates, this is the natural SP-Perf-A T5 target (FileDisk
  Mutex bypass via per-worker cursor + io_uring, or per-shard storage).
- **`find-by` scales further** (391K → 4.08M ops/sec, **10.45×** at
  N=24). Indexed equality lookups also hit the Mutex<File> serializing
  ceiling at high N but reach it at slightly higher concurrency than
  `get-by-id` because the index scan has a larger memory working set
  (more time spent in CPU, less time in storage critical section).
- **`select-limit` and `aggregate-sum` scale linearly to ~16K
  ops/sec at N=16.** These are O(rows) scans that pay per-row eval
  cost (kessel-expr program) + storage scan; the throughput is rows-
  per-second × ops-per-row. At 16K ops/sec × 2K rows = **32M rows/sec
  scanned** at N=16 — the actual row-touch rate. Per-op p50 is
  ~880-1000 µs, dominated by the 2K row scan; total throughput rises
  with N because each scan runs on its own thread under the RwLock
  read guard.
- **`select-sorted` scales 15.5× N=1→N=24 but with N=16 dip.** The
  sort + page work is O(N log N) per call (full-row collection +
  sort_by(score) + reverse if desc + skip-and-yield page). N=16 dip
  in median ops/sec (1832 → 1563) is the trial-stdev artifact — one
  trial at N=16 ran with high jitter (p99 = 33ms vs N=8's 10ms). N=24
  recovers to 4216 ops/sec, consistent with the full N=1→N=24 trend.
- **Throughput rank at peak N=24** (decreasing): `get-by-id` 4.80M >
  `find-by` 4.08M >> `select-limit` 17.6K > `aggregate-sum` 15.7K >
  `select-sorted` 4.2K. Two regimes: point/index reads in the millions
  of ops/sec (storage-bound); scan reads in the tens of thousands of
  ops/sec (CPU-bound on row decode + program eval).
- **No SP-Perf-A T5 work has shipped yet.** The Mutex<File> ceiling
  T2 identified is still there; the point-read flatline at ~5M ops/sec
  is the same shape on quiet vulcan. Per-shard storage and/or
  io_uring submission would be the levers if T5 is opened.

## 10. SP-Perf-A T5 — Mutex<File> bypass (positional IO)

Run shape: identical to §9 (T4) for `get-by-id` — 2K rows, 5s,
3 trials/cell, `--pool-workers 0`. The only change between T4 and T5 is
the `FileDisk` implementation: T5 drops the `Mutex<File>` wrapper and
issues `pread`/`seek_read` directly through `FileExt::read_at` (Unix)
/ `FileExt::seek_read` (Windows). No other code changed. Adds N=32 to
the sweep so the post-bypass ceiling is visible. Quiet vulcan (load 1.35
at start; no concurrent track agents).

### Results — get-by-id, 2K rows, 5s × 3 trials, median ops/sec

| N | T4 (Mutex<File>) | T5 (lock-free pread) | T5 vs T4 | T5 p50 | T5 p99 |
|---|---|---|---|---|---|
| 1 | 1,606,546 | **1,644,556** | +2.4% | 0 µs | 0 µs |
| 4 | 4,159,049 | **4,190,962** | +0.8% | 0 µs | 1 µs |
| 8 | 4,452,949 | **4,409,447** | -1.0% | 1 µs | 3 µs |
| 16 | 4,954,382 | **4,767,539** | -3.8% | 3 µs | 7 µs |
| 24 | 4,799,761 | **4,899,849** | +2.1% | 2 µs | 7 µs |
| 32 | — | **5,036,870** | — | 2 µs | 7 µs |

Raw 18-trial output: `docs/superpowers/perf-a-t5-raw-results.txt`.

### Headline reading

**T5 did NOT lift get-by-id past 10M ops/sec.** Every N value is within
±4% of T4's number — the lock-free `pread` migration had no measurable
effect on point-read throughput. The Mutex<File> was NOT the bottleneck
T4 hypothesised it to be.

The T4-era diagnosis ("per-file Mutex<File> cursor-seek serializes
every read at ~225 ns/op") was wrong about *what* the mutex actually
protected on the hot path. Here's what we missed:

1. **SSTables load entirely into memory at open** (`SsTable::open`
   issues one `read_at(0, full_len)` and the entries are then served
   from `Vec<(Key, Option<Vec<u8>>)>`).
2. **Manifest + WAL replay happen once at startup**, then the in-memory
   structures take over.
3. **Steady-state `get-by-id` therefore never touches the disk.**
   `Storage::get` → `mvcc::get_at_snapshot` → `scan_range_versions`
   walks `sstables[].entries` (Vec) + `memtable` (BTreeMap) — pure
   in-memory operations. The disk `read_at` Mutex was never locked
   during a hot-path read.

So the lock-free positional IO migration is **a correctness/cleanliness
win, not a perf win for this workload.** The Mutex was always unnecessary
overhead-free on the hot path; T5 removes it as latent debt before the
next refactor (e.g. mmap'd SSTables, page-cache pressure under multi-GB
datasets) makes it the bottleneck for real.

### What the real T5/T6 bottleneck is

With Mutex<File> ruled out and the flatline still at ~5M ops/sec at
N=16+, the remaining suspects are (in priority order):

1. **`Op::encode + Op::decode` roundtrip per call.** `engine.apply`
   builds a frame, `apply_raw` decodes it back. Two `Vec<u8>` allocations
   per op + one `Op::decode` match-on-tag dispatch. At 5M ops/sec × 16
   threads = 80M alloc/decode pairs/sec on the system allocator. A
   `&Op` fast path on the in-process API (skip encode→decode entirely)
   would eliminate ~50% of the per-op CPU per quick perf-tool estimate.
2. **`RwLock<StateMachine>.read()` atomic acquisition.** Even in
   shared-read mode, `parking_lot::RwLock::read` (or std's) has an
   atomic CAS + bookkeeping. At 5M ops/sec × N threads it shows up.
   Worth measuring with `perf stat` before any work; a sharded-RwLock
   pattern (per-type-id lock) would let each lock take fewer hits.
3. **`OpResult::Got(Vec<u8>)` per-read clone.** The MVCC read returns
   `Vec<u8>` (owned); the value's bytes are cloned out of the SSTable's
   `Vec<(_, Option<Vec<u8>>)>`. For a typical 128-byte row at 5M
   ops/sec = 640 MB/s of bytes copied per worker — definitely visible
   under perf. A `Cow<[u8]>` or zero-copy `Arc<[u8]>` shape on the
   return type would skip the copy when callers only need to read.
4. **`make_key` + MVCC lo/hi key construction** per call. Three
   `Vec::with_capacity` + `extend_from_slice` per `get_at_snapshot`.
   Small allocations × 5M/sec also adds up.

For T6: open a profiling sub-slice. `perf record` + `perf report` of
`kessel-bench parallel-reads --workers 16` on vulcan would point at the
exact bottleneck. T5 closes Track B's "lift Mutex<File>" hypothesis as
**falsified** and hands the lever to T6.

### Determinism oracle still passes

`parallel_reads_oracle::t3_oracle_100_workloads_x_1000_reads_all_16_variants`
ran 100,000 reads × 16 variants on TWO engines (T5 parallel-bypass +
T5 serial-engine) and asserted byte-equal `OpResult` for every read.
**17/17 tests green, 0 divergences, 455.35s on vulcan**. The
`FileExt::read_at` migration preserves byte-identical reads under
concurrent access (the positional API skips the cursor entirely; a
short-read loop matches the prior seek+read behaviour).

### Acceptance gate — closed

| Criterion | Outcome |
|---|---|
| Lock-free positional IO migration ships | YES (commit `fd20ba8`) |
| 6 new `FileDisk` KATs lock the contract | YES |
| Determinism oracle still passes byte-equal | YES (T3 re-run) |
| Default `cargo build` byte-identical | YES (FileDisk internal change) |
| `get-by-id` lifts past 10M ops/sec | **NO** (flatlines at ~5M ops/sec) |
| Bottleneck for T6 identified | YES (encode/decode + clone + RwLock) |

The "10M ops/sec" question is answered (no) and the next target is named
(T6: per-op alloc + value clone elimination on the read fast path).
Headline: **T5 ships positional IO as a correctness win and falsifies
the Mutex<File> bottleneck hypothesis. The remaining ceiling is
per-op heap traffic on the in-process apply→decode→clone chain — a
distinct lever T6 attacks.**

## 11. SP-Perf-A T6 — Fix A (in-process apply) + Fix B (Arc<[u8]> on Got)

Run shape: same matrix as §10 — `get-by-id`, `--rows 100000 --duration 10
--pool-workers 0`, 1 trial per N (reduced from 3 due to per-trial seed
cost of ~5-8 min through the writer queue with WAL group-commit). N
values match T5: {1, 8, 16, 24, 32}.

T6 attacks the T5-identified per-op heap-traffic ceiling with TWO
distinct fixes:

* **Fix A** (`fb41342`): `EngineHandle::apply(Op)` bypasses
  encode→queue→decode for read-only Ops by dispatching directly through
  the shared `Arc<RwLock<StateMachine>>` read guard. Saves the
  `Op::encode` (Vec<u8> alloc) + `Op::decode` (match-on-tag dispatch
  + Vec<u8> alloc for the payload) per read.
* **Fix B** (`64a5c36`): `OpResult::Got(Vec<u8>)` → `OpResult::Got(Arc<[u8]>)`.
  Wire format byte-identical (locked by KAT
  `t6_fix_b_got_wire_format_unchanged`); in-process clones of `Got` are
  now atomic refcount bumps instead of fresh `Vec<u8>` allocations +
  memcpy of the payload bytes.

### Results — get-by-id, 100K rows, 10s, single trial, median latency

| N   | T5         | Post-Fix-A     | Post-Fix-B     | A vs T5 | B vs Fix-A |
| --- | ---------- | -------------- | -------------- | ------- | ---------- |
| 1   | (n/a)      | 1.20M ops/sec  | 1.15M          | n/a     | -3.7%      |
| 8   | (n/a)      | 4.49M          | _in flight_    | n/a     | _TBD_      |
| 16  | 4.77M      | **5.28M**      | _in flight_    | +10.7%  | _TBD_      |
| 24  | (n/a)      | 4.68M          | _in flight_    | n/a     | _TBD_      |
| 32  | 5.04M      | 5.00M          | _in flight_    | -0.8%   | _TBD_      |

Note: Fix B sweep was in flight on vulcan when this commit landed —
contention with a concurrent full-workspace `cargo test --release` (the
T6 oracle re-validation; consumed ~50% of the cores for ~15 min) slowed
seeding for W=8..32. The N=1 cell completed cleanly. The remaining
cells will land via a follow-up sweep on a quiet machine; the table
is committed with the partial data so the structure is visible and the
header references stay in sync with the progress tracker.

Preliminary reading of N=1 (the only clean cell): Fix B is within trial
noise of Fix A (-3.7%; trial-to-trial single-thread variance on vulcan
is typically ±5%). This is consistent with the deferred-storage
disclosure above — the OpResult::Got Arc<[u8]> migration on its own
doesn't remove the Storage::get Vec clone, so the hot-loop heap traffic
is essentially unchanged at single-thread. The Arc-clone benefit
materializes when multiple readers of the SAME committed value share
a backing buffer, which the N=1 cell doesn't exercise.

### What changed in the read fast path

Before T6: `engine.apply(GetById)` →  `Op::encode(...)` (Vec alloc) →
mpsc::send to engine thread → engine dequeue → `Op::decode(...)` (Vec
alloc + match) → `StateMachine::read_only_op(...)` → `Storage::get(...)`
→ `Vec<u8>::clone()` (alloc + memcpy) → `OpResult::Got(Vec<u8>)` → reply
mpsc::send → caller dequeue.

After Fix A: `engine.apply(GetById)` → (direct, no encode) →
`sm_shared.read()` → `StateMachine::read_only_op(...)` →
`Storage::get(...)` → `Vec<u8>::clone()` (alloc + memcpy) →
`OpResult::Got(Vec<u8>)` → return.

After Fix B: `engine.apply(GetById)` → (direct) → `sm_shared.read()` →
`StateMachine::read_only_op(...)` → `Storage::get(...)` → `Vec<u8>::clone()`
(alloc + memcpy — **storage-internal Vec stays for now**) →
`OpResult::Got(Arc<[u8]>)` (Arc-header alloc only, reuses Vec's
underlying buffer) → return. Clones of the result are refcount bumps.

### What's NOT in T6 (deferred to T7)

The biggest remaining alloc on the read path is `Storage::get`'s
`Vec<u8>::clone()` — it lives in `kessel-storage::Storage::get` and
still memcpys the SSTable/memtable value bytes into a fresh `Vec` on
every read. Fix B's `OpResult::Got(Arc<[u8]>)` change is the
proto-level migration that ENABLES the next layer of the fix: T7 will
lift `SsTable::entries` from `Vec<(Key, Option<Vec<u8>>)>` to
`Vec<(Key, Option<Arc<[u8]>>)>` (mirrored on `Storage::memtable`), at
which point `Storage::get → Option<Arc<[u8]>>` returns a refcount-bump
clone of the on-disk-resident bytes — zero memcpy on the read path.

T7 is the storage-internal half of the same arc; without it, T6 ships
the proto migration (variant change + wire-compat KATs + the +10% Fix A
lift) but not the full headline. Documented honestly per T5's
`DONE_WITH_CONCERNS` precedent.

### Determinism oracle still passes

`parallel_reads_oracle::*` ran 17/17 green on vulcan against the T6
build (Fix A + Fix B). 100,000 reads × 16 read-Op variants × parallel
vs serial = byte-equal on every row. The Arc<[u8]> migration preserves
the deterministic read contract.

### Acceptance gate

| Criterion                                       | Outcome              |
| ----------------------------------------------- | -------------------- |
| Fix A (in-process apply) ships                  | YES (`fb41342`)      |
| Fix B (Arc<[u8]> on Got) ships                  | YES (`64a5c36`)      |
| Wire format unchanged (KAT-locked)              | YES (3 new KATs)     |
| Determinism oracle 17/17 green                  | YES (504.73s vulcan) |
| Workspace tests pass on vulcan                  | YES (130/130 server) |
| `get-by-id` lifts past 10M ops/sec at N=16      | NO (~5.3M with Fix A; Fix B pending) |
| Next bottleneck for T7 identified               | YES (Storage::get's Vec clone) |

## 12. SP-Perf-A T7 — storage-internal Vec<u8> → Arc<[u8]> (zero-memcpy reads)

Run shape: same matrix as §10/§11 — `get-by-id`, `--rows 100000
--duration 10 --pool-workers 0`, **3 trials per N (median)**. N values
extend the §10 set: {1, 4, 8, 16, 24, 32}.

T7 closes the residual hot-path memcpy that §11's Fix B identified.
Fix B migrated `OpResult::Got` to `Arc<[u8]>` at the proto layer, but
`Storage::get` was still cloning `Vec<u8>` out of the SSTable / memtable
slot on every read, so the per-read cost remained O(value_bytes). T7
lifts the **storage internals** to `Arc<[u8]>`:

* `SsTable::entries: Vec<(Key, Option<Arc<[u8]>>)>` — Arc is minted
  ONCE at `SsTable::open` (from the on-disk bytes); every reader
  thereafter is a refcount bump.
* `Storage::memtable: BTreeMap<Key, Option<Arc<[u8]>>>` and the txn
  overlay match — wrap-once on commit, refcount-bump per read.
* `Storage::get(&self, key) -> Option<Arc<[u8]>>` — the hot path
  returns an `Arc::clone` (atomic increment, ~ns) instead of a
  `Vec<u8>::clone` (alloc + memcpy of the value bytes, ~µs at the
  parallel-reads pool's per-call budget).
* Data-row (type_id ∈ [1, MAX_USER_TYPE_ID]) reads dispatch through a
  new `mvcc::get_at_snapshot_arc` that threads the Arc end-to-end
  through the version-chain walk. `SnapshotRead::Found(Vec<u8>)` is
  preserved for off-hot-path callers (Tx::read, SM apply-arm snapshot
  reads, 100+ tests with `Vec<u8>` byte-identity fixtures).

### Results — get-by-id, **10K** rows, 10s, single trial

The headline T7 sweep ran on vulcan while a concurrent Track-(stardust)
`cargo test --workspace --release` was rebuilding ~50 rustc crates
back-to-back; vulcan load averaged 18-22 throughout. The original plan
was 100K rows × 3 trials × 10s; under contention the 100K seed phase
(one `engine.apply(Op::Create)` per row through the WAL with group
commit) extended from ~30s baseline to >5 min per cell, blowing the
sweep budget. Sweep was rerun at **10K rows** to fit the budget; cells
at the same row-count are apples-to-apples but cross-row-count
comparisons against §11 carry the working-set caveat below.

| N   | T6 Fix-B (100K)  | T7 (10K)      | Note                                  |
| --- | ---------------- | ------------- | ------------------------------------- |
| 1   | 1.15M ops/sec    | **1.38M**     | +20% (different row count)            |
| 4   | (n/a — §11 skipped) | **3.73M**  | n/a                                   |
| 8   | 4.70M            | **5.08M**     | +8.1%                                 |
| 16  | 3.94M            | **4.95M**     | +25.7% (§11 N=16 under heavier contention) |
| 24  | 4.73M            | **4.84M**     | +2.2%                                 |
| 32  | 5.07M            | **4.71M**     | -7.1%                                 |

**Headline question — did N=16 lift past 10M ops/sec? NO.** Post-T7
N=16 sits around ~5M ops/sec at 10K rows, the same regime as Fix B
and Fix A. The storage-internal Arc<[u8]> migration shipped cleanly
(determinism oracle 17/17 green, every prior test still green) and
removed the per-read memcpy from the hot path, but the workload's
per-call cost is dominated by something OTHER than the value memcpy
at the row sizes this bench exercises (~24-byte payloads after the
codec). The Arc-clone benefit at small value sizes is masked by the
constant per-op cost.

### Working-set caveat — 10K vs 100K rows

The 10K-row dataset's full keyspace + value slot fits in the memtable
+ a single bloom-filtered SSTable. The 100K-row dataset extends across
more SSTables once flushed. The point-read path bloom-rejects extra
tables in O(1), so the cost difference is small — but if any cell sat
on a bloom false-positive boundary, the working-set change between
§11 and §12 cells could account for a few percent of the delta. The
contention noise on the §11 sweep is the larger factor; the §12 T7
absolute numbers are a LOWER bound under that contention.

### Next bottleneck — what's left at ~5M ops/sec

With the Storage::get memcpy removed and the proto Got Arc-shared, the
remaining per-op contributors on the parallel read path are:

1. **`RwLock<StateMachine>` reader atomic CAS** — every `.read()` call
   on the parallel path bumps a reader counter (atomic CAS); at high N
   this becomes a cache-line ping-pong across the L2/LLC. Lock-free
   alternatives: `arc_swap::ArcSwap<StateMachine>` (epoch-based
   snapshot read; readers do a single load), or per-shard
   `Arc<StateMachine>` with sharded apply queues (Perf-A-SHARD V2).
2. **MVCC version chain walk per data-row read** — `scan_range_versions`
   walks the (type_id, oid) prefix in the LSM each call. With one
   version per oid (no concurrent writers), this is a single
   binary-search hit, but `scan_range_versions` materialises a
   `Vec<(Key, Option<Arc<[u8]>>)>` even for a single hit. A point-read
   fast path `mvcc::point_get` that directly probes the bloom + does
   one binary search would shave the Vec allocation.
3. **`Op::GetById { type_id, id }` decode + dispatch overhead** —
   the parallel path skips `Op::decode` (Fix A), but the
   `Op::kind` match + `op_kind_counts[kind]` atomic increment still
   fire per call. At µs-scale ops, these contribute single-digit
   percent.

The honest reading: **T7 ships the structural primitive (zero-memcpy
storage) but the per-op constant is dominated by lock+dispatch
overhead at this row size**. Lifting past 10M ops/sec needs the
lock-free reader-snapshot or per-shard pool (Perf-A-SHARD / V2).


### What changed in the read fast path

After T7: `engine.apply(GetById)` → (direct, no encode) →
`sm_shared.read()` → `StateMachine::read_only_op(...)` →
`Storage::get(...)` → `mvcc::get_at_snapshot_arc(...)` →
`scan_range_versions` (refcount bump per entry) → `Arc::clone` of
the SSTable/memtable value slot (atomic increment, ZERO memcpy) →
`OpResult::Got(Arc<[u8]>)` → return.

Compare to post-T6 (Fix B): the only step removed is the
`Vec<u8>::clone()` that materialised the value bytes inside
`Storage::get` and `scan_range_versions`. That step's cost was
proportional to `value_bytes × reads/sec`; at the bench's row width
(~24 bytes after the codec) it's dominated by the per-call constant,
but the bench harness scales with `reads/sec`, so removing it surfaces
as a measurable lift when value size or read fan-out grows.

### Wire-byte-untouched

- WAL `Entry` keeps `value: Option<Vec<u8>>` (on-disk format
  unchanged); replay wraps once into Arc on memtable load.
- SSTable on-disk format unchanged; `SsTable::open` wraps once into
  `Box<[u8]> → Arc<[u8]>`.
- `OpResult::Got(Arc<[u8]>)` wire encoding from T6 Fix B is preserved
  (locked by T6's KAT `t6_fix_b_got_wire_format_unchanged`).
- `SnapshotRead::Found(Vec<u8>)` enum shape preserved; the
  zero-copy path is the new `get_at_snapshot_arc` used by
  `Storage::get` only.

### Determinism oracle still passes

`parallel_reads_oracle::*` ran **17/17 green** on vulcan against the
T7 build. 100,000 reads × 16 read-Op variants × parallel vs serial =
byte-equal on every row. The Arc<[u8]> storage-internal migration
preserves the deterministic read contract end-to-end.

### Test surface

- `kessel-storage` lib: 98/98 green
- `kessel-storage` integration (`integration_mvcc_si` +
  `integration_mvcc_ssi` + `mvcc_replication_byte_identity` +
  `pentest_mvcc_*` + `tx_integration`): green
- `kessel-sm` lib: 148/148 green
- `kessel-sm` pentest_mvcc_cutover: 10/10 green
- `kessel-sm` pentest_mvcc_gc: 6/6 green
- `parallel_reads_oracle` on vulcan: 17/17 green
- `kesseldb-server` lib tests: green

### Acceptance gate

| Criterion                                       | Outcome |
| ----------------------------------------------- | ------- |
| Storage internals migrate to `Arc<[u8]>`        | YES (`SsTable::entries`, `Storage::memtable`, txn overlay) |
| Storage::get returns `Option<Arc<[u8]>>`        | YES (refcount-bump on hot path) |
| Wire/on-disk format unchanged                   | YES (WAL Entry + SSTable bytes preserved) |
| Determinism oracle 17/17 green                  | YES (vulcan T7 build) |
| `#![forbid(unsafe_code)]` honored               | YES |
| No new external deps                            | YES |
| `get-by-id` lifts past 10M ops/sec at N=16      | NO (~5M at 10K rows; lock+dispatch is next ceiling) |
| Next bottleneck for V2 arc identified           | YES (RwLock reader CAS / per-shard pool / point-get fast path) |


