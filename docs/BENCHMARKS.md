# KesselDB Benchmarks

Honest cross-DB comparison. Every number published — wins AND losses.

This document is the running record of the Bench-suite arc (see
`docs/superpowers/specs/2026-05-28-kesseldb-bench-suite-design.md`). T1
ships **YCSB-C only**; T2-T4 will add YCSB-A/B, sysbench OLTP, and TPC-H
Q1/Q6 against the same harness, on the same hardware, against the same DBs.

If you want one number for "how fast is KesselDB", these aren't it — a
single workload measures one slice. The honest read is in §3 (per-workload
table) plus §6 (caveats).

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

| DB | Version | Driver | Durability tier (T1) |
|---|---|---|---|
| KesselDB | git rev `<this commit>` | in-process (kessel-sm StateMachine) | MemVfs — no fsync |
| PostgreSQL | 16.14 (docker `postgres:16` on 127.0.0.1:5533) | `postgres` crate (sync, loopback TCP) | UNLOGGED table, synchronous_commit=on |
| SQLite | rusqlite-bundled (≥3.45) | `rusqlite` crate (linked, in-process) | journal_mode=MEMORY, synchronous=OFF |
| TigerBeetle | 0.17.4 (`~/bench/bin/tigerbeetle`) | (T1 stub — see §5) | default |

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
| TigerBeetle | (T1 stub) | — | (T1 stub) | — | (T1 stub) | — | — | — |

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

## 4. Raw results

All 36 trial-rows from the T1 measurement run are preserved in
`/tmp/bench-ycsb-c.json` on vulcan (one JSON object per line). The schema:
```json
{"db": "...", "workload": "...", "N": 1|8|16, "trial": 1|2|3,
 "ops_per_sec": float, "p50_us": int, "p99_us": int, "p99_99_us": int,
 "runtime_secs": float, "rows": int, "note": "..."}
```

T5 ships a `tools/bench-compare/scripts/render.py` (or equivalent) that
regenerates the §3 table from the JSON.

---

## 5. TigerBeetle status (T1 honest disclosure)

TigerBeetle 0.17.4 IS installed on vulcan (~/bench/bin/tigerbeetle, version
verified). T1 ships a **driver stub** that returns 0 ops/sec with a `note`
flagging "deferred to T2". The reason is methodological, not technical:

TigerBeetle's API is not generic key→value. It is account/transfer-shaped
(`create_accounts`, `lookup_accounts`, `create_transfers`,
`lookup_transfers`). For YCSB-C, the honest translation is:

- Each YCSB row → one TigerBeetle `Account` (id = row id, ledger = 1,
  code = 1, flags = 0, debits/credits = 0, user_data fields = first 32 B
  of the row payload).
- Each YCSB read → `lookup_accounts([id])` over the cluster connection.

This is what T2 ships, alongside a clear caveat that YCSB-A/B (which are
50% / 5% UPDATE workloads) do not map cleanly to TigerBeetle's
append-only ledger — there's no "update a row in place" operation. T2
documents that asymmetry honestly and publishes whatever maps, plus a
"could not translate" row for whatever doesn't.

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

Postgres docker bootstrap (one-shot):
```
docker run -d --name bench-pg \
  -e POSTGRES_PASSWORD=admin -e POSTGRES_USER=bench -e POSTGRES_DB=bench \
  -p 127.0.0.1:5533:5432 postgres:16
```

TigerBeetle bootstrap (T1 — installed but the driver wires up in T2):
```
mkdir -p ~/bench/bin && cd ~/bench/bin
curl -sSL https://github.com/tigerbeetle/tigerbeetle/releases/latest/download/tigerbeetle-x86_64-linux.zip -o tb.zip
unzip -o tb.zip && rm tb.zip
./tigerbeetle version    # should print: TigerBeetle version 0.17.4+...
```

---

## 8. Next slices

- **T2** — YCSB-A (50/50 read/update) + YCSB-B (95/5); TigerBeetle real
  wiring for YCSB-C via lookup_accounts; document YCSB-A/B TigerBeetle
  asymmetry honestly.
- **T3** — sysbench OLTP read-only / write-only / mixed.
- **T4** — TPC-H Q1 / Q6 (single-table aggregates).
- **T5** — JSON → markdown generator script; arc closure docs; README perf
  section.
- **T6** — quiet-vulcan final sweep (no iddb running) with all workloads × all DBs.

See the design spec for the full task decomposition.
