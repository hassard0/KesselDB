# SP-Perf-A-SHARD — per-CPU sharded apply queues + read pools — Progress tracker

Date created: 2026-05-30
Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-design.md`
Parent: SP-Perf-A T7 closed `DONE_WITH_CONCERNS` with `get-by-id`
flatlining at ~5M ops/sec at N=16 on vulcan. The diagnosis named
`RwLock<StateMachine>` reader CAS ping-pong + per-op lock+dispatch as
the next ceiling. SHARD attacks that ceiling by partitioning the
key space into K shards, each with its own state-machine + read lock.

## Status: SHARD-APPLY DONE (K=N apply path SHIPPED; 3.19× lift at K=8 on vulcan, breaks 10M ops/sec ceiling)

**Update (2026-05-30 — SHARD-APPLY closed)**: V2's K=N apply plumbing
slice SHIPPED. The K=N apply path is wired, KAT-locked, and benchmarked
on vulcan: K=8 = 14.93M ops/sec (3.19× over the SP-Perf-A T7 ~5M
ceiling); K=16 = 16.72M ops/sec (3.57×). The next sub-arcs (SHARD-READ,
SHARD-SCAN, SHARD-XTXN, SHARD-BENCH-full) remain named.

## Historical status (pre-SHARD-APPLY): PAUSED at SHARD-1 DONE

This is a **multi-arc project**, not a single arc. SHARD-1 (this)
ships the design + scaffold. The K=N apply plumbing is the
multi-week core of the project and is named-paused as
`SP-Perf-A-SHARD-APPLY` (V2).

## What SHARD-1 ships

V1 of the SHARD project = "the type signatures + the K=1 regression-
lock so the next 5 sub-arcs have a foundation to build on." After
SHARD-1 lands, the codebase has:

1. `ShardedStateMachine` type with `shard_of_key` + `shard_of_op`
   helpers.
2. `ServerConfig.shard_count: Option<usize>` (default `None` = K=1
   collapse).
3. KAT proving K=1 behaviour matches the SP-Perf-A T7 single-SM
   shape byte-for-byte.
4. The 6-arc named decomposition so the project has handles.

**Out-of-scope (V1 — each is its own arc):**
- K=N apply plumbing (SP-Perf-A-SHARD-APPLY, V2)
- K=N read dispatch (SP-Perf-A-SHARD-READ, V2)
- Cross-shard scan scatter-merge (SP-Perf-A-SHARD-SCAN, V2)
- Cross-shard atomic txns (SP-Perf-A-SHARD-XTXN, V2)
- K=N measured benchmark sweep (SP-Perf-A-SHARD-BENCH, V2)

See design spec §3 + §9 for the full scoping rationale + roadmap.

## Slice plan (mirrors design spec §9)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec (11 sections + 8 weak-spots + 7 locked invariants + 6-arc decomposition) + progress tracker (this file). NO runtime code change. | **DONE** | `f634f07` |
| **T2** | Scaffold: `crates/kesseldb-server/src/sharded_sm.rs` with `ShardedStateMachine<V>`, `shard_of_key` (K=1 short-circuit + K>=2 fxhash-mod), `shard_of_op` (point-data → `Single`, scans/joins/Op::Txn-at-K>=2 → `FanOut`), `read_only_op_k1` (panics on K>=2 — that path is V2 work), local `fxhash_fold` (no new dep), 11 KATs (fxhash determinism + input-distinguishing, K=1 collapse, K=4 deterministic + distributes, K=1 classifications, K=2 point-op deterministic, **headline `shard_k1_matches_unsharded_sm_byte_equal` regression-lock**, K>=2 panic fail-fast, accessors, K=0 rejected). `ServerConfig.shard_count: Option<usize>` field added but NOT wired into `spawn_engine_cfg` (the K=N engine wiring is the SP-Perf-A-SHARD-APPLY arc). Default `cargo build` byte-identical. kesseldb-server lib 148 → 159 tests (+11, 0 regressions); kessel-sim release 3/3 green; `cargo build --workspace` clean. | **DONE** | `d5691a6` |
| **SP-Perf-A-SHARD-APPLY T1** | Per-shard `EngineHandle` scaffold + `route_op` key→shard routing classifier + `spawn_sharded_engine_cfg` (spawns K vanilla sub-engines rooted at `data_dir/shard-<i>/` + a router shell at `data_dir/router/` whose `EngineHandle.sharded = Some(dispatcher)`). `apply_raw`/`apply`/`apply_op` short-circuit through the dispatcher. DDL broadcasts to every shard sequentially (catalog stays byte-identical by deterministic allocator). Scan/Txn/admin route to shard 0 (V1 limitation, named SHARD-SCAN follow-up). 9 routing unit KATs. **DONE.** | `76d5a50` |
| **SP-Perf-A-SHARD-APPLY T2** | 4 end-to-end integration KATs: K=4 16-row write/read round-trip; K=8 64-row round-trip; **HEADLINE K=1/K=4/K=8 byte-equal determinism oracle** on 100-row workload (proves routing is correct end-to-end); K=4 200-row write distribution KAT (every shard receives >= 10/200, sum=200). kesseldb-server lib 159 → 172 tests. **DONE.** | `37371fd` |
| **SP-Perf-A-SHARD-APPLY T3** | `--shard-count N` flag on `kessel-bench parallel-reads` so the same harness measures K=1/2/4/8/16. **DONE.** | `27e3092` |
| **SP-Perf-A-SHARD-APPLY T5** | vulcan YCSB-C sweep: K=baseline 4.68M ops/sec; K=2 7.30M (1.56×); **K=4 11.08M (2.37×, blows past 6M target)**; **K=8 14.93M (3.19×, BREAKS the 10M ceiling — HEADLINE)**; K=16 16.72M (3.57×). Diminishing returns past K=8 suggest routing-layer overhead is the next ceiling — V2 SHARD-READ should help. **DONE.** | (this commit) |
| **SP-Perf-A-SHARD-APPLY T6** | Arc closure: STATUS, BENCHMARKS §13, progress tracker. **DONE.** | (this commit) |
| **SP-Perf-A-SHARD-APPLY-WAL** | V1 reuses each sub-engine's existing per-shard WAL (each sub-engine is a vanilla `StateMachine` with its own `data_dir/shard-<i>/wal`, recovers independently via `StateMachine::open`). The "per-shard WAL fsync contention" concern named in the dispatch doesn't apply because each sub-engine has its OWN WAL on its own data dir — no shared fsync to contend on. **CLOSED by virtue of T1's per-shard sub-engine shape.** | (subsumed) |
| **SP-Perf-A-SHARD-READ** | `read_pool` workers dispatch reads to their shard's read-lock. V1 already enables per-sub-engine `read_workers` so reads go through each sub-engine's existing T6 in-process fast path; SHARD-READ as a separate arc is now about making the OUTER router-shell read pool shard-aware (skipping the dispatcher overhead). **Named, not started; would lift the K=16 1.12× ceiling further.** | Named, not started | — |
| **SP-Perf-A-SHARD-SCAN** | In-process scatter-merge for fan-out scan ops; reuse `scatter_scan` merge contract. **V1 SHARD-APPLY routes scans to shard 0 ONLY — incorrect for spread data.** This arc is the production-correctness fix for scan ops at K>=2. | Named, not started | — |
| **SP-Perf-A-SHARD-XTXN** | Cross-shard atomic txns via XSHARD keyspace 2PC. V1 routes Op::Txn to shard 0 only. | Named, not started | — |
| **SP-Perf-A-SHARD-BENCH** | Multi-workload K=N sweep on vulcan (YCSB-A/B/C, sysbench OLTP, TPC-H). T5 shipped the YCSB-C cell; the full matrix is its own arc. | Named, not started (T5 = YCSB-C only) | — |
| **SP-Perf-A-SHARD-READ** | `read_pool` workers dispatch reads to their shard's read-lock. | Named, not started | — |
| **SP-Perf-A-SHARD-SCAN** | In-process scatter-merge for fan-out scan ops; reuse `scatter_scan` merge contract. | Named, not started | — |
| **SP-Perf-A-SHARD-XTXN** | Cross-shard atomic txns via XSHARD keyspace 2PC. | Named, not started | — |
| **SP-Perf-A-SHARD-BENCH** | Measured K=N vs K=1 on vulcan; closes (or falsifies) the ≥2× lift hypothesis. | Named, not started | — |

## Honest framing

This arc is **substantively harder than the others in the SP-Perf-A
family.** SP-Perf-A T1-T7 each took 1-3 commits and shipped a
measurable lift. SHARD's K=N path requires:

- A per-shard apply thread (touches the engine spawn shape).
- A write routing layer (touches `EngineHandle::apply` /
  `apply_raw`).
- A per-shard WAL or a per-shard slice of the global WAL (touches
  storage internals).
- A scatter-merge layer for scan ops (touches `read_only_op` for
  every scan variant).
- Cross-shard Txn handling (the V1 fallback is correct but slow;
  V2 needs real 2PC).
- A determinism oracle extension that runs at K=N (the current
  oracle is single-shard).
- A benchmark sweep proving the ≥2× lift (or honestly documenting
  failure, T5-style).

The honest deliverable for SHARD-1 is **design + scaffold + a
regression-lock KAT proving K=1 doesn't break.** That's a real
deliverable: it locks the type signatures, names the sub-arcs, and
gives the next 5 arcs concrete file paths to work in. **It does
NOT lift the throughput ceiling — that's SHARD-BENCH's job, which
runs once SHARD-APPLY + SHARD-READ + SHARD-SCAN ship.**

## Acceptance gate — V1 (this arc, SHARD-1) — MET

| Criterion | Outcome |
|---|---|
| Design spec written + 8 weak-spots named | YES (`f634f07`) |
| 6-arc decomposition named with status | YES (`f634f07`) |
| K=1 byte-equal regression-lock KAT passes | YES (`shard_k1_matches_unsharded_sm_byte_equal` green in `d5691a6`) |
| Default `cargo build` byte-identical | YES (`shard_count` defaults to `None`; `ShardedStateMachine` never constructed by `spawn_engine_cfg`) |
| Workspace tests pass on vulcan | YES (kesseldb-server lib 159/159; kessel-sim release 3/3) |
| `#![forbid(unsafe_code)]` honored | YES |
| No new external runtime deps | YES (`fxhash_fold` inline, 8 lines) |

## Acceptance gate — V2 (the multi-arc completion)

| Criterion | Owning arc |
|---|---|
| K=N apply runs per-shard | SHARD-APPLY |
| `get-by-id` N=16 lifts ≥2× over T7 ~5M ops/sec ceiling on vulcan | SHARD-BENCH |
| Scan ops (Select, Aggregate, GroupAggregate) byte-identical at K=N vs K=1 | SHARD-SCAN |
| Cross-shard Op::Txn correct (V1 fallback acceptable; V2 2PC removes the global lock) | SHARD-XTXN |

## Standing invariants (inherited from SP-Perf-A)

- All cargo on vulcan uses `CARGO_TARGET_DIR=/tmp/kdb-target-shard`
  (per-arc target dir).
- Commits straight to main; no Co-Authored-By; no `-S`; push after each.
- Memory files OUTSIDE the repo — NEVER git-add.
- seed-7 GREEN every commit.
- Default tree-grep EMPTY (no new external runtime deps).
- `#![forbid(unsafe_code)]` honored.
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched.

## File registry

- **Spec**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-design.md`
- **Tracker (this file)**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-progress.md`
- **(T2)** Scaffold: `crates/kesseldb-server/src/sharded_sm.rs`
- **(T2)** Config field: `ServerConfig.shard_count` in `crates/kesseldb-server/src/lib.rs`
