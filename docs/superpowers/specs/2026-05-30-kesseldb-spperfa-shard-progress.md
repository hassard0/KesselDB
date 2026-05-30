# SP-Perf-A-SHARD — per-CPU sharded apply queues + read pools — Progress tracker

Date created: 2026-05-30
Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-design.md`
Parent: SP-Perf-A T7 closed `DONE_WITH_CONCERNS` with `get-by-id`
flatlining at ~5M ops/sec at N=16 on vulcan. The diagnosis named
`RwLock<StateMachine>` reader CAS ping-pong + per-op lock+dispatch as
the next ceiling. SHARD attacks that ceiling by partitioning the
key space into K shards, each with its own state-machine + read lock.

## Status: OPEN — first slice (design + scaffold + K=1 KAT)

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
| **T1** | Design spec (11 sections + 8 weak-spots + 7 locked invariants + 6-arc decomposition) + progress tracker (this file). NO runtime code change. | **DONE** | (this commit) |
| **T2 (optional)** | Scaffold types: `crates/kesseldb-server/src/sharded_sm.rs` with `ShardedStateMachine<V>`, `shard_of_key`, `shard_of_op`, K=1 collapse. `ServerConfig.shard_count: Option<usize>` defaulting to `None`. KAT `shard_k1_matches_unsharded_sm_byte_equal` runs the determinism oracle's 100×10-op workload against (SM, ShardedSM{vec![SM]}) and asserts byte-equal results for every read. Default `cargo build` byte-identical (no construction unless opted in). | Planned (dispatch attempt) | — |
| **SP-Perf-A-SHARD-APPLY** | K=N apply plumbing: per-shard apply thread, write routing layer, per-shard WAL group-commit. **MULTI-WEEK CORE.** | Named, not started | — |
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

## Acceptance gate — V1 (this arc, SHARD-1)

| Criterion | Outcome |
|---|---|
| Design spec written + 8 weak-spots named | YES (this commit) |
| 6-arc decomposition named with status | YES (this commit) |
| K=1 KAT runs determinism oracle 100×10 byte-equal | (T2-dependent) |
| Default `cargo build` byte-identical | (T2 invariant — `shard_count = None`) |
| Workspace tests pass on vulcan | (T2-dependent) |
| `#![forbid(unsafe_code)]` honored | (T2 invariant) |

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
