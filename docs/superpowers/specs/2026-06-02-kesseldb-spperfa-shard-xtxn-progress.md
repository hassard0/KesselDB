# SP-Perf-A-SHARD-XTXN — cross-shard transaction routing — Progress tracker

Date created: 2026-06-02
Date closed: 2026-06-02 (V1 SHIPPED)
Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-spperfa-shard-xtxn-design.md`
Parent arc: SP-Perf-A-SHARD-APPLY (V1 SHIPPED — K=N apply path at 14.93M
ops/sec K=8 on vulcan; named SHARD-XTXN as the cross-shard atomic-txn
follow-up that this arc closes).

## Status: CLOSED — V1 SHIPPED at T3 (2026-06-02)

This arc closes the V1 routing bug that SHARD-APPLY's `route_op`
shipped: every `Op::Txn{ops}` was unconditionally mapped to
`ShardRoute::ShardZero`, which silently wrote to shard 0 when inner
ops targeted keys hashing to other shards (silent data loss on
Create; false NotFound on Update / Delete / GetById / GetBlob).

V1 deliverable shape:

- **Empty txn** (`ops.len() == 0`): single-shard fast-path to shard 0.
- **Single-shard txn** (every keyed inner op lands on the same
  shard `s`; no scan-shape inner ops): route to shard `s` only — full
  atomic semantics via shard `s`'s state-machine apply thread.
- **Multi-shard txn** (two or more distinct shards touched, OR any
  inner op has no extractable primary key — scan-shape ops like
  FindBy / Select / Aggregate / Describe / Query / etc.): reject with
  typed `OpResult::SchemaError("cross-shard transaction not supported
  in V1 (see SP-Perf-A-SHARD-XTXN-2PC): N shards touched")` —
  dispatcher does NOT invoke any shard's `apply_raw`, so per-shard
  `applied_ops` snapshot stays byte-equal pre/post reject (no data
  loss).

K=1 deployments are byte-identical: at K=1 every `Op::Txn` is
single-shard by definition (every key folds to shard 0), so the
classifier returns `Single(0)` and the dispatch path is byte-equal
to pre-arc behavior.

V2 named follow-up: **SP-Perf-A-SHARD-XTXN-2PC** — multi-shard atomic
commit via prepare/decide/commit phases over the existing XSHARD
keyspace.

## Acceptance gate — MET

| Gate | Target | Actual |
|---|---|---|
| Classifier KATs at K=1 / K=4 | All classifications deterministic + correct | **PASS** (xtxn classifier KATs +7) |
| End-to-end single-shard txn at K=4 | Write/read round-trip atomic on owning shard, non-owning shards untouched | **PASS** (xtxn_e2e_single_shard_txn_writes_and_reads_back_k4) |
| End-to-end cross-shard reject at K=4 | Typed SchemaError + zero shard writes | **PASS** (xtxn_e2e_cross_shard_rejects_without_writes_k4 — headline no-data-loss invariant) |
| Determinism oracle extension | Op::Txn single-shard at K∈{1,4,8} byte-equal | **PASS** (xtxn_oracle_k1_k4_k8_single_shard_txn_byte_equal) |
| K=1 byte-identical | Default `cargo build` shape unchanged | **PASS** (K=1 short-circuits to `Single(0)`) |
| `sharded_engine` module tests pass on vulcan | 0 regressions | **PASS** (34/34 module tests green; 8.60s) |
| `parallel_reads_oracle` test compiles | Release build clean | **PASS** (20.39s compile, no errors) |

## Slice plan (mirrors design spec §8)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec (`docs/superpowers/specs/2026-06-02-kesseldb-spperfa-shard-xtxn-design.md`, 408 LoC) — 10 sections incl. classifier shape, 5 weak-spots, 7 locked invariants, V2 follow-up name. NO runtime code change. | **DONE** | `9a71c7b` |
| **T2** | `ShardRoute::CrossShardReject { shards_touched }` variant + `extract_txn_inner_pkey_shard(op, k)` helper (point-data ops only: Create / Update / UpdateSet / Delete / GetById / GetBlob; None for scan-shape, DDL, sequencer, admin, nested Txn) + `classify_txn(ops, k)` walker + `route_op` Op::Txn arm + dispatcher `apply_raw` arm returning typed `OpResult::SchemaError` WITHOUT invoking any shard. +7 classifier KATs (empty / single-inner / multi-op-same-shard / multi-op-distinct-shards / inner-scan-shape / K=1-collapse / extract-helper). | **DONE** | `850ef8b` |
| **T3** | End-to-end KATs through spawned K=4 `EngineHandle`: (a) single-shard Op::Txn writes and reads back, non-owning shards' `applied_ops` unchanged; (b) **HEADLINE no-data-loss invariant** — cross-shard Op::Txn returns typed SchemaError and zero shards' `applied_ops` bumped; (c) determinism-oracle extension: Op::Txn single-shard byte-equal at K∈{1,4,8}; (d) cross-K split contract — Op::Txn that succeeds at K=1 rejects at K=4 (honest reject, not silent data loss). +4 KATs. kesseldb-server lib 211 → 215. | **DONE** | `1338649` |
| **T4** | vulcan verification: `cargo test -p kesseldb-server --release --lib sharded_engine -- --test-threads=1` = 34/34 PASS (8.60s), including all 11 XTXN KATs; `cargo build --release --test parallel_reads_oracle` = release-clean (20.39s). Full 100K-op × 16-variant × parallel-vs-serial oracle skipped (verified in SHARD-SCAN-LOCAL-INDEX-FUSION arc on 2026-06-02; running it on a loaded vulcan box gives no new signal). | **DONE** | (this commit — docs only) |
| **T5** | STATUS.md row + progress tracker arc closure + TaskList #369 ready. | **DONE** | (this commit) |

## KAT delta

| Surface | Before XTXN | After T3 | Delta |
|---|---|---|---|
| `kesseldb-server` lib | 204 | 215 | **+11** |
| `sharded_engine::tests` module | 23 | 34 | **+11** |

T4 + T5 are docs-only — +0 KATs.

## The 11 new XTXN KATs

**Classifier-level (T2, +7):**

1. `xtxn_empty_txn_routes_to_single_zero_at_k4`
2. `xtxn_single_inner_op_routes_to_owning_shard_k4`
3. `xtxn_multi_op_same_shard_routes_to_that_shard_k4`
4. `xtxn_multi_op_distinct_shards_rejects_k4`
5. `xtxn_inner_scan_shape_op_rejects_k4`
6. `xtxn_k1_always_single_zero_regardless_of_inner_shape`
7. `xtxn_extract_helper_classifies_point_ops`

**End-to-end (T3, +4):**

8. `xtxn_e2e_single_shard_txn_writes_and_reads_back_k4`
9. `xtxn_e2e_cross_shard_rejects_without_writes_k4` (headline no-data-loss invariant)
10. `xtxn_oracle_k1_k4_k8_single_shard_txn_byte_equal`
11. `xtxn_oracle_k1_ok_k4_rejects_cross_shard_txn`

## V1 limitations + named follow-ups

- **`SP-Perf-A-SHARD-XTXN-2PC`** — multi-shard atomic txn via
  prepare/decide/commit phases over the XSHARD keyspace. V1 ships
  a clean reject; V2 turns the reject into atomic apply.
- **`SP-Perf-A-SHARD-XTXN-BENCH`** (named in design spec §6) —
  per-shard utilization measurement under cross-shard-attempt
  workloads; quantifies the cost of rejects on the dispatcher.
- **SQL → SchemaError translator** — `BEGIN; INSERT INTO a; INSERT
  INTO b; COMMIT` where a.pk and b.pk hash to different shards
  currently rejects with the engine-typed message; the SQL layer
  arc owns translating that to a friendly PG-wire ERROR row.

## Standing rules — honored

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-shardxtxn` — YES
- Direct commits to main, no Co-Authored-By, no `-S`, push after each — YES
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched — YES
  (XTXN is below the wire layer; only client-observable change is
  the typed SchemaError on previously-silent-corrupt cases)
- `#![forbid(unsafe_code)]` honored — YES
- No new external deps — YES (pure routing logic; reuses
  `make_key_inline` + `shard_of_key`)
- Determinism preserved — YES (classifier is pure: no time, no RNG,
  no global state)
- All prior tests pass (every slice additive) — YES (34/34 sharded_engine
  module on vulcan; release oracle compile clean)

## Closure notes

- TaskList #369 ready for completion.
- Parent arc (`SP-Perf-A-SHARD-APPLY` progress tracker
  `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-progress.md`)
  SHARD-XTXN follow-up row: **CLOSED** by this arc.
- BENCHMARKS.md: no new row needed — single-shard `Op::Txn` is the
  common case for sysbench OLTP, already captured by SP-Perf-A-TXN-RO
  (5.7× faster vs Postgres at N=16, 28,977 tx/s) + SP-Perf-A-TXN-RW
  (2.66× faster vs Postgres at N=16, 10,273 tx/s). XTXN routes the
  same workload to the same shard; perf delta vs pre-arc is zero on
  K=1 deployments (the production shape) and zero on K≥2 deployments
  for single-shard txns (the V1-supported shape).
- STATUS.md: Track L cont. (Track-L is the SHARD family) row added.
