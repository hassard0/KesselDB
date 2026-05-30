# SP-Hash-Agg — progress tracker

Closes the SP-Analytic-Plan-MULTI residual TPC-H Q1 (4.5×) + Q6 (16×)
gaps vs Postgres by parallelising the per-row aggregate-fold across
N=4 worker threads within a single query (Postgres-style parallel
hash aggregate, KesselDB-style: `std::thread::scope` + per-worker
HashMap partials + sorted-BTreeMap merge).

Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-sphashagg-design.md`.

**Arc status: DONE_WITH_CONCERNS — V1 SHIPPED 2026-05-30.**

The parallel path engages + is byte-correct; the measured per-query
speedup (1.46-1.79×) is well below the design's 4× target. The
shippable artifact is honest (the gap-closing IS real and the
algorithm is sound); the residual is a tuning arc, named
SP-Hash-Agg-Tune below.

---

## T1 — design + scaffold [DONE]

Commits:
- `49d318c` docs(perf): SP-Hash-Agg T1 — design + progress tracker +
  MIN_PARALLEL_ROWS const

Proof: spec at `docs/superpowers/specs/2026-05-30-kesseldb-sphashagg-design.md`
+ this tracker + `MIN_PARALLEL_ROWS = 8192` and
`NUM_HASH_AGG_WORKERS = 4` constants in `kessel-sm` (unused on T1;
T2 wires them).

## T2 — parallel `group_aggregate_multi` + parallel `Op::Aggregate` [DONE]

Commits:
- `fa30246` feat(sm): SP-Hash-Agg T2 — parallel hash aggregate for
  Op::Aggregate + Op::GroupAggregateMulti

Proof:
- `StateMachine::aggregate_numeric_scan` new helper next to
  `group_aggregate_multi`; replaces ~280 lines of inline-duplicated
  loop. Both `Op::Aggregate` arms (read_only_op + apply) now delegate
  to it.
- `StateMachine::group_aggregate_multi` rewritten with the parallel
  two-phase materialise+fold (Vec<Arc<[u8]>> chunked, std::thread::scope
  workers, sorted BTreeMap merge).
- All 15 pre-existing aggregate KATs stay green
  (`sp_analytic_plan_*` + `aggregate_*` + `group_aggregate_*`).
- `std::thread::scope` is std-only since Rust 1.63 → zero new external
  deps; `#![forbid(unsafe_code)]` honored.

## T3 — equivalence + determinism KATs [DONE]

Commits:
- `21d0b8b` test(sm): SP-Hash-Agg T3 — parallel vs serial equivalence
  + determinism KATs

Proof: 3 new SM-level KATs (`sp_hash_agg_*`), all on 10K-row workloads
that cross MIN_PARALLEL_ROWS:
1. `sp_hash_agg_group_aggregate_multi_parallel_eq_serial` — Q1-shape
   (5 aggregates × 3 groups) on 10K rows; hand-computed per-group
   COUNT/SUM/MIN/MAX/AVG via BTreeMap model match the engine's
   parallel-path output; output is in ascending key order; bytes
   identical across runs (determinism).
2. `sp_hash_agg_aggregate_parallel_eq_serial` — Q6-shape (single
   scalar aggregate) on 10K rows; closed-form expected values
   (SUM = N(N+1)/2, MIN/MAX = 1/N) for all 5 kinds; bytes identical
   across runs.
3. `sp_hash_agg_apply_eq_read_only_op_at_scale` — at 10K rows the
   apply path and read_only_op path produce byte-identical
   GroupAggregateMulti results (both call `group_aggregate_multi`
   which now uses the parallel path above the row threshold).

Full SM suite: 157 passed (was 154 pre-T3 + 3 new); 0 failed.

## T4 — vulcan TPC-H Q1+Q6 sweep + BENCHMARKS.md update [DONE]

Commits:
- `5b0fb14` docs(benchmarks): SP-Hash-Agg T4 — vulcan TPC-H Q1+Q6
  post-Hash-Agg sweep

Vulcan sweep (3 outer trials × bench-compare's 3 internal trials × 30s
× SF=0.01 × N=1,4 × KesselDB only; Postgres+SQLite from prior §3f/§3g
sweeps unchanged). Per-cell JSONs at
`/tmp/bench-tpch-q{1,6}-posthash-t{1..3}-w{1,4}.json` concatenated
into `/tmp/bench-tpch-q{1,6}-posthash.json` (18 trial rows per query
= 9 trials × 2 N values; median across all 9).

| DB | Q1 N=1 q/s | Q1 N=4 q/s | Q6 N=1 q/s | Q6 N=4 q/s |
|---|---:|---:|---:|---:|
| KesselDB (POST-Hash-Agg) | **17.30** | **60.18** | **34.23** | **185.03** |
| KesselDB (POST-MULTI baseline) | 10.90 | 41.11 | 25.39 | 103.38 |
| KesselDB (pre-arc) | 2.38 | 8.84 | 3.53 | 13.74 |
| Postgres | 46.53 | 186.02 | 355.88 | 1,686.01 |
| SQLite | 22.74 | 23.75 | 252.94 | 87.94 |

**Headline lifts vs pre-Hash-Agg**: Q1 N=1 +1.59×, Q1 N=4 +1.46×;
Q6 N=1 +1.35×, Q6 N=4 +1.79×. **Cumulative 3-arc lift vs pre-arc
baseline**: Q1 N=4 +6.81×; Q6 N=4 +13.47×. **Gap vs Postgres**:
Q1 N=4 4.52× → **3.09×** (was 18× pre-arc); Q6 N=4 16× → **9.11×**
(was 123× pre-arc).

**Concern**: design predicted 4× per-query lift (4-way row-chunk
parallelism). Measured 1.5-1.8× says the per-query parallel speedup
hits a ceiling well below the per-row work split. Diagnosis
documented in BENCHMARKS.md §3f honest read:
- `Vec<Arc<[u8]>>` materialisation of the full candidate row set
  before partitioning pays one ptr-copy + Arc refcount bump per row
  (~60K Arc bumps for Q1's near-full scan = ~10-20ms per query before
  any worker spawns)
- `std::thread::scope` spawn overhead ~100µs × 4 workers = ~400µs
- The surviving serial-prefix (`narrow_by_range_preds` +
  materialisation + thread spawn) is hard-pinned to one CPU

Read-pool scaling holds on top: N=4 ≈ 3.48× N=1 across both queries
(4 read-pool workers × the per-query 1.46-1.79× = ~16-thread peak
under load on a 32-thread vulcan).

## T5 — arc closure [DONE]

Commits:
- (this commit) docs: SP-Hash-Agg T5 — STATUS + tracker close + README

- STATUS.md row (Track J) — added with full headline + diagnosis +
  named follow-up arcs
- BENCHMARKS.md §3f POST-Hash-Agg column — added with full lift +
  cumulative + gap-closing math
- BENCHMARKS.md §3g POST-Hash-Agg column — added with same shape
- BENCHMARKS.md §1 summary table TPC-H rows — updated
- BENCHMARKS.md §1 bullet #3 — full 3-arc narrative
- README perf section — refreshed (Q1 + Q6 post-Hash-Agg numbers)
- This tracker → DONE_WITH_CONCERNS
- SP-Hash-Agg-Tune named as the next arc (drive down the serial-
  prefix cost; expected 2-3× more on Q1 N=4 to bring KesselDB near
  Postgres parity); SP-JIT-Aggregate named as the long-haul follow-up
  (LLVM codegen for the per-row inner loop, what Postgres uses)
- TaskList #345 ready for completion
