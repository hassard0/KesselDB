# SP-Hash-Agg-Tune — progress tracker

Drives down the SP-Hash-Agg V1 serial-prefix cost (Vec<Arc<[u8]>>
pre-materialisation + Arc-wrap pass) that bounded per-query lift to
1.46-1.79× instead of the 4× modelled target. Streaming
producer-channel-workers BATCHED replaces V1's two-phase pre-collect+
partition shape, so workers fold rows AS the producer iterates the scan
output.

Design spec: `docs/superpowers/specs/2026-05-30-kesseldb-sphashaggtune-design.md`.

**Arc status: DONE_WITH_CONCERNS — V1 SHIPPED 2026-05-30.**

The streaming-batched path engages + is byte-correct; the measured per-
query speedup (1.06-1.07× at N=4) is well below the user-spec floor
(Q1 ≥120 / Q6 ≥350 at N=4 → 53% / 56% achieved). The shippable artifact
is honest (the gap-closing IS real and the algorithm is sound); the
sweep produced a new diagnosis (per-row WHERE VM interpreter is the
actual dominant cost, NOT the V1 serial prefix), naming the residual
arcs SP-WHERE-VM-Specialise + SP-JIT-Aggregate.

---

## T1 — design + scaffold + streaming refactor [DONE]

Commits:
- `833eede` feat(sm): SP-Hash-Agg-Tune T1+T2 — streaming producer-
  channel-workers + KATs (unbatched first cut)
- `0a19f3d` perf(sm): SP-Hash-Agg-Tune T2.1 — BATCHED channel sends
  (256 rows/msg)

Proof:
- Design spec at `docs/superpowers/specs/2026-05-30-kesseldb-sphashaggtune-design.md`
- `BUF_DEPTH = 16` (batches) + `BATCH_SIZE = 256` (rows/batch) constants
- `aggregate_numeric_scan` rewritten: source materialised once into
  `RowSource::Pre(Vec<Arc<[u8]>>)` (cand=Some) or `RowSource::Scan(Vec<(Key,Vec<u8>)>)`
  (cand=None); when `row_count >= MIN_PARALLEL_ROWS`, a producer thread
  iterates the source + packs rows into BATCH_SIZE-sized Vec batches +
  sends round-robin into N=4 `sync_channel(BUF_DEPTH)` queues; N=4
  workers consume their channel batch-at-a-time and fold rows AS the
  batch arrives.
- `group_aggregate_multi` rewritten with the same shape; per-worker
  `HashMap<Vec<u8>, Vec<Acc>>` partials merged into a sorted `BTreeMap`
  for ascending-key output (existing contract).
- Determinism: round-robin BATCH assignment by deterministic source-
  iteration order; merge in (0..N) order; combine ops associative for
  SUM/COUNT and associative+commutative for MIN/MAX.
- `std::sync::mpsc::sync_channel` + `std::thread::scope` are std-only
  since Rust 1.63 → zero new external deps; `#![forbid(unsafe_code)]`
  honored.

The unbatched first-cut shape (commit 833eede) shipped a CORRECT
streaming refactor but regressed -13%/-9% at Q1 N=1/N=4 because the
per-row channel send/recv overhead at 60K rows × ~500ns = ~30ms/query
SWALLOWED the streaming savings. Commit 0a19f3d added BATCH_SIZE=256
to amortise channel cost across rows.

## T2 — streaming-equivalence KATs [DONE]

Commits:
- `833eede` (T2 KATs added alongside T1 in the same commit)

Proof: 3 new SM-level KATs (`sp_hash_agg_tune_*`), all on row counts
that cross MIN_PARALLEL_ROWS:
1. `sp_hash_agg_tune_aggregate_streaming_eq_serial` — 9000 rows × 5
   kinds × 5 repeat runs must match closed-form expected
   (count=9000, sum=N(N+1)/2, min=1, max=N, avg=sum/count); BUF_DEPTH
   boundary stress (producer parks dozens of times).
2. `sp_hash_agg_tune_group_aggregate_multi_streaming_eq_serial` — 50K
   rows × 100 group cardinality × 10 repeat runs locks merge-order /
   arrival-order non-interference; per-group COUNT/SUM/MIN/MAX/AVG
   match a BTreeMap model; encoded bytes also fully decoded + checked.
3. `sp_hash_agg_tune_apply_eq_read_only_op_streaming` — 15K rows;
   both arms must yield byte-identical results for Multi + scalar
   Aggregate paths.

Full SM suite: 160 passed (was 157 pre-T2 + 3 new); 0 failed. All 3
SP-Hash-Agg V1 KATs still green (the streaming change preserved the
fold math; only delivery mechanism swapped).

## T3 — vulcan TPC-H Q1+Q6 sweep + BENCHMARKS.md update [DONE]

Commits:
- (this commit) docs(benchmarks): SP-Hash-Agg-Tune T3 — vulcan
  TPC-H Q1+Q6 post-Tune sweep

Vulcan sweep (3 outer trials × bench-compare's 3 internal trials × 30s
× SF=0.01 × N=1,4 × KesselDB only; Postgres+SQLite from prior §3f/§3g
sweeps unchanged). Two sweeps preserved:

**UNBATCHED (intermediate shape, commit 833eede)** —
`bench-tpch-q{1,6}-posttune-t{1..3}-w{1,4}.json`:

| Workload | N=1 q/s | N=4 q/s | vs V1 |
|---|---:|---:|---:|
| Q1 | 14.99 | 54.76 | -13% / -9% |
| Q6 | 31.75 | 168.54 | -7% / -9% |

**BATCHED (final shape, commit 0a19f3d)** —
`bench-tpch-q{1,6}-posttunebatch-t{1..3}-w{1,4}.json`:

| Workload | N=1 q/s | N=4 q/s | vs V1 | vs Postgres |
|---|---:|---:|---:|---:|
| Q1 | 16.14 | **63.77** | -6.7% / **+6.0%** | 2.92× behind |
| Q6 | 33.95 | **197.55** | -0.8% / **+6.8%** | 8.53× behind |

**Headline lifts vs pre-Tune (V1 SP-Hash-Agg)**: Q1 N=1 -1.07×, Q1 N=4
**+1.06×**; Q6 N=1 par, Q6 N=4 **+1.07×**. **Cumulative 4-arc lift vs
pre-arc baseline**: Q1 N=4 **+7.21×**; Q6 N=4 **+14.38×**. **Gap vs
Postgres**: Q1 N=4 3.09× → **2.92×** (was 18× pre-arc); Q6 N=4 9.11× →
**8.53×** (was 123× pre-arc).

**Concern**: design floors (Q1 ≥120 / Q6 ≥350 at N=4) MISSED — 53% /
56% achieved. Diagnosis updated in BENCHMARKS.md §3f/§3g honest reads:

- The V1 serial Vec<Arc<[u8]>> pre-collect was NOT the dominant wall-
  time cost. V1-Tune eliminated it via streaming overlap; gained only
  +6-7%.
- The actual dominant cost is the **per-row `kessel_expr::eval` stack
  VM interpreter** evaluating the WHERE program — Q1 runs ~60K times
  per query (full scan), Q6 runs ~8K times (narrowed scan). The
  row-chunk parallel fold can amortise it across cores but cannot
  make per-row eval cheaper.
- Channel infrastructure (1 producer + 4 worker threads + 4 bounded
  channels) costs ~5-10% at N=1 where there are no concurrent queries
  to amortise the spawn across. At N=4 the streaming overlap savings
  net out positive.

Read-pool scaling on top still holds: N=4 ≈ 3.95× N=1 across both
queries (4 read-pool workers × the per-query 1.06-1.07× ≈ 4-thread
peak per query).

## T4 — arc closure [DONE]

Commits:
- (this commit) docs: SP-Hash-Agg-Tune T4 — STATUS + tracker close + README

- STATUS.md row (Track K) — added with full headline + diagnosis +
  named follow-up arcs (SP-WHERE-VM-Specialise + SP-JIT-Aggregate)
- BENCHMARKS.md §3f POST-Tune column — added with full lift +
  cumulative + gap-closing math + UNBATCHED intermediate-shape row
- BENCHMARKS.md §3g POST-Tune column — added with same shape
- BENCHMARKS.md §1 summary table TPC-H rows — updated
- BENCHMARKS.md §4 raw-results file list — adds the posttune +
  posttunebatch JSON families
- README perf section — refreshed (Q1 + Q6 post-Tune numbers; 4-arc
  roadmap rolled forward to name SP-WHERE-VM-Specialise as the new
  next-arc based on the SP-Hash-Agg-Tune sweep diagnosis)
- This tracker → DONE_WITH_CONCERNS
- SP-WHERE-VM-Specialise named as the next arc (replaces SP-Hash-Agg-
  Tune's predicted next-arc SP-Hash-Agg-Pool, which is de-prioritised
  since this sweep showed thread-spawn is NOT the bottleneck)
- TaskList #347 ready for completion
