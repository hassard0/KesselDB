# SP-Hash-Agg — parallel hash aggregate design

**Arc:** SP-Hash-Agg (V1)
**Track:** Analytics planner — closes the SP-Analytic-Plan-MULTI residual
TPC-H Q1 + Q6 gaps vs Postgres' parallel hash aggregate.
**Status:** T1 design (this doc).
**Date:** 2026-05-30.
**Parents:** SP-Analytic-Plan (V1 SHIPPED 2026-05-29) +
SP-Analytic-Plan-MULTI (V1 SHIPPED 2026-05-30) — both arcs named
SP-Hash-Agg in their "next roadmap arc" sections as the third prong.

---

## 1. Context — what the predecessor arcs left on the table

`docs/BENCHMARKS.md` §3f + §3g, vulcan 3-trial median, SF=0.01 ≈ 60K
lineitem rows:

| Workload | KesselDB pre-MULTI | KesselDB post-MULTI | Postgres | Gap |
|---|---:|---:|---:|---:|
| TPC-H Q1 N=4 (multi-aggregate GROUP BY) | 10.14 q/s | **41.11 q/s** | 186 q/s | Postgres 4.5× ahead |
| TPC-H Q6 N=4 (single SUM w/ WHERE) | 13.74 q/s | 103.38 q/s | 1,686 q/s | Postgres 16× ahead |

SP-Analytic-Plan-MULTI lifted Q1 by **4.05× at N=4** (10.14 → 41.11)
by collapsing 4 separate `Op::GroupAggregate` scans into 1
`Op::GroupAggregateMulti`. The remaining 4.5× gap on Q1 and 16× gap on
Q6 is structural: **Postgres parallelises the per-row aggregate-fold
across multiple backends; KesselDB folds serially in the single read
worker that picked up the op.**

The Perf-A T2 read pool (8 workers) already gives N×scaling across
*queries* (Q1 N=4 = 3.77× N=1, Q6 N=4 = 4.07× N=1) — but for a single
query the BTreeMap aggregator is single-threaded. Postgres' parallel
hash aggregate partitions the scan by chunk and folds in parallel
backends, then merges partials. That's the gap we close in V1.

## 2. Scope

### V1 IN-SCOPE

1. **Parallel scan + per-worker hash partials + merge** for
   `Op::GroupAggregateMulti` (closes the Q1 prong). The shared
   `group_aggregate_multi()` helper in `kessel-sm` materialises the
   candidate row set, partitions by row offset, spawns N workers via
   `std::thread::scope` (Rust 1.63+, std-only, zero new external deps),
   each builds a local `HashMap<group_key, Vec<Acc>>`, then merges all
   N partials into one sorted `BTreeMap` for ascending-key output.
2. **Parallel scan + per-worker scalar partials + merge** for
   `Op::Aggregate` (closes the Q6 prong). Same structure but no
   group-key — each worker accumulates a scalar
   `(count, sum, min, max)` tuple and the final merge is a single
   reduce.
3. **Threshold gate** — only parallelise when the materialised row
   count ≥ `MIN_PARALLEL_ROWS` (default 8192). Smaller scans use the
   existing single-threaded fast path verbatim (no overhead penalty
   for OLTP-shape aggregates).
4. **Worker count** — pinned to `NUM_HASH_AGG_WORKERS` (default 4).
   This matches the typical `--connections 4` benchmark shape; the
   read pool dispatches one Op per worker, and *within* one Op this
   arc spawns 4 more, for `~4 × 4 = 16` cores active at peak. vulcan
   has 80 logical cores so the oversubscription is safe.
5. **Determinism** — per-aggregate combine is associative for
   COUNT/SUM (and AVG = SUM/COUNT, computed AFTER merge so the integer
   division is also identical), and associative+commutative for
   MIN/MAX. The merge step folds partials in the deterministic order
   `(0..n_workers)`; the final output is sorted by group key
   (`BTreeMap` from the merged set). Identical bytes regardless of
   thread-scheduling order.

### V1 OUT-OF-SCOPE

- **JIT codegen** for the per-row aggregate-update inner loop
  (Postgres uses LLVM codegen). Future arc **SP-JIT-Aggregate**.
- **Cost-based parallelism decision** — V1 uses a fixed row-count
  threshold + fixed worker count. A cost-based planner would tune
  these per-query. Future arc **SP-Cost-Planner**.
- **Parallel index narrowing** — `narrow_by_range_preds()` still runs
  on the single thread before the row materialisation phase. The
  narrowing itself is fast (single index range scan); parallelising
  it would add overhead with no win.
- **`Op::GroupAggregate`** (the single-aggregate-per-call shape) —
  not on the hot path for TPC-H any more (Q1 now uses Multi, Q6 uses
  Aggregate). Could be added in V2 by routing through the same
  parallel helper, but no perf evidence calls for it yet.
- **Per-worker materialisation** — V1 materialises the FULL candidate
  row list into a `Vec<Vec<u8>>` once on the dispatcher thread, then
  partitions by offset. A streaming variant would let workers pull
  from a shared cursor; out of scope (the materialisation cost is
  dominated by the per-row decode/eval work that follows).

### What V1 will NOT change (back-compat guards)

- **Wire format** — zero new variants; no proto changes. The new
  parallel path is internal to the SM apply arms.
- **Determinism oracle** — parallel result is byte-identical to
  serial result on the same data; locked by 2-3 new KATs.
- **HTTP/1.1 + WebSocket + binary + PG-wire surfaces** byte-untouched.
- **Replication (VSR)** — aggregate ops are reads (never replicated),
  so WAL footprint stays empty.
- **#![forbid(unsafe_code)]** — `std::thread::scope` is safe.
- **No new external deps** — std-only.

## 3. Architecture

### 3a. The two-phase split

The existing `group_aggregate_multi` interleaves three steps in one
loop:
1. Fetch row bytes
2. Run WHERE program
3. Fold into BTreeMap

V1 splits this into:

**Phase A (dispatcher thread):** materialise the candidate row set
into a `Vec<Vec<u8>>`. For `cand=Some(ids)`, that's
`ids.iter().filter_map(|id| storage.get(...))`; for `cand=None`,
that's the type-keyspace `scan_range` collect. The WHERE program is
NOT applied yet (we push it into workers so its cost parallelises).

**Phase B (parallel workers):** each worker takes one chunk of the
materialised vec and runs:
- WHERE program eval per row
- group-key extract
- fold into local `HashMap<Vec<u8>, Vec<Acc>>`

**Phase C (dispatcher thread):** merge all N partial HashMaps into
one `BTreeMap<Vec<u8>, Vec<Acc>>`. The BTreeMap iteration order
guarantees ascending-key output (the existing contract).

### 3b. Threshold + worker-count gate

```rust
const MIN_PARALLEL_ROWS: usize = 8192;
const NUM_HASH_AGG_WORKERS: usize = 4;

if rows.len() < MIN_PARALLEL_ROWS {
    // single-threaded fast path (existing code)
} else {
    let chunk_size = (rows.len() + NUM_HASH_AGG_WORKERS - 1) / NUM_HASH_AGG_WORKERS;
    let partials: Vec<HashMap<Vec<u8>, Vec<Acc>>> = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(NUM_HASH_AGG_WORKERS);
        for chunk in rows.chunks(chunk_size) {
            handles.push(scope.spawn(move || build_partial(chunk, ...)));
        }
        handles.into_iter().map(|h| h.join().expect("worker panic")).collect()
    });
    // merge into BTreeMap
}
```

### 3c. Determinism contract

- **Merge order** — fold partials in `(0..N)` order (the spawn order).
  Combine ops are associative for SUM (+) and COUNT (+), and
  associative+commutative for MIN (min) and MAX (max), so the result
  is identical regardless of fold order — but pinning the fold order
  removes any non-determinism risk from a future re-ordering.
- **AVG** — computed POST-merge from the merged `(sum, count)` via
  integer division `sum / count`, matching existing semantics
  byte-for-byte.
- **Group output order** — final BTreeMap iteration is ascending key,
  same as today's single-threaded path.
- **Partition assignment** — by row OFFSET (not by hash), so the same
  input row always lands in the same partition regardless of the data
  values. Combined with deterministic fetch (storage is read-only
  under the read-pool guard), the per-worker partials are themselves
  deterministic.

### 3d. Op::Aggregate (Q6) parallel path

Same structure, simpler aggregator:

```rust
struct ScalarAcc { count: i128, sum: i128, mn: Option<i128>, mx: Option<i128> }
impl ScalarAcc {
    fn merge(&mut self, other: ScalarAcc) {
        self.count += other.count;
        self.sum = self.sum.wrapping_add(other.sum);
        self.mn = match (self.mn, other.mn) { (a, None) => a, (None, b) => b, (Some(a), Some(b)) => Some(a.min(b)) };
        self.mx = match (self.mx, other.mx) { (a, None) => a, (None, b) => b, (Some(a), Some(b)) => Some(a.max(b)) };
    }
}
```

Q6 specifically uses `kind=SUM, field_id=L_Q6_REVENUE` with
`range_preds` narrowing on `l_shipdate`. The narrowed candidate set is
~8K rows (the 1994 window). 8K is exactly at threshold — V1 default
threshold of 8192 keeps Q6's narrowed scan on the parallel path. The
expected lift: 4 workers each fold ~2K rows in parallel ⇒ ~3.5×
amortised speedup (read pool already gives N×4 across queries; this
is the 4× WITHIN a query).

### 3e. The MIN/MAX vord fast path stays untouched

`Op::Aggregate { kind: 2|3, field on var-order index }` has a special
fast path (`agg_extreme_var`) that reads the extreme straight from the
index without scanning rows. This bypasses both serial and parallel
folds — V1 leaves it unchanged. Only the row-scanning fold path
parallelises.

## 4. Acceptance criteria

- **TPC-H Q1 N=4 on vulcan** lifts from 41.11 q/s → **≥ 100 q/s**
  (target = 4× chunk parallelism × current 41 = 164; the 100-q/s
  floor leaves headroom for thread-spawn overhead + merge cost).
  Gap vs Postgres closes from 4.5× to **≤ 2×**.
- **TPC-H Q6 N=4 on vulcan** lifts from 103.38 q/s → **≥ 400 q/s**
  (target = 4× × 103 = 412; the 400-q/s floor leaves headroom for
  the narrower scan having less work per worker). Gap vs Postgres
  closes from 16× to **≤ 5×**. (The user's spec named 500 q/s as
  the stretch goal; 400 is the floor and we'll report whatever the
  vulcan sweep shows.)
- **Equivalence** — parallel result byte-equal to serial result on
  same data. 2 SM-level KATs: one for `Op::GroupAggregateMulti`
  (Q1-shape, 5 aggregates × 3 groups), one for `Op::Aggregate`
  (Q6-shape, single SUM with range narrowing).
- **All pre-arc tests pass** — existing GroupAggregateMulti +
  Aggregate KATs stay green; the parallel path is gated by row
  count so the small-row tests stay on the serial path.
- **CI green** on every push.
- **No new external deps** — std::thread::scope only.

## 5. Task decomposition

| Task | Description | Acceptance |
|---|---|---|
| **T1** | Design + scaffold | This doc + `MIN_PARALLEL_ROWS`/`NUM_HASH_AGG_WORKERS` constants + skeleton equivalence KATs (currently single-threaded; will stay green when T2 lands) |
| **T2** | SM parallel `group_aggregate_multi` + parallel `Op::Aggregate` | Both paths gain parallel scan + per-worker hash partials + merge for row-counts ≥ threshold; small-row path unchanged |
| **T3** | Equivalence KATs | 2 new SM-level KATs (one Multi, one Aggregate) lock parallel == serial byte-for-byte across ~10-100K row workloads |
| **T4** | vulcan TPC-H Q1+Q6 sweep + BENCHMARKS.md update | 3 trials × 30s × SF=0.01 × N=1,4 × KesselDB only (Postgres+SQLite unchanged from §3f/§3g sweep); §3f + §3g get POST-HASH columns |
| **T5** | arc closure | STATUS row (next Track letter) + progress tracker → CLOSED or DONE_WITH_CONCERNS + README perf section refresh + TaskList #345 ready |

## 6. Six-plus weak-spots self-review

1. **Thread-spawn overhead per query.** Spawning 4 threads via
   `std::thread::scope` for every aggregate op pays ~10-100µs of OS
   overhead. The Q6 single-aggregate path runs in ~10ms today; 100µs
   spawn cost is 1% — acceptable. The threshold gate (8192 rows)
   keeps OLTP-shape sub-ms queries on the serial path so overhead
   stays out of the OLTP hot path.
2. **Hash-table memory blow-up at high group cardinality.** Each
   worker's local HashMap could grow large; on Q1 the cardinality is
   tiny (3-4 groups) so this isn't a Q1 issue. For a future
   high-cardinality workload (e.g. GROUP BY customer_id with 100K
   distinct), the per-worker map could hit 100K entries × 4 workers
   = 400K entries before merge. Memory bounded; V1 accepts this.
   Future V2 could spill to disk or use a bounded variant.
3. **Decision threshold may be wrong.** 8192 is a guess based on the
   Q6-narrowed-scan size. If it's too high, Q6's narrowed window
   stays on the serial path and we get no Q6 lift. If it's too low,
   small OLTP-shape aggregates pay thread-spawn overhead. V1 plumbs
   it as a `const` (easy to retune); T4 measurements will validate.
4. **The Perf-A read pool already dispatches one Op per worker.**
   This arc spawns 4 MORE threads WITHIN one Op. With N=4 read-pool
   workers each fielding one query, peak parallelism is 4 × 4 = 16
   cores. vulcan has 80 logical cores so safe; but on a 4-core box
   the oversubscription would hurt. V1 accepts vulcan-tuned defaults;
   a future env var `KDB_HASH_AGG_WORKERS=0` to disable could be
   added.
5. **Materialisation memory.** Phase A buffers ALL candidate rows
   into a `Vec<Vec<u8>>` before partitioning. For Q1 that's ~60K rows
   × ~150B each = ~9MB per query — fine. For a 100M-row scan it'd be
   15GB — bad. V1 accepts the Q1/Q6 scale; a future streaming
   variant could partition during scan.
6. **Determinism**: merge order is pinned (worker `(0..N)` order),
   combine ops are associative for SUM/COUNT and
   associative+commutative for MIN/MAX. AVG is computed POST-merge so
   the integer division matches the serial path byte-for-byte. Locked
   by 2 KATs that compare parallel vs serial byte output across
   varied data shapes.
7. **Storage Sync.** `Storage<MemVfs>` and `Storage<DiskVfs>` contain
   no interior mutability fields on the read path (memtable is
   `BTreeMap<Key, Option<Arc<[u8]>>>`, sstables/manifest are
   `Vec<SsTable>`/`Manifest`). The `&self` borrow held by
   `read_only_op`/`apply` is Sync. Worker closures borrow
   `&self.storage` via the materialised Vec — actually they don't
   borrow storage at all; Phase A materialises and Phase B only
   touches the owned chunk + the immutable program/ot. No storage
   access in workers ⇒ Sync question is moot.
8. **WHERE program eval inside the parallel section** — `kessel_expr::eval`
   takes `&[u8]` (program) + `&ObjectType` + `&[u8]` (record) by
   reference; no interior mutability; threads can share `&program` +
   `&ot` immutably. Confirmed by `cargo check` after T2 lands.
9. **Worker panic propagation.** `handle.join().expect("worker
   panic")` propagates; the surrounding `read_only_op`/`apply` arm is
   already panic-unwinding (caller catches via `OpResult`). V1
   accepts this; a future variant could return `OpResult::SchemaError`
   on worker panic.
10. **AVG correctness across partials.** AVG can't be combined
    directly (the AVG-of-AVGs is wrong if group sizes differ). V1
    sidesteps this by carrying `(count, sum, mn, mx)` per slot in
    every accumulator (the existing shape already does this for the
    serial path); AVG is computed only at the final result-encode
    step, post-merge, as `sum / count`. Identical to today's
    semantics.

## 7. Files

- `docs/superpowers/specs/2026-05-30-kesseldb-sphashagg-design.md` —
  this spec
- `docs/superpowers/specs/2026-05-30-kesseldb-sphashagg-progress.md` —
  progress tracker (T1-T5)
- `crates/kessel-sm/src/lib.rs` — `MIN_PARALLEL_ROWS` +
  `NUM_HASH_AGG_WORKERS` consts; `group_aggregate_multi()` rewritten
  with parallel path; `Op::Aggregate` apply arms gain parallel path;
  new equivalence KATs
- `docs/BENCHMARKS.md` — §3f + §3g get POST-HASH columns
- `docs/STATUS.md` — Track row added
- `README.md` — perf section refresh (Q1 + Q6 post-Hash-Agg)

## 8. Standing rules acknowledgement

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-hash`.
- Direct commits to main, no Co-Authored-By, no `-S`, push after each.
- CI green check after push.
- Memory files OUTSIDE repo.
- `#![forbid(unsafe_code)]` honored (std::thread::scope is safe).
- No new external deps (std::thread::scope is std-only since 1.63).
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched.
- Determinism oracle still passes (parallel == serial byte-for-byte).
