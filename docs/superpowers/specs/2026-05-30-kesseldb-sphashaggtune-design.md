# SP-Hash-Agg-Tune — streaming producer-channel-workers hash aggregate

**Arc:** SP-Hash-Agg-Tune (V1)
**Track:** Analytics planner — drives down the SP-Hash-Agg V1 serial-prefix cost
that bounded per-query lift to 1.46-1.79× instead of the modelled 4×.
**Status:** T1 design (this doc).
**Date:** 2026-05-30.
**Parent:** SP-Hash-Agg (V1 SHIPPED 2026-05-30 DONE_WITH_CONCERNS) — progress
tracker explicitly named **SP-Hash-Agg-Tune** as the residual-cost arc.

---

## 1. Context — what SP-Hash-Agg V1 left on the table

`docs/BENCHMARKS.md` §3f + §3g, vulcan 3-trial median, SF=0.01 ≈ 60K
lineitem rows:

| Workload | Pre-Hash-Agg | Post-Hash-Agg V1 | Postgres | V1 lift | Modelled |
|---|---:|---:|---:|---:|---:|
| TPC-H Q1 N=4 | 41.11 q/s | **60.18 q/s** | 186 q/s | **1.46×** | 3-4× |
| TPC-H Q6 N=4 | 103.38 q/s | **185.03 q/s** | 1,686 q/s | **1.79×** | ~4× |

The 4-way `std::thread::scope` partition IS engaging (`MIN_PARALLEL_ROWS = 8192`
gate holds; equivalence KATs prove parallel == serial); the per-query speedup
ceiling is well below the design's 4× target. SP-Hash-Agg's progress tracker
diagnosed the root cause and named this arc:

> The surviving serial-prefix (`narrow_by_range_preds` + `Vec<Arc<[u8]>>`
> materialisation + thread spawn) is hard-pinned to one CPU and accounts
> for the bulk of wall-time at N=1.

Concretely, the V1 control flow is:
```
serial: narrow_by_range_preds          (~milliseconds, untouched here)
serial: scan_range  -> Vec<(Key,Vec<u8>)>   (~12MB allocated + memcpy'd on ONE CPU at Q1 scale)
serial: Vec<u8> -> Arc<[u8]> wrap pass      (~60K Arc bumps on ONE CPU)
parallel: std::thread::scope spawn 4 workers, each folds its chunk
serial: merge partials
```

The two serial passes (scan-materialise + Arc-wrap) MUST complete before
the workers can start. At 60K × ~200 B = ~12 MB of memcpy per query
serially on one core, the worker fold has very little wall-time left to
parallelise. This arc closes that gap.

## 2. Scope

### V1 IN-SCOPE

1. **Producer-channel-workers streaming** for `Op::Aggregate` numeric
   scan (Q6 prong) and `Op::GroupAggregateMulti` (Q1 prong). The
   dispatcher thread becomes a *producer* that iterates the scan result
   AS IT COMES BACK and dispatches rows round-robin to N
   `sync_channel(BUF_DEPTH=64)` workers via `std::thread::scope`. Workers
   start folding as soon as the first row hits their channel — overlap
   between row delivery and per-row WHERE+fold work.
2. **Zero-Arc-wrap pass for the cand=Some path.** When the candidate set
   comes from index narrowing (`cand=Some(ids)`), the `storage.get(...)`
   already returns `Arc<[u8]>` (SP-Perf-A T7). V1's Vec<Arc<[u8]>>
   collect pass is preserved AS the producer iteration — no separate
   wrap pass — but per-row `storage.get(...)` happens AS we send, not
   in a pre-collected loop. Same byte-cost, parallelised with fold.
3. **Zero-Arc-wrap pass for the cand=None path.** When the candidate set
   is the full type-keyspace (`cand=None`), the producer drains
   `storage.scan_range(...)` (which already returned the full Vec —
   we cannot avoid that serial scan in V1 without a streaming scan
   API; out of scope) and sends raw `Vec<u8>` rows directly. Workers
   receive `Vec<u8>`, not `Arc<[u8]>` — eliminating the V1 Arc::from
   bump pass (1 allocation + refcount per row × ~60K rows on Q1).
4. **Bounded back-pressure.** `sync_channel(BUF_DEPTH=64)` per worker
   caps the producer's lead over the slowest worker at 64 rows. Prevents
   memory blow-up if a worker stalls (e.g. paging) and gives natural
   back-pressure on the producer.
5. **Round-robin worker assignment.** `next_worker = (next_worker + 1) % N`
   — deterministic, no hashing, no shared state. Workers see the SAME
   rows on every run regardless of thread scheduling (same row index ⇒
   same worker, the producer iteration order is deterministic).
6. **Keep the gate.** `MIN_PARALLEL_ROWS = 8192` still gates the parallel
   path — small scans use the existing single-threaded fold verbatim
   (zero overhead for OLTP-shape aggregates). The gate check fires
   AFTER the cand decision but BEFORE the producer/workers spawn, using
   a cheap pre-count: `cand.as_ref().map(|s| s.len())` for the
   narrowed path, or a single `scan_range` length probe for the
   full-scan path (still serial; this is the same length probe V1 does).

### V1 OUT-OF-SCOPE

- **Truly streaming `scan_range`.** The storage layer's `scan_range`
  returns `Vec<(Key, Vec<u8>)>` — it allocates the full result before
  returning. To make the full-scan path truly stream from the SST/memtable
  iterator would require a `scan_range_iter()` API. Named future arc:
  **SP-Storage-Scan-Iter**. V1 still streams the *delivery* (Vec
  drained one row at a time as we send) so the worker fold overlaps
  the producer's per-row `into_iter().next()` cost — partial win.
- **Work-stealing queue.** Round-robin is simpler + deterministic. Work-
  stealing would help if one worker hits expensive WHERE predicates
  while another doesn't, but Q1 + Q6 have uniform per-row cost.
- **Thread-pool reuse.** Each query still spawns its own 4 workers via
  `std::thread::scope`. A reusable pool (e.g. one pool per read worker)
  would amortise spawn cost across queries; out of scope here. Named
  follow-up arc: **SP-Hash-Agg-Pool**.
- **JIT codegen for the per-row inner loop.** Postgres uses LLVM
  codegen. Future arc **SP-JIT-Aggregate**.
- **Op::GroupAggregate (single-aggregate-per-call shape).** Same as
  V1's scope — not on the hot path for TPC-H any more.

### What V1 will NOT change (back-compat guards)

- **Wire format** — zero new variants; no proto changes.
- **Determinism oracle** — streaming result is byte-identical to
  serial result on the same data; locked by 2-3 new KATs.
- **HTTP/1.1 + WebSocket + binary + PG-wire surfaces byte-untouched.**
- **Replication (VSR)** — aggregate ops are reads (never replicated).
- **`#![forbid(unsafe_code)]`** — `std::sync::mpsc::sync_channel` +
  `std::thread::scope` are safe std.
- **No new external deps** — std-only.

## 3. Architecture

### 3a. Producer-channel-workers shape

The new control flow:
```
serial: narrow_by_range_preds                (untouched)
parallel start:
  producer thread (spawned via scope):
    iterates cand IDs OR scan_range result
    for each row: pick next worker round-robin; send row over channel
    on exhaustion: drop all tx ends to signal workers to stop
  N=4 worker threads (spawned via scope):
    rx.recv() loop:
      run WHERE program on row
      extract group key (Multi only)
      update local partial HashMap / scalar acc
    on channel disconnect: return partial
serial: scope joins workers + producer; merge partials
```

### 3b. Sketch (Q6-shape, simpler)

```rust
let (txs, rxs): (Vec<_>, Vec<_>) = (0..N)
    .map(|_| std::sync::mpsc::sync_channel::<Arc<[u8]>>(BUF_DEPTH))
    .unzip();

let partials: Vec<ScalarAcc> = std::thread::scope(|scope| {
    let producer = scope.spawn(move || {
        let mut next = 0usize;
        match &cand {
            Some(ids) => {
                for id in ids {
                    if let Some(r) = self.storage.get(&make_key(type_id, id)) {
                        let _ = txs[next].send(r);
                        next = (next + 1) % N;
                    }
                }
            }
            None => {
                let lo = make_key(type_id, &[0u8; 16]);
                let hi = make_key(type_id, &[0xFFu8; 16]);
                for (_, rec) in self.storage.scan_range(&lo, &hi) {
                    let arc: Arc<[u8]> = Arc::from(rec.into_boxed_slice());
                    let _ = txs[next].send(arc);
                    next = (next + 1) % N;
                }
            }
        }
        drop(txs); // signal workers to stop
    });

    let workers: Vec<_> = rxs.into_iter().map(|rx| scope.spawn(move || {
        let mut acc = (0i128, 0i128, None::<i128>, None::<i128>);
        while let Ok(rec) = rx.recv() {
            // ... WHERE + fold update ...
        }
        acc
    })).collect();

    producer.join().unwrap();
    workers.into_iter().map(|h| h.join().unwrap()).collect()
});
```

### 3c. The expected speedup

- **Q6 (narrowed scan)**: 8K rows from cand=Some(ids), ~200 µs of
  storage.get + Arc-bump on one CPU pre-V1; workers wake at row 1 and
  by row 8K have eaten ~25% of the wall-time. Modelled lift: ~2× on
  top of V1 (which was already 1.79× of pre-arc). Target: Q6 N=4 from
  185 → ≥350 q/s (user spec floor); stretch ≥500 q/s.
- **Q1 (near-full scan)**: 60K rows via cand=None scan_range. The
  scan_range itself is still serial (~10-20ms collecting Vec); but
  the Arc-wrap pass (V1 paid ~60K Arc::from + ~60K refcount bumps
  serially before workers started) now happens DURING worker fold.
  Modelled lift: ~1.5-2× on top of V1. Target: Q1 N=4 from 60 → ≥120
  q/s (user spec floor); stretch ≥164 q/s (full 4× modelled).

### 3d. Determinism contract — UNCHANGED from V1

- **Partition assignment** — by row INDEX in the producer iteration
  (`next = (next + 1) % N`). Producer iteration order is deterministic
  (BTreeSet for cand=Some, sorted BTreeMap drain for scan_range), so
  the same row always lands in the same worker.
- **Merge order** — fold partials in `(0..N)` order (the spawn order).
- **Combine ops** — associative for SUM/COUNT, associative+commutative
  for MIN/MAX.
- **AVG** — computed POST-merge from `(sum, count)` via integer
  division — matches existing semantics byte-for-byte.
- **Group output order** — final BTreeMap iteration is ascending key,
  same as today.
- **Conclusion**: streaming-delivered parallel result is byte-equal to
  the V1 pre-collected parallel result, which is byte-equal to the
  V0 serial result. Locked by KATs.

### 3e. Channel send/recv overhead — is this worth it?

`std::sync::mpsc::sync_channel` is a parking-lot-style bounded channel.
At BUF_DEPTH=64 the producer can run 64 rows ahead before blocking on
recv; the typical send/recv pair is a few hundred ns when the receiver
is parked + ~tens of ns when both are awake. For Q1's 60K rows × ~250 ns
average = ~15ms of channel overhead per query — comparable to the V1
Arc-wrap pass we're eliminating (60K × ~200ns = ~12ms). Net should be
roughly break-even on the overhead axis, with the big win being that
the producer's per-row work overlaps with the worker's per-row work
instead of serialising.

For Q6's 8K narrowed rows, channel overhead is ~2ms — well under V1's
storage.get + Arc-bump serial prefix of ~5-10ms. Clear win.

If the measured channel overhead bites harder than modelled, BUF_DEPTH
is a tunable const + we can fall back to the V1 pre-collected shape
with a feature flag. T3 + T4 will validate.

## 4. Acceptance criteria

- **TPC-H Q1 N=4 on vulcan** lifts from 60.18 q/s → **≥ 120 q/s**
  (floor — user spec) ; stretch ≥ 164 q/s. The 120 q/s floor brings
  the gap vs Postgres from 3.09× to ≤ 1.55×.
- **TPC-H Q6 N=4 on vulcan** lifts from 185.03 q/s → **≥ 350 q/s**
  (floor — user spec); stretch ≥ 500 q/s. The 350 q/s floor brings
  the gap vs Postgres from 9.11× to ≤ 4.82×.
- **Equivalence** — streaming result byte-equal to serial result on
  same data. 2-3 new SM-level KATs (parallels the V1 KATs):
  - `sp_hash_agg_tune_group_aggregate_multi_streaming_eq_serial`
  - `sp_hash_agg_tune_aggregate_streaming_eq_serial`
  - `sp_hash_agg_tune_apply_eq_read_only_op_at_scale`
- **All pre-arc tests pass** — the 3 SP-Hash-Agg KATs stay green
  (they pinned parallel == serial; we only changed the parallel
  delivery mechanism, not the math). All 15 pre-MULTI aggregate KATs
  also stay green.
- **CI green** on every push.
- **No new external deps** — `std::sync::mpsc::sync_channel` +
  `std::thread::scope` only.
- **`#![forbid(unsafe_code)]`** honored.

## 5. Task decomposition

| Task | Description | Acceptance |
|---|---|---|
| **T1** | Design + scaffold + streaming refactor | This doc + `BUF_DEPTH` const + `aggregate_numeric_scan` + `group_aggregate_multi` rewritten with producer-channel-workers; all existing tests green |
| **T2** | Streaming-equivalence KATs | 2-3 new SM-level KATs lock streaming == serial byte-for-byte at 10K-row scale |
| **T3** | vulcan TPC-H Q1+Q6 sweep + BENCHMARKS.md update | 3 trials × 30s × SF=0.01 × N=1,4 × KesselDB only; §3f + §3g get POST-TUNE columns |
| **T4** | arc closure | STATUS row (next Track letter) + progress tracker → CLOSED or DONE_WITH_CONCERNS + README perf section refresh + TaskList #347 ready |

## 6. Six-plus weak-spot self-review

1. **Producer is still single-threaded scan.** `storage.scan_range`
   itself runs on the producer thread and materialises the full Vec
   before the producer can iterate. For Q1's full-scan, the ~10-20ms
   scan cost is unmoved. Mitigation: the user's spec acknowledges this
   ("producer thread is single-threaded scan → if scan_range is slow,
   no parallelism gained"). The named follow-up arc
   **SP-Storage-Scan-Iter** would make scan_range return an iterator
   so workers see rows as soon as the SST iterator yields them.
2. **Channel send/recv overhead at high row counts.** At ~250 ns per
   send/recv, 60K rows = ~15 ms — comparable to the V1 Arc-wrap pass
   we eliminate. If T3 shows a regression, BUF_DEPTH can be tuned or
   the V1 shape kept as a fallback.
3. **Back-pressure when workers are slow.** BUF_DEPTH=64 means the
   producer parks after sending 64 rows ahead of the slowest worker.
   Prevents memory blow-up. Down-side: a slow worker can stall the
   producer + thereby all other workers (round-robin assignment).
   Mitigation: workers have identical work, so steady-state imbalance
   is minimal. Worst case for Q6 (uniform WHERE) is ~zero.
4. **Producer thread spawn overhead.** Adds 1 more thread to the V1
   4-worker scope (5 threads total per query vs 4 in V1). The extra
   ~100µs per query is dwarfed by the savings.
5. **Determinism via round-robin.** Round-robin assignment by producer
   iteration order is deterministic IF the producer iteration order
   is deterministic. cand=Some(ids) is BTreeSet (sorted iteration
   order — deterministic). cand=None scan_range returns Vec sorted
   by key (deterministic). Confirmed: same row → same worker → same
   per-worker partial → same merged result → byte-identical output.
6. **Worker drop signaling.** Producer must drop ALL txs after the
   scan exhausts; the workers' `rx.recv()` returns Err when ALL txs
   are dropped. We drop the txs Vec at the end of the producer
   closure (consumes the txs since they're moved into the closure).
   `std::thread::scope` enforces this at compile-time: workers borrow
   their rx, producer owns its txs.
7. **Producer error propagation.** Producer panics during scan
   propagate via `producer.join().expect("producer panicked")` — same
   shape as V1's worker join. The surrounding `read_only_op` / `apply`
   arm is panic-unwinding (caller catches via `OpResult`). A future
   variant could return `OpResult::SchemaError` on producer panic.
8. **Worker WHERE-eval errors.** `kessel_expr::eval` can return Err
   (program bytecode invalid). V1 returned `OpResult::SchemaError`
   via a `Result<...>` shape from each worker. V1-Tune keeps this:
   each worker returns `Result<ScalarAcc, String>` /
   `Result<HashMap, String>` — the merge step returns the first
   error (same behavior as V1).
9. **Sync trait constraints.** `std::sync::mpsc::SyncSender<T>`
   requires `T: Send`. `Vec<u8>` and `Arc<[u8]>` are both Send.
   `std::thread::scope` already constrains the worker closures to
   `'scope`; channel ends live entirely within the scope. No
   Sync/Send issues.
10. **Op::GroupAggregateMulti — row shape.** The Multi worker
    additionally extracts the group key + updates a HashMap. The
    HashMap construction work happens AT THE WORKER (parallel) —
    the V1 win-region is preserved, with the new addition of
    overlapping the row-delivery (producer iteration) with worker
    fold work.

## 7. Files

- `docs/superpowers/specs/2026-05-30-kesseldb-sphashaggtune-design.md` — this spec
- `docs/superpowers/specs/2026-05-30-kesseldb-sphashaggtune-progress.md` — progress tracker (T1-T4)
- `crates/kessel-sm/src/lib.rs` — `BUF_DEPTH` const; `aggregate_numeric_scan` + `group_aggregate_multi` rewritten with producer-channel-workers; new equivalence KATs
- `docs/BENCHMARKS.md` — §3f + §3g get POST-TUNE columns
- `docs/STATUS.md` — Track row added
- `README.md` — perf section refresh (Q1 + Q6 post-Tune)

## 8. Standing rules acknowledgement

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-hashtune`.
- Direct commits to main, no Co-Authored-By, no `-S`, push after each.
- CI green check after push.
- `#![forbid(unsafe_code)]` honored (sync_channel + thread::scope safe).
- No new external deps.
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched.
- Determinism oracle still passes (streaming == serial byte-for-byte).
