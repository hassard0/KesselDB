## SP-Perf-A-SHARD-SCAN-POOL-SCALEOUT — close the scan-limit / scan-sorted regression FASTPATH left open — design spec

Date: 2026-06-01
Parent: SP-Perf-A-SHARD-SCAN-FASTPATH (V1 SHIPPED 2026-05-30, DONE for
find-by perf, named POOL-SCALEOUT as the follow-up for the corner-case
scan regressions).

This arc closes the corner-case regression FASTPATH left open: the
persistent ScatterPool recovered `find-by` at K=4 by 105× (10K →
1,066K ops/sec, 59% of K=1 baseline), but it regressed two longer-
running scan ops at K=4 against their pre-FASTPATH numbers:

| Workload | K=1 | K=4 pre-FASTPATH | K=4 POST-FASTPATH | regression |
|---|---|---|---|---|
| `select-limit` (LIMIT 10) | 2,576 | 1,903 | **958** | **-50%** |
| `select-sorted` (LIMIT 10) | 674 | 695 | **214** | **-69%** |

Root cause is named in BENCHMARKS §14b honest-gap #1 and the
FASTPATH closure tracker:

> pool's `sync_channel(1)` bound serializes 16 dispatcher threads → 4
> workers under saturation.

`kessel-bench parallel-reads --workers 16` opens 16 concurrent client
connections; each connection becomes its own dispatcher thread inside
`ShardedDispatcher`. Every one of those 16 dispatcher threads calls
`scatter_and_merge_via_pool` which sends a `PoolWorkItem` to each of
the K pool worker channels. With `sync_channel(1)`, a worker accepts
the next item ONLY when the dispatcher of the previous item has
already drained its reply — so at K=4 / 16 dispatchers, the four
worker channels become FIFO queues that serialize 16 dispatcher
threads onto 4 workers. The longer the per-op work (`select-limit`
scans 100K rows × 10K-row LIMIT-clip; `select-sorted` does the same
plus a sort), the worse the head-of-line blocking.

`find-by` doesn't see this because the per-op cost (~5µs) is shorter
than the dispatch+send overhead (~10-100µs), so the queues drain
faster than they fill. The headline FASTPATH win is preserved.

Honest framing: **the FASTPATH regression is real but bounded** — it's
only on scan-limit / scan-sorted at K=4. Point-shaped ops (find-by)
already recovered to 1.07M at K=4. POOL-SCALEOUT is a "close the
corner case" arc, not a headline.

---

### 1. Fix candidates considered

**Approach A: bigger channel bound.** Bump `sync_channel(1)` to
`sync_channel(N)` for some N >> 1. Doesn't eliminate serialization
(workers still process one item at a time) but lets multiple in-flight
items queue per worker instead of blocking the dispatcher's `send()`.

Risk: queue depth affects latency. May not actually help if workers
can't keep up (the bottleneck would just shift from `send()` to
`recv_timeout()` on the dispatcher's reply rx).

Cost: single-line constant change. Smallest possible risk surface.

**Approach B: per-dispatcher pool replicas.** Each dispatcher thread
(each connection) owns its own ScatterPool. Eliminates cross-dispatcher
contention but allocates 16 × K worker threads at peak.

Risk: thread overhead — at K=8 with 16 connections, that's 128 worker
threads. Vulcan has 24 cores; oversubscribed. Also breaks the "one
pool per `ShardedDispatcher`" invariant FASTPATH baked in.

Cost: ScatterPool lifecycle change; per-connection allocator path;
risk of OOM under connection spikes.

**Approach C: shared pool with M >> K workers.** ONE pool with M
workers (M = num_dispatchers × K, e.g. 64 workers at K=4 with 16
dispatchers). Each worker can handle any shard's op. Dispatcher sends
op + target_shard_id to a shared queue; first available worker for
that shard takes it.

Risk: per-op shard-target routing adds complexity but eliminates
contention. Workers need shard-id-keyed dispatch tables (each worker
must hold K closures, not 1) which inflates pool footprint by K×.

Cost: significant refactor of `pool_worker_loop` + `PoolWorkItem` to
carry shard-id + dispatch-table indexing.

---

### 2. Recommendation: start with Approach A

Smallest change. If it doesn't recover the regression, escalate to B
(per-dispatcher replicas) or C (shared queue) in T4.

Picking `POOL_BOUND = 64`:

- Large enough to absorb 16 dispatchers × multiple in-flight items
  without `send()` blocking under typical load.
- Small enough to bound the memory ceiling (a `PoolWorkItem` is
  ~64-128 bytes counting the Op clone, so 64 × K workers = a few KB).
- Powers-of-2 not required; 64 is a defensible "an order of magnitude
  bigger than dispatcher count" choice.

`POOL_BOUND` is named as a private `const` in `scatter_scan.rs` so a
future tuning slice can change it via a single edit (and a single
KAT lock to update). It is NOT a runtime knob — adding tuning surface
would require config plumbing through `ShardedDispatcher::new`, which
is out of scope for this arc.

---

### 3. Determinism preserved

The channel-bound bump is a backpressure-only change. It does NOT
change:

- Per-shard dispatch ordering (one tx per shard, in shard-id order).
- Per-shard reply ordering (each worker still sends exactly one
  `OpResult` per `PoolWorkItem`).
- Merge semantics (same `merge_scan_results` over shard-id-ordered
  `Vec<OpResult>`).
- K-invariance — `t3_shard_scan_k_invariance_oracle_12_ops` and the
  ~40 existing scatter_scan KATs stay GREEN unchanged.

The only observable effect on the wire/replay is that under
saturation, `send()` from a dispatcher returns sooner (work item
queued instead of blocked) — but the work is processed in the same
shard-id order, with the same per-item dispatch closure, with the
same reply contract.

---

### 4. Worst-case worker death contract

A worker dying mid-flight with N items queued ahead means the
dispatcher's `send()` succeeds (slot was empty) but the reply never
arrives — the existing `recv_timeout(per_shard_timeout)` deadline
catches this and substitutes `OpResult::Unavailable` for the slot,
which the V1 hard-fail merger surfaces. Bound increase doesn't
change this; in fact a bigger queue means MORE in-flight items will
time out from a dead worker, all surfaced as `Unavailable` — clean,
no leak, no deadlock.

---

### 5. Acceptance criteria

1. `select-limit` at K=4 lifts back to within 90% of K=1 baseline
   (≥2,318 ops/sec from K=1 2,576). If not, escalate to B.
2. `select-sorted` at K=4 lifts back to within 90% of K=1 baseline
   (≥607 ops/sec from K=1 674). If not, escalate to B.
3. `find-by` at K=4 stays at ≥1M ops/sec (no regression on the
   headline FASTPATH win).
4. `aggregate-sum` at K=4 / K=8 stays within ±10% of POST-FASTPATH
   (no regression on the previously-recovered shape).
5. K-invariance oracle `t3_shard_scan_k_invariance_oracle_12_ops`
   stays GREEN — no semantic change.
6. All existing scatter_scan KATs (~40+) stay GREEN — the bound is
   a drop-in change.
7. New KAT: high-concurrency dispatch (16 dispatcher threads × 100
   ops each → all complete with the new bound) PASSES.
8. `cargo build --workspace` byte-identical default profile.
9. `#![forbid(unsafe_code)]` honored.
10. Zero new external deps.

---

### 6. Task decomposition (T1-T4)

| T# | Scope |
|---|---|
| **T1** | This design spec + Approach A scaffold + KAT. Single-line `POOL_BOUND` const bump in `scatter_scan.rs::ScatterPool::new` (1 → 64) + new KAT for 16 dispatcher threads × 100 ops. |
| **T2** | (combined with T1 in this arc — change is a const + KAT.) |
| **T3** | vulcan bench: rerun §14b workloads at --workers 16 × K∈{1,4,8}, capture POST-SCALEOUT column, decide A-sufficient vs escalate to B/C. |
| **T4** | Arc closure: BENCHMARKS §14b POST-SCALEOUT column, STATUS row, progress tracker → CLOSED, TaskList #354 ready. |

---

### 7. Escalation tree (if Approach A insufficient)

If T3 shows select-limit / select-sorted still regressed >10% vs K=1
baseline after the bound bump:

- **First escalation: Approach B (per-dispatcher pool replicas).**
  Lift the `Arc<ScatterPool>` shared with every dispatcher into a
  per-dispatcher allocation. ScatterPool stays the same internally;
  the change is in `sharded_engine.rs` where the pool is constructed
  (allocate lazily per dispatcher thread, not once at startup).
  Worker count ceiling: connection-count × K. At 16 conns × K=8 =
  128 workers on a 24-core vulcan — oversubscribed but workable.

- **Second escalation: Approach C (shared M-worker pool with shard
  routing).** Refactor `PoolWorkItem` to carry `shard_id: u8`; each
  worker holds the full `Vec<Box<dyn Fn>>` and dispatches based on
  the item's `shard_id`. Single shared queue (`mpsc::sync_channel
  (POOL_BOUND_SHARED)`); first available worker takes the next item.
  Loses the per-shard FIFO guarantee within the channel, but each
  shard's *replies* still merge in shard-id order via the
  dispatcher's `Vec<Receiver>` ordering.

Both escalations preserve K-invariance + determinism (replies merge
in shard-id order via the dispatcher's own `Vec<Receiver>` ordering
regardless of which worker fulfilled the item).

---

### 8. File registry

- **Spec (this file)**: `docs/superpowers/specs/2026-06-01-kesseldb-spperfa-shard-scan-pool-scaleout-design.md`
- **Tracker**: `docs/superpowers/specs/2026-06-01-kesseldb-spperfa-shard-scan-pool-scaleout-progress.md`
- **Pool bound bump**: `crates/kesseldb-server/src/scatter_scan.rs` (`POOL_BOUND` const + `ScatterPool::new`)
- **KAT**: `crates/kesseldb-server/src/scatter_scan.rs::tests::pool_high_concurrency_*`
- **BENCHMARKS §14b POST-SCALEOUT update**: `docs/BENCHMARKS.md`
- **STATUS entry**: `docs/STATUS.md`

---

### 9. Standing rules

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-poolscale`
- Direct commits to main, no Co-Authored-By, no `-S`, push after each
- CI green check after each push
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched
- `#![forbid(unsafe_code)]` honored
- No new external deps
- Default `cargo build -p kesseldb-server` byte-identical
- K-invariance must still hold byte-equal
