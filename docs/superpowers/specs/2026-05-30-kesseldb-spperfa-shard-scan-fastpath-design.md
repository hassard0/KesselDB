## SP-Perf-A-SHARD-SCAN-FASTPATH — persistent thread pool for in-process scatter — design spec

Date: 2026-05-30
Parent: SP-Perf-A-SHARD-SCAN (V1 SHIPPED — correctness fix for 12 scan
ops at K>=2, but DONE_WITH_CONCERNS for performance shape).

This arc closes the perf regression V1 left open: `find-by` at K=4 is
**180× slower** than K=1 (1,805,405 → ~10K ops/sec — `aggregate-sum`
regressed too at K=8, and `select-limit` regressed even at K=4). The
root cause is well-understood and named in BENCHMARKS §14:

> per-request 4-thread-spawn + scatter overhead = ~1500µs vs ~500ns
> direct path

`std::thread::spawn` is ~250µs per thread (the kernel allocates a stack,
sets up TLS, registers in the scheduler). For K=4 that's ~1000µs of
fixed overhead per scatter call — three orders of magnitude bigger
than the actual indexed-lookup work for `find-by`.

---

### 1. Approach A chosen as V1 of FASTPATH; B as the V2 safety net

**Approach A (this arc): persistent worker pool.** Replace per-call
`std::thread::Builder::new().spawn(...)` with N long-lived worker
threads that block on a `sync_channel(1)` waiting for work. Per-call
overhead drops from ~1500µs (thread spawn) to ~5-10µs (channel send +
recv + condvar wake). Workers stay alive for the lifetime of the
`ScatterPool` (one pool per `ShardedDispatcher`, i.e. one per server
process).

**Approach B (deferred unless A doesn't recover find-by):** detect
"tiny op" (FindBy with primitive value, expected result set <10) and
run sequentially across shards on the calling thread. Total time =
K × per-op cost (≈ 4µs at K=8 for an indexed lookup) which beats even
pool dispatch overhead. Predicate: `is_tiny_scan(op) -> bool` returns
true for FindBy/FindByComposite where the (type_id, field_id) is an
equality-indexed column and the value is fully specified. Sequential
walk on the dispatcher thread skips the pool entirely.

Ship A first. Measure. If find-by recovers to within 2× of K=1 baseline,
B is not needed. Otherwise add B in a follow-up commit.

---

### 2. ScatterPool design

```rust
pub struct ScatterPool {
    // One worker per shard, indexed by shard_id. Each worker thread
    // blocks on its own sync_channel waiting for `WorkItem`s.
    worker_txs: Vec<mpsc::SyncSender<WorkItem>>,
    // Worker JoinHandles, kept so Drop can shut down the pool cleanly
    // by closing every tx (worker sees RecvError, exits loop, returns).
    workers: Vec<thread::JoinHandle<()>>,
}

struct WorkItem {
    // The caller for THIS dispatch (boxed because we hand a different
    // ShardCaller to the worker every dispatch — the dispatcher owns
    // the Arc<EngineHandle>s and constructs a fresh InProcShardCaller
    // per call to avoid worker-side mutation aliasing).
    caller: Box<dyn ShardCaller>,
    op: Op,
    cancel: Arc<AtomicBool>,
    // Reply channel — the dispatcher creates a fresh (tx, rx) per
    // scatter call and ships the tx with each work item. The worker
    // sends exactly one OpResult per work item.
    reply_tx: mpsc::SyncSender<OpResult>,
}
```

Worker loop:

```rust
fn worker_loop(work_rx: mpsc::Receiver<WorkItem>) {
    while let Ok(mut item) = work_rx.recv() {
        let r = match item.caller.call_with_cancel(&item.op, &item.cancel) {
            Ok(r) => r,
            Err(_) => OpResult::Unavailable,
        };
        let _ = item.reply_tx.send(r);
    }
    // RecvError = pool dropped; clean exit.
}
```

Dispatcher (replaces today's `scatter_and_merge_ctx` thread-spawn block):

```rust
fn dispatch_via_pool<C: ShardCaller>(
    pool: &ScatterPool,
    callers: Vec<C>,  // dispatcher-supplied, one per shard
    op: &Op,
    cancel: Arc<AtomicBool>,
) -> Vec<mpsc::Receiver<OpResult>> {
    let mut rxs = Vec::with_capacity(callers.len());
    for (i, caller) in callers.into_iter().enumerate() {
        let (tx, rx) = mpsc::sync_channel(1);
        let item = WorkItem {
            caller: Box::new(caller),
            op: op.clone(),
            cancel: cancel.clone(),
            reply_tx: tx,
        };
        // Send to worker `i` — blocks if worker is still processing a
        // previous request (sync_channel(1) bound). In practice the
        // dispatcher waits for ALL replies before issuing the next
        // batch, so the channel is empty by then.
        pool.worker_txs[i].send(item).expect("pool worker died");
        rxs.push(rx);
    }
    rxs
}
```

The rest of `scatter_and_merge_ctx` (drain replies, merge, cancel
laggards) is unchanged — the pool just replaces the spawn-per-call
machinery with channel-send-per-call.

---

### 3. Determinism preserved

- Each worker `i` has a stable `shard_id = i`. The dispatcher hands
  the per-shard caller to the worker indexed by that shard_id, so
  worker 3 always handles shard 3's reply.
- Replies are merged in shard-id order (the dispatcher iterates
  `rxs[0..K]` sequentially per the existing merge contract). No
  arrival-order semantics.
- K-invariance preserved byte-equal — the merge happens after every
  worker replies, exactly as today.

---

### 4. Lifecycle

- **Created** lazily on first scatter call (constructor takes K = number
  of shards). One pool per `ShardedDispatcher`. The pool lives as
  long as the dispatcher (i.e. for the server's lifetime).
- **Dropped** when the `ShardedDispatcher` drops: `Drop` for
  `ScatterPool` closes every `worker_tx` (drops the Sender), which
  causes each worker's `recv()` to return `Err(RecvError)`, the worker
  exits its loop and the JoinHandle joins cleanly.

The pool itself stores `Vec<mpsc::SyncSender<WorkItem>>` and
`Vec<JoinHandle<()>>`. Dropping the Senders before joining the
handles is required (otherwise the workers block forever on `recv`).

---

### 5. Sharing the pool

The pool is created with N = K workers (matching shard count). The
dispatcher's `scatter_dispatch` borrows the pool and routes the K
per-shard work items in. No per-call thread allocation; no per-call
JoinHandle bookkeeping.

The pool is wrapped in `Arc` and shared with every clone of
`SharedDispatcher` (it already was — `EngineHandle` holds
`Arc<ShardedDispatcher>`). The pool's `Vec<SyncSender>` is internally
mutable across threads because each `SyncSender` is `Sync` (the bound
sync_channel sender is shareable).

---

### 6. Acceptance criteria

1. `find-by` at K=4 lifts from ~10K ops/sec to **≥500K ops/sec** (at
   least 50× recovery — half of the baseline). Ideal: within 2× of
   K=1 (≥900K ops/sec).
2. `find-by` at K=8 lifts proportionally (less aggressive target:
   ≥250K ops/sec — 25× recovery).
3. `aggregate-sum` at K=8 should also lift (currently 1,293 vs K=1
   1,480; expect to clear the K=4 number 1,748).
4. `select-limit` at K=4 should lift (currently 1,903 vs K=1 2,549;
   expect parity or small lift).
5. K-invariance oracle (`t3_shard_scan_k_invariance_oracle_12_ops`)
   stays GREEN — no semantic change.
6. All existing scatter_scan KATs (~40+) stay GREEN — the pool is
   a drop-in replacement for the spawn-per-call path.
7. `cargo build --workspace` byte-identical default profile.
8. `#![forbid(unsafe_code)]` honored.
9. Zero new external deps (std::thread + std::sync::mpsc only).

---

### 7. Task decomposition (T1-T4)

| T# | Scope |
|---|---|
| **T1** | This design spec + `ScatterPool` scaffold in `scatter_scan.rs` + Drop impl + unit KATs (pool spawns N workers, dispatch returns rxs in shard order, Drop joins cleanly). |
| **T2** | Wire `ScatterPool` into `ShardedDispatcher` — replace `scatter_and_merge` call with pool-backed dispatch. New `scatter_and_merge_via_pool` helper preserving the same merge contract. Existing per-call `scatter_and_merge` path stays for the cluster router (still needs spawn-per-call because per-shard TCP clients are not pool-sharable). |
| **T3** | vulcan bench: YCSB-A/C + find-by sweep. BENCHMARKS §14 POST-FASTPATH column. |
| **T4** | Arc closure: STATUS, BENCHMARKS, progress tracker → CLOSED. |

---

### 8. File registry

- **Spec (this file)**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-scan-fastpath-design.md`
- **Tracker**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-scan-fastpath-progress.md`
- **Pool scaffold**: `crates/kesseldb-server/src/scatter_scan.rs` (new `ScatterPool` struct + worker loop)
- **Dispatcher wire-up**: `crates/kesseldb-server/src/sharded_engine.rs` (ScatterPool owned by dispatcher; scatter_dispatch routes via pool)

---

### 9. Standing rules

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-shardscanfast`
- Direct commits to main, no Co-Authored-By, no `-S`, push after each
- CI green check after each push
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched
- `#![forbid(unsafe_code)]` honored
- No new external deps (no rayon)
- Default `cargo build -p kesseldb-server` byte-identical
- K-invariance must still hold byte-equal
