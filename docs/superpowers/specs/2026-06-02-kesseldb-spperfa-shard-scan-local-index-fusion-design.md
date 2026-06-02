# SP-Perf-A-SHARD-SCAN-LOCAL-INDEX-FUSION — Design

Date: 2026-06-02
Status: DESIGN (T1)
Parent: SP-Perf-A-SHARD-SCAN-POOL-SCALEOUT (V1 SHIPPED 2026-06-01)
Sibling diagnosis: SP-Perf-A-SHARD-SCAN-TINY-INLINE forensics
(BLOCKED — concluded the 41% K=4 find-by gap is the structural
floor for the current sharding model: there's no "primary key
field" concept to enable single-shard routing for FindBy).

## Context

POST-FASTPATH+POOL-SCALEOUT find-by numbers (vulcan, --pool-workers
16, --workers 16, 100K rows, 10s, --shard-count {1,4,8}):

| Workload | K=1 | K=4 | K=8 |
|---|---|---|---|
| find-by | 1,810K ops/s | 1,067K ops/s | 844K ops/s |
| K-vs-K=1 gap | — | **41%** | **53%** |

The TINY-INLINE forensics correctly diagnosed the *structural* floor:
FindBy probes a secondary equality index; without a "primary key
field" concept, every shard must be queried because rows of the same
type-id spread across shards by `hash(type_id, oid) % K`. So
single-shard routing is not available, and the dispatcher legitimately
must walk K shards.

But the *implementation* of the walk has slack. `scatter_serial`
currently calls:

```rust
for shard in &self.shards {
    let r = shard.apply_op(op);
    gathered.push(r);
}
```

`shard.apply_op(op)` — when the sub-engine was spawned with
`read_workers = Some(_)` — DOES already take the SP-Perf-A T6 in-process
fast path: `sm_shared.read().read_only_op(op.clone())`. **BUT** when
the bench is run *without* `--pool-workers` (the documented
POST-SCALEOUT command path), `cfg.read_workers = None`, which
propagates to every sub-engine via `sub_cfg.clone()` in
`spawn_sharded_engine_cfg`. With `read_workers = None`, the sub-engine
has `sm_shared = None`, so `apply_op` falls through to
`apply_raw(op.encode())` — which incurs:

  - `Op::encode()` → `Vec<u8>` alloc
  - `inflight.fetch_add` atomic CAS
  - `sync_channel(1)` send to engine thread
  - engine thread `Op::decode()`
  - engine thread `RwLock.write()` (because the writer path holds the
    write guard for the whole batch)
  - `apply_one()` (which for read-only ops still does the same logical
    work as `read_only_op`, but under the wrong guard kind)
  - sync_channel reply send
  - dispatcher recv
  - `inflight.fetch_sub`

Per the FASTPATH design, every step is ~µs-scale, and at K=4 with 16
workers contending for 4 sub-engine apply threads (each holding its
write guard for the batch), serialization kicks in further.

## The optimization (V1 scope)

Two complementary fixes:

1. **Force `sm_shared` to be populated on every sub-engine** by setting
   `sub_cfg.read_workers = Some(0)` in `spawn_sharded_engine_cfg`. With
   `Some(0)`, `perfa_enabled = true` so the SM is wrapped in
   `Arc<RwLock<>>`; the read pool is constructed with 0 workers
   (graceful fall-through, no real worker threads). **Net cost on
   sub-engines**: one Arc<RwLock> wrapper around the SM (already
   exercised by the K=1 read-workers code path), no extra threads.

2. **`scatter_serial` borrows the shared SM directly**, bypassing
   `apply_op`'s sharded.is_some() branch + `is_read_only` classifier
   recursion + op_kind_counts atomic bump + the redundant
   `sm_shared.is_some()` check (we already know it is — we just
   populated it). The dispatch becomes:

```rust
for sm_shared in &self.shard_sms {
    let g = sm_shared.read().expect("rwlock");
    let r = g.read_only_op(op.clone());
    if !matches!(r, OpResult::Got(_)) { return r; }
    gathered.push(r);
}
```

Equivalent to `apply_op` for FindBy/FindByComposite (both routes call
`StateMachine::read_only_op` against the same SM under a read guard),
but with 4-5 fewer instructions per shard and one fewer Arc clone (the
sm_shared field is owned by the dispatcher, not refcount-cloned from
`EngineHandle::sm_shared()` per call).

### Out of scope (V1)

- Extending direct-borrow to the pool-driven scatter path (Approach A
  from FASTPATH = the parallel pool). Pool dispatch is still useful
  for ops where per-shard work is non-trivial (`select-*`,
  `aggregate-*`) — those benefit from real parallelism on multi-core,
  whereas tiny FindBy is dominated by overhead. Promoting the parallel
  pool to direct-borrow is a separate slice if SCAN-FUSION ships
  meaningful lift and the same shape applies upstream.
- Changing `apply_op` itself. The optimization touches only
  `ShardedDispatcher::scatter_serial`; every other dispatch site is
  byte-untouched.

## Acceptance

| Criterion | Target |
|---|---|
| find-by K=4 ops/sec | ≥ 1.2M (≥12% lift over POST-SCALEOUT 1.07M) |
| find-by K=4 stretch | ≥ 1.4M (77% of K=1) |
| find-by K=8 | no regression vs POST-SCALEOUT 0.84M |
| K-invariance oracle (`t3_shard_scan_k_invariance_oracle_12_ops`) | byte/multiset-equal stays GREEN |
| `cargo test --workspace` | GREEN |
| Default `cargo build` byte-identical | YES — `sm_shareds` field only constructed when `shard_count >= 2` |
| `#![forbid(unsafe_code)]` honored | YES |
| No new external runtime deps | YES |
| Wire surfaces (HTTP/1.1 + WS + binary + PG-wire) byte-untouched | YES |
| K-invariance preserved | YES — same merge contract (`merge_scan_results` with same ScatterKind) |

## Implementation slices

- **T1** (this doc) — design spec + scaffold `shard_sms` field on
  `ShardedDispatcher` populated from `EngineHandle::sm_shared()`. KATs
  prove the field is Some for every shard when bench spawn path runs.
- **T2** — `scatter_serial` direct-borrow path: if every `shard_sms[i]`
  is Some, dispatch via `read_only_op` directly; else fall back to
  existing `apply_op` channel path. Equivalence KAT proves byte-equal
  output between fast and channel paths.
- **T3** — vulcan bench: 3 trials × K ∈ {1,4,8} for find-by, capture
  POST-FUSION column in BENCHMARKS §14d.
- **T4** — STATUS row, progress tracker → CLOSED, TaskList #363.

## K-invariance

`scatter_serial` already collects results in shard-id order (the `for
shard in &self.shards` walk). Direct-borrow path preserves the same
order — `for sm_shared in &self.shard_sms` walks the same shard-id
order. Merge step is unchanged (`merge_scan_results(gathered, kind)`).
Byte-equal output to the previous path.

## Risk

- **`spawn_sharded_engine_cfg` ownership shape change**: forcing
  `sub_cfg.read_workers = Some(0)` changes the SM ownership shape on
  sub-engines from inline owned `StateMachine` to
  `Arc<RwLock<StateMachine>>`. This is the same code path that
  `read_workers = Some(N)` exercises — it has been GREEN at K=1 for
  weeks (SP-Perf-A T2 onward). KAT coverage already proves the
  switchover at K=1.
- **Write throughput**: writes still go through the per-shard apply
  thread, which now uses `Arc<RwLock<>>` write guard. Same shape as
  K=1 `read_workers = Some(_)` write path; no new regression vector.

## Forward references

If FUSION ships and find-by K=4 hits ≥1.2M, the next arc is
**SHARD-SCAN-PARALLEL-FUSION** which promotes the parallel pool
(Approach A) to direct-borrow for non-tiny scans, targeting
`select-*` / `aggregate-*` lift. Out of scope for V1.
