# KesselDB — Subproject 155 (SP-A): cross-shard scatter scan / filter reads — DESIGN

**Status:** design — closes the OLDEST open TaskList ticket (#75 "SP-A: cross-shard scatter scan/filter reads (fan-out + ordered merge)") as **DESIGNED**, not implemented. A future executing session reads this cold and ships T0..T13.

**Builds on:**
- `kessel-shard` (M4 SP78 — `ShardMap` rendezvous routing, single-shard groundwork).
- `crates/kesseldb-server/src/router.rs` (SP78 + SP80 + SP81 — point-op routing, schema broadcast, deterministic cross-shard **writes** via XshardApply / XshardDecide / XshardCommit + global sequencer).
- `Op::Select` / `Op::QueryRows` / `Op::SelectFields` / `Op::SelectSorted` / `Op::Aggregate` / `Op::GroupAggregate` (the per-shard read ops the router already supports for single-shard cases — the building blocks SP-A fans out).
- `kessel-expr` (compiled WHERE bytecode — already serializable, already pure-deterministic, already pushdown-shaped).

**Arc context (SP96 assessment, verbatim from STATUS.md §"What this is NOT (yet)"):**

> cross-shard scatter-gather *reads* / SQL-text routing (distinct from cross-shard *transactions*, which are delivered — now **scoped**: SP96 assessment slices this into **SP-A scan-fanout → SP-B aggregate combine → SP-C sorted k-way merge → SP-D group merge → SP-E SQL-text routing**; cross-shard `Join` and a cross-shard consistent snapshot are explicit documented non-goals)

This document is SP-A only (scan-fanout). SP-B/C/D/E are scoped at the end (§11) as feed-forward.

**Process note.** Per `feedback_cani_autonomous_build` (mandate substitution) + `feedback_kesseldb_autonomous_build`, brainstorm gate substituted: the user explicitly said "write a proper design spec so a future session can execute". This is a DESIGN ticket, not an implementation ticket.

---

## 1. Problem

### 1.1 What the router does today

`crates/kesseldb-server/src/router.rs` (SP78) routes by *operation*:

- **Point ops** (`Create`/`Update`/`Delete`/`GetById`) → exactly one shard (the rendezvous-hash owner of the 20-byte key).
- **Schema/DDL** → broadcast to every shard (identical catalogs ⇒ deterministic per-shard execution).
- **Single-shard `Txn`** → that shard.
- **Cross-shard `Txn`** → SP80/SP81 deterministic Calvin-style commit (sequencer + decide + commit).
- **Everything else** (`Select`, `QueryRows`, `SelectFields`, `SelectSorted`, `Aggregate`, `GroupAggregate`, `FindBy`, `FindByComposite`, `Join`, …) → `Route::Unsupported("router (multi-shard, this slice) handles point ops, DDL, and single/rejected-cross transactions; scatter-gather reads and SQL text are a later slice")`.

That `Unsupported` branch is the SP-A gap. In production today, **any non-point read returns a clean error in a K>1 deployment**. K=1 (single shard) trivially works because every key is on shard 0 — but a K>1 deployment is read-locked-out for the bulk of the workload.

### 1.2 Why scatter scan matters

A scan-without-key (e.g. `SELECT * FROM acct WHERE balance < 0`) cannot be routed by key — the predicate doesn't constrain the storage key. With K shards, every row in the result is on *some* shard, and we don't know which until we look. There are three ways out:

1. **Reject** (today). Honest but unshippable for any K>1 deployment with non-point reads.
2. **Send to one shard arbitrarily**. WRONG ANSWER. The shard sees only its 1/K of the table.
3. **Fan out to every shard, merge results.** Correct. This is SP-A.

### 1.3 Latency / throughput cliff this creates

For a 100M-row table sharded K=4:

- Serial dispatch (one shard at a time, even within the router): **4× the per-shard scan latency**. A 4s per-shard scan is 16s wall-clock. Worse for K=8/16.
- Parallel fan-out: **~max(per-shard latencies)** + merge overhead. With 4 shards, that's roughly the per-shard scan time (4s) — **4× speedup**, and the scan is throughput-bound on each shard's local I/O independently.
- Filter pushdown matters even more: a `WHERE balance < 0` that touches 0.01% of rows should NOT ship 100M rows back to the router. Each shard runs the predicate locally, ships only the survivors. The current per-shard `Select`/`QueryRows` already does this — SP-A just composes it.

### 1.4 The non-trivial bits

It is not "fan out and concat". Three sub-problems:

- **Ordered merge.** `SelectSorted` returns rows in `(sort_field, object_id)` order *per shard*. The merged result across shards must be in the SAME total order. That's a heap-based k-way merge across per-shard streams, NOT concatenation.
- **Filter pushdown.** WHERE bytecode is already compiled to a portable `Vec<u8>` (kessel-expr); ship the same bytecode to each shard. Aggregate WHERE is the same shape. Already done at the per-shard level — SP-A's job is to NOT decompile/re-route.
- **LIMIT + cancellation.** `SELECT … LIMIT 100` across 4 shards: pull from each, stop the others as soon as 100 are buffered. Don't pull the entire scan from every shard "just in case". This needs a per-shard cancellation channel.

---

## 2. Goals and non-goals

### 2.1 Goals (V1 = SP-A only)

- **Parallel fan-out**: simultaneous per-shard dispatch (NOT round-robin sequential).
- **Ordered merge**: k-way merge across shards for `SelectSorted` (and any future ORDER BY surface) producing byte-identical output to a "single fat shard" baseline.
- **Filter pushdown**: the WHERE bytecode runs on each shard. Only surviving rows are shipped to the router.
- **LIMIT cancellation**: once `limit` rows are accumulated at the router, in-flight per-shard scans are signalled to stop and their result-buffers dropped.
- **Bounded buffers**: each per-shard stream has a bounded in-memory queue. Skewed shards (one shard has 90% of the matches) don't OOM the router.
- **Deterministic merge order**: ties broken by `(shard_id, object_id)` — stable, reproducible, replay-safe.
- **Zero new dependencies**: std-thread + std::sync::mpsc (or std::sync::Mutex<VecDeque>) — NO tokio, NO rayon. Per `feedback_kesseldb_zero_dep` (and the kernel's existing stance — the workspace has 0 non-Rust-std runtime deps).
- **Surfaces covered**: `Select`, `QueryRows`, `SelectFields`, `SelectSorted` (the four core scan ops). `Aggregate` / `GroupAggregate` are scoped to **SP-B / SP-D** (NOT this slice — see §11).

### 2.2 Non-goals (named, deferred)

- **`Aggregate` / `GroupAggregate` combine** — SP-B (per-shard partial → router final combine for COUNT/SUM/MIN/MAX) and SP-D (cross-shard GROUP BY group-merge). Separate spec.
- **`Join` across shards** — explicit documented non-goal (STATUS.md §"What this is NOT yet" + ARCHITECTURE.md §Sharding). Cross-shard join needs a distributed snapshot or per-shard broadcast/shuffle; out of arc.
- **`FindBy` / `FindByComposite` across shards** — these are equality lookups on an indexed field; the index is per-shard, so fan-out is still required. SP-A's machinery handles this trivially (same `Vec<u8>` shape), but the spec lists them as a follow-up T-task (T11) to keep the V1 surface tight.
- **SQL-text routing** (parse SQL at the router, derive routing) — SP-E. SP-A operates at the `Op::` level, BELOW SQL parsing.
- **Cross-shard consistent snapshot** — distinct from a scatter read. A scatter read can return rows that were committed on different shards at different per-shard opnums (each shard is its own VSR group). Documented honestly: "scatter reads are eventually-per-shard consistent, NOT cross-shard snapshot-consistent". The MVCC SI work (S2 arc) is per-shard. **Non-goal here.**
- **Distributed transactions** — already handled by SP80/SP81 for writes. Reads are simpler (no write conflicts to resolve); SP-A is read-only.
- **Elastic resharding** — `ShardMap::new(K)` is set at boot; changing K mid-flight is a separate concern (rendezvous hashing minimizes remap, but the data-movement is a future spec).
- **Per-shard MVCC snapshot pinning** — a scatter read at "time T" cannot snapshot every shard at the same global T (no global clock; per-shard opnums are independent). Documented; SP-A inherits per-shard MVCC and reports the row-set as the union of per-shard snapshots taken at request-arrival.

---

## 3. Architecture

### 3.1 Where the dispatch sits

**Decision: the router.** Specifically inside `crates/kesseldb-server/src/router.rs`, in a new helper method `Conn::scatter_read(&mut self, op: &Op) -> OpResult`, dispatched from `forward` when `route(op)` returns the new `Route::Scatter(...)` variant.

Alternatives considered:

- **A new crate `kessel-scatter`**: REJECTED. The router already owns per-shard `ClusterClient`s, the `Conn` state, and the `Route` decision. Splitting scatter logic into a new crate doubles the surface for negligible benefit.
- **A shared SM-layer helper**: REJECTED. Scatter is a router-level concern. Each shard's SM (kessel-sm) sees only its own keyspace — by design. The SM apply layer MUST NOT be involved in cross-shard reads (each shard is an independent VSR group; deterministic replay is per-shard).
- **A new `Op::ScatterScan` variant**: NO — at the wire layer, the scatter is transparent. The router translates a `Select` from the client into K parallel `Select`s to the shards. The shards see the SAME `Op::Select` they would see today; the *router* is what's new. **Wire-compat for clients.** A `Select` issued against a K=1 deployment behaves identically; against K=4, the router does the fan-out and the client never knows.

### 3.2 The new `Route` variant

```rust
enum Route {
    One(usize),
    All,
    Cross(Vec<usize>),
    Refresh,
    /// SP-A: scatter to every shard, ordered merge at router.
    /// `kind` discriminates merge strategy:
    ///   - Unordered (Select / QueryRows / SelectFields) — concatenate respecting limit
    ///   - Sorted { field, desc } (SelectSorted) — k-way heap merge
    Scatter(ScatterKind),
    Unsupported(&'static str),
}

enum ScatterKind {
    Unordered { limit: u32 },
    Sorted { sort_field: u16, desc: bool, offset: u32, limit: u32 },
}
```

`route(op)` is extended:

```rust
Op::Select { limit, .. }
| Op::QueryRows { limit, .. }
| Op::SelectFields { limit, .. } => Route::Scatter(ScatterKind::Unordered { limit: *limit }),
Op::SelectSorted { sort_field, desc, offset, limit, .. } =>
    Route::Scatter(ScatterKind::Sorted { sort_field: *sort_field, desc: *desc, offset: *offset, limit: *limit }),
Op::Aggregate { .. } | Op::GroupAggregate { .. } | Op::FindBy { .. } | Op::FindByComposite { .. } | Op::Join { .. } =>
    Route::Unsupported("scatter scope is SP-A (Select/QueryRows/SelectFields/SelectSorted); Aggregate=SP-B, GroupAggregate=SP-D, FindBy=T11 follow-up, Join=non-goal"),
```

### 3.3 Fan-out mechanism (zero-dep)

Per the zero-dep stance: **std::thread + std::sync::mpsc** + a `Drop`-channel for cancellation. NO tokio.

```rust
use std::sync::mpsc::{sync_channel, Sender, Receiver};
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

struct ShardStream {
    rx: Receiver<ShardChunk>,
    cancel: Arc<AtomicBool>,
    handle: thread::JoinHandle<()>,
}

enum ShardChunk {
    Rows(Vec<u8>),       // [u32 rowlen][record]* — same format the per-shard Op returns
    EndOfStream,
    Err(String),
}
```

Each scatter request spawns **K worker threads** (where K = `self.router.shards()`), one per shard. Each worker:

1. Owns a `ClusterClient` reference (cloned from the connection's `Conn::clients[i]`).
2. Issues the SAME `Op` to its shard via `client.call(op)`. (Per-shard limit is bumped — see §3.4 below.)
3. On reply, parses the rowset and sends `ShardChunk::Rows(...)` to its bounded channel.
4. On `EndOfStream` or error, signals and exits.
5. On `cancel.load(SeqCst) == true`, drops the in-flight reply if any and exits.

The router then drives the merge (§3.5).

**Why threads not async:** The per-shard `ClusterClient::call` is synchronous and blocking (TCP `read_frame` / `write_frame`). K shards × 1 blocking call each = K threads. K is small (typical deployments: K=4, K=8, K=16). Thread creation cost is ~10us; per-shard scan latencies are ms+. The async win would be ~0; the dependency cost (tokio = 200+ transitive crates) would be enormous. Zero-dep wins by an order of magnitude on every axis.

**Thread lifecycle:** Threads are spawned per-request, NOT pooled. Per-request cost ≈ K × thread-spawn ≈ 80us at K=8. Acceptable. A future micro-optimization could thread-pool (T12), but V1 is simpler.

### 3.4 Filter pushdown

**Already free.** Every per-shard scan op (`Select`, `QueryRows`, `SelectFields`, `SelectSorted`) carries a compiled `program: Vec<u8>` (the kessel-expr WHERE bytecode). The router ships the IDENTICAL op to every shard. Each shard's apply runs the bytecode against its own rows.

**Per-shard limit bump.** For an unordered `SELECT … LIMIT N` over K shards, the router cannot know which shard has the matching rows. It cannot send each shard `LIMIT N/K` (a shard with more matches than that would under-return). It cannot send each shard `LIMIT N` (each shard might return N matches; then the router gets K×N rows back to truncate to N).

V1 strategy: **send `LIMIT N` to every shard** (worst-case K×N rows transit the network in flight), the router accumulates rows and STOPS pulling as soon as it has N. With the cancellation channel (§3.3), in-flight shard scans whose chunks the router stops draining will see their `sync_channel(bound=4)` block, AND the cancel flag will be checked between chunks. A future spec (SP-A follow-up T13) can add adaptive per-shard limits via re-pull cycles, but V1 is correct and bounded.

For `SelectSorted` with `OFFSET m LIMIT n` — every shard must return `OFFSET 0 LIMIT m+n` rows (the router does the offset in the merge phase, since rows are interleaved across shards). Per-shard skip-the-first-m is NOT correct in a sorted merge: shard A's rows 0..m and shard B's rows 0..m can be in arbitrary order in the merged stream.

### 3.5 Ordered merge (the heap)

For `ScatterKind::Sorted { sort_field, desc, offset, limit }`:

The router maintains a `BinaryHeap<(Reverse<SortKey>, shard_id, RowBytes)>` (with `Reverse` to make it a min-heap; for `desc=true`, flip the wrapper).

Algorithm:

```rust
let mut heap: BinaryHeap<(SortKeyOrd, usize, Vec<u8>)> = BinaryHeap::new();
let mut streams: Vec<ShardStream> = spawn_all();
let mut emitted = 0u32;
let mut skipped = 0u32;
let mut out: Vec<u8> = Vec::new();

// Prime: one row from each shard
for i in 0..K {
    if let Some(row) = streams[i].next_row()? {
        heap.push((sort_key_of(&row, sort_field, desc), i, row));
    }
}

while let Some((_key, shard_i, row)) = heap.pop() {
    if skipped < offset { skipped += 1; }
    else {
        out.extend_from_slice(&(row.len() as u32).to_le_bytes());
        out.extend_from_slice(&row);
        emitted += 1;
        if limit > 0 && emitted >= limit { break; }
    }
    // Refill from the same shard
    if let Some(next_row) = streams[shard_i].next_row()? {
        heap.push((sort_key_of(&next_row, sort_field, desc), shard_i, next_row));
    }
}

// Cancel remaining streams
for s in &streams { s.cancel.store(true, SeqCst); }
// Drain in finite time + join (or detach with a timeout)
for s in streams.into_iter().flat_map(|s| s.handle.into_iter()) { let _ = s.join(); }

OpResult::Got(out)
```

**Tie-breaking (determinism):** if two rows have equal `sort_field` value across shards, the heap order is `(sort_field, object_id)` — same tiebreak as single-shard `SelectSorted`. `object_id` is part of the row payload (always at field-0 by encoding convention). The merge tiebreak is `(value, oid)` NOT `(value, shard_id)` — `shard_id` is NOT involved, because that would create an implementation-detail ordering. **Two different K values must yield the same answer.**

### 3.6 Unordered merge

For `ScatterKind::Unordered { limit }`:

The router pulls rows from each shard concurrently and appends to a single output buffer until `limit` is reached (or all streams ended).

Order considerations:
- For `Select` / `QueryRows` / `SelectFields` — single-shard returns rows in *insertion order* (storage key order). A scatter merge MUST also be deterministic. **Decision:** the merge order is `shard_0 rows, then shard_1 rows, ...` — strict shard-id order, NOT arrival order. (Arrival order is non-deterministic and would break replay/test reproducibility.)
- This costs latency (we cannot emit shard_1's rows until shard_0 is fully drained at the limit), so V1 uses a per-shard buffer per shard and drains them in shard-id order at the merge stage. Pull-side parallelism is preserved (workers are running); only the merge consumer is sequential.

Concretely: each shard worker fills its bounded `sync_channel(bound=4)`. Merge consumer reads shard_0 to completion (or LIMIT), then shard_1, etc.

**Caveat:** under tight LIMIT (say `LIMIT 10` across 4 shards), the merge consumer might never read from shard_2 / shard_3. They sit blocked on the channel, eventually see the cancel flag, and exit. **Correct, deterministic, no wasted shard-side I/O after cancel.**

### 3.7 Cancellation

```rust
struct ShardStream {
    cancel: Arc<AtomicBool>,
    rx: Receiver<ShardChunk>,
    handle: thread::JoinHandle<()>,
}

impl Drop for ShardStream {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::SeqCst);
        // The handle is not joined here (would block in Drop); merge
        // already joined post-LIMIT. A drop-without-join is the panic
        // path — threads finish on their own + don't leak (cancel flag).
    }
}
```

Workers check `cancel.load(Ordering::SeqCst)` between chunks (cheap relaxed load) and exit cleanly. The pending TCP response from the shard is consumed and discarded; the client connection stays usable for the next request.

**Honest gap (named):** a shard worker mid-`read_frame` cannot be interrupted (std::net::TcpStream has no cancellable read). The worker is stuck until the shard finishes sending. Mitigation: per-shard `read_timeout` on the TCP stream (default 30s — open question §10). For most workloads, scans finish in seconds; cancel-then-wait is fine.

### 3.8 Bounded buffers (skew defense)

`sync_channel(bound=4)`. Each chunk is up to `max_chunk_rows` rows (default 1024 rows). At K=8 shards × 4 chunks × 1024 rows × ~200 bytes/row, worst-case router-side buffer ≈ 6 MiB. Acceptable.

**Why bound=4 not 0:** bound=0 (rendezvous channel) over-serializes. bound=∞ OOMs under skew. bound=4 is the sweet spot — workers can prefetch a chunk or two ahead of the consumer.

**Result-size cap:** `max_response_size = 64 MiB` (matching the kessel-proto wire frame cap). Each shard chunks at `<= 16 MiB` per `ShardChunk::Rows`. Router-side total cap = `max_response_size` regardless of K; a `Bad("scatter result exceeds 64 MiB")` is returned with no partial result.

---

## 4. Wire shape

### 4.1 No new Op variant

The client sends an existing `Op::Select` / `Op::QueryRows` / `Op::SelectFields` / `Op::SelectSorted`. The router translates it to K parallel sends of the SAME `Op` to each shard. The shard sees nothing new.

**Wire-compat:** zero. Clients (kessel-client, Python SDK, http-gateway) need no changes.

### 4.2 Optional new Op variant: `Op::ScatterScan` — REJECTED

We considered:

```rust
Op::ScatterScan { type_id, filter: Vec<u8>, sort: Option<SortKey>, limit: u32 }
```

REJECTED because:
- Duplicates existing surface (`Select` + `SelectSorted`).
- Forces clients to know they're in a sharded deployment.
- Breaks the SP78 router invariant ("multi-shard is transparent to point-op clients") for reads.
- Creates a wire-version migration burden.

The router does the work. Clients send what they always sent.

### 4.3 Response shape

UNCHANGED. The merged result is `OpResult::Got([u32 rowlen][record]*)` — byte-identical to what a single fat shard would return. This is **the** correctness criterion: a scatter scan over K=4 shards must produce byte-identical output to a scatter scan over K=1 of the same dataset.

### 4.4 Per-shard chunking (NEW, internal)

For very large per-shard results, the shard already returns the entire result in one `OpResult::Got(...)` reply. SP-A introduces NO new per-shard chunking — the per-shard scan ops return whole results today (capped by `limit`), and that's fine for V1.

A future spec ("SP-A follow-up T14") can add per-shard *streaming* — `Op::SelectChunked` — for arbitrarily large scans without per-shard buffering. Out of scope here.

---

## 5. Deterministic implications

### 5.1 The SM apply layer is NOT involved

Each shard's SM (kessel-sm) is an independent VSR replicated state machine. **Reads do not enter the apply log.** A `Select` op is read-only — it runs against the SM's committed state at the time the leader receives it, with no log append. (This is already how per-shard reads work today; see `kessel-sm`'s read path.)

Scatter scan inherits this property: each per-shard read is a per-shard local read. The router's merge is a pure function of K per-shard results, NOT a replicated operation.

### 5.2 Determinism boundary

Per-shard reads ARE deterministic with respect to the shard's committed log prefix. The router's merge is a deterministic function of (K per-shard results, sort_field, desc, limit, offset). Therefore:

- **Same query, same per-shard data ⇒ byte-identical result** (replay-safe).
- **Different K with the same total data ⇒ same row-set, same order** (this is the property test, §7.2).

The ONLY non-determinism is the per-shard data itself — and that's already deterministic per-shard via VSR. SP-A introduces NO new determinism risk.

### 5.3 Cross-shard snapshot is NOT consistent

A scatter read can see rows committed on shard A at opnum_a=100 and rows on shard B at opnum_b=200 (those are independent counters). **There is no cross-shard "snapshot at time T".** The per-shard MVCC SI work (S2 arc) is per-shard — it does NOT extend across shards.

Documented honestly in the result semantics: "a scatter read returns the union of per-shard committed states as observed at request-arrival on each shard. There is no cross-shard consistent snapshot." Future spec ("cross-shard consistent snapshot") is an explicit non-goal of SP-A and the SP-A/B/C/D/E arc as a whole.

### 5.4 Merge order is a pure function of inputs

`object_id` tiebreak (NOT shard_id) guarantees that two deployments — K=4 and K=8 — sharding the SAME rows differently produce the same merged result for the same query. Test in §7.2.

---

## 6. Failure modes

| Failure | Behaviour |
|---|---|
| **Shard timeout** (per-shard `read_timeout` exceeded) | Configurable: `scatter_partial_on_timeout` (default `false`). False ⇒ hard fail `OpResult::Unavailable` (clean, retryable). True ⇒ return what we have with a `Got([..])` and a warning header (FUTURE — V1 ships `false` only). |
| **Shard unavailable** (`ClusterClient::call` returns `Err`) | Hard fail `OpResult::Unavailable`. NO zombie partial result. Cancel all other shard workers. |
| **Shard returns oversized payload** (>16 MiB per chunk OR cumulative >64 MiB) | `OpResult::SchemaError("scatter result exceeds 64 MiB; tighten LIMIT or WHERE")`. NO partial result. Cancel all workers. |
| **Shard returns malformed row stream** (wrong frame, bad row-length prefix) | Treated identically to a malformed reply from any single-shard op today: `OpResult::SchemaError("shard {i}: malformed scatter response")`. All workers cancelled. |
| **Router itself fails mid-merge** (e.g. OOM) | Drop unwinds all `ShardStream`s; cancel flags fire; threads exit. Client sees a connection drop, retries via `ClusterClient` rotation (already implemented in kessel-client). |
| **Shard count mismatch** (router thinks K=4, actually K=3) | Caught at router boot in `Router::new`; doesn't reach scatter path. |
| **Cancellation during long-running shard scan** | Worker checks `cancel` flag between chunks; mid-read, blocked on TCP. Per-shard TCP `read_timeout` kicks in. |
| **One shard returns 0 rows, others return many** | Trivially handled — that shard's worker emits `EndOfStream` immediately; the heap simply has fewer entries. |
| **All shards return 0 rows** | `OpResult::Got([0,0,0,0])` (empty row count). Correct. |
| **Cumulative result exceeds operator-configured `max_scatter_response_bytes`** | Typed `Bad("scatter response exceeds operator cap")`. Configurable via env / Router builder. |

---

## 7. Test plan

### 7.1 Multi-shard localhost integration (the headline test)

Spin up 4 in-process shards (`kessel-sm` instances over `kessel-sim` test transport, à la existing cross-shard write tests), router on top, inject a known dataset of 10k rows distributed by rendezvous hash (~2500 rows per shard), execute:

- `Op::Select { program: compile("balance < 0"), limit: 0 }` → assert the result row count matches a brute-force filter over the test dataset.
- `Op::SelectSorted { sort_field: balance_field, desc: false, offset: 100, limit: 50 }` → assert byte-identical to a `SelectSorted` run against the SAME dataset on a single K=1 shard.
- `Op::SelectFields { program: …, fields: [name_field, balance_field], limit: 10 }` → assert projection works through scatter.

### 7.2 Property: scatter on N shards == scatter on 1 shard

For random datasets of 1000 rows and random K ∈ {1, 2, 4, 8, 16}: a `SelectSorted` query must produce **byte-identical** results across all K choices. This is the killer correctness test for the merge.

Implementation: parameterize the test harness over K, run the same query against each, hash the result, assert all hashes equal.

### 7.3 LIMIT cancellation

`Op::Select { program: ALWAYS_TRUE, limit: 10 }` over 4 shards each holding 100k rows. Assert:
- Result has exactly 10 rows.
- Total shard-side rows scanned is bounded (each shard scanned `<= 10 + per-shard-buffer` rows, not all 100k). Measurable via per-shard scan-counter instrumentation.
- All worker threads exit cleanly (no leaks).

### 7.4 Skew defense

Dataset where 99% of matches are on one shard. Assert:
- Router-side memory stays bounded (instrumented via `Vec::capacity` tracking).
- Result is correct (matches a single-shard ground truth).

### 7.5 Pentest suite

| Pentest | Attack | Expected |
|---|---|---|
| `sp_a_pt_hostile_shard_timeout` | One shard's response thread `thread::sleep(60s)` | `OpResult::Unavailable` after `scatter_per_shard_timeout` (default 30s); other workers cancelled cleanly |
| `sp_a_pt_hostile_shard_oversized` | One shard returns a 1 GiB response | `OpResult::SchemaError("scatter result exceeds 64 MiB")`; no router OOM |
| `sp_a_pt_hostile_shard_malformed_rows` | One shard returns malformed row-length prefix (claims 4 GiB row) | `OpResult::SchemaError("shard {i}: malformed scatter response")`; no panic |
| `sp_a_pt_hostile_shard_partial_then_close` | One shard returns half a row then closes the connection | `OpResult::Unavailable`; no panic |
| `sp_a_pt_shard_dies_mid_scan` | One shard's connection drops mid-stream | `OpResult::Unavailable`; cancel propagates; thread joins |
| `sp_a_pt_router_drop_under_limit` | Client drops connection mid-LIMIT-pull | All worker threads exit within 5s; no leak |
| `sp_a_pt_cancel_atomic_visibility` | Workers see `cancel.store(true)` before next chunk | Stress test: 1000 iterations, assert all workers exit within 100ms of cancel |
| `sp_a_pt_zero_shards` | K=0 edge case | Caught at `Router::new`; doesn't reach scatter |
| `sp_a_pt_one_shard` | K=1 (degenerate scatter) | Byte-identical to direct shard call; one worker thread, trivial merge |
| `sp_a_pt_determinism_replay` | Same query 100× | Byte-identical result every time (NO shard-id-based ordering leaks) |

### 7.6 SQL-text smoke (forward-looking)

Even though SQL-text routing is SP-E, the http-gateway / SQL driver currently lowers a SQL statement to an `Op::Select` / `Op::SelectSorted`. Run a smoke through that path on a K=4 deployment:

- `SELECT * FROM acct WHERE balance < 0` → routes to scatter, returns merged rows.
- `SELECT * FROM acct WHERE balance < 0 ORDER BY balance LIMIT 100` → scatter + sorted merge + limit.

This confirms the user-facing surface lights up without SP-E being done. (SP-E will add SQL-text-level routing for queries that CAN be routed to one shard by the WHERE — an optimisation, not a correctness fix.)

---

## 8. Task decomposition (T0..T13)

| Task | Scope | Estimate (LoC delta) |
|---|---|---|
| **T0** | Brainstorm gate substitution + this design spec read + plan sign-off (autonomous) | 0 |
| **T1** | Extend `Route` enum with `Scatter(ScatterKind)`. Extend `route(op)` to return it for the four scan ops. Wire to a NEW `Conn::scatter_read` stub that returns `OpResult::SchemaError("SP-A T2 incomplete")`. Existing `Unsupported` branch is retired for these ops. Tests: route-decision unit tests for each scan op. | ~120 |
| **T2** | The `ShardStream` plumbing: spawn K worker threads, `sync_channel(bound=4)`, `Arc<AtomicBool> cancel`. Worker loop = single `client.call(op)` + parse + send chunk. Thread join + cancel on Drop. Tests: K=4 dummy scatter (each shard returns 0 rows) → router gets `Got([])`; assert all threads joined. | ~180 |
| **T3** | Unordered merge (`ScatterKind::Unordered`): drain shard_0 to LIMIT, then shard_1, etc. Tests: 4 shards × N rows, verify merged result == concatenation in shard-id order; LIMIT short-circuit works; cancel propagates. | ~140 |
| **T4** | Sorted merge (`ScatterKind::Sorted`): `BinaryHeap<(SortKey, shard_i, RowBytes)>`. Sort-key extraction from row bytes (re-use catalog field metadata). `desc` flips heap polarity. OFFSET + LIMIT in the merge loop. Tests: byte-identical to K=1 baseline. | ~220 |
| **T5** | Determinism property test: random 1k-row datasets, K ∈ {1,2,4,8,16}, query is `SelectSorted` — assert hash-identical results across K. The killer correctness check. | ~120 |
| **T6** | LIMIT cancellation correctness: per-shard scan counter, assert `scanned <= limit + buffer_slack` per shard. Tests: 4 shards × 100k rows, LIMIT 10, assert total scanned < 1000. | ~80 |
| **T7** | Skew defense + bounded buffers: one-shard-has-99% test. Assert router memory bounded via `Vec::capacity` probe. | ~80 |
| **T8** | Pentest sweep (10 pentests from §7.5). Hostile shards via test transport injecting timeouts/oversized/malformed responses. | ~300 |
| **T9** | Error-path completeness: `OpResult::Unavailable`, `SchemaError`, partial-result guard. Tests: every failure-mode row in §6. | ~150 |
| **T10** | Documentation: `docs/ARCHITECTURE.md §Sharding` updated; `docs/STATUS.md` row; `crates/kesseldb-server/src/router.rs` doc-comment paragraph; the open-limitation paragraph ("scatter-gather reads … later slice") REMOVED from STATUS.md. | ~80 |
| **T11** | Follow-up: extend `Route::Scatter` to `FindBy` / `FindByComposite`. Trivial — same fan-out, no merge (concat). Pentest. | ~100 |
| **T12** | (Optional) Thread-pool the workers (current V1 spawns per-request). Measure: at K=8 + 100 QPS, is thread-spawn overhead measurable? Only ship this if yes. | ~150 |
| **T13** | (Optional, performance) Adaptive per-shard LIMIT for unordered scatter: send `LIMIT N/K * 2` first, re-pull if needed. Reduces wasted network for small LIMITs over many shards. | ~200 |

**Total V1 (T1-T10):** ~1370 LoC. **With T11:** ~1470. Plausible for a 2-3 session arc.

---

## 9. Acceptance criteria

A future session is **DONE with SP-A** when:

1. **Correctness:** the property test (§7.2) passes for K ∈ {1, 2, 4, 8, 16} on a random 1k-row dataset.
2. **Wire-compat:** an unmodified `kessel-client` from a session BEFORE SP-A can issue `Op::Select` / `Op::SelectSorted` against a K=4 router and get a correct merged result.
3. **Failure modes:** all 10 pentests in §7.5 pass.
4. **Determinism:** seed-7 partition corpus (across all shards in a K=4 deployment) is green; no per-shard determinism regressions.
5. **Performance proof (operator-runnable):** a benchmark over a 4-shard deployment shows scatter scan of a 100k-row table is **3-4× faster** than serial round-robin dispatch of the same op to each shard (current SP-A-absent fallback would be N/A since today it's `Unsupported`; the comparison is against a hypothetical serial-dispatch implementation, NOT the rejection). Specifically: a `Select` over 100k rows with `limit=0`:
   - Serial: K × per-shard scan latency.
   - Parallel (SP-A): ≈ max(per-shard scan latencies) + merge overhead (~5%).
   - Assert: parallel < 0.4 × serial for K=4. (Headline benchmark documented in `docs/PERFORMANCE.md`.)
6. **Memory bound:** under skew (one shard has 99% of matches), router-side peak memory is bounded by `K × buffer_per_shard + result_buffer`. Assert via instrumented test.
7. **STATUS.md:** the "cross-shard scatter-gather reads / SQL-text routing" open-limitation paragraph is updated — SP-A is REMOVED from the open list; SP-B/C/D/E remain.
8. **ARCHITECTURE.md:** §Sharding has a new sub-section "Cross-shard reads (SP-A)" describing the fan-out, the merge, and the snapshot non-property.
9. **CI green** on the workspace; **seed-7** unchanged; **kernel zero-dep** (verify with `cargo tree --no-default-features | wc -l` — unchanged).

---

## 10. Open questions (for the executing session)

| # | Question | Default if not resolved |
|---|---|---|
| OQ1 | Default per-shard timeout? Current `ClusterClient` uses 30s | 30s — make configurable via `Router::with_scatter_per_shard_timeout(Duration)` |
| OQ2 | `scatter_partial_on_timeout` — ship in V1 or punt? | Punt. V1 ships hard-fail only. T9 keeps the type signature ready for a future flag |
| OQ3 | Per-shard chunk size cap (default 16 MiB)? | 16 MiB. Match wire-frame cap minus headers |
| OQ4 | Aggregate combine (SP-B) — start immediately after SP-A, or wait for SP-A in production? | Wait. Get SP-A green + benchmarks + a real workload first; SP-B's math depends on SP-A's per-shard partial-result shape |
| OQ5 | Should sort_field that doesn't exist in the catalog error at the router or at the shard? | Router pre-validates (cheap call to `Op::Describe` shard 0 once, cached) — fast-fail before fanning out |
| OQ6 | Cross-shard `Aggregate(COUNT(*))` — SP-A or SP-B? | SP-B. COUNT(*) needs a per-shard partial-then-sum, which IS the SP-B pattern. Documented in T1's `Unsupported` reason |
| OQ7 | `FindBy(field_id=indexed_field)` — V1 (T11) or wait for SP-B? | V1 follow-up T11. It's a degenerate scatter (no merge, just concat), trivial to add |
| OQ8 | Per-shard `SelectSorted` ordering vs. heap-merge: does each shard's reply already include the sort-field value parseable from the row blob? | YES — rows are full record encodings; the catalog tells us the field offset. But: confirm during T4 that field-offset parsing is feasible without a full row decode. If not, ship a new `Op::SelectSortedWithKey` that prepends the sort-key |
| OQ9 | Test transport for pentests (§7.5) — extend kessel-sim, or run real localhost TCP? | Localhost TCP with a hostile-shard test-double process (matches existing cross-shard write test style) |
| OQ10 | Should the router pre-warm its per-shard `ClusterClient`s on boot or lazy on first request? | Lazy (current behavior). First-request latency penalty (~K × TCP handshake) is acceptable; pre-warm is a micro-opt |
| OQ11 | What does `OpResult::Got([])` mean? Empty result, or `OpResult::NotFound`? | `OpResult::Got([])` — empty result. Matches per-shard `Select` semantics (an empty filter result is `Got([])`, not `NotFound`) |
| OQ12 | Header / version field in the merged response? | NO. The response is byte-identical to single-shard. Wire-compat trumps debuggability. Add a `--debug` mode in a follow-up if needed |

---

## 11. Feed-forward: SP-B / SP-C / SP-D / SP-E

For the future executor who reads this and asks "what's next after SP-A":

| Sub | Scope | Why scoped here |
|---|---|---|
| **SP-B — Aggregate combine** | `Op::Aggregate { kind: COUNT/SUM/MIN/MAX, … }` across shards. Each shard returns its partial. Router combines: COUNT → sum partials; SUM → sum partials; MIN → min of partials; MAX → max of partials. AVG is COUNT+SUM with a final divide. Result is byte-identical to single-shard. ~200 LoC | Reuses SP-A's fan-out plumbing 1:1; only the merge function differs. Trivial after SP-A. |
| **SP-C — Sorted merge over indexes** | `SelectSorted` with an ordered index — push the ordered scan into each shard, then heap-merge AS streams (not buffered result blobs). Lower memory at very large result sizes. ~250 LoC | Performance optimization, not a correctness gain. Honest deferral. |
| **SP-D — GROUP BY combine** | `Op::GroupAggregate` across shards. Each shard returns its grouped partials. Router merges by group-key, combining aggregates per-group. ~300 LoC | Builds on SP-B's combine functions + a hash-merge across shards. |
| **SP-E — SQL-text routing** | Router parses SQL at the wire layer (kessel-sql.parser path). For queries that CAN route to one shard by the WHERE (e.g. `WHERE id = ?` over a sharded-by-id table), skip the fan-out. ~200 LoC | Performance optimization. Without it, every SQL query that lowers to `Op::Select` does a fan-out — correct but wasteful for point-shaped queries. |

After SP-A through SP-E ship, the STATUS.md "What this is NOT yet" entry collapses to just **cross-shard `Join`** and **cross-shard consistent snapshot** — both explicit non-goals at the arc level.

---

## 12. Self-review (against autonomous mandate)

### 12.1 What's currently shipped in `kessel-shard`?

**`ShardMap` only.** 121 LoC. Rendezvous-hash key→shard mapping + `is_cross_shard(a,b)` helper. No scan logic, no fan-out, no merge. **Truly greenfield** for SP-A — the existing crate provides the routing primitive, the SP-A work lives in `kesseldb-server/src/router.rs`.

### 12.2 What's `Op::XshardApply` doing?

**Cross-shard WRITES only.** It's part of the SP80 Calvin-style deterministic write protocol:
- `SeqAppend` / `SeqAppendOnce` — global sequencer assigns a monotonic seq to a cross-shard transaction descriptor.
- `XshardApply { seq, ops }` — original SP80 mechanism: each shard applies its slice idempotently at the global seq, cursor advances.
- `XshardDecide { seq, ops }` — SP81 phase-1: dry-run, persist stable verdict, apply nothing.
- `XshardCommit { seq, ops, commit }` — SP81 phase-2: apply iff decided commit, advance cursor either way.

**Different from SP-A.** SP-A is reads. SP-A doesn't touch the sequencer, doesn't append to any cross-shard log, doesn't enter any apply layer. The connection is purely lexical: both involve K shards. The mechanics are entirely separate.

The router today contains `commit_cross_shard` (for writes) — SP-A adds `scatter_read` alongside it. No code reuse beyond the per-shard `ClusterClient` pool.

### 12.3 Existing `Route::Unsupported` for scatter reads

`route()` returns `Unsupported("router (multi-shard, this slice) handles point ops, DDL, and single/rejected-cross transactions; scatter-gather reads and SQL text are a later slice")` for the four scan ops + Aggregate + GroupAggregate + Join + FindBy. **That's the SP-A gap, exactly as STATUS.md scoped it.** SP-A retires this rejection for the four scan ops; SP-B/C/D retire it for Aggregate/GroupAggregate; SP-E for SQL-text; Join + cross-shard snapshot stay rejected forever (non-goals).

### 12.4 Key open questions flagged for the future executor

The 12 in §10. The high-impact ones:

- **OQ1/OQ2**: timeout policy and partial-result-on-timeout. V1 ships strict (no partials). Defaults are operator-tunable.
- **OQ4**: SP-B timing — wait for SP-A in production before designing SP-B's partial-result shape.
- **OQ7**: should `FindBy` ship in SP-A (T11) or wait for SP-B? Recommendation: T11.
- **OQ8**: sort-key parseability from the row blob — needs a quick T4 spike. If blocked, fall back to a new `Op::SelectSortedWithKey` that the shard prepends the sort-key bytes to each row.
- **OQ12**: response header / version. NO — wire-compat trumps debuggability.

### 12.5 Where I'd push back if I were the future executor

- The decision to not introduce `Op::ScatterScan` (§4.2) might feel uncomfortable. The justification (wire-compat, transparency) is right, but the executor should verify that no per-shard `Op::Select` has hidden assumptions about "this is the only Select for this query" (e.g. cursor state). I scanned the SM apply path; no such state exists. SHOULD HOLD up. If it doesn't, add `Op::ScatterScan` and move on.
- The `bound=4` channel size is a tuning guess. T7 (skew defense) should sweep it and confirm.
- The thread-per-request model (vs pool) is V1-simple. Under high QPS, T12 may be necessary. Defer the call to the executor.

---

## 13. References

- `docs/STATUS.md` lines 353-357 (SP-A/B/C/D/E scoping).
- `docs/ARCHITECTURE.md` §"Sharding & cross-shard transactions" lines 108-149.
- `crates/kessel-shard/src/lib.rs` (121 LoC, `ShardMap`).
- `crates/kesseldb-server/src/router.rs` (1665 LoC; `Route::Unsupported` for scan ops at line ~185; `commit_cross_shard` at line ~304).
- `crates/kessel-proto/src/lib.rs` (`Op::Select` / `SelectSorted` / `QueryRows` / `SelectFields` definitions; `OpResult::Got` response shape).
- `crates/kessel-expr/src/lib.rs` (compiled WHERE bytecode — the unit that flows shard-side intact).
- `docs/superpowers/specs/2026-05-24-mvcc-cutover-s2-6-continuation-sp116-brainstorm.md` (per-shard MVCC context — relevant to §5.3 non-snapshot semantics).
- This spec: `docs/superpowers/specs/2026-05-26-kesseldb-spa-cross-shard-scatter-scan-design.md`.

---

*End of SP155 design. Ready for a future session to read cold and execute T0..T13.*
