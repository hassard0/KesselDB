## SP-Perf-A-SHARD-SCAN — in-process scatter-merge for scan ops — design spec

Date: 2026-05-30
Parent: SP-Perf-A-SHARD-APPLY (shipped K=N apply path at 14.93M ops/sec
at K=8 on vulcan, 3.19× over the SP-Perf-A T7 ~5M ceiling).

This arc fixes the **scan-correctness gap** SHARD-APPLY explicitly named:
the sharded engine routes point ops by `hash(primary_key) % K`, which
spreads rows across all K shards. But scan ops have NO primary key —
they iterate the whole table. Today SHARD-APPLY routes every scan to
**shard 0 only**, which means a `SELECT *` on a K=4 deployment misses
75% of the data.

This is the production-correctness fix.

---

### 1. Context — the gap SHARD-APPLY left open

`sharded_engine.rs` (the SHARD-APPLY routing layer) ships
`route_op(&Op, k) -> ShardRoute` with three buckets:

- `Single(s)` — point ops (Create/Update/Delete/GetById/GetBlob,
  per-type ops like FindBy/FindRange/Describe). Routes by primary
  key → owning shard.
- `Broadcast` — DDL ops. Applies to every shard sequentially.
- `ShardZero` — **everything that doesn't fit**: Select, QueryRows,
  SelectFields, SelectSorted, Aggregate, GroupAggregate,
  GroupAggregateMulti, Query, QueryExpr, Join, Txn, XSHARD admin
  frames. V1 sends them all to shard 0.

The `ShardZero` bucket is wrong for scan ops at K>=2: shard 0 sees
~1/K of the data (its 1/K hash slice), so a scan reports 1/K of the
true result. SHARD-APPLY documented this honestly (`sharded_engine.rs`
module-doc §3) and named SHARD-SCAN as the production-correctness fix.

The 12 scan ops in V1 scope:

| Op | Shape | Per-shard reply |
|---|---|---|
| `Op::Select` | filtered scan, LIMIT | `[u32 rowlen][record]*` |
| `Op::QueryRows` | filter+predicate, LIMIT | `[u32 rowlen][record]*` |
| `Op::SelectFields` | projection, LIMIT | `[u32 rowlen][projected]*` |
| `Op::SelectSorted` | sort+OFFSET+LIMIT | `[u32 rowlen][record]*` (sorted) |
| `Op::Aggregate` | COUNT/SUM/MIN/MAX/AVG | `[i128 LE]` (or raw bytes for var-MIN/MAX) |
| `Op::GroupAggregate` | per-group COUNT/SUM/MIN/MAX/AVG | `[u32 ngroups][[u32 keylen][key][i128 result]]*` |
| `Op::GroupAggregateMulti` | per-group, multiple aggs | `[u32 ngroups][[u32 keylen][key][16B * n_aggs]]*` |
| `Op::FindBy` | secondary-index eq lookup | `[16B oid]*` |
| `Op::FindByComposite` | composite-index eq lookup | `[16B oid]*` |
| `Op::FindRange` | range scan | `[16B oid]*` |
| `Op::Query` | predicate AND list | `[16B oid]*` |
| `Op::QueryExpr` | program filter | `[16B oid]*` |

**Op::Join is intentionally out of scope** (cross-shard join needs a
build-side broadcast or shuffle; that's its own SHARD-JOIN arc).
SHARD-APPLY's `route_op` still classifies Join as `ShardZero` after
this arc — Join works at K=1 only and returns wrong results at K>=2
(documented limitation). The new in-process scan router INTENTIONALLY
does NOT touch Op::Join.

---

### 2. Approach — reuse SP-A's scatter machinery in-process

The SP-A cluster router already solved cross-shard scatter for the
network-attached cluster path (`scatter_scan.rs`, ~4300 lines of
thoroughly-tested fan-out + merge logic). It exposes the contract via
a `ShardCaller` trait whose `call(&op) -> Result<OpResult, String>`
is the only thing the merge layer needs.

SP-A is wired in `router.rs` as `impl ShardCaller for ClusterClient`
(network-attached TCP shard). SHARD-SCAN wires it for the
in-process sharded engine as `impl ShardCaller for InProcShardCaller`
(direct call into `EngineHandle::apply_op`, zero network).

**Same machinery, same merge contract, just a different transport.**

```rust
// In sharded_engine.rs (this arc):
pub struct InProcShardCaller {
    engine: EngineHandle,  // The sub-engine for this shard
}

impl ShardCaller for InProcShardCaller {
    fn call(&mut self, op: &Op) -> Result<OpResult, String> {
        // Direct in-process dispatch — no TCP, no serialization.
        // The sub-engine has the SP-Perf-A T6 read fast path
        // enabled, so this is a single RwLock read.
        Ok(self.engine.apply_op(op))
    }
}
```

Then in `ShardedDispatcher::apply_raw` (today's `ShardRoute::ShardZero`
arm for scan ops), we replace `self.shards[0].apply_raw(frame)` with
`scatter_and_merge(N InProcShardCallers, op, timeout, ScatterKind::*, cancel)`.

---

### 3. New ScatterKind variants — aggregate merge

The existing `ScatterKind` has three variants — `Unordered`, `Sorted`,
`OidConcat`. They cover 9 of the 12 scan ops. The remaining 3 —
`Aggregate`, `GroupAggregate`, `GroupAggregateMulti` — need new merge
strategies because their results are partial aggregates that must be
combined, not just concatenated.

**`ScatterKind::AggregateMerge { kind, agg_field_kind, agg_field_width }`**:

- **kind=0 (COUNT)**: sum the per-shard i128 counts ⇒ single i128.
- **kind=1 (SUM)**: sum the per-shard i128 sums ⇒ single i128.
- **kind=2 (MIN)**: min of per-shard values. For numeric ≤8B, decode
  per-shard i128 + take min. For var-width MIN (CHAR/BYTES/U128/I128),
  the per-shard reply is raw bytes — use `cmp_field` of the kind.
- **kind=3 (MAX)**: same as MIN, with max.
- **kind=4 (AVG)**: **HONEST GAP**: the per-shard reply is `sum / count`,
  which is NOT enough information to compute the global average
  (you can't average averages without weighting by count). Cross-shard
  AVG would need the per-shard wire reply to include `(sum, count)`
  separately. **V1 SHARD-SCAN returns SchemaError** for `Op::Aggregate
  { kind: 4 }` at K>=2 and documents the limitation; SHARD-SCAN-AVG (a
  follow-up arc) would change the wire shape of the agg reply to fix
  this. (The kind=0..3 cases work correctly; kind=4 was always a thin
  layer over kind=1+kind=0 anyway.)

**`ScatterKind::GroupAggregateMerge { kind, agg_field_kind, agg_field_width }`**:

- Per-shard reply is `[u32 ngroups][[u32 keylen][key][i128 result]]*`.
- Merge: parse each shard's groups, accumulate by group key. The
  combination function depends on `kind`:
  - kind=0 (COUNT): sum per-group counts
  - kind=1 (SUM): sum per-group sums
  - kind=2 (MIN), kind=3 (MAX): same per-group with min/max
  - kind=4 (AVG): same SchemaError as Aggregate.
- Output: re-encode as `[u32 ngroups][[u32 keylen][key][i128 result]]*`.
- Group order: per-shard results are emitted via `BTreeMap`-ordered
  iteration (kessel-sm uses BTreeMap), so the merge can use the same
  order. To stay byte-identical to K=1, the merged output groups MUST
  also be in sorted-by-key order.

**`ScatterKind::GroupAggregateMultiMerge { aggregates: Vec<(u8, u16)> }`**:

- Same as `GroupAggregateMerge` but each group carries N aggregate
  results instead of one. Per-shard reply is `[u32 ngroups][[u32 keylen]
  [key][16B * n_aggs]]*`.
- Each `aggregates[i].0` is the kind for slot i; merge slot-wise by
  the same rules. AVG slots (kind=4) hard-fail with SchemaError.

---

### 4. Routing changes in `sharded_engine::route_op`

The existing classifier returns `ShardZero` for all 12 scan ops. We
add a new `ShardRoute::Scatter(ScatterKind)` variant and re-classify:

```rust
pub enum ShardRoute {
    Single(usize),
    Broadcast,
    ShardZero,
    Scatter(ScatterKind),  // NEW
}
```

Routing changes:

| Op | Pre-arc route | Post-arc route |
|---|---|---|
| `Op::Select { limit, .. }` | `ShardZero` | `Scatter(Unordered { limit })` |
| `Op::QueryRows { limit, .. }` | `ShardZero` | `Scatter(Unordered { limit })` |
| `Op::SelectFields { limit, .. }` | `ShardZero` | `Scatter(Unordered { limit })` |
| `Op::SelectSorted { sort_field, desc, offset, limit, .. }` | `ShardZero` | `Scatter(Sorted { ... })` *(with width=0 sentinel)* |
| `Op::Aggregate { kind, field_id, .. }` | `ShardZero` | `Scatter(AggregateMerge { kind, field_id })` *(field width resolved at dispatch)* |
| `Op::GroupAggregate { kind, .. }` | `ShardZero` | `Scatter(GroupAggregateMerge { kind })` |
| `Op::GroupAggregateMulti { aggregates, .. }` | `ShardZero` | `Scatter(GroupAggregateMultiMerge { aggregates })` |
| `Op::FindBy { .. }` | `Single(per-type-pin)` *(SHARD-APPLY)* | `Scatter(OidConcat)` *(THIS IS A CHANGE — the per-type pin was a hack)* |
| `Op::FindByComposite { .. }` | `Single(per-type-pin)` | `Scatter(OidConcat)` |
| `Op::FindRange { .. }` | `Single(per-type-pin)` | `Scatter(OidConcat)` |
| `Op::Query { .. }` | `ShardZero` | `Scatter(OidConcat)` |
| `Op::QueryExpr { .. }` | `ShardZero` | `Scatter(OidConcat)` |
| `Op::Join { .. }` | `ShardZero` | `ShardZero` *(unchanged — non-goal V1)* |
| `Op::Txn { .. }` | `ShardZero` | `ShardZero` *(SHARD-XTXN handles this)* |

**Note on per-type pinning regression**: SHARD-APPLY's design pinned
all rows of a given `type_id` to the same shard (via `hash((type_id, 0))`)
specifically so per-type scans could be served by that one shard. Once
we have scatter-merge for `FindBy/FindRange/Describe`, the per-type pin
isn't needed for correctness — and removing it lets *every* type's
rows distribute across shards (better load balancing for workloads
that hit one hot type heavily).

**V1 keeps the per-type pin** (because removing it requires re-hashing
the routing for Create/Update/Delete/GetById on per-type-pinned ops,
which would invalidate any existing on-disk shard layout). The
per-type pin becomes RIDUNDANT but not wrong — FindBy/FindRange of
a per-type-pinned type still produces correct results via the scatter
path (every shard answers, but K-1 shards return empty payloads).

This is OK for correctness, costs a small fixed overhead for queries
against pinned types. SHARD-APPLY-2 (follow-up) can lift the pin.

---

### 5. Determinism + K-invariance

The SP-A scatter machinery already guarantees per-K determinism
(shard-id-ordered merging, deterministic tie-breaks). The new arc
extends this to the in-process router with the same guarantees.

**K-invariance** (the load-bearing oracle):

- For `Op::Select / QueryRows / SelectFields` with a deterministic
  hashing of records across shards, the K=1 result and K=N result
  must be **multiset-equal** (same records, possibly different order
  because the merge is shard-id-ordered concat).
- For `Op::SelectSorted`, the K=1 result and K=N result must be
  **byte-identical for unique sort keys** (tie-break ambiguity for
  duplicate sort values is the SP-A T2 §5.4 caveat — documented).
- For `Op::Aggregate kind=0,1,2,3` (COUNT/SUM/MIN/MAX), K=1 and
  K=N must produce byte-identical results.
- For `Op::GroupAggregate / GroupAggregateMulti kind=0,1,2,3`, K=1
  and K=N must produce byte-identical results (groups are sorted
  by key in both paths).
- For `Op::FindBy / FindByComposite / Query / QueryExpr / FindRange`,
  K=1 and K=N must be **multiset-equal** on the returned oid set.
- For `Op::Aggregate kind=4` (AVG), K=N returns SchemaError; K=1
  returns the correct value. **Asymmetric — documented in
  BENCHMARKS / progress tracker.**

---

### 6. Acceptance criteria

1. All 12 scan-shape Op KATs pass at K=4 + K=8 on the in-process
   sharded engine.
2. K-invariance oracle (new): for each scan op + a 100-row seeded
   workload, K=1 and K=N produce byte-equal (Sorted) or multiset-equal
   (Unordered/OidConcat) results.
3. `cargo test --workspace` continues to pass.
4. vulcan bench: scan throughput should LIFT at K>1 (previously
   bound to shard 0). Specifically:
   - YCSB-A (50/50): writes already scaled with SHARD-APPLY; now
     scans also fan out → marginal improvement on mixed workload
     (writes dominate).
   - TPC-H Q6 (SUM): scan was bound to shard 0 = ~25% of data on K=4.
     With scatter, every shard contributes its slice; correctness +
     throughput both fixed.
5. `#![forbid(unsafe_code)]` honored; no new external deps.
6. Default `cargo build` byte-identical (route_op classification
   change only activates when K>=2; K=1 paths are unaffected).

---

### 7. Task decomposition (T1-T5)

| T# | Scope |
|---|---|
| **T1** | This design spec + `InProcShardCaller` scaffold in `sharded_engine.rs` + new `ScatterKind::AggregateMerge / GroupAggregateMerge / GroupAggregateMultiMerge` variants in `scatter_scan.rs` + their merge functions + unit KATs. |
| **T2** | Sharded engine routes scan ops via `scatter_and_merge` with N InProcShardCallers. New `ShardRoute::Scatter(ScatterKind)` variant + the routing-table swap. Integration KATs at K=4/K=8 for all 12 scan ops. |
| **T3** | K-invariance oracle: 100-row seeded workload × 12 scan variants × K∈{1,4,8} byte/multiset-equal assertion. |
| **T4** | vulcan bench: YCSB-A/B sweep + TPC-H Q6 + BENCHMARKS update. |
| **T5** | Arc closure: STATUS, BENCHMARKS, progress tracker, TaskList ready. |

---

### 8. File registry

- **Spec (this file)**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-scan-design.md`
- **Tracker**: `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-scan-progress.md`
- **Scaffold + routing**: `crates/kesseldb-server/src/sharded_engine.rs`
- **New ScatterKind variants + merges**: `crates/kesseldb-server/src/scatter_scan.rs`
- **K-invariance oracle**: `crates/kesseldb-server/src/sharded_engine.rs` (test mod)

---

### 9. Standing rules

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-shardscan`
- Direct commits to main, no Co-Authored-By, no `-S`, push after each
- CI green check after each push
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched
- `#![forbid(unsafe_code)]` honored
- No new external deps
- Default `cargo build -p kesseldb-server` byte-identical
- K-invariance must hold byte-equal (no correctness regression)
