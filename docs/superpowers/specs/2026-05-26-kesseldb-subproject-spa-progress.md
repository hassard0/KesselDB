# SP-A — Cross-shard scatter scan / filter reads — SP-arc Progress Tracker

Date created: 2026-05-26
Design spec: `docs/superpowers/specs/2026-05-26-kesseldb-spa-cross-shard-scatter-scan-design.md`
TaskList: closes the OLDEST open ticket **#75 "SP-A: cross-shard scatter scan/filter reads (fan-out + ordered merge)"**.

## What this SP-arc ships

A K-shard deployment can issue any `Op::Select` / `Op::QueryRows` /
`Op::SelectFields` / `Op::SelectSorted` against the router and get a
single byte-identical merged result. Today (pre-SP-A) the router
returns `OpResult::SchemaError("…scatter-gather reads and SQL text are
a later slice")` for any of these against K≥2 — i.e. K>1 deployments
are read-locked-out for non-point reads. SP-A unlocks it.

**Out-of-scope (named, deferred):** SP-B aggregate combine, SP-C
streamed sorted merge, SP-D GROUP BY combine, SP-E SQL-text routing,
cross-shard `Join`, cross-shard consistent snapshot. See spec §11.

## Slice plan (mirrors design spec §8)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T0** | Baseline: seed-7 PASSES, design spec read | DONE | (no-code) |
| **T1** | Scaffold the router-side helper (`scatter_scan.rs` module): `ShardCaller` trait + `scatter_scan_fanout` (std::thread per shard, per-shard timeout) + `merge_scan_results` stub + 9 KATs. The helper is callable today; the call-site wiring lands in T2. | **DONE** | `195ecd6` |
| T2 | `Route::Scatter(ScatterKind)` variant + `route()` returns it for the 4 scan ops + `Conn::scatter_read` driver + sorted k-way heap merge | OPEN | — |
| T3 | Unordered merge (shard-id-ordered concat respecting LIMIT) | OPEN | — |
| T4 | Sort-key extraction from row bytes + OFFSET+LIMIT in the merge loop | OPEN | — |
| T5 | Determinism property test: K∈{1,2,4,8,16} hash-identical merged result | OPEN | — |
| T6 | LIMIT cancellation correctness + cancel flag | OPEN | — |
| T7 | Skew defense + bounded buffers | OPEN | — |
| T8 | Pentest sweep (10 pentests from spec §7.5) | OPEN | — |
| T9 | Error-path completeness + partial-result guard | OPEN | — |
| T10 | Documentation: ARCHITECTURE.md sub-section + STATUS.md row update | OPEN | — |
| T11 | (Follow-up) Extend to `FindBy` / `FindByComposite` | OPEN | — |
| T12 | (Optional perf) Thread-pool the workers | OPEN | — |
| T13 | (Optional perf) Adaptive per-shard LIMIT | OPEN | — |

## T1 — what landed (2026-05-26, commit `195ecd6`)

**New module:** `crates/kesseldb-server/src/scatter_scan.rs` (~330 LoC
incl. tests + doc-comments).

**Public surface:**

```rust
pub trait ShardCaller: Send + 'static {
    fn call(&mut self, op: &Op) -> Result<OpResult, String>;
}

pub const DEFAULT_PER_SHARD_TIMEOUT: Duration = Duration::from_secs(30);

pub fn scatter_scan_fanout<C: ShardCaller>(
    shards: Vec<C>,
    op: &Op,
    per_shard_timeout: Duration,
) -> Vec<OpResult>;

pub fn merge_scan_results(results: Vec<OpResult>) -> OpResult;
```

**9 KATs (all passing):**

| KAT | Asserts |
|---|---|
| `fan_out_to_one_shard_returns_that_shards_result` | K=1 degenerate; byte-identical to a direct `client.call` |
| `fan_out_to_three_shards_returns_three_results_in_shard_order` | Deterministic shard-id ordering, NOT arrival order — locks SP155 §3.6 |
| `a_shard_that_times_out_returns_unavailable_for_that_slot` | SP155 §6 "Shard timeout" → V1 hard-fail to `Unavailable` |
| `fan_out_to_empty_shards_returns_empty_vec` | K=0 edge; no threads spawned, no panic |
| `fan_out_preserves_scan_filter_predicates` | SP155 §3.4 — every shard sees the byte-identical `Op`; sorted-merge determinism depends on this |
| `threads_join_within_bounded_time_no_leak` | Fan-out returns inside the timeout window with all workers joined — no leaked threads |
| `merge_empty_results_is_empty_got` | SP155 OQ11 — empty result is `Got([])`, not `NotFound` |
| `merge_propagates_first_non_got_slot` | V1 hard-fail per SP155 §6 |
| `merge_stub_is_first_got_slot` | **REGRESSION-LOCK** for the T1 stub. T2/T3 MUST update this test alongside the merge implementation — flipping this lock is the gate that catches a half-shipped T2 |

**Zero-dep stance preserved:** `std::thread` + `std::sync::mpsc` only.
No tokio. No rayon. Per `feedback_kesseldb_zero_dep`. Worker threads
are joined before `scatter_scan_fanout` returns — verified by
`threads_join_within_bounded_time_no_leak`.

**What T1 deliberately did NOT do:**
- The real merge: T2 (sorted heap) / T3 (unordered concat). Stub is
  intentionally wrong-for-K>1 + locked by a regression-test pinning
  the wrongness so the merge replacement is *forced* to update it.
- `Route::Scatter(ScatterKind)` enum variant + `route()` return value
  + `Conn::scatter_read` call site in `router.rs::forward`. The
  helper is callable today via direct API; the router wiring is T2.
- Cancellation flag (T8). T1 ships strict timeout only — a hostile
  shard whose reply is dropped continues until its own `call`
  returns (then exits). The next session adds the `Arc<AtomicBool>`
  cancel pattern per SP155 §3.7.
- Multi-shard integration test via `kessel-sim` (T5/T8).

**Why a trait, not a `ClusterClient` direct dep?** The spec calls for
shipping `ClusterClient` per-shard fan-out. Using a `ShardCaller`
trait lets the unit tests in this module drive the fan-out logic
without spawning TCP shards (the existing router integration tests
do that — see `cluster.rs` + `router.rs` mod tests). T2 wires
`ClusterClient` as a `ShardCaller` impl in `router.rs` (a one-line
impl block at the call site).

**Test counts:** Workspace 1290→1299 default / 1323→1332 featured
(+9 each, matching the 9 KATs). seed-7 GREEN; tree-grep EMPTY;
`#![forbid(unsafe_code)]` honored.

## Pickup hint for the next session

Start at **T2**. The work:

1. Add `Route::Scatter(ScatterKind)` variant to `router::Route`.
2. Extend `route()` so `Op::Select` / `Op::QueryRows` /
   `Op::SelectFields` return `Route::Scatter(ScatterKind::Unordered
   { limit })` and `Op::SelectSorted` returns
   `Route::Scatter(ScatterKind::Sorted { sort_field, desc, offset,
   limit })`.
3. Add `impl ShardCaller for ClusterClient` (in `router.rs` — a one-
   line wrapper around the existing `ClusterClient::call`).
4. Add `Conn::scatter_read(&mut self, op: &Op, kind: ScatterKind) ->
   OpResult` that builds a `Vec<ClusterClient>` snapshot from
   `self.clients`, calls `scatter_scan_fanout`, then dispatches
   merge:
   - `ScatterKind::Unordered { limit }` → shard-id-ordered concat
     respecting `limit` (this is the T3 unordered merge; the spec
     bundles T3 into the same slice as T2 for the rollup
     correctness test).
   - `ScatterKind::Sorted { … }` → `BinaryHeap` k-way merge.
5. Update the `Route::Unsupported` branch in `forward` to dispatch
   the new `Route::Scatter` arm.
6. Replace the `merge_stub_is_first_got_slot` regression-lock with
   the real merge KATs.
7. Add a `kessel-sim`-based 4-shard integration test demonstrating
   `SelectSorted` byte-identical to K=1.

Estimated effort: 0.5-1 session (~200-300 LoC).

## References

- Design: `docs/superpowers/specs/2026-05-26-kesseldb-spa-cross-shard-scatter-scan-design.md` (539 lines)
- Helper module: `crates/kesseldb-server/src/scatter_scan.rs`
- Router (to be wired in T2): `crates/kesseldb-server/src/router.rs`
- Existing `ClusterClient::call`: `crates/kessel-client/src/lib.rs` line 745
- TaskList ticket: #75
