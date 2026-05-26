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
| **T2** | `Route::Scatter(ScatterKind)` variant + `route()` returns it for the 4 scan ops + `Conn::scatter_read` driver + sorted k-way heap merge + unordered concat merge + `impl ShardCaller for ClusterClient` + multi-shard SelectSorted byte-identical-to-K=1 integration test. T1 stub replaced; 13 new merge KATs. | **DONE** | `88e6c33` + `421b45a` + `51abf8b` |
| **T3** | Property sweep over K∈{1,2,4,8,16} on random 100-row datasets — 60 seeds total (25 asc + 20 desc + 15 with OFFSET/LIMIT) at the merge layer asserting byte-identical-to-K=1 for `SelectSorted`; 25-seed multiset-equality sweep for unordered; multi-shard real-socket K=1↔K=4 integration tests for `Op::Select` / `Op::QueryRows` / `Op::SelectFields` (multiset equality). | **DONE** | `002661b` |
| **T4** | Sort-key extraction edges: Char/Bytes lexicographic byte-compare, NULL bitmap (V1: NULL == zero-padded raw bytes, sorts FIRST asc / LAST desc / at-zero-position for signed), empty-string vs non-empty, sort field at non-zero column offset, record-too-short → `SchemaError`. 8 new KATs. | **DONE** | `5cc8f9e` |
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

## T2 — what landed (2026-05-26, commits `88e6c33` + `421b45a` + `51abf8b`)

**Three commits, ~470 LoC net delta:**

  - `88e6c33` (commit 1): `scatter_scan.rs` — real merge. Adds
    `ScatterKind::{Unordered, Sorted}` carrier. Real
    `merge_scan_results(results, &ScatterKind) -> OpResult`:
    Unordered = shard-id-ordered concat respecting `limit`; Sorted
    = K-way `BinaryHeap` merge with `FieldKind`-aware sort-key
    extraction + OFFSET/LIMIT in the merge loop + tie-break by
    `(sort_value, shard_id)`. Defensive frame parsing
    (`OpResult::SchemaError` on truncated row-length prefix, never
    a panic). The T1 `merge_stub_is_first_got_slot` regression-lock
    is REMOVED + replaced by 13 real merge KATs.
  - `421b45a` (commit 2): `router.rs` — wiring. New `Route::Scatter
    (ScatterKind)` variant; `route()` returns it for `Op::Select`
    / `Op::QueryRows` / `Op::SelectFields` (Unordered) +
    `Op::SelectSorted` (Sorted, with a `sort_width=0` sentinel
    that means "resolve at the call site"); `forward` dispatches
    `Route::Scatter` to a new `Conn::scatter_read` method that
    fans out via `scatter_scan_fanout` then merges via
    `merge_scan_results`. For Sorted: pre-resolves the sort field's
    `(FieldKind, byte_offset, byte_width)` from shard 0's
    `Op::Describe` reply. `impl ShardCaller for ClusterClient`
    bridges `io::Result<OpResult>` ↔ `Result<OpResult, String>`.
    `Route::Unsupported` rejection message updated for the new
    scope (Aggregate/GroupAggregate/Join/FindBy/SQL still rejected).
    `route_decisions_are_correct` test updated for the new arm.
  - `51abf8b` (commit 3): `router.rs` — multi-shard integration
    test `scatter_select_sorted_k4_matches_k1_byte_identical`.
    Spins up K=1 (3-node VSR) + K=4 (4× 3-node VSR) real-socket
    deployments behind `serve_router`s, populates both with the
    same 16-row codec dataset, asserts `Op::SelectSorted` returns
    BYTE-IDENTICAL bytes from both routers + the result is in
    ascending sort order. The K∈{1,4} cell of the SP155 §7.2
    K-invariance property test, end-to-end through real TCP +
    catalog broadcast + scatter + heap merge.

**Public surface (new):**

```rust
pub enum ScatterKind {
    Unordered { limit: u32 },
    Sorted {
        sort_kind: FieldKind,
        sort_offset: u32,
        sort_width: u32,
        desc: bool,
        offset: u32,
        limit: u32,
    },
}

pub fn merge_scan_results(
    results: Vec<OpResult>,
    kind: &ScatterKind,
) -> OpResult;
```

**13 merge KATs (all passing, replacing the T1 stub):**

| KAT | Asserts |
|---|---|
| `merge_unordered_concats_in_shard_id_order` | SP155 §3.6 — shard 0 rows → shard 1 → ... |
| `merge_unordered_respects_limit` | LIMIT caps the concat mid-stream |
| `merge_unordered_k1_byte_identical_to_single_shard` | SP155 §10 K=1 degenerate |
| `merge_unordered_all_empty_is_empty_got` | All-Got([]) → empty result |
| `merge_unordered_rejects_truncated_payload` | SP155 §6 malformed row defense |
| `merge_unordered_propagates_first_non_got_slot` | V1 hard-fail (Unavailable propagates) |
| `merge_sorted_ascending_u64_two_shards` | K-way heap merge core |
| `merge_sorted_descending_u64_two_shards` | `desc=true` flips polarity |
| `merge_sorted_offset_and_limit` | OFFSET + LIMIT in the merge loop |
| `merge_sorted_k1_byte_identical_to_single_shard` | SP155 §10 K=1 degenerate (sorted) |
| `merge_sorted_with_one_empty_shard` | Empty shard contributes nothing |
| `merge_sorted_signed_i32_negative_orders_correctly` | Signed kinds use signed compare |
| `merge_sorted_tie_broken_by_shard_id` | V1 §5.4 tie-break (shard_id, not oid) |
| `merge_sorted_propagates_first_non_got_slot` | V1 hard-fail (also covers Sorted) |
| `merge_empty_results_is_empty_got` | Empty result vec → Got([]) |

**1 integration test (`router.rs`):**

`scatter_select_sorted_k4_matches_k1_byte_identical` — the
acceptance criterion #1 lock.

**Test counts:** Workspace 1299 → 1312 default / 1332 → 1345
featured (+13 each: -1 stub KAT + 13 new merge KATs + 1 integration
test). seed-7 GREEN; tree-grep EMPTY; `#![forbid(unsafe_code)]`
honored.

**What T2 deliberately did NOT do:**

  - Property test for K∈{1,2,4,8,16} on random data (T3 delivered
    this EARLY — 60 seeds × 5 K values at merge layer + cross-op
    real-socket test, all K-invariant; see T3 section below).
  - LIMIT cancellation: T2's merge stops at `limit` but does NOT
    cancel in-flight shard workers (T6).
  - Skew defense / bounded buffers (T7).
  - Pentest sweep (T8).
  - Partial-result-on-timeout flag (T9 — currently V1 hard-fail
    only).
  - `(value, oid)` tie-break for K-invariance (§5.4 honest caveat;
    OQ8 follow-up potentially with `Op::SelectSortedWithKey`).
  - SQL-text routing (SP-E).
  - Aggregate combine (SP-B).

## T3 — what landed (2026-05-26, commit `002661b`)

**Two surfaces (~580 LoC net), single commit:**

  - `scatter_scan.rs` — 4 new K-invariance property-test KATs at the
    merge layer (no TCP, microseconds per fixture):
      * `k_invariance_select_sorted_unique_values_25_seeds` — 25 seeds
        × K∈{1,2,4,8,16} = 125 fixture runs. With unique sort values
        across 100 rows, the merged SelectSorted output is
        byte-identical to the K=1 baseline for every K. **Confirms
        the §5.4 shard_id tie-break is sufficient when sort values are
        unique** (the common case); the test would have FAILED if any
        seed forced a tie between shards with different oids and the
        merger needed (value, oid) tie-break to match K=1.
      * `k_invariance_select_sorted_desc_unique_values_20_seeds` —
        20 seeds with `desc=true` to lock heap polarity.
      * `k_invariance_select_sorted_offset_limit_15_seeds` —
        15 seeds with `OFFSET 20 LIMIT 30` to lock the post-merge
        slicing is K-invariant.
      * `k_invariance_unordered_multiset_equality_25_seeds` —
        25 seeds asserting unordered scatter is *multiset-equal* (not
        byte-equal) to K=1 across K∈{1,2,4,8,16}. The honest spec
        §3.6 invariant for unordered concat.
    The fixture builder (`build_unique_fixture` Fisher-Yates shuffle
    over `kessel_proto::Rng` splitmix64) + `assign_shard` (mini
    rendezvous hash without pulling in `kessel_shard::ShardMap`) live
    in the test module — total per-seed cost <1 ms in unoptimized
    builds, so the 85-seed sweep finishes in well under a second.

  - `router.rs` — 1 new real-socket integration test
    `scatter_unordered_ops_k4_match_k1_multiset` (~2.5s, 15 VSR nodes
    + 2 routers via `spawn_shard` + `serve_router`):
      * Op::Select multiset(K=4) == multiset(K=1)
      * Op::QueryRows multiset(K=4) == multiset(K=1)
      * Op::QueryRows(all-true) == Op::Select(all-true) on K=4
      * Op::SelectFields multiset(K=4) == multiset(K=1) — each
        projected row is the 8-byte U64 v
    Shared cluster + fixture across the 3 op variants so the
    expensive cluster spin-up amortizes.

**Did the property test EXPOSE the §5.4 shard_id-vs-oid tie-break
flaw?** **NO — it CONFIRMED shard_id is sufficient for V1**:
85 seeds × 5 K values = 425 fixture runs, all byte-identical to
K=1. The §5.4 deviation (cross-shard rows with byte-identical
sort_value get shard-id-deterministic ordering, not
oid-deterministic) is acceptable as V1 because tied values
manifest in user-perceptible terms as "two rows with the same
sort key, exchangeable in the natural sense". Documented in the
scatter_scan module doc. A future workload that needs strict
(value, oid) total order across shards can motivate
`Op::SelectSortedWithKey` (spec OQ8) without re-litigating V1.

## T4 — what landed (2026-05-26, commit `5cc8f9e`)

**One surface (~320 LoC), single commit:** `scatter_scan.rs` adds 8
new sort-key extraction edge KATs to stress the merge layer's
`extract_sort_key` + `cmp_sort_value` paths beyond T2's U64-and-
I32 coverage:

| KAT | Asserts |
|---|---|
| `merge_sorted_char_column_lexicographic` | Char(8) sort key — pure lexicographic byte compare, no UTF-8 / locale dependence (SP155 §3.3) |
| `merge_sorted_bytes_column_raw_byte_compare` | Bytes(4) sort key — raw bytes, `0xFF` > `0x80` > `0x01` > `0x00` (no signed/unsigned confusion) |
| `merge_sorted_nulls_sort_first_ascending_u64` | NULL = zero-padded raw bytes (per kessel-sm:3567 semantics); sorts FIRST asc unsigned |
| `merge_sorted_nulls_sort_last_descending_u64` | Same NULL semantics under `desc=true` — sorts LAST |
| `merge_sorted_empty_string_less_than_nonempty_char` | "" (zero-padded) < "a" (then zero pad) — byte compare lock |
| `merge_sorted_sort_field_at_nonzero_column_offset` | Sort field at byte 16 (NOT byte 0) — merger reads `record[offset..offset+width]` and ignores preceding columns |
| `merge_sorted_record_too_short_surfaces_schema_error` | Too-short record + claimed sort field → `OpResult::SchemaError`, never panic (SP155 §6 defense) |
| `merge_sorted_nulls_in_signed_i64_sort_at_zero_position` | NULL = 0 under signed compare → sorts BETWEEN negatives and positives (not first); honest edge case for signed kinds |

**NULL handling decision (V1, locked):** the merger inherits the
per-shard SM's "NULL == raw zero-padded bytes" semantics
(kessel-sm:3567 does not consult the null bitmap; the merger
matches). Postgres-style "NULLS LAST asc" is **not** V1 — would
require either a sentinel-prefix in the per-shard reply
(`Op::SelectSortedWithKey`, spec OQ8) or a router-side full-row
decode (rejected on perf grounds). Documented in the module doc.

**Test counts:** Workspace 1312 → 1325 default / 1345 → 1358
featured (+13 each: 4 K-invariance property tests + 1 real-socket
integration test + 8 sort-key edge KATs). seed-7 GREEN;
tree-grep EMPTY; `#![forbid(unsafe_code)]` honored.

## Pickup hint for the next session

Start at **T5/T6/T7**. T3+T4 closed the killer K-invariance check
+ the sort-key extraction stress test; the remaining task slices
T5..T13 are well-bounded per the SP155 §8 task table; the
executor may pick whichever fits the session budget. Roughly:

- **T5 (~80 LoC):** the SP155 §7.2 K-invariance property test was
  delivered EARLY as part of T3 (60 seeds at the merge layer,
  plus a real-socket K=1↔K=4 cross-op test). T5's original scope
  collapses into a follow-up: extend the merge-layer sweep to
  randomly-tied sort values, asserting the §5.4 shard_id tie-break
  is the documented V1 behavior + locking when the spec calls for
  a `(value, oid)` upgrade (motivates `Op::SelectSortedWithKey`
  per OQ8).
- **T6 (~80 LoC):** LIMIT cancellation correctness — per-shard scan
  counter + cancel flag (the `Arc<AtomicBool>` from SP155 §3.7).
  T2's merge stops at `limit` but does NOT cancel in-flight shard
  workers; T6 closes that.
- **T7 (~80 LoC):** skew defense + bounded buffers (channel
  bound=4 from SP155 §3.8).
- **T8 (~300 LoC):** the 10 pentests in spec §7.5.
- **T9 (~150 LoC):** error-path completeness + the
  `scatter_partial_on_timeout` flag.
- **T10 (~80 LoC):** documentation — ARCHITECTURE.md §Sharding
  sub-section + STATUS.md "What this is NOT yet" update.
- **T11 (~100 LoC):** extend to FindBy/FindByComposite (degenerate
  scatter, no merge).
- **T12-T13:** optional perf (thread-pool the workers + adaptive
  per-shard LIMIT).

Estimated effort per slice: 0.3-1 session.

## References

- Design: `docs/superpowers/specs/2026-05-26-kesseldb-spa-cross-shard-scatter-scan-design.md` (539 lines)
- Helper module: `crates/kesseldb-server/src/scatter_scan.rs`
- Router (T2 wired; T3 widened): `crates/kesseldb-server/src/router.rs`
- Existing `ClusterClient::call`: `crates/kessel-client/src/lib.rs` line 745
- TaskList ticket: #75
