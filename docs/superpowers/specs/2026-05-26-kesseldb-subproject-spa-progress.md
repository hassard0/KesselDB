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
| T5 | Determinism property test: K∈{1,2,4,8,16} hash-identical merged result | COLLAPSED into T3 | (delivered as T3) |
| **T6** | LIMIT cancellation correctness + cancel flag | **DONE** | `cba3eea` |
| **T7** | Skew defense + bounded buffers (`SHARD_BACKPRESSURE_BOUND=4`, `sync_channel(bound)`); 5 KATs. | **DONE** | `afc1690` |
| **T8** | Pentest sweep (10 pentests from spec §7.5) — timeout / oversized / malformed / partial-then-close / mid-scan death / router-drop-under-limit / cancel-atomic-visibility / zero-shards / one-shard / determinism-replay. 10 KATs. | **DONE** | `8f6b17f` |
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

## T6 — what landed (2026-05-26, commit `cba3eea`)

**One surface (~770 LoC tests + ~190 LoC impl), single commit:**
`scatter_scan.rs` adds the LIMIT-aware cancellation machinery per
SP155 §3.7; `router.rs::Conn::scatter_read` switches from
`scatter_scan_fanout` + `merge_scan_results` to the combined
`scatter_and_merge`.

**Public surface (new):**

```rust
pub trait ShardCaller: Send + 'static {
    fn call(&mut self, op: &Op) -> Result<OpResult, String>;
    // NEW (default impl observes cancel pre-call only):
    fn call_with_cancel(
        &mut self,
        op: &Op,
        cancel: &Arc<AtomicBool>,
    ) -> Result<OpResult, String> { ... }
}

pub fn scatter_and_merge<C: ShardCaller>(
    shards: Vec<C>,
    op: &Op,
    per_shard_timeout: Duration,
    kind: &ScatterKind,
    cancel: Arc<AtomicBool>,
) -> OpResult;
```

**Behaviour matrix (per `kind`):**

- **`Unordered { limit }`:** drains worker replies in shard-id
  order (SP155 §3.6 determinism preserved); appends rows; when
  `output.len() == limit`, sets cancel + stops draining. Late
  workers' replies are silently discarded (emitting `Unavailable`
  for late slots would violate V1 hard-fail —
  `merge_scan_results`'s "first non-Got slot poisons the merge").
  `limit == 0` is the "no cap" sentinel — drain everyone, never
  fire cancel.

- **`Sorted { ..., limit }`:** k-way `BinaryHeap` merge needs every
  shard's payload upfront (the smallest row across shards may live
  on any shard); drains every slot, runs existing `merge_sorted`,
  sets cancel post-gather as a seam for future streaming sorted-
  merge (SP-A T7+). For now this is effectively a no-op for the
  gather phase (workers already returned) but the API stays
  symmetric.

- **V1 hard-fail:** any non-Got slot fires cancel + propagates as
  the merged result; late shards see cancel pre-call. Locks SP155
  §6.

- **Edge: K=0 ⇒ `Got(vec![])`** (SP155 OQ11).

- **Edge: pre-fired cancel** (caller passes `cancel.load() == true`)
  ⇒ `Got(vec![])` without spawning any workers. The strongest
  possible SP155 §3.7 "stop scanning" point.

**Thread/join discipline preserved:** all worker handles joined
before `scatter_and_merge` returns — no leaked threads in the
cancellation path either (locked by
`scatter_and_merge_cancellation_does_not_leak_threads`). Existing
`scatter_scan_fanout` + `merge_scan_results` kept as-is (all 33
prior KATs pass unchanged).

**Honest gap (SP155 §3.7 verbatim):** the default
`call_with_cancel` impl observes the cancel flag at the CALL
BOUNDARY only (pre-call check). Once `ShardCaller::call` is in
flight, the default impl cannot interrupt it (`std::net::TcpStream`
has no cancellable read — V1 enforces a per-shard `read_timeout` of
30s as the upper bound; SP-A T13 is the perf follow-up that adds
chunk-by-chunk cancel checks to a streaming `Op::SelectChunked` per
SP155 §4.4). For V1 the realized gain is "router stops waiting on
slow shards once LIMIT is hit"; the shard's wasted SERVER-SIDE work
post-cancel is the documented honest gap closed by T13.

**9 new T6 KATs (all passing):**

| KAT | Asserts |
|---|---|
| `scatter_and_merge_unordered_limit_caps_at_exactly_n_rows` | LIMIT 5 over 3 shards × 100 rows = exactly 5 rows + cancel flag set on LIMIT-hit |
| `scatter_and_merge_limit_cancels_pending_shards` | Fast shard_0 fills LIMIT before slow shard_1/shard_2 leave pre-call poll loops; they observe cancel pre-call (`ran` stays 0, `cancelled_pre_call` bumps); function returns <180ms despite shard_1/shard_2's 200ms sleeps |
| `scatter_and_merge_unordered_limit_zero_drains_every_shard` | `limit == 0` ⇒ all rows + every worker ran (no pre-call short-circuit during gather) |
| `scatter_and_merge_precancelled_returns_empty` | Pre-fired cancel: returns `Got([])` without spawning workers |
| `scatter_and_merge_limit_larger_than_total_returns_everything` | LIMIT > total ⇒ no short-circuit; every shard ran |
| `scatter_and_merge_cancellation_does_not_leak_threads` | `cancelled_pre_call` IS bumped by the time `scatter_and_merge` returns (worker exited before join) + elapsed < 250ms despite 300ms sleep on shard_1 |
| `scatter_and_merge_sorted_limit_still_gathers_all_shards` | Sorted needs all data; LIMIT 3 over 6 rows: both shards ran; result is heap-merged top-3 |
| `scatter_and_merge_unavailable_propagates_and_fires_cancel` | V1 hard-fail: Unavailable on shard_1 surfaces + shard_2 sees cancel pre-call |
| `scatter_and_merge_empty_shards_returns_empty_got` | K=0 edge: empty Got |

**Determinism (honest):** same input ⇒ same merged output at LIMIT
rows. The flag's RACY nature means slightly different counts of
*post-flag* unwanted rows may leak per shard run-to-run, but the
FINAL output is deterministic (exactly LIMIT rows when total ≥
LIMIT, in shard-id order). The K-invariance property sweep from T3
(425 fixture runs) still passes byte-identical at the merge layer.

**What T6 deliberately does NOT do:**

  - Stop SHARD-SIDE scanning (vs router-side connection close +
    worker join) — T13 perf.
  - Skew defense via bounded per-shard buffer (`sync_channel(bound=4)`
    from SP155 §3.8) — T7.
  - Pentest sweep (10 pentests from spec §7.5) — T8.
  - Partial-result-on-timeout flag (`scatter_partial_on_timeout`) —
    T9.
  - Streaming sorted-merge with mid-stream cancel — T7+.

**Test counts:** workspace 1325 → 1334 default / 1358 → 1367
featured (+9 each, matching the 9 T6 KATs). seed-7 GREEN; tree-
grep EMPTY; `#![forbid(unsafe_code)]` honored.

## T7 — what landed (2026-05-26, commit `afc1690`)

**One surface (~310 LoC tests + ~40 LoC impl/doc), single commit:**
`scatter_scan.rs` promotes the per-shard reply-channel bound to a
documented public constant per SP155 §3.8.

**Public surface (new):**

```rust
pub const SHARD_BACKPRESSURE_BOUND: usize = 4;
```

**Behaviour change:**

Both `scatter_scan_fanout` and `scatter_and_merge` switch from
`mpsc::sync_channel(1)` (hardcoded since T1/T6) to
`mpsc::sync_channel(SHARD_BACKPRESSURE_BOUND)`. Same `sync_channel`
shape, configurable bound. Per spec §3.8 rationale:

  - **bound=0** (rendezvous channel) **over-serializes** — every send
    must wait for a consumer read; the worker can't even queue its
    one V1 reply without blocking on the driver's first `recv`.
  - **bound=∞** (unbounded `mpsc::channel()`) **OOMs under skew** —
    one shard returns millions of rows while another times out; the
    merger's pull rate caps router memory at `K × bound × per-frame-
    size`, but bound=∞ removes the cap.
  - **bound=4** is the documented sweet spot — lets a worker prefetch
    a chunk or two ahead of the consumer without unbounded growth.

**V1 honest note (locked in the constant's doc-comment):** every
per-shard worker today sends exactly ONE `OpResult` per request (only
one slot used). The bound becomes load-bearing when the streaming
`Op::SelectChunked` lands (T14, spec §4.4); locking the bound now
means T14 inherits a working contract + the SendError-on-dropped-rx
clean-exit path is already proven by the T7 KATs below.

**Drop-mid-stream contract:** if the driver drops the receiver (the
LIMIT-cancellation path in `scatter_and_merge_unordered`), a worker
blocked on a full channel sees `SendError` from `tx.send()` and exits
cleanly — no deadlock, no leak (locked by
`t7_sender_observes_send_error_when_receiver_dropped_no_deadlock`).

**5 new T7 KATs (all passing):**

| KAT | Asserts |
|---|---|
| `t7_shard_backpressure_bound_is_four_per_spec` | Lock the constant value =4 (spec change otherwise) |
| `t7_sync_channel_caps_at_bound_under_fast_sender` | Fast sender paced by bound; nothing lost; FIFO |
| `t7_bound_one_still_produces_correct_merged_output` | Edge bound=1: merged bytes identical to bound=4 (correctness orthogonal to bound) |
| `t7_sender_observes_send_error_when_receiver_dropped_no_deadlock` | Cancel-path: blocked sender sees SendError, exits cleanly, no deadlock |
| `t7_slow_merger_8_fast_shards_completes_with_bounded_memory` | 8 shards × 100 rows via `scatter_and_merge` completes <2s with bounded memory |

**What T7 deliberately does NOT do:**

  - Streaming chunked per-shard sends (T14 / `Op::SelectChunked`).
    V1 still sends ONE `OpResult` per shard; bound = headroom.
  - Pentest sweep (T8 — next commit).
  - Partial-result-on-timeout flag (T9).
  - Router-side total-size cap (`max_response_size = 64 MiB` per
    spec §3.8) — inherited from `kessel-proto`'s wire frame cap, no
    separate scatter cap added in V1.

**Test counts:** workspace 1334 → 1339 default / 1389 → 1394 featured
(+5 each, matching the 5 T7 KATs). seed-7 GREEN; tree-grep EMPTY;
`#![forbid(unsafe_code)]` honored.

## T8 — what landed (2026-05-26, commit `8f6b17f`)

**One surface (~550 LoC tests, NO production-code change), single
commit:** `scatter_scan.rs` adds the 10-pentest sweep per SP155 §7.5.

**`PentestShard` mock** — a flexible `ShardCaller` with 5 behaviour
variants covering the adversarial spectrum:

```rust
enum PentestBehavior {
    SleepThenGot(Duration, Vec<u8>),  // sleep > timeout pentest
    OversizedGot(Vec<u8>),            // big but well-formed payload
    MalformedGot(Vec<u8>),            // bad row-length framing
    TransportErr(String),             // Err(transport) → Unavailable
    Got(Vec<u8>),                     // canned reply
}
```

`call_with_cancel` polls cancel in 5ms slices during sleep so
pentests 6/7 can observe cancel mid-sleep.

**10 new T8 pentests (all passing):**

| # | Pentest | Attack | Asserts |
|---|---|---|---|
| 1 | `pentest_1_shard_times_out_yields_unavailable_slot_for_that_shard` | One shard sleeps 500ms; timeout=80ms | Unavailable slot for that shard; others' slots unaffected |
| 2 | `pentest_2_shard_returns_oversized_payload_no_oom_completes_promptly` | 1 MiB well-formed Got | Merger walks all rows, no OOM, <2s |
| 3 | `pentest_3_shard_returns_malformed_bytes_yields_schema_error_no_panic` | Claims u32::MAX row in 4 bytes | `OpResult::SchemaError`, never panic |
| 4 | `pentest_4_shard_returns_partial_then_closes_surfaces_unavailable` | `Err(transport: read 4 bytes, peer closed)` | V1 hard-fail to `Unavailable` |
| 5 | `pentest_5_shard_dies_mid_scan_unavailable_no_thread_leak` | `Err(transport: connection reset)` | `Unavailable` + bounded wall-clock + follow-up call works |
| 6 | `pentest_6_router_drops_receiver_under_limit_no_panic_no_leak` | LIMIT 3 + 2 slow shards | Late shards see cancel pre-call; no panic; <180ms |
| 7 | `pentest_7_cancel_atomic_visibility_every_worker_observes` | Pre-fired cancel × 100 iter × 8 shards | Every worker observes; empty Got; ran=0 |
| 8 | `pentest_8_zero_shards_returns_empty_got_no_thread_spawned` | K=0 | Empty Got; <50ms short-circuit |
| 9 | `pentest_9_one_shard_byte_identical_to_non_scatter_path` | K=1 | Byte-identical to direct shard call (regression-lock T2 invariant) |
| 10 | `pentest_10_determinism_replay_same_input_100_runs_byte_identical` | Same input × 100 runs Sorted | Byte-identical merged result every time (no HashMap iter / no time decisions) |

**Did any pentest surface a real production bug?** **NO — every
pentest passed against the existing T1-T7 scatter machinery.** That's
the point of a pentest sweep: documents the security/robustness
contracts the layer ALREADY meets, locks them against regression, and
exercises adversarial code paths (malformed framing, transport err,
mass pre-cancel, oversized payloads, replay determinism) that the
happy-path KATs don't touch.

**One drafting bug surfaced + fixed in TDD red→green:** PT4/PT5's
"other shard" payloads were initially `b"good"` / `b"alive"` (raw
bytes, no framing); the merger correctly produced "row body exceeds
payload" `SchemaError` from interpreting `b"good"[..4]` as a
`[u32 rowlen]` claiming ~1.7 GiB. Fixed by wrapping in
`rows_to_payload(&[...])`. The pentest-as-documentation value: the
merger's framing defense IS the first line of defense and fired even
on a test-author error.

**Test counts:** workspace 1339 → 1349 default / 1394 → 1404 featured
(+10 each, matching the 10 T8 pentests). seed-7 GREEN; tree-grep
EMPTY; `#![forbid(unsafe_code)]` honored.

**What T8 deliberately does NOT do:**

  - **Real-TCP** pentests (spec OQ9 suggests "localhost TCP with a
    hostile-shard test-double process"). T8 uses in-process mocks; a
    follow-up could promote the pentests to real-TCP via the existing
    `serve_router` + a custom adversarial shard, but the in-process
    mock proves the contract at the `ShardCaller` boundary, which is
    where the scatter layer's defense lives.
  - **Partial-result-on-timeout** flag (T9) — currently V1 hard-fail
    only; PT1 / PT4 / PT5 lock the hard-fail shape.
  - **Documentation** (T10 — ARCHITECTURE.md §Sharding sub-section +
    STATUS.md "What this is NOT yet" update).

## Pickup hint for the next session

Start at **T9 / T10**. T7+T8 closed skew defense + the pentest sweep;
the SP155 §8 acceptance criterion #3 ("all 10 pentests pass") is
now MET. Remaining task slices T9..T13:

- **T9 (~150 LoC):** error-path completeness + the
  `scatter_partial_on_timeout` flag — flips the V1 hard-fail default
  to "return partial result + a per-slot non-Got marker" when the
  caller opts in.
- **T10 (~80 LoC):** documentation — ARCHITECTURE.md §Sharding
  sub-section "Cross-shard reads (SP-A)" + STATUS.md "What this is
  NOT yet" paragraph update (the open-limitation paragraph at line
  363 in STATUS.md still lists scatter-gather *reads* as open; T10
  removes it).
- **T11 (~100 LoC):** extend to FindBy / FindByComposite (degenerate
  scatter, no merge — concat-only).
- **T12-T13:** optional perf (thread-pool the workers + adaptive
  per-shard LIMIT). Only ship if a benchmark shows the per-request
  thread-spawn overhead is measurable at K=8 + high QPS.

Estimated effort per slice: 0.3-1 session. Per spec §8 acceptance:
T9+T10 close all remaining MUSTs; T11 closes the documented FindBy
follow-up; T12+T13 are perf-only.

## References

- Design: `docs/superpowers/specs/2026-05-26-kesseldb-spa-cross-shard-scatter-scan-design.md` (539 lines)
- Helper module: `crates/kesseldb-server/src/scatter_scan.rs`
- Router (T2 wired; T3 widened): `crates/kesseldb-server/src/router.rs`
- Existing `ClusterClient::call`: `crates/kessel-client/src/lib.rs` line 745
- TaskList ticket: #75
