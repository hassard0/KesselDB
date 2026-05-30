//! SP-A scatter-scan router-side helper (SP155 design, T2).
//!
//! Cross-shard scatter scan / filter reads. The router fans out an
//! existing scan-shaped `Op` (`Select` / `QueryRows` / `SelectFields` /
//! `SelectSorted`) to every shard, collects per-shard `OpResult`s, then
//! merges them into a single byte-shaped result. T1 shipped the fan-out
//! scaffold + a stub merge; T2 ships the **real** merge:
//!
//! - [`ShardCaller`] — the trait per-shard dispatch needs (a single
//!   `call(&op) -> Result<OpResult, String>`). The router's `ClusterClient`
//!   implements it (see `router.rs::impl ShardCaller for ClusterClient`);
//!   the unit tests below drive a mock without spawning real shards.
//! - [`scatter_scan_fanout`] — spawns one `std::thread` per shard,
//!   collects per-shard `OpResult`s in **shard-id order** (NOT arrival
//!   order — replay-determinism trumps "fastest wins"), with a per-shard
//!   bounded timeout (default 30s, configurable). The threads are joined
//!   within the timeout window; a shard that exceeds the timeout
//!   contributes `OpResult::Unavailable` to its slot.
//! - [`ScatterKind`] — the merge strategy discriminator. `Unordered` for
//!   `Select` / `QueryRows` / `SelectFields` (shard-id-ordered concat of
//!   per-shard `[u32 rowlen][record]*` payloads, capped at `limit`).
//!   `Sorted` for `SelectSorted` (k-way `BinaryHeap` merge over per-shard
//!   already-sorted streams, with OFFSET + LIMIT in the merge loop,
//!   tie-break by `(sort_value, shard_id)` — see §5.4 caveat below).
//! - [`merge_scan_results`] — applies the merge strategy to a
//!   shard-id-ordered `Vec<OpResult>`. Hard-fails the whole merge to the
//!   first non-`Got` slot per SP155 §6 (V1 default; partial-on-timeout
//!   is a T9 follow-up flag).
//!
//! Determinism + zero-dep per SP155 §3.3: `std::thread` + `std::sync::mpsc`
//! + `std::collections::BinaryHeap` only. No tokio. No rayon. Worker
//! threads are joined within bounded time (the timeout); a `Drop` on the
//! returned join handles is a no-op by design (each handle has already
//! been joined before this function returns). The result vec has length
//! equal to `shards.len()`, ordered by shard index — the same total order
//! a single-shard run would observe with K=1, just K-way.
//!
//! Wire-shape note (SP155 §4.1): the router ships the SAME `Op` to every
//! shard. There is NO new `Op` variant for scatter. Clients keep sending
//! `Op::Select` / `Op::SelectSorted` / etc. — the router does the work.
//!
//! §5.4 honest caveat (T2 tie-break shape): the design spec calls for a
//! `(sort_value, object_id)` tiebreak so two deployments sharding the
//! SAME rows differently (K=4 vs K=8) produce the same merged answer.
//! Per-shard `SelectSorted` returns ONLY the record bytes (oid lives in
//! the storage key, not the record), so the router cannot reconstruct
//! the oid for tiebreak. T2 ships `(sort_value, shard_id)` tiebreak —
//! deterministic and reproducible for a fixed K, but NOT K-invariant
//! for sort-value ties. This is acceptable because: (a) within a shard's
//! own stream the per-shard sort is already `(value, oid)` stable, so a
//! shard's slice of the merged output is K-invariant; (b) the K-
//! invariance property test (T5) will either confirm the tie shape is
//! robust enough OR motivate the `Op::SelectSortedWithKey` follow-up
//! (spec OQ8). Documented honestly here so a future executor finds it.

use kessel_proto::{Op, OpResult};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use kessel_catalog::FieldKind;

/// Per-shard dispatch trait. The router's real per-shard `ClusterClient`
/// implements this (T2/T3 follow-ups wire it through); test code uses
/// a mock to drive scatter logic without spawning shards over TCP.
///
/// `Send + 'static` is needed because the fan-out spawns one
/// `std::thread` per shard (zero-dep — no tokio).
pub trait ShardCaller: Send + 'static {
    /// Send `op` to this shard and block for its result. Network/
    /// transport errors surface as `Err(String)`; the scatter layer
    /// translates them to `OpResult::Unavailable` for the shard's slot.
    fn call(&mut self, op: &Op) -> Result<OpResult, String>;

    /// SP-A T6 (SP155 §3.7): cancellation-aware dispatch. Default impl
    /// observes the flag at the call boundary only (pre-check + post-
    /// check) — a `true` flag short-circuits the pre-check to
    /// `Unavailable` without invoking `call()`. Once `call()` is in
    /// flight, the default impl cannot interrupt it (std::net::TcpStream
    /// has no cancellable read — see SP155 §3.7 "Honest gap"); a follow-
    /// up `ShardCaller` impl (the streaming `Op::SelectChunked` per
    /// SP155 §4.4 / SP-A T14) can override this to check the flag
    /// between TCP read chunks for finer-grained cancellation. For V1
    /// the gain is "router stops waiting on slow shards once LIMIT is
    /// hit"; the shard's wasted server-side work is the documented T13
    /// perf follow-up.
    fn call_with_cancel(
        &mut self,
        op: &Op,
        cancel: &Arc<AtomicBool>,
    ) -> Result<OpResult, String> {
        // Pre-call: if cancel already fired (e.g. LIMIT 0 sentinel
        // path), skip the call entirely. Cheap relaxed load per
        // SP155 §3.7.
        if cancel.load(AtomicOrdering::SeqCst) {
            return Ok(OpResult::Unavailable);
        }
        self.call(op)
    }
}

/// Default per-shard scatter timeout: 30s, matching the SP155 OQ1
/// (per-shard `ClusterClient` default). Exposed as `pub const` so the
/// router can override via a future `Router::with_scatter_per_shard_timeout`
/// (SP-A T9 in the backlog).
pub const DEFAULT_PER_SHARD_TIMEOUT: Duration = Duration::from_secs(30);

/// SP-A T9 (SP155 §3.6 / §6 / OQ2): partial-result + per-shard config
/// carrier for [`scatter_and_merge_ctx`]. V1 default
/// `partial_on_timeout: false` is the hard-fail mode shipped in T1-T8
/// (a single non-`Got` shard slot poisons the merged result with that
/// slot's typed error — `Unavailable`, `SchemaError`, …). Setting
/// `partial_on_timeout: true` opts in to "best-effort" mode: per-shard
/// non-`Got` slots are OMITTED from the merge instead of poisoning it;
/// the other shards' rows merge normally; the caller receives the list
/// of failed shard ids as the second tuple element returned by
/// [`scatter_and_merge_ctx`].
///
/// **Why the flag is opt-in, not the default**, per spec §6:
/// hard-fail is the safest behaviour for a strongly-consistent query
/// surface — a partial result that *looks* like a complete result is a
/// silent-correctness footgun. Opt-in keeps callers honest: "I am
/// asking for best-effort, I will inspect `failed_shards`".
///
/// **What "partial" applies to**, intentionally narrow:
///   - Per-shard `OpResult::Unavailable` (timeout, transport error,
///     shard down).
///   - Per-shard `OpResult::SchemaError` (a shard rejected the op —
///     e.g. catalog skew between shards, malformed reply).
///   - Per-shard `OpResult::Bad(_)` / `OpResult::Constraint(_)` etc.
///     (any non-`Got` slot).
///
/// **What "partial" does NOT apply to** (V1 deliberate non-coverage):
///   - The framing-malformed defense INSIDE the merger (a `Got(bytes)`
///     whose `[u32 rowlen][record]*` decoder rejects mid-stream): the
///     merger still surfaces this as `SchemaError(...)` because a
///     malformed Got is a transport/framing bug, NOT a per-shard
///     availability event. Partial mode does NOT silently drop garbage
///     bytes from one shard.
///   - The Sorted heap merge's K-invariance: with shards omitted, the
///     merged Sorted output is naturally byte-NON-IDENTICAL to a K=1
///     baseline (some rows are missing). The caller MUST consult
///     `failed_shards` to know the result is partial. Documented
///     honest gap.
///
/// **`reserved`**: a field-shape hint so future extensions (e.g. T9.1
/// partial-on-SchemaError vs partial-on-Unavailable discrimination,
/// or T9.2 per-shard timeout override) don't require breaking the
/// struct's pub-API. Set to `()` for now; callers shouldn't construct
/// the struct directly except via [`ScatterContext::default()`] or
/// [`ScatterContext::with_partial_on_timeout`].
#[derive(Clone, Debug, Default)]
pub struct ScatterContext {
    /// `true` ⇒ omit failed shards' slots from the merge (best-effort,
    /// caller inspects the `failed_shards` vec). `false` (V1 default) ⇒
    /// V1 hard-fail per SP155 §6 (any non-`Got` slot poisons the merge
    /// with that slot's typed error). Documented at the field site as
    /// the spec contract.
    pub partial_on_timeout: bool,
}

impl ScatterContext {
    /// Construct a hard-fail context (V1 default).
    pub fn hard_fail() -> Self {
        Self { partial_on_timeout: false }
    }

    /// Construct a partial-result-on-timeout context.
    pub fn partial() -> Self {
        Self { partial_on_timeout: true }
    }

    /// Builder-style mutator.
    pub fn with_partial_on_timeout(mut self, on: bool) -> Self {
        self.partial_on_timeout = on;
        self
    }
}

/// SP-A T7 (SP155 §3.8): bound on the per-shard reply channel for
/// **skew defense**. A `sync_channel(SHARD_BACKPRESSURE_BOUND)` is
/// installed for every per-shard worker → driver communication. Once
/// `bound` items are queued, the worker's next `send()` blocks until
/// the driver drains a slot — naturally pacing a fast shard to the
/// driver's consumption rate. This is **bounded buffering, not lost
/// work** (every row eventually transits).
///
/// Spec rationale (§3.8): `bound=0` (rendezvous) over-serializes;
/// `bound=∞` (unbounded `channel()`) OOMs under skew (one shard
/// returns millions of rows while another times out); `bound=4` is
/// the sweet spot — workers can prefetch a chunk or two ahead of the
/// consumer without unbounded growth.
///
/// **V1 note (honest):** every per-shard worker today sends exactly
/// ONE `OpResult` (the final reply for the SAME `Op` shipped to each
/// shard). Bound=4 is therefore "headroom" for V1 — only one slot is
/// ever used per shard. The bound becomes load-bearing when the
/// streaming `Op::SelectChunked` lands (SP-A T14 / spec §4.4), at
/// which point a chunked shard's bursty output is paced naturally.
/// Locking the bound now (and proving the sender / cancel-path
/// interaction is correct) means T14 inherits a working contract.
///
/// Drop-mid-stream behaviour: if the driver drops the receiver (the
/// LIMIT-cancellation path in `scatter_and_merge_unordered`), a
/// worker blocked on a full channel sees `SendError` from `tx.send()`
/// and exits cleanly — no deadlock, no leak (locked by
/// `t7_sender_observes_send_error_when_receiver_dropped_no_deadlock`).
pub const SHARD_BACKPRESSURE_BOUND: usize = 4;

/// Fan out `op` to every shard in `shards`, in **parallel**, with a
/// per-shard timeout. Returns one `OpResult` per shard, in **shard-id
/// order** (NOT arrival order — replay-determinism per SP155 §3.6).
///
/// Algorithm (zero-dep, std-thread only):
///
/// 1. Spawn one `std::thread` per shard. Each worker `call`s its shard
///    and sends the result over a `mpsc::sync_channel(
///    SHARD_BACKPRESSURE_BOUND)` (SP155 §3.8 skew defense; T7).
/// 2. The driver waits up to `per_shard_timeout` for each worker's
///    reply; if the worker hasn't replied by then, that slot becomes
///    `OpResult::Unavailable` (the worker thread continues until its
///    own `call` returns, but its reply is discarded — a cancellation
///    channel is SP-A T8's job; T1 ships the strict timeout).
/// 3. All worker join handles are joined before returning — no leaked
///    threads (verified by the `threads_join_within_bounded_time` test).
///
/// The op itself is **not mutated** — every shard receives the
/// identical bytes (verified by `fan_out_preserves_scan_filter_predicates`).
///
/// Edge cases:
/// - `shards.is_empty()` ⇒ returns an empty `Vec`. No threads spawned.
/// - One shard ⇒ a single worker; functionally identical to a direct
///   `client.call(op)` but routed through the fan-out machinery for a
///   uniform code path (per SP155 §10 OQ — the K=1 degenerate case).
pub fn scatter_scan_fanout<C: ShardCaller>(
    shards: Vec<C>,
    op: &Op,
    per_shard_timeout: Duration,
) -> Vec<OpResult> {
    let k = shards.len();
    if k == 0 {
        return Vec::new();
    }
    // Per-shard reply channels. `sync_channel(SHARD_BACKPRESSURE_BOUND)`
    // for skew defense (SP155 §3.8 / T7) — a fast shard's worker blocks
    // on `send()` once the bound is full instead of accumulating
    // unbounded in-flight work. For V1 each worker sends exactly one
    // `OpResult` (only one slot used), but the bound is load-bearing
    // for the future streaming chunked path (T14).
    let mut rxs: Vec<mpsc::Receiver<OpResult>> = Vec::with_capacity(k);
    let mut handles: Vec<thread::JoinHandle<()>> = Vec::with_capacity(k);
    for (i, mut caller) in shards.into_iter().enumerate() {
        let (tx, rx) =
            mpsc::sync_channel::<OpResult>(SHARD_BACKPRESSURE_BOUND);
        let op_for_worker = op.clone();
        let h = thread::Builder::new()
            .name(format!("kdb-scatter-shard-{i}"))
            .spawn(move || {
                let r = match caller.call(&op_for_worker) {
                    Ok(r) => r,
                    // Per SP155 §6 "Shard unavailable" row: hard fail to
                    // `Unavailable` (clean, retryable). A network/transport
                    // error becomes this shard's slot value; the merge in
                    // T2 decides whether one Unavailable poisons the whole
                    // result (V1 default per SP155: yes, hard fail).
                    Err(_e) => OpResult::Unavailable,
                };
                // Best-effort send; if the driver already dropped `rx`
                // (timeout fired and moved on), the value is discarded
                // — `SendError` is the documented exit path (T7).
                let _ = tx.send(r);
            })
            .expect("kdb-scatter: thread spawn (std::thread; zero-dep)");
        rxs.push(rx);
        handles.push(h);
    }
    // Drain replies in shard-id order with the per-shard deadline.
    // Each slot has at most `per_shard_timeout` wall-clock to surface a
    // reply; past that the slot is `Unavailable`. Because the previous
    // slot might have spent its full quota, later slots may have a tighter
    // *effective* wait — that's intentional: this is a per-shard cap, not
    // a fair per-slot cap. Worker threads are still running in parallel
    // (spawned upfront), so a fast shard 1 doesn't wait on a slow shard 0.
    let deadlines: Vec<Instant> = (0..k)
        .map(|_| Instant::now() + per_shard_timeout)
        .collect();
    let mut out: Vec<OpResult> = Vec::with_capacity(k);
    for (rx, deadline) in rxs.into_iter().zip(deadlines.into_iter()) {
        let now = Instant::now();
        let remaining = if deadline > now {
            deadline - now
        } else {
            Duration::from_millis(0)
        };
        let r = match rx.recv_timeout(remaining) {
            Ok(r) => r,
            Err(_) => OpResult::Unavailable,
        };
        out.push(r);
    }
    // Join every worker — no leaked threads. Workers whose reply we
    // dropped (timeout) are still draining their `client.call`; the
    // join here blocks until they're done. T8 will add a cancel flag
    // so a hostile shard cannot keep us pinned indefinitely; T1's
    // contract is "threads joined, no leak" and that's what this loop
    // delivers.
    for h in handles {
        let _ = h.join();
    }
    out
}

/// SP155 §3.2: the merge-strategy discriminator. Decoupled from the
/// `Op` enum (the router builds this from `Route::Scatter(...)`) so the
/// scatter-scan module stays purely functional over its inputs.
///
/// - `Unordered { limit }`: applies to `Op::Select`, `Op::QueryRows`,
///   `Op::SelectFields`. The merged output is the concatenation of every
///   shard's `[u32 rowlen][record]*` payload in **shard-id order**
///   (NOT arrival order; replay-determinism per SP155 §3.6), truncated
///   to the first `limit` rows (0 = no cap).
/// - `Sorted { sort_kind, sort_offset, sort_width, desc, offset, limit }`:
///   applies to `Op::SelectSorted`. The merged output is the k-way
///   `BinaryHeap` merge of the per-shard already-sorted streams.
///   `(sort_kind, sort_offset, sort_width)` describe how to extract the
///   sort key from each row record; OFFSET + LIMIT are applied in the
///   merge loop. The catalog lookup that produces these parameters
///   lives at the router call site (`Conn::scatter_read` in router.rs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScatterKind {
    /// Unordered scan ops: shard-id-ordered concat respecting `limit`.
    Unordered { limit: u32 },
    /// Sorted scan op (`Op::SelectSorted`): k-way heap merge with
    /// OFFSET + LIMIT applied in the merge loop. The catalog-derived
    /// sort parameters (`sort_kind`, `sort_offset`, `sort_width`) let
    /// the merger extract the sort key from each row's record bytes
    /// without re-loading the catalog (the router has it; the merger
    /// just needs the field's byte slice + its compare flavour).
    Sorted {
        sort_kind: FieldKind,
        /// Byte offset of the sort field within each row record.
        sort_offset: u32,
        /// Byte width of the sort field within each row record.
        sort_width: u32,
        /// `true` for descending order.
        desc: bool,
        offset: u32,
        limit: u32,
    },
    /// SP-A T11: secondary-index equality lookup (`Op::FindBy` /
    /// `Op::FindByComposite`). The per-shard reply is a *raw* concat
    /// of 16-byte object ids (NO `[u32 rowlen]` framing — see
    /// `kessel-sm` `Op::FindBy` arm). Each shard's secondary index is
    /// per-shard (a row's oid → field-value entry lives on the same
    /// shard as the row), so fan-out is required: a `field = value`
    /// lookup that returns N matches across the cluster needs every
    /// shard's local index to be consulted, and the merged result is
    /// the **union** of every shard's oid list.
    ///
    /// Merge shape: shard-id-ordered concat of every shard's
    /// `[16-byte oid]*` payload. NO LIMIT — equality lookups return
    /// their full match set per the underlying SM contract; the
    /// caller filters / paginates client-side if needed (a future
    /// T-slice could surface a LIMIT shape on FindBy if a workload
    /// motivates it). The oid sets across shards are disjoint by
    /// construction (a given oid lives on exactly one shard via the
    /// rendezvous map), so the union is a simple concatenation; no
    /// dedup pass is needed.
    ///
    /// Determinism: shard-id-ordered concat is replay-safe (same
    /// across any K choice for the same dataset distribution — the
    /// caller MAY get oids in a different order than K=1 would,
    /// because each shard's own index returns its slice in
    /// per-shard-index order, which mirrors per-shard insertion
    /// order, NOT the cross-shard order). Documented honest gap:
    /// FindBy's output is multiset-equal to K=1 (every matching oid
    /// is present exactly once); byte-identical to K=1 only when
    /// every match lives on a single shard.
    OidConcat,
    /// SP-Perf-A-SHARD-SCAN: K-invariant secondary-index oid merge for
    /// `Op::Query / QueryExpr / FindRange` whose K=1 baselines emit
    /// oids in **sorted** order (via `matched.sort_unstable()` in
    /// kessel-sm). To stay byte-identical to K=1, the cross-shard
    /// merge must dedup + sort the union (every shard returns a
    /// sub-set of oids; the union is then sorted).
    ///
    /// **OidConcat vs OidSortedUnion**: OidConcat is the right shape
    /// for `FindBy / FindByComposite` whose K=1 baseline emits oids in
    /// secondary-index iteration order (which is "insertion-order on
    /// the index entries", NOT lexical). OidSortedUnion is the right
    /// shape for ops whose K=1 baseline sort+dedups before emitting
    /// (`Query`, `QueryExpr`, `FindRange`). Validates per-shard payload
    /// length % 16 == 0 like OidConcat.
    OidSortedUnion,
    /// SP-Perf-A-SHARD-SCAN: aggregate-merge for `Op::Aggregate`.
    /// `kind` is the aggregate kind (0=COUNT, 1=SUM, 2=MIN, 3=MAX,
    /// 4=AVG). For numeric ≤8B fields the per-shard reply is `[i128 LE]`
    /// (16 bytes). For var-width MIN/MAX (CHAR/BYTES/U128/I128) it's
    /// raw bytes of `field_kind` width — the merger uses `cmp_field`
    /// of `field_kind` to compare.
    ///
    /// **AVG (kind=4) is rejected with SchemaError at K>=2**: the
    /// per-shard reply is `sum/count`, which can't be re-averaged
    /// without weighting. SHARD-SCAN-AVG (follow-up) would change the
    /// wire shape to ship `(sum, count)` so the merger can compute
    /// the global average; V1 hard-fails (K=1 AVG unchanged).
    AggregateMerge {
        /// 0=COUNT, 1=SUM, 2=MIN, 3=MAX, 4=AVG.
        kind: u8,
        /// FieldKind of the aggregated field (for var-width MIN/MAX).
        /// COUNT/SUM ignore this (always i128 LE).
        field_kind: FieldKind,
    },
    /// SP-Perf-A-SHARD-SCAN: group-aggregate merge for
    /// `Op::GroupAggregate`. The per-shard reply is
    /// `[u32 ngroups][[u32 keylen][key][i128 result]]*`. The merger
    /// accumulates per-group values across shards using the
    /// kind-appropriate combine (sum for 0/1, min for 2, max for 3,
    /// reject for 4). Output is byte-identical to K=1 (groups sorted
    /// by key bytes since kessel-sm emits via BTreeMap).
    GroupAggregateMerge {
        /// 0=COUNT, 1=SUM, 2=MIN, 3=MAX, 4=AVG.
        kind: u8,
    },
    /// SP-Perf-A-SHARD-SCAN: multi-aggregate group merge for
    /// `Op::GroupAggregateMulti`. Per-shard reply is
    /// `[u32 ngroups][[u32 keylen][key][16B * n_aggs]]*`. Per-group
    /// per-slot combination follows the `kinds` vector (one entry
    /// per aggregate slot). Any kind=4 (AVG) slot hard-fails.
    GroupAggregateMultiMerge {
        /// Per-slot kinds; same `aggregates` shape `Op::GroupAggregateMulti`
        /// carries (kind, field_id), but the merger only needs the kind.
        kinds: Vec<u8>,
    },
}

/// Merge per-shard results into a single `OpResult` per the strategy
/// in `kind`. SP155 §3.5 / §3.6 implementation.
///
/// Behaviour:
/// - **Empty input** ⇒ `OpResult::Got(vec![].into())` (SP155 OQ11 — empty
///   filter result is `Got([])`, not `NotFound`).
/// - **Any non-`Got` slot** ⇒ the first non-`Got` slot, in shard-id
///   order. V1 hard-fail per SP155 §6 (`scatter_partial_on_timeout`
///   default `false`). The merge does NOT fall back to a partial
///   result; the whole scatter fails clean.
/// - **All-`Got`** ⇒ merge per `kind`:
///   - `Unordered`: shard-id-ordered concat of every `[u32 rowlen]
///     [record]*` payload, truncated to `limit` rows (0 = unlimited).
///   - `Sorted`: K-way `BinaryHeap` merge of the per-shard
///     already-sorted streams, applying OFFSET + LIMIT in the merge
///     loop, tie-breaking by `(sort_value, shard_id)` (see module-doc
///     §5.4 caveat for the spec-vs-impl tradeoff).
///
/// Determinism (SP155 §5.4): the output is a pure function of the
/// input vec + `kind` for a fixed K. K-invariance under sort-value
/// ties is an honest deviation documented in the module doc.
///
/// Per-row malformed-record defense (cheap): the per-shard
/// `[u32 rowlen][record]*` format is parsed length-first; a malformed
/// frame is caught with `OpResult::SchemaError("scatter merge: \
/// malformed per-shard row payload from shard {i}")`. The single-
/// shard scan ops produce these payloads server-side from in-memory
/// records, so this branch fires only under adversarial / corrupted
/// transport (T8 pentest territory).
pub fn merge_scan_results(
    results: Vec<OpResult>,
    kind: &ScatterKind,
) -> OpResult {
    if results.is_empty() {
        return OpResult::Got(Vec::<u8>::new().into());
    }
    // V1 hard-fail (SP155 §6): surface the first non-Got slot
    // (Unavailable / SchemaError / etc.) so the caller sees a clean
    // failure instead of partial-then-merged.
    for r in &results {
        if !matches!(r, OpResult::Got(_)) {
            return r.clone();
        }
    }
    // Past this point every slot is `Got(_)`. Extract the per-shard
    // payload byte-slices in shard-id order; never copy until the
    // merge produces output bytes.
    let payloads: Vec<&[u8]> = results
        .iter()
        .map(|r| match r {
            // SP-Perf-A T6 Fix B: Arc<[u8]>::as_slice is unstable; deref + slice.
            OpResult::Got(b) => &b[..],
            _ => unreachable!("non-Got slot was returned above"),
        })
        .collect();
    match kind {
        ScatterKind::Unordered { limit } => merge_unordered(&payloads, *limit),
        ScatterKind::Sorted {
            sort_kind,
            sort_offset,
            sort_width,
            desc,
            offset,
            limit,
        } => merge_sorted(
            &payloads,
            *sort_kind,
            *sort_offset as usize,
            *sort_width as usize,
            *desc,
            *offset,
            *limit,
        ),
        ScatterKind::OidConcat => merge_oid_concat(&payloads),
        ScatterKind::OidSortedUnion => merge_oid_sorted_union(&payloads),
        ScatterKind::AggregateMerge { kind, field_kind } => {
            merge_aggregate(&payloads, *kind, *field_kind)
        }
        ScatterKind::GroupAggregateMerge { kind } => {
            merge_group_aggregate(&payloads, *kind)
        }
        ScatterKind::GroupAggregateMultiMerge { kinds } => {
            merge_group_aggregate_multi(&payloads, kinds)
        }
    }
}

/// SP-A T11 (SP155 §2.2 / §8): raw 16-byte oid concat across shards.
///
/// FindBy / FindByComposite return `OpResult::Got([16-byte oid]*)`
/// from each shard — a row's secondary-index entry lives on the
/// shard that owns the row, so the per-shard reply is the SHARD's
/// matching-oid list (raw, no framing). The cross-shard merge is the
/// shard-id-ordered concat of every shard's payload — the union of
/// matching oids across the cluster.
///
/// Validation: each per-shard payload's length must be a multiple of
/// 16 (the oid width). A malformed payload (length not divisible by
/// 16) surfaces `SchemaError`, never a panic.
///
/// Zero-copy: a single `Vec<u8>` is allocated for the concatenated
/// output. The per-shard byte-slices are appended directly.
fn merge_oid_concat(payloads: &[&[u8]]) -> OpResult {
    // Total output length: sum of per-shard payload lengths. Defense:
    // if any shard returned a payload whose length isn't a multiple
    // of 16, surface SchemaError (the SM contract is "16-byte oid
    // concat"; any other shape is a corrupt reply).
    let mut total = 0usize;
    for (i, p) in payloads.iter().enumerate() {
        if p.len() % 16 != 0 {
            return OpResult::SchemaError(format!(
                "scatter FindBy merge: shard {i} returned {} bytes \
                 (not a multiple of 16-byte oid width)",
                p.len()
            ));
        }
        total = total.saturating_add(p.len());
    }
    let mut out: Vec<u8> = Vec::with_capacity(total);
    for p in payloads {
        out.extend_from_slice(p);
    }
    OpResult::Got(out.into())
}

/// SP-Perf-A-SHARD-SCAN: sorted, deduplicated union of per-shard
/// 16-byte oid lists.
///
/// `Op::Query`, `Op::QueryExpr`, `Op::FindRange` (and any other op
/// whose K=1 reply emits oids in sorted order) need the cross-shard
/// merge to preserve that order. The per-shard reply is the same
/// `[16-byte oid]*` shape as `OidConcat`, but the merge step
/// sort_unstable + dedup the union.
///
/// Dedup: oids are unique across shards by construction (each row
/// lives on exactly one shard, so its oid only appears in that
/// shard's reply). But the SAME oid could appear within a single
/// shard's reply IF a secondary index has duplicate entries — defensive
/// dedup keeps the merge byte-identical to a deduplicated K=1 source.
fn merge_oid_sorted_union(payloads: &[&[u8]]) -> OpResult {
    let mut total = 0usize;
    for (i, p) in payloads.iter().enumerate() {
        if p.len() % 16 != 0 {
            return OpResult::SchemaError(format!(
                "scatter sorted-oid merge: shard {i} returned {} bytes \
                 (not a multiple of 16-byte oid width)",
                p.len()
            ));
        }
        total = total.saturating_add(p.len());
    }
    let mut ids: Vec<[u8; 16]> = Vec::with_capacity(total / 16);
    for p in payloads {
        for chunk in p.chunks(16) {
            // Safe: length-checked above; chunks of size 16 only.
            let mut a = [0u8; 16];
            a.copy_from_slice(chunk);
            ids.push(a);
        }
    }
    ids.sort_unstable();
    ids.dedup();
    let mut out: Vec<u8> = Vec::with_capacity(ids.len() * 16);
    for id in ids {
        out.extend_from_slice(&id);
    }
    OpResult::Got(out.into())
}

/// SP-Perf-A-SHARD-SCAN: aggregate-merge for `Op::Aggregate` across
/// shards.
///
/// Per-shard reply shape (mirrors `kessel-sm`'s `Op::Aggregate` arm):
/// - COUNT (kind=0), SUM (kind=1): 16 bytes `i128 LE` (one i128).
/// - MIN (kind=2), MAX (kind=3) for numeric ≤8B fields: 16 bytes
///   `i128 LE` (per-shard min/max as i128). For var-width MIN/MAX
///   (`Char(_)`, `Bytes(_)`, `U128`, `I128`): raw field-width bytes
///   (may be empty for "no matching rows"). The merger uses
///   `cmp_field` to compare across shards.
/// - AVG (kind=4): **rejected with SchemaError at K>=2** — the
///   per-shard reply is `sum/count`, which can't be re-averaged
///   without weighting. V1 limitation; SHARD-SCAN-AVG follow-up
///   would change the wire shape to include `(sum, count)`.
///
/// Determinism: byte-identical to the K=1 baseline for kinds 0..=3
/// (sum/min/max are associative). AVG hard-fails at K>=2 by design.
fn merge_aggregate(
    payloads: &[&[u8]],
    kind: u8,
    field_kind: FieldKind,
) -> OpResult {
    match kind {
        0 | 1 => {
            // COUNT or SUM: every shard returns i128 LE; sum across.
            let mut acc: i128 = 0;
            for (i, p) in payloads.iter().enumerate() {
                if p.is_empty() {
                    // Empty payload means "no rows"; contributes 0.
                    continue;
                }
                if p.len() != 16 {
                    return OpResult::SchemaError(format!(
                        "scatter agg COUNT/SUM merge: shard {i} returned {} \
                         bytes (expected 16)",
                        p.len()
                    ));
                }
                let mut le = [0u8; 16];
                le.copy_from_slice(&p[..16]);
                acc = acc.wrapping_add(i128::from_le_bytes(le));
            }
            OpResult::Got(acc.to_le_bytes().to_vec().into())
        }
        2 | 3 => {
            // MIN/MAX: numeric ≤8B path returns 16 bytes (i128 LE);
            // var-width MIN/MAX returns raw field-width bytes.
            // We distinguish by `field_kind.width()`: ≤8 = numeric i128
            // path, >8 OR variable-width = raw-bytes path.
            //
            // Numeric-path detection uses the OUTER bound (≤8B) since
            // kessel-sm's Aggregate routes <=8B numerics through
            // `aggregate_numeric_scan` (which emits i128 LE) and >8B
            // (U128/I128) or char/bytes through `agg_extreme_var`
            // (which emits raw bytes).
            let numeric_path = matches!(
                field_kind,
                FieldKind::U8
                    | FieldKind::U16
                    | FieldKind::U32
                    | FieldKind::U64
                    | FieldKind::I8
                    | FieldKind::I16
                    | FieldKind::I32
                    | FieldKind::I64
                    | FieldKind::Bool
                    | FieldKind::Timestamp
                    | FieldKind::Fixed { .. }
            );
            if numeric_path {
                let want_max = kind == 3;
                let mut best: Option<i128> = None;
                for (i, p) in payloads.iter().enumerate() {
                    if p.is_empty() {
                        continue; // shard had no matching rows
                    }
                    if p.len() != 16 {
                        return OpResult::SchemaError(format!(
                            "scatter agg MIN/MAX merge: shard {i} returned \
                             {} bytes (expected 16 for numeric ≤8B)",
                            p.len()
                        ));
                    }
                    let mut le = [0u8; 16];
                    le.copy_from_slice(&p[..16]);
                    let v = i128::from_le_bytes(le);
                    best = Some(match best {
                        None => v,
                        Some(b) => {
                            if want_max {
                                b.max(v)
                            } else {
                                b.min(v)
                            }
                        }
                    });
                }
                // Mirrors the K=1 var-empty path: when no shard had
                // any rows, emit i128 0 (matches the numeric default).
                let out_v = best.unwrap_or(0);
                OpResult::Got(out_v.to_le_bytes().to_vec().into())
            } else {
                // Variable-width MIN/MAX: raw bytes per shard. Compare
                // via cmp_sort_value (kind-aware). Empty payload means
                // "no matching rows" — skip.
                let want_max = kind == 3;
                let mut best: Option<Vec<u8>> = None;
                for p in payloads {
                    if p.is_empty() {
                        continue;
                    }
                    let candidate: Vec<u8> = p.to_vec();
                    best = Some(match best {
                        None => candidate,
                        Some(b) => {
                            let ord = cmp_sort_value(field_kind, &candidate, &b);
                            let take = if want_max {
                                ord == Ordering::Greater
                            } else {
                                ord == Ordering::Less
                            };
                            if take {
                                candidate
                            } else {
                                b
                            }
                        }
                    });
                }
                OpResult::Got(best.unwrap_or_default().into())
            }
        }
        4 => {
            // AVG: documented hard-fail at K>=2.
            OpResult::SchemaError(
                "scatter agg AVG (kind=4) not supported at K>=2: per-shard \
                 reply is sum/count, which cannot be re-averaged without \
                 weights. See SHARD-SCAN-AVG follow-up; K=1 AVG works."
                    .into(),
            )
        }
        _ => OpResult::SchemaError(format!("scatter agg: unknown kind {kind}")),
    }
}

/// SP-Perf-A-SHARD-SCAN: group-aggregate merge for
/// `Op::GroupAggregate`.
///
/// Per-shard reply shape (from `kessel-sm`'s `Op::GroupAggregate`
/// arm):
///   `[u32 ngroups]` then `ngroups × ([u32 keylen][key bytes][i128 LE])`.
///
/// The merger walks every shard's groups into a single
/// `BTreeMap<Vec<u8>, i128>` (combining values per `kind`), then
/// re-emits in sorted-by-key order to match the K=1 baseline (which
/// also uses a `BTreeMap`).
///
/// Combine functions per kind:
///   - 0 (COUNT), 1 (SUM): sum
///   - 2 (MIN): min
///   - 3 (MAX): max
///   - 4 (AVG): SchemaError (same V1 gap as `merge_aggregate`)
fn merge_group_aggregate(payloads: &[&[u8]], kind: u8) -> OpResult {
    if kind == 4 {
        return OpResult::SchemaError(
            "scatter group-agg AVG (kind=4) not supported at K>=2: same \
             wire-shape gap as Op::Aggregate AVG; see SHARD-SCAN-AVG."
                .into(),
        );
    }
    if kind > 4 {
        return OpResult::SchemaError(format!(
            "scatter group-agg: unknown kind {kind}"
        ));
    }
    let mut acc: std::collections::BTreeMap<Vec<u8>, i128> =
        std::collections::BTreeMap::new();
    for (sid, p) in payloads.iter().enumerate() {
        if p.is_empty() {
            continue;
        }
        if p.len() < 4 {
            return OpResult::SchemaError(format!(
                "scatter group-agg merge: shard {sid} payload too short \
                 ({} bytes, expected >=4 for ngroups header)",
                p.len()
            ));
        }
        let ngroups = u32::from_le_bytes(p[..4].try_into().unwrap()) as usize;
        let mut pos = 4usize;
        for g in 0..ngroups {
            if pos + 4 > p.len() {
                return OpResult::SchemaError(format!(
                    "scatter group-agg: shard {sid} group {g} truncated \
                     keylen prefix"
                ));
            }
            let keylen = u32::from_le_bytes(p[pos..pos + 4].try_into().unwrap())
                as usize;
            pos += 4;
            if pos + keylen + 16 > p.len() {
                return OpResult::SchemaError(format!(
                    "scatter group-agg: shard {sid} group {g} truncated \
                     key+value (keylen={keylen})"
                ));
            }
            let key = p[pos..pos + keylen].to_vec();
            pos += keylen;
            let mut le = [0u8; 16];
            le.copy_from_slice(&p[pos..pos + 16]);
            let v = i128::from_le_bytes(le);
            pos += 16;
            acc.entry(key)
                .and_modify(|cur| {
                    *cur = match kind {
                        0 | 1 => cur.wrapping_add(v),
                        2 => (*cur).min(v),
                        3 => (*cur).max(v),
                        _ => unreachable!("kind 4/>4 rejected above"),
                    };
                })
                .or_insert(v);
        }
    }
    // Re-encode.
    let mut out = Vec::new();
    out.extend_from_slice(&(acc.len() as u32).to_le_bytes());
    for (k, v) in acc {
        out.extend_from_slice(&(k.len() as u32).to_le_bytes());
        out.extend_from_slice(&k);
        out.extend_from_slice(&v.to_le_bytes());
    }
    OpResult::Got(out.into())
}

/// SP-Perf-A-SHARD-SCAN: multi-aggregate group merge for
/// `Op::GroupAggregateMulti`.
///
/// Per-shard reply (from `kessel-sm`'s `group_aggregate_multi`):
///   `[u32 ngroups]` then `ngroups × ([u32 keylen][key bytes][16B * n_aggs])`.
///
/// Each per-group payload has one `i128 LE` slot per aggregate; the
/// merger combines per-slot using the `kinds[slot]` policy (sum for
/// 0/1, min for 2, max for 3, SchemaError for 4).
fn merge_group_aggregate_multi(payloads: &[&[u8]], kinds: &[u8]) -> OpResult {
    if kinds.is_empty() {
        return OpResult::SchemaError(
            "scatter group-agg-multi merge: empty kinds vector".into(),
        );
    }
    for (i, k) in kinds.iter().enumerate() {
        if *k == 4 {
            return OpResult::SchemaError(format!(
                "scatter group-agg-multi merge: slot {i} kind=4 (AVG) not \
                 supported at K>=2; see SHARD-SCAN-AVG follow-up"
            ));
        }
        if *k > 4 {
            return OpResult::SchemaError(format!(
                "scatter group-agg-multi merge: slot {i} unknown kind {k}"
            ));
        }
    }
    let n_aggs = kinds.len();
    let slot_bytes = n_aggs * 16;
    let mut acc: std::collections::BTreeMap<Vec<u8>, Vec<i128>> =
        std::collections::BTreeMap::new();
    for (sid, p) in payloads.iter().enumerate() {
        if p.is_empty() {
            continue;
        }
        if p.len() < 4 {
            return OpResult::SchemaError(format!(
                "scatter group-agg-multi: shard {sid} payload too short \
                 ({} bytes, expected >=4)",
                p.len()
            ));
        }
        let ngroups = u32::from_le_bytes(p[..4].try_into().unwrap()) as usize;
        let mut pos = 4usize;
        for g in 0..ngroups {
            if pos + 4 > p.len() {
                return OpResult::SchemaError(format!(
                    "scatter group-agg-multi: shard {sid} group {g} \
                     truncated keylen prefix"
                ));
            }
            let keylen = u32::from_le_bytes(p[pos..pos + 4].try_into().unwrap())
                as usize;
            pos += 4;
            if pos + keylen + slot_bytes > p.len() {
                return OpResult::SchemaError(format!(
                    "scatter group-agg-multi: shard {sid} group {g} \
                     truncated key+slots"
                ));
            }
            let key = p[pos..pos + keylen].to_vec();
            pos += keylen;
            let mut slot_vals: Vec<i128> = Vec::with_capacity(n_aggs);
            for _ in 0..n_aggs {
                let mut le = [0u8; 16];
                le.copy_from_slice(&p[pos..pos + 16]);
                slot_vals.push(i128::from_le_bytes(le));
                pos += 16;
            }
            acc.entry(key)
                .and_modify(|cur| {
                    for (i, v) in slot_vals.iter().enumerate() {
                        cur[i] = match kinds[i] {
                            0 | 1 => cur[i].wrapping_add(*v),
                            2 => cur[i].min(*v),
                            3 => cur[i].max(*v),
                            _ => unreachable!("validated above"),
                        };
                    }
                })
                .or_insert(slot_vals);
        }
    }
    let mut out = Vec::new();
    out.extend_from_slice(&(acc.len() as u32).to_le_bytes());
    for (k, slots) in acc {
        out.extend_from_slice(&(k.len() as u32).to_le_bytes());
        out.extend_from_slice(&k);
        for v in slots {
            out.extend_from_slice(&v.to_le_bytes());
        }
    }
    OpResult::Got(out.into())
}

/// SP-A T6 (SP155 §3.7): fanout + merge with LIMIT cancellation.
///
/// This is the **production entry point** the router uses. Combines
/// `scatter_scan_fanout`-shaped fan-out with `merge_scan_results`-
/// shaped merge into a single pass so the merge layer can fire the
/// shared `cancel` flag the INSTANT it has enough rows (`Unordered`
/// LIMIT hit) — late workers see the flag on their way out and don't
/// keep the router pinned waiting.
///
/// Behaviour matrix (per `kind`):
///
/// - **`Unordered { limit }`**: drains worker replies in shard-id order
///   (same determinism as `scatter_scan_fanout`); for each Got slot
///   appends rows to the output buffer; **the instant `output.len()
///   == limit`** sets `cancel` and stops draining remaining slots. The
///   merged result is exactly `limit` rows (when total ≥ limit). Late
///   workers' replies are silently discarded — they're no longer needed
///   AND emitting an `Unavailable` for those slots would violate V1
///   hard-fail (`merge_scan_results`'s "first non-Got slot poisons the
///   merge"). `limit == 0` means "no cap" — gather everything, never
///   fire cancel.
///
/// - **`Sorted { ..., limit }`**: needs all per-shard already-sorted
///   payloads upfront for the k-way `BinaryHeap` merge (the smallest
///   row across all shards may live on any shard). Drains every slot,
///   then runs the heap merge with OFFSET + LIMIT in the merge loop
///   (existing `merge_sorted`). Sets `cancel` AFTER the gather phase
///   (effectively a no-op for the gather since all workers already
///   returned, but kept for symmetric API + so a future streaming
///   sorted-merge (T7) can short-circuit at this point).
///
/// - **Any worker reports a non-Got slot** (`Unavailable`,
///   `SchemaError`, etc.): V1 hard-fail per SP155 §6 — the gather
///   short-circuits, `cancel` fires, and the first non-Got slot is
///   returned as the merged result.
///
/// - **Empty shards (`shards.is_empty()`)** ⇒ `OpResult::Got(vec![].into())`
///   per SP155 OQ11 (matches `merge_scan_results(empty, ...)`).
///
/// Thread/join discipline:
///
/// - One `std::thread` per shard (zero-dep, same as
///   `scatter_scan_fanout`).
/// - Workers receive a clone of `cancel` + dispatch via
///   `ShardCaller::call_with_cancel` — the default impl observes the
///   flag at the call boundary (skips the call entirely if `cancel`
///   was set before the worker started).
/// - All worker handles are joined before this function returns — no
///   leaked threads, including the cancellation path (verified by
///   `scatter_and_merge_cancellation_does_not_leak_threads`).
///
/// `cancel` is taken by `Arc` (shared with the spawned workers) and
/// SHOULD start `false`. The function NEVER resets the flag — a
/// caller passing a flag that's already `true` gets an immediate
/// `OpResult::Got(vec![].into())` (the LIMIT-already-satisfied edge: no
/// shards consulted; see `scatter_and_merge_precancelled_returns_empty`).
pub fn scatter_and_merge<C: ShardCaller>(
    shards: Vec<C>,
    op: &Op,
    per_shard_timeout: Duration,
    kind: &ScatterKind,
    cancel: Arc<AtomicBool>,
) -> OpResult {
    // T9: thin back-compat wrapper around the context-aware entry
    // point. The V1 default (`partial_on_timeout: false`) preserves
    // the SP155 §6 hard-fail behaviour every T1-T8 KAT locks. New
    // callers that want partial-result semantics call
    // [`scatter_and_merge_ctx`] directly and inspect `failed_shards`.
    let (out, _failed) = scatter_and_merge_ctx(
        shards,
        op,
        per_shard_timeout,
        kind,
        cancel,
        ScatterContext::hard_fail(),
    );
    out
}

/// SP-A T9 (SP155 §3.6 / §6): context-aware fanout + merge with
/// partial-result opt-in.
///
/// Identical to [`scatter_and_merge`] (T6 LIMIT cancellation + T7
/// bounded buffers + T8 pentest-locked failure contracts), with one
/// behaviour switch driven by [`ScatterContext::partial_on_timeout`]:
///
/// - **`partial_on_timeout: false` (V1 default, hard-fail)**: identical
///   to `scatter_and_merge` — any non-`Got` slot poisons the merged
///   result with that slot's typed error per SP155 §6. The returned
///   `Vec<u32>` is always empty in this mode (the fail-fast path never
///   collects a partial-failure list).
///
/// - **`partial_on_timeout: true` (best-effort)**: per-shard non-`Got`
///   slots (Unavailable / SchemaError / Bad / Constraint / ...) are
///   OMITTED from the merge — their rows simply don't show up in the
///   merged output. The other shards' `Got(payload)` slots merge
///   normally per `kind`. The returned `Vec<u32>` lists the shard ids
///   that failed (in shard-id order). The caller is RESPONSIBLE for
///   checking this list and surfacing "partial result" to the user;
///   the merged `OpResult` itself looks indistinguishable from a
///   complete result.
///
///   Edge: if EVERY shard fails, the merged result is `Got(vec![])`
///   (empty payload) and `failed_shards` is the full `0..K` range —
///   the caller MUST check `failed_shards.len() == K` to distinguish
///   "everything failed" from "everything returned 0 rows".
///
/// - **`OpResult::SchemaError` from the MERGER itself** (malformed row
///   framing inside a `Got(bytes)` payload — caught by `iter_rows`)
///   is **NOT** considered a per-shard availability failure: it's a
///   framing/transport corruption bug. In both modes the merger
///   surfaces this as `SchemaError(...)` and fires the cancel flag.
///   Partial mode does NOT silently drop garbage bytes from one shard;
///   the user gets a clean error instead of a silently-wrong result.
///   (Documented honest gap; the V1 wire contract is "shards return
///   well-framed Got or a typed non-Got; never a Got with garbage".)
///
/// - **LIMIT cancellation** (T6) continues to work in both modes. In
///   partial mode the LIMIT-hit cancel path STILL fires (rows are
///   complete past LIMIT regardless of partial-mode), and the failed-
///   shards list reflects only the shards that returned non-Got
///   slots whose deadlines elapsed BEFORE the LIMIT-hit short-circuit
///   ran. Shards we never read from (because LIMIT was hit on an
///   earlier shard) are NOT counted as "failed" — they're just
///   "unread" (deterministic by shard-id ordering).
///
/// - **K-invariance** (T3 property sweep): preserved in
///   `partial_on_timeout: false`. In partial mode, K-invariance is
///   "K-invariance MODULO the failed-shard subset" — given the same
///   set of failed shards across two K-equivalent deployments, the
///   merged output is byte-identical. With a different failure set
///   the byte output naturally differs (some rows are missing). The
///   caller's `failed_shards` distinguishes the two cases.
pub fn scatter_and_merge_ctx<C: ShardCaller>(
    shards: Vec<C>,
    op: &Op,
    per_shard_timeout: Duration,
    kind: &ScatterKind,
    cancel: Arc<AtomicBool>,
    ctx: ScatterContext,
) -> (OpResult, Vec<u32>) {
    let k = shards.len();
    if k == 0 {
        // SP155 OQ11: empty filter result is Got([]), not NotFound.
        // Both modes return empty failed-shards (no shards = no
        // failures).
        return (OpResult::Got(Vec::<u8>::new().into()), Vec::new());
    }
    // Honor a pre-fired cancel: caller already knows LIMIT is satisfied
    // (e.g. LIMIT 0 on a downstream caller, or a Drop-time cancel from
    // a concurrent timeout). Don't spawn anything; return an empty Got
    // immediately. This matches the SP155 §3.7 "cancel = stop scanning"
    // intent at the strongest possible point.
    if cancel.load(AtomicOrdering::SeqCst) {
        return (OpResult::Got(Vec::<u8>::new().into()), Vec::new());
    }
    // Spawn workers upfront so per-shard work overlaps (the merge
    // consumer may stay sequential per SP155 §3.6).
    let mut rxs: Vec<mpsc::Receiver<OpResult>> = Vec::with_capacity(k);
    let mut handles: Vec<thread::JoinHandle<()>> = Vec::with_capacity(k);
    for (i, mut caller) in shards.into_iter().enumerate() {
        // Bounded per-shard channel for skew defense (SP155 §3.8 / T7).
        let (tx, rx) =
            mpsc::sync_channel::<OpResult>(SHARD_BACKPRESSURE_BOUND);
        let op_for_worker = op.clone();
        let cancel_for_worker = cancel.clone();
        let h = thread::Builder::new()
            .name(format!("kdb-scatter-shard-{i}"))
            .spawn(move || {
                let r = match caller
                    .call_with_cancel(&op_for_worker, &cancel_for_worker)
                {
                    Ok(r) => r,
                    Err(_e) => OpResult::Unavailable,
                };
                // SendError on dropped rx (cancellation / LIMIT) is the
                // documented clean-exit path (T7); discard the value.
                let _ = tx.send(r);
            })
            .expect("kdb-scatter: thread spawn (std::thread; zero-dep)");
        rxs.push(rx);
        handles.push(h);
    }
    let deadlines: Vec<Instant> = (0..k)
        .map(|_| Instant::now() + per_shard_timeout)
        .collect();
    let mut failed_shards: Vec<u32> = Vec::new();
    // Drain replies in shard-id order. For Unordered with a limit, we
    // can short-circuit mid-drain (set cancel, stop reading). For
    // Sorted + OidConcat we drain everyone (k-way heap merge needs
    // all data; FindBy needs every shard's match set).
    let result = match kind {
        ScatterKind::Unordered { limit } => {
            scatter_and_merge_unordered_ctx(
                &mut rxs.into_iter().zip(deadlines.into_iter()).collect::<Vec<_>>(),
                *limit,
                &cancel,
                ctx.partial_on_timeout,
                &mut failed_shards,
            )
        }
        ScatterKind::OidConcat => {
            // Gather every shard's `[16-byte oid]*` reply (no LIMIT
            // shape, no early-out — equality lookups always return
            // the full match set). Partial mode skips failed slots
            // (recorded in failed_shards); hard-fail propagates the
            // first non-Got.
            let mut gathered: Vec<OpResult> = Vec::with_capacity(k);
            let mut first_bad: Option<OpResult> = None;
            for (i, (rx, deadline)) in
                rxs.into_iter().zip(deadlines.into_iter()).enumerate()
            {
                let now = Instant::now();
                let remaining = if deadline > now {
                    deadline - now
                } else {
                    Duration::from_millis(0)
                };
                let r = match rx.recv_timeout(remaining) {
                    Ok(r) => r,
                    Err(_) => OpResult::Unavailable,
                };
                if !matches!(r, OpResult::Got(_)) {
                    if ctx.partial_on_timeout {
                        failed_shards.push(i as u32);
                        gathered.push(OpResult::Got(Vec::<u8>::new().into()));
                        continue;
                    } else if first_bad.is_none() {
                        first_bad = Some(r.clone());
                        cancel.store(true, AtomicOrdering::SeqCst);
                    }
                }
                gathered.push(r);
            }
            if let Some(bad) = first_bad {
                bad
            } else {
                merge_scan_results(gathered, kind)
            }
        }
        ScatterKind::Sorted { .. }
        | ScatterKind::OidSortedUnion
        | ScatterKind::AggregateMerge { .. }
        | ScatterKind::GroupAggregateMerge { .. }
        | ScatterKind::GroupAggregateMultiMerge { .. } => {
            // Sorted + aggregate-shaped kinds need every shard's
            // payload first; gather all, then merge via
            // `merge_scan_results`. Same skew/timeout/partial-mode
            // contract as the Sorted gather above.
            let mut gathered: Vec<OpResult> = Vec::with_capacity(k);
            let mut first_bad: Option<OpResult> = None;
            for (i, (rx, deadline)) in
                rxs.into_iter().zip(deadlines.into_iter()).enumerate()
            {
                let now = Instant::now();
                let remaining = if deadline > now {
                    deadline - now
                } else {
                    Duration::from_millis(0)
                };
                let r = match rx.recv_timeout(remaining) {
                    Ok(r) => r,
                    Err(_) => OpResult::Unavailable,
                };
                if !matches!(r, OpResult::Got(_)) {
                    if ctx.partial_on_timeout {
                        // Partial mode: record the shard id, substitute
                        // an empty Got so the merger skips it cleanly.
                        failed_shards.push(i as u32);
                        gathered.push(OpResult::Got(Vec::<u8>::new().into()));
                        continue;
                    } else if first_bad.is_none() {
                        // V1 hard-fail surfaces the first non-Got slot.
                        // Fire cancel so the remaining workers exit fast
                        // (default-impl observes it at the call boundary).
                        first_bad = Some(r.clone());
                        cancel.store(true, AtomicOrdering::SeqCst);
                    }
                }
                gathered.push(r);
            }
            if let Some(bad) = first_bad {
                bad
            } else {
                merge_scan_results(gathered, kind)
            }
        }
    };
    // Cancel any laggards that we still haven't read from (Unordered
    // short-circuit) + signal any pre-call worker to skip its call.
    // (Already done inside scatter_and_merge_unordered for the LIMIT-
    // hit path; this is the belt-and-suspenders.)
    cancel.store(true, AtomicOrdering::SeqCst);
    // Join every worker. Workers whose reply we didn't drain are
    // still pushing to the channel (bounded
    // `sync_channel(SHARD_BACKPRESSURE_BOUND)`; T7) — they either
    // already sent OR are blocked trying to send to a dropped rx
    // (SendError clean-exit). The `rx` drop above releases them; this
    // join waits for their own `call()` to return per the SP155 §3.7
    // honest gap.
    for h in handles {
        let _ = h.join();
    }
    (result, failed_shards)
}

/// Helper for `scatter_and_merge`'s Unordered path: drain shard
/// replies in shard-id order; for each Got slot append rows; **set
/// cancel + stop draining** when `output.len() == limit`. `limit == 0`
/// is "unlimited" (drain everyone, never fire cancel).
///
/// `partial_on_timeout` (SP-A T9): when `true`, a non-Got slot is
/// recorded in `failed_shards` and skipped (other shards' rows merge
/// normally); when `false` (V1 hard-fail), the first non-Got slot
/// fires cancel and is returned as-is.
///
/// Returns the merged `OpResult` directly (Got with the payload, OR
/// the first non-Got slot per V1 hard-fail when partial mode is off).
fn scatter_and_merge_unordered_ctx(
    rxs_with_deadlines: &mut Vec<(mpsc::Receiver<OpResult>, Instant)>,
    limit: u32,
    cancel: &Arc<AtomicBool>,
    partial_on_timeout: bool,
    failed_shards: &mut Vec<u32>,
) -> OpResult {
    let mut out: Vec<u8> = Vec::new();
    let mut emitted: u32 = 0;
    let mut shard_id: usize = 0;
    for (rx, deadline) in rxs_with_deadlines.drain(..) {
        let now = Instant::now();
        let remaining = if deadline > now {
            deadline - now
        } else {
            Duration::from_millis(0)
        };
        let r = match rx.recv_timeout(remaining) {
            Ok(r) => r,
            Err(_) => OpResult::Unavailable,
        };
        // V1 hard-fail OR T9 partial-mode skip: a non-Got slot
        // either poisons the merge (hard-fail) OR is recorded in
        // `failed_shards` and dropped, depending on
        // `partial_on_timeout`. In partial mode the cancel flag is
        // NOT fired — other shards continue normally.
        let payload = match r {
            OpResult::Got(b) => b,
            other => {
                if partial_on_timeout {
                    failed_shards.push(shard_id as u32);
                    shard_id += 1;
                    continue;
                }
                cancel.store(true, AtomicOrdering::SeqCst);
                return other;
            }
        };
        // Append this shard's rows; track LIMIT.
        let it = match iter_rows(&payload) {
            Ok(it) => it,
            Err(e) => {
                // Framing/malformed payloads are NOT a per-shard
                // availability event; even partial mode surfaces this
                // as a clean SchemaError + fires cancel. See T9
                // doc-comment: partial mode does NOT silently drop
                // garbage bytes from a shard.
                cancel.store(true, AtomicOrdering::SeqCst);
                return OpResult::SchemaError(format!(
                    "scatter merge: shard {shard_id} payload framing: {e}"
                ));
            }
        };
        for row in it {
            match row {
                Ok(rec) => {
                    append_row(&mut out, rec);
                    emitted = emitted.saturating_add(1);
                    if limit != 0 && emitted >= limit {
                        // LIMIT hit. Fire cancel so the remaining
                        // shards' workers see it (the default
                        // `call_with_cancel` impl observes the flag
                        // pre-call; once they're in flight they're
                        // committed to finish — SP155 §3.7 honest
                        // gap). Stop draining.
                        cancel.store(true, AtomicOrdering::SeqCst);
                        return OpResult::Got(out.into());
                    }
                }
                Err(e) => {
                    cancel.store(true, AtomicOrdering::SeqCst);
                    return OpResult::SchemaError(format!(
                        "scatter merge: shard {shard_id} row: {e}"
                    ));
                }
            }
        }
        shard_id += 1;
    }
    OpResult::Got(out.into())
}

/// Iterate `[u32 rowlen][record]*` payload, yielding `(rowlen_le_bytes,
/// record)` slices in stream order. Returns `Err` if the frame is
/// truncated or claims a row larger than the remaining payload.
///
/// Zero-copy: yields slices into `payload`, no `Vec` allocation per
/// row. The whole scatter merge is one final `Vec<u8>` allocation
/// for the output payload.
fn iter_rows(payload: &[u8]) -> Result<RowIter<'_>, &'static str> {
    Ok(RowIter { payload, pos: 0 })
}

struct RowIter<'a> {
    payload: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for RowIter<'a> {
    type Item = Result<&'a [u8], &'static str>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos == self.payload.len() {
            return None;
        }
        if self.pos + 4 > self.payload.len() {
            return Some(Err("truncated row-length prefix"));
        }
        let lenb: [u8; 4] = match self.payload[self.pos..self.pos + 4]
            .try_into()
        {
            Ok(b) => b,
            Err(_) => return Some(Err("row-length prefix slice")),
        };
        let len = u32::from_le_bytes(lenb) as usize;
        let body_start = self.pos + 4;
        let body_end = match body_start.checked_add(len) {
            Some(e) if e <= self.payload.len() => e,
            _ => return Some(Err("row body exceeds payload")),
        };
        let rec = &self.payload[body_start..body_end];
        self.pos = body_end;
        Some(Ok(rec))
    }
}

/// Append `record` to `out` as `[u32 len][record]`.
fn append_row(out: &mut Vec<u8>, record: &[u8]) {
    out.extend_from_slice(&(record.len() as u32).to_le_bytes());
    out.extend_from_slice(record);
}

/// SP155 §3.6: shard-id-ordered concat of per-shard `[u32 rowlen]
/// [record]*` payloads, truncated to `limit` rows (0 = unlimited).
fn merge_unordered(payloads: &[&[u8]], limit: u32) -> OpResult {
    let mut out: Vec<u8> = Vec::new();
    let mut emitted: u32 = 0;
    for (i, payload) in payloads.iter().enumerate() {
        let it = match iter_rows(payload) {
            Ok(it) => it,
            Err(e) => {
                return OpResult::SchemaError(format!(
                    "scatter merge: shard {i} payload framing: {e}"
                ))
            }
        };
        for r in it {
            match r {
                Ok(rec) => {
                    append_row(&mut out, rec);
                    emitted = emitted.saturating_add(1);
                    if limit != 0 && emitted >= limit {
                        return OpResult::Got(out.into());
                    }
                }
                Err(e) => {
                    return OpResult::SchemaError(format!(
                        "scatter merge: shard {i} row: {e}"
                    ))
                }
            }
        }
    }
    OpResult::Got(out.into())
}

/// Key-extraction helper: copy `width` bytes starting at `offset`
/// inside `record` into a fresh `Vec<u8>`. Returns `None` if the row
/// is too short to contain the field. Same shape as the per-shard
/// SM `SelectSorted` extraction (kessel-sm cmp_field), so the merger
/// can compare bytes the same way.
fn extract_sort_key(record: &[u8], offset: usize, width: usize) -> Option<Vec<u8>> {
    if width == 0 {
        return Some(Vec::new());
    }
    record.get(offset..offset.checked_add(width)?).map(|s| s.to_vec())
}

/// Compare two extracted sort-key byte slices using `FieldKind`-
/// aware semantics — byte-identical to the per-shard SM's
/// `cmp_field` (so the merge produces the same total order as a
/// fat K=1 single-shard sort).
fn cmp_sort_value(kind: FieldKind, a: &[u8], b: &[u8]) -> Ordering {
    use FieldKind::*;
    let w = kind.width() as usize;
    // Defensive pad: a malformed-too-short slice compares as if
    // zero-padded; the parent merger guarantees length >= w in
    // normal paths via `extract_sort_key`.
    let pad = |x: &[u8]| -> [u8; 16] {
        let mut le = [0u8; 16];
        le[..w.min(16).min(x.len())]
            .copy_from_slice(&x[..w.min(16).min(x.len())]);
        le
    };
    let load_u = |x: &[u8]| u128::from_le_bytes(pad(x));
    let load_i = |x: &[u8]| -> i128 {
        let mut le = pad(x);
        if w < 16
            && w > 0
            && x.get(w - 1).copied().unwrap_or(0) & 0x80 != 0
        {
            for byte in le.iter_mut().skip(w) {
                *byte = 0xFF;
            }
        }
        i128::from_le_bytes(le)
    };
    match kind {
        U8 | U16 | U32 | U64 | U128 | Bool | Timestamp => {
            load_u(a).cmp(&load_u(b))
        }
        I8 | I16 | I32 | I64 | I128 | Fixed { .. } => {
            load_i(a).cmp(&load_i(b))
        }
        Char(_) | Bytes(_) | Ref | OverflowRef => a.cmp(b),
    }
}

/// Heap node for the k-way sorted merge. Each node carries the
/// per-shard already-decoded current row's sort key + record bytes +
/// the source shard id (for tie-break + refill). The `Ord` impl
/// produces a `min-heap` for ascending order: smaller `(value,
/// shard_id)` is "greater" so `BinaryHeap::pop` returns it first.
///
/// For descending (`desc=true`) the caller flips the comparison.
struct HeapNode {
    /// Extracted sort-field bytes (length-aware, kind-aware compare
    /// done in `Ord`).
    sort_key: Vec<u8>,
    /// Shard this row came from — refill source + tie-break.
    shard_id: usize,
    /// The full row record (owned so the heap can hold it across
    /// pops; output emits a fresh length-prefixed copy).
    record: Vec<u8>,
    /// Field-kind for the compare (cloned into every node to keep
    /// `Ord` purely a function of the node — no external lookup
    /// in the hot loop).
    sort_kind: FieldKind,
    /// `true` flips the polarity (descending order).
    desc: bool,
}

impl PartialEq for HeapNode {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for HeapNode {}
impl PartialOrd for HeapNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapNode {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; for ascending we want the
        // *smaller* sort_key to come out first, so we reverse.
        // Tie-break by shard_id (smaller shard first ⇒ deterministic
        // when sort values tie). §5.4 caveat: spec calls for oid
        // tiebreak; the record doesn't carry the oid in V1.
        let primary =
            cmp_sort_value(self.sort_kind, &self.sort_key, &other.sort_key);
        let primary = if self.desc { primary } else { primary.reverse() };
        // For tie (primary == Equal), smaller shard_id should be
        // popped first ⇒ reverse so "smaller is greater".
        primary.then_with(|| other.shard_id.cmp(&self.shard_id))
    }
}

/// SP155 §3.5: K-way `BinaryHeap` merge of per-shard already-sorted
/// `[u32 rowlen][record]*` streams. Applies OFFSET + LIMIT in the
/// merge loop. Tie-break by shard_id (V1; §5.4 caveat for the
/// `(value, oid)` deviation).
fn merge_sorted(
    payloads: &[&[u8]],
    sort_kind: FieldKind,
    sort_offset: usize,
    sort_width: usize,
    desc: bool,
    offset: u32,
    limit: u32,
) -> OpResult {
    // Per-shard row iterators, indexed by shard id.
    let mut iters: Vec<RowIter<'_>> = Vec::with_capacity(payloads.len());
    for p in payloads {
        iters.push(match iter_rows(p) {
            Ok(it) => it,
            Err(e) => {
                return OpResult::SchemaError(format!(
                    "scatter merge sorted: payload framing: {e}"
                ))
            }
        });
    }
    let mut heap: BinaryHeap<HeapNode> = BinaryHeap::with_capacity(payloads.len());
    // Prime: one row from each shard.
    for (i, it) in iters.iter_mut().enumerate() {
        if let Some(r) = it.next() {
            let rec = match r {
                Ok(rec) => rec.to_vec(),
                Err(e) => {
                    return OpResult::SchemaError(format!(
                        "scatter merge sorted: shard {i} row: {e}"
                    ))
                }
            };
            let sort_key = match extract_sort_key(&rec, sort_offset, sort_width) {
                Some(k) => k,
                None => {
                    return OpResult::SchemaError(format!(
                        "scatter merge sorted: shard {i} row too short for \
                         sort field (offset={sort_offset}, width={sort_width}, \
                         record len={})",
                        rec.len()
                    ))
                }
            };
            heap.push(HeapNode {
                sort_key,
                shard_id: i,
                record: rec,
                sort_kind,
                desc,
            });
        }
    }
    let mut out: Vec<u8> = Vec::new();
    let mut skipped: u32 = 0;
    let mut emitted: u32 = 0;
    while let Some(node) = heap.pop() {
        if skipped < offset {
            skipped += 1;
        } else {
            append_row(&mut out, &node.record);
            emitted = emitted.saturating_add(1);
            if limit != 0 && emitted >= limit {
                break;
            }
        }
        // Refill from the same shard.
        let sid = node.shard_id;
        if let Some(r) = iters[sid].next() {
            let rec = match r {
                Ok(rec) => rec.to_vec(),
                Err(e) => {
                    return OpResult::SchemaError(format!(
                        "scatter merge sorted: shard {sid} row: {e}"
                    ))
                }
            };
            let sort_key = match extract_sort_key(&rec, sort_offset, sort_width) {
                Some(k) => k,
                None => {
                    return OpResult::SchemaError(format!(
                        "scatter merge sorted: shard {sid} row too short for \
                         sort field"
                    ))
                }
            };
            heap.push(HeapNode {
                sort_key,
                shard_id: sid,
                record: rec,
                sort_kind,
                desc,
            });
        }
    }
    OpResult::Got(out.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_proto::TypeId;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    /// In-process mock shard. Returns a pre-canned `OpResult` after an
    /// optional `sleep` (to simulate slow shards / timeouts). Records
    /// every received op so KATs can assert preservation of predicates.
    struct MockShard {
        canned: OpResult,
        sleep: Duration,
        seen: Arc<Mutex<Vec<Op>>>,
        /// Bumped on every `call`. Lets the joined-by-deadline KAT prove
        /// the worker actually ran (vs being silently skipped).
        ran: Arc<AtomicUsize>,
    }

    impl MockShard {
        fn new(canned: OpResult) -> Self {
            MockShard {
                canned,
                sleep: Duration::from_millis(0),
                seen: Arc::new(Mutex::new(Vec::new())),
                ran: Arc::new(AtomicUsize::new(0)),
            }
        }
        fn slow(mut self, d: Duration) -> Self {
            self.sleep = d;
            self
        }
    }

    impl ShardCaller for MockShard {
        fn call(&mut self, op: &Op) -> Result<OpResult, String> {
            self.ran.fetch_add(1, Ordering::SeqCst);
            self.seen.lock().unwrap().push(op.clone());
            if self.sleep > Duration::from_millis(0) {
                thread::sleep(self.sleep);
            }
            Ok(self.canned.clone())
        }
    }

    fn dummy_select() -> Op {
        Op::Select {
            type_id: 1 as TypeId,
            program: vec![0xAB, 0xCD, 0xEF],
            limit: 10,
        }
    }

    /// K=1: a single-shard scatter returns that shard's reply, byte-
    /// identical. The degenerate case the SP155 spec calls out (§10).
    #[test]
    fn fan_out_to_one_shard_returns_that_shards_result() {
        let s = MockShard::new(OpResult::Got(vec![1, 2, 3, 4].into()));
        let out = scatter_scan_fanout(
            vec![s],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], OpResult::Got(vec![1, 2, 3, 4].into()));
    }

    /// K=3: the result is in **shard-id order**, NOT arrival order, even
    /// though shard 0 sleeps and shard 2 returns instantly. This is the
    /// SP155 §3.6 determinism property — replay-safe ordering.
    #[test]
    fn fan_out_to_three_shards_returns_three_results_in_shard_order() {
        let s0 = MockShard::new(OpResult::Got(b"shard-0".to_vec().into()))
            .slow(Duration::from_millis(50));
        let s1 = MockShard::new(OpResult::Got(b"shard-1".to_vec().into()));
        let s2 = MockShard::new(OpResult::Got(b"shard-2".to_vec().into()));
        let out = scatter_scan_fanout(
            vec![s0, s1, s2],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
        );
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], OpResult::Got(b"shard-0".to_vec().into()));
        assert_eq!(out[1], OpResult::Got(b"shard-1".to_vec().into()));
        assert_eq!(out[2], OpResult::Got(b"shard-2".to_vec().into()));
    }

    /// A shard that exceeds the per-shard timeout contributes
    /// `Unavailable` to its slot. Other shards' replies are unaffected.
    /// Per SP155 §6 "Shard timeout" row (V1 hard-fail default).
    #[test]
    fn a_shard_that_times_out_returns_unavailable_for_that_slot() {
        let s0 = MockShard::new(OpResult::Got(b"fast".to_vec().into()));
        let s1 = MockShard::new(OpResult::Got(b"too-slow".to_vec().into()))
            .slow(Duration::from_millis(300));
        let s2 = MockShard::new(OpResult::Got(b"also-fast".to_vec().into()));
        let out = scatter_scan_fanout(
            vec![s0, s1, s2],
            &dummy_select(),
            Duration::from_millis(80),
        );
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], OpResult::Got(b"fast".to_vec().into()));
        assert_eq!(out[1], OpResult::Unavailable);
        // Even though shard 2 is fast, its deadline was set at spawn time
        // — by the time the driver gets to it, the shared timeout window
        // has already elapsed waiting for shard 1. This is the intentional
        // per-shard deadline; T2 may adjust by using independent deadlines.
        // For T1: shard 2's worker DID run, but its reply may or may not
        // have arrived before the driver's deadline window expired. Lock
        // the contract that BOTH outcomes are valid (Got or Unavailable),
        // and that shard 2 is NEVER mis-attributed shard 1's payload.
        match &out[2] {
            OpResult::Got(b) => assert_eq!(&b[..], b"also-fast"),
            OpResult::Unavailable => {} // acceptable per per-shard deadline
            other => panic!("shard 2 unexpected slot: {other:?}"),
        }
    }

    /// K=0: an empty shard list returns an empty result vec. No threads
    /// spawned, no panics. Caught at the function entry so the merge
    /// stub doesn't have to special-case it either.
    #[test]
    fn fan_out_to_empty_shards_returns_empty_vec() {
        // Use the MockShard type so the empty Vec is well-typed without
        // forcing a turbofish at the call site.
        let shards: Vec<MockShard> = Vec::new();
        let out = scatter_scan_fanout(
            shards,
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
        );
        assert_eq!(out, Vec::<OpResult>::new());
    }

    /// Every shard sees the **identical** op bytes — the router does not
    /// mutate the filter predicates / sort field / limit between shards.
    /// This is the SP155 §3.4 filter-pushdown invariant ("the router
    /// ships the IDENTICAL op to every shard"). T2/T3's merge layer
    /// relies on this — if the per-shard ops drift, sorted-merge
    /// determinism is destroyed.
    #[test]
    fn fan_out_preserves_scan_filter_predicates() {
        let s0 = MockShard::new(OpResult::Got(vec![].into()));
        let s1 = MockShard::new(OpResult::Got(vec![].into()));
        let s2 = MockShard::new(OpResult::Got(vec![].into()));
        let seen0 = s0.seen.clone();
        let seen1 = s1.seen.clone();
        let seen2 = s2.seen.clone();
        let op = Op::SelectSorted {
            type_id: 7 as TypeId,
            program: vec![0xCA, 0xFE, 0xBA, 0xBE],
            sort_field: 42,
            desc: true,
            offset: 100,
            limit: 50,
        };
        let _ = scatter_scan_fanout(
            vec![s0, s1, s2],
            &op,
            DEFAULT_PER_SHARD_TIMEOUT,
        );
        for seen in [seen0, seen1, seen2] {
            let v = seen.lock().unwrap();
            assert_eq!(v.len(), 1, "each shard received exactly one op");
            assert_eq!(v[0], op, "shard's op must be byte-identical");
        }
    }

    /// All worker threads are joined before `scatter_scan_fanout`
    /// returns. Even if a shard's reply was dropped (timeout), its
    /// worker has finished its own `call` before we return — no leaked
    /// thread (T8 will add a cancel flag; T1 ships join-before-return).
    /// We verify by recording `ran` on a slow shard and asserting it
    /// reached 1 by the time fan-out returned (i.e. the worker thread
    /// actually executed `call`, which only happens before the worker
    /// thread exits).
    #[test]
    fn threads_join_within_bounded_time_no_leak() {
        let s = MockShard::new(OpResult::Got(b"ok".to_vec().into()))
            .slow(Duration::from_millis(40));
        let ran = s.ran.clone();
        let t0 = Instant::now();
        let out = scatter_scan_fanout(
            vec![s],
            &dummy_select(),
            Duration::from_millis(500),
        );
        let elapsed = t0.elapsed();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], OpResult::Got(b"ok".to_vec().into()));
        // Worker did run before the function returned: `call` was
        // invoked at least once.
        assert!(
            ran.load(Ordering::SeqCst) >= 1,
            "worker thread must have executed before scatter returned"
        );
        // Sanity: we DID wait for the slow worker (>= 40ms) but DID NOT
        // wait the full timeout (500ms). Bounds are intentionally loose
        // to avoid timing flakes on busy CI.
        assert!(
            elapsed >= Duration::from_millis(30),
            "elapsed {elapsed:?} must be >= the worker's sleep"
        );
        assert!(
            elapsed < Duration::from_millis(450),
            "elapsed {elapsed:?} must be << the full timeout (no leak)"
        );
    }

    // ---------- T2 merge KATs (real) ----------
    //
    // T1's `merge_stub_is_first_got_slot` regression-lock has been
    // intentionally REMOVED — its sole purpose was to force T2 to
    // touch the merge logic in the same commit as the stub. T2's
    // real merge KATs below replace it.

    /// Build `[u32 rowlen][record]*` payload bytes from a list of row
    /// record byte-slices — the per-shard scan-op output shape.
    fn rows_to_payload(rows: &[&[u8]]) -> Vec<u8> {
        let mut p = Vec::new();
        for r in rows {
            p.extend_from_slice(&(r.len() as u32).to_le_bytes());
            p.extend_from_slice(r);
        }
        p
    }

    /// `merge_scan_results` on an empty result vec returns an empty
    /// `Got([])` — matches per-shard `Select` semantics (an empty
    /// filter result is `Got([])`, not `NotFound`) per SP155 OQ11.
    #[test]
    fn merge_empty_results_is_empty_got() {
        let out =
            merge_scan_results(Vec::new(), &ScatterKind::Unordered { limit: 0 });
        assert_eq!(out, OpResult::Got(Vec::<u8>::new().into()));
    }

    /// V1 hard-fail per SP155 §6: any non-`Got` slot propagates — the
    /// merge does NOT mix a partial result with the error. Same
    /// semantic for `Unordered` and `Sorted`.
    #[test]
    fn merge_propagates_first_non_got_slot_unordered() {
        let r = merge_scan_results(
            vec![
                OpResult::Got(b"a".to_vec().into()),
                OpResult::Unavailable,
                OpResult::Got(b"c".to_vec().into()),
            ],
            &ScatterKind::Unordered { limit: 0 },
        );
        assert_eq!(r, OpResult::Unavailable);
    }

    /// V1 hard-fail also covers Sorted merges — propagate, no
    /// partial-then-merged fallback.
    #[test]
    fn merge_propagates_first_non_got_slot_sorted() {
        let r = merge_scan_results(
            vec![
                OpResult::Got(rows_to_payload(&[&[1u8; 8]]).into()),
                OpResult::SchemaError("oops".into()),
            ],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::U64,
                sort_offset: 0,
                sort_width: 8,
                desc: false,
                offset: 0,
                limit: 0,
            },
        );
        assert_eq!(r, OpResult::SchemaError("oops".into()));
    }

    /// **Unordered merge KAT**: shard-id-ordered concat of the per-
    /// shard `[u32 rowlen][record]*` payloads (SP155 §3.6). NOT
    /// arrival order; `shard_0 rows` then `shard_1 rows` then
    /// `shard_2 rows`, byte-identical to a K=1 "fat shard" run.
    #[test]
    fn merge_unordered_concats_in_shard_id_order() {
        let s0 = rows_to_payload(&[b"row-a", b"row-b"]);
        let s1 = rows_to_payload(&[b"row-c"]);
        let s2 = rows_to_payload(&[b"row-d", b"row-e", b"row-f"]);
        let r = merge_scan_results(
            vec![
                OpResult::Got(s0.into()),
                OpResult::Got(s1.into()),
                OpResult::Got(s2.into()),
            ],
            &ScatterKind::Unordered { limit: 0 },
        );
        let expected = rows_to_payload(&[
            b"row-a", b"row-b", b"row-c", b"row-d", b"row-e", b"row-f",
        ]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    /// **Unordered LIMIT KAT**: `limit > 0` caps the merge to that
    /// many rows in shard-id order. LIMIT=4 over (2,1,3) per shard
    /// yields shard0[0..2] ++ shard1[0..1] ++ shard2[0..1] — never
    /// dipping past the cap.
    #[test]
    fn merge_unordered_respects_limit() {
        let s0 = rows_to_payload(&[b"a", b"b"]);
        let s1 = rows_to_payload(&[b"c"]);
        let s2 = rows_to_payload(&[b"d", b"e", b"f"]);
        let r = merge_scan_results(
            vec![
                OpResult::Got(s0.into()),
                OpResult::Got(s1.into()),
                OpResult::Got(s2.into()),
            ],
            &ScatterKind::Unordered { limit: 4 },
        );
        let expected = rows_to_payload(&[b"a", b"b", b"c", b"d"]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    /// **K=1 byte-identical (unordered)**: a single-shard scatter is
    /// byte-identical to the per-shard reply (modulo the LIMIT cap,
    /// which is enforced shard-side too). Locks SP155 §10 "K=1
    /// degenerate case" for the merge layer.
    #[test]
    fn merge_unordered_k1_byte_identical_to_single_shard() {
        let payload =
            rows_to_payload(&[b"only-shard-row-0", b"only-shard-row-1"]);
        let r = merge_scan_results(
            vec![OpResult::Got(payload.clone().into())],
            &ScatterKind::Unordered { limit: 0 },
        );
        assert_eq!(r, OpResult::Got(payload.into()));
    }

    /// **Empty shards in unordered merge**: an "all-`Got([])`" input
    /// yields an empty result (NOT NotFound; SP155 OQ11).
    #[test]
    fn merge_unordered_all_empty_is_empty_got() {
        let r = merge_scan_results(
            vec![
                OpResult::Got(Vec::<u8>::new().into()),
                OpResult::Got(Vec::<u8>::new().into()),
                OpResult::Got(Vec::<u8>::new().into()),
            ],
            &ScatterKind::Unordered { limit: 0 },
        );
        assert_eq!(r, OpResult::Got(Vec::<u8>::new().into()));
    }

    /// **Malformed payload (unordered)**: a truncated row-length
    /// prefix is caught and surfaced as `SchemaError` — NOT a panic.
    /// Defensive merge frame parsing per SP155 §6 "malformed rows"
    /// row.
    #[test]
    fn merge_unordered_rejects_truncated_payload() {
        // Claims a 99-byte row but the payload has only 4 bytes after
        // the prefix.
        let bad = {
            let mut v = (99u32).to_le_bytes().to_vec();
            v.extend_from_slice(&[1, 2, 3, 4]);
            v
        };
        let r = merge_scan_results(
            vec![OpResult::Got(bad.into())],
            &ScatterKind::Unordered { limit: 0 },
        );
        assert!(
            matches!(r, OpResult::SchemaError(_)),
            "truncated payload must surface as SchemaError, got {r:?}"
        );
    }

    /// **Sorted merge KAT (ascending U64)**: per-shard streams are
    /// already in `(value, oid)` order; the merge produces the
    /// globally sorted stream by `value` ascending. shard 0 has
    /// records with U64 values [1,4,9]; shard 1 has [2,3,7]. Merged
    /// = [1,2,3,4,7,9].
    #[test]
    fn merge_sorted_ascending_u64_two_shards() {
        let rec = |v: u64| -> Vec<u8> { v.to_le_bytes().to_vec() };
        let s0 = rows_to_payload(&[&rec(1), &rec(4), &rec(9)]);
        let s1 = rows_to_payload(&[&rec(2), &rec(3), &rec(7)]);
        let r = merge_scan_results(
            vec![OpResult::Got(s0.into()), OpResult::Got(s1.into())],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::U64,
                sort_offset: 0,
                sort_width: 8,
                desc: false,
                offset: 0,
                limit: 0,
            },
        );
        let expected = rows_to_payload(&[
            &rec(1), &rec(2), &rec(3), &rec(4), &rec(7), &rec(9),
        ]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    /// **Sorted merge — descending**: same data, `desc=true` flips
    /// the order. Merged = [9,7,4,3,2,1].
    #[test]
    fn merge_sorted_descending_u64_two_shards() {
        let rec = |v: u64| -> Vec<u8> { v.to_le_bytes().to_vec() };
        // Per-shard SelectSorted with desc=true returns rows in
        // descending order; replicate that here.
        let s0 = rows_to_payload(&[&rec(9), &rec(4), &rec(1)]);
        let s1 = rows_to_payload(&[&rec(7), &rec(3), &rec(2)]);
        let r = merge_scan_results(
            vec![OpResult::Got(s0.into()), OpResult::Got(s1.into())],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::U64,
                sort_offset: 0,
                sort_width: 8,
                desc: true,
                offset: 0,
                limit: 0,
            },
        );
        let expected = rows_to_payload(&[
            &rec(9), &rec(7), &rec(4), &rec(3), &rec(2), &rec(1),
        ]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    /// **Sorted merge — OFFSET + LIMIT**: OFFSET 2 LIMIT 3 over the
    /// merged [1,2,3,4,7,9] picks [3,4,7]. OFFSET applies AFTER the
    /// merge (per spec §3.4 — sorted-merge OFFSET cannot be pushed
    /// shard-side because rows interleave).
    #[test]
    fn merge_sorted_offset_and_limit() {
        let rec = |v: u64| -> Vec<u8> { v.to_le_bytes().to_vec() };
        let s0 = rows_to_payload(&[&rec(1), &rec(4), &rec(9)]);
        let s1 = rows_to_payload(&[&rec(2), &rec(3), &rec(7)]);
        let r = merge_scan_results(
            vec![OpResult::Got(s0.into()), OpResult::Got(s1.into())],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::U64,
                sort_offset: 0,
                sort_width: 8,
                desc: false,
                offset: 2,
                limit: 3,
            },
        );
        let expected = rows_to_payload(&[&rec(3), &rec(4), &rec(7)]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    /// **Sorted merge — K=1 byte-identical**: with one shard, the
    /// sorted merge is byte-identical to that shard's payload
    /// (modulo OFFSET/LIMIT, which the per-shard SelectSorted
    /// applies anyway). The killer "scatter on K=1 == single fat
    /// shard" property at the merge layer.
    #[test]
    fn merge_sorted_k1_byte_identical_to_single_shard() {
        let rec = |v: u64| -> Vec<u8> { v.to_le_bytes().to_vec() };
        let payload = rows_to_payload(&[&rec(2), &rec(5), &rec(11)]);
        let r = merge_scan_results(
            vec![OpResult::Got(payload.clone().into())],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::U64,
                sort_offset: 0,
                sort_width: 8,
                desc: false,
                offset: 0,
                limit: 0,
            },
        );
        assert_eq!(r, OpResult::Got(payload.into()));
    }

    /// **Sorted merge — empty shard mixed with non-empty**: an empty
    /// shard contributes nothing; the others' rows still merge in
    /// the correct sorted order.
    #[test]
    fn merge_sorted_with_one_empty_shard() {
        let rec = |v: u64| -> Vec<u8> { v.to_le_bytes().to_vec() };
        let s0 = rows_to_payload(&[&rec(1), &rec(5)]);
        let s1 = rows_to_payload(&[]); // empty middle shard
        let s2 = rows_to_payload(&[&rec(3), &rec(7)]);
        let r = merge_scan_results(
            vec![
                OpResult::Got(s0.into()),
                OpResult::Got(s1.into()),
                OpResult::Got(s2.into()),
            ],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::U64,
                sort_offset: 0,
                sort_width: 8,
                desc: false,
                offset: 0,
                limit: 0,
            },
        );
        let expected = rows_to_payload(&[
            &rec(1), &rec(3), &rec(5), &rec(7),
        ]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    /// **Sorted merge — signed (I32) negative ordering**: signed
    /// kinds use signed compare. Without the I-kind branch the
    /// negative values would sort as huge unsigned numbers. Locks
    /// that the merger uses `cmp_field`-shaped semantics (SP23 per-
    /// shard ordering invariant).
    #[test]
    fn merge_sorted_signed_i32_negative_orders_correctly() {
        // Records are 4-byte little-endian i32 only (simulates a
        // minimal type with one I32 field at offset 0).
        let rec = |v: i32| -> Vec<u8> { v.to_le_bytes().to_vec() };
        // Per shard: already in ascending signed order.
        let s0 = rows_to_payload(&[&rec(-100), &rec(0), &rec(50)]);
        let s1 = rows_to_payload(&[&rec(-10), &rec(20)]);
        let r = merge_scan_results(
            vec![OpResult::Got(s0.into()), OpResult::Got(s1.into())],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::I32,
                sort_offset: 0,
                sort_width: 4,
                desc: false,
                offset: 0,
                limit: 0,
            },
        );
        let expected = rows_to_payload(&[
            &rec(-100), &rec(-10), &rec(0), &rec(20), &rec(50),
        ]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    /// **Sorted merge — value tie tie-broken by shard_id**:
    /// two shards each have a row with the same U64 value. T2
    /// tie-breaks by shard_id (§5.4 caveat); the shard-0 row
    /// emerges first. Deterministic for fixed K.
    #[test]
    fn merge_sorted_tie_broken_by_shard_id() {
        let rec = |v: u64, tag: u8| -> Vec<u8> {
            // 8-byte sort field then a 1-byte tail tag so we can
            // tell the two same-key rows apart in the output.
            let mut r = v.to_le_bytes().to_vec();
            r.push(tag);
            r
        };
        let s0 = rows_to_payload(&[&rec(42, b'A')]);
        let s1 = rows_to_payload(&[&rec(42, b'B')]);
        let r = merge_scan_results(
            vec![OpResult::Got(s0.into()), OpResult::Got(s1.into())],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::U64,
                sort_offset: 0,
                sort_width: 8,
                desc: false,
                offset: 0,
                limit: 0,
            },
        );
        // shard 0 first (tie-break: smaller shard_id wins).
        let expected = rows_to_payload(&[&rec(42, b'A'), &rec(42, b'B')]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    // ---------- T3 K-invariance property sweep (SP155 §7.2 + acceptance #1) ----------
    //
    // The killer correctness check: for K ∈ {1, 2, 4, 8, 16}, the merged
    // SelectSorted output must be byte-identical to the K=1 baseline,
    // when the input rows are deterministically split across K shards
    // by a rendezvous-like hash + each shard's slice is locally sorted
    // (which is what the per-shard SM does, per kessel-sm:3540-3589).
    //
    // The property test runs at the *merge layer* — no TCP, no VSR,
    // no real shards. That keeps the per-seed cost at ~microseconds so
    // we can sweep 20+ seeds × 5 K values without bloating CI. The
    // real-socket K=1↔K=4 byte-identical test ships in `router.rs`
    // (T2's `scatter_select_sorted_k4_matches_k1_byte_identical`); this
    // sweep widens K-coverage at the merge layer where the tie-break
    // decision lives.
    //
    // We use **unique sort values** in the main property sweep — that's
    // the user-facing common case (a `created_at` timestamp, a unique
    // row id, etc.). The §5.4 tie-break caveat manifests only when
    // multiple rows have *byte-identical* sort values that fall on
    // different shards; that case is locked separately in
    // `merge_sorted_tie_broken_by_shard_id` (single-K determinism only,
    // not K-invariance).

    /// Mini rendezvous hash — same shape as `kessel_shard::ShardMap`
    /// but inlined to keep this module zero-dep on `kessel-shard`. Uses
    /// the splitmix64 PRNG from `kessel-proto` for hashing (the same
    /// PRNG the SM uses for deterministic seeds), so the assignment is
    /// reproducible and balanced.
    fn assign_shard(oid: &[u8; 16], k: usize) -> usize {
        if k <= 1 {
            return 0;
        }
        // Hash the (oid, shard_id) pair via splitmix64-folded bytes;
        // pick the shard with the max weight. Same idea as crc32c
        // rendezvous but without pulling in kessel-storage's key type.
        let oid_word_lo = u64::from_le_bytes(oid[..8].try_into().unwrap());
        let oid_word_hi = u64::from_le_bytes(oid[8..].try_into().unwrap());
        let mut best = (0usize, 0u64);
        for s in 0..k as u64 {
            let mut rng = kessel_proto::Rng::new(
                oid_word_lo
                    .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    .wrapping_add(oid_word_hi)
                    .wrapping_add(s.wrapping_mul(0xBF58_476D_1CE4_E5B9)),
            );
            let w = rng.next_u64();
            if w > best.1 {
                best = (s as usize, w);
            }
        }
        best.0
    }

    /// One row in the property-test fixture. Mimics the per-shard SM
    /// shape: an `oid` (16-byte object id) + a `record` blob whose
    /// sort field lives at `(offset, width)`.
    #[derive(Clone)]
    struct PropRow {
        oid: [u8; 16],
        record: Vec<u8>,
    }

    /// Build a fixture: `n` rows with unique U64 sort values + unique
    /// oids. The sort field is at byte offset 0, width 8.
    ///
    /// The sort values are a *random permutation* of `0..n` so they're
    /// unique and in a non-trivial order. The oids are `oid_seed[i]`
    /// — unique 16-byte sequences derived from the same PRNG.
    fn build_unique_fixture(seed: u64, n: u64) -> Vec<PropRow> {
        let mut rng = kessel_proto::Rng::new(seed);
        // Pre-allocate sort values 0..n, shuffle with Fisher-Yates so
        // each fixture is a different permutation but every value
        // shows up exactly once.
        let mut vals: Vec<u64> = (0..n).collect();
        for i in (1..vals.len()).rev() {
            let j = (rng.next_u64() as usize) % (i + 1);
            vals.swap(i, j);
        }
        // Build the rows: record = [sort_field_u64 LE][oid bytes] (8+16=24 bytes).
        // No header — keep the fixture minimal. The merger reads only
        // the `(offset, width)` slice it's told to.
        (0..n as usize)
            .map(|i| {
                let mut oid = [0u8; 16];
                rng.fill(&mut oid);
                let mut record = Vec::with_capacity(24);
                record.extend_from_slice(&vals[i].to_le_bytes());
                record.extend_from_slice(&oid);
                PropRow { oid, record }
            })
            .collect()
    }

    /// Distribute `rows` across `k` shards using the rendezvous mock,
    /// then for each shard sort that shard's rows by (sort_field_u64,
    /// oid) ascending — exactly the per-shard SM's `SelectSorted` total
    /// order (kessel-sm:3572). Return the per-shard `OpResult::Got`
    /// vec in shard-id order, ready for `merge_scan_results`.
    fn distribute_and_sort(
        rows: &[PropRow],
        k: usize,
        desc: bool,
    ) -> Vec<OpResult> {
        let mut shards: Vec<Vec<PropRow>> = (0..k).map(|_| Vec::new()).collect();
        for r in rows {
            let s = assign_shard(&r.oid, k);
            shards[s].push(r.clone());
        }
        shards
            .into_iter()
            .map(|mut s| {
                // Per-shard (value, oid) sort — matches kessel-sm:3572.
                s.sort_by(|a, b| {
                    let av = u64::from_le_bytes(a.record[..8].try_into().unwrap());
                    let bv = u64::from_le_bytes(b.record[..8].try_into().unwrap());
                    av.cmp(&bv).then(a.oid.cmp(&b.oid))
                });
                if desc {
                    s.reverse();
                }
                let payload: Vec<u8> = {
                    let mut p = Vec::new();
                    for r in &s {
                        p.extend_from_slice(&(r.record.len() as u32).to_le_bytes());
                        p.extend_from_slice(&r.record);
                    }
                    p
                };
                OpResult::Got(payload.into())
            })
            .collect()
    }

    /// **K-invariance property sweep (SP155 §7.2)**: for unique sort
    /// values, the merged `SelectSorted` output at K ∈ {1, 2, 4, 8, 16}
    /// MUST be byte-identical to the K=1 baseline. 25 seeds × 5 K values
    /// = 125 fixture runs; 100 rows × 24 bytes/row keeps the total merge
    /// cost in the milliseconds even in unoptimized test builds.
    ///
    /// This is the killer correctness check: the merge produces the
    /// same byte sequence regardless of K when the inputs are
    /// per-shard-sorted slices of the same underlying row set.
    #[test]
    fn k_invariance_select_sorted_unique_values_25_seeds() {
        let n_rows: u64 = 100;
        let ks: &[usize] = &[1, 2, 4, 8, 16];
        for seed in 0..25u64 {
            let rows = build_unique_fixture(seed, n_rows);
            // K=1 baseline: a single shard holding all rows, fully sorted.
            let k1 = distribute_and_sort(&rows, 1, false);
            let baseline = merge_scan_results(
                k1,
                &ScatterKind::Sorted {
                    sort_kind: FieldKind::U64,
                    sort_offset: 0,
                    sort_width: 8,
                    desc: false,
                    offset: 0,
                    limit: 0,
                },
            );
            for &k in ks {
                let per_shard = distribute_and_sort(&rows, k, false);
                let merged = merge_scan_results(
                    per_shard,
                    &ScatterKind::Sorted {
                        sort_kind: FieldKind::U64,
                        sort_offset: 0,
                        sort_width: 8,
                        desc: false,
                        offset: 0,
                        limit: 0,
                    },
                );
                assert_eq!(
                    merged, baseline,
                    "seed={seed} k={k}: K-invariance violated (\
                     SelectSorted merged bytes differ from K=1 baseline)"
                );
            }
        }
    }

    /// **K-invariance with descending order**: same property but with
    /// `desc=true`. Locks that the descending heap polarity is correct
    /// across K.
    #[test]
    fn k_invariance_select_sorted_desc_unique_values_20_seeds() {
        let n_rows: u64 = 100;
        let ks: &[usize] = &[1, 2, 4, 8, 16];
        for seed in 200..220u64 {
            let rows = build_unique_fixture(seed, n_rows);
            let k1 = distribute_and_sort(&rows, 1, true);
            let baseline = merge_scan_results(
                k1,
                &ScatterKind::Sorted {
                    sort_kind: FieldKind::U64,
                    sort_offset: 0,
                    sort_width: 8,
                    desc: true,
                    offset: 0,
                    limit: 0,
                },
            );
            for &k in ks {
                let per_shard = distribute_and_sort(&rows, k, true);
                let merged = merge_scan_results(
                    per_shard,
                    &ScatterKind::Sorted {
                        sort_kind: FieldKind::U64,
                        sort_offset: 0,
                        sort_width: 8,
                        desc: true,
                        offset: 0,
                        limit: 0,
                    },
                );
                assert_eq!(
                    merged, baseline,
                    "seed={seed} k={k} desc=true: K-invariance violated"
                );
            }
        }
    }

    /// **K-invariance with OFFSET + LIMIT**: same property but with a
    /// non-zero OFFSET / LIMIT in the merge loop. Locks that the
    /// post-merge slicing is K-invariant.
    #[test]
    fn k_invariance_select_sorted_offset_limit_15_seeds() {
        let n_rows: u64 = 100;
        let ks: &[usize] = &[1, 2, 4, 8];
        for seed in 500..515u64 {
            let rows = build_unique_fixture(seed, n_rows);
            let k1 = distribute_and_sort(&rows, 1, false);
            // OFFSET 20 LIMIT 30 over 100 rows = rows [20..50] of the
            // sorted output.
            let baseline = merge_scan_results(
                k1,
                &ScatterKind::Sorted {
                    sort_kind: FieldKind::U64,
                    sort_offset: 0,
                    sort_width: 8,
                    desc: false,
                    offset: 20,
                    limit: 30,
                },
            );
            for &k in ks {
                let per_shard = distribute_and_sort(&rows, k, false);
                let merged = merge_scan_results(
                    per_shard,
                    &ScatterKind::Sorted {
                        sort_kind: FieldKind::U64,
                        sort_offset: 0,
                        sort_width: 8,
                        desc: false,
                        offset: 20,
                        limit: 30,
                    },
                );
                assert_eq!(
                    merged, baseline,
                    "seed={seed} k={k} offset=20 limit=30: K-invariance violated"
                );
            }
        }
    }

    /// **Unordered K-invariance (multiset equality)**: for `Select` /
    /// `QueryRows` / `SelectFields`, the merge is shard-id-ordered
    /// concat — the *byte* sequence varies with K (because rows
    /// distribute differently across shards), but the **multiset of
    /// rows** must be identical to K=1.
    ///
    /// Lock the multiset invariance: parse the merged payload, collect
    /// rows into a `BTreeSet`, assert equality across K. This is the
    /// honest acceptance criterion for unordered scatter merges per
    /// the spec's §3.6 "deterministic but order varies with K"
    /// language.
    #[test]
    fn k_invariance_unordered_multiset_equality_25_seeds() {
        use std::collections::BTreeSet;
        let n_rows: u64 = 100;
        let ks: &[usize] = &[1, 2, 4, 8, 16];

        fn payload_to_set(out: &OpResult) -> std::collections::BTreeSet<Vec<u8>> {
            let bytes = match out {
                OpResult::Got(b) => b,
                other => panic!("expected Got, got {other:?}"),
            };
            let mut set = BTreeSet::new();
            let mut p = 0;
            while p < bytes.len() {
                let len = u32::from_le_bytes(
                    bytes[p..p + 4].try_into().unwrap(),
                ) as usize;
                p += 4;
                set.insert(bytes[p..p + len].to_vec());
                p += len;
            }
            set
        }

        for seed in 1000..1025u64 {
            let rows = build_unique_fixture(seed, n_rows);
            // For unordered scans, each shard returns rows in its
            // local storage-key order (i.e. `oid` ascending — matches
            // kessel-sm:3540's scan_range over `[(type,0)..(type,FF)]`).
            // Build that shape and merge.
            let mk_unordered = |k: usize| -> OpResult {
                let mut shards: Vec<Vec<PropRow>> =
                    (0..k).map(|_| Vec::new()).collect();
                for r in &rows {
                    let s = assign_shard(&r.oid, k);
                    shards[s].push(r.clone());
                }
                let per_shard: Vec<OpResult> = shards
                    .into_iter()
                    .map(|mut s| {
                        s.sort_by(|a, b| a.oid.cmp(&b.oid));
                        let mut p = Vec::new();
                        for r in &s {
                            p.extend_from_slice(
                                &(r.record.len() as u32).to_le_bytes(),
                            );
                            p.extend_from_slice(&r.record);
                        }
                        OpResult::Got(p.into())
                    })
                    .collect();
                merge_scan_results(
                    per_shard,
                    &ScatterKind::Unordered { limit: 0 },
                )
            };
            let baseline_set = payload_to_set(&mk_unordered(1));
            assert_eq!(
                baseline_set.len(),
                n_rows as usize,
                "seed={seed}: K=1 baseline must have all {n_rows} rows"
            );
            for &k in ks {
                let set = payload_to_set(&mk_unordered(k));
                assert_eq!(
                    set, baseline_set,
                    "seed={seed} k={k}: unordered scatter multiset \
                     diverged from K=1 baseline"
                );
            }
        }
    }

    // ---------- T4 sort-key extraction edges (SP155 §3.3) ----------
    //
    // The current `extract_sort_key(record, offset, width)` is a simple
    // byte-slice copy + `cmp_sort_value` dispatches on `FieldKind`. T4
    // stresses the edges: Char (variable-length lexicographic), Bytes
    // (raw bytes, no UTF-8 assumption), NULL bitmap (per spec §3.3),
    // empty-string vs absent column, sort field at non-zero offset.
    //
    // Lock the decisions:
    //
    //   - **NULLs sort by their raw stored bytes** (typically all-zero
    //     padding). The per-shard SM (kessel-sm:3567) reads the field's
    //     fixed-width byte slice without consulting the null bitmap;
    //     the merger matches that. NULLs therefore sort FIRST in
    //     ascending order (zero bytes ≤ any positive value). Documented;
    //     a future `Op::SelectSortedWithKey` (spec OQ8) could surface a
    //     NULL discriminator if Postgres-style "NULLS LAST" is needed.
    //
    //   - **Empty string vs single-char**: "" (zero-padded full width)
    //     vs "a" (then zero pad) compare as `cmp(&"")=Less` because the
    //     'a' byte > 0 — lexicographic byte order.
    //
    //   - **Sort field at non-zero offset**: the merger reads exactly
    //     `record[offset..offset+width]`. Any preceding fields are
    //     untouched. Locked by `merge_sorted_sort_field_at_nonzero_column_offset`.

    /// **Char column sort key (lexicographic, byte-correct, no locale)**.
    /// Sort field is a Char(8) — fixed-width 8-byte text. Records have
    /// the Char field at offset 0. Locks that lexicographic compare is
    /// pure byte compare (no UTF-8 / locale dependence per spec §3.3).
    #[test]
    fn merge_sorted_char_column_lexicographic() {
        // Char(8): each record is exactly 8 bytes, zero-padded.
        let s = |text: &str| -> Vec<u8> {
            let mut r = [0u8; 8];
            let bytes = text.as_bytes();
            let n = bytes.len().min(8);
            r[..n].copy_from_slice(&bytes[..n]);
            r.to_vec()
        };
        // Per-shard already in lex order.
        let s0 = rows_to_payload(&[&s("apple"), &s("banana"), &s("zebra")]);
        let s1 = rows_to_payload(&[&s("aardvark"), &s("cat"), &s("yak")]);
        let r = merge_scan_results(
            vec![OpResult::Got(s0.into()), OpResult::Got(s1.into())],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::Char(8),
                sort_offset: 0,
                sort_width: 8,
                desc: false,
                offset: 0,
                limit: 0,
            },
        );
        let expected = rows_to_payload(&[
            &s("aardvark"),
            &s("apple"),
            &s("banana"),
            &s("cat"),
            &s("yak"),
            &s("zebra"),
        ]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    /// **Bytes column sort key (raw bytes, no UTF-8 assumption)**. Same
    /// as Char but the column may contain non-printable / non-UTF-8
    /// bytes (binary keys, hashes, etc). The merger uses byte compare,
    /// so `0xFF` > `0x80` > `0x01` > `0x00`.
    #[test]
    fn merge_sorted_bytes_column_raw_byte_compare() {
        let b = |bs: &[u8]| -> Vec<u8> {
            let mut r = vec![0u8; 4];
            r[..bs.len().min(4)].copy_from_slice(&bs[..bs.len().min(4)]);
            r
        };
        let s0 = rows_to_payload(&[
            &b(&[0x00, 0x00, 0x00, 0x00]),
            &b(&[0x7F, 0xFF, 0x00, 0x00]),
        ]);
        let s1 = rows_to_payload(&[
            &b(&[0x01, 0x00, 0x00, 0x00]),
            &b(&[0xFF, 0xFE, 0xFD, 0xFC]),
        ]);
        let r = merge_scan_results(
            vec![OpResult::Got(s0.into()), OpResult::Got(s1.into())],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::Bytes(4),
                sort_offset: 0,
                sort_width: 4,
                desc: false,
                offset: 0,
                limit: 0,
            },
        );
        let expected = rows_to_payload(&[
            &b(&[0x00, 0x00, 0x00, 0x00]),
            &b(&[0x01, 0x00, 0x00, 0x00]),
            &b(&[0x7F, 0xFF, 0x00, 0x00]),
            &b(&[0xFF, 0xFE, 0xFD, 0xFC]),
        ]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    /// **NULL sort key handling (NULLs sort FIRST in ascending)**. The
    /// per-shard SM (kessel-sm:3567) does NOT consult the null bitmap
    /// — it reads the field's fixed-width slice raw. A NULL field
    /// stored as zero-padded bytes therefore compares as if it were
    /// the zero value of the field's kind. For U64 that's value `0`,
    /// which sorts FIRST in ascending order.
    ///
    /// This KAT locks the decision: V1 inherits the SM's
    /// "NULL == zero-padded raw bytes" semantics. Documented in the
    /// module doc; a future `SelectSortedWithKey` (spec OQ8) could
    /// surface an explicit NULL discriminator if Postgres-style
    /// "NULLS LAST" is wanted.
    #[test]
    fn merge_sorted_nulls_sort_first_ascending_u64() {
        let rec = |v: u64| -> Vec<u8> { v.to_le_bytes().to_vec() };
        // NULL stored as 8 zero bytes (matches the per-shard SM's
        // zero-padded slice for a not-yet-written nullable field).
        let null_row: Vec<u8> = vec![0u8; 8];
        let s0 = rows_to_payload(&[&null_row, &rec(5), &rec(100)]);
        let s1 = rows_to_payload(&[&rec(2), &rec(50)]);
        let r = merge_scan_results(
            vec![OpResult::Got(s0.into()), OpResult::Got(s1.into())],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::U64,
                sort_offset: 0,
                sort_width: 8,
                desc: false,
                offset: 0,
                limit: 0,
            },
        );
        // NULL (0 bytes) sorts first; ascending: 0, 2, 5, 50, 100.
        let expected = rows_to_payload(&[
            &null_row,
            &rec(2),
            &rec(5),
            &rec(50),
            &rec(100),
        ]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    /// **NULLs sort LAST descending** (the natural consequence of the
    /// "NULL == zero-padded" decision: descending flips the order).
    #[test]
    fn merge_sorted_nulls_sort_last_descending_u64() {
        let rec = |v: u64| -> Vec<u8> { v.to_le_bytes().to_vec() };
        let null_row: Vec<u8> = vec![0u8; 8];
        // Per-shard SM in desc mode returns rows desc-sorted; replicate
        // that (the NULL on shard 0 ends up last there, and on shard 1
        // there are no NULLs).
        let s0 = rows_to_payload(&[&rec(100), &rec(5), &null_row]);
        let s1 = rows_to_payload(&[&rec(50), &rec(2)]);
        let r = merge_scan_results(
            vec![OpResult::Got(s0.into()), OpResult::Got(s1.into())],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::U64,
                sort_offset: 0,
                sort_width: 8,
                desc: true,
                offset: 0,
                limit: 0,
            },
        );
        let expected = rows_to_payload(&[
            &rec(100),
            &rec(50),
            &rec(5),
            &rec(2),
            &null_row,
        ]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    /// **Empty string vs non-empty Char(8)**. The empty string is
    /// 8 zero bytes; "a" is `[b'a', 0, 0, 0, 0, 0, 0, 0]`. Byte compare:
    /// empty < "a" because 0 < 'a'. Locks the per spec §3.3 decision
    /// "" < any non-empty string in ascending order.
    #[test]
    fn merge_sorted_empty_string_less_than_nonempty_char() {
        let s = |text: &str| -> Vec<u8> {
            let mut r = [0u8; 8];
            let bytes = text.as_bytes();
            let n = bytes.len().min(8);
            r[..n].copy_from_slice(&bytes[..n]);
            r.to_vec()
        };
        let empty = s("");
        let s0 = rows_to_payload(&[&empty, &s("a"), &s("z")]);
        let s1 = rows_to_payload(&[&s("b"), &s("m")]);
        let r = merge_scan_results(
            vec![OpResult::Got(s0.into()), OpResult::Got(s1.into())],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::Char(8),
                sort_offset: 0,
                sort_width: 8,
                desc: false,
                offset: 0,
                limit: 0,
            },
        );
        let expected = rows_to_payload(&[
            &empty,
            &s("a"),
            &s("b"),
            &s("m"),
            &s("z"),
        ]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    /// **Sort field at a non-zero column offset**. Simulate a row with
    /// a leading "header" of bytes the merger should ignore, then the
    /// sort field at offset 16, width 8 (a U64). The merger must read
    /// bytes [16..24] for the sort key and leave the rest of the row
    /// untouched.
    #[test]
    fn merge_sorted_sort_field_at_nonzero_column_offset() {
        // record = [16 bytes of "header" / preceding columns][U64 sort field][trailing tag byte]
        // total len = 16 + 8 + 1 = 25 bytes.
        let rec = |hdr_byte: u8, sort_val: u64, tag: u8| -> Vec<u8> {
            let mut r = Vec::with_capacity(25);
            r.extend(std::iter::repeat(hdr_byte).take(16));
            r.extend_from_slice(&sort_val.to_le_bytes());
            r.push(tag);
            r
        };
        // Per-shard already in (sort_value) ascending order.
        let s0 = rows_to_payload(&[
            &rec(0xAA, 1, b'a'),
            &rec(0xBB, 7, b'b'),
        ]);
        let s1 = rows_to_payload(&[
            &rec(0xCC, 3, b'c'),
            &rec(0xDD, 5, b'd'),
        ]);
        let r = merge_scan_results(
            vec![OpResult::Got(s0.into()), OpResult::Got(s1.into())],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::U64,
                sort_offset: 16,
                sort_width: 8,
                desc: false,
                offset: 0,
                limit: 0,
            },
        );
        // Expected merge by sort field (at offset 16): 1, 3, 5, 7.
        let expected = rows_to_payload(&[
            &rec(0xAA, 1, b'a'),
            &rec(0xCC, 3, b'c'),
            &rec(0xDD, 5, b'd'),
            &rec(0xBB, 7, b'b'),
        ]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    /// **Record shorter than sort field offset+width → SchemaError**.
    /// Defense against a malformed row that's too short to contain the
    /// claimed sort field. The merger surfaces `SchemaError`, not a
    /// panic, per SP155 §6 malformed-row defense.
    #[test]
    fn merge_sorted_record_too_short_surfaces_schema_error() {
        // Sort field claims offset=0 width=8 but the record is only 4 bytes.
        let short = vec![1u8, 2, 3, 4];
        let s0 = rows_to_payload(&[&short]);
        let r = merge_scan_results(
            vec![OpResult::Got(s0.into())],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::U64,
                sort_offset: 0,
                sort_width: 8,
                desc: false,
                offset: 0,
                limit: 0,
            },
        );
        assert!(
            matches!(r, OpResult::SchemaError(_)),
            "record too short for sort field must surface SchemaError, got {r:?}"
        );
    }

    /// **I64 negative signed compare with NULL (zero bytes)**. NULL == 0
    /// per the §3.3 decision; in signed compare 0 is greater than any
    /// negative value, so NULLs sort BETWEEN negatives and positives
    /// (i.e. at the "0" position) — NOT first. Documented edge: NULL
    /// semantics under signed kinds differ from unsigned.
    #[test]
    fn merge_sorted_nulls_in_signed_i64_sort_at_zero_position() {
        let rec = |v: i64| -> Vec<u8> { v.to_le_bytes().to_vec() };
        let null_row: Vec<u8> = vec![0u8; 8];
        let s0 = rows_to_payload(&[&rec(-100), &null_row, &rec(50)]);
        let s1 = rows_to_payload(&[&rec(-10), &rec(20)]);
        let r = merge_scan_results(
            vec![OpResult::Got(s0.into()), OpResult::Got(s1.into())],
            &ScatterKind::Sorted {
                sort_kind: FieldKind::I64,
                sort_offset: 0,
                sort_width: 8,
                desc: false,
                offset: 0,
                limit: 0,
            },
        );
        // Ascending signed: -100, -10, 0(NULL), 20, 50.
        let expected = rows_to_payload(&[
            &rec(-100),
            &rec(-10),
            &null_row,
            &rec(20),
            &rec(50),
        ]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    // ---------- T6 LIMIT-cancellation KATs (SP155 §3.7 / §7.3) ----------
    //
    // T2-T5 ship the merge with a LIMIT cap (`merge_unordered_respects_limit`
    // locks the cap). T6 adds **cancellation propagation**: when the
    // unordered merge has its LIMIT, the shared `Arc<AtomicBool>` cancel
    // flag fires the instant the buffer fills, so late shards (whose
    // slots the merge doesn't need) see the flag at their `call_with_
    // cancel` boundary and don't keep the router pinned. The default
    // `call_with_cancel` impl observes the flag pre-call only (SP155
    // §3.7 honest gap: std::net::TcpStream has no cancellable read);
    // these KATs exercise that boundary by driving cancellation against
    // a mock that respects the flag.
    //
    // The mock used here (`CancellableMockShard`) is a `ShardCaller`
    // with a built-in pre-call cancel check + a configurable sleep,
    // letting us deterministically order: shard_0 returns enough rows
    // to fill LIMIT, shard_1+'s sleep is interrupted by cancel firing
    // BEFORE its `call` even starts — its `ran` counter stays at 0.

    use std::sync::atomic::AtomicBool;

    /// Mock shard that respects `cancel` at the call boundary AND
    /// optionally polls during a configurable sleep — gives the KATs
    /// fine-grained control over when the cancel flag is observed.
    struct CancellableMockShard {
        canned: OpResult,
        /// Sleep BEFORE `call()` returns. Polled in 10ms increments
        /// against `cancel` so the worker exits promptly when the
        /// merge fires the flag.
        sleep_pre: Duration,
        /// Bumped on every `call_with_cancel` invocation that GETS
        /// PAST the pre-call cancel check (i.e. actually ran).
        ran: Arc<AtomicUsize>,
        /// Bumped if the pre-call cancel check fired — proves the
        /// flag was seen.
        cancelled_pre_call: Arc<AtomicUsize>,
    }

    impl CancellableMockShard {
        fn new(canned: OpResult) -> Self {
            CancellableMockShard {
                canned,
                sleep_pre: Duration::from_millis(0),
                ran: Arc::new(AtomicUsize::new(0)),
                cancelled_pre_call: Arc::new(AtomicUsize::new(0)),
            }
        }
        fn slow(mut self, d: Duration) -> Self {
            self.sleep_pre = d;
            self
        }
    }

    impl ShardCaller for CancellableMockShard {
        fn call(&mut self, _op: &Op) -> Result<OpResult, String> {
            self.ran.fetch_add(1, Ordering::SeqCst);
            if self.sleep_pre > Duration::from_millis(0) {
                thread::sleep(self.sleep_pre);
            }
            Ok(self.canned.clone())
        }
        fn call_with_cancel(
            &mut self,
            op: &Op,
            cancel: &Arc<AtomicBool>,
        ) -> Result<OpResult, String> {
            // Pre-call: check + poll in small slices so the cancel
            // flag set by the merge propagates promptly. Locks the
            // SP155 §3.7 "cancel observed pre-call" contract for
            // workers whose `call` hasn't started yet.
            if cancel.load(Ordering::SeqCst) {
                self.cancelled_pre_call.fetch_add(1, Ordering::SeqCst);
                return Ok(OpResult::Unavailable);
            }
            if self.sleep_pre > Duration::from_millis(0) {
                // Poll cancel every 5ms during the sleep — KATs can
                // assert workers exit promptly after the flag fires.
                let step = Duration::from_millis(5);
                let mut remaining = self.sleep_pre;
                while remaining > Duration::from_millis(0) {
                    if cancel.load(Ordering::SeqCst) {
                        self.cancelled_pre_call
                            .fetch_add(1, Ordering::SeqCst);
                        return Ok(OpResult::Unavailable);
                    }
                    let s = if remaining > step { step } else { remaining };
                    thread::sleep(s);
                    remaining = remaining.saturating_sub(s);
                }
            }
            self.ran.fetch_add(1, Ordering::SeqCst);
            self.call(op)
        }
    }

    /// **T6 KAT — LIMIT 5 over 3 shards × 100 rows each returns exactly
    /// 5 rows + the merged payload is shard-0's first 5 rows.**
    /// `Unordered` merge is shard-id-ordered concat (SP155 §3.6); LIMIT
    /// 5 stops mid-shard-0 and emits exactly 5 rows. Locks that the
    /// scatter_and_merge integration produces the same result as the
    /// gather+merge composition.
    #[test]
    fn scatter_and_merge_unordered_limit_caps_at_exactly_n_rows() {
        let rec = |v: u8| -> Vec<u8> { vec![v; 8] };
        let mk_payload = |start: u8| -> OpResult {
            let rows: Vec<Vec<u8>> =
                (0..100u8).map(|i| rec(start.wrapping_add(i))).collect();
            let refs: Vec<&[u8]> = rows.iter().map(|v| v.as_slice()).collect();
            OpResult::Got(rows_to_payload(&refs).into())
        };
        let s0 = CancellableMockShard::new(mk_payload(0));
        let s1 = CancellableMockShard::new(mk_payload(100));
        let s2 = CancellableMockShard::new(mk_payload(200));
        let cancel = Arc::new(AtomicBool::new(false));
        let out = scatter_and_merge(
            vec![s0, s1, s2],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 5 },
            cancel.clone(),
        );
        let bytes = match &out {
            OpResult::Got(b) => b,
            other => panic!("expected Got, got {other:?}"),
        };
        // Walk the payload: count rows.
        let mut p = 0usize;
        let mut count = 0usize;
        while p < bytes.len() {
            let len =
                u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
            p += 4 + len;
            count += 1;
        }
        assert_eq!(count, 5, "LIMIT 5 must yield exactly 5 rows");
        // Cancel flag must be set (the merge fired it on LIMIT-hit).
        assert!(
            cancel.load(Ordering::SeqCst),
            "cancel flag must be set after LIMIT-hit"
        );
    }

    /// **T6 KAT — LIMIT-hit propagates cancel to NOT-YET-STARTED
    /// shards.** shard_0 returns 100 rows instantly so LIMIT 5 fires
    /// after draining shard_0's slot; shards 1+2 sleep 200ms PRE-
    /// CALL. By the time shard_0's slot is drained and cancel fires,
    /// shard_1/shard_2 are still in their pre-call poll loop — they
    /// see the flag and exit early (`cancelled_pre_call` bumped,
    /// `ran` stays 0). This is the SP155 §3.7 contract: cancel
    /// observed pre-call.
    #[test]
    fn scatter_and_merge_limit_cancels_pending_shards() {
        let rec = |v: u8| -> Vec<u8> { vec![v; 8] };
        let mk_payload = |n: usize, base: u8| -> OpResult {
            let rows: Vec<Vec<u8>> = (0..n as u8)
                .map(|i| rec(base.wrapping_add(i)))
                .collect();
            let refs: Vec<&[u8]> = rows.iter().map(|v| v.as_slice()).collect();
            OpResult::Got(rows_to_payload(&refs).into())
        };
        // shard_0: fast, 100 rows.
        let s0 = CancellableMockShard::new(mk_payload(100, 0));
        // shard_1, shard_2: SLOW (200ms pre-call), 100 rows each.
        let s1 = CancellableMockShard::new(mk_payload(100, 100))
            .slow(Duration::from_millis(200));
        let s2 = CancellableMockShard::new(mk_payload(100, 200))
            .slow(Duration::from_millis(200));
        // Capture the counters BEFORE moving the shards.
        let ran1 = s1.ran.clone();
        let ran2 = s2.ran.clone();
        let cancelled1 = s1.cancelled_pre_call.clone();
        let cancelled2 = s2.cancelled_pre_call.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        let t0 = Instant::now();
        let out = scatter_and_merge(
            vec![s0, s1, s2],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 5 },
            cancel.clone(),
        );
        let elapsed = t0.elapsed();
        // Result locked: exactly 5 rows from shard_0.
        let bytes = match &out {
            OpResult::Got(b) => b,
            other => panic!("expected Got, got {other:?}"),
        };
        let mut count = 0usize;
        let mut p = 0usize;
        while p < bytes.len() {
            let len =
                u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
            p += 4 + len;
            count += 1;
        }
        assert_eq!(count, 5, "LIMIT 5 must yield 5 rows");
        // shard_1 + shard_2 saw the cancel flag pre-call (their `ran`
        // never incremented; their `cancelled_pre_call` did).
        assert_eq!(
            ran1.load(Ordering::SeqCst),
            0,
            "shard_1 must NOT have completed its call"
        );
        assert_eq!(
            ran2.load(Ordering::SeqCst),
            0,
            "shard_2 must NOT have completed its call"
        );
        assert!(
            cancelled1.load(Ordering::SeqCst) >= 1,
            "shard_1 must have observed cancel pre-call"
        );
        assert!(
            cancelled2.load(Ordering::SeqCst) >= 1,
            "shard_2 must have observed cancel pre-call"
        );
        // Wall-clock: should return MUCH faster than the full 200ms
        // sleep — the cancel-poll step is 5ms, so we expect <100ms
        // even on busy CI. (Loose bound: well under 200ms × 2.)
        assert!(
            elapsed < Duration::from_millis(180),
            "scatter_and_merge must return promptly after LIMIT-hit \
             (elapsed {elapsed:?})"
        );
    }

    /// **T6 KAT — LIMIT 0 returns ALL rows + cancel never fires
    /// (during the gather; the post-gather belt-and-suspenders does
    /// set it, but no shard is short-circuited).** `limit == 0` is
    /// the "no cap" sentinel; every shard contributes.
    #[test]
    fn scatter_and_merge_unordered_limit_zero_drains_every_shard() {
        let rec = |v: u8| -> Vec<u8> { vec![v; 8] };
        let mk_payload = |n: usize, base: u8| -> OpResult {
            let rows: Vec<Vec<u8>> =
                (0..n as u8).map(|i| rec(base.wrapping_add(i))).collect();
            let refs: Vec<&[u8]> = rows.iter().map(|v| v.as_slice()).collect();
            OpResult::Got(rows_to_payload(&refs).into())
        };
        let s0 = CancellableMockShard::new(mk_payload(3, 0));
        let s1 = CancellableMockShard::new(mk_payload(3, 100));
        let s2 = CancellableMockShard::new(mk_payload(3, 200));
        let ran0 = s0.ran.clone();
        let ran1 = s1.ran.clone();
        let ran2 = s2.ran.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        let out = scatter_and_merge(
            vec![s0, s1, s2],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel.clone(),
        );
        let bytes = match &out {
            OpResult::Got(b) => b,
            other => panic!("expected Got, got {other:?}"),
        };
        let mut count = 0usize;
        let mut p = 0usize;
        while p < bytes.len() {
            let len =
                u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
            p += 4 + len;
            count += 1;
        }
        assert_eq!(count, 9, "LIMIT 0 must yield all 3×3=9 rows");
        // Every shard's call DID run (no pre-call short-circuit
        // during the gather; the post-gather belt-and-suspenders
        // sets cancel but at that point every worker already ran).
        assert!(ran0.load(Ordering::SeqCst) >= 1);
        assert!(ran1.load(Ordering::SeqCst) >= 1);
        assert!(ran2.load(Ordering::SeqCst) >= 1);
    }

    /// **T6 KAT — pre-cancelled flag returns an empty Got + spawns
    /// no workers.** If the caller passes a flag that's already set
    /// (e.g. a Drop-time cancel from a concurrent timeout, or a
    /// downstream LIMIT-already-satisfied state), the function
    /// short-circuits before spawning. Locks the SP155 §3.7 "cancel
    /// = stop scanning at the strongest possible point" contract.
    #[test]
    fn scatter_and_merge_precancelled_returns_empty() {
        let s0 = CancellableMockShard::new(OpResult::Got(vec![1, 2, 3, 4].into()));
        let s1 = CancellableMockShard::new(OpResult::Got(vec![5, 6, 7, 8].into()));
        let ran0 = s0.ran.clone();
        let ran1 = s1.ran.clone();
        let cancel = Arc::new(AtomicBool::new(true)); // pre-cancelled
        let out = scatter_and_merge(
            vec![s0, s1],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel,
        );
        assert_eq!(
            out,
            OpResult::Got(Vec::<u8>::new().into()),
            "pre-cancelled scatter returns empty Got"
        );
        // No workers ran (we never spawned).
        assert_eq!(
            ran0.load(Ordering::SeqCst),
            0,
            "no worker spawned for shard_0"
        );
        assert_eq!(
            ran1.load(Ordering::SeqCst),
            0,
            "no worker spawned for shard_1"
        );
    }

    /// **T6 KAT — LIMIT > total rows: no cancel mid-gather; every
    /// shard contributes; result is every row in shard-id-ordered
    /// concat.** Locks that "LIMIT larger than total" doesn't trip
    /// the short-circuit.
    #[test]
    fn scatter_and_merge_limit_larger_than_total_returns_everything() {
        let rec = |v: u8| -> Vec<u8> { vec![v; 8] };
        let mk_payload = |n: usize, base: u8| -> OpResult {
            let rows: Vec<Vec<u8>> =
                (0..n as u8).map(|i| rec(base.wrapping_add(i))).collect();
            let refs: Vec<&[u8]> = rows.iter().map(|v| v.as_slice()).collect();
            OpResult::Got(rows_to_payload(&refs).into())
        };
        let s0 = CancellableMockShard::new(mk_payload(2, 0));
        let s1 = CancellableMockShard::new(mk_payload(2, 100));
        let s2 = CancellableMockShard::new(mk_payload(2, 200));
        let ran0 = s0.ran.clone();
        let ran1 = s1.ran.clone();
        let ran2 = s2.ran.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        let out = scatter_and_merge(
            vec![s0, s1, s2],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 100 }, // > 6 total
            cancel,
        );
        let bytes = match &out {
            OpResult::Got(b) => b,
            other => panic!("expected Got, got {other:?}"),
        };
        let mut count = 0usize;
        let mut p = 0usize;
        while p < bytes.len() {
            let len =
                u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
            p += 4 + len;
            count += 1;
        }
        assert_eq!(count, 6, "LIMIT 100 over 6 rows total yields 6 rows");
        // All three shards' workers completed (no pre-call cancel
        // during the gather phase).
        assert!(ran0.load(Ordering::SeqCst) >= 1);
        assert!(ran1.load(Ordering::SeqCst) >= 1);
        assert!(ran2.load(Ordering::SeqCst) >= 1);
    }

    /// **T6 KAT — cancellation does NOT leak threads.** All worker
    /// handles are joined before `scatter_and_merge` returns, even
    /// for the LIMIT-cancellation path. We assert this two ways:
    /// (a) the function returns within a bounded wall-clock window;
    /// (b) the slow workers' `ran`+`cancelled_pre_call` counters
    /// REACH their terminal state by the time we return (i.e. the
    /// worker thread has exited and updated its counter — `ran` only
    /// bumps inside the worker thread).
    #[test]
    fn scatter_and_merge_cancellation_does_not_leak_threads() {
        let rec = |v: u8| -> Vec<u8> { vec![v; 8] };
        let mk_payload = |n: usize, base: u8| -> OpResult {
            let rows: Vec<Vec<u8>> =
                (0..n as u8).map(|i| rec(base.wrapping_add(i))).collect();
            let refs: Vec<&[u8]> = rows.iter().map(|v| v.as_slice()).collect();
            OpResult::Got(rows_to_payload(&refs).into())
        };
        // shard_0: fast, fills LIMIT.
        let s0 = CancellableMockShard::new(mk_payload(10, 0));
        // shard_1: slow (300ms), should be cancelled pre-call.
        let s1 = CancellableMockShard::new(mk_payload(10, 100))
            .slow(Duration::from_millis(300));
        let cancelled1 = s1.cancelled_pre_call.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        let t0 = Instant::now();
        let _out = scatter_and_merge(
            vec![s0, s1],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 3 },
            cancel,
        );
        let elapsed = t0.elapsed();
        // BY THE TIME we return, every worker has joined — proven
        // by shard_1's `cancelled_pre_call` having been bumped from
        // inside the worker thread. (Atomic bumps from a not-yet-
        // joined thread are visible but the function's join loop
        // guarantees the thread completed.)
        assert!(
            cancelled1.load(Ordering::SeqCst) >= 1,
            "shard_1 worker thread must have run + bumped \
             cancelled_pre_call before scatter_and_merge returned"
        );
        // Wall-clock: well under the full 300ms shard_1 sleep,
        // proving the cancel propagated promptly (no leaked thread
        // that would hold us until 300ms elapsed).
        assert!(
            elapsed < Duration::from_millis(250),
            "elapsed {elapsed:?} must be << 300ms (worker cancelled \
             promptly, no leak)"
        );
    }

    /// **T6 KAT — Sorted merge gathers all shards even with LIMIT.**
    /// A k-way heap merge needs every shard's payload upfront to
    /// peek the next smallest row; LIMIT is applied in the merge
    /// loop, NOT the gather. Locks that Sorted's cancel behavior is
    /// "fire post-gather" (a no-op for the current gather, but kept
    /// as a seam for future streaming sorted-merge — SP-A T7+).
    #[test]
    fn scatter_and_merge_sorted_limit_still_gathers_all_shards() {
        let rec = |v: u64| -> Vec<u8> { v.to_le_bytes().to_vec() };
        let s0_payload = rows_to_payload(&[&rec(1), &rec(4), &rec(9)]);
        let s1_payload = rows_to_payload(&[&rec(2), &rec(3), &rec(7)]);
        let s0 = CancellableMockShard::new(OpResult::Got(s0_payload.into()));
        let s1 = CancellableMockShard::new(OpResult::Got(s1_payload.into()));
        let ran0 = s0.ran.clone();
        let ran1 = s1.ran.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        let out = scatter_and_merge(
            vec![s0, s1],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Sorted {
                sort_kind: FieldKind::U64,
                sort_offset: 0,
                sort_width: 8,
                desc: false,
                offset: 0,
                limit: 3, // way less than 6 total
            },
            cancel.clone(),
        );
        // Both shards' workers ran (Sorted needs all data).
        assert!(ran0.load(Ordering::SeqCst) >= 1);
        assert!(ran1.load(Ordering::SeqCst) >= 1);
        // Result: ascending [1, 2, 3] (LIMIT 3).
        let expected = rows_to_payload(&[&rec(1), &rec(2), &rec(3)]);
        assert_eq!(out, OpResult::Got(expected.into()));
        // Post-gather cancel set.
        assert!(cancel.load(Ordering::SeqCst));
    }

    /// **T6 KAT — non-Got slot fires cancel + V1 hard-fail surfaces.**
    /// A shard returning `Unavailable` short-circuits the gather (per
    /// SP155 §6); cancel fires; the merged result is that first non-
    /// Got slot. Late shards see cancel pre-call.
    #[test]
    fn scatter_and_merge_unavailable_propagates_and_fires_cancel() {
        let s0 = CancellableMockShard::new(OpResult::Got(vec![].into()));
        let s1 = CancellableMockShard::new(OpResult::Unavailable);
        // shard_2 is SLOW + comes AFTER the Unavailable shard — must
        // observe cancel pre-call (its `ran` stays 0).
        let s2 = CancellableMockShard::new(OpResult::Got(vec![1, 2, 3, 4].into()))
            .slow(Duration::from_millis(200));
        let ran2 = s2.ran.clone();
        let cancelled2 = s2.cancelled_pre_call.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        let out = scatter_and_merge(
            vec![s0, s1, s2],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel.clone(),
        );
        assert_eq!(out, OpResult::Unavailable, "V1 hard-fail propagates");
        assert!(
            cancel.load(Ordering::SeqCst),
            "cancel flag must be set on non-Got slot"
        );
        assert_eq!(
            ran2.load(Ordering::SeqCst),
            0,
            "shard_2 must NOT have completed its call"
        );
        assert!(
            cancelled2.load(Ordering::SeqCst) >= 1,
            "shard_2 observed cancel pre-call"
        );
    }

    /// **T6 KAT — K=0 empty shards returns empty Got.** Matches the
    /// `merge_scan_results(empty, ...)` contract; locks that the
    /// combined path keeps this edge.
    #[test]
    fn scatter_and_merge_empty_shards_returns_empty_got() {
        let shards: Vec<CancellableMockShard> = Vec::new();
        let cancel = Arc::new(AtomicBool::new(false));
        let out = scatter_and_merge(
            shards,
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 10 },
            cancel,
        );
        assert_eq!(out, OpResult::Got(Vec::<u8>::new().into()));
    }

    // ---------- T7 skew-defense / bounded-buffer KATs (SP155 §3.8) ----------
    //
    // Per SP155 §3.8: each per-shard reply channel is a bounded
    // `sync_channel(SHARD_BACKPRESSURE_BOUND)`. A fast shard's worker
    // thread blocks on `send()` once the bound is hit, naturally pacing
    // it to the merger's consumption rate. This is **bounded buffering,
    // not lost work** — every row eventually transits the channel.
    //
    // For V1 each shard sends exactly ONE `OpResult` per request (a
    // streaming `Op::SelectChunked` per spec §4.4 / T14 may later send
    // many chunks); the bound therefore protects against:
    //
    // 1. A future regression that switches the channel to unbounded
    //    `mpsc::channel()` — the bound-respected KAT catches it.
    // 2. The streaming-chunked path landing without a bound (would let
    //    a fast shard accumulate millions of in-flight rows ahead of a
    //    slow merger). Same bound applies.
    // 3. The cancel-path interaction: when the merger drops `rx` (e.g.
    //    LIMIT cancellation), a sender blocked on `send()` returns
    //    SendError cleanly and the worker exits — no deadlock.
    //
    // Per spec rationale: bound=0 (rendezvous) over-serializes;
    // bound=∞ OOMs under skew; bound=4 lets workers prefetch a chunk
    // or two ahead of the consumer.
    //
    // These KATs exercise the channel-bound contract directly (not via
    // scatter_and_merge) using mpsc::sync_channel + the constant we
    // promote to a public API.

    /// **T7 KAT — `SHARD_BACKPRESSURE_BOUND` is the documented bound (=4).**
    /// Lock the constant value so a regression that bumps it to 0 (over-
    /// serialize) or ∞ (OOM-prone) is caught at compile/test time. Spec
    /// §3.8 picks 4 explicitly — "workers can prefetch a chunk or two
    /// ahead of the consumer".
    #[test]
    fn t7_shard_backpressure_bound_is_four_per_spec() {
        assert_eq!(
            SHARD_BACKPRESSURE_BOUND, 4,
            "SP155 §3.8 specifies bound=4; bump = spec change"
        );
    }

    /// **T7 KAT — a fast sender blocks once the bound is full + the
    /// merger's reads unblock it.** Drive a single `sync_channel(
    /// SHARD_BACKPRESSURE_BOUND)` from a spawned thread that tries to
    /// push BOUND+3 items; the main thread reads them slowly. Assert
    /// the channel's pending-message count never exceeded the bound.
    ///
    /// We probe the bound by tracking the spawned thread's `sent`
    /// counter: after starting it but before reading, the counter
    /// reaches at most `BOUND + 1` (the `+1` is the in-flight send the
    /// sync_channel allows on top of the queued items, per std::sync::
    /// mpsc semantics — see Rust stdlib's sync_channel docs). Once the
    /// main thread starts reading, the counter climbs to the full N.
    #[test]
    fn t7_sync_channel_caps_at_bound_under_fast_sender() {
        let (tx, rx) = mpsc::sync_channel::<u32>(SHARD_BACKPRESSURE_BOUND);
        let sent = Arc::new(AtomicUsize::new(0));
        let n_items = SHARD_BACKPRESSURE_BOUND + 8; // try to push more than the bound
        let sent_for_worker = sent.clone();
        let h = thread::spawn(move || {
            for i in 0..n_items as u32 {
                tx.send(i).expect("send");
                sent_for_worker.fetch_add(1, Ordering::SeqCst);
            }
        });
        // Give the worker a chance to fill the bound.
        thread::sleep(Duration::from_millis(50));
        // At this point the worker should have queued BOUND items and
        // be parked on the next send. Per std::sync::mpsc::sync_channel
        // docs: with bound=N, up to N items can be in the channel; the
        // (N+1)th send blocks. The worker's counter increments AFTER
        // send returns, so a parked sender shows counter == N (not N+1).
        let observed = sent.load(Ordering::SeqCst);
        assert!(
            observed <= SHARD_BACKPRESSURE_BOUND,
            "sender must be paced by the bound: observed {observed} sent \
             before any read, bound = {SHARD_BACKPRESSURE_BOUND}"
        );
        // Now drain: every item must arrive (no lost work).
        let mut got: Vec<u32> = Vec::with_capacity(n_items);
        for _ in 0..n_items {
            got.push(rx.recv().expect("recv"));
        }
        h.join().expect("join");
        assert_eq!(got.len(), n_items, "no items lost");
        assert_eq!(
            sent.load(Ordering::SeqCst),
            n_items,
            "every send eventually completed"
        );
        // And drained in order (sync_channel is FIFO).
        for (i, v) in got.iter().enumerate() {
            assert_eq!(*v as usize, i, "FIFO order");
        }
    }

    /// **T7 KAT — bound=1 still produces a correct merged output.**
    /// Edge case: the smallest non-rendezvous bound. The K-shard fan-out
    /// degenerates to "every send blocks until the merger reads", but
    /// the final merged bytes are identical to bound=4. Locks that the
    /// bound is purely a backpressure knob, not a correctness knob.
    #[test]
    fn t7_bound_one_still_produces_correct_merged_output() {
        // We can't reach into scatter_and_merge to override the bound,
        // but we CAN simulate the bound=1 contract by using a smaller
        // sync_channel(1) directly + asserting that draining N items
        // in shard-id order produces the same bytes as bound=4.
        let make_rows = |base: u8, n: usize| -> Vec<u8> {
            let mut v = Vec::new();
            for i in 0..n {
                let rec = vec![base.wrapping_add(i as u8); 8];
                v.extend_from_slice(&(rec.len() as u32).to_le_bytes());
                v.extend_from_slice(&rec);
            }
            v
        };
        let s0_payload = make_rows(0, 3);
        let s1_payload = make_rows(100, 3);
        // Bound=1 simulation: drain in shard-id order.
        let (tx0, rx0) = mpsc::sync_channel::<OpResult>(1);
        let (tx1, rx1) = mpsc::sync_channel::<OpResult>(1);
        let s0_for_thread = s0_payload.clone();
        let s1_for_thread = s1_payload.clone();
        let h0 = thread::spawn(move || tx0.send(OpResult::Got(s0_for_thread.into())));
        let h1 = thread::spawn(move || tx1.send(OpResult::Got(s1_for_thread.into())));
        let r0 = rx0.recv().unwrap();
        let r1 = rx1.recv().unwrap();
        h0.join().unwrap().unwrap();
        h1.join().unwrap().unwrap();
        let merged = merge_scan_results(
            vec![r0, r1],
            &ScatterKind::Unordered { limit: 0 },
        );
        // Same expected bytes as a bound=4 (or any bound) run: shard-id-
        // ordered concat of the two payloads.
        let mut expected = s0_payload.clone();
        expected.extend_from_slice(&s1_payload);
        assert_eq!(merged, OpResult::Got(expected.into()));
    }

    /// **T7 KAT — sender does NOT deadlock when the merger drops the
    /// receiver mid-stream (LIMIT-cancellation interaction).** The
    /// merger's cancel-path drops the per-shard `rx` after fulfilling
    /// LIMIT; any worker still blocked on a `send()` (because the
    /// bound is full) must observe `SendError` and exit cleanly. Same
    /// for a worker about to send its first frame after the rx is
    /// already dropped.
    #[test]
    fn t7_sender_observes_send_error_when_receiver_dropped_no_deadlock() {
        let (tx, rx) = mpsc::sync_channel::<u32>(SHARD_BACKPRESSURE_BOUND);
        // Fill the bound so the next send blocks.
        for i in 0..SHARD_BACKPRESSURE_BOUND as u32 {
            tx.send(i).expect("priming send");
        }
        // Spawn a worker that tries to send one more — it will block
        // until the rx is dropped (or accepts a recv).
        let send_done = Arc::new(AtomicBool::new(false));
        let send_err = Arc::new(AtomicBool::new(false));
        let send_done_for_worker = send_done.clone();
        let send_err_for_worker = send_err.clone();
        let h = thread::spawn(move || {
            match tx.send(999) {
                Ok(_) => {}
                Err(_e) => {
                    send_err_for_worker.store(true, Ordering::SeqCst);
                }
            }
            send_done_for_worker.store(true, Ordering::SeqCst);
        });
        // Give the worker time to block.
        thread::sleep(Duration::from_millis(50));
        assert!(
            !send_done.load(Ordering::SeqCst),
            "worker must be blocked on send (bound full, no reader)"
        );
        // Drop the receiver — the blocked send returns SendError.
        drop(rx);
        // Worker must exit promptly (no deadlock).
        let t0 = Instant::now();
        h.join().expect("worker joined");
        let elapsed = t0.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "worker must exit promptly after rx drop (elapsed {elapsed:?})"
        );
        assert!(
            send_done.load(Ordering::SeqCst),
            "worker reached the post-send statement"
        );
        assert!(
            send_err.load(Ordering::SeqCst),
            "send must surface SendError when rx is dropped"
        );
    }

    /// **T7 KAT — slow merger doesn't OOM under fast shards.** Drive
    /// 8 mock shards each producing 1000 small "rows" worth of payload
    /// through the real `scatter_and_merge` Unordered path; the merger
    /// drains serially. The bound = `SHARD_BACKPRESSURE_BOUND` caps
    /// per-shard in-flight memory at `bound × OpResult-size`. We assert
    /// the merged output has the expected total row count + the test
    /// completes in well-under a second (bounded, not pathological).
    #[test]
    fn t7_slow_merger_8_fast_shards_completes_with_bounded_memory() {
        let rec = |v: u8| -> Vec<u8> { vec![v; 8] };
        let mk_payload = |n: usize, base: u8| -> OpResult {
            let rows: Vec<Vec<u8>> = (0..n as u8)
                .map(|i| rec(base.wrapping_add(i)))
                .collect();
            let refs: Vec<&[u8]> = rows.iter().map(|v| v.as_slice()).collect();
            OpResult::Got(rows_to_payload(&refs).into())
        };
        // 8 shards × 100 rows each = 800 rows total. (1000 per spec
        // hint, but mocks store the payload eagerly so we keep it
        // smaller for the unoptimized test build — the bound logic is
        // identical at 100 or 1000.)
        let mut shards: Vec<CancellableMockShard> = Vec::with_capacity(8);
        for i in 0..8u8 {
            shards.push(CancellableMockShard::new(mk_payload(
                100,
                i.wrapping_mul(100),
            )));
        }
        let cancel = Arc::new(AtomicBool::new(false));
        let t0 = Instant::now();
        let out = scatter_and_merge(
            shards,
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel,
        );
        let elapsed = t0.elapsed();
        let bytes = match &out {
            OpResult::Got(b) => b,
            other => panic!("expected Got, got {other:?}"),
        };
        let mut count = 0usize;
        let mut p = 0usize;
        while p < bytes.len() {
            let len =
                u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
            p += 4 + len;
            count += 1;
        }
        assert_eq!(count, 800, "8 shards × 100 rows = 800 rows");
        // Bound: should complete WELL under a second on any modern CI.
        assert!(
            elapsed < Duration::from_secs(2),
            "8-shard fanout with bounded buffers should be <2s, was {elapsed:?}"
        );
    }

    // ---------- T8 pentest sweep (SP155 §7.5 — 10 adversarial cases) ----------
    //
    // Each pentest:
    //   1. Constructs an adversarial `ShardCaller` (returns oversized,
    //      malformed, timing-out, error-on-call, etc.).
    //   2. Drives `scatter_and_merge` (or `scatter_scan_fanout` where
    //      appropriate).
    //   3. Asserts the typed response — `Got` / `Unavailable` /
    //      `SchemaError` per the spec's expected column.
    //   4. Where relevant, asserts post-conditions:
    //        - merger doesn't OOM (test still completes)
    //        - no panic (test reaches its end)
    //        - merger thread doesn't leak (the next scatter call works
    //          + bounded wall-clock)
    //
    // This is the SP155 §7.5 acceptance criterion #3 ("all 10 pentests
    // pass"). Per spec rationale: each pentest stays under ~1s so the
    // sweep doesn't bloat CI.

    /// `PentestShard` — a flexible adversarial mock used across the
    /// 10 pentests. The variants cover: sleep > timeout, oversized
    /// payload, malformed framing, transport error (Err(string)),
    /// instant-error, and a normal Got reply.
    enum PentestBehavior {
        /// Sleep for `d` then return `canned`. Used to drive timeout-
        /// expiry pentests.
        SleepThenGot(Duration, Vec<u8>),
        /// Return an oversized `Got(payload)` (still well-formed; the
        /// merger's `iter_rows` walks the whole frame — assert no OOM).
        OversizedGot(Vec<u8>),
        /// Return `Got(bytes)` where bytes are a malformed `[u32
        /// rowlen][record]*` frame (claims a row larger than payload).
        MalformedGot(Vec<u8>),
        /// Return `Err(transport_error_string)` — simulates a partial-
        /// frame-then-close OR a TCP RST mid-stream (the `ShardCaller`
        /// abstraction surfaces both as `Err(String)` per its contract;
        /// the scatter layer translates to `Unavailable`).
        TransportErr(String),
        /// Plain `Got(canned)` after no sleep.
        Got(Vec<u8>),
    }

    struct PentestShard {
        behavior: PentestBehavior,
        ran: Arc<AtomicUsize>,
        cancelled_pre_call: Arc<AtomicUsize>,
    }

    impl PentestShard {
        fn new(behavior: PentestBehavior) -> Self {
            PentestShard {
                behavior,
                ran: Arc::new(AtomicUsize::new(0)),
                cancelled_pre_call: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl ShardCaller for PentestShard {
        fn call(&mut self, _op: &Op) -> Result<OpResult, String> {
            self.ran.fetch_add(1, Ordering::SeqCst);
            match &self.behavior {
                PentestBehavior::SleepThenGot(d, payload) => {
                    thread::sleep(*d);
                    Ok(OpResult::Got(payload.clone().into()))
                }
                PentestBehavior::OversizedGot(p) => Ok(OpResult::Got(p.clone().into())),
                PentestBehavior::MalformedGot(p) => Ok(OpResult::Got(p.clone().into())),
                PentestBehavior::TransportErr(e) => Err(e.clone()),
                PentestBehavior::Got(p) => Ok(OpResult::Got(p.clone().into())),
            }
        }
        fn call_with_cancel(
            &mut self,
            op: &Op,
            cancel: &Arc<AtomicBool>,
        ) -> Result<OpResult, String> {
            if cancel.load(Ordering::SeqCst) {
                self.cancelled_pre_call.fetch_add(1, Ordering::SeqCst);
                return Ok(OpResult::Unavailable);
            }
            // For SleepThenGot we poll cancel in 5ms slices (lets
            // pentest 6 / 7 observe cancel mid-sleep without unbound
            // wait).
            if let PentestBehavior::SleepThenGot(d, payload) = &self.behavior {
                let step = Duration::from_millis(5);
                let mut remaining = *d;
                while remaining > Duration::from_millis(0) {
                    if cancel.load(Ordering::SeqCst) {
                        self.cancelled_pre_call
                            .fetch_add(1, Ordering::SeqCst);
                        return Ok(OpResult::Unavailable);
                    }
                    let s = if remaining > step { step } else { remaining };
                    thread::sleep(s);
                    remaining = remaining.saturating_sub(s);
                }
                self.ran.fetch_add(1, Ordering::SeqCst);
                return Ok(OpResult::Got(payload.clone().into()));
            }
            self.call(op)
        }
    }

    // -- Pentest 1: shard_times_out --

    /// **PT1 — `shard_times_out`**: one shard sleeps PAST the per-
    /// shard timeout; merger returns the partial/short slot as
    /// `Unavailable` for that shard (per `scatter_scan_fanout`'s T1
    /// contract). The other shards' replies are unaffected.
    ///
    /// We use `scatter_scan_fanout` (not `scatter_and_merge`) because
    /// the fanout is where the per-shard deadline lives; merging is a
    /// separate concern. `scatter_and_merge` would V1-hard-fail on
    /// the first `Unavailable`, which is also documented behavior.
    #[test]
    fn pentest_1_shard_times_out_yields_unavailable_slot_for_that_shard() {
        let s0 = PentestShard::new(PentestBehavior::Got(b"fast-0".to_vec()));
        let s1 = PentestShard::new(PentestBehavior::SleepThenGot(
            Duration::from_millis(500),
            b"too-slow".to_vec(),
        ));
        let s2 = PentestShard::new(PentestBehavior::Got(b"fast-2".to_vec()));
        let out = scatter_scan_fanout(
            vec![s0, s1, s2],
            &dummy_select(),
            Duration::from_millis(80),
        );
        assert_eq!(out.len(), 3, "every shard has a slot");
        assert_eq!(out[0], OpResult::Got(b"fast-0".to_vec().into()));
        assert_eq!(
            out[1],
            OpResult::Unavailable,
            "the timed-out shard contributes Unavailable"
        );
        // shard_2 may be Got OR Unavailable depending on whether its
        // deadline window already elapsed waiting for shard_1; both
        // outcomes are valid per the existing T1 KAT contract.
        match &out[2] {
            OpResult::Got(b) => assert_eq!(&b[..], b"fast-2"),
            OpResult::Unavailable => {} // acceptable
            other => panic!("shard 2 unexpected slot: {other:?}"),
        }
    }

    // -- Pentest 2: shard_returns_oversized_payload --

    /// **PT2 — `shard_returns_oversized_payload`**: one shard returns
    /// a (well-formed) ~1 MiB payload. Merger walks the rows, doesn't
    /// OOM, completes promptly. The spec's "1 GiB" attack is mirrored
    /// at 1 MiB here for CI speed — same logic, smaller magnitude.
    /// The merger has NO router-side size cap today (V1 documented
    /// honest gap; spec §3.8 mentions a 64 MiB cap as a follow-up),
    /// so this pentest documents the V1 reality: it accepts the
    /// payload AS LONG AS it's well-formed framing. The defense
    /// against the actual 1 GiB attack is bounded by per-`Op` wire
    /// size cap in `kessel-proto` (a separate layer); the scatter
    /// merger inherits that cap implicitly.
    #[test]
    fn pentest_2_shard_returns_oversized_payload_no_oom_completes_promptly() {
        // 1 MiB payload, 1024 rows × 1024 bytes each.
        let row_count = 1024usize;
        let row_size = 1024usize;
        let one_row = vec![0xABu8; row_size];
        let mut big_payload = Vec::with_capacity(row_count * (4 + row_size));
        for _ in 0..row_count {
            big_payload.extend_from_slice(&(row_size as u32).to_le_bytes());
            big_payload.extend_from_slice(&one_row);
        }
        let s0 = PentestShard::new(PentestBehavior::OversizedGot(big_payload));
        let cancel = Arc::new(AtomicBool::new(false));
        let t0 = Instant::now();
        let out = scatter_and_merge(
            vec![s0],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel,
        );
        let elapsed = t0.elapsed();
        // Bytes are well-formed → merger emits them as-is.
        let bytes = match &out {
            OpResult::Got(b) => b,
            other => panic!("expected Got, got {other:?}"),
        };
        // Walk the merged payload — count rows; row count should match.
        let mut count = 0usize;
        let mut p = 0usize;
        while p < bytes.len() {
            let len =
                u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
            p += 4 + len;
            count += 1;
        }
        assert_eq!(count, row_count, "every row passed through");
        // No OOM + completes within a generous CI bound.
        assert!(
            elapsed < Duration::from_secs(2),
            "1 MiB oversized payload must merge in <2s (no OOM, no \
             pathological behavior), was {elapsed:?}"
        );
    }

    // -- Pentest 3: shard_returns_malformed_bytes --

    /// **PT3 — `shard_returns_malformed_bytes`**: one shard returns
    /// a `Got(bytes)` whose `[u32 rowlen][record]*` framing is broken
    /// (claims a row larger than the remaining payload). Merger must
    /// surface `SchemaError`, NEVER panic.
    #[test]
    fn pentest_3_shard_returns_malformed_bytes_yields_schema_error_no_panic() {
        // Claim a 4 GiB row; provide only 4 bytes of body. iter_rows
        // detects body_end overflow → "row body exceeds payload" →
        // merger surfaces SchemaError.
        let bad = {
            let mut v = (u32::MAX).to_le_bytes().to_vec();
            v.extend_from_slice(&[1, 2, 3, 4]);
            v
        };
        let s0 = PentestShard::new(PentestBehavior::MalformedGot(bad));
        let cancel = Arc::new(AtomicBool::new(false));
        let out = scatter_and_merge(
            vec![s0],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel,
        );
        match out {
            OpResult::SchemaError(msg) => {
                assert!(
                    msg.contains("scatter merge") || msg.contains("framing")
                        || msg.contains("payload"),
                    "SchemaError message must mention merger/framing: {msg}"
                );
            }
            other => panic!("expected SchemaError, got {other:?}"),
        }
    }

    // -- Pentest 4: shard_returns_partial_then_closes --

    /// **PT4 — `shard_returns_partial_then_closes`**: simulates a
    /// half-frame + EOF — the transport layer (`ClusterClient::call`)
    /// would surface this as `Err(String)` (a read error). Scatter
    /// translates `Err(_)` → `OpResult::Unavailable` per its T1
    /// contract. Merger then V1-hard-fails the whole result to
    /// `Unavailable` per SP155 §6.
    #[test]
    fn pentest_4_shard_returns_partial_then_closes_surfaces_unavailable() {
        let s0 = PentestShard::new(PentestBehavior::Got(
            rows_to_payload(&[b"good"]),
        ));
        let s1 = PentestShard::new(PentestBehavior::TransportErr(
            "transport: read 4 bytes, peer closed".into(),
        ));
        let cancel = Arc::new(AtomicBool::new(false));
        let out = scatter_and_merge(
            vec![s0, s1],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel,
        );
        assert_eq!(
            out,
            OpResult::Unavailable,
            "transport-err shard surfaces as Unavailable per V1 hard-fail"
        );
    }

    // -- Pentest 5: shard_dies_mid_scan --

    /// **PT5 — `shard_dies_mid_scan`**: same shape as PT4 but more
    /// violent — TCP RST mid-stream surfaces as `Err(...)` from
    /// `ShardCaller::call`. Scatter translates → `Unavailable`, merger
    /// V1-hard-fails. Additionally: we lock that ALL OTHER worker
    /// threads JOIN cleanly (no leak) — `scatter_and_merge` returns
    /// promptly + the next call works.
    #[test]
    fn pentest_5_shard_dies_mid_scan_unavailable_no_thread_leak() {
        let s0 = PentestShard::new(PentestBehavior::Got(
            rows_to_payload(&[b"alive"]),
        ));
        let s1 = PentestShard::new(PentestBehavior::TransportErr(
            "transport: connection reset by peer".into(),
        ));
        let s2 = PentestShard::new(PentestBehavior::Got(
            rows_to_payload(&[b"also-alive"]),
        ));
        let cancel = Arc::new(AtomicBool::new(false));
        let t0 = Instant::now();
        let out = scatter_and_merge(
            vec![s0, s1, s2],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel,
        );
        let elapsed = t0.elapsed();
        assert_eq!(out, OpResult::Unavailable);
        // Bounded wall-clock + a follow-up call works (no leaked
        // threads holding state).
        assert!(
            elapsed < Duration::from_millis(500),
            "mid-scan death must surface promptly, was {elapsed:?}"
        );
        // Follow-up call works — listener/scatter machinery still alive.
        let s_follow = PentestShard::new(PentestBehavior::Got(
            rows_to_payload(&[b"follow-up"]),
        ));
        let cancel2 = Arc::new(AtomicBool::new(false));
        let out2 = scatter_and_merge(
            vec![s_follow],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel2,
        );
        assert_eq!(
            out2,
            OpResult::Got(rows_to_payload(&[b"follow-up"]).into()),
            "scatter still works after a mid-scan death"
        );
    }

    // -- Pentest 6: router_drops_receiver_under_limit --

    /// **PT6 — `router_drops_receiver_under_limit`**: LIMIT-cancellation
    /// — fast shard fulfills LIMIT, slow shards see cancel pre-call,
    /// senders observe SendError if they were already past their pre-
    /// call check. No panic, no leak. (This is the inside-out view of
    /// the T6 cancel KATs, framed as a pentest.)
    #[test]
    fn pentest_6_router_drops_receiver_under_limit_no_panic_no_leak() {
        let s0 = PentestShard::new(PentestBehavior::Got(rows_to_payload(&[
            b"r0", b"r1", b"r2", b"r3", b"r4", b"r5",
        ])));
        let s1 = PentestShard::new(PentestBehavior::SleepThenGot(
            Duration::from_millis(200),
            rows_to_payload(&[b"x", b"y", b"z"]),
        ));
        let s2 = PentestShard::new(PentestBehavior::SleepThenGot(
            Duration::from_millis(200),
            rows_to_payload(&[b"p", b"q", b"r"]),
        ));
        let cancelled1 = s1.cancelled_pre_call.clone();
        let cancelled2 = s2.cancelled_pre_call.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        let t0 = Instant::now();
        let out = scatter_and_merge(
            vec![s0, s1, s2],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 3 },
            cancel.clone(),
        );
        let elapsed = t0.elapsed();
        // LIMIT 3 satisfied from shard_0 alone.
        let bytes = match &out {
            OpResult::Got(b) => b,
            other => panic!("expected Got, got {other:?}"),
        };
        let mut count = 0usize;
        let mut p = 0usize;
        while p < bytes.len() {
            let len =
                u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
            p += 4 + len;
            count += 1;
        }
        assert_eq!(count, 3);
        // Late shards observed cancel + bailed cleanly.
        assert!(cancelled1.load(Ordering::SeqCst) >= 1);
        assert!(cancelled2.load(Ordering::SeqCst) >= 1);
        // No leak / no deadlock — function returns inside a tight
        // window despite 200ms sleeps.
        assert!(
            elapsed < Duration::from_millis(180),
            "router-drop-under-limit must return promptly, was {elapsed:?}"
        );
        assert!(cancel.load(Ordering::SeqCst));
    }

    // -- Pentest 7: cancel_atomic_visibility --

    /// **PT7 — `cancel_atomic_visibility`**: set cancel from main
    /// thread (via the pre-fired flag); verify EVERY worker observes
    /// it. We pre-cancel before calling `scatter_and_merge` — the
    /// function short-circuits BEFORE spawning workers (per the SP155
    /// §3.7 "stop scanning at the strongest possible point"
    /// contract). Result: empty Got; zero workers ran.
    ///
    /// For a stress test of cancel propagation MID-fanout, see PT6.
    #[test]
    fn pentest_7_cancel_atomic_visibility_every_worker_observes() {
        // Stress: 8 shards, 100 iterations, each iteration creates a
        // fresh cancel flag set BEFORE the call. Every worker must see
        // the flag at its pre-call check (or the scatter must short-
        // circuit before spawning).
        for iter in 0..100u32 {
            let shards: Vec<PentestShard> = (0..8u8)
                .map(|i| {
                    PentestShard::new(PentestBehavior::Got(rows_to_payload(
                        &[&[i; 4]],
                    )))
                })
                .collect();
            let rans: Vec<Arc<AtomicUsize>> =
                shards.iter().map(|s| s.ran.clone()).collect();
            let cancel = Arc::new(AtomicBool::new(true)); // pre-fired
            let out = scatter_and_merge(
                shards,
                &dummy_select(),
                DEFAULT_PER_SHARD_TIMEOUT,
                &ScatterKind::Unordered { limit: 0 },
                cancel,
            );
            assert_eq!(
                out,
                OpResult::Got(Vec::<u8>::new().into()),
                "iter {iter}: pre-cancelled scatter must return empty Got"
            );
            for (i, ran) in rans.iter().enumerate() {
                assert_eq!(
                    ran.load(Ordering::SeqCst),
                    0,
                    "iter {iter} shard {i}: worker must NOT have run \
                     (cancel observed at the top of scatter_and_merge)"
                );
            }
        }
    }

    // -- Pentest 8: zero_shards --

    /// **PT8 — `zero_shards`**: fan-out to empty shard list returns
    /// empty result, no thread spawned. (The spec column says
    /// "caught at Router::new"; for the scatter layer the
    /// `shards.is_empty()` early-return is the equivalent shield.)
    #[test]
    fn pentest_8_zero_shards_returns_empty_got_no_thread_spawned() {
        let shards: Vec<PentestShard> = Vec::new();
        let cancel = Arc::new(AtomicBool::new(false));
        // Bound the wall-clock: zero shards must short-circuit at the
        // top of the function (no thread creation / no channel
        // operations / no per-shard deadline window).
        let t0 = Instant::now();
        let out = scatter_and_merge(
            shards,
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 100 },
            cancel,
        );
        let elapsed = t0.elapsed();
        assert_eq!(out, OpResult::Got(Vec::<u8>::new().into()));
        assert!(
            elapsed < Duration::from_millis(50),
            "K=0 must short-circuit instantly, was {elapsed:?}"
        );
        // Also verify scatter_scan_fanout has the same short-circuit.
        let shards2: Vec<PentestShard> = Vec::new();
        let out2 = scatter_scan_fanout(
            shards2,
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
        );
        assert_eq!(out2, Vec::<OpResult>::new());
    }

    // -- Pentest 9: one_shard --

    /// **PT9 — `one_shard`**: K=1 case is byte-identical to the
    /// non-scatter direct call. (Regression-lock for the T2
    /// `merge_unordered_k1_byte_identical_to_single_shard` invariant,
    /// re-stated through the full scatter_and_merge pipeline.)
    #[test]
    fn pentest_9_one_shard_byte_identical_to_non_scatter_path() {
        let payload = rows_to_payload(&[
            b"only-shard-row-0",
            b"only-shard-row-1",
            b"only-shard-row-2",
        ]);
        let s0 = PentestShard::new(PentestBehavior::Got(payload.clone()));
        let cancel = Arc::new(AtomicBool::new(false));
        let out = scatter_and_merge(
            vec![s0],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel,
        );
        assert_eq!(
            out,
            OpResult::Got(payload.into()),
            "K=1 scatter must be byte-identical to a direct shard call"
        );
    }

    // -- Pentest 10: determinism_replay --

    /// **PT10 — `determinism_replay`**: same input × 100 runs ⇒ same
    /// merged bytes every time. Locks: no HashMap iteration order
    /// leak, no time-based decisions, no per-run shard_id reassignment.
    #[test]
    fn pentest_10_determinism_replay_same_input_100_runs_byte_identical() {
        // Build deterministic per-shard payloads (Sorted to exercise
        // the heap merge — the most likely vector for non-determinism).
        let rec = |v: u64| -> Vec<u8> { v.to_le_bytes().to_vec() };
        let s0_payload = rows_to_payload(&[&rec(1), &rec(4), &rec(9)]);
        let s1_payload = rows_to_payload(&[&rec(2), &rec(3), &rec(7)]);
        let s2_payload = rows_to_payload(&[&rec(5), &rec(6), &rec(8)]);
        let mut first: Option<OpResult> = None;
        for run in 0..100u32 {
            let s0 = PentestShard::new(PentestBehavior::Got(s0_payload.clone()));
            let s1 = PentestShard::new(PentestBehavior::Got(s1_payload.clone()));
            let s2 = PentestShard::new(PentestBehavior::Got(s2_payload.clone()));
            let cancel = Arc::new(AtomicBool::new(false));
            let out = scatter_and_merge(
                vec![s0, s1, s2],
                &dummy_select(),
                DEFAULT_PER_SHARD_TIMEOUT,
                &ScatterKind::Sorted {
                    sort_kind: FieldKind::U64,
                    sort_offset: 0,
                    sort_width: 8,
                    desc: false,
                    offset: 0,
                    limit: 0,
                },
                cancel,
            );
            match &first {
                None => first = Some(out),
                Some(baseline) => {
                    assert_eq!(
                        &out, baseline,
                        "run {run}: determinism violated — merged bytes \
                         differ from run 0"
                    );
                }
            }
        }
        // Cross-check: the deterministic merged bytes are the ascending
        // sequence [1, 2, 3, 4, 5, 6, 7, 8, 9].
        let expected = rows_to_payload(&[
            &rec(1), &rec(2), &rec(3), &rec(4), &rec(5),
            &rec(6), &rec(7), &rec(8), &rec(9),
        ]);
        assert_eq!(first.unwrap(), OpResult::Got(expected.into()));
    }

    // ---------- T9 partial-result opt-in KATs (SP155 §3.6 / §6 / OQ2) ----------
    //
    // T1-T8 ship V1 hard-fail (any non-Got slot poisons the merged
    // result with that slot's typed error). T9 adds an opt-in
    // `ScatterContext::partial_on_timeout` flag: when true, per-shard
    // non-Got slots are OMITTED from the merge and the caller gets a
    // `Vec<u32>` failed-shards list as the second tuple element from
    // `scatter_and_merge_ctx`. V1 default stays hard-fail (a regression-
    // lock here catches an accidental flip).
    //
    // The KATs below cover, in order:
    //   1. V1 default (hard-fail) still fires (regression-lock — flipping
    //      the default would silently degrade callers).
    //   2. partial=true + 1/3 shards timeout: returns rows from the 2
    //      surviving shards + failed_shards = [the timed-out one].
    //   3. partial=true + 0 shards fail: result == V1 default
    //      (failed_shards empty).
    //   4. partial=true + ALL shards fail: empty Got + failed_shards =
    //      0..K (caller MUST check len == K).
    //   5. partial=true + LIMIT: cancellation still fires on LIMIT-hit
    //      (rows complete past LIMIT regardless of partial mode).
    //   6. partial=true preserves K-invariance MODULO removal of the
    //      failed-shard subset (given the same failure set, byte-identical
    //      across K).
    //   7. partial=true + Sorted: failed shards' slots are omitted; the
    //      remaining shards' rows merge per the heap.
    //   8. partial=true + malformed-Got from one shard: STILL surfaces
    //      SchemaError (framing bugs are not "availability events" —
    //      partial mode does not silently drop garbage bytes; T9 doc).

    /// **T9.1 — V1 default is hard-fail (regression-lock).** Default
    /// `scatter_and_merge` (no ScatterContext) and
    /// `scatter_and_merge_ctx(..., ScatterContext::hard_fail())` BOTH
    /// surface the first non-Got slot as the merged result. A flipped
    /// default here would be a silent correctness degradation.
    #[test]
    fn t9_default_is_hard_fail_v1_regression_lock() {
        let s0 = PentestShard::new(PentestBehavior::Got(rows_to_payload(&[b"a"])));
        let s1 = PentestShard::new(PentestBehavior::TransportErr(
            "transport: shard down".into(),
        ));
        let s2 = PentestShard::new(PentestBehavior::Got(rows_to_payload(&[b"c"])));
        let cancel = Arc::new(AtomicBool::new(false));
        // Via the back-compat wrapper.
        let out = scatter_and_merge(
            vec![s0, s1, s2],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel,
        );
        assert_eq!(
            out,
            OpResult::Unavailable,
            "V1 default MUST still hard-fail (regression-lock)"
        );

        // Also via the ctx entry point with explicit hard_fail() —
        // identical behaviour.
        let s0b = PentestShard::new(PentestBehavior::Got(rows_to_payload(&[b"a"])));
        let s1b = PentestShard::new(PentestBehavior::TransportErr(
            "transport: shard down".into(),
        ));
        let s2b = PentestShard::new(PentestBehavior::Got(rows_to_payload(&[b"c"])));
        let cancel2 = Arc::new(AtomicBool::new(false));
        let (out2, failed) = scatter_and_merge_ctx(
            vec![s0b, s1b, s2b],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel2,
            ScatterContext::hard_fail(),
        );
        assert_eq!(out2, OpResult::Unavailable);
        assert!(
            failed.is_empty(),
            "hard-fail mode never populates failed_shards: {failed:?}"
        );
    }

    /// **T9.2 — partial=true + 1/3 shards fails: 2 shards' rows
    /// merged + failed_shards = [the failed one].**
    #[test]
    fn t9_partial_one_shard_fails_returns_others_plus_failed_marker() {
        let s0 = PentestShard::new(PentestBehavior::Got(
            rows_to_payload(&[b"row-a", b"row-b"]),
        ));
        let s1 = PentestShard::new(PentestBehavior::TransportErr(
            "transport: timeout".into(),
        ));
        let s2 = PentestShard::new(PentestBehavior::Got(
            rows_to_payload(&[b"row-c"]),
        ));
        let cancel = Arc::new(AtomicBool::new(false));
        let (out, failed) = scatter_and_merge_ctx(
            vec![s0, s1, s2],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel,
            ScatterContext::partial(),
        );
        // The merged result is shard_0 ++ shard_2 (shard_1 omitted).
        let expected = rows_to_payload(&[b"row-a", b"row-b", b"row-c"]);
        assert_eq!(
            out,
            OpResult::Got(expected.into()),
            "partial mode: surviving shards' rows merge in shard-id order"
        );
        assert_eq!(
            failed,
            vec![1u32],
            "failed_shards lists the timed-out shard"
        );
    }

    /// **T9.3 — partial=true + 0 shards fail: identical to V1 default
    /// (Got + empty failed_shards).**
    #[test]
    fn t9_partial_no_shards_fail_equals_v1_default() {
        let mk = || -> Vec<PentestShard> {
            vec![
                PentestShard::new(PentestBehavior::Got(rows_to_payload(&[b"a"]))),
                PentestShard::new(PentestBehavior::Got(rows_to_payload(&[b"b"]))),
                PentestShard::new(PentestBehavior::Got(rows_to_payload(&[b"c"]))),
            ]
        };
        // Hard-fail result.
        let cancel_hf = Arc::new(AtomicBool::new(false));
        let (hf_out, hf_failed) = scatter_and_merge_ctx(
            mk(),
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel_hf,
            ScatterContext::hard_fail(),
        );
        // Partial result with no failures.
        let cancel_p = Arc::new(AtomicBool::new(false));
        let (p_out, p_failed) = scatter_and_merge_ctx(
            mk(),
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel_p,
            ScatterContext::partial(),
        );
        assert_eq!(
            hf_out, p_out,
            "with 0 failures, partial mode matches hard-fail byte-for-byte"
        );
        assert!(hf_failed.is_empty());
        assert!(p_failed.is_empty());
    }

    /// **T9.4 — partial=true + ALL shards fail: empty Got +
    /// failed_shards = full 0..K range.** The caller MUST check
    /// `failed_shards.len() == K` to distinguish "all failed" from
    /// "all returned 0 rows".
    #[test]
    fn t9_partial_all_shards_fail_returns_empty_plus_full_failed_list() {
        let s0 = PentestShard::new(PentestBehavior::TransportErr("e0".into()));
        let s1 = PentestShard::new(PentestBehavior::TransportErr("e1".into()));
        let s2 = PentestShard::new(PentestBehavior::TransportErr("e2".into()));
        let cancel = Arc::new(AtomicBool::new(false));
        let (out, failed) = scatter_and_merge_ctx(
            vec![s0, s1, s2],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel,
            ScatterContext::partial(),
        );
        assert_eq!(
            out,
            OpResult::Got(Vec::<u8>::new().into()),
            "all-fail in partial mode returns empty Got"
        );
        assert_eq!(failed, vec![0u32, 1, 2], "failed_shards == 0..K");
    }

    /// **T9.5 — partial=true + LIMIT-hit still cancels late shards.**
    /// shard_0 returns enough rows to satisfy LIMIT; the slow shards
    /// must still see the cancel flag pre-call. partial mode does NOT
    /// suppress the LIMIT-cancellation path.
    #[test]
    fn t9_partial_mode_limit_still_cancels_pending_shards() {
        let rec = |v: u8| -> Vec<u8> { vec![v; 8] };
        let mk_payload = |n: usize, base: u8| -> OpResult {
            let rows: Vec<Vec<u8>> = (0..n as u8)
                .map(|i| rec(base.wrapping_add(i)))
                .collect();
            let refs: Vec<&[u8]> = rows.iter().map(|v| v.as_slice()).collect();
            OpResult::Got(rows_to_payload(&refs).into())
        };
        // shard_0: fast, 100 rows.
        let s0 = CancellableMockShard::new(mk_payload(100, 0));
        // shard_1, shard_2: SLOW, 100 rows each.
        let s1 = CancellableMockShard::new(mk_payload(100, 100))
            .slow(Duration::from_millis(200));
        let s2 = CancellableMockShard::new(mk_payload(100, 200))
            .slow(Duration::from_millis(200));
        let ran1 = s1.ran.clone();
        let ran2 = s2.ran.clone();
        let cancelled1 = s1.cancelled_pre_call.clone();
        let cancelled2 = s2.cancelled_pre_call.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        let t0 = Instant::now();
        let (out, failed) = scatter_and_merge_ctx(
            vec![s0, s1, s2],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 5 },
            cancel.clone(),
            ScatterContext::partial(),
        );
        let elapsed = t0.elapsed();
        // Exactly 5 rows (LIMIT 5).
        let bytes = match &out {
            OpResult::Got(b) => b,
            other => panic!("expected Got, got {other:?}"),
        };
        let mut count = 0usize;
        let mut p = 0usize;
        while p < bytes.len() {
            let len =
                u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
            p += 4 + len;
            count += 1;
        }
        assert_eq!(count, 5, "LIMIT 5 still caps at 5 rows in partial mode");
        // Late shards cancelled pre-call (didn't even run).
        assert_eq!(ran1.load(Ordering::SeqCst), 0);
        assert_eq!(ran2.load(Ordering::SeqCst), 0);
        assert!(cancelled1.load(Ordering::SeqCst) >= 1);
        assert!(cancelled2.load(Ordering::SeqCst) >= 1);
        // Fast wall-clock (cancel propagated).
        assert!(
            elapsed < Duration::from_millis(180),
            "LIMIT-cancellation still fast in partial mode, was {elapsed:?}"
        );
        // failed_shards: under LIMIT-hit short-circuit, the late
        // shards are NEVER READ FROM by the merger (the LIMIT-hit
        // returns mid-drain). They're "unread", NOT "failed" — a
        // deterministic distinction by shard-id ordering. The
        // failed_shards list therefore stays empty for the
        // LIMIT-hit-before-late-shard case. This is honest: the
        // caller knows the merger didn't even ASK the late shards
        // (their cancel-pre-call is a router-side optimization, not
        // a "shard failure"). Documented honest gap: a future
        // T-slice could surface "unread shards" separately if a
        // workload needs it.
        assert!(
            failed.is_empty(),
            "LIMIT-hit short-circuit doesn't classify late shards as \
             failed (they were unread): {failed:?}"
        );
        assert!(
            cancel.load(Ordering::SeqCst),
            "LIMIT-hit fires cancel even in partial mode"
        );
    }

    /// **T9.6 — partial=true preserves K-invariance MODULO removal of
    /// the failed-shard subset.** Given the SAME failure pattern
    /// across two equivalent fixtures, partial-mode merged bytes are
    /// byte-identical. Locks that partial mode is a deterministic
    /// function of (per-shard results, kind, failed-shards subset).
    #[test]
    fn t9_partial_mode_is_deterministic_replay_safe() {
        let mk_shards = || -> Vec<PentestShard> {
            vec![
                PentestShard::new(PentestBehavior::Got(rows_to_payload(&[b"a0", b"a1"]))),
                PentestShard::new(PentestBehavior::TransportErr("down".into())),
                PentestShard::new(PentestBehavior::Got(rows_to_payload(&[b"c0"]))),
                PentestShard::new(PentestBehavior::TransportErr("down".into())),
            ]
        };
        let mut first: Option<(OpResult, Vec<u32>)> = None;
        for run in 0..20u32 {
            let cancel = Arc::new(AtomicBool::new(false));
            let pair = scatter_and_merge_ctx(
                mk_shards(),
                &dummy_select(),
                DEFAULT_PER_SHARD_TIMEOUT,
                &ScatterKind::Unordered { limit: 0 },
                cancel,
                ScatterContext::partial(),
            );
            match &first {
                None => first = Some(pair),
                Some(baseline) => assert_eq!(
                    pair, *baseline,
                    "run {run}: partial-mode bytes / failed_shards diverged"
                ),
            }
        }
        let (out, failed) = first.unwrap();
        assert_eq!(
            out,
            OpResult::Got(rows_to_payload(&[b"a0", b"a1", b"c0"]).into()),
            "shard_0 and shard_2 contribute; 1+3 omitted"
        );
        assert_eq!(failed, vec![1u32, 3]);
    }

    /// **T9.7 — partial=true + Sorted (heap merge): failed shards
    /// omitted; surviving shards' rows heap-merge per the sort
    /// strategy.** Sorted needs every shard's payload upfront for the
    /// k-way merge; in partial mode failed slots are substituted with
    /// empty Got payloads and the heap merge naturally skips them.
    #[test]
    fn t9_partial_sorted_failed_shards_omitted_others_merge_correctly() {
        let rec = |v: u64| -> Vec<u8> { v.to_le_bytes().to_vec() };
        let s0 = PentestShard::new(PentestBehavior::Got(rows_to_payload(&[
            &rec(1), &rec(5),
        ])));
        // shard_1 dies.
        let s1 = PentestShard::new(PentestBehavior::TransportErr("down".into()));
        let s2 = PentestShard::new(PentestBehavior::Got(rows_to_payload(&[
            &rec(2), &rec(7),
        ])));
        let cancel = Arc::new(AtomicBool::new(false));
        let (out, failed) = scatter_and_merge_ctx(
            vec![s0, s1, s2],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Sorted {
                sort_kind: FieldKind::U64,
                sort_offset: 0,
                sort_width: 8,
                desc: false,
                offset: 0,
                limit: 0,
            },
            cancel,
            ScatterContext::partial(),
        );
        // Heap merge of shard_0 + shard_2 only: [1, 2, 5, 7].
        let expected = rows_to_payload(&[&rec(1), &rec(2), &rec(5), &rec(7)]);
        assert_eq!(out, OpResult::Got(expected.into()));
        assert_eq!(failed, vec![1u32]);
    }

    /// **T9.8 — partial=true + malformed Got from one shard: SchemaError
    /// still surfaces.** Per T9 doc: partial mode is for per-shard
    /// AVAILABILITY events (Unavailable / SchemaError-from-shard / etc.).
    /// A `Got(garbage)` with bad framing is a TRANSPORT/FRAMING bug,
    /// not an availability event — the merger still surfaces it cleanly
    /// instead of silently dropping garbage bytes from a shard.
    #[test]
    fn t9_partial_mode_does_not_swallow_malformed_payload_framing() {
        let s0 = PentestShard::new(PentestBehavior::Got(rows_to_payload(&[b"good"])));
        // bad framing: claims 4 GiB row in 4 bytes
        let bad = {
            let mut v = (u32::MAX).to_le_bytes().to_vec();
            v.extend_from_slice(&[1, 2, 3, 4]);
            v
        };
        let s1 = PentestShard::new(PentestBehavior::MalformedGot(bad));
        let cancel = Arc::new(AtomicBool::new(false));
        let (out, _failed) = scatter_and_merge_ctx(
            vec![s0, s1],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::Unordered { limit: 0 },
            cancel,
            ScatterContext::partial(),
        );
        assert!(
            matches!(out, OpResult::SchemaError(_)),
            "malformed framing surfaces SchemaError even in partial mode, got {out:?}"
        );
    }

    // ---------- T11 OidConcat (FindBy / FindByComposite) KATs ----------
    //
    // FindBy / FindByComposite return `OpResult::Got([16-byte oid]*)` —
    // a row's secondary-index entry lives on the shard owning the row,
    // so cross-shard fan-out is required. The merge is shard-id-ordered
    // concat of every shard's 16-byte oid payload (the oid sets across
    // shards are disjoint by construction — every oid lives on exactly
    // one shard via rendezvous mapping — so no dedup is needed).
    //
    // These KATs exercise the new `ScatterKind::OidConcat` merge path:
    //   1. Single-shard K=1 case is byte-identical to the shard's payload
    //      (regression-lock for the Route::Direct → Route::Scatter
    //      transition; same shape as the SP155 §10 K=1 invariant).
    //   2. Two shards' replies merge in shard-id order.
    //   3. Empty-shard mixed with non-empty: empty contributes nothing.
    //   4. K=0 returns empty Got (matches other ScatterKind variants).
    //   5. Malformed payload (length not multiple of 16) surfaces
    //      SchemaError, never panic.
    //   6. V1 hard-fail: one shard returning Unavailable poisons the
    //      whole merge (other shards' replies discarded).
    //   7. Partial mode: failed shard omitted; failed_shards records it.

    fn oid(b: u8) -> [u8; 16] {
        let mut o = [0u8; 16];
        o[0] = b;
        o
    }

    fn oids_payload(oids: &[[u8; 16]]) -> Vec<u8> {
        let mut p = Vec::with_capacity(oids.len() * 16);
        for o in oids {
            p.extend_from_slice(o);
        }
        p
    }

    /// **T11.1 — K=1 OidConcat is byte-identical to the shard's
    /// payload.** Regression-lock: the new Route::Scatter path for
    /// FindBy must not change the wire shape vs the pre-T11 direct-
    /// shard path on a single-shard deployment.
    #[test]
    fn t11_oid_concat_k1_byte_identical_to_single_shard() {
        let payload = oids_payload(&[oid(1), oid(2), oid(3)]);
        let r = merge_scan_results(
            vec![OpResult::Got(payload.clone().into())],
            &ScatterKind::OidConcat,
        );
        assert_eq!(r, OpResult::Got(payload.into()));
    }

    /// **T11.2 — Two-shard merge concatenates in shard-id order.**
    /// shard_0 returns oids [1, 2]; shard_1 returns oid [9]. The
    /// merged payload is the concat: [1, 2, 9] (16-byte each).
    #[test]
    fn t11_oid_concat_two_shards_merges_in_shard_id_order() {
        let s0 = oids_payload(&[oid(1), oid(2)]);
        let s1 = oids_payload(&[oid(9)]);
        let r = merge_scan_results(
            vec![OpResult::Got(s0.into()), OpResult::Got(s1.into())],
            &ScatterKind::OidConcat,
        );
        let expected = oids_payload(&[oid(1), oid(2), oid(9)]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    /// **T11.3 — Empty shard contributes nothing.** A shard with no
    /// matching oids returns `Got([])`; the merge skips it cleanly.
    #[test]
    fn t11_oid_concat_with_one_empty_shard() {
        let s0 = oids_payload(&[oid(1)]);
        let s1 = oids_payload(&[]); // empty middle shard
        let s2 = oids_payload(&[oid(5), oid(7)]);
        let r = merge_scan_results(
            vec![
                OpResult::Got(s0.into()),
                OpResult::Got(s1.into()),
                OpResult::Got(s2.into()),
            ],
            &ScatterKind::OidConcat,
        );
        let expected = oids_payload(&[oid(1), oid(5), oid(7)]);
        assert_eq!(r, OpResult::Got(expected.into()));
    }

    /// **T11.4 — K=0 returns empty Got (matches the
    /// `merge_scan_results(empty, ...)` shape for all variants).**
    #[test]
    fn t11_oid_concat_empty_input_is_empty_got() {
        let r = merge_scan_results(Vec::new(), &ScatterKind::OidConcat);
        assert_eq!(r, OpResult::Got(Vec::<u8>::new().into()));
    }

    /// **T11.5 — Malformed payload (length not a multiple of 16)
    /// surfaces SchemaError, never panic.** Defense against a
    /// corrupted reply from a shard.
    #[test]
    fn t11_oid_concat_rejects_non_16_byte_aligned_payload() {
        // 17 bytes — claims one 16-byte oid + a stray trailing byte.
        let bad = vec![0xAAu8; 17];
        let r = merge_scan_results(
            vec![OpResult::Got(bad.into())],
            &ScatterKind::OidConcat,
        );
        assert!(
            matches!(r, OpResult::SchemaError(_)),
            "malformed oid payload must surface SchemaError, got {r:?}"
        );
    }

    /// **T11.6 — V1 hard-fail propagates the first non-Got slot.**
    /// One shard returns Unavailable; the merged result is
    /// Unavailable (the other shards' replies are NOT mixed in).
    #[test]
    fn t11_oid_concat_propagates_first_non_got_slot() {
        let r = merge_scan_results(
            vec![
                OpResult::Got(oids_payload(&[oid(1)]).into()),
                OpResult::Unavailable,
                OpResult::Got(oids_payload(&[oid(3)]).into()),
            ],
            &ScatterKind::OidConcat,
        );
        assert_eq!(r, OpResult::Unavailable);
    }

    /// **T11.7 — End-to-end OidConcat via scatter_and_merge_ctx +
    /// partial mode.** Two shards' oid lists merge in shard-id order;
    /// a third shard fails; partial mode omits the failed slot and
    /// records it in failed_shards.
    #[test]
    fn t11_oid_concat_partial_mode_omits_failed_shard() {
        let s0 = PentestShard::new(PentestBehavior::Got(
            oids_payload(&[oid(1), oid(2)]),
        ));
        let s1 = PentestShard::new(PentestBehavior::TransportErr("down".into()));
        let s2 = PentestShard::new(PentestBehavior::Got(
            oids_payload(&[oid(9)]),
        ));
        let cancel = Arc::new(AtomicBool::new(false));
        let (out, failed) = scatter_and_merge_ctx(
            vec![s0, s1, s2],
            // FindBy op (the actual op flowing through; any FindBy
            // works since the mock returns its canned payload).
            &Op::FindBy {
                type_id: 1,
                field_id: 2,
                value: vec![0xAA, 0xBB],
            },
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::OidConcat,
            cancel,
            ScatterContext::partial(),
        );
        let expected = oids_payload(&[oid(1), oid(2), oid(9)]);
        assert_eq!(out, OpResult::Got(expected.into()));
        assert_eq!(failed, vec![1u32]);
    }

    /// **T11.8 — End-to-end OidConcat hard-fail propagates and fires
    /// cancel.** The combined fan-out + merge entry point preserves
    /// the V1 hard-fail contract for OidConcat as well as the
    /// Unordered / Sorted variants.
    #[test]
    fn t11_oid_concat_hard_fail_propagates_unavailable() {
        let s0 = PentestShard::new(PentestBehavior::Got(
            oids_payload(&[oid(1)]),
        ));
        let s1 = PentestShard::new(PentestBehavior::TransportErr("down".into()));
        // shard_2 is slow + comes AFTER the Unavailable shard — must
        // observe cancel pre-call (its `ran` stays 0).
        let s2 = PentestShard::new(PentestBehavior::SleepThenGot(
            Duration::from_millis(200),
            oids_payload(&[oid(7)]),
        ));
        let ran2 = s2.ran.clone();
        let cancelled2 = s2.cancelled_pre_call.clone();
        let cancel = Arc::new(AtomicBool::new(false));
        let out = scatter_and_merge(
            vec![s0, s1, s2],
            &Op::FindBy {
                type_id: 1,
                field_id: 2,
                value: vec![0xAA],
            },
            DEFAULT_PER_SHARD_TIMEOUT,
            &ScatterKind::OidConcat,
            cancel.clone(),
        );
        assert_eq!(out, OpResult::Unavailable, "V1 hard-fail propagates");
        assert!(cancel.load(Ordering::SeqCst), "cancel fires on non-Got");
        assert_eq!(
            ran2.load(Ordering::SeqCst),
            0,
            "shard_2 must NOT have completed its call"
        );
        assert!(
            cancelled2.load(Ordering::SeqCst) >= 1,
            "shard_2 observed cancel pre-call"
        );
    }

    // ====================================================================
    // SP-Perf-A-SHARD-SCAN unit tests — new ScatterKind merge functions
    // ====================================================================

    /// `merge_oid_sorted_union` should sort+dedup the union of per-shard
    /// 16-byte oid payloads (matches `Op::Query/QueryExpr/FindRange` K=1
    /// behaviour which sort_unstable+dedups before emit).
    #[test]
    fn shard_scan_oid_sorted_union_sorts_and_dedups() {
        let id = |n: u8| {
            let mut a = [0u8; 16];
            a[15] = n;
            a
        };
        // Shard 0 has ids 3, 1; shard 1 has ids 2, 1 (1 is dup across
        // shards — defensive dedup); shard 2 has nothing.
        let s0: Vec<u8> = id(3).iter().chain(id(1).iter()).copied().collect();
        let s1: Vec<u8> = id(2).iter().chain(id(1).iter()).copied().collect();
        let s2: Vec<u8> = Vec::new();
        let payloads: Vec<&[u8]> = vec![&s0, &s1, &s2];
        let r = merge_oid_sorted_union(&payloads);
        match r {
            OpResult::Got(b) => {
                // Expected: sorted [1, 2, 3], deduped, each 16 bytes.
                assert_eq!(b.len(), 48, "expected 3×16 bytes, got {}", b.len());
                let mut got: Vec<u8> = Vec::new();
                for chunk in b.chunks(16) {
                    got.push(chunk[15]);
                }
                assert_eq!(got, vec![1u8, 2, 3]);
            }
            other => panic!("expected Got, got {other:?}"),
        }
    }

    #[test]
    fn shard_scan_oid_sorted_union_rejects_unaligned_payload() {
        // Shard 0 returns 15 bytes (not a multiple of 16) → SchemaError.
        let s0: Vec<u8> = vec![0u8; 15];
        let payloads: Vec<&[u8]> = vec![&s0];
        let r = merge_oid_sorted_union(&payloads);
        assert!(matches!(r, OpResult::SchemaError(_)));
    }

    /// `merge_aggregate` for COUNT (kind=0) sums per-shard i128 counts.
    #[test]
    fn shard_scan_agg_merge_count_sums() {
        // Three shards: 5 rows + 3 rows + 0 rows = 8 total.
        let s0 = 5i128.to_le_bytes().to_vec();
        let s1 = 3i128.to_le_bytes().to_vec();
        let s2: Vec<u8> = Vec::new(); // empty = no rows
        let payloads: Vec<&[u8]> = vec![&s0, &s1, &s2];
        let r = merge_aggregate(&payloads, 0, FieldKind::U8);
        match r {
            OpResult::Got(b) => {
                let mut le = [0u8; 16];
                le.copy_from_slice(&b[..16]);
                assert_eq!(i128::from_le_bytes(le), 8);
            }
            other => panic!("got {other:?}"),
        }
    }

    /// `merge_aggregate` for SUM (kind=1) sums per-shard i128 sums.
    #[test]
    fn shard_scan_agg_merge_sum_combines() {
        let s0 = 100i128.to_le_bytes().to_vec();
        let s1 = (-30i128).to_le_bytes().to_vec();
        let s2 = 7i128.to_le_bytes().to_vec();
        let payloads: Vec<&[u8]> = vec![&s0, &s1, &s2];
        let r = merge_aggregate(&payloads, 1, FieldKind::I64);
        match r {
            OpResult::Got(b) => {
                let mut le = [0u8; 16];
                le.copy_from_slice(&b[..16]);
                assert_eq!(i128::from_le_bytes(le), 77);
            }
            other => panic!("got {other:?}"),
        }
    }

    /// `merge_aggregate` for MIN (kind=2) takes min across shards.
    /// Numeric ≤8B path: 16-byte i128 LE per shard.
    #[test]
    fn shard_scan_agg_merge_min_numeric() {
        let s0 = 50i128.to_le_bytes().to_vec();
        let s1 = 10i128.to_le_bytes().to_vec();
        let s2 = 100i128.to_le_bytes().to_vec();
        let payloads: Vec<&[u8]> = vec![&s0, &s1, &s2];
        let r = merge_aggregate(&payloads, 2, FieldKind::U64);
        match r {
            OpResult::Got(b) => {
                let mut le = [0u8; 16];
                le.copy_from_slice(&b[..16]);
                assert_eq!(i128::from_le_bytes(le), 10);
            }
            other => panic!("got {other:?}"),
        }
    }

    /// `merge_aggregate` for MAX (kind=3) takes max across shards.
    #[test]
    fn shard_scan_agg_merge_max_numeric() {
        let s0 = 50i128.to_le_bytes().to_vec();
        let s1 = 10i128.to_le_bytes().to_vec();
        let s2 = 100i128.to_le_bytes().to_vec();
        let payloads: Vec<&[u8]> = vec![&s0, &s1, &s2];
        let r = merge_aggregate(&payloads, 3, FieldKind::U64);
        match r {
            OpResult::Got(b) => {
                let mut le = [0u8; 16];
                le.copy_from_slice(&b[..16]);
                assert_eq!(i128::from_le_bytes(le), 100);
            }
            other => panic!("got {other:?}"),
        }
    }

    /// `merge_aggregate` for AVG (kind=4) hard-fails at K>=2 by design.
    #[test]
    fn shard_scan_agg_merge_avg_rejected() {
        let s0 = 50i128.to_le_bytes().to_vec();
        let payloads: Vec<&[u8]> = vec![&s0];
        let r = merge_aggregate(&payloads, 4, FieldKind::U64);
        match r {
            OpResult::SchemaError(msg) => {
                assert!(msg.contains("AVG"), "msg: {msg}");
            }
            other => panic!("expected SchemaError, got {other:?}"),
        }
    }

    /// `merge_aggregate` MIN with var-width path (CHAR) compares bytes.
    #[test]
    fn shard_scan_agg_merge_min_var_width_char() {
        // 4-byte CHAR field; shard 0 has "ccc", shard 1 has "aaa".
        let s0 = b"ccc\0".to_vec();
        let s1 = b"aaa\0".to_vec();
        let payloads: Vec<&[u8]> = vec![&s0, &s1];
        let r = merge_aggregate(&payloads, 2, FieldKind::Char(4));
        match r {
            OpResult::Got(b) => {
                assert_eq!(&b[..], b"aaa\0");
            }
            other => panic!("got {other:?}"),
        }
    }

    /// `merge_group_aggregate` combines per-group i128 values across shards.
    /// Group "A" has SUM 10 on shard 0, SUM 5 on shard 1 → merged SUM 15.
    /// Group "B" only on shard 1, SUM 3.
    /// Group "C" only on shard 0, SUM 7.
    #[test]
    fn shard_scan_group_agg_merge_sum() {
        fn encode_groups(items: &[(&[u8], i128)]) -> Vec<u8> {
            let mut out = (items.len() as u32).to_le_bytes().to_vec();
            for (k, v) in items {
                out.extend_from_slice(&(k.len() as u32).to_le_bytes());
                out.extend_from_slice(k);
                out.extend_from_slice(&v.to_le_bytes());
            }
            out
        }
        let s0 = encode_groups(&[(b"A", 10), (b"C", 7)]);
        let s1 = encode_groups(&[(b"A", 5), (b"B", 3)]);
        let payloads: Vec<&[u8]> = vec![&s0, &s1];
        let r = merge_group_aggregate(&payloads, 1); // SUM
        match r {
            OpResult::Got(b) => {
                // Expected: 3 groups, sorted by key: A=15, B=3, C=7.
                let n = u32::from_le_bytes(b[..4].try_into().unwrap()) as usize;
                assert_eq!(n, 3, "expected 3 merged groups");
                let mut pos = 4;
                let mut got: Vec<(Vec<u8>, i128)> = Vec::new();
                for _ in 0..n {
                    let kl = u32::from_le_bytes(
                        b[pos..pos + 4].try_into().unwrap(),
                    ) as usize;
                    pos += 4;
                    let k = b[pos..pos + kl].to_vec();
                    pos += kl;
                    let mut le = [0u8; 16];
                    le.copy_from_slice(&b[pos..pos + 16]);
                    let v = i128::from_le_bytes(le);
                    pos += 16;
                    got.push((k, v));
                }
                assert_eq!(
                    got,
                    vec![
                        (b"A".to_vec(), 15),
                        (b"B".to_vec(), 3),
                        (b"C".to_vec(), 7),
                    ]
                );
            }
            other => panic!("got {other:?}"),
        }
    }

    /// `merge_group_aggregate` with MIN takes per-group min across shards.
    #[test]
    fn shard_scan_group_agg_merge_min() {
        fn encode_groups(items: &[(&[u8], i128)]) -> Vec<u8> {
            let mut out = (items.len() as u32).to_le_bytes().to_vec();
            for (k, v) in items {
                out.extend_from_slice(&(k.len() as u32).to_le_bytes());
                out.extend_from_slice(k);
                out.extend_from_slice(&v.to_le_bytes());
            }
            out
        }
        let s0 = encode_groups(&[(b"x", 50), (b"y", 5)]);
        let s1 = encode_groups(&[(b"x", 10), (b"y", 100)]);
        let payloads: Vec<&[u8]> = vec![&s0, &s1];
        let r = merge_group_aggregate(&payloads, 2); // MIN
        match r {
            OpResult::Got(b) => {
                let n = u32::from_le_bytes(b[..4].try_into().unwrap()) as usize;
                assert_eq!(n, 2);
                let mut pos = 4;
                let mut got: Vec<(Vec<u8>, i128)> = Vec::new();
                for _ in 0..n {
                    let kl = u32::from_le_bytes(
                        b[pos..pos + 4].try_into().unwrap(),
                    ) as usize;
                    pos += 4;
                    let k = b[pos..pos + kl].to_vec();
                    pos += kl;
                    let mut le = [0u8; 16];
                    le.copy_from_slice(&b[pos..pos + 16]);
                    pos += 16;
                    got.push((k, i128::from_le_bytes(le)));
                }
                assert_eq!(
                    got,
                    vec![(b"x".to_vec(), 10), (b"y".to_vec(), 5),]
                );
            }
            other => panic!("got {other:?}"),
        }
    }

    /// `merge_group_aggregate_multi`: two aggregates per group (COUNT + SUM).
    /// Group "A": shard0 (cnt=2, sum=10), shard1 (cnt=3, sum=5)
    ///        → merged (cnt=5, sum=15).
    /// Group "B": only on shard0 (cnt=1, sum=7).
    #[test]
    fn shard_scan_group_agg_multi_merge() {
        fn encode_groups(items: &[(&[u8], Vec<i128>)]) -> Vec<u8> {
            let mut out = (items.len() as u32).to_le_bytes().to_vec();
            for (k, vs) in items {
                out.extend_from_slice(&(k.len() as u32).to_le_bytes());
                out.extend_from_slice(k);
                for v in vs {
                    out.extend_from_slice(&v.to_le_bytes());
                }
            }
            out
        }
        let s0 = encode_groups(&[(b"A", vec![2, 10]), (b"B", vec![1, 7])]);
        let s1 = encode_groups(&[(b"A", vec![3, 5])]);
        let payloads: Vec<&[u8]> = vec![&s0, &s1];
        // kinds: [COUNT, SUM] = [0, 1]
        let r = merge_group_aggregate_multi(&payloads, &[0u8, 1u8]);
        match r {
            OpResult::Got(b) => {
                let n = u32::from_le_bytes(b[..4].try_into().unwrap()) as usize;
                assert_eq!(n, 2);
                let mut pos = 4;
                let mut got: Vec<(Vec<u8>, Vec<i128>)> = Vec::new();
                for _ in 0..n {
                    let kl = u32::from_le_bytes(
                        b[pos..pos + 4].try_into().unwrap(),
                    ) as usize;
                    pos += 4;
                    let k = b[pos..pos + kl].to_vec();
                    pos += kl;
                    let mut slots = Vec::new();
                    for _ in 0..2 {
                        let mut le = [0u8; 16];
                        le.copy_from_slice(&b[pos..pos + 16]);
                        slots.push(i128::from_le_bytes(le));
                        pos += 16;
                    }
                    got.push((k, slots));
                }
                assert_eq!(
                    got,
                    vec![
                        (b"A".to_vec(), vec![5, 15]),
                        (b"B".to_vec(), vec![1, 7]),
                    ]
                );
            }
            other => panic!("got {other:?}"),
        }
    }

    /// `merge_group_aggregate_multi` rejects AVG (kind=4) in any slot.
    #[test]
    fn shard_scan_group_agg_multi_merge_rejects_avg_slot() {
        let payloads: Vec<&[u8]> = vec![];
        let r = merge_group_aggregate_multi(&payloads, &[0u8, 4u8]);
        match r {
            OpResult::SchemaError(msg) => {
                assert!(msg.contains("AVG"), "msg: {msg}");
            }
            other => panic!("got {other:?}"),
        }
    }
}
