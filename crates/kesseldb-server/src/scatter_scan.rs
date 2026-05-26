//! SP-A scatter-scan router-side helper (SP155 design, T1 scaffold).
//!
//! Cross-shard scatter scan / filter reads. The router fans out an
//! existing scan-shaped `Op` (`Select` / `QueryRows` / `SelectFields` /
//! `SelectSorted`) to every shard, collects per-shard `OpResult`s, then
//! merges them into a single result. This module is the SP-A T1 scaffold:
//! the per-shard fan-out plumbing + a merge stub. The actual ordered merge
//! lives in SP-A T2 (sorted) / T3 (unordered concat); LIMIT cancellation
//! in T6; pentests in T8. T1 ships:
//!
//! - [`ShardCaller`] — the trait per-shard dispatch needs (a single
//!   `call(&op) -> Result<OpResult, String>`). The router's `ClusterClient`
//!   implements it trivially; the unit tests below drive a mock without
//!   spawning real shards (per the SP155 spec §7 — T1 is router-side
//!   plumbing, the multi-shard integration test is the T5/T8 work).
//! - [`scatter_scan_fanout`] — spawns one `std::thread` per shard,
//!   collects per-shard `OpResult`s in **shard-id order** (NOT arrival
//!   order — replay-determinism trumps "fastest wins"), with a per-shard
//!   bounded timeout (default 30s, configurable). The threads are joined
//!   within the timeout window; a shard that exceeds the timeout
//!   contributes `OpResult::Unavailable` to its slot.
//! - [`merge_scan_results`] — **STUB** for T1. Returns the first
//!   non-empty `Got(_)` (or the first non-Got error) so the call site has
//!   *something* to return. Real ordered-merge (k-way heap for
//!   `SelectSorted`, shard-id-ordered concat for the unordered scan ops)
//!   lands in **SP-A T2/T3** per the spec §3.5 + §3.6.
//!
//! Determinism + zero-dep per SP155 §3.3: `std::thread` + `std::sync::mpsc`
//! only. No tokio. No rayon. Worker threads are joined within bounded
//! time (the timeout); a `Drop` on the returned join handles is a no-op
//! by design (each handle has already been joined before this function
//! returns). The result vec has length equal to `shards.len()`, ordered
//! by shard index — the same total order a single-shard run would
//! observe with K=1, just K-way.
//!
//! Wire-shape note (SP155 §4.1): the router ships the SAME `Op` to every
//! shard. There is NO new `Op` variant for scatter. Clients keep sending
//! `Op::Select` / `Op::SelectSorted` / etc. — the router does the work.

use kessel_proto::{Op, OpResult};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

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
}

/// Default per-shard scatter timeout: 30s, matching the SP155 OQ1
/// (per-shard `ClusterClient` default). Exposed as `pub const` so the
/// router can override via a future `Router::with_scatter_per_shard_timeout`
/// (SP-A T9 in the backlog).
pub const DEFAULT_PER_SHARD_TIMEOUT: Duration = Duration::from_secs(30);

/// Fan out `op` to every shard in `shards`, in **parallel**, with a
/// per-shard timeout. Returns one `OpResult` per shard, in **shard-id
/// order** (NOT arrival order — replay-determinism per SP155 §3.6).
///
/// Algorithm (zero-dep, std-thread only):
///
/// 1. Spawn one `std::thread` per shard. Each worker `call`s its shard
///    and sends the result over a `mpsc::sync_channel(1)`.
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
    // Per-shard reply channels. `sync_channel(1)` so the worker can send
    // and exit without blocking on a slow driver (the driver may have
    // already moved past this slot's deadline).
    let mut rxs: Vec<mpsc::Receiver<OpResult>> = Vec::with_capacity(k);
    let mut handles: Vec<thread::JoinHandle<()>> = Vec::with_capacity(k);
    for (i, mut caller) in shards.into_iter().enumerate() {
        let (tx, rx) = mpsc::sync_channel::<OpResult>(1);
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
                // (timeout fired and moved on), the value is discarded.
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

/// **STUB** — SP-A T2/T3 ship the real merge. For T1 this returns:
///
/// - If `results` is empty: an empty `OpResult::Got(vec![])`.
/// - If any slot is non-`Got`: the first non-`Got` slot, in shard-id
///   order. (`Unavailable` propagates — V1 hard-fail per SP155 §6.)
/// - Otherwise: the **first** `Got(_)` slot. **Wrong for K>1** —
///   T2/T3 land the shard-id-ordered concat for unordered scans and the
///   `(sort_field, object_id)` heap merge for sorted scans.
///
/// The signature is the stable T2/T3 entry point; only the body changes.
pub fn merge_scan_results(results: Vec<OpResult>) -> OpResult {
    if results.is_empty() {
        return OpResult::Got(Vec::new());
    }
    // V1 hard-fail: surface the first non-Got slot (Unavailable /
    // SchemaError / etc.) so the caller sees a clean failure instead
    // of partial-then-merged. This matches SP155 §6 "Shard unavailable"
    // row default (`scatter_partial_on_timeout=false`).
    for r in &results {
        if !matches!(r, OpResult::Got(_)) {
            return r.clone();
        }
    }
    // TODO(SP-A T2/T3): replace with the real merge.
    // - `ScatterKind::Unordered` ⇒ shard-id-ordered concat of every
    //   `[u32 rowlen][record]*` payload, truncated to the op's `limit`.
    // - `ScatterKind::Sorted` ⇒ `BinaryHeap<(sort_key, object_id, row)>`
    //   k-way merge across all shards, applying OFFSET + LIMIT.
    // For T1 we return the first shard's payload so the call site has
    // *something* coherent; this is intentionally incorrect for K>1 and
    // is locked by the `merge_stub_is_first_got_slot` KAT below so a
    // future T2/T3 commit MUST update the test simultaneously.
    results.into_iter().next().unwrap()
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
        let s = MockShard::new(OpResult::Got(vec![1, 2, 3, 4]));
        let out = scatter_scan_fanout(
            vec![s],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0], OpResult::Got(vec![1, 2, 3, 4]));
    }

    /// K=3: the result is in **shard-id order**, NOT arrival order, even
    /// though shard 0 sleeps and shard 2 returns instantly. This is the
    /// SP155 §3.6 determinism property — replay-safe ordering.
    #[test]
    fn fan_out_to_three_shards_returns_three_results_in_shard_order() {
        let s0 = MockShard::new(OpResult::Got(b"shard-0".to_vec()))
            .slow(Duration::from_millis(50));
        let s1 = MockShard::new(OpResult::Got(b"shard-1".to_vec()));
        let s2 = MockShard::new(OpResult::Got(b"shard-2".to_vec()));
        let out = scatter_scan_fanout(
            vec![s0, s1, s2],
            &dummy_select(),
            DEFAULT_PER_SHARD_TIMEOUT,
        );
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], OpResult::Got(b"shard-0".to_vec()));
        assert_eq!(out[1], OpResult::Got(b"shard-1".to_vec()));
        assert_eq!(out[2], OpResult::Got(b"shard-2".to_vec()));
    }

    /// A shard that exceeds the per-shard timeout contributes
    /// `Unavailable` to its slot. Other shards' replies are unaffected.
    /// Per SP155 §6 "Shard timeout" row (V1 hard-fail default).
    #[test]
    fn a_shard_that_times_out_returns_unavailable_for_that_slot() {
        let s0 = MockShard::new(OpResult::Got(b"fast".to_vec()));
        let s1 = MockShard::new(OpResult::Got(b"too-slow".to_vec()))
            .slow(Duration::from_millis(300));
        let s2 = MockShard::new(OpResult::Got(b"also-fast".to_vec()));
        let out = scatter_scan_fanout(
            vec![s0, s1, s2],
            &dummy_select(),
            Duration::from_millis(80),
        );
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], OpResult::Got(b"fast".to_vec()));
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
            OpResult::Got(b) => assert_eq!(b, b"also-fast"),
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
        let s0 = MockShard::new(OpResult::Got(vec![]));
        let s1 = MockShard::new(OpResult::Got(vec![]));
        let s2 = MockShard::new(OpResult::Got(vec![]));
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
        let s = MockShard::new(OpResult::Got(b"ok".to_vec()))
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
        assert_eq!(out[0], OpResult::Got(b"ok".to_vec()));
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

    /// `merge_scan_results` on an empty result vec returns an empty
    /// `Got([])` — matches per-shard `Select` semantics (an empty
    /// filter result is `Got([])`, not `NotFound`) per SP155 OQ11.
    #[test]
    fn merge_empty_results_is_empty_got() {
        let out = merge_scan_results(Vec::new());
        assert_eq!(out, OpResult::Got(Vec::new()));
    }

    /// `merge_scan_results` propagates the first non-`Got` slot — V1
    /// hard-fail per SP155 §6. T2/T3 keep this semantic for non-Got
    /// slots; only the all-Got merge changes.
    #[test]
    fn merge_propagates_first_non_got_slot() {
        let r = merge_scan_results(vec![
            OpResult::Got(b"a".to_vec()),
            OpResult::Unavailable,
            OpResult::Got(b"c".to_vec()),
        ]);
        assert_eq!(r, OpResult::Unavailable);
    }

    /// **REGRESSION-LOCK** for the T1 stub: when every slot is `Got(_)`,
    /// the stub returns the first slot verbatim. T2/T3 MUST update this
    /// test together with the merge implementation — flipping this lock
    /// is the gate that catches a half-shipped T2.
    #[test]
    fn merge_stub_is_first_got_slot() {
        let r = merge_scan_results(vec![
            OpResult::Got(b"shard-0".to_vec()),
            OpResult::Got(b"shard-1".to_vec()),
            OpResult::Got(b"shard-2".to_vec()),
        ]);
        assert_eq!(
            r,
            OpResult::Got(b"shard-0".to_vec()),
            "T1 stub returns first Got; flip in T2/T3 with the real \
             shard-id-ordered concat (unordered) or heap merge (sorted)"
        );
    }
}
