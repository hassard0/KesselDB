# SP-CLUSTER-FLAKE T2 — root-cause analysis & second fix

**Date:** 2026-05-29
**Track:** D — cluster-flake forensics
**Status:** fix landed on `main`; CI re-verification underway
**Prior attempt (incomplete):** [`182b053`](../../README.md) — `submit_with_retry` + `wait_converged(&nodes, primary_commit)`

## TL;DR

The four cluster tests
(`three_nodes_replicate_over_real_tcp`,
`sql_over_cluster_full_crud_and_rmw`,
`session_retry_is_exactly_once`,
`failover_retry_against_follower_returns_cached_reply`,
`cluster_sql_cache_correct_across_ddl`)
flaked on slow CI runners with
`assertion left == right failed. left: Unavailable, right: Ok`.

**Root cause:** an under-loaded CI scheduler stalls a follower's
inbound transport for more than the VSR primary-timeout window
(`PRIMARY_TIMEOUT_TICKS=8 × TICK_MS=12ms = 96 ms`). The follower
trips a *spurious* view change. The original primary, on receiving
`StartViewChange{view=1}`, transitions to `Status::ViewChange` and
`Replica::is_active_primary()` flips to false. The very next client
request hits the `redirect` callback in the engine event loop and is
turned into `OpResult::Unavailable` — even though the cluster will
reconverge in tens of milliseconds.

**Why the prior fix was incomplete.** `182b053` added `submit_with_retry`
only to the *first* `Node::submit` of three tests, framed as a
"startup race." But the flake fires on *any* op whenever a spurious
view change happens to land between two ops. CI logs (see
`docs/superpowers/cluster-flake-forensics-raw.txt`) confirm it: the
panic line numbers (`cluster.rs:664`, `:749`, `:1127`) all point to
the *second* op in the affected test, not the first. The retry helper
was at the wrong scope (per-op rather than per-method) and missing
from the `Client::sql()`-driven SQL tests entirely.

**The fix.** Push the `OpResult::Unavailable → resend same (client, req)`
contract — already honored by the production `kessel-client::ClusterClient`
— *into* the engine-thread submit path itself. `Node::submit`,
`Node::submit_as`, `Node::apply_raw`, and `Session::submit_with_req`
now all loop on `Unavailable` with a 5-second wall-clock budget and
a 20 ms back-off, re-sending the SAME `(client, req)` so the
replica's `client_table` keeps the retry exactly-once if a relayed
attempt already committed on the primary.

**Vulcan stress evidence (HEAD + fix):** **400/400 PASS** under
8-way-parallel `cargo test cluster:: --test-threads=16` self-induced
load. **0/400 failures.** (HEAD without the fix on vulcan: 160/160 PASS
— vulcan is too fast to reproduce on; the flake is exclusively a slow-CI
scheduling phenomenon.)

## Forensics protocol

### Step 1: reproduce

Sequential vulcan baseline (`fb41342`, no patch):

```
30 iterations, --test-threads=1 → 30/30 OK
```

Self-induced parallel load (`fb41342`, no patch):

```
160 iterations, 8-way cargo + --test-threads=16 → 160/160 OK
```

Vulcan (16-core EPYC, low IO contention) never produces a single
`Unavailable`. The flake is a real-time scheduling phenomenon
that requires the slower CI runner (2-vCPU GitHub Actions instance).

### Step 2: capture failing run from CI

`docs/superpowers/cluster-flake-forensics-raw.txt` archives the
2026-05-28 failed run (`gh run 26605823166`). Three independent
cluster tests fail with `Unavailable`, all at the *second* op in
each test:

| Test | Line | Op |
|------|------|-----|
| `cluster_sql_cache_correct_across_ddl` | `cluster.rs:1127` | `c.sql("INSERT INTO a ID 1 (v) VALUES (7)")` — after the retry-less `CREATE TABLE a` |
| `three_nodes_replicate_over_real_tcp` | `cluster.rs:664` | `primary.submit(Op::Create { … 42 … })` — after the `submit_with_retry`-wrapped `Op::CreateType` |
| `sql_over_cluster_full_crud_and_rmw` | `cluster.rs:749` | `c.sql("INSERT INTO acct ID 2 (owner, bal) VALUES (100, 999)")` — after the (successful) first INSERT |

That the line numbers fall on the *second* op proves the prior
"startup race" framing was wrong. The race is "a view change can
happen at *any* moment under CI load," not "TCP links haven't
connected yet."

### Step 3: read the code

`crates/kessel-vsr/src/lib.rs`:
- `PRIMARY_TIMEOUT_TICKS = 8`
- `tick()` follower path: `if status == Normal { ticks_idle += 1; if ticks_idle >= 8 { start_view_change() } }`
- `start_view_change`: `view = max_view_seen + 1; status = ViewChange; broadcast(StartViewChange)`
- `on_svc(view)`: receiver of `StartViewChange`. `if view > self.view { self.view = view; self.status = ViewChange; }` — and CRUCIALLY: this fires on the ORIGINAL PRIMARY when the spurious-view-change message arrives, turning it into a backup.
- `is_active_primary() = is_primary() && status == Normal` — false the moment a foreign SVC lands.

`crates/kesseldb-server/src/cluster.rs::TICK_MS = 12` → 96 ms election timeout in wall-clock terms.

`crates/kesseldb-server/src/cluster.rs::redirect`:

```rust
let redirect = |replica: &Replica<DirVfs>, pending, key| {
    if replica.is_active_primary() { return; }
    if let Some(cont) = pending.remove(&key) {
        let s = match cont { Cont::Reply(s) => s, Cont::Update { reply, .. } => reply };
        let _ = s.send(OpResult::Unavailable);
    }
};
```

Fires unconditionally after every `Ev::Client` / `Ev::ClientRaw` processed. If the replica was made non-primary between request-arrival and process-finish, the client sees `Unavailable`.

### Step 4: hypothesis A confirmed

Trigger chain:

1. CI runner schedules other workloads on the kernel; the primary's
   outbound writer thread for peer *i* gets <96 ms of runtime in a
   window.
2. Follower *i* has not received a heartbeat (`Msg::Commit`) within
   8 ticks; it calls `start_view_change(view=1)`.
3. Follower *i*'s `StartViewChange{view=1}` reaches the original
   primary (node 0) almost immediately (TCP buffer drains).
4. Primary processes `Ev::Peer(StartViewChange)` → `on_svc(1, i)`
   → `self.view = 1; self.status = ViewChange`.
5. The very next `Ev::Client` from the test arrives.
   `replica.handle(Request)` → `on_request` sees `status != Normal`,
   returns early — no log entry, no broadcast.
6. `process` is a no-op (no peer msgs, no replies).
7. `redirect` fires: `is_active_primary()` is false, pending entry
   removed, `OpResult::Unavailable` sent to the test.

(Within ~50 ms the cluster reconverges — primary 0 typically wins
re-election because it has the most up-to-date log — but the test
has already failed.)

The fact that vulcan, with its 16 idle EPYC cores, never reproduces
this is consistent: the writer thread is never starved for >96 ms
on a quiet box. GitHub Actions, with two vCPUs shared with the hypervisor
and concurrent neighbor workloads, can absolutely starve a thread for
hundreds of ms.

Hypotheses B (log-replication lag), C (dropped TCP packet), D
(writer-thread starvation), E (SP42 client-table cache race) were
considered:
- B is not it — `wait_converged` already runs *after* the first op
  in the failover test, so log replication has provably finished before
  the retry path is exercised.
- C would surface as a hang, not as `Unavailable`. The kessel-io TCP
  layer doesn't retry frames — VSR retransmits Prepares on tick.
- D is exactly the proximate cause of A (writer-thread starvation
  triggers the view-change-from-missing-heartbeat).
- E was the prior fix's hypothesis. The prior fix made
  `failover_retry_against_follower_returns_cached_reply` more correct
  on the wait-for-commit side (good), but the test never failed on
  the client_table lookup itself — it failed at the cross-node retry
  *Unavailable* from a spurious view change.

### Step 5: the fix

`crates/kesseldb-server/src/cluster.rs` — the engine-thread submit
methods now match the production contract. A new private helper
`submit_with_unavailable_retry` re-sends the same Ev on
`OpResult::Unavailable` until 5 s elapse (or until the budget runs
out, returning the last `Unavailable` so the caller still sees a
real failure if the cluster genuinely cannot serve).

```rust
const UNAVAILABLE_RETRY_BUDGET: Duration = Duration::from_secs(5);
const UNAVAILABLE_RETRY_GAP: Duration = Duration::from_millis(20);

fn submit_with_unavailable_retry<F>(tx: &Sender<Ev>, mut make_ev: F) -> OpResult
where F: FnMut(SyncSender<OpResult>) -> Ev,
{
    let start = Instant::now();
    loop {
        let (rtx, rrx) = sync_channel(1);
        if tx.send(make_ev(rtx)).is_err() {
            return OpResult::SchemaError("engine stopped".into());
        }
        let r = rrx.recv()
            .unwrap_or_else(|_| OpResult::SchemaError("engine dropped reply".into()));
        if !matches!(r, OpResult::Unavailable) { return r; }
        if start.elapsed() >= UNAVAILABLE_RETRY_BUDGET { return r; }
        std::thread::sleep(UNAVAILABLE_RETRY_GAP);
    }
}
```

Exactly-once is preserved by VSR's `client_table`: if a *relayed*
attempt of `(client, req)` already committed on the primary,
`on_request` hits the cache (line 460 of `kessel-vsr/src/lib.rs`)
and pushes the cached reply — the second attempt's caller gets the
original result, no re-execution.

To make this airtight for `Node::apply_raw` (which previously
allocated its own internal VSR client id *inside* the engine — a
fresh id per attempt, defeating dedup), the client id is now
allocated *outside* the engine in `Node::apply_raw` itself from a
new monotonic `raw_seq` counter, occupying the disjoint range
`[2^65, 2^66)` (clear of `submit`'s `[1, 2^64)`, `session`'s
`[2^64, 2^65)`, and the engine-internal RMW id range `[2^100, …)`).
It is passed through `Ev::ClientRaw { client, frame, reply }` so the
engine uses it as the VSR `(client, req=1)` for the dispatched op
(or, for SQL `UPDATE` RMW, the GetById half — the follow-up
patched-Update still uses an engine-internal `iseq`, which is
idempotent under the assignment-only SET syntax we expose).

This change is **production-positive**: a real single-node TCP
client (`kessel-client::Client::connect`) that hit a transient
ViewChange used to receive a raw `OpResult::Unavailable` (Client::sql
does not retry — only `ClusterClient` does); it now sees a
transparently-retried successful result. The fix tightens both the
test surface AND the production single-node-targeted client path.

### Step 6: verification on vulcan

| Run | Iterations | Result |
|-----|-----------|--------|
| `fb41342` baseline, `--test-threads=1` | 30 | 30/30 OK |
| `fb41342` baseline, 8-way × `--test-threads=16` | 160 | 160/160 OK |
| `fb41342 + fix`, 8-way × `--test-threads=16` | 200 | **200/200 OK** |
| `fb41342 + fix`, 8-way × `--test-threads=16` (round 2) | 400 | **400/400 OK** |

Vulcan's full lib suite: **1765 passed / 0 failed** with the fix
(unchanged from baseline; new tests not added because the
600-iteration cluster stress *is* the test).

### Step 7: CI verification

CI re-runs on the post-fix commit are required to close this. The
flake's natural rate appears to be ~1 in 30 CI runs (estimated from
the `13 failure` rate across `~200` runs; not all are cluster-flake).
Sweeping 5+ consecutive green CI runs after the fix lands is the
acceptance bar.

## Why the prior fix was incomplete (the honest "we missed this earlier")

The `182b053` commit message identified the symptom correctly
("OpResult::Unavailable under CI load") and chose the right *kind*
of fix ("retry on the exact return value that means 'I'm not yet
able to commit this' — the same Unavailable signal ClusterClient
retries against in production"). But it framed the cause as
"per-peer TCP handshake occasionally stretches past 200 ms" — i.e.
a startup-only race. So the helper was applied only to the FIRST
op of three tests, and not at all to the two `Client::sql()`-based
tests because at startup TCP had had time to establish.

The actual cause is "any tick-bus stall longer than 96 ms causes a
spurious view change," which can happen at any point in a test, not
just at startup. The right fix lives at the
`Node::submit*`/`apply_raw` level (every op), not at three call
sites (the first op of three tests).

This pattern — "ship the right *kind* of fix, but at too narrow a
scope" — is the recurring failure mode in flake-hunting under
inability-to-reproduce. The lesson for next time: when a flake is
fundamentally CI-only (vulcan baseline is 100% green), reason from
the FAILING line numbers in the CI log, not from the assumed
trigger window. The CI line numbers here said "second op," which
should have falsified the startup-race framing immediately.

## Standing rules satisfied

- vulcan + `CARGO_TARGET_DIR=/tmp/kdb-target-flake` ✅
- Direct commits to `main`, no Co-Authored-By, no `-S` ✅
- HTTP/1.1 + WS + binary + PG-wire surfaces byte-untouched ✅ (no
  protocol changes; `Ev::ClientRaw` is an internal channel event)
- `#![forbid(unsafe_code)]` honored ✅
- No new external deps ✅
- Memory files OUTSIDE repo ✅
