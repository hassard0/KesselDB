# KesselDB — Subproject 142: HTTP gateway hardening pass

**Status:** design — approved by autonomous mandate substitution; implementation plan to follow.

**Builds on:** SP141 (`docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md`) — closes two of the nine documented known follow-ups.

**Process note (autonomous mandate substitution).** Per `feedback_kesseldb_autonomous_build` and the just-shipped SP141 internal record's "Known follow-ups" section (which already documents the design path for both issues fixed here), the brainstorming user-review gate is substituted by this spec. The two-stage spec-then-quality subagent review gate during implementation is retained.

---

## 1. Problem

SP141 shipped the opt-in HTTP/1.1 gateway. The final whole-implementation reviewer flagged 9 known follow-ups, all hardening/observability rather than correctness gaps — except two that DO affect production behavior:

1. **`snapshot_metrics` round-trips through the engine** via `STATS_TAG` (`self.stats()`). Under engine saturation (`inflight >= max_inflight`), `apply_raw` returns `OpResult::Unavailable` and the stats decode falls through to `ServerStats { applied_ops: 0, digest: 0, uptime_secs: 0 }`. The Prometheus metrics endpoint then emits `kesseldb_last_op_number 0` and `kesseldb_ops_total{kind="applied"} 0`. Prometheus interprets `counter` values that decrease as **counter resets** — the `rate()` function returns a transient spike or negative value, and grafana dashboards show wrong ops/sec. The trait doc in `crates/kessel-http-gateway/src/engine.rs` already promises "Cheap — atomic loads on shared `Arc<AtomicU64>` counters; no engine apply." — but the impl violates that promise.

2. **e2e `spawn_server` uses a 150ms `thread::sleep`** rather than a connect-retry loop. SP141 T5 added 17 pentest tests that each call `spawn_server`, multiplying the cumulative sleep budget to ~2.5s per test run. On a loaded CI runner that schedules the gateway listener thread late, the connection probe may race the bind and produce flakes (`Connection refused`).

Both fixes are small and additive; neither changes the gateway's public API surface.

---

## 2. Goals and non-goals

**Goals (V1).**

- Add an `Arc<AtomicU64>` for `applied_ops` to `EngineHandle`, populated atomically from the engine thread on every successful apply.
- Expose a new `pub fn applied_ops_snapshot(&self) -> u64` accessor on `EngineHandle` that reads the atomic directly — no `apply_raw` round-trip.
- Update `impl EngineApply for EngineHandle::snapshot_metrics` and `snapshot_health` to use the new direct accessor instead of `self.stats()`. The `kessel_http_gateway::engine.rs` trait doc's promise becomes truthful.
- Replace the 150ms `thread::sleep` in `crates/kessel-http-gateway/tests/common/mod.rs::spawn_server*` with a connect-retry loop (max 50 attempts × 10ms = 500ms cap, returning immediately on first success).
- Document both fixes in the SP142 internal record and link from STATUS.md / SP141 internal record's follow-ups list (mark items 2 and 8 as CLOSED).

**Non-goals (named, deferred — remain SP141 follow-ups).**

- Per-`Op::kind()` counter array (SP141 follow-up #1) — requires the per-kind atomic counter array on the engine. Defer to a dedicated slice.
- Per-`(path, status)` HTTP request counter wired through accept loop (SP141 #3) — defer.
- HTTP/2, WebSocket, PostgreSQL wire compat (SP141 #4) — design spec §2 non-goals.
- HTTP/1.1 keep-alive (SP141 #5) — defer.
- `OpResult::Unauthorized` HTTP 401 disambiguation (SP141 #6) — defer; needs JSON contract change.
- `exactly_once_binding` dedicated variant (SP141 #7) — cosmetic; defer.
- Tighter pentest body assertions (SP141 #9) — cosmetic; defer.

---

## 3. Architecture

### 3.1 Engine-side atomic counter

`EngineHandle` (in `crates/kesseldb-server/src/lib.rs:404-460`) gains a new field:

```rust
pub struct EngineHandle {
    tx: Sender<EngineMsg>,
    inflight: Arc<AtomicUsize>,
    max_inflight: usize,
    /// SP142: direct-read counter for /v1/metrics — populated atomically
    /// from the engine thread on every applied op. Avoids the STATS_TAG
    /// round-trip in snapshot_metrics/snapshot_health, which would return
    /// 0 under engine saturation (Prometheus counter-reset).
    applied_ops_atomic: Arc<AtomicU64>,
}
```

`spawn_engine_cfg` (currently around line 467-720) initializes the new field with `Arc::new(AtomicU64::new(0))`. The engine thread closure captures a clone; immediately after `let r = sm.apply(*n, op)` (or wherever the op count increments — find the `*n` increment site), the atomic is bumped:

```rust
applied_ops_atomic.fetch_add(1, Ordering::AcqRel);
```

The atomic is incremented AFTER the apply completes, so a partial-apply (engine panic mid-frame, recovered) does not over-count.

The existing `STATS_TAG` round-trip stays as-is (the `stats()` method is still used by the `stats_and_snapshot_are_consistent_and_recoverable` test and possibly external consumers). We add a parallel accessor — we do NOT remove `stats()`.

### 3.2 Direct accessor

```rust
impl EngineHandle {
    /// SP142: direct atomic read of the applied-op count. Cheap — no
    /// engine round-trip, immune to backpressure. Use this for
    /// observability paths (`/v1/metrics`, `/v1/health`).
    pub fn applied_ops_snapshot(&self) -> u64 {
        self.applied_ops_atomic.load(Ordering::Acquire)
    }
}
```

### 3.3 `EngineApply` impl updates

The `impl EngineApply for EngineHandle` block (under `#[cfg(feature = "http-gateway")]`) is updated:

```rust
    fn snapshot_health(&self) -> kessel_http_gateway::HealthSnapshot {
        kessel_http_gateway::HealthSnapshot {
            primary: true,
            view: 0,
            op_number: self.applied_ops_snapshot(),  // direct atomic
            role: "primary",
        }
    }
    fn snapshot_metrics(&self) -> kessel_http_gateway::MetricsSnapshot {
        let ops = self.applied_ops_snapshot();
        kessel_http_gateway::MetricsSnapshot {
            ops_total: vec![
                kessel_http_gateway::OpKindCounter {
                    kind: "applied",
                    count: ops,
                },
            ],
            inflight: self.inflight_snapshot(),
            last_op_number: ops,
            view_number: 0,
            is_primary: true,
            http_requests_total: Vec::new(),
        }
    }
```

Behavior under saturation now: `applied_ops_snapshot()` always returns the real current count (atomic load is lock-free); Prometheus sees a monotonic counter.

### 3.4 e2e `spawn_server` connect-retry loop

Replace the 150ms sleep in `crates/kessel-http-gateway/tests/common/mod.rs::spawn_server_with_token`:

```rust
// Before:
std::thread::sleep(std::time::Duration::from_millis(150));

// After:
wait_for_listener(http_addr);
(http_addr, guard)
```

with a new helper:

```rust
/// Poll TcpStream::connect_timeout up to 50 times at 10ms intervals
/// (500ms cap). Returns on first success; panics on timeout.
fn wait_for_listener(addr: std::net::SocketAddr) {
    for _ in 0..50 {
        if std::net::TcpStream::connect_timeout(
            &addr,
            std::time::Duration::from_millis(50),
        ).is_ok() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("gateway listener never bound: {addr}");
}
```

On a fast machine: first iteration succeeds in ~1-5ms — total wait ~5ms (30× faster than 150ms sleep). On a slow CI runner that takes 200ms to bind: 20 iterations × 10ms = 200ms — still passes but adapts. The 500ms cap is generous enough to never spuriously fail on production CI, but it caps panic-instead-of-hang behavior if the gateway thread crashes during bind.

### 3.5 Test updates

The atomic counter is already populated correctly per the engine thread's `*n` semantics. Verify with one new test in `crates/kesseldb-server/src/lib.rs` (or extend an existing one):

```rust
#[test]
fn applied_ops_snapshot_increments_on_apply() {
    let dir = tempdir().unwrap();
    let engine = spawn_engine(dir.path()).unwrap();
    assert_eq!(engine.applied_ops_snapshot(), 0);
    let _ = engine.apply(Op::Ping);  // or any benign op
    assert_eq!(engine.applied_ops_snapshot(), 1);
    let _ = engine.apply(Op::Ping);
    assert_eq!(engine.applied_ops_snapshot(), 2);
}
```

(Use the actual `Op` variant the test infrastructure already uses. If `Op::Ping` doesn't exist, use whatever idempotent op the existing `stats_and_snapshot_are_consistent_and_recoverable` test uses.)

Update the SP141 metrics_e2e `metrics_counter_monotonic_under_load` test to verify the counter increments after a real op (it already does via `CREATE TABLE`, but tighten the assertion from `>=` to `>` since the atomic path is now race-free).

---

## 4. What stays UNCHANGED (V1 invariants)

- Binary wire byte-identical (no change to `handle_conn`, the `[0xFE]++SQL` frame format, the `Op::encode()` round-trip, or any pipelined-batch semantics).
- Default `cargo build -p kesseldb-server` byte-identical to SP141 ship (the new atomic counter is an additive field on a struct already in the binary; no new linked crate; no new feature flag).
- `cargo tree -p kesseldb-server --no-default-features` empty for HTTP/gateway regex.
- Workspace test count rises by +1 (the new atomic-increment unit test); featured count rises by 0 (the metrics_e2e tightening replaces an existing assertion, no new test count).
- `ServerStats` struct unchanged. The STATS_TAG path through `apply_raw` is unchanged. Existing `stats_and_snapshot_are_consistent_and_recoverable` test stays green untouched.
- All 7 Parquet pyarrow e2e oracles, the 2 external-source oracles, the TLS oracle, the objstore oracle, the 17-row pentest matrix, all 8 e2e tests — green untouched.
- seed-7 (`kessel-vsr::large_seed_corpus_is_deterministic_and_converges`) GREEN.

---

## 5. Out of scope (named, deferred)

The 7 remaining SP141 follow-ups (per the SP141 internal record §"Known follow-ups"):

- #1 Per-`Op::kind()` counter array
- #3 Per-`(path, status)` HTTP request counter
- #4 HTTP/2 / WebSocket / PostgreSQL wire compat
- #5 HTTP/1.1 keep-alive
- #6 `OpResult::Unauthorized` 401 disambiguation
- #7 `exactly_once_binding` dedicated variant
- #9 Pentest body assertions tightening

Each is a candidate for a future dedicated slice. None block production use of the gateway after SP142 ships.

---

## 6. Test plan

T0 baseline: 931 default / 958 featured (from SP141 ship).

T-slices:
- **T0:** Determinism baseline (record measured workspace count, seed-7, tree-grep).
- **T1:** Add `applied_ops_atomic` field + accessor + engine-thread increment + unit test. Update `EngineApply` impl to use direct accessor.
- **T2:** Replace e2e `spawn_server` 150ms sleep with `wait_for_listener` connect-retry loop. Run the full e2e + pentest + metrics_e2e suite to verify no regressions.
- **T3:** Docs slice — SP142 internal record + STATUS.md row + update SP141 internal record's follow-up list to mark #2 and #8 as CLOSED + memory.

Determinism gate every task:
- `cargo test --workspace --release` FAILED=0; count = 931 baseline + 1 (T1 atomic test) = 932 by T3
- `cargo test --workspace --release --features kessel-http-gateway/test-server` FAILED=0; count = 958 + 1 = 959 (same +1 unit test propagates)
- seed-7 GREEN
- Default tree-grep EMPTY

---

## 7. Acceptance criteria

1. `EngineHandle.applied_ops_atomic: Arc<AtomicU64>` field present, populated by engine thread.
2. `EngineHandle::applied_ops_snapshot(&self) -> u64` is `pub` and returns the atomic load.
3. `impl EngineApply for EngineHandle::snapshot_metrics` and `snapshot_health` no longer call `self.stats()` — both read `applied_ops_snapshot()` directly.
4. e2e `spawn_server*` uses connect-retry loop (max 50 × 10ms = 500ms cap), no `thread::sleep(150ms)`.
5. New unit test `applied_ops_snapshot_increments_on_apply` green.
6. Workspace 932/0/0 default, 959/0/0 featured. (+1 from baseline.)
7. seed-7 GREEN.
8. Default `cargo tree -p kesseldb-server --no-default-features` empty for HTTP/gateway regex.
9. Default `cargo build -p kesseldb-server` byte-identical to SP141 ship (no new linked crates).
10. SP141 internal record updated to mark follow-ups #2 and #8 as CLOSED with backlinks to SP142.
11. STATUS.md row added after SP141.
