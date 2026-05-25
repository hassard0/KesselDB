# KesselDB — Subproject 142: HTTP gateway hardening pass

**Status:** done — code + tests committed and passing.

**Builds on:**
- `docs/superpowers/specs/2026-05-25-kesseldb-http-gateway-hardening-design.md` (SP142 design spec).
- `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md` (SP141 — the HTTP gateway this slice hardens; closes follow-ups #2 and #8).

---

## Outcome

Closed two of the nine SP141 known follow-ups:

**Follow-up #2 (snapshot_metrics counter-reset under saturation) → CLOSED.**
- Added `EngineHandle.applied_ops_atomic: Arc<AtomicU64>` field, populated by the engine thread via a delta-publish at the group-commit loop boundary (`n_before = n; compute(); fetch_add(n - n_before)` — captures all three `*n += 1` sites including the SQL UPDATE RMW GetById bump, mathematically equivalent to per-site bump but robust against future apply-path changes).
- Added `pub fn applied_ops_snapshot(&self) -> u64` direct-read accessor.
- `impl EngineApply for EngineHandle::snapshot_metrics` and `snapshot_health` now read the atomic directly — no `apply_raw` round-trip, immune to backpressure. The `kessel-http-gateway::engine.rs` trait doc's promise of "atomic loads, no engine apply" is now truthful.
- New unit test `applied_ops_snapshot_increments_on_apply` with strict equality assertion (`after_two == after_one + 1`) catches both under- and over-counting.

**Follow-up #8 (e2e spawn_server 150ms sleep flakiness) → CLOSED.**
- `wait_for_listener` connect-retry loop: 50 iterations × 10ms with 50ms per-attempt `connect_timeout`, panics on cap (500ms+ total) instead of hanging.
- Adaptive: returns on first successful TCP connect (~1-5ms on healthy machines), tolerant of slow CI runners (up to ~500ms).
- ~20× speedup on the pentest suite (~3s → 0.16s for the 17 pentests in aggregate).

Binary wire byte-identical. Default `cargo build -p kesseldb-server` byte-identical to SP141 ship. `cargo tree -p kesseldb-server` (no features) empty for HTTP/gateway crates.

---

## Gate reconciliation (honest)

- Before (SP141 ship): 931 PASSED / 0 FAILED default; 958 / 0 with `--features kessel-http-gateway/test-server`.
- After SP142 T3 (measured): **932** PASSED / 0 / 0 default; **959** PASSED / 0 / 0 featured. **+1** (the new `applied_ops_snapshot_increments_on_apply` unit test).
- Per-slice delta:
  - T0 baseline: +0
  - T1 atomic accessor + EngineApply impl + unit test: +1
  - T2 wait_for_listener (behavior-only): +0
  - T3 docs (this task): +0
  - Sum: +1 ✓
- Pentest-suite wall-clock (informational, not gated): ~3s → ~0.16s.
- `cargo tree -p kesseldb-server --no-default-features | grep -E "hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp|kessel-http-gateway"`: empty.
- `cargo build -p kesseldb-server` (no features) byte-identical to SP141 ship.
- `kessel-vsr::large_seed_corpus_is_deterministic_and_converges`: GREEN.
- Existing `stats_and_snapshot_are_consistent_and_recoverable`: GREEN (the STATS_TAG path is unchanged; we added a parallel accessor, not replaced).
- All 7 Parquet pyarrow e2e oracles, 2 external-source, 1 TLS, 1 objstore, 17 pentest, 8 e2e, 2 metrics_e2e oracles: green untouched.

---

## Remaining SP141 follow-ups (still open)

After SP142 closes #2 and #8, seven SP141 follow-ups remain:

1. Per-`Op::kind()` counter array on `EngineHandle`
3. Per-`(path, status)` HTTP request counter wired through accept loop
4. HTTP/2 / gRPC / WebSocket / SSE / PostgreSQL wire compat
5. HTTP/1.1 keep-alive on the gateway
6. `OpResult::Unauthorized` HTTP 401 disambiguation
7. `exactly_once_binding` dedicated `ParseError` variant
9. Pentest body-text assertions tightening

Each is non-blocking; the gateway is production-ready after SP142.

---

## Cross-links

- STATUS row: `docs/STATUS.md` (SP142 row, after SP141).
- SP141 internal record: `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md` (follow-ups #2 and #8 now marked CLOSED with backlinks here).
- Design spec: `docs/superpowers/specs/2026-05-25-kesseldb-http-gateway-hardening-design.md`.
- Memory: `memory/project_kesseldb.md` (SP142 block) + `MEMORY.md` (KesselDB line).
