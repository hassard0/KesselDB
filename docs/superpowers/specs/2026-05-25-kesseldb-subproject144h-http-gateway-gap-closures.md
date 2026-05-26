# KesselDB — Subproject 144H: HTTP gateway gap closures

**Status:** done — code + tests committed and passing.

**Builds on:**
- `docs/superpowers/specs/2026-05-25-kesseldb-http-gateway-gap-closures-design.md` (SP144H design spec).
- `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md` (SP141 — the HTTP gateway this slice hardens).
- `docs/superpowers/specs/2026-05-25-kesseldb-subproject142-http-gateway-hardening.md` (SP142 — closed follow-ups #2 + #8).

(Note: SP144H is named with the "H" suffix to disambiguate from OBJ-2c-5 SP144 — the next Parquet nested-decode slice covering Map + struct columns.)

---

## Outcome

Closed 4 of the 7 remaining SP141 follow-ups in one focused arc:

**Follow-up #1 (per-Op-kind counter array) → CLOSED.**
- `EngineHandle.op_kind_counts: Arc<[AtomicU64; 64]>` — sized for the 46 current `Op::kind()` values + headroom for special tags (0xFE SQL, etc.).
- `pub fn op_kind_counts_snapshot(&self) -> Vec<(u8, u64)>` — returns non-zero rows only (bounded by ≤46 active kinds).
- Engine thread bumps via tag-byte indexing alongside the existing `applied_ops_atomic` publish, gated on the same `n > n_before` group-commit boundary (so STATS_TAG / SNAPSHOT_TAG / pipeline-control frames don't bump).
- `impl EngineApply for EngineHandle::snapshot_metrics` emits one `OpKindCounter` per non-zero tag (label `kind_<tag>`) plus the rolled-up `kind="applied"` row for backward compat with existing Prometheus dashboards.

**Follow-up #3 (per-(path, status) HTTP request counter) → CLOSED.**
- `HttpRequestCountersStatic` — 4×16 dense atomic matrix (paths: `/v1/sql`, `/v1/op`, `/v1/health`, `/v1/metrics`; statuses: 13 mapped codes + 3 spare). Bounded ≤64 atomics (512 B).
- `serve()` and `serve_tls()` plumb `Arc<HttpRequestCountersStatic>` through `handle_one*`.
- `routes::handle` and all 4 route handlers + `write_op_result` use new `write_*_counted` helpers in `response.rs` (atomic bump after response write).
- `EngineHandle.http_counters` field (cfg-gated to `http-gateway`) — same Arc shared between accept loop (writer) and `snapshot_metrics` (reader).
- `MetricsSnapshot.http_requests_total` populated with non-zero rows only.

**Follow-up #6 (Unauthorized 401 disambiguation) → CLOSED.**
- `routes::handle` auth-layer rejects emit `{"status":"unauthorized","message":"missing bearer"}` or `{"status":"unauthorized","message":"bearer mismatch"}` depending on cause.
- `write_op_result` engine-layer `OpResult::Unauthorized` emits `{"status":"unauthorized","message":"engine denied"}`.
- HTTP status stable at 401 in all cases (status code is the operator-facing signal; message disambiguates for debugging).
- Side effect: `OpResult::Unavailable` body also changed from `{"status":"unavailable"}` → `{"status":"unavailable","message":"engine unavailable"}` (HTTP 503 unchanged).
- `kessel_client::format_result_json` UNCHANGED — the gateway uses its own helper for the disambig path.

**Follow-up #7 (IncompleteSessionBinding ParseError variant) → CLOSED.**
- Dedicated `ParseError::IncompleteSessionBinding` variant added.
- `routes::exactly_once_binding` returns this variant instead of `BadHeaderValue(String)`.
- `server::write_parse_error` maps to (400, "Bad Request") with the documented message text.
- Tests now assert on the variant (`assert_eq!(err, ParseError::IncompleteSessionBinding)`) instead of grepping the message string — cleaner T5/T10 pentest assertions.

---

## Gate reconciliation (honest)

- Before (SP143 ship): 976 PASSED / 0 / 0 default; 1003 / 0 / 0 featured.
- After SP144H T5 (measured): **978** PASSED / 0 / 0 default (+2); **1007** PASSED / 0 / 0 featured (+4).
- Per-slice delta:
  - T1 per-Op-kind: +1 unit test
  - T2 per-(path, status): +1 e2e test (behind test-server feature, so +0 default / +1 featured)
  - T3+T4 disambig + variant: +1 e2e test (behind test-server, so +0 default / +1 featured) + 1 KAT (default, so +1 default / +1 featured)
  - T5 docs: +0
  - Sum default: 1+0+1+0 = +2 ✓
  - Sum featured: 1+1+1+1+0 = +4 ✓
- `cargo tree -p kesseldb-server --no-default-features | grep -E "hyper|httparse|h2|tokio|mio|socket2|axum|actix|warp|kessel-http-gateway"`: empty.
- `cargo build -p kesseldb-server` (no features) byte-identical to SP143 ship.
- `kessel-vsr::large_seed_corpus_is_deterministic_and_converges`: GREEN.
- All SP140-SP143 oracles (7 Parquet pyarrow, 5 Parquet nested-list pyarrow, 2 external-source, 1 TLS, 1 objstore, 17 pentest, 8 SP141 e2e, 2 SP141 metrics e2e, 14 SP143 pentest, 3 SP143 inline-nested roundtrip): green untouched.

---

## Remaining SP141 follow-ups (3 still open)

After SP144H closes #1, #3, #6, #7 (and SP142 closed #2 + #8), only three SP141 follow-ups remain:

- **#4**: HTTP/2 / gRPC / WebSocket / SSE / PostgreSQL wire compat — non-goal of SP141; own slice if a real consumer asks.
- **#5**: HTTP/1.1 keep-alive on the gateway — V1 always sends `Connection: close`. Needs response-side state machine work; own slice.
- **#9**: Pentest body-text assertions tightening — cosmetic hardening; small slice.

All three are non-blocking; the gateway is production-ready post-SP144H. The just-shipped per-Op-kind + per-(path,status) counters meaningfully improve operator observability of the production HTTP gateway.

---

## Cross-links

- STATUS row: `docs/STATUS.md` (SP144H row, after SP143).
- SP141 internal record: `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md` (follow-ups #1, #3, #6, #7 now marked CLOSED with backlinks here).
- Design spec: `docs/superpowers/specs/2026-05-25-kesseldb-http-gateway-gap-closures-design.md`.
- Memory: `memory/project_kesseldb.md` (SP144H block) + `MEMORY.md` (KesselDB line).
