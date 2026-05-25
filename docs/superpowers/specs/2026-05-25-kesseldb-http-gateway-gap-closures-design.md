# KesselDB — Subproject 144H: HTTP gateway gap closures

**Status:** design — approved by autonomous mandate substitution; implementation plan to follow.

**Builds on:**
- `docs/superpowers/specs/2026-05-24-kesseldb-subproject141-http-gateway.md` (SP141 HTTP gateway).
- `docs/superpowers/specs/2026-05-25-kesseldb-subproject142-http-gateway-hardening.md` (SP142 closed follow-ups #2 + #8).

**Process note.** Per `feedback_kesseldb_autonomous_build`, the brainstorm gate is substituted. The user (intermittent) explicitly authorized: "either continue implementing for completeness here or on the http wire interface gaps you haven't implemented yet". This slice closes 4 of the remaining 7 SP141 follow-ups in one focused arc. SP144H disambiguates from OBJ-2c-5 SP144 (Map + struct Parquet).

---

## 1. Problem

SP141 shipped the HTTP gateway; SP142 closed 2 of 9 documented follow-ups. Seven remain:

| # | Follow-up | Status |
|---|---|---|
| 1 | Per-`Op::kind()` counter array (vs single rolled-up "applied" counter) | this slice |
| 3 | Per-`(path, status)` HTTP request counter wired through accept loop | this slice |
| 4 | HTTP/2 / gRPC / WebSocket / Postgres-wire | non-goal (own slice) |
| 5 | HTTP/1.1 keep-alive | deferred (not in this slice) |
| 6 | `OpResult::Unauthorized` HTTP 401 disambiguation | this slice |
| 7 | `exactly_once_binding` dedicated `ParseError` variant | this slice |
| 9 | Pentest body assertions tightening | deferred (cosmetic) |

Items 1, 3, 6, 7 are small, additive, and concrete production-readiness improvements. This slice ships all four.

---

## 2. Goals and non-goals

**Goals (V1).**

- **#1**: Add `EngineHandle.op_kind_counts: Arc<[AtomicU64; 64]>` (sized for the 46 current `Op::kind()` values + headroom). Engine thread increments the right index per successful apply. `snapshot_metrics` returns one `OpKindCounter` per kind that has non-zero count (suppressing the zero rows keeps Prometheus output bounded but informative).
- **#3**: Add `Arc<HttpRequestCounters>` shared between the gateway accept loop and the metrics snapshot. After every routed request, increment `(path, status)` counter. Snapshot returns one `HttpRequestCounter` per `(path, status)` pair seen. Bounded cardinality: 4 paths × ~10 status codes = ≤40 series.
- **#6**: Disambiguate `Unauthorized` HTTP 401 between (a) the gateway's auth-layer rejection ("missing Bearer" / "wrong Bearer") and (b) `OpResult::Unauthorized` from the engine (engine ACL). Both stay 401 (status code stable), but the JSON body's `message` field differs:
  - Auth-layer: `{"status":"unauthorized","message":"bearer mismatch"}` or `{"message":"missing bearer"}`
  - Engine: `{"status":"unauthorized","message":"engine denied"}` (or whatever the engine signaled; pass through the OpResult-encoded text if any)
- **#7**: Add `ParseError::IncompleteSessionBinding` variant (currently stuffed into `BadHeaderValue(String)`). Map to 400 with `{"status":"error","message":"both X-Kessel-Client-Id and X-Kessel-Req-Seq required together"}`. The dedicated variant unlocks cleaner T5 pentest assertions (`assert_eq!(err, ParseError::IncompleteSessionBinding)` vs string-grep).

**Non-goals (named, deferred).**

- HTTP/2, WebSocket, Postgres-wire — own slice if requested.
- HTTP/1.1 keep-alive — defer; needs response-side state machine work.
- Pentest body assertions tightening — cosmetic; deferred.
- Per-Op-kind name labels for the Prometheus `kind=` label — V1 uses the kind id as the label string ("kind_1", "kind_2", etc.) to avoid binding to a particular name mapping. A follow-up can convert to human-readable names (CreateType, Update, etc.) via a const lookup table.

---

## 3. Architecture

### 3.1 Per-Op-kind counter (#1)

`EngineHandle` gains:

```rust
pub struct EngineHandle {
    // ... existing fields ...
    /// SP144H: per-Op::kind() counters. Indexed by `Op::kind() as usize`.
    /// Sized 64 for the 46 current kinds + headroom. Engine thread bumps
    /// the right index after each successful apply; out-of-range kinds
    /// (if a new variant is added without resizing) overflow into a
    /// catch-all index 0 (which also doubles as "kind unknown").
    op_kind_counts: Arc<[AtomicU64; 64]>,
}
```

`spawn_engine_cfg` initializes; the engine thread closure clones the Arc and, in the same place SP142 publishes `applied_ops_atomic`, ALSO publishes per-kind: decode the frame's first byte to determine the Op kind (or call `Op::decode(frame).map(|op| op.kind())` if cheap), then `op_kind_counts[kind as usize].fetch_add(1, AcqRel)`.

Decoding the Op in the engine's group-commit loop ISN'T cheap (Op::decode allocates). Better: have the SM tell us the kind cheaply. Look for an existing way to determine kind without full Op::decode — e.g. `frame.first()` for the tag byte (the bare-Op frames have the kind encoded as the first byte per kessel-proto::Op::encode). For SQL frames (`[0xFE]++SQL`), use a dedicated "sql" pseudo-kind = 0xFE (overlapping with the headroom slot).

Implementation simpler:

```rust
// In the engine thread, after compute() returns OpResult::Ok-ish:
if let Some(&tag) = frame.first() {
    let idx = tag as usize;
    if idx < 64 {
        op_kind_counts[idx].fetch_add(1, AcqRel);
    }
}
```

This treats `0xFE` (SQL) and `0xFC` (token-auth — already filtered by handle_conn but defensive) as their own buckets. Kind 0 is reserved for "unknown / unmapped".

`snapshot_metrics` iterates all 64 slots, emitting only rows with count > 0. Bounded cardinality: ≤46 series.

For Prometheus labels, use the tag byte as a stringified id: `kind="1"`, `kind="2"`, etc. Plus `kind="254"` for SQL. (Human-readable names are a follow-up.)

### 3.2 Per-(path, status) HTTP counter (#3)

Add to `kessel-http-gateway::engine`:

```rust
pub struct HttpRequestCounters {
    /// SP144H: per-(path, status) counter. Path is one of the 4 known paths;
    /// status is the decimal HTTP code as a string.
    counters: dashmap_free_map_via_mutex,  // see below
}
```

We need a concurrent map without external deps. Use `Arc<Mutex<HashMap<(&'static str, u16), AtomicU64>>>` or pre-allocate the full cross-product:

**BOLD design**: pre-allocate a 4×16 dense matrix (4 known paths × 16 status code buckets covering 200/400/401/404/405/411/413/414/415/417/429/500/503/+spare). This is 64 atomics = 512 bytes shared. Pass via `Arc<HttpRequestCountersStatic>` into `serve()`:

```rust
pub struct HttpRequestCountersStatic {
    /// Indexed [path_idx][status_idx].
    counts: [[AtomicU64; 16]; 4],
}

impl HttpRequestCountersStatic {
    pub fn new() -> Self { /* default-zero */ }
    pub fn bump(&self, path: &str, status: u16) {
        let pi = path_idx(path);  // 0..=3 or skip
        let si = status_idx(status);  // 0..=15 or skip
        self.counts[pi][si].fetch_add(1, AcqRel);
    }
    pub fn snapshot(&self) -> Vec<HttpRequestCounter> {
        // Iterate 4×16, emit non-zero rows
    }
}

fn path_idx(p: &str) -> usize {
    match p { "/v1/sql" => 0, "/v1/op" => 1, "/v1/health" => 2, "/v1/metrics" => 3, _ => 0 /* drop */ }
}

fn status_idx(s: u16) -> usize {
    match s {
        200 => 0, 400 => 1, 401 => 2, 404 => 3, 405 => 4, 411 => 5,
        413 => 6, 414 => 7, 415 => 8, 417 => 9, 429 => 10, 500 => 11,
        503 => 12, _ => 15,
    }
}
```

The `serve()` signature gains a counters parameter; `kesseldb-server::serve_cfg` constructs the Arc and threads it. Routes call `counters.bump(path, status_code)` after writing the response.

`EngineApply::snapshot_metrics`'s `http_requests_total` field is populated from this counters snapshot.

### 3.3 Unauthorized 401 disambiguation (#6)

In `routes::handle`:

```rust
if let Some(expected) = token {
    match extract_bearer(&req.headers) {
        Ok(Some(given)) => {
            if !ct_eq(given, expected) {
                return write_error_json(w, (401, "Unauthorized"),
                    "unauthorized", "bearer mismatch");
            }
        }
        Ok(None) => {
            return write_error_json(w, (401, "Unauthorized"),
                "unauthorized", "missing bearer");
        }
        // ... etc
    }
}
```

`write_op_result` for `OpResult::Unauthorized`:

```rust
OpResult::Unauthorized => write_error_json(w, (401, "Unauthorized"),
    "unauthorized", "engine denied"),
```

JSON body now distinguishes:
- `{"status":"unauthorized","message":"bearer mismatch"}` — wrong Bearer at gateway
- `{"status":"unauthorized","message":"missing bearer"}` — token-mode but no Bearer
- `{"status":"unauthorized","message":"engine denied"}` — engine returned Unauthorized

`format_result_json` in `kessel-client` is UNCHANGED (the gateway maps via its own helper, not the locked client contract). The JSON contract stays additive.

### 3.4 `IncompleteSessionBinding` ParseError variant (#7)

In `parse.rs`:

```rust
pub enum ParseError {
    // ... existing ...
    /// SP144H: Both `X-Kessel-Client-Id` and `X-Kessel-Req-Seq` headers
    /// are required together (both-or-neither). One present without the
    /// other is rejected with this variant.
    IncompleteSessionBinding,
}
```

`routes::exactly_once_binding`:

```rust
_ => Err(ParseError::IncompleteSessionBinding),
```

`server::write_parse_error`:

```rust
ParseError::IncompleteSessionBinding =>
    ((400, "Bad Request"), "error",
     "both X-Kessel-Client-Id and X-Kessel-Req-Seq required together".into()),
```

T10 pentest `pentest_client_id_alone_400` keeps its 400-only assertion (the variant change is internal), but a unit-level KAT can pin the specific variant.

---

## 4. Test plan

Pre-SP144H baseline: 976 default / 1003 featured (post-SP143).

Expected DELTA:
- T1 per-Op-kind: +1-2 (engine-thread bump test + snapshot reads N kinds)
- T2 per-(path, status): +1-2 (counter bump test + snapshot reads via /v1/metrics e2e)
- T3 Unauthorized disambig: +1 (e2e check different messages for missing-bearer vs wrong-bearer)
- T4 IncompleteSessionBinding: +1 (KAT pinning variant)
- T5 docs: +0

Sum: ~6 tests. Honest reconciliation in T5 — measure actual.

Determinism gate every task:
- `cargo test --workspace --release` FAILED=0
- seed-7 GREEN
- Default `cargo tree -p kesseldb-server` empty for HTTP/gateway regex
- All SP140-SP143 oracles green untouched

---

## 5. Task decomposition

- **T0**: Baseline (already known: 976/1003).
- **T1**: `EngineHandle.op_kind_counts: Arc<[AtomicU64; 64]>` + tag-byte indexing in engine thread + `pub fn op_kind_count_snapshot() -> Vec<OpKindCounter>` accessor + `EngineApply::snapshot_metrics` reads it. +1-2 tests.
- **T2**: `HttpRequestCountersStatic` shared struct + `serve()`/`serve_tls()` gain counters Arc parameter + routes bump after writing + `kesseldb-server`'s `EngineApply::snapshot_metrics` reads counters Arc snapshot. +1-2 tests.
- **T3**: 3-way Unauthorized disambig in `routes::handle` + `write_op_result`. e2e test asserting message field varies.
- **T4**: `ParseError::IncompleteSessionBinding` + `exactly_once_binding` return + `write_parse_error` arm + KAT pinning variant.
- **T5**: Docs slice — STATUS row, SP144H internal record, SP141 internal record marks #1, #3, #6, #7 CLOSED, memory.

---

## 6. Acceptance criteria

1. `EngineHandle.op_kind_counts` field populated by engine thread; `op_kind_count_snapshot()` returns non-zero rows.
2. `snapshot_metrics`'s `ops_total` returns per-kind counts (not just rolled-up "applied").
3. `serve()` accepts an `Arc<HttpRequestCountersStatic>` parameter; routes bump after every response; metrics snapshot includes per-(path, status) counters.
4. Gateway 401 JSON body distinguishes auth-layer reject ("bearer mismatch" / "missing bearer") from engine reject ("engine denied").
5. `ParseError::IncompleteSessionBinding` variant exists; `exactly_once_binding` returns it; `write_parse_error` maps to 400 with the documented message.
6. Workspace 976 → 982ish default; 1003 → 1009ish featured (real measured DELTA).
7. seed-7 GREEN.
8. Default `cargo tree -p kesseldb-server` empty for HTTP/gateway regex.
9. SP141 internal record updated: follow-ups #1, #3, #6, #7 marked CLOSED with backlinks to SP144H. Remaining open: #4 (HTTP/2/etc.), #5 (keep-alive), #9 (pentest tightening).
