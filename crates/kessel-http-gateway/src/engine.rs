//! Engine-apply trait + snapshot value types. Defined HERE (not in
//! `kesseldb-server`) so the dependency direction is one-way:
//! `kesseldb-server` (when built with `--features http-gateway`) depends on
//! this crate and `impl EngineApply for EngineHandle`. This crate has no
//! `kesseldb-server` dep — no cycle.

use kessel_proto::{ClientId, Op, OpResult};

/// Apply entry-point the gateway uses to dispatch into the engine.
///
/// Implemented by `kesseldb-server::EngineHandle` under
/// `#[cfg(feature = "http-gateway")]`. Trait-object friendly
/// (`Arc<dyn EngineApply>`).
pub trait EngineApply: Send + Sync + 'static {
    /// Apply a bare `Op` (the binary `/v1/op` body, post-decode).
    fn apply_op(&self, op: Op) -> OpResult;

    /// Apply a bare `Op` under a `(client_id, req_seq)` exactly-once binding.
    /// When both `X-Kessel-Client-Id` and `X-Kessel-Req-Seq` are present, the
    /// gateway routes through this entry-point; the engine's existing dedup
    /// map deduplicates retries of the same `(client_id, req_seq)`.
    fn apply_op_with_session(
        &self,
        client: ClientId,
        req: u64,
        op: Op,
    ) -> OpResult;

    /// Apply raw SQL text (the `/v1/sql` body, validated UTF-8). Wraps as
    /// `[0xFE] ++ sql_bytes` and dispatches through `apply_raw`.
    fn apply_sql(&self, sql: &str) -> OpResult;

    /// Apply raw SQL under a (client_id, req_seq) exactly-once binding.
    /// `kesseldb-server`'s impl wraps the SQL as `[0xFE] ++ sql_bytes` and
    /// routes through the engine's existing session-aware raw path (or, in
    /// V1, simply falls through to `apply_sql` if session dedup for raw-SQL
    /// frames is not yet wired — documented in spec §11 open questions).
    fn apply_sql_with_session(
        &self,
        client: ClientId,
        req: u64,
        sql: &str,
    ) -> OpResult;

    /// Snapshot of liveness state for `GET /v1/health`. Cheap — three
    /// integers + a bool — no engine apply.
    fn snapshot_health(&self) -> HealthSnapshot;

    /// Snapshot of metric counters/gauges for `GET /v1/metrics`. Cheap —
    /// atomic loads on shared `Arc<AtomicU64>` counters; no engine apply.
    fn snapshot_metrics(&self) -> MetricsSnapshot;
}

/// Liveness snapshot — see spec §7.
#[derive(Clone, Debug)]
pub struct HealthSnapshot {
    pub primary: bool,
    pub view: u64,
    pub op_number: u64,
    /// "primary" or "backup".
    pub role: &'static str,
}

/// One Op-kind counter row — see spec §6.
#[derive(Clone, Debug)]
pub struct OpKindCounter {
    pub kind: &'static str,
    pub count: u64,
}

/// Metrics snapshot — see spec §6. The op-kinds vector is the closed set of
/// `Op::kind()` values; size is bounded by construction.
#[derive(Clone, Debug)]
pub struct MetricsSnapshot {
    pub ops_total: Vec<OpKindCounter>,
    pub inflight: u64,
    pub last_op_number: u64,
    pub view_number: u64,
    pub is_primary: bool,
    /// HTTP-side counters indexed by (path, status). Path is one of the four
    /// known route strings; status is the decimal HTTP code as `&str`. Bounded
    /// cardinality.
    pub http_requests_total: Vec<HttpRequestCounter>,
}

#[derive(Clone, Debug)]
pub struct HttpRequestCounter {
    pub path: &'static str,
    pub status: &'static str,
    pub count: u64,
}
