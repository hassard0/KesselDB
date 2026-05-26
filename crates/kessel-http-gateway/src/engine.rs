//! Engine-apply trait + snapshot value types. Defined HERE (not in
//! `kesseldb-server`) so the dependency direction is one-way:
//! `kesseldb-server` (when built with `--features http-gateway`) depends on
//! this crate and `impl EngineApply for EngineHandle`. This crate has no
//! `kesseldb-server` dep — no cycle.

use kessel_proto::{ClientId, Op, OpResult};
use std::sync::atomic::{AtomicU64, Ordering};

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

/// SP144H T2: dense 4×16 atomic-counter matrix indexed by (path, status).
///
/// - Path dimension: the 4 known gateway routes (`/v1/sql`, `/v1/op`,
///   `/v1/health`, `/v1/metrics`). Out-of-set paths are dropped (no
///   bump) — they can't reach the bump call anyway (routes::handle
///   already 404s unknown paths before the bump).
/// - Status dimension: 16 buckets covering 200/400/401/404/405/411/413/
///   414/415/417/429/500/503/+3 spare slots for future status codes
///   (bumps to unmapped statuses fall into slot 15).
///
/// Bounded cardinality: ≤ 4×16 = 64 atomic counters (512 B). Same shape
/// as `op_kind_counts` (SP144H T1). Constructed once per server start
/// and shared via Arc between the gateway accept loop and the metrics
/// snapshot path.
pub struct HttpRequestCountersStatic {
    counts: [[AtomicU64; 16]; 4],
}

impl HttpRequestCountersStatic {
    pub fn new() -> Self {
        Self {
            counts: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
        }
    }

    pub fn bump(&self, path: &str, status: u16) {
        let pi = path_idx(path);
        let si = status_idx(status);
        self.counts[pi][si].fetch_add(1, Ordering::AcqRel);
    }

    pub fn snapshot(&self) -> Vec<HttpRequestCounter> {
        let mut out = Vec::new();
        for (pi, row) in self.counts.iter().enumerate() {
            let path = path_label(pi);
            for (si, slot) in row.iter().enumerate() {
                let count = slot.load(Ordering::Acquire);
                if count > 0 {
                    out.push(HttpRequestCounter {
                        path,
                        status: status_label(si),
                        count,
                    });
                }
            }
        }
        out
    }
}

impl Default for HttpRequestCountersStatic {
    fn default() -> Self { Self::new() }
}

fn path_idx(p: &str) -> usize {
    match p {
        "/v1/sql" => 0,
        "/v1/op" => 1,
        "/v1/health" => 2,
        "/v1/metrics" => 3,
        // Unknown path: drop into the /v1/sql bucket as a defensive default
        // (can't actually happen — routes::handle 404s before reaching bump).
        _ => 0,
    }
}

fn path_label(i: usize) -> &'static str {
    match i {
        0 => "/v1/sql",
        1 => "/v1/op",
        2 => "/v1/health",
        3 => "/v1/metrics",
        _ => "/v1/sql",
    }
}

fn status_idx(s: u16) -> usize {
    match s {
        200 => 0, 400 => 1, 401 => 2, 404 => 3, 405 => 4, 411 => 5,
        413 => 6, 414 => 7, 415 => 8, 417 => 9, 429 => 10, 500 => 11,
        503 => 12,
        _ => 15,  // unknown status → spare slot
    }
}

fn status_label(i: usize) -> &'static str {
    match i {
        0 => "200",  1 => "400",  2 => "401",  3 => "404",
        4 => "405",  5 => "411",  6 => "413",  7 => "414",
        8 => "415",  9 => "417", 10 => "429", 11 => "500",
        12 => "503",
        _ => "other",
    }
}
