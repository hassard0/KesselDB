//! Prometheus text-format v0.0.4 writer. Hand-rolled — no `prometheus` crate.
//! The format is a sequence of HELP + TYPE + sample lines, terminated by a
//! newline. Reference: openmetrics-spec / prometheus exposition format.

use crate::engine::MetricsSnapshot;

pub fn render(snap: &MetricsSnapshot) -> String {
    let mut s = String::with_capacity(2048);

    s.push_str("# HELP kesseldb_ops_total Number of Ops applied since process start.\n");
    s.push_str("# TYPE kesseldb_ops_total counter\n");
    for row in &snap.ops_total {
        s.push_str(&format!(
            "kesseldb_ops_total{{kind=\"{}\"}} {}\n", row.kind, row.count));
    }

    s.push_str("# HELP kesseldb_inflight Number of Ops currently in flight to the engine.\n");
    s.push_str("# TYPE kesseldb_inflight gauge\n");
    s.push_str(&format!("kesseldb_inflight {}\n", snap.inflight));

    s.push_str("# HELP kesseldb_last_op_number Highest applied op_number on this replica.\n");
    s.push_str("# TYPE kesseldb_last_op_number gauge\n");
    s.push_str(&format!("kesseldb_last_op_number {}\n", snap.last_op_number));

    s.push_str("# HELP kesseldb_view_number Current VSR view number.\n");
    s.push_str("# TYPE kesseldb_view_number gauge\n");
    s.push_str(&format!("kesseldb_view_number {}\n", snap.view_number));

    s.push_str("# HELP kesseldb_is_primary 1 if this replica is the primary in the current view.\n");
    s.push_str("# TYPE kesseldb_is_primary gauge\n");
    s.push_str(&format!("kesseldb_is_primary {}\n", if snap.is_primary { 1 } else { 0 }));

    s.push_str("# HELP kesseldb_http_requests_total HTTP gateway requests by path and status.\n");
    s.push_str("# TYPE kesseldb_http_requests_total counter\n");
    for row in &snap.http_requests_total {
        s.push_str(&format!(
            "kesseldb_http_requests_total{{path=\"{}\",status=\"{}\"}} {}\n",
            row.path, row.status, row.count,
        ));
    }

    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{HttpRequestCounter, MetricsSnapshot, OpKindCounter};

    #[test]
    fn render_empty_snapshot() {
        let snap = MetricsSnapshot {
            ops_total: Vec::new(),
            inflight: 0,
            last_op_number: 0,
            view_number: 0,
            is_primary: true,
            http_requests_total: Vec::new(),
        };
        let text = render(&snap);
        // Six HELP/TYPE blocks, all gauges/counters at 0 or absent.
        assert!(text.contains("# HELP kesseldb_ops_total"));
        assert!(text.contains("# TYPE kesseldb_ops_total counter"));
        assert!(text.contains("kesseldb_inflight 0\n"));
        assert!(text.contains("kesseldb_last_op_number 0\n"));
        assert!(text.contains("kesseldb_view_number 0\n"));
        assert!(text.contains("kesseldb_is_primary 1\n"));
        assert!(text.contains("# HELP kesseldb_http_requests_total"));
    }

    #[test]
    fn render_populated_snapshot() {
        let snap = MetricsSnapshot {
            ops_total: vec![
                OpKindCounter { kind: "applied", count: 42 },
            ],
            inflight: 7,
            last_op_number: 100,
            view_number: 3,
            is_primary: false,
            http_requests_total: vec![
                HttpRequestCounter { path: "/v1/health", status: "200", count: 5 },
            ],
        };
        let text = render(&snap);
        assert!(text.contains("kesseldb_ops_total{kind=\"applied\"} 42\n"));
        assert!(text.contains("kesseldb_inflight 7\n"));
        assert!(text.contains("kesseldb_last_op_number 100\n"));
        assert!(text.contains("kesseldb_view_number 3\n"));
        assert!(text.contains("kesseldb_is_primary 0\n"));
        assert!(text.contains("kesseldb_http_requests_total{path=\"/v1/health\",status=\"200\"} 5\n"));
    }
}
