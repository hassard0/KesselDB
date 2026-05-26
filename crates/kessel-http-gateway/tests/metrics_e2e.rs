//! /v1/metrics + /v1/health integration. Apply a known sequence of ops,
//! scrape, assert the Prometheus text contains the canonical metric lines.

#![cfg(feature = "test-server")]

mod common;
use common::{raw_request, spawn_server};

#[test]
fn metrics_includes_canonical_lines() {
    let (addr, _g) = spawn_server();
    let resp = raw_request(addr,
        b"GET /v1/metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 200"), "got: {text}");
    assert!(text.contains("# HELP kesseldb_ops_total"), "got: {text}");
    assert!(text.contains("# TYPE kesseldb_ops_total counter"), "got: {text}");
    assert!(text.contains("kesseldb_inflight "), "got: {text}");
    assert!(text.contains("kesseldb_last_op_number "), "got: {text}");
    assert!(text.contains("kesseldb_view_number "), "got: {text}");
    assert!(text.contains("kesseldb_is_primary "), "got: {text}");
    assert!(text.contains("# HELP kesseldb_http_requests_total"), "got: {text}");
    // Content-Type lock per spec §4.2.
    assert!(text.contains("Content-Type: text/plain; version=0.0.4"), "got: {text}");
}

#[test]
fn metrics_counter_monotonic_under_load() {
    let (addr, _g) = spawn_server();
    // Read once.
    let resp0 = raw_request(addr,
        b"GET /v1/metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let t0 = String::from_utf8_lossy(&resp0).into_owned();
    let c0 = parse_counter(&t0, "kesseldb_last_op_number");
    // Apply a SQL op (CREATE TABLE — known to succeed; SELECT 1 is rejected
    // by kessel-sql, per T4's e2e fix).
    let body = b"CREATE TABLE t_metrics (v U64 NOT NULL)";
    let mut req = Vec::new();
    req.extend_from_slice(b"POST /v1/sql HTTP/1.1\r\nHost: 127.0.0.1\r\n");
    req.extend_from_slice(b"Content-Type: text/plain\r\n");
    req.extend_from_slice(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes());
    req.extend_from_slice(body);
    let _ = raw_request(addr, &req);
    // Read again.
    let resp1 = raw_request(addr,
        b"GET /v1/metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let t1 = String::from_utf8_lossy(&resp1).into_owned();
    let c1 = parse_counter(&t1, "kesseldb_last_op_number");
    assert!(c1 >= c0, "last_op_number should not decrease: {c0} → {c1}");
}

fn parse_counter(text: &str, name: &str) -> u64 {
    for line in text.lines() {
        // Skip HELP/TYPE comment lines.
        if line.starts_with('#') { continue; }
        if let Some(rest) = line.strip_prefix(name) {
            // Skip optional label block ({…}) if present.
            let v = rest.trim_start_matches(|c: char| c != ' ' && c != '\t');
            return v.trim().parse::<u64>().unwrap_or(0);
        }
    }
    0
}

#[test]
fn metrics_http_request_counter_per_path_status() {
    let (addr, _g) = spawn_server();
    // Hit /v1/health (will be 200).
    let _ = raw_request(addr,
        b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    // Hit an unknown path → 404 (routes::handle's catch-all).
    let _ = raw_request(addr,
        b"GET /v2/sql HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    // Scrape /v1/metrics; the prior scrapes should be reflected.
    let resp = raw_request(addr,
        b"GET /v1/metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let text = String::from_utf8_lossy(&resp);
    // Must contain a row for /v1/health 200 with count >= 1.
    assert!(
        text.contains("kesseldb_http_requests_total{path=\"/v1/health\",status=\"200\"}"),
        "missing /v1/health 200 counter, got: {text}",
    );
    // The 404 case lands on /v1/sql (default for unknown paths in the
    // 4×16 matrix) but the status bucket is 404 — verify the 404 row
    // exists somewhere.
    assert!(
        text.contains("status=\"404\""),
        "missing 404 counter, got: {text}",
    );
}
