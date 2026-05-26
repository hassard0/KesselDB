//! Four route handlers — single source of truth for /v1/sql, /v1/op,
//! /v1/health. /v1/metrics handler shipped here as a placeholder; T6
//! replaces it with the Prometheus text writer.

#![allow(dead_code)]

use crate::engine::{EngineApply, HttpRequestCountersStatic};
use crate::parse::{
    extract_bearer, extract_client_id, extract_req_seq, ParseError, Request,
};
use crate::response::{
    write_error_json_counted, write_json_counted, write_prometheus_counted,
};
use kessel_client::format_result_json;
use kessel_proto::{Op, OpResult};
use std::io::Write;
use std::sync::Arc;

/// Auth + dispatch.
pub fn handle<W: Write>(
    w: &mut W,
    req: &Request<'_>,
    token: Option<&[u8]>,
    engine: &Arc<dyn EngineApply>,
    http_counters: &Arc<HttpRequestCountersStatic>,
) -> std::io::Result<()> {
    // Auth first (open-mode lets every request through; token-mode requires
    // a matching Bearer).
    if let Some(expected) = token {
        match extract_bearer(&req.headers) {
            Ok(Some(given)) => {
                if !ct_eq(given, expected) {
                    return write_json_counted(w, (401, "Unauthorized"),
                        r#"{"status":"unauthorized"}"#,
                        http_counters, req.path);
                }
            }
            Ok(None) => {
                return write_json_counted(w, (401, "Unauthorized"),
                    r#"{"status":"unauthorized"}"#,
                    http_counters, req.path);
            }
            Err(e) => {
                return write_error_json_counted(w, (400, "Bad Request"),
                    "error", &format!("{:?}", e),
                    http_counters, req.path);
            }
        }
    }

    match req.path {
        "/v1/sql" => handle_sql(w, req, engine, http_counters),
        "/v1/op" => handle_op(w, req, engine, http_counters),
        "/v1/health" => handle_health(w, engine, http_counters),
        "/v1/metrics" => handle_metrics(w, engine, http_counters),
        _ => write_error_json_counted(w, (404, "Not Found"),
            "error", "not found",
            http_counters, req.path),
    }
}

fn handle_sql<W: Write>(
    w: &mut W,
    req: &Request<'_>,
    engine: &Arc<dyn EngineApply>,
    http_counters: &Arc<HttpRequestCountersStatic>,
) -> std::io::Result<()> {
    if let Some(ct) = req.content_type.as_deref() {
        if !ct.eq_ignore_ascii_case("text/plain") {
            return write_error_json_counted(w, (415, "Unsupported Media Type"),
                "error", "unsupported media type",
                http_counters, req.path);
        }
    }
    let sql = match std::str::from_utf8(req.body.as_ref()) {
        Ok(s) => s,
        Err(_) => return write_error_json_counted(w, (400, "Bad Request"),
            "error", "invalid UTF-8 in SQL body",
            http_counters, req.path),
    };
    let result = match exactly_once_binding(req) {
        Ok(Some((cid, seq))) => engine.apply_sql_with_session(cid, seq, sql),
        Ok(None) => engine.apply_sql(sql),
        Err(e) => return write_error_json_counted(w, (400, "Bad Request"),
            "error", &format!("{:?}", e),
            http_counters, req.path),
    };
    write_op_result(w, &result, http_counters, req.path)
}

fn handle_op<W: Write>(
    w: &mut W,
    req: &Request<'_>,
    engine: &Arc<dyn EngineApply>,
    http_counters: &Arc<HttpRequestCountersStatic>,
) -> std::io::Result<()> {
    let ct = req.content_type.as_deref().unwrap_or("");
    if !ct.eq_ignore_ascii_case("application/x-kessel-op")
        && !ct.eq_ignore_ascii_case("application/octet-stream")
    {
        return write_error_json_counted(w, (415, "Unsupported Media Type"),
            "error", "unsupported media type",
            http_counters, req.path);
    }
    let op = match Op::decode(req.body.as_ref()) {
        Some(op) => op,
        None => return write_error_json_counted(w, (400, "Bad Request"),
            "error", "undecodable Op bytes",
            http_counters, req.path),
    };
    let result = match exactly_once_binding(req) {
        Ok(Some((cid, seq))) => engine.apply_op_with_session(cid, seq, op),
        Ok(None) => engine.apply_op(op),
        Err(e) => return write_error_json_counted(w, (400, "Bad Request"),
            "error", &format!("{:?}", e),
            http_counters, req.path),
    };
    write_op_result(w, &result, http_counters, req.path)
}

fn handle_health<W: Write>(
    w: &mut W,
    engine: &Arc<dyn EngineApply>,
    http_counters: &Arc<HttpRequestCountersStatic>,
) -> std::io::Result<()> {
    let s = engine.snapshot_health();
    if !s.primary {
        return write_json_counted(w, (503, "Service Unavailable"),
            r#"{"status":"unavailable"}"#,
            http_counters, "/v1/health");
    }
    let body = format!(
        r#"{{"status":"ok","primary":{},"view":{},"op_number":{},"role":"{}"}}"#,
        s.primary, s.view, s.op_number, s.role,
    );
    write_json_counted(w, (200, "OK"), &body, http_counters, "/v1/health")
}

fn handle_metrics<W: Write>(
    w: &mut W,
    engine: &Arc<dyn EngineApply>,
    http_counters: &Arc<HttpRequestCountersStatic>,
) -> std::io::Result<()> {
    use crate::metrics_writer::render;
    let snap = engine.snapshot_metrics();
    let text = render(&snap);
    write_prometheus_counted(w, &text, http_counters, "/v1/metrics")
}

/// Map an OpResult to (HTTP status, JSON body via format_result_json).
fn write_op_result<W: Write>(
    w: &mut W,
    r: &OpResult,
    http_counters: &Arc<HttpRequestCountersStatic>,
    path: &str,
) -> std::io::Result<()> {
    let body = format_result_json(r);
    let status = match r {
        OpResult::Unauthorized => (401, "Unauthorized"),
        OpResult::Unavailable => (503, "Service Unavailable"),
        _ => (200, "OK"),
    };
    write_json_counted(w, status, &body, http_counters, path)
}

/// Both-or-neither: either both headers present (Ok(Some)), both absent
/// (Ok(None)), or one present without the other (Err).
fn exactly_once_binding(
    req: &Request<'_>,
) -> Result<Option<(u128, u64)>, ParseError> {
    let cid = extract_client_id(&req.headers)?;
    let seq = extract_req_seq(&req.headers)?;
    match (cid, seq) {
        (Some(c), Some(s)) => Ok(Some((c, s))),
        (None, None) => Ok(None),
        _ => Err(ParseError::BadHeaderValue(
            "both X-Kessel-Client-Id and X-Kessel-Req-Seq required together".into())),
    }
}

/// Constant-time compare — mirror `kesseldb-server::ct_eq`. Reimplemented
/// here so the gateway crate has no `kesseldb-server` dep.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let n = a.len().max(b.len());
    let mut diff = (a.len() ^ b.len()) as u8;
    for i in 0..n {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= x ^ y;
    }
    diff == 0
}
