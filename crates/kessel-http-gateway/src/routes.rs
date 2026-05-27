//! Four route handlers — single source of truth for /v1/sql, /v1/op,
//! /v1/health. /v1/metrics handler shipped here as a placeholder; T6
//! replaces it with the Prometheus text writer.
//!
//! SP147 (HTTP/1.1 keep-alive): `handle` computes `keep_alive` ONCE from
//! `parse::wants_close(&req.headers)` and threads it through every
//! `write_*` path. It returns `Result<bool, io::Error>` where the bool is
//! "should this TCP connection close after this response" — `true` when
//! the client asked for `Connection: close` (or when an internal-policy
//! reason forces close). `false` means the per-connection loop in
//! `server::handle_one_stream` should keep reading the next request.

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

/// Auth + dispatch. Returns `Ok(close_after)` — true iff the per-connection
/// loop should close the TCP connection after this response. SP147: derived
/// once from `parse::wants_close(&req.headers)` (RFC 9112 §9.3 persistent
/// default). `keep_alive = !close_after` is plumbed through to every
/// `write_*_counted` so the response's `Connection:` header matches.
pub fn handle<W: Write>(
    w: &mut W,
    req: &Request<'_>,
    token: Option<&[u8]>,
    engine: &Arc<dyn EngineApply>,
    http_counters: &Arc<HttpRequestCountersStatic>,
) -> std::io::Result<bool> {
    let close_after = crate::parse::wants_close(&req.headers);
    let keep_alive = !close_after;
    // Auth first (open-mode lets every request through; token-mode requires
    // a matching Bearer). SP144H T3: the `message` field disambiguates the
    // 401 source — auth-layer ("missing bearer" / "bearer mismatch") vs
    // engine ("engine denied"; see write_op_result below). HTTP status
    // remains 401 in both cases.
    if let Some(expected) = token {
        match extract_bearer(&req.headers) {
            Ok(Some(given)) => {
                if !ct_eq(given, expected) {
                    write_error_json_counted(w, (401, "Unauthorized"),
                        "unauthorized", "bearer mismatch",
                        http_counters, req.path, keep_alive)?;
                    return Ok(close_after);
                }
            }
            Ok(None) => {
                write_error_json_counted(w, (401, "Unauthorized"),
                    "unauthorized", "missing bearer",
                    http_counters, req.path, keep_alive)?;
                return Ok(close_after);
            }
            Err(e) => {
                // SP148 follow-up: use the friendly message from
                // parse_error_to_status_message (not Debug format) so the
                // user-facing JSON body matches the messages emitted by
                // server::write_parse_error.
                let (_status, _semantic, msg) =
                    crate::server::parse_error_to_status_message(&e);
                write_error_json_counted(w, (400, "Bad Request"),
                    "error", &msg,
                    http_counters, req.path, keep_alive)?;
                return Ok(close_after);
            }
        }
    }

    // SP-WS T2: WebSocket upgrade arm. `is_websocket_upgrade` checks both
    // `Upgrade: websocket` AND `Connection: upgrade` (RFC 6455 §4.1 + RFC
    // 9110 §7.6.1/§7.8); when both are present we hijack the stream and
    // hand it to `ws::handle_upgrade`, which writes the 101 (or a 400/
    // 401/405 error response) directly. After this returns — success or
    // failure — the HTTP/1.1 keep-alive loop MUST exit: success means
    // the next bytes on the wire are WebSocket frames (NOT HTTP); failure
    // means we wrote a defensive `Connection: close` error response. We
    // signal both by returning `close_after = true`.
    //
    // CRITICAL: this arm intentionally precedes the generic `match
    // req.path` table so that an `Upgrade: websocket` request on a path
    // *other* than `/v1/ws` still falls through to the path table (the
    // catch-all 404 there is the right behavior for a misdirected
    // upgrade attempt). We gate on `req.path == WEBSOCKET_PATH &&
    // is_websocket_upgrade` so the arm only fires on the exact upgrade
    // shape.
    if req.path == crate::ws::WEBSOCKET_PATH
        && crate::ws::is_websocket_upgrade(&req.headers)
    {
        // T2 ships the handshake only; T5 will spawn the per-connection
        // session loop. Until T5, success means the handshake completed
        // and the stream is now WebSocket (no further frames flow in T2).
        // Both success and failure paths require the HTTP loop to close
        // — success because the bytes are no longer HTTP, failure
        // because the error response carried `Connection: close`.
        let _ = crate::ws::handle_upgrade(w, req, token, engine);
        return Ok(true);
    }

    match req.path {
        "/v1/sql" => handle_sql(w, req, engine, http_counters, keep_alive)?,
        "/v1/op" => handle_op(w, req, engine, http_counters, keep_alive)?,
        "/v1/health" => handle_health(w, engine, http_counters, keep_alive)?,
        "/v1/metrics" => handle_metrics(w, engine, http_counters, keep_alive)?,
        _ => write_error_json_counted(w, (404, "Not Found"),
            "error", "not found",
            http_counters, req.path, keep_alive)?,
    }
    Ok(close_after)
}

fn handle_sql<W: Write>(
    w: &mut W,
    req: &Request<'_>,
    engine: &Arc<dyn EngineApply>,
    http_counters: &Arc<HttpRequestCountersStatic>,
    keep_alive: bool,
) -> std::io::Result<()> {
    if let Some(ct) = req.content_type.as_deref() {
        if !ct.eq_ignore_ascii_case("text/plain") {
            return write_error_json_counted(w, (415, "Unsupported Media Type"),
                "error", "unsupported media type",
                http_counters, req.path, keep_alive);
        }
    }
    let sql = match std::str::from_utf8(req.body.as_ref()) {
        Ok(s) => s,
        Err(_) => return write_error_json_counted(w, (400, "Bad Request"),
            "error", "invalid UTF-8 in SQL body",
            http_counters, req.path, keep_alive),
    };
    let result = match exactly_once_binding(req) {
        Ok(Some((cid, seq))) => engine.apply_sql_with_session(cid, seq, sql),
        Ok(None) => engine.apply_sql(sql),
        Err(e) => {
            // SP148 follow-up: friendly message via parse_error_to_status_message
            // (not Debug format) so exactly_once_binding's IncompleteSessionBinding
            // returns "both X-Kessel-Client-Id and X-Kessel-Req-Seq required
            // together" instead of leaking the variant name.
            let (_status, _semantic, msg) =
                crate::server::parse_error_to_status_message(&e);
            return write_error_json_counted(w, (400, "Bad Request"),
                "error", &msg,
                http_counters, req.path, keep_alive);
        }
    };
    write_op_result(w, &result, http_counters, req.path, keep_alive)
}

fn handle_op<W: Write>(
    w: &mut W,
    req: &Request<'_>,
    engine: &Arc<dyn EngineApply>,
    http_counters: &Arc<HttpRequestCountersStatic>,
    keep_alive: bool,
) -> std::io::Result<()> {
    let ct = req.content_type.as_deref().unwrap_or("");
    if !ct.eq_ignore_ascii_case("application/x-kessel-op")
        && !ct.eq_ignore_ascii_case("application/octet-stream")
    {
        return write_error_json_counted(w, (415, "Unsupported Media Type"),
            "error", "unsupported media type",
            http_counters, req.path, keep_alive);
    }
    let op = match Op::decode(req.body.as_ref()) {
        Some(op) => op,
        None => return write_error_json_counted(w, (400, "Bad Request"),
            "error", "undecodable Op bytes",
            http_counters, req.path, keep_alive),
    };
    let result = match exactly_once_binding(req) {
        Ok(Some((cid, seq))) => engine.apply_op_with_session(cid, seq, op),
        Ok(None) => engine.apply_op(op),
        Err(e) => {
            // SP148 follow-up: friendly message via parse_error_to_status_message
            // (not Debug format) so exactly_once_binding's IncompleteSessionBinding
            // returns "both X-Kessel-Client-Id and X-Kessel-Req-Seq required
            // together" instead of leaking the variant name.
            let (_status, _semantic, msg) =
                crate::server::parse_error_to_status_message(&e);
            return write_error_json_counted(w, (400, "Bad Request"),
                "error", &msg,
                http_counters, req.path, keep_alive);
        }
    };
    write_op_result(w, &result, http_counters, req.path, keep_alive)
}

fn handle_health<W: Write>(
    w: &mut W,
    engine: &Arc<dyn EngineApply>,
    http_counters: &Arc<HttpRequestCountersStatic>,
    keep_alive: bool,
) -> std::io::Result<()> {
    let s = engine.snapshot_health();
    if !s.primary {
        return write_json_counted(w, (503, "Service Unavailable"),
            r#"{"status":"unavailable"}"#,
            http_counters, "/v1/health", keep_alive);
    }
    let body = format!(
        r#"{{"status":"ok","primary":{},"view":{},"op_number":{},"role":"{}"}}"#,
        s.primary, s.view, s.op_number, s.role,
    );
    write_json_counted(w, (200, "OK"), &body, http_counters, "/v1/health",
        keep_alive)
}

fn handle_metrics<W: Write>(
    w: &mut W,
    engine: &Arc<dyn EngineApply>,
    http_counters: &Arc<HttpRequestCountersStatic>,
    keep_alive: bool,
) -> std::io::Result<()> {
    use crate::metrics_writer::render;
    let snap = engine.snapshot_metrics();
    let text = render(&snap);
    write_prometheus_counted(w, &text, http_counters, "/v1/metrics", keep_alive)
}

/// Map an OpResult to (HTTP status, JSON body).
///
/// SP144H T3: Unauthorized/Unavailable get the disambiguating `message`
/// field via `write_error_json_counted` (auth-layer 401s use "missing
/// bearer"/"bearer mismatch" in `handle()`; the engine-side 401 here uses
/// "engine denied"). The Unavailable body changes shape too — was
/// `{"status":"unavailable"}`, now
/// `{"status":"unavailable","message":"engine unavailable"}`. All other
/// OpResult variants still go through `format_result_json` verbatim so the
/// kessel-client JSON contract for Ok/Exists/NotFound/etc. is unchanged.
fn write_op_result<W: Write>(
    w: &mut W,
    r: &OpResult,
    http_counters: &Arc<HttpRequestCountersStatic>,
    path: &str,
    keep_alive: bool,
) -> std::io::Result<()> {
    match r {
        OpResult::Unauthorized => write_error_json_counted(
            w, (401, "Unauthorized"),
            "unauthorized", "engine denied",
            http_counters, path, keep_alive,
        ),
        OpResult::Unavailable => write_error_json_counted(
            w, (503, "Service Unavailable"),
            "unavailable", "engine unavailable",
            http_counters, path, keep_alive,
        ),
        _ => {
            let body = format_result_json(r);
            write_json_counted(w, (200, "OK"), &body, http_counters, path,
                keep_alive)
        }
    }
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
        // SP144H T4: dedicated ParseError variant (was previously
        // BadHeaderValue(String) — fragile because callers had to
        // string-grep the message).
        _ => Err(ParseError::IncompleteSessionBinding),
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
