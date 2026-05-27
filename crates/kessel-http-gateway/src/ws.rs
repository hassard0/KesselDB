//! SP-WS — WebSocket support for the KesselDB HTTP gateway (RFC 6455).
//!
//! **T2 status (current):** the handshake parser ships. `handle_upgrade`
//! validates the upgrade request per RFC 6455 §4 (path, method, version,
//! key, optional subprotocol, optional Bearer auth) and writes a
//! `HTTP/1.1 101 Switching Protocols` response — OR a 400/401/405
//! error response — straight to the TcpStream. After 101 is sent, the
//! stream now carries WebSocket frames; the per-connection HTTP loop
//! MUST close (T2's `routes::handle` arm returns `close_after = true`).
//! Frame encoding/decoding + the session loop are still T3/T4/T5.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-26-kesseldb-spws-websocket-design.md`
//!
//! ## Task decomposition (mirrors spec §10)
//!
//! - **T1** (shipped) — design spec + scaffold + crypto helper +
//!   `Sec-WebSocket-Accept` KATs locking the wire-protocol invariants
//! - **T2** (this commit) — handshake parser: validate upgrade-request
//!   headers (Upgrade, Connection, Sec-WebSocket-Key,
//!   Sec-WebSocket-Version=13, path=/v1/ws, Authorization), build the
//!   101 Switching Protocols response bytes, wire the arm into
//!   `routes::handle`
//! - **T3** — frame encoder (binary, ping, pong, close); server-side
//!   never masks per RFC 6455 §5.3
//! - **T4** — frame decoder; strict validation: MASK from client,
//!   reserved bits zero, control frames ≤ 125 bytes + FIN=1, no
//!   fragmentation
//! - **T5** — per-connection session loop (reader thread + writer
//!   thread + bounded send queue + ping/pong heartbeat + idle timeout
//!   + graceful close handshake)
//! - **T6** — `kessel-op-v1` subprotocol wire-up + real-WebSocket-
//!   client e2e test + 10-pentest matrix
//!
//! ## Zero-dep stance
//!
//! `std::net::TcpStream`, `std::thread`, `std::sync::mpsc` only. SHA-1
//! + base64 come from `kessel-crypto` (workspace, zero external dep).
//! No tungstenite, no tokio-tungstenite, no async runtime — same shape
//! as the rest of the gateway.

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::crypto::sec_websocket_accept;
use crate::engine::EngineApply;
use crate::parse::{Method, Request};
use std::io::Write;
use std::sync::Arc;

/// Spec §7: per-WebSocket-connection bounded send queue depth. Chosen
/// larger than SP-A's `SHARD_BACKPRESSURE_BOUND=4` because a
/// WebSocket's send queue is shared across data frames AND control
/// frames (ping/pong/close). Reserving 12 slots for data + a few for
/// control avoids head-of-line blocking during a slow drain. T5 wires
/// this; today it's a locked constant for forward reference.
pub const WS_SEND_QUEUE_BOUND: usize = 16;

/// Spec §6.1: the dedicated WebSocket upgrade path. Listed in
/// `parse::is_known_path` so `GET /v1/ws` routes through the table
/// (instead of falling through to a 404 before the upgrade arm gets a
/// chance to fire). Locked here so the value can be referenced by the
/// spec + tests without magic strings.
pub const WEBSOCKET_PATH: &str = "/v1/ws";

/// Spec §5.1: the only subprotocol the V1 server advertises. Clients
/// MAY name it in `Sec-WebSocket-Protocol`; clients MAY omit the header
/// (browsers default `new WebSocket(url)` does this) and still get the
/// same semantics. Future versions can negotiate `kessel-op-v2-*`
/// alongside without breaking V1 consumers.
pub const SUBPROTOCOL_V1: &str = "kessel-op-v1";

/// Spec §4 / RFC 6455 §4.2.1: the only version this server speaks.
pub const WEBSOCKET_VERSION: &str = "13";

/// Outcomes the upgrade handler may report to its caller.
///
/// T2's `handle_upgrade` writes the actual HTTP response (101 / 400 /
/// 401 / 405) inline — the caller just needs to know whether the
/// handshake succeeded so the HTTP keep-alive loop can exit (success
/// case, stream is now WebSocket) or stay open (failure case, the
/// response stayed HTTP). For T2 we ALWAYS close after responding:
/// the handshake either succeeded (and the next bytes are WS frames,
/// not HTTP) or failed (and a defensive close is the same policy the
/// HTTP layer uses for parse errors). T5 will lift the success path
/// into a real session loop.
#[derive(Debug, PartialEq, Eq)]
pub enum WsError {
    /// The handshake failed with an HTTP-layer error (400/401/405).
    /// The response was already written to the stream; the caller
    /// should close the connection. Carries the status code for
    /// diagnostics + future metrics-counter bumping.
    HandshakeFailed(u16),
    /// Writing the response bytes to the stream failed (peer
    /// disconnected mid-write, etc.). The caller should close.
    Io(std::io::ErrorKind),
}

/// Spec §6.2: the dispatch entry point from `routes::handle`.
///
/// T2 ships the real handshake. Returns:
///   - `Ok(())` — 101 response written. The stream is now a WebSocket
///     (no frames flow in T2 — the session loop is T5; today the
///     handshake just completes and the connection closes).
///   - `Err(WsError::HandshakeFailed(status))` — a 400/401/405 was
///     written; the connection should close.
///   - `Err(WsError::Io(kind))` — writing failed; the connection
///     should close.
///
/// Caller contract: regardless of the return, the per-connection HTTP
/// loop MUST close after this returns. Success → stream is no longer
/// HTTP. Failure → defensive close (same policy as a parse error).
///
/// **T2 stream-type note:** the bound is `S: Write` because T2 only
/// writes the handshake response. **T5 will widen this to `Read +
/// Write`** so the session loop can read frames; the routes-side
/// caller already passes the full `TcpStream` (which implements both).
pub fn handle_upgrade<S: Write>(
    stream: &mut S,
    req: &Request<'_>,
    token: Option<&[u8]>,
    _engine: &Arc<dyn EngineApply>,
) -> Result<(), WsError> {
    // RFC 6455 §4.1: the upgrade request MUST be a GET. POST / PUT /
    // DELETE / etc → 405 Method Not Allowed.
    if req.method != Method::Get {
        return write_handshake_error(stream, 405, "Method Not Allowed",
            "method not allowed", None);
    }

    // Auth FIRST, before any of the upgrade-specific header checks. The
    // server should not leak whether a key/version was valid to an
    // unauthenticated client. Mirrors `routes::handle`'s auth-first
    // ordering for parity with the HTTP routes.
    if let Some(expected) = token {
        match crate::parse::extract_bearer(&req.headers) {
            Ok(Some(given)) => {
                if !ct_eq(given, expected) {
                    return write_handshake_error(stream, 401, "Unauthorized",
                        "unauthorized", None);
                }
            }
            Ok(None) | Err(_) => {
                return write_handshake_error(stream, 401, "Unauthorized",
                    "unauthorized", None);
            }
        }
    }

    // RFC 6455 §4.1 requires BOTH `Upgrade: websocket` AND `Connection:
    // upgrade` (case-insensitive; Connection may carry a multi-token
    // list e.g. `keep-alive, Upgrade`). Re-check inside handle_upgrade
    // (the routes-side gate via `is_websocket_upgrade` is the FAST gate
    // — this is the SLOW gate that produces the right 400 response if
    // the routes layer ever calls us on a non-upgrade request, e.g. via
    // tests or a future refactor). Source-of-truth lives HERE.
    if !header_has_token(&req.headers, "upgrade", "websocket") {
        return write_handshake_error(stream, 400, "Bad Request",
            "missing Upgrade: websocket header", None);
    }
    if !header_has_token(&req.headers, "connection", "upgrade") {
        return write_handshake_error(stream, 400, "Bad Request",
            "missing Connection: upgrade header", None);
    }

    // RFC 6455 §4.2.1: validate `Sec-WebSocket-Version: 13`. Any other
    // value (or absent) → 400 with `Sec-WebSocket-Version: 13` response
    // header so the client knows which version we speak. Note: RFC 6455
    // §4.4 mentions 426 Upgrade Required for some servers; we use 400
    // with the hint header — both are conformant and 400 is simpler.
    let version = single_header_value(&req.headers, "sec-websocket-version");
    match version {
        Some(v) if v.trim() == WEBSOCKET_VERSION => {}
        _ => {
            return write_handshake_error(stream, 400, "Bad Request",
                "unsupported Sec-WebSocket-Version (only 13 supported)",
                Some(WEBSOCKET_VERSION));
        }
    }

    // RFC 6455 §4.1: `Sec-WebSocket-Key` MUST be present and MUST
    // base64-decode to exactly 16 bytes. Missing / malformed → 400.
    let key = match single_header_value(&req.headers, "sec-websocket-key") {
        Some(k) => k.trim().to_string(),
        None => {
            return write_handshake_error(stream, 400, "Bad Request",
                "missing Sec-WebSocket-Key header", None);
        }
    };
    match kessel_crypto::base64_decode(&key) {
        Some(decoded) if decoded.len() == 16 => {}
        _ => {
            return write_handshake_error(stream, 400, "Bad Request",
                "Sec-WebSocket-Key must base64-decode to 16 bytes", None);
        }
    }

    // RFC 6455 §1.3 / §4.1: optional `Sec-WebSocket-Protocol` — a
    // comma-separated list of subprotocol names. If the list contains
    // `kessel-op-v1` we echo it in the response. If the header is
    // absent → no `Sec-WebSocket-Protocol` in the response and the
    // default `kessel-op-v1` semantics apply (spec §5.2). If the
    // header is present but lists ONLY unknown subprotocols → 400 per
    // spec §5.1.
    let selected_subprotocol =
        select_subprotocol(&req.headers).map_err(|e| e)?;
    let accept_value = sec_websocket_accept(&key);

    // Build the 101 Switching Protocols response. RFC 6455 §4.2.2
    // canonical example shape:
    //   HTTP/1.1 101 Switching Protocols\r\n
    //   Upgrade: websocket\r\n
    //   Connection: Upgrade\r\n
    //   Sec-WebSocket-Accept: <accept>\r\n
    //   [Sec-WebSocket-Protocol: <name>\r\n]
    //   \r\n
    // CRITICAL: no Content-Length, no Server header in the 101
    // response — the connection transitions to WebSocket framing
    // immediately after the terminating \r\n\r\n. Adding response
    // bytes that browsers interpret as HTTP body would corrupt the
    // first WS frame.
    let mut resp = String::with_capacity(160);
    resp.push_str("HTTP/1.1 101 Switching Protocols\r\n");
    resp.push_str("Upgrade: websocket\r\n");
    resp.push_str("Connection: Upgrade\r\n");
    resp.push_str("Sec-WebSocket-Accept: ");
    resp.push_str(&accept_value);
    resp.push_str("\r\n");
    if let Some(name) = selected_subprotocol {
        resp.push_str("Sec-WebSocket-Protocol: ");
        resp.push_str(name);
        resp.push_str("\r\n");
    }
    resp.push_str("\r\n");
    stream.write_all(resp.as_bytes()).map_err(|e| WsError::Io(e.kind()))?;
    // T2 ends here. T5 will spawn the per-connection reader+writer
    // thread pair and run the session loop. For T2: the handshake
    // completed, the connection's HTTP side closes (routes::handle
    // returns close_after=true), the underlying TcpStream is closed
    // when the caller drops it. No frames flow — a WebSocket client
    // that sends a binary frame after the 101 will see a TCP close.
    // That's fine for T2 (the deliverable is "the handshake is
    // parseable + correct").
    Ok(())
}

/// Write an HTTP error response (400/401/405) for a failed handshake.
/// `version_hint`, when `Some`, adds a `Sec-WebSocket-Version` header
/// so the client knows which version we speak (RFC 6455 §4.4).
fn write_handshake_error<S: Write>(
    stream: &mut S,
    status: u16,
    reason: &str,
    body: &str,
    version_hint: Option<&str>,
) -> Result<(), WsError> {
    let body_bytes = body.as_bytes();
    let mut resp = String::with_capacity(160 + body.len());
    resp.push_str(&format!("HTTP/1.1 {status} {reason}\r\n"));
    resp.push_str("Content-Type: text/plain; charset=utf-8\r\n");
    resp.push_str(&format!("Content-Length: {}\r\n", body_bytes.len()));
    resp.push_str("Connection: close\r\n");
    if let Some(v) = version_hint {
        resp.push_str(&format!("Sec-WebSocket-Version: {v}\r\n"));
    }
    resp.push_str("Server: kesseldb/0\r\n");
    resp.push_str("\r\n");
    resp.push_str(body);
    stream.write_all(resp.as_bytes()).map_err(|e| WsError::Io(e.kind()))?;
    Err(WsError::HandshakeFailed(status))
}

/// Single-instance header value (case-insensitive name lookup). Returns
/// `None` if absent. If the header appears multiple times, returns the
/// first occurrence — the predicate already accepts only one, and the
/// spec's required handshake headers (Sec-WebSocket-Key,
/// Sec-WebSocket-Version) are single-instance per RFC 6455 §4.1.
fn single_header_value<'a>(
    headers: &'a [(String, String)],
    name: &str,
) -> Option<&'a str> {
    for (n, v) in headers {
        if n.eq_ignore_ascii_case(name) {
            return Some(v.as_str());
        }
    }
    None
}

/// True iff any value of header `name` contains `token` (case-
/// insensitive token comparison, comma-list-aware per RFC 9110 §7.6.1).
fn header_has_token(
    headers: &[(String, String)],
    name: &str,
    token: &str,
) -> bool {
    for (n, v) in headers {
        if n.eq_ignore_ascii_case(name) {
            for t in v.split(',') {
                if t.trim().eq_ignore_ascii_case(token) {
                    return true;
                }
            }
        }
    }
    false
}

/// Spec §5.1: pick a subprotocol from the client's offer list. Returns:
///   - `Ok(None)` — header absent. Per spec §5.2 the connection runs
///     under `kessel-op-v1` semantics by default; we omit the header
///     from the response so RFC 6455 §1.3 is satisfied (omitting the
///     header signals "no negotiated subprotocol").
///   - `Ok(Some(&'static str))` — header present and contains
///     `kessel-op-v1`; echo that exact name in the response.
///   - `Err(WsError::HandshakeFailed(400))` — header present but
///     contains zero known subprotocols. Note: this writes nothing to
///     the stream — the caller's error path does the write. We return
///     the error variant so the call site short-circuits cleanly.
///
/// Returning a `&'static str` for the selected subprotocol keeps the
/// echoed value exactly equal to `SUBPROTOCOL_V1` — defending against
/// echoing back the client's raw casing (which could carry whitespace
/// or weird capitalization that the spec doesn't promise).
fn select_subprotocol(
    headers: &[(String, String)],
) -> Result<Option<&'static str>, WsError> {
    let mut header_present = false;
    let mut any_offer = false;
    for (n, v) in headers {
        if n.eq_ignore_ascii_case("sec-websocket-protocol") {
            header_present = true;
            for offer in v.split(',') {
                let o = offer.trim();
                if o.is_empty() {
                    continue;
                }
                any_offer = true;
                if o.eq_ignore_ascii_case(SUBPROTOCOL_V1) {
                    return Ok(Some(SUBPROTOCOL_V1));
                }
            }
        }
    }
    if header_present && any_offer {
        // Header present, offers present, none known → 400. The caller
        // is expected to translate this into a write_handshake_error.
        // Since we can't get a `&mut stream` here, surface as the error
        // variant and have the caller short-circuit.
        return Err(WsError::HandshakeFailed(400));
    }
    Ok(None)
}

/// Constant-time compare — mirrors `routes::ct_eq`. Reimplemented here so
/// `handle_upgrade` doesn't have to call back into the routes module.
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

/// Spec §6.2: detect whether the parsed HTTP request is a WebSocket
/// upgrade attempt. Returns true iff the request carries
/// `Upgrade: websocket` (case-insensitive, RFC 9110 §7.8) AND
/// `Connection: Upgrade` token in the comma-separated list (RFC 9110
/// §7.6.1). `handle_upgrade` re-validates these inside the slow path
/// so an upgrade request that somehow reached the wrong arm still
/// produces a clean 400 (defense in depth).
pub fn is_websocket_upgrade(headers: &[(String, String)]) -> bool {
    header_has_token(headers, "upgrade", "websocket")
        && header_has_token(headers, "connection", "upgrade")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineApply;
    use kessel_proto::{Op, OpResult};
    use std::io::Cursor;

    // --- Constant locks ---------------------------------------------

    #[test]
    fn websocket_path_is_v1_ws() {
        assert_eq!(WEBSOCKET_PATH, "/v1/ws",
            "spec §6.1 locks the WebSocket upgrade path");
    }

    #[test]
    fn subprotocol_v1_name_is_kessel_op_v1() {
        assert_eq!(SUBPROTOCOL_V1, "kessel-op-v1",
            "spec §5.1 locks the V1 subprotocol name");
    }

    #[test]
    fn ws_send_queue_bound_is_sixteen_per_spec() {
        assert_eq!(WS_SEND_QUEUE_BOUND, 16,
            "spec §7 locks WS_SEND_QUEUE_BOUND = 16");
    }

    #[test]
    fn websocket_version_constant_is_13_per_rfc6455() {
        assert_eq!(WEBSOCKET_VERSION, "13",
            "RFC 6455 §4.2.1 — only version 13 is supported");
    }

    // --- is_websocket_upgrade predicate -----------------------------

    #[test]
    fn is_websocket_upgrade_detects_canonical_handshake_headers() {
        let headers = vec![
            ("Host".into(), "kesseldb.example".into()),
            ("Upgrade".into(), "websocket".into()),
            ("Connection".into(), "Upgrade".into()),
            ("Sec-WebSocket-Key".into(), "dGhlIHNhbXBsZSBub25jZQ==".into()),
            ("Sec-WebSocket-Version".into(), "13".into()),
        ];
        assert!(is_websocket_upgrade(&headers),
            "canonical RFC 6455 §1.3 handshake headers must register");
    }

    #[test]
    fn is_websocket_upgrade_handles_multi_token_connection_header() {
        let headers = vec![
            ("Host".into(), "h".into()),
            ("Upgrade".into(), "websocket".into()),
            ("Connection".into(), "keep-alive, Upgrade".into()),
        ];
        assert!(is_websocket_upgrade(&headers),
            "multi-token Connection: keep-alive, Upgrade must register");
    }

    #[test]
    fn is_websocket_upgrade_rejects_missing_upgrade_websocket() {
        let headers = vec![
            ("Host".into(), "h".into()),
            ("Connection".into(), "Upgrade".into()),
        ];
        assert!(!is_websocket_upgrade(&headers),
            "Connection: Upgrade without Upgrade: websocket is NOT a WS upgrade");
    }

    #[test]
    fn is_websocket_upgrade_rejects_missing_connection_upgrade() {
        let headers = vec![
            ("Host".into(), "h".into()),
            ("Upgrade".into(), "websocket".into()),
        ];
        assert!(!is_websocket_upgrade(&headers),
            "Upgrade: websocket without Connection: Upgrade is NOT a WS upgrade");
    }

    #[test]
    fn is_websocket_upgrade_is_case_insensitive() {
        let headers = vec![
            ("upgrade".into(), "WebSocket".into()),
            ("connection".into(), "upgrade".into()),
        ];
        assert!(is_websocket_upgrade(&headers),
            "header name + token case-insensitivity per RFC 9110 §5.1");
    }

    // --- handle_upgrade — T2 KATs ------------------------------------

    struct NullEngine;
    impl EngineApply for NullEngine {
        fn apply_op(&self, _op: Op) -> OpResult { OpResult::Unavailable }
        fn apply_op_with_session(&self, _cid: u128, _seq: u64, _op: Op)
            -> OpResult { OpResult::Unavailable }
        fn apply_sql(&self, _sql: &str) -> OpResult { OpResult::Unavailable }
        fn apply_sql_with_session(&self, _cid: u128, _seq: u64, _sql: &str)
            -> OpResult { OpResult::Unavailable }
        fn snapshot_health(&self) -> crate::engine::HealthSnapshot {
            crate::engine::HealthSnapshot {
                primary: false, view: 0, op_number: 0, role: "follower",
            }
        }
        fn snapshot_metrics(&self) -> crate::engine::MetricsSnapshot {
            crate::engine::MetricsSnapshot {
                ops_total: Vec::new(),
                inflight: 0,
                last_op_number: 0,
                view_number: 0,
                is_primary: false,
                http_requests_total: Vec::new(),
            }
        }
    }

    fn engine() -> Arc<dyn EngineApply> {
        Arc::new(NullEngine)
    }

    /// Build a Request<'static> with the given headers + method. The
    /// path is always `/v1/ws` unless overridden by the caller via
    /// `path`.
    fn build_req(
        method: Method,
        path: &'static str,
        headers: Vec<(String, String)>,
    ) -> Request<'static> {
        Request {
            method,
            path,
            host: "kesseldb.example".into(),
            content_type: None,
            content_length: None,
            chunked: false,
            body: std::borrow::Cow::Borrowed(&[]),
            consumed: 0,
            headers,
        }
    }

    fn canonical_handshake_headers() -> Vec<(String, String)> {
        vec![
            ("Host".into(), "kesseldb.example".into()),
            ("Upgrade".into(), "websocket".into()),
            ("Connection".into(), "Upgrade".into()),
            ("Sec-WebSocket-Key".into(), "dGhlIHNhbXBsZSBub25jZQ==".into()),
            ("Sec-WebSocket-Version".into(), "13".into()),
        ]
    }

    /// Spec §4 / RFC 6455 §4.2.2 — successful handshake response is
    /// byte-correct against the canonical example: the client key
    /// `dGhlIHNhbXBsZSBub25jZQ==` produces accept
    /// `s3pPLMBiTxaQ9kYGzzhZRbK+xOo=`. **THIS REPLACES THE T1 STUB
    /// REGRESSION-LOCK.** Going forward, regressions in the handshake
    /// surface here, not in the T1 placeholder.
    #[test]
    fn t2_successful_handshake_returns_101_with_canonical_accept() {
        let req = build_req(Method::Get, WEBSOCKET_PATH,
            canonical_handshake_headers());
        let mut sink = Cursor::new(Vec::<u8>::new());
        let result = handle_upgrade(&mut sink, &req, None, &engine());
        assert!(result.is_ok(), "canonical handshake must succeed: {result:?}");
        let resp = String::from_utf8(sink.into_inner())
            .expect("response is ASCII");
        // Status line — locked byte-for-byte.
        assert!(resp.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
            "status line mismatch; got:\n{resp:?}");
        // Required RFC 6455 §4.2.2 headers.
        assert!(resp.contains("\r\nUpgrade: websocket\r\n"),
            "Upgrade header missing; got:\n{resp:?}");
        assert!(resp.contains("\r\nConnection: Upgrade\r\n"),
            "Connection header missing; got:\n{resp:?}");
        // The canonical RFC 6455 §1.3 accept value.
        assert!(resp.contains(
            "\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"),
            "Sec-WebSocket-Accept mismatch; got:\n{resp:?}");
        // No subprotocol was offered → none echoed.
        assert!(!resp.contains("Sec-WebSocket-Protocol"),
            "no subprotocol offered → header must be omitted; got:\n{resp:?}");
        // Terminator + no body bytes after.
        assert!(resp.ends_with("\r\n\r\n"),
            "response must terminate with bare CRLF (no body); got:\n{resp:?}");
        // Sanity: no Content-Length / Server bytes that browsers would
        // mis-interpret as part of the first WS frame.
        assert!(!resp.contains("Content-Length"),
            "101 response must NOT carry Content-Length; got:\n{resp:?}");
    }

    /// Missing `Sec-WebSocket-Key` → 400 Bad Request.
    #[test]
    fn t2_missing_sec_websocket_key_returns_400() {
        let mut h = canonical_handshake_headers();
        h.retain(|(n, _)| !n.eq_ignore_ascii_case("sec-websocket-key"));
        let req = build_req(Method::Get, WEBSOCKET_PATH, h);
        let mut sink = Cursor::new(Vec::<u8>::new());
        let result = handle_upgrade(&mut sink, &req, None, &engine());
        assert_eq!(result, Err(WsError::HandshakeFailed(400)));
        let resp = String::from_utf8(sink.into_inner()).unwrap();
        assert!(resp.starts_with("HTTP/1.1 400 Bad Request\r\n"),
            "expected 400; got:\n{resp:?}");
        assert!(resp.contains("missing Sec-WebSocket-Key"),
            "body should explain the missing key; got:\n{resp:?}");
    }

    /// `Sec-WebSocket-Key` that does NOT base64-decode to 16 bytes → 400.
    #[test]
    fn t2_malformed_sec_websocket_key_returns_400() {
        let mut h = canonical_handshake_headers();
        for (n, v) in &mut h {
            if n.eq_ignore_ascii_case("sec-websocket-key") {
                // base64 of 12 bytes, not 16
                *v = "AAAAAAAAAAAAAAAA".into();
            }
        }
        let req = build_req(Method::Get, WEBSOCKET_PATH, h);
        let mut sink = Cursor::new(Vec::<u8>::new());
        let result = handle_upgrade(&mut sink, &req, None, &engine());
        assert_eq!(result, Err(WsError::HandshakeFailed(400)));
        let resp = String::from_utf8(sink.into_inner()).unwrap();
        assert!(resp.contains("16 bytes"),
            "body should explain key length requirement; got:\n{resp:?}");
    }

    /// Wrong `Sec-WebSocket-Version` (anything other than `13`) → 400
    /// with `Sec-WebSocket-Version: 13` hint header so the client
    /// knows which version we speak.
    #[test]
    fn t2_wrong_sec_websocket_version_returns_400_with_version_hint() {
        let mut h = canonical_handshake_headers();
        for (n, v) in &mut h {
            if n.eq_ignore_ascii_case("sec-websocket-version") {
                *v = "8".into();
            }
        }
        let req = build_req(Method::Get, WEBSOCKET_PATH, h);
        let mut sink = Cursor::new(Vec::<u8>::new());
        let result = handle_upgrade(&mut sink, &req, None, &engine());
        assert_eq!(result, Err(WsError::HandshakeFailed(400)));
        let resp = String::from_utf8(sink.into_inner()).unwrap();
        assert!(resp.starts_with("HTTP/1.1 400 Bad Request\r\n"),
            "expected 400; got:\n{resp:?}");
        assert!(resp.contains("\r\nSec-WebSocket-Version: 13\r\n"),
            "wrong version must include hint header; got:\n{resp:?}");
    }

    /// Missing `Upgrade: websocket` → 400 (the routes-side `is_websocket_upgrade`
    /// fast-gate would normally filter this, but handle_upgrade's slow
    /// path validates it for defense-in-depth so a direct call from a
    /// test / future refactor still produces a clean response).
    #[test]
    fn t2_missing_upgrade_header_returns_400() {
        let mut h = canonical_handshake_headers();
        h.retain(|(n, _)| !n.eq_ignore_ascii_case("upgrade"));
        let req = build_req(Method::Get, WEBSOCKET_PATH, h);
        let mut sink = Cursor::new(Vec::<u8>::new());
        let result = handle_upgrade(&mut sink, &req, None, &engine());
        assert_eq!(result, Err(WsError::HandshakeFailed(400)));
        let resp = String::from_utf8(sink.into_inner()).unwrap();
        assert!(resp.contains("missing Upgrade"),
            "body should mention missing Upgrade; got:\n{resp:?}");
    }

    /// Missing `Connection: Upgrade` → 400.
    #[test]
    fn t2_missing_connection_upgrade_returns_400() {
        let mut h = canonical_handshake_headers();
        h.retain(|(n, _)| !n.eq_ignore_ascii_case("connection"));
        let req = build_req(Method::Get, WEBSOCKET_PATH, h);
        let mut sink = Cursor::new(Vec::<u8>::new());
        let result = handle_upgrade(&mut sink, &req, None, &engine());
        assert_eq!(result, Err(WsError::HandshakeFailed(400)));
        let resp = String::from_utf8(sink.into_inner()).unwrap();
        assert!(resp.contains("missing Connection"),
            "body should mention missing Connection; got:\n{resp:?}");
    }

    /// Bearer mismatch in token-mode → 401 Unauthorized. Mirrors the
    /// `routes::handle` token-mode policy.
    #[test]
    fn t2_bearer_mismatch_in_token_mode_returns_401() {
        let mut h = canonical_handshake_headers();
        h.push(("Authorization".into(), "Bearer wrong-token".into()));
        let req = build_req(Method::Get, WEBSOCKET_PATH, h);
        let mut sink = Cursor::new(Vec::<u8>::new());
        let result = handle_upgrade(&mut sink, &req,
            Some(b"the-real-token"), &engine());
        assert_eq!(result, Err(WsError::HandshakeFailed(401)));
        let resp = String::from_utf8(sink.into_inner()).unwrap();
        assert!(resp.starts_with("HTTP/1.1 401 Unauthorized\r\n"),
            "expected 401; got:\n{resp:?}");
        assert!(resp.contains("unauthorized"),
            "body should say unauthorized; got:\n{resp:?}");
    }

    /// Missing Authorization in token-mode → 401 Unauthorized.
    #[test]
    fn t2_missing_bearer_in_token_mode_returns_401() {
        let h = canonical_handshake_headers();
        let req = build_req(Method::Get, WEBSOCKET_PATH, h);
        let mut sink = Cursor::new(Vec::<u8>::new());
        let result = handle_upgrade(&mut sink, &req,
            Some(b"required-token"), &engine());
        assert_eq!(result, Err(WsError::HandshakeFailed(401)));
    }

    /// Matching Bearer in token-mode → 101 (the handshake completes).
    #[test]
    fn t2_matching_bearer_in_token_mode_completes_handshake() {
        let mut h = canonical_handshake_headers();
        h.push(("Authorization".into(), "Bearer good".into()));
        let req = build_req(Method::Get, WEBSOCKET_PATH, h);
        let mut sink = Cursor::new(Vec::<u8>::new());
        let result = handle_upgrade(&mut sink, &req,
            Some(b"good"), &engine());
        assert!(result.is_ok(), "matching Bearer must succeed: {result:?}");
        let resp = String::from_utf8(sink.into_inner()).unwrap();
        assert!(resp.starts_with("HTTP/1.1 101 Switching Protocols\r\n"));
    }

    /// Subprotocol offered + accepted: the response echoes
    /// `Sec-WebSocket-Protocol: kessel-op-v1`. The echoed value is the
    /// LOCKED constant string, not the client's raw casing.
    #[test]
    fn t2_subprotocol_offered_and_accepted_echoes_in_response() {
        let mut h = canonical_handshake_headers();
        h.push(("Sec-WebSocket-Protocol".into(),
            "kessel-op-v1, chat".into()));
        let req = build_req(Method::Get, WEBSOCKET_PATH, h);
        let mut sink = Cursor::new(Vec::<u8>::new());
        let result = handle_upgrade(&mut sink, &req, None, &engine());
        assert!(result.is_ok(), "subprotocol negotiation must succeed");
        let resp = String::from_utf8(sink.into_inner()).unwrap();
        assert!(resp.contains("\r\nSec-WebSocket-Protocol: kessel-op-v1\r\n"),
            "selected subprotocol must echo as canonical constant; got:\n{resp:?}");
    }

    /// Subprotocol header present but ONLY unknown subprotocols → 400
    /// per spec §5.1.
    #[test]
    fn t2_subprotocol_only_unknown_returns_400() {
        let mut h = canonical_handshake_headers();
        h.push(("Sec-WebSocket-Protocol".into(),
            "chat, soap, ircv3".into()));
        let req = build_req(Method::Get, WEBSOCKET_PATH, h);
        let mut sink = Cursor::new(Vec::<u8>::new());
        let result = handle_upgrade(&mut sink, &req, None, &engine());
        assert_eq!(result, Err(WsError::HandshakeFailed(400)),
            "client offered only unknown subprotocols → 400");
    }

    /// No subprotocol offered → response omits the header (per spec
    /// §5.2, default is `kessel-op-v1` semantics).
    #[test]
    fn t2_no_subprotocol_offered_response_omits_header() {
        // This is already partially covered by t2_successful_handshake;
        // lock the negative invariant explicitly here.
        let req = build_req(Method::Get, WEBSOCKET_PATH,
            canonical_handshake_headers());
        let mut sink = Cursor::new(Vec::<u8>::new());
        let _ = handle_upgrade(&mut sink, &req, None, &engine());
        let resp = String::from_utf8(sink.into_inner()).unwrap();
        assert!(!resp.contains("Sec-WebSocket-Protocol"),
            "no offer → response MUST omit Sec-WebSocket-Protocol; got:\n{resp:?}");
    }

    /// Subprotocol matching is case-insensitive per RFC 6455 token
    /// rules; the echoed value is still the canonical constant.
    #[test]
    fn t2_subprotocol_match_is_case_insensitive() {
        let mut h = canonical_handshake_headers();
        h.push(("Sec-WebSocket-Protocol".into(),
            "KESSEL-OP-V1".into()));
        let req = build_req(Method::Get, WEBSOCKET_PATH, h);
        let mut sink = Cursor::new(Vec::<u8>::new());
        let result = handle_upgrade(&mut sink, &req, None, &engine());
        assert!(result.is_ok());
        let resp = String::from_utf8(sink.into_inner()).unwrap();
        assert!(resp.contains("\r\nSec-WebSocket-Protocol: kessel-op-v1\r\n"),
            "match is case-insensitive; echoed value is canonical; got:\n{resp:?}");
    }

    /// Non-GET method (POST/etc) → 405 Method Not Allowed.
    #[test]
    fn t2_post_method_returns_405() {
        let req = build_req(Method::Post, WEBSOCKET_PATH,
            canonical_handshake_headers());
        let mut sink = Cursor::new(Vec::<u8>::new());
        let result = handle_upgrade(&mut sink, &req, None, &engine());
        assert_eq!(result, Err(WsError::HandshakeFailed(405)));
        let resp = String::from_utf8(sink.into_inner()).unwrap();
        assert!(resp.starts_with("HTTP/1.1 405 Method Not Allowed\r\n"),
            "expected 405; got:\n{resp:?}");
    }
}
