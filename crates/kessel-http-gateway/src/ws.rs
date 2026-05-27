//! SP-WS — WebSocket support for the KesselDB HTTP gateway (RFC 6455).
//!
//! **This is the T1 scaffold.** No live wire code yet — the module is
//! the surface area that T2-T6 fill in. Calling `handle_upgrade` today
//! returns `Err(WsError::NotYetImplemented)`; the per-connection HTTP
//! loop must NOT route to this module until T2 lands the handshake
//! parser.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-26-kesseldb-spws-websocket-design.md`
//!
//! ## Task decomposition (mirrors spec §10)
//!
//! - **T1** (this commit) — design spec + scaffold + crypto helper +
//!   `Sec-WebSocket-Accept` KATs locking the wire-protocol invariants
//! - **T2** — handshake parser: validate the upgrade request headers
//!   (Upgrade, Connection, Sec-WebSocket-Key, Sec-WebSocket-Version=13,
//!   path=/v1/ws, Authorization), build the 101 Switching Protocols
//!   response bytes, hand the hijacked TcpStream off to the session
//!   loop
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

use crate::engine::EngineApply;
use crate::parse::Request;
use std::io::{Read, Write};
use std::sync::Arc;

/// Spec §7: per-WebSocket-connection bounded send queue depth. Chosen
/// larger than SP-A's `SHARD_BACKPRESSURE_BOUND=4` because a
/// WebSocket's send queue is shared across data frames AND control
/// frames (ping/pong/close). Reserving 12 slots for data + a few for
/// control avoids head-of-line blocking during a slow drain. T5 wires
/// this; today it's a locked constant for forward reference.
pub const WS_SEND_QUEUE_BOUND: usize = 16;

/// Spec §6.1: the dedicated WebSocket upgrade path. The gateway's
/// route table (`parse::is_known_path`) does NOT include this yet —
/// T2 adds it alongside the handshake parser. Locked here so the value
/// can be referenced by the spec + future tests without magic strings.
pub const WEBSOCKET_PATH: &str = "/v1/ws";

/// Spec §5.1: the only subprotocol the V1 server advertises. Clients
/// MAY name it in `Sec-WebSocket-Protocol`; clients MAY omit the header
/// (browsers default `new WebSocket(url)` does this) and still get the
/// same semantics. Future versions can negotiate `kessel-op-v2-*`
/// alongside without breaking V1 consumers.
pub const SUBPROTOCOL_V1: &str = "kessel-op-v1";

/// Outcomes the upgrade handler may report. T1 only emits
/// `NotYetImplemented`; T2 expands this enum with the handshake-
/// failure variants the response writer maps to HTTP status codes.
#[derive(Debug)]
pub enum WsError {
    /// T1 scaffold sentinel — the upgrade arm is wired into
    /// `routes.rs` as a NO-OP that returns this error so the caller's
    /// response-writing path remains the existing HTTP error path. T2
    /// replaces every callsite that emits this with a real handshake
    /// response.
    NotYetImplemented,
}

/// Spec §6.2: the dispatch entry point from `routes::handle`.
///
/// **T1 placeholder.** Calling this returns `Err(WsError::NotYetImplemented)`
/// without touching the stream — the per-connection HTTP loop in
/// `server::handle_one_stream` MUST NOT route to this module until T2
/// makes it functional. The function signature is fixed here so T2 can
/// be a contained patch (one module body change + one routes.rs arm
/// flip + one Cargo flag) rather than a refactor.
///
/// `stream` is the TcpStream the HTTP/1.1 parser was reading from. Once
/// T2 sends the 101 Switching Protocols response, the stream is no
/// longer HTTP — it carries WebSocket frames. The session loop (T5)
/// owns the stream from that point forward; the caller MUST close the
/// HTTP keep-alive loop after this returns.
///
/// `req` is the parsed HTTP/1.1 upgrade request — headers carry
/// `Authorization: Bearer`, `Sec-WebSocket-Key`,
/// `Sec-WebSocket-Version`, etc.
///
/// `token` is the optional Bearer (open-mode passes None). Same
/// constant-time compare as the HTTP routes.
///
/// `engine` is the same `Arc<dyn EngineApply>` the HTTP routes use —
/// the WS dispatch will `apply_op` against this.
pub fn handle_upgrade<S: Read + Write>(
    _stream: &mut S,
    _req: &Request<'_>,
    _token: Option<&[u8]>,
    _engine: &Arc<dyn EngineApply>,
) -> Result<(), WsError> {
    // T2 lands the real handshake validation + response write here.
    // Until then: surface the not-implemented marker; the caller in
    // routes.rs (T2) will translate this to an HTTP 501 response. T1
    // leaves the caller side untouched so a wrong-by-accident dispatch
    // is impossible.
    Err(WsError::NotYetImplemented)
}

/// Spec §6.2: detect whether the parsed HTTP request is a WebSocket
/// upgrade attempt. Returns true iff the request carries
/// `Upgrade: websocket` (case-insensitive, RFC 9110 §7.8) AND
/// `Connection: Upgrade` token in the comma-separated list (RFC 9110
/// §7.6.1). T2 widens this to also check version=13 and key presence;
/// T1 ships the minimal version so the `routes.rs` arm has a stable
/// predicate to gate on.
pub fn is_websocket_upgrade(headers: &[(String, String)]) -> bool {
    let mut has_upgrade_websocket = false;
    let mut has_connection_upgrade = false;
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("upgrade") {
            // RFC 9110 §7.8 — Upgrade is a comma-separated list of
            // protocol tokens; we only accept `websocket`.
            for token in value.split(',') {
                if token.trim().eq_ignore_ascii_case("websocket") {
                    has_upgrade_websocket = true;
                    break;
                }
            }
        } else if name.eq_ignore_ascii_case("connection") {
            for token in value.split(',') {
                if token.trim().eq_ignore_ascii_case("upgrade") {
                    has_connection_upgrade = true;
                    break;
                }
            }
        }
    }
    has_upgrade_websocket && has_connection_upgrade
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Spec §6.1 — the WebSocket upgrade path is `/v1/ws`. Locked here
    /// so a typo (e.g. `/v1/websocket`) surfaces as a test failure
    /// instead of a silent 404 for every browser client.
    #[test]
    fn websocket_path_is_v1_ws() {
        assert_eq!(WEBSOCKET_PATH, "/v1/ws",
            "spec §6.1 locks the WebSocket upgrade path");
    }

    /// Spec §5.1 — the V1 subprotocol name is `kessel-op-v1`. Locked
    /// to prevent silent rename (the value travels back to clients in
    /// `Sec-WebSocket-Protocol`).
    #[test]
    fn subprotocol_v1_name_is_kessel_op_v1() {
        assert_eq!(SUBPROTOCOL_V1, "kessel-op-v1",
            "spec §5.1 locks the V1 subprotocol name");
    }

    /// Spec §7 — the per-connection send-queue bound is 16. Locked so
    /// a change to this value MUST update the spec rationale
    /// (control-frame headroom + browser burst tolerance).
    #[test]
    fn ws_send_queue_bound_is_sixteen_per_spec() {
        assert_eq!(WS_SEND_QUEUE_BOUND, 16,
            "spec §7 locks WS_SEND_QUEUE_BOUND = 16");
    }

    /// `is_websocket_upgrade` requires BOTH `Upgrade: websocket` AND
    /// `Connection: Upgrade` (RFC 6455 §4.1). Locks the happy-path
    /// predicate the T2 routes.rs arm will gate on.
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
            "canonical RFC 6455 §1.3 handshake headers must register as a WS upgrade");
    }

    /// Connection header is a comma-separated list per RFC 9110 §7.6.1.
    /// `keep-alive, Upgrade` (the shape browsers emit) MUST register
    /// the upgrade.
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

    /// `Connection: Upgrade` without `Upgrade: websocket` is NOT a
    /// WebSocket upgrade — it's some other protocol upgrade attempt.
    /// Lock the negative case so a future loosening doesn't silently
    /// admit non-WS upgrades into the WS handler.
    #[test]
    fn is_websocket_upgrade_rejects_missing_upgrade_websocket() {
        let headers = vec![
            ("Host".into(), "h".into()),
            ("Connection".into(), "Upgrade".into()),
        ];
        assert!(!is_websocket_upgrade(&headers),
            "Connection: Upgrade without Upgrade: websocket is NOT a WS upgrade");
    }

    /// `Upgrade: websocket` without `Connection: Upgrade` is malformed
    /// per RFC 6455 §4.1 (the Connection header must list the Upgrade
    /// token). T2 will reject this with a 400 at the handshake; T1's
    /// predicate excludes it so the T2 router never even calls
    /// `handle_upgrade` on a malformed request.
    #[test]
    fn is_websocket_upgrade_rejects_missing_connection_upgrade() {
        let headers = vec![
            ("Host".into(), "h".into()),
            ("Upgrade".into(), "websocket".into()),
        ];
        assert!(!is_websocket_upgrade(&headers),
            "Upgrade: websocket without Connection: Upgrade is NOT a WS upgrade");
    }

    /// Case-insensitivity: the canonical form is `Upgrade: websocket`
    /// + `Connection: Upgrade`, but RFC 9110 §5.1 says header field
    /// names are case-insensitive and tokens like `websocket` /
    /// `Upgrade` are too. Lock both axes.
    #[test]
    fn is_websocket_upgrade_is_case_insensitive() {
        let headers = vec![
            ("upgrade".into(), "WebSocket".into()),
            ("connection".into(), "upgrade".into()),
        ];
        assert!(is_websocket_upgrade(&headers),
            "header name + token case-insensitivity per RFC 9110 §5.1");
    }

    /// T1 placeholder: `handle_upgrade` returns `NotYetImplemented`
    /// without touching the stream. **Regression-lock against
    /// accidentally wiring this scaffold into production before T2
    /// ships.** T2 MUST update this test alongside the real handshake
    /// response — flipping this lock is the gate that catches a
    /// half-shipped T2.
    #[test]
    fn t1_handle_upgrade_returns_not_yet_implemented_stub() {
        use crate::engine::EngineApply;
        use kessel_proto::{Op, OpResult};
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
        let engine: Arc<dyn EngineApply> = Arc::new(NullEngine);
        // Read+Write stream stand-in — std::io::Cursor<Vec<u8>> implements
        // both for this test's needs (T1 placeholder doesn't actually
        // read or write).
        let mut sink = std::io::Cursor::new(Vec::<u8>::new());
        // Build a minimal Request<'_> for the test. We hand-build it
        // because parse_request requires a full HTTP/1.1 byte stream;
        // the placeholder doesn't inspect any fields.
        let headers = vec![
            ("Host".into(), "h".into()),
            ("Upgrade".into(), "websocket".into()),
            ("Connection".into(), "Upgrade".into()),
        ];
        let req = Request {
            method: crate::parse::Method::Get,
            path: WEBSOCKET_PATH,
            host: "h".into(),
            content_type: None,
            content_length: None,
            chunked: false,
            body: std::borrow::Cow::Borrowed(&[]),
            consumed: 0,
            headers,
        };
        let result = handle_upgrade(&mut sink, &req, None, &engine);
        assert!(matches!(result, Err(WsError::NotYetImplemented)),
            "T1 scaffold MUST return NotYetImplemented; T2 replaces with real handshake");
        assert!(sink.get_ref().is_empty(),
            "T1 scaffold MUST NOT write anything to the stream");
    }
}
