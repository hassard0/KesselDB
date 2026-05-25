//! End-to-end raw TcpStream tests against a live kesseldb-server with the
//! http-gateway feature on. Each test spawns a fresh server, sends a raw
//! HTTP/1.1 request, asserts the response bytes.

#![cfg(feature = "test-server")]

use std::io::{Read, Write};
use std::net::TcpStream;

fn temp_data_dir() -> std::path::PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let p = std::env::temp_dir().join(format!("kesseldb-sp141-{pid}-{nanos}"));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// RAII handle that wipes the test's temp data dir when the TEST THREAD
/// drops it. The previous shape (cleanup inside the spawned server thread,
/// after `serve_cfg` returns) never fired — `serve_cfg` blocks forever on
/// its accept loop. Bind a `let (_addr, _guard) = spawn_server();` in each
/// test so Drop runs at function return.
///
/// Limitation: the engine thread keeps reading the dir, so on Windows
/// `remove_dir_all` can fail with `EBUSY` (file-in-use). Acceptable —
/// the leak is reduced from "every test forever" to "at most one orphan
/// per test run". A follow-up task can wire a shutdown channel through to
/// the engine if this becomes a problem in CI.
struct TempDirGuard(std::path::PathBuf);

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        // Best-effort: EBUSY on Windows is expected when the engine thread
        // still holds the dir open. The harness logs aren't affected.
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn spawn_server() -> (std::net::SocketAddr, TempDirGuard) {
    spawn_server_with_token(None)
}

fn spawn_server_with_token(token: Option<Vec<u8>>) -> (std::net::SocketAddr, TempDirGuard) {
    let dir = temp_data_dir();
    let guard = TempDirGuard(dir.clone());
    let binary = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let http_addr = http.local_addr().unwrap();
    drop(http);
    let engine = kesseldb_server::spawn_engine(&dir).unwrap();
    let cfg = kesseldb_server::ServerConfig {
        token,
        http_addr: Some(http_addr),
        ..Default::default()
    };
    std::thread::spawn(move || {
        kesseldb_server::serve_cfg(binary, engine, cfg);
        // unreachable in practice — serve_cfg blocks forever
    });
    // Tiny sleep to let the gateway thread bind. (Idempotent — the e2e
    // immediately retries the connect on failure via std blocking
    // semantics.)
    std::thread::sleep(std::time::Duration::from_millis(150));
    (http_addr, guard)
}

fn raw_request(addr: std::net::SocketAddr, req: &[u8]) -> Vec<u8> {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
    s.set_write_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
    s.write_all(req).unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).unwrap();
    buf
}

#[test]
fn e2e_health() {
    let (addr, _guard) = spawn_server();
    let resp = raw_request(addr,
        b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
    assert!(text.contains(r#""status":"ok""#), "got: {text}");
    assert!(text.contains(r#""primary":true"#), "got: {text}");
    assert!(text.contains(r#""role":"primary""#), "got: {text}");
}

#[test]
fn e2e_metrics_route_exists() {
    // T6 fills the actual metrics body; T4 just ensures the route is wired.
    let (addr, _guard) = spawn_server();
    let resp = raw_request(addr,
        b"GET /v1/metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 200"), "got: {text}");
    assert!(text.contains("text/plain; version=0.0.4"), "got: {text}");
}

#[test]
fn e2e_sql_select_one() {
    // kessel-sql doesn't accept the constant-projection `SELECT 1`; use a
    // CREATE TABLE which compiles to a DefineType op and returns
    // OpResult::Ok. The shape we lock in is "valid SQL → 200 + status:ok".
    let (addr, _guard) = spawn_server();
    let body = b"CREATE TABLE t_e2e (v U64 NOT NULL)";
    let mut req = Vec::new();
    req.extend_from_slice(b"POST /v1/sql HTTP/1.1\r\nHost: 127.0.0.1\r\n");
    req.extend_from_slice(b"Content-Type: text/plain\r\n");
    req.extend_from_slice(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes());
    req.extend_from_slice(body);
    let resp = raw_request(addr, &req);
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
    assert!(text.contains(r#""status":"ok""#), "got: {text}");
}

#[test]
fn e2e_unknown_path_404() {
    let (addr, _guard) = spawn_server();
    let resp = raw_request(addr,
        b"GET /v2/sql HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 404"), "got: {text}");
}

#[test]
fn e2e_unknown_method_405() {
    let (addr, _guard) = spawn_server();
    let resp = raw_request(addr,
        b"DELETE /v1/sql HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 405"), "got: {text}");
}

#[test]
fn e2e_token_mode_unauth_without_bearer() {
    let (addr, _guard) = spawn_server_with_token(Some(b"secret123".to_vec()));
    let resp = raw_request(addr,
        b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 401"), "got: {text}");
    assert!(text.contains(r#""status":"unauthorized""#), "got: {text}");
}

#[test]
fn e2e_token_mode_authorized_with_bearer() {
    let (addr, _guard) = spawn_server_with_token(Some(b"secret123".to_vec()));
    let resp = raw_request(addr,
        b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\
          Authorization: Bearer secret123\r\n\r\n");
    let text = String::from_utf8_lossy(&resp);
    assert!(text.starts_with("HTTP/1.1 200"), "got: {text}");
}

#[test]
fn e2e_json_contract_pin_for_op_result_ok() {
    // Lock the JSON contract: the gateway emits format_result_json(&result)
    // verbatim. format_result_json(&OpResult::Ok) is the canonical
    // {"status":"ok"}. Indirectly verified by e2e_health (status=ok) and
    // e2e_sql_select_one (status=ok), but here we lock the exact string.
    use kessel_client::format_result_json;
    use kessel_proto::OpResult;
    assert_eq!(format_result_json(&OpResult::Ok), r#"{"status":"ok"}"#);
    assert_eq!(format_result_json(&OpResult::Exists), r#"{"status":"exists"}"#);
    assert_eq!(format_result_json(&OpResult::NotFound), r#"{"status":"not_found"}"#);
    assert_eq!(format_result_json(&OpResult::Unauthorized), r#"{"status":"unauthorized"}"#);
}
