//! Shared test helpers used by both e2e_curl.rs and pentest.rs. See the
//! Cargo book chapter on "Tests" / "Submodules in integration tests" for
//! the `tests/<name>/mod.rs` convention (NOT `tests/common.rs`, which Cargo
//! treats as its own test binary).
#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::TcpStream;

pub struct TempDirGuard(pub std::path::PathBuf);

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        // On Windows the data dir may still be in use by the engine thread
        // (which `serve_cfg` blocks forever in), so remove_dir_all can fail
        // with EBUSY. Acceptable — orphaned temp dirs are bounded at one
        // per test run and cleaned up by the OS on reboot.
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn temp_data_dir() -> std::path::PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let p = std::env::temp_dir().join(format!("kesseldb-sp141-{pid}-{nanos}"));
    std::fs::create_dir_all(&p).unwrap();
    p
}

pub fn spawn_server() -> (std::net::SocketAddr, TempDirGuard) {
    spawn_server_with_token(None)
}

pub fn spawn_server_with_token(
    token: Option<Vec<u8>>,
) -> (std::net::SocketAddr, TempDirGuard) {
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
    });
    wait_for_listener(http_addr);
    (http_addr, guard)
}

/// SP142 T2: wait for the gateway listener to bind before the first
/// connect. Replaces the prior 150ms thread::sleep — adaptive (returns
/// on first successful connect) and capped (panics after 500ms so a
/// truly broken listener fails fast instead of hanging the test run).
///
/// First iteration on a fast machine succeeds in ~1-5ms (30× faster than
/// the prior 150ms sleep × ~25 calls). On a slow CI runner that takes
/// 200ms to bind, ~20 iterations × 10ms = 200ms — still adaptive.
fn wait_for_listener(addr: std::net::SocketAddr) {
    for _ in 0..50 {
        if std::net::TcpStream::connect_timeout(
            &addr,
            std::time::Duration::from_millis(50),
        ).is_ok() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("gateway listener never bound: {addr}");
}

/// Single-shot request → full response read (server sends Connection: close).
pub fn raw_request(addr: std::net::SocketAddr, req: &[u8]) -> Vec<u8> {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
    s.set_write_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
    s.write_all(req).unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).unwrap();
    buf
}

/// After an adversarial request, send a benign GET /v1/health on a FRESH
/// TcpStream. Asserts the listener still accepts the next connection
/// (proves adversarial input did NOT corrupt listener state).
pub fn assert_listener_alive(addr: std::net::SocketAddr) {
    let resp = raw_request(
        addr,
        b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
    );
    let text = String::from_utf8_lossy(&resp);
    assert!(
        text.starts_with("HTTP/1.1 200"),
        "listener died after adversarial input — follow-up GET /v1/health returned: {text}",
    );
}
