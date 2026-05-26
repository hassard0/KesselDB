//! End-to-end raw TcpStream tests against a live kesseldb-server with the
//! http-gateway feature on. Each test spawns a fresh server, sends a raw
//! HTTP/1.1 request, asserts the response bytes.

#![cfg(feature = "test-server")]

mod common;
use common::{raw_request, spawn_server, spawn_server_with_token};

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
fn e2e_unauthorized_messages_distinguish_auth_layer_vs_engine() {
    // SP144H T3: 401 JSON body's `message` field differs by source.
    // Auth-layer: "missing bearer" / "bearer mismatch"
    // Engine: "engine denied"
    //
    // V1 scope: we verify the auth-layer disambig directly via the
    // running server. The engine-side OpResult::Unauthorized requires a
    // test that triggers an engine rejection, which the current test
    // infrastructure doesn't easily produce (the engine doesn't have a
    // built-in ACL path that returns Unauthorized for a benign request).
    // So this test verifies the two auth-layer messages; the engine-side
    // path is exercised by source-level audit (write_op_result maps
    // OpResult::Unauthorized → message="engine denied").
    let (addr, _g) = spawn_server_with_token(Some(b"secret123".to_vec()));

    // 1. No Bearer: missing bearer
    let r1 = raw_request(addr,
        b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
    let t1 = String::from_utf8_lossy(&r1);
    assert!(t1.starts_with("HTTP/1.1 401"), "got: {t1}");
    assert!(t1.contains(r#""message":"missing bearer""#), "got: {t1}");

    // 2. Wrong Bearer: bearer mismatch
    let r2 = raw_request(addr,
        b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\
          Authorization: Bearer wrongvalue\r\n\r\n");
    let t2 = String::from_utf8_lossy(&r2);
    assert!(t2.starts_with("HTTP/1.1 401"), "got: {t2}");
    assert!(t2.contains(r#""message":"bearer mismatch""#), "got: {t2}");
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

// =========================================================================
// SP147 T4: HTTP/1.1 keep-alive end-to-end. The legacy raw_request helper
// injects `Connection: close` to preserve single-shot semantic — these
// tests bypass it and drive the raw TcpStream directly so they actually
// exercise the per-connection loop in server::handle_one_stream.
//
// `read_one_response` is the bounded-read primitive: read until we have a
// complete response framed by Content-Length (the gateway emits one on every
// response). Plain `read(&mut buf)` is unsafe here because TCP can fragment
// the response across multiple read() calls — we'd assert on a partial
// "HTTP/1.1 " prefix.
// =========================================================================

#[cfg(test)]
fn read_one_response(s: &mut std::net::TcpStream) -> Vec<u8> {
    use std::io::Read;
    let mut out: Vec<u8> = Vec::new();
    let mut buf = [0u8; 4096];
    // Phase 1: read until we have the header terminator \r\n\r\n.
    let header_end = loop {
        if let Some(pos) = out.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        let n = s.read(&mut buf).expect("read headers");
        if n == 0 {
            panic!("EOF before complete response headers; got: {}",
                String::from_utf8_lossy(&out));
        }
        out.extend_from_slice(&buf[..n]);
    };
    // Phase 2: parse Content-Length from the headers.
    let head = std::str::from_utf8(&out[..header_end]).unwrap();
    let mut content_length: usize = 0;
    for line in head.split("\r\n") {
        if let Some(rest) = line
            .to_ascii_lowercase()
            .strip_prefix("content-length:")
        {
            content_length = rest.trim().parse().expect("Content-Length numeric");
            break;
        }
    }
    let total = header_end + content_length;
    while out.len() < total {
        let n = s.read(&mut buf).expect("read body");
        if n == 0 {
            panic!("EOF before complete body; want {} got {}",
                total, out.len());
        }
        out.extend_from_slice(&buf[..n]);
    }
    out.truncate(total);
    out
}

#[test]
fn keepalive_two_requests_same_connection() {
    use std::io::Write;
    use std::net::TcpStream;
    let (addr, _g) = spawn_server();
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
    s.set_write_timeout(Some(std::time::Duration::from_secs(5))).unwrap();

    // First request — keep-alive (no Connection header → HTTP/1.1 default
    // per RFC 9112 §9.3).
    s.write_all(b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n").unwrap();
    let r1_bytes = read_one_response(&mut s);
    let r1 = String::from_utf8_lossy(&r1_bytes);
    assert!(r1.starts_with("HTTP/1.1 200"), "got: {r1}");
    assert!(r1.contains("Connection: keep-alive"),
        "first response should keep-alive: {r1}");

    // Second request on the SAME socket — proves handle_one_stream's loop
    // serves a second request without the client opening a new connection.
    s.write_all(b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n").unwrap();
    let r2_bytes = read_one_response(&mut s);
    let r2 = String::from_utf8_lossy(&r2_bytes);
    assert!(r2.starts_with("HTTP/1.1 200"),
        "second response succeeded on same socket: {r2}");
    assert!(r2.contains("Connection: keep-alive"),
        "second response should keep-alive: {r2}");
}

#[test]
fn keepalive_explicit_close_closes_after_response() {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    let (addr, _g) = spawn_server();
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
    s.set_write_timeout(Some(std::time::Duration::from_secs(5))).unwrap();

    s.write_all(b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\
                  Connection: close\r\n\r\n").unwrap();
    // read_to_end works ONLY because the server closes after this response —
    // that's exactly what we're asserting. If keep-alive negotiation were
    // broken (server kept the connection), this read would hang to the 5s
    // timeout and panic.
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).unwrap();
    let r = String::from_utf8_lossy(&buf);
    assert!(r.starts_with("HTTP/1.1 200"), "got: {r}");
    assert!(r.contains("Connection: close"),
        "explicit close honored in response header: {r}");
}

#[test]
fn keepalive_legacy_http_keep_alive_token_recognized() {
    // RFC 9112 §9.3: `Connection: keep-alive` is HTTP/1.0 legacy, but
    // wants_close accepts it as an explicit affirmative for clarity.
    use std::io::Write;
    use std::net::TcpStream;
    let (addr, _g) = spawn_server();
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
    s.set_write_timeout(Some(std::time::Duration::from_secs(5))).unwrap();

    s.write_all(b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\
                  Connection: keep-alive\r\n\r\n").unwrap();
    let r_bytes = read_one_response(&mut s);
    let r = String::from_utf8_lossy(&r_bytes);
    assert!(r.starts_with("HTTP/1.1 200"), "got: {r}");
    assert!(r.contains("Connection: keep-alive"), "got: {r}");
}

#[test]
fn keepalive_many_requests_on_one_connection() {
    // SP147 T3: per-connection cap is 1000 by default. We don't loop to
    // 1001 here (slow + the cap behavior is the same shape regardless of
    // N); 100 sequential requests on one TcpStream is enough to prove the
    // loop actually loops and the buffer drain doesn't corrupt parsing.
    use std::io::Write;
    use std::net::TcpStream;
    let (addr, _g) = spawn_server();
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(std::time::Duration::from_secs(10))).unwrap();
    s.set_write_timeout(Some(std::time::Duration::from_secs(10))).unwrap();

    for i in 0..100 {
        s.write_all(b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n").unwrap();
        let r_bytes = read_one_response(&mut s);
        let r = String::from_utf8_lossy(&r_bytes);
        assert!(r.starts_with("HTTP/1.1 200"), "iter {i}: got: {r}");
        assert!(r.contains("Connection: keep-alive"),
            "iter {i}: should still be keep-alive: {r}");
    }
}
