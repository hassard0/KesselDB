//! Spins a real localhost TCP server returning a fixed body, then
//! drives the full fetch_rows path. No external network.
use kessel_catalog::FieldKind;
use kessel_fetch::{fetch_rows, Auth, ColumnMap, Format, DEFAULT_MAX_BODY};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

fn serve_once(body: &'static str, expect_auth: Option<&'static str>) -> (u16, thread::JoinHandle<()>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        let mut buf = [0u8; 2048];
        let n = s.read(&mut buf).unwrap();
        let req = String::from_utf8_lossy(&buf[..n]).to_string();
        if let Some(a) = expect_auth {
            assert!(req.contains(a), "missing auth header: {req}");
        }
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        s.write_all(resp.as_bytes()).unwrap();
    });
    (port, handle)
}

#[test]
fn json_over_http_with_bearer_round_trips() {
    let (port, handle) = serve_once(
        r#"[{"id":7,"name":"zed"}]"#,
        Some("Authorization: Bearer T0K"),
    );
    let cols = vec![
        ColumnMap { name: "id".into(), kind: FieldKind::U32, source: "id".into() },
        ColumnMap { name: "name".into(), kind: FieldKind::Char(8), source: "name".into() },
    ];
    let rows = fetch_rows(
        &format!("http://127.0.0.1:{port}/data"),
        &Auth::Bearer("T0K".into()),
        Format::Json,
        &cols,
        DEFAULT_MAX_BODY,
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], vec![7, 0, 0, 0]);
    assert_eq!(rows[0][1], b"zed\0\0\0\0\0".to_vec());
    handle.join().expect("stub server thread panicked");
}

#[test]
fn body_too_large_is_typed_error() {
    let (port, handle) = serve_once(r#"[{"id":1}]"#, None);
    let cols = vec![ColumnMap {
        name: "id".into(),
        kind: FieldKind::U32,
        source: "id".into(),
    }];
    let e = fetch_rows(
        &format!("http://127.0.0.1:{port}/d"),
        &Auth::None,
        Format::Json,
        &cols,
        4,
    )
    .unwrap_err();
    assert!(matches!(e, kessel_fetch::FetchError::TooLarge(4)));
    let _ = handle.join();
}

#[test]
fn truncated_chunked_body_is_typed_error_not_panic() {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let h = std::thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        let mut b = [0u8; 1024];
        let _ = std::io::Read::read(&mut s, &mut b);
        // chunked, declares 5 bytes, sends "hello" but NO trailing CRLF then closes
        let _ = std::io::Write::write_all(
            &mut s,
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello",
        );
    });
    let cols = vec![kessel_fetch::ColumnMap {
        name: "id".into(),
        kind: kessel_catalog::FieldKind::U32,
        source: "id".into(),
    }];
    let e = kessel_fetch::fetch_rows(
        &format!("http://127.0.0.1:{port}/d"),
        &kessel_fetch::Auth::None,
        kessel_fetch::Format::Json,
        &cols,
        kessel_fetch::DEFAULT_MAX_BODY,
    )
    .unwrap_err();
    assert!(matches!(e, kessel_fetch::FetchError::Http(_)), "expected Http err, got {e:?}");
    let _ = h.join();
}

#[test]
fn ndjson_over_http_round_trips() {
    let (port, h) = serve_once("{\"id\":3}\n{\"id\":4}\n", None);
    let cols = vec![
        ColumnMap { name: "id".into(), kind: FieldKind::U32, source: "id".into() },
    ];
    let rows = fetch_rows(
        &format!("http://127.0.0.1:{port}/d"),
        &Auth::None,
        Format::Ndjson,
        &cols,
        DEFAULT_MAX_BODY,
    )
    .unwrap();
    assert_eq!(rows, vec![vec![vec![3,0,0,0]], vec![vec![4,0,0,0]]]);
    let _ = h.join();
}

#[test]
fn get_resp_exposes_link_header() {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let h = std::thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        let mut b = [0u8; 512]; let _ = s.read(&mut b);
        let body = b"[]";
        let _ = s.write_all(format!(
            "HTTP/1.1 200 OK\r\nLink: <http://x/p2>; rel=\"next\"\r\nContent-Length: {}\r\n\r\n",
            body.len()).as_bytes());
        let _ = s.write_all(body);
    });
    let (headers, body) = kessel_fetch::http_get_resp_for_test(
        &format!("http://127.0.0.1:{port}/d"), kessel_fetch::DEFAULT_MAX_BODY);
    assert_eq!(body, b"[]");
    assert!(headers.iter().any(|(k,v)|
        k.eq_ignore_ascii_case("link") && v.contains("rel=\"next\"")),
        "headers were {headers:?}");
    let _ = h.join();
}

#[cfg(not(feature = "tls"))]
#[test]
fn https_without_tls_feature_is_typed_error_naming_the_feature() {
    // Default build (no `tls`): https:// must be a clean typed error
    // that names the feature to enable — never a panic, never a
    // silent plaintext downgrade.
    let cols = vec![ColumnMap {
        name: "id".into(),
        kind: FieldKind::U32,
        source: "id".into(),
    }];
    let e = fetch_rows(
        "https://example.invalid/d",
        &Auth::None,
        Format::Json,
        &cols,
        DEFAULT_MAX_BODY,
    )
    .unwrap_err();
    match e {
        kessel_fetch::FetchError::Http(m) => assert!(
            m.contains("external-sources-tls"),
            "message must name the feature, got: {m}"
        ),
        other => panic!("expected Http error, got {other:?}"),
    }
}
