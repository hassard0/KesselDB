//! Spins a real localhost TCP server returning a fixed body, then
//! drives the full fetch_rows path. No external network.
use kessel_catalog::FieldKind;
use kessel_fetch::{fetch_rows, Auth, ColumnMap, Format, DEFAULT_MAX_BODY};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

fn serve_once(body: &'static str, expect_auth: Option<&'static str>) -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    thread::spawn(move || {
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
    port
}

#[test]
fn json_over_http_with_bearer_round_trips() {
    let port = serve_once(
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
}

#[test]
fn body_too_large_is_typed_error() {
    let port = serve_once(r#"[{"id":1}]"#, None);
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
}
