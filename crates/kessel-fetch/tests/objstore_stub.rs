//! `fetch_rows_signed` over a localhost rustls stub. Only compiled
//! with `--features object-store`. Reuses the SP99 TLS fixture.
#![cfg(feature = "object-store")]

use kessel_catalog::FieldKind;
use kessel_fetch::{
    fetch_rows_signed, fetch_rows_signed_test, ColumnMap, Format,
    DEFAULT_MAX_BODY,
};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

const CERT_PEM: &[u8] = include_bytes!("fixtures/localhost.pem");
const KEY_PEM: &[u8] = include_bytes!("fixtures/localhost.key.pem");

fn server_config() -> Arc<rustls::ServerConfig> {
    let certs: Vec<_> =
        rustls_pemfile::certs(&mut std::io::BufReader::new(CERT_PEM))
            .collect::<Result<_, _>>()
            .unwrap();
    let key =
        rustls_pemfile::private_key(&mut std::io::BufReader::new(KEY_PEM))
            .unwrap()
            .unwrap();
    Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap(),
    )
}

fn stub(body: &'static str) -> (u16, Arc<std::sync::Mutex<String>>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let cfg = server_config();
    let seen = Arc::new(std::sync::Mutex::new(String::new()));
    let s2 = seen.clone();
    thread::spawn(move || {
        if let Ok((sock, _)) = l.accept() {
            let c = rustls::ServerConnection::new(cfg).unwrap();
            let mut tls = rustls::StreamOwned::new(c, sock);
            let mut buf = [0u8; 4096];
            let n = tls.read(&mut buf).unwrap_or(0);
            *s2.lock().unwrap() =
                String::from_utf8_lossy(&buf[..n]).into_owned();
            let _ = tls.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                )
                .as_bytes(),
            );
        }
    });
    (port, seen)
}

#[test]
fn fetch_rows_signed_passes_headers_and_decodes() {
    let (port, seen) = stub(r#"[{"id":42}]"#);
    let cols = vec![ColumnMap {
        name: "id".into(),
        kind: FieldKind::U32,
        source: "id".into(),
    }];
    let headers = vec![
        ("Authorization".to_string(), "AWS4-HMAC-SHA256 Test".to_string()),
        ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
    ];
    let rows = fetch_rows_signed_test(
        &format!("https://localhost:{port}/bucket/k.json"),
        &headers,
        Format::Json,
        &cols,
        None,
        DEFAULT_MAX_BODY,
        CERT_PEM,
    )
    .unwrap();
    assert_eq!(rows, vec![vec![vec![42, 0, 0, 0]]]);
    let req = seen.lock().unwrap().clone();
    assert!(req.contains("Authorization: AWS4-HMAC-SHA256 Test"), "{req}");
    assert!(req.contains("x-amz-date: 20130524T000000Z"), "{req}");
    assert!(req.starts_with("GET /bucket/k.json HTTP/1.1"), "{req}");
}

#[test]
fn fetch_rows_signed_non_https_is_typed_error() {
    let cols = vec![ColumnMap {
        name: "id".into(),
        kind: FieldKind::U32,
        source: "id".into(),
    }];
    let e = fetch_rows_signed(
        "http://localhost/x",
        &[],
        Format::Json,
        &cols,
        None,
        DEFAULT_MAX_BODY,
    )
    .unwrap_err();
    assert!(
        matches!(e, kessel_fetch::FetchError::Http(_)),
        "got {e:?}"
    );
}
