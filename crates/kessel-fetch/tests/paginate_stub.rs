//! Integration tests for `fetch_rows_paginated`: a localhost stub that
//! serves a queue of `(extra_headers, body)` pairs, one per connection.
use kessel_catalog::FieldKind;
use kessel_fetch::{
    fetch_rows_paginated, Auth, ColumnMap, FetchError, Format, Pagination,
    DEFAULT_MAX_BODY,
};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// Spin up a stub on 127.0.0.1:0 serving `pages` (extra_headers, body)
/// pairs in order, one per accepted connection. Returns the bound port
/// and the server thread handle (join it at the end of each test).
fn stub(pages: Vec<(String, String)>) -> (u16, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let queue = Arc::new(Mutex::new(pages));
    let h = std::thread::spawn(move || {
        loop {
            let remaining = {
                let q = queue.lock().expect("lock");
                q.len()
            };
            if remaining == 0 {
                break;
            }
            let (mut conn, _) = match listener.accept() {
                Ok(c) => c,
                Err(_) => break,
            };
            let (extra, body) = {
                let mut q = queue.lock().expect("lock");
                if q.is_empty() {
                    break;
                }
                q.remove(0)
            };
            drain_request(&mut conn);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nConnection: close\r\n{extra}\
                 Content-Length: {}\r\n\r\n{body}",
                body.as_bytes().len()
            );
            let _ = conn.write_all(resp.as_bytes());
            let _ = conn.flush();
        }
    });
    (port, h)
}

/// Read the HTTP request up to the blank line so the client's write
/// completes before we respond (with Connection: close).
fn drain_request(conn: &mut TcpStream) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        match conn.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

fn id_cols() -> Vec<ColumnMap> {
    vec![ColumnMap {
        name: "id".into(),
        kind: FieldKind::U32,
        source: "id".into(),
    }]
}

#[test]
fn next_url_json_walks_pages() {
    // Bind to learn a free port, drop it, then start the stub on that
    // same port so page1's next URL can embed the real port.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    let page1 = format!(
        r#"{{"items":[{{"id":1}}],"pg":{{"next":"http://127.0.0.1:{port}/p2"}}}}"#
    );
    let page2 = r#"{"items":[{"id":2}],"pg":{"next":null}}"#.to_string();
    let (real_port, h) = stub_at(port, vec![
        (String::new(), page1),
        (String::new(), page2),
    ]);
    let base = format!("http://127.0.0.1:{real_port}/p1");
    let got = fetch_rows_paginated(
        &base,
        &Auth::None,
        Format::Json,
        &id_cols(),
        Some("items"),
        &Pagination::NextUrlJson("pg.next".into()),
        DEFAULT_MAX_BODY,
    )
    .expect("paginated");
    h.join().ok();
    assert_eq!(got, vec![vec![vec![1, 0, 0, 0]], vec![vec![2, 0, 0, 0]]]);
}

/// Stub bound to a specific (already-known-free) port so callers can
/// embed the port into a page body before the server starts.
fn stub_at(port: u16, pages: Vec<(String, String)>) -> (u16, JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", port)).expect("rebind");
    let port = listener.local_addr().expect("addr").port();
    let queue = Arc::new(Mutex::new(pages));
    let h = std::thread::spawn(move || loop {
        let remaining = {
            let q = queue.lock().expect("lock");
            q.len()
        };
        if remaining == 0 {
            break;
        }
        let (mut conn, _) = match listener.accept() {
            Ok(c) => c,
            Err(_) => break,
        };
        let (extra, body) = {
            let mut q = queue.lock().expect("lock");
            if q.is_empty() {
                break;
            }
            q.remove(0)
        };
        drain_request(&mut conn);
        let resp = format!(
            "HTTP/1.1 200 OK\r\nConnection: close\r\n{extra}\
             Content-Length: {}\r\n\r\n{body}",
            body.as_bytes().len()
        );
        let _ = conn.write_all(resp.as_bytes());
        let _ = conn.flush();
    });
    (port, h)
}

#[test]
fn next_link_header_walks_then_stops() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    let link = format!(
        "Link: <http://127.0.0.1:{port}/p2>; rel=\"next\"\r\n"
    );
    let (port, h) = stub_at(
        port,
        vec![
            (link, r#"[{"id":5}]"#.into()),
            (String::new(), r#"[{"id":6}]"#.into()),
        ],
    );
    let base = format!("http://127.0.0.1:{port}/p1");
    let got = fetch_rows_paginated(
        &base,
        &Auth::None,
        Format::Json,
        &id_cols(),
        None,
        &Pagination::NextLink,
        DEFAULT_MAX_BODY,
    )
    .expect("paginated");
    h.join().ok();
    assert_eq!(got, vec![vec![vec![5, 0, 0, 0]], vec![vec![6, 0, 0, 0]]]);
}

#[test]
fn cursor_token_into_param() {
    let (port, h) = stub(vec![
        (
            String::new(),
            r#"{"items":[{"id":7}],"meta":{"cur":"C2"}}"#.into(),
        ),
        (
            String::new(),
            r#"{"items":[{"id":8}],"meta":{"cur":null}}"#.into(),
        ),
    ]);
    let base = format!("http://127.0.0.1:{port}/feed");
    let got = fetch_rows_paginated(
        &base,
        &Auth::None,
        Format::Json,
        &id_cols(),
        Some("items"),
        &Pagination::CursorJson {
            path: "meta.cur".into(),
            param: "cursor".into(),
        },
        DEFAULT_MAX_BODY,
    )
    .expect("paginated");
    h.join().ok();
    assert_eq!(got, vec![vec![vec![7, 0, 0, 0]], vec![vec![8, 0, 0, 0]]]);
}

#[test]
fn loop_detection_is_typed_error() {
    // Every page points back at the base URL => the loop guard must
    // fire on the second iteration (URL already seen). Queue >=2
    // identical pages so the guard triggers before the queue drains.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    let body = format!(
        r#"{{"items":[{{"id":1}}],"pg":{{"next":"http://127.0.0.1:{port}/loop"}}}}"#
    );
    let (port, h) = stub_at(
        port,
        vec![
            (String::new(), body.clone()),
            (String::new(), body.clone()),
            (String::new(), body),
        ],
    );
    let base = format!("http://127.0.0.1:{port}/loop");
    let got = fetch_rows_paginated(
        &base,
        &Auth::None,
        Format::Json,
        &id_cols(),
        Some("items"),
        &Pagination::NextUrlJson("pg.next".into()),
        DEFAULT_MAX_BODY,
    );
    // Drain whatever the server still has queued so its thread exits.
    let _ = TcpStream::connect(("127.0.0.1", port));
    let _ = TcpStream::connect(("127.0.0.1", port));
    h.join().ok();
    assert!(matches!(got, Err(FetchError::Http(_))), "got {got:?}");
}

#[test]
fn ndjson_link_pagination() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    let link = format!(
        "Link: <http://127.0.0.1:{port}/p2>; rel=\"next\"\r\n"
    );
    let (port, h) = stub_at(
        port,
        vec![
            (link, "{\"id\":3}\n".into()),
            (String::new(), "{\"id\":4}\n".into()),
        ],
    );
    let base = format!("http://127.0.0.1:{port}/p1");
    let got = fetch_rows_paginated(
        &base,
        &Auth::None,
        Format::Ndjson,
        &id_cols(),
        None,
        &Pagination::NextLink,
        DEFAULT_MAX_BODY,
    )
    .expect("paginated");
    h.join().ok();
    assert_eq!(got, vec![vec![vec![3, 0, 0, 0]], vec![vec![4, 0, 0, 0]]]);
}
