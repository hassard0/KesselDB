//! End-to-end: `REFRESH` of an `s3://` source. A localhost rustls
//! stub stands in for S3. The production router uses webpki-roots
//! full-verify (SP99) which does NOT trust the self-signed localhost
//! fixture, so REFRESH FAILS CLOSED with a typed SchemaError — this
//! proves the do_refresh → kessel_objstore sign → fetch_rows_signed
//! wiring is reached and the atomic-abort/fail-closed contract holds
//! (prior state intact). The TRUSTED signing+header-passthrough happy
//! path is covered at the kessel-fetch layer by objstore_stub.rs
//! (Task 4); injecting fixture trust into the production router would
//! be a forbidden bypass (SP99 precedent). Only compiled with
//! `--features external-sources-objstore`.
#![cfg(feature = "external-sources-objstore")]

use kessel_client::Client;
use kessel_proto::{Op, OpResult};
use kesseldb_server::cluster::{serve_clients, spawn_node};
use kesseldb_server::router::{serve_router, Router};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

const CERT_PEM: &[u8] =
    include_bytes!("../../kessel-fetch/tests/fixtures/localhost.pem");
const KEY_PEM: &[u8] =
    include_bytes!("../../kessel-fetch/tests/fixtures/localhost.key.pem");

fn spawn_shard(tag: &str) -> Vec<String> {
    let n = 3;
    let peers: Vec<TcpListener> = (0..n)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let paddrs: Vec<SocketAddr> =
        peers.iter().map(|l| l.local_addr().unwrap()).collect();
    let mut caddrs = Vec::new();
    for (i, pl) in peers.into_iter().enumerate() {
        let dir = std::env::temp_dir().join(format!(
            "kesseldb-extobjstore-{}-{tag}-{i}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let node = Arc::new(spawn_node(i, pl, paddrs.clone(), dir).unwrap());
        let cl = TcpListener::bind("127.0.0.1:0").unwrap();
        caddrs.push(cl.local_addr().unwrap().to_string());
        std::thread::spawn(move || serve_clients(cl, node));
    }
    caddrs
}

fn tls_stub(body: &'static str) -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let certs: Vec<_> =
        rustls_pemfile::certs(&mut std::io::BufReader::new(CERT_PEM))
            .collect::<Result<_, _>>()
            .unwrap();
    let key =
        rustls_pemfile::private_key(&mut std::io::BufReader::new(KEY_PEM))
            .unwrap()
            .unwrap();
    let cfg = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap(),
    );
    std::thread::spawn(move || {
        for conn in l.incoming() {
            let sock = match conn {
                Ok(s) => s,
                Err(_) => continue,
            };
            let c = match rustls::ServerConnection::new(cfg.clone()) {
                Ok(c) => c,
                Err(_) => continue,
            };
            let mut tls = rustls::StreamOwned::new(c, sock);
            let mut b = [0u8; 2048];
            let _ = tls.read(&mut b);
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
    port
}

#[test]
fn refresh_from_s3_endpoint_fails_closed_and_state_intact() {
    // The router's TLS client uses the production webpki-roots config,
    // which will NOT trust the localhost fixture cert. This e2e therefore
    // asserts the do_refresh → kessel_objstore::sign_get →
    // kessel_fetch::fetch_rows_signed path is wired and FAILS CLOSED on
    // an untrusted cert, and that the atomic-abort contract holds: prior
    // (empty) state is intact and SELECT works after the failed REFRESH.
    // The trusted happy-path (SigV4 headers present + HTTP 200 body
    // parsed) is proven at the kessel-fetch layer by objstore_stub.rs
    // (Task 4) — injecting fixture trust here would bypass SP99.

    // Credential env vars: values live in process env, names persisted in
    // the recipe (SP97/SP99 model).
    std::env::set_var("OBJ_T10_KEYID", "AKIAEXAMPLE");
    std::env::set_var("OBJ_T10_SECRET", "secretexamplekey");

    let port = tls_stub(r#"[{"id":7,"nm":"zed"}]"#);
    let shard = spawn_shard("b");
    let router = Arc::new(Router::new(vec![shard.clone()]));
    let rl = TcpListener::bind("127.0.0.1:0").unwrap();
    let raddr = rl.local_addr().unwrap();
    {
        let r = router.clone();
        std::thread::spawn(move || serve_router(rl, r));
    }
    std::thread::sleep(Duration::from_millis(1400));

    let mut sc = shard
        .iter()
        .find_map(|a| {
            Client::connect(a.parse::<SocketAddr>().unwrap()).ok()
        })
        .expect("connect shard");

    let ddl = format!(
        "CREATE EXTERNAL SOURCE feed (\
           id U64 NOT NULL FROM 'id', nm CHAR(16) NOT NULL FROM 'nm'\
         ) FROM 's3://bucket/data.json' FORMAT JSON KEY id \
         REGION 'us-east-1' \
         ENDPOINT 'https://127.0.0.1:{port}' \
         AUTH OBJSTORE S3 KEYID ENV 'OBJ_T10_KEYID' SECRET ENV 'OBJ_T10_SECRET'"
    );
    assert!(
        matches!(
            sc.sql(&ddl).expect("ddl wire"),
            OpResult::Ok | OpResult::TypeCreated(_)
        ),
        "CREATE EXTERNAL SOURCE must succeed (URL is opaque)"
    );

    let mut rc = Client::connect(raddr).expect("connect router");
    let res = rc
        .call(&Op::RefreshExternalSource { name: "feed".into() })
        .expect("refresh wire");

    // Untrusted self-signed cert ⇒ typed failure surfaced at REFRESH.
    // The TLS handshake fails before any request bytes are sent, so the
    // stub may receive no data — only the SchemaError matters here.
    // Error wrapping:
    //   fetch_rows_signed error  → "refresh: {e}"
    //   sign_get error           → "REFRESH `feed`: sign: {e}"
    //   connect/TLS error        → contained in the fetch error ↑
    match &res {
        OpResult::SchemaError(msg) => assert!(
            msg.contains("refresh:")
                || msg.contains("sign:")
                || msg.to_lowercase().contains("tls")
                || msg.to_lowercase().contains("connect"),
            "REFRESH must fail via the do_refresh objstore fetch path, got SchemaError({msg:?})"
        ),
        other => panic!(
            "REFRESH over untrusted https objstore must fail typed SchemaError, got {other:?}"
        ),
    }

    // Atomic abort held: SELECT still works and returns no rows.
    let blob = match sc.sql("SELECT * FROM feed").expect("select wire") {
        OpResult::Got(b) => b,
        o => panic!("SELECT: {o:?}"),
    };
    assert!(blob.is_empty(), "no rows must have been materialized");
}
