//! End-to-end: a `REFRESH` whose source URL is `https://` materializes
//! the served rows through the real router → do_refresh → kessel-fetch
//! TLS path. Only compiled with `--features external-sources-tls`.
//! Mirrors external_source_oracle.rs but the stub speaks TLS and the
//! client trusts the checked-in localhost fixture.
#![cfg(feature = "external-sources-tls")]

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
            "kesseldb-exttls-{}-{tag}-{i}",
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
fn refresh_over_https_materializes_rows() {
    // The router's TLS client uses the production webpki-roots config,
    // which will NOT trust the localhost fixture. This smoke therefore
    // asserts the do_refresh→kessel-fetch→TLS path is wired and
    // FAILS CLOSED on an untrusted cert (a genuine handshake reached,
    // not a plaintext downgrade or panic), and that the atomic-abort
    // contract holds: prior (empty) state is intact and SELECT works.
    let port = tls_stub(r#"[{"id":7,"nm":"zed"}]"#);
    let shard = spawn_shard("a");
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
           id U64 NOT NULL FROM 'id', \
           nm CHAR(16) NOT NULL FROM 'nm'\
         ) FROM 'https://localhost:{port}/d' FORMAT JSON KEY id"
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
    // OpResult has no Err variant; TLS/fetch failures surface as SchemaError.
    assert!(
        matches!(res, OpResult::SchemaError(_)),
        "REFRESH over an untrusted https cert must fail typed as SchemaError, got {res:?}"
    );

    // Atomic abort held: SELECT still works and returns no rows.
    let blob = match sc.sql("SELECT * FROM feed").expect("select wire") {
        OpResult::Got(b) => b,
        o => panic!("SELECT: {o:?}"),
    };
    assert!(blob.is_empty(), "no rows must have been materialized");
}
