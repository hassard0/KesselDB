//! End-to-end oracle for router-side `REFRESH` of an external source
//! (EXT slice 1). A real localhost stub HTTP server serves fixed bodies;
//! a 1-shard VSR cluster sits behind a `Router`. `CREATE EXTERNAL
//! SOURCE` / `SELECT` are driven as SQL straight at the shard (exactly
//! the `sql_over_cluster` e2e shape); `REFRESH` goes through the
//! `Router` so `do_refresh` runs and submits the captured rows back
//! through the replicated path.
//!
//! Asserts (none weakened):
//!   1. REFRESH materializes EXACTLY the served rows (independent model).
//!   2. Re-REFRESH with the identical body is idempotent (digest stable).
//!   3. A changed row is updated in place (same id, no duplicate).
//!   4. A schema-violating row aborts REFRESH atomically; prior data
//!      is byte-for-byte unchanged.
#![cfg(feature = "external-sources")]

use kessel_catalog::ObjectType;
use kessel_client::{Client, ClusterClient};
use kessel_proto::{Op, OpResult};
use kesseldb_server::cluster::{serve_clients, spawn_node};
use kesseldb_server::router::{serve_router, Router};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// --- a shard group = an independent 3-node VSR cluster (the proven
// configuration; a 1-node "cluster" never reaches a commit quorum).
// Lifted verbatim from the in-crate router test harness.
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
            "kesseldb-extoracle-{}-{tag}-{i}",
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

/// A localhost stub HTTP server that serves a queue of fixed bodies,
/// one per accepted connection, in order (model of `kessel-fetch`'s
/// `stub_server.rs`). Returns the port and a shared body queue the test
/// drives.
fn stub_server() -> (u16, Arc<Mutex<Vec<String>>>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let q = bodies.clone();
    std::thread::spawn(move || {
        for conn in l.incoming() {
            let mut s = match conn {
                Ok(s) => s,
                Err(_) => continue,
            };
            let mut buf = [0u8; 4096];
            let _ = s.read(&mut buf);
            let body = {
                let mut g = q.lock().unwrap();
                if g.is_empty() {
                    String::from("[]")
                } else {
                    g.remove(0)
                }
            };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = s.write_all(resp.as_bytes());
        }
    });
    (port, bodies)
}

/// Independent model: decode `SELECT * FROM feed` into a SET of
/// (id u128, name String) pairs. The filtered SELECT wire shape is
/// `[u32 len][record]*`; each record decodes against the type def.
fn select_set(shard: &mut Client, type_id: u32) -> Vec<(u64, String)> {
    let typedef = match shard
        .call(&Op::Describe { type_id })
        .expect("describe wire")
    {
        OpResult::Got(b) => b,
        o => panic!("describe: {o:?}"),
    };
    let (name, fields) = kessel_catalog::decode_type_def(&typedef).unwrap();
    let ot = ObjectType::from_def(name, fields);
    let blob = match shard.sql("SELECT * FROM feed").expect("select wire") {
        OpResult::Got(b) => b,
        o => panic!("SELECT * FROM feed: {o:?}"),
    };
    let mut out = Vec::new();
    let mut p = 0usize;
    while p + 4 <= blob.len() {
        let len =
            u32::from_le_bytes(blob[p..p + 4].try_into().unwrap()) as usize;
        p += 4;
        let rec = &blob[p..p + len];
        p += len;
        let vals = kessel_codec::decode(&ot, rec).unwrap();
        let id = match &vals[0] {
            kessel_codec::Value::Uint(u) => *u as u64,
            v => panic!("id not uint: {v:?}"),
        };
        let nm = match &vals[1] {
            kessel_codec::Value::Blob(b) => {
                let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
                String::from_utf8_lossy(&b[..end]).to_string()
            }
            v => panic!("nm not blob: {v:?}"),
        };
        out.push((id, nm));
    }
    assert_eq!(p, blob.len(), "SELECT blob fully consumed");
    out.sort();
    out
}

fn sorted(mut v: Vec<(u64, String)>) -> Vec<(u64, String)> {
    v.sort();
    v
}

#[test]
fn refresh_oracle_materializes_idempotent_upserts_and_atomic_abort() {
    let (port, bodies) = stub_server();
    let shard = spawn_shard("a");
    let router = Arc::new(Router::new(vec![shard.clone()]));
    let rl = TcpListener::bind("127.0.0.1:0").unwrap();
    let raddr = rl.local_addr().unwrap();
    {
        let r = router.clone();
        std::thread::spawn(move || serve_router(rl, r));
    }
    // Let the 3 nodes establish peer links + elect a primary.
    std::thread::sleep(Duration::from_millis(1400));

    // CREATE EXTERNAL SOURCE — SQL straight at the (single) shard, the
    // same way the sql_over_cluster e2e test submits DDL.
    let mut sc = Client::connect(shard[0].parse::<SocketAddr>().unwrap())
        .or_else(|_| {
            // ClusterClient finds the primary; for a plain Client we may
            // need any node — try each.
            shard
                .iter()
                .find_map(|a| Client::connect(a.parse::<SocketAddr>().unwrap()).ok())
                .ok_or(std::io::Error::new(std::io::ErrorKind::Other, "no node"))
        })
        .expect("connect shard");
    let ddl = format!(
        "CREATE EXTERNAL SOURCE feed (\
           id U64 NOT NULL FROM 'id', \
           nm CHAR(16) NOT NULL FROM 'nm'\
         ) FROM 'http://127.0.0.1:{port}/d' FORMAT JSON KEY id"
    );
    assert!(
        matches!(sc.sql(&ddl).expect("ddl wire"), OpResult::Ok | OpResult::TypeCreated(_)),
        "CREATE EXTERNAL SOURCE must succeed"
    );

    // Resolve the backing type id once (catalog is global).
    let cc = ClusterClient::new(shard.clone());
    let mut cc = cc;
    let tid = {
        // Describe by scanning type ids; feed is the only user type.
        let mut found = None;
        for t in 1..8u32 {
            if let OpResult::Got(def) =
                cc.call(&Op::Describe { type_id: t }).unwrap()
            {
                let (n, _) = kessel_catalog::decode_type_def(&def).unwrap();
                if n == "feed" {
                    found = Some(t);
                    break;
                }
            }
        }
        found.expect("feed type exists")
    };

    let mut rc = Client::connect(raddr).expect("connect router");

    // === Assertion 1: REFRESH materializes EXACTLY the served rows. ===
    *bodies.lock().unwrap() =
        vec![r#"[{"id":1,"nm":"alpha"},{"id":2,"nm":"beta"}]"#.into()];
    assert_eq!(
        rc.call(&Op::RefreshExternalSource { name: "feed".into() })
            .expect("refresh wire"),
        OpResult::Ok,
        "REFRESH #1 must succeed"
    );
    let model1 =
        sorted(vec![(1, "alpha".into()), (2, "beta".into())]);
    assert_eq!(
        select_set(&mut sc, tid),
        model1,
        "after REFRESH #1 the table is exactly the served rows"
    );

    // === Assertion 2: identical re-REFRESH is idempotent (digest). ===
    // The raw `SELECT *` blob is a deterministic fingerprint of the
    // materialized row state (record bytes + order); an idempotent
    // refresh (same id, Update with the same record) must not perturb it.
    let blob_before = select_blob(&mut sc);
    *bodies.lock().unwrap() =
        vec![r#"[{"id":1,"nm":"alpha"},{"id":2,"nm":"beta"}]"#.into()];
    assert_eq!(
        rc.call(&Op::RefreshExternalSource { name: "feed".into() })
            .expect("refresh wire"),
        OpResult::Ok,
        "REFRESH #2 (identical body) must succeed"
    );
    assert_eq!(
        select_set(&mut sc, tid),
        model1,
        "identical re-REFRESH leaves the row set unchanged"
    );
    let blob_after = select_blob(&mut sc);
    assert_eq!(
        blob_before, blob_after,
        "identical re-REFRESH must not change materialized engine state"
    );

    // === Assertion 3: a changed row is updated in place (same id). ===
    *bodies.lock().unwrap() =
        vec![r#"[{"id":1,"nm":"ALPHA2"},{"id":2,"nm":"beta"}]"#.into()];
    assert_eq!(
        rc.call(&Op::RefreshExternalSource { name: "feed".into() })
            .expect("refresh wire"),
        OpResult::Ok,
        "REFRESH #3 (one changed row) must succeed"
    );
    let model3 =
        sorted(vec![(1, "ALPHA2".into()), (2, "beta".into())]);
    let got3 = select_set(&mut sc, tid);
    assert_eq!(got3, model3, "changed row updated in place");
    assert_eq!(got3.len(), 2, "no duplicate row introduced");

    // === Assertion 4: schema-violating row → atomic abort. ===
    // 'nm' is CHAR(16); a 20-char value overflows the column.
    *bodies.lock().unwrap() = vec![concat!(
        r#"[{"id":3,"nm":"ok"},"#,
        r#"{"id":4,"nm":"WAY_TOO_LONG_FOR_CHR16"}]"#
    )
    .into()];
    let r = rc
        .call(&Op::RefreshExternalSource { name: "feed".into() })
        .expect("refresh wire");
    assert!(
        matches!(r, OpResult::SchemaError(_) | OpResult::Constraint(_)),
        "REFRESH with a schema-violating row must error, got {r:?}"
    );
    assert_eq!(
        select_set(&mut sc, tid),
        model3,
        "atomic abort: prior data must be byte-for-byte unchanged"
    );
}

/// The raw `SELECT * FROM feed` blob — a deterministic fingerprint of
/// the materialized row state (record bytes + scan order).
fn select_blob(shard: &mut Client) -> Vec<u8> {
    match shard.sql("SELECT * FROM feed").expect("select wire") {
        OpResult::Got(b) => b,
        o => panic!("SELECT * FROM feed: {o:?}"),
    }
}

// ---------------------------------------------------------------------
// EXT pagination oracle (Task 10): the router-side `do_refresh` now
// dispatches to `fetch_rows_paginated` when the recipe carries a PAGE
// clause. A localhost stub serves a 2-page JSON envelope; the test
// proves SELECT == union-of-pages, re-REFRESH is byte-identical
// (deterministic-id upsert dedups), and a self-looping page makes
// REFRESH error while the prior good rows stay intact (all-or-nothing).
// ---------------------------------------------------------------------

/// Stub bound to a specific (already-known-free) port so the caller can
/// embed the real port into page1's `next` URL *before* the server is
/// listening. Serves a queue of bodies, one per accepted connection,
/// and exits once the queue drains (join the handle at test end).
/// Adapts (header-stripped, body-only form of) kessel-fetch/tests/paginate_stub.rs's stub_at.
fn stub_at(port: u16, pages: Vec<String>) -> std::thread::JoinHandle<()> {
    let listener = TcpListener::bind(("127.0.0.1", port)).expect("rebind");
    let queue = Arc::new(Mutex::new(pages));
    std::thread::spawn(move || loop {
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
        let body = {
            let mut q = queue.lock().expect("lock");
            if q.is_empty() {
                break;
            }
            q.remove(0)
        };
        // Read the request up to the blank line so the client's write
        // completes before we reply with `Connection: close`.
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
        let resp = format!(
            "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
            body.as_bytes().len(),
            body
        );
        let _ = conn.write_all(resp.as_bytes());
        let _ = conn.flush();
    })
}

/// Independent model: decode `SELECT * FROM <table>` into a sorted SET
/// of (id u64, name String) pairs. Same wire shape / decode as the
/// slice-1 `select_set`, parameterized on the table + its type id.
fn select_set_tbl(
    shard: &mut Client,
    table: &str,
    type_id: u32,
) -> Vec<(u64, String)> {
    let typedef = match shard
        .call(&Op::Describe { type_id })
        .expect("describe wire")
    {
        OpResult::Got(b) => b,
        o => panic!("describe: {o:?}"),
    };
    let (name, fields) = kessel_catalog::decode_type_def(&typedef).unwrap();
    let ot = ObjectType::from_def(name, fields);
    let sql = format!("SELECT * FROM {table}");
    let blob = match shard.sql(&sql).expect("select wire") {
        OpResult::Got(b) => b,
        o => panic!("{sql}: {o:?}"),
    };
    let mut out = Vec::new();
    let mut p = 0usize;
    while p + 4 <= blob.len() {
        let len =
            u32::from_le_bytes(blob[p..p + 4].try_into().unwrap()) as usize;
        p += 4;
        let rec = &blob[p..p + len];
        p += len;
        let vals = kessel_codec::decode(&ot, rec).unwrap();
        let id = match &vals[0] {
            kessel_codec::Value::Uint(u) => *u as u64,
            v => panic!("id not uint: {v:?}"),
        };
        let nm = match &vals[1] {
            kessel_codec::Value::Blob(b) => {
                let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
                String::from_utf8_lossy(&b[..end]).to_string()
            }
            v => panic!("nm not blob: {v:?}"),
        };
        out.push((id, nm));
    }
    assert_eq!(p, blob.len(), "SELECT blob fully consumed");
    out.sort();
    out
}

/// Find a user type's id by name, polling until the DDL has replicated
/// to the shard the Router reads (catalog is global; scan a small id
/// range exactly like the slice-1 test does for `feed`). Polling closes
/// the create→refresh replication race: `do_refresh` resolves the source
/// by name from shard 0's catalog, so the name must be visible there
/// before REFRESH (else `do_refresh` returns `NotFound` having fetched
/// nothing).
fn type_id_of(cc: &mut ClusterClient, want: &str) -> u32 {
    for _ in 0..50 {
        for t in 1..16u32 {
            if let OpResult::Got(def) =
                cc.call(&Op::Describe { type_id: t }).unwrap()
            {
                let (n, _) =
                    kessel_catalog::decode_type_def(&def).unwrap();
                if n == want {
                    return t;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("type `{want}` never became visible");
}

/// Drain a `stub_at` server whose queue may still hold bodies the
/// client never consumed (e.g. `do_refresh` errored before/while
/// fetching). Repeatedly opens-and-drops connections — each wakes one
/// blocked `accept()`, draining one queued body — until the server
/// thread exits, then joins it. Robust regardless of how many bodies
/// were left (no fixed connect count to get wrong / deadlock on).
fn drain_and_join(h: std::thread::JoinHandle<()>, port: u16) {
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let d2 = done.clone();
    let jh = std::thread::spawn(move || {
        h.join().expect("stub thread panicked");
        d2.store(true, std::sync::atomic::Ordering::SeqCst);
    });
    while !done.load(std::sync::atomic::Ordering::SeqCst) {
        let _ = std::net::TcpStream::connect(("127.0.0.1", port));
        std::thread::sleep(Duration::from_millis(20));
    }
    jh.join().ok();
}

#[test]
fn refresh_oracle_paginates_union_idempotent_and_loop_aborts() {
    // One 3-node shard + Router, exactly the slice-1 bring-up.
    let shard = spawn_shard("p");
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
    let mut cc = ClusterClient::new(shard.clone());
    let mut rc = Client::connect(raddr).expect("connect router");

    // Bind→learn free port→drop→rebuild bodies with the real port→
    // start the stub on that port (the paginate_stub.rs technique).
    let probe = TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let port = probe.local_addr().expect("addr").port();
    drop(probe);

    // Two-page JSON envelope; page1.next is the absolute URL of page2
    // on the very same stub (substituting the real bound port).
    let page1 = format!(
        r#"{{"items":[{{"id":1,"nm":"a"}}],"pg":{{"next":"http://127.0.0.1:{port}/p2"}}}}"#
    );
    let page2 =
        r#"{"items":[{"id":2,"nm":"b"}],"pg":{"next":null}}"#.to_string();

    let ddl = format!(
        "CREATE EXTERNAL SOURCE feedp (\
           id U64 NOT NULL FROM 'id', \
           nm CHAR(8) NOT NULL FROM 'nm'\
         ) FROM 'http://127.0.0.1:{port}/p1' FORMAT JSON KEY id \
         ROWS 'items' PAGE NEXT JSON 'pg.next'"
    );
    assert!(
        matches!(
            sc.sql(&ddl).expect("ddl wire"),
            OpResult::Ok | OpResult::TypeCreated(_)
        ),
        "CREATE EXTERNAL SOURCE feedp must succeed"
    );
    let tid = type_id_of(&mut cc, "feedp");

    // === Assertion 1: REFRESH walks BOTH pages; SELECT == union. ===
    let h = stub_at(port, vec![page1.clone(), page2.clone()]);
    assert_eq!(
        rc.call(&Op::RefreshExternalSource {
            name: "feedp".into()
        })
        .expect("refresh wire"),
        OpResult::Ok,
        "paginated REFRESH #1 must succeed"
    );
    drain_and_join(h, port);
    let model = sorted(vec![(1, "a".into()), (2, "b".into())]);
    assert_eq!(
        select_set_tbl(&mut sc, "feedp", tid),
        model,
        "REFRESH must materialize the UNION of both served pages"
    );
    let blob_after_first = match sc
        .sql("SELECT * FROM feedp")
        .expect("select wire")
    {
        OpResult::Got(b) => b,
        o => panic!("SELECT * FROM feedp: {o:?}"),
    };

    // === Assertion 2: identical re-REFRESH is byte-identical. ===
    // Serve the SAME two pages again; the deterministic-id upsert must
    // dedup so the materialized blob is byte-for-byte unchanged.
    let h = stub_at(port, vec![page1.clone(), page2.clone()]);
    assert_eq!(
        rc.call(&Op::RefreshExternalSource {
            name: "feedp".into()
        })
        .expect("refresh wire"),
        OpResult::Ok,
        "paginated REFRESH #2 (identical pages) must succeed"
    );
    drain_and_join(h, port);
    assert_eq!(
        select_set_tbl(&mut sc, "feedp", tid),
        model,
        "idempotent re-REFRESH leaves the row set unchanged"
    );
    let blob_after_second = match sc
        .sql("SELECT * FROM feedp")
        .expect("select wire")
    {
        OpResult::Got(b) => b,
        o => panic!("SELECT * FROM feedp: {o:?}"),
    };
    assert_eq!(
        blob_after_first, blob_after_second,
        "idempotent re-REFRESH must not perturb materialized state"
    );

    // === Assertion 3: a self-looping page ⇒ error + prior data intact.
    // Re-use `feedp` (already proven resolvable — no second-DDL
    // replication race): its source URL is `http://127.0.0.1:{port}/p1`.
    // Bind a fresh stub on that SAME `port` whose page's `pg.next`
    // points back at the base URL (a cycle). `fetch_rows_paginated`'s
    // loop guard fires ⇒ Err ⇒ `do_refresh` returns SchemaError and
    // submits NOTHING (all-or-nothing), so feedp's previously
    // materialized rows must be byte-for-byte unchanged.
    let loop_body = format!(
        r#"{{"items":[{{"id":9,"nm":"z"}}],"pg":{{"next":"http://127.0.0.1:{port}/p1"}}}}"#
    );
    let h = stub_at(
        port,
        vec![loop_body.clone(), loop_body.clone(), loop_body],
    );
    let r = rc
        .call(&Op::RefreshExternalSource {
            name: "feedp".into(),
        })
        .expect("refresh wire");
    // The looped fetch consumes >=1 body then errors; any remaining
    // queued bodies are drained until the stub thread exits.
    drain_and_join(h, port);
    assert!(
        !matches!(r, OpResult::Ok),
        "a self-looping page must make REFRESH error, got {r:?}"
    );
    assert!(
        matches!(r, OpResult::SchemaError(_)),
        "loop ⇒ do_refresh returns SchemaError, got {r:?}"
    );
    // feedp untouched: all-or-nothing across the looped fetch.
    assert_eq!(
        select_set_tbl(&mut sc, "feedp", tid),
        model,
        "loop-source REFRESH must leave feedp's rows intact"
    );
    let blob_after_loop = match sc
        .sql("SELECT * FROM feedp")
        .expect("select wire")
    {
        OpResult::Got(b) => b,
        o => panic!("SELECT * FROM feedp: {o:?}"),
    };
    assert_eq!(
        blob_after_second, blob_after_loop,
        "all-or-nothing: prior good rows byte-identical after loop abort"
    );
}
