//! End-to-end: `REFRESH` of an `s3://` source with `FORMAT PARQUET`.
//! A localhost rustls stub stands in for S3, serving the real
//! `flat_required.parquet` fixture produced by pyarrow (Task 7).
//! The production router uses webpki-roots full-verify (SP99) which
//! does NOT trust the self-signed localhost fixture cert, so REFRESH
//! FAILS CLOSED with a typed SchemaError — this proves the
//! do_refresh → kessel_objstore sign → fetch_rows_signed wiring is
//! reached for `FORMAT PARQUET` sources and the atomic-abort /
//! fail-closed contract holds (prior state intact). The TRUSTED
//! Parquet-decode happy path is proven at the kessel-fetch layer by
//! `parquet_decode.rs` (Task 8); injecting fixture trust into the
//! production router would be a forbidden bypass (SP100 precedent).
//! Only compiled with `--features external-sources-objstore`.
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
            "kesseldb-extparquet-{}-{tag}-{i}",
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

static PARQUET_FIXTURE: &[u8] =
    include_bytes!("../../kessel-parquet/tests/fixtures/flat_required.parquet");

static DICT_PARQUET_FIXTURE: &[u8] =
    include_bytes!("../../kessel-parquet/tests/fixtures/dict_flat.parquet");

static SNAPPY_DICT_PARQUET_FIXTURE: &[u8] =
    include_bytes!("../../kessel-parquet/tests/fixtures/snappy_dict.parquet");

static NULLABLE_PARQUET_FIXTURE: &[u8] =
    include_bytes!("../../kessel-parquet/tests/fixtures/nullable.parquet");

static GZIP_DICT_PARQUET_FIXTURE: &[u8] =
    include_bytes!("../../kessel-parquet/tests/fixtures/gzip_dict.parquet");

static V2_DICT_PARQUET_FIXTURE: &[u8] =
    include_bytes!("../../kessel-parquet/tests/fixtures/v2_dict.parquet");

fn tls_stub_with_fixture(fixture: &'static [u8]) -> u16 {
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
            let mut response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                fixture.len()
            )
            .into_bytes();
            response.extend_from_slice(fixture);
            let _ = tls.write_all(&response);
        }
    });
    port
}

/// Parameters bundled into a struct for `run_fail_closed_parquet_e2e`
/// (behavior-preserving refactor; all fields map 1-to-1 to the prior
/// positional params — see helper doc for semantics).
/// All str fields are `&'static str` because every call-site passes
/// string literals; `fixture` is `&'static [u8]` for the same reason.
/// Zero allocation — no `String` ownership needed (review).
struct FailClosedCase {
    fixture: &'static [u8],
    tag: &'static str,
    keyid_env: &'static str,
    secret_env: &'static str,
    keyid_val: &'static str,
    secret_val: &'static str,
    source: &'static str,
    ddl_cols: &'static str,
    s3_path: &'static str,
}

/// Shared fail-closed e2e: serves `fixture` over a self-signed
/// localhost TLS stub; the production webpki-roots client must
/// reject it; REFRESH must return OpResult::SchemaError (refresh/
/// sign/tls/connect) and the subsequent SELECT must be empty
/// (atomic-abort, state intact). No router fixture-trust bypass.
///
/// Per-test varying fields (all must reproduce the original test's
/// exact byte-for-byte observable statements) — params bundled into
/// FailClosedCase struct (review):
/// - `fixture`:     the parquet bytes served by the TLS stub
/// - `tag`:         shard temp-dir discriminator
/// - `keyid_env`:   name of the env var holding the AWS key ID
/// - `secret_env`:  name of the env var holding the AWS secret
/// - `keyid_val`:   the literal value written into `keyid_env`
/// - `secret_val`:  the literal value written into `secret_env`
/// - `source`:      the external-source name (DDL + REFRESH + SELECT)
/// - `ddl_cols`:    the column list in the CREATE EXTERNAL SOURCE DDL
///                  (e.g. `"id U64 NOT NULL FROM 'id', nm CHAR(16) NOT NULL FROM 'nm'"`)
/// - `s3_path`:     the object path within `s3://bucket/` (e.g. `"data.parquet"`)
fn run_fail_closed_parquet_e2e(c: FailClosedCase) {
    std::env::set_var(c.keyid_env, c.keyid_val);
    std::env::set_var(c.secret_env, c.secret_val);

    let port = tls_stub_with_fixture(c.fixture);
    let shard = spawn_shard(c.tag);
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
        "CREATE EXTERNAL SOURCE {source} (\
           {ddl_cols}\
         ) FROM 's3://bucket/{s3_path}' FORMAT PARQUET KEY id \
         REGION 'us-east-1' \
         ENDPOINT 'https://127.0.0.1:{port}' \
         AUTH OBJSTORE S3 KEYID ENV '{keyid_env}' SECRET ENV '{secret_env}'",
        source = c.source,
        ddl_cols = c.ddl_cols,
        s3_path = c.s3_path,
        port = port,
        keyid_env = c.keyid_env,
        secret_env = c.secret_env,
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
        .call(&Op::RefreshExternalSource { name: c.source.into() })
        .expect("refresh wire");

    // Untrusted self-signed cert ⇒ typed failure surfaced at REFRESH.
    // The TLS handshake fails before any request bytes are sent, so the
    // stub may receive no data — only the SchemaError matters here.
    // Error wrapping:
    //   fetch_rows_signed error  → "refresh: {e}"
    //   sign_get error           → "REFRESH `{source}`: sign: {e}"
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
    let blob = match sc.sql(&format!("SELECT * FROM {}", c.source)).expect("select wire") {
        OpResult::Got(b) => b,
        o => panic!("SELECT {}: {:?}", c.source, o),
    };
    assert!(blob.is_empty(), "no rows must have been materialized for {}", c.source);
}

#[test]
fn refresh_parquet_from_s3_fails_closed_and_state_intact() {
    // The router's TLS client uses the production webpki-roots config,
    // which will NOT trust the localhost fixture cert. This e2e therefore
    // asserts the do_refresh → kessel_objstore::sign_get →
    // kessel_fetch::fetch_rows_signed path is wired for FORMAT PARQUET
    // sources and FAILS CLOSED on an untrusted cert, and that the
    // atomic-abort contract holds: prior (empty) state is intact and
    // SELECT works after the failed REFRESH.
    // The trusted happy-path (Parquet bytes decoded to rows) is proven
    // at the kessel-fetch layer by parquet_decode.rs (Task 8) —
    // injecting fixture trust here would bypass SP100.
    run_fail_closed_parquet_e2e(FailClosedCase {
        fixture: PARQUET_FIXTURE,
        tag: "pq",
        keyid_env: "OBJ_PQ_KEYID",
        secret_env: "OBJ_PQ_SECRET",
        keyid_val: "AKIAEXAMPLE",
        secret_val: "secretexamplekey",
        source: "feed",
        ddl_cols: "id U64 NOT NULL FROM 'id', nm CHAR(16) NOT NULL FROM 'nm'",
        s3_path: "data.parquet",
    });
}

/// Mirrors `refresh_parquet_from_s3_fails_closed_and_state_intact` for the
/// real pyarrow use_dictionary fixture (OBJ-2b-2). The same fail-closed
/// contract applies: the production webpki-roots TLS client does NOT trust
/// the self-signed localhost cert, so REFRESH returns a typed SchemaError
/// via the do_refresh → kessel_objstore::sign_get → kessel_fetch path,
/// and prior (empty) state remains intact. The trusted dict-decode happy
/// path is proven at the kessel-parquet layer by `fixture_roundtrip.rs`.
#[test]
fn refresh_dict_parquet_from_s3_fails_closed_and_state_intact() {
    run_fail_closed_parquet_e2e(FailClosedCase {
        fixture: DICT_PARQUET_FIXTURE,
        tag: "dpq",
        keyid_env: "OBJ_DPQ_KEYID",
        secret_env: "OBJ_DPQ_SECRET",
        keyid_val: "AKIAEXAMPLE2",
        secret_val: "secretexamplekey2",
        source: "dfeed",
        ddl_cols: "id U64 NOT NULL FROM 'id', s CHAR(4) NOT NULL FROM 's'",
        s3_path: "dict.parquet",
    });
}

/// Mirrors `refresh_parquet_from_s3_fails_closed_and_state_intact` for the
/// real pyarrow Snappy-compressed use_dictionary fixture (OBJ-2b-3). The
/// same fail-closed contract applies: the production webpki-roots TLS client
/// does NOT trust the self-signed localhost cert, so REFRESH returns a typed
/// SchemaError via the do_refresh → kessel_objstore::sign_get →
/// kessel_fetch path, and prior (empty) state remains intact. The trusted
/// Snappy-decode happy path is proven at the kessel-parquet layer by
/// `fixture_roundtrip::snappy_fixtures_roundtrip`. No fixture-trust bypass
/// is introduced here (SP100/SP101 precedent).
#[test]
fn refresh_snappy_parquet_from_s3_fails_closed_and_state_intact() {
    run_fail_closed_parquet_e2e(FailClosedCase {
        fixture: SNAPPY_DICT_PARQUET_FIXTURE,
        tag: "spq",
        keyid_env: "OBJ_SPQ_KEYID",
        secret_env: "OBJ_SPQ_SECRET",
        keyid_val: "AKIAEXAMPLE3",
        secret_val: "secretexamplekey3",
        source: "sfeed",
        ddl_cols: "id U64 NOT NULL FROM 'id', s CHAR(4) NOT NULL FROM 's'",
        s3_path: "snappy.parquet",
    });
}

/// Mirrors `refresh_parquet_from_s3_fails_closed_and_state_intact` for the
/// real pyarrow nullable.parquet fixture (OBJ-2b-4). The same fail-closed
/// contract applies: the production webpki-roots TLS client does NOT trust
/// the self-signed localhost cert, so REFRESH returns a typed SchemaError
/// via the do_refresh → kessel_objstore::sign_get → kessel_fetch path,
/// and prior (empty) state remains intact. The trusted nullable-decode happy
/// path is proven at the kessel-parquet layer by
/// `fixture_roundtrip::nullable_parquet_fixture_roundtrips`. No
/// fixture-trust bypass is introduced here (SP100/SP101 precedent).
#[test]
fn refresh_nullable_parquet_from_s3_fails_closed_and_state_intact() {
    run_fail_closed_parquet_e2e(FailClosedCase {
        fixture: NULLABLE_PARQUET_FIXTURE,
        tag: "npq",
        keyid_env: "OBJ_NPQ_KEYID",
        secret_env: "OBJ_NPQ_SECRET",
        keyid_val: "AKIAEXAMPLE4",
        secret_val: "secretexamplekey4",
        source: "nfeed",
        ddl_cols: "id U64 NOT NULL FROM 'id', s CHAR(4) NOT NULL FROM 's'",
        s3_path: "nullable.parquet",
    });
}

/// Mirrors `refresh_parquet_from_s3_fails_closed_and_state_intact` for the
/// real pyarrow gzip_dict.parquet fixture (OBJ-2c-1). The same fail-closed
/// contract applies: the production webpki-roots TLS client does NOT trust
/// the self-signed localhost cert, so REFRESH returns a typed SchemaError
/// via the do_refresh → kessel_objstore::sign_get → kessel_fetch path,
/// and prior (empty) state remains intact. The trusted GZIP-decode happy
/// path is proven at the kessel-parquet layer by
/// `fixture_roundtrip::gzip_fixtures_roundtrip`. No fixture-trust bypass
/// is introduced here (SP100/SP101 precedent).
#[test]
fn refresh_gzip_parquet_from_s3_fails_closed_and_state_intact() {
    run_fail_closed_parquet_e2e(FailClosedCase {
        fixture: GZIP_DICT_PARQUET_FIXTURE,
        tag: "gpq",
        keyid_env: "OBJ_GPQ_KEYID",
        secret_env: "OBJ_GPQ_SECRET",
        keyid_val: "AKIAEXAMPLE5",
        secret_val: "secretexamplekey5",
        source: "gfeed",
        ddl_cols: "id U64 NOT NULL FROM 'id', s CHAR(4) NOT NULL FROM 's'",
        s3_path: "gzip.parquet",
    });
}

/// Mirrors `refresh_parquet_from_s3_fails_closed_and_state_intact` for the
/// real pyarrow v2_dict.parquet fixture (OBJ-2c-3, DataPageHeaderV2). The
/// same fail-closed contract applies: the production webpki-roots TLS client
/// does NOT trust the self-signed localhost cert, so REFRESH returns a typed
/// SchemaError via the do_refresh → kessel_objstore::sign_get →
/// kessel_fetch path, and prior (empty) state remains intact. The trusted
/// V2-decode happy path is proven at the kessel-parquet layer by
/// `fixture_roundtrip::v2_dict_fixture_roundtrips`. No fixture-trust bypass
/// is introduced here (SP100/SP101 precedent).
#[test]
fn refresh_v2_parquet_from_s3_fails_closed_and_state_intact() {
    run_fail_closed_parquet_e2e(FailClosedCase {
        fixture: V2_DICT_PARQUET_FIXTURE,
        tag: "v2pq",
        keyid_env: "OBJ_V2PQ_KEYID",
        secret_env: "OBJ_V2PQ_SECRET",
        keyid_val: "AKIAEXAMPLE6",
        secret_val: "secretexamplekey6",
        source: "v2feed",
        ddl_cols: "id U64 NOT NULL FROM 'id', s CHAR(4) NOT NULL FROM 's'",
        s3_path: "v2dict.parquet",
    });
}

static DECIMAL_FLBA_PARQUET_FIXTURE: &[u8] =
    include_bytes!("../../kessel-parquet/tests/fixtures/decimal_flba.parquet");

/// Mirrors `refresh_parquet_from_s3_fails_closed_and_state_intact` for the
/// real pyarrow decimal_flba.parquet fixture (OBJ-2c-4, DECIMAL FLBA,
/// precision=30 scale=5, type_length=13). The same fail-closed contract
/// applies: the production webpki-roots TLS client does NOT trust the
/// self-signed localhost cert, so REFRESH returns a typed SchemaError via
/// the do_refresh → kessel_objstore::sign_get → kessel_fetch path, and
/// prior (empty) state remains intact. The trusted DECIMAL-FLBA decode
/// happy path is proven at the kessel-parquet layer by
/// `fixture_roundtrip::decimal_flba_fixture_roundtrips`. No fixture-trust
/// bypass is introduced here (SP100/SP101 precedent).
/// DDL maps the DECIMAL column (source name 'd') to an I128 column named
/// `id` to satisfy the hardcoded `KEY id` in the helper; the TLS error fires
/// before any column decode reaches the server.
#[test]
fn refresh_decimal_parquet_from_s3_fails_closed_and_state_intact() {
    run_fail_closed_parquet_e2e(FailClosedCase {
        fixture: DECIMAL_FLBA_PARQUET_FIXTURE,
        tag: "decpq",
        keyid_env: "OBJ_DEC_KEYID",
        secret_env: "OBJ_DEC_SECRET",
        keyid_val: "AKIAEXAMPLE7",
        secret_val: "secretexamplekey7",
        source: "decfeed",
        ddl_cols: "id I128 NOT NULL FROM 'd'",
        s3_path: "decimal.parquet",
    });
}
