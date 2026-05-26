//! `kesseldb` — the runnable node binary.
//!
//! Usage: `kesseldb [LISTEN_ADDR] [DATA_DIR]`
//! defaults: 127.0.0.1:7878  ./kesseldb-data
//!
//! Environment variables (all optional):
//!   KESSELDB_TOKEN          — enable token-mode auth (`Authorization: Bearer <token>`)
//!   KESSELDB_HTTP_ADDR      — enable opt-in HTTP/1.1 gateway on the given addr
//!                              (requires --features http-gateway build)
//!   KESSELDB_HTTP_TLS_ADDR  — enable HTTPS gateway (requires --features http-gateway,tls)

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1:7878".into());
    let dir = args.get(2).cloned().unwrap_or_else(|| "kesseldb-data".into());

    let mut cfg = kesseldb_server::ServerConfig::default();
    if let Ok(t) = std::env::var("KESSELDB_TOKEN") {
        if !t.is_empty() {
            cfg.token = Some(t.into_bytes());
        }
    }
    if let Ok(a) = std::env::var("KESSELDB_HTTP_ADDR") {
        if let Ok(parsed) = a.parse() {
            cfg.http_addr = Some(parsed);
        } else {
            eprintln!("kesseldb: bad KESSELDB_HTTP_ADDR {a:?} — ignoring");
        }
    }
    if let Ok(a) = std::env::var("KESSELDB_HTTP_TLS_ADDR") {
        if let Ok(parsed) = a.parse() {
            cfg.http_tls_addr = Some(parsed);
        } else {
            eprintln!("kesseldb: bad KESSELDB_HTTP_TLS_ADDR {a:?} — ignoring");
        }
    }

    println!(
        "KesselDB listening on {addr}, data dir {dir}{}{}",
        cfg.http_addr.map(|a| format!(", http={a}")).unwrap_or_default(),
        cfg.http_tls_addr.map(|a| format!(", https={a}")).unwrap_or_default(),
    );
    if let Err(e) = kesseldb_server::run_cfg(&addr, &dir, cfg) {
        eprintln!("kesseldb: fatal: {e}");
        std::process::exit(1);
    }
}
