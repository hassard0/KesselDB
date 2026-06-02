//! `kesseldb` — the runnable node binary.
//!
//! ## Single-node mode (default)
//!
//! Usage: `kesseldb [LISTEN_ADDR] [DATA_DIR]`
//! defaults: 127.0.0.1:7878  ./kesseldb-data
//!
//! Environment variables (all optional):
//!   KESSELDB_TOKEN          — enable token-mode auth (`Authorization: Bearer <token>`)
//!   KESSELDB_HTTP_ADDR      — enable opt-in HTTP/1.1 gateway on the given addr
//!                              (requires --features http-gateway build)
//!   KESSELDB_HTTP_TLS_ADDR  — enable HTTPS gateway (requires --features http-gateway,tls)
//!   KESSELDB_PG_ADDR        — enable opt-in PostgreSQL Frontend/Backend
//!                              v3.0 wire gateway (default port 5432;
//!                              requires --features pg-gateway build +
//!                              KESSELDB_TOKEN for SCRAM-SHA-256 auth)
//!
//! ## Cluster mode (SP-Cloud-Cluster T2)
//!
//! Add `--cluster` to opt into a VSR-replicated multi-node deployment.
//! Required companion flags (or their env-var equivalents):
//!
//!   --replica-idx N         (env: KESSELDB_CLUSTER_REPLICA_IDX)
//!       This pod's index into the peer list (0..K-1).
//!
//!   --peer-addrs A,B,C      (env: KESSELDB_CLUSTER_PEER_ADDRS)
//!       Comma-separated peer addresses (`host:port`). Each entry MUST
//!       resolve to exactly one socket addr; resolution happens at
//!       startup, so the peer DNS must be reachable. `K = addrs.len()`
//!       must be odd and >= 3 (legal: 3 or 5).
//!
//! Optional cluster flags / env:
//!
//!   --view-change-timeout 5s (env: KESSELDB_CLUSTER_VIEW_CHANGE_TIMEOUT;
//!       informational in V1 — the default 12 ms tick is what `kessel-vsr`
//!       uses internally and is not yet exposed as a runtime knob).
//!
//! CLI flags take precedence over env vars. The first positional arg is
//! still the client listen address (default `0.0.0.0:6532` in cluster
//! mode entry points; the chart entrypoint passes `0.0.0.0:6532`); the
//! second positional is the data dir. The peer-listen address is derived
//! from `peer_addrs[replica_idx]` (the binary listens on that port for
//! incoming peer dials and dials the others).
//!
//! HTTP/HTTPS/PG-wire gateway env vars are honored in single-node mode
//! only. In cluster mode they are accepted but ignored (the cluster path
//! exposes the binary client protocol on a single port — the gateway
//! cluster surfaces are a documented V2 follow-up).

use std::net::{SocketAddr, ToSocketAddrs};

/// Parsed CLI/env shape — fully resolved by the time main does anything.
struct Args {
    /// Positional: client listen address.
    client_addr: String,
    /// Positional: data directory.
    data_dir: String,
    /// `--cluster` flag (or env-driven equivalent).
    cluster: bool,
    /// `--replica-idx N`. Required if `cluster` is set.
    replica_idx: Option<usize>,
    /// `--peer-addrs A,B,C`. Required if `cluster` is set.
    peer_addrs_raw: Option<String>,
    /// `--view-change-timeout T`. Optional.
    view_change_timeout_raw: Option<String>,
}

/// Best-effort arg parser. Recognises:
///   `--cluster`
///   `--replica-idx N`
///   `--peer-addrs A,B,C`
///   `--view-change-timeout 5s`
/// Anything else is treated as a positional (first positional = client
/// listen addr; second positional = data dir). Unknown long options are
/// rejected with a clear error so a typo doesn't silently fall through.
fn parse_args(raw: Vec<String>) -> Result<Args, String> {
    let mut positional: Vec<String> = Vec::new();
    let mut cluster = false;
    let mut replica_idx: Option<usize> = None;
    let mut peer_addrs_raw: Option<String> = None;
    let mut view_change_timeout_raw: Option<String> = None;

    let mut i = 1;
    while i < raw.len() {
        let a = &raw[i];
        match a.as_str() {
            "--cluster" => {
                cluster = true;
                i += 1;
            }
            "--replica-idx" => {
                let v = raw
                    .get(i + 1)
                    .ok_or_else(|| "--replica-idx requires a value".to_string())?;
                let n: usize = v
                    .parse()
                    .map_err(|_| format!("--replica-idx: not a usize: {v:?}"))?;
                replica_idx = Some(n);
                i += 2;
            }
            "--peer-addrs" => {
                let v = raw
                    .get(i + 1)
                    .ok_or_else(|| "--peer-addrs requires a value".to_string())?
                    .clone();
                peer_addrs_raw = Some(v);
                i += 2;
            }
            "--view-change-timeout" => {
                let v = raw
                    .get(i + 1)
                    .ok_or_else(|| {
                        "--view-change-timeout requires a value".to_string()
                    })?
                    .clone();
                view_change_timeout_raw = Some(v);
                i += 2;
            }
            other if other.starts_with("--") => {
                return Err(format!("unknown argument: {other}"));
            }
            _ => {
                positional.push(a.clone());
                i += 1;
            }
        }
    }

    let client_addr = positional
        .first()
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:7878".into());
    let data_dir = positional
        .get(1)
        .cloned()
        .unwrap_or_else(|| "kesseldb-data".into());

    Ok(Args {
        client_addr,
        data_dir,
        cluster,
        replica_idx,
        peer_addrs_raw,
        view_change_timeout_raw,
    })
}

/// Resolve `host:port` strings into `SocketAddr`s. Each entry must
/// resolve to exactly one socket addr — if DNS returns multiple
/// (IPv4 + IPv6), we take the first. Empty list returns an error.
fn resolve_peer_addrs(raw: &str) -> Result<Vec<SocketAddr>, String> {
    let parts: Vec<&str> = raw
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    if parts.is_empty() {
        return Err("peer-addrs: empty list".into());
    }
    let mut out = Vec::with_capacity(parts.len());
    for p in parts {
        let mut iter = p
            .to_socket_addrs()
            .map_err(|e| format!("peer-addrs: {p:?} resolve failed: {e}"))?;
        let addr = iter
            .next()
            .ok_or_else(|| format!("peer-addrs: {p:?} resolved to nothing"))?;
        out.push(addr);
    }
    Ok(out)
}

fn main() {
    let raw: Vec<String> = std::env::args().collect();
    let args = match parse_args(raw) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("kesseldb: {e}");
            std::process::exit(2);
        }
    };

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
    if let Ok(a) = std::env::var("KESSELDB_PG_ADDR") {
        if let Ok(parsed) = a.parse() {
            cfg.pg_addr = Some(parsed);
        } else {
            eprintln!("kesseldb: bad KESSELDB_PG_ADDR {a:?} — ignoring");
        }
    }

    // SP-Cloud-Cluster T2 — env-var fallback. Setting *either* of the two
    // mandatory cluster env vars implies cluster mode (unless `--cluster`
    // was passed, which always wins). CLI flag values take precedence
    // over env values.
    let env_idx: Option<usize> = std::env::var("KESSELDB_CLUSTER_REPLICA_IDX")
        .ok()
        .and_then(|s| s.parse().ok());
    let env_peers: Option<String> = std::env::var("KESSELDB_CLUSTER_PEER_ADDRS").ok();
    let env_view_timeout: Option<String> =
        std::env::var("KESSELDB_CLUSTER_VIEW_CHANGE_TIMEOUT").ok();
    let cluster_env_present = env_idx.is_some() || env_peers.is_some();
    let want_cluster = args.cluster || cluster_env_present;

    if want_cluster {
        let replica_idx = match args.replica_idx.or(env_idx) {
            Some(n) => n,
            None => {
                eprintln!(
                    "kesseldb: cluster mode requires --replica-idx N \
                     (or env KESSELDB_CLUSTER_REPLICA_IDX)"
                );
                std::process::exit(2);
            }
        };
        let peer_addrs_raw = match args.peer_addrs_raw.clone().or(env_peers) {
            Some(s) => s,
            None => {
                eprintln!(
                    "kesseldb: cluster mode requires --peer-addrs A,B,C \
                     (or env KESSELDB_CLUSTER_PEER_ADDRS)"
                );
                std::process::exit(2);
            }
        };
        let peer_addrs = match resolve_peer_addrs(&peer_addrs_raw) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("kesseldb: {e}");
                std::process::exit(2);
            }
        };
        if replica_idx >= peer_addrs.len() {
            eprintln!(
                "kesseldb: --replica-idx {replica_idx} out of range for an \
                 {n}-node cluster",
                n = peer_addrs.len()
            );
            std::process::exit(2);
        }
        // View-change timeout is parsed for informational logging only in
        // V1; it's not yet plumbed into Replica::new (the underlying
        // 12 ms tick is what kessel-vsr uses internally).
        let vct_str = args
            .view_change_timeout_raw
            .as_deref()
            .or(env_view_timeout.as_deref())
            .unwrap_or("5s");

        let peer_listen = peer_addrs[replica_idx];
        eprintln!(
            "kesseldb cluster: starting replica {replica_idx}/{n}, \
             client_addr={ca}, peer_listen={pl}, data={d}, \
             view_change_timeout={vct} (informational), \
             token={tok}, peers=[{peers}]",
            n = peer_addrs.len(),
            ca = args.client_addr,
            pl = peer_listen,
            d = args.data_dir,
            vct = vct_str,
            tok = if cfg.token.is_some() { "set" } else { "open" },
            peers = peer_addrs
                .iter()
                .map(|a| a.to_string())
                .collect::<Vec<_>>()
                .join(","),
        );
        if cfg.http_addr.is_some() || cfg.http_tls_addr.is_some() || cfg.pg_addr.is_some() {
            eprintln!(
                "kesseldb cluster: NOTE — HTTP / HTTPS / PG-wire gateway \
                 env vars are accepted but IGNORED in cluster mode V1 \
                 (binary client protocol only on this slice; gateway \
                 cluster surfaces are a documented V2 follow-up)."
            );
        }
        // Use 0.0.0.0:<peer_port> so the bind matches every routable
        // interface, not just the address the peer is reachable as from
        // outside the pod. `peer_addrs[self_idx]` is the EXTERNAL form;
        // the pod's own listen socket binds on all interfaces with the
        // same port.
        let peer_listen_local =
            format!("0.0.0.0:{}", peer_listen.port());
        if let Err(e) = kesseldb_server::run_cluster_cfg(
            &args.client_addr,
            &peer_listen_local,
            &args.data_dir,
            replica_idx,
            peer_addrs,
            cfg,
        ) {
            eprintln!("kesseldb: fatal (cluster): {e}");
            std::process::exit(1);
        }
        return;
    }

    println!(
        "KesselDB listening on {addr}, data dir {dir}{}{}{}",
        cfg.http_addr.map(|a| format!(", http={a}")).unwrap_or_default(),
        cfg.http_tls_addr.map(|a| format!(", https={a}")).unwrap_or_default(),
        cfg.pg_addr.map(|a| format!(", pg={a}")).unwrap_or_default(),
        addr = args.client_addr,
        dir = args.data_dir,
    );
    if let Err(e) = kesseldb_server::run_cfg(&args.client_addr, &args.data_dir, cfg) {
        eprintln!("kesseldb: fatal: {e}");
        std::process::exit(1);
    }
}
