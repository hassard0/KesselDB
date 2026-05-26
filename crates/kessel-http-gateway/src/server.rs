//! TCP accept loop + per-connection thread. Mirrors the binary listener:
//! one thread per connection, atomic in-flight counter for backpressure
//! coordination with the engine. We send `Connection: close` on every
//! response so a single TCP connection serves a single HTTP request.

#![allow(dead_code)]

use crate::engine::{EngineApply, HttpRequestCountersStatic};
use crate::parse::{
    parse_request, ParseError, MAX_HEADER_BYTES,
};
use crate::response::write_error_json_counted;
use crate::routes;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Default connection cap, applied PER LISTENER (binary, HTTP, HTTPS each
/// independently cap at this value). A process with the gateway feature
/// enabled may hold up to `DEFAULT_MAX_CONNS × num_listeners` concurrent
/// connections. The cap is intentionally per-listener so a misbehaving HTTP
/// client can't starve the binary protocol (and vice-versa).
pub const DEFAULT_MAX_CONNS: usize = 1024;

pub fn serve(
    listener: TcpListener,
    engine: Arc<dyn EngineApply>,
    token: Option<Vec<u8>>,
    max_conns: usize,
    max_body: usize,
    http_counters: Arc<HttpRequestCountersStatic>,
) {
    let active = Arc::new(AtomicUsize::new(0));
    for stream in listener.incoming().flatten() {
        if active.load(Ordering::Acquire) >= max_conns {
            drop(stream);
            continue;
        }
        let _ = stream.set_nodelay(true);
        let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
        let _ = stream.set_write_timeout(Some(Duration::from_secs(30)));
        active.fetch_add(1, Ordering::AcqRel);
        let e = engine.clone();
        let t = token.clone();
        let a = active.clone();
        let c = http_counters.clone();
        std::thread::spawn(move || {
            handle_one(stream, &e, t.as_deref(), max_body, &c);
            a.fetch_sub(1, Ordering::AcqRel);
        });
    }
}

fn handle_one(
    mut s: TcpStream,
    engine: &Arc<dyn EngineApply>,
    token: Option<&[u8]>,
    max_body: usize,
    http_counters: &Arc<HttpRequestCountersStatic>,
) {
    let _ = handle_one_stream(&mut s, engine, token, max_body, http_counters);
}

fn handle_one_stream<S: Read + Write>(
    s: &mut S,
    engine: &Arc<dyn EngineApply>,
    token: Option<&[u8]>,
    max_body: usize,
    http_counters: &Arc<HttpRequestCountersStatic>,
) -> std::io::Result<()> {
    let mut raw: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut chunk = [0u8; 8192];
    loop {
        let n = match s.read(&mut chunk) {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            Err(_) => return Ok(()),
        };
        raw.extend_from_slice(&chunk[..n]);
        if raw.len() > MAX_HEADER_BYTES + max_body {
            // Parse failed before the path was known — bump against the
            // defensive default ("/v1/sql"). The status bucket (413) is
            // what operators actually monitor. SP147: defensive close —
            // abuse path always closes regardless of negotiation.
            let _ = write_error_json_counted(s, (413, "Payload Too Large"),
                "error", "payload too large",
                http_counters, "/v1/sql", /*keep_alive=*/ false);
            return Ok(());
        }
        // Honor the configured `http_max_body` at parse time, not just at the
        // outer raw.len() guard above — the inner `decode_body` path checks
        // `max_body` against decoded chunk-stream output and Content-Length
        // values, so a 16 MiB Content-Length only succeeds when the
        // operator configured `http_max_body = 16 MiB` (or larger).
        match parse_request(&raw, max_body) {
            Ok(req) => {
                // SP147 T2: `routes::handle` returns close_after; T2 still
                // ignores the value (single-shot per-connection behavior
                // preserved). T3 will wire the loop.
                let _ = routes::handle(s, &req, token, engine, http_counters);
                return Ok(());
            }
            Err(ParseError::NoHeaderTerminator) => continue,
            Err(ParseError::ShortBody) => continue,
            Err(e) => {
                // SP147: parse errors always close (defensive — a malformed
                // request could mis-frame subsequent bytes on the connection).
                let _ = write_parse_error(s, &e, http_counters, /*keep_alive=*/ false);
                return Ok(());
            }
        }
    }
}

fn write_parse_error<W: Write>(
    w: &mut W,
    e: &ParseError,
    http_counters: &Arc<HttpRequestCountersStatic>,
    keep_alive: bool,
) -> std::io::Result<()> {
    let (status, semantic, msg): ((u16, &'static str), &str, String) = match e {
        ParseError::BadRequestLine =>
            ((400, "Bad Request"), "error", "bad request line".into()),
        ParseError::MethodNotAllowed =>
            ((405, "Method Not Allowed"), "error", "method not allowed".into()),
        ParseError::NotFound =>
            ((404, "Not Found"), "error", "not found".into()),
        ParseError::MissingHost =>
            ((400, "Bad Request"), "error", "missing Host".into()),
        ParseError::Ipv6LiteralHost =>
            ((400, "Bad Request"), "error",
             "IPv6 literal Host not supported".into()),
        ParseError::LengthRequired =>
            ((411, "Length Required"), "error", "length required".into()),
        ParseError::HeaderTooLarge =>
            ((414, "URI Too Long"), "error", "URI too long".into()),
        ParseError::BodyTooLarge =>
            ((413, "Payload Too Large"), "error", "payload too large".into()),
        ParseError::UnsupportedMediaType =>
            ((415, "Unsupported Media Type"), "error",
             "unsupported media type".into()),
        ParseError::ShortBody =>
            ((400, "Bad Request"), "error", "short body".into()),
        ParseError::NoHeaderTerminator =>
            ((400, "Bad Request"), "error", "no header terminator".into()),
        ParseError::BadHeaderValue(m) =>
            ((400, "Bad Request"), "error", m.clone()),
        ParseError::ConflictingFraming =>
            ((400, "Bad Request"), "error",
             "conflicting framing: both Content-Length and Transfer-Encoding".into()),
        ParseError::DuplicateHost =>
            ((400, "Bad Request"), "error", "duplicate Host header".into()),
        ParseError::DuplicateContentLength =>
            ((400, "Bad Request"), "error",
             "differing Content-Length headers".into()),
        ParseError::DuplicateHeader(name) =>
            ((400, "Bad Request"), "error",
             format!("duplicate {name} header")),
        ParseError::BadChunk(m) =>
            ((400, "Bad Request"), "error",
             format!("bad chunked encoding: {m}")),
        ParseError::UnsupportedTransferEncoding(m) =>
            ((400, "Bad Request"), "error",
             format!("unsupported Transfer-Encoding: {m}")),
        ParseError::ExpectationFailed =>
            ((417, "Expectation Failed"), "error",
             "expectation failed".into()),
        ParseError::IncompleteSessionBinding =>
            ((400, "Bad Request"), "error",
             "both X-Kessel-Client-Id and X-Kessel-Req-Seq required together".into()),
    };
    // SP144H T2: parse errors happen BEFORE the path is known. We bump
    // against "/v1/sql" as a defensive default (the status bucket is what
    // operators actually monitor; the path label is a minor accounting
    // inaccuracy on malformed requests).
    write_error_json_counted(w, status, semantic, &msg, http_counters, "/v1/sql",
        keep_alive)
}

// =========================================================================
// HTTPS variant — TLS-acceptor trait keeps this crate rustls-dep-free.
// =========================================================================

pub trait TlsAccept: Send + Sync + 'static {
    type Stream: Read + Write + Send + 'static;
    fn accept(&self, sock: TcpStream) -> Option<Self::Stream>;
}

pub fn serve_tls<A>(
    listener: TcpListener,
    acceptor: A,
    engine: Arc<dyn EngineApply>,
    token: Option<Vec<u8>>,
    max_conns: usize,
    max_body: usize,
    http_counters: Arc<HttpRequestCountersStatic>,
) where
    A: TlsAccept,
{
    let acceptor = Arc::new(acceptor);
    let active = Arc::new(AtomicUsize::new(0));
    for stream in listener.incoming().flatten() {
        if active.load(Ordering::Acquire) >= max_conns {
            drop(stream);
            continue;
        }
        let _ = stream.set_nodelay(true);
        // Slowloris guard: cap how long an attacker can pin a thread by
        // opening TCP and then never sending ClientHello / dribbling bytes.
        // Mirrors the plaintext `serve()` path above; without these, an
        // attacker could hold a TLS-acceptor thread until the OS kernel
        // socket timeout (minutes) just by completing the TCP handshake.
        let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
        let _ = stream.set_write_timeout(Some(Duration::from_secs(30)));
        active.fetch_add(1, Ordering::AcqRel);
        let e = engine.clone();
        let t = token.clone();
        let a = active.clone();
        let acc = acceptor.clone();
        let c = http_counters.clone();
        std::thread::spawn(move || {
            if let Some(mut tls) = acc.accept(stream) {
                let _ = handle_one_stream(&mut tls, &e, t.as_deref(), max_body, &c);
            }
            a.fetch_sub(1, Ordering::AcqRel);
        });
    }
}
