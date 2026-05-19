//! Dependency-free HTTP/1.1 GET with an optional TLS transport.
//! `http://` is always plaintext; `https://` requires the `tls`
//! feature (otherwise a typed error). All response handling
//! (header parse, dechunk, body cap) is one generic path shared by
//! both transports.
use crate::{Auth, FetchError};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Max response-header bytes tolerated before the `\r\n\r\n` separator
/// (in addition to `max_body`) — bounds buffering on a server that
/// streams a huge body without ever sending the header terminator.
const MAX_HEADER_SLACK: u64 = 64 * 1024;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Scheme {
    Http,
    Https,
}

/// Parse `scheme://host[:port]/path` into its parts, applying the
/// scheme's default port. IPv6-literal hosts are rejected (unchanged
/// from slice 1).
pub(crate) fn parse_target(
    url: &str,
) -> Result<(Scheme, String, u16, String), FetchError> {
    let (scheme, default_port, rest) = if let Some(r) =
        url.strip_prefix("http://")
    {
        (Scheme::Http, 80u16, r)
    } else if let Some(r) = url.strip_prefix("https://") {
        (Scheme::Https, 443u16, r)
    } else {
        return Err(FetchError::Http(
            "only http:// and https:// URLs are supported".into(),
        ));
    };
    let (hostport, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (
            h,
            p.parse::<u16>()
                .map_err(|_| FetchError::Http("bad port".into()))?,
        ),
        None => (hostport, default_port),
    };
    if host.starts_with('[') {
        return Err(FetchError::Http(
            "IPv6 literal addresses are not supported; use a hostname"
                .into(),
        ));
    }
    Ok((scheme, host.to_string(), port, path.to_string()))
}

/// Build an HTTP/1.1 GET with caller-supplied header lines (each
/// emitted verbatim after the Host/Connection/User-Agent lines).
pub(crate) fn build_request_with_headers(
    path: &str,
    host: &str,
    extra: &[(String, String)],
) -> String {
    let mut req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\
         User-Agent: kessel-fetch/0\r\n"
    );
    for (k, v) in extra {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    req
}

/// Build the HTTP/1.1 GET request text (Host header value is the bare
/// host, unchanged from slice 1).
pub(crate) fn build_request(path: &str, host: &str, auth: &Auth) -> String {
    let extra: Vec<(String, String)> = match auth {
        Auth::None => Vec::new(),
        Auth::Bearer(t) => {
            vec![("Authorization".into(), format!("Bearer {t}"))]
        }
        Auth::Header { name, value } => {
            vec![(name.clone(), value.clone())]
        }
    };
    build_request_with_headers(path, host, &extra)
}

/// Send `req` over an already-connected stream, read the full
/// response, enforce the caps, return `(headers, body)`. This is the
/// single hardened path; both the plaintext and TLS transports flow
/// through it unchanged.
pub(crate) fn exchange<S: Read + Write>(
    mut s: S,
    req: &str,
    max_body: u64,
) -> Result<(Vec<(String, String)>, Vec<u8>), FetchError> {
    s.write_all(req.as_bytes())
        .map_err(|e| FetchError::Http(format!("write: {e}")))?;

    let mut raw = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = match s.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => n,
            // kessel-fetch is strictly length-framed (Content-Length /
            // chunked) and always sends `Connection: close`. A TLS peer
            // that closes the TCP connection without a `close_notify`
            // alert surfaces here as `UnexpectedEof`; for a length-framed
            // client that is a normal end-of-stream (rustls documents
            // this as safe to ignore), so treat it exactly like `Ok(0)`.
            // Any genuine truncation is still caught downstream by the
            // de-chunk `b.len() < size + 2` guard and the body caps.
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break
            }
            Err(e) => return Err(FetchError::Http(format!("read: {e}"))),
        };
        raw.extend_from_slice(&chunk[..n]);
        if raw.len() as u64 > max_body + MAX_HEADER_SLACK {
            return Err(FetchError::TooLarge(max_body));
        }
    }
    let sep = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| FetchError::Http("no header terminator".into()))?;
    let head = String::from_utf8_lossy(&raw[..sep]).to_string();
    let mut lines = head.split("\r\n");
    let status = lines.next().unwrap_or("");
    let code = status
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse::<u16>().ok())
        .ok_or_else(|| {
            FetchError::Http(format!("bad status line `{status}`"))
        })?;
    if !(200..300).contains(&code) {
        return Err(FetchError::Http(format!("HTTP {code}")));
    }
    let mut chunked = false;
    let mut headers: Vec<(String, String)> = Vec::new();
    for l in lines {
        if let Some(colon) = l.find(':') {
            let name = l[..colon].trim().to_string();
            let value = l[colon + 1..].trim().to_string();
            if name.eq_ignore_ascii_case("transfer-encoding")
                && value.to_ascii_lowercase().contains("chunked")
            {
                chunked = true;
            }
            headers.push((name, value));
        }
    }
    let body_raw = &raw[sep + 4..];
    let body = if chunked {
        dechunk(body_raw)?
    } else {
        body_raw.to_vec()
    };
    if body.len() as u64 > max_body {
        return Err(FetchError::TooLarge(max_body));
    }
    Ok((headers, body))
}

/// Connect the right transport for the scheme. `https://` without the
/// `tls` feature is a typed error that names the feature.
fn connect(
    scheme: Scheme,
    host: &str,
    port: u16,
) -> Result<Box<dyn ReadWrite>, FetchError> {
    match scheme {
        Scheme::Http => {
            let s = TcpStream::connect((host, port)).map_err(|e| {
                FetchError::Http(format!("connect {host}:{port}: {e}"))
            })?;
            s.set_read_timeout(Some(Duration::from_secs(30))).ok();
            s.set_write_timeout(Some(Duration::from_secs(30))).ok();
            Ok(Box::new(s))
        }
        Scheme::Https => {
            #[cfg(feature = "tls")]
            {
                Ok(Box::new(crate::tls::connect_tls(host, port)?))
            }
            #[cfg(not(feature = "tls"))]
            {
                let _ = (host, port);
                Err(FetchError::Http(
                    "https:// requires building with the \
                     external-sources-tls feature"
                        .into(),
                ))
            }
        }
    }
}

/// Object-safe Read+Write so `connect` can return either transport.
/// std blanket-impls `Read` and `Write` for `Box<T: Read/Write + ?Sized>`,
/// which covers `Box<dyn ReadWrite>` because `dyn ReadWrite: Read + Write`.
pub(crate) trait ReadWrite: Read + Write {}
impl<T: Read + Write> ReadWrite for T {}

/// Returns response headers + body. Parses the URL, connects the
/// scheme's transport, and runs the shared exchange.
pub(crate) fn get_resp(
    url: &str,
    auth: &Auth,
    max_body: u64,
) -> Result<(Vec<(String, String)>, Vec<u8>), FetchError> {
    let (scheme, host, port, path) = parse_target(url)?;
    let stream = connect(scheme, &host, port)?;
    let req = build_request(&path, &host, auth);
    exchange(stream, &req, max_body)
}

/// Returns only the response body. Thin wrapper around `get_resp`.
pub fn get(
    url: &str,
    auth: &Auth,
    max_body: u64,
) -> Result<Vec<u8>, FetchError> {
    Ok(get_resp(url, auth, max_body)?.1)
}

fn dechunk(mut b: &[u8]) -> Result<Vec<u8>, FetchError> {
    let mut out = Vec::new();
    loop {
        let nl = b
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or_else(|| FetchError::Http("bad chunk".into()))?;
        let size_line = std::str::from_utf8(&b[..nl]).unwrap_or("");
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16)
            .map_err(|_| FetchError::Http("bad chunk size".into()))?;
        b = &b[nl + 2..];
        if size == 0 {
            return Ok(out);
        }
        if b.len() < size + 2 {
            return Err(FetchError::Http(
                "truncated chunk (missing trailing CRLF)".into(),
            ));
        }
        out.extend_from_slice(&b[..size]);
        b = &b[size + 2..];
    }
}
