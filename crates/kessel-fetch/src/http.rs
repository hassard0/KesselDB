//! Dependency-free HTTP/1.1 GET. Parses scheme://host[:port]/path,
//! sends a GET, reads the response, enforces a body cap, returns the
//! body bytes. HTTPS is intentionally unsupported in slice 1 (use a
//! TLS-terminating sidecar — see the design doc).
use crate::{Auth, FetchError};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Max response-header bytes tolerated before the `\r\n\r\n` separator
/// (in addition to `max_body`) — bounds buffering on a server that
/// streams a huge body without ever sending the header terminator.
const MAX_HEADER_SLACK: u64 = 64 * 1024;

/// Like `get` but also returns the response headers as `(name, value)` pairs
/// (names stored as-received; callers compare with `eq_ignore_ascii_case`).
pub(crate) fn get_resp(
    url: &str,
    auth: &Auth,
    max_body: u64,
) -> Result<(Vec<(String, String)>, Vec<u8>), FetchError> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| {
            FetchError::Http(
                "only http:// is supported in slice 1 (use a TLS sidecar)".into(),
            )
        })?;
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
        None => (hostport, 80u16),
    };
    if host.starts_with('[') {
        return Err(FetchError::Http(
            "IPv6 literal addresses are not supported in slice 1; \
             use a hostname or a TLS/proxy sidecar".into(),
        ));
    }
    let mut req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\
         User-Agent: kessel-fetch/0\r\n"
    );
    match auth {
        Auth::None => {}
        Auth::Bearer(t) => req.push_str(&format!("Authorization: Bearer {t}\r\n")),
        Auth::Header { name, value } => {
            req.push_str(&format!("{name}: {value}\r\n"))
        }
    }
    req.push_str("\r\n");

    let mut s = TcpStream::connect((host, port))
        .map_err(|e| FetchError::Http(format!("connect {host}:{port}: {e}")))?;
    s.set_read_timeout(Some(Duration::from_secs(30))).ok();
    s.set_write_timeout(Some(Duration::from_secs(30))).ok();
    s.write_all(req.as_bytes())
        .map_err(|e| FetchError::Http(format!("write: {e}")))?;

    let mut raw = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = s
            .read(&mut chunk)
            .map_err(|e| FetchError::Http(format!("read: {e}")))?;
        if n == 0 {
            break;
        }
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
        .ok_or_else(|| FetchError::Http(format!("bad status line `{status}`")))?;
    if !(200..300).contains(&code) {
        return Err(FetchError::Http(format!("HTTP {code}")));
    }
    let mut chunked = false;
    let mut headers: Vec<(String, String)> = Vec::new();
    for l in lines {
        // Collect every header line as (name, value), split on the first ':'.
        if let Some(colon) = l.find(':') {
            let name = l[..colon].trim().to_string();
            let value = l[colon + 1..].trim().to_string();
            // Keep existing chunked detection working by deriving it from collected headers.
            let ll = l.to_ascii_lowercase();
            if ll.starts_with("transfer-encoding:") && ll.contains("chunked") {
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

/// Returns only the response body. Thin wrapper around `get_resp`.
pub fn get(url: &str, auth: &Auth, max_body: u64) -> Result<Vec<u8>, FetchError> {
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
