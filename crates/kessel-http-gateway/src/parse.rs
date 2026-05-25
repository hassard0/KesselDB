//! HTTP/1.1 request parser (request line + headers + Content-Length body),
//! hand-rolled per RFC 9112. Mirrors the bounds-checked style of
//! `kessel-fetch::http`. T3 adds chunked transfer-encoding, body caps,
//! Bearer + X-Kessel-* extractors, and three smuggling-relevant fixes
//! (RFC 9112 §6.3.5 duplicate-CL, §3.2 duplicate-Host, §6.1 TE+CL conflict).

#![allow(dead_code)]

use std::borrow::Cow;

/// Hard caps applied at parse time. The body cap is configurable via
/// `decode_body`; the header cap stays fixed at 64 KiB per spec §4.1.
pub const MAX_HEADER_BYTES: usize = 64 * 1024;

/// Default body cap (spec §4.1: 8 MiB). T4 plumbs a configurable override via
/// `ServerConfig.http_max_body`.
pub const DEFAULT_MAX_BODY: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Method {
    Get,
    Post,
}

#[derive(Clone, Debug)]
pub struct Request<'a> {
    pub method: Method,
    pub path: &'a str,
    pub host: String,
    pub content_type: Option<String>,
    pub content_length: Option<u64>,
    /// True iff `Transfer-Encoding: chunked` was present (and no other
    /// encoding). RFC 9112 §6.1 — mutually exclusive with `content_length`.
    pub chunked: bool,
    /// Decoded body bytes. `Cow::Borrowed` for the Content-Length path
    /// (zero-copy slice into the input buffer); `Cow::Owned` for the
    /// chunked path (dechunked into a fresh `Vec<u8>`).
    pub body: Cow<'a, [u8]>,
    /// Total bytes consumed (headers + body).
    pub consumed: usize,
    /// Raw header lines preserved for later passes (T3 reads Bearer +
    /// X-Kessel-* from here without re-parsing).
    pub headers: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseError {
    BadRequestLine,
    MethodNotAllowed,
    NotFound,
    MissingHost,
    Ipv6LiteralHost,
    LengthRequired,
    BadHeaderValue(String),
    NoHeaderTerminator,
    HeaderTooLarge,
    UnsupportedMediaType,
    ShortBody,
    /// T3 adds these.
    BodyTooLarge,
}

/// Parse one HTTP/1.1 request. Returns `Ok(Request)` if well-formed AND
/// fully received; `Err(ParseError)` otherwise. `consumed` reports how many
/// bytes of `buf` belong to this request (so the caller can drop them).
pub fn parse_request(buf: &[u8]) -> Result<Request<'_>, ParseError> {
    // Cap headers up-front.
    let header_end = find_header_terminator(buf)?;
    if header_end > MAX_HEADER_BYTES {
        return Err(ParseError::HeaderTooLarge);
    }
    let head = std::str::from_utf8(buf.get(..header_end).unwrap_or(&[]))
        .map_err(|_| ParseError::BadRequestLine)?;
    let mut lines = head.split("\r\n");
    let req_line = lines.next().ok_or(ParseError::BadRequestLine)?;
    let (method, path) = parse_request_line(req_line)?;
    if !is_known_path(path) {
        return Err(ParseError::NotFound);
    }

    let mut host: Option<String> = None;
    let mut content_type: Option<String> = None;
    let mut content_length: Option<u64> = None;
    let mut chunked: bool = false;
    let mut headers: Vec<(String, String)> = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let colon = line.find(':').ok_or_else(||
            ParseError::BadHeaderValue(format!("missing colon: {line:?}")))?;
        let name = line.get(..colon).unwrap_or("").trim().to_string();
        let value = line.get(colon + 1..).unwrap_or("").trim().to_string();
        if name.eq_ignore_ascii_case("host") {
            // RFC 9112 §3.2 — duplicate Host is a smuggling primitive; reject.
            if host.is_some() {
                return Err(ParseError::BadHeaderValue(
                    "duplicate Host header".into()));
            }
            if value.starts_with('[') {
                return Err(ParseError::Ipv6LiteralHost);
            }
            host = Some(value.clone());
        } else if name.eq_ignore_ascii_case("content-type") {
            // Strip `; charset=…` and any other parameter; keep the
            // media-type only.
            let media = value.split(';').next().unwrap_or("").trim();
            content_type = Some(media.to_string());
        } else if name.eq_ignore_ascii_case("content-length") {
            // RFC 9112 §6.3.5 — duplicate Content-Length is OK only if
            // every value is equal; differing values → smuggling, reject.
            let new = value.parse::<u64>().map_err(|_|
                ParseError::BadHeaderValue(format!("Content-Length: {value:?}")))?;
            if let Some(existing) = content_length {
                if existing != new {
                    return Err(ParseError::BadHeaderValue(
                        format!("conflicting Content-Length: {existing} vs {new}")));
                }
            } else {
                content_length = Some(new);
            }
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            // RFC 9112 §7 — we only support the `chunked` token. Anything
            // else (gzip/identity/deflate/comma-list) is rejected so a
            // misconfigured peer can't ambiguously frame a request.
            if value.eq_ignore_ascii_case("chunked") {
                chunked = true;
            } else {
                return Err(ParseError::BadHeaderValue(
                    format!("unsupported Transfer-Encoding: {value:?}")));
            }
        }
        headers.push((name, value));
    }
    let host = host.ok_or(ParseError::MissingHost)?;

    // RFC 9112 §6.1 — if BOTH framings are present, reject (smuggling).
    if chunked && content_length.is_some() {
        return Err(ParseError::BadHeaderValue(
            "ConflictingFraming: both Content-Length and Transfer-Encoding".into()));
    }

    let body: Cow<'_, [u8]>;
    let consumed: usize;
    match method {
        Method::Get => {
            body = Cow::Borrowed(&[]);
            consumed = header_end;
        }
        Method::Post => {
            let body_start = header_end;
            let remaining = buf.get(body_start..).unwrap_or(&[]);
            let decoded = decode_body(
                remaining, content_length, chunked, DEFAULT_MAX_BODY)?;
            // `consumed` reports headers + framed-body-bytes-on-the-wire.
            // For Content-Length that's exactly `cl`; for chunked it's the
            // entire `remaining` (we required a 0-CRLF-CRLF terminator).
            let on_wire = match (content_length, chunked) {
                (Some(cl), false) => usize::try_from(cl).map_err(|_|
                    ParseError::BodyTooLarge)?,
                (None, true) => remaining.len(),
                // The (Some, true) and (None, false) cases already errored
                // inside decode_body — unreachable here.
                _ => 0,
            };
            consumed = body_start.checked_add(on_wire).ok_or(
                ParseError::BodyTooLarge)?;
            body = decoded;
        }
    }

    Ok(Request {
        method,
        path,
        host,
        content_type,
        content_length,
        chunked,
        body,
        consumed,
        headers,
    })
}

/// `\r\n\r\n` terminator → returns index just past it (so `header_end` is
/// also `body_start`).
fn find_header_terminator(buf: &[u8]) -> Result<usize, ParseError> {
    buf.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i + 4)
        .ok_or(ParseError::NoHeaderTerminator)
}

fn parse_request_line(line: &str) -> Result<(Method, &str), ParseError> {
    // RFC 9112 §3: METHOD SP PATH SP VERSION
    let mut parts = line.splitn(3, ' ');
    let m = parts.next().ok_or(ParseError::BadRequestLine)?;
    let p = parts.next().ok_or(ParseError::BadRequestLine)?;
    let v = parts.next().ok_or(ParseError::BadRequestLine)?;
    if v != "HTTP/1.1" {
        return Err(ParseError::BadRequestLine);
    }
    let method = match m {
        "GET" => Method::Get,
        "POST" => Method::Post,
        _ => return Err(ParseError::MethodNotAllowed),
    };
    Ok((method, p))
}

fn is_known_path(p: &str) -> bool {
    matches!(p, "/v1/sql" | "/v1/op" | "/v1/health" | "/v1/metrics")
}

/// Decode the body slice according to framing headers. Returns `Cow::Borrowed`
/// for the Content-Length path (zero-copy) and `Cow::Owned` for chunked.
pub fn decode_body<'a>(
    buf: &'a [u8],
    content_length: Option<u64>,
    chunked: bool,
    max_body: usize,
) -> Result<Cow<'a, [u8]>, ParseError> {
    match (content_length, chunked) {
        (Some(_), true) => Err(ParseError::BadHeaderValue(
            "ConflictingFraming: both Content-Length and Transfer-Encoding".into())),
        (None, false) => Err(ParseError::LengthRequired),
        (Some(cl), false) => {
            let cl_usize = usize::try_from(cl).map_err(|_|
                ParseError::BodyTooLarge)?;
            if cl_usize > max_body {
                return Err(ParseError::BodyTooLarge);
            }
            if buf.len() < cl_usize {
                return Err(ParseError::ShortBody);
            }
            Ok(Cow::Borrowed(buf.get(..cl_usize).unwrap_or(&[])))
        }
        (None, true) => {
            let owned = dechunk(buf, max_body)?;
            Ok(Cow::Owned(owned))
        }
    }
}

/// Decode RFC 9112 §7.1 chunked transfer-encoding. Cap on the OUTPUT length —
/// a lying chunk-size header can't exhaust memory because we check against
/// `max_body` on every appended chunk.
pub fn dechunk(mut b: &[u8], max_body: usize) -> Result<Vec<u8>, ParseError> {
    let mut out: Vec<u8> = Vec::new();
    loop {
        let nl = b.windows(2).position(|w| w == b"\r\n").ok_or(
            ParseError::BadHeaderValue("BadChunk: missing chunk-size CRLF".into()))?;
        let line = std::str::from_utf8(b.get(..nl).unwrap_or(&[])).map_err(|_|
            ParseError::BadHeaderValue("BadChunk: chunk-size not ASCII".into()))?;
        // Strip any chunk-ext after a ';'.
        let size_hex = line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16).map_err(|_|
            ParseError::BadHeaderValue("BadChunk: bad chunk size".into()))?;
        b = b.get(nl + 2..).unwrap_or(&[]);
        if size == 0 {
            return Ok(out);
        }
        if b.len() < size + 2 {
            return Err(ParseError::BadHeaderValue(
                "BadChunk: short chunk-data or missing trailing CRLF".into()));
        }
        if out.len().saturating_add(size) > max_body {
            return Err(ParseError::BodyTooLarge);
        }
        out.extend_from_slice(b.get(..size).unwrap_or(&[]));
        b = b.get(size + 2..).unwrap_or(&[]);
    }
}

/// Extract `Authorization: Bearer <token>` value as raw bytes, or None if
/// the header is absent / scheme is not Bearer.
pub fn extract_bearer(headers: &[(String, String)]) -> Option<&[u8]> {
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("authorization") {
            if let Some(tok) = value.strip_prefix("Bearer ") {
                return Some(tok.as_bytes());
            }
        }
    }
    None
}

/// Extract `X-Kessel-Client-Id` as a `u128`. Returns:
///   Ok(Some(id)) when present and well-formed (32 lowercase hex chars),
///   Ok(None) when absent,
///   Err(BadHeaderValue) when present but malformed.
pub fn extract_client_id(
    headers: &[(String, String)],
) -> Result<Option<u128>, ParseError> {
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("x-kessel-client-id") {
            if value.len() != 32 {
                return Err(ParseError::BadHeaderValue(
                    format!("X-Kessel-Client-Id length {} (want 32)", value.len())));
            }
            if !value.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
                return Err(ParseError::BadHeaderValue(
                    "X-Kessel-Client-Id must be 32 lowercase hex chars".into()));
            }
            let id = u128::from_str_radix(value, 16).map_err(|e|
                ParseError::BadHeaderValue(format!("X-Kessel-Client-Id parse: {e}")))?;
            return Ok(Some(id));
        }
    }
    Ok(None)
}

/// Extract `X-Kessel-Req-Seq` as a `u64` (decimal). Same shape as
/// `extract_client_id`.
pub fn extract_req_seq(
    headers: &[(String, String)],
) -> Result<Option<u64>, ParseError> {
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("x-kessel-req-seq") {
            let seq = value.parse::<u64>().map_err(|e|
                ParseError::BadHeaderValue(format!("X-Kessel-Req-Seq parse: {e}")))?;
            return Ok(Some(seq));
        }
    }
    Ok(None)
}
