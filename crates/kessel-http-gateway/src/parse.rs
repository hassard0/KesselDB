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
    /// Generic catch-all for malformed header values that don't fit a more
    /// specific variant (e.g. missing colon, non-decimal Content-Length).
    BadHeaderValue(String),
    NoHeaderTerminator,
    HeaderTooLarge,
    UnsupportedMediaType,
    ShortBody,
    /// T3 adds these.
    BodyTooLarge,
    /// Both `Content-Length` and `Transfer-Encoding` present in the same
    /// request — RFC 9112 §6.1 smuggling primitive; reject.
    ConflictingFraming,
    /// Duplicate `Host` header. RFC 9112 §3.2.
    DuplicateHost,
    /// Differing `Content-Length` values across multiple Content-Length
    /// headers. RFC 9112 §6.3.5.
    DuplicateContentLength,
    /// A duplicate single-instance header (Authorization, X-Kessel-Client-Id,
    /// X-Kessel-Req-Seq). The String carries the header name.
    DuplicateHeader(String),
    /// Malformed chunked transfer-encoding (bad size, missing CRLF, etc.).
    /// The String carries a debugging detail.
    BadChunk(String),
    /// Transfer-Encoding token other than `chunked` (V1 only supports
    /// chunked). The String carries the offending token for diagnostics.
    UnsupportedTransferEncoding(String),
    /// `Expect: 100-continue` is not supported by the V1 gateway (we read the
    /// whole body off the wire eagerly into one parse buffer; we cannot
    /// honor a hold-and-wait dance). RFC 9110 §10.1.1 — answer 417.
    ExpectationFailed,
    /// SP144H T4: Both `X-Kessel-Client-Id` and `X-Kessel-Req-Seq` headers
    /// are required together (both-or-neither). One present without the
    /// other is rejected with this dedicated variant (was previously
    /// stuffed into BadHeaderValue(String) in SP141 — fragile because
    /// KAT assertions string-grepped the message).
    IncompleteSessionBinding,
}

/// Parse one HTTP/1.1 request. Returns `Ok(Request)` if well-formed AND
/// fully received; `Err(ParseError)` otherwise. `consumed` reports how many
/// bytes of `buf` belong to this request (so the caller can drop them).
///
/// `max_body` caps the decoded body length (RFC 9112 §4.1 — V1 spec defaults
/// to `DEFAULT_MAX_BODY` = 8 MiB; `ServerConfig.http_max_body` overrides via
/// `serve()`'s `max_body` parameter).
pub fn parse_request(buf: &[u8], max_body: usize) -> Result<Request<'_>, ParseError> {
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
                return Err(ParseError::DuplicateHost);
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
                    return Err(ParseError::DuplicateContentLength);
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
                return Err(ParseError::UnsupportedTransferEncoding(value.clone()));
            }
        }
        headers.push((name, value));
    }
    let host = host.ok_or(ParseError::MissingHost)?;

    // RFC 9112 §6.1 — if BOTH framings are present, reject (smuggling).
    if chunked && content_length.is_some() {
        return Err(ParseError::ConflictingFraming);
    }

    // RFC 9110 §10.1.1 — `Expect: 100-continue` requires the server to
    // respond 100 (Continue) before the client sends the body. The V1
    // gateway reads the body eagerly off a single parse buffer and cannot
    // honor the hold-and-wait dance; advertise 417 so clients fall back.
    // Only flag when there's actually a body to expect (avoids penalizing
    // a GET that carries the header by mistake).
    let has_body = content_length.unwrap_or(0) > 0 || chunked;
    if has_body {
        for (name, value) in &headers {
            if name.eq_ignore_ascii_case("expect")
                && value.eq_ignore_ascii_case("100-continue")
            {
                return Err(ParseError::ExpectationFailed);
            }
        }
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
            let (decoded, body_consumed_bytes) = decode_body(
                remaining, content_length, chunked, max_body)?;
            // `consumed` reports headers + framed-body-bytes-on-the-wire.
            // For Content-Length that's exactly `cl`; for chunked it's the
            // exact byte count `dechunk` walked (post-0-CRLF trailer CRLF
            // included).
            consumed = body_start.checked_add(body_consumed_bytes).ok_or(
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

/// Convenience wrapper: parse with the spec's `DEFAULT_MAX_BODY` cap. Used
/// by parse-time unit tests where the configurable cap is not under test.
/// Production callers must use `parse_request` directly and pass the
/// `ServerConfig.http_max_body` they were configured with.
pub fn parse_request_default(buf: &[u8]) -> Result<Request<'_>, ParseError> {
    parse_request(buf, DEFAULT_MAX_BODY)
}

/// SP147 T1: HTTP/1.1 keep-alive negotiation. Returns true if the request
/// asked the server to close the connection after responding. Per RFC 9112
/// §9.3, HTTP/1.1 is persistent by default — keep-alive unless explicitly
/// `Connection: close` is sent. (The legacy `Connection: keep-alive` header
/// is accepted as an explicit affirmative for clarity.)
pub fn wants_close(headers: &[(String, String)]) -> bool {
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("connection") {
            // RFC 9110 §7.6.1: Connection header is a comma-separated list
            // of options. Look for "close" token (case-insensitive).
            for token in value.split(',') {
                let t = token.trim();
                if t.eq_ignore_ascii_case("close") {
                    return true;
                }
            }
        }
    }
    false
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
    // SP-WS T2: `/v1/ws` is the dedicated WebSocket upgrade path per
    // spec §6.1. Listed here so `GET /v1/ws` parses through the route
    // table instead of surfacing as a 404. The actual upgrade arm in
    // `routes::handle` gates on `ws::is_websocket_upgrade(&headers)` so
    // a plain `GET /v1/ws` (no Upgrade header) still routes through
    // `handle()` and falls through to the catch-all 404 — only a true
    // upgrade request reaches `ws::handle_upgrade`.
    matches!(p, "/v1/sql" | "/v1/op" | "/v1/health" | "/v1/metrics"
        | "/v1/ws")
}

/// Decode the body slice according to framing headers. Returns the decoded
/// body bytes plus the number of bytes consumed from `buf` (which equals
/// `cl` for Content-Length and the exact chunk-stream length for chunked).
/// `Cow::Borrowed` for Content-Length (zero-copy) and `Cow::Owned` for chunked.
pub fn decode_body<'a>(
    buf: &'a [u8],
    content_length: Option<u64>,
    chunked: bool,
    max_body: usize,
) -> Result<(Cow<'a, [u8]>, usize), ParseError> {
    match (content_length, chunked) {
        (Some(_), true) => Err(ParseError::ConflictingFraming),
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
            Ok((Cow::Borrowed(buf.get(..cl_usize).unwrap_or(&[])), cl_usize))
        }
        (None, true) => {
            let (owned, consumed) = dechunk(buf, max_body)?;
            Ok((Cow::Owned(owned), consumed))
        }
    }
}

/// Decode RFC 9112 §7.1 chunked transfer-encoding. Cap on the OUTPUT length —
/// a lying chunk-size header can't exhaust memory because we check against
/// `max_body` on every appended chunk. Returns `(decoded, consumed)` where
/// `consumed` is the number of bytes from `b` that belonged to the
/// chunk-stream (last-chunk `0\r\n` + trailer-section CRLF inclusive).
pub fn dechunk(b: &[u8], max_body: usize) -> Result<(Vec<u8>, usize), ParseError> {
    let start_len = b.len();
    let mut b = b;
    let mut out: Vec<u8> = Vec::new();
    loop {
        let nl = b.windows(2).position(|w| w == b"\r\n").ok_or(
            ParseError::BadChunk("missing chunk-size CRLF".into()))?;
        let line = std::str::from_utf8(b.get(..nl).unwrap_or(&[])).map_err(|_|
            ParseError::BadChunk("chunk-size not ASCII".into()))?;
        // Strip any chunk-ext after a ';'.
        let size_hex = line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16).map_err(|_|
            ParseError::BadChunk("bad chunk size".into()))?;
        b = b.get(nl + 2..).unwrap_or(&[]);
        if size == 0 {
            // RFC 9112 §7.1: last-chunk = "0" CRLF (above), then
            // trailer-section, then a final CRLF. We don't support trailers,
            // so the only thing we expect here is a bare CRLF closing the
            // trailer-section. Consume it if present so `consumed` reflects
            // the full chunk-stream length.
            if let Some(rest) = b.strip_prefix(b"\r\n") {
                b = rest;
            }
            return Ok((out, start_len - b.len()));
        }
        // CRITICAL: a lying `size` like `ffffffffffffffff` parses to
        // `usize::MAX`; `size + 2` would panic in debug or wrap to 1 in
        // release. Use `checked_add` so any chunk-size that overflows the
        // address space is treated as oversized body, not arithmetic UB.
        let needed = size.checked_add(2).ok_or(ParseError::BodyTooLarge)?;
        if b.len() < needed {
            return Err(ParseError::BadChunk(
                "short chunk-data or missing trailing CRLF".into()));
        }
        if out.len().saturating_add(size) > max_body {
            return Err(ParseError::BodyTooLarge);
        }
        out.extend_from_slice(b.get(..size).unwrap_or(&[]));
        b = b.get(needed..).unwrap_or(&[]);
    }
}

/// Extract `Authorization: Bearer <token>` value as raw bytes, or `Ok(None)`
/// if the header is absent / scheme is not Bearer. Returns
/// `Err(DuplicateHeader)` if the header appears more than once — Authorization
/// is an exactly-once header per the gateway spec, and accepting duplicates
/// would let a peer smuggle a second token past header-aware proxies.
pub fn extract_bearer(headers: &[(String, String)]) -> Result<Option<&[u8]>, ParseError> {
    let mut found: Option<&[u8]> = None;
    let mut seen = false;
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("authorization") {
            if seen {
                return Err(ParseError::DuplicateHeader("Authorization".into()));
            }
            seen = true;
            // RFC 6750 §2.1 — the `Bearer` scheme name is case-insensitive.
            // Match the scheme via a lowercase comparison while preserving
            // the original casing of the token bytes that follow.
            let lower = value.to_ascii_lowercase();
            if let Some(rest) = lower.strip_prefix("bearer ") {
                let token_start = value.len() - rest.len();
                found = Some(value.as_bytes().get(token_start..).unwrap_or(&[]));
            }
            // Wrong scheme → treated as "no Bearer found", not an error.
        }
    }
    Ok(found)
}

/// Extract `X-Kessel-Client-Id` as a `u128`. Returns:
///   Ok(Some(id)) when present and well-formed (32 lowercase hex chars),
///   Ok(None) when absent,
///   Err(BadHeaderValue) when present but malformed,
///   Err(DuplicateHeader) when the header appears more than once.
pub fn extract_client_id(
    headers: &[(String, String)],
) -> Result<Option<u128>, ParseError> {
    let mut found: Option<u128> = None;
    let mut seen = false;
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("x-kessel-client-id") {
            if seen {
                return Err(ParseError::DuplicateHeader(
                    "X-Kessel-Client-Id".into()));
            }
            seen = true;
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
            found = Some(id);
        }
    }
    Ok(found)
}

/// Extract `X-Kessel-Req-Seq` as a `u64` (decimal). Same shape as
/// `extract_client_id`: exactly-once, duplicates rejected.
pub fn extract_req_seq(
    headers: &[(String, String)],
) -> Result<Option<u64>, ParseError> {
    let mut found: Option<u64> = None;
    let mut seen = false;
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("x-kessel-req-seq") {
            if seen {
                return Err(ParseError::DuplicateHeader(
                    "X-Kessel-Req-Seq".into()));
            }
            seen = true;
            let seq = value.parse::<u64>().map_err(|e|
                ParseError::BadHeaderValue(format!("X-Kessel-Req-Seq parse: {e}")))?;
            found = Some(seq);
        }
    }
    Ok(found)
}
