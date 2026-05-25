//! HTTP/1.1 request parser (request line + headers + Content-Length body),
//! hand-rolled per RFC 9112. Mirrors the bounds-checked style of
//! `kessel-fetch::http`. Chunked transfer-encoding, body caps, Bearer, and
//! the X-Kessel-* exactly-once headers come in T3.

#![allow(dead_code)]

/// Hard caps applied at parse time (T3 makes the body cap configurable;
/// header cap stays fixed at 64 KiB per spec §4.1).
pub const MAX_HEADER_BYTES: usize = 64 * 1024;

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
    pub body: &'a [u8],
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
            content_length = Some(value.parse::<u64>().map_err(|_|
                ParseError::BadHeaderValue(format!("Content-Length: {value:?}")))?);
        }
        headers.push((name, value));
    }
    let host = host.ok_or(ParseError::MissingHost)?;

    // POSTs require Content-Length (T3 adds chunked-encoding support).
    let body: &[u8];
    let consumed: usize;
    match method {
        Method::Get => {
            body = &[];
            consumed = header_end;
        }
        Method::Post => {
            let cl = content_length.ok_or(ParseError::LengthRequired)?;
            let cl_usize = usize::try_from(cl).map_err(|_|
                ParseError::BodyTooLarge)?;
            let body_start = header_end;
            let body_end = body_start.checked_add(cl_usize).ok_or(
                ParseError::BodyTooLarge)?;
            if buf.len() < body_end {
                return Err(ParseError::ShortBody);
            }
            body = buf.get(body_start..body_end).unwrap_or(&[]);
            consumed = body_end;
        }
    }

    Ok(Request {
        method,
        path,
        host,
        content_type,
        content_length,
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
