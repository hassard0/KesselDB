//! kessel-fetch: external JSON/CSV-over-HTTP source fetch + parse.
//!
//! Optional, off by default. NEVER linked into the deterministic
//! kernel; the router uses it out-of-band and feeds only captured
//! rows back into the replicated log.
#![forbid(unsafe_code)]

mod coerce;
mod csv;
mod ndjson;
mod http;
pub(crate) mod json;

use kessel_catalog::FieldKind;

#[derive(Debug, PartialEq, Eq)]
pub enum FetchError {
    Http(String),
    Parse(String),
    Type(String),
    Auth(String),
    TooLarge(u64),
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchError::Http(s) => write!(f, "http: {s}"),
            FetchError::Parse(s) => write!(f, "parse: {s}"),
            FetchError::Type(s) => write!(f, "type: {s}"),
            FetchError::Auth(s) => write!(f, "auth: {s}"),
            FetchError::TooLarge(n) => write!(f, "body exceeds {n} bytes"),
        }
    }
}

/// One declared output column: its `FieldKind` and where to read it.
#[derive(Clone, Debug)]
pub struct ColumnMap {
    pub name: String,
    pub kind: FieldKind,
    /// JSON dotted path (FORMAT JSON / FORMAT NDJSON) or CSV header name (FORMAT CSV).
    pub source: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Json,
    Csv,
    Ndjson,
}

/// Auth resolved by the caller (router) from its own env — a value,
/// never a reference, and never persisted.
#[derive(Clone, Debug)]
pub enum Auth {
    None,
    Bearer(String),
    Header { name: String, value: String },
}

pub const DEFAULT_MAX_BODY: u64 = 64 * 1024 * 1024;

/// Test-only accessor for `http::get_resp` (the crate-internal function that
/// exposes response headers). Not part of the stable public API.
#[doc(hidden)]
pub fn http_get_resp_for_test(url: &str, max_body: u64) -> (Vec<(String, String)>, Vec<u8>) {
    http::get_resp(url, &Auth::None, max_body).expect("get_resp")
}

/// How to find the next page. Declared per source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pagination {
    /// JSON path (in the envelope) yielding the absolute next URL.
    NextUrlJson(String),
    /// Use the `Link: …; rel="next"` response header.
    NextLink,
    /// JSON path yielding an opaque token, set as `?param=token`.
    CursorJson { path: String, param: String },
}

/// Slice-1 hard caps (no per-source knobs yet).
const MAX_PAGES: u32 = 1000;
const MAX_TOTAL_BODY: u64 = 8 * DEFAULT_MAX_BODY;

/// Paginated fetch. Same return contract as `fetch_rows` — the
/// concatenated rows of every page. Bounded (MAX_PAGES, MAX_TOTAL_BODY)
/// + loop-detected; ANY error ⇒ Err (the caller must submit nothing).
pub fn fetch_rows_paginated(
    base_url: &str,
    auth: &Auth,
    format: Format,
    cols: &[ColumnMap],
    rows_path: Option<&str>,
    pagination: &Pagination,
    per_page_max_body: u64,
) -> Result<Vec<Vec<Vec<u8>>>, FetchError> {
    let mut out: Vec<Vec<Vec<u8>>> = Vec::new();
    let mut url = base_url.to_string();
    let mut seen: Vec<String> = Vec::new();
    let mut total: u64 = 0;
    for page in 0..=MAX_PAGES {
        if page == MAX_PAGES {
            return Err(FetchError::Http(format!(
                "pagination exceeded {MAX_PAGES} pages"
            )));
        }
        if seen.iter().any(|u| u == &url) {
            return Err(FetchError::Http(format!(
                "pagination loop detected at `{url}`"
            )));
        }
        seen.push(url.clone());
        let (headers, body) = http::get_resp(&url, auth, per_page_max_body)?;
        total = total.saturating_add(body.len() as u64);
        if total > MAX_TOTAL_BODY {
            return Err(FetchError::TooLarge(MAX_TOTAL_BODY));
        }
        let raw = match format {
            Format::Json => json::rows_at(&body, cols, rows_path)?,
            Format::Ndjson => ndjson::extract(&body, cols)?,
            Format::Csv => csv::extract(&body, cols)?,
        };
        for r in raw {
            let mut row = Vec::with_capacity(cols.len());
            for (i, cell) in r.into_iter().enumerate() {
                row.push(coerce::to_field_bytes(&cols[i].kind, cell)?);
            }
            out.push(row);
        }
        let next: Option<String> = match pagination {
            Pagination::NextUrlJson(p) => json::opt_string_at(&body, p)?,
            Pagination::CursorJson { path, param } => {
                match json::opt_string_at(&body, path)? {
                    None => None,
                    Some(tok) => Some(set_query_param(base_url, param, &tok)),
                }
            }
            Pagination::NextLink => link_next(&headers),
        };
        match next {
            None => return Ok(out),
            Some(n) => url = n,
        }
    }
    unreachable!()
}

/// Replace/append `?param=value` on `base` (slice-1: opaque
/// API-supplied ASCII tokens; no percent-encoding).
fn set_query_param(base: &str, param: &str, value: &str) -> String {
    let (path, query) = match base.split_once('?') {
        Some((p, q)) => (p, q),
        None => (base, ""),
    };
    let mut parts: Vec<String> = query
        .split('&')
        .filter(|kv| {
            !kv.is_empty() && !kv.starts_with(&format!("{param}="))
        })
        .map(|s| s.to_string())
        .collect();
    parts.push(format!("{param}={value}"));
    format!("{path}?{}", parts.join("&"))
}

/// Find the `rel="next"` target across ALL `Link` response headers
/// (RFC 8288). A server may send multiple `Link` headers OR one
/// header with comma-separated entries — handle both. The first
/// `rel=next` (or `rel="next"`) match wins.
fn link_next(headers: &[(String, String)]) -> Option<String> {
    for (k, v) in headers {
        if !k.eq_ignore_ascii_case("link") {
            continue;
        }
        for part in v.split(',') {
            let p = part.trim();
            if p.contains("rel=\"next\"") || p.contains("rel=next") {
                let s = p.find('<')?;
                let e = p[s + 1..].find('>')? + s + 1;
                return Some(p[s + 1..e].to_string());
            }
        }
    }
    None
}

/// Fetch + parse. Returns one `Vec<(column-index, raw FieldKind bytes)>`
/// per row, columns in `cols` order. Pure given the bytes the server
/// returned (the only nondeterminism is the network, owned by `http`).
pub fn fetch_rows(
    url: &str,
    auth: &Auth,
    format: Format,
    cols: &[ColumnMap],
    max_body: u64,
) -> Result<Vec<Vec<Vec<u8>>>, FetchError> {
    let body = http::get(url, auth, max_body)?;
    let raw_rows = match format {
        Format::Json => json::extract(&body, cols)?,
        Format::Csv => csv::extract(&body, cols)?,
        Format::Ndjson => ndjson::extract(&body, cols)?,
    };
    let mut out = Vec::with_capacity(raw_rows.len());
    for r in raw_rows {
        let mut row = Vec::with_capacity(cols.len());
        for (i, cell) in r.into_iter().enumerate() {
            row.push(coerce::to_field_bytes(&cols[i].kind, cell)?);
        }
        out.push(row);
    }
    Ok(out)
}
