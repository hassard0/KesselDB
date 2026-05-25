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
#[cfg(feature = "tls")]
mod tls;
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
    Parquet,
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
        let raw: Vec<Vec<json::Cell>> = match format {
            Format::Json => json::rows_at(&body, cols, rows_path)?,
            Format::Ndjson => ndjson::extract(&body, cols)?,
            Format::Csv => csv::extract(&body, cols)?,
            // Parquet is whole-object only; paginated streaming makes no sense.
            Format::Parquet => {
                return Err(FetchError::Parse(
                    "FORMAT PARQUET is single-object only (no pagination)".into(),
                ))
            }
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
            Pagination::CursorJson { path, param } => json::opt_string_at(
                &body, path,
            )?
            .map(|tok| set_query_param(base_url, param, &tok)),
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
    // Slice-1: a data endpoint fragment is not meaningful; strip it so
    // the appended query can never land after a `#`.
    let base = base.split_once('#').map_or(base, |(b, _)| b);
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

/// True if a Link entry's `rel=` param contains the `next` token
/// (RFC 8288 allows space-separated rel tokens, e.g. `rel="first next"`).
fn rel_has_next(part: &str) -> bool {
    part.split(';').skip(1).any(|p| {
        let p = p.trim();
        let Some(v) = p.strip_prefix("rel=") else { return false };
        v.trim_matches('"')
            .split_ascii_whitespace()
            .any(|t| t.eq_ignore_ascii_case("next"))
    })
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
            if rel_has_next(p) {
                let s = p.find('<')?;
                let e = p[s + 1..].find('>')? + s + 1;
                return Some(p[s + 1..e].to_string());
            }
        }
    }
    None
}

/// Decode a fetched body into coerced rows. Used by `fetch_rows`.
/// `fetch_rows_paginated` still has its own inline copy of this
/// decode+coerce tail; unifying it is a tracked follow-up (it is
/// behaviorally identical — `json::rows_at`/`csv::extract`/
/// `ndjson::extract` then the per-cell coerce loop). `rows_path` is
/// honored for `Format::Json` only (NDJSON/CSV ignore it, exactly as
/// the paginated loop does).
pub(crate) fn rows_from_body(
    body: &[u8],
    format: Format,
    cols: &[ColumnMap],
    rows_path: Option<&str>,
) -> Result<Vec<Vec<Vec<u8>>>, FetchError> {
    let raw: Vec<Vec<json::Cell>> = match format {
        Format::Json => json::rows_at(body, cols, rows_path)?,
        Format::Csv => csv::extract(body, cols)?,
        Format::Ndjson => ndjson::extract(body, cols)?,
        #[cfg(feature = "object-store")]
        Format::Parquet => {
            let names: Vec<&str> =
                cols.iter().map(|c| c.source.as_str()).collect();
            let pv = kessel_parquet::extract(body, &names)
                .map_err(|e| FetchError::Parse(e.to_string()))?;
            pv.into_iter()
                .map(|row| row.into_iter().map(pq_to_cell).collect())
                .collect()
        }
        #[cfg(not(feature = "object-store"))]
        Format::Parquet => {
            return Err(FetchError::Parse(
                "FORMAT PARQUET requires the object-store build".into(),
            ))
        }
    };
    let mut out = Vec::with_capacity(raw.len());
    for r in raw {
        let mut row = Vec::with_capacity(cols.len());
        for (i, cell) in r.into_iter().enumerate() {
            row.push(coerce::to_field_bytes(&cols[i].kind, cell)?);
        }
        out.push(row);
    }
    Ok(out)
}

/// Map a `PqValue` to the `json::Cell` representation that `coerce::to_field_bytes`
/// accepts, byte-identically to the JSON path for the same value.
#[cfg(feature = "object-store")]
fn pq_to_cell(v: kessel_parquet::PqValue) -> json::Cell {
    use kessel_parquet::PqValue::*;
    match v {
        Null => json::Cell::Null,
        Bool(b) => json::Cell::Bool(b),
        // Integers rendered via to_string() — the same textual form a JSON
        // numeric token carries, so coerce::to_field_bytes is byte-identical.
        I64(i) => json::Cell::Text(i.to_string()),
        F64(f) => json::Cell::Text(json::canonical_f64(f)),
        // BYTE_ARRAY/UTF8: lossy is intentional — same string semantics as
        // the JSON path (non-UTF8 bytes → replacement chars); do NOT change
        // to from_utf8()? (would break legitimate replacement-char inputs
        // and shift error handling on this determinism-relevant decode arm).
        Bytes(b) => json::Cell::Text(
            String::from_utf8_lossy(&b).into_owned(),
        ),
        // INT96 → ns since the Unix epoch (T3 OBJ-2c-4): surfaced as
        // decimal text. The typed `FieldKind::Timestamp` 8-byte
        // mapping (incl. sign handling for pre-1970) is the explicit
        // SP108 T6 follow-up; the Text-decimal path here is
        // end-to-end-correct for any downstream FieldKind::I64/Text.
        Timestamp(ns) => json::Cell::Text(ns.to_string()),
        // DECIMAL → unscaled i128 + scale (T3 OBJ-2c-4): surfaced as
        // the unscaled integer in decimal text. Scale is intentionally
        // dropped at the fetch boundary in this slice — the typed
        // `FieldKind::Fixed{scale}` mapping is the explicit T6
        // follow-up. Users targeting `FieldKind::I64` (or any text
        // sink) get the unscaled value losslessly today.
        Decimal { unscaled, scale: _ } => json::Cell::Text(unscaled.to_string()),
        // SP143 T2: LIST<primitive> — serialize the element vector to
        // a small JSON-shaped UTF-8 string and surface as Cell::Text.
        // Routing through Cell::Text (rather than adding a new
        // Cell::List variant) keeps the existing `Cell` enum + binary
        // protocol UNCHANGED in this slice. A typed Cell::List + a
        // FieldKind::List mapping is the explicit SP144 follow-up.
        // `pqvalue_list_to_json` emits ASCII-safe JSON (non-printable
        // bytes escaped as \uXXXX), so downstream string sinks are
        // round-trip-safe today.
        List(items) => json::Cell::Text(
            String::from_utf8(kessel_parquet::pqvalue_list_to_json(&items))
                .expect("pqvalue_list_to_json emits ASCII-safe UTF-8"),
        ),
    }
}

/// Test-only accessor for `rows_from_body` (the crate-internal decode+coerce
/// entry point). Not part of the stable public API.
#[doc(hidden)]
pub fn rows_from_body_for_test(
    body: &[u8],
    format: Format,
    cols: &[ColumnMap],
    rows_path: Option<&str>,
) -> Result<Vec<Vec<Vec<u8>>>, FetchError> {
    rows_from_body(body, format, cols, rows_path)
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
    rows_from_body(&body, format, cols, None)
}

/// Test-only: fetch over HTTPS with a caller-supplied trust config
/// (the localhost fixture). Reuses the exact production exchange +
/// decode path; differs from production only in WHICH roots are
/// trusted. Never reachable from any production caller.
#[cfg(feature = "tls")]
#[doc(hidden)]
pub fn fetch_rows_https_test(
    url: &str,
    auth: &Auth,
    format: Format,
    cols: &[ColumnMap],
    max_body: u64,
    trust_pem: &[u8],
) -> Result<Vec<Vec<Vec<u8>>>, FetchError> {
    let (scheme, host, port, path) = http::parse_target(url)?;
    assert_eq!(scheme, http::Scheme::Https, "test entry is https-only");
    let cfg = tls::test_config_trusting(trust_pem);
    let stream = tls::connect_tls_with(cfg, &host, port)?;
    let req = http::build_request(&path, &host, auth);
    let (_headers, body) = http::exchange(stream, &req, max_body)?;
    rows_from_body(&body, format, cols, None)
}

/// Fetch a single object over HTTPS using caller-supplied (already
/// signed) request headers, then decode exactly like `fetch_rows`.
/// HTTPS-only (object storage is always TLS); reuses the production
/// rustls transport + `exchange` + `rows_from_body`. Used by the
/// router's object-store path (`s3://` / `az://`).
#[cfg(feature = "object-store")]
pub fn fetch_rows_signed(
    https_url: &str,
    headers: &[(String, String)],
    format: Format,
    cols: &[ColumnMap],
    rows_path: Option<&str>,
    max_body: u64,
) -> Result<Vec<Vec<Vec<u8>>>, FetchError> {
    let (scheme, host, port, path) = http::parse_target(https_url)?;
    if scheme != http::Scheme::Https {
        return Err(FetchError::Http(
            "object-store fetch requires https://".into(),
        ));
    }
    let stream = tls::connect_tls(&host, port)?;
    let req = http::build_request_with_headers(&path, &host, headers);
    let (_h, body) = http::exchange(stream, &req, max_body)?;
    rows_from_body(&body, format, cols, rows_path)
}

/// Test-only companion to `fetch_rows_signed`: same logic but trusts
/// only the certs in `trust_pem` (the checked-in localhost fixture).
/// Mirrors `fetch_rows_https_test`; never reachable from production.
#[cfg(feature = "object-store")]
#[doc(hidden)]
pub fn fetch_rows_signed_test(
    https_url: &str,
    headers: &[(String, String)],
    format: Format,
    cols: &[ColumnMap],
    rows_path: Option<&str>,
    max_body: u64,
    trust_pem: &[u8],
) -> Result<Vec<Vec<Vec<u8>>>, FetchError> {
    let (scheme, host, port, path) = http::parse_target(https_url)?;
    if scheme != http::Scheme::Https {
        return Err(FetchError::Http(
            "object-store fetch requires https://".into(),
        ));
    }
    let cfg = tls::test_config_trusting(trust_pem);
    let stream = tls::connect_tls_with(cfg, &host, port)?;
    let req = http::build_request_with_headers(&path, &host, headers);
    let (_h, body) = http::exchange(stream, &req, max_body)?;
    rows_from_body(&body, format, cols, rows_path)
}

#[cfg(test)]
mod ptests {
    use super::*;

    #[test]
    fn set_query_param_handles_fragment_and_replace() {
        assert_eq!(set_query_param("http://h/p", "c", "T"), "http://h/p?c=T");
        assert_eq!(set_query_param("http://h/p#frag", "c", "T"), "http://h/p?c=T");
        assert_eq!(set_query_param("http://h/p?c=OLD&x=1", "c", "T"), "http://h/p?x=1&c=T");
        assert_eq!(set_query_param("http://h/p?x=1#f", "c", "T"), "http://h/p?x=1&c=T");
        // prefix-name must not be false-dropped
        assert_eq!(set_query_param("http://h/p?cx=1", "c", "T"), "http://h/p?cx=1&c=T");
    }

    #[test]
    fn link_next_matches_rel_token_set() {
        let h = |v: &str| vec![("Link".to_string(), v.to_string())];
        assert_eq!(link_next(&h(r#"<http://x/2>; rel="next""#)), Some("http://x/2".into()));
        assert_eq!(link_next(&h(r#"<http://x/2>; rel=next"#)), Some("http://x/2".into()));
        // space-separated rel token set (RFC 8288) — was previously missed
        assert_eq!(link_next(&h(r#"<http://x/2>; rel="first next""#)), Some("http://x/2".into()));
        // rel="nextpage" must NOT match `next`
        assert_eq!(link_next(&h(r#"<http://x/2>; rel="nextpage""#)), None);
        // multiple Link headers: pick the one with rel next
        let multi = vec![
            ("Link".to_string(), r#"<http://x/prev>; rel="prev""#.to_string()),
            ("link".to_string(), r#"<http://x/2>; rel="next""#.to_string()),
        ];
        assert_eq!(link_next(&multi), Some("http://x/2".into()));
        // no link header => None
        assert_eq!(link_next(&[("X".into(), "y".into())]), None);
    }

    #[test]
    fn rows_from_body_decodes_json_like_fetch_rows() {
        let cols = vec![ColumnMap {
            name: "id".into(),
            kind: FieldKind::U32,
            source: "id".into(),
        }];
        let rows =
            rows_from_body(br#"[{"id":9}]"#, Format::Json, &cols, None)
                .unwrap();
        assert_eq!(rows, vec![vec![vec![9, 0, 0, 0]]]);
    }
}
