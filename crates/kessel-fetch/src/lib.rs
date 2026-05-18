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
    /// JSON dotted path (FORMAT JSON) or CSV header name (FORMAT CSV).
    pub source: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Json,
    Csv,
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
