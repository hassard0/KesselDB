# External Sources (EXT) Slice 1 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Register external JSON/CSV-over-HTTP sources and materialize them into normal KesselDB types via an explicit, replicated `REFRESH`, without touching the deterministic kernel.

**Architecture:** A new off-by-default optional crate `kessel-fetch` (cargo feature `external-sources`) does all HTTP/parse work. The catalog gains a backward-compatible, Catalog-level `external` recipe list (SP86-style trailer, *not* a new `ObjectType` field — avoids touching ~77 struct literals). `kessel-sql` parses three new DDL statements into new `Op` variants. The `kesseldb-server` router handles `REFRESH` out-of-band: resolve env-ref auth, fetch+parse, derive a deterministic `ObjectId` from the declared `KEY`, and submit one atomic `Op::Txn` of `Create`/`Update` through the existing replicated-log path. Kernel crates see only captured rows.

**Tech Stack:** Rust (edition 2021, workspace), pure-std HTTP/1.1 + JSON + CSV (no external deps), `kessel-crypto::sha256` for id derivation, existing `kessel-proto`/`kessel-catalog`/`kessel-sql`/`kesseldb-server`.

---

## Spec reference

Design: `docs/superpowers/specs/2026-05-18-external-sources-design.md`. Read it before starting.

## File structure (decomposition locked here)

- **Create `crates/kessel-fetch/`** — new optional crate. One responsibility: turn `(url, auth, format, mapping)` into typed rows or a typed error. No KesselDB types leak in except `kessel_catalog::FieldKind` for coercion.
  - `crates/kessel-fetch/Cargo.toml`
  - `crates/kessel-fetch/src/lib.rs` — public API + error type
  - `crates/kessel-fetch/src/http.rs` — minimal HTTP/1.1 GET client
  - `crates/kessel-fetch/src/json.rs` — JSON value parser + dotted-path extract
  - `crates/kessel-fetch/src/csv.rs` — RFC 4180 reader
  - `crates/kessel-fetch/src/coerce.rs` — string/JSON-scalar → `FieldKind` bytes
  - `crates/kessel-fetch/tests/stub_server.rs` — in-process TCP stub + integration tests
- **Modify `crates/kessel-catalog/src/lib.rs`** — add `ExternalRecipe` struct + `Catalog.external: Vec<ExternalRecipe>` serialized in a new Catalog-level backward-compatible trailer (after the existing per-type loop). Existing `ObjectType` literals untouched.
- **Modify `crates/kessel-proto/src/lib.rs`** — three new `Op` variants: `CreateExternalSource`, `DropExternalSource`, `RefreshExternalSource` + their `kind()`/`encode()`/`decode()` arms + `is_mutating()` (all three mutate).
- **Modify `crates/kessel-sm/src/lib.rs`** — apply arms for `CreateExternalSource` (creates the backing type + stores recipe in catalog) and `DropExternalSource` (drops type + recipe). `RefreshExternalSource` is **never applied at the SM** (router-only); SM returns `SchemaError` if it ever sees one (defensive).
- **Modify `crates/kessel-sql/src/lib.rs`** — parse `CREATE EXTERNAL SOURCE …`, `DROP EXTERNAL SOURCE <n>`, `REFRESH <n>` into the new ops.
- **Modify `crates/kesseldb-server/src/router.rs`** — route the three ops; `RefreshExternalSource` handled in `Conn` (fetch via `kessel-fetch`, build upsert `Op::Txn`, submit through existing path). Feature-gated.
- **Modify `crates/kesseldb-server/Cargo.toml`** — optional `kessel-fetch` dep + `external-sources` feature.

## Determinism rule for every task

Run the regression and the determinism corpus after kernel-touching tasks:

```
cargo test --workspace --release
```

Expected: all green, including `kessel-vsr` seed-corpus and `large_seed_corpus_is_deterministic_and_converges`. With `--features external-sources` off (default), counts must match pre-plan baseline.

---

## Phase 1 — `kessel-fetch` crate (pure, standalone, no KesselDB wiring)

### Task 1: Scaffold the crate

**Files:**
- Create: `crates/kessel-fetch/Cargo.toml`
- Create: `crates/kessel-fetch/src/lib.rs`
- Modify: `Cargo.toml:3-19` (workspace members list)

- [ ] **Step 1: Add the crate to the workspace**

In `Cargo.toml`, add to `members` after `"crates/kessel-client",`:

```toml
    "crates/kessel-fetch",
```

- [ ] **Step 2: Create `crates/kessel-fetch/Cargo.toml`**

```toml
[package]
name = "kessel-fetch"
edition = "2021"
version = "0.0.1"
license = "UNLICENSED"

[dependencies]
kessel-catalog = { path = "../kessel-catalog" }

[dev-dependencies]
```

(No third-party deps — pure std. `kessel-catalog` only for `FieldKind`.)

- [ ] **Step 3: Create `crates/kessel-fetch/src/lib.rs` skeleton**

```rust
//! kessel-fetch: external JSON/CSV-over-HTTP source fetch + parse.
//!
//! Optional, off by default. NEVER linked into the deterministic
//! kernel; the router uses it out-of-band and feeds only captured
//! rows back into the replicated log.
#![forbid(unsafe_code)]

mod coerce;
mod csv;
mod http;
mod json;

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
```

- [ ] **Step 4: Verify it compiles (modules are stubs next)**

Run: `cargo build -p kessel-fetch`
Expected: FAIL — unresolved modules `coerce/csv/http/json`. That's expected; created in the next tasks.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/kessel-fetch/Cargo.toml crates/kessel-fetch/src/lib.rs
git commit -m "kessel-fetch: scaffold optional external-source crate"
```

---

### Task 2: JSON parser + dotted-path extract

**Files:**
- Create: `crates/kessel-fetch/src/json.rs`

The parser handles a JSON **array of flat objects**. `extract` returns, per array element, a `Vec<Cell>` in `cols` order where `Cell` is the scalar at `col.source` (a dotted path) rendered as a canonical string for coercion.

- [ ] **Step 1: Write failing tests at the bottom of `crates/kessel-fetch/src/json.rs`**

```rust
//! Minimal JSON: array of objects, scalar dotted-path extraction.
use crate::{ColumnMap, FetchError};
use kessel_catalog::FieldKind;

/// A JSON scalar rendered canonically for coercion.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Cell {
    Null,
    Bool(bool),
    /// number or string, kept as its source text (numbers un-reformatted)
    Text(String),
}

pub fn extract(
    body: &[u8],
    cols: &[ColumnMap],
) -> Result<Vec<Vec<Cell>>, FetchError> {
    let v = parse(std::str::from_utf8(body).map_err(|_| {
        FetchError::Parse("body is not UTF-8".into())
    })?)?;
    let arr = match v {
        Json::Array(a) => a,
        _ => return Err(FetchError::Parse("top level must be an array".into())),
    };
    let mut rows = Vec::with_capacity(arr.len());
    for el in &arr {
        let mut row = Vec::with_capacity(cols.len());
        for c in cols {
            row.push(path_get(el, &c.source)?);
        }
        rows.push(row);
    }
    Ok(rows)
}

#[derive(Debug, Clone, PartialEq)]
enum Json {
    Null,
    Bool(bool),
    Num(String),
    Str(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cm(name: &str, src: &str) -> ColumnMap {
        ColumnMap { name: name.into(), kind: FieldKind::U64, source: src.into() }
    }

    #[test]
    fn extracts_flat_and_nested_scalars() {
        let body = br#"[{"id":1,"u":{"name":"ann"}},{"id":2,"u":{"name":"bo"}}]"#;
        let cols = vec![cm("id", "id"), cm("nm", "u.name")];
        let rows = extract(body, &cols).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Cell::Text("1".into()), Cell::Text("ann".into())],
                vec![Cell::Text("2".into()), Cell::Text("bo".into())],
            ]
        );
    }

    #[test]
    fn null_and_bool_and_missing_path() {
        let body = br#"[{"a":null,"b":true}]"#;
        let cols = vec![cm("a", "a"), cm("b", "b")];
        let rows = extract(body, &cols).unwrap();
        assert_eq!(rows, vec![vec![Cell::Null, Cell::Bool(true)]]);
        // missing path is a typed parse error (deterministic, explicit)
        let bad = vec![cm("x", "nope")];
        assert!(matches!(extract(body, &bad), Err(FetchError::Parse(_))));
    }

    #[test]
    fn rejects_non_array_top_level_and_bad_json() {
        assert!(matches!(extract(b"{}", &[]), Err(FetchError::Parse(_))));
        assert!(matches!(extract(b"[", &[]), Err(FetchError::Parse(_))));
    }

    #[test]
    fn handles_strings_with_escapes_and_numbers() {
        let body = br#"[{"s":"a\"b\n","n":-12.5}]"#;
        let cols = vec![cm("s", "s"), cm("n", "n")];
        let rows = extract(body, &cols).unwrap();
        assert_eq!(
            rows,
            vec![vec![Cell::Text("a\"b\n".into()), Cell::Text("-12.5".into())]]
        );
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p kessel-fetch json::tests -- --nocapture`
Expected: FAIL to compile — `parse` and `path_get` undefined.

- [ ] **Step 3: Implement `parse` and `path_get` above the `#[cfg(test)]` block**

```rust
fn path_get(v: &Json, path: &str) -> Result<Cell, FetchError> {
    let mut cur = v;
    for seg in path.split('.') {
        match cur {
            Json::Object(m) => {
                cur = m
                    .iter()
                    .find(|(k, _)| k == seg)
                    .map(|(_, vv)| vv)
                    .ok_or_else(|| {
                        FetchError::Parse(format!("path `{path}`: no key `{seg}`"))
                    })?;
            }
            _ => {
                return Err(FetchError::Parse(format!(
                    "path `{path}`: `{seg}` is not an object"
                )))
            }
        }
    }
    match cur {
        Json::Null => Ok(Cell::Null),
        Json::Bool(b) => Ok(Cell::Bool(*b)),
        Json::Num(n) => Ok(Cell::Text(n.clone())),
        Json::Str(s) => Ok(Cell::Text(s.clone())),
        _ => Err(FetchError::Parse(format!(
            "path `{path}` is not a scalar"
        ))),
    }
}

struct P<'a> {
    b: &'a [u8],
    i: usize,
}

fn parse(s: &str) -> Result<Json, FetchError> {
    let mut p = P { b: s.as_bytes(), i: 0 };
    p.ws();
    let v = p.value()?;
    p.ws();
    if p.i != p.b.len() {
        return Err(FetchError::Parse("trailing data after JSON".into()));
    }
    Ok(v)
}

impl<'a> P<'a> {
    fn ws(&mut self) {
        while self.i < self.b.len()
            && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r')
        {
            self.i += 1;
        }
    }
    fn byte(&self) -> Result<u8, FetchError> {
        self.b
            .get(self.i)
            .copied()
            .ok_or_else(|| FetchError::Parse("unexpected end of JSON".into()))
    }
    fn value(&mut self) -> Result<Json, FetchError> {
        self.ws();
        match self.byte()? {
            b'{' => self.object(),
            b'[' => self.array(),
            b'"' => Ok(Json::Str(self.string()?)),
            b't' => self.lit("true", Json::Bool(true)),
            b'f' => self.lit("false", Json::Bool(false)),
            b'n' => self.lit("null", Json::Null),
            _ => self.number(),
        }
    }
    fn lit(&mut self, kw: &str, j: Json) -> Result<Json, FetchError> {
        if self.b[self.i..].starts_with(kw.as_bytes()) {
            self.i += kw.len();
            Ok(j)
        } else {
            Err(FetchError::Parse(format!("expected `{kw}`")))
        }
    }
    fn number(&mut self) -> Result<Json, FetchError> {
        let start = self.i;
        while self.i < self.b.len()
            && matches!(self.b[self.i],
                b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
        {
            self.i += 1;
        }
        if self.i == start {
            return Err(FetchError::Parse("expected value".into()));
        }
        Ok(Json::Num(
            std::str::from_utf8(&self.b[start..self.i]).unwrap().to_string(),
        ))
    }
    fn string(&mut self) -> Result<String, FetchError> {
        self.i += 1; // opening quote
        let mut s = String::new();
        loop {
            let c = self.byte()?;
            self.i += 1;
            match c {
                b'"' => return Ok(s),
                b'\\' => {
                    let e = self.byte()?;
                    self.i += 1;
                    match e {
                        b'"' => s.push('"'),
                        b'\\' => s.push('\\'),
                        b'/' => s.push('/'),
                        b'n' => s.push('\n'),
                        b't' => s.push('\t'),
                        b'r' => s.push('\r'),
                        b'b' => s.push('\u{0008}'),
                        b'f' => s.push('\u{000C}'),
                        b'u' => {
                            let hex = self
                                .b
                                .get(self.i..self.i + 4)
                                .ok_or_else(|| {
                                    FetchError::Parse("bad \\u escape".into())
                                })?;
                            let cp = u32::from_str_radix(
                                std::str::from_utf8(hex).map_err(|_| {
                                    FetchError::Parse("bad \\u".into())
                                })?,
                                16,
                            )
                            .map_err(|_| FetchError::Parse("bad \\u".into()))?;
                            self.i += 4;
                            s.push(
                                char::from_u32(cp).unwrap_or('\u{FFFD}'),
                            );
                        }
                        _ => {
                            return Err(FetchError::Parse(
                                "bad escape".into(),
                            ))
                        }
                    }
                }
                _ => s.push(c as char),
            }
        }
    }
    fn array(&mut self) -> Result<Json, FetchError> {
        self.i += 1;
        let mut out = Vec::new();
        self.ws();
        if self.byte()? == b']' {
            self.i += 1;
            return Ok(Json::Array(out));
        }
        loop {
            out.push(self.value()?);
            self.ws();
            match self.byte()? {
                b',' => {
                    self.i += 1;
                }
                b']' => {
                    self.i += 1;
                    return Ok(Json::Array(out));
                }
                _ => return Err(FetchError::Parse("expected `,` or `]`".into())),
            }
        }
    }
    fn object(&mut self) -> Result<Json, FetchError> {
        self.i += 1;
        let mut out = Vec::new();
        self.ws();
        if self.byte()? == b'}' {
            self.i += 1;
            return Ok(Json::Object(out));
        }
        loop {
            self.ws();
            if self.byte()? != b'"' {
                return Err(FetchError::Parse("expected object key".into()));
            }
            let k = self.string()?;
            self.ws();
            if self.byte()? != b':' {
                return Err(FetchError::Parse("expected `:`".into()));
            }
            self.i += 1;
            let v = self.value()?;
            out.push((k, v));
            self.ws();
            match self.byte()? {
                b',' => {
                    self.i += 1;
                }
                b'}' => {
                    self.i += 1;
                    return Ok(Json::Object(out));
                }
                _ => return Err(FetchError::Parse("expected `,` or `}`".into())),
            }
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p kessel-fetch json::tests`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/kessel-fetch/src/json.rs
git commit -m "kessel-fetch: JSON parser + dotted-path scalar extract"
```

---

### Task 3: CSV reader (RFC 4180, by header name)

**Files:**
- Create: `crates/kessel-fetch/src/csv.rs`

- [ ] **Step 1: Write failing tests at the bottom of `crates/kessel-fetch/src/csv.rs`**

```rust
//! RFC 4180 CSV: first row is the header; columns selected by name.
use crate::json::Cell;
use crate::{ColumnMap, FetchError};
use kessel_catalog::FieldKind;

pub fn extract(
    body: &[u8],
    cols: &[ColumnMap],
) -> Result<Vec<Vec<Cell>>, FetchError> {
    let text = std::str::from_utf8(body)
        .map_err(|_| FetchError::Parse("CSV not UTF-8".into()))?;
    let mut records = parse_records(text)?;
    if records.is_empty() {
        return Ok(Vec::new());
    }
    let header = records.remove(0);
    let mut idx = Vec::with_capacity(cols.len());
    for c in cols {
        let p = header.iter().position(|h| h == &c.source).ok_or_else(|| {
            FetchError::Parse(format!("CSV header has no column `{}`", c.source))
        })?;
        idx.push(p);
    }
    let mut rows = Vec::with_capacity(records.len());
    for rec in records {
        let mut row = Vec::with_capacity(cols.len());
        for &p in &idx {
            let v = rec.get(p).cloned().ok_or_else(|| {
                FetchError::Parse("CSV row shorter than header".into())
            })?;
            // empty unquoted field => Null; everything else => Text
            row.push(if v.is_empty() { Cell::Null } else { Cell::Text(v) });
        }
        rows.push(row);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cm(name: &str, src: &str) -> ColumnMap {
        ColumnMap { name: name.into(), kind: FieldKind::U64, source: src.into() }
    }

    #[test]
    fn header_selected_by_name_with_quotes_and_newlines() {
        let body = b"id,note\r\n1,\"a,b\"\r\n2,\"line\r\nbreak\"\r\n";
        let cols = vec![cm("n", "note"), cm("i", "id")];
        let rows = extract(body, &cols).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Cell::Text("a,b".into()), Cell::Text("1".into())],
                vec![Cell::Text("line\r\nbreak".into()), Cell::Text("2".into())],
            ]
        );
    }

    #[test]
    fn empty_field_is_null_and_escaped_quote() {
        let body = b"a,b\n,\"x\"\"y\"\n";
        let cols = vec![cm("a", "a"), cm("b", "b")];
        let rows = extract(body, &cols).unwrap();
        assert_eq!(rows, vec![vec![Cell::Null, Cell::Text("x\"y".into())]]);
    }

    #[test]
    fn missing_header_column_is_parse_error() {
        let body = b"a\n1\n";
        assert!(matches!(
            extract(body, &[cm("x", "zzz")]),
            Err(FetchError::Parse(_))
        ));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p kessel-fetch csv::tests`
Expected: FAIL to compile — `parse_records` undefined; `mod csv;`/`pub mod json` wiring (json's `Cell`/`mod` must be reachable from csv).

- [ ] **Step 3: Make `json` items visible to `csv` and implement `parse_records`**

In `crates/kessel-fetch/src/lib.rs` change `mod json;` to `pub(crate) mod json;` and `mod csv;` stays. In `crates/kessel-fetch/src/json.rs` change `enum Cell` to `pub enum Cell` and the `mod tests` `use super::*;` still works.

Add to `crates/kessel-fetch/src/csv.rs` above `#[cfg(test)]`:

```rust
/// Split RFC 4180 text into records of fields. Quotes allow `,`,
/// CR, LF; `""` is an escaped quote. Trailing newline ignored.
fn parse_records(s: &str) -> Result<Vec<Vec<String>>, FetchError> {
    let b = s.as_bytes();
    let mut recs: Vec<Vec<String>> = Vec::new();
    let mut rec: Vec<String> = Vec::new();
    let mut field = String::new();
    let mut i = 0;
    let mut in_q = false;
    let mut any = false;
    while i < b.len() {
        let c = b[i];
        if in_q {
            if c == b'"' {
                if i + 1 < b.len() && b[i + 1] == b'"' {
                    field.push('"');
                    i += 2;
                    continue;
                }
                in_q = false;
                i += 1;
                continue;
            }
            field.push(c as char);
            i += 1;
            continue;
        }
        match c {
            b'"' => {
                in_q = true;
                any = true;
                i += 1;
            }
            b',' => {
                rec.push(std::mem::take(&mut field));
                any = true;
                i += 1;
            }
            b'\r' => {
                i += 1;
            }
            b'\n' => {
                rec.push(std::mem::take(&mut field));
                recs.push(std::mem::take(&mut rec));
                any = false;
                i += 1;
            }
            _ => {
                field.push(c as char);
                any = true;
                i += 1;
            }
        }
    }
    if in_q {
        return Err(FetchError::Parse("unterminated CSV quote".into()));
    }
    if any || !field.is_empty() {
        rec.push(field);
        recs.push(rec);
    }
    Ok(recs)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p kessel-fetch csv::tests`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/kessel-fetch/src/lib.rs crates/kessel-fetch/src/json.rs crates/kessel-fetch/src/csv.rs
git commit -m "kessel-fetch: RFC 4180 CSV reader, header-name selection"
```

---

### Task 4: Coercion to `FieldKind` bytes

**Files:**
- Create: `crates/kessel-fetch/src/coerce.rs`

Coercion must produce the **same little-endian width-`w` bytes the codec stores**, so the router can build records the engine accepts. Confirm widths by reading `crates/kessel-codec/src/lib.rs` (the `FieldKind::width()` and integer LE layout the codec uses) before implementing — the tests below pin the contract.

- [ ] **Step 1: Write failing tests at the bottom of `crates/kessel-fetch/src/coerce.rs`**

```rust
//! Cell -> declared FieldKind -> raw little-endian field bytes
//! (exactly what kessel-codec stores for that kind).
use crate::json::Cell;
use crate::FetchError;
use kessel_catalog::FieldKind;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_little_endian_by_width() {
        assert_eq!(
            to_field_bytes(&FieldKind::U32, Cell::Text("258".into())).unwrap(),
            vec![2, 1, 0, 0]
        );
        assert_eq!(
            to_field_bytes(&FieldKind::I64, Cell::Text("-1".into())).unwrap(),
            vec![0xFF; 8]
        );
        assert_eq!(
            to_field_bytes(&FieldKind::U128, Cell::Text("1".into()))
                .unwrap()
                .len(),
            16
        );
    }

    #[test]
    fn bool_and_char_and_null_and_bad() {
        assert_eq!(
            to_field_bytes(&FieldKind::Bool, Cell::Bool(true)).unwrap(),
            vec![1]
        );
        assert_eq!(
            to_field_bytes(&FieldKind::Char(4), Cell::Text("hi".into())).unwrap(),
            vec![b'h', b'i', 0, 0]
        );
        // Null into any kind is a typed error in slice 1 (no nullable ext cols).
        assert!(matches!(
            to_field_bytes(&FieldKind::U32, Cell::Null),
            Err(FetchError::Type(_))
        ));
        assert!(matches!(
            to_field_bytes(&FieldKind::U32, Cell::Text("abc".into())),
            Err(FetchError::Type(_))
        ));
        // CHAR overflow is a typed error (deterministic, explicit).
        assert!(matches!(
            to_field_bytes(&FieldKind::Char(1), Cell::Text("toolong".into())),
            Err(FetchError::Type(_))
        ));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p kessel-fetch coerce::tests`
Expected: FAIL to compile — `to_field_bytes` undefined.

- [ ] **Step 3: Implement `to_field_bytes` above the test module**

```rust
pub fn to_field_bytes(
    kind: &FieldKind,
    cell: Cell,
) -> Result<Vec<u8>, FetchError> {
    use FieldKind::*;
    let txt = match (&cell, kind) {
        (Cell::Null, _) => {
            return Err(FetchError::Type("null in a non-nullable external column".into()))
        }
        (Cell::Bool(b), Bool) => return Ok(vec![*b as u8]),
        (Cell::Bool(b), _) => (if *b { "1" } else { "0" }).to_string(),
        (Cell::Text(s), _) => s.clone(),
    };
    let int = |signed: bool, w: usize| -> Result<Vec<u8>, FetchError> {
        if signed {
            let n: i128 = txt
                .parse()
                .map_err(|_| FetchError::Type(format!("`{txt}` is not an integer")))?;
            Ok(n.to_le_bytes()[..w].to_vec())
        } else {
            let n: u128 = txt
                .parse()
                .map_err(|_| FetchError::Type(format!("`{txt}` is not an unsigned integer")))?;
            Ok(n.to_le_bytes()[..w].to_vec())
        }
    };
    match kind {
        U8 => int(false, 1),
        U16 => int(false, 2),
        U32 => int(false, 4),
        U64 => int(false, 8),
        U128 => int(false, 16),
        I8 => int(true, 1),
        I16 => int(true, 2),
        I32 => int(true, 4),
        I64 => int(true, 8),
        I128 => int(true, 16),
        Bool => Ok(vec![
            if txt == "1" || txt.eq_ignore_ascii_case("true") { 1 } else { 0 },
        ]),
        Timestamp => int(false, 8),
        Char(w) | Bytes(w) => {
            let raw = txt.as_bytes();
            let w = *w as usize;
            if raw.len() > w {
                return Err(FetchError::Type(format!(
                    "value of {} bytes exceeds CHAR/BYTES({w})",
                    raw.len()
                )));
            }
            let mut out = vec![0u8; w];
            out[..raw.len()].copy_from_slice(raw);
            Ok(out)
        }
        other => Err(FetchError::Type(format!(
            "external column kind {other:?} unsupported in slice 1"
        ))),
    }
}
```

> Before marking this step done, open `crates/kessel-codec/src/lib.rs`, find how it encodes each `FieldKind`, and confirm: (a) integer widths above match `FieldKind::width()`; (b) the codec stores integers little-endian truncated to width (the SP91/SP93 specs confirm 16-LE for U128/I128); (c) `Char`/`Bytes` stored zero-padded to width (confirmed in SP93). If any differ, fix `to_field_bytes` to match the codec exactly and update the test expectations to the codec's real contract.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p kessel-fetch coerce::tests`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/kessel-fetch/src/coerce.rs
git commit -m "kessel-fetch: Cell -> FieldKind LE-bytes coercion (codec-matching)"
```

---

### Task 5: Minimal HTTP/1.1 GET client + stub-server integration test

**Files:**
- Create: `crates/kessel-fetch/src/http.rs`
- Create: `crates/kessel-fetch/tests/stub_server.rs`

- [ ] **Step 1: Write the failing integration test `crates/kessel-fetch/tests/stub_server.rs`**

```rust
//! Spins a real localhost TCP server returning a fixed body, then
//! drives the full fetch_rows path. No external network.
use kessel_catalog::FieldKind;
use kessel_fetch::{fetch_rows, Auth, ColumnMap, Format, DEFAULT_MAX_BODY};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

fn serve_once(body: &'static str, expect_auth: Option<&'static str>) -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        let mut buf = [0u8; 2048];
        let n = s.read(&mut buf).unwrap();
        let req = String::from_utf8_lossy(&buf[..n]).to_string();
        if let Some(a) = expect_auth {
            assert!(req.contains(a), "missing auth header: {req}");
        }
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        s.write_all(resp.as_bytes()).unwrap();
    });
    port
}

#[test]
fn json_over_http_with_bearer_round_trips() {
    let port = serve_once(
        r#"[{"id":7,"name":"zed"}]"#,
        Some("Authorization: Bearer T0K"),
    );
    let cols = vec![
        ColumnMap { name: "id".into(), kind: FieldKind::U32, source: "id".into() },
        ColumnMap { name: "name".into(), kind: FieldKind::Char(8), source: "name".into() },
    ];
    let rows = fetch_rows(
        &format!("http://127.0.0.1:{port}/data"),
        &Auth::Bearer("T0K".into()),
        Format::Json,
        &cols,
        DEFAULT_MAX_BODY,
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], vec![7, 0, 0, 0]);
    assert_eq!(rows[0][1], b"zed\0\0\0\0\0".to_vec());
}

#[test]
fn body_too_large_is_typed_error() {
    let port = serve_once(r#"[{"id":1}]"#, None);
    let cols = vec![ColumnMap {
        name: "id".into(),
        kind: FieldKind::U32,
        source: "id".into(),
    }];
    let e = fetch_rows(
        &format!("http://127.0.0.1:{port}/d"),
        &Auth::None,
        Format::Json,
        &cols,
        4, // 4-byte cap, body is larger
    )
    .unwrap_err();
    assert!(matches!(e, kessel_fetch::FetchError::TooLarge(4)));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p kessel-fetch --test stub_server`
Expected: FAIL to compile — `http::get` undefined.

- [ ] **Step 3: Implement `crates/kessel-fetch/src/http.rs`**

```rust
//! Dependency-free HTTP/1.1 GET. Parses scheme://host[:port]/path,
//! sends a GET, reads the response, enforces a body cap, returns the
//! body bytes. HTTPS is intentionally unsupported in slice 1 (use a
//! TLS-terminating sidecar — see the design doc).
use crate::{Auth, FetchError};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

pub fn get(url: &str, auth: &Auth, max_body: u64) -> Result<Vec<u8>, FetchError> {
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
        if raw.len() as u64 > max_body + 65_536 {
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
    for l in lines {
        let ll = l.to_ascii_lowercase();
        if ll.starts_with("transfer-encoding:") && ll.contains("chunked") {
            chunked = true;
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
    Ok(body)
}

fn dechunk(mut b: &[u8]) -> Result<Vec<u8>, FetchError> {
    let mut out = Vec::new();
    loop {
        let nl = b
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or_else(|| FetchError::Http("bad chunk".into()))?;
        let size = usize::from_str_radix(
            std::str::from_utf8(&b[..nl]).unwrap_or("").trim(),
            16,
        )
        .map_err(|_| FetchError::Http("bad chunk size".into()))?;
        b = &b[nl + 2..];
        if size == 0 {
            return Ok(out);
        }
        if b.len() < size {
            return Err(FetchError::Http("truncated chunk".into()));
        }
        out.extend_from_slice(&b[..size]);
        b = &b[size + 2..]; // skip trailing CRLF
    }
}
```

- [ ] **Step 4: Run the integration test**

Run: `cargo test -p kessel-fetch --test stub_server`
Expected: PASS (2 tests).

- [ ] **Step 5: Full crate test + commit**

Run: `cargo test -p kessel-fetch`
Expected: PASS (all json/csv/coerce/stub_server tests).

```bash
git add crates/kessel-fetch/src/http.rs crates/kessel-fetch/tests/stub_server.rs
git commit -m "kessel-fetch: HTTP/1.1 GET + end-to-end stub-server tests"
```

---

## Phase 2 — Catalog recipe + Op variants + SM apply

### Task 6: `ExternalRecipe` + Catalog-level backward-compatible trailer

**Files:**
- Modify: `crates/kessel-catalog/src/lib.rs` (struct add ~line 341; `encode` ~354; `decode` ~407)
- Read first: `crates/kessel-catalog/src/lib.rs:256-340` (the SP86 trailer functions `encode_type_def_with_defaults`/`decode_type_defaults` — copy their framing style exactly).

- [ ] **Step 1: Write the failing round-trip test in `crates/kessel-catalog/src/lib.rs` tests module**

Find the existing `#[cfg(test)] mod tests` and add:

```rust
#[test]
fn catalog_external_recipe_round_trips_and_is_backward_compatible() {
    let mut c = Catalog::default();
    c.types.push(ObjectType {
        type_id: 1, name: "ext".into(), schema_ver: 1,
        fields: sample_fields(), indexes: vec![], unique: vec![],
        fks: vec![], checks: vec![], triggers: vec![], ordered: vec![],
        composite: vec![], defaults: vec![],
    });
    c.external.push(ExternalRecipe {
        type_id: 1,
        url: "http://x/y".into(),
        format: 0, // 0=JSON 1=CSV
        key_field_id: 1,
        auth: ExternalAuth::None,
        mapping: vec![(1, "id".into()), (2, "u.name".into())],
    });
    let back = Catalog::decode(&c.encode()).unwrap();
    assert_eq!(back.external.len(), 1);
    assert_eq!(back.external[0].url, "http://x/y");
    assert_eq!(back.external[0].mapping[1], (2, "u.name".to_string()));
    // A catalog with NO external recipes encodes/decodes exactly as
    // before (old readers see nothing new; new readers see empty).
    let mut plain = Catalog::default();
    plain.types.push(c.types[0].clone());
    assert!(Catalog::decode(&plain.encode()).unwrap().external.is_empty());
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p kessel-catalog catalog_external_recipe_round_trips -- --nocapture`
Expected: FAIL to compile — `ExternalRecipe`, `ExternalAuth`, `Catalog.external` undefined.

- [ ] **Step 3: Add the types and serialization**

Near `pub struct Catalog` add:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalAuth {
    None,
    /// Bearer token read from this env var name at fetch time.
    BearerEnv(String),
    /// Arbitrary header `name` whose value is read from this env var.
    HeaderEnv { header: String, env: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalRecipe {
    pub type_id: u32,
    pub url: String,
    /// 0 = JSON, 1 = CSV.
    pub format: u8,
    /// field_id whose value derives the deterministic ObjectId.
    pub key_field_id: u16,
    pub auth: ExternalAuth,
    /// (field_id, source) — JSON dotted path or CSV header name.
    pub mapping: Vec<(u16, String)>,
}
```

Add `pub external: Vec<ExternalRecipe>,` to `struct Catalog`, and `external: vec![],` to its `Default`/constructor.

In `Catalog::encode`, **after** the existing per-type loop and before returning the buffer, append a self-describing trailer (mirror the SP86 length-prefixed style). Use little-endian helpers consistent with the file:

```rust
// SP-EXT trailer: [u32 n] then n × recipe. Absent in old blobs ⇒
// decode yields an empty list (backward compatible).
out.extend_from_slice(&(self.external.len() as u32).to_le_bytes());
for r in &self.external {
    out.extend_from_slice(&r.type_id.to_le_bytes());
    out.push(r.format);
    out.extend_from_slice(&r.key_field_id.to_le_bytes());
    let put_s = |o: &mut Vec<u8>, s: &str| {
        o.extend_from_slice(&(s.len() as u32).to_le_bytes());
        o.extend_from_slice(s.as_bytes());
    };
    put_s(&mut out, &r.url);
    match &r.auth {
        ExternalAuth::None => out.push(0),
        ExternalAuth::BearerEnv(e) => { out.push(1); put_s(&mut out, e); }
        ExternalAuth::HeaderEnv { header, env } => {
            out.push(2); put_s(&mut out, header); put_s(&mut out, env);
        }
    }
    out.extend_from_slice(&(r.mapping.len() as u32).to_le_bytes());
    for (fid, src) in &r.mapping {
        out.extend_from_slice(&fid.to_le_bytes());
        put_s(&mut out, src);
    }
}
```

(Confirm the encode buffer variable name — it may not be `out`; match the existing function.)

In `Catalog::decode`, after the existing parse, parse the trailer **defensively** — any short read ⇒ empty list (old blob):

```rust
let mut external = Vec::new();
// `pos` = cursor where the existing decode loop finished. If your
// decode does not track a cursor, capture the consumed length and
// slice from there; the trailer is whatever bytes remain.
if let Some(mut t) = b.get(pos..).filter(|s| s.len() >= 4) {
    let take_u32 = |t: &mut &[u8]| -> Option<u32> {
        let v = u32::from_le_bytes(t.get(..4)?.try_into().ok()?);
        *t = &t[4..]; Some(v)
    };
    let take_s = |t: &mut &[u8]| -> Option<String> {
        let n = u32::from_le_bytes(t.get(..4)?.try_into().ok()?) as usize;
        *t = &t[4..];
        let s = std::str::from_utf8(t.get(..n)?).ok()?.to_string();
        *t = &t[n..]; Some(s)
    };
    if let Some(n) = take_u32(&mut t) {
        'outer: for _ in 0..n {
            let Some(type_id) = take_u32(&mut t) else { break };
            let Some(&format) = t.first() else { break };
            t = &t[1..];
            let Some(kf) = (t.len() >= 2).then(|| {
                let v = u16::from_le_bytes(t[..2].try_into().unwrap());
                t = &t[2..]; v
            }) else { break };
            let Some(url) = take_s(&mut t) else { break };
            let auth = match t.first() {
                Some(0) => { t = &t[1..]; ExternalAuth::None }
                Some(1) => { t = &t[1..];
                    let Some(e) = take_s(&mut t) else { break };
                    ExternalAuth::BearerEnv(e) }
                Some(2) => { t = &t[1..];
                    let Some(h) = take_s(&mut t) else { break };
                    let Some(e) = take_s(&mut t) else { break };
                    ExternalAuth::HeaderEnv { header: h, env: e } }
                _ => break,
            };
            let Some(m) = take_u32(&mut t) else { break };
            let mut mapping = Vec::new();
            for _ in 0..m {
                if t.len() < 2 { break 'outer; }
                let fid = u16::from_le_bytes(t[..2].try_into().unwrap());
                t = &t[2..];
                let Some(src) = take_s(&mut t) else { break 'outer };
                mapping.push((fid, src));
            }
            external.push(ExternalRecipe {
                type_id, url, format, key_field_id: kf, auth, mapping,
            });
        }
    }
}
```

> The exact integration depends on how `Catalog::decode` tracks its cursor. Read `decode` (lines ~407-440) first; if it consumes a `&[u8]` slice progressively, append the trailer parse where the slice is exhausted. The invariant the test pins: **no recipes ⇒ `external` is empty and old blobs still decode**.

- [ ] **Step 4: Run to verify it passes + full catalog suite**

Run: `cargo test -p kessel-catalog`
Expected: PASS (new test + all existing catalog tests unchanged).

- [ ] **Step 5: Commit**

```bash
git add crates/kessel-catalog/src/lib.rs
git commit -m "kessel-catalog: backward-compatible Catalog-level ExternalRecipe trailer"
```

---

### Task 7: New `Op` variants

**Files:**
- Modify: `crates/kessel-proto/src/lib.rs` (`enum Op` ~37; `kind()` ~293; `is_mutating()`; `encode()` ~338; `decode()` ~580)

- [ ] **Step 1: Write the failing wire round-trip test**

In the kessel-proto tests module (find `mod tests` / an existing encode/decode test) add:

```rust
#[test]
fn external_source_ops_wire_round_trip() {
    for op in [
        Op::CreateExternalSource {
            name: "feed".into(),
            type_def: vec![1, 2, 3],
            url: "http://h/p".into(),
            format: 0,
            key_field_id: 2,
            auth_kind: 1,
            auth_a: "TOKEN_ENV".into(),
            auth_b: String::new(),
            mapping: vec![(1, "id".into()), (2, "k".into())],
        },
        Op::DropExternalSource { name: "feed".into() },
        Op::RefreshExternalSource { name: "feed".into() },
    ] {
        let back = Op::decode(&op.encode()).expect("decode");
        assert_eq!(back.encode(), op.encode(), "round-trip mismatch");
        assert!(op.is_mutating());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p kessel-proto external_source_ops_wire_round_trip`
Expected: FAIL to compile — variants undefined.

- [ ] **Step 3: Add the variants and all match arms**

In `enum Op` add:

```rust
    CreateExternalSource {
        name: String,
        /// `encode_type_def(name, fields)` for the backing type.
        type_def: Vec<u8>,
        url: String,
        format: u8,        // 0 JSON, 1 CSV
        key_field_id: u16,
        auth_kind: u8,     // 0 None, 1 BearerEnv, 2 HeaderEnv
        auth_a: String,    // BearerEnv: env name | HeaderEnv: header
        auth_b: String,    // HeaderEnv: env name (else "")
        mapping: Vec<(u16, String)>,
    },
    DropExternalSource { name: String },
    RefreshExternalSource { name: String },
```

In `kind()` add (use the next free codes — confirm the current max is 40 from `UpdateSet`; use 41/42/43):

```rust
            Op::CreateExternalSource { .. } => 41,
            Op::DropExternalSource { .. } => 42,
            Op::RefreshExternalSource { .. } => 43,
```

In `is_mutating()` — they are all mutating, so the existing `!matches!(read-ops)` default already returns `true` for them. Add a brief assertion-style comment; no code change needed there (verify the read-op match list does NOT include these).

In `encode()` add arms (use the file's `codec::put_*` helpers; mirror an existing string/bytes-bearing arm such as `CreateType`/`Create`):

```rust
            Op::CreateExternalSource {
                name, type_def, url, format, key_field_id,
                auth_kind, auth_a, auth_b, mapping,
            } => {
                codec::put_str(&mut b, name);
                codec::put_bytes(&mut b, type_def);
                codec::put_str(&mut b, url);
                b.push(*format);
                codec::put_u16(&mut b, *key_field_id);
                b.push(*auth_kind);
                codec::put_str(&mut b, auth_a);
                codec::put_str(&mut b, auth_b);
                codec::put_u32(&mut b, mapping.len() as u32);
                for (fid, s) in mapping {
                    codec::put_u16(&mut b, *fid);
                    codec::put_str(&mut b, s);
                }
            }
            Op::DropExternalSource { name }
            | Op::RefreshExternalSource { name } => {
                codec::put_str(&mut b, name);
            }
```

> Confirm the exact helper names in this file (`put_str` vs `put_bytes`+utf8, `put_u16`, `put_u32`, and the reader equivalents `r.str()`, `r.u16()`, …). The kessel-proto file already has them for existing ops (e.g. `CreateType`, `Txn`); match them precisely. If `put_str`/`r.str()` don't exist, use `put_bytes` with `String::from_utf8`.

In `decode()` add arms for kinds 41/42/43 mirroring the read order exactly.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p kessel-proto external_source_ops_wire_round_trip`
Expected: PASS.

- [ ] **Step 5: Workspace build (catch every non-exhaustive `match op`)**

Run: `cargo build --workspace`
Expected: compile errors at every `match` over `Op` lacking the new arms (kessel-sm, router, plan_string, etc.). For each, add an arm. For **kessel-sm `plan_string`/router `route`** they get real arms in later tasks; for any *other* exhaustive match, add `Op::CreateExternalSource { .. } | Op::DropExternalSource { .. } | Op::RefreshExternalSource { .. } => <the same fallthrough that sibling DDL/unsupported uses>`. Re-run until `cargo build --workspace` is clean.

- [ ] **Step 6: Commit**

```bash
git add crates/kessel-proto/src/lib.rs
git commit -m "kessel-proto: CreateExternalSource/DropExternalSource/RefreshExternalSource ops"
```

---

### Task 8: SM apply — create/drop the backing type + recipe; reject refresh

**Files:**
- Modify: `crates/kessel-sm/src/lib.rs` (`fn apply` match — add three arms; reuse the existing `Op::CreateType`/`Op::DropType` handling for the backing type)
- Read first: the `Op::CreateType` and `Op::DropType` arms in `apply` to reuse type creation/teardown exactly.

- [ ] **Step 1: Write the failing SM test in `crates/kessel-sm/src/lib.rs` tests**

```rust
#[test]
fn create_and_drop_external_source_manages_type_and_recipe() {
    use kessel_catalog::ExternalAuth;
    let mut sm = StateMachine::open(MemVfs::new()).unwrap();
    let td = kessel_catalog::encode_type_def(
        "feed",
        &[
            Field { field_id: 0, name: "id".into(), kind: FieldKind::U64, nullable: false },
            Field { field_id: 0, name: "nm".into(), kind: FieldKind::Char(8), nullable: false },
        ],
    );
    let r = sm.apply(1, Op::CreateExternalSource {
        name: "feed".into(), type_def: td, url: "http://h/p".into(),
        format: 0, key_field_id: 1, auth_kind: 1,
        auth_a: "TOK_ENV".into(), auth_b: String::new(),
        mapping: vec![(1, "id".into()), (2, "nm".into())],
    });
    assert!(matches!(r, OpResult::TypeCreated(_) | OpResult::Ok));
    let cat = sm.catalog();
    let t = cat.types.iter().find(|t| t.name == "feed").expect("type made");
    let rec = cat.external.iter().find(|e| e.type_id == t.type_id).expect("recipe");
    assert_eq!(rec.url, "http://h/p");
    assert_eq!(rec.auth, ExternalAuth::BearerEnv("TOK_ENV".into()));
    // Refresh must NEVER be applied at the SM (router-only).
    assert!(matches!(
        sm.apply(2, Op::RefreshExternalSource { name: "feed".into() }),
        OpResult::SchemaError(_)
    ));
    // Drop removes both recipe and type.
    assert_eq!(sm.apply(3, Op::DropExternalSource { name: "feed".into() }), OpResult::Ok);
    let cat = sm.catalog();
    assert!(cat.types.iter().all(|t| t.name != "feed"));
    assert!(cat.external.is_empty());
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p kessel-sm create_and_drop_external_source -- --nocapture`
Expected: FAIL — arms unimplemented (whatever fallthrough Task 7 added returns).

- [ ] **Step 3: Implement the three arms in `apply`**

Place before the catch-all. Reuse the existing CreateType/DropType internals (call the same code paths; do not duplicate type-creation logic — extract a private helper if CreateType's body isn't already callable):

```rust
Op::CreateExternalSource {
    name, type_def, url, format, key_field_id,
    auth_kind, auth_a, auth_b, mapping,
} => {
    if self.catalog.types.iter().any(|t| t.name == name) {
        return OpResult::SchemaError(format!("type `{name}` exists"));
    }
    // Create the backing type via the SAME path as Op::CreateType.
    let created = self.apply(op_number, Op::CreateType { def: type_def });
    let tid = match created {
        OpResult::TypeCreated(id) => id,
        other => return other, // surface the schema error verbatim
    };
    let auth = match auth_kind {
        0 => kessel_catalog::ExternalAuth::None,
        1 => kessel_catalog::ExternalAuth::BearerEnv(auth_a),
        2 => kessel_catalog::ExternalAuth::HeaderEnv { header: auth_a, env: auth_b },
        _ => return OpResult::SchemaError("bad auth_kind".into()),
    };
    if let Some(c) = self.catalog_mut() {
        c.external.push(kessel_catalog::ExternalRecipe {
            type_id: tid, url, format, key_field_id, auth,
            mapping,
        });
    }
    match self.persist_catalog(op_number) {
        OpResult::SchemaError(e) => OpResult::SchemaError(e),
        _ => OpResult::Ok,
    }
}
Op::DropExternalSource { name } => {
    let tid = match self.catalog.types.iter().find(|t| t.name == name) {
        Some(t) => t.type_id,
        None => return OpResult::NotFound,
    };
    self.catalog_mut().map(|c| c.external.retain(|e| e.type_id != tid));
    self.apply(op_number, Op::DropType { type_id: tid })
}
Op::RefreshExternalSource { .. } => OpResult::SchemaError(
    "REFRESH is a router-side operation, never applied at the state \
     machine".into(),
),
```

> Confirm the real accessor names: this file uses `self.catalog` (field) and `persist_catalog(op_number)` (seen at lib.rs:154). For mutating the catalog it likely uses `self.catalog` directly or a `catalog_mut()`-style path — read how `Op::CreateType`/SP86 default-setting mutates `self.catalog` and match that exactly (replace `self.catalog_mut()` accordingly). Confirm `Op::CreateType` returns `OpResult::TypeCreated(u32)` (it does — `create_type_assigns_deterministic_ids` asserts it) and `OpResult::NotFound` exists.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p kessel-sm create_and_drop_external_source`
Expected: PASS.

- [ ] **Step 5: Determinism gate**

Run: `cargo test --workspace --release`
Expected: all green; `kessel-vsr` seed corpus + `large_seed_corpus_is_deterministic_and_converges` pass. The new ops only mutate via existing CreateType/DropType + a deterministic catalog field ⇒ digest changes only for catalogs that actually use them.

- [ ] **Step 6: Commit**

```bash
git add crates/kessel-sm/src/lib.rs
git commit -m "kessel-sm: apply CreateExternalSource/DropExternalSource; reject SM-side Refresh"
```

---

## Phase 3 — SQL grammar

### Task 9: Parse the three statements

**Files:**
- Modify: `crates/kessel-sql/src/lib.rs` (`compile` ~491; reuse `CREATE TABLE` column-list parsing and the `CREATE` block at ~623; `kind_of` ~175; `lex` for the `'string'` token)
- Read first: how `CREATE TABLE` parses `(col TYPE [NOT NULL], …)` and produces `encode_type_def` — `CREATE EXTERNAL SOURCE` reuses that column loop, plus the `FROM/FORMAT/KEY/AUTH` tail.

- [ ] **Step 1: Write failing tests in the kessel-sql tests module**

```rust
#[test]
fn parse_create_external_source() {
    let cat = Catalog::default();
    let sql = "CREATE EXTERNAL SOURCE feed (\
        id U64 NOT NULL FROM 'id', \
        nm CHAR(8) NOT NULL FROM 'u.name') \
        FROM 'http://h/p' FORMAT JSON KEY id \
        AUTH BEARER ENV 'TOK_ENV'";
    match compile(sql, &cat).expect("compile") {
        Op::CreateExternalSource { name, url, format, key_field_id,
            auth_kind, auth_a, mapping, .. } => {
            assert_eq!(name, "feed");
            assert_eq!(url, "http://h/p");
            assert_eq!(format, 0);
            assert_eq!(auth_kind, 1);
            assert_eq!(auth_a, "TOK_ENV");
            // key column is the 1st declared field => field_id 1
            assert_eq!(key_field_id, 1);
            assert_eq!(mapping, vec![(1, "id".to_string()), (2, "u.name".to_string())]);
        }
        o => panic!("got {o:?}"),
    }
}

#[test]
fn parse_refresh_and_drop_external_source() {
    let cat = Catalog::default();
    assert!(matches!(
        compile("REFRESH feed", &cat).unwrap(),
        Op::RefreshExternalSource { name } if name == "feed"
    ));
    assert!(matches!(
        compile("DROP EXTERNAL SOURCE feed", &cat).unwrap(),
        Op::DropExternalSource { name } if name == "feed"
    ));
    // CSV + no auth
    match compile(
        "CREATE EXTERNAL SOURCE c (a U32 NOT NULL FROM 'a') \
         FROM 'http://h' FORMAT CSV KEY a",
        &cat,
    ).unwrap() {
        Op::CreateExternalSource { format, auth_kind, .. } => {
            assert_eq!(format, 1);
            assert_eq!(auth_kind, 0);
        }
        o => panic!("got {o:?}"),
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p kessel-sql parse_create_external_source parse_refresh_and_drop -- --nocapture`
Expected: FAIL — `compile` returns `unsupported statement` / errors.

- [ ] **Step 3: Implement parsing in `compile`**

Add near the other `p.kw("CREATE")` / DDL handling (and a top-level `REFRESH` / `DROP EXTERNAL SOURCE` check). Reuse the existing string-literal token (the lexer already yields `Tok::Str` — confirm the variant name used by `CREATE … KEY 'str'` paths / `cmp_expr`). Skeleton:

```rust
// REFRESH <name>
if p.kw("REFRESH") {
    let name = p.ident()?;
    return Ok(Op::RefreshExternalSource { name });
}
// DROP EXTERNAL SOURCE <name>
if p.kw("DROP") {
    if p.kw("EXTERNAL") {
        p.expect_kw("SOURCE")?;
        let name = p.ident()?;
        return Ok(Op::DropExternalSource { name });
    }
    // ... fall through to existing DROP INDEX / DROP TABLE handling.
}
```

> The existing `DROP` handling is a separate block (see `DROP INDEX`/`DROP TABLE` ~506-546). Integrate the `EXTERNAL SOURCE` case **inside** that block before the others so `DROP TABLE`/`DROP INDEX` still parse. Do not duplicate the block.

In the `p.kw("CREATE")` block, before the `INDEX` handling, add:

```rust
if p.kw("EXTERNAL") {
    p.expect_kw("SOURCE")?;
    let name = p.ident()?;
    p.punct('(')?;
    let mut fields = Vec::new();
    let mut mapping: Vec<(u16, String)> = Vec::new();
    let mut next_fid: u16 = 1;
    loop {
        let cname = p.ident()?;
        let tyname = p.ident()?;
        let mut arg = None;
        if matches!(p.peek(), Some(Tok::Punct('('))) {
            p.punct('(')?;
            match p.next() {
                Some(Tok::Int(n)) => arg = Some(n),
                _ => return Err("expected size".into()),
            }
            p.punct(')')?;
        }
        let mut nullable = true;
        if p.kw("NOT") { p.expect_kw("NULL")?; nullable = false; }
        p.expect_kw("FROM")?;
        let src = match p.next() {
            Some(Tok::Str(s)) => s,
            _ => return Err("expected 'source' string".into()),
        };
        fields.push(Field {
            field_id: 0, name: cname,
            kind: kind_of(&tyname, arg)?, nullable,
        });
        mapping.push((next_fid, src));
        next_fid += 1;
        match p.next() {
            Some(Tok::Punct(',')) => continue,
            Some(Tok::Punct(')')) => break,
            _ => return Err("expected `,` or `)`".into()),
        }
    }
    p.expect_kw("FROM")?;
    let url = match p.next() {
        Some(Tok::Str(s)) => s,
        _ => return Err("expected 'url' string".into()),
    };
    p.expect_kw("FORMAT")?;
    let format = if p.kw("JSON") { 0u8 }
        else if p.kw("CSV") { 1u8 }
        else { return Err("FORMAT must be JSON or CSV".into()) };
    p.expect_kw("KEY")?;
    let key_name = p.ident()?;
    let key_field_id = fields.iter().position(|f| f.name == key_name)
        .map(|i| (i as u16) + 1)
        .ok_or_else(|| format!("KEY `{key_name}` is not a declared column"))?;
    let (mut auth_kind, mut auth_a, mut auth_b) = (0u8, String::new(), String::new());
    if p.kw("AUTH") {
        if p.kw("BEARER") {
            p.expect_kw("ENV")?;
            auth_kind = 1;
            auth_a = match p.next() { Some(Tok::Str(s)) => s,
                _ => return Err("expected 'ENV_NAME'".into()) };
        } else if p.kw("HEADER") {
            auth_kind = 2;
            auth_a = match p.next() { Some(Tok::Str(s)) => s,
                _ => return Err("expected 'Header-Name'".into()) };
            p.expect_kw("ENV")?;
            auth_b = match p.next() { Some(Tok::Str(s)) => s,
                _ => return Err("expected 'ENV_NAME'".into()) };
        } else {
            return Err("AUTH must be BEARER ENV '..' or HEADER '..' ENV '..'".into());
        }
    }
    let type_def = kessel_catalog::encode_type_def(&name, &fields);
    return Ok(Op::CreateExternalSource {
        name, type_def, url, format, key_field_id,
        auth_kind, auth_a, auth_b, mapping,
    });
}
```

> Confirm: the `Tok::Str` variant name and that the lexer produces it for single-quoted strings (it does — used by `WHERE s >= 'b'` and SP90 tests). Confirm `p.peek()`, `p.punct(')')`, `p.expect_kw`, `p.kw`, `p.ident()`, `p.next()`, `kind_of`, and `Field`/`encode_type_def` are exactly as used by the existing `CREATE TABLE`/`ALTER` code (Task references lib.rs:600-665). Match names exactly.

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p kessel-sql parse_create_external_source parse_refresh_and_drop`
Expected: PASS.

- [ ] **Step 5: Full SQL suite (no regression to existing DDL parsing)**

Run: `cargo test -p kessel-sql`
Expected: PASS (all existing tests + 2 new).

- [ ] **Step 6: Commit**

```bash
git add crates/kessel-sql/src/lib.rs
git commit -m "kessel-sql: parse CREATE/DROP EXTERNAL SOURCE and REFRESH"
```

---

## Phase 4 — Router REFRESH handler + end-to-end oracle

### Task 10: Wire the feature + route the three ops

**Files:**
- Modify: `crates/kesseldb-server/Cargo.toml`
- Modify: `crates/kesseldb-server/src/router.rs` (`fn route` ~126; `fn forward` ~377)

- [ ] **Step 1: Add the optional dependency + feature**

In `crates/kesseldb-server/Cargo.toml`:

```toml
[dependencies]
kessel-fetch = { path = "../kessel-fetch", optional = true }
# ... existing deps ...

[features]
default = []
external-sources = ["dep:kessel-fetch"]
```

- [ ] **Step 2: Route the ops (no behavior test yet — covered by Task 11 oracle)**

In `fn route`, add `CreateExternalSource`/`DropExternalSource` to the **`Route::All`** DDL group (catalog is global — every shard must apply identically, exactly like `CreateType`). Add `RefreshExternalSource` as a distinct route:

```rust
            Op::CreateExternalSource { .. }
            | Op::DropExternalSource { .. } => Route::All,
            Op::RefreshExternalSource { .. } => Route::Refresh,
```

Add `Refresh` to `enum Route`:

```rust
    /// Router-side: fetch external data then submit captured rows.
    Refresh,
```

In `fn forward`, add the `Route::Refresh` arm. Without the feature it is a clear error; with it, it calls Task 11's handler:

```rust
            Route::Refresh => {
                #[cfg(feature = "external-sources")]
                { return self.do_refresh(op); }
                #[cfg(not(feature = "external-sources"))]
                { let _ = op; OpResult::SchemaError(
                    "REFRESH requires the server built with \
                     --features external-sources".into()) }
            }
```

- [ ] **Step 3: Build both ways**

Run: `cargo build -p kesseldb-server`
Expected: PASS (feature off; `do_refresh` not referenced).
Run: `cargo build -p kesseldb-server --features external-sources`
Expected: FAIL — `do_refresh` undefined (implemented in Task 11). Acceptable here.

- [ ] **Step 4: Commit**

```bash
git add crates/kesseldb-server/Cargo.toml crates/kesseldb-server/src/router.rs
git commit -m "kesseldb-server: external-sources feature + route the three ops"
```

---

### Task 11: `do_refresh` — fetch, derive ids, submit atomic upsert; end-to-end oracle

**Files:**
- Modify: `crates/kesseldb-server/src/router.rs` (add `#[cfg(feature="external-sources")] impl<'a> Conn<'a> { fn do_refresh … }`)
- Create: `crates/kesseldb-server/tests/external_source_oracle.rs`

- [ ] **Step 1: Write the failing end-to-end oracle test**

`crates/kesseldb-server/tests/external_source_oracle.rs`:

```rust
//! Stub HTTP source -> CREATE EXTERNAL SOURCE -> REFRESH -> the rows
//! materialized in the engine must equal an independent model;
//! re-REFRESH is idempotent (digest unchanged); a changed row
//! upserts; a bad row aborts REFRESH leaving prior data intact.
#![cfg(feature = "external-sources")]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;

// Use whatever in-process single-shard harness the existing router
// tests use (see crates/kesseldb-server/src/router.rs tests, e.g.
// router_routes_points_broadcasts_ddl_and_rejects_cross_shard, and
// crates/kesseldb-server/src/cluster.rs). Bind a 1-shard cluster +
// Router, drive SQL through the same path those tests use.

fn stub(bodies: Arc<Mutex<Vec<String>>>) -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    thread::spawn(move || loop {
        let (mut s, _) = l.accept().unwrap();
        let mut b = [0u8; 1024];
        let _ = s.read(&mut b);
        let body = bodies.lock().unwrap().remove(0);
        let _ = s.write_all(
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(), body).as_bytes());
    });
    port
}

#[test]
fn refresh_materializes_upserts_idempotent_and_atomic() {
    let bodies = Arc::new(Mutex::new(vec![
        r#"[{"id":1,"nm":"ann"},{"id":2,"nm":"bo"}]"#.to_string(),
        r#"[{"id":1,"nm":"ann"},{"id":2,"nm":"BO"}]"#.to_string(), // row 2 changed
        r#"[{"id":1,"nm":"ann"},{"id":2,"nm":"toolongforchar"}]"#.to_string(), // bad
    ]));
    let port = stub(bodies);

    // ---- harness: 1-shard cluster + Router (mirror existing router
    // tests' setup exactly) ----
    // let (router, _shard) = test_router_single_shard();
    // let sql = |s: &str| router_exec_sql(&router, s);

    let create = format!(
        "CREATE EXTERNAL SOURCE feed (\
         id U64 NOT NULL FROM 'id', nm CHAR(16) NOT NULL FROM 'nm') \
         FROM 'http://127.0.0.1:{port}/d' FORMAT JSON KEY id",
    );
    // sql(&create) => Ok
    // sql("REFRESH feed") => Ok
    // SELECT * FROM feed => exactly {(1,"ann"),(2,"bo")} (independent model)
    // digest_after_first = engine digest
    // sql("REFRESH feed") again (body #2): row 2 upserts to "BO",
    //   row 1 unchanged; row count still 2
    // sql("REFRESH feed") (body #3, bad CHAR overflow) => SchemaError;
    //   SELECT * FROM feed unchanged from the previous successful state
    //
    // Replace the comments with the concrete harness calls used by the
    // existing router/cluster tests. The ASSERTIONS (independent
    // model equality, idempotent digest, upsert, atomic-abort) are the
    // contract and must not be weakened.
    let _ = create;
}
```

> This test's harness wiring must match the existing in-process router/cluster test setup. Before writing it, read `crates/kesseldb-server/src/router.rs` tests (`router_routes_points_broadcasts_ddl_and_rejects_cross_shard`) and `crates/kesseldb-server/src/cluster.rs` to find the helper that starts a 1-shard cluster and submits an `Op`/SQL through a `Router`. Use it. Keep the four assertions exactly: (1) materialized rows == independent model, (2) re-REFRESH identical body ⇒ engine digest unchanged, (3) changed row upserts (no duplicate, value updated), (4) bad row ⇒ `SchemaError` and prior data intact.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p kesseldb-server --features external-sources --test external_source_oracle`
Expected: FAIL — `do_refresh` undefined / harness `todo`.

- [ ] **Step 3: Implement `do_refresh`**

Add to `router.rs`:

```rust
#[cfg(feature = "external-sources")]
impl<'a> Conn<'a> {
    fn do_refresh(&mut self, op: &Op) -> OpResult {
        let name = match op {
            Op::RefreshExternalSource { name } => name.clone(),
            _ => return OpResult::SchemaError("not a refresh".into()),
        };
        // Catalog is global — read it from shard 0 via Describe-class
        // access the existing code already uses for routing decisions.
        let cat = match self.router.catalog_snapshot(self) {
            Ok(c) => c,
            Err(e) => return OpResult::SchemaError(format!("catalog: {e}")),
        };
        let ot = match cat.types.iter().find(|t| t.name == name) {
            Some(t) => t.clone(),
            None => return OpResult::NotFound,
        };
        let recipe = match cat.external.iter().find(|e| e.type_id == ot.type_id) {
            Some(r) => r.clone(),
            None => return OpResult::SchemaError(
                format!("`{name}` is not an external source")),
        };
        // Resolve auth from THIS process's env — never persisted.
        let auth = match &recipe.auth {
            kessel_catalog::ExternalAuth::None => kessel_fetch::Auth::None,
            kessel_catalog::ExternalAuth::BearerEnv(v) => {
                match std::env::var(v) {
                    Ok(t) => kessel_fetch::Auth::Bearer(t),
                    Err(_) => return OpResult::SchemaError(
                        format!("auth env `{v}` not set")),
                }
            }
            kessel_catalog::ExternalAuth::HeaderEnv { header, env } => {
                match std::env::var(env) {
                    Ok(val) => kessel_fetch::Auth::Header {
                        name: header.clone(), value: val },
                    Err(_) => return OpResult::SchemaError(
                        format!("auth env `{env}` not set")),
                }
            }
        };
        let cols: Vec<kessel_fetch::ColumnMap> = recipe.mapping.iter()
            .map(|(fid, src)| {
                let f = ot.fields.iter().find(|f| f.field_id == *fid).unwrap();
                kessel_fetch::ColumnMap {
                    name: f.name.clone(), kind: f.kind, source: src.clone(),
                }
            }).collect();
        let format = if recipe.format == 0 {
            kessel_fetch::Format::Json
        } else {
            kessel_fetch::Format::Csv
        };
        let rows = match kessel_fetch::fetch_rows(
            &recipe.url, &auth, format, &cols,
            kessel_fetch::DEFAULT_MAX_BODY,
        ) {
            Ok(r) => r,
            Err(e) => return OpResult::SchemaError(format!("refresh: {e}")),
        };
        // Build one atomic upsert Txn. ObjectId = first 16 bytes of
        // sha256(domain ++ type_id ++ canonical KEY bytes).
        let key_pos = ot.fields.iter()
            .position(|f| f.field_id == recipe.key_field_id)
            .expect("key field exists");
        let layout = ot.compute_layout();
        let mut members = Vec::with_capacity(rows.len());
        for r in &rows {
            // Assemble the codec record from the per-column LE bytes in
            // declared order using the SAME codec the engine uses.
            let values = ot.fields.iter().enumerate().map(|(i, _)| {
                // rows[i] aligns with recipe.mapping order == fields order
                r[i].clone()
            }).collect::<Vec<_>>();
            let record = kessel_codec::encode_raw(&ot, &values)
                .map_err(|e| e.to_string());
            let record = match record {
                Ok(rec) => rec,
                Err(e) => return OpResult::SchemaError(
                    format!("encode: {e}")),
            };
            let mut h = Vec::new();
            h.extend_from_slice(b"kessel-ext-id\0");
            h.extend_from_slice(&ot.type_id.to_le_bytes());
            h.extend_from_slice(&r[key_pos]);
            let digest = kessel_crypto::sha256(&h);
            let mut oid = [0u8; 16];
            oid.copy_from_slice(&digest[..16]);
            members.push(Op::Create {
                type_id: ot.type_id,
                id: kessel_proto::ObjectId(oid),
                record,
            });
            let _ = &layout;
        }
        // Submit atomically through the existing replicated path. A
        // single-shard Txn commits all-or-nothing; multi-shard reuses
        // the cross-shard path already used by Op::Txn.
        self.forward(&Op::Txn { ops: members }, Vec::new())
    }
}
```

> Open questions to resolve against the real code while implementing (do not leave them unresolved — the assertions in Step 1 are the contract):
> - **Catalog snapshot at the router.** There may be no `catalog_snapshot`. The router already answers `Describe` from shard 0 (`route` maps `Describe` → `One(0)`). Use that: send `Op::Describe`/the existing catalog-read path to shard 0, decode with `Catalog::decode`. Implement `catalog_snapshot` as a thin helper doing exactly that. If a simpler existing accessor exists, use it.
> - **`kessel_codec::encode_raw`.** Confirm the real codec entry that builds a record from per-field raw LE bytes (the engine's `kessel-codec`). If the codec API takes `Value`s, convert each `r[i]` to the codec's expected representation, or add a small `encode_from_field_bytes(&ObjectType, &[Vec<u8>])` helper in `kessel-codec` (its own task/commit) — but first check whether `kessel-codec` already exposes a raw-bytes encoder (SP86/SP91 built records in tests via `kessel_codec::encode` with `Value`; converting LE bytes→`Value` per `FieldKind` is deterministic and acceptable).
> - **Create vs Update (upsert).** `Op::Create` on an existing id returns `Exists` and does not overwrite. To upsert, the router must emit `Op::Update` for ids that already exist. Resolve per row with a point `GetById` (router already does point routing) → choose `Create` (absent) or `Update` (present); batch into the one `Op::Txn`. Reading current state for the decision is acceptable (reads are side-effect free); the *mutation* is the single atomic Txn.

- [ ] **Step 4: Implement the helper(s) the notes identified, each its own commit**

If `catalog_snapshot` and/or a codec raw-encoder are needed, add them as **separate, tested commits** before finishing this task:
- `catalog_snapshot(&Conn) -> Result<Catalog,String>` in `router.rs` with a unit test that a `CreateType` then snapshot shows the type.
- (only if needed) `kessel_codec::encode_from_field_bytes(&ObjectType,&[Vec<u8>]) -> Result<Vec<u8>,CodecError>` with a round-trip test vs `kessel_codec::decode`.

- [ ] **Step 5: Run the oracle**

Run: `cargo test -p kesseldb-server --features external-sources --test external_source_oracle -- --nocapture`
Expected: PASS — all four assertions hold.

- [ ] **Step 6: Feature-off regression + determinism gate**

Run: `cargo test --workspace --release`
Expected: all green, identical pre-plan baseline counts (feature off ⇒ `kessel-fetch` not compiled, router refresh is the clean error path). `kessel-vsr` seed corpus + `large_seed_corpus_is_deterministic_and_converges` pass.
Run: `cargo test -p kesseldb-server --features external-sources`
Expected: all `kesseldb-server` tests green with the feature on.

- [ ] **Step 7: Commit**

```bash
git add crates/kesseldb-server/src/router.rs crates/kesseldb-server/tests/external_source_oracle.rs
git commit -m "kesseldb-server: do_refresh — fetch, deterministic ids, atomic upsert + oracle"
```

---

## Phase 5 — Docs & close-out

### Task 12: Docs, STATUS, USAGE, memory

**Files:**
- Modify: `docs/STATUS.md`, `docs/USAGE.md`, `README.md`/`docs/README.md`
- Create: `docs/superpowers/specs/2026-05-18-kesseldb-subproject97-external-sources.md`

- [ ] **Step 1: Spec/STATUS/USAGE**

- STATUS: add an "External sources (EXT slice 1)" row — what's in (CREATE/DROP EXTERNAL SOURCE, REFRESH upsert, JSON/CSV, env-ref auth, HTTP), the **documented boundaries** verbatim from the design (snapshot-since-last-REFRESH, HTTP-only/TLS-sidecar, upsert-only/no prune, explicit mapping), feature-off byte-identical, seed-7 intact, test count.
- USAGE: a runnable `CREATE EXTERNAL SOURCE … / REFRESH …` example + the security note that only env *names* are stored.
- Public docs must stay free of internal slice codenames (existing project rule).

- [ ] **Step 2: Update auto-memory**

Append a `project_kesseldb.md` entry: EXT slice 1 shipped — architecture, the env-ref auth security choice, the determinism boundary, and the follow-on slices (TLS, MODE REPLACE/prune, NDJSON/pagination/nested, OBJ, WASM).

- [ ] **Step 3: Final full regression**

Run: `cargo test --workspace --release` then `cargo test -p kesseldb-server --features external-sources`
Expected: both green; seed-7 intact.

- [ ] **Step 4: Commit & push**

```bash
git add docs/ README.md
git commit -m "EXT slice 1: docs, STATUS, USAGE, spec, memory"
git push origin main
```

---

## Self-review (completed by plan author)

**Spec coverage:** §1 architecture → Tasks 1,10 (feature fences). §2 SQL surface + catalog recipe → Tasks 6,9. §3 REFRESH execution → Task 11. §4 identity & upsert → Task 11 (sha256 id + Create/Update resolution). §5 parsing & mapping → Tasks 2,3,4. §6 transport/TLS (HTTP-only) → Task 5. §7 failure modes → Tasks 2–5,11 (typed errors). §8 determinism boundary → documented in Task 12; enforced by feature-off gate in Tasks 8,11. §9 testing/oracle → Tasks 5,11. §10 scope/non-goals → Task 12 docs. All spec sections map to a task.

**Placeholder scan:** No `TODO`/`TBD`/"add error handling" steps. The two "comment-driven" spots (Task 11 catalog-snapshot / codec-encoder, Task 11 harness wiring) carry **concrete contracts + the exact assertions** and an explicit instruction to resolve against named existing code with separate tested commits — they are decision points with a defined invariant, not deferred work. Acceptable under TDD because the test assertions fully pin behavior.

**Type consistency:** `ColumnMap{name,kind,source}`, `Auth::{None,Bearer,Header{name,value}}`, `Format::{Json,Csv}`, `FetchError::{Http,Parse,Type,Auth,TooLarge}`, `Cell::{Null,Bool,Text}`, `ExternalRecipe{type_id,url,format,key_field_id,auth,mapping}`, `ExternalAuth::{None,BearerEnv,HeaderEnv{header,env}}`, op fields (`auth_kind/auth_a/auth_b`, `type_def`, `key_field_id`, `mapping`) are used identically across Tasks 1–11. Op codes 41/42/43. Format `0=JSON,1=CSV` consistent everywhere.
