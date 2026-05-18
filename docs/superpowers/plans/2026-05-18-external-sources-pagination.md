# External Sources: Pagination + NDJSON — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A single `REFRESH` can materialize a multi-page JSON/NDJSON source via cursor/next-URL pagination, fully captured-once at the router and deterministic, with the kernel and feature-off build unchanged.

**Architecture:** Approach A — all new logic in the optional `kessel-fetch` crate (a new `fetch_rows_paginated` returning the **same `Vec<Vec<Vec<u8>>>`** as `fetch_rows`, an NDJSON parser, a bounded page loop with 3 cursor forms + ROWS-path) + backward-compatible additive fields on `kessel_catalog::ExternalRecipe` and `Op::CreateExternalSource` + `kessel-sql` grammar (`FORMAT NDJSON`, `ROWS`, 3 `PAGE` clauses, CREATE-time compatibility validation) + a one-branch dispatch in `kesseldb-server::Conn::do_refresh` + an extended e2e oracle.

**Tech Stack:** Rust (workspace, edition 2021), pure-std (no new third-party deps), the existing `kessel-fetch`/`kessel-catalog`/`kessel-proto`/`kessel-sql`/`kesseldb-server`/`kessel-codec`/`kessel-crypto`. Cargo feature `external-sources` (default off).

---

## Spec reference

Design: `docs/superpowers/specs/2026-05-18-external-sources-pagination-design.md`. Read it before starting. Slice-1 record: `docs/superpowers/specs/2026-05-18-kesseldb-subproject97-external-sources.md`.

## Determinism / regression gate (run on every kernel-touching task)

```
cargo test --workspace --release
```
Expected: `test result: FAILED` count **0**; the established workspace TOTAL (currently **222** with the feature OFF — default); `large_seed_corpus_is_deterministic_and_converges ... ok`. Feature-OFF build must stay byte-identical (the `kessel-fetch` crate isn't compiled by default; the catalog/proto additive fields encode byte-identically when absent). Feature-ON check where relevant: `cargo test -p kesseldb-server --features external-sources`.

## Current APIs this builds on (verified in the slice-1 code)

- `kessel-fetch/src/lib.rs`: `pub fn fetch_rows(url:&str, auth:&Auth, format:Format, cols:&[ColumnMap], max_body:u64) -> Result<Vec<Vec<Vec<u8>>>, FetchError>`; `pub enum Format { Json, Csv }`; `pub enum Auth { None, Bearer(String), Header{name,value} }`; `pub struct ColumnMap { name, kind, source }`; `pub enum FetchError { Http, Parse, Type, Auth, TooLarge(u64) }`; `pub const DEFAULT_MAX_BODY`; modules `mod coerce; mod csv; mod http; pub(crate) mod json;`.
- `kessel-fetch/src/json.rs`: `pub enum Cell { Null, Bool(bool), Text(String) }`; `pub fn extract(body,&[ColumnMap])->Result<Vec<Vec<Cell>>,FetchError>`; private `enum Json { Null,Bool,Num(String),Str(String),Array(Vec<Json>),Object(Vec<(String,Json)>) }`, `fn parse(&str)->Result<Json,FetchError>`, `fn path_get(&Json,path)->Result<Cell,FetchError>`.
- `kessel-fetch/src/http.rs`: `pub fn get(url,&Auth,max_body)->Result<Vec<u8>,FetchError>` (returns body only; discards headers; parses status line + finds `\r\n\r\n`; handles chunked).
- `kessel-catalog/src/lib.rs`: `pub enum ExternalAuth { None, BearerEnv(String), HeaderEnv{header,env} }`; `pub struct ExternalRecipe { pub type_id:u32, pub url:String, pub format:u8, pub key_field_id:u16, pub auth:ExternalAuth, pub mapping:Vec<(u16,String)> }`; `pub external: Vec<ExternalRecipe>` on `Catalog`; helpers `fn put_str32(&mut Vec<u8>,&str)`, `fn get_str32(&[u8],&mut usize)->Option<String>`. Trailer: `Catalog::encode` writes the external block only `if !self.external.is_empty()` as `[u32 n] then n×(type_id u32, format u8, key_field_id u16, url str32, auth(tag u8 + str32s), mapping(u32 len + (fid u16 + str32)))`; `Catalog::decode` parses it inside a closure ending `.unwrap_or_default()`.
- `kessel-proto/src/lib.rs`: `Op::CreateExternalSource { name:String, type_def:Vec<u8>, url:String, format:u8, key_field_id:u16, auth_kind:u8, auth_a:String, auth_b:String, mapping:Vec<(u16,String)> }` kind `41`; encode uses `codec::put_bytes`/`put_u32`/`b.push`/`to_le_bytes`; decode arm `41 =>` uses a `codec::Cursor c` with `c.bytes()?`, `c.u8()?`, `c.u16()?`, `c.u32()?`.
- `kessel-sql/src/lib.rs`: the `if p.kw("EXTERNAL")` CREATE block (~line 642) builds `fields`/`mapping`, parses `FROM '<url>'`, `FORMAT JSON|CSV` (→ `0u8`/`1u8`), `KEY <col>`, optional `AUTH BEARER ENV '..'`/`AUTH HEADER '..' ENV '..'`, then `encode_type_def(&name,&fields)` and returns `Op::CreateExternalSource{..}`. Helpers: `p.kw`, `p.expect_kw`, `p.ident`, `p.punct`, `p.peek`, `p.next`, `Tok::{Str,Int,Punct,Ident}`, `kind_of`, `Field`. `SqlError = String`.
- `kesseldb-server/src/router.rs`: `#[cfg(feature="external-sources")] impl<'a> Conn<'a> { fn do_refresh(&mut self, op:&Op, dedup:Vec<u8>) -> OpResult }` — builds `cols:Vec<ColumnMap>` from `recipe.mapping`+type fields, `let format = match recipe.format { 0=>Format::Json, 1=>Format::Csv, n=>err }`, `key_idx`, then `let rows = match fetch_rows(&recipe.url,&auth,format,&cols,DEFAULT_MAX_BODY) { Ok(r)=>r, Err(e)=>return SchemaError }`, then per-row codec record + sha256 id + atomic upsert `Op::Txn` via `self.forward(&Op::Txn{ops},dedup)`.

## File structure

- `crates/kessel-fetch/src/json.rs` — bump `Json`, `parse`, `path_get` to `pub(crate)`; add `pub(crate) fn rows_at(body,&[ColumnMap],rows_path:Option<&str>)` and `pub(crate) fn opt_string_at(body,path)`.
- `crates/kessel-fetch/src/ndjson.rs` — **new**: `pub fn extract(body,&[ColumnMap])->Result<Vec<Vec<Cell>>,FetchError>` (object-per-line).
- `crates/kessel-fetch/src/http.rs` — add `pub(crate) fn get_resp(url,&Auth,max_body)->Result<(Vec<(String,String)>,Vec<u8>),FetchError>`; make `get` a thin wrapper.
- `crates/kessel-fetch/src/lib.rs` — add `Format::Ndjson`; `pub enum Pagination { NextUrlJson(String), NextLink, CursorJson{path:String,param:String} }`; caps consts; `pub fn fetch_rows_paginated(...)`; route `Format::Ndjson` in `fetch_rows`.
- `crates/kessel-fetch/tests/paginate_stub.rs` — **new**: multi-page stub-server integration tests.
- `crates/kessel-catalog/src/lib.rs` — `ExternalRecipe` gains `rows_path:Option<String>`, `pagination:Option<PaginationRecipe>`; add `pub enum PaginationRecipe`; **versioned** trailer (v2 sentinel) preserving v1 backward-decode.
- `crates/kessel-proto/src/lib.rs` — `Op::CreateExternalSource` gains `rows_path:Option<String>`, `pagination:Option<(u8,String,String)>` (tag,a,b); additive tolerant encode/decode.
- `crates/kessel-sql/src/lib.rs` — `FORMAT NDJSON` (→`2u8`), optional `ROWS '<path>'`, optional `PAGE …` clauses, CREATE-time compatibility-matrix errors; thread into `Op::CreateExternalSource`.
- `crates/kessel-sm/src/lib.rs` — `Op::CreateExternalSource` apply arm: persist the two new fields into `ExternalRecipe` (currently constructs it without them).
- `crates/kesseldb-server/src/router.rs` — `do_refresh`: map `recipe.format==2 ⇒ Format::Ndjson`; one-branch dispatch to `fetch_rows_paginated` when `recipe.pagination.is_some()`.
- `crates/kesseldb-server/tests/external_source_oracle.rs` — extend with a paginated scenario.
- docs: new internal spec `docs/superpowers/specs/2026-05-18-kesseldb-subproject98-ext-pagination.md`; STATUS/USAGE/README additions (codename-free public docs).

---

## Phase 1 — kessel-fetch primitives

### Task 1: NDJSON parser

**Files:** Create `crates/kessel-fetch/src/ndjson.rs`; Modify `crates/kessel-fetch/src/lib.rs` (module decl), `crates/kessel-fetch/src/json.rs` (visibility).

- [ ] **Step 1: make json reusable.** In `crates/kessel-fetch/src/json.rs` change `enum Json` → `pub(crate) enum Json`, `fn parse` → `pub(crate) fn parse`, `fn path_get` → `pub(crate) fn path_get`. No behavior change.

- [ ] **Step 2: declare the module.** In `crates/kessel-fetch/src/lib.rs`, add `mod ndjson;` next to `mod csv;` (alphabetical: after `mod csv;`).

- [ ] **Step 3: write the failing test** at the bottom of `crates/kessel-fetch/src/ndjson.rs`:

```rust
//! NDJSON: one JSON object per line; blank lines skipped.
use crate::json::{path_get, parse, Cell};
use crate::{ColumnMap, FetchError};

pub fn extract(
    body: &[u8],
    cols: &[ColumnMap],
) -> Result<Vec<Vec<Cell>>, FetchError> {
    let text = std::str::from_utf8(body)
        .map_err(|_| FetchError::Parse("NDJSON not UTF-8".into()))?;
    let mut rows = Vec::new();
    for (lineno, line) in text.split('\n').enumerate() {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        let v = parse(t).map_err(|e| {
            FetchError::Parse(format!("NDJSON line {}: {e}", lineno + 1))
        })?;
        let mut row = Vec::with_capacity(cols.len());
        for c in cols {
            row.push(path_get(&v, &c.source)?);
        }
        rows.push(row);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_catalog::FieldKind;
    fn cm(n: &str, s: &str) -> ColumnMap {
        ColumnMap { name: n.into(), kind: FieldKind::U64, source: s.into() }
    }

    #[test]
    fn objects_per_line_blanks_skipped() {
        let body = b"{\"id\":1,\"u\":{\"n\":\"a\"}}\n\n{\"id\":2,\"u\":{\"n\":\"b\"}}\n";
        let cols = vec![cm("id", "id"), cm("nm", "u.n")];
        assert_eq!(
            extract(body, &cols).unwrap(),
            vec![
                vec![Cell::Text("1".into()), Cell::Text("a".into())],
                vec![Cell::Text("2".into()), Cell::Text("b".into())],
            ]
        );
    }

    #[test]
    fn malformed_line_is_typed_error() {
        let body = b"{\"id\":1}\n{not json}\n";
        assert!(matches!(
            extract(body, &[cm("id", "id")]),
            Err(FetchError::Parse(_))
        ));
    }

    #[test]
    fn no_trailing_newline_ok() {
        let body = b"{\"id\":7}";
        assert_eq!(
            extract(body, &[cm("id", "id")]).unwrap(),
            vec![vec![Cell::Text("7".into())]]
        );
    }
}
```

- [ ] **Step 4: run RED.** `cd /c/Users/ihass/KesselDB && export PATH=$HOME/.cargo/bin:$PATH && cargo test -p kessel-fetch --lib ndjson:: 2>&1 | tail -15`. Expected: compile error until Step 1/2 done, then GREEN (the `extract` body is written above — this task is implementation-with-tests; the "RED" is the pre-visibility-bump compile failure). Confirm 3 tests pass.

- [ ] **Step 5: full crate test.** `cargo test -p kessel-fetch 2>&1 | tail -8` — all prior tests + 3 new pass; `git status --porcelain` clean of stray files.

- [ ] **Step 6: commit.**
```
git add crates/kessel-fetch/src/json.rs crates/kessel-fetch/src/lib.rs crates/kessel-fetch/src/ndjson.rs
git commit -m "kessel-fetch: NDJSON parser (object-per-line)"
```
(no Co-Authored-By/signing — match `git log -3 --format='%s'`.)

### Task 2: `Format::Ndjson` wired into `fetch_rows`

**Files:** Modify `crates/kessel-fetch/src/lib.rs`.

- [ ] **Step 1: failing test** — add to `crates/kessel-fetch/tests/stub_server.rs` (it already drives `fetch_rows` over a stub):

```rust
#[test]
fn ndjson_over_http_round_trips() {
    let port = serve_once("{\"id\":3}\n{\"id\":4}\n", None);
    let cols = vec![ColumnMap {
        name: "id".into(), kind: FieldKind::U32, source: "id".into(),
    }];
    let rows = fetch_rows(
        &format!("http://127.0.0.1:{port}/d"),
        &Auth::None, Format::Ndjson, &cols, DEFAULT_MAX_BODY,
    ).unwrap();
    assert_eq!(rows, vec![vec![vec![3,0,0,0]], vec![vec![4,0,0,0]]]);
}
```

- [ ] **Step 2: run RED.** `cargo test -p kessel-fetch --test stub_server ndjson_over_http_round_trips 2>&1 | tail -10`. Expected: compile error `no variant Ndjson`.

- [ ] **Step 3: implement.** In `crates/kessel-fetch/src/lib.rs`: add `Ndjson` to `pub enum Format` (now `Json, Csv, Ndjson`). In `fetch_rows`, change the match to:
```rust
    let raw_rows = match format {
        Format::Json => json::extract(&body, cols)?,
        Format::Csv => csv::extract(&body, cols)?,
        Format::Ndjson => ndjson::extract(&body, cols)?,
    };
```

- [ ] **Step 4: run GREEN.** `cargo test -p kessel-fetch --test stub_server 2>&1 | tail -8` — all stub tests incl. the new one pass.

- [ ] **Step 5: workspace builds (Format is exhaustively matched in do_refresh).** `cargo build -p kesseldb-server --features external-sources 2>&1 | tail -5` — EXPECTED to FAIL only with a non-exhaustive `match recipe.format`/`Format` arm in `do_refresh` (Task 9 wires it). Note this is expected; do NOT fix here. `cargo build --workspace 2>&1 | tail -3` (default features) must be clean (kessel-fetch not compiled into default).

- [ ] **Step 6: commit.**
```
git add crates/kessel-fetch/src/lib.rs crates/kessel-fetch/tests/stub_server.rs
git commit -m "kessel-fetch: Format::Ndjson in fetch_rows"
```

### Task 3: response headers (`get_resp`) for the Link header

**Files:** Modify `crates/kessel-fetch/src/http.rs`.

- [ ] **Step 1: failing test** at the bottom of `crates/kessel-fetch/tests/stub_server.rs`:

```rust
#[test]
fn get_resp_exposes_headers() {
    // serve_once-style server that emits a Link header
    use std::io::{Read, Write};
    use std::net::TcpListener;
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let h = std::thread::spawn(move || {
        let (mut s, _) = l.accept().unwrap();
        let mut b = [0u8; 512]; let _ = s.read(&mut b);
        let body = b"[]";
        let _ = s.write_all(format!(
            "HTTP/1.1 200 OK\r\nLink: <http://x/p2>; rel=\"next\"\r\nContent-Length: {}\r\n\r\n",
            body.len()).as_bytes());
        let _ = s.write_all(body);
    });
    let (headers, body) = kessel_fetch::http_get_resp_for_test(
        &format!("http://127.0.0.1:{port}/d"), DEFAULT_MAX_BODY);
    assert_eq!(body, b"[]");
    assert!(headers.iter().any(|(k,v)|
        k.eq_ignore_ascii_case("link") && v.contains("rel=\"next\"")));
    let _ = h.join();
}
```

- [ ] **Step 2: run RED.** `cargo test -p kessel-fetch --test stub_server get_resp_exposes_headers 2>&1 | tail -10` → unresolved `http_get_resp_for_test`.

- [ ] **Step 3: implement.** In `crates/kessel-fetch/src/http.rs`: refactor so a `pub(crate) fn get_resp(url:&str, auth:&Auth, max_body:u64) -> Result<(Vec<(String,String)>, Vec<u8>), FetchError>` does what `get` does today but also returns the parsed response headers as `(name,value)` pairs (split each header line on the first `:`, trim; you already iterate header lines for `Transfer-Encoding` — collect them all into the Vec there). Make `pub fn get(url,auth,max_body)->Result<Vec<u8>,FetchError> { Ok(get_resp(url,auth,max_body)?.1) }`. In `crates/kessel-fetch/src/lib.rs` add a test-only shim: `#[doc(hidden)] pub fn http_get_resp_for_test(url:&str, max_body:u64) -> (Vec<(String,String)>, Vec<u8>) { http::get_resp(url,&Auth::None,max_body).unwrap() }` (guarded `#[cfg(any(test, feature="__test"))]` is overkill — just `#[doc(hidden)] pub`; it's an optional-crate internal). If you prefer not to expose a shim, instead put `get_resp_exposes_headers` as a `#[cfg(test)] mod tests` inside `http.rs` calling `get_resp` directly and delete the stub_server version — either is fine; keep ONE.

- [ ] **Step 4: run GREEN.** `cargo test -p kessel-fetch 2>&1 | tail -8` — all pass.

- [ ] **Step 5: commit.**
```
git add crates/kessel-fetch/src/http.rs crates/kessel-fetch/src/lib.rs crates/kessel-fetch/tests/stub_server.rs
git commit -m "kessel-fetch: get_resp returns response headers (for Link pagination)"
```

### Task 4: json `rows_at` (ROWS path) + `opt_string_at` (body cursor)

**Files:** Modify `crates/kessel-fetch/src/json.rs`.

- [ ] **Step 1: failing tests** in `crates/kessel-fetch/src/json.rs` `#[cfg(test)] mod tests` (add a `mod tests` if absent; otherwise append):

```rust
    #[test]
    fn rows_at_navigates_envelope_then_extracts() {
        let body = br#"{"data":{"items":[{"id":1},{"id":2}]},"p":{"next":"http://x/2"}}"#;
        let cols = vec![ColumnMap{name:"id".into(),kind:kessel_catalog::FieldKind::U64,source:"id".into()}];
        let rows = rows_at(body, &cols, Some("data.items")).unwrap();
        assert_eq!(rows, vec![vec![Cell::Text("1".into())], vec![Cell::Text("2".into())]]);
        // None rows_path == top-level array (delegates to extract())
        let arr = br#"[{"id":9}]"#;
        assert_eq!(rows_at(arr, &cols, None).unwrap(), vec![vec![Cell::Text("9".into())]]);
        // path not an array => Parse error
        assert!(matches!(rows_at(body, &cols, Some("p")), Err(FetchError::Parse(_))));
    }

    #[test]
    fn opt_string_at_reads_cursor_or_none() {
        let b = br#"{"p":{"next":"http://x/2"},"empty":"","nul":null}"#;
        assert_eq!(opt_string_at(b, "p.next").unwrap(), Some("http://x/2".to_string()));
        assert_eq!(opt_string_at(b, "empty").unwrap(), None);   // empty string => stop
        assert_eq!(opt_string_at(b, "nul").unwrap(), None);     // null => stop
        assert_eq!(opt_string_at(b, "missing").unwrap(), None); // absent => stop
        // a number cursor is rendered as its text
        let n = br#"{"c":42}"#;
        assert_eq!(opt_string_at(n, "c").unwrap(), Some("42".to_string()));
    }
```

- [ ] **Step 2: run RED.** `cargo test -p kessel-fetch --lib json::tests 2>&1 | tail -12` → unresolved `rows_at`/`opt_string_at`.

- [ ] **Step 3: implement** in `crates/kessel-fetch/src/json.rs` (above `#[cfg(test)]`):

```rust
/// Navigate `rows_path` (if any) to a JSON array, then extract `cols`
/// from each element (same scalar dotted-path rule as `extract`).
/// `None` rows_path == today's top-level-array behavior.
pub(crate) fn rows_at(
    body: &[u8],
    cols: &[ColumnMap],
    rows_path: Option<&str>,
) -> Result<Vec<Vec<Cell>>, FetchError> {
    let rp = match rows_path {
        None => return extract(body, cols),
        Some(p) => p,
    };
    let v = parse(std::str::from_utf8(body).map_err(|_| {
        FetchError::Parse("body is not UTF-8".into())
    })?)?;
    // walk rp to a node
    let mut cur = &v;
    for seg in rp.split('.') {
        match cur {
            Json::Object(m) => {
                cur = m.iter().find(|(k, _)| k == seg).map(|(_, x)| x)
                    .ok_or_else(|| FetchError::Parse(format!(
                        "ROWS `{rp}`: no key `{seg}`")))?;
            }
            _ => return Err(FetchError::Parse(format!(
                "ROWS `{rp}`: `{seg}` is not an object"))),
        }
    }
    let arr = match cur {
        Json::Array(a) => a,
        _ => return Err(FetchError::Parse(format!(
            "ROWS `{rp}` is not an array"))),
    };
    let mut rows = Vec::with_capacity(arr.len());
    for el in arr {
        let mut row = Vec::with_capacity(cols.len());
        for c in cols { row.push(path_get(el, &c.source)?); }
        rows.push(row);
    }
    Ok(rows)
}

/// Read the scalar at `path`. `None` if absent / JSON null / empty
/// string (the pagination "stop" signals). Numbers render as text.
pub(crate) fn opt_string_at(
    body: &[u8],
    path: &str,
) -> Result<Option<String>, FetchError> {
    let v = parse(std::str::from_utf8(body).map_err(|_| {
        FetchError::Parse("body is not UTF-8".into())
    })?)?;
    let mut cur = &v;
    for seg in path.split('.') {
        match cur {
            Json::Object(m) => match m.iter().find(|(k, _)| k == seg) {
                Some((_, x)) => cur = x,
                None => return Ok(None), // absent => stop
            },
            _ => return Ok(None),
        }
    }
    Ok(match cur {
        Json::Null => None,
        Json::Str(s) if s.is_empty() => None,
        Json::Str(s) => Some(s.clone()),
        Json::Num(n) => Some(n.clone()),
        _ => None,
    })
}
```

- [ ] **Step 4: run GREEN.** `cargo test -p kessel-fetch --lib json:: 2>&1 | tail -10` — all json tests pass.

- [ ] **Step 5: commit.**
```
git add crates/kessel-fetch/src/json.rs
git commit -m "kessel-fetch: json rows_at (ROWS path) + opt_string_at (cursor)"
```

### Task 5: `Pagination` type, caps, and `fetch_rows_paginated`

**Files:** Modify `crates/kessel-fetch/src/lib.rs`; Create `crates/kessel-fetch/tests/paginate_stub.rs`.

- [ ] **Step 1: failing integration test** `crates/kessel-fetch/tests/paginate_stub.rs`:

```rust
//! Multi-page localhost stub exercising each cursor form + caps.
use kessel_catalog::FieldKind;
use kessel_fetch::{fetch_rows_paginated, Auth, ColumnMap, Format,
    Pagination, DEFAULT_MAX_BODY};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;

/// Serves queued (status_line_extra_headers, body) per connection.
fn server(pages: Vec<(String, String)>) -> (u16, thread::JoinHandle<()>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let q = Arc::new(Mutex::new(pages));
    let h = thread::spawn(move || {
        loop {
            let (mut s, _) = match l.accept() { Ok(x)=>x, Err(_)=>break };
            let mut b = [0u8; 1024]; let _ = s.read(&mut b);
            let mut g = q.lock().unwrap();
            if g.is_empty() { break; }
            let (hdrs, body) = g.remove(0);
            let _ = s.write_all(format!(
                "HTTP/1.1 200 OK\r\n{hdrs}Content-Length: {}\r\n\r\n{body}",
                body.len()).as_bytes());
            if g.is_empty() { break; }
        }
    });
    (port, h)
}
fn col() -> Vec<ColumnMap> {
    vec![ColumnMap{name:"id".into(),kind:FieldKind::U32,source:"id".into()}]
}

#[test]
fn next_url_json_walks_pages() {
    let (port, h) = server(vec![
        (String::new(), r#"{"items":[{"id":1}],"pg":{"next":"PORT/p2"}}"#.into()),
        (String::new(), r#"{"items":[{"id":2}],"pg":{"next":null}}"#.into()),
    ]);
    // patch PORT placeholder
    // (server returns the literal; rewrite by serving absolute URLs)
    let base = format!("http://127.0.0.1:{port}/p1");
    // Re-stage with the real port substituted:
    // (simplest: the test server above ignores path; the recipe's
    // next path yields the next absolute URL we control)
    let _ = &base; let _ = &h;
    // Build pages with the actual port:
    let (port, h) = server(vec![
        (String::new(), format!(r#"{{"items":[{{"id":1}}],"pg":{{"next":"http://127.0.0.1:{p}/p2"}}}}"#, p=port)),
        (String::new(), r#"{"items":[{"id":2}],"pg":{"next":null}}"#.into()),
    ]);
    let rows = fetch_rows_paginated(
        &format!("http://127.0.0.1:{port}/p1"), &Auth::None,
        Format::Json, &col(), Some("items"),
        &Pagination::NextUrlJson("pg.next".into()), DEFAULT_MAX_BODY,
    ).unwrap();
    assert_eq!(rows, vec![vec![vec![1,0,0,0]], vec![vec![2,0,0,0]]]);
    let _ = h.join();
}

#[test]
fn next_link_header_walks_then_stops() {
    let (port, h) = server(vec![
        (String::new(), String::new()), // placeholder, restaged below
    ]);
    let _ = h;
    let (port, h) = server(vec![
        (format!("Link: <http://127.0.0.1:{p}/p2>; rel=\"next\"\r\n", p=port),
         r#"[{"id":5}]"#.into()),
        (String::new(), r#"[{"id":6}]"#.into()), // no Link => stop
    ]);
    let rows = fetch_rows_paginated(
        &format!("http://127.0.0.1:{port}/p1"), &Auth::None,
        Format::Json, &col(), None, &Pagination::NextLink, DEFAULT_MAX_BODY,
    ).unwrap();
    assert_eq!(rows, vec![vec![vec![5,0,0,0]], vec![vec![6,0,0,0]]]);
    let _ = h.join();
}

#[test]
fn cursor_token_into_param() {
    let (port, h) = server(vec![
        (String::new(), r#"{"items":[{"id":7}],"meta":{"cur":"C2"}}"#.into()),
        (String::new(), r#"{"items":[{"id":8}],"meta":{"cur":null}}"#.into()),
    ]);
    let rows = fetch_rows_paginated(
        &format!("http://127.0.0.1:{port}/feed"), &Auth::None,
        Format::Json, &col(), Some("items"),
        &Pagination::CursorJson{path:"meta.cur".into(), param:"cursor".into()},
        DEFAULT_MAX_BODY,
    ).unwrap();
    assert_eq!(rows, vec![vec![vec![7,0,0,0]], vec![vec![8,0,0,0]]]);
    let _ = h.join();
}

#[test]
fn loop_detection_is_typed_error() {
    let (port, h) = server(vec![
        (String::new(), format!(r#"{{"items":[{{"id":1}}],"pg":{{"next":"http://127.0.0.1:{p}/same"}}}}"#, p=port)),
        (String::new(), format!(r#"{{"items":[{{"id":1}}],"pg":{{"next":"http://127.0.0.1:{p}/same"}}}}"#, p=port)),
    ]);
    let e = fetch_rows_paginated(
        &format!("http://127.0.0.1:{port}/same"), &Auth::None,
        Format::Json, &col(), Some("items"),
        &Pagination::NextUrlJson("pg.next".into()), DEFAULT_MAX_BODY,
    ).unwrap_err();
    assert!(matches!(e, kessel_fetch::FetchError::Http(_) | kessel_fetch::FetchError::Parse(_)));
    let _ = h.join();
}
```

- [ ] **Step 2: run RED.** `cargo test -p kessel-fetch --test paginate_stub 2>&1 | tail -10` → unresolved `Pagination`/`fetch_rows_paginated`.

- [ ] **Step 3: implement** in `crates/kessel-fetch/src/lib.rs`:

```rust
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
/// concatenated rows of every page. Bounded + loop-detected; any
/// error ⇒ Err (caller submits nothing).
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
                "pagination exceeded {MAX_PAGES} pages")));
        }
        if seen.iter().any(|u| u == &url) {
            return Err(FetchError::Http(format!(
                "pagination loop detected at `{url}`")));
        }
        seen.push(url.clone());
        let (headers, body) = http::get_resp(&url, auth, per_page_max_body)?;
        total += body.len() as u64;
        if total > MAX_TOTAL_BODY {
            return Err(FetchError::TooLarge(MAX_TOTAL_BODY));
        }
        // rows of this page
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
        // next pointer
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

/// Replace/append `?param=value` on `base` (slice-1: simple, no
/// percent-encoding — tokens are opaque API-supplied ASCII).
fn set_query_param(base: &str, param: &str, value: &str) -> String {
    let (path, query) = match base.split_once('?') {
        Some((p, q)) => (p, q),
        None => (base, ""),
    };
    let mut parts: Vec<String> = query
        .split('&')
        .filter(|kv| !kv.is_empty()
            && !kv.starts_with(&format!("{param}=")))
        .map(|s| s.to_string())
        .collect();
    parts.push(format!("{param}={value}"));
    format!("{path}?{}", parts.join("&"))
}

/// Parse `Link: <url>; rel="next"` (RFC 8288, the one rel we need).
fn link_next(headers: &[(String, String)]) -> Option<String> {
    let link = headers.iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("link"))
        .map(|(_, v)| v.as_str())?;
    for part in link.split(',') {
        let p = part.trim();
        if p.contains("rel=\"next\"") || p.contains("rel=next") {
            let s = p.find('<')?;
            let e = p[s + 1..].find('>')? + s + 1;
            return Some(p[s + 1..e].to_string());
        }
    }
    None
}
```
Ensure `mod ndjson;` and the `coerce`/`csv`/`json`/`http` modules are reachable (they are, same crate). `Format` now has `Json|Csv|Ndjson`.

- [ ] **Step 4: run GREEN.** `cargo test -p kessel-fetch --test paginate_stub 2>&1 | tail -12` — 4 tests pass. Then `cargo test -p kessel-fetch 2>&1 | tail -8` — whole crate green.

- [ ] **Step 5: commit.**
```
git add crates/kessel-fetch/src/lib.rs crates/kessel-fetch/tests/paginate_stub.rs
git commit -m "kessel-fetch: fetch_rows_paginated (3 cursor forms, caps, loop-detect)"
```

---

## Phase 2 — catalog + proto (backward-compatible additive)

### Task 6: `ExternalRecipe` + versioned trailer

**Files:** Modify `crates/kessel-catalog/src/lib.rs`. Read first: the v1 trailer encode (`if !self.external.is_empty()` block) and decode closure.

- [ ] **Step 1: failing test** in the catalog `#[cfg(test)] mod tests` (sibling to `catalog_external_recipe_round_trips_and_is_backward_compatible`):

```rust
#[test]
fn external_recipe_pagination_round_trips_and_v1_back_compat() {
    let mut c = Catalog::default();
    c.types.push(ObjectType{ type_id:1, name:"t".into(), schema_ver:1,
        fields: sample_fields(), indexes:vec![], unique:vec![], fks:vec![],
        checks:vec![], triggers:vec![], ordered:vec![], composite:vec![],
        defaults:vec![] });
    c.external.push(ExternalRecipe{
        type_id:1, url:"http://x".into(), format:2, key_field_id:1,
        auth:ExternalAuth::None, mapping:vec![(1,"id".into())],
        rows_path: Some("data.items".into()),
        pagination: Some(PaginationRecipe::CursorJson{
            path:"m.cur".into(), param:"cursor".into() }),
    });
    let back = Catalog::decode(&c.encode()).unwrap();
    assert_eq!(back.external.len(), 1);
    assert_eq!(back.external[0].rows_path.as_deref(), Some("data.items"));
    assert_eq!(back.external[0].pagination,
        Some(PaginationRecipe::CursorJson{path:"m.cur".into(),param:"cursor".into()}));
    // a slice-1 (v1) recipe with NEITHER new field decodes to None/None
    let mut c1 = Catalog::default();
    c1.types.push(c.types[0].clone());
    c1.external.push(ExternalRecipe{
        type_id:1, url:"http://y".into(), format:0, key_field_id:1,
        auth:ExternalAuth::None, mapping:vec![(1,"id".into())],
        rows_path: None, pagination: None });
    let b1 = Catalog::decode(&c1.encode()).unwrap();
    assert_eq!(b1.external[0].rows_path, None);
    assert_eq!(b1.external[0].pagination, None);
    // empty external => zero trailer bytes (digest unchanged)
    let mut e = Catalog::default(); e.types.push(c.types[0].clone());
    assert!(Catalog::decode(&e.encode()).unwrap().external.is_empty());
}
```
Also add a **hard v1-bytes test** (a recipe encoded by the slice-1 layout must still decode under the new code): construct the v1 byte layout by hand for one recipe and assert `Catalog::decode` yields it with `rows_path=None, pagination=None`. Concretely:
```rust
#[test]
fn decodes_a_handwritten_v1_external_trailer() {
    // minimal catalog header+0 types, then a v1 external trailer:
    // [u32 n=1][type_id u32=1][format u8=0][kfid u16=1]
    // [url str32 "u"][auth tag 0][map len u32=1][(fid u16=1)(src str32 "s")]
    let mut blob = Catalog::default().encode(); // header, no external
    // append v1 trailer bytes:
    blob.extend_from_slice(&1u32.to_le_bytes());          // n
    blob.extend_from_slice(&1u32.to_le_bytes());          // type_id
    blob.push(0);                                         // format
    blob.extend_from_slice(&1u16.to_le_bytes());          // key_field_id
    blob.extend_from_slice(&1u32.to_le_bytes()); blob.push(b'u'); // url str32
    blob.push(0);                                         // auth None
    blob.extend_from_slice(&1u32.to_le_bytes());          // mapping len
    blob.extend_from_slice(&1u16.to_le_bytes());          // fid
    blob.extend_from_slice(&1u32.to_le_bytes()); blob.push(b's'); // src str32
    let cat = Catalog::decode(&blob).unwrap();
    assert_eq!(cat.external.len(), 1);
    assert_eq!(cat.external[0].url, "u");
    assert_eq!(cat.external[0].rows_path, None);
    assert_eq!(cat.external[0].pagination, None);
}
```
(If `Catalog::default().encode()` already ends with no external bytes, appending a v1 trailer reproduces exactly what a slice-1 binary persisted. This pins backward-compat.)

- [ ] **Step 2: run RED.** `cargo test -p kessel-catalog external_recipe_pagination decodes_a_handwritten_v1 2>&1 | tail -12` → unresolved `PaginationRecipe`/fields.

- [ ] **Step 3: implement.** Add near `ExternalAuth`:
```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PaginationRecipe {
    NextUrlJson(String),
    NextLink,
    CursorJson { path: String, param: String },
}
```
Add to `struct ExternalRecipe`: `pub rows_path: Option<String>,` and `pub pagination: Option<PaginationRecipe>,`. Update EVERY `ExternalRecipe { … }` literal in the file (incl. the slice-1 test literals) to add `rows_path: None, pagination: None` (or real values where the new test sets them) — `cargo build -p kessel-catalog --tests` will list them; fix each.

Trailer **versioning** — change the encode block (only entered when `!self.external.is_empty()`): write a **v2 sentinel** first so a slice-1 reader is unaffected and a new reader can tell versions apart. v1 wrote `[u32 n>=1]…`. v2 writes:
```rust
        if !self.external.is_empty() {
            // v2 trailer: leading [u32 0] sentinel (v1 never emits 0 —
            // it only writes the trailer when n>=1), then [u8 ver=2]
            // [u32 n] then per-recipe v2 layout.
            b.extend_from_slice(&0u32.to_le_bytes());
            b.push(2u8);
            b.extend_from_slice(&(self.external.len() as u32).to_le_bytes());
            for r in &self.external {
                b.extend_from_slice(&r.type_id.to_le_bytes());
                b.push(r.format);
                b.extend_from_slice(&r.key_field_id.to_le_bytes());
                put_str32(&mut b, &r.url);
                match &r.auth {
                    ExternalAuth::None => b.push(0),
                    ExternalAuth::BearerEnv(e) => { b.push(1); put_str32(&mut b, e); }
                    ExternalAuth::HeaderEnv{header,env} => {
                        b.push(2); put_str32(&mut b, header); put_str32(&mut b, env); }
                }
                b.extend_from_slice(&(r.mapping.len() as u32).to_le_bytes());
                for (fid, src) in &r.mapping {
                    b.extend_from_slice(&fid.to_le_bytes());
                    put_str32(&mut b, src);
                }
                // NEW v2 fields:
                match &r.rows_path {
                    None => b.push(0),
                    Some(s) => { b.push(1); put_str32(&mut b, s); }
                }
                match &r.pagination {
                    None => b.push(0),
                    Some(PaginationRecipe::NextUrlJson(p)) => { b.push(1); put_str32(&mut b, p); }
                    Some(PaginationRecipe::NextLink) => b.push(2),
                    Some(PaginationRecipe::CursorJson{path,param}) => {
                        b.push(3); put_str32(&mut b, path); put_str32(&mut b, param); }
                }
            }
        }
```
Decode — branch on the leading u32 (read it where the v1 decode read `n`): if it is `0`, it's v2 → read `[u8 ver][u32 n]` then the v2 per-recipe layout (incl. the two new fields). If it is `>= 1`, it's a v1 trailer → run the EXISTING slice-1 per-recipe parse and set `rows_path: None, pagination: None`. Keep the whole thing inside the existing `(|| -> Option<Vec<ExternalRecipe>> { … })().unwrap_or_default()` closure so any short/garbled read still yields empty (slice-1 philosophy). For the v2 auth tag, reuse the same 0/1/2 logic; for `rows_path`: `0`⇒None, `1`⇒Some(get_str32); pagination tag `0`⇒None,`1`⇒NextUrlJson(get_str32),`2`⇒NextLink,`3`⇒CursorJson{get_str32,get_str32}, other⇒return None (same "unknown ⇒ drop" stance as the auth tag, comment it identically).

- [ ] **Step 4: run GREEN.** `cargo test -p kessel-catalog 2>&1 | tail -12` — all catalog tests (incl. the 2 new + the existing slice-1 round-trip) pass.

- [ ] **Step 5: determinism gate.** `cargo test --workspace --release 2>&1 > /tmp/p6.txt; grep -cE "test result: FAILED" /tmp/p6.txt; awk '/test result: ok\./ {match($0,/ok\. ([0-9]+) passed/,a); s+=a[1]} END{print "TOTAL",s}' /tmp/p6.txt; grep -c "large_seed_corpus_is_deterministic_and_converges ... ok" /tmp/p6.txt`. REQUIRE FAILED=0, seed-corpus=1, TOTAL = 222 + (number of NEW catalog `#[test]` fns added = 2) = **224**. Empty-external still appends zero bytes ⇒ existing digests/seed-7 unaffected.

- [ ] **Step 6: commit.**
```
git add crates/kessel-catalog/src/lib.rs
git commit -m "kessel-catalog: v2 external trailer adds rows_path + pagination (v1 back-compat)"
```

### Task 7: `Op::CreateExternalSource` additive fields

**Files:** Modify `crates/kessel-proto/src/lib.rs`. Read first: the `41 =>` decode arm, the encode arm, and the `codec::Cursor` API (whether its readers return `Option` via `?` and whether it exposes a "bytes remaining"/`is_empty` check — needed for tolerant decode of pre-pagination frames).

- [ ] **Step 1: failing test** in the proto tests (sibling to `external_source_ops_wire_round_trip`):

```rust
#[test]
fn create_external_source_pagination_wire_round_trip_and_back_compat() {
    let op = Op::CreateExternalSource{
        name:"f".into(), type_def:vec![1], url:"u".into(), format:2,
        key_field_id:1, auth_kind:0, auth_a:String::new(), auth_b:String::new(),
        mapping:vec![(1,"id".into())],
        rows_path: Some("d.items".into()),
        pagination: Some((3, "m.c".into(), "cursor".into())),
    };
    let back = Op::decode(&op.encode()).expect("decode");
    assert_eq!(back, op);
    assert_eq!(op.kind(), op.encode()[0]);
    assert!(op.is_mutating());
    // back-compat: a frame WITHOUT the trailing pagination fields
    // (slice-1 layout) must decode to rows_path=None,pagination=None.
    // Build a slice-1-shaped kind-41 frame by encoding the same op with
    // None/None and truncating is unsafe; instead assert the None/None
    // op round-trips AND that decoding a None/None encoding yields None.
    let op0 = Op::CreateExternalSource{
        name:"f".into(), type_def:vec![1], url:"u".into(), format:0,
        key_field_id:1, auth_kind:0, auth_a:String::new(), auth_b:String::new(),
        mapping:vec![(1,"id".into())], rows_path:None, pagination:None };
    assert_eq!(Op::decode(&op0.encode()).unwrap(), op0);
}
```
PLUS a hard pre-pagination-bytes test: hand-build a kind-41 frame in the **slice-1 layout** (no trailing fields) and assert decode yields `rows_path:None, pagination:None`:
```rust
#[test]
fn decodes_pre_pagination_create_external_source_frame() {
    // kind 41 + slice-1 fields ONLY, no trailing rows/pagination bytes.
    let mut b = vec![41u8];
    let put = |b:&mut Vec<u8>, s:&[u8]| { b.extend_from_slice(&(s.len() as u32).to_le_bytes()); b.extend_from_slice(s); };
    put(&mut b, b"f");            // name
    put(&mut b, &[1]);            // type_def
    put(&mut b, b"u");            // url
    b.push(0);                    // format
    b.extend_from_slice(&1u16.to_le_bytes()); // key_field_id
    b.push(0);                    // auth_kind
    put(&mut b, b"");             // auth_a
    put(&mut b, b"");             // auth_b
    b.extend_from_slice(&1u32.to_le_bytes()); // mapping len
    b.extend_from_slice(&1u16.to_le_bytes()); put(&mut b, b"id"); // (fid,src)
    let op = Op::decode(&b).expect("slice-1 frame must still decode");
    match op {
        Op::CreateExternalSource{ rows_path, pagination, .. } => {
            assert_eq!(rows_path, None);
            assert_eq!(pagination, None);
        }
        _ => panic!("wrong variant"),
    }
}
```

- [ ] **Step 2: run RED.** `cargo test -p kessel-proto create_external_source_pagination decodes_pre_pagination 2>&1 | tail -12` → field/struct errors.

- [ ] **Step 3: implement.** Add to the `Op::CreateExternalSource { … }` variant: `rows_path: Option<String>,` and `pagination: Option<(u8, String, String)>,` (tag 1=NextUrlJson(a), 2=NextLink, 3=CursorJson{a=path,b=param}; a/b unused fields = `String::new()`). Update the encode arm: after the mapping loop, append:
```rust
                match rows_path {
                    None => b.push(0),
                    Some(s) => { b.push(1); codec::put_bytes(&mut b, s.as_bytes()); }
                }
                match pagination {
                    None => b.push(0),
                    Some((tag, a, c)) => {
                        b.push(*tag); // 1|2|3
                        codec::put_bytes(&mut b, a.as_bytes());
                        codec::put_bytes(&mut b, c.as_bytes());
                    }
                }
```
Update the `41 =>` decode arm: after the mapping loop, read the new fields **tolerantly** — a pre-pagination (slice-1) frame has no trailing bytes, so the cursor is exhausted there and the new reads must default to `None` instead of failing the whole decode. Inspect `codec::Cursor`: if it offers a remaining-bytes/`is_empty` check, gate the reads on it; if its readers return `Option`, use that to default. Concretely (adapt to the real Cursor API — this is the one resolve-against-real-code point; the two tests above pin the contract):
```rust
                // tolerant: absent trailing bytes ⇒ slice-1 frame
                let rows_path = match c.u8() {
                    Some(0) | None => None,
                    Some(1) => Some(String::from_utf8_lossy(&c.bytes()?).into_owned()),
                    Some(_) => None,
                };
                let pagination = match c.u8() {
                    Some(0) | None => None,
                    Some(t @ (1 | 2 | 3)) => {
                        let a = String::from_utf8_lossy(&c.bytes()?).into_owned();
                        let cc = String::from_utf8_lossy(&c.bytes()?).into_owned();
                        Some((t, a, cc))
                    }
                    Some(_) => None,
                };
```
If `codec::Cursor`'s `u8()` returns `Option<u8>` (not `Result`/`?`), the `Some(_)|None` arms above are exactly right. If it returns via `?` only (no peek/len), add a `pub(crate) fn remaining(&self)->usize` or `is_empty` to `codec::Cursor` (small, its own micro-change in the same file) and gate: `if c.is_empty() { (None,None) } else { …read… }`. Whichever the real API supports — the back-compat test is the contract. Also confirm `is_mutating()` still returns true (the variant set is unchanged for that match) and add `rows_path`/`pagination` to the encode destructure pattern.

- [ ] **Step 4: run GREEN.** `cargo test -p kessel-proto 2>&1 | tail -10` — all proto tests pass (incl. the 2 new + the slice-1 `external_source_ops_wire_round_trip`). `cargo build --workspace 2>&1 | tail -3` — fix any non-exhaustive `Op::CreateExternalSource{..}` destructures the compiler flags (kessel-sm/kessel-sql construct it; they're updated in Tasks 8/9 — for now if a match/destructure breaks, add the two fields; if kessel-sm/kessel-sql *construct* it they'll need the new fields which Tasks 8/9 supply, so a temporary `rows_path:None,pagination:None` at those construction sites is acceptable and will be replaced — note it).

- [ ] **Step 5: determinism gate.** Run the gate (as Task 6 Step 5). REQUIRE FAILED=0, seed-corpus=1, TOTAL = 224 + 2 new proto tests = **226**.

- [ ] **Step 6: commit.**
```
git add crates/kessel-proto/src/lib.rs
git commit -m "kessel-proto: CreateExternalSource gains rows_path + pagination (tolerant back-compat decode)"
```

---

## Phase 3 — SM persist + SQL grammar

### Task 8: SM persists the new recipe fields

**Files:** Modify `crates/kessel-sm/src/lib.rs` (the `Op::CreateExternalSource` apply arm that builds `ExternalRecipe`).

- [ ] **Step 1: failing test** — extend the existing `create_and_drop_external_source_manages_type_and_recipe` test (do NOT add a new `#[test]` — keeps the gate count predictable): after the existing create, add a second source carrying pagination and assert the persisted recipe has it:
```rust
    // pagination/rows_path persisted into the catalog recipe
    let td2 = kessel_catalog::encode_type_def("feed2",
        &[Field{field_id:0,name:"id".into(),kind:FieldKind::U64,nullable:false}]);
    assert_eq!(sm.apply(7, Op::CreateExternalSource{
        name:"feed2".into(), type_def:td2, url:"http://h".into(), format:2,
        key_field_id:1, auth_kind:0, auth_a:String::new(), auth_b:String::new(),
        mapping:vec![(1,"id".into())],
        rows_path: Some("d.items".into()),
        pagination: Some((1, "p.next".into(), String::new())),
    }), OpResult::Ok);
    let cat = sm.catalog();
    let r = cat.external.iter().find(|e|
        e.type_id == cat.types.iter().find(|t|t.name=="feed2").unwrap().type_id
    ).unwrap();
    assert_eq!(r.rows_path.as_deref(), Some("d.items"));
    assert_eq!(r.pagination,
        Some(kessel_catalog::PaginationRecipe::NextUrlJson("p.next".into())));
```

- [ ] **Step 2: run RED.** `cargo test -p kessel-sm create_and_drop_external_source 2>&1 | tail -10` → the `ExternalRecipe{..}` built in `apply` lacks the new fields (compile error) and/or the asserts fail.

- [ ] **Step 3: implement.** In the `Op::CreateExternalSource` apply arm, the destructure must bind `rows_path` and `pagination`; the `kessel_catalog::ExternalRecipe { … }` it pushes must set `rows_path: rows_path,` and `pagination: pagination.map(|(t,a,b)| match t { 1=>PaginationRecipe::NextUrlJson(a), 2=>PaginationRecipe::NextLink, 3=>PaginationRecipe::CursorJson{path:a,param:b}, _=>return OpResult::SchemaError("bad pagination tag".into()) })`. (Map the proto `(u8,String,String)` wire form to the catalog `PaginationRecipe` enum. Reject unknown tag with a typed SchemaError — pre-mutation, consistent with slice-1's C1/I1 ordering: validate the tag BEFORE creating the backing type, same as `auth_kind`.) Keep the existing CreateType-reuse + persist flow unchanged.

- [ ] **Step 4: run GREEN.** `cargo test -p kessel-sm create_and_drop_external_source 2>&1 | tail -8` — passes.

- [ ] **Step 5: determinism gate.** Gate as before. REQUIRE FAILED=0, seed-corpus=1, TOTAL **226** (extended an existing test, no new `#[test]` fn).

- [ ] **Step 6: commit.**
```
git add crates/kessel-sm/src/lib.rs
git commit -m "kessel-sm: persist rows_path + pagination into ExternalRecipe"
```

### Task 9: SQL grammar — `FORMAT NDJSON`, `ROWS`, `PAGE`, compat matrix

**Files:** Modify `crates/kessel-sql/src/lib.rs` (the `if p.kw("EXTERNAL")` CREATE block).

- [ ] **Step 1: failing tests** in the kessel-sql tests:
```rust
#[test]
fn parse_external_source_pagination_forms() {
    let cat = Catalog::default();
    let q = "CREATE EXTERNAL SOURCE f (id U64 NOT NULL FROM 'id') \
        FROM 'http://h' FORMAT JSON KEY id \
        ROWS 'data.items' PAGE NEXT JSON 'p.next'";
    match compile(q,&cat).unwrap() {
        Op::CreateExternalSource{ rows_path, pagination, format, .. } => {
            assert_eq!(format, 0);
            assert_eq!(rows_path.as_deref(), Some("data.items"));
            assert_eq!(pagination, Some((1,"p.next".into(),String::new())));
        } o=>panic!("{o:?}"),
    }
    // NDJSON + LINK header, no ROWS
    match compile("CREATE EXTERNAL SOURCE g (id U64 NOT NULL FROM 'id') \
        FROM 'http://h' FORMAT NDJSON KEY id PAGE NEXT LINK",&cat).unwrap() {
        Op::CreateExternalSource{ format, rows_path, pagination, .. } => {
            assert_eq!(format, 2); assert_eq!(rows_path, None);
            assert_eq!(pagination, Some((2,String::new(),String::new())));
        } o=>panic!("{o:?}"),
    }
    // CURSOR token form
    match compile("CREATE EXTERNAL SOURCE h (id U64 NOT NULL FROM 'id') \
        FROM 'http://h' FORMAT JSON KEY id ROWS 'items' \
        PAGE CURSOR JSON 'm.c' PARAM 'cursor'",&cat).unwrap() {
        Op::CreateExternalSource{ pagination, .. } =>
            assert_eq!(pagination, Some((3,"m.c".into(),"cursor".into()))),
        o=>panic!("{o:?}"),
    }
}

#[test]
fn external_source_compat_matrix_rejected() {
    let cat = Catalog::default();
    // JSON + body cursor without ROWS => error
    assert!(compile("CREATE EXTERNAL SOURCE a (id U64 NOT NULL FROM 'id') \
        FROM 'http://h' FORMAT JSON KEY id PAGE NEXT JSON 'p.next'",&cat).is_err());
    // NDJSON + body cursor => error
    assert!(compile("CREATE EXTERNAL SOURCE b (id U64 NOT NULL FROM 'id') \
        FROM 'http://h' FORMAT NDJSON KEY id PAGE NEXT JSON 'p.next'",&cat).is_err());
    // CSV + body cursor => error
    assert!(compile("CREATE EXTERNAL SOURCE c (id U64 NOT NULL FROM 'id') \
        FROM 'http://h' FORMAT CSV KEY id PAGE CURSOR JSON 'm' PARAM 'p'",&cat).is_err());
    // CSV + LINK => OK
    assert!(compile("CREATE EXTERNAL SOURCE d (id U64 NOT NULL FROM 'id') \
        FROM 'http://h' FORMAT CSV KEY id PAGE NEXT LINK",&cat).is_ok());
}
```

- [ ] **Step 2: run RED.** `cargo test -p kessel-sql parse_external_source_pagination external_source_compat 2>&1 | tail -12` → parse errors / missing fields.

- [ ] **Step 3: implement.** In the `if p.kw("EXTERNAL")` block: extend `FORMAT` parsing to also accept `NDJSON ⇒ 2u8` (`else if p.kw("NDJSON") { 2u8 }`). AFTER the `AUTH` block and BEFORE `encode_type_def`, parse the two optional clauses (order: `ROWS` then `PAGE`):
```rust
            let mut rows_path: Option<String> = None;
            if p.kw("ROWS") {
                rows_path = Some(match p.next() {
                    Some(Tok::Str(s)) => s,
                    _ => return Err("expected 'rows-path' string".into()),
                });
            }
            let mut pagination: Option<(u8,String,String)> = None;
            if p.kw("PAGE") {
                if p.kw("NEXT") {
                    if p.kw("JSON") {
                        let path = match p.next() { Some(Tok::Str(s))=>s,
                            _=>return Err("expected 'path' string".into()) };
                        pagination = Some((1, path, String::new()));
                    } else if p.kw("LINK") {
                        pagination = Some((2, String::new(), String::new()));
                    } else { return Err("PAGE NEXT must be JSON '..' or LINK".into()); }
                } else if p.kw("CURSOR") {
                    p.expect_kw("JSON")?;
                    let path = match p.next() { Some(Tok::Str(s))=>s,
                        _=>return Err("expected 'path' string".into()) };
                    p.expect_kw("PARAM")?;
                    let param = match p.next() { Some(Tok::Str(s))=>s,
                        _=>return Err("expected 'param' string".into()) };
                    pagination = Some((3, path, param));
                } else { return Err("PAGE must be NEXT … or CURSOR …".into()); }
            }
            // compatibility matrix (slice-1):
            let body_cursor = matches!(pagination, Some((1,_,_)) | Some((3,_,_)));
            if body_cursor {
                if format == 0 && rows_path.is_none() {
                    return Err("FORMAT JSON with a body cursor requires ROWS '<path>'".into());
                }
                if format == 1 {
                    return Err("FORMAT CSV cannot use a body cursor (use PAGE NEXT LINK)".into());
                }
                if format == 2 {
                    return Err("FORMAT NDJSON cannot use a body cursor (use PAGE NEXT LINK)".into());
                }
            }
```
Add `rows_path,` and `pagination,` to the returned `Op::CreateExternalSource { … }`. (The proto variant from Task 7 has these fields.)

- [ ] **Step 4: run GREEN.** `cargo test -p kessel-sql 2>&1 | tail -12` — all kessel-sql tests pass (incl. the 2 new + the slice-1 parse tests + DROP/CREATE TABLE not regressed).

- [ ] **Step 5: determinism gate.** Gate. REQUIRE FAILED=0, seed-corpus=1, TOTAL = 226 + 2 new sql tests = **228**.

- [ ] **Step 6: commit.**
```
git add crates/kessel-sql/src/lib.rs
git commit -m "kessel-sql: FORMAT NDJSON, ROWS, PAGE clauses + compatibility matrix"
```

---

## Phase 4 — router dispatch + oracle

### Task 10: `do_refresh` one-branch dispatch + NDJSON format

**Files:** Modify `crates/kesseldb-server/src/router.rs` (`do_refresh`).

- [ ] **Step 1: failing oracle test** — add to `crates/kesseldb-server/tests/external_source_oracle.rs` (`#![cfg(feature="external-sources")]`), reusing the existing harness (cluster + Router + stub server) exactly as the slice-1 oracle does. A 2-page JSON source with `ROWS 'items'` + `PAGE NEXT JSON 'pg.next'`:
```rust
#[test]
fn refresh_paginated_materializes_union_of_pages() {
    // (reuse the slice-1 oracle's cluster+router bring-up helper)
    // stub serves page1 {items:[{id:1,nm:"a"}], pg:{next:"<p2 url>"}}
    // then page2 {items:[{id:2,nm:"b"}], pg:{next:null}}
    // CREATE EXTERNAL SOURCE feedp (id U64 NOT NULL FROM 'id',
    //   nm CHAR(8) NOT NULL FROM 'nm') FROM '<p1>' FORMAT JSON KEY id
    //   ROWS 'items' PAGE NEXT JSON 'pg.next'
    // REFRESH feedp ; SELECT * FROM feedp == {(1,a),(2,b)} (union model)
    // re-REFRESH identical pages => SELECT * blob byte-identical
    // a 3rd staged run whose page1 exceeds caps OR loops => REFRESH
    //   SchemaError AND SELECT * unchanged from prior good state
}
```
Fill the body using the SAME helpers the slice-1 `refresh_oracle_*` test uses (study it; do not invent a new bring-up). Keep these assertions, not weakened: union-of-pages == independent model; idempotent re-REFRESH (byte-identical `SELECT *`); a loop/cap source ⇒ `REFRESH` error + prior data intact.

- [ ] **Step 2: run RED.** `cargo test -p kesseldb-server --features external-sources --test external_source_oracle refresh_paginated 2>&1 | tail -12` → fails (no NDJSON format mapping / no pagination dispatch).

- [ ] **Step 3: implement.** In `do_refresh`: (a) extend the format match to `2 => Format::Ndjson` (and keep the `n =>` error for anything else). (b) Replace the single `fetch_rows(&recipe.url,&auth,format,&cols,DEFAULT_MAX_BODY)` call with a one-branch dispatch:
```rust
        let rows = {
            use kessel_fetch::{fetch_rows_paginated, Pagination};
            let res = match &recipe.pagination {
                None => fetch_rows(&recipe.url, &auth, format, &cols, DEFAULT_MAX_BODY),
                Some(pr) => {
                    let pg = match pr {
                        kessel_catalog::PaginationRecipe::NextUrlJson(p) =>
                            Pagination::NextUrlJson(p.clone()),
                        kessel_catalog::PaginationRecipe::NextLink =>
                            Pagination::NextLink,
                        kessel_catalog::PaginationRecipe::CursorJson{path,param} =>
                            Pagination::CursorJson{path:path.clone(),param:param.clone()},
                    };
                    fetch_rows_paginated(
                        &recipe.url, &auth, format, &cols,
                        recipe.rows_path.as_deref(), &pg, DEFAULT_MAX_BODY)
                }
            };
            match res {
                Ok(r) => r,
                Err(e) => return OpResult::SchemaError(format!("refresh: {e}")),
            }
        };
```
Everything after (`rows` → codec record → sha256 id → atomic upsert `Op::Txn` via `self.forward(&Op::Txn{ops},dedup)`) is **unchanged**. Add `use kessel_fetch::fetch_rows;` if the existing `use` line needs the symbol (it imports `fetch_rows` already — keep it).

- [ ] **Step 4: run GREEN.** `cargo test -p kesseldb-server --features external-sources 2>&1 | tail -12` — slice-1 oracle + the new paginated oracle + all server lib tests pass.

- [ ] **Step 5: determinism gate (feature OFF).** Gate. REQUIRE FAILED=0, seed-corpus=1, TOTAL **228** (the new oracle test is `#![cfg(feature="external-sources")]` ⇒ not in the default run). Also `cargo build --workspace 2>&1 | tail -3` clean.

- [ ] **Step 6: commit.**
```
git add crates/kesseldb-server/src/router.rs crates/kesseldb-server/tests/external_source_oracle.rs
git commit -m "kesseldb-server: do_refresh dispatches to fetch_rows_paginated + NDJSON"
```

---

## Phase 5 — docs

### Task 11: docs / STATUS / USAGE / internal spec

**Files:** Create `docs/superpowers/specs/2026-05-18-kesseldb-subproject98-ext-pagination.md`; Modify `docs/STATUS.md`, `docs/USAGE.md`, `README.md`.

- [ ] **Step 1.** Read `docs/superpowers/specs/2026-05-18-external-sources-pagination-design.md` and the slice-1 docs additions (the `SP97 — External sources` STATUS row, USAGE §7c). Create the internal spec `…-subproject98-ext-pagination.md` mirroring the structure of `…-subproject97-…`: what shipped per crate, the SQL surface (`FORMAT NDJSON`, `ROWS`, the 3 `PAGE` forms), the compatibility matrix, the caps + loop-detection, the captured-once/deterministic argument, the verified test evidence (kessel-fetch ndjson/paginate tests; catalog v2/v1-backcompat; proto back-compat; sql parse+matrix; the paginated oracle; feature-off gate FAILED=0/TOTAL/seed-corpus), and the honest boundaries + deferred items (per-source MAX knobs, Retry-After, concurrent prefetch, CSV body cursor, nested-array rows). SPxx label allowed in this internal file.

- [ ] **Step 2.** STATUS.md: extend the existing `SP97 — External sources` row (or add a tight follow-on row `SP98 — External sources: pagination + NDJSON`) in the same table format; update the slice-1 non-goals paragraph to remove "NDJSON/pagination" from deferred and list the *new* deferred items. USAGE.md: extend §7c with a paginated example (`… FORMAT JSON KEY id ROWS 'data.items' PAGE NEXT JSON 'paging.next'` and a `PAGE CURSOR … PARAM …` one), the compat-matrix one-liner, and the "bounded: fixed page/byte caps, error-on-exceed" note. README.md: one codename-free line in the feature/doc table. **Public docs (README/USAGE/STATUS prose) must contain no `SP##`/"subproject"/"slice"/"Task N" in the ADDED lines** (STATUS table rows may keep the `SP##` prefix convention — match the existing rows exactly). Grep-prove: `git show <commit> -- README.md docs/USAGE.md | grep -nE '^\+' | grep -E 'SP[0-9]|subproject|slice [0-9]|Task [0-9]'` empty.

- [ ] **Step 3.** Final gates: `cargo test --workspace --release` (FAILED=0, TOTAL **228**, seed-corpus ok) and `cargo test -p kesseldb-server --features external-sources` (slice-1 + paginated oracle green). `git status --porcelain` shows only docs.

- [ ] **Step 4: commit & push.**
```
git add docs/ README.md
git commit -m "external sources: pagination + NDJSON — STATUS/USAGE/README + slice spec"
git push origin main
```

---

## Self-review

**Spec coverage:** §1 architecture/invariants → Tasks 5,10 (all logic in kessel-fetch + one-branch dispatch; captured-once unchanged). §2 recipe/SQL surface → Tasks 6,7,8,9. §3 compatibility matrix → Task 9 (CREATE-time, all four rules + the four tests). §4 fetch loop & caps → Task 5 (MAX_PAGES, MAX_TOTAL_BODY, loop-detect, all-or-nothing Err) + Task 1 (NDJSON). §5 determinism/ordering → unchanged downstream (Task 10 keeps the slice-1 id/upsert/Txn path) + documented in Task 11. §6 testing → Tasks 1–10 unit/integration + the paginated oracle (Task 10) + feature-off gate every kernel task. §7 scope/non-goals → Task 11 docs. Every spec section maps to a task.

**Placeholder scan:** No "TBD"/"add error handling"/"similar to". The two resolve-against-real-code points (proto `codec::Cursor` tolerant-decode mechanism in Task 7; reuse of the slice-1 oracle bring-up helper in Task 10) carry **concrete contracts + the exact pinning tests** (`decodes_pre_pagination_create_external_source_frame`, `decodes_a_handwritten_v1_external_trailer`, the 4-assertion paginated oracle) — decisions with defined invariants, not deferred work, consistent with how slice-1's plan handled the same kind of point.

**Type consistency:** `Pagination::{NextUrlJson(String),NextLink,CursorJson{path,param}}` (kessel-fetch) ↔ `PaginationRecipe::{NextUrlJson(String),NextLink,CursorJson{path,param}}` (kessel-catalog) ↔ proto wire `Option<(u8,String,String)>` with tags **1=NextUrlJson(a), 2=NextLink, 3=CursorJson{a=path,b=param}** — the tag mapping is identical in Tasks 7 (proto), 8 (sm→catalog enum), 9 (sql→wire), 10 (catalog enum→kessel-fetch). `Format::{Json=0,Csv=1,Ndjson=2}` consistent across kessel-fetch/sql/do_refresh. `rows_path: Option<String>` everywhere. `fetch_rows_paginated(base_url,auth,format,cols,rows_path:Option<&str>,pagination:&Pagination,per_page_max_body)` signature used identically in Task 5 (def) and Task 10 (call). Caps `MAX_PAGES=1000`, `MAX_TOTAL_BODY=8*DEFAULT_MAX_BODY` match the design. Gate TOTAL accounting: 222 → +2 (Task 6) → 224 → +2 (Task 7) → 226 → +0 (Task 8 extends) → +2 (Task 9) → 228 → +0 (Task 10, feature-gated) → 228 final.
