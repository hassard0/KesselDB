# KesselDB Sub-project 98 — External Sources: pagination + NDJSON (follow-on slice)

**Date:** 2026-05-18  **Status:** shipped. Full workspace regression
245 green (feature OFF — the default); feature-ON oracle green.

EXT slice 2 (SP98): adds **NDJSON** format and **cursor/next-URL
pagination** to the external-sources feature. A single `REFRESH` can
now materialize a multi-page HTTP source. Every slice-1 invariant is
preserved: captured-once at the router, one atomic upsert `Op::Txn`,
deterministic, off by default, kernel/seed-7 corpus unaffected.

---

## What shipped, per crate

### `kessel-fetch` — new NDJSON format + `fetch_rows_paginated`

Two additions on top of the slice-1 `fetch_rows`:

**NDJSON parser (`Format::Ndjson`):** splits the response body on
newlines, skips blank lines, parses each non-blank line as one JSON
object via the existing JSON value parser, and extracts declared columns
via the same dotted-path logic. A malformed (non-object, invalid JSON)
line ⇒ `FetchError::Parse`.

**`fetch_rows_paginated(url, auth, format, cols, max_body, rows_path,
pagination, max_pages, max_total_body) -> Result<Vec<Vec<Vec<u8>>>,
FetchError>`:** self-contained page loop. Per page:

1. Resolve the page URL (page 1 = base URL; subsequent = the extracted
   next-URL or base + `?<qp>=<token>`).
2. `http::get_resp` (returns the response body **and** headers — a new
   helper; `http::get` retains the old signature).
3. Enforce the aggregate-bytes cap (`MAX_TOTAL_BODY = 8 ×
   DEFAULT_MAX_BODY`) and max-pages cap (`MAX_PAGES = 1000`).
4. **Loop-detection:** if the next URL / cursor token exactly equals one
   already seen ⇒ `FetchError::Parse` ("pagination loop detected").
5. Parse this page's rows: top-level array | `ROWS` dotted path | NDJSON
   lines (reusing slice-1 `json`/`csv`/`coerce`).
6. Extract the next pointer: body `<path>` | `Link: …; rel="next"` header
   | opaque token → `<qp>`.
7. Stop when the next pointer is absent, JSON null, or an empty string.
8. Return the **concatenation of all pages' rows** in `Vec<Vec<Vec<u8>>>`
   — the same shape/contract as `fetch_rows`.

New public items:

```rust
pub enum Pagination {
    NextUrlJson(String),             // body-path → absolute next-URL
    NextLink,                        // Link: <url>; rel="next" header
    CursorJson { path: String, param: String }, // body-path token → ?param=
}

pub fn fetch_rows_paginated(
    url, auth, format, cols, max_body,
    rows_path: Option<String>,
    pagination: Option<Pagination>,
    max_pages: usize, max_total_body: usize,
) -> Result<Vec<Vec<Vec<u8>>>, FetchError>

pub fn rows_at(val, path) -> Result<…>  // ROWS-path envelope extraction
pub fn opt_string_at(val, path) -> Option<String>  // cursor extraction
```

`http::get_resp` exposes both the body bytes and the raw `Link` header.
Constants: `MAX_PAGES = 1000`, `MAX_TOTAL_BODY = 8 * DEFAULT_MAX_BODY`.

Any error (HTTP non-2xx, parse, coerce, body cap, aggregate cap,
max-pages, loop detected) ⇒ `Err(FetchError)` ⇒ `do_refresh` submits
**nothing** — all-or-nothing atomic abort, unchanged from slice-1.

### `kessel-catalog` — versioned (v2) backward-compatible trailer

`ExternalRecipe` gained two optional fields:

| Field | Type | Notes |
|---|---|---|
| `rows_path` | `Option<String>` | dotted path to the row array inside an envelope |
| `pagination` | `Option<PaginationRecipe>` | enum mirroring the three `PAGE` forms + param name |

Serialized via a **v2 trailer** in the same length-delimited
SP86/Task-6 format: the v1 decoder path (slice-1 catalog blobs) reads
the trailer without the new fields and produces `rows_path = None,
pagination = None` — byte-identical decode. A recipe with neither field
encodes byte-identically to a slice-1 recipe ⇒ existing catalogs,
digests, and the seed-7 corpus are unaffected.

### `kessel-proto` — tolerant back-compat decode

`Op::CreateExternalSource` gained two optional wire fields (`rows_path`
and `pagination`) appended after all existing fields. A slice-1-persisted
WAL frame (without the new fields) decodes with `rows_path = None`, `pagination = None` — the back-compat path is exercised by a
hand-built-bytes test. All downstream logic (SM, catalog, router) treats
`None/None` as slice-1 behavior.

### `kessel-sm` — persists new fields; pagination validated pre-backing-type

`Op::CreateExternalSource` now carries the two new fields through to the
`ExternalRecipe`. The pagination compatibility check (see §Compatibility
matrix) runs in `kessel-sql` before the op is submitted — the SM
receives only valid combinations. The validation is a pure pre-check with
no side effects; the irreversible backing-type creation never happens for
an invalid combination.

### `kessel-sql` — grammar extensions

New FORMAT value and optional clauses on `CREATE EXTERNAL SOURCE`:

```sql
FORMAT NDJSON

ROWS '<json-dotted-path>'

PAGE NEXT JSON '<path>'
PAGE NEXT LINK
PAGE CURSOR JSON '<path>' PARAM '<query-param>'
```

The parser extends `compile_stmt` recursively; backward-compatible (new
keywords not reachable from any prior SQL).

**Compatibility matrix** enforced at `CREATE`, before any op is applied:

| Format | Pagination | Verdict |
|---|---|---|
| `JSON` | `PAGE NEXT JSON` or `PAGE CURSOR JSON` | `ROWS` **required** |
| `JSON` or `NDJSON` or `CSV` | `PAGE NEXT LINK` | always valid |
| `NDJSON` | `PAGE NEXT JSON` or `PAGE CURSOR JSON` | **rejected** (no body envelope) |
| `CSV` | `PAGE NEXT JSON` or `PAGE CURSOR JSON` | **rejected** (no body envelope) |
| any | none | single-page fetch (slice-1) |

Errors are typed and actionable (e.g. "use PAGE NEXT LINK or omit PAGE"
for CSV/NDJSON + body-cursor; "ROWS required when FORMAT JSON and body
cursor are combined").

### `kesseldb-server` — one-branch dispatch in `do_refresh`

`do_refresh` now tests whether the loaded `ExternalRecipe` carries a
pagination descriptor. If yes: call `fetch_rows_paginated` (the full
page loop). If no: call the existing `fetch_rows` (slice-1 path). All
downstream logic — deterministic `ObjectId`, upsert `Op::Txn`, atomic
abort — is byte-for-byte unchanged.

---

## Deterministic ObjectId scheme

Unchanged from slice-1:

```
ObjectId = SHA-256( b"extkey:" || be_bytes(key_value) )[0..16]
```

Cross-page duplicate KEY rows resolve through the existing KEY→`ObjectId`
upsert: the last captured page containing that key wins, deterministically,
given the captured page sequence. Because the entire walk is captured once
at the router and replicated as one `Op::Txn`, every replica applies the
identical result.

---

## Captured-once / determinism argument

The multi-page fetch happens **entirely within `fetch_rows_paginated`**,
which is called **once on the router**, and returns the concatenated
`Vec<Vec<Vec<u8>>>`. This value enters `do_refresh` exactly as slice-1's
`fetch_rows` result did. Everything downstream (deterministic `ObjectId`,
upsert `Op::Txn`, exactly-once dedup, all-or-nothing atomic abort,
captured-once → replicate) is byte-for-byte unchanged. The entire fetch
— including all page walks, cursor extraction, and row concatenation —
is invisible to the replicated log: only the final captured rows enter
the `Op::Txn`.

---

## Backward-compatibility design

Two independent compatibility guarantees, each pinned by a
hand-written-bytes test:

**v2 catalog trailer:** A slice-1-persisted catalog blob (without
`rows_path`/`pagination`) decodes on the v2 decoder with `None/None`.
A recipe with both fields `None` encodes byte-identically to a slice-1
recipe (digest/seed-7 unaffected).

**Tolerant proto decode:** A slice-1-persisted WAL frame for
`Op::CreateExternalSource` (without the new optional wire fields)
decodes with `rows_path = None, pagination = None`. A hand-built bytes
test (`proto_back_compat_create_external_source_old_frame_tolerant`)
asserts this decode path explicitly.

---

## Verified test evidence

### `kessel-fetch` — new tests (added to the slice-1 suite)

**Unit/integration (added):**

| Test | What it proves |
|---|---|
| `ndjson::tests::parses_objects_skips_blank_lines` | NDJSON objects + blank-line skip |
| `ndjson::tests::malformed_ndjson_line_is_parse_error` | non-object line → `FetchError::Parse` |
| `json::tests::rows_at_extracts_envelope` | `rows_at` — dotted ROWS path |
| `json::tests::opt_string_at_cursor_forms` | `opt_string_at` — absent/null/empty/present |
| `paginate_stub::next_url_json_paginates_3_pages` | body-path next-URL, stop on absent |
| `paginate_stub::next_link_header_paginates_3_pages` | Link header, stop on absent |
| `paginate_stub::cursor_json_param_paginates_3_pages` | token→`?qp=`, stop on empty |
| `paginate_stub::loop_detection_returns_error` | repeated next-URL → error |
| `paginate_stub::max_pages_cap_returns_error` | exceed `MAX_PAGES` → error |
| `paginate_stub::aggregate_bytes_cap_returns_error` | exceed `MAX_TOTAL_BODY` → error |
| `paginate_stub::ndjson_with_link_header_paginates` | NDJSON + `PAGE NEXT LINK` |

### `kessel-catalog` — v2/v1 backward-compat hand-written-bytes test

`catalog_external_recipe_v2_backward_compat_hand_built_bytes`:
- A hand-written byte vector matching a slice-1 catalog blob (no new
  fields) decodes to `rows_path = None, pagination = None`.
- A recipe with `rows_path` + each of the three pagination variants
  round-trips (encode → decode → same struct).
- A recipe with `None/None` encodes to the same byte sequence as a
  slice-1 recipe.

### `kessel-proto` — tolerant back-compat hand-built-frame test

`proto_back_compat_create_external_source_old_frame_tolerant`:
A hand-built byte frame for a slice-1 `Op::CreateExternalSource` (no
`rows_path`/`pagination` fields) decodes with `rows_path = None,
pagination = None`.

### `kessel-sql` — parse tests + 4 compatibility-matrix rejections

`parse_create_external_source_ndjson`:
- `FORMAT NDJSON` → `format = 2`; no pagination → single-page.

`parse_create_external_source_rows_and_pagination`:
- `ROWS 'data.items'` + `PAGE NEXT JSON 'paging.next'`.
- `ROWS 'results'` + `PAGE NEXT LINK`.
- `ROWS 'items'` + `PAGE CURSOR JSON 'meta.cursor' PARAM 'cursor'`.

`parse_create_external_source_compat_matrix_rejections` (4 cases):
- `FORMAT JSON` + `PAGE NEXT JSON` without `ROWS` → `SchemaError`.
- `FORMAT JSON` + `PAGE CURSOR JSON` without `ROWS` → `SchemaError`.
- `FORMAT NDJSON` + `PAGE NEXT JSON` → `SchemaError`.
- `FORMAT CSV` + `PAGE CURSOR JSON` → `SchemaError`.

### `kesseldb-server` — paginated e2e oracle (2 oracle tests; the paginated one proves 3 properties)

`paginated_external_source_oracle` (real TCP cluster + stub HTTP server):

1. `REFRESH` with a paginated source materializes the **union of all
   pages** (independent model comparison: `SELECT * FROM t` == all rows
   from all stub pages).
2. Idempotent re-`REFRESH` with the same upstream data: row set and raw
   blob fingerprint byte-identical (determinism + no duplicate rows).
3. A source that exceeds `MAX_PAGES` returns a `REFRESH` error and
   **prior data is intact** (byte-for-byte unchanged; all-or-nothing).

### Feature-OFF full workspace gate

```
cargo test --workspace --release
```

FAILED = 0 · TOTAL = 245 · `large_seed_corpus_is_deterministic_and_converges` = 1 (ok)

The feature is compiled in but the router's paginated dispatch path and
the oracle test (`#![cfg(feature = "external-sources")]`) are excluded
when the feature flag is off. Feature-ON: `cargo test -p kesseldb-server
--features external-sources` ⇒ 25 lib tests + 2 oracle tests pass (slice-1 + the paginated oracle; the paginated oracle proves 3 properties: union-of-pages, idempotent re-REFRESH, loop ⇒ error + prior data intact).

The deterministic kernel digest is unchanged: the new `ExternalRecipe`
fields are not part of any op's determinism domain; existing type-defs
are unaffected; the seed-7 corpus is green. A feature-OFF build is
byte-identical to the slice-1 build for all non-external ops.

---

## Honest boundaries

- **Captured-once / all-or-nothing.** The entire multi-page walk happens
  once on the router; the concatenated rows enter the replicated log as a
  single atomic `Op::Txn` upsert, identical to slice-1. If any page
  fails (HTTP error, parse error, type coercion, cap exceeded) nothing is
  materialized — the prior data is intact.
- **Fixed safety caps.** `MAX_PAGES = 1000` pages; `MAX_TOTAL_BODY = 8 ×
  DEFAULT_MAX_BODY` aggregate decompressed bytes across all pages;
  `DEFAULT_MAX_BODY` per-page body cap. Exceeding any cap ⇒ `REFRESH`
  error + all-or-nothing abort. Per-source `MAX PAGES` / `MAX BYTES` SQL
  knobs are a deferred micro-follow-on.
- **Loop-detection.** If the extracted next-URL / cursor token exactly
  equals one already seen in this walk ⇒ `FetchError::Parse` ("pagination
  loop detected"), all-or-nothing abort.
- **Compatibility matrix enforced at `CREATE`.** `NDJSON`/`CSV` +
  body-cursor rejected; `FORMAT JSON` + body-cursor requires `ROWS`.
  Surfaces as a `SchemaError` before any op is applied.
- **Cross-page duplicate KEY rows** resolve via the existing KEY→`ObjectId`
  upsert: last captured page wins, deterministically given the captured
  page sequence.
- **Snapshot since last `REFRESH`.** Unchanged from slice-1 — queries
  read the materialized snapshot, never live upstream.
- **HTTP only.** `kessel-fetch` speaks plain HTTP/1.1. TLS in
  `kessel-fetch` is a deferred follow-on; use a TLS-terminating sidecar
  or proxy for HTTPS upstreams.
- **Upsert only.** Rows deleted from the upstream source are not
  automatically removed (no prune). `REFRESH … MODE REPLACE` is a
  deferred follow-on.

---

## Deferred follow-ons

| Item | Status |
|---|---|
| Per-source `MAX PAGES` / `MAX BYTES` SQL knobs | deferred micro-follow-on |
| `Retry-After` / rate-limit backoff | deferred |
| Concurrent page prefetch | deferred |
| Auth refresh mid-pagination | deferred |
| Nested/array-of-array row extraction | deferred |
| CSV body pagination | deferred |
| TLS in `kessel-fetch` | deferred (use a TLS-terminating sidecar) |
| `REFRESH … MODE REPLACE` (prune rows deleted upstream) | deferred |
| Scheduled / automatic refresh (cron-style) | deferred |
