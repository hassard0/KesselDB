# KesselDB — External Sources: pagination + NDJSON (follow-on slice): design

**Date:** 2026-05-18  **Status:** design approved, pre-implementation.

A follow-on to the shipped External Sources feature (design:
`docs/superpowers/specs/2026-05-18-external-sources-design.md`;
internal record: `…-subproject97-external-sources.md`). It adds
**NDJSON** parsing and **cursor/next-URL pagination** so a single
`REFRESH` can materialize a multi-page source — while preserving
every slice-1 invariant (captured-once at the router, one atomic
upsert `Txn`, deterministic, off-by-default, kernel/seed-7
untouched).

## 1. Architecture & invariants

The deterministic kernel is unchanged. All new logic lives in the
optional `kessel-fetch` crate + the backward-compatible catalog
recipe + the `kessel-sql` grammar + a one-line dispatch in the
router's `do_refresh`. **Approach A:** a new self-contained
`kessel_fetch::fetch_rows_paginated` fully encapsulates the page
loop, safety caps, cursor extraction, and per-page parse, and
returns the **exact same `Vec<Vec<Vec<u8>>>` shape** `fetch_rows`
already returns. `do_refresh` changes by one branch only: if the
recipe carries a pagination descriptor, call the paginated variant;
otherwise call the existing `fetch_rows`. Everything downstream
(deterministic `ObjectId`, upsert `Op::Txn`, exactly-once `dedup`,
all-or-nothing atomic abort, captured-once → replicate) is
**byte-for-byte unchanged**. The entire multi-page fetch happens
once, at the router; only the concatenated captured rows enter the
replicated log. Feature remains `external-sources`, default off.

Rejected alternatives: router-driven page loop (spreads pagination
into kesseldb-server, worse cohesion, no gain — we concat anyway);
a streaming "source iterator" abstraction (over-engineered — there
is no streaming consumer; rows are materialized+concatenated
regardless).

## 2. Recipe / SQL surface (additive, backward-compatible)

`FORMAT` is extended to three values: `JSON` (top-level array of
objects — slice-1 behavior) | `CSV` (slice-1) | **`NDJSON`**
(one JSON object per line).

New optional clauses on `CREATE EXTERNAL SOURCE … FROM '<url>'
FORMAT … KEY … [AUTH …]`:

- `ROWS '<json-path>'` — dotted path to the array of row-objects
  inside an envelope object. Absent ⇒ the response is a top-level
  array (slice-1 behavior).
- exactly one pagination clause (absent ⇒ single fetch = slice-1):
  - `PAGE NEXT JSON '<path>'` — `<path>` in the envelope yields the
    **absolute next-page URL**; GET it next.
  - `PAGE NEXT LINK` — use the URL from the response
    `Link: …; rel="next"` header.
  - `PAGE CURSOR JSON '<path>' PARAM '<qp>'` — `<path>` yields an
    **opaque token**; each subsequent request is the **original
    recipe URL** with query parameter `<qp>` set to the latest
    token (replacing any pre-existing `<qp>` in that URL).
  - Stop when the declared next value is **absent, JSON null, or an
    empty string**.

`kessel_catalog::ExternalRecipe` gains two optional fields —
`rows_path: Option<String>` and `pagination: Option<Pagination>`
(an enum mirroring the three clauses + the param name) — serialized
in the **same SP86/Task-6 backward-compatible catalog trailer**.
A recipe with neither field encodes byte-identically to a slice-1
recipe ⇒ existing catalogs/digests/seed-7 corpus are unaffected.
`kessel-proto`'s `Op::CreateExternalSource` gains the matching
optional wire fields (additive, after the existing fields, decoded
absent ⇒ slice-1 behavior).

## 3. Compatibility matrix (enforced at `CREATE`, typed error)

- `FORMAT JSON` + `PAGE NEXT JSON` or `PAGE CURSOR JSON` ⇒ **`ROWS`
  is required** (both the row array and the body cursor are read
  from the same envelope object).
- `PAGE NEXT LINK` ⇒ valid with a top-level array, a `ROWS` path,
  **or** `FORMAT NDJSON` (the cursor is out-of-band in the header).
- `FORMAT NDJSON` + a body/token cursor (`PAGE NEXT JSON` /
  `PAGE CURSOR JSON`) ⇒ **rejected at `CREATE`** with a clear,
  actionable error (an NDJSON stream has no envelope object to
  carry a body cursor).
- `FORMAT CSV` + any pagination ⇒ slice-1 supports `PAGE NEXT LINK`
  only (CSV has no body to read a cursor from); body/token cursor
  on CSV ⇒ rejected at `CREATE`. (CSV pagination is niche; Link is
  the only coherent form.)

Validation runs in `kessel-sql` at parse/compile of
`CREATE EXTERNAL SOURCE` so the error surfaces immediately, before
any op is applied.

## 4. Fetch loop & safety bounds (`kessel-fetch::fetch_rows_paginated`)

Loop, per page: resolve the page URL (page 1 = base URL; later =
the extracted next URL, or base+`?<qp>=<token>`) → `http::get`
(existing per-page body cap `DEFAULT_MAX_BODY`) → enforce the
**aggregate-bytes cap** and **max-pages cap**; **loop-detection**:
if the next URL/cursor exactly equals one already seen ⇒ error →
parse this page's rows (top-level array | `ROWS` path | NDJSON
lines) and coerce per the column map (reusing slice-1
`json`/`csv`/`coerce`) → extract the next pointer (body `<path>` |
`Link` header | token→`<qp>`) → repeat until the next pointer is
absent/null/empty → return the **concatenation of all pages' rows**
in `Vec<Vec<Vec<u8>>>` (same shape/contract as `fetch_rows`).

Caps in slice-1 are **fixed named constants** in `kessel-fetch`:
`MAX_PAGES = 1000` and `MAX_TOTAL_BODY = 8 * DEFAULT_MAX_BODY`
(aggregate decompressed body across all pages); the existing
per-page `DEFAULT_MAX_BODY` still applies to each request.
Per-source `MAX PAGES` / `MAX BYTES` SQL knobs are an explicit
deferred micro-follow-on.
Any error (HTTP non-2xx, parse, coercion, body cap, aggregate cap,
max-pages, loop detected, declared-incompatible reached at runtime)
⇒ `Err(FetchError)` ⇒ `do_refresh` returns the typed error and
submits **nothing** — slice-1's all-or-nothing atomic abort,
unchanged.

NDJSON parsing is a new small `kessel-fetch` module: split on
newlines, skip blank lines, parse each non-blank line as one JSON
object via the existing JSON value parser, extract the declared
columns via the same dotted-path logic as the array path.

## 5. Determinism & ordering boundary (documented)

The page sequence is the upstream API's order. Cross-page duplicate
KEY values resolve through the **existing KEY→`ObjectId` upsert**:
the last captured page containing that key wins, deterministically,
given the captured page sequence. Because the whole walk is
captured once at the router and replicated as one `Txn`, every
replica applies the identical result. Boundaries remain exactly as
slice-1: snapshot-since-last-`REFRESH` (never live), HTTP and HTTPS
(`https://` via the optional `--features external-sources-tls` build,
shipped in subproject 99 — see
`docs/superpowers/specs/2026-05-18-kesseldb-subproject99-ext-tls.md`;
TLS-terminating sidecar now optional), materialize+replicate,
captured-once, upsert-only (no upstream-delete prune).

## 6. Testing

- `kessel-fetch` unit/integration: NDJSON parser (objects, blank
  lines, malformed line ⇒ typed error); a multi-page localhost stub
  HTTP server exercising **each cursor form** (body-path next-URL,
  `Link` header, token→param), `ROWS`-path extraction, NDJSON +
  `Link`, stop conditions (absent/null/empty), and **every cap**
  (per-page body, aggregate bytes, max-pages, loop-detection) ⇒
  typed error.
- `kessel-sql`: parse tests for `FORMAT NDJSON`, `ROWS`, the three
  `PAGE` clauses, and the compatibility-matrix rejections.
- Catalog: round-trip of a recipe with `rows_path`+`pagination`
  (all three pagination variants) **and** the byte-identical
  backward-compat assertion (no new fields ⇒ slice-1 bytes).
- End-to-end oracle (feature-on) extending
  `external_source_oracle`: a paginated stub source ⇒
  `SELECT *` == the independent union model across pages;
  idempotent re-`REFRESH` (state byte-identical); a
  cap-exceeding source ⇒ `REFRESH` error **and** prior data
  intact.
- Feature-off: full workspace gate unchanged (FAILED=0, the
  established TOTAL, `large_seed_corpus_…` green) — the crate isn't
  compiled by default.

## 7. Slice scope vs. follow-ons

**In:** `FORMAT NDJSON`; the three cursor forms; the `ROWS` path;
the CREATE-time compatibility matrix; fixed safety caps +
loop-detection with all-or-nothing error; recipe/catalog +
`kessel-proto` + `kessel-sql` + `do_refresh` wiring; the oracle;
docs (codename-free public; internal slice record).

**Deferred (each its own micro-slice):** per-source `MAX PAGES` /
`MAX BYTES` SQL knobs; `Retry-After`/rate-limit backoff;
concurrent page prefetch; auth refresh mid-pagination;
nested/array-of-array row extraction; CSV body pagination.

## Non-goals (explicit)

Streaming/incremental materialization (we always concat then one
atomic `Txn`); live per-query pagination (still
snapshot-since-`REFRESH`); pagination over CSV body cursors;
unbounded fetch (hard caps, no "just keep going"); schema inference
(unchanged from slice-1).
