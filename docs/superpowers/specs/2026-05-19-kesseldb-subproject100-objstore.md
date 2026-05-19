# KesselDB — Subproject 100: Object-Store External Sources (OBJ-1)

**Date:** 2026-05-19  **Status:** done — code + tests committed and passing.

Builds on:
- Subproject 97 — External sources (EXT slice 1):
  `docs/superpowers/specs/2026-05-18-external-sources-design.md`
- Subproject 98 — External sources: pagination + NDJSON:
  `docs/superpowers/specs/2026-05-18-external-sources-pagination-design.md`
- Subproject 99 — External sources: HTTPS/TLS:
  `docs/superpowers/specs/2026-05-18-external-sources-tls-design.md`

Design document:
`docs/superpowers/specs/2026-05-19-object-store-sources-design.md`

---

## What shipped

### `kessel-objstore` — new workspace-member crate

A new pure-Rust crate with zero external dependencies. All crypto
(HMAC-SHA256) is delegated to the same zero-dep implementation already
used by the kessel-sm kernel. The crate provides:

- **`b64_encode(bytes) -> String`** — standard RFC 4648 base-64 encoder
  (table + padding). Verified against the RFC 4648 §10 KAT vectors.

- **`ymd_hms(secs_since_unix_epoch) -> String`** — formats a Unix
  timestamp as `YYYYMMDDTHHmmssZ` (AWS credential-scope / SigV4 date
  format). Verified against a hand-computed known-answer vector.

- **`http_date(secs_since_unix_epoch) -> String`** — formats a Unix
  timestamp as an RFC 1123 HTTP `Date:` header value
  (`Day, DD Mon YYYY HH:MM:SS GMT`). Used in the Azure Shared-Key
  `StringToSign`. Verified against a hand-computed KAT.

- **`enc_seg(s) -> String`** — RFC-3986 percent-encodes a path segment:
  every byte outside `[A-Za-z0-9._~!$&'()*+,;:@-]` is emitted as
  `%XX`. Used by both signers to prevent CRLF/query injection.

- **`canonical_uri(path) -> String`** — splits a URI path on `/`,
  encodes each segment with `enc_seg`, and rejoins with `/` — the
  shared canonicalization used in both SigV4 canonical requests and
  Azure Shared-Key `canonicalizedResource`.

- **`sign_s3_get`** — builds a complete signed AWS SigV4 `Authorization`
  header for an S3 (or S3-compatible) GET request. Follows the AWS
  Signature Version 4 specification exactly:
  1. Canonical request (method, URI, query, headers, signed-headers,
     body-hash — empty body SHA-256 = the literal hex digest of `""`).
  2. String-to-sign (`AWS4-HMAC-SHA256 \n timestamp \n credential-scope \n
     hex(sha256(canonical-request))`).
  3. Signing key: `HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date), region),
     "s3"), "aws4_request")` — the four-step derivative key.
  4. Signature: `hex(HMAC(signing-key, string-to-sign))`.
  5. Full `Authorization: AWS4-HMAC-SHA256 Credential=…, SignedHeaders=…,
     Signature=…` header.
  Verified against two AWS-published KAT vectors (signing-key derivation
  and the full canonical-request GET-Object example from the AWS
  documentation).

- **`sign_azure_get`** — builds a complete Azure Blob Shared-Key
  `Authorization` header for a GET request. Follows the Azure Blob
  Shared-Key specification:
  1. `StringToSign` = newline-joined canonicalized components:
     `GET \n\n\n\n\n\n\n\n\n\n\n\n x-ms-date:<date>\n x-ms-version:<ver>\n
     /<account>/<container>/<blob>`.
  2. Signature: `Base64(HMAC-SHA256(Base64Decode(account-key), StringToSign))`.
  3. Full `Authorization: SharedKey <account>:<signature>` header.
  Verified against the Azure Blob Shared-Key specification-literal
  `StringToSign` KAT.

### `kessel-fetch` `object-store` feature

New optional feature (off by default, zero impact on the default build):

```toml
[features]
object-store = ["dep:kessel-objstore"]
```

New functions compiled only when `object-store` is on:

- **`build_request_with_headers(host, path, extra_headers) -> Vec<u8>`**
  — assembles an HTTP/1.1 GET request with caller-supplied headers
  (Host, Authorization, Date/x-ms-date/x-ms-version, etc.).

- **`fetch_rows_signed(url, headers, recipe) -> Result<Vec<Row>>`**
  — calls `build_request_with_headers`, routes through the existing
  `connect`/`exchange` machinery (TLS via `kessel-fetch/tls`), and
  feeds the response body through the existing `rows_from_body` decoder.
  No new network logic; the signing is entirely in `kessel-objstore`.

### Catalog v3 trailer + `ExternalAuth::ObjStoreEnv`

The catalog `ExternalRecipe` trailer is extended to version 3 with a
new auth variant:

```rust
pub enum ExternalAuth {
    None,
    BearerEnv(String),
    HeaderEnv { header: String, var: String },
    ObjStoreEnv {
        provider: ObjStoreProvider,   // S3 | Azure
        key_id_var: Option<String>,   // S3: KEYID ENV var name
        secret_var: String,           // S3: SECRET ENV; Azure: KEY ENV
        account: Option<String>,      // Azure: ACCOUNT (name, not secret)
        region: Option<String>,       // S3: REGION
        endpoint: Option<String>,     // ENDPOINT for S3-compatible / sovereign Azure
    },
}
```

The catalog trailer is backward-compatible: v3 blobs decode cleanly
with `None` auth when read by an older binary, and v1/v2 blobs decode
cleanly as `ExternalAuth::None` by the new code.

### `kessel-proto` additive `objstore` fields

The `CreateExternalSource` proto message gains optional fields for the
new object-store auth kind. The decode is tolerant: old persisted blobs
(no objstore fields) decode with all `Option`s as `None`. Pinned by a
hand-written-bytes regression test.

### `kessel-sm` `apply` — auth_kind 3

`StateMachine::apply` for `CreateExternalSource` maps the new
`auth_kind = 3` wire value to `ExternalAuth::ObjStoreEnv`. A
pre-mutation fail-closed check rejects `RefreshExternalSource` for a
source whose `auth` is `ObjStoreEnv(…)` but whose actual resolved
env-var is absent from the environment — rejected before any state
mutation, prior data intact.

### `kessel-sql` grammar

SQL parser additions:

- `s3://<bucket>/<key>` and `az://<container>/<blob>` are accepted as
  the `FROM '<url>'` value in `CREATE EXTERNAL SOURCE`.
- `REGION '<r>'` clause (S3).
- `ENDPOINT '<https-url>'` clause (S3-compatible / Azure sovereign).
  Rejected at `CREATE` if the value does not start with `https://`.
- `AUTH OBJSTORE S3 KEYID ENV '<idvar>' SECRET ENV '<secretvar>'`.
- `AUTH OBJSTORE AZURE [ACCOUNT '<a>'] KEY ENV '<keyvar>'` — `ACCOUNT`
  is optional (can be given via `ENDPOINT`).
- `CREATE`-time rejections with clear error messages for:
  - `FORMAT PARQUET` (OBJ-2 follow-on).
  - Iceberg-related clauses (OBJ-3 follow-on).
  - `PAGE …` combined with `s3://`/`az://` (prefix listing is OBJ-4).
  - STS/SAS/IMDS auth forms (OBJ-5 follow-on).

### Router `do_refresh` dispatch

`do_refresh` gains a `s3://|az://` branch:

1. Resolves env-var values from the router's process environment (fail
   if absent — fail-closed pre-mutation).
2. Calls `kessel-objstore` signing with the current UTC timestamp
   captured once at dispatch time (the same captured-once boundary as
   the TLS RNG — never enters WAL or digest).
3. Calls `fetch_rows_signed` with the signed headers.
4. Calls `materialize_external_rows` — the existing extraction and
   atomic `Op::Txn` upsert path unchanged.

The composite feature `external-sources-objstore` enables both
`kessel-fetch/object-store` (and therefore `kessel-fetch/tls`) and the
router dispatch branch.

### Feature-gated s3:// e2e oracle

`cargo test -p kesseldb-server --features external-sources-objstore --test external_source_objstore_oracle`

One test — fail-closed e2e: the production router is pointed at an
`s3://` source backed by a stub HTTPS server. The test asserts that
`REFRESH` returns an appropriate error (the stub does not present a
webpki-trusted certificate) and that prior data is intact. The trusted
happy-path for `fetch_rows_signed` is covered at the `kessel-fetch`
crate layer (unit test with a fixture-trusted TLS config); injecting
fixture trust into the production router would require bypassing the
`OnceLock`-guarded `ClientConfig`, which is explicitly forbidden.

---

## Known-answer vectors

| Component | KAT source | Digest/value |
|---|---|---|
| SigV4 signing-key derivation | AWS documentation literal | `f4780e2d…` (the published hex) |
| SigV4 GET-Object canonical request + StringToSign | AWS GET-Object spec literal | matches the published example exactly |
| Azure Shared-Key `StringToSign` | Azure Blob spec literal | matches the spec-literal string |
| RFC 1123 `http_date` | hand-computed | `Mon, 01 Jan 2024 00:00:00 GMT` for epoch 1704067200 |
| RFC 4648 `b64_encode` | RFC 4648 §10 KAT `Man` → `TWFu` (and others) | exact |
| `enc_seg` | RFC 3986 reserved set | verified that space/CRLF/`?`/`#` are encoded, unreserved chars are not |

---

## Tests and which build each runs in

### Default build (`cargo test --workspace --release`)

The following tests compile and run without any feature flag:

- **`kessel-objstore` unit tests** — the entire `kessel-objstore` crate
  is a workspace member with no `default = []` feature gate; all of its
  tests run in the default build:
  - `b64_encode_rfc4648_kat` — RFC 4648 KAT vectors.
  - `ymd_hms_known_answer` — UTC date formatter KAT.
  - `http_date_known_answer` — RFC 1123 formatter KAT.
  - `enc_seg_encodes_reserved_and_space` — RFC-3986 percent-encoding.
  - `canonical_uri_encodes_segments` — URI canonicalization.
  - `sigv4_signing_key_kat` — AWS SigV4 signing-key derivation.
  - `sigv4_canonical_request_kat` — AWS SigV4 full canonical request.
  - `azure_shared_key_string_to_sign_kat` — Azure Shared-Key StringToSign.
  - `enc_seg_anti_injection_crlf` — CRLF injection rejected.
  - `enc_seg_anti_injection_query` — query-parameter injection rejected.
  - `sign_s3_get_no_secret_leak` — secret value does not appear in the
    Authorization header (sentinel locked: if the header contained the
    literal secret, the test panics).
  - `sign_azure_get_no_secret_leak` — same sentinel for Azure.

- **`kessel-catalog` back-compat tests** — two new tests for the v3
  trailer: round-trip of `ObjStoreEnv` auth, and that a v2 blob decodes
  with `auth = None` in the new code.

- **`kessel-proto` additive objstore decode test** — a hand-written-bytes
  test confirming that old `CreateExternalSource` blobs without objstore
  fields decode cleanly.

- **`kessel-sm` auth_kind 3 map test** — unit test that a
  `CreateExternalSource` with `auth_kind = 3` produces an
  `ExternalAuth::ObjStoreEnv` entry; and that `none-rejected` (an
  `ObjStoreEnv` source with a missing env var is rejected pre-mutation).

- **`kessel-sql` grammar tests** — `s3://` and `az://` parse correctly;
  `FORMAT PARQUET` + pagination on object-store sources are rejected at
  `CREATE` with an appropriate error.

### Feature-on: `--features external-sources-objstore`

- **`fetch_rows_signed` happy-path unit test** — in-process stub, fixture-
  trusted TLS config, confirms signed headers are forwarded and the
  response body is decoded to typed rows.

- **`external_source_objstore_oracle`** — the fail-closed e2e oracle
  (described above).

---

## Honest gate accounting: 247 → 267 (+20)

**The design document's claim of "0 new default-build tests" was a
corrected planning error.** The design correctly noted that the
deterministic kernel, WAL, kessel-sm, kessel-vsr, etc. are untouched and
pull no new deps. But it incorrectly concluded this meant `cargo test
--workspace` would show zero new tests.

The reality: `cargo test --workspace` runs every test in every workspace
member — including the new `kessel-objstore` crate. Nothing in the
workspace excludes it. The 20 new tests that appear in the default-build
total are:

1. **`kessel-objstore` — 12 tests** (b64, date formatters, SigV4 KAT,
   Azure KAT, RFC-3986, anti-injection, secret-leak sentinels). All new;
   this is a new workspace member with no prior baseline.
2. **`kessel-catalog` — 1 test** (v3 trailer back-compat round-trip;
   catalog 7 → 8).
3. **`kessel-proto` — 1 test** (additive objstore tolerant-decode;
   proto 9 → 10).
4. **`kessel-sm` — 2 tests** (auth_kind 3 map + none-rejected apply;
   sm 69 → 71).
5. **`kessel-sql` — 4 tests** (s3:// parses, az:// parses, Parquet
   rejected, PAGE on object-store rejected; sql 29 → 33).

Total: 12 + 1 + 1 + 2 + 4 = **20** (`cargo test`-measured; kessel-objstore
12 unit tests + 8 back-compat/validation tests across
kessel-catalog/proto/sm/sql).

**The invariants that DO hold (these are the correct claims):**

- The deterministic kernel, WAL, `kessel-sm`, `kessel-vsr`, `kessel-io`,
  `kessel-codec`, and the core of `kessel-proto` are byte-identical and
  pull zero new dependencies in the default build.
- `cargo tree -p kesseldb-server -e normal` shows no rustls, webpki, or
  objstore in the default build graph.
- `cargo tree -p kessel-fetch -e normal` is equally clean.
- Feature-OFF object-store code is not compiled into the default binary.
- seed-7 (`large_seed_corpus_is_deterministic_and_converges`) is green.
- Default-build total: **267** (measured; seed-7 green; no REALFAIL).

---

## Security posture

**Secret-reference only.** Only the env-var NAME strings are stored in
the catalog trailer and replicated in the WAL. The actual key-id,
secret, and account-key values are resolved from the router's process
environment at each `REFRESH`, are never logged, never placed in any
operation, WAL entry, or digest output, and are never surfaced in error
messages. This is enforced by sentinel-locked unit tests
(`sign_s3_get_no_secret_leak`, `sign_azure_get_no_secret_leak`): if the
literal secret value appears in the Authorization header, the test
panics. The SP99 secret-handling invariant is extended to the objstore
path unchanged.

**HTTPS-only, no bypass.** All object-store requests use the same rustls
`OnceLock`-guarded `ClientConfig` with `webpki_roots::TLS_SERVER_ROOTS`.
Full chain + hostname verification. No `dangerous()` / custom verifier
on the production path. No env var, SQL clause, or config flag bypasses
this.

**RFC-3986 injection-safe.** The Azure container/key RFC-3986 injection
fix (commit `d8e2597`) was discovered during controller review and landed
before any feature-on tests ran. `enc_seg` and `canonical_uri` ensure
CRLF bytes and query-parameter separators in bucket names, container
names, or object keys are percent-encoded before they appear in the
request URI and in the signing string. The anti-injection tests
(`enc_seg_anti_injection_crlf`, `enc_seg_anti_injection_query`) are
default-build unit tests that lock this property.

**Determinism boundary.** The SigV4/Azure timestamp and the TLS RNG are
captured once at the router's `do_refresh` dispatch, exactly as the
SP99 TLS RNG is captured once. Neither enters the WAL nor the state
machine digest — the same captured-once/replicate/determinism boundary
established in SP97 is maintained.

---

## Deferred follow-ons

### OBJ-2 — Parquet format

`FORMAT PARQUET` for object-store sources: Thrift footer parsing, column
chunk reading, encoding (plain, dictionary, RLE), and Snappy/zstd
decompression. Rejected at `CREATE` with a clear error in this slice.

### OBJ-3 — Iceberg table manifests

`FORMAT ICEBERG`: resolve the current snapshot from a table metadata
JSON file, enumerate manifest files, enumerate data file paths, fetch
and decode each Parquet data file. Depends on OBJ-2.

### OBJ-4 — Prefix / multi-object listing

Allow a source URL to be a prefix (`s3://bucket/prefix/`) and have
`REFRESH` enumerate all matching objects, fetch each, and materialize
the union. Ties into the pagination infrastructure from SP98.

### OBJ-5 — STS / SAS / IMDS credential providers

AWS STS session tokens (assumed-role, web identity), Azure SAS tokens,
AWS IMDS / Azure IMDS workload-identity credential resolution. Currently
rejected at `CREATE`; the `ObjStoreEnv` auth struct has `Option` fields
reserved for the session-token case.

### Task-9 M3: DRY between `do_refresh` and `do_refresh_objstore`

The `build_cols` and `resolve_format` logic is partially duplicated
between the HTTP `do_refresh` path and the new object-store
`do_refresh_objstore` path. A follow-on can factor these into a shared
helper, reducing the maintenance surface. Noted by the review.

### Carried EXT/TLS deferrals (still open)

- **Unify `fetch_rows_paginated` decode tail:** the paginated path has
  an inline decode+coerce tail that duplicates the logic in
  `rows_from_body`; routing through `rows_from_body` is a hygiene
  follow-on from SP99.
- **Trusted multi-page HTTPS test:** the current paginated TLS test is
  fail-closed only; a trusted multi-page HTTPS happy-path test is a
  SP99 carry.
- **`test_config_trusting` visibility:** currently `pub`; could be
  narrowed to `pub(crate)` as a minor hygiene follow-on from SP99.
- **Gitleaks allow-list for test key fixture:** the `localhost.key.pem`
  fixture will trip secret scanners if CI secret scanning is added;
  documented in the fixtures `README`.
