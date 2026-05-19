# KesselDB — Object-Store External Sources (OBJ slice 1): design

**Date:** 2026-05-19  **Status:** design approved under the standing
KesselDB autonomous-build mandate (user asleep; decisions made
deliberately and documented here per
`feedback_kesseldb_autonomous_build`). Pre-implementation.

A follow-on to the External Sources line (subproject97 EXT slice-1,
subproject98 pagination, subproject99 HTTPS/TLS). It lets an external
source read its bytes **directly from S3-compatible or Azure Blob
object storage** — `CREATE EXTERNAL SOURCE … FROM 's3://…' | 'az://…'`
— by resolving the object to a **signed HTTPS GET** at the router and
feeding the object body through the *already shipped* fetch → decode →
capture-once → atomic `Op::Txn` → replicate → materialize pipeline.
This is the foundational OBJ piece: object storage becomes a
first-class source using everything already built, with the only hard
new logic being correct request signing.

## 0. Scope & decomposition (OBJ is large — this slice is OBJ-1 only)

- **OBJ-1 (this slice):** object-store GET as an external-source
  transport for the **existing formats** (`JSON` / `CSV` / `NDJSON`).
  AWS Signature V4 (S3 + S3-compatible: MinIO/R2/Ceph via
  `ENDPOINT`/path-style) and Azure Blob **Shared Key** signing. Single
  object per source.
- **OBJ-2 (follow-on):** a columnar **Parquet** reader (`FORMAT
  PARQUET`) — its own slice (Thrift footer, column chunks, encodings,
  Snappy/zstd) — explicitly **not** in OBJ-1.
- **OBJ-3 (follow-on):** Iceberg table-metadata / manifest traversal
  (`FORMAT ICEBERG`, resolve current snapshot → data files).
- **OBJ-4 (follow-on):** prefix/`directory` listing → multi-object
  source (ties into pagination/`LIST`).
- **OBJ-5 (follow-on):** AWS STS/session-token, Azure SAS, IMDS/
  workload-identity credential providers.

Decomposition rationale: a single Parquet/Iceberg implementation is a
multi-slice effort on its own; OBJ-1 delivers end-to-end "read your
data straight out of S3/R2/MinIO/Azure" value now and is the
substrate every later OBJ slice builds on.

## 1. Architecture & invariants

The deterministic kernel, WAL, `kessel-sm`, `kessel-vsr`,
`kessel-io`, `kessel-codec`, and the core of `kessel-proto` are
**untouched**. New logic lives in a new optional crate
`kessel-objstore`, a new optional `kessel-fetch` feature, a
backward-compatible catalog recipe extension, additive `kessel-proto`
`CreateExternalSource` fields, `kessel-sql` grammar, and a one-branch
dispatch in the router's `do_refresh`. Feature OFF ⇒ the default
workspace build is byte-identical, pulls zero new dependencies, and
the seed-7 corpus is untouched **by construction** (none of these
crates/paths are compiled).

Resolution path (router-side, in `do_refresh`, mirroring SP97/99
exactly):

1. Parse the recipe URL scheme. `http(s)://` → the existing
   `fetch_rows`/`fetch_rows_paginated` path (unchanged). `s3://` or
   `az://` → the OBJ path.
2. Resolve credentials **router-side** from the env-var **names** in
   the recipe (never values in the recipe/op/WAL/log/digest — the
   SP97 security constraint, extended verbatim).
3. `kessel_objstore::sign_get(provider, host_parts, key, region,
   endpoint, &creds, now) -> (https_url: String, headers:
   Vec<(String,String)>)` — pure function given `now`; produces the
   canonical request and the signed `Authorization` (+ `x-amz-date` /
   `x-ms-date` / `x-amz-content-sha256` / `host` etc.) headers.
4. `kessel_fetch::fetch_rows_signed(&https_url, &headers, format,
   cols, rows_path, max_body)` — builds the HTTP/1.1 GET with the
   supplied signed headers and runs the **existing** hardened
   `exchange` + `rows_from_body` over the **existing** rustls
   transport (object storage is HTTPS-only).
5. Everything downstream — deterministic `ObjectId = sha256(b"kessel
   -ext-id\0" ++ type_id_le ++ KEY_raw)[..16]`, atomic upsert
   `Op::Txn`, exactly-once `dedup`, all-or-nothing abort, captured
   -once → replicate — is **byte-for-byte the EXT path**, unchanged.

**Determinism boundary (unchanged from SP97/99):** SigV4/Shared-Key
embed a wall-clock timestamp (`x-amz-date`); the TLS handshake uses an
RNG. Both are router-side network I/O, **captured once** and
replicated as a single `Op::Txn`. The signing timestamp and TLS bytes
never enter the WAL, digest, or seed-7 corpus. Every replica applies
the identical captured rows.

Rejected alternatives: (a) a generic "object store" abstraction layer
with pluggable backends — YAGNI for two providers, over-engineered;
(b) signing inside `kessel-fetch` — wrong cohesion (fetch is
transport; signing is provider auth); (c) shelling out to `aws`/`az`
CLIs — violates zero-dep ethos and determinism, and is not
self-contained.

## 2. Crate & feature gating

New crate `crates/kessel-objstore/` — pure Rust, **only** dependency
`kessel-crypto` (already in-tree, zero-dep; exposes `sha256`,
`hmac_sha256`, `hex` — exactly the SigV4/Shared-Key primitives, so
**no new external dependency anywhere**).

`crates/kessel-fetch/Cargo.toml`:

```toml
kessel-objstore = { path = "../kessel-objstore", optional = true }

[features]
# object storage is HTTPS-only ⇒ implies the tls transport
object-store = ["tls", "dep:kessel-objstore"]
```

`crates/kesseldb-server/Cargo.toml` (mirrors the `external-sources
-tls` composite from SP99):

```toml
external-sources-objstore = ["external-sources", "kessel-fetch/object-store", "dep:rustls", "dep:rustls-pemfile"]
```

`external-sources` and `external-sources-tls` are **unchanged**;
`default = []` unchanged. `kessel-fetch`'s default build still
compiles (all OBJ code is `#[cfg(feature = "object-store")]`). The
determinism-gate build (`cargo test --workspace --release`, default
features) compiles none of it.

## 3. `kessel-objstore` — signing (the only hard new logic)

Pure, deterministic given `now`. No I/O. Files kept small and
single-purpose:

- `src/lib.rs` — `pub fn sign_get(req: ObjGetRequest, now:
  DateTime) -> Result<SignedRequest, ObjError>`; `pub struct
  ObjGetRequest { provider, bucket_or_container, key, region:
  Option<String>, endpoint: Option<String>, creds: ObjCreds }`;
  `pub enum ObjCreds { S3 { key_id, secret }, AzureSharedKey {
  account, key_b64 } }`; `pub struct SignedRequest { https_url:
  String, headers: Vec<(String,String)> }`; `pub enum ObjError
  { BadUrl, BadEndpoint, Cred, Encoding }`. `now` is passed in
  (a tiny `pub struct DateTime { secs_since_epoch: u64 }` with a
  pure `fn amz_basic()`/`fn http_date()` formatter — no `chrono`,
  no `std::time` call inside the signer; the caller passes the
  clock so the signer is unit-testable against fixed vectors).
- `src/sigv4.rs` — AWS Signature Version 4 for GET: canonical
  request → string-to-sign → signing key (`HMAC` chain
  `AWS4`+secret → date → region → `s3` → `aws4_request`) →
  `Authorization` header. Payload hash for a GET = the well-known
  `UNSIGNED-PAYLOAD` is **not** used; we send
  `x-amz-content-sha256: e3b0c4…` (SHA-256 of empty body) so the
  request is fully signed. Virtual-hosted
  (`https://bucket.s3.<region>.amazonaws.com/key`) by default;
  **path-style** (`https://<endpoint>/bucket/key`) when `ENDPOINT`
  is set (MinIO/Ceph/R2). RFC-3986 path/query encoding done here.
- `src/azure.rs` — Azure Blob **Shared Key** GET:
  `https://<account>.blob.core.windows.net/<container>/<blob>`
  (or `ENDPOINT` override), `x-ms-date`, `x-ms-version`,
  canonicalized headers + resource, `Authorization: SharedKey
  account:base64(HMAC-SHA256(key, string-to-sign))`. Minimal
  base64 decode of the account key + encode of the MAC (small
  pure-Rust helper in `src/b64.rs`).

`kessel-crypto` provides `sha256`/`hmac_sha256`/`hex`; OBJ adds only
the base64 helper and the canonicalization string-building.

## 4. `kessel-fetch::fetch_rows_signed` (thin, reuses everything)

```rust
#[cfg(feature = "object-store")]
pub fn fetch_rows_signed(
    https_url: &str,
    headers: &[(String, String)],
    format: Format,
    cols: &[ColumnMap],
    rows_path: Option<&str>,
    max_body: u64,
) -> Result<Vec<Vec<Vec<u8>>>, FetchError>
```

It parses the (always `https://`) URL via the existing
`http::parse_target`, builds the GET request reusing the existing
request-builder but emitting the supplied signed headers verbatim
(a small `build_request_with_headers` extracted from / alongside
`build_request` — single responsibility, no behavior change to the
existing `Auth`-based `build_request`), connects via the **existing**
`tls::connect_tls`, runs the **existing** `http::exchange`, then the
**existing** `rows_from_body`. No new transport, no new decode — OBJ-1
adds *signing + a header-passthrough entrypoint* and nothing else on
the fetch side.

## 5. Recipe / proto / SQL surface (additive, backward-compatible)

`kessel_catalog::ExternalRecipe` gains object-store fields carried in
the **same SP86/SP98 versioned trailer** (a v3 sentinel section;
v1/v2 recipes encode byte-identically as before — pinned by a
hand-written-bytes back-compat test, the load-bearing invariant from
SP98). New `ExternalAuth` variant **`ObjStoreEnv { provider: u8,
a_env: String, b_env: String, account: Option<String> }`**
(provider 1=S3 → a=key-id-env,b=secret-env; 2=Azure → a=account-key
-env, account=storage-account, b unused) — env-var **names only**.
New optional recipe fields `region: Option<String>`,
`endpoint: Option<String>`.

`kessel_proto::Op::CreateExternalSource` gains matching **additive,
optional, tolerant-decode** fields (exactly the SP98 discipline:
absent ⇒ slice-1 behavior; unknown PRESENT tag ⇒ fail decode, never
silent corruption).

`kessel-sql` `CREATE EXTERNAL SOURCE` grammar additions:

- `FROM 's3://bucket/key'` or `FROM 'az://container/blob'`. The Azure
  **storage account is not part of the URL** (it is an identity, not
  a path component): it is supplied by the `AUTH OBJSTORE AZURE
  ACCOUNT '<acct>'` clause (or derived from a custom `ENDPOINT`).
  This keeps `az://container/blob` parallel to `s3://bucket/key`.
  Exactly one of `ACCOUNT` or `ENDPOINT` must be present for `az://`.
- `REGION '<r>'` — required for AWS S3 (`s3://` without `ENDPOINT`),
  ignored for Azure.
- `ENDPOINT '<https-url>'` — S3-compatible/MinIO/R2 (selects
  path-style) or a custom Azure endpoint.
- credentials via an extended `AUTH` form:
  `AUTH OBJSTORE S3 KEYID ENV '<idvar>' SECRET ENV '<secretvar>'`
  | `AUTH OBJSTORE AZURE ACCOUNT '<acct>' KEY ENV '<keyvar>'`.
- `FORMAT` stays `JSON|CSV|NDJSON` (`PARQUET` reserved for OBJ-2 —
  rejected at CREATE for `s3://`/`az://` with a clear "Parquet over
  object store is OBJ-2 (not yet shipped)" error so the boundary is
  honest).
- Pagination clauses (`PAGE …`) are **rejected at CREATE** for
  `s3://`/`az://` (a single object has no pages; multi-object
  listing is OBJ-4) with a clear typed error. `ROWS '<path>'` is
  still allowed (a JSON envelope inside one object).

CREATE-time compatibility validation lives in `kessel-sql` so the
error surfaces before any op is applied (mirrors SP98).

## 6. Security

- **Single secret-handling rule (extends SP97 verbatim):** only env
  -var **names** are ever persisted/replicated/logged. Secret values
  (`SECRET_ACCESS_KEY`, Azure account key) are resolved by
  `std::env::var` **in `do_refresh` at fetch time**, used to compute
  the signature, and dropped. They never appear in the op, WAL,
  digest, logs, or **error messages** (signing errors are typed
  `ObjError` with no secret material; `do_refresh` maps them to
  `OpResult::SchemaError("refresh: …")` with the *provider/url*, not
  the key).
- HTTPS-only (the `object-store` feature implies `tls`); the
  production webpki-roots full-verify path from SP99 applies — no
  bypass. An S3-compatible `ENDPOINT` must be `https://` (an
  `http://` endpoint is rejected at CREATE: object credentials must
  not traverse plaintext).
- Fail-closed: any signing, credential-missing, HTTP non-2xx, TLS,
  parse, or coercion error ⇒ typed error ⇒ `do_refresh` submits
  **nothing** (SP97 all-or-nothing abort, unchanged).

## 7. Testing

- `kessel-objstore` unit: **AWS SigV4 known-answer vectors** (the
  canonical AWS `aws-sig-v4-test-suite` GET case + an S3
  `x-amz-content-sha256` empty-body case) — assert the exact
  `Authorization` string for fixed creds + fixed `now`. **Azure
  Shared-Key known-answer** vector. Path-style vs virtual-hosted
  URL construction. RFC-3986 key encoding (spaces, `/`, unicode).
  base64 round-trip. All pure, deterministic, no network.
- `kessel-fetch` (`#[cfg(feature="object-store")]`): a localhost
  rustls stub (reuse the SP99 `tls_stub` fixture/harness) that
  asserts the inbound request carries a well-formed `Authorization`
  + `x-amz-date` and returns a JSON/CSV/NDJSON body; assert
  `fetch_rows_signed` returns the expected rows. A bad-host/cert
  case ⇒ typed error (fail-closed).
- Catalog: round-trip a recipe with the new ObjStore auth + region/
  endpoint **and** the hand-written-v1/v2-bytes back-compat
  assertion (new fields absent ⇒ pre-OBJ bytes, digests, seed-7
  unaffected).
- `kessel-sql`: parse tests for `s3://`/`az://`, `REGION`,
  `ENDPOINT`, the two `AUTH OBJSTORE` forms, and the rejections
  (`FORMAT PARQUET` over object store, `PAGE …` over object store,
  `http://` endpoint).
- Server e2e (`#[cfg(feature="external-sources-objstore")]`,
  mirroring `external_source_tls_oracle`): `CREATE EXTERNAL SOURCE
  … FROM 's3://b/k' … ENDPOINT 'https://127.0.0.1:<port>'` against
  a localhost S3-emulating rustls stub that validates the SigV4
  `Authorization` header shape and serves a fixed JSON body ⇒
  `REFRESH` materializes exactly those rows; a wrong-credential
  case ⇒ fail-closed `SchemaError`, prior state intact.
- Determinism gate: `cargo test --workspace --release` ⇒ FAILED=0,
  seed-7 green; the default-build total delta is **only** any
  intentionally non-gated unit test (target: **0** new default
  tests — all OBJ tests are feature-gated or in the
  `#[cfg(feature="object-store")]` crate; `kessel-objstore`'s own
  unit tests run only when that crate is built, which the default
  workspace build does **not** do since nothing depends on it
  without the feature). The plan states the exact expected default
  total explicitly and reconciles README/STATUS honestly.

## 8. Non-goals (explicit, kept honest in docs)

Parquet/Iceberg (OBJ-2/3); prefix or multi-object/`LIST` sources
(OBJ-4); STS/session tokens, Azure SAS, IMDS/workload identity
(OBJ-5); object **writes** (KesselDB never writes upstream — read/
materialize only); range/multipart GET; per-object pagination;
streaming (the whole object is fetched once, decoded, concatenated,
one atomic `Txn` — identical to EXT); schema inference (explicit
column mapping only, unchanged). The deterministic kernel is not
touched; OBJ never runs in the kernel or enters the replicated log
except as already-captured rows.

## 9. Process note (autonomous mandate)

Per `feedback_kesseldb_autonomous_build` the user is unavailable and
delegated decisions; the brainstorming user-review gate is satisfied
by the standing mandate. The implementation still goes through
`writing-plans` → `subagent-driven-development` with the full
two-stage (spec-compliance then code-quality) review per task and a
final whole-implementation review. KesselDB non-negotiables hold:
zero-dep deterministic kernel, seed-7 green, honest docs, single
-branch commits straight to `main` (no Co-Authored-By, no signing,
matching `git log` style), full `cargo test --workspace --release`
each kernel-adjacent task.
