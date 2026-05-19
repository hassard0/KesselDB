# KesselDB — Subproject 99: External Sources HTTPS/TLS

**Date:** 2026-05-18  **Status:** done — code + tests committed and passing.

Builds on:
- Subproject 97 — External sources (EXT slice 1):
  `docs/superpowers/specs/2026-05-18-external-sources-design.md`
- Subproject 98 — External sources: pagination + NDJSON:
  `docs/superpowers/specs/2026-05-18-external-sources-pagination-design.md`

Design document:
`docs/superpowers/specs/2026-05-18-external-sources-tls-design.md`

---

## What shipped

### `kessel-fetch` `tls` feature

Optional dependencies (off by default, zero impact on the default
build's dependency graph):

```toml
rustls       = { version = "0.23", optional = true }
webpki-roots = { version = "1",    optional = true }
rustls-pemfile = { version = "2",  optional = true }

[features]
tls = ["dep:rustls", "dep:webpki-roots", "dep:rustls-pemfile"]
```

### `kesseldb-server` composite feature

```toml
external-sources-tls = [
    "external-sources",
    "kessel-fetch/tls",
    "dep:rustls",
    "dep:rustls-pemfile",
]
```

`external-sources` alone is unchanged and dep-free. A plain-HTTP /
sidecar deployment continues to pull zero new dependencies.

### Code changes in `crates/kessel-fetch/`

**`src/http.rs` refactored:**
- `parse_target(url)` — splits scheme/host/port/path.
- `build_request(host, path, headers)` → `Vec<u8>` request bytes.
- `ReadWrite` trait: `trait ReadWrite: std::io::Read + std::io::Write {}`
  with a blanket impl — carries the boxed transport seam.
- `connect(scheme, host, port)` → `Box<dyn ReadWrite>`: dispatches
  `http://` to a plain `TcpStream` (exactly today's behavior, port 80
  default) and `https://` (when `tls` feature is on) to
  `tls::connect_tls`; when `tls` is off, `https://` returns a typed
  `FetchError` naming the `external-sources-tls` feature.
- `exchange<S: Read+Write>(stream, request, …)` — generic over stream
  type; contains the existing request-send, response-read loop,
  `MAX_HEADER_SLACK` cap, status parse, header collection, `dechunk`,
  and body-cap logic unchanged.

**`src/tls.rs`** (entire file is `#[cfg(feature = "tls")]`):
- Process-wide `rustls::ClientConfig` built once via `std::sync::OnceLock`.
  Roots = `webpki_roots::TLS_SERVER_ROOTS` loaded into
  `rustls::RootCertStore`; config built with
  `ClientConfig::builder().with_root_certificates(roots).with_no_client_auth()`.
  **Full chain + hostname verification** via rustls's default verifier.
  No `dangerous()` / custom verifier anywhere on the production path.
  No bypass under any flag.
- `pub(crate) fn connect_tls(host: &str, port: u16) -> Result<…, FetchError>`:
  TCP-connect with the same 30 s read/write timeouts as the plaintext path;
  builds a `ClientConnection` with
  `rustls::pki_types::ServerName::try_from(host)` (SNI + cert name check
  bound to URL host); wraps into `StreamOwned`. Handshake / cert-invalid /
  bad-SNI errors map to `FetchError::Http("tls: …")`.
- `pub(crate) fn connect_tls_with(cfg: Arc<ClientConfig>, host, port)` —
  shared connect seam; `connect_tls` calls it with the production
  `client_config()`, the test entrypoint with a fixture-trusting config.
- `#[doc(hidden)] pub fn test_config_trusting(pem: &[u8])` — builds a
  `ClientConfig` trusting every cert parsed from the supplied PEM bytes
  (the checked-in localhost fixture); only reachable from
  `#[cfg(feature = "tls")]` test entrypoints.

**`src/lib.rs`:**
- `rows_from_body(body, recipe)` — factored out of the inline fetch
  path; decodes and coerces a body slice into typed rows.
- `#[cfg(feature = "tls")] #[doc(hidden)] pub fn fetch_rows_https_test(…)`
  — test-only entry that accepts an explicit `ClientConfig`, used by
  `tls_stub.rs` to inject fixture trust without touching the production
  `OnceLock`.

### Test fixture

Long-lived (year 4025) self-signed cert for `localhost` with SAN
`DNS:localhost`. Critically: `CA:FALSE` (BasicConstraints not a CA).
Checked in under `crates/kessel-fetch/tests/fixtures/` with a `README`
documenting the one-line regen command. Fixed bytes: no `rcgen`/openssl
test-time dependency; the far-future expiry is an intentional,
documented caveat (no time-bomb risk within any plausible project
lifetime).

---

## Tests

### Default build (`cargo test --workspace --release`)

Two new tests are compiled and run in the default (no-feature) build:

1. `crates/kessel-fetch/tests/stub_server.rs::https_without_tls_feature_is_typed_error_naming_the_feature`
   (Task 2) — asserts that an `https://` URL with `tls` feature OFF
   returns a typed `FetchError` whose message names the
   `external-sources-tls` feature.
2. `crates/kessel-fetch/src/lib.rs` ptests:
   `rows_from_body_decodes_json_like_fetch_rows` (Task 4) — unit test
   for the factored `rows_from_body` decode path.

**Default-build total: 245 → 247 (+2). Seed-7 green.**

Verification:
```
cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus"
# sum of all "N passed" = 247
# test sim::tests::large_seed_corpus_is_deterministic_and_converges ... ok
```

Default `cargo tree` confirms no rustls/webpki in the default build:
```
cargo tree -p kessel-fetch -e normal | grep -E "rustls|webpki" || echo CLEAN
# → CLEAN
```

### Feature-on: `cargo test -p kessel-fetch --features tls`

`crates/kessel-fetch/tests/tls_stub.rs` — 4 tests:
- Happy path: trusted fixture CA, `fetch_rows` over
  `https://localhost:<port>/…` returns the same rows as the `http://`
  stub test.
- Paginated TLS: `fetch_rows_paginated` over `https://` with one
  `Link`-header page-walk (proves pagination composes with TLS).
- Production rejects self-signed: production `webpki-roots` config
  pointed at the self-signed stub asserts `Err(FetchError::Http(_))`
  (proves untrusted certs are not silently accepted).
- Hostname mismatch: connect via `127.0.0.1` to the `localhost`-only
  cert asserts `Err(FetchError::Http(_))`.

### Feature-on: `cargo test -p kesseldb-server --features external-sources-tls --test external_source_tls_oracle`

1 test — fail-closed e2e: the production router (webpki-roots trust
path) attempts a `REFRESH` of an `https://` source backed by the
self-signed fixture stub and asserts the operation returns an error
and leaves prior data intact. The trusted happy path is covered at the
`kessel-fetch` layer (above); injecting fixture-trust into the
production router would require a forbidden bypass of the
`OnceLock`-guarded config.

---

## Security posture

Exactly one production trust path: `webpki-roots::TLS_SERVER_ROOTS`
with rustls full chain + hostname verification. No bypass under any
flag or environment variable. The only non-webpki trust path
(`test_config_trusting`) is `#[doc(hidden)]` and only reachable from
`#[cfg(feature = "tls")]` test entrypoints — it is never compiled into
or callable from a production binary built without `--cfg test`.

---

## Key correctness decisions discovered during build

**(a) Fixture must be `CA:FALSE`.** rustls 0.23 rejects a certificate
with `BasicConstraints: CA:TRUE` as a server end-entity certificate
(`CaUsedAsEndEntity` error). The fixture was regenerated with
`CA:FALSE`; the `README` documents this as a required constraint.

**(b) `exchange()` treats `io::ErrorKind::UnexpectedEof` as clean
end-of-stream.** rustls reports a server closing the connection without
sending a TLS `close_notify` alert as `UnexpectedEof` at the `Read`
level. kessel-fetch is strictly length-framed (`Content-Length` or
chunked) and always sends `Connection: close`; this is the
rustls-endorsed behavior for HTTP/1.1 clients. Downstream
dechunk/body-cap/parse guards still catch genuine truncation.

**(c) The server e2e test is deliberately fail-closed.** The
production router's `OnceLock`-guarded `ClientConfig` trusts only
`webpki-roots`. There is no injection point for fixture trust at the
router level — introducing one would constitute a production bypass,
which is explicitly forbidden in this slice. The trusted happy-path
coverage lives at the `kessel-fetch` crate layer, where
`fetch_rows_https_test` accepts an explicit `ClientConfig`.

---

## Deferred follow-ons

### From the design's own deferred list

- OS / enterprise trust store integration (`rustls-native-certs`).
- Per-source `TLS INSECURE` opt-out (explicit SQL flag; intentionally
  absent in this slice — no bypass under any flag is the stated invariant).
- Client-cert / mTLS.
- Custom CA-bundle path in the recipe.
- HTTPS proxy `CONNECT` tunneling.
- Configurable TLS version / cipher policy.

### Found during build (tracked here so the boundary stays honest)

- **DRY follow-up:** `fetch_rows_paginated`'s inline decode+coerce tail
  duplicates the logic now in `rows_from_body` — it is behaviorally
  identical but not yet unified. A follow-on can route the paginated
  path through `rows_from_body` as well.
- **Trusted multi-page HTTPS happy-path test missing:** the current
  paginated TLS test (`tls_stub.rs`) is fail-closed only. A trusted
  multi-page HTTPS test (exercising the paginated `Link`-header walk
  end-to-end with a fixture-trusted config) is a follow-on addition.
- **`test_config_trusting` visibility tightening:** currently `pub`;
  only the in-crate `fetch_rows_https_test` calls it. Could be
  narrowed to `pub(crate)` as a minor hygiene follow-on.
- **Secret-scanner allow-list:** the test-only `localhost.key.pem`
  fixture will trip pattern-based secret scanners (gitleaks, GitHub
  Advanced Security). If/when the repo gains CI secret scanning, add a
  scanner allow-list entry for the fixtures directory. This is
  documented in the fixtures `README` rationale.
