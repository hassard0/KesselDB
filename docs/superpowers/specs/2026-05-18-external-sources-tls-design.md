# KesselDB — External Sources: HTTPS / TLS in `kessel-fetch` (follow-on slice): design

**Date:** 2026-05-18  **Status:** design approved, pre-implementation.

A follow-on to the shipped External Sources feature (slice-1 design:
`docs/superpowers/specs/2026-05-18-external-sources-design.md`,
internal record `…-subproject97-external-sources.md`; pagination
design `…-external-sources-pagination-design.md`, record
`…-subproject98-ext-pagination.md`). It lets external sources fetch
`https://` URLs **directly**, removing the single biggest usability
caveat ("HTTP-only — front it with a TLS-terminating sidecar"), while
preserving every slice-1 invariant: the deterministic kernel stays
zero-dependency, the default workspace build pulls no new dependencies and the kernel/WAL/seed-7 outputs are byte-identical (the only default-build change is +2 feature-exempt tests — see §4).

## 1. Architecture & invariants

The deterministic kernel, WAL, `kessel-catalog`, `kessel-proto`,
`kessel-sql`, and the router's `do_refresh` are **untouched**. The
entire change lives in the optional `kessel-fetch` crate, plus one
Cargo feature line in `kesseldb-server`, plus docs. A URL is already
an opaque string throughout the recipe / wire / SQL surface, so there
are **no new persisted fields, no catalog trailer bump, no proto
change** — backward-compatibility is preserved by construction and
the seed-7 corpus cannot be affected.

`kessel-fetch::http::get_resp` gains a scheme split. A small
`connect()` step replaces the inline `TcpStream::connect` and yields
a boxed transport; the request build, response read loop, the
`MAX_HEADER_SLACK` cap, status parse, header collection, `dechunk`,
and the body cap are **moved verbatim** to operate on that boxed
stream:

- `http://` → a plaintext `TcpStream` — literally today's bytes and
  behavior, default port 80.
- `https://` (only when the crate's `tls` feature is on) → a
  `rustls::StreamOwned<rustls::ClientConnection, TcpStream>`, default
  port 443.

A private transport seam carries this:

```rust
trait ReadWrite: std::io::Read + std::io::Write {}
impl<T: std::io::Read + std::io::Write> ReadWrite for T {}
```

`connect()` returns `Box<dyn ReadWrite>`. Both `TcpStream` and the
rustls `StreamOwned` satisfy it. Everything downstream of `connect()`
is unchanged code paths.

**Determinism boundary (unchanged from slice-1):** the TLS handshake
uses an RNG and is non-deterministic at the socket, but every
external fetch is **captured once at the router** and replicated as a
single atomic `Op::Txn`. TLS bytes never enter the WAL, the digest,
or the seed-7 corpus. Feature OFF ⇒ `rustls` / `webpki-roots` are
absent from the dependency graph and `cargo test --workspace
--release` kernel/WAL/determinism output is byte-identical to today and the default build pulls no new deps (the default-build test total rises by 2 feature-exempt tests — see §4).

Rejected alternatives: a separate `https_get` parallel to `get_resp`
(duplicates the hardened header/dechunk/cap/loop logic into a second
path that must be kept bug-for-bug in sync — the exact drift the
slice-1/pagination reviews caught); a public pluggable-transport
trait callers inject (YAGNI — no consumer needs it, and it would
leak rustls types into the public API even when the feature is off).

## 2. Feature gating & dependencies

`crates/kessel-fetch/Cargo.toml`:

```toml
[dependencies]
rustls       = { version = "0.23", optional = true }
webpki-roots = { version = "1",    optional = true }

[features]
tls = ["dep:rustls", "dep:webpki-roots"]
```

`rustls 0.23` is the exact crate + version already vetted in the
workspace by `kesseldb-server`'s server-side `tls` feature; we reuse
its **client** side. The `webpki-roots` major must be whichever one
re-exports trust anchors as the `rustls-pki-types` `TrustAnchor` that
`rustls 0.23`'s `RootCertStore` accepts (1.x at time of writing; the
implementation plan pins it with a compile check rather than guessing
the patch here). `crates/kesseldb-server/Cargo.toml` composes the
two existing opt-ins into one new feature:

```toml
external-sources-tls = ["external-sources", "kessel-fetch/tls"]
```

`external-sources` **alone is unchanged** — it does not enable
`kessel-fetch/tls`, so a plain-HTTP / TLS-sidecar deployment pulls
**zero** new dependencies. `kessel-fetch`'s default (no-feature)
build still compiles: `tls.rs` is entirely `#[cfg(feature = "tls")]`,
and the `https://` arm of `get_resp` is `#[cfg(feature = "tls")]`
versus a `#[cfg(not(feature = "tls"))]` arm returning a typed error.
The default workspace build/test (the determinism gate) compiles
`kessel-fetch` exactly as today.

## 3. TLS module & verification policy

New `crates/kessel-fetch/src/tls.rs`, fully `#[cfg(feature = "tls")]`:

- A process-wide `rustls::ClientConfig` built once via
  `std::sync::OnceLock` (rebuilding the root store per page during
  pagination would be wasteful). Roots =
  `webpki-roots::TLS_SERVER_ROOTS` loaded into a
  `rustls::RootCertStore`; config =
  `ClientConfig::builder().with_root_certificates(roots)
  .with_no_client_auth()`. **Full chain + hostname verification** via
  rustls's default verifier. No `dangerous()` / custom verifier
  anywhere on the production path — there is no bypass under any flag
  in this slice.
- `pub(crate) fn connect_tls(host: &str, port: u16)
  -> Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>,
  FetchError>`: TCP-connect with the same 30s read/write timeouts as
  the plaintext path; build a `ClientConnection` with
  `rustls::pki_types::ServerName::try_from(host)` (SNI, and the
  certificate name check is bound to the URL host); wrap into
  `StreamOwned`. Handshake / cert-invalid / bad-SNI errors map to
  `FetchError::Http("tls: …")`.

`get_resp` scheme handling:

- `http://` — byte-for-byte the current path (port 80 default).
- `https://` with `tls` on — port 443 default, transport from
  `connect_tls`.
- `https://` with `tls` off — `FetchError::Http("https:// requires
  building with the external-sources-tls feature")`, replacing
  today's "only http:// is supported … use a TLS sidecar" message.
  This surfaces at `REFRESH`, exactly like every other fetch error in
  slice-1 (the router has no compiled-feature knowledge to validate
  earlier, and feature flags are build-time).
- IPv6-literal hosts stay rejected as today (out of scope, unchanged
  message).

## 4. Testing

**Fixtures.** Check in a self-signed CA + leaf for `localhost` (SAN
`DNS:localhost`) under `crates/kessel-fetch/tests/fixtures/` with a
deliberately far-future expiry (≈ year 4096) so the fixed bytes never
time-bomb the suite; a `README` beside them documents the one-line
regen command. Fixed bytes ⇒ no `rcgen`/openssl test-time
dependency; the long expiry is an intentional, documented
test-fixture caveat.

**TLS happy path** (`#[cfg(feature = "tls")]`): a localhost HTTPS
stub (`rustls::ServerConfig` from the fixture cert/key) serving the
slice-1 JSON sample; the test connects with a **test-only**
`ClientConfig` whose `RootCertStore` trusts *only* the fixture CA.
Assert `fetch_rows` over `https://localhost:<port>/…` returns the
same rows the existing `http://` stub test asserts. A second test
exercises `fetch_rows_paginated` over `https://` (one `Link`-header
page-walk) to prove pagination composes with TLS.

**Verification is real** (`#[cfg(feature = "tls")]`): point the
**production** `webpki-roots` config at the self-signed stub ⇒ assert
`Err(FetchError::Http(_))` (proves untrusted certs are not silently
accepted). A hostname-mismatch case (connect via `127.0.0.1` to the
`localhost`-only cert) ⇒ same typed error.

**Feature OFF** (default, no `tls`):
`get_resp("https://example.invalid/…", …)` ⇒
`Err(FetchError::Http(msg))` with `msg` naming the
`external-sources-tls` feature; `rustls` / `webpki-roots` absent from
the default build. The existing `http://` unit/integration tests stay
green unchanged.

**Determinism gate.** `cargo test --workspace --release` ⇒
FAILED=0. The default-build test total increases by **two** (245 →
247): the feature-off `https://` rejection test
(`stub_server.rs::https_without_tls_feature_is_typed_error_naming_the_feature`)
and the `rows_from_body` decode unit test (`lib.rs` ptests). Every
other new test is `#[cfg(feature = "tls")]` /
`#[cfg(feature = "external-sources-tls")]` and is not compiled by the
default workspace build. `README.md` / `STATUS.md` test counts are
bumped accordingly. Seed-7 (`large_seed_corpus_is_deterministic_and_converges`)
stays green; the kernel/default build pulls no rustls/webpki (verified
via `cargo tree`).
Feature-on coverage runs via `cargo test -p kessel-fetch --features
tls`. A feature-gated server-level smoke
(`#[cfg(feature = "external-sources-tls")]`) does one `REFRESH` of an
`https://` localhost-stub recipe and asserts the materialized rows,
proving the `do_refresh` → TLS path end-to-end.

## 5. Docs, scope & non-goals

**Docs (codename-free public).** `docs/USAGE.md` §7c/§7d and the EXT
design + pagination docs currently say *"HTTP-only; HTTPS via a
TLS-terminating sidecar; TLS in `kessel-fetch` deferred."* Update the
boundary to: *"`http://` always; `https://` when built with
`--features external-sources-tls` (bundled Mozilla roots, full
certificate + hostname verification, no bypass); the sidecar is now
optional."* `docs/STATUS.md` gets the slice line and SP99 row.
`README.md` test-count line is bumped from 245 to 247 (the two
default-build additions — see §4 above). A new internal slice record
`docs/superpowers/specs/2026-05-18-kesseldb-subproject99-ext-tls.md`.

**In scope:** the scheme-dispatch + boxed-stream seam in `http.rs`;
`tls.rs` (rustls client + webpki-roots, verify-always); the two Cargo
feature lines; the feature-off typed error; all tests above; docs.

**Deferred (each its own micro-slice, named so the boundary stays
honest):** OS/enterprise trust store (`rustls-native-certs`); a
per-source `TLS INSECURE` opt-out; client-cert / mTLS; a custom
CA-bundle path in the recipe; HTTPS proxy `CONNECT`; configurable TLS
version / cipher policy.

**Non-goals (explicit):** no wire / catalog / SQL / proto change; no
production verification bypass under any flag in this slice; no
IPv6-literal support (unchanged); TLS does not alter the determinism
model (captured-once at the router, unchanged).
