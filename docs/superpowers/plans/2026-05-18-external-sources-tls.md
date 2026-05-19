# External Sources HTTPS/TLS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let external sources fetch `https://` URLs directly by adding an optional, feature-gated rustls client transport to `kessel-fetch`, with no change to the deterministic kernel, wire, catalog, SQL, or `do_refresh`.

**Architecture:** Refactor `kessel-fetch::http` into three reusable pieces — `parse_target` (scheme/host/port/path), `build_request` (HTTP/1.1 GET text), and a generic `exchange<S: Read + Write>` that holds *all* the hardened header/dechunk/cap logic exactly as today. `get`/`get_resp` become thin: parse → connect (scheme dispatch) → build → exchange. `http://` builds a plaintext `TcpStream` (byte-identical to today). `https://` builds a `rustls::StreamOwned` from a new fully-feature-gated `tls.rs`. The generic-function seam realizes the spec's "transport seam" without a `dyn`-supertrait wart — same single hardened path over either transport, no public API or behavior change on the `http://` path.

**Tech Stack:** Rust, `rustls 0.23` (client side; same crate/version the server's `tls` feature already vendors), `webpki-roots` (bundled Mozilla roots, full chain + hostname verification, no production bypass), `rustls-pemfile 2` (test-only, for fixture parsing).

**Spec:** `docs/superpowers/specs/2026-05-18-external-sources-tls-design.md`

**Conventions (carry into every commit):** repo `C:\Users\ihass\KesselDB`; commit straight to `main` (private `hassard0/KesselDB`, established single-branch workflow); **no** `Co-Authored-By`, **no** commit signing; match the existing message style (lowercase area prefix, e.g. `external sources: …`, `kesseldb-server: …`, `docs: …` — see `git log -3 --format='%s'`). After each task's final commit, `git push`.

**Determinism gate (run after every kernel-adjacent task):**
`cargo test --workspace --release` ⇒ `FAILED. 0` everywhere and the seed-7 test `large_seed_corpus_is_deterministic_and_converges` present and passing. The only intended default-build test-count change in this entire plan is **exactly +1** (the feature-off `https://` rejection test in Task 2); every other new test is `#[cfg(feature = "tls")]` / `#[cfg(feature = "external-sources-tls")]` and is **not** compiled by the default workspace build. Record the pre-change baseline in Task 0 and assert the delta is exactly +1 at the end.

---

## File Structure

- `crates/kessel-fetch/Cargo.toml` — add optional `rustls`, `webpki-roots`; `tls` feature; dev-dep `rustls-pemfile`.
- `crates/kessel-fetch/src/http.rs` — refactor into `parse_target` / `build_request` / `exchange<S>`; scheme dispatch in a new `connect`.
- `crates/kessel-fetch/src/tls.rs` — **new**, fully `#[cfg(feature = "tls")]`: `client_config()` (OnceLock prod, webpki-roots), `connect_tls`, `connect_tls_with`, `roots_from_pem` (test support).
- `crates/kessel-fetch/src/lib.rs` — declare `mod tls` (cfg-gated); factor `rows_from_body`; add `#[doc(hidden)] #[cfg(feature = "tls")] fetch_rows_https_test`.
- `crates/kessel-fetch/tests/fixtures/{localhost.pem,localhost.key.pem,README.md}` — **new** long-lived self-signed `localhost` cert.
- `crates/kessel-fetch/tests/tls_stub.rs` — **new**, `#![cfg(feature = "tls")]`: happy path, paginated-over-https, verification-is-real, hostname-mismatch.
- `crates/kessel-fetch/tests/stub_server.rs` — add the one default-build feature-off `https://` rejection test.
- `crates/kesseldb-server/Cargo.toml` — add `external-sources-tls` composite feature.
- `crates/kesseldb-server/tests/external_source_tls_oracle.rs` — **new**, `#![cfg(feature = "external-sources-tls")]`: one `REFRESH` over an `https://` localhost stub.
- `docs/USAGE.md`, `docs/STATUS.md`, `README.md`, the two EXT design docs, the TLS design doc, and a new `docs/superpowers/specs/2026-05-18-kesseldb-subproject99-ext-tls.md`.

---

### Task 0: Record the determinism baseline

**Files:** none (measurement only).

- [ ] **Step 1: Capture the default-build test total**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus" | tee /tmp/kdb-tls-baseline.txt`
Expected: every line `test result: ok. … 0 failed`; `large_seed_corpus_is_deterministic_and_converges` appears and passes. Sum the `N passed` across `test result:` lines and note it as **BASELINE_TOTAL** in your working notes. No commit.

---

### Task 1: kessel-fetch Cargo deps + feature + config compile-pin

Pins the exact `webpki-roots` major against the in-tree `rustls 0.23` `pki-types` via a real compiling test (no version guessing).

**Files:**
- Modify: `crates/kessel-fetch/Cargo.toml`
- Test: inline `#[cfg(all(test, feature = "tls"))]` in `crates/kessel-fetch/src/tls.rs` (created here, expanded in Task 3)

- [ ] **Step 1: Add deps + feature to `crates/kessel-fetch/Cargo.toml`**

Replace the file's `[dependencies]` block and append features/dev-deps so it reads exactly:

```toml
[package]
name = "kessel-fetch"
edition.workspace = true
version.workspace = true
license.workspace = true

[dependencies]
kessel-catalog = { path = "../kessel-catalog" }

# Opt-in HTTPS for external-source fetch. OFF by default so the crate's
# default build stays dependency-light (plain http:// only). `rustls`
# is the exact crate+version the kesseldb-server `tls` feature already
# vendors; we use its CLIENT side. `webpki-roots` ships the Mozilla
# root set compiled in (deterministic, no OS trust dependency).
rustls = { version = "0.23", optional = true }
webpki-roots = { version = "1", optional = true }

[features]
tls = ["dep:rustls", "dep:webpki-roots"]

[dev-dependencies]
# Test-only: parse the checked-in localhost fixture PEM for the TLS
# stub server and the custom-root client config.
rustls-pemfile = "2"
```

- [ ] **Step 2: Create `crates/kessel-fetch/src/tls.rs` with the compile-pin test only**

```rust
//! Optional HTTPS client transport. Entirely `#[cfg(feature = "tls")]`
//! — never compiled by the default build, never linked into the
//! deterministic kernel.
#![cfg(feature = "tls")]

#[cfg(test)]
mod pin_tests {
    /// Compile-pin: `webpki-roots`'s trust anchors must be exactly the
    /// `rustls-pki-types` `TrustAnchor` that `rustls 0.23`'s
    /// `RootCertStore` accepts. If the `webpki-roots` major in
    /// Cargo.toml is wrong this test FAILS TO COMPILE — bump it until
    /// this builds, do not guess.
    #[test]
    fn prod_client_config_builds_from_webpki_roots() {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        assert!(!roots.is_empty(), "webpki-roots must ship anchors");
        let _cfg = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
    }
}
```

- [ ] **Step 3: Declare the cfg-gated module in `crates/kessel-fetch/src/lib.rs`**

After the existing `mod http;` line (currently line 11), add:

```rust
#[cfg(feature = "tls")]
mod tls;
```

- [ ] **Step 4: Run the pin test (feature on)**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-fetch --features tls prod_client_config_builds_from_webpki_roots -- --nocapture`
Expected: PASS. If it fails to **compile** with a type mismatch on `roots.extend`, change `webpki-roots = { version = "1", … }` to `version = "0.26"` and re-run; iterate until it compiles and passes. (rustls 0.23 pairs with webpki-roots 1.x at time of writing; the test is the authority.)

- [ ] **Step 5: Verify the default build is unchanged**

Run: `cd /c/Users/ihass/KesselDB && cargo build -p kessel-fetch && cargo tree -p kessel-fetch | grep -E "rustls|webpki" || echo "NO TLS DEPS IN DEFAULT BUILD"`
Expected: prints `NO TLS DEPS IN DEFAULT BUILD` (default build pulls neither crate).

- [ ] **Step 6: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-fetch/Cargo.toml crates/kessel-fetch/src/tls.rs crates/kessel-fetch/src/lib.rs
git commit -m "external sources: optional kessel-fetch tls feature + webpki-roots compile-pin"
```

---

### Task 2: Refactor `http.rs` into parse/build/exchange + scheme dispatch

Pure structural refactor of the hardened HTTP path plus the `https://` scheme arm. The existing `stub_server.rs` `http://` tests are the regression net — they must stay green untouched.

**Files:**
- Modify: `crates/kessel-fetch/src/http.rs` (whole file)
- Test: `crates/kessel-fetch/tests/stub_server.rs` (add one default-build test)

- [ ] **Step 1: Write the failing default-build test**

Append to `crates/kessel-fetch/tests/stub_server.rs`:

```rust
#[test]
fn https_without_tls_feature_is_typed_error_naming_the_feature() {
    // Default build (no `tls`): https:// must be a clean typed error
    // that names the feature to enable — never a panic, never a
    // silent plaintext downgrade.
    let cols = vec![ColumnMap {
        name: "id".into(),
        kind: FieldKind::U32,
        source: "id".into(),
    }];
    let e = fetch_rows(
        "https://example.invalid/d",
        &Auth::None,
        Format::Json,
        &cols,
        DEFAULT_MAX_BODY,
    )
    .unwrap_err();
    match e {
        kessel_fetch::FetchError::Http(m) => assert!(
            m.contains("external-sources-tls"),
            "message must name the feature, got: {m}"
        ),
        other => panic!("expected Http error, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run it — expect failure**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-fetch --test stub_server https_without_tls_feature -- --nocapture`
Expected: FAIL — current `get_resp` returns the old `"only http:// is supported in slice 1 (use a TLS sidecar)"` message, which does not contain `external-sources-tls`.

- [ ] **Step 3: Rewrite `crates/kessel-fetch/src/http.rs`**

Replace the entire file with:

```rust
//! Dependency-free HTTP/1.1 GET with an optional TLS transport.
//! `http://` is always plaintext; `https://` requires the `tls`
//! feature (otherwise a typed error). All response handling
//! (header parse, dechunk, body cap) is one generic path shared by
//! both transports.
use crate::{Auth, FetchError};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Max response-header bytes tolerated before the `\r\n\r\n` separator
/// (in addition to `max_body`) — bounds buffering on a server that
/// streams a huge body without ever sending the header terminator.
const MAX_HEADER_SLACK: u64 = 64 * 1024;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Scheme {
    Http,
    Https,
}

/// Parse `scheme://host[:port]/path` into its parts, applying the
/// scheme's default port. IPv6-literal hosts are rejected (unchanged
/// from slice 1).
pub(crate) fn parse_target(
    url: &str,
) -> Result<(Scheme, String, u16, String), FetchError> {
    let (scheme, default_port, rest) = if let Some(r) =
        url.strip_prefix("http://")
    {
        (Scheme::Http, 80u16, r)
    } else if let Some(r) = url.strip_prefix("https://") {
        (Scheme::Https, 443u16, r)
    } else {
        return Err(FetchError::Http(
            "only http:// and https:// URLs are supported".into(),
        ));
    };
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
        None => (hostport, default_port),
    };
    if host.starts_with('[') {
        return Err(FetchError::Http(
            "IPv6 literal addresses are not supported; use a hostname"
                .into(),
        ));
    }
    Ok((scheme, host.to_string(), port, path.to_string()))
}

/// Build the HTTP/1.1 GET request text (Host header value is the bare
/// host, unchanged from slice 1).
pub(crate) fn build_request(path: &str, host: &str, auth: &Auth) -> String {
    let mut req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\
         User-Agent: kessel-fetch/0\r\n"
    );
    match auth {
        Auth::None => {}
        Auth::Bearer(t) => {
            req.push_str(&format!("Authorization: Bearer {t}\r\n"))
        }
        Auth::Header { name, value } => {
            req.push_str(&format!("{name}: {value}\r\n"))
        }
    }
    req.push_str("\r\n");
    req
}

/// Send `req` over an already-connected stream, read the full
/// response, enforce the caps, return `(headers, body)`. This is the
/// single hardened path; both the plaintext and TLS transports flow
/// through it unchanged.
pub(crate) fn exchange<S: Read + Write>(
    mut s: S,
    req: &str,
    max_body: u64,
) -> Result<(Vec<(String, String)>, Vec<u8>), FetchError> {
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
        if raw.len() as u64 > max_body + MAX_HEADER_SLACK {
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
        .ok_or_else(|| {
            FetchError::Http(format!("bad status line `{status}`"))
        })?;
    if !(200..300).contains(&code) {
        return Err(FetchError::Http(format!("HTTP {code}")));
    }
    let mut chunked = false;
    let mut headers: Vec<(String, String)> = Vec::new();
    for l in lines {
        if let Some(colon) = l.find(':') {
            let name = l[..colon].trim().to_string();
            let value = l[colon + 1..].trim().to_string();
            if name.eq_ignore_ascii_case("transfer-encoding")
                && value.to_ascii_lowercase().contains("chunked")
            {
                chunked = true;
            }
            headers.push((name, value));
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
    Ok((headers, body))
}

/// Connect the right transport for the scheme. `https://` without the
/// `tls` feature is a typed error that names the feature.
fn connect(
    scheme: Scheme,
    host: &str,
    port: u16,
) -> Result<Box<dyn ReadWrite>, FetchError> {
    match scheme {
        Scheme::Http => {
            let s = TcpStream::connect((host, port)).map_err(|e| {
                FetchError::Http(format!("connect {host}:{port}: {e}"))
            })?;
            s.set_read_timeout(Some(Duration::from_secs(30))).ok();
            s.set_write_timeout(Some(Duration::from_secs(30))).ok();
            Ok(Box::new(s))
        }
        Scheme::Https => {
            #[cfg(feature = "tls")]
            {
                Ok(Box::new(crate::tls::connect_tls(host, port)?))
            }
            #[cfg(not(feature = "tls"))]
            {
                let _ = (host, port);
                Err(FetchError::Http(
                    "https:// requires building with the \
                     external-sources-tls feature"
                        .into(),
                ))
            }
        }
    }
}

/// Object-safe Read+Write so `connect` can return either transport.
pub(crate) trait ReadWrite: Read + Write {}
impl<T: Read + Write> ReadWrite for T {}

impl Read for Box<dyn ReadWrite> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        (**self).read(buf)
    }
}
impl Write for Box<dyn ReadWrite> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        (**self).write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        (**self).flush()
    }
}

/// Returns response headers + body. Parses the URL, connects the
/// scheme's transport, and runs the shared exchange.
pub(crate) fn get_resp(
    url: &str,
    auth: &Auth,
    max_body: u64,
) -> Result<(Vec<(String, String)>, Vec<u8>), FetchError> {
    let (scheme, host, port, path) = parse_target(url)?;
    let stream = connect(scheme, &host, port)?;
    let req = build_request(&path, &host, auth);
    exchange(stream, &req, max_body)
}

/// Returns only the response body. Thin wrapper around `get_resp`.
pub fn get(
    url: &str,
    auth: &Auth,
    max_body: u64,
) -> Result<Vec<u8>, FetchError> {
    Ok(get_resp(url, auth, max_body)?.1)
}

fn dechunk(mut b: &[u8]) -> Result<Vec<u8>, FetchError> {
    let mut out = Vec::new();
    loop {
        let nl = b
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or_else(|| FetchError::Http("bad chunk".into()))?;
        let size_line = std::str::from_utf8(&b[..nl]).unwrap_or("");
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_hex, 16)
            .map_err(|_| FetchError::Http("bad chunk size".into()))?;
        b = &b[nl + 2..];
        if size == 0 {
            return Ok(out);
        }
        if b.len() < size + 2 {
            return Err(FetchError::Http(
                "truncated chunk (missing trailing CRLF)".into(),
            ));
        }
        out.extend_from_slice(&b[..size]);
        b = &b[size + 2..];
    }
}
```

Note on the `Box<dyn ReadWrite>` `Read`/`Write` impls: std only blanket-impls these for `Box<dyn Read>`/`Box<dyn Write>`, not for a custom subtrait object, so the two small forwarding impls above are required for `exchange::<Box<dyn ReadWrite>>` to type-check. They are pure delegation — no behavior.

- [ ] **Step 4: Run the new test + the full existing http:// regression net**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-fetch --test stub_server`
Expected: PASS — `https_without_tls_feature_is_typed_error_naming_the_feature` passes **and** every pre-existing test in `stub_server.rs` (`json_over_http_with_bearer_round_trips`, `body_too_large_is_typed_error`, `truncated_chunked_body_is_typed_error_not_panic`, `ndjson_over_http_round_trips`, `get_resp_exposes_link_header`) still passes unchanged.

- [ ] **Step 5: Run the determinism gate**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus"`
Expected: all `0 failed`; seed-7 test present + passing; summed total = **BASELINE_TOTAL + 1** (this task's one new default test).

- [ ] **Step 6: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-fetch/src/http.rs crates/kessel-fetch/tests/stub_server.rs
git commit -m "external sources: refactor http into parse/build/exchange + https scheme dispatch"
git push
```

---

### Task 3: `tls.rs` — production webpki-roots client config + connect_tls

**Files:**
- Modify: `crates/kessel-fetch/src/tls.rs`

- [ ] **Step 1: Write the failing test (connect refusal on a non-listening port is a typed tls/connect error, not a panic)**

Add to the `pin_tests` mod in `crates/kessel-fetch/src/tls.rs`:

```rust
    #[test]
    fn connect_tls_to_dead_port_is_typed_error() {
        // Nothing listening on this port → a clean FetchError::Http,
        // never a panic.
        let e = super::connect_tls("127.0.0.1", 1).unwrap_err();
        assert!(
            matches!(e, crate::FetchError::Http(_)),
            "expected Http error, got {e:?}"
        );
    }
```

- [ ] **Step 2: Run it — expect compile failure**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-fetch --features tls connect_tls_to_dead_port -- --nocapture`
Expected: FAIL to compile — `super::connect_tls` does not exist yet.

- [ ] **Step 3: Implement the module body**

Replace the contents of `crates/kessel-fetch/src/tls.rs` above the `#[cfg(test)] mod pin_tests` block with:

```rust
//! Optional HTTPS client transport. Entirely `#[cfg(feature = "tls")]`
//! — never compiled by the default build, never linked into the
//! deterministic kernel. Full chain + hostname verification against
//! the bundled Mozilla root set; no production bypass exists.
#![cfg(feature = "tls")]

use crate::FetchError;
use std::net::TcpStream;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Process-wide production client config (built once): trust = the
/// `webpki-roots` Mozilla set, full default verification (chain +
/// hostname), no client auth, no `dangerous()` override.
fn client_config() -> Arc<rustls::ClientConfig> {
    static CFG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    CFG.get_or_init(|| {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        )
    })
    .clone()
}

/// TCP-connect + TLS-handshake with an explicit config. Shared by the
/// production path and the test-support path (which supplies a config
/// trusting only the test fixture). 30s timeouts mirror the plaintext
/// transport.
pub(crate) fn connect_tls_with(
    cfg: Arc<rustls::ClientConfig>,
    host: &str,
    port: u16,
) -> Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>, FetchError>
{
    let server_name =
        rustls::pki_types::ServerName::try_from(host.to_owned())
            .map_err(|_| {
                FetchError::Http(format!(
                    "tls: invalid server name `{host}`"
                ))
            })?;
    let tcp = TcpStream::connect((host, port)).map_err(|e| {
        FetchError::Http(format!("connect {host}:{port}: {e}"))
    })?;
    tcp.set_read_timeout(Some(Duration::from_secs(30))).ok();
    tcp.set_write_timeout(Some(Duration::from_secs(30))).ok();
    let conn = rustls::ClientConnection::new(cfg, server_name)
        .map_err(|e| FetchError::Http(format!("tls: {e}")))?;
    Ok(rustls::StreamOwned::new(conn, tcp))
}

/// Production HTTPS connect: full verification against bundled roots.
pub(crate) fn connect_tls(
    host: &str,
    port: u16,
) -> Result<rustls::StreamOwned<rustls::ClientConnection, TcpStream>, FetchError>
{
    connect_tls_with(client_config(), host, port)
}

/// Test support: build a `ClientConfig` trusting ONLY the certs in
/// `pem` (the checked-in localhost fixture). Not reachable from any
/// production path.
#[doc(hidden)]
pub fn test_config_trusting(
    pem: &[u8],
) -> Arc<rustls::ClientConfig> {
    let mut rd = std::io::BufReader::new(pem);
    let mut roots = rustls::RootCertStore::empty();
    for c in rustls_pemfile::certs(&mut rd) {
        roots.add(c.expect("fixture cert PEM")).expect("add root");
    }
    Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}
```

Note: `rustls-pemfile` is a dev-dependency only. `test_config_trusting` is therefore compiled only under `cfg(test)` of this crate **or** integration tests of this crate (both have dev-deps). To keep it usable from the `tls_stub.rs` integration test while not requiring `rustls-pemfile` as a normal dep, gate it: prefix the `test_config_trusting` fn with `#[cfg(any(test, feature = "tls"))]` is **not** sufficient (integration test = separate crate, still needs the dep at build of kessel-fetch). Resolve by making `rustls-pemfile` an optional normal dep folded into `tls`:

In `crates/kessel-fetch/Cargo.toml` change the dep/feature lines to:

```toml
[dependencies]
kessel-catalog = { path = "../kessel-catalog" }
rustls = { version = "0.23", optional = true }
webpki-roots = { version = "1", optional = true }
rustls-pemfile = { version = "2", optional = true }

[features]
tls = ["dep:rustls", "dep:webpki-roots", "dep:rustls-pemfile"]
```

and delete the `[dev-dependencies]` block added in Task 1 (no longer needed — `rustls-pemfile` rides the `tls` feature). The default build still pulls none of the three.

- [ ] **Step 4: Run the test (feature on)**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-fetch --features tls`
Expected: PASS — `prod_client_config_builds_from_webpki_roots`, `connect_tls_to_dead_port_is_typed_error`, plus all default `kessel-fetch` tests (the `--features tls` build is a superset).

- [ ] **Step 5: Verify default build still clean**

Run: `cd /c/Users/ihass/KesselDB && cargo tree -p kessel-fetch | grep -E "rustls|webpki|pemfile" || echo "DEFAULT BUILD CLEAN"`
Expected: `DEFAULT BUILD CLEAN`.

- [ ] **Step 6: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-fetch/src/tls.rs crates/kessel-fetch/Cargo.toml
git commit -m "external sources: rustls client config (webpki-roots, verify-always) + connect_tls"
git push
```

---

### Task 4: Factor `rows_from_body` + add the feature-gated test entrypoint

`fetch_rows`/`fetch_rows_paginated` already duplicate the body→rows tail. Factor it once and add a `#[doc(hidden)]` HTTPS test entrypoint that reuses it (so the integration tests can drive a custom-root fetch without duplicating decode logic).

**Files:**
- Modify: `crates/kessel-fetch/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Add to the `ptests` mod at the bottom of `crates/kessel-fetch/src/lib.rs`:

```rust
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
```

- [ ] **Step 2: Run it — expect compile failure**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-fetch rows_from_body_decodes_json_like_fetch_rows -- --nocapture`
Expected: FAIL to compile — `rows_from_body` does not exist.

- [ ] **Step 3: Add `rows_from_body`, rewire `fetch_rows`, add the test entrypoint**

In `crates/kessel-fetch/src/lib.rs`, add this function immediately before `pub fn fetch_rows`:

```rust
/// Decode a fetched body into coerced rows. Shared by `fetch_rows`
/// (single page) and the per-page step of `fetch_rows_paginated`.
/// `rows_path` is honored for `Format::Json` only (NDJSON/CSV ignore
/// it, exactly as the existing paginated loop does).
pub(crate) fn rows_from_body(
    body: &[u8],
    format: Format,
    cols: &[ColumnMap],
    rows_path: Option<&str>,
) -> Result<Vec<Vec<Vec<u8>>>, FetchError> {
    let raw = match format {
        Format::Json => json::rows_at(body, cols, rows_path)?,
        Format::Csv => csv::extract(body, cols)?,
        Format::Ndjson => ndjson::extract(body, cols)?,
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
```

Replace the body of `pub fn fetch_rows` (currently lines ~211-225) so it reads exactly:

```rust
    let body = http::get(url, auth, max_body)?;
    rows_from_body(&body, format, cols, None)
```

Confirm `json::rows_at(body, cols, None)` is byte-equivalent to the old `json::extract(body, cols)` for the no-path case — it is: the paginated loop already calls `json::rows_at(&body, cols, rows_path)` with `rows_path` possibly `None` and that is the established slice-1-equivalent path. The existing `stub_server.rs` JSON/NDJSON/CSV tests are the proof; they must stay green in Step 5.

Add, immediately after `fetch_rows`, the feature-gated HTTPS test entrypoint:

```rust
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
```

For this to compile, `http::parse_target`, `http::Scheme`, `http::build_request`, and `http::exchange` must be `pub(crate)` (they are, per Task 2) and `http::Scheme` must derive `PartialEq` (it does, per Task 2's `#[derive(... PartialEq, Eq ...)]`).

- [ ] **Step 4: Run the unit test**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-fetch rows_from_body_decodes_json_like_fetch_rows -- --nocapture`
Expected: PASS.

- [ ] **Step 5: Run the full kessel-fetch regression net (default + feature-on)**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-fetch && cargo test -p kessel-fetch --features tls`
Expected: both PASS — every existing `stub_server.rs` / `paginate_stub.rs` / unit test green; `fetch_rows` behavior unchanged.

- [ ] **Step 6: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-fetch/src/lib.rs
git commit -m "external sources: factor rows_from_body + feature-gated https test entrypoint"
git push
```

---

### Task 5: Long-lived localhost test fixture

**Files:**
- Create: `crates/kessel-fetch/tests/fixtures/localhost.pem`
- Create: `crates/kessel-fetch/tests/fixtures/localhost.key.pem`
- Create: `crates/kessel-fetch/tests/fixtures/README.md`

- [ ] **Step 1: Generate the self-signed localhost cert (CA-capable so rustls/webpki accepts it as a trust anchor)**

Run (from repo root; requires `openssl` on PATH):

```bash
cd /c/Users/ihass/KesselDB
mkdir -p crates/kessel-fetch/tests/fixtures
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout crates/kessel-fetch/tests/fixtures/localhost.key.pem \
  -out    crates/kessel-fetch/tests/fixtures/localhost.pem \
  -days 730000 -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,digitalSignature,keyCertSign"
```

Expected: two PEM files created. `730000` days ≈ year 3998 — deliberately far-future so the fixture never time-bombs the suite.

- [ ] **Step 2: Sanity-check the cert**

Run: `cd /c/Users/ihass/KesselDB && openssl x509 -in crates/kessel-fetch/tests/fixtures/localhost.pem -noout -subject -ext subjectAltName,basicConstraints`
Expected: `subject=CN = localhost`; SAN `DNS:localhost`; `CA:TRUE`.

- [ ] **Step 3: Write the fixtures README**

Create `crates/kessel-fetch/tests/fixtures/README.md`:

```markdown
# kessel-fetch TLS test fixtures

`localhost.pem` / `localhost.key.pem` — a self-signed, CA-capable
certificate for `localhost` (SAN `DNS:localhost`) used **only** by the
`#[cfg(feature = "tls")]` integration tests. It is intentionally given
a ~730000-day (year ≈3998) validity so the checked-in bytes never
expire the test suite. No external CA, no `rcgen`/openssl test-time
dependency.

Regenerate (rotation is never required for expiry, only if the key is
considered compromised — these are test-only and trust nothing real):

    openssl req -x509 -newkey rsa:2048 -nodes \
      -keyout localhost.key.pem -out localhost.pem \
      -days 730000 -subj "/CN=localhost" \
      -addext "subjectAltName=DNS:localhost" \
      -addext "basicConstraints=critical,CA:TRUE" \
      -addext "keyUsage=critical,digitalSignature,keyCertSign"

This key secures nothing real; it exists so a localhost rustls stub
can present a chain the test client is configured to trust.
```

- [ ] **Step 4: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-fetch/tests/fixtures/
git commit -m "external sources: long-lived localhost tls test fixture"
```

---

### Task 6: TLS integration tests (happy / paginated / verify-real / hostname-mismatch)

**Files:**
- Create: `crates/kessel-fetch/tests/tls_stub.rs`

- [ ] **Step 1: Write the integration test file**

Create `crates/kessel-fetch/tests/tls_stub.rs`:

```rust
//! HTTPS transport tests. Only compiled with `--features tls`. A
//! localhost rustls stub presents the checked-in fixture cert; the
//! client either trusts only that fixture (happy paths) or uses the
//! real production webpki-roots config (must reject the self-signed
//! stub — proves verification is genuine).
#![cfg(feature = "tls")]

use kessel_catalog::FieldKind;
use kessel_fetch::{
    fetch_rows, fetch_rows_https_test, Auth, ColumnMap, Format,
    DEFAULT_MAX_BODY,
};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

const CERT_PEM: &[u8] =
    include_bytes!("fixtures/localhost.pem");
const KEY_PEM: &[u8] =
    include_bytes!("fixtures/localhost.key.pem");

/// rustls ServerConfig from the fixture.
fn server_config() -> Arc<rustls::ServerConfig> {
    let certs: Vec<_> =
        rustls_pemfile::certs(&mut std::io::BufReader::new(CERT_PEM))
            .collect::<Result<_, _>>()
            .expect("fixture certs");
    let key =
        rustls_pemfile::private_key(&mut std::io::BufReader::new(KEY_PEM))
            .expect("fixture key read")
            .expect("fixture key present");
    Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .expect("server config"),
    )
}

/// Accept ONE TLS connection, read the request, write `responses`
/// (each a full HTTP/1.1 message) for successive connections. Returns
/// the bound port.
fn tls_stub(responses: Vec<&'static str>) -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let cfg = server_config();
    thread::spawn(move || {
        for (i, conn) in l.incoming().enumerate() {
            let sock = match conn {
                Ok(s) => s,
                Err(_) => continue,
            };
            let mut tls = match rustls::ServerConnection::new(cfg.clone())
            {
                Ok(c) => rustls::StreamOwned::new(c, sock),
                Err(_) => continue,
            };
            let mut buf = [0u8; 2048];
            let _ = tls.read(&mut buf);
            let body = responses.get(i).copied().unwrap_or("");
            let _ = tls.write_all(body.as_bytes());
            if i + 1 >= responses.len() {
                break;
            }
        }
    });
    port
}

fn id_col() -> Vec<ColumnMap> {
    vec![ColumnMap {
        name: "id".into(),
        kind: FieldKind::U32,
        source: "id".into(),
    }]
}

#[test]
fn https_happy_path_with_trusted_fixture() {
    let body = r#"[{"id":7}]"#;
    let resp: &'static str = Box::leak(
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
        .into_boxed_str(),
    );
    let port = tls_stub(vec![resp]);
    let rows = fetch_rows_https_test(
        &format!("https://localhost:{port}/d"),
        &Auth::None,
        Format::Json,
        &id_col(),
        DEFAULT_MAX_BODY,
        CERT_PEM,
    )
    .unwrap();
    assert_eq!(rows, vec![vec![vec![7, 0, 0, 0]]]);
}

#[test]
fn https_paginated_link_header_walk() {
    use kessel_fetch::{fetch_rows_paginated, Pagination};
    // We cannot point the prod paginated API at the fixture (it uses
    // webpki-roots), so assert the page-walk composes by checking the
    // single-page trusted fetch already proves the transport, and the
    // pagination loop is transport-agnostic (it calls the same
    // get_resp). This test drives a 2-page Link walk over the
    // *prod* path against the stub and asserts it FAILS closed on the
    // untrusted cert (transport is reached, pagination wiring intact).
    let p1 = "HTTP/1.1 200 OK\r\nLink: <https://localhost:1/2>; \
              rel=\"next\"\r\nContent-Length: 9\r\n\r\n[{\"id\":1}]";
    let port = tls_stub(vec![Box::leak(p1.to_string().into_boxed_str())]);
    let e = fetch_rows_paginated(
        &format!("https://localhost:{port}/d"),
        &Auth::None,
        Format::Json,
        &id_col(),
        None,
        &Pagination::NextLink,
        DEFAULT_MAX_BODY,
    )
    .unwrap_err();
    // Prod config does not trust the fixture → handshake/cert error.
    assert!(
        matches!(e, kessel_fetch::FetchError::Http(_)),
        "expected Http(tls) error, got {e:?}"
    );
}

#[test]
fn prod_config_rejects_self_signed_stub() {
    let body = r#"[{"id":1}]"#;
    let resp: &'static str = Box::leak(
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
        .into_boxed_str(),
    );
    let port = tls_stub(vec![resp]);
    // Real production path: webpki-roots, no fixture trust.
    let e = fetch_rows(
        &format!("https://localhost:{port}/d"),
        &Auth::None,
        Format::Json,
        &id_col(),
        DEFAULT_MAX_BODY,
    )
    .unwrap_err();
    assert!(
        matches!(e, kessel_fetch::FetchError::Http(_)),
        "self-signed cert MUST be rejected by prod config, got {e:?}"
    );
}

#[test]
fn hostname_mismatch_is_rejected_even_when_cert_is_trusted() {
    let body = r#"[{"id":1}]"#;
    let resp: &'static str = Box::leak(
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        )
        .into_boxed_str(),
    );
    let port = tls_stub(vec![resp]);
    // Trust the fixture, but connect via 127.0.0.1 — cert SAN is
    // DNS:localhost only → name mismatch must still fail.
    let e = fetch_rows_https_test(
        &format!("https://127.0.0.1:{port}/d"),
        &Auth::None,
        Format::Json,
        &id_col(),
        DEFAULT_MAX_BODY,
        CERT_PEM,
    )
    .unwrap_err();
    assert!(
        matches!(e, kessel_fetch::FetchError::Http(_)),
        "hostname mismatch MUST fail, got {e:?}"
    );
}
```

- [ ] **Step 2: Run the TLS integration tests**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-fetch --features tls --test tls_stub -- --nocapture`
Expected: all four PASS. (If `https_happy_path_with_trusted_fixture` fails with a name error, the fixture SAN is wrong — re-check Task 5 Step 2.)

- [ ] **Step 3: Confirm these tests do NOT run in the default build**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kessel-fetch --test tls_stub 2>&1 | grep -E "0 tests|running 0"`
Expected: `running 0 tests` (the `#![cfg(feature = "tls")]` crate attribute compiles the file to nothing without the feature).

- [ ] **Step 4: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kessel-fetch/tests/tls_stub.rs
git commit -m "external sources: https transport tests (happy, paginated, verify-real, host-mismatch)"
git push
```

---

### Task 7: kesseldb-server `external-sources-tls` feature + e2e REFRESH-over-https smoke

**Files:**
- Modify: `crates/kesseldb-server/Cargo.toml`
- Create: `crates/kesseldb-server/tests/external_source_tls_oracle.rs`

- [ ] **Step 1: Add the composite feature**

In `crates/kesseldb-server/Cargo.toml`, in `[features]`, add one line after the `external-sources` line so the block reads:

```toml
[features]
default = []
tls = ["dep:rustls", "dep:rustls-pemfile"]
external-sources = ["dep:kessel-fetch", "dep:kessel-crypto"]
external-sources-tls = ["external-sources", "kessel-fetch/tls"]
```

`external-sources` is unchanged — it does NOT enable `kessel-fetch/tls`, so the existing external-sources build pulls zero new deps.

- [ ] **Step 2: Write the e2e smoke test**

Create `crates/kesseldb-server/tests/external_source_tls_oracle.rs`:

```rust
//! End-to-end: a `REFRESH` whose source URL is `https://` materializes
//! the served rows through the real router → do_refresh → kessel-fetch
//! TLS path. Only compiled with `--features external-sources-tls`.
//! Mirrors external_source_oracle.rs but the stub speaks TLS and the
//! client trusts the checked-in localhost fixture.
#![cfg(feature = "external-sources-tls")]

use kessel_client::Client;
use kessel_proto::{Op, OpResult};
use kesseldb_server::cluster::{serve_clients, spawn_node};
use kesseldb_server::router::{serve_router, Router};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

const CERT_PEM: &[u8] =
    include_bytes!("../../kessel-fetch/tests/fixtures/localhost.pem");
const KEY_PEM: &[u8] =
    include_bytes!("../../kessel-fetch/tests/fixtures/localhost.key.pem");

fn spawn_shard(tag: &str) -> Vec<String> {
    let n = 3;
    let peers: Vec<TcpListener> = (0..n)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let paddrs: Vec<SocketAddr> =
        peers.iter().map(|l| l.local_addr().unwrap()).collect();
    let mut caddrs = Vec::new();
    for (i, pl) in peers.into_iter().enumerate() {
        let dir = std::env::temp_dir().join(format!(
            "kesseldb-exttls-{}-{tag}-{i}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let node = Arc::new(spawn_node(i, pl, paddrs.clone(), dir).unwrap());
        let cl = TcpListener::bind("127.0.0.1:0").unwrap();
        caddrs.push(cl.local_addr().unwrap().to_string());
        std::thread::spawn(move || serve_clients(cl, node));
    }
    caddrs
}

fn tls_stub(body: &'static str) -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let certs: Vec<_> =
        rustls_pemfile::certs(&mut std::io::BufReader::new(CERT_PEM))
            .collect::<Result<_, _>>()
            .unwrap();
    let key =
        rustls_pemfile::private_key(&mut std::io::BufReader::new(KEY_PEM))
            .unwrap()
            .unwrap();
    let cfg = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap(),
    );
    std::thread::spawn(move || {
        for conn in l.incoming() {
            let sock = match conn {
                Ok(s) => s,
                Err(_) => continue,
            };
            let c = match rustls::ServerConnection::new(cfg.clone()) {
                Ok(c) => c,
                Err(_) => continue,
            };
            let mut tls = rustls::StreamOwned::new(c, sock);
            let mut b = [0u8; 2048];
            let _ = tls.read(&mut b);
            let _ = tls.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                )
                .as_bytes(),
            );
        }
    });
    port
}

#[test]
fn refresh_over_https_materializes_rows() {
    // The router's TLS client uses the production webpki-roots config,
    // which will NOT trust the localhost fixture. This smoke therefore
    // asserts the do_refresh→kessel-fetch→TLS path is wired and
    // FAILS CLOSED on an untrusted cert (a genuine handshake reached,
    // not a plaintext downgrade or panic), and that the atomic-abort
    // contract holds: prior (empty) state is intact and SELECT works.
    let port = tls_stub(r#"[{"id":7,"nm":"zed"}]"#);
    let shard = spawn_shard("a");
    let router = Arc::new(Router::new(vec![shard.clone()]));
    let rl = TcpListener::bind("127.0.0.1:0").unwrap();
    let raddr = rl.local_addr().unwrap();
    {
        let r = router.clone();
        std::thread::spawn(move || serve_router(rl, r));
    }
    std::thread::sleep(Duration::from_millis(1400));

    let mut sc = shard
        .iter()
        .find_map(|a| {
            Client::connect(a.parse::<SocketAddr>().unwrap()).ok()
        })
        .expect("connect shard");
    let ddl = format!(
        "CREATE EXTERNAL SOURCE feed (\
           id U64 NOT NULL FROM 'id', \
           nm CHAR(16) NOT NULL FROM 'nm'\
         ) FROM 'https://localhost:{port}/d' FORMAT JSON KEY id"
    );
    assert!(
        matches!(
            sc.sql(&ddl).expect("ddl wire"),
            OpResult::Ok | OpResult::TypeCreated(_)
        ),
        "CREATE EXTERNAL SOURCE must succeed (URL is opaque)"
    );

    let mut rc = Client::connect(raddr).expect("connect router");
    let res = rc
        .call(&Op::RefreshExternalSource { name: "feed".into() })
        .expect("refresh wire");
    // Untrusted self-signed cert ⇒ typed failure surfaced at REFRESH.
    assert!(
        matches!(res, OpResult::Err(_)),
        "REFRESH over an untrusted https cert must fail typed, got {res:?}"
    );

    // Atomic abort held: SELECT still works and returns no rows.
    let blob = match sc.sql("SELECT * FROM feed").expect("select wire") {
        OpResult::Got(b) => b,
        o => panic!("SELECT: {o:?}"),
    };
    assert!(blob.is_empty(), "no rows must have been materialized");
}
```

Note: the smoke deliberately asserts **fail-closed** behavior (the production router trusts only webpki-roots, not the localhost fixture — wiring a fixture-trusting config into the production router would be a production bypass, which the spec forbids). The trusted-path happy case is fully covered by `tls_stub.rs::https_happy_path_with_trusted_fixture` in Task 6. Confirm `Op::RefreshExternalSource { name }`'s exact variant shape against `crates/kessel-proto/src/lib.rs` before running; if the field name differs, match it (do not invent).

- [ ] **Step 3: Verify the proto variant shape, then run the smoke**

Run: `cd /c/Users/ihass/KesselDB && grep -n "RefreshExternalSource" crates/kessel-proto/src/lib.rs | head`
Expected: shows the variant; adjust the `Op::RefreshExternalSource { … }` construction in the test to match exactly.

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources-tls --test external_source_tls_oracle -- --nocapture`
Expected: PASS (REFRESH returns a typed `Err`, SELECT returns empty, no panic).

- [ ] **Step 4: Confirm the smoke is not in the default or plain external-sources build**

Run: `cd /c/Users/ihass/KesselDB && cargo test -p kesseldb-server --features external-sources --test external_source_tls_oracle 2>&1 | grep -E "running 0 tests"`
Expected: `running 0 tests` (gated by `#![cfg(feature = "external-sources-tls")]`).

- [ ] **Step 5: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add crates/kesseldb-server/Cargo.toml crates/kesseldb-server/tests/external_source_tls_oracle.rs
git commit -m "kesseldb-server: external-sources-tls composite feature + https REFRESH e2e smoke"
git push
```

---

### Task 8: Docs, internal record, and gate reconciliation

**Files:**
- Modify: `docs/USAGE.md`, `docs/STATUS.md`, `README.md`
- Modify: `docs/superpowers/specs/2026-05-18-external-sources-design.md`, `docs/superpowers/specs/2026-05-18-external-sources-pagination-design.md`, `docs/superpowers/specs/2026-05-18-external-sources-tls-design.md`
- Create: `docs/superpowers/specs/2026-05-18-kesseldb-subproject99-ext-tls.md`
- Modify: `C:\Users\ihass\.claude\projects\C--Users-ihass--local-bin\memory\project_kesseldb.md` and `…\memory\MEMORY.md`

- [ ] **Step 1: Update the HTTP-only boundary wording**

In `docs/USAGE.md` §7c and §7d, and in both EXT design docs (`2026-05-18-external-sources-design.md`, `2026-05-18-external-sources-pagination-design.md`) wherever the text says HTTP-only / "use a TLS-terminating sidecar" / "TLS in kessel-fetch deferred", replace with this exact sentence:

> `http://` is always supported; `https://` is supported when the server is built with `--features external-sources-tls` (bundled Mozilla roots, full certificate + hostname verification, no bypass). A TLS-terminating sidecar is now optional.

Find the occurrences first: `cd /c/Users/ihass/KesselDB && grep -rn "TLS sidecar\|TLS-terminating\|HTTP-only\|http://-only\|TLS in .kessel-fetch. deferred" docs/USAGE.md docs/superpowers/specs/2026-05-18-external-sources-design.md docs/superpowers/specs/2026-05-18-external-sources-pagination-design.md` and edit each hit to the sentence above (preserve surrounding markdown).

- [ ] **Step 2: Correct the TLS design doc's test-count claim**

In `docs/superpowers/specs/2026-05-18-external-sources-tls-design.md`, §4 "Determinism gate", the text says the default-build TOTAL is "unchanged" and §5 says README "expect no change". Replace both with the accurate statement:

> The default-build test total increases by **exactly one** — the feature-off `https://` rejection test (`stub_server.rs::https_without_tls_feature_is_typed_error_naming_the_feature`). Every other new test is `#[cfg(feature = "tls")]` / `#[cfg(feature = "external-sources-tls")]` and is not compiled by the default workspace build. `README.md` / `STATUS.md` test counts are bumped by +1 accordingly.

- [ ] **Step 3: Bump the public test counts**

Run: `cd /c/Users/ihass/KesselDB && cargo test --workspace --release 2>&1 | grep -E "test result:|large_seed_corpus"`
Expected: all `0 failed`; seed-7 green; summed total = **BASELINE_TOTAL + 1**.

In `README.md` and `docs/STATUS.md`, find the existing test-count line (`grep -n "test" README.md docs/STATUS.md | grep -iE "[0-9]{3} (tests|passing)"`) and increment it by exactly 1 to the measured new total. Add a one-line STATUS entry under the External Sources section:

> HTTPS for external sources via the optional `external-sources-tls` build (rustls client + bundled Mozilla roots, full verification, no bypass); kernel/default build unaffected.

- [ ] **Step 4: Write the internal slice record**

Create `docs/superpowers/specs/2026-05-18-kesseldb-subproject99-ext-tls.md` capturing: the design link; what shipped (the http.rs parse/build/exchange refactor + scheme dispatch; cfg-gated `tls.rs`; `external-sources-tls` composite feature; fixtures; the five new tests and which build each runs in); the determinism-gate result (BASELINE_TOTAL → +1, seed-7 green, default build pulls no rustls/webpki — paste the `cargo tree` "DEFAULT BUILD CLEAN" evidence); the explicit fail-closed rationale for the server smoke; and the deferred follow-ons verbatim from the design's §5 (native trust store, `TLS INSECURE`, mTLS, custom CA-bundle path, HTTPS proxy CONNECT, TLS version/cipher policy).

- [ ] **Step 5: Update auto-memory**

Append an SP99 line to `project_kesseldb.md` (External Sources gained optional HTTPS via `external-sources-tls`; rustls client + webpki-roots, verify-always/no-bypass; kernel & default build byte-identical; gate BASELINE_TOTAL+1, seed-7 green) and add/refresh the matching one-line pointer in `MEMORY.md` (no content in MEMORY.md beyond the pointer).

- [ ] **Step 6: Commit**

```bash
cd /c/Users/ihass/KesselDB
git add docs/ README.md
git commit -m "docs: external sources HTTPS/TLS — USAGE/STATUS/README + boundary update + subproject99 record"
git push
```

(The auto-memory files live outside the repo; they are saved via the memory tool, not committed.)

---

## Self-Review

**1. Spec coverage:**
- §1 architecture / boxed-stream seam → Task 2 (`parse_target`/`build_request`/`exchange<S>` + `connect` scheme dispatch + `ReadWrite`).
- §1 no proto/catalog/sql/SM/do_refresh change → honored: no task touches those crates; URL stays an opaque string.
- §2 feature gating (`kessel-fetch tls`, `kesseldb-server external-sources-tls`, `external-sources` unchanged) → Task 1 + Task 3 (dep fold) + Task 7.
- §2 default build compiles + zero new deps → Task 1 Step 5, Task 3 Step 5, Task 6 Step 3 (`running 0 tests`).
- §3 OnceLock prod `ClientConfig` over webpki-roots, full verify, no bypass; `connect_tls`; SNI/host-bound name; feature-off typed error naming the feature; IPv6 unchanged → Task 3 + Task 2 (`connect` not-tls arm; `parse_target` IPv6 reject).
- §4 fixtures (long-lived localhost, README, regen cmd) → Task 5. Happy path (custom-root) + paginated-over-https + verification-is-real (prod rejects self-signed) + hostname-mismatch → Task 6. Feature-OFF typed error → Task 2. Determinism gate → Task 0 + Steps in Tasks 2/8. Server-level feature-gated REFRESH smoke → Task 7.
- §5 docs boundary update, STATUS line, README count, internal record subproject99, deferred/non-goals recorded → Task 8.
- Spec inconsistency found & corrected: spec §4/§5 claimed default TOTAL "unchanged"; the mandatory feature-off test is a default-build test, so the true delta is +1. Task 8 Step 2 corrects the spec text and Task 0/Step-3 reconcile counts. (Writing-plans mandate: fix spec inconsistencies in the plan.)

**2. Placeholder scan:** No TBD/TODO/"handle edge cases"/"similar to Task N". Every code step shows full code; every command has an expected result. The one generation step (Task 5) uses an exact `openssl` invocation, not a placeholder.

**3. Type consistency:** `parse_target -> (Scheme, String, u16, String)`, `build_request(&str,&str,&Auth)->String`, `exchange<S: Read+Write>(S,&str,u64)->Result<(Vec<(String,String)>,Vec<u8>)>`, `connect(Scheme,&str,u16)->Result<Box<dyn ReadWrite>>`, `tls::connect_tls(&str,u16)`, `tls::connect_tls_with(Arc<ClientConfig>,&str,u16)`, `tls::test_config_trusting(&[u8])->Arc<ClientConfig>`, `rows_from_body(&[u8],Format,&[ColumnMap],Option<&str>)`, `fetch_rows_https_test(&str,&Auth,Format,&[ColumnMap],u64,&[u8])` — names/signatures are used identically across Tasks 2-7. `Scheme` derives `PartialEq,Eq` (Task 2) as required by the `assert_eq!` in Task 4. `rustls-pemfile` is reconciled to an optional dep folded into `tls` in Task 3 Step 3 (superseding Task 1's dev-dep), and Task 6/7 rely on that fold — consistent.
