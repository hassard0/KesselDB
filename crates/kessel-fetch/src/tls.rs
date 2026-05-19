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
#[allow(dead_code)] // used by integration tests in Task 6 (tls_stub.rs)
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
}
