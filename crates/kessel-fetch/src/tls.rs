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
