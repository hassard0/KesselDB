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
