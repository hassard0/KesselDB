# kessel-fetch TLS test fixtures

`localhost.pem` / `localhost.key.pem` — a self-signed certificate for
`localhost` (SAN `DNS:localhost`) used **only** by the
`#[cfg(feature = "tls")]` integration tests. It is a `CA:FALSE`
end-entity leaf, self-signed and added directly to the test client's
trust store (rustls rejects a `CA:TRUE` cert used as a server leaf).
It is intentionally given a ~730000-day (year 4025) validity so the
checked-in bytes never expire the test suite. No external CA, no
`rcgen`/openssl test-time dependency.

Regenerate (rotation is never required for expiry, only if the key is
considered compromised — these are test-only and trust nothing real):

    openssl req -x509 -newkey rsa:2048 -nodes \
      -keyout localhost.key.pem -out localhost.pem \
      -days 730000 -subj "/CN=localhost" \
      -addext "subjectAltName=DNS:localhost" \
      -addext "basicConstraints=critical,CA:FALSE" \
      -addext "keyUsage=critical,digitalSignature,keyEncipherment"

This key secures nothing real; it exists so a localhost rustls stub
can present a chain the test client is configured to trust. This
self-signed, localhost-only pattern is for this test fixture
exclusively and must NOT be used for production or staging certificates.
