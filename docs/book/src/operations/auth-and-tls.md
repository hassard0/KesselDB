# Authentication & TLS

KesselDB has **one credential surface**: a shared-secret Bearer token,
compared in constant time. The same token authorizes the binary wire
(`0xFC` handshake), HTTP (`Authorization: Bearer …`), WebSocket
(handshake-time), and the PostgreSQL wire (the token IS the SCRAM
password input). Rotating the token rotates every wire surface at once.

**TLS** is **opt-in** to preserve the zero-dependency default:

- `--features tls` — terminate TLS for the binary wire (rustls).
- `--features http-gateway,tls` — terminate HTTPS on
  `ServerConfig.http_tls_addr` (same rustls config).
- `--features external-sources-tls` — allow `https://` external
  sources (rustls + bundled Mozilla webpki roots; full certificate +
  hostname verification, no bypass).
- `--features external-sources-objstore` — implies `external-sources-tls`;
  every `s3://` and `az://` request is HTTPS-only.

The default plaintext binary wire is token-authenticated; deploy
behind a TLS-terminating reverse proxy, or on a private network
(WireGuard, tailnet, VPC) if you don't want the rustls dependency.

Full reference (auth config, connection quotas, backpressure):
[Usage guide (full) §8](../usage/full-usage.md#8-authentication-quotas--backpressure).
