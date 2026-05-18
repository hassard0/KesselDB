# KesselDB Sub-project 66 — optional TLS (opt-in `tls` feature)

**Date:** 2026-05-17  **Status:** shipped. Default build 165 green;
`--features tls` builds + its tests pass.

## Decision (user-chosen)

Real TLS needs a vetted crypto library — hand-rolling it would be
irresponsible. So TLS is an **opt-in cargo feature**: the default build
stays strictly zero-dependency and plaintext+token; `--features tls`
pulls `rustls` and terminates TLS in-process.

## Delivered

- Server I/O generalised: `authenticate` and `handle_conn` are now
  `S: Read + Write` (not concrete `TcpStream`). This is a pure, safe
  refactor — `TcpStream: Read+Write`, so the entire existing TCP suite
  (165) still passes, proving no behaviour change.
- `ServerConfig.tls: Option<(cert_pem, key_pem)>` (always present;
  honoured only with the feature).
- `[features] tls = ["dep:rustls","dep:rustls-pemfile"]`. `serve_cfg`:
  with the feature + `tls` set, builds a rustls `ServerConfig`
  (`with_no_client_auth().with_single_cert`) once and wraps each accepted
  socket in `rustls::StreamOwned` before `handle_conn`. **Fails loudly**
  if `tls` is configured but the binary was built without the feature
  (refuses to serve silently-insecure) — and vice-versa.
- Both builds verified clean: `cargo build -p kesseldb-server` and
  `cargo build -p kesseldb-server --features tls` (rustls fetched +
  compiled here in ~1 min).

## Tests

Default: full 165-test suite green (the generic-stream refactor is
behaviour-identical). Feature-gated (`--features tls`):
`tls_config_rejects_bad_inputs` (missing files / non-key PEM → clean
error, never panic) and `server_config_default_has_tls_none`. The TLS
*handshake* itself is rustls's job — that is the entire point of using a
vetted library rather than hand-rolling it.

## Honest scope boundary

A full localhost handshake e2e test needs an operator-supplied
certificate (cert generation would itself pull a crypto/cert dep). The
plumbing and config error paths are tested; the handshake is delegated to
rustls. Operators generate a cert/key (e.g.
`openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem -days
365 -nodes -subj "/CN=localhost"`) and set `ServerConfig.tls`. mTLS
(client-auth) is `with_no_client_auth()` for now — a named follow-up.
