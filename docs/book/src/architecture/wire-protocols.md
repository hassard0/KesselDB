# Wire protocols

KesselDB exposes the same `Op` apply path through four wire surfaces:

- **Binary** on the primary port — deterministic fast path, default
  `cargo build`, no external deps; length-prefixed `[u32 LE
  len][payload]` frames.
- **HTTP/1.1** with `--features http-gateway` — `/v1/sql`, `/v1/op`,
  `/v1/health`, `/v1/metrics`. JSON responses, Prometheus metrics.
- **WebSocket** with `--features http-gateway` (same crate, same
  feature) — `/v1/ws` upgrade, `kessel-op-v1` subprotocol, binary
  frames carrying `Op::encode()`.
- **PostgreSQL Frontend/Backend v3.0** with `--features pg-gateway` —
  Simple Query + SCRAM-SHA-256 + Bearer↔SCRAM bridge; `pg_catalog` +
  `information_schema` stubs (SP-PG-CAT) so pgAdmin, DBeaver,
  DataGrip, Metabase, Tableau connect + browse out of the box.

Per-listener `max_conns` caps mean a saturated gateway can never starve
the binary protocol. The shared engine `max_inflight` bounds total
in-flight ops across listeners.

Full reference: [Architecture → Wire protocol gateways](overview.md#wire-protocol-gateways).
Wire-shape constants and admin-frame tags: [Wire protocol](../reference/wire-protocol.md).
