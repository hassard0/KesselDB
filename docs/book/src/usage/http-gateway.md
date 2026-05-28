# HTTP gateway

Opt-in HTTP/1.1 surface for operators, browsers, and tools that prefer
HTTP/JSON. Build with `--features http-gateway`; add `,tls` for HTTPS.

Routes: `/v1/sql`, `/v1/op`, `/v1/health`, `/v1/metrics`
(Prometheus text v0.0.4). Authorization is `Bearer` with constant-time
comparison; exactly-once headers `X-Kessel-Client-Id` +
`X-Kessel-Req-Seq` are both-or-neither. Full route table, status-code
mapping, Prometheus metric names, and curl examples:
[Usage guide (full) §10](full-usage.md#10-http-gateway).

The binary wire protocol on the primary port is byte-untouched whether
the HTTP gateway runs or not.
