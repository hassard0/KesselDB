# WebSocket gateway

`GET /v1/ws` upgrade, `kessel-op-v1` subprotocol, RFC 6455 strict
handshake. Each binary frame is one `Op::encode()` request; the server
replies one `OpResult::encode()`. Bounded send queue (16 messages),
30 s ping/pong heartbeat, 30 s idle close. Bearer auth checked once at
handshake.

Ships under the same `--features http-gateway` flag — there is no
separate `ws-gateway` feature; the WS arm lives in the same
`kessel-http-gateway` crate.

Browser example, full wire-shape spec, backpressure model:
[Usage guide (full) §10 → WebSocket](full-usage.md#websocket-gateway-sp-ws).
