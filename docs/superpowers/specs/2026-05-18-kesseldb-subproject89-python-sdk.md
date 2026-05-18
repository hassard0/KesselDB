# KesselDB Sub-project 89 — dependency-free Python reference SDK

**Date:** 2026-05-18  **Status:** shipped. Turns the "client SDKs
beyond Rust" item from a deferral into a delivered reference.

The zero-external-dependency North Star is about the *engine*; a
separate-language client legitimately uses that language's stdlib. The
wire protocol (USAGE §10) is intentionally tiny, so this is a faithful,
single-file, stdlib-only client — and the template for any other
language.

## What

`clients/python/kesseldb.py` — Python 3, standard library only:

- length-prefixed framing (`[u32 LE len][payload]`), SQL request
  (`0xFE ++ utf8`), optional token auth (`0xFC ++ token`);
- a full `OpResult` decoder for every wire tag (Ok/Got/Exists/
  NotFound/TypeCreated/SchemaError/Constraint/Unavailable/
  Unauthorized), with the common 16-byte scalar decoded to an int;
- `connect("host:port"[, token=...])` → `Client.sql(str) -> OpResult`,
  context-manager support, `TCP_NODELAY` (same Nagle fix as the Rust
  client);
- a `__main__` one-shot mode with the same reliable exit codes as the
  `kessel` CLI (0 ok / 1 error / 2 usage).

## Verified

`kesseldb-server::python_sdk_round_trips_over_the_wire` — finds
`python3`/`python` on PATH; if present, drives the **whole loop
through the Python SDK** over real sockets (CREATE, two INSERTs,
`SELECT SUM` → asserts the decoded scalar `= 1049`, and a bad
statement → non-zero exit + `ERROR` line). If no Python is on PATH it
**skips cleanly** (test still passes) so CI is green everywhere; where
Python exists it is a real cross-language end-to-end check. Confirmed
green here against Python 3.11.

Docs: README documentation table + USAGE §3 now point at the SDK with
a runnable example; STATUS no longer lists "client SDKs beyond Rust"
as out of scope.

## Honest boundary

It implements the SQL/auth surface (what a client overwhelmingly
needs); raw `Op::encode` requests, `0xFD` session frames (exactly-once
failover) and the admin frames are deliberately not wrapped — the
documented protocol is right there and the Rust `kessel-client`
remains the full-surface client. Further-language SDKs are
straightforward over §10 and welcome, but not tracked as deferred
work.
