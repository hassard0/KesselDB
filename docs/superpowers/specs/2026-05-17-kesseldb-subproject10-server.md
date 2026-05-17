# KesselDB Sub-project 10 — Runnable TCP server + client

**Date:** 2026-05-17  **Status:** spec + build (autonomous continuation)
**Builds on:** SP1–SP9. Turns the engine from a set of crates into an
actually-runnable database.

## Goal

A `kesseldb` binary that listens on TCP and serves the full op set with real
fsync, plus a `kessel-client` library, plus an end-to-end socket test.

## Design

- **Wire framing** (`kessel_proto::wire`): `[u32 LE len][payload]`. Request
  payload = `Op::encode()`, response = `OpResult::encode()` (added in this
  SP — full round-trip tested).
- **Single owning engine thread.** `StateMachine<DirVfs>` holds
  `Box<dyn Disk>` (not `Send`), so it is created on and never leaves one
  thread. Connection threads submit `(Op, oneshot)` over a channel; the
  engine applies serially (op_number monotonic) and replies. This is not a
  workaround — it *is* the single-threaded deterministic core, with the
  network as the only concurrent edge.
- **`kesseldb` binary**: `kesseldb [addr] [data_dir]` (defaults
  `127.0.0.1:7878`, `./kesseldb-data`). Real `DirVfs` ⇒ real fsync /
  crash recovery.
- **`kessel-client`**: `Client::connect(addr)` + `call(&Op) -> OpResult`.

## Scope / non-goals (honest)

- Single-node only. Multi-node VSR-over-sockets is still deferred (the
  protocol/`kessel-vsr` is transport-agnostic; wiring it to real sockets is
  a later spec). Documented, not hidden.
- No auth/TLS/back-pressure/connection limits — it's a usable engine
  endpoint, not a hardened public server.
- One op per request/response (batching is available via `Op::Txn`).

## Tests

`kessel-proto`: `opresult_roundtrip_all_variants`.
`kesseldb-server`: `end_to_end_over_real_sockets` — real TCP + real fsync:
CreateType, Create, GetById, Exists, a second connection observing committed
state, and an atomic `Op::Txn` over the wire. `kesseldb.exe` builds (362 KB).
91 tests total green.
