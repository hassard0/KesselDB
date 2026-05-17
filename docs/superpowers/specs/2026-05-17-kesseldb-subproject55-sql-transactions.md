# KesselDB Sub-project 55 — SQL `BEGIN` / `COMMIT` / `ROLLBACK`

**Date:** 2026-05-17  **Status:** shipped, tested. 151 green.
Production-feature-gap pass, slice 2.

## The gap

Atomic transactions existed at the op level (`Op::Txn`, SP9) but there
was **no SQL surface** — clients couldn't group statements
transactionally. A fully featured SQL database needs `BEGIN` /
`COMMIT` / `ROLLBACK`.

## Design (connection-scoped buffering)

Transaction state lives in the **connection handler**, not the engine, so
it is naturally per-connection and the single engine thread never blocks
on one client's open transaction:

- `BEGIN` / `START TRANSACTION` → start a per-connection statement buffer
  (reply `Ok`, no engine round-trip).
- Each subsequent SQL statement → appended to the buffer (reply `Ok`,
  deferred — like a queued statement).
- `COMMIT` → the handler builds one `TXN_TAG` (`0xF9`) frame
  (`[0xF9][u32 n]` then `n × [u32 len][utf8 SQL]`) and sends it to the
  engine, which compiles every statement against the live catalog and
  applies them as a **single atomic `Op::Txn`** (SP9 all-or-nothing). Any
  compile failure aborts the whole transaction with zero effect; a
  statement that needs server-side RMW (`UPDATE`) is rejected up front.
- `ROLLBACK` → discard the buffer (reply `Ok`); `COMMIT`/`ROLLBACK`
  without `BEGIN` → clean `SchemaError`.

Catalog-mutating statements inside the txn still bump `catalog_epoch` via
`persist_catalog`, so the compile caches stay correct.

## Test (1 new, 151 total)

`sql_transactions_are_atomic` (real socket + `Client`):

- `BEGIN; INSERT; INSERT; COMMIT` → both rows visible (atomic apply).
- `BEGIN; INSERT; ROLLBACK` → the row does **not** exist.
- `COMMIT` / `ROLLBACK` without `BEGIN` → `SchemaError`.
- **Atomicity**: a duplicate-id `INSERT` inside the txn makes `COMMIT`
  fail and the *other* buffered row (`id 4`) is **rolled back too**.
- The connection is still usable after an aborted transaction.

## Honest scope boundary

- `UPDATE` inside a transaction is **explicitly rejected** (it needs a
  server-side read-modify-write that must run within the txn overlay; the
  2-round RMW path doesn't compose with the batch yet). Named follow-up,
  not a silent gap — the error message says so.
- `SELECT` inside a transaction is buffered like any statement (no
  mid-transaction read-your-writes result is returned to the client);
  transactions are write-batch oriented for now.
- Single-node server (`handle_conn`). The cluster client front does not
  yet intercept transaction keywords — the documented, consistent
  cluster-follow-up boundary (cf. SP47→SP51). `Op::Txn` itself already
  works over the cluster for programmatic callers.
