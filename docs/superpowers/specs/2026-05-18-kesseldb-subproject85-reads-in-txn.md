# KesselDB Sub-project 85 — reads in a transaction (honest reclassification)

**Date:** 2026-05-18  **Status:** shipped. Resolves the "SELECT inside
a transaction is buffered, not executed" deferral — by correcting it,
not faking it.

## Finding

`Storage::scan_range` is **already overlay-aware** (since SP25): an
in-flight transaction's buffered writes are visible to range scans, and
SP84 proved read-your-writes for *mutations* within an `Op::Txn` batch
(an `UpdateSet` sees a `Create` made earlier in the same batch).

The remaining "deferred" item — interactive `SELECT` mid-`BEGIN`/
`COMMIT` returning its rows with read-your-writes — is **not a missing
implementation; it is a deliberate model boundary**. KesselDB
transactions are atomic, *non-interactive* write batches (serializable
by construction; the SP9 spec lists "no interactive/long-lived
transactions" as an explicit non-goal). Returning interactive
read-your-writes mid-transaction would require holding the single
engine overlay across client round-trips, serializing the whole engine
— which contradicts the non-blocking single-writer core. Implementing
it would be the wrong thing, like making TLS a non-opt-in default
would violate the zero-dep North Star.

## Change (honest behaviour + correct docs, no fake interactivity)

- A `SELECT`/`DESCRIBE`/`EXPLAIN` issued inside `BEGIN`/`COMMIT` now
  returns a **clear error** instead of a silent buffered `Ok` whose
  rows are discarded. Writes are still buffered for the atomic commit.
- `docs/USAGE.md` reclassified: this is a *by-design* model boundary
  (atomic non-interactive write batch), not a TODO; read-your-writes
  is documented as holding for writes within the batch; the
  `UPDATE … SET col = NULL`-in-txn boundary (SP84) is noted.

## Verified

`kesseldb-server::reads_in_txn_rejected_writes_read_your_writes`: a
`SELECT`/`DESCRIBE` inside `BEGIN`/`COMMIT` is a clean `SchemaError`;
`ROLLBACK` discards the buffered writes; and an `UPDATE` that depends
on an `INSERT` made earlier in the **same** transaction commits with
the dependent value (read-your-writes for writes within the batch).
The pre-existing `sql_transactions_are_atomic` is unaffected. 189
green; determinism / VSR partition corpus (incl. seed 7) unchanged.

## Honest boundary

Interactive transactions / mid-transaction reader isolation are a
deliberate non-goal of the atomic-batch model — explicitly documented,
not hidden, and not faked.
