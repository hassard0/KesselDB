# KesselDB Sub-project 9 — Atomic multi-op transactions

**Date:** 2026-05-17  **Status:** spec + build (autonomous continuation)
**Builds on:** SP1–SP8. Adds the missing core DB primitive: ACID
all-or-nothing across a batch — the substrate for TigerBeetle-style linked
transfers, but general.

## Goal

`Op::Txn { ops }` — apply every inner op all-or-nothing. Any failure
(`Exists`/`NotFound`/`SchemaError`/`Constraint`) rolls the **entire** batch
back with zero trace and returns the failing op's result. Replicated as a
single op, so every replica commits-or-rolls-back identically.

## Design

- **Storage transaction overlay.** `begin_txn` installs an in-memory overlay;
  put/delete buffer there (NOT WAL/memtable); reads consult it first
  (read-your-writes). `commit_txn` appends every buffered entry to the WAL
  with **one fsync** then makes them visible (crash-consistent: a torn WAL
  tail loses the whole batch). `abort_txn` just drops the overlay — nothing
  reached the WAL/memtable so there is literally nothing to undo.
- **State machine.** `Op::Txn` rejects DDL and nested txns up front (the
  overlay deliberately does not cover the catalog or `scan_range`, so only
  data ops + reads may participate). It begins the storage txn, applies each
  inner op via the normal `apply` path (so constraints, indexes, triggers,
  overflow all compose and all route through the overlay), aborts on the
  first failure, else commits.
- **Cache safety.** On abort the read cache is cleared (entries written
  during the txn referenced uncommitted overlay values).
- **Determinism.** Overlay ops are deterministic; the whole `Op::Txn` is one
  replicated entry, so replicas converge (VSR test with deliberately
  colliding txns that must roll back uniformly on all 3 nodes).

## Scope / non-goals (honest)

- Data ops only inside a txn (Create/Update/Delete + reads). DDL/schema
  changes and nested transactions are rejected (documented).
- Single-shard (matches current single-shard deployment); cross-shard
  atomicity remains the documented sharding limitation.
- No interactive/long-lived transactions or isolation levels beyond
  "the batch is one atomic step" (the state machine is serial, so the
  effective isolation is serializable by construction).

## Tests

`kessel-storage`: `txn_is_atomic_commit_and_abort` (read-your-writes, abort
trace-free, commit durable across reopen).
`kessel-sm`: `txn_commits_all_or_nothing`, `txn_rolls_back_on_midbatch_failure`
(data + index clean after rollback), `txn_rejects_ddl_and_nested`,
`txn_is_deterministic` (400 random txns).
`kessel-vsr`: `atomic_txn_replicates_and_converges`. 89 tests total green.
