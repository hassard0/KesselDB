# KesselDB Sub-project 74 — DROP INDEX

**Date:** 2026-05-18  **Status:** shipped. 175 green. First of the
non-gating-roadmap items.

`Op::DropIndex { type_id, fields }` (wire kind 30, encoded like
`AddCompositeIndex`). SQL: `DROP INDEX ON <t> (cols)`.

- One column ⇒ drops its equality index (and the `UNIQUE` flag) and/or
  its range index; multiple ⇒ the composite index whose field list
  matches exactly.
- Index LSM entries are deleted by a prefix range scan over the
  reserved index keyspace (`0xFFFE` equality/composite, `0xFFFD`
  ordered) and the catalog is updated + persisted (bumps the
  compile-cache epoch).
- A dropped **composite** slot is *emptied, not removed* — composite
  entries are keyed by slot index, so removing the `Vec` would
  renumber later composites and orphan their keys; `idx_maintain` now
  skips empty slots.
- `NotFound` if there is no such index (clean, not a crash, not a
  false `Ok`).

**Correctness:** dropping an index never changes a query's answer — the
planner falls back to a verified scan and the `QueryRows`
program-verify invariant guarantees the same rows. The test asserts
identical results for equality, range/band and composite queries
before vs after the drop, idempotent `NotFound`, re-`CREATE INDEX`
still correct, and that the operation is deterministic (two identical
histories ⇒ identical digest). Determinism / VSR partition corpus
(incl. seed 7) unchanged. `DropIndex` is DDL — rejected inside `Txn`
like the other schema ops.

**Honest boundary:** drops *secondary* indexes only (the primary-key
keyspace is the table itself — that is `DROP TABLE`). Emptied composite
slots retain a catalog slot (inert) rather than compacting the list, by
design.
