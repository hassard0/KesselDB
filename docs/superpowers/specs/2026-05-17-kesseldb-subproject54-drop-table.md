# KesselDB Sub-project 54 — `DROP TABLE` (destructive DDL)

**Date:** 2026-05-17  **Status:** shipped, tested, e2e-verified. 150 green.
First slice of the production-feature-gap pass (DDL completeness).

## The gap

Only `CREATE` existed — no way to remove a table. A fully featured SQL
database must support destructive DDL. SP54 adds `DROP TABLE`.

## Delivered

- **`Op::DropType { type_id }`** (wire kind 29; encode/decode + roundtrip
  test). One deterministic, replicated op.
- **State-machine apply**:
  1. Unknown type → `NotFound` (clean, idempotent-ish).
  2. **Referential integrity** — if any *other* table still has a foreign
     key referencing this type, return `Constraint(...)` with **no
     effect** (you must drop the children first).
  3. Otherwise, atomically (own txn if not already in one): scan the
     type's row range, and for each row call the existing `idx_maintain`
     with `new = None` to remove its equality / ordered / composite index
     entries, then delete the row; finally drop the type from the catalog
     and `persist_catalog` (which bumps `catalog_epoch`, so the SP47/SP51
     compile caches invalidate automatically). Any error aborts the txn
     with zero effect.
- **SQL**: `DROP TABLE <name>` → resolves the name via the catalog →
  `Op::DropType`. Added to `kesseldb_server::mutates_schema` (kind 29) so
  the single-node prepared-statement cache invalidates too.

Determinism preserved: removal is driven purely by the op/flush stream;
index/row deletes and the catalog change are reflected in the digest, so
replicas converge — proven by the full VSR/determinism corpus staying
green (150, 0 failed).

## Tests (2 new + roundtrip, 150 total)

- `kessel-sm::drop_table_removes_rows_and_type_and_guards_fks`: drop
  removes the type (catalog gone, `DESCRIBE` → `NotFound`, name reusable);
  non-existent drop → `NotFound`; **FK guard** — a referenced parent
  refuses to drop (and is left intact) until the child is dropped first.
- `kessel-sql::drop_table_sql`: `DROP TABLE acct` compiles + applies
  `Ok`; subsequent compilation against the dropped name fails.
- `kessel-proto::op_roundtrip_all_variants` extended with `DropType`.
- E2E (CLI, live server): `CREATE` → `INSERT` → `DROP TABLE` (exit 0) →
  `SELECT … ` → `ERROR unknown table` (exit 1).

## Honest scope boundary

`DROP INDEX` is **not** included — an index has no first-class external
identity in the current grammar (indexes are declared by column), so a
correct `DROP INDEX` needs an index-naming/identity decision. Named as
the next DDL-completeness follow-up, not silently skipped. `DROP TABLE`
is the high-value, well-defined piece and is complete + tested.
