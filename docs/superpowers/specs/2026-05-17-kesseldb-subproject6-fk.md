# KesselDB Sub-project 6 — Foreign-key constraints

**Date:** 2026-05-17  **Status:** spec + build (autonomous continuation)
**Builds on:** SP1–SP5. Completes the classic constraint trio
(NOT NULL = SP4, UNIQUE = SP4, **FOREIGN KEY = SP6**).

## Goal

`Op::AddForeignKey { type_id, field_id, ref_type_id }` — the field's value
(padded to 16 bytes) must be an existing object id of `ref_type_id` at
write time. Validates current data before enabling; rejects via
`OpResult::Constraint`.

## Design

- **Catalog:** `ObjectType.fks: Vec<(field_id, ref_type_id)>`, persisted in
  the replicated catalog.
- **Enforcement** (Create/Update, after UNIQUE): for each FK, read the
  field bytes, pad to a 16-byte id, and require
  `storage.get(make_key(ref_type, id)).is_some()`. A read-only check against
  committed state ⇒ deterministic and replication-safe.
- **Codec-record scoped:** enforced only for well-formed codec records
  (`len == record_size` && `field_count == #fields`); raw/opaque writers opt
  out by construction (same documented scoping as NOT NULL). NULL FK fields
  are skipped (SQL-like).
- **`AddForeignKey`** validates every existing (codec, non-null) row's
  reference; if any is dangling it refuses and does **not** enable the
  constraint (no half-apply). Idempotent. Rejects `OverflowRef` fields.

## Scope / non-goals (honest)

- **No referential actions.** There is no `ON DELETE`/`ON UPDATE`
  cascade/restrict: deleting a parent does **not** cascade and is **not**
  blocked (FK is checked only when a child row is written). This is a
  documented limitation; cascade/restrict is future work.
- Single-field FK only (no composite FK).
- Codec-record scoped (raw writers opt out, as documented).

## Tests

`foreign_key_enforced_and_validates_existing` (clean enable, idempotent,
valid child Ok, missing-parent create + update rejected),
`add_foreign_key_rejects_existing_dangling` (refuses on pre-existing dangling
row, succeeds once fixed, then enforces),
`foreign_key_replicates_and_converges` (3-node VSR convergence). 67 tests
total green.
