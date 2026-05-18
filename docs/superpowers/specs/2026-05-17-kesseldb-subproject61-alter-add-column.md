# KesselDB Sub-project 61 — `ALTER TABLE … ADD COLUMN` (online, no lock)

**Date:** 2026-05-17  **Status:** shipped, tested, e2e-verified. 159 green.
Production-feature-gap pass, slice 8. Also fixes a real correctness bug.

## The gap + the "wow"

The engine had online `Op::AlterTypeAddField` (no table lock, no row
rewrite — tested since M-series) but no SQL surface. SP61 adds
`ALTER TABLE <t> ADD [COLUMN] <name> <type>[(n)] [NOT NULL]`. The result
is a genuine differentiator: adding a column to a live table is **instant
and lock-free** — existing rows up-project the new column as `NULL`
(offsets of existing fields are invariant under appends), no full-table
rewrite like a naive `ALTER` would do.

## Delivered

- `kessel-sql`: an `ALTER` branch reusing the `CREATE TABLE` column
  grammar (`kind_of`, optional `(n)`, optional `NOT NULL`); `COLUMN` is an
  optional noise word. Compiles to `Op::AlterTypeAddField { type_id,
  encode_field(field) }`. The engine assigns the real field id and
  enforces the online-DDL rule that an added column must be nullable — a
  `NOT NULL` add surfaces as a clean `SchemaError` at apply (rule kept in
  one place, not duplicated).

## Correctness fix (honest — a real bug found and fixed)

Writing the test exposed a pre-existing bug: the **expr VM's**
`is_codec_record` used the heuristic `rec.len() == current_record_size &&
fc == fields.len()`. After `ALTER ADD COLUMN`, an older row has *fewer*
stored fields and the *smaller* record size of the schema it was written
under, so the heuristic misclassified it as opaque — meaning
`WHERE … IS NULL`, `CHECK`, and triggers saw an added column as
*present/garbage* instead of `NULL`. `kessel_codec::decode` already did
this correctly; the VM did not. Fixed by recognising a codec record via
the record size of the schema **truncated to its stored field count**
(`ObjectType::from_def(ot.fields[..fc])`, reusing SP53) — exactly the
codec's up-projection semantics, pure and deterministic. The full
VSR/determinism corpus stays green (159), proving the relaxation changes
nothing for current-schema or opaque records.

## Tests (1 new, 159 total)

`kessel-sql::alter_table_add_column`: `ALTER … ADD COLUMN note I64` and
`ADD tag U16` (COLUMN optional); insert using the new columns; the **old
row reads back `note IS NULL`** (the bug that's now fixed), the new row
`note = 7`; a `NOT NULL` add → `SchemaError`; unknown table → compile
error. E2E (CLI, live server): online `ALTER`, then `SELECT *` shows the
old row with `NULL` and the new row with a value.

## Honest scope boundary

`ADD COLUMN` only. `DROP COLUMN` / `ALTER COLUMN` / `RENAME` are named
follow-ups (a drop/rewrite has different storage semantics). The
nullable-only rule is the online-DDL invariant, surfaced clearly.
