# KesselDB Sub-project 75 — destructive ALTER (DROP / RENAME COLUMN)

**Date:** 2026-05-18  **Status:** shipped. 176 green.

`Op::RenameField` (kind 32) and `Op::DropField` (kind 31). SQL:
`ALTER TABLE t RENAME COLUMN a TO b` and
`ALTER TABLE t DROP COLUMN c`.

## RENAME COLUMN — catalog only

Indexes are keyed by **field id**, not name, and the codec layout is
positional, so a rename touches nothing but `Field.name` (rejects a
name collision). The range index built on the old name keeps working
under the new name unchanged (asserted).

## DROP COLUMN — physical re-encode (no downstream special case)

Chose the honest destructive semantics over a "logical/hidden" flag:
the column is genuinely gone, so every existing code path that
iterates `ot.fields` (SELECT \*, DESCRIBE, WHERE, INSERT, indexes,
codec) needs **zero** new conditionals.

1. Conservative guards (correct over clever): refuse if it is the last
   column (use `DROP TABLE`), an `OverflowRef` column, an FK-backing
   column, or the table has any `CHECK`/triggers (their bytecode is
   position-encoded, so a reshape would silently corrupt them) —
   each a clean `SchemaError`/`Constraint`, documented.
2. Delete the column's own index entries (equality/UNIQUE/range) and
   empty any composite slot that referenced it (kept inert, like
   SP74). **Surviving indexes are not rebuilt** — they are keyed by
   `(field_id, value)` and those values are unchanged, so they stay
   valid; only the dropped column's entries go.
3. Re-encode every row: decode against the old type, drop the value,
   encode against the shrunk type, `put` the same key. Atomic via the
   same own-txn pattern as `DROP TABLE` (abort on any decode/encode
   error).
4. Remove the field from the catalog and persist (schema_ver bumped,
   compile-cache epoch advanced).

## Verified

`alter_drop_and_rename_column`: rename keeps range-index lookups
correct under the new name; drop removes the column from the catalog
and `SELECT *`, leaves surviving-column aggregates byte-identical,
keeps an unrelated index correct, empties the composite that included
the column, supports a subsequent `ADD COLUMN`, refuses an unknown
column (compile) and the last-column drop (engine), and is
deterministic (identical histories ⇒ identical digest). Full workspace
regression clean; determinism / VSR partition corpus (incl. seed 7)
unchanged. Both ops are DDL — rejected inside `Txn`.

## Honest boundary

`DROP COLUMN` is unsupported on a table with `CHECK`/triggers, an
`OverflowRef` column, or as the last column — by design, surfaced as a
clean error, not silently mis-handled. Lifting the CHECK/trigger
restriction needs field-reference rewriting in the expr bytecode (a
separate, larger piece).
