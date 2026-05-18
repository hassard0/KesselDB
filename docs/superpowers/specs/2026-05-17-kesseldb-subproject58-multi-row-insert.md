# KesselDB Sub-project 58 — multi-row INSERT (atomic, one round-trip)

**Date:** 2026-05-17  **Status:** shipped, tested, e2e-verified. 154 green.
Production-feature-gap pass, slice 5.

## The gap + the "wow"

`INSERT` was single-row only and required the legacy `ID <n>` syntax. A
fully featured SQL database needs Postgres-shaped multi-row inserts — and
KesselDB can make that genuinely *faster*: a naive client doing N inserts
pays N network round-trips and N replicated consensus ops; a KesselDB
multi-row `INSERT` is **one statement → one atomic `Op::Txn` → one
round-trip, one consensus op** for the whole batch.

## Delivered

`kessel-sql` `INSERT` now accepts two forms:

- *legacy* (unchanged, back-compat): `INSERT INTO t ID <n> (cols) VALUES (..)`
- *general* (Postgres-shaped): an `id` pseudo-column in the column list,
  with **one or more** value tuples:

```sql
INSERT INTO acct (id, owner, bal)
VALUES (1, 100, 50), (2, 100, 999), (3, 200, 7);
```

- A single tuple compiles to one `Op::Create` (back-compatible behaviour).
- Multiple tuples compile to a single **atomic `Op::Txn`** — all rows
  land or none do, replicated as one operation.
- Missing row id (`ID <n>` *and* no `id` column) → clean `SqlError`.
- `id` is a reserved pseudo-column (the 128-bit row id), not a field;
  unlisted nullable fields default to `Null` per tuple as before.

## Test (1 new, 154 total)

`multi_row_insert_is_atomic`: legacy form still works; `id`-column
single-row works; a 3-row batch inserts atomically (`COUNT(*)` checks);
**atomicity** — a duplicate id inside a batch rejects the *whole*
statement (no row, including the valid one, is inserted); missing-id →
compile error. E2E (CLI, live server): a 3-row insert in one statement
(`exit 0`), then `SELECT *` shows the rows in an aligned table. Full
workspace regression green (154); the INSERT-path rewrite preserves every
existing `INSERT INTO t ID n …` test (back-compat proven).

## Why it's faster (honest, architectural)

The speedup is not a microbenchmark trick: a multi-row insert is *one*
client→server round-trip and *one* replicated `Op::Txn` (one WAL group
commit, one consensus decision) instead of N of each. The atomicity is
free — it reuses the SP9 all-or-nothing transaction that the engine and
the determinism/VSR corpus already prove correct.

## Honest scope boundary

Values are literals (ints/strings) as elsewhere; expressions in `VALUES`
are not supported (consistent with the rest of the SQL surface).
`INSERT ... SELECT` is a separate, named follow-up.
