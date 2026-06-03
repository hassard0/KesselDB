# SP-PG-SERIAL-RETURNING — deterministic autoincrement + INSERT RETURNING — SP-arc Progress Tracker

Date created: 2026-06-02

**Status: CLOSED — DONE (2026-06-02).** Closes the two coupled named
follow-ups `SP-PG-SERIAL` (deterministic autoincrement) + `SP-PG-RETURNING`
(return server-assigned values) TOGETHER. Real ORM models overwhelmingly
use AUTOINCREMENT (the app omits `id`, the DB assigns it, the ORM reads
it back via `INSERT … RETURNING id`). A **SQLAlchemy 2.0 autoincrement
model declared WITHOUT an explicit id** now does full CRUD on vulcan and
reads the DB-assigned id back: **autoincrement smoke 6/6**. +~30 KATs,
all touched-crate suites green (catalog 9, proto 6, sql, sm 172, client
90, pg-gateway 975, server 224), zero regressions. Determinism PROVEN:
3-replica byte-identity digest + the seed-7 / sim determinism gates green.

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgserialreturning-design.md`
Smoke transcript: `docs/superpowers/sppgserialreturning-t5-smoke-2026-06-02.txt`

## Slice plan

| T# | Scope | Status | Commit(s) |
|---|---|---|---|
| T1 | Design (determinism analysis: counter-in-digest, apply-thread-only, replicated + crash-safe) | DONE | `d19ed2e` |
| T2 | catalog `serial_pk`/`serial_field_id` (2nd backward-compat trailer) + `OpResult::Created{id}` + SM deterministic per-type counter + KATs | DONE | `35fd899`, `3221d20`, `c991664` |
| T3 | kessel-sql: CREATE TABLE flags SERIAL-on-PK; INSERT-omitting-id → SERIAL_SENTINEL Create; `insert_returning()` parse; KATs | DONE | `7aae692` |
| T4 | gateway: `render_insert_returning` on simple + extended paths; `OpResult::Created` plumbed; KATs | DONE | `470d35e`, `3e114ec`, `f7e7caf`, `e343f06`, `87d3172`, `c25e831` |
| T5 | vulcan SQLAlchemy autoincrement smoke (no explicit id) → 6/6 | DONE | `5e86e68`, `99a8713`, `a77c99a` |
| T6 | STATUS + USAGE §9 + tracker closure | DONE | (this commit) |

## How it works (the determinism-critical path)

- **Counter in the digest.** A per-type sequence counter lives in a
  reserved 20-byte storage keyspace (`SERIAL_TYPE = 0xFFFF_FFF4`, keyed
  by `type_id`), so it is covered by `Storage::digest` (which only skips
  the 28-byte MVCC keyspace). It is the exact proven pattern of the SP79
  global sequencer (`0xFFFF_FFF0`).
- **Advanced only on the apply thread.** The SM's `Op::Create` arm reads
  the counter, `next = cur + 1`, and writes it back via
  `storage.put(op_number, …)` — strictly in op-number order on the
  single deterministic thread. No RNG, no wall-clock ⇒ every replica
  computes the identical gap-free sequence (3-replica byte-identity KAT).
  WAL-backed ⇒ crash + replay resumes it (crash+replay KAT); the SP94
  replay guard prevents double-advance.
- **Gateway never touches the counter.** A serial INSERT (id omitted on a
  `serial_pk` type) compiles to an `Op::Create` carrying the
  `SERIAL_SENTINEL` id (`u128::MAX`); the SM swaps in the assigned value
  as the ObjectId AND patches it into the stored `id` field (so `SELECT
  id` reads it), returning `OpResult::Created { id }`.
- **RETURNING.** `kessel_sql::insert_returning(sql)` locates the clause +
  column list; the gateway reads the just-written row back via the normal
  `SELECT * FROM t WHERE id = <id>` path and projects the RETURNING
  columns into a DataRow (assigned id + any client-supplied columns), on
  BOTH `dispatch_query` and `dispatch_query_with_params` (Extended Query
  — the SQLAlchemy autoincrement flush rides this).
- **Gap semantics.** The counter advances only on the successful-write
  path, so a rejected insert consumes no value. PostgreSQL itself does
  not roll a sequence back on abort; documented, not a bug.

## Incidental ORM unblock

SQLAlchemy's post-flush refresh emits `SELECT widgets.id AS widgets_id,
… FROM widgets WHERE id = n`. The `col AS alias` projection (parser +
gateway `select_columns`) is now accept-and-skipped (project + name by
the SOURCE column). True alias-named RowDescription output is the named
follow-up `SP-PG-SQL-PROJ-ALIAS`.

## Named follow-up arcs (still out after this arc)

| Arc | Notes |
|---|---|
| `SP-PG-SQL-RETURNING-DML` | UPDATE/DELETE … RETURNING (V1 scopes to INSERT RETURNING). |
| `SP-PG-SEQUENCE-DDL` | `CREATE SEQUENCE` + `nextval`/`setval` functions. |
| `SP-PG-SERIAL-NONPK` | a SERIAL column that is NOT the primary key (V1 assigns only the PK/ObjectId). |
| `SP-PG-RETURNING-MULTIROW` | multi-row INSERT … RETURNING (incl. SQLAlchemy's batched `insertmanyvalues` SELECT-VALUES-ORDER BY shape). |
| `SP-PG-RETURNING-STAR` | `RETURNING *` (all columns) vs an explicit list. |
| `SP-PG-SQL-PROJ-ALIAS` | alias-named RowDescription output for `col AS alias`. |
