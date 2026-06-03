# SP-PG-SQL-ORM-PARSE — qualified cols + projection render + ANY(ARRAY[]) — SP-arc Progress Tracker

Date created: 2026-06-02

**Status: CLOSED — DONE (2026-06-02).** Closed the three keystone
ORM-shape blockers named by SP-PG-ORM-SQLALCHEMY + two surfaced
DDL-spelling gaps, taking the **SQLAlchemy 2.0 declarative-ORM CRUD smoke
from 2/8 → 7/7 (full CRUD pass)** on vulcan. Every meaningful ORM stage
PASSES end-to-end: connect, `create_all` DDL, multi-row INSERT,
qualified-column SELECT + filter, by-PK UPDATE + DELETE. +18 KATs,
1055+ kessel-sql + gateway KATs green, zero regressions, gateway log
clean.

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgsqlormparse-design.md`
Smoke transcript (after): `docs/superpowers/sppgsqlormparse-t5-smoke-2026-06-02.txt`

## Slice plan

| T# | Scope | Status | Commit(s) |
|---|---|---|---|
| T1 | Design + diagnosis | DONE | `fcaa48a` |
| T2 | kessel-sql qualified columns (`table.col`) + ORM UPDATE/DELETE `SET…WHERE id=n` | DONE | `a6abfb3`, `c627e64`, `72f7bad` |
| T3 | gateway projection-list render (`render_select_got` + `emit_projected_rows`) | DONE | `1d5aed1`, `c04752c` |
| T4 | `= ANY (ARRAY[…])` desugar + `[`/`]` lex + create_all relname probe in pg_catalog | DONE | `53af3b3`, `cabd15f` |
| T5 | SERIAL aliases + `PRIMARY KEY` accept-skip; vulcan ORM smoke 2/8→7/7 | DONE | `b5102ef`, `ea5f6f4`, `c31874b` |
| T6 | STATUS + USAGE §9 + tracker closure | DONE | (this commit) |

## What closed each gap

- **G2a Qualified columns** (`SP-PG-SQL-QUALIFIED-COLS`): kessel-sql
  `col_ident()` reads `IDENT (DOT IDENT)?`, strips the qualifier (lenient
  V1) in projection / WHERE term / SET / ORDER BY / GROUP BY;
  `strip_span_qualifiers` normalizes the index-hint span so a qualified
  query compiles BYTE-IDENTICALLY to bare (determinism contract).
- **G2b Projection render** (`SP-PG-SQL-PROJECTION-RENDER`): gateway
  `render_select_got` dispatches `select_columns` (projection) vs
  `select_star_table` (whole row); `emit_projected_rows` decodes the
  `Op::SelectFields` raw-fixed-width row stream (no record header / null
  bitmap). `select_columns` strips qualifiers.
- **G1 `= ANY (ARRAY[…])`** (`SP-PG-SQL-ANY-ARRAY`): lexes `[`/`]`;
  desugars to IN→OR-of-eq (byte-identical to IN); pg_catalog hook
  recognizes SQLAlchemy's `create_all` relname-existence probe +
  synthesizes the existence answer (1 row if exists, else 0).
- **EXTRA ORM UPDATE/DELETE**: `parse_where_id_eq` maps `SET … WHERE
  [t.]id = n` / `DELETE … WHERE [t.]id = n` to the id-based RMW.
- **EXTRA DDL spelling**: `BIGSERIAL`/`SERIAL`/`SMALLSERIAL` → plain int
  width aliases; table-level + inline `PRIMARY KEY` accept-and-skip.

## Named follow-up arcs (still out after this arc)

| Arc | Notes |
|---|---|
| ~~`SP-PG-SERIAL` / `SP-PG-RETURNING`~~ | **CLOSED 2026-06-02** by SP-PG-SERIAL-RETURNING — deterministic autoincrement PK + `INSERT … RETURNING id`; SQLAlchemy autoincrement model (no explicit id) full CRUD 6/6. Tracker: `2026-06-02-kesseldb-subproject-sppgserialreturning-progress.md`. |
| `SP-PG-SQL-UPDATE-WHERE-GENERAL` | non-PK / multi-row UPDATE/DELETE WHERE (needs a server-side scan-resolve-RMW). V1 covers `WHERE id = <int>`. |
| `SP-PG-SQL-QUALIFIER-STRICT` | strict qualifier validation (reject `wrong_table.id`); V1 lenient. |
| `SP-PG-SQL-FROM-ALIAS` | parse/bind `FROM t AS x` aliases. |
| `SP-PG-SQL-ANY-SUBQUERY` | `= ANY (SELECT …)` vs array literal. |
| `SP-PG-SQL-PROJ-EXPR` / `-PROJ-NULL` | `col AS alias`, computed projection, projected-NULL fidelity. |
| `SP-PG-SQL-JOIN-PROJ-QUAL` | qualified cols in a JOIN projection that must disambiguate the owning table. |
| `SP-PG-DDL-COMPOSITE-PK` | true multi-column PRIMARY KEY index. |
| `SP-PG-ORM-RELATIONSHIPS` / `-ALEMBIC` | relationship/join/lazy-load; Alembic autogenerate. |
