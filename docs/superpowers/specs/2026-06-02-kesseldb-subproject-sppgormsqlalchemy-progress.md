# SP-PG-ORM-SQLALCHEMY — SQLAlchemy 2.0 declarative-ORM integration validation — SP-arc Progress Tracker

Date created: 2026-06-02

**FOLLOW-UP RESOLVED (2026-06-02): SP-PG-SQL-ORM-PARSE took this from
2/8 → 7/7 (full declarative-ORM CRUD pass).** The three named follow-up
arcs below (`SP-PG-SQL-QUALIFIED-COLS`, `-PROJECTION-RENDER`,
`-ANY-ARRAY`) plus two surfaced DDL-spelling gaps (`BIGSERIAL`,
`PRIMARY KEY (id)`) are all CLOSED. See
`docs/superpowers/specs/2026-06-02-kesseldb-subproject-sppgsqlormparse-progress.md`
+ transcript `docs/superpowers/sppgsqlormparse-t5-smoke-2026-06-02.txt`.

**Status: CLOSED — DONE_WITH_CONCERNS (2026-06-02).** The PG-wire
SUBSTRATE composes (engine.connect + Extended Query probe PASS;
`VARCHAR(n)` DDL, INSERT, `SELECT *`[+WHERE] all PASS), but a REAL
SQLAlchemy 2.0 **declarative-ORM** CRUD workload does NOT yet compose
end-to-end — it is blocked by three SQL-shape gaps in the kessel-sql
parser / PG-wire render path. Smoke = **2/8 ORM stages PASS**. One
pre-named surgical fix shipped (`VARCHAR(n)` DDL alias); the three ORM-
shape blockers are NAMED as follow-up arcs (each larger than surgical).
TaskList ready for completion.

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgormsqlalchemy-design.md`
Smoke transcript: `docs/superpowers/sppgormsqlalchemy-t2-smoke-2026-06-02.txt`

## Slice plan

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + this tracker + `scripts/sppgormsqlalchemy-smoke.py` (triage harness). | **DONE** | `59a5147` |
| **T2** | vulcan: pull + build (`/tmp/kdb-target-ormsa`) + boot on 5540/6540 + run smoke; capture stdout + psql isolation. | **DONE** | `36119f3` (fix) |
| **T3** | Triage per-stage PASS/FAIL + exact emitted SQL/error; `VARCHAR(n)` DDL alias surgical fix + KAT; name follow-ups. | **DONE** | `36119f3` |
| **T4** | USAGE §9 ORM-boundary table + STATUS row + this tracker → CLOSED + smoke transcript. | **DONE** | (this commit) |

KAT delta: **+1** (`kessel-sql::tests::pg_varchar_alias_maps_to_char`,
green on vulcan). No PG-wire / HTTP / WS / binary surface byte touched;
`#![forbid(unsafe_code)]` honored; no new runtime deps.

## Headline

A real SQLAlchemy declarative-ORM workload does NOT yet compose
end-to-end against KesselDB (**2/8 ORM stages PASS**). The PG-wire
substrate is solid — `engine.connect()` + Extended Query probe, `VARCHAR(n)`
DDL (NEW this arc), `INSERT`, and `SELECT *`[+WHERE] all PASS — but the
ORM's own emitted SQL hits three independent SQL-shape gaps:

- **G1** `create_all` inspector probe → `relkind = ANY (ARRAY[…])` →
  `unexpected char '['`. Blocks ALL ORM DDL.
- **G2** ORM SELECT qualifies columns + uses an explicit projection list
  (`SELECT t.id, t.name FROM t`) → parser `expected FROM` for
  `table.col`, and the render path only emits `SELECT *` (even unqualified
  `SELECT id,name FROM t` → "V1 PG-wire only renders `SELECT * FROM <table>`").
- **G3** ORM UPDATE/DELETE qualify the WHERE column (`WHERE t.id = $1`) →
  `expected ID`.

## What landed (T2/T3 surgical fix — `36119f3`)

- `kessel-sql::kind_of`: `VARCHAR(n)` → `FieldKind::Char(n)` DDL alias,
  mirroring the SP-PG-CAT-T8 `BIGINT`/`INTEGER`/`SMALLINT`/`BOOLEAN`
  aliases. SQLAlchemy `Column(String(32))` → `VARCHAR(32)` DDL now
  compiles (verified `CREATE TABLE … VARCHAR(32)` + `\d` on vulcan).
  Bare `VARCHAR` w/o `(n)` rejects with a precise reason.
- KAT `pg_varchar_alias_maps_to_char` (green on vulcan).
- The gateway already encoded the `varchar` OID 1043 on the read/cast
  side (`cast_stripper`, `binary_results`); this closes the write/DDL side.

## Named follow-up arcs (gaps NOT fixed inline)

| Arc | Unblocks | Notes |
|---|---|---|
| `SP-PG-SQL-QUALIFIED-COLS` | G2-parse + G3 | accept `table.col` in projection + WHERE/SET; strip the redundant single-table qualifier (JOIN already parses `t.c`). The single highest-leverage arc — unblocks EVERY ORM SELECT/UPDATE/DELETE parse. |
| `SP-PG-SQL-PROJECTION-RENDER` | G2-render | PG-wire render of an explicit projection list, not just `SELECT *`. The kessel-sql parser already has the SP-Analytic-Plan-MULTI projection parser; the gateway render side is the gap. |
| `SP-PG-SQL-ANY-ARRAY` | G1 | `col = ANY (ARRAY[…])` (+ inverse). Used by SQLAlchemy's create_all/inspector existence probe AND IN-list lowering. |
| `SP-PG-DDL-VARCHAR-UNBOUNDED` | — | bare `VARCHAR`/`TEXT`/multi-word `CHARACTER VARYING` DDL types. |
| `SP-PG-DDL-VARCHAR-NATIVE` | — | true variable-length storage vs fixed-CHAR alias (CHAR-pad vs VARCHAR-trim). |
| `SP-PG-RETURNING` / `SP-PG-SERIAL` | — | server-generated/autoincrement PKs + `INSERT … RETURNING`. Not hit by the explicit-id model used here, but the next ORM model shape needs them. |
| `SP-PG-ORM-RELATIONSHIPS` / `SP-PG-ORM-ALEMBIC` | — | relationship/join/lazy-load; Alembic migration autogenerate. |

## Honesty note

This REFINES the earlier ORM-compat-matrix "SQLAlchemy 2.0 ✓" claim:
that ✓ is for the **raw-driver** path
(`conn.execute(text("SELECT * FROM t WHERE id=:id"))`), which remains
green. The **declarative-ORM** path is the boundary documented here.
Closing `SP-PG-SQL-QUALIFIED-COLS` + `-PROJECTION-RENDER` + `-ANY-ARRAY`
takes the declarative ORM from 2/8 to a full CRUD pass.
