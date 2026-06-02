# SP-PG-ORM-SQLALCHEMY — SQLAlchemy 2.0 declarative-ORM integration validation — SP-arc Progress Tracker

Date created: 2026-06-02

**Status: IN PROGRESS — T1 landed; T2/T3/T4 pending vulcan run.**

Design spec: `docs/superpowers/specs/2026-06-02-kesseldb-sppgormsqlalchemy-design.md`

## What this SP-arc is

The INTEGRATION validation of tonight's ~46 PG-wire arcs: a real
SQLAlchemy 2.0 **declarative-ORM** CRUD workload (NOT raw
`cursor.execute`) run end-to-end against KesselDB on vulcan. Proves the
cumulative stack composes for a real application's ORM layer and pins the
EXACT boundary of ORM support today.

This is a VALIDATION arc: documents what works + names gaps as follow-up
arcs; a SMALL surgical fix is allowed only if it unblocks a MAJOR ORM
path (candidate: the missing `VARCHAR` DDL alias).

## Slice plan

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec + this tracker + `scripts/sppgormsqlalchemy-smoke.py` (triage harness). | **DONE** | (this commit) |
| **T2** | vulcan: pull + build (`/tmp/kdb-target-ormsa`) + boot on 5540/6540 + run smoke; capture stdout + gateway log. | PENDING | |
| **T3** | Triage per-stage PASS/FAIL + exact emitted SQL/error; VARCHAR-alias surgical fix iff it unblocks DDL; name follow-ups. | PENDING | |
| **T4** | USAGE §9 row + STATUS row + tracker → CLOSED/DONE_WITH_CONCERNS + smoke transcript. | PENDING | |

## Headline (to be filled by T3)

TBD — how much of create_all / insert / select / filter / update / delete
works end-to-end.

## Named follow-up arcs (provisional, confirmed in T3)

- `SP-PG-RETURNING` — `INSERT … RETURNING`.
- `SP-PG-SERIAL` — `SERIAL`/`BIGSERIAL` autoincrement PK + sequences.
- `SP-PG-DDL-VARCHAR-NATIVE` — true variable-length VARCHAR storage.
- `SP-PG-ORM-RELATIONSHIPS` — relationship/join/lazy-load.
- `SP-PG-ORM-ALEMBIC` — Alembic migration autogenerate.
- `SP-PG-ORM-ASYNCPG` — ORM over `postgresql+asyncpg`.
