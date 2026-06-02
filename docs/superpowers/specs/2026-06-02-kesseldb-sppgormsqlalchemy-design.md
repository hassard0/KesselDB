# SP-PG-ORM-SQLALCHEMY — SQLAlchemy 2.0 declarative-ORM integration validation — design

Date: 2026-06-02

## 1. Context

Tonight KesselDB shipped ~46 PG-wire arcs (Extended Query, binary
params/results, NUMERIC OID, COPY in text/CSV/binary, cast validation,
typed params, CHAR-pad compare, DESCRIBE-version, …). Every one of those
was verified by a *targeted* smoke driving the specific wire shape it
added — psql / asyncpg / pgJDBC / raw psycopg2 `cursor.execute`.

This arc is the **INTEGRATION validation**: a single real **SQLAlchemy 2.0
declarative-ORM** workload (NOT raw `cursor.execute`) run end-to-end against
KesselDB on vulcan, proving the cumulative stack *composes* for a real
application's ORM layer. The headline question this arc answers honestly is:

> How much of a real SQLAlchemy declarative-ORM CRUD workload
> (create_all DDL / insert / select / filter / update / delete)
> works end-to-end against KesselDB today, and where is the boundary?

It is a **validation arc**: it documents what works and names bugs as
follow-up arcs rather than fixing them inline (keeps the arc clean). A
SMALL surgical fix is allowed only if it unblocks a *major* ORM path;
otherwise the gap is named.

## 2. Scope

- SQLAlchemy 2.0.45 + psycopg2 2.9.12 (already on vulcan) declarative model:

  ```python
  class User(Base):
      __tablename__ = "orm_users"
      id   = Column(BigInteger, primary_key=True)
      name = Column(String(32))
  ```

- Stages, each measured independently (triage harness — one try/except
  per stage so an early failure does not mask later results):
  1. `connect` — `engine.connect()` + Extended Query probe.
  2. `create_all_ddl` — `Base.metadata.create_all(engine)` (pg_catalog
     existence probe → `CREATE TABLE`).
  3. `orm_insert` — `session.add(User(...))` + `commit()`.
  4. `orm_select_all` — `session.execute(select(User)).scalars().all()`.
  5. `orm_filter` — `select(User).where(User.id == 1)` (parameterized).
  6. `orm_update` — `update(User).where(...).values(...)`.
  7. `orm_delete` — `delete(User).where(...)`.

Smoke script: `scripts/sppgormsqlalchemy-smoke.py`.

## 3. Pre-run static recon (what we expect to hit)

Read of `crates/kessel-sql/src/lib.rs::kind_of` and the gateway dispatch
path before the run:

- **DDL type map** aliases `BIGINT`→I64, `INTEGER`/`INT`→I32,
  `SMALLINT`→I16, `BOOLEAN`→Bool (SP-PG-CAT-T8) **but NOT `VARCHAR`**.
  SQLAlchemy's `String(32)` renders `VARCHAR(32)`. `CREATE TABLE` is
  passed **opaque** to the engine by `dispatch.rs`, so `VARCHAR(32)`
  reaches `kind_of` and is expected to fail `unknown type \`VARCHAR\``.
  → **expected first friction point** at `create_all_ddl`.
- VARCHAR *is* recognised on the cast / result-encode / COPY paths
  (`cast_stripper.rs`, `binary_results.rs`, `copy/mod.rs`) — only the
  **DDL alias** is missing.
- SQLAlchemy INSERT of an explicit-PK model does **not** request
  `RETURNING` (RETURNING is only auto-emitted when the PK is
  server-generated / autoincrement). Our model supplies explicit `id`,
  so the INSERT path should be plain `INSERT … VALUES (…)` — no
  RETURNING dependency. (If SQLAlchemy 2.0 still appends
  `RETURNING id`, that is the named `SP-PG-RETURNING` follow-up.)
- BEGIN/COMMIT bracketing is already handled by the EXTQ
  DISCARD/BEGIN interception.

The harness therefore retries DDL with a `CHAR(32)` mapping (and, if that
still fails, a raw `CREATE TABLE … CHAR(32)`) so the CRUD stages run and
get measured **independently of the VARCHAR-DDL gap** — the gap is about
DDL type spelling, not about whether ORM insert/select/update/delete
work.

## 4. Surgical-fix decision rule

If the ONLY thing blocking the entire ORM DDL path is the missing
`VARCHAR` (and `CHARACTER VARYING`) alias in `kessel-sql::kind_of`, that
is a SMALL surgical fix that unblocks a MAJOR ORM path (every SQLAlchemy /
Django / Rails string column renders VARCHAR). In that case add the alias
(`VARCHAR`/`CHARACTER VARYING`/`TEXT` → a CHAR field; choose width from
the `(n)` arg, default a sane cap) — mirroring the existing BIGINT alias —
and re-run. Anything larger (RETURNING, SERIAL/sequences, autoincrement)
is **named, not fixed**:

- `SP-PG-RETURNING` — `INSERT … RETURNING` support.
- `SP-PG-SERIAL` — `SERIAL`/`BIGSERIAL`/sequence-backed autoincrement PK.
- `SP-PG-DDL-VARCHAR-NATIVE` — a true variable-length VARCHAR storage
  type (vs. aliasing VARCHAR→fixed CHAR), if the CHAR-pad semantics
  surface in ORM round-trips.

## 5. Steps

- **T1** — this design + `scripts/sppgormsqlalchemy-smoke.py`.
- **T2** — vulcan: pull + build (`CARGO_TARGET_DIR=/tmp/kdb-target-ormsa`)
  + boot kesseldb on `127.0.0.1:5540`/`6540` + run the smoke; capture
  stdout AND the gateway log (shows the verbatim SQL SQLAlchemy emitted).
- **T3** — triage: per-stage PASS/FAIL with the exact emitted SQL +
  error; apply the VARCHAR alias surgical fix iff it unblocks the major
  DDL path; name follow-ups for everything else.
- **T4** — USAGE §9 "SQLAlchemy ORM (declarative models)" row with the
  HONEST result; STATUS row; progress tracker → CLOSED /
  DONE_WITH_CONCERNS; smoke transcript at
  `docs/superpowers/sppgormsqlalchemy-t2-smoke-2026-06-02.txt`.

## 6. Out of scope (named)

- Relationship loading / joins / lazy-load (`SP-PG-ORM-RELATIONSHIPS`).
- Alembic migrations / autogenerate (`SP-PG-ORM-ALEMBIC`).
- Server-generated PKs / RETURNING (`SP-PG-RETURNING`, `SP-PG-SERIAL`).
- asyncio SQLAlchemy (`postgresql+asyncpg`) ORM (covered for raw asyncpg
  by SP-PG-EXTQ-BIN-R; ORM-over-asyncpg is `SP-PG-ORM-ASYNCPG`).

## 7. Honesty stance

The headline is the *real boundary*. Even a partial result (e.g. DDL gap
but full CRUD via fallback table) is valuable signal and ships as
DONE_WITH_CONCERNS with the boundary named precisely.
