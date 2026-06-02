#!/usr/bin/env python3
"""SP-PG-ORM-SQLALCHEMY — integration validation smoke.

Runs a REAL SQLAlchemy 2.0 declarative-ORM CRUD workload end-to-end against
KesselDB's PG-wire gateway (NOT raw cursor.execute). This proves the
cumulative PG-wire stack (Extended Query, binary params/results, NUMERIC,
cast validation, typed params, pg_catalog stubs, …) composes for a real
application's ORM layer.

The script is written as a *triage harness*: every ORM operation runs inside
its own try/except so a failure in one stage (e.g. create_all DDL) does not
mask the result of later stages. Each stage prints `STAGE <name>: PASS|FAIL`
plus the exception text on failure, so the gateway log + this stdout together
pin the EXACT boundary of ORM support.

Connection: postgresql+psycopg2://test:admin@127.0.0.1:5540/kesseldb
(port 5540 to avoid colliding with sibling agents).
"""
import sys
import traceback

from sqlalchemy import (
    create_engine,
    Column,
    BigInteger,
    String,
    select,
    update,
    delete,
)
from sqlalchemy.orm import declarative_base, Session

DSN = "postgresql+psycopg2://test:admin@127.0.0.1:5540/kesseldb"

results = []  # (stage, ok, detail)


def stage(name, fn):
    """Run a stage, record PASS/FAIL, never raise."""
    try:
        detail = fn()
        results.append((name, True, detail))
        print(f"STAGE {name}: PASS {detail if detail else ''}".rstrip())
        return True
    except Exception as e:  # noqa: BLE001 — triage harness wants every error
        msg = f"{type(e).__name__}: {e}"
        results.append((name, False, msg))
        print(f"STAGE {name}: FAIL {msg}")
        # one-line traceback tail for the transcript
        tb = traceback.format_exc().strip().splitlines()
        if tb:
            print(f"    {tb[-1]}")
        return False


Base = declarative_base()


class User(Base):
    __tablename__ = "orm_users"
    id = Column(BigInteger, primary_key=True)
    # String(32) -> VARCHAR(32). KesselDB's DDL type map (kessel-sql
    # `kind_of`) aliases BIGINT/INTEGER/SMALLINT/BOOLEAN but NOT VARCHAR;
    # this is the headline create_all friction point we are validating.
    name = Column(String(32))


def main():
    engine = create_engine(DSN)
    print(f"# SQLAlchemy {__import__('sqlalchemy').__version__} -> {DSN}")

    # --- engine.connect probe (Extended Query handshake) ---
    def _connect():
        with engine.connect() as c:
            c.exec_driver_sql("SELECT 1")
        return "engine.connect() + probe OK"

    stage("connect", _connect)

    # --- DDL via ORM metadata (create_all) ---
    # This emits a pg_catalog existence check (SP-PG-CAT stubs) THEN
    # CREATE TABLE orm_users (id BIGINT NOT NULL, name VARCHAR(32), PRIMARY KEY(id)).
    stage("create_all_ddl", lambda: (Base.metadata.create_all(engine), "CREATE TABLE via ORM metadata")[1])

    # If the VARCHAR(32) DDL was rejected, retry create_all with a model whose
    # String maps to a KesselDB-known fixed CHAR type, so the remaining CRUD
    # stages still run and we can measure ORM insert/select/update/delete
    # independently of the VARCHAR DDL gap.
    ddl_ok = results[-1][1]
    if not ddl_ok:
        print("# create_all(VARCHAR) rejected — retrying DDL with explicit CHAR(32) mapping")

        def _create_char():
            from sqlalchemy import CHAR
            # rebuild the table object with CHAR(32) -> KesselDB CHAR(n)
            Base.metadata.remove(User.__table__)
            User.__table__.c.name.type = CHAR(32)
            Base.metadata.create_all(engine)
            return "CREATE TABLE via ORM metadata (CHAR fallback)"

        stage("create_all_ddl_char_fallback", _create_char)

    # If DDL still failed, create the table out-of-band so the CRUD stages can
    # be measured against the live gateway regardless of the DDL boundary.
    if not results[-1][1]:
        def _create_raw():
            with engine.connect() as c:
                c.exec_driver_sql(
                    "CREATE TABLE orm_users (id I64 NOT NULL, name CHAR(32))"
                )
                c.commit()
            return "CREATE TABLE via raw exec_driver_sql (DDL fallback)"

        stage("create_table_raw_fallback", _create_raw)

    table_exists = any(
        r[1] for r in results
        if r[0] in ("create_all_ddl", "create_all_ddl_char_fallback", "create_table_raw_fallback")
    )
    if not table_exists:
        print("# no table — skipping CRUD stages")
        finish()
        return

    # --- ORM INSERT via session.add(<object>) ---
    def _insert():
        with Session(engine) as s:
            s.add(User(id=1, name="alice"))
            s.add(User(id=2, name="bob"))
            s.commit()
        return "INSERT 2 ORM objects"

    insert_ok = stage("orm_insert", _insert)

    # --- ORM SELECT all via select(User) ---
    def _select_all():
        with Session(engine) as s:
            rows = s.execute(select(User)).scalars().all()
            data = sorted((u.id, u.name) for u in rows)
        return f"select(User) -> {data}"

    stage("orm_select_all", _select_all)

    # --- ORM parameterized filter via .where(User.id == 1) ---
    def _filter():
        with Session(engine) as s:
            u = s.execute(
                select(User).where(User.id == 1)
            ).scalar_one_or_none()
        return f"where(id==1) -> {(u.id, u.name) if u else None}"

    stage("orm_filter", _filter)

    # --- ORM UPDATE via update(User).where(...).values(...) ---
    def _update():
        with Session(engine) as s:
            s.execute(update(User).where(User.id == 2).values(name="bobby"))
            s.commit()
        with Session(engine) as s:
            u = s.execute(
                select(User).where(User.id == 2)
            ).scalar_one_or_none()
        return f"update id=2 name=bobby -> {(u.id, u.name) if u else None}"

    stage("orm_update", _update)

    # --- ORM DELETE via delete(User).where(...) ---
    def _delete():
        with Session(engine) as s:
            s.execute(delete(User).where(User.id == 1))
            s.commit()
        with Session(engine) as s:
            rows = s.execute(select(User)).scalars().all()
            data = sorted((u.id, u.name) for u in rows)
        return f"delete id=1 -> remaining {data}"

    stage("orm_delete", _delete)

    finish()


def finish():
    print("\n=== SQLALCHEMY ORM SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("SQLALCHEMY ORM SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0)
