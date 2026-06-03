#!/usr/bin/env python3
"""SP-PG-SERIAL-RETURNING — autoincrement ORM smoke.

The SP-PG-SQL-ORM-PARSE arc proved SQLAlchemy 2.0 declarative-ORM CRUD at
7/7 — but ONLY for models with an EXPLICIT primary key. Real ORM models
overwhelmingly use AUTOINCREMENT: the application does NOT supply `id`,
the DB assigns it, and the ORM reads it back via `INSERT ... RETURNING
id`. This smoke drives a model declared WITHOUT an explicit id and
verifies the DB-assigned id is read back — the headline.

Connection: postgresql+psycopg2://test:admin@127.0.0.1:5543/kesseldb
(port 5543 per the SP-PG-SERIAL-RETURNING design to avoid colliding with
sibling agents).
"""
import sys
import traceback

from sqlalchemy import (
    create_engine,
    Column,
    BigInteger,
    String,
    select,
    delete,
)
from sqlalchemy.orm import declarative_base, Session

DSN = "postgresql+psycopg2://test:admin@127.0.0.1:5543/kesseldb"

results = []  # (stage, ok, detail)


def stage(name, fn):
    try:
        detail = fn()
        results.append((name, True, detail))
        print(f"STAGE {name}: PASS {detail if detail else ''}".rstrip())
        return True
    except Exception as e:  # noqa: BLE001 — triage harness wants every error
        msg = f"{type(e).__name__}: {e}"
        results.append((name, False, msg))
        print(f"STAGE {name}: FAIL {msg}")
        tb = traceback.format_exc().strip().splitlines()
        if tb:
            print(f"    {tb[-1]}")
        return False


Base = declarative_base()


class Widget(Base):
    __tablename__ = "widgets"
    # NO explicit id on insert — the DB assigns it (BIGSERIAL). SQLAlchemy
    # emits `INSERT INTO widgets (name) VALUES (...) RETURNING id` and
    # reads the assigned value back into `w.id` after the flush.
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String(32))


def main():
    # `use_insertmanyvalues=False` makes SQLAlchemy emit the classic
    # per-row `INSERT INTO t (cols) VALUES (...) RETURNING id` shape rather
    # than its batched `INSERT ... SELECT (VALUES ...) ORDER BY sen_counter`
    # optimization (which is a multi-row + ORDER BY form out of V1 scope —
    # named follow-up SP-PG-RETURNING-MULTIROW). This is the dominant
    # real-world ORM insert shape (one INSERT per flushed object).
    engine = create_engine(DSN, use_insertmanyvalues=False)
    print(f"# SQLAlchemy {__import__('sqlalchemy').__version__} -> {DSN}")

    stage(
        "connect",
        lambda: (
            [c.exec_driver_sql("SELECT 1") for c in [engine.connect()]],
            "engine.connect() + probe OK",
        )[1],
    )

    # create_all emits CREATE TABLE widgets (id BIGSERIAL NOT NULL,
    # name VARCHAR(32), PRIMARY KEY (id)).
    def _create():
        Base.metadata.create_all(engine)
        return "CREATE TABLE widgets via ORM metadata (BIGSERIAL PK)"

    if not stage("create_all_ddl", _create):
        # CHAR fallback so the autoincrement path is still measured even if
        # VARCHAR DDL regressed (it should NOT — kept for triage parity).
        def _create_char():
            from sqlalchemy import CHAR
            Base.metadata.remove(Widget.__table__)
            Widget.__table__.c.name.type = CHAR(32)
            Base.metadata.create_all(engine)
            return "CREATE TABLE widgets (CHAR fallback)"
        stage("create_all_ddl_char_fallback", _create_char)

    table_ok = any(
        r[1] for r in results
        if r[0].startswith("create_all_ddl")
    )
    if not table_ok:
        print("# no table — skipping autoincrement stages")
        finish()
        return

    # --- THE HEADLINE: insert WITHOUT an id, read the assigned id back. ---
    assigned_ids = {}

    def _insert_autoincrement():
        with Session(engine) as s:
            w1 = Widget(name="gadget")   # no id!
            w2 = Widget(name="sprocket")  # no id!
            s.add(w1)
            s.add(w2)
            s.commit()
            # After commit, SQLAlchemy has populated the PKs from RETURNING.
            assigned_ids["w1"] = w1.id
            assigned_ids["w2"] = w2.id
        if w1.id is None or w2.id is None:
            raise AssertionError("assigned id is None — RETURNING did not populate the PK")
        if w1.id == w2.id:
            raise AssertionError(f"non-unique assigned ids: {w1.id} == {w2.id}")
        return f"assigned ids w1={w1.id} w2={w2.id} (read back via RETURNING)"

    headline_ok = stage("autoincrement_insert_returns_id", _insert_autoincrement)

    # --- SELECT back: the rows carry the assigned ids. ---
    def _select_all():
        with Session(engine) as s:
            rows = s.execute(select(Widget)).scalars().all()
            data = sorted((w.id, w.name) for w in rows)
        return f"select(Widget) -> {data}"

    stage("autoincrement_select_all", _select_all)

    # --- filter by the assigned id (by-PK CRUD on the auto id). ---
    def _filter():
        wid = assigned_ids.get("w1")
        with Session(engine) as s:
            w = s.get(Widget, wid)
        if w is None:
            raise AssertionError(f"could not fetch the autoincrement row id={wid}")
        return f"get(Widget, {wid}) -> {(w.id, w.name)}"

    stage("autoincrement_filter_by_assigned_id", _filter)

    # --- delete by the assigned id. ---
    def _delete():
        wid = assigned_ids.get("w1")
        with Session(engine) as s:
            s.execute(delete(Widget).where(Widget.id == wid))
            s.commit()
        with Session(engine) as s:
            rows = s.execute(select(Widget)).scalars().all()
            data = sorted((w.id, w.name) for w in rows)
        return f"delete id={wid} -> remaining {data}"

    stage("autoincrement_delete_by_assigned_id", _delete)

    finish()


def finish():
    print("\n=== SQLALCHEMY AUTOINCREMENT SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("SQLALCHEMY AUTOINCREMENT SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0)
