#!/usr/bin/env python3
"""SP-PG-RETURNING-MULTIROW-STAR — SQLAlchemy DEFAULT-config smoke.

The headline: KesselDB works with SQLAlchemy's OUT-OF-THE-BOX engine
config. The SP-PG-SERIAL-RETURNING smoke had to pass
`use_insertmanyvalues=False`; that is NOT the SQLAlchemy default. By
default (`use_insertmanyvalues=True`) SQLAlchemy 2.0 BATCHES multiple
pending objects into ONE statement —

    INSERT INTO widgets (name) VALUES ('a'),('b'),('c') RETURNING id

— and expects N DataRows back (one assigned id per row, in order). This
smoke constructs the engine with NO use_insertmanyvalues override (the
default) and verifies a batched `add_all([...]); commit()` reads every
assigned id back, plus an explicit multi-row `RETURNING *`.

Connection: postgresql+psycopg2://test:admin@127.0.0.1:5544/kesseldb
(port 5544 per the SP-PG-RETURNING-MULTIROW-STAR design).
"""
import sys
import traceback

from sqlalchemy import (
    create_engine,
    Column,
    BigInteger,
    String,
    select,
    text,
)
from sqlalchemy.orm import declarative_base, Session

DSN = "postgresql+psycopg2://test:admin@127.0.0.1:5544/kesseldb"

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
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String(32))


def main():
    # *** NO use_insertmanyvalues=False *** — SQLAlchemy DEFAULT config.
    # With the default (use_insertmanyvalues=True), a batched add_all flush
    # emits ONE multi-row `INSERT ... VALUES (...),(...) RETURNING id` and
    # expects N DataRows back.
    engine = create_engine(DSN)
    sa = __import__("sqlalchemy")
    print(f"# SQLAlchemy {sa.__version__} -> {DSN}  (DEFAULT config, no use_insertmanyvalues override)")

    stage(
        "connect",
        lambda: (
            [c.exec_driver_sql("SELECT 1") for c in [engine.connect()]],
            "engine.connect() + probe OK",
        )[1],
    )

    def _create():
        Base.metadata.create_all(engine)
        return "CREATE TABLE widgets via ORM metadata (BIGSERIAL PK)"

    if not stage("create_all_ddl", _create):
        print("# no table — skipping RETURNING stages")
        finish()
        return

    # --- THE HEADLINE: batched add_all → multi-row INSERT RETURNING id. ---
    assigned = {}

    def _batched_insert():
        with Session(engine) as s:
            ws = [Widget(name="a"), Widget(name="b"), Widget(name="c")]
            s.add_all(ws)             # batched into ONE statement by default
            s.commit()
            ids = [w.id for w in ws]
        if any(i is None for i in ids):
            raise AssertionError(f"a batched-insert id is None — multi-row RETURNING did not populate PKs: {ids}")
        if len(set(ids)) != len(ids):
            raise AssertionError(f"non-unique assigned ids from the batch: {ids}")
        assigned["batch"] = ids
        return f"add_all([a,b,c]) batched -> assigned ids {ids} (all read back via multi-row RETURNING)"

    headline_ok = stage("batched_multirow_insert_returning_id", _batched_insert)

    # --- SELECT back: all three rows present with their assigned ids. ---
    def _select_all():
        with Session(engine) as s:
            rows = s.execute(select(Widget)).scalars().all()
            data = sorted((w.id, w.name) for w in rows)
        if len(data) < 3:
            raise AssertionError(f"expected >=3 rows after the batch, got {data}")
        return f"select(Widget) -> {data}"

    stage("select_all_after_batch", _select_all)

    # --- Explicit multi-row RETURNING * via raw SQL (all columns back). ---
    def _returning_star():
        with engine.connect() as c:
            rs = c.exec_driver_sql(
                "INSERT INTO widgets (name) VALUES ('x'),('y') RETURNING *"
            )
            rows = rs.fetchall()
            c.commit()
        if len(rows) != 2:
            raise AssertionError(f"RETURNING * should return 2 rows, got {len(rows)}: {rows}")
        # Each row must carry BOTH columns (id + name).
        for r in rows:
            if len(r) < 2:
                raise AssertionError(f"RETURNING * row missing columns: {r}")
        return f"RETURNING * -> {[tuple(r) for r in rows]} (all columns, 2 rows)"

    stage("explicit_multirow_returning_star", _returning_star)

    finish()


def finish():
    print("\n=== SQLALCHEMY DEFAULT-CONFIG MULTIROW SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("MULTIROW SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0)
