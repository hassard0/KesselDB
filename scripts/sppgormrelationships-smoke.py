#!/usr/bin/env python3
"""SP-PG-ORM-RELATIONSHIPS — SQLAlchemy 2.0 multi-table FK relationship smoke.

Runs a REAL SQLAlchemy 2.0 declarative-ORM TWO-MODEL relationship workload
(Author 1—N Book, FK + relationship()) end-to-end against KesselDB's PG-wire
gateway. Probes the four relational surfaces:

  1. FK DDL          — create_all of 2 tables, the 2nd with a FK constraint
  2. cascade INSERT  — a.books = [...]; s.add(a); commit  (parent + children)
  3. JOIN query      — select(Author.name, Book.title).join(Book, ...)
  4. lazy-load nav    — author.books  (SELECT books.* WHERE author_id = $1)

Triage harness: every stage runs in its own try/except so a failure in one
stage does not mask later stages. Each prints `STAGE <name>: PASS|FAIL`.

Connection: postgresql+psycopg2://test:admin@127.0.0.1:5549/kesseldb
"""
import sys
import traceback

from sqlalchemy import (
    create_engine,
    Column,
    BigInteger,
    String,
    ForeignKey,
    select,
)
from sqlalchemy.orm import declarative_base, Session, relationship

DSN = "postgresql+psycopg2://test:admin@127.0.0.1:5549/kesseldb"

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


class Author(Base):
    __tablename__ = "authors"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String(32))
    books = relationship("Book", back_populates="author")


class Book(Base):
    __tablename__ = "books"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    title = Column(String(64))
    author_id = Column(BigInteger, ForeignKey("authors.id"))
    author = relationship("Author", back_populates="books")


def main():
    engine = create_engine(DSN)
    print(f"# SQLAlchemy {__import__('sqlalchemy').__version__} -> {DSN}")

    # Stage 1 — FK DDL (2 CREATE TABLEs, the 2nd with a FK constraint).
    stage(
        "create_all_fk_ddl",
        lambda: (Base.metadata.create_all(engine), "2 CREATE TABLE (2nd w/ FK)")[1],
    )

    table_ok = results[-1][1]
    if not table_ok:
        print("# create_all failed — skipping relationship stages")
        finish()
        return

    author_id_holder = {}

    # Stage 2 — relationship cascade INSERT.
    def _cascade():
        with Session(engine) as s:
            a = Author(name="tolkien")
            a.books = [Book(title="hobbit"), Book(title="lotr")]
            s.add(a)
            s.commit()
            author_id_holder["id"] = a.id
            book_ids = [b.id for b in a.books]
        return f"author id={author_id_holder['id']} book ids={book_ids}"

    cascade_ok = stage("cascade_insert", _cascade)

    # Stage 3 — JOIN query (qualified projection over an inner equi-join).
    def _join():
        with Session(engine) as s:
            rows = s.execute(
                select(Author.name, Book.title).join(
                    Book, Author.id == Book.author_id
                )
            ).all()
            data = sorted((r[0], r[1]) for r in rows)
        return f"joined -> {data}"

    stage("join_query", _join)

    # Stage 4 — lazy-load relationship navigation.
    def _lazy():
        with Session(engine) as s:
            author = s.execute(select(Author)).scalars().first()
            titles = sorted(b.title for b in author.books)
        return f"author.books (lazy) -> {titles}"

    stage("lazy_load_nav", _lazy)

    finish()


def finish():
    print("\n=== SQLALCHEMY RELATIONSHIPS SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, _detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("RELATIONSHIPS SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0)
