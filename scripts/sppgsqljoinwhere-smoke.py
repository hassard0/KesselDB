#!/usr/bin/env python3
"""SP-PG-SQL-JOIN-WHERE — SQLAlchemy 2.0 FILTERED inner-join smoke.

Extends the relationship workload (Author 1—N Book) with the most common
real-app join pattern beyond a bare join: a JOIN with a WHERE filter over the
combined rows, e.g. SQLAlchemy `select(...).join(Book).where(Book.title == x)`
→ `SELECT authors.name, books.title FROM authors JOIN books
    ON authors.id = books.author_id WHERE books.title = $1`.

Stages:
  1. create_all_fk_ddl   — 2 CREATE TABLEs (2nd w/ FK)            [setup]
  2. cascade_insert      — a.books = [hobbit, lotr]; commit       [setup]
  3. bare_join           — join, NO filter → both books            (regression)
  4. filtered_join_right — .where(Book.title == 'lotr') → 1 row    (b-col)
  5. filtered_join_left  — .where(Author.name == 'tolkien') → 2    (a-col)
  6. filtered_join_and   — title=='lotr' AND name=='tolkien' → 1   (AND both)
  7. filtered_join_empty — .where(Book.title == 'nope') → 0 rows   (0 matches)

Each stage runs in its own try/except (triage harness). HEADLINE: the
filtered join returns ONLY the matching combined rows.

Connection: postgresql+psycopg2://test:admin@127.0.0.1:5550/kesseldb
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

DSN = "postgresql+psycopg2://test:admin@127.0.0.1:5550/kesseldb"

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

    stage(
        "create_all_fk_ddl",
        lambda: (Base.metadata.create_all(engine), "2 CREATE TABLE (2nd w/ FK)")[1],
    )
    if not results[-1][1]:
        print("# create_all failed — skipping join stages")
        finish()
        return

    def _cascade():
        with Session(engine) as s:
            a = Author(name="tolkien")
            a.books = [Book(title="hobbit"), Book(title="lotr")]
            s.add(a)
            s.commit()
            ids = [b.id for b in a.books]
        return f"author + books ids={ids}"

    stage("cascade_insert", _cascade)

    # Bare join (regression): both books come back.
    def _bare():
        with Session(engine) as s:
            rows = s.execute(
                select(Author.name, Book.title).join(
                    Book, Author.id == Book.author_id
                )
            ).all()
            data = sorted((r[0], r[1]) for r in rows)
        assert data == [("tolkien", "hobbit"), ("tolkien", "lotr")], data
        return f"bare join -> {data}"

    stage("bare_join", _bare)

    # Filtered join on a RIGHT-table (Book) column → only (tolkien, lotr).
    def _filt_right():
        with Session(engine) as s:
            rows = s.execute(
                select(Author.name, Book.title)
                .join(Book, Author.id == Book.author_id)
                .where(Book.title == "lotr")
            ).all()
            data = sorted((r[0], r[1]) for r in rows)
        assert data == [("tolkien", "lotr")], data
        return f"filtered (Book.title=='lotr') -> {data}"

    stage("filtered_join_right", _filt_right)

    # Filtered join on a LEFT-table (Author) column → both books.
    def _filt_left():
        with Session(engine) as s:
            rows = s.execute(
                select(Author.name, Book.title)
                .join(Book, Author.id == Book.author_id)
                .where(Author.name == "tolkien")
            ).all()
            data = sorted((r[0], r[1]) for r in rows)
        assert data == [("tolkien", "hobbit"), ("tolkien", "lotr")], data
        return f"filtered (Author.name=='tolkien') -> {data}"

    stage("filtered_join_left", _filt_left)

    # AND of a left-col and a right-col predicate → only (tolkien, lotr).
    def _filt_and():
        with Session(engine) as s:
            rows = s.execute(
                select(Author.name, Book.title)
                .join(Book, Author.id == Book.author_id)
                .where(Book.title == "lotr")
                .where(Author.name == "tolkien")
            ).all()
            data = sorted((r[0], r[1]) for r in rows)
        assert data == [("tolkien", "lotr")], data
        return f"filtered AND -> {data}"

    stage("filtered_join_and", _filt_and)

    # Filter matching 0 rows → empty result.
    def _filt_empty():
        with Session(engine) as s:
            rows = s.execute(
                select(Author.name, Book.title)
                .join(Book, Author.id == Book.author_id)
                .where(Book.title == "nope")
            ).all()
        assert rows == [], rows
        return "filtered (no match) -> 0 rows"

    stage("filtered_join_empty", _filt_empty)

    finish()


def finish():
    print("\n=== SQLALCHEMY JOIN-WHERE SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, _detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("JOIN-WHERE SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0)
