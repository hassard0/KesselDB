#!/usr/bin/env python3
"""SP-PG-ORM-REALAPP — capstone realistic multi-model SQLAlchemy app smoke.

A realistic BLOG application (User 1—N Post 1—N Comment, FKs + declarative
`relationship()`) exercising the full query range a real app uses, end-to-end
against KesselDB's PG-wire gateway. This is the CAPSTONE validation across the
whole relational surface that landed tonight.

Stages (each isolated in its own try/except — a failure does NOT mask later
stages, so we get a full per-query PASS/GAP picture):

  schema       — create_all of 3 tables (users, posts, comments) w/ 2 FKs
  cascade_seed — alice/bob + posts via relationship cascade; then comments
  Q1 join          — list all posts with author name (JOIN)
  Q2 filtered_join — posts by a specific author (JOIN … WHERE)
  Q3 group_agg     — comment count per post (GROUP BY over JOIN)
  Q4 paginate      — recent posts (ORDER BY + LIMIT)
  Q5 nav           — relationship navigation (alice.posts)
  Q6 update_delete — UPDATE … WHERE + DELETE … WHERE

Connection: postgresql+psycopg2://test:admin@127.0.0.1:5556/kesseldb
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
    func,
    update,
    delete,
)
from sqlalchemy.orm import declarative_base, Session, relationship

DSN = "postgresql+psycopg2://test:admin@127.0.0.1:5556/kesseldb"

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
        print(f"STAGE {name}: GAP  {msg}")
        tb = traceback.format_exc().strip().splitlines()
        if tb:
            print(f"    {tb[-1]}")
        return False


Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String(32))
    posts = relationship("Post", back_populates="author")


class Post(Base):
    __tablename__ = "posts"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    title = Column(String(64))
    user_id = Column(BigInteger, ForeignKey("users.id"))
    author = relationship("User", back_populates="posts")
    comments = relationship("Comment", back_populates="post")


class Comment(Base):
    __tablename__ = "comments"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    body = Column(String(128))
    post_id = Column(BigInteger, ForeignKey("posts.id"))
    post = relationship("Post", back_populates="comments")


def main():
    engine = create_engine(DSN)
    print(f"# SQLAlchemy {__import__('sqlalchemy').__version__} -> {DSN}")

    # schema — 3 CREATE TABLEs, two with FKs.
    schema_ok = stage(
        "schema",
        lambda: (Base.metadata.create_all(engine), "3 CREATE TABLE (2 w/ FK)")[1],
    )
    if not schema_ok:
        print("# create_all failed — skipping app stages")
        finish()
        return

    # cascade_seed — multi-level relationship cascade insert + comments.
    def _seed():
        with Session(engine) as s:
            alice = User(name="alice")
            bob = User(name="bob")
            alice.posts = [Post(title="hello world"), Post(title="kesseldb rocks")]
            bob.posts = [Post(title="bob's first post")]
            s.add_all([alice, bob])
            s.commit()
            # add comments to alice's first post
            p1 = s.execute(
                select(Post).where(Post.title == "hello world")
            ).scalar_one()
            p1.comments = [Comment(body="great!"), Comment(body="nice")]
            s.commit()
        return "2 users, 3 posts, 2 comments"

    seed_ok = stage("cascade_seed", _seed)

    # Q1 — list all posts with author name (JOIN).
    def _q1():
        with Session(engine) as s:
            rows = s.execute(
                select(Post.title, User.name).join(User, Post.user_id == User.id)
            ).all()
        return f"posts+author -> {sorted((r[0], r[1]) for r in rows)}"

    stage("Q1_join", _q1)

    # Q2 — posts by a specific author (filtered JOIN).
    def _q2():
        with Session(engine) as s:
            rows = s.execute(
                select(Post.title)
                .join(User, Post.user_id == User.id)
                .where(User.name == "alice")
            ).all()
        return f"alice's posts -> {sorted(r[0] for r in rows)}"

    stage("Q2_filtered_join", _q2)

    # Q3 — comment count per post (GROUP BY over JOIN).
    def _q3():
        with Session(engine) as s:
            rows = s.execute(
                select(Post.title, func.count(Comment.id))
                .join(Comment, Post.id == Comment.post_id)
                .group_by(Post.title)
            ).all()
        return f"comment counts -> {sorted((r[0], r[1]) for r in rows)}"

    stage("Q3_group_agg", _q3)

    # Q4 — recent posts paginated (ORDER BY + LIMIT).
    def _q4():
        with Session(engine) as s:
            rows = s.execute(
                select(Post.title).order_by(Post.title).limit(2)
            ).all()
        return f"paginated -> {[r[0] for r in rows]}"

    stage("Q4_paginate", _q4)

    # Q5 — relationship navigation.
    def _q5():
        with Session(engine) as s:
            u = s.execute(select(User).where(User.name == "alice")).scalar_one()
            titles = sorted(p.title for p in u.posts)
        return f"alice.posts -> {titles}"

    stage("Q5_nav", _q5)

    # Q6 — UPDATE … WHERE + DELETE … WHERE.
    def _q6():
        with Session(engine) as s:
            s.execute(
                update(Post)
                .where(Post.title == "hello world")
                .values(title="hello, world!")
            )
            s.commit()
            s.execute(delete(Comment).where(Comment.body == "nice"))
            s.commit()
            cnt = s.execute(select(func.count(Comment.id))).scalar()
        return f"after update+delete, comment count -> {cnt}"

    stage("Q6_update_delete", _q6)

    finish()


def finish():
    print("\n=== REALAPP (BLOG) SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, _detail in results:
        print(f"  {'PASS' if ok else 'GAP '}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("REALAPP SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0)
