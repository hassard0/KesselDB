#!/usr/bin/env python3
"""SP-PG-SQL-MULTI-JOIN — chained N-way (3+ table) INNER equi-join smoke
(psycopg2).

`Op::Join` was BINARY (exactly two tables). Real apps + analytics constantly
chain joins:

    SELECT u.name, p.title, c.body
      FROM users u JOIN posts p ON u.id = p.user_id
                   JOIN comments c ON p.id = c.post_id;

Before this arc the planner handled exactly ONE JOIN; a second JOIN failed to
compile. This arc adds an additive, marker-guarded `extra_joins: Vec<JoinStep>`
to `Op::Join`; the engine folds each step (INNER equi-join the running combined
row set against the next table on the ON columns), widening the combined KTR1
schema each step, so 3+ table chains work end-to-end over the PG wire. An empty
extra_joins ⇒ a normal binary join ⇒ byte-identical Op frame to before.

Data — a classic users → posts → comments FK chain:
    users:    (1 alice), (2 bob)
    posts:    (10 user 1 "hello"), (11 user 1 "world"), (12 user 2 "solo")
    comments: (100 post 10 "nice"), (101 post 10 "ok"), (102 post 11 "wow")
              (post 12 has NO comments → INNER chain drops it)

The 3-way INNER chain users JOIN posts JOIN comments yields exactly the rows
where a user has a post AND that post has a comment:
    alice / hello / nice
    alice / hello / ok
    alice / world / wow

Stages:
  1. ddl            — CREATE users, posts, comments                     [setup]
  2. seed           — 2 users, 3 posts, 3 comments                      [setup]
  3. three_way      — SELECT u.name, p.title, c.body  (HEADLINE)
  4. three_way_star — SELECT *  across the 3-table chain
  5. three_way_where— filtered 3-way join (WHERE u.id = 1)

HEADLINE: a real 3-table chained INNER join returns the correct combined rows
over the PG wire. On pre-arc origin/main the second JOIN failed to compile.

The script LAUNCHES its own kesseldb-server (built with --features pg-gateway)
on 127.0.0.1:5552, runs the stages, then tears the server down. Pass
--no-server to point at an already-running server.

Connection: postgresql://test:admin@127.0.0.1:5552/kesseldb
"""
import os
import signal
import subprocess
import sys
import tempfile
import time
import traceback

import psycopg2

PG_ADDR = "127.0.0.1:5552"
DSN = "postgresql://test:admin@127.0.0.1:5552/kesseldb"
CLIENT_ADDR = "127.0.0.1:7882"

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


def launch_server(repo_root):
    """Build (if needed) + launch kesseldb-server with the PG gateway.

    NOTE: the binary MUST be built with --features pg-gateway or the PG
    listener is compiled out and 5552 will never bind.
    """
    print("# building kesseldb-server --features pg-gateway (release)…")
    build = subprocess.run(
        ["cargo", "build", "--release", "-p", "kesseldb-server",
         "--features", "pg-gateway"],
        cwd=repo_root,
        env={**os.environ},
    )
    if build.returncode != 0:
        print("BUILD FAILED", file=sys.stderr)
        sys.exit(2)
    bin_path = os.path.join(repo_root, "target", "release", "kesseldb")
    data_dir = tempfile.mkdtemp(prefix="kdb-mj-data-")
    env = {**os.environ, "KESSELDB_PG_ADDR": PG_ADDR, "KESSELDB_TOKEN": "admin"}
    print(f"# launching {bin_path} {CLIENT_ADDR} {data_dir} on PG {PG_ADDR}…")
    proc = subprocess.Popen([bin_path, CLIENT_ADDR, data_dir], cwd=repo_root, env=env)
    for _ in range(60):
        try:
            c = psycopg2.connect(DSN, connect_timeout=1)
            c.close()
            print("# server is up")
            return proc
        except Exception:
            if proc.poll() is not None:
                print("SERVER EXITED EARLY (built without --features pg-gateway?)",
                      file=sys.stderr)
                sys.exit(2)
            time.sleep(0.5)
    print("SERVER DID NOT COME UP", file=sys.stderr)
    proc.terminate()
    sys.exit(2)


def main():
    no_server = "--no-server" in sys.argv
    proc = None
    if not no_server:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        proc = launch_server(repo_root)

    try:
        run_stages()
    finally:
        if proc is not None:
            print("# tearing down server…")
            try:
                proc.send_signal(signal.SIGINT)
                proc.wait(timeout=10)
            except Exception:
                proc.kill()


def run_stages():
    print(f"# psycopg2 {psycopg2.__version__} -> {DSN}")
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()

    def _ddl():
        cur.execute("CREATE TABLE users (id BIGINT, name CHAR(16))")
        cur.execute("CREATE TABLE posts (id BIGINT, user_id BIGINT, title CHAR(16))")
        cur.execute("CREATE TABLE comments (id BIGINT, post_id BIGINT, body CHAR(16))")
        return "users, posts, comments created"

    if not stage("ddl", _ddl) or not results[-1][1]:
        finish()
        return

    def _seed():
        users = [(1, "alice"), (2, "bob")]
        posts = [(10, 1, "hello"), (11, 1, "world"), (12, 2, "solo")]
        comments = [
            (100, 10, "nice"), (101, 10, "ok"), (102, 11, "wow"),
        ]
        for uid, name in users:
            cur.execute("INSERT INTO users (id, name) VALUES (%s, %s)", (uid, name))
        for pid, uid, title in posts:
            cur.execute(
                "INSERT INTO posts (id, user_id, title) VALUES (%s, %s, %s)",
                (pid, uid, title),
            )
        for cid, pid, body in comments:
            cur.execute(
                "INSERT INTO comments (id, post_id, body) VALUES (%s, %s, %s)",
                (cid, pid, body),
            )
        return "2 users, 3 posts, 3 comments"

    stage("seed", _seed)

    def fetch(sql):
        cur.execute(sql)
        return cur.fetchall()

    def norm(rows):
        out = []
        for r in rows:
            out.append(tuple(c.strip() if isinstance(c, str) else c for c in r))
        return out

    # HEADLINE — a real 3-table chained INNER join.
    def _three_way():
        rows = fetch(
            "SELECT u.name, p.title, c.body "
            "FROM users u JOIN posts p ON u.id = p.user_id "
            "JOIN comments c ON p.id = c.post_id"
        )
        got = sorted(norm(rows))
        expected = sorted([
            ("alice", "hello", "nice"),
            ("alice", "hello", "ok"),
            ("alice", "world", "wow"),
        ])
        assert got == expected, f"got {got}"
        # post 12 (bob/solo) has no comments → INNER chain drops it; bob absent.
        assert all(r[0] != "bob" for r in got), f"bob leaked: {got}"
        return f"3-way combined rows: {got}"

    stage("three_way", _three_way)

    def _three_way_star():
        rows = fetch(
            "SELECT * "
            "FROM users u JOIN posts p ON u.id = p.user_id "
            "JOIN comments c ON p.id = c.post_id"
        )
        got = norm(rows)
        # 3 combined rows; each row has all columns of all 3 tables:
        # users(id,name) + posts(id,user_id,title) + comments(id,post_id,body) = 8.
        assert len(got) == 3, f"expected 3 rows, got {len(got)}: {got}"
        assert all(len(r) == 8 for r in got), f"row widths: {[len(r) for r in got]}"
        # Spot-check the full combined tuple for the (alice, world, wow) row.
        wow = [r for r in got if r[7] == "wow"]
        assert len(wow) == 1, f"wow row: {wow}"
        r = wow[0]
        # users.id=1 users.name=alice posts.id=11 posts.user_id=1 posts.title=world
        # comments.id=102 comments.post_id=11 comments.body=wow
        assert r == (1, "alice", 11, 1, "world", 102, 11, "wow"), f"combined: {r}"
        return f"SELECT * → 3 rows × 8 cols; wow row = {r}"

    stage("three_way_star", _three_way_star)

    def _three_way_where():
        # Filtered 3-way join: only alice's chain (u.id = 1). Same 3 rows here
        # (all combined rows belong to alice), but the WHERE must compile +
        # apply over the full 3-table combined schema.
        rows = fetch(
            "SELECT u.name, p.title, c.body "
            "FROM users u JOIN posts p ON u.id = p.user_id "
            "JOIN comments c ON p.id = c.post_id "
            "WHERE u.id = 1"
        )
        got = sorted(norm(rows))
        expected = sorted([
            ("alice", "hello", "nice"),
            ("alice", "hello", "ok"),
            ("alice", "world", "wow"),
        ])
        assert got == expected, f"got {got}"
        # And WHERE u.id = 2 (bob) → zero rows (bob's post has no comment).
        rows2 = fetch(
            "SELECT u.name, p.title, c.body "
            "FROM users u JOIN posts p ON u.id = p.user_id "
            "JOIN comments c ON p.id = c.post_id "
            "WHERE u.id = 2"
        )
        assert norm(rows2) == [], f"u.id=2 should be empty: {norm(rows2)}"
        return f"WHERE u.id=1 → {got}; WHERE u.id=2 → []"

    stage("three_way_where", _three_way_where)

    cur.close()
    conn.close()
    finish()


def finish():
    print("\n=== SP-PG-SQL-MULTI-JOIN SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, _detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("MULTI-JOIN SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0 if all(ok for _, ok, _ in results) else 1)
