#!/usr/bin/env python3
"""SP-PG-SQL-JOIN-ALIAS — table aliases in JOIN queries smoke (psycopg2).

The parser accepted `FROM users u JOIN posts p ON …` but column qualifiers only
resolved against the FULL table name, so the universal ORM/SQL form failed:

    SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id;

This arc builds an alias→table map from the FROM/JOIN clause and resolves every
qualifier (ON, WHERE, projection, ORDER BY) through it, for binary AND
multi-table (3+) INNER joins. Resolution happens entirely in kessel-sql — the
alias is rewritten to the full table name before matching the combined KTR1
schema — so the wire `Op` is byte-identical to the spelled-out form, and the
full-table-name form keeps working (back-compat).

Data — the classic users → posts → comments FK chain:
    users:    (1 alice), (2 bob)
    posts:    (10 user 1 "hello"), (11 user 1 "world"), (12 user 2 "solo")
    comments: (100 post 10 "nice"), (101 post 10 "ok"), (102 post 11 "wow")
              (post 12 has NO comments → INNER chain drops bob)

Stages:
  1. ddl              — CREATE users, posts, comments                    [setup]
  2. seed             — 2 users, 3 posts, 3 comments                     [setup]
  3. alias_binary     — SELECT u.name, p.title FROM users u JOIN posts p (HEADLINE)
  4. full_names       — SAME query with FULL table names (back-compat)
  5. as_form          — FROM users AS u JOIN posts AS p
  6. alias_three_way  — aliased 3-way chain projecting u.name, p.title, c.body
  7. alias_where      — aliased WHERE (u.id = 1)
  8. alias_order_by   — aliased ORDER BY (p.title)

HEADLINE: `SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id`
returns the correct rows over the PG wire, AND the full-table-name form returns
identical rows.

The script LAUNCHES its own kesseldb-server (built with --features pg-gateway)
on 127.0.0.1:5553, runs the stages, then tears the server down. Pass
--no-server to point at an already-running server.

Connection: postgresql://test:admin@127.0.0.1:5553/kesseldb
"""
import os
import signal
import subprocess
import sys
import tempfile
import time
import traceback

import psycopg2

PG_ADDR = "127.0.0.1:5553"
DSN = "postgresql://test:admin@127.0.0.1:5553/kesseldb"
CLIENT_ADDR = "127.0.0.1:7883"

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
    listener is compiled out and 5553 will never bind.
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
    data_dir = tempfile.mkdtemp(prefix="kdb-ja-data-")
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
        comments = [(100, 10, "nice"), (101, 10, "ok"), (102, 11, "wow")]
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

    # alice has posts hello + world; bob has solo (no comment). The aliased
    # binary join users↔posts pairs every user with each of their posts.
    BINARY_EXPECTED = sorted([
        ("alice", "hello"),
        ("alice", "world"),
        ("bob", "solo"),
    ])

    # HEADLINE — aliased binary join.
    def _alias_binary():
        rows = fetch(
            "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id"
        )
        got = sorted(norm(rows))
        assert got == BINARY_EXPECTED, f"got {got}"
        return f"u.name/p.title (aliased) → {got}"

    stage("alias_binary", _alias_binary)

    # Back-compat — the SAME query with FULL table names returns identical rows.
    def _full_names():
        rows = fetch(
            "SELECT users.name, posts.title "
            "FROM users JOIN posts ON users.id = posts.user_id"
        )
        got = sorted(norm(rows))
        assert got == BINARY_EXPECTED, f"full-name back-compat got {got}"
        return f"full table names → {got} (identical to aliased)"

    stage("full_names", _full_names)

    # `AS` form — FROM users AS u JOIN posts AS p.
    def _as_form():
        rows = fetch(
            "SELECT u.name, p.title "
            "FROM users AS u JOIN posts AS p ON u.id = p.user_id"
        )
        got = sorted(norm(rows))
        assert got == BINARY_EXPECTED, f"AS-form got {got}"
        return f"FROM users AS u JOIN posts AS p → {got}"

    stage("as_form", _as_form)

    # Aliased 3-way chain — u.name, p.title, c.body.
    def _alias_three_way():
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
        assert all(r[0] != "bob" for r in got), f"bob leaked: {got}"
        return f"3-way aliased chain → {got}"

    stage("alias_three_way", _alias_three_way)

    # Aliased WHERE — only alice's chain (u.id = 1).
    def _alias_where():
        rows = fetch(
            "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id "
            "WHERE u.id = 1"
        )
        got = sorted(norm(rows))
        expected = sorted([("alice", "hello"), ("alice", "world")])
        assert got == expected, f"WHERE u.id=1 got {got}"
        # And u.id = 2 (bob) → just his solo post.
        rows2 = fetch(
            "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id "
            "WHERE u.id = 2"
        )
        got2 = sorted(norm(rows2))
        assert got2 == [("bob", "solo")], f"WHERE u.id=2 got {got2}"
        return f"WHERE u.id=1 → {got}; WHERE u.id=2 → {got2}"

    stage("alias_where", _alias_where)

    # Aliased ORDER BY — sort by p.title ascending.
    def _alias_order_by():
        rows = fetch(
            "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id "
            "ORDER BY p.title"
        )
        got = norm(rows)
        titles = [r[1] for r in got]
        assert titles == sorted(titles), f"not sorted by p.title: {titles}"
        # hello < solo < world alphabetically.
        assert titles == ["hello", "solo", "world"], f"order: {titles}"
        return f"ORDER BY p.title → {titles}"

    stage("alias_order_by", _alias_order_by)

    cur.close()
    conn.close()
    finish()


def finish():
    print("\n=== SP-PG-SQL-JOIN-ALIAS SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, _detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("JOIN-ALIAS SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0 if all(ok for _, ok, _ in results) else 1)
