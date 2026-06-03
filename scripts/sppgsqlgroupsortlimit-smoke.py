#!/usr/bin/env python3
"""SP-PG-SQL-GROUP-SORT-LIMIT — plain (non-JOIN) GROUP BY ORDER BY / LIMIT /
OFFSET smoke (psycopg2).

A plain `SELECT g, COUNT(*) AS n FROM t GROUP BY g ORDER BY n DESC LIMIT k
OFFSET m` is the headline top-N-per-group analytics query ("the K categories
with the most rows", "top sellers by revenue"). Before this arc the SQL layer
PARSED the ORDER BY / LIMIT / OFFSET but DROPPED them — Op::GroupAggregate /
Op::GroupAggregateMulti carried no sort/page fields, so the engine always
returned ALL groups in ascending group-key order regardless. This arc threads
them into a `GroupSort` on those ops and the engine APPLIES them (filter →
sort → offset → limit). This smoke proves the new behaviour over the PG wire.

Data — a `products` table grouped by `category`, DISTINCT row counts so the
descending-count order is unambiguous:
    books    : 4 rows  (prices 10, 20, 30, 40 → sum 100)
    gadgets  : 3 rows  (prices 50, 60, 70     → sum 180)
    toys     : 2 rows  (prices  5, 15         → sum  20)
    misc     : 1 row   (price 100             → sum 100)

Ascending key order is [books, gadgets, misc, toys].
Descending COUNT(*) order is [books(4), gadgets(3), toys(2), misc(1)].

Stages:
  1. ddl              — CREATE products                                [setup]
  2. seed             — 10 products across 4 categories                [setup]
  3. order_count_desc — ORDER BY COUNT(*) DESC → descending-count order (HEADLINE)
  4. order_limit2     — ... DESC LIMIT 2 → ONLY the top 2 groups
  5. order_limit_off  — ... DESC LIMIT 2 OFFSET 1 → the right window
  6. order_key_asc    — ORDER BY category ASC → sort by key (still works)
  7. having_order_lim — HAVING + ORDER BY + LIMIT compose

HEADLINE: `... ORDER BY COUNT(*) DESC` returns groups in DESCENDING count order
(NOT ascending key order). On pre-fix origin/main this returned all 4 groups in
key order [books, gadgets, misc, toys] regardless.

The script LAUNCHES its own kesseldb-server (built with --features pg-gateway)
on 127.0.0.1:5551, runs the stages, then tears the server down. Pass
--no-server to point at an already-running server.

Connection: postgresql://test:admin@127.0.0.1:5551/kesseldb
"""
import os
import signal
import subprocess
import sys
import tempfile
import time
import traceback

import psycopg2

PG_ADDR = "127.0.0.1:5551"
DSN = "postgresql://test:admin@127.0.0.1:5551/kesseldb"
CLIENT_ADDR = "127.0.0.1:7879"

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
    listener is compiled out and 5551 will never bind.
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
    data_dir = tempfile.mkdtemp(prefix="kdb-gsl-data-")
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
        cur.execute("CREATE TABLE products (id BIGINT, category CHAR(16), price BIGINT)")
        return "products created"

    if not stage("ddl", _ddl) or not results[-1][1]:
        finish()
        return

    def _seed():
        rows = [
            (1, "books", 10), (2, "books", 20), (3, "books", 30), (4, "books", 40),
            (5, "gadgets", 50), (6, "gadgets", 60), (7, "gadgets", 70),
            (8, "toys", 5), (9, "toys", 15),
            (10, "misc", 100),
        ]
        for pid, cat, price in rows:
            cur.execute(
                "INSERT INTO products (id, category, price) VALUES (%s, %s, %s)",
                (pid, cat, price),
            )
        return f"{len(rows)} products in 4 categories (counts 4/3/2/1)"

    stage("seed", _seed)

    def fetch(sql):
        cur.execute(sql)
        return cur.fetchall()

    # HEADLINE — descending COUNT(*) order, NOT ascending key order. ORDER
    # PRESERVED (list, not set): proves the engine reordered.
    def _order_count_desc():
        rows = fetch(
            "SELECT category, COUNT(*) AS n FROM products GROUP BY category "
            "ORDER BY n DESC"
        )
        got = [(r[0].strip(), int(r[1])) for r in rows]
        # Descending count: books(4), gadgets(3), toys(2), misc(1).
        assert got == [("books", 4), ("gadgets", 3), ("toys", 2), ("misc", 1)], got
        # And it must NOT be ascending key order (the pre-fix behaviour).
        key_order = [("books", 4), ("gadgets", 3), ("misc", 1), ("toys", 2)]
        assert got != key_order, "still in key order — ORDER BY not applied!"
        return f"DESC count order (not key order): {got}"

    stage("order_count_desc", _order_count_desc)

    def _order_limit2():
        rows = fetch(
            "SELECT category, COUNT(*) AS n FROM products GROUP BY category "
            "ORDER BY n DESC LIMIT 2"
        )
        got = [(r[0].strip(), int(r[1])) for r in rows]
        # ONLY the top 2 groups (4 total exist).
        assert got == [("books", 4), ("gadgets", 3)], got
        return f"top 2 of 4 only: {got}"

    stage("order_limit2", _order_limit2)

    def _order_limit_offset():
        rows = fetch(
            "SELECT category, COUNT(*) AS n FROM products GROUP BY category "
            "ORDER BY n DESC LIMIT 2 OFFSET 1"
        )
        got = [(r[0].strip(), int(r[1])) for r in rows]
        # Skip 1 (books), take 2: gadgets(3), toys(2).
        assert got == [("gadgets", 3), ("toys", 2)], got
        return f"window LIMIT 2 OFFSET 1: {got}"

    stage("order_limit_offset", _order_limit_offset)

    def _order_key_asc():
        rows = fetch(
            "SELECT category, COUNT(*) AS n FROM products GROUP BY category "
            "ORDER BY category ASC"
        )
        got = [(r[0].strip(), int(r[1])) for r in rows]
        # Ascending key (alpha): books, gadgets, misc, toys.
        assert got == [("books", 4), ("gadgets", 3), ("misc", 1), ("toys", 2)], got
        return f"ORDER BY key ASC: {got}"

    stage("order_key_asc", _order_key_asc)

    def _having_order_limit():
        # HAVING COUNT(*) > 1 drops misc(1); then ORDER BY SUM(price) DESC LIMIT 2.
        # Sums of survivors: books=100, gadgets=180, toys=20.
        # DESC by sum: gadgets(180), books(100), toys(20); LIMIT 2 → gadgets, books.
        rows = fetch(
            "SELECT category, COUNT(*) AS n, SUM(price) AS s FROM products "
            "GROUP BY category HAVING COUNT(*) > 1 ORDER BY s DESC LIMIT 2"
        )
        got = [(r[0].strip(), int(r[1]), int(r[2])) for r in rows]
        assert got == [("gadgets", 3, 180), ("books", 4, 100)], got
        return f"HAVING→sort(SUM desc)→LIMIT 2: {got}"

    stage("having_order_limit", _having_order_limit)

    cur.close()
    conn.close()
    finish()


def finish():
    print("\n=== SP-PG-SQL-GROUP-SORT-LIMIT SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, _detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("GROUP-SORT-LIMIT SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0 if all(ok for _, ok, _ in results) else 1)
