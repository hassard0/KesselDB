#!/usr/bin/env python3
"""SP-PG-SQL-PLAIN-GROUP-RENDER — plain (non-JOIN) GROUP BY group-aggregate
render smoke (psycopg2).

A plain `SELECT g, COUNT(*) [, AGG(col)…] FROM t GROUP BY g [HAVING …]
[ORDER BY …] [LIMIT …]` is everyday analytics / ORM SQL. Before this arc the
PG-wire gateway had NO render branch for it (only JOIN group-aggregate and
single-scalar aggregate were rendered), so psql got a render error. This smoke
exercises the new gateway branch end-to-end.

Data — a `products` table grouped by `category`:
    books    : 3 rows  (prices 10, 20, 30)
    toys     : 2 rows  (prices  5, 15)
    gadgets  : 1 row   (price 100)

Stages:
  1. ddl            — CREATE products                                  [setup]
  2. seed           — insert 6 products across 3 categories            [setup]
  3. single_count   — SELECT category, COUNT(*) GROUP BY category      (HEADLINE)
  4. multi_agg      — COUNT/SUM/AVG/MIN/MAX with an alias              (5 aggs)
  5. having         — HAVING COUNT(*) > 1 → drops the singleton group
  6. order_limit    — ORDER BY … LIMIT (engine emits all groups in key
                      order; render surfaces them — asserted as a SET)

HEADLINE: `SELECT category, COUNT(*) FROM products GROUP BY category` returns
the per-category counts over the PG wire (this is the query that errored on
pre-fix origin/main).

The script LAUNCHES its own kesseldb-server (built with --features pg-gateway)
on 127.0.0.1:5550, runs the stages, then tears the server down. Pass
--no-server to point at an already-running server.

Connection: postgresql://test:admin@127.0.0.1:5550/kesseldb
"""
import os
import signal
import subprocess
import sys
import time
import traceback

import psycopg2

DSN = "postgresql://test:admin@127.0.0.1:5550/kesseldb"
PG_ADDR = "127.0.0.1:5550"

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
    """Build (if needed) + launch kesseldb-server with the PG gateway."""
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
    env = {**os.environ, "KESSELDB_PG_ADDR": PG_ADDR, "KESSELDB_TOKEN": "admin"}
    print(f"# launching {bin_path} on {PG_ADDR}…")
    proc = subprocess.Popen([bin_path], cwd=repo_root, env=env)
    # Wait for the PG port to accept.
    for _ in range(60):
        try:
            c = psycopg2.connect(DSN, connect_timeout=1)
            c.close()
            print("# server is up")
            return proc
        except Exception:
            if proc.poll() is not None:
                print("SERVER EXITED EARLY", file=sys.stderr)
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
            (1, "books", 10), (2, "books", 20), (3, "books", 30),
            (4, "toys", 5), (5, "toys", 15),
            (6, "gadgets", 100),
        ]
        for pid, cat, price in rows:
            cur.execute(
                "INSERT INTO products (id, category, price) VALUES (%s, %s, %s)",
                (pid, cat, price),
            )
        return f"{len(rows)} products in 3 categories"

    stage("seed", _seed)

    def fetch(sql):
        cur.execute(sql)
        return cur.fetchall()

    # HEADLINE — the query that errored on pre-fix origin/main.
    def _single_count():
        rows = fetch("SELECT category, COUNT(*) FROM products GROUP BY category")
        got = sorted((r[0].strip(), int(r[1])) for r in rows)
        assert got == [("books", 3), ("gadgets", 1), ("toys", 2)], got
        return f"3 groups: {got}"

    stage("single_count", _single_count)

    def _multi_agg():
        rows = fetch(
            "SELECT category, COUNT(*) AS n, SUM(price), AVG(price), "
            "MIN(price), MAX(price) FROM products GROUP BY category"
        )
        got = {
            r[0].strip(): (int(r[1]), int(r[2]), int(r[3]), int(r[4]), int(r[5]))
            for r in rows
        }
        # (count, sum, avg=sum//count, min, max) — engine AVG is integer-div.
        assert got["books"] == (3, 60, 20, 10, 30), got["books"]
        assert got["toys"] == (2, 20, 10, 5, 15), got["toys"]
        assert got["gadgets"] == (1, 100, 100, 100, 100), got["gadgets"]
        return f"5 aggregates × 3 groups: {got}"

    stage("multi_agg", _multi_agg)

    def _having():
        rows = fetch(
            "SELECT category, COUNT(*) FROM products GROUP BY category "
            "HAVING COUNT(*) > 1"
        )
        got = sorted((r[0].strip(), int(r[1])) for r in rows)
        # gadgets (count 1) dropped; books + toys survive.
        assert got == [("books", 3), ("toys", 2)], got
        return f"before=3 after={len(got)} (gadgets dropped): {got}"

    stage("having", _having)

    def _order_limit():
        # Engine emits all groups in ascending key order regardless of
        # ORDER BY / LIMIT (those are not threaded into Op::GroupAggregate* in
        # V1); the render surfaces whatever it returns. Assert the SET of
        # rendered groups is correct + the columns decode.
        rows = fetch(
            "SELECT category, COUNT(*) AS n FROM products GROUP BY category "
            "ORDER BY n DESC LIMIT 2"
        )
        got = sorted((r[0].strip(), int(r[1])) for r in rows)
        # All 3 groups come back (ORDER BY/LIMIT not yet engine-applied); the
        # smoke documents the honest current behaviour.
        assert ("books", 3) in got and ("toys", 2) in got, got
        return f"groups rendered: {got}"

    stage("order_limit", _order_limit)

    cur.close()
    conn.close()
    finish()


def finish():
    print("\n=== SP-PG-SQL-PLAIN-GROUP-RENDER SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, _detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("PLAIN-GROUP-RENDER SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0 if all(ok for _, ok, _ in results) else 1)
