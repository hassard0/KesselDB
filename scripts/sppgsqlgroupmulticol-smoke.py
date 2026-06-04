#!/usr/bin/env python3
"""SP-PG-SQL-GROUP-MULTI-COL — composite (multi-column) GROUP BY smoke (psycopg2).

Real analytics constantly groups by SEVERAL columns:
    SELECT region, category, COUNT(*), SUM(amount)
      FROM sales GROUP BY region, category;
Before this arc the SQL/engine/gateway only carried ONE group column, so a
two-column GROUP BY was rejected (or collapsed to the first column). This arc
adds a marker-guarded, additive `extra_group_fields` to the group ops, builds a
COMPOSITE group key in the SM, emits each extra column's value in the result
stream after the primary key, and teaches the gateway to recover + render N
group columns. A SINGLE-column GROUP BY stays byte-identical.

Data — a `sales` table with a (region, category) pair having multiple distinct
combinations + repeats:
    east / books    : 3 rows  (amounts 10, 20, 30  → sum  60)
    east / gadgets  : 2 rows  (amounts 40, 50      → sum  90)
    west / books    : 1 row   (amount 100          → sum 100)
    west / gadgets  : 4 rows  (amounts  1,2,3,4    → sum  10)
    north/ toys     : 1 row   (amount 7            → sum   7)

So 5 distinct composite groups; per-region rollup is east=5, west=5, north=1.

Stages:
  1. ddl              — CREATE sales
  2. seed             — 11 rows across 5 (region,category) groups
  3. composite_count  — GROUP BY region, category → one row per combo (HEADLINE)
  4. composite_multi  — COUNT + SUM per composite group
  5. composite_having — ... HAVING COUNT(*) > 1 over composite groups
  6. composite_topn   — ... ORDER BY COUNT(*) DESC LIMIT n over composite groups
  7. single_col_back  — back-compat: GROUP BY region still rolls up per region

HEADLINE: `GROUP BY region, category` returns ONE correct row per distinct
(region, category) with the right COUNT(*).

Env / args (matches the dispatched-task contract):
  KESSELDB_PG_ADDR=127.0.0.1:5557  KESSELDB_TOKEN=admin
  positional: <client_addr> <data_dir>  (the server's own args)

The script LAUNCHES its own kesseldb-server (built --features pg-gateway) on
127.0.0.1:5557, runs the stages, then tears it down. Pass --no-server to point
at an already-running server.

Connection: postgresql://test:admin@127.0.0.1:5557/kesseldb
"""
import os
import signal
import subprocess
import sys
import tempfile
import time
import traceback

import psycopg2

PG_ADDR = os.environ.get("KESSELDB_PG_ADDR", "127.0.0.1:5557")
DSN = f"postgresql://test:admin@{PG_ADDR}/kesseldb"
CLIENT_ADDR = "127.0.0.1:7887"

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
    data_dir = tempfile.mkdtemp(prefix="kdb-gmc-data-")
    env = {**os.environ, "KESSELDB_PG_ADDR": PG_ADDR, "KESSELDB_TOKEN": "admin"}
    print(f"# launching {bin_path} {CLIENT_ADDR} {data_dir} on PG {PG_ADDR}…")
    proc = subprocess.Popen([bin_path, CLIENT_ADDR, data_dir], cwd=repo_root, env=env)
    for _ in range(60):
        try:
            c = psycopg2.connect(DSN, connect_timeout=1)
            c.close()
            print("# server is up")
            return proc, data_dir
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
        proc, _ = launch_server(repo_root)

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
        cur.execute(
            "CREATE TABLE sales (id BIGINT, region CHAR(8), "
            "category CHAR(16), amount BIGINT)"
        )
        return "sales created"

    if not stage("ddl", _ddl) or not results[-1][1]:
        finish()
        return

    def _seed():
        rows = [
            (1, "east", "books", 10), (2, "east", "books", 20), (3, "east", "books", 30),
            (4, "east", "gadgets", 40), (5, "east", "gadgets", 50),
            (6, "west", "books", 100),
            (7, "west", "gadgets", 1), (8, "west", "gadgets", 2),
            (9, "west", "gadgets", 3), (10, "west", "gadgets", 4),
            (11, "north", "toys", 7),
        ]
        for sid, region, cat, amt in rows:
            cur.execute(
                "INSERT INTO sales (id, region, category, amount) "
                "VALUES (%s, %s, %s, %s)",
                (sid, region, cat, amt),
            )
        return f"{len(rows)} rows across 5 (region,category) groups"

    stage("seed", _seed)

    def fetch(sql):
        cur.execute(sql)
        return cur.fetchall()

    # HEADLINE — one row per distinct (region, category) with the right count.
    def _composite_count():
        rows = fetch(
            "SELECT region, category, COUNT(*) FROM sales "
            "GROUP BY region, category"
        )
        got = sorted((r[0].strip(), r[1].strip(), int(r[2])) for r in rows)
        want = sorted([
            ("east", "books", 3), ("east", "gadgets", 2),
            ("west", "books", 1), ("west", "gadgets", 4),
            ("north", "toys", 1),
        ])
        assert got == want, f"{got} != {want}"
        assert len(got) == 5, f"expected 5 composite groups, got {len(got)}"
        return f"5 composite groups, correct counts: {got}"

    stage("composite_count", _composite_count)

    def _composite_multi():
        rows = fetch(
            "SELECT region, category, COUNT(*) AS n, SUM(amount) AS s "
            "FROM sales GROUP BY region, category"
        )
        got = sorted((r[0].strip(), r[1].strip(), int(r[2]), int(r[3])) for r in rows)
        want = sorted([
            ("east", "books", 3, 60), ("east", "gadgets", 2, 90),
            ("west", "books", 1, 100), ("west", "gadgets", 4, 10),
            ("north", "toys", 1, 7),
        ])
        assert got == want, f"{got} != {want}"
        return f"COUNT+SUM per composite group: {got}"

    stage("composite_multi", _composite_multi)

    def _composite_having():
        # HAVING COUNT(*) > 1 keeps east/books(3), east/gadgets(2), west/gadgets(4).
        rows = fetch(
            "SELECT region, category, COUNT(*) AS n FROM sales "
            "GROUP BY region, category HAVING COUNT(*) > 1"
        )
        got = sorted((r[0].strip(), r[1].strip(), int(r[2])) for r in rows)
        want = sorted([
            ("east", "books", 3), ("east", "gadgets", 2), ("west", "gadgets", 4),
        ])
        assert got == want, f"{got} != {want}"
        return f"HAVING COUNT(*)>1 over composite groups: {got}"

    stage("composite_having", _composite_having)

    def _composite_topn():
        # ORDER BY COUNT(*) DESC LIMIT 2 over the 5 composite groups.
        # Counts: west/gadgets=4, east/books=3, east/gadgets=2, then ties at 1.
        rows = fetch(
            "SELECT region, category, COUNT(*) AS n FROM sales "
            "GROUP BY region, category ORDER BY n DESC LIMIT 2"
        )
        got = [(r[0].strip(), r[1].strip(), int(r[2])) for r in rows]
        assert got == [("west", "gadgets", 4), ("east", "books", 3)], got
        return f"top-2 composite groups by count DESC: {got}"

    stage("composite_topn", _composite_topn)

    def _single_col_back():
        # Back-compat: single-column GROUP BY region still rolls up per region.
        rows = fetch(
            "SELECT region, COUNT(*) AS n FROM sales GROUP BY region"
        )
        got = sorted((r[0].strip(), int(r[1])) for r in rows)
        want = sorted([("east", 5), ("west", 5), ("north", 1)])
        assert got == want, f"{got} != {want}"
        return f"single-col GROUP BY region rollup unchanged: {got}"

    stage("single_col_back", _single_col_back)

    cur.close()
    conn.close()
    finish()


def finish():
    print("\n=== SP-PG-SQL-GROUP-MULTI-COL SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, _detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("GROUP-MULTI-COL SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0 if all(ok for _, ok, _ in results) else 1)
