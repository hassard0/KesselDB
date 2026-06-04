#!/usr/bin/env python3
"""SP-PG-SQL-SUBQUERY-WHERE — non-correlated subqueries in WHERE (psycopg2).

KesselDB now supports the three universal non-correlated WHERE-subquery shapes:

    SELECT name FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100);
    SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM banned);
    SELECT name FROM products WHERE price = (SELECT MAX(price) FROM products);

Two-phase at the gateway: the inner SELECT runs through the normal engine SQL
render path first, its single projected column's values are spliced into the
outer query as a literal list / scalar, and the rewritten outer re-dispatches
through the normal path. No engine / Op / wire change.

Data:
    users:    (1 alice), (2 bob), (3 carol), (4 dave)
    orders:   (10 user 1 total 150), (11 user 2 total 50), (12 user 3 total 200)
              → users 1 and 3 have a qualifying order (total > 100)
    banned:   (user 2)
    products: (1 widget 100), (2 gadget 250 cat 'tools'), (3 gizmo 250 cat 'toys'),
              (4 sprocket 80 cat 'tools')
    featured: (cat 'tools')

Stages:
  1. ddl                — CREATE users, orders, banned, products, featured  [setup]
  2. seed               — populate the tables                               [setup]
  3. in_subquery        — id IN (SELECT user_id FROM orders WHERE total>100) (HEADLINE)
  4. not_in_subquery    — id NOT IN (SELECT user_id FROM banned)
  5. scalar_max         — price = (SELECT MAX(price) FROM products)  (scalar) (HEADLINE)
  6. empty_in           — id IN (SELECT user_id FROM orders WHERE total>9999) → 0 rows
  7. empty_not_in       — id NOT IN (empty) → all users (documented NULL edge)
  8. string_subquery    — category IN (SELECT cat FROM featured) — value quoting
  9. wrong_col_count    — inner projects 2 cols → clean error
 10. scalar_multi_row   — scalar inner returns >1 row → clean cardinality error

HEADLINE: `WHERE id IN (SELECT …)` returns only the qualifying users AND the
scalar `WHERE price = (SELECT MAX(price) …)` returns the max-price product(s).

Usage: sppgsqlsubquerywhere-smoke.py [<client_addr> <data_dir>] [--no-server]
Env: KESSELDB_PG_ADDR=127.0.0.1:5559  KESSELDB_TOKEN=admin
"""
import os
import signal
import subprocess
import sys
import tempfile
import time
import traceback

import psycopg2

PG_ADDR = os.environ.get("KESSELDB_PG_ADDR", "127.0.0.1:5559")
DSN = f"postgresql://test:admin@{PG_ADDR}/kesseldb"
CLIENT_ADDR = "127.0.0.1:7889"

results = []  # (stage, ok, detail)


def stage(name, fn):
    try:
        detail = fn()
        results.append((name, True, detail))
        print(f"STAGE {name}: PASS {detail if detail else ''}".rstrip())
        return True
    except Exception as e:  # noqa: BLE001
        msg = f"{type(e).__name__}: {e}"
        results.append((name, False, msg))
        print(f"STAGE {name}: FAIL {msg}")
        tb = traceback.format_exc().strip().splitlines()
        if tb:
            print(f"    {tb[-1]}")
        return False


def launch_server(repo_root):
    print("# building kesseldb-server --features pg-gateway (release)…")
    target_dir = os.environ.get("CARGO_TARGET_DIR")
    build = subprocess.run(
        ["cargo", "build", "--release", "-p", "kesseldb-server",
         "--features", "pg-gateway"],
        cwd=repo_root, env={**os.environ},
    )
    if build.returncode != 0:
        print("BUILD FAILED", file=sys.stderr)
        sys.exit(2)
    base = target_dir if target_dir else os.path.join(repo_root, "target")
    bin_path = os.path.join(base, "release", "kesseldb")
    data_dir = tempfile.mkdtemp(prefix="kdb-sq-data-")
    env = {**os.environ, "KESSELDB_PG_ADDR": PG_ADDR, "KESSELDB_TOKEN": "admin"}
    client_addr = sys.argv[1] if len(sys.argv) > 2 and not sys.argv[1].startswith("--") else CLIENT_ADDR
    if len(sys.argv) > 2 and not sys.argv[2].startswith("--"):
        data_dir = sys.argv[2]
    print(f"# launching {bin_path} {client_addr} {data_dir} on PG {PG_ADDR}…")
    proc = subprocess.Popen([bin_path, client_addr, data_dir], cwd=repo_root, env=env)
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
        cur.execute("CREATE TABLE orders (id BIGINT, user_id BIGINT, total BIGINT)")
        cur.execute("CREATE TABLE banned (id BIGINT, user_id BIGINT)")
        cur.execute("CREATE TABLE products (id BIGINT, name CHAR(16), price BIGINT, category CHAR(16))")
        cur.execute("CREATE TABLE featured (id BIGINT, cat CHAR(16))")
        return "5 tables created"

    if not stage("ddl", _ddl) or not results[-1][1]:
        finish()
        return

    def _seed():
        for uid, name in [(1, "alice"), (2, "bob"), (3, "carol"), (4, "dave")]:
            cur.execute("INSERT INTO users (id, name) VALUES (%s, %s)", (uid, name))
        for oid, uid, total in [(10, 1, 150), (11, 2, 50), (12, 3, 200)]:
            cur.execute("INSERT INTO orders (id, user_id, total) VALUES (%s, %s, %s)", (oid, uid, total))
        cur.execute("INSERT INTO banned (id, user_id) VALUES (%s, %s)", (1, 2))
        for pid, name, price, cat in [
            (1, "widget", 100, "tools"), (2, "gadget", 250, "tools"),
            (3, "gizmo", 250, "toys"), (4, "sprocket", 80, "tools"),
        ]:
            cur.execute("INSERT INTO products (id, name, price, category) VALUES (%s, %s, %s, %s)",
                        (pid, name, price, cat))
        cur.execute("INSERT INTO featured (id, cat) VALUES (%s, %s)", (1, "tools"))
        return "4 users, 3 orders, 1 banned, 4 products, 1 featured"

    stage("seed", _seed)

    def fetch(sql):
        cur.execute(sql)
        return cur.fetchall()

    def names(rows):
        return sorted(c.strip() if isinstance(c, str) else c for (c,) in rows)

    # HEADLINE — IN subquery: users 1 (alice) and 3 (carol) have total>100.
    def _in_subquery():
        rows = fetch("SELECT name FROM users WHERE id IN "
                     "(SELECT user_id FROM orders WHERE total > 100)")
        got = names(rows)
        assert got == ["alice", "carol"], f"got {got}"
        return f"id IN (SELECT user_id WHERE total>100) → {got}"

    stage("in_subquery", _in_subquery)

    # NOT IN — complement of banned (user 2 = bob): alice, carol, dave.
    def _not_in_subquery():
        rows = fetch("SELECT name FROM users WHERE id NOT IN "
                     "(SELECT user_id FROM banned)")
        got = names(rows)
        assert got == ["alice", "carol", "dave"], f"got {got}"
        return f"id NOT IN (SELECT user_id FROM banned) → {got}"

    stage("not_in_subquery", _not_in_subquery)

    # HEADLINE — scalar MAX: max price is 250 → gadget + gizmo.
    def _scalar_max():
        rows = fetch("SELECT name FROM products WHERE price = "
                     "(SELECT MAX(price) FROM products)")
        got = names(rows)
        assert got == ["gadget", "gizmo"], f"got {got}"
        return f"price = (SELECT MAX(price)) → {got}"

    stage("scalar_max", _scalar_max)

    # Empty inner result → IN returns no rows.
    def _empty_in():
        rows = fetch("SELECT name FROM users WHERE id IN "
                     "(SELECT user_id FROM orders WHERE total > 9999)")
        got = names(rows)
        assert got == [], f"expected empty, got {got}"
        return "id IN (empty) → 0 rows"

    stage("empty_in", _empty_in)

    # Empty inner result → NOT IN returns all rows (KesselDB non-NULL rows).
    def _empty_not_in():
        rows = fetch("SELECT name FROM users WHERE id NOT IN "
                     "(SELECT user_id FROM orders WHERE total > 9999)")
        got = names(rows)
        assert got == ["alice", "bob", "carol", "dave"], f"got {got}"
        return f"id NOT IN (empty) → all users {got} (NULL-row edge documented)"

    stage("empty_not_in", _empty_not_in)

    # String-valued subquery — proves value quoting/escaping. featured.cat =
    # 'tools' → products in category 'tools': widget, gadget, sprocket.
    def _string_subquery():
        rows = fetch("SELECT name FROM products WHERE category IN "
                     "(SELECT cat FROM featured)")
        got = names(rows)
        assert got == ["gadget", "sprocket", "widget"], f"got {got}"
        return f"category IN (SELECT cat FROM featured) → {got}"

    stage("string_subquery", _string_subquery)

    # Inner projects 2 columns → clean error (not silently-wrong rows).
    def _wrong_col_count():
        try:
            fetch("SELECT name FROM users WHERE id IN "
                  "(SELECT id, user_id FROM orders)")
            raise AssertionError("expected an error for 2-column subquery")
        except psycopg2.Error as e:
            return f"2-col subquery rejected: {str(e).strip()[:60]}"

    stage("wrong_col_count", _wrong_col_count)

    # Scalar subquery returning >1 row → clean cardinality error.
    def _scalar_multi_row():
        try:
            fetch("SELECT name FROM products WHERE price = "
                  "(SELECT price FROM products)")
            raise AssertionError("expected cardinality error for multi-row scalar")
        except psycopg2.Error as e:
            return f"multi-row scalar rejected: {str(e).strip()[:60]}"

    stage("scalar_multi_row", _scalar_multi_row)

    cur.close()
    conn.close()
    finish()


def finish():
    print("\n=== SP-PG-SQL-SUBQUERY-WHERE SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, _detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("SUBQUERY-WHERE SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0 if all(ok for _, ok, _ in results) else 1)
