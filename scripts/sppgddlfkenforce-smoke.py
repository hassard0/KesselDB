#!/usr/bin/env python3
"""SP-PG-DDL-FK-ENFORCE — DDL FOREIGN KEY is ENFORCED over the PG wire (psycopg2).

A `FOREIGN KEY` declared in `CREATE TABLE` DDL used to be parsed-and-ignored; this
arc wires it to the pre-existing engine FK enforcement. This smoke proves it
end-to-end against KesselDB's PG-wire gateway with HARD asserts on SQLSTATE.

Stages:
  1. ddl            — CREATE parent + child with FOREIGN KEY(parent_id) REFERENCES parent(id) ON DELETE RESTRICT
  2. seed_parent    — INSERT a parent row (id=1)
  3. good_insert    — INSERT child -> EXISTING parent 1: SUCCEEDS                 (HEADLINE+)
  4. bad_insert     — INSERT child -> MISSING parent 999: FAILS with SQLSTATE 23503 (HEADLINE)
  5. null_fk        — INSERT child with NULL parent_id: SUCCEEDS (NULL allowed)
  6. restrict_block — DELETE the referenced parent 1 while a child refs it: FAILS 23503
  7. restrict_clear — DELETE the child, THEN delete parent 1: SUCCEEDS

The script LAUNCHES its own kesseldb-server (built with --features pg-gateway) on
127.0.0.1:5554, runs the stages, then tears the server down. Positional args:
  <client_addr> <data_dir>   (mirrors launch_server in sppgsqljoinalias-smoke.py)
Pass --no-server to point at an already-running server.

Connection: postgresql://test:admin@127.0.0.1:5554/kesseldb
"""
import os
import signal
import subprocess
import sys
import tempfile
import time
import traceback

import psycopg2
from psycopg2 import errorcodes

PG_ADDR = "127.0.0.1:5554"
DSN = "postgresql://test:admin@127.0.0.1:5554/kesseldb"
CLIENT_ADDR = "127.0.0.1:7884"

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
    data_dir = tempfile.mkdtemp(prefix="kdb-fk-data-")
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
        cur.execute("CREATE TABLE parent (id BIGINT, label CHAR(16))")
        cur.execute(
            "CREATE TABLE child (id BIGINT, parent_id BIGINT, note CHAR(16), "
            "FOREIGN KEY(parent_id) REFERENCES parent(id) ON DELETE RESTRICT)"
        )
        return "parent + child(FK parent_id->parent.id ON DELETE RESTRICT)"

    if not stage("ddl", _ddl) or not results[-1][1]:
        finish()
        return

    def _seed_parent():
        cur.execute("INSERT INTO parent (id, label) VALUES (1, 'p1')")
        return "parent id=1"

    stage("seed_parent", _seed_parent)

    # HEADLINE+ — good insert referencing an EXISTING parent SUCCEEDS.
    def _good_insert():
        cur.execute(
            "INSERT INTO child (id, parent_id, note) VALUES (10, 1, 'ok')"
        )
        cur.execute("SELECT id, parent_id FROM child WHERE id = 10")
        row = cur.fetchone()
        assert row == (10, 1), f"child row not stored: {row}"
        return f"child(10 -> parent 1) inserted -> {row}"

    stage("good_insert", _good_insert)

    # HEADLINE — bad insert referencing a MISSING parent FAILS with 23503.
    def _bad_insert():
        try:
            cur.execute(
                "INSERT INTO child (id, parent_id, note) VALUES (11, 999, 'bad')"
            )
        except psycopg2.Error as e:
            assert e.pgcode == errorcodes.FOREIGN_KEY_VIOLATION, (
                f"expected SQLSTATE 23503, got {e.pgcode} ({e.pgerror})"
            )
            # the failed statement aborted the autocommit txn-less stmt — fine
            return f"rejected with SQLSTATE {e.pgcode} (foreign_key_violation): {str(e).strip()}"
        raise AssertionError("INSERT with a dangling FK should have FAILED")

    stage("bad_insert", _bad_insert)

    # NULL FK is allowed.
    def _null_fk():
        cur.execute(
            "INSERT INTO child (id, parent_id, note) VALUES (12, NULL, 'orphan')"
        )
        cur.execute("SELECT id, parent_id FROM child WHERE id = 12")
        row = cur.fetchone()
        assert row == (12, None), f"NULL-FK child not stored as NULL: {row}"
        return f"child(12, NULL fk) inserted -> {row}"

    stage("null_fk", _null_fk)

    # ON DELETE RESTRICT — deleting a referenced parent FAILS with 23503.
    def _restrict_block():
        try:
            cur.execute("DELETE FROM parent WHERE id = 1")
        except psycopg2.Error as e:
            assert e.pgcode == errorcodes.FOREIGN_KEY_VIOLATION, (
                f"expected SQLSTATE 23503 on RESTRICT, got {e.pgcode} ({e.pgerror})"
            )
            # parent must be untouched
            cur.execute("SELECT id FROM parent WHERE id = 1")
            assert cur.fetchone() == (1,), "parent 1 must survive a blocked delete"
            return f"RESTRICT blocked parent delete with SQLSTATE {e.pgcode}; parent survives"
        raise AssertionError("DELETE of a referenced parent should have FAILED")

    stage("restrict_block", _restrict_block)

    # Clear the referencing child, THEN the parent delete succeeds.
    def _restrict_clear():
        cur.execute("DELETE FROM child WHERE id = 10")
        cur.execute("DELETE FROM parent WHERE id = 1")
        cur.execute("SELECT id FROM parent WHERE id = 1")
        assert cur.fetchone() is None, "parent 1 should be gone after child removed"
        return "child 10 removed -> parent 1 delete succeeds"

    stage("restrict_clear", _restrict_clear)

    cur.close()
    conn.close()
    finish()


def finish():
    print("\n=== SP-PG-DDL-FK-ENFORCE SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, _detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("DDL-FK-ENFORCE SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0 if all(ok for _, ok, _ in results) else 1)
