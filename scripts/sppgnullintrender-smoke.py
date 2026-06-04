#!/usr/bin/env python3
"""SP-PG-NULL-INT-RENDER — an omitted/explicit-NULL nullable column reads back as
SQL NULL over the PG wire (psycopg2 `None`), NOT 0/empty.

Root cause (see design doc): the engine's narrow projection stream
(`Op::SelectFields`) carries NO null bitmap, so a nullable column that was
omitted at INSERT (stored as NULL in the record's bitmap) was rendered as its
zero bytes (`0` for an int, empty for text) by the projection-list SELECT path.
The fix routes a non-sorted projection through `SELECT *` (full records, which
DO carry the on-disk null bitmap) and re-projects in the gateway with NULL
fidelity. `SELECT *` was already correct.

HARD asserts (psycopg2):
  1. star_omitted_null   — INSERT omits nullable int; `SELECT *`  → None
  2. proj_omitted_null   — same row; `SELECT thatcol`            → None
  3. explicit_value      — INSERT explicit value; reads back the value (not None)
  4. explicit_null       — INSERT … VALUES (id, NULL); reads back None
  5. text_omitted_null   — nullable TEXT/CHAR omitted; reads back None (generic)
  6. notnull_backcompat  — a NOT-NULL / PK column still reads its real value

The script LAUNCHES its own kesseldb-server (built with --features pg-gateway)
on 127.0.0.1:5555, runs the stages, then tears the server down. Positional args:
  <client_addr> <data_dir>   (mirrors sppgddlfkenforce-smoke.py)
Pass --no-server to point at an already-running server.

Connection: postgresql://test:admin@127.0.0.1:5555/kesseldb
"""
import os
import signal
import subprocess
import sys
import tempfile
import time
import traceback

import psycopg2

PG_ADDR = os.environ.get("KESSELDB_PG_ADDR", "127.0.0.1:5555")
_HOST, _PORT = PG_ADDR.split(":")
DSN = f"postgresql://test:admin@{_HOST}:{_PORT}/kesseldb"
CLIENT_ADDR = "127.0.0.1:7885"

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
    data_dir = tempfile.mkdtemp(prefix="kdb-nz-data-")
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
        # `id` is the NOT-NULL pseudo-PK; `n` a nullable int; `note` a
        # nullable CHAR (proves the fix is generic across column kinds).
        cur.execute("CREATE TABLE t (id BIGINT, n BIGINT, note CHAR(16))")
        return "t(id BIGINT pk, n BIGINT NULL, note CHAR(16) NULL)"

    if not stage("ddl", _ddl) or not results[-1][1]:
        finish(); return

    # 1. Omitted nullable int → `SELECT *` reads it back as None.
    def _star_omitted_null():
        cur.execute("INSERT INTO t (id, note) VALUES (1, 'hi')")
        cur.execute("SELECT * FROM t WHERE id = 1")
        row = cur.fetchone()
        # columns: (id, n, note)
        assert row[0] == 1, f"id mismatch: {row}"
        assert row[1] is None, f"omitted nullable int must be None, got {row[1]!r} (full row {row})"
        return f"SELECT * -> {row}  (n is None)"

    stage("star_omitted_null", _star_omitted_null)

    # 2. Same row via PROJECTION `SELECT n` → None (the headline fix).
    def _proj_omitted_null():
        cur.execute("SELECT n FROM t WHERE id = 1")
        row = cur.fetchone()
        assert row == (None,), f"projected omitted nullable int must be None, got {row!r}"
        return f"SELECT n -> {row}"

    stage("proj_omitted_null", _proj_omitted_null)

    # 3. Explicit value round-trips (both * and projection).
    def _explicit_value():
        cur.execute("INSERT INTO t (id, n, note) VALUES (2, 42, 'x')")
        cur.execute("SELECT n FROM t WHERE id = 2")
        assert cur.fetchone() == (42,), "explicit value must read back as 42"
        cur.execute("SELECT * FROM t WHERE id = 2")
        srow = cur.fetchone()
        assert srow[1] == 42, f"SELECT * explicit value: {srow}"
        return "n=42 round-trips via projection AND *"

    stage("explicit_value", _explicit_value)

    # 4. Explicit NULL literal → None.
    def _explicit_null():
        cur.execute("INSERT INTO t (id, n, note) VALUES (3, NULL, 'y')")
        cur.execute("SELECT n FROM t WHERE id = 3")
        assert cur.fetchone() == (None,), "explicit NULL must read back None (projection)"
        cur.execute("SELECT * FROM t WHERE id = 3")
        srow = cur.fetchone()
        assert srow[1] is None, f"SELECT * explicit NULL: {srow}"
        return "INSERT … VALUES(3, NULL, 'y') -> n is None"

    stage("explicit_null", _explicit_null)

    # 5. Generic: a nullable TEXT/CHAR column omitted → None (not empty string).
    def _text_omitted_null():
        cur.execute("INSERT INTO t (id, n) VALUES (4, 7)")  # note omitted
        cur.execute("SELECT note FROM t WHERE id = 4")
        row = cur.fetchone()
        assert row == (None,), f"omitted nullable CHAR must be None, got {row!r}"
        # And the explicit int still reads back.
        cur.execute("SELECT n FROM t WHERE id = 4")
        assert cur.fetchone() == (7,), "sibling explicit int must survive"
        return f"SELECT note (omitted) -> {row}  (generic across kinds)"

    stage("text_omitted_null", _text_omitted_null)

    # 6. Back-compat: a NOT-NULL / PK column reads its REAL value (never None).
    def _notnull_backcompat():
        cur.execute("SELECT id FROM t WHERE id = 1")
        assert cur.fetchone() == (1,), "PK id must read its real value, not NULL"
        # explicit non-null note also intact
        cur.execute("SELECT note FROM t WHERE id = 1")
        r = cur.fetchone()
        assert r is not None and r[0] is not None and r[0].strip() == "hi", \
            f"explicit CHAR note must survive: {r!r}"
        return "PK id=1 + note='hi' read their real values"

    stage("notnull_backcompat", _notnull_backcompat)

    cur.close()
    conn.close()
    finish()


def finish():
    print("\n=== SP-PG-NULL-INT-RENDER SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, _detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("NULL-INT-RENDER SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0 if all(ok for _, ok, _ in results) else 1)
