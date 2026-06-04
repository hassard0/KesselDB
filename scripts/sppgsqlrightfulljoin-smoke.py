#!/usr/bin/env python3
"""SP-PG-SQL-RIGHT-FULL-JOIN — RIGHT/FULL outer joins smoke (psycopg2).

Completes the JOIN-type matrix. KesselDB already supports INNER + LEFT on a
binary equi-join; this arc adds RIGHT [OUTER] JOIN and FULL [OUTER] JOIN.

Combined output column order is ALWAYS `a.*, b.*` (the user wrote
`FROM a <flavour> JOIN b`). The four flavours differ only in which unmatched
rows they emit:

    INNER  → matched pairs only
    LEFT   → matched + unmatched-left  (b.* NULL)
    RIGHT  → matched + unmatched-right (a.* NULL)
    FULL   → matched + unmatched-left + unmatched-right (no dup of matched)

Data — two tables with matched AND unmatched rows on BOTH sides:
    authors:  (1 tolkien), (2 orphan)            -- orphan has NO books
    books:    (1 author 1 "lotr"),
              (2 author 1 "hobbit"),
              (3 author 1 "silmarillion"),
              (4 author 99 "homeless")            -- author 99 does NOT exist

  Matched pairs:    tolkien×{lotr, hobbit, silmarillion}
  Unmatched-left:   orphan author (no book)       -> RIGHT NEVER shows it
  Unmatched-right:  homeless book (no author)      -> LEFT NEVER shows it

Stages:
  1. ddl    — CREATE authors, books                                  [setup]
  2. seed   — 2 authors, 4 books (orphan author + homeless book)     [setup]
  3. inner  — only matched pairs (no orphan, no homeless)
  4. left   — matched + orphan author (b.title None)
  5. right  — matched + homeless book (a.name None), column order a.*,b.* (HEADLINE)
  6. full   — matched + orphan author + homeless book, no dup        (HEADLINE)
  7. nulls  — the NULL-filled side reads back as Python None on RIGHT & FULL
  8. right_outer / full_outer — OUTER noise word accepted

HEADLINE: RIGHT JOIN returns matched pairs + the unmatched RIGHT row with the
LEFT columns reading Python None, and the projection column order stays a.*,b.*;
FULL JOIN returns matched + both unmatched sides with no duplicate of the
matched pairs.

The script LAUNCHES its own kesseldb-server (built with --features pg-gateway)
on 127.0.0.1:5556, runs the stages, then tears it down. Pass --no-server to
point at an already-running server.

Positional args: <client_addr> <data_dir> (both optional; defaults provided).
Env: KESSELDB_PG_ADDR=127.0.0.1:5556  KESSELDB_TOKEN=admin

Connection: postgresql://test:admin@127.0.0.1:5556/kesseldb
"""
import os
import signal
import subprocess
import sys
import tempfile
import time
import traceback

import psycopg2

PG_ADDR = "127.0.0.1:5556"
DSN = "postgresql://test:admin@127.0.0.1:5556/kesseldb"
DEFAULT_CLIENT_ADDR = "127.0.0.1:7886"

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


def launch_server(repo_root, client_addr, data_dir):
    """Build (if needed) + launch kesseldb-server with the PG gateway.

    NOTE: the binary MUST be built with --features pg-gateway or the PG
    listener is compiled out and 5556 will never bind.
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
    env = {**os.environ, "KESSELDB_PG_ADDR": PG_ADDR, "KESSELDB_TOKEN": "admin"}
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
    positional = [a for a in sys.argv[1:] if not a.startswith("--")]
    client_addr = positional[0] if len(positional) >= 1 else DEFAULT_CLIENT_ADDR
    data_dir = positional[1] if len(positional) >= 2 else tempfile.mkdtemp(prefix="kdb-rf-data-")
    proc = None
    if not no_server:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        proc = launch_server(repo_root, client_addr, data_dir)

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
        cur.execute("CREATE TABLE authors (id BIGINT, name CHAR(16))")
        cur.execute("CREATE TABLE books (id BIGINT, author_id BIGINT, title CHAR(16))")
        return "authors, books created"

    if not stage("ddl", _ddl) or not results[-1][1]:
        finish()
        return

    def _seed():
        authors = [(1, "tolkien"), (2, "orphan")]  # orphan has no books
        books = [
            (1, 1, "lotr"),
            (2, 1, "hobbit"),
            (3, 1, "silmarillion"),
            (4, 99, "homeless"),  # author 99 does not exist
        ]
        for aid, name in authors:
            cur.execute("INSERT INTO authors (id, name) VALUES (%s, %s)", (aid, name))
        for bid, aid, title in books:
            cur.execute(
                "INSERT INTO books (id, author_id, title) VALUES (%s, %s, %s)",
                (bid, aid, title),
            )
        return "2 authors (1 orphan), 4 books (1 homeless)"

    stage("seed", _seed)

    def fetch(sql):
        cur.execute(sql)
        return cur.fetchall()

    def norm(rows):
        # Strip CHAR padding; leave None (SQL NULL) untouched.
        out = []
        for r in rows:
            out.append(tuple(c.strip() if isinstance(c, str) else c for c in r))
        return out

    MATCHED = [
        ("tolkien", "lotr"),
        ("tolkien", "hobbit"),
        ("tolkien", "silmarillion"),
    ]

    # INNER — only matched pairs.
    def _inner():
        rows = norm(fetch(
            "SELECT a.name, b.title FROM authors a JOIN books b "
            "ON a.id = b.author_id"
        ))
        got = sorted(rows)
        assert got == sorted(MATCHED), f"got {got}"
        assert ("orphan", None) not in got, "INNER must not show orphan author"
        assert (None, "homeless") not in got, "INNER must not show homeless book"
        return f"matched only → {got}"

    stage("inner", _inner)

    # LEFT — matched + orphan author (b.title None). homeless book absent.
    def _left():
        rows = norm(fetch(
            "SELECT a.name, b.title FROM authors a LEFT JOIN books b "
            "ON a.id = b.author_id"
        ))
        got = sorted(rows, key=lambda t: (t[0] or "", t[1] or ""))
        assert ("orphan", None) in got, f"LEFT must include orphan author None: {got}"
        assert all(r[0] != "homeless" for r in got)
        assert (None, "homeless") not in got, "LEFT must NOT show homeless book"
        assert sorted(MATCHED) == sorted([r for r in got if r != ("orphan", None)])
        return f"matched + (orphan, None) → {got}"

    stage("left", _left)

    # HEADLINE — RIGHT — matched + homeless book (a.name None). Column order a.*,b.*.
    def _right():
        rows = norm(fetch(
            "SELECT a.name, b.title FROM authors a RIGHT JOIN books b "
            "ON a.id = b.author_id"
        ))
        got = sorted(rows, key=lambda t: (t[0] or "", t[1] or ""))
        # The unmatched-RIGHT row: a.name NULL, b.title = "homeless".
        assert (None, "homeless") in got, \
            f"RIGHT must include (None, 'homeless'); column order a.*,b.*: {got}"
        # No orphan author (RIGHT drops unmatched-left).
        assert ("orphan", None) not in got, "RIGHT must NOT show orphan author"
        # The remaining rows are exactly the matched pairs.
        assert sorted(MATCHED) == sorted([r for r in got if r != (None, "homeless")])
        # The NULL-filled LEFT column reads back as Python None (not "").
        homeless = [r for r in got if r[1] == "homeless"][0]
        assert homeless[0] is None, f"a.name must be Python None, got {homeless[0]!r}"
        return f"matched + (None, 'homeless') [a.name is None] → {got}"

    stage("right", _right)

    # HEADLINE — FULL — matched + orphan author + homeless book, no dup of matched.
    def _full():
        rows = norm(fetch(
            "SELECT a.name, b.title FROM authors a FULL JOIN books b "
            "ON a.id = b.author_id"
        ))
        got = sorted(rows, key=lambda t: (t[0] or "", t[1] or ""))
        expected = sorted(
            MATCHED + [("orphan", None), (None, "homeless")],
            key=lambda t: (t[0] or "", t[1] or ""),
        )
        assert got == expected, f"FULL got {got}, expected {expected}"
        # No matched pair duplicated.
        for m in MATCHED:
            assert got.count(m) == 1, f"matched pair {m} duplicated in FULL: {got}"
        return f"matched + (orphan,None) + (None,'homeless'), no dup → {got}"

    stage("full", _full)

    # NULL-filled columns read back as Python None on BOTH RIGHT and FULL.
    def _nulls():
        # RIGHT: the homeless row's a.name is None.
        r = norm(fetch(
            "SELECT a.name, b.title FROM authors a RIGHT JOIN books b "
            "ON a.id = b.author_id WHERE b.title = 'homeless'"
        ))
        assert r == [(None, "homeless")], f"RIGHT NULL row: {r}"
        assert r[0][0] is None, "RIGHT a.name must be None"
        # FULL: the orphan row's b.title is None.
        f = norm(fetch(
            "SELECT a.name, b.title FROM authors a FULL JOIN books b "
            "ON a.id = b.author_id WHERE a.name = 'orphan'"
        ))
        assert f == [("orphan", None)], f"FULL NULL row: {f}"
        assert f[0][1] is None, "FULL b.title must be None"
        return "RIGHT a.name=None & FULL b.title=None both read as Python None"

    stage("nulls", _nulls)

    # OUTER noise word accepted on RIGHT/FULL.
    def _right_outer():
        rows = norm(fetch(
            "SELECT a.name, b.title FROM authors a RIGHT OUTER JOIN books b "
            "ON a.id = b.author_id"
        ))
        got = sorted(rows, key=lambda t: (t[0] or "", t[1] or ""))
        assert (None, "homeless") in got, f"RIGHT OUTER: {got}"
        return "RIGHT OUTER JOIN accepted"

    stage("right_outer", _right_outer)

    def _full_outer():
        rows = norm(fetch(
            "SELECT a.name, b.title FROM authors a FULL OUTER JOIN books b "
            "ON a.id = b.author_id"
        ))
        got = sorted(rows, key=lambda t: (t[0] or "", t[1] or ""))
        assert ("orphan", None) in got and (None, "homeless") in got, f"FULL OUTER: {got}"
        return "FULL OUTER JOIN accepted"

    stage("full_outer", _full_outer)

    cur.close()
    conn.close()
    finish()


def finish():
    print("\n=== SP-PG-SQL-RIGHT-FULL-JOIN SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, _detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("RIGHT-FULL-JOIN SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0 if all(ok for _, ok, _ in results) else 1)
