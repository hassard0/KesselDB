#!/usr/bin/env python3
"""SP-PG-SQL-DISTINCT — `SELECT DISTINCT` row-dedup render smoke (psycopg2).

`SELECT DISTINCT col FROM t` (get the unique values of a column) and
`SELECT DISTINCT a, b FROM t` (unique tuples) are everyday SQL. Before this arc
the PG-wire gateway returned ALL rows for a DISTINCT query (the keyword was
silently dropped), so a client got duplicates. This arc implements DISTINCT at
the RENDER layer: the engine returns every row, the gateway dedups the emitted
DataRows (keeping the FIRST occurrence in scan order) and the `SELECT N` tag
reports the DEDUPED count. NULL is NOT distinct from NULL (SQL semantics).

Data — an `events` table with deliberate duplicates and a NULL region:
    (1, 'us',   'click')
    (2, 'us',   'view')
    (3, 'eu',   'click')
    (4, 'eu',   'click')   <- dup of (region,category)=('eu','click')
    (5, 'us',   'click')   <- dup region 'us', dup (us,click) of row 1
    (6, NULL,   'view')    <- NULL region
    (7, NULL,   'view')    <- dup of NULL/view (NULL not distinct from NULL)

Distinct regions   : {us, eu, NULL}           → 3
Distinct (r,c) pairs: us/click, us/view, eu/click, NULL/view → 4
Total rows          : 7

Stages:
  1. ddl              — CREATE events                                   [setup]
  2. seed             — insert 7 rows incl. dups + 2 NULL-region rows   [setup]
  3. distinct_region  — SELECT DISTINCT region → 3 unique (count<total)  (HEADLINE)
  4. nondistinct_back — SELECT region (NO distinct) → all 7 rows         (back-compat)
  5. distinct_pair    — SELECT DISTINCT region, category → 4 unique pairs
  6. distinct_null    — NULL region appears EXACTLY once under DISTINCT
  7. distinct_star    — SELECT DISTINCT * → unique whole rows (7 here, all uniq)
  8. distinct_dup_star— a real whole-row dup collapses under DISTINCT *

HEADLINE: `SELECT DISTINCT region FROM events` returns the 3 UNIQUE regions
(count 3 < 7 total) while the non-distinct `SELECT region FROM events` still
returns all 7 rows.

The script LAUNCHES its own kesseldb-server (built with --features pg-gateway)
on 127.0.0.1:5558 (positional `<client_addr> <data_dir>`), runs the stages,
then tears the server down. Pass --no-server to point at a running server.

Connection: postgresql://test:admin@127.0.0.1:5558/kesseldb
"""
import os
import signal
import subprocess
import sys
import tempfile
import time
import traceback

import psycopg2

PG_ADDR = "127.0.0.1:5558"
DSN = "postgresql://test:admin@127.0.0.1:5558/kesseldb"
CLIENT_ADDR = "127.0.0.1:7888"

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
    listener is compiled out and 5558 will never bind.
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
    # Honor CARGO_TARGET_DIR (vulcan runs with a per-worktree isolated target
    # dir); fall back to the in-tree target/ when it is unset.
    target_dir = os.environ.get(
        "CARGO_TARGET_DIR", os.path.join(repo_root, "target")
    )
    bin_path = os.path.join(target_dir, "release", "kesseldb")
    data_dir = tempfile.mkdtemp(prefix="kdb-di-data-")
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
        cur.execute(
            "CREATE TABLE events (id BIGINT, region CHAR(8), category CHAR(8))"
        )
        return "events created"

    if not stage("ddl", _ddl) or not results[-1][1]:
        finish()
        return

    def _seed():
        rows = [
            (1, "us", "click"),
            (2, "us", "view"),
            (3, "eu", "click"),
            (4, "eu", "click"),   # dup (eu, click)
            (5, "us", "click"),   # dup region us; dup (us, click)
            (6, None, "view"),    # NULL region
            (7, None, "view"),    # dup NULL/view
        ]
        for rid, region, cat in rows:
            cur.execute(
                "INSERT INTO events (id, region, category) VALUES (%s, %s, %s)",
                (rid, region, cat),
            )
        return f"{len(rows)} events (with dups + 2 NULL-region rows)"

    stage("seed", _seed)

    def fetch(sql):
        cur.execute(sql)
        return cur.fetchall()

    def norm(v):
        # CHAR(8) comes back space/NUL padded; strip. NULL stays None.
        return None if v is None else v.strip()

    # HEADLINE — DISTINCT region returns the 3 UNIQUE regions (count < total).
    def _distinct_region():
        rows = fetch("SELECT DISTINCT region FROM events")
        got = sorted(
            (norm(r[0]) if r[0] is not None else "<NULL>") for r in rows
        )
        assert len(rows) == 3, f"expected 3 distinct regions, got {len(rows)}: {got}"
        assert got == ["<NULL>", "eu", "us"], got
        return f"3 unique regions {got} (< 7 total rows)"

    stage("distinct_region", _distinct_region)

    # BACK-COMPAT — the NON-distinct projection still returns ALL 7 rows.
    def _nondistinct_back():
        rows = fetch("SELECT region FROM events")
        assert len(rows) == 7, f"non-distinct must return all 7 rows, got {len(rows)}"
        return f"non-distinct SELECT region → all {len(rows)} rows (dups kept)"

    stage("nondistinct_back", _nondistinct_back)

    def _distinct_pair():
        rows = fetch("SELECT DISTINCT region, category FROM events")
        got = sorted(
            (
                (norm(r[0]) if r[0] is not None else "<NULL>"),
                norm(r[1]),
            )
            for r in rows
        )
        expected = sorted([
            ("us", "click"),
            ("us", "view"),
            ("eu", "click"),
            ("<NULL>", "view"),
        ])
        assert len(rows) == 4, f"expected 4 unique pairs, got {len(rows)}: {got}"
        assert got == expected, got
        return f"4 unique (region,category) pairs: {got}"

    stage("distinct_pair", _distinct_pair)

    def _distinct_null():
        # NULL is NOT distinct from NULL — the two NULL-region rows collapse to
        # exactly one NULL in DISTINCT region.
        rows = fetch("SELECT DISTINCT region FROM events")
        nulls = [r for r in rows if r[0] is None]
        assert len(nulls) == 1, f"NULL must appear exactly once, got {len(nulls)}"
        return "NULL region appears exactly once under DISTINCT"

    stage("distinct_null", _distinct_null)

    def _distinct_star():
        # Every row has a distinct id ⇒ all 7 whole rows are unique.
        rows = fetch("SELECT DISTINCT * FROM events")
        assert len(rows) == 7, f"7 distinct whole rows expected, got {len(rows)}"
        ids = sorted(int(r[0]) for r in rows)
        assert ids == [1, 2, 3, 4, 5, 6, 7], ids
        return f"SELECT DISTINCT * → {len(rows)} unique whole rows"

    stage("distinct_star", _distinct_star)

    def _distinct_dup_star():
        # Insert a TRUE whole-row duplicate of an existing row (same id+region+
        # category) so DISTINCT * actually collapses it. Non-distinct sees 8;
        # DISTINCT * sees 7.
        cur.execute(
            "INSERT INTO events (id, region, category) VALUES (%s, %s, %s)",
            (1, "us", "click"),  # exact dup of row 1
        )
        total = fetch("SELECT * FROM events")
        distinct_rows = fetch("SELECT DISTINCT * FROM events")
        assert len(total) == 8, f"non-distinct now {len(total)} (expected 8)"
        assert len(distinct_rows) == 7, (
            f"DISTINCT * should collapse the dup to 7, got {len(distinct_rows)}"
        )
        return f"non-distinct={len(total)} distinct*={len(distinct_rows)} (dup collapsed)"

    stage("distinct_dup_star", _distinct_dup_star)

    cur.close()
    conn.close()
    finish()


def finish():
    print("\n=== SP-PG-SQL-DISTINCT SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, _detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("SP-PG-SQL-DISTINCT SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0 if all(ok for _, ok, _ in results) else 1)
