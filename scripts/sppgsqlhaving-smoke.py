#!/usr/bin/env python3
"""SP-PG-SQL-HAVING — HAVING-over-JOIN group-aggregate smoke (psycopg2).

A HAVING clause filters aggregate GROUPS after grouping. This smoke
exercises the gateway-rendered path: a join-group-aggregate
(`SELECT a.name, COUNT(b.id) FROM a JOIN b ON … GROUP BY a.name`) with a
trailing `HAVING COUNT(b.id) <cmp> <literal>`, asserting that the engine
drops the groups whose aggregate fails the predicate.

Data (3 authors, differing book counts):
    tolkien   -> 3 books   (hobbit, lotr, silmarillion)
    asimov    -> 2 books   (foundation, i_robot)
    lonely    -> 1 book    (solo)

Stages:
  1. ddl              — CREATE author, CREATE book                  [setup]
  2. seed             — insert the 3 authors + 6 books              [setup]
  3. baseline_groups  — GROUP BY name, COUNT(book.id) → 3 groups    (no HAVING)
  4. having_gt2       — HAVING COUNT(book.id) > 2  → {tolkien:3}     (1 group)
  5. having_ge2       — HAVING COUNT(book.id) >= 2 → tolkien,asimov  (2 groups)
  6. having_eq1       — HAVING COUNT(book.id) = 1  → {lonely:1}      (1 group)
  7. having_ne3       — HAVING COUNT(book.id) <> 3 → asimov,lonely   (2 groups)
  8. having_none      — HAVING COUNT(book.id) > 99 → 0 groups        (empty)

HEADLINE: the same GROUP BY query, once WITH and once WITHOUT a HAVING,
returns the FULL group set vs only the surviving groups — i.e. HAVING
filters groups correctly (before/after group counts printed per stage).

Connection: postgresql://test:admin@127.0.0.1:5550/kesseldb
"""
import sys
import traceback

import psycopg2

DSN = "postgresql://test:admin@127.0.0.1:5550/kesseldb"

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


JOIN = (
    "SELECT author.name, COUNT(book.id) "
    "FROM author JOIN book ON author.id = book.aid "
    "GROUP BY author.name"
)


def main():
    print(f"# psycopg2 {psycopg2.__version__} -> {DSN}")
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()

    def _ddl():
        cur.execute("CREATE TABLE author (id BIGINT, name CHAR(32))")
        cur.execute("CREATE TABLE book (id BIGINT, aid BIGINT, title CHAR(32))")
        return "author + book created"

    stage("ddl", _ddl)
    if not results[-1][1]:
        finish()
        return

    def _seed():
        authors = [(1, "tolkien"), (2, "asimov"), (3, "lonely")]
        for aid, name in authors:
            cur.execute(
                "INSERT INTO author (id, name) VALUES (%s, %s)", (aid, name)
            )
        books = [
            (10, 1, "hobbit"), (11, 1, "lotr"), (12, 1, "silmarillion"),
            (20, 2, "foundation"), (21, 2, "i_robot"),
            (30, 3, "solo"),
        ]
        for bid, aid, title in books:
            cur.execute(
                "INSERT INTO book (id, aid, title) VALUES (%s, %s, %s)",
                (bid, aid, title),
            )
        return f"{len(authors)} authors, {len(books)} books"

    stage("seed", _seed)

    def fetch_groups(having=None):
        sql = JOIN + (f" HAVING {having}" if having else "")
        cur.execute(sql)
        rows = cur.fetchall()
        # name comes back CHAR(32)-padded; strip for comparison.
        return sorted((r[0].strip(), int(r[1])) for r in rows)

    # Baseline (no HAVING) — all 3 groups.
    def _baseline():
        g = fetch_groups()
        assert g == [("asimov", 2), ("lonely", 1), ("tolkien", 3)], g
        return f"3 groups (no HAVING): {g}"

    stage("baseline_groups", _baseline)
    baseline_n = len(fetch_groups()) if results[-1][1] else None

    def _gt2():
        g = fetch_groups("COUNT(book.id) > 2")
        assert g == [("tolkien", 3)], g
        return f"before=3 after={len(g)} -> {g}"

    stage("having_gt2", _gt2)

    def _ge2():
        g = fetch_groups("COUNT(book.id) >= 2")
        assert g == [("asimov", 2), ("tolkien", 3)], g
        return f"before=3 after={len(g)} -> {g}"

    stage("having_ge2", _ge2)

    def _eq1():
        g = fetch_groups("COUNT(book.id) = 1")
        assert g == [("lonely", 1)], g
        return f"before=3 after={len(g)} -> {g}"

    stage("having_eq1", _eq1)

    def _ne3():
        g = fetch_groups("COUNT(book.id) <> 3")
        assert g == [("asimov", 2), ("lonely", 1)], g
        return f"before=3 after={len(g)} -> {g}"

    stage("having_ne3", _ne3)

    def _none():
        g = fetch_groups("COUNT(book.id) > 99")
        assert g == [], g
        return f"before=3 after=0 -> {g}"

    stage("having_none", _none)

    cur.close()
    conn.close()
    finish()


def finish():
    print("\n=== SP-PG-SQL-HAVING SMOKE SUMMARY ===")
    npass = sum(1 for _, ok, _ in results if ok)
    for name, ok, _detail in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print(f"--- {npass}/{len(results)} stages PASS ---")
    print("HAVING SMOKE COMPLETE")


if __name__ == "__main__":
    main()
    sys.exit(0)
