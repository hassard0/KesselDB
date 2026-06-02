#!/usr/bin/env python3
"""SP-PG-EXTQ-CAST-VALIDATE-COMPAT T3 smoke — vulcan psycopg3 PQ-layer.

Exercises the V2 widening on a live kesseldb gateway:

1. HEADLINE — Parse with param_oid=INT4 (23), SQL `... = $1::int8`,
   Bind text "42". V1 strict rejected with 42846; V2 COMPAT accepts.
2. INT8 param + INT4 cast — symmetric widening.
3. TEXT param + VARCHAR cast — 'S' category widening.
4. Cross-category TEXT + INT8 cast — STILL rejects with 42846.
5. Same-OID (V1 strict equality, INT8 + INT8) — still works.

Uses psycopg3's `connection.pgconn` PQ-layer because the high-level
cursor.execute coerces all parameters into text without preserving
the per-position param_oid array — we need direct PQ control over
the param_oids list to exercise the COMPAT widening.
"""
import psycopg
from psycopg.pq import Format

print("\n=== SP-PG-EXTQ-CAST-VALIDATE-COMPAT T3 smoke ===\n")

# OIDs from PG pg_type.dat.
INT4 = 23
INT8 = 20
TEXT = 25
VARCHAR = 1043

def _connect():
    c = psycopg.connect(
        host="127.0.0.1",
        port=5532,
        user="test",
        password="admin",
        dbname="kesseldb",
    )
    c.autocommit = True
    return c


# Setup connection — drop & recreate table.
setup = _connect()
try:
    setup.execute("DROP TABLE compat")
except Exception:
    pass
setup.execute("CREATE TABLE compat (id BIGINT, n CHAR(32))")
setup.close()
print("table created")


def pq_parse_bind_execute(label, sql, param_oids, param_values, expect_ok):
    """Parse + Bind + Execute via the low-level PQ API on a FRESH
    connection so a prior rejection's error_state doesn't leak.
    """
    print(f"\n--- {label} ---")
    print(f"  Parse  sql={sql!r} param_oids={param_oids}")
    conn = _connect()
    pq = conn.pgconn
    res = pq.prepare(b"", sql.encode(), tuple(param_oids))
    if res.status not in (1, 2):  # CMD_OK or TUPLES_OK
        msg = (res.error_message or b"").decode().strip()
        print(f"  Parse rejected: {msg}")
        conn.close()
        if not expect_ok and ("42846" in msg or "cannot cast" in msg.lower()):
            print("  -> OK Parse-time rejection")
            return True
        return not expect_ok
    print(f"  Bind   values={param_values}")
    res2 = pq.exec_prepared(
        b"",
        tuple(param_values),
        tuple(0 for _ in param_values),
        0,  # result format text
    )
    msg = (res2.error_message or b"").decode().strip()
    try:
        if expect_ok:
            if res2.status in (1, 2):
                print(f"  -> OK  status={res2.status}")
                return True
            print(f"  -> UNEXPECTED FAIL  status={res2.status}  msg={msg}")
            return False
        else:
            # Expect rejection with 42846 cannot_coerce.
            if res2.status in (1, 2):
                print(f"  -> UNEXPECTED OK  status={res2.status}  (expected 42846)")
                return False
            if "42846" in msg or "cannot_coerce" in msg or "cannot cast" in msg.lower():
                print(f"  -> OK rejected: {msg}")
                return True
            print(f"  -> WRONG ERROR msg={msg}")
            return False
    finally:
        conn.close()


results = []

# 1. HEADLINE — INT4 param + INT8 cast (pgJDBC default int-against-bigint).
results.append((
    "INT4 param + INT8 cast (HEADLINE)",
    pq_parse_bind_execute(
        "INT4 param + INT8 cast (HEADLINE)",
        "INSERT INTO compat (id, n) VALUES ($1::int8, $2)",
        [INT4, TEXT],
        [b"42", b"compat"],
        expect_ok=True,
    ),
))

# 2. INT8 param + INT4 cast (symmetric widening).
results.append((
    "INT8 param + INT4 cast",
    pq_parse_bind_execute(
        "INT8 param + INT4 cast",
        "INSERT INTO compat (id, n) VALUES ($1::int4, $2)",
        [INT8, TEXT],
        [b"7", b"narrow"],
        expect_ok=True,
    ),
))

# 3. TEXT param + VARCHAR cast.
results.append((
    "TEXT param + VARCHAR cast",
    pq_parse_bind_execute(
        "TEXT param + VARCHAR cast",
        "INSERT INTO compat (id, n) VALUES (1, $1::varchar)",
        [TEXT],
        [b"hello"],
        expect_ok=True,
    ),
))

# 4. Cross-category — TEXT param + INT8 cast — MUST still reject.
results.append((
    "Cross-category TEXT + INT8 (must reject)",
    pq_parse_bind_execute(
        "Cross-category TEXT + INT8 (must reject)",
        "INSERT INTO compat (id, n) VALUES ($1::int8, $2)",
        [TEXT, TEXT],
        [b"99", b"reject"],
        expect_ok=False,
    ),
))

# 5. Strict equality (INT8 + INT8) — base case still works.
results.append((
    "Strict-equality INT8 + INT8",
    pq_parse_bind_execute(
        "Strict-equality INT8 + INT8",
        "INSERT INTO compat (id, n) VALUES ($1::int8, $2)",
        [INT8, TEXT],
        [b"1000", b"base"],
        expect_ok=True,
    ),
))

# Sanity: the table should now have rows from cases 1, 2, 3, 5.
verify = _connect()
cur2 = verify.cursor()
cur2.execute("SELECT * FROM compat")
rows = cur2.fetchall()
print(f"\nFinal table rows (expect 4 inserts from cases 1, 2, 3, 5):")
for row in rows:
    print(f"  {row}")
verify.close()

print("\n=== Results ===")
all_ok = True
for label, ok in results:
    flag = "PASS" if ok else "FAIL"
    print(f"  [{flag}] {label}")
    if not ok:
        all_ok = False

if all_ok:
    print("\nSP-PG-EXTQ-CAST-VALIDATE-COMPAT SMOKE: ALL PASS\n")
else:
    print("\nSP-PG-EXTQ-CAST-VALIDATE-COMPAT SMOKE: FAILURES SEEN\n")
    exit(1)
