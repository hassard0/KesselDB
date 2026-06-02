#!/usr/bin/env python3
"""SP-PG-EXTQ-BIN T3 — real asyncpg + psycopg3 smoke on vulcan.

Validates the binary-format-parameter unlock:
- asyncpg always sends binary params; this script confirms the V1
  binary-format gap is now CLOSED.
- psycopg3 with the DEFAULT cursor (NOT ClientCursor — the T8 PARTIAL
  workaround) confirms the gap is closed for psycopg3 too.

Each driver:
1. connects + completes SCRAM-SHA-256 handshake
2. creates a table with various types
3. inserts via parameterized INSERT (binary params)
4. SELECTs via parameterized WHERE (binary params)
5. reports the row(s) round-tripped

Exit code 0 = PASS; non-zero = FAIL.
"""

import asyncio
import sys


async def asyncpg_smoke():
    import asyncpg

    print(f"asyncpg {asyncpg.__version__}")
    try:
        conn = await asyncpg.connect(
            host="127.0.0.1",
            port=5532,
            user="test",
            password="admin",
            database="kesseldb",
        )
        print("  connect: OK")
    except Exception as e:
        print(f"  connect: FAIL ({e})")
        return False

    try:
        await conn.execute(
            "CREATE TABLE asyncpg_bin_smoke (id BIGINT, name CHAR(32))"
        )
        print("  CREATE TABLE: OK")
    except Exception as e:
        print(f"  CREATE TABLE: FAIL ({e})")
        await conn.close()
        return False

    try:
        await conn.execute(
            "INSERT INTO asyncpg_bin_smoke (id, name) VALUES ($1, $2)",
            42, "hello",
        )
        print("  INSERT (binary params): OK")
    except Exception as e:
        print(f"  INSERT (binary params): FAIL ({e})")
        await conn.close()
        return False

    try:
        rows = await conn.fetch(
            "SELECT * FROM asyncpg_bin_smoke WHERE id = $1", 42,
        )
        print(f"  SELECT (binary param): OK — {len(rows)} rows = {list(rows)}")
    except Exception as e:
        print(f"  SELECT (binary param): FAIL ({e})")
        await conn.close()
        return False

    await conn.close()
    print("  === asyncpg PASS ===")
    return True


def psycopg3_default_smoke():
    import psycopg

    print(f"psycopg3 {psycopg.__version__}")
    try:
        conn = psycopg.connect(
            "host=127.0.0.1 port=5532 user=test password=admin dbname=kesseldb",
            autocommit=True,
            # NOTE: no cursor_factory=ClientCursor — default cursor!
        )
        print("  connect: OK")
    except Exception as e:
        print(f"  connect: FAIL ({e})")
        return False

    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE psycopg3_bin_smoke (id BIGINT, name CHAR(32))")
        print("  CREATE TABLE: OK")
    except Exception as e:
        print(f"  CREATE TABLE: FAIL ({e})")
        conn.close()
        return False

    try:
        cur.execute(
            "INSERT INTO psycopg3_bin_smoke (id, name) VALUES (%s, %s)",
            (43, "psycopg3"),
        )
        print("  INSERT (default cursor): OK")
    except Exception as e:
        print(f"  INSERT (default cursor): FAIL ({e})")
        conn.close()
        return False

    try:
        cur.execute(
            "SELECT * FROM psycopg3_bin_smoke WHERE id = %s", (43,),
        )
        rows = cur.fetchall()
        print(f"  SELECT (default cursor): OK — {len(rows)} rows = {rows}")
    except Exception as e:
        print(f"  SELECT (default cursor): FAIL ({e})")
        conn.close()
        return False

    conn.close()
    print("  === psycopg3 default cursor PASS ===")
    return True


async def main():
    a = await asyncpg_smoke()
    print()
    b = psycopg3_default_smoke()
    if a and b:
        print()
        print("=== ALL TESTS PASS — V1 binary-format params unlock CONFIRMED ===")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
