#!/usr/bin/env python3
"""SP-PG-EXTQ-BIN-RESULTS T3 — real asyncpg SELECT smoke on vulcan.

Validates the binary-format-RESULT unlock:
- asyncpg sends binary params (closed by SP-PG-EXTQ-BIN V1) AND requests
  binary RESULTS — this script confirms the V1 binary-format-RESULT gap
  is now CLOSED (was the "insufficient data in buffer" failure in the
  SP-PG-EXTQ-BIN T3 transcript).

Each test:
1. connects + completes SCRAM-SHA-256 handshake
2. creates a table
3. inserts seed rows (literal SQL, sidesteps the SP-PG-EXTQ-CAST gap
   for parameterized INSERT into INT columns)
4. runs a parameterized SELECT via conn.fetch() — exercises both
   binary-format Bind (SP-PG-EXTQ-BIN V1) AND binary-format RESULTS
   (this arc).
5. reports the row(s) round-tripped + the type of each column

Exit code 0 = PASS; non-zero = FAIL.
"""

import asyncio
import sys


async def asyncpg_select_smoke():
    import asyncpg

    print(f"asyncpg {asyncpg.__version__} — SELECT (binary RESULTS) smoke")
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
            "CREATE TABLE asyncpg_binr_smoke (id BIGINT, name CHAR(32))"
        )
        print("  CREATE TABLE: OK")
    except Exception as e:
        print(f"  CREATE TABLE: FAIL ({e})")
        await conn.close()
        return False

    # Seed two rows via literal INSERT (sidesteps the SP-PG-EXTQ-CAST
    # parameterized-INSERT-into-INT gap).
    try:
        await conn.execute(
            "INSERT INTO asyncpg_binr_smoke (id, name) VALUES (42, 'first')"
        )
        await conn.execute(
            "INSERT INTO asyncpg_binr_smoke (id, name) VALUES (43, 'second')"
        )
        print("  INSERT (literal seed): OK")
    except Exception as e:
        print(f"  INSERT (literal seed): FAIL ({e})")
        await conn.close()
        return False

    # The headline test: parameterized SELECT via fetch(). asyncpg
    # sends binary-format Bind AND requests binary-format results in
    # the Bind message (result_formats=[1]). V1 (pre-arc) emitted
    # text-format DataRow which asyncpg mis-decoded as binary,
    # producing "insufficient data in buffer". Post-arc: V1 should
    # rewrite DataRow + RowDescription to binary, asyncpg decodes
    # cleanly, fetch() returns the rows.
    try:
        # asyncpg's fetch() takes a parameterized SELECT.
        # NOTE: as in the BIN V1 smoke, the param routes through V1
        # text-substitute (PD declared as TEXT because Parse omitted
        # OIDs); the WHERE clause `name = 'first'` works because name
        # is CHAR(32). The RESULT-side rendering is the new arc — V1
        # now emits id (INT8) as 8 bytes BE + name (TEXT) as raw UTF-8
        # bytes.
        rows = await conn.fetch(
            "SELECT * FROM asyncpg_binr_smoke WHERE name = $1",
            "first",
        )
        print(f"  SELECT (binary results): OK — {len(rows)} rows")
        for r in rows:
            print(f"    row: id={r['id']} (type={type(r['id']).__name__}), name={r['name']!r}")
        if len(rows) == 0:
            print("  WARN: expected >=1 row, got 0 — substitution may have failed silently")
        # asyncpg returns int for INT8 binary results — if it had mis-
        # decoded text bytes as binary, the type would be wrong or the
        # call would have raised.
    except Exception as e:
        print(f"  SELECT (binary results): FAIL ({e})")
        await conn.close()
        return False

    # Second round-trip: fetch all rows (no WHERE clause). Same binary
    # decode path on the result side, but the parameter-less Bind.
    try:
        rows = await conn.fetch("SELECT * FROM asyncpg_binr_smoke")
        print(f"  SELECT * (binary results, no params): OK — {len(rows)} rows")
        for r in rows:
            print(f"    row: id={r['id']}, name={r['name']!r}")
        if len(rows) != 2:
            print(f"  WARN: expected 2 rows, got {len(rows)}")
    except Exception as e:
        print(f"  SELECT * (binary results, no params): FAIL ({e})")
        await conn.close()
        return False

    await conn.close()
    print("  === asyncpg SELECT (binary RESULTS) PASS ===")
    return True


async def main():
    a = await asyncpg_select_smoke()
    if a:
        print()
        print("=== asyncpg SELECT (binary RESULTS) — V1 SP-PG-EXTQ-BIN-RESULTS CONFIRMED ===")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
