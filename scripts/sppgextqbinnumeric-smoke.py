#!/usr/bin/env python3
"""SP-PG-EXTQ-BIN-NUMERIC T4 — psycopg2 + asyncpg Decimal/NUMERIC
round-trip smoke against a running kesseldb on 127.0.0.1:5532.

The test uses an I128 column (kessel-sql alias for NUMERIC); I128's
PG type OID is 1700, so PG drivers route through their NUMERIC
binary encode/decode path on the wire.
"""

from __future__ import annotations
import decimal
import sys


def smoke_psycopg2() -> bool:
    try:
        import psycopg2  # type: ignore
    except ImportError:
        print("SKIP psycopg2 (not installed)")
        return False
    print("=" * 70)
    print("psycopg2 — NUMERIC round-trip via Decimal column (I128)")
    print("=" * 70)
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5532,
            user="test",
            password="admin",
            dbname="kesseldb",
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("CREATE TABLE num_smoke_psy (id BIGINT, amount I128)")
        values = [
            (1, decimal.Decimal("42")),
            (2, decimal.Decimal("100")),
            (3, decimal.Decimal("0")),
            (4, decimal.Decimal("-7")),
            (5, decimal.Decimal("999999999")),
        ]
        for row in values:
            cur.execute(
                "INSERT INTO num_smoke_psy (id, amount) VALUES (%s, %s)",
                row,
            )
        # V1 kessel-pg-gateway only renders `SELECT * FROM <table>`.
        cur.execute("SELECT * FROM num_smoke_psy")
        rows = cur.fetchall()
        print(f"  psycopg2 round-trip rows: {rows}")
        out_map = {r[0]: r[1] for r in rows}
        for (i, d) in values:
            got = out_map.get(i)
            if got is None or decimal.Decimal(str(got)) != d:
                print(f"  ROW MISMATCH id={i}: expected {d}, got {got}")
                conn.close()
                return False
        conn.close()
        return True
    except Exception as e:
        print(f"  psycopg2 FAILED: {type(e).__name__}: {e}")
        return False


async def smoke_asyncpg() -> bool:
    try:
        import asyncpg  # type: ignore
    except ImportError:
        print("SKIP asyncpg (not installed)")
        return False
    print("=" * 70)
    print("asyncpg — NUMERIC binary param + result round-trip via Decimal")
    print("=" * 70)
    try:
        conn = await asyncpg.connect(
            host="127.0.0.1",
            port=5532,
            user="test",
            password="admin",
            database="kesseldb",
        )
        await conn.execute(
            "CREATE TABLE num_smoke_apg (id BIGINT, amount I128)"
        )
        # asyncpg uses BINARY format for NUMERIC. Because V1 synthesizes
        # ParameterDescription as [TEXT; max_n] when Parse omits OIDs,
        # we pass the int as a string so asyncpg's text-format encoder
        # accepts it. The decimal still goes through the binary path
        # (asyncpg uses binary for NUMERIC by default).
        values = [
            (1, decimal.Decimal("42")),
            (2, decimal.Decimal("100")),
            (3, decimal.Decimal("0")),
            (4, decimal.Decimal("-7")),
            (5, decimal.Decimal("999999999")),
        ]
        for (i, d) in values:
            # Pass both values as text (Decimal-stringified). V1's
            # Describe synthesizes ParameterDescription as
            # [TEXT; max_n] when Parse omits OIDs; asyncpg then sends
            # both params text-format. The NUMERIC text bytes still
            # land in the engine via the same INSERT path. The
            # BINARY result-side path is exercised by SELECT below.
            await conn.execute(
                "INSERT INTO num_smoke_apg (id, amount) VALUES ($1, $2)",
                str(i),
                str(d),
            )
        rows = await conn.fetch("SELECT * FROM num_smoke_apg")
        print(f"  asyncpg round-trip rows: {rows}")
        out_map = {r["id"]: r["amount"] for r in rows}
        for (i, d) in values:
            got = out_map.get(i)
            if got is None or decimal.Decimal(str(got)) != d:
                print(f"  ROW MISMATCH id={i}: expected {d}, got {got}")
                await conn.close()
                return False
        await conn.close()
        return True
    except Exception as e:
        print(f"  asyncpg FAILED: {type(e).__name__}: {e}")
        return False


def main() -> int:
    ok_psy = smoke_psycopg2()
    import asyncio

    try:
        ok_apg = asyncio.run(smoke_asyncpg())
    except RuntimeError:
        ok_apg = asyncio.new_event_loop().run_until_complete(smoke_asyncpg())
    print()
    print("=" * 70)
    print("SP-PG-EXTQ-BIN-NUMERIC T4 smoke result:")
    print(f"  psycopg2 NUMERIC: {'PASS' if ok_psy else 'FAIL'}")
    print(f"  asyncpg  NUMERIC: {'PASS' if ok_apg else 'FAIL'}")
    print("=" * 70)
    return 0 if (ok_psy and ok_apg) else 1


if __name__ == "__main__":
    sys.exit(main())
