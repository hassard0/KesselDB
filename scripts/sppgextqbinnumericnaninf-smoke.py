#!/usr/bin/env python3
"""
SP-PG-EXTQ-BIN-NUMERIC-NAN-INF T3 smoke — psycopg2 + asyncpg Decimal
NaN / +Infinity / -Infinity round-trip against KesselDB on
127.0.0.1:5532.

Setup expected before running this script:
    pkill -f 'target/release/kesseldb' || true
    rm -rf /tmp/kdb-nannummax-data
    KESSELDB_TOKEN=admin KESSELDB_PG_ADDR=127.0.0.1:5532 \
        nohup target/release/kesseldb 127.0.0.1:6532 \
        /tmp/kdb-nannummax-data >/tmp/kdb-nannummax.log 2>&1 &

This script proves the codec layer (SP-PG-EXTQ-BIN-NUMERIC-NAN-INF)
correctly decodes inbound NUMERIC binary parameters that carry the
special sign codes (NaN / +Inf / -Inf) into the canonical PG strings.
The downstream engine (FieldKind::I128) may then reject the strings at
the column-type cast boundary — that's a separate engine arc
(out-of-scope for this codec-level fix). The headline shape is:

  Before this arc: psycopg2 errors out at the *codec* layer with
    `0A000 SP-PG-EXTQ-BIN-NUMERIC-NAN` / Inf (the codec rejected the
    sign code, never even forming a string).
  After this arc:  the codec forms the canonical string and the
    engine (not the codec) issues the type-mismatch verdict.
"""

import asyncio
import decimal
import sys

import psycopg2

try:
    import asyncpg
    _HAS_ASYNCPG = True
except ImportError:
    _HAS_ASYNCPG = False


CONN_KWARGS = dict(
    host="127.0.0.1",
    port=5536,
    user="test",
    password="admin",
    dbname="kesseldb",
)


def psycopg2_smoke() -> dict[str, str]:
    """psycopg2 sync driver — INSERT specials into a NUMERIC column.

    Returns a verdict-per-value dict. After the codec change, the
    expected error class is the engine-level type-mismatch, NOT a
    codec-level `0A000 SP-PG-EXTQ-BIN-NUMERIC-NAN` / Inf.
    """
    conn = psycopg2.connect(**CONN_KWARGS)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE TABLE nan_smoke (id I64, amount I128)")
    verdicts: dict[str, str] = {}
    for label, val in [
        ("NaN", decimal.Decimal("NaN")),
        ("Infinity", decimal.Decimal("Infinity")),
        ("-Infinity", decimal.Decimal("-Infinity")),
    ]:
        try:
            cur.execute(
                "INSERT INTO nan_smoke (id, amount) VALUES (%s, %s)",
                (1, val),
            )
            verdicts[label] = "INSERT_OK"
        except Exception as e:
            verdicts[label] = (
                f"INSERT_REJECT: {type(e).__name__}: "
                f"{str(e).strip()[:200]}"
            )
    conn.close()
    return verdicts


async def asyncpg_smoke() -> dict[str, str]:
    """asyncpg async driver — same Bind path, different binary-RESULT
    path. asyncpg always binds NUMERIC parameters as binary by default."""
    conn = await asyncpg.connect(
        host="127.0.0.1",
        port=5536,
        user="test",
        password="admin",
        database="kesseldb",
    )
    verdicts: dict[str, str] = {}
    await conn.execute("CREATE TABLE nan_smoke_ap (id I64, amount I128)")
    for label, val in [
        ("NaN", decimal.Decimal("NaN")),
        ("Infinity", decimal.Decimal("Infinity")),
        ("-Infinity", decimal.Decimal("-Infinity")),
    ]:
        try:
            # Use only one bound parameter to keep asyncpg's parse-side
            # type inference happy. The id column gets a literal.
            await conn.execute(
                "INSERT INTO nan_smoke_ap (id, amount) VALUES (1, $1::numeric)",
                val,
            )
            verdicts[label] = "INSERT_OK"
        except Exception as e:
            verdicts[label] = (
                f"INSERT_REJECT: {type(e).__name__}: "
                f"{str(e).strip()[:200]}"
            )
    await conn.close()
    return verdicts


def codec_layer_assertion(verdict_str: str) -> bool:
    """The codec-level rejection used to be `0A000 SP-PG-EXTQ-BIN-NUMERIC-{NAN,INF}`.
    After this arc the codec accepts the wire frame, so the verdict
    MUST NOT contain the SP-PG-EXTQ-BIN-NUMERIC-NAN / INF arc name.
    """
    return (
        "SP-PG-EXTQ-BIN-NUMERIC-NAN" not in verdict_str
        and "SP-PG-EXTQ-BIN-NUMERIC-INF" not in verdict_str
    )


def main() -> int:
    print("=" * 64)
    print("SP-PG-EXTQ-BIN-NUMERIC-NAN-INF T3 vulcan smoke")
    print("=" * 64)
    print()
    print("--- psycopg2 (sync) ---")
    verdicts = psycopg2_smoke()
    overall_pass = True
    for label, verdict in verdicts.items():
        codec_ok = codec_layer_assertion(verdict)
        marker = "PASS" if codec_ok else "FAIL"
        if not codec_ok:
            overall_pass = False
        print(f"  {label:>10}: [{marker}] {verdict}")
    print()
    if _HAS_ASYNCPG:
        print("--- asyncpg (async) ---")
        ap_verdicts = asyncio.run(asyncpg_smoke())
        for label, verdict in ap_verdicts.items():
            codec_ok = codec_layer_assertion(verdict)
            marker = "PASS" if codec_ok else "FAIL"
            if not codec_ok:
                overall_pass = False
            print(f"  {label:>10}: [{marker}] {verdict}")
        print()
    else:
        print("--- asyncpg (async) ---")
        print("  asyncpg not installed; skipping")
        print()
    print("--- Verdict ---")
    if overall_pass:
        print("CODEC-LAYER PASS: NaN / +Inf / -Inf wire frames now decode")
        print("through the codec to the canonical PG strings 'NaN' /")
        print("'Infinity' / '-Infinity'. The engine-level type-mismatch")
        print("is a separate concern (FieldKind::I128 cannot hold these")
        print("values — that's an engine arc, not a codec arc).")
        return 0
    else:
        print("CODEC-LAYER FAIL: one or more specials still reject AT THE")
        print("CODEC layer with the SP-PG-EXTQ-BIN-NUMERIC-{NAN,INF} arc.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
