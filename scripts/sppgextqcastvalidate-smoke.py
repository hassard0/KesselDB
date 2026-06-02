#!/usr/bin/env python3
"""
SP-PG-EXTQ-CAST-VALIDATE T3 smoke — psycopg3 PQ-layer Parse/Bind
round-trip against KesselDB on vulcan, proving:

1. PASS — Parse(SQL `... WHERE id = $1::int8`, param_types=[INT8/20])
   + exec_prepared('42') succeeds (matching cast).
2. HEADLINE — Parse(SQL `... WHERE id = $1::int8`, param_types=[TEXT/25])
   + exec_prepared('42') returns `42846 cannot_coerce` ErrorResponse
   (mismatched cast — CLOSES the V1 SP-PG-EXTQ-CAST silent-coercion
   vector).
3. PASS — Parse with no param_types hint (param_types=[0]) +
   exec_prepared('42') succeeds (omitted-OID skip rule).

Setup expected before running this script:
    rm -rf /tmp/kdb-castvalid-data
    KESSELDB_TOKEN=admin KESSELDB_PG_ADDR=127.0.0.1:5532 \\
        nohup /tmp/kdb-target-castvalid/release/kesseldb 127.0.0.1:6532 \\
        /tmp/kdb-castvalid-data >/tmp/kdb-castvalid.log 2>&1 &
    PGPASSWORD=admin psql -h 127.0.0.1 -p 5532 -U test -d kesseldb -c \\
        'CREATE TABLE cast_v_smoke (id BIGINT, n CHAR(32))'
    PGPASSWORD=admin psql -h 127.0.0.1 -p 5532 -U test -d kesseldb -c \\
        "INSERT INTO cast_v_smoke (id, n) VALUES (1, 'hello')"
"""
import sys

import psycopg.pq

PG_TYPE_INT8 = 20
PG_TYPE_TEXT = 25

CONN_STR = "host=127.0.0.1 port=5532 user=test password=admin dbname=kesseldb"


def _new_conn() -> psycopg.pq.PGconn:
    c = psycopg.pq.PGconn.connect(CONN_STR.encode())
    if c.status != psycopg.pq.ConnStatus.OK:
        raise RuntimeError(
            f"connect failed: {c.error_message.decode(errors='replace')}"
        )
    return c


def _prepare_and_exec(
    sql: str,
    param_types: list[int],
    param_values: list[bytes],
) -> tuple[str, str | None, str | None]:
    """Run Prepare(sql, param_types) + ExecPrepared(values). Returns
    `(status_name, sqlstate, error_message)`. Status_name is the
    `ExecStatus` enum name; sqlstate/error are None on success."""
    conn = _new_conn()
    try:
        # PG_TYPES => convert to bytes-array form expected by send_prepare.
        # Empty list = "let server infer".
        types_arg = param_types if param_types else None
        # `prepare` is sync — returns a PGresult.
        result = conn.prepare(b"s_validate", sql.encode(), types_arg)
        if result.status != psycopg.pq.ExecStatus.COMMAND_OK:
            return (
                psycopg.pq.ExecStatus(result.status).name,
                (result.error_field(psycopg.pq.DiagnosticField.SQLSTATE) or b"").decode(),
                (result.error_message or b"").decode(),
            )
        # exec_prepared takes the values + optional formats. All-text.
        result = conn.exec_prepared(b"s_validate", param_values)
        return (
            psycopg.pq.ExecStatus(result.status).name,
            (result.error_field(psycopg.pq.DiagnosticField.SQLSTATE) or b"").decode()
            or None,
            (result.error_message or b"").decode() or None,
        )
    finally:
        conn.finish()


def case_matching_oid_succeeds() -> str:
    """PASS — declared cast INT8 + Parse INT8 → no error."""
    status, sqlstate, msg = _prepare_and_exec(
        "SELECT * FROM cast_v_smoke WHERE id = $1::int8",
        [PG_TYPE_INT8],
        [b"1"],
    )
    if status in ("TUPLES_OK", "COMMAND_OK"):
        return "PASS"
    return f"FAIL: status={status} sqlstate={sqlstate} msg={msg!r}"


def case_mismatched_oid_returns_42846() -> str:
    """HEADLINE — declared cast INT8 + Parse TEXT → 42846 cannot_coerce."""
    status, sqlstate, msg = _prepare_and_exec(
        "SELECT * FROM cast_v_smoke WHERE id = $1::int8",
        [PG_TYPE_TEXT],
        [b"42"],
    )
    if sqlstate == "42846":
        return f"PASS: {msg!r}"
    return (
        f"FAIL: expected 42846, got status={status} sqlstate={sqlstate} msg={msg!r}"
    )


def case_omitted_oid_skips_validation() -> str:
    """asyncpg / psycopg3 default shape — no OID hint → validator skips."""
    status, sqlstate, msg = _prepare_and_exec(
        "SELECT * FROM cast_v_smoke WHERE id = $1::int8",
        [],
        [b"1"],
    )
    if status in ("TUPLES_OK", "COMMAND_OK"):
        return "PASS"
    return f"FAIL: status={status} sqlstate={sqlstate} msg={msg!r}"


def main() -> int:
    results = {
        "matching OID succeeds": case_matching_oid_succeeds(),
        "HEADLINE — mismatched OID returns 42846": case_mismatched_oid_returns_42846(),
        "omitted OID skips validation": case_omitted_oid_skips_validation(),
    }
    for name, verdict in results.items():
        print(f"  {name}: {verdict}")
    if any(not v.startswith("PASS") for v in results.values()):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
