## SP-PG-EXTQ-DESCRIBE-VERSION — JDBC extended-mode scalar Describe synthesizer

Date created: 2026-06-02 (working branch closed same-day at T4)
Parent SP-arc: SP-PG-JDBC-SMOKE (closed 2026-06-02 at T3 — DONE_WITH_CONCERNS).
The smoke transcript recorded in
`docs/superpowers/sppgjdbcsmoke-t2-smoke-2026-06-02.txt` §3 + §5 logged
the precise failure mode this arc closes:

```
FE=> Parse(stmt=null,query="SELECT version()",oids={})
FE=> Bind ... Describe ... Execute ... Sync
<=BE ParseComplete | BindComplete | NoData            ← BUG: should be RowDescription
                      | RowDescription(1)              ← then this arrives — too late
                      | DataRow(len=30)
                      | CommandStatus(SELECT 1)
                      | Terminate
→ IllegalStateException: Received resultset tuples, but no field structure for them
```

pgJDBC respects `NoData` as authoritative ("no rows will follow"). When the
subsequent `DataRow` arrives, the driver raises and tears down the connection.
The root cause is gateway-side: the extended-query Describe step
(`extq::mod::row_description_or_no_data_for_sql`) only recognizes
`SELECT * FROM <table>` shapes — every other SELECT falls through to NoData,
including the scalar SELECTs that SP-PG-EXTQ T7 added Simple-Query handlers
for (`SELECT version()`, `SELECT current_user`, `SELECT 1`, etc.). Those
T7 handlers emit valid DataRows, but the Describe step doesn't know what
shape they'll produce.

## 1 — Scope

### 1.1 V1 in-scope

Recognize the scalar SELECT patterns SP-PG-EXTQ T7 added handlers for, and
have Describe emit a matching `RowDescription` with one column of the right
type. The recognized patterns (all case-insensitive after normalization,
trailing `;` tolerated, trailing `AS <alias>` tolerated):

| # | Pattern                            | Column name      | Column type | Notes                            |
|---|------------------------------------|------------------|-------------|----------------------------------|
| 1 | `SELECT version()`                 | `version`        | TEXT  (25)  | canned KesselDB version string   |
| 2 | `SELECT pg_catalog.version()`      | `version`        | TEXT  (25)  | SQLAlchemy 2.0 prefix variant    |
| 3 | `SELECT current_user`              | `current_user`   | TEXT  (25)  | also accepts `SELECT user`       |
| 4 | `SELECT session_user`              | `session_user`   | TEXT  (25)  |                                  |
| 5 | `SELECT current_database()`        | `current_database` | TEXT (25) | also accepts `current_catalog`   |
| 6 | `SELECT current_schema()`          | `current_schema` | TEXT  (25)  | with/without parens              |
| 7 | `SELECT 1`                         | `?column?`       | INT4  (23)  | PG canonical for anonymous int   |
| 8 | `SELECT 'hello'`                   | `?column?`       | TEXT  (25)  | string literal                   |
| 9 | `SELECT NULL`                      | `?column?`       | TEXT  (25)  | PG default for untyped NULL      |
| 10| `SELECT true` / `SELECT false`     | `bool`           | BOOL  (16)  |                                  |
| 11| `SELECT 1::int8` (post cast-strip) | `?column?`       | INT4  (23)  | cast stripper runs at Bind; the  |
|   |                                    |                  |             | Describe SQL sees `SELECT 1`     |

### 1.2 V1 out-of-scope (named follow-up arcs)

- `SELECT 1 + 2` / `SELECT a + b` (arbitrary expressions) — V2
  `SP-PG-EXTQ-DESCRIBE-EXPR`.
- `SELECT * FROM (subquery)` — V2 `SP-PG-EXTQ-DESCRIBE-SUBQUERY`.
- Multi-projection SELECTs without FROM (e.g. `SELECT 1, 2, 'x'`) —
  V2 `SP-PG-EXTQ-DESCRIBE-MULTI-PROJ`.
- `SELECT col FROM t` (single-column projection) — V2 `SP-A T14`.

The pgAdmin 4-function probe `SELECT version(), current_database(), current_user,
current_schema()` lands in the multi-projection out-of-scope, BUT the Simple
Query path already handles it (synthesize_pgadmin_multi_helper). The
extended-query Describe path stays Nodata for that shape in V1 — pgAdmin
uses simple mode for the probe.

## 2 — Where the new logic plugs in

The current Describe step is
`extq::mod::row_description_or_no_data_for_sql(engine, sql)` (around L897).
It does:

1. trim whitespace + trailing `;` + comments
2. `kessel_sql::select_star_table(sql)` — Some(table) iff SQL is `SELECT * FROM t`
3. `engine.describe_table(&table)` → columns → RowDescription
4. either step yields None → NoData

The fix adds a NEW check BEFORE step 4 (and BEFORE step 2 in fact, since the
scalar SELECTs would never match `select_star_table` anyway):

```
if let Some(rd_bytes) = scalar_row_descriptions::row_description_for_scalar(sql) {
    return rd_bytes;
}
// existing select_star_table path...
```

The new function lives in a sibling module
`extq::scalar_row_descriptions` to keep the pattern table separate from the
dispatcher. The module mirrors the pattern table in
`pg_catalog::synthesize::synthesize_helper_function` so the Simple Query
path and the Extended-Query Describe path share the same recognition logic
(though they emit different bytes — Simple Query emits the full T+D+C+Z
stream, Describe emits only the T frame).

## 3 — Pattern recognition shape

The matcher mirrors `pg_catalog::normalize_for_match` (lowercase, comment
strip, whitespace collapse, trailing `;` strip) + a final `strip_select_alias`
to tolerate `SELECT version() AS v`. This is the EXACT same normalization the
Simple Query path uses, so the two paths recognize the exact same set of
SQLs.

The match table is a flat list (no regex; sub-µs cost):

```rust
match normalized {
    "select version()" | "select pg_catalog.version()" =>
        Some(scalar_rd("version", PG_TYPE_TEXT)),
    "select current_database()" | "select current_catalog" =>
        Some(scalar_rd("current_database", PG_TYPE_TEXT)),
    "select current_schema()" | "select current_schema" =>
        Some(scalar_rd("current_schema", PG_TYPE_TEXT)),
    "select current_user" | "select user" =>
        Some(scalar_rd("current_user", PG_TYPE_TEXT)),
    "select session_user" =>
        Some(scalar_rd("session_user", PG_TYPE_TEXT)),
    "select 1" =>
        Some(scalar_rd("?column?", PG_TYPE_INT4)),
    "select true" | "select false" =>
        Some(scalar_rd("bool", PG_TYPE_BOOL)),
    "select null" =>
        Some(scalar_rd("?column?", PG_TYPE_TEXT)),
    _ => None,
}
```

For string literal SELECTs we add a small extractor:
- `select '...'` (no internal single quotes) → ("?column?", TEXT)
- `select n` where n parses as i64 → ("?column?", INT4)

These extractors stay narrow: they only fire if the body has NO whitespace
after the literal (i.e. exactly `SELECT '...'` or `SELECT 42`). Anything more
complex falls through to NoData (V1 out-of-scope).

## 4 — Acceptance criteria

A1. `Describe('S', "SELECT version()")` emits a 1-column `RowDescription`
    with column name `"version"` and type OID 25 (TEXT). The bytes are
    byte-equal to what `single_text_row("version", _)` emits FOR THE T
    FRAME ONLY (no D, no C, no Z).

A2. Same for `current_user`, `current_database()`, `current_schema[()]`,
    `session_user`, `SELECT 1`, `SELECT true`, `SELECT 'hello'`,
    `SELECT NULL`.

A3. `Describe('P', portal)` where the portal's stmt is `SELECT version()`
    behaves the same as A1.

A4. After the cast stripper runs at `Bind` (V1 already runs cast strip via
    dispatch_query at Execute, not Describe), an untouched `SELECT 1::int8`
    SQL stored in the prepared stmt would NOT match. **Adjustment:** the
    scalar matcher ALSO runs `cast_stripper::strip_pg_casts` BEFORE
    pattern matching, so `SELECT 1::int8` is recognized as `SELECT 1` →
    INT4.

A5. `Describe('S', "SELECT * FROM t")` still emits the existing
    table-shape RowDescription (no regression — the new matcher returns
    None for this).

A6. `Describe('S', "SELECT version(); SELECT 1")` returns NoData (the
    matcher is single-statement; multi-statement strings don't match the
    normalized patterns).

A7. pgJDBC extended-mode `SELECT version()` smoke against vulcan:
    `JdbcSmoke extended` HEADLINE → ALL TESTS PASS without removing
    the `version()` probe.

## 5 — Implementation plan

### T1 — Design doc + scalar synthesizer module + KATs

Files:
- `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqdescribeversion-design.md` (this file).
- `crates/kessel-pg-gateway/src/extq/scalar_row_descriptions.rs` (new).
- `crates/kessel-pg-gateway/src/extq/mod.rs::row_description_or_no_data_for_sql` (extended).

KATs (8-12):
- `t1_scalar_rd_for_version_text`
- `t1_scalar_rd_for_current_user_text`
- `t1_scalar_rd_for_current_database_text`
- `t1_scalar_rd_for_current_schema_text`
- `t1_scalar_rd_for_select_1_int4`
- `t1_scalar_rd_for_string_literal_text`
- `t1_scalar_rd_for_null_text`
- `t1_scalar_rd_for_int8_cast_strip_int4`
- `t1_scalar_rd_for_select_star_table_falls_through`
- `t1_scalar_rd_for_multi_statement_returns_none`
- `t1_describe_emits_rowdescription_for_version_via_dispatcher`

### T2 — vulcan pgJDBC extended-mode smoke

```bash
ssh admin@192.168.4.178 "pkill -f 'target/release/kesseldb' || true"
ssh admin@192.168.4.178 "cd ~/KesselDB && git pull origin main && \
  CARGO_TARGET_DIR=/tmp/kdb-target-describever cargo build --release \
  --bin kesseldb --features pg-gateway 2>&1 | tail -3"
ssh admin@192.168.4.178 "cd ~/KesselDB && rm -rf /tmp/kdb-describever-data && \
  KESSELDB_TOKEN=admin KESSELDB_PG_ADDR=127.0.0.1:5532 \
  nohup ./target/release/kesseldb 127.0.0.1:6532 /tmp/kdb-describever-data \
  >/tmp/kdb-describever.log 2>&1 & echo started"
sleep 2
ssh admin@192.168.4.178 "cd ~/jdbc-smoke && \
  PATH=~/jdbc-smoke/jdk-21.0.2/bin:\$PATH java -cp .:postgresql.jar JdbcSmoke \
  2>&1 | tee /tmp/jdbc-describever-smoke.log"
```

Pass criteria: `SELECT version()` returns the canned string in extended mode
without `IllegalStateException`.

### T3 — USAGE.md §9 caveat drop + STATUS.md row + arc closure

## 6 — Risk + non-goals

- Risk: a new SELECT shape (e.g. `SELECT my_func()`) gets misclassified.
  Mitigation: the matcher is whitelist (closed set of normalized SQLs);
  unknown shapes fall through to NoData (the existing behavior).
- Risk: a scalar SELECT we don't recognize at Describe but DO handle at
  Execute (via Simple-Query catalog_query_hook) creates the same
  IllegalStateException. Mitigation: the V1 pattern set is the EXACT
  same set as the Simple-Query handler set; future additions must update
  both tables in lockstep. Documented in the module header.
- Non-goal: V1 does NOT detect a stored prepared statement's SQL after
  Bind has substituted parameters. The cast stripper runs at the SQL
  text the client Parsed; that's good enough for the JDBC scalar case.
