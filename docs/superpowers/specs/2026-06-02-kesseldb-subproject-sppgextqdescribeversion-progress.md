# SP-PG-EXTQ-DESCRIBE-VERSION — JDBC extended-mode scalar Describe synthesizer — SP-arc Progress Tracker

Date created: 2026-06-02
**Status: CLOSED — V1 SHIPPED at T3 (2026-06-02) — DONE.**
Extended-mode `SELECT version()` (and the eight other scalar-SELECT
shapes SP-PG-EXTQ T7 added Simple-Query handlers for) now round-trips
end-to-end through real pgJDBC 42.7.4 + OpenJDK 21 on vulcan against
KesselDB pg-gateway. The gateway's `extq::row_description_or_no_data_for_sql`
helper recognizes the closed-set whitelist of scalar SELECTs at
Describe time and emits a matching `RowDescription` instead of the
authoritative `NoData` that previously tripped pgJDBC into raising
`IllegalStateException: Received resultset tuples, but no field
structure for them`. TaskList #366 ready for completion.

Smoke transcript: `docs/superpowers/sppgextqdescribeversion-t3-smoke-2026-06-02.txt`
Parent SP-arc: SP-PG-JDBC-SMOKE V1 (closed 2026-06-02 at T2 —
DONE_WITH_CONCERNS); the V1 out-of-scope clause named this arc as
the follow-up to "extended-mode `SELECT version()` Describe/NoData
ordering bug."

## What this SP-arc ships

V1 = "the closed set of scalar SELECT shapes that SP-PG-EXTQ T7
added Simple-Query handlers for emit a matching RowDescription at
extended-query Describe time, instead of NoData; this closes the
pgJDBC IllegalStateException on extended-mode `SELECT version()`
without touching the engine boundary or any other wire surface."

The fix sits ENTIRELY inside `crates/kessel-pg-gateway/src/extq/`:

- New module `scalar_row_descriptions.rs` (437 lines) — closed-set
  whitelist of normalized SQLs (lowercase, comment-strip, whitespace-
  collapse, trailing `;` strip, trailing ` AS <alias>` strip, `::TYPE`
  cast strip) mapped to (column_name, type_oid) pairs.
- Existing `extq::row_description_or_no_data_for_sql` extended to
  call `scalar_row_descriptions::row_description_for_scalar_select`
  BEFORE the `select_star_table` probe; if the matcher returns
  `Some(bytes)`, the gateway emits those RowDescription bytes; if
  `None`, falls through to the existing path.

Recognized patterns (V1 — all case-insensitive after normalization,
trailing `;` tolerated, trailing ` AS <alias>` tolerated, `::TYPE`
cast suffixes stripped):

| Pattern                            | Column name        | Column type | Notes                            |
|------------------------------------|--------------------|-------------|----------------------------------|
| `SELECT version()`                 | `version`          | TEXT  (25)  | canned KesselDB version string   |
| `SELECT pg_catalog.version()`      | `version`          | TEXT  (25)  | SQLAlchemy 2.0 prefix variant    |
| `SELECT current_user`              | `current_user`     | TEXT  (25)  | also accepts `SELECT user`       |
| `SELECT session_user`              | `session_user`     | TEXT  (25)  |                                  |
| `SELECT current_database()`        | `current_database` | TEXT  (25)  | also accepts `current_catalog`   |
| `SELECT current_schema()`          | `current_schema`   | TEXT  (25)  | with/without parens              |
| `SELECT 1`                         | `?column?`         | INT4  (23)  | PG canonical for anonymous int   |
| `SELECT 'hello'`                   | `?column?`         | TEXT  (25)  | bare string literal              |
| `SELECT NULL`                      | `?column?`         | TEXT  (25)  | PG default for untyped NULL      |
| `SELECT true` / `SELECT false`     | `bool`             | BOOL  (16)  |                                  |
| `SELECT 1::int8` (post cast-strip) | `?column?`         | INT4  (23)  |                                  |

**Out-of-scope (named, deferred — each is its own future arc):**

- **`SP-PG-EXTQ-DESCRIBE-EXPR` (V2)** — arbitrary expression SELECTs
  (`SELECT 1 + 2`, `SELECT length('abc')`). The matcher's recognition
  table is closed; expression evaluation requires a real SQL AST
  walk that V1 doesn't have at Describe time.
- **`SP-PG-EXTQ-DESCRIBE-MULTI-PROJ` (V2)** — multi-projection scalar
  SELECTs without FROM (`SELECT version(), current_user`,
  `SELECT 1, 2, 'x'`). The pgAdmin 4-function probe lands here but
  pgAdmin uses simple-mode for it, so the Extended-Query Describe
  gap doesn't matter in practice yet.
- **`SP-PG-EXTQ-DESCRIBE-SUBQUERY` (V2)** — `SELECT col FROM (subquery)`.
- **`SP-A T14`** — single-column projection from a real table
  (`SELECT col FROM t`). V1's `select_star_table` matcher only
  recognizes `SELECT * FROM t`; lifting to per-column projection
  shapes is a real SQL AST job tracked under SP-A.

## Slice plan

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design doc + `extq::scalar_row_descriptions` module with the closed pattern table + 15 lib KATs covering each pattern + post-cast-strip + fall-through rejection + locked recognition table. | **DONE** | `4bbb5d2` |
| **T2** | Wire the matcher into `extq::mod::row_description_or_no_data_for_sql` (call BEFORE the existing `select_star_table` probe; if the matcher returns Some, emit those bytes; otherwise fall through). 3 dispatcher-integration KATs in `extq::mod` covering `SELECT version()`, `SELECT 1` via portal, and `SELECT 1::int8` via stored stmt. | **DONE** | `4bbb5d2` |
| **T3** | vulcan pgJDBC extended-mode smoke — rebuild kesseldb on vulcan with `CARGO_TARGET_DIR=/tmp/kdb-target-describever`, launch, run `~/jdbc-smoke/JdbcSmoke extended` against it, capture verbatim output to `docs/superpowers/sppgextqdescribeversion-t3-smoke-2026-06-02.txt`. USAGE §9 ORM matrix JDBC row flip from "PASS\*\* + two residual gaps" to "PASS\* + one residual gap (SP-PG-SQL-PAREN-VALUES)". STATUS row + progress tracker + arc closure. | **DONE** | (this commit) |

(T1 + T2 landed in the same commit `4bbb5d2` whose commit message
reads "SP-PG-SQL-PAREN-VALUES T2 KAT fix" — the parser-fix commit
absorbed the scalar Describe synthesizer because both arcs touched
the JDBC simple-mode + extended-mode path in the same session. This
T3 closure session split the commit's actual deliverables into two
named arcs (this one + SP-PG-SQL-PAREN-VALUES).)

## Acceptance criteria

1. **pgJDBC extended-mode `SELECT version()` PASS end-to-end via real
   pgJDBC 42.7.4 on vulcan.** ✅ Met.
   ```
   JDBC smoke ? mode=extended url=jdbc:postgresql://127.0.0.1:5532/kesseldb
   Connected. driver=42.7.4
   CREATE TABLE: OK
   INSERT: 1 row(s)
   Row: id=42, name=hello-jdbc
   Param SELECT: id=42, name=hello-jdbc
   Server version: PostgreSQL 14.0 (KesselDB 1.0)
   ALL TESTS PASS
   ```
   No `IllegalStateException`. No fallback to a stripped-down
   harness. The full CRUD path round-trips INCLUDING the
   version-probe line.

2. **Describe('S', "SELECT version()") emits ParameterDescription(0) +
   RowDescription("version", TEXT) — NOT NoData.** ✅ Met by
   `sppgextqdescribeversion_describe_statement_select_version_emits_row_desc`.

3. **Describe('P', portal_of("SELECT 1")) emits
   RowDescription("?column?", INT4) — NOT NoData.** ✅ Met by
   `sppgextqdescribeversion_describe_portal_select_1_emits_row_desc`.

4. **Describe('S', "SELECT 1::int8") (post cast-strip) emits the same
   shape as `SELECT 1` — locks the SP-PG-EXTQ-CAST x
   SP-PG-EXTQ-DESCRIBE-VERSION interaction.** ✅ Met by
   `sppgextqdescribeversion_describe_statement_select_1_int8_cast_emits_int4_rd`.

5. **The recognition table is byte-stable across refactors.** ✅ Met
   by `t1_pattern_recognition_table_is_stable` (a closed-set assertion
   over 15 canonical inputs).

6. **`SELECT * FROM t` MUST fall through to the existing `select_star_table`
   path (no regression).** ✅ Met by
   `t1_scalar_rd_for_select_star_table_falls_through` (and confirmed
   by the existing 776 KATs in `kessel-pg-gateway` still passing).

7. **`SELECT 1, 2` / `SELECT version(), current_user` (multi-projection
   without FROM) MUST return None — out-of-scope for V1.** ✅ Met by
   `t1_scalar_rd_for_multi_statement_returns_none`.

8. **KAT delta ≥ 8 (target was 8–12).** ✅ Met with **+18**:
   - 15 lib KATs in `extq::scalar_row_descriptions`.
   - 3 integration KATs in `extq::mod` driving `try_dispatch_extq`.

## Risks + non-goals

- **Risk:** a future scalar SELECT handler added to
  `pg_catalog::synthesize::synthesize_helper_function` without a
  matching entry here would re-trigger the original JDBC
  `IllegalStateException` for that new SQL. **Mitigation:** the
  module header explicitly notes the requirement to update both
  tables in lockstep; the recognition-table KAT prints the canonical
  expected mappings so any silent removal would fail loudly.
- **Risk:** the matcher misclassifies a SELECT that doesn't actually
  match the V1 set. **Mitigation:** the matcher is whitelist-only —
  any SQL not in the closed set returns `None` (the existing
  fall-through behavior). The KAT corpus covers every flavour of
  rejection (multi-projection, expressions, single-column projection,
  unrecognized function, empty, comment-only).
- **Non-goal:** V1 does NOT attempt to parse SQL beyond the closed
  set. The Describe step intentionally stays narrow; arbitrary
  expression typing is real SQL planner work tracked under
  SP-PG-EXTQ-DESCRIBE-EXPR.

## What lands on `main` for this slice (commit-by-commit)

| Commit  | What                                                                                                     |
|---------|----------------------------------------------------------------------------------------------------------|
| `4bbb5d2` | T1 + T2 — design spec, `extq::scalar_row_descriptions.rs`, +18 KATs, dispatcher wire-up.                |
| THIS    | T3 — smoke transcript + USAGE.md §9 ORM matrix flip + STATUS row + progress tracker + arc closure.       |

## TaskList

**TaskList #366 ready for completion.**
