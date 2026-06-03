# SP-PG-SQL-ORM-PARSE — qualified columns + projection render + ANY(ARRAY[]) — design

Date: 2026-06-02

## 1. Context

The SQLAlchemy 2.0 declarative-ORM integration smoke
(`SP-PG-ORM-SQLALCHEMY`, transcript
`docs/superpowers/sppgormsqlalchemy-t2-smoke-2026-06-02.txt`) proved that
the PG-wire SUBSTRATE is solid (connect, Extended-Query probe, VARCHAR(n)
DDL, INSERT, `SELECT *`[+WHERE] all green) but a REAL declarative-ORM CRUD
workload only composes **2/8** stages. The boundary is THREE SQL-shape
gaps in the kessel-sql parser + the gateway's projection render path —
not the engine, not the wire codec. This arc closes those gaps.

The ORM emits SQL that ALWAYS qualifies columns with the table name and
ALWAYS uses an explicit projection list — the two shapes V1 never needed
because every prior smoke drove `SELECT *` and bare columns. Concretely:

```
SELECT orm_users.id, orm_users.name FROM orm_users
SELECT orm_users.id, orm_users.name FROM orm_users WHERE orm_users.id = $1
UPDATE orm_users SET name=$1 WHERE orm_users.id = $1
DELETE FROM orm_users WHERE orm_users.id = $1
SELECT ... WHERE relkind = ANY (ARRAY['r','p','f','v','m'])   -- create_all probe
```

## 2. The three keystone gaps (from the ORM smoke)

- **G2a — qualified column references.** `table.col` in projection,
  WHERE, SET, ORDER BY, GROUP BY positions. The lexer already tokenizes
  `.` as `Punct('.')`; the parser's `ident()`/`term`/projection paths
  read a bare `IDENT` and stop. Fix: a `col_ident()` helper that reads
  `IDENT (DOT IDENT)?` and returns the LAST ident as the column name.
  V1 LENIENT: the qualifier is accepted and ignored (any qualifier, not
  just the FROM table/alias). Strict qualifier validation is a named
  follow-up. This is THE highest-leverage fix — it unblocks every ORM
  SELECT/UPDATE/DELETE.

- **G2b — explicit projection list render.** `SELECT c1, c2 FROM t`
  (qualified or not). kessel-sql ALREADY parses the projection list and
  emits `Op::SelectFields` (SP21 projection); the engine ALREADY returns
  the projected columns. The gap is purely the GATEWAY render path:
  `dispatch.rs` only recognized `SELECT * FROM t` via `select_star_table`
  and rejected everything else with `V1 PG-wire only renders SELECT *`.
  Fix: when the SQL is `select_columns(sql) == Some((table, cols))`,
  build a projected RowDescription (the named columns, in order) and
  decode the `Op::SelectFields` row stream (concatenated fixed-width
  field bytes, NO record header / NO null-bitmap — a different shape from
  the full-record `emit_data_rows` path) via a dedicated
  `emit_projected_rows` helper.

- **G1 — `relkind = ANY (ARRAY[...])` create_all catalog probe.** The
  lexer rejects `[`. Fix two layers: (a) lex `[`/`]` as punctuation and
  desugar `col = ANY (ARRAY[a, b, c])` → `col IN (a, b, c)` → OR-of-eq
  (reuse the SP56 IN lowering) in kessel-sql — broadly useful for
  user-table queries; (b) the create_all probe targets `pg_catalog`, so
  the `pg_catalog::catalog_query_hook` relname-existence matcher must
  recognize the probe and synthesize a response. The probe's
  `relkind = ANY(ARRAY[...])` clause is already inside a query the hook
  pattern-matches on `relname = $table` — confirm the hook tolerates the
  extra clause (the lexer change alone unblocks the `[` rejection so the
  query at least parses for any downstream consumer).

## 3. ORM UPDATE/DELETE shape (extra gap surfaced by the smoke)

The ORM emits `UPDATE t SET col=val WHERE pk = id` and
`DELETE FROM t WHERE pk = id` — NOT the V1 `UPDATE t ID <n> SET ...` /
`DELETE FROM t ID <n>` shape. Because KesselDB rows are keyed by the
`id` pseudo-column (the ObjectId), a `WHERE id = <int>` clause on the
primary key maps DIRECTLY to the existing id-based `Stmt::Update` /
`Op::Delete` path. Fix: accept the standard `SET ... WHERE id = <int>`
shape, strip any table qualifier off the WHERE column, require the
WHERE column to be `id` (the pseudo-PK) with an `=` int comparison, and
extract the id. Non-`id` WHERE columns and non-eq comparisons are a
named follow-up (`SP-PG-SQL-UPDATE-WHERE-GENERAL`) — V1 covers the
ORM's by-PK CRUD, which is the dominant traffic shape.

## 4. Scope

T2 — kessel-sql qualified columns: `col_ident()` helper threaded through
projection, WHERE term, UPDATE SET, ORDER BY, GROUP BY. Lenient
qualifier (ignored). Bare columns byte-identical (regression-locked).

T3 — projection render in the gateway: `select_columns`-driven
RowDescription + `emit_projected_rows` for both `dispatch_query` and
`dispatch_query_with_params`.

T4 — `= ANY (ARRAY[...])` desugar to IN; `[`/`]` lexed; create_all
catalog probe recognized.

T2-extra — ORM `UPDATE t SET ... WHERE id = n` / `DELETE FROM t WHERE
id = n` shapes mapped to the id-based path.

## 5. Acceptance

The SQLAlchemy declarative-ORM smoke advances from **2/8** to **≥6/8**
(ideally full 8/8 CRUD: connect, create_all_ddl_char_fb, orm_insert,
orm_select_all, orm_filter, orm_update, orm_delete + the connect probe).
Measured before/after on vulcan. All existing kessel-sql + gateway KATs
pass (regression guard). Determinism preserved: a qualified-col query
compiles to the BYTE-IDENTICAL Op as the bare-col equivalent.

## 6. Weak spots (named, not all fixed in V1)

1. **Lenient qualifier validation** — V1 accepts ANY qualifier
   (`wrong_table.id` compiles as `id`). Strict validation
   (`wrong_table.id` → error) is `SP-PG-SQL-QUALIFIER-STRICT`.
2. **Alias tracking** — `SELECT t.id FROM orm_users AS t` (table alias in
   FROM). V1 ignores the qualifier so it works incidentally, but the
   alias itself (`AS t`) is not parsed/bound — `SP-PG-SQL-FROM-ALIAS`.
3. **Multi-table FROM / JOIN qualified cols** — the JOIN path already
   parses `t.c` in ON; qualified cols in a JOIN projection that must
   disambiguate which table owns a column is `SP-PG-SQL-JOIN-PROJ-QUAL`.
4. **ANY with a subquery vs array literal** — `= ANY (SELECT ...)` is a
   different shape from `= ANY (ARRAY[...])`; only the array-literal form
   is desugared. `SP-PG-SQL-ANY-SUBQUERY`.
5. **Projection of expressions / aliases** — `SELECT id AS k, name`
   projects only bare columns; `col AS alias` and computed expressions
   are `SP-PG-SQL-PROJ-EXPR`.
6. **General WHERE UPDATE/DELETE** — non-PK WHERE predicates
   (`UPDATE t SET ... WHERE name = 'x'`, multi-row) need a server-side
   scan-resolve-RMW. V1 covers `WHERE id = <int>` only.
   `SP-PG-SQL-UPDATE-WHERE-GENERAL`.
7. **`ARRAY[...]` as a value** outside `= ANY (...)` (array columns /
   array literals in projection) is unsupported — `SP-PG-SQL-ARRAY-TYPE`.

## 7. Residual follow-ups (still out after this arc)

`SP-PG-RETURNING` (server-generated PK INSERT...RETURNING),
`SP-PG-SERIAL` (autoincrement), `SP-PG-ORM-RELATIONSHIPS` (joins /
lazy-load), `SP-PG-ORM-ALEMBIC` (migrations). Named in the smoke design;
unchanged by this arc.

## 8. Execution

4-7 commits: T1 design, T2 qualified cols + KATs, T3 projection render +
KATs, T4 ANY-array desugar + KATs, T5 vulcan ORM smoke, T6 closure. All
cargo on vulcan with `CARGO_TARGET_DIR=/tmp/kdb-target-ormparse`. Direct
commits to main; CI green is the gate.
