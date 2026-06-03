# SP-PG-ORM-RELATIONSHIPS — SQLAlchemy 2.0 multi-table FK relationships (design)

Date: 2026-06-03
Arc type: **VALIDATION** (measure the boundary, name gaps, surgical fix only if it unblocks a major path).

## Context

SQLAlchemy 2.0 + Django 6.0 already do full **single-table** CRUD against
KesselDB's PG-wire gateway (SP-PG-SQL-ORM-PARSE → 7/7, SP-PG-DJANGO-COMPLETE
→ 8/8). The next real-world frontier is **multi-table relationships** —
foreign keys + JOINs, the heart of relational apps.

A two-model `Author ←→ Book` relationship exercises four distinct surfaces:

1. **FK DDL** — `Base.metadata.create_all()` emits a 2nd `CREATE TABLE` with
   a table-level `FOREIGN KEY(author_id) REFERENCES authors (id)` constraint.
2. **Relationship cascade INSERT** — `a.books = [Book(...), Book(...)]; s.add(a)`
   flushes the parent, then the children with the parent's assigned id
   (`INSERT INTO books (...) VALUES (...) RETURNING id`).
3. **JOIN query** — `select(Author.name, Book.title).join(Book, ...)` emits
   `SELECT authors.name, books.title FROM authors JOIN books ON authors.id = books.author_id`.
4. **Lazy-load navigation** — `author.books` triggers
   `SELECT books.* FROM books WHERE books.author_id = $1`.

## Scope

Run the real SQLAlchemy two-model relationship workload (smoke
`scripts/sppgormrelationships-smoke.py`) against KesselDB on vulcan, score
each stage PASS/GAP, document the exact boundary, name follow-ups. Surgical
fixes are in scope **only** where a small change unblocks a major path.

## Recon — the reality before this arc

- **FK DDL**: `kessel-sql`'s CREATE TABLE parser (crate `kessel-sql`,
  `compile_stmt`) handles `PRIMARY KEY (col)` table-constraint + inline
  `PRIMARY KEY` / `GENERATED … IDENTITY` / `DEFAULT` modifiers, but has **no**
  handling for `FOREIGN KEY(col) REFERENCES tbl(col)` table-constraint nor an
  inline `REFERENCES tbl(col)` column modifier. → the 2nd `CREATE TABLE`
  **fails to parse** today. KesselDB does have FKs at the engine layer
  (`Op::AddForeignKey`, SP6) but the **DDL spelling** is unwired.
- **JOIN engine**: `Op::Join` (SP36) exists and works — inner equi-join,
  returns a self-describing `KTR1` typed result
  (`[KTR1][u32 deflen][combined typedef][ [u32 reclen][full record] ]*`) whose
  embedded schema names columns `<table>.<col>`. `kessel-sql` compiles
  `SELECT * FROM a JOIN b ON a.x = b.y` AND `SELECT a.c1, b.c2 FROM a JOIN b
  ON …` to `Op::Join` (the projection list is parsed but **discarded** — the
  qualifier-stripping projection walk runs, then the JOIN branch returns
  `Op::Join` carrying no projection, i.e. all combined columns).
- **JOIN render**: the gateway (`kessel-pg-gateway::dispatch::render_select_got`)
  has THREE SELECT render shapes — scalar-aggregate, explicit projection
  (single-table), whole-row `SELECT *`. **None render a JOIN.** A JOIN SQL
  makes `select_columns()` and `select_star_table()` both return `None`, so
  the result falls into the "0A000 only renders `SELECT *`" arm → **error**.
  This is the keystone gap: the relational *engine* joins, but the *gateway*
  can't speak the result to a PG client.

## Surgical fixes planned (both unblock a major path)

### Fix A — FK DDL parse (accept-and-skip)
In `kessel-sql` CREATE TABLE:
- table-constraint `FOREIGN KEY ( col [,col]* ) REFERENCES tbl [ ( col [,col]* ) ] [ON DELETE/UPDATE …]` → consume + skip.
- inline column modifier `REFERENCES tbl [ ( col ) ] [ON DELETE/UPDATE …]` → consume + skip in the modifier loop.
KesselDB keys every row by the `id` pseudo-PK; the FK column is stored as its
declared type. V1 does **not** enforce referential integrity at the SQL DDL
layer (that's the engine `Op::AddForeignKey` path, named follow-up
`SP-PG-DDL-FK-ENFORCE`). This unblocks `create_all` of the 2nd table.

### Fix B — JOIN result render (the keystone)
In `kessel-pg-gateway`:
- add `kessel_sql::join_projection(sql) -> Option<(Vec<QualifiedCol>, is_star)>`
  that extracts the JOIN's projection list (qualified names preserved) so the
  gateway can map them onto the combined schema. `SELECT *` over a JOIN →
  all combined columns.
- add a JOIN render shape in `render_select_got`: detect the `KTR1` magic on
  the `Got` bytes, decode the embedded combined typedef
  (`kessel_catalog::decode_type_def`), build `PgColumn`s, resolve the
  projection (`authors.name` → combined `authors.name`), emit
  RowDescription + projected DataRows via the existing record decoder.

Both fixes are additive: non-JOIN, non-FK SQL is byte-untouched (existing
render shapes checked first; `KTR1` detection is gated on the magic prefix).

## Determinism

Neither fix touches the engine's apply path or the `Op` encoding — Fix A is
parser accept-and-skip (compiles to the identical `Op::CreateType` as the
table without the FK clause, since the FK clause produces no field), Fix B is
pure render (reads `Got` bytes the engine already produced). Seed-7/sim stays
green.

## Out of scope (named follow-ups, set by what the smoke surfaces)

- `SP-PG-DDL-FK-ENFORCE` — wire the parsed FK to `Op::AddForeignKey` +
  referential-integrity enforcement at the DDL/DML layer.
- `SP-PG-SQL-JOIN-WHERE` — `JOIN … WHERE` / filtered joins.
- `SP-PG-SQL-OUTER-JOIN` — LEFT/RIGHT/FULL OUTER JOIN (`Op::Join` is inner-only).
- `SP-PG-SQL-MULTI-JOIN` — 3+ table joins / join chains.
- `SP-PG-SQL-JOIN-ALIAS` — `FROM a AS x JOIN b AS y`.

## Success criterion

Honest boundary statement: which of {FK DDL, cascade insert, JOIN query,
lazy-load} PASS, and the next keystone gap named as a follow-up.
