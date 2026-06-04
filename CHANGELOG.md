# Changelog

All notable changes to KesselDB will be documented in this file.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), versioning [SemVer](https://semver.org).

## [Unreleased]

### Fixed

- **Omitted / explicit-NULL nullable columns now render as SQL NULL over the
  PG wire (SP-PG-NULL-INT-RENDER, 2026-06-03)** — a nullable column that was
  omitted at INSERT (`INSERT INTO t (id, note) VALUES (1, 'x')` with a nullable
  `n` left out), or set to an explicit `NULL`, now reads back as a real SQL
  NULL (psycopg2 `None`) over the PG wire instead of `0` (int) / empty (text).
  The bug was in the **non-sorted projection render path** (`SELECT col FROM t`):
  the engine's narrow `Op::SelectFields` stream carries no null mask, so a
  NULL field's stored zero bytes were rendered as a value. `SELECT *` was
  already correct (it honors the on-disk null bitmap). The fix re-issues a
  non-sorted projection as `SELECT *` (full records, which carry the bitmap)
  and re-projects in the gateway — a **pure render-layer change, no storage /
  wire / `Op` format change**, so the determinism oracles are byte-untouched.
  Generic across column kinds (int + text/char + numeric). Also adds explicit
  `NULL` literal support to `INSERT … VALUES (…, NULL)` (a NOT NULL column or
  the `id` primary key rejects NULL cleanly). A NOT-NULL / PK column still
  reads its real value.

### Added

- **RIGHT + FULL outer joins complete the join-type matrix
  (SP-PG-SQL-RIGHT-FULL-JOIN, 2026-06-03)** — `RIGHT [OUTER] JOIN` and
  `FULL [OUTER] JOIN` join the existing `[INNER] JOIN` and `LEFT [OUTER] JOIN`
  on a binary (two-table) equi-join, so the full INNER / LEFT / RIGHT / FULL
  matrix is available over the PG wire. RIGHT returns matched pairs + every
  unmatched RIGHT row with the LEFT (`a.*`) columns NULL; FULL returns matched
  pairs + unmatched-left (`b.*` NULL) + unmatched-right (`a.*` NULL) with no
  duplicate of the matched pairs. The combined column ORDER stays `a.* ++ b.*`
  for every flavour (the JOIN drive direction is swapped, not the output
  order), and NULL-filled columns read back as SQL NULL (psycopg2 `None`).
  `JoinType` gained `Right` (wire tag 2) and `Full` (tag 3) — **purely
  additive**: the tag byte is emitted only when non-Inner, so every INNER join
  stays byte-identical to a pre-arc frame and LEFT (tag 1) is unchanged; no new
  struct field, determinism oracles byte-untouched. Row order is a deterministic
  function of the inputs (matched/unmatched-left in left-key scan order, then
  unmatched-right in right-table scan order). RIGHT/FULL compose with WHERE /
  ORDER BY / LIMIT / OFFSET / GROUP BY / table aliases exactly like LEFT, and
  the pg-gateway JOIN renderer needed **no change** (same KTR1 combined-schema
  stream shape). RIGHT/FULL mixed into a 3+ table CHAIN is a named follow-up
  (rejected with a clear error); INNER chains keep working. Live vulcan psql
  smoke (`scripts/sppgsqlrightfulljoin-smoke.py`): **9/9** stages PASS.
- **DDL FOREIGN KEY is now ENFORCED (SP-PG-DDL-FK-ENFORCE, 2026-06-03)** —
  a `FOREIGN KEY (col) REFERENCES tbl [(col)] [ON DELETE …]` declared in
  `CREATE TABLE` (table-level or the inline `col … REFERENCES tbl(col)` form)
  now ENFORCES referential integrity. Previously the FK was parsed and thrown
  away. An INSERT/UPDATE of a child whose non-NULL FK value has no matching
  parent row is rejected with PostgreSQL SQLSTATE **23503**
  (`foreign_key_violation`); a NULL FK is allowed. `ON DELETE` actions
  `NO ACTION` / `RESTRICT` / `CASCADE` / `SET NULL` / `SET DEFAULT` are all
  honored (RESTRICT blocks deleting a referenced parent with 23503; CASCADE
  removes the children). This is a WIRING arc — the engine FK machinery
  (Sub-projects 6 + 11) pre-existed; the DDL parser now captures the FK
  descriptor BY NAME, threads it through the `CreateType` op in a
  marker-guarded ADDITIVE trailer (a no-FK `CREATE TABLE` is byte-identical
  to before — determinism preserved), and the engine resolves the names to
  ids + registers the FK at apply time through the same path
  `Op::AddForeignKey` uses. A forward reference (parent table not yet
  created) or unknown column is a clean DDL error with NO half-created table.
  Deferred: composite FKs (`SP-PG-DDL-COMPOSITE-FK`), `ON UPDATE` actions
  (`SP-PG-DDL-FK-ON-UPDATE`).
- **Table aliases in JOIN queries (SP-PG-SQL-JOIN-ALIAS, 2026-06-03)** —
  `SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id`
  (and the `FROM users AS u` form) now resolve. Previously the parser accepted
  the alias but column qualifiers only resolved against the FULL table name, so
  the universal SQLAlchemy/Django/Rails aliased-join form failed. An alias→table
  map built from the FROM/JOIN clause resolves EVERY qualifier — projection, ON,
  WHERE, ORDER BY, GROUP BY — to the full table name, for binary AND multi-table
  (3+) INNER joins. A bare full-table-name qualifier (`users.name`) keeps
  working (back-compat) and `SELECT *` is unchanged. A duplicate/ambiguous
  alias, an alias that shadows another table's name, or an unknown qualifier is
  a clean error rather than a silent mis-resolution; a self-join under two
  aliases of the SAME table is the named follow-up `SP-PG-SQL-SELF-JOIN`
  (rejected, since the combined `KTR1` schema would have duplicate
  `<table>.<col>` names). **Determinism:** resolution is entirely in
  `kessel-sql` — the alias is rewritten to the full table name during parse, so
  an aliased join compiles to the **byte-identical** wire `Op` as its full-name
  twin. No `Op`/proto change, no construction-site churn, no oracle literal
  changes; `crates/kessel-pg-gateway` is unchanged. Live vulcan psql smoke
  (`scripts/sppgsqljoinalias-smoke.py`): 8/8 stages PASS.
- **Chained N-way (3+ table) INNER equi-joins (SP-PG-SQL-MULTI-JOIN,
  2026-06-03)** — `SELECT users.name, posts.title, comments.body FROM users
  JOIN posts ON users.id = posts.user_id JOIN comments ON posts.id =
  comments.post_id` now works end-to-end over the PG wire (columns qualified by
  the full table name; aliases are the `SP-PG-SQL-JOIN-ALIAS` follow-up). The
  planner previously handled exactly ONE `JOIN`; a second
  `JOIN` segment failed to compile. `Op::Join` gained an additive,
  marker-guarded `extra_joins: Vec<JoinStep>` (each step = the next table + its
  ON `left_combined_field = right_field`). The engine's `apply_multi_join`
  folds each step (INNER hash equi-join the running combined row set against the
  next table), widening the self-describing `KTR1` combined schema each step;
  `WHERE` / `ORDER BY` / `LIMIT` / `OFFSET` apply over the full N-table combined
  schema, and `SELECT *` returns every column of every joined table. The
  gateway's existing `render_join_result` + `join_projection` handle 3+ tables
  with no data-path change (the combined schema just grows). **Determinism:**
  `extra_joins` is emitted on the wire ONLY when non-empty (distinct marker `2`
  vs. the group-aggregate marker `1`, so a 2-table or group-aggregate frame is
  BYTE-IDENTICAL to before); the multi-join is a pure deterministic function of
  the input tables (left-key/right-scan object-id order preserved at every
  step). V1 is INNER chains only — mixing LEFT/RIGHT/FULL into a chain, or
  GROUP BY over a chain, are named follow-ups (explicit errors). psql 3-table
  smoke `scripts/sppgsqlmultijoin-smoke.py`.
- **`ORDER BY` / `LIMIT` / `OFFSET` on a plain `GROUP BY` now take effect
  (SP-PG-SQL-GROUP-SORT-LIMIT, 2026-06-03)** — closes the V1 caveat the
  PLAIN-GROUP-RENDER arc surfaced. `SELECT g, COUNT(*) AS n FROM t GROUP BY g
  ORDER BY n DESC LIMIT 5 OFFSET 1` is now sorted + windowed by the engine
  instead of returning all groups in key order. `Op::GroupAggregate` /
  `Op::GroupAggregateMulti` gained an additive, marker-guarded
  `sort: Option<GroupSort>` (mirroring the HAVING marker-guard and the JOIN
  `order_by`/`limit_n`/`offset_n`). The `ORDER BY` target may be a projected
  aggregate (by alias `ORDER BY n`, position `ORDER BY 2`, or expression
  `ORDER BY COUNT(*)`) or the group key (`ORDER BY g` / `ORDER BY 1`); `DESC`
  reverses; ties break by ascending group key; `LIMIT`/`OFFSET` apply AFTER the
  sort; `HAVING` filters BEFORE it (filter → sort → offset → limit). A shared
  `emit_group_results` helper backs both the apply and read-only paths plus the
  single- and multi-aggregate ops. **Determinism:** the new field is emitted on
  the wire ONLY when present, so a no-`ORDER BY`/`LIMIT`/`OFFSET` query produces
  BYTE-IDENTICAL `Op` frames to before; corpus / partition / 3-replica
  byte-identity oracles stay green; every `Op::GroupAggregate{,Multi}`
  construction site (proto/sm/sql/read_pool/sharded_engine/parallel_reads_oracle/
  bench) was updated with `sort: None`. ORDER BY over a JOIN group-aggregate
  remains the separate follow-up `SP-PG-SQL-JOIN-AGG-ORDERBY-AGG`. vulcan psql
  smoke (`scripts/sppgsqlgroupsortlimit-smoke.py`): `ORDER BY COUNT(*) DESC`
  returns **books(4), gadgets(3), toys(2), misc(1)** in descending-count order
  (pre-fix returned all 4 in key order); `LIMIT 2` returns only the top 2;
  `LIMIT 2 OFFSET 1` returns the right window; `ORDER BY category ASC` and
  `HAVING + ORDER BY SUM(price) DESC + LIMIT` also PASS.

- **Plain (non-JOIN) `GROUP BY` renders over the PG wire
  (SP-PG-SQL-PLAIN-GROUP-RENDER, 2026-06-03)** — `SELECT category, COUNT(*)
  [AS n] [, SUM(price), AVG(price), MIN(price), MAX(price)] FROM products
  GROUP BY category [HAVING …]`, the everyday analytics / ORM aggregation, now
  renders correctly over psql. The planner + state machine already
  compiled/executed plain GROUP BY (`Op::GroupAggregate` /
  `Op::GroupAggregateMulti`) and `HAVING` already filtered at the SM layer, but
  the gateway's `render_select_got` only routed group-aggregates through
  `render_join_group_aggregate` — which REQUIRES a JOIN — so a plain
  group-aggregate fell through to the bottom render error
  (`0A000 only renders SELECT *`) even though the engine grouped correctly. New
  `kessel_sql::plain_group_aggregate` recognizer (returns `Some` ONLY for a
  plain group-aggregate; `None` for JOIN-agg, single scalar agg, plain
  projection, and no-GROUP-BY — every existing render path is byte-untouched) +
  `render_plain_group_aggregate` (decodes the value-only group stream, types the
  group key from the FROM-table schema, types aggregate OIDs COUNT/SUM → int8,
  AVG → numeric, MIN/MAX → source-column type). **Render-only — NO `Op` or
  wire-format change**, so the corpus / partition / 3-replica byte-identity
  oracles stay green. **V1 caveat (now resolved by SP-PG-SQL-GROUP-SORT-LIMIT,
  see above):** a trailing `ORDER BY … LIMIT … OFFSET …` on a plain GROUP BY was
  parsed but not yet engine-applied — it is now sorted + windowed by the engine.
  vulcan psql
  smoke: the headline `SELECT category, COUNT(*) FROM products GROUP BY category`
  ERRORED on pre-fix `origin/main` and renders **{books:3, gadgets:1, toys:2}**
  post-fix; multi-agg + `HAVING` also PASS.
- **`HAVING` filters aggregate groups (SP-PG-SQL-HAVING, 2026-06-03)** — a
  `HAVING <AGG>(...) <cmp> <literal>` clause now filters GROUPS after
  aggregation, on the plain (`SELECT col, COUNT(*) FROM t GROUP BY col HAVING
  COUNT(*) >= 3`) and the over-JOIN (`SELECT a.name, COUNT(b.id) FROM a JOIN b
  ON … GROUP BY a.name HAVING COUNT(b.id) > 2`) forms. Spans all three
  group-aggregate ops (`Op::GroupAggregate`, `Op::GroupAggregateMulti`, and
  `Op::Join`'s `JoinGroupAgg`) via ONE additive, marker-guarded
  `Option<HavingPred>` field — a query with NO `HAVING` produces **byte-identical
  `Op` frames** to before, so the determinism oracles stay green. The SQL layer
  parses `HAVING` after `GROUP BY`, matches its aggregate to a SELECTed aggregate
  by `(function, arg)`, supports `> >= < <= = <> !=` (the lexer gained the
  SQL-standard `<>`) and a negative literal RHS, and cleanly rejects a `HAVING`
  aggregate not in the projection (V1). The engine applies the filter on the
  single deterministic apply thread over the already-deterministic per-group
  result, before order/limit paging. Gateway unchanged (fewer groups → fewer
  rows). vulcan psql smoke: baseline 3 groups → `HAVING COUNT(book.id) > 2` →
  **1 group**; `>= 2` → 2; `= 1` → 1; `<> 3` → 2; `> 99` → 0.
- **CAPSTONE: realistic multi-model SQLAlchemy blog app — 8/8
  (SP-PG-ORM-REALAPP, 2026-06-03)** — a realistic THREE-model SQLAlchemy 2.0
  application (`User` 1—N `Post` 1—N `Comment`, FKs + declarative
  `relationship()`, insertmanyvalues batching ON) exercising the full query
  range a real app uses — FK schema, multi-level cascade insert, inner JOIN,
  filtered JOIN, GROUP-BY-COUNT over a JOIN, paginated ORDER-BY query, lazy
  relationship navigation, and UPDATE/DELETE — now runs END-TO-END over the PG
  wire, **8/8 stages, every query returning real data**. Two surgical
  correctness fixes (below) closed the only two gaps the workload surfaced.

### Fixed

- **SQL-standard doubled-quote string escape (SP-PG-ORM-REALAPP, 2026-06-03)**
  — the `kessel-sql` lexer now decodes `'bob''s post'` as the value `bob's
  post` (PG §4.1.2.1). The previous single-quote lexer stopped at the first
  inner `'`, truncating the string and then failing to parse — which broke ANY
  statement whose data contained an apostrophe (names, titles, prose). The fix
  mirrors the existing `"` delimited-identifier escape (doubled `''` → one
  `'`); a string with no embedded quote is byte-identical to the pre-fix token.
- **`ORDER BY` over a column projection renders (SP-PG-ORM-REALAPP,
  2026-06-03)** — `SELECT title FROM posts ORDER BY title [LIMIT n]` lowers to
  `Op::SelectSorted`, which returns FULL records (the projection is dropped at
  the engine layer), so the gateway's narrow projected-row decoder mismatched
  the row width. The gateway now detects the sorted-projection shape
  (`kessel_sql::select_projection_is_sorted`) and decodes the full records,
  re-projecting the requested columns with proper null-bitmap NULL fidelity. A
  non-sorted projection keeps the byte-identical narrow path. Neither fix
  touches the engine apply path or the Op wire encoding; determinism preserved.

- **Grouped aggregates over joins — `JOIN … GROUP BY + COUNT/SUM/MIN/MAX/AVG`
  (SP-PG-SQL-JOIN-AGG, 2026-06-03)** — `SELECT a.name, COUNT(b.id) FROM a JOIN b
  ON a.id=b.aid [WHERE …] GROUP BY a.name`, the dashboard/reporting query that
  counts (or sums / …) the related rows per parent. Composes the SP22 / SP-
  Analytic-Plan-MULTI group-aggregate fold with the combined join rows. `Op::Join`
  gained ONE additive field `group_aggregate: Option<JoinGroupAgg>` (a combined-
  schema `group_field` + `Vec<(kind, field_id)>` aggregate list, both referencing
  the `(a ++ b)` layout). When set, the engine groups the surviving combined
  `Vec<Value>` rows by the group field into a `BTreeMap` (ascending key order ⇒
  deterministic) and folds the aggregates per group over the DECODED Values,
  emitting the `[u32 ngroups][u32 keylen][key][16B i128 × n_aggs]` group-aggregate
  result (the `Op::GroupAggregateMulti` shape) instead of the join row stream.
  Because the fold runs over decoded Values, PostgreSQL NULL semantics fall out:
  `COUNT(b.id)` on a LEFT-join unmatched parent counts 0 (the NULL b.id is not
  counted) while `COUNT(*)` counts 1 (the combined row exists) — the classic
  LEFT-JOIN-COUNT gotcha, exact. `COUNT(*)` is encoded with a `COUNT_STAR_FIELD`
  sentinel field id; a qualified `COUNT(b.id)` disambiguates `id` across the two
  tables. kessel-sql resolves the group + aggregate field ids against the same
  combined schema the engine builds; both `apply_join` sites (main + RO-Txn
  bypass) share the fold. The PG gateway gains the FIRST group-aggregate render
  (`render_join_group_aggregate` + the `join_group_aggregate` text helper):
  RowDescription = [group col (its OID), agg cols (int8)], one DataRow per group
  (group key decoded by its FieldKind, each i128 → decimal). The wire change is
  additive — a marker-guarded ga block appended ONLY when `group_aggregate` is
  set, so every non-grouped join (bare / filtered / left / paginated) is byte-
  identical to the pre-arc frame and a corrupt marker is rejected at decode.
  Determinism (BTreeMap ascending key order + associative per-slot fold over the
  deterministic combined-row scan order ⇒ byte-identical on every replica) —
  VSR seed-7 + 3-replica oracle green. vulcan smoke: `SELECT author.name,
  COUNT(book.id) … GROUP BY author.name` → `tolkien 2, lewis 1`. Named follow-ups:
  SP-PG-SQL-HAVING, SP-PG-SQL-JOIN-GROUP-MULTI, SP-PG-SQL-JOIN-AGG-3TABLE,
  SP-PG-SQL-JOIN-AGG-ORDERBY-AGG.

- **Paginated joins — `JOIN … ORDER BY / LIMIT / OFFSET` (SP-PG-SQL-JOIN-QUERY,
  2026-06-03)** — `SELECT a.name, b.title FROM a JOIN b ON a.id=b.aid [WHERE …]
  ORDER BY b.created LIMIT 20 OFFSET 40`, the paginated-list-view shape behind
  every real app's list endpoint. This composes the SP23 (`Op::SelectSorted`)
  sort/page machinery with the combined join rows. `Op::Join` gained three
  additive fields: `order_by: Option<(field, desc)>` (a reference into the
  COMBINED `(a ++ b)` schema), `limit_n`, and `offset_n`. The engine stable-sorts
  the surviving combined rows by the qualified ORDER BY column (from EITHER table)
  with a NULL-aware, kind-aware comparator (numeric by kind, CHAR-pad-trimmed —
  mirroring SP23's `cmp_field`), reverses for `DESC`, then applies
  `offset_n`/`limit_n`. Both apply sites (main + RO-Txn bypass) share ONE
  `apply_join` helper so a paginated join inside a read-only Txn is byte-identical
  to a bare one. kessel-sql parses the trailing `ORDER BY <qualified col>
  [ASC|DESC]` + `LIMIT`/`OFFSET` after the optional `WHERE`, resolving the column
  against the same combined schema the engine builds. A bare `JOIN … LIMIT n` (no
  ORDER BY / OFFSET) keeps using the legacy pre-sort `limit` field so existing
  join frames stay wire-identical; ORDER BY / OFFSET route pagination to the
  post-sort fields. A LEFT-join unmatched right (`b.*`) NULL sort value orders
  NULLS LAST for ASC / NULLS FIRST for DESC — PostgreSQL's default. The wire
  change is additive: a marker-guarded page block is appended ONLY when
  order_by/limit_n/offset_n is set, so every non-paginated join (inner / filtered
  / left) is byte-identical to the pre-arc frame and older logs decode to all-None;
  a corrupt marker is rejected at decode. Determinism holds (stable sort over rows
  collected in the deterministic left-key/right-scan order ⇒ a total order with a
  scan-position tiebreak; no clock/RNG) — VSR seed-7 + 3-replica oracle green.
  vulcan smoke: `JOIN … ORDER BY b.title LIMIT 2` returns `hobbit, lotr` (sorted +
  paginated). Named follow-ups: SP-PG-SQL-JOIN-ORDERBY-MULTI,
  SP-PG-SQL-JOIN-ORDERBY-EXPR, SP-PG-SQL-JOIN-AGG, SP-PG-SQL-JOIN-NULLS-ORDER.

- **`LEFT [OUTER] JOIN` — outer joins (SP-PG-SQL-OUTER-JOIN, 2026-06-03)** —
  `SELECT a.name, b.title FROM a LEFT JOIN b ON a.id = b.aid`, the join every
  real ORM emits for an OPTIONAL relationship (SQLAlchemy `isouter=True`, the
  default for a nullable FK). `Op::Join` gained a `join_type` field (Inner |
  Left). LEFT mode emits EVERY left row; a left row with NO matching right row
  comes back ONCE with all right (`b.*`) fields NULL. The combined `KTR1`
  result's null bitmap carries the NULLs, so the gateway renders the PG
  `i32 -1` NULL sentinel with ZERO render-side change (the existing
  `decode_record` + `encode_data_row` already route NULL). kessel-sql parses
  `LEFT [OUTER] JOIN` (OUTER is a noise word); the three join-shape detectors
  learn the prefix so LEFT joins route to the join renderer. A `WHERE` on a
  right (`b.*`) column of a LEFT join drops the unmatched rows — standard
  PostgreSQL semantics. The wire change is additive: a one-byte join-type tag
  is appended ONLY when non-Inner, so every INNER join (filtered or not) is
  byte-identical to the pre-arc frame and older logs decode to Inner; an
  unknown tag is rejected at decode. Determinism holds (unmatched rows emit in
  left-key scan order; no clock/RNG) — VSR seed-7 + 3-replica oracle green.
  vulcan smoke: `LEFT JOIN` over `{tolkien, orphan}` returns 2 rows incl.
  `(orphan, NULL)`. Named follow-ups: SP-PG-SQL-RIGHT-JOIN,
  SP-PG-SQL-FULL-JOIN, SP-PG-SQL-MULTI-JOIN.

- **Filtered inner joins — `JOIN … WHERE` (SP-PG-SQL-JOIN-WHERE,
  2026-06-03)** — `SELECT a.name, b.title FROM a JOIN b ON a.id = b.aid
  WHERE b.title = $1 [AND a.name = $2]`, the most common real-app join
  beyond bare joins (SQLAlchemy `query.join(Book).filter(Book.title == x)`).
  `Op::Join` gained an OPTIONAL `kessel-expr` filter program over the
  COMBINED (a-fields ++ b-fields) schema: the engine joins, then runs the
  predicate per combined row, keeping only matches. kessel-sql compiles the
  qualified `WHERE` after the `ON` clause against the combined layout
  (`a.x` → left field, `b.y` → right; bare `col` by suffix with an
  ambiguity error when present in both tables); `AND`/`OR`/`NOT`/`IN`/
  `BETWEEN`/`LIKE` and params all work over the join. Gateway render reused
  (a filtered join just returns fewer combined rows). The wire change is
  additive — the filter is a trailing optional field, so a bare join is
  byte-identical to the pre-arc frame — and the filter is a pure function of
  the combined row, so seed-7 + 3-replica determinism holds. Filtered
  SQLAlchemy join smoke 7/7 on vulcan.

- **Zero-config SQLAlchemy: multi-row INSERT RETURNING + `RETURNING *`
  (SP-PG-RETURNING-MULTIROW-STAR V1, 2026-06-03)** — KesselDB now works
  with SQLAlchemy's OUT-OF-THE-BOX engine config (`create_engine(url)`,
  no `use_insertmanyvalues=False`). SQLAlchemy 2.0's DEFAULT
  (`use_insertmanyvalues=True`) batches a multi-object flush into ONE
  statement and expects N rows back. A multi-row
  `INSERT … VALUES (…),(…) RETURNING id` now returns **N DataRows** (one
  assigned id per row, in insertion order), and `RETURNING *` expands to
  every table column. New additive `OpResult::CreatedMany { ids }` (tag
  16); the `Op::Txn` apply arm threads each inner serial Create's
  assigned id back (deterministic — N applications of the proven
  single-row counter advance; 3-replica byte-identity green). The gateway
  desugars SQLAlchemy's `insertmanyvalues` form
  (`INSERT … SELECT … FROM (VALUES …) AS sen(…) ORDER BY sen_counter
  RETURNING …`) to plain multi-row VALUES before the literal-cast
  validator. SQLAlchemy DEFAULT-config CRUD 5/5 on vulcan. Closes the
  named follow-ups `SP-PG-RETURNING-MULTIROW` + `SP-PG-RETURNING-STAR`.
- **Deterministic autoincrement + `INSERT … RETURNING` (SP-PG-SERIAL-
  RETURNING V1, 2026-06-02)** — closes the two coupled follow-ups
  `SP-PG-SERIAL` + `SP-PG-RETURNING` together. A `BIGSERIAL`/`SERIAL`
  PRIMARY KEY column now autoincrements: an INSERT that omits the id is
  assigned the next per-table sequence value by the engine, and `INSERT
  … RETURNING id` reads it back. The sequence counter lives in the
  replicated state digest (reserved keyspace `0xFFFF_FFF4`) and advances
  ONLY on the deterministic apply thread in op-number order ⇒ every
  replica computes the identical gap-free sequence, crash + WAL replay
  resumes it exactly (no RNG, no wall-clock; 3-replica byte-identity
  proven). New `OpResult::Created { id }`; gateway renders RETURNING on
  both the simple- and extended-query paths. A SQLAlchemy 2.0
  autoincrement model declared WITHOUT an explicit id does full CRUD on
  vulcan and reads `w.id` back after commit — autoincrement smoke 6/6.
  Out-of-scope follow-ups: UPDATE/DELETE RETURNING, `CREATE SEQUENCE`,
  non-PK SERIAL, multi-row RETURNING.
- **PostgreSQL Extended Query protocol (SP-PG-EXTQ V1, 2026-05-29)** —
  full V1 message set `P` (Parse) / `B` (Bind) / `D` (Describe) /
  `E` (Execute) / `S` (Sync) / `C` (Close) / `H` (Flush). Per-connection
  `SessionState` with named + unnamed prepared statements + portals up
  to `MAX_PREPARED_STATEMENTS_PER_CONN = MAX_PORTALS_PER_CONN = 4096`.
  Real `psycopg2.connect(...)` + `cur.execute("…WHERE id = %s", (42,))`
  returns rows on vulcan.
- **PostgreSQL Extended Query binary-format parameters (SP-PG-EXTQ-BIN
  V1, 2026-06-01)** — binary Bind admission for the 10 V1 supported PG
  scalar types (INT2/INT4/INT8/FLOAT4/FLOAT8/BOOL/TEXT/VARCHAR/BYTEA/
  TIMESTAMPTZ). Decodes each binary param at Execute time into a SQL
  literal that flows through the existing substitute layer. Closes the
  T8 PARTIAL gap for asyncpg + psycopg3 DEFAULT cursor.
- **PostgreSQL Extended Query binary-format RESULTS (SP-PG-EXTQ-BIN-RESULTS
  V1, 2026-06-01)** — symmetric DataRow + RowDescription post-processor
  for portals with `result_formats=[1]`. Re-encodes each buffered DataRow
  per-column into PG binary format. asyncpg `conn.fetch(...)` round-trips
  end-to-end on vulcan.
- **PostgreSQL JDBC simple-mode `::cast` rewrite (SP-PG-EXTQ-CAST V1,
  2026-06-02)** — `cast_stripper::strip_pg_casts(sql) -> String`
  single-pass state-machine scanner strips `::TYPE[(args)]` while
  preserving cast-like text inside strings/comments. Wires in at
  `dispatch_query` entry; covers both Simple Query and Extended Query
  Execute paths.
- **pgJDBC extended-mode `SELECT version()` Describe synthesizer
  (SP-PG-EXTQ-DESCRIBE-VERSION V1, 2026-06-02)** —
  `extq::scalar_row_descriptions` with closed-set whitelist of scalar
  SELECT patterns (`SELECT version()`, `SELECT current_user`,
  `SELECT 1`, etc.) emits matching RowDescription at Describe time
  instead of `NoData`. pgJDBC extended-mode `SELECT version()` round-trips
  via real pgJDBC 42.7.4 on vulcan.
- **pgJDBC simple-mode paren-wrapped VALUES (SP-PG-SQL-PAREN-VALUES V1,
  2026-06-02)** — kessel-sql's VALUES tuple parser now accepts
  `(LITERAL)` paren-wrapped literals up to depth 8 (anti-stack-bomb
  cap at 9). Same arc adds `Str → numeric` coercion in the WHERE term
  parser when the LHS is a numeric column (PG's `'42'::int8` semantic
  preserved across the cast strip). Real pgJDBC simple-mode
  `PreparedStatement` INSERT + SELECT WHERE id=? round-trip
  end-to-end on vulcan.
- **CHAR(N) padding-aware comparison (SP-CHAR-PAD-COMPARE V1, 2026-06-02)**
  — engine-side fix in `kessel-expr` EQ/NE/LT/LE/GT/GE for `Value::Bytes`
  and `kessel-sm::cmp_field` for `Char(_) | Bytes(_)`: `right_trim_char_pad`
  drops trailing NUL (0x00) + space (0x20) before compare. PG SQL §9.20
  semantic generalised to NUL (engine stores fixed-width values
  NUL-padded). asyncpg `WHERE name = $1` against CHAR(N) now returns
  matching rows on vulcan.
- **Real pgJDBC end-to-end smoke (SP-PG-JDBC-SMOKE V1, 2026-06-02)** —
  user-space OpenJDK 21 + pgJDBC 42.7.4 + scripts/JdbcSmoke.java harness
  drives KesselDB on vulcan. Full CRUD PASS in both simple AND extended
  modes: CREATE TABLE, `PreparedStatement` INSERT (`setLong`+`setString`),
  SELECT *, `PreparedStatement` SELECT WHERE id=?, `SELECT version()`.
- **PostgreSQL COPY FROM STDIN / COPY TO STDOUT (SP-PG-COPY V1,
  2026-05-30)** — text-format end-to-end for both directions. Per-connection
  CopyIn state machine: CopyData / CopyDone / CopyFail handled while in
  CopyIn; any other tag = `08P01` + state clear + STAY ALIVE. Unlocks
  `pg_dump` restore, `sysbench prepare`, and `psql \copy` workflows.
- **PostgreSQL COPY CSV format (SP-PG-COPY-CSV V1, 2026-06-01)** —
  `WITH (FORMAT csv [, DELIMITER 'X'] [, QUOTE 'X'] [, ESCAPE 'X']
  [, NULL 'string'] [, HEADER])` for both directions. RFC 4180 +
  PG superset; doubled-quote escape; embedded-delimiter/quote/newline
  quoting; record-oriented parser reassembles quoted-newline records
  across CopyData frame boundaries. Unlocks `pg_dump --csv`,
  `psql \copy ... CSV HEADER`, every spreadsheet/pandas analyst on-ramp.
- **PostgreSQL COPY binary format (SP-PG-COPY-BIN V1, 2026-06-02)** —
  `WITH (FORMAT binary)` per PG §55.2.7. 19-byte signature header +
  per-row length-prefixed field values + 2-byte i16 -1 EOD marker. Same
  10 supported types as SP-PG-EXTQ-BIN-RESULTS via reuse of
  `encode_binary_value` (TO) and `decode_binary_param` (FROM). Unlocks
  `pg_dump --format=custom`, JDBC `CopyManager`, `pg_bulkload`,
  `pgloader`, Stitch, Fivetran, Airbyte binary bulk-loaders.
- **PostgreSQL COPY bulk-apply throughput (SP-PG-COPY-BULKAPPLY V1,
  2026-05-30)** — COPY FROM STDIN now buffers up to `COPY_BATCH_SIZE`
  rows (default 1024, env-overridable via `KESSELDB_COPY_BATCH_SIZE`)
  and flushes each batch as ONE multi-row `INSERT INTO t (cols) VALUES
  (...), ...` which kessel-sql compiles to `Op::Txn { ops: Vec<Op::Create> }`.
  One apply round-trip + one WAL fsync per batch instead of one per row.
- **Cross-DB benchmark suite (SP-Bench-Suite T1-T4)** — KesselDB vs
  Postgres + SQLite + TigerBeetle reproducible head-to-head harness at
  `tools/bench-compare/`. Workloads: YCSB-A/B/C, sysbench OLTP RO/WO/RW,
  TPC-H Q1+Q6. Wins AND losses published verbatim in `docs/BENCHMARKS.md`.
- **Helm chart + Fly.io deploy (SP-Cloud-Deploy, 2026-05-30)** — Helm
  chart at `deploy/helm/kesseldb/` (single-pod ReadWriteOnce PVC,
  ClusterIP service); `fly.toml` at `deploy/fly/`. Helm chart verified
  end-to-end on vulcan (kind v0.24.0 + Kubernetes v1.31.0 + helm v3.16.3).
- **Multi-arch Docker image + DX polish (SP-DX-superior V1, 2026-05-29)**
  — `ghcr.io/hassard0/kesseldb:latest` multi-arch (`linux/amd64` +
  `linux/arm64`) ~77 MiB stripped, pushed on every `v*` tag via
  `release.yml`. Did-you-mean SQL suggestions on `unknown table` /
  `unknown column` (zero-dep edit-distance + inlined column-list head);
  `kessel` CLI differentiates connection-refused / wrong-token /
  DNS-failure / timeout with env-var-pointing hints; embedded example
  at `crates/kesseldb-server/examples/embedded.rs`.

### Performance

- **SP-Perf-A-SHARD-APPLY V1 (2026-05-30)** — K independent per-shard
  sub-engines (each its own `Arc<RwLock<StateMachine>>` + apply thread +
  WAL + SSTables, rooted at `data_dir/shard-<i>/`); routes every Op via
  `hash(make_key(type_id, oid)) % K`. Opt-in via `ServerConfig.shard_count
  = Some(K)`. Vulcan get-by-id sweep (10K rows, 16 workers, 10s):
  K=baseline 4.68M ops/s → K=2 7.30M → K=4 11.08M → **K=8 14.93M
  (3.19× — breaks the ~5M `RwLock`-reader ceiling)** → K=16 16.72M.
- **SP-Perf-A-SHARD-SCAN + -FASTPATH + -POOL-SCALEOUT + -LOCAL-INDEX-FUSION
  (2026-05-30 → 2026-06-02)** — scan-side companions to SHARD-APPLY.
  K-invariance for scatter-gather scan ops; find-by perf at K≥2
  recovered by 105×; every scan workload at K=4 scales POSITIVELY;
  sharded find-by parity without requiring `--pool-workers`.
- **SP-Perf-A-TXN-RO V1 (2026-05-29)** — static all-RO `Op::Txn`
  classification routes through the Perf-A read-pool bypass. N=16 lift
  **42.6×** (680 → 28,977 tx/s); KesselDB now **5.7× Postgres** at N=16
  oltp-RO.
- **SP-Perf-A-TXN-RW V1 (2026-05-30)** — driver-level split-phase
  dispatch on (R*, W*)-shape Txns. Read prefix routes via TXN-RO bypass,
  write suffix via `sm.write().apply`. N=16 lift **14.43×** (712 → 10,273
  tx/s); KesselDB now **2.66× Postgres + 2.60× SQLite** at N=16 oltp-RW.
- **SP-Analytic-Plan + -MULTI + SP-Hash-Agg + -Tune + SP-WHERE-VM-Specialise
  V1 (2026-05-29 → 2026-06-01)** — five sequential arcs for the TPC-H
  Q1/Q6 losses. Cumulative gap-closing: **Q1 18× → 2.17×**, **Q6 123× → 3.07×**.
  Q6 design floor (≥400 q/s) + stretch (≥500 q/s) both EXCEEDED.
  Next: **SP-JIT-Aggregate**.
- Sub-µs p50 read latency at N=16 (Perf-A T2 + T7); 4.75M ops/sec
  single-shard parallel-read; 53,409 tx/s sysbench WO at N=8
  (**5.2× Postgres**); 51,840 rows/sec PG COPY FROM STDIN (**181.9×**
  lift via SP-PG-COPY-BULKAPPLY).

### Fixed

- **Cluster test flakes (SP-CLUSTER-FLAKE T2, root-cause fix)** —
  `Node::submit*` / `apply_raw` now retry transient `ViewChange` →
  `Unavailable` the same way production `ClusterClient` does. The fix
  lives in the production code path, not a test relaxation. Closes the
  long-standing CI intermittent surfaced by stress runs.

### Compatibility

- psycopg2 ✓ SQLAlchemy 2.0 ✓ psycopg3 ✓ asyncpg ✓ all PASS on vulcan
  with default settings (no `ClientCursor` workaround needed).
- pgJDBC 42.7.4 ✓ — real-driver verified on vulcan in **both simple
  AND extended modes**: CREATE TABLE, `PreparedStatement` INSERT
  (`setLong`+`setString`), SELECT *, `PreparedStatement` SELECT WHERE
  id=?, `SELECT version()`.
- pgx (Go), Drizzle/Prisma (Node), sqlx (Rust) — runtime not on vulcan
  smoke host; tracked as V2 `SP-PG-GO-SMOKE` / `SP-PG-NODE-SMOKE` /
  `SP-SQLX-SMOKE`. Same binary Bind + binary RESULTS unlock shape
  as asyncpg / JDBC.

### Documentation

- README rewritten above the fold with the 2026-06-02 headlines
  (14.93M ops/sec sharded reads + real ORM compat + 6/8 cross-DB wins +
  COPY in 3 formats + Helm/Fly).
- STATUS preamble bumped to 2026-06-02 with the coherent state-of-the-union.
- USAGE §9 ORM matrix flipped to all-PASS rows.
- BENCHMARKS summary table refreshed with the post-WHERE-VM headlines.

### Tests

- **2442 default / 2470 with `--features pg-gateway` / 2503 with all
  gateway features** (vulcan-measured 2026-06-02 at HEAD `f2a18e5`, fresh
  full sweep; the prior 2063 / 2074 / 2078 figures had drifted from the
  actual workspace count).

## [1.0.0] — 2026-05-28

Initial public release.

### Added

- **Binary protocol over TCP** — length-prefixed `Op::encode()` framing
  with mode tags `0xFE` (SQL), `0xFD` (session / exactly-once), `0xFC`
  (auth handshake), `0xFB` (admin stats), `0xFA` (snapshot). Zero
  external dependencies.
- **HTTP/1.1 gateway** (SP141, opt-in `--features http-gateway`) —
  `/v1/sql`, `/v1/op`, `/v1/health`, `/v1/metrics` (Prometheus text
  v0.0.4). `Authorization: Bearer` constant-time auth + optional
  `X-Kessel-Client-Id` + `X-Kessel-Req-Seq` exactly-once headers.
- **WebSocket gateway** (SP-WS, same `--features http-gateway`) —
  `/v1/ws` upgrade, `kessel-op-v1` subprotocol, binary frames carrying
  `Op::encode()`. RFC 6455 strict handshake, 16-message bounded send
  queue, 30 s ping/pong heartbeat.
- **PostgreSQL Frontend/Backend Protocol v3.0** (SP-PG, opt-in
  `--features pg-gateway`) — Simple Query path + SCRAM-SHA-256
  authentication with the Bearer↔SCRAM bridge (the operator's Bearer
  token IS the SCRAM password input — one credential surface, rotating
  the token rotates HTTP + WS + PG together).
- **`pg_catalog` + `information_schema` stubs** (SP-PG-CAT V1) — synth
  responses for `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`,
  `pg_index`, `pg_constraint`, plus 7 `information_schema` views
  (`tables`, `columns`, `schemata`, `key_column_usage`,
  `table_constraints`, `views`, `routines`). psql, pgcli, pgAdmin 4,
  DBeaver, DataGrip, Metabase, Tableau, Looker, Hex, Superset, and
  pgJDBC all connect + browse out of the box, verified by
  synthetic-peer KATs.
- **Cross-shard scatter scan (SP-A)** — `Select` / `QueryRows` /
  `SelectFields` / `SelectSorted` fan out across K shard groups via
  `scatter_scan` with std-thread workers + bounded per-shard channels.
  Unordered = shard-id-deterministic concat; sorted = `BinaryHeap`
  k-way merge. K-invariance locked across K ∈ {1, 2, 4, 8, 16} by an
  85-seed property sweep.
- **Parquet codec matrix** — 6 of 7 codecs supported
  (UNCOMPRESSED, Snappy, GZIP, zstd, LZ4_RAW, Brotli). SP154 closed
  OBJ-2c-2 with a hand-rolled zero-dep RFC 7932 Brotli decoder
  comparable in scope to the SP125-SP140 zstd arc. Legacy LZ4 framing
  (codec id 5) and LZO remain unsupported; modern LZ4_RAW (codec id 7)
  is fully supported via SP149.
- **Strategic-tier rigor artifacts** — mechanically-verified TLA+
  (S1, `Replication.tla` TLC: 528M states / depth 21 / 0 violations)
  over 7 layered modules (Replication → MVCCStorage → MVCCTx →
  MVCCSi → MVCCSsi → MVCCGc → MVCCCutover); serializable MVCC with
  Cahill SSI (S2); 5 hand-derived Jepsen-style linearizability tests
  under partition + message loss (S3); deterministic WASM-MVP UDF
  interpreter (S4).
- **External sources** — `REGISTER` + `REFRESH` JSON/NDJSON/CSV/Parquet
  from HTTP/HTTPS endpoints (`--features external-sources` /
  `external-sources-tls`) or S3-compatible / Azure Blob object storage
  (`--features external-sources-objstore`).
- **MIT License**.

### Changed

- Cluster test wait-for-primary now uses `submit_with_retry` (the
  test-side analog of the production SP42 `ClusterClient` retry
  contract). Fixes a long-standing intermittent CI flake that depended
  on the primary's commit-counter racing with the test's first op.

### Security

- One credential surface across binary + HTTP + WebSocket + PostgreSQL
  wire (Bearer token, constant-time compared; rotating it rotates
  every listener atomically).
- SCRAM-SHA-256 password derivation via PBKDF2-HMAC-SHA-256 (RFC 8018
  §5.2), zero-dep implementation in `kessel-crypto`.
- HTTPS for external sources via rustls + bundled Mozilla webpki roots
  with full certificate + hostname verification (no bypass, no flag
  to disable). Object-store transport is HTTPS-only.

### Tests

- 1792 default / 1820 with `--features pg-gateway` / 1875 with all
  gateway features. Includes seeded partition/fault simulation,
  multi-replica Jepsen linearizability, MVCC TLA+ refinement, pyarrow
  Parquet round-trips, WASM-MVP KATs, the SP-A 85-seed K-invariance
  sweep, and synthetic-peer suites verifying each GUI tool's verbatim
  introspection SQL.

### Documentation

- README + `docs/USAGE.md` + `docs/ARCHITECTURE.md` + `docs/STATUS.md`
  + `AGENTS.md` shipped polished to coherent terminology and
  consistent test counts.
- `docs/book/` mdBook GitHub Pages site (built + deployed by
  `.github/workflows/pages.yml`); single source of truth — each
  chapter either uses `{{#include}}` against the existing root doc or
  is a thin cross-link landing page.

[1.0.0]: https://github.com/hassard0/KesselDB/releases/tag/v1.0.0
