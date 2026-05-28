# SP-PG-CAT — `pg_catalog.*` introspection stubs for KesselDB PG wire — DESIGN

**Status:** design — scopes the V2 follow-up arc that unlocks GUI
admin tools (pgAdmin / DBeaver / DataGrip / Metabase / Tableau /
Looker / Mode / Hex / Superset / Redash / sqlmesh / dbt-postgres /
schemaspy / datadog-postgres-integration / prometheus-postgres-
exporter) into 8 concrete task slices and locks the invariants the
implementation will de-risk one at a time. Companion progress
tracker at
`docs/superpowers/specs/2026-05-27-kesseldb-subproject-sppgcat-progress.md`.

**Builds on:**
- **SP-PG V1** (`docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`)
  — the closed-out arc that ships psql / pgcli / JDBC / psycopg /
  pgx / sqlx-pg / `pg`-Node / Drizzle / Prisma / Diesel-pg simple-
  query CRUD. V1 §2.2 and progress-tracker §"Out-of-scope" both
  name this arc as the very first V2 follow-up: "minimal
  `pg_catalog.*` stubs (pg_type, pg_class, pg_attribute,
  pg_namespace) — enough for psql `\dt` / `\d <table>` not to crash;
  pgAdmin / DBeaver gateway." V1 §11 weak-spot #8 lays out the
  problem statement verbatim: "pgAdmin and DBeaver issue ~50
  introspection queries against `pg_catalog.*` on the first connect.
  V1 returns `42P01` undefined_table for each. pgAdmin in particular
  may refuse to show the connection in its UI without these. The
  honest gap is V1 supports CLI clients and language-driver
  programmatic clients but NOT GUI admin tools." V2 fixes; V1
  documents the boundary.
- **`crates/kessel-pg-gateway/src/dispatch.rs`** — the simple-query
  dispatch that this arc hooks BEFORE the `engine.apply_sql` call.
  V1's `dispatch_query(sql, engine) -> Vec<u8>` is the chokepoint
  every Q message flows through; adding the pg_catalog interceptor
  here is a single-call-site change that does NOT touch the engine
  trait, the SQL parser, or the response encoders.
- **`crates/kessel-pg-gateway/src/engine.rs`** — `EngineApply` trait
  with `apply_sql` + `describe_table(name)` (T8 added the latter).
  This arc adds a third method `list_tables() -> Vec<String>` so
  the synthesizer has a data source for `pg_class` rows. Default
  impl returns an empty Vec for back-compat (existing impls don't
  break at the T1 commit boundary).
- **`crates/kessel-pg-gateway/src/types.rs`** — the FieldKind →
  PG type-OID map (V1 T4). The pg_attribute synthesizer reuses
  `field_kind_to_oid` to fill `pg_attribute.atttypid` per column.
- **`crates/kessel-pg-gateway/src/response.rs`** — the V1 T5/T6
  RowDescription + DataRow + CommandComplete + ReadyForQuery
  encoders. The synthesizer emits the same wire shapes; this arc
  adds NO new encoder, just new callers.
- **`crates/kessel-catalog/src/lib.rs`** — the KesselDB authoritative
  type catalog (ObjectType / Field / FieldKind). The synthesizer
  walks the live catalog (via `EngineApply::list_tables` +
  `EngineApply::describe_table`) and maps each table+column to its
  pg_catalog row equivalent.

---

## 1. Context — why pg_catalog stubs unlock the GUI ecosystem

SP-PG V1 closed shipping psql + every libpq-based programmatic
driver. V1's named-scope-boundary in `docs/USAGE.md §9` is:

> CLI clients (`psql`, `pgcli`) and programmatic drivers (JDBC,
> psycopg, pgx, sqlx-pg, `pg`-Node, Drizzle, Prisma, Diesel-pg)
> work; GUI admin tools (pgAdmin, DBeaver, DataGrip, Metabase,
> Tableau, …) do NOT work because V1 returns `42P01`
> undefined_table for every `pg_catalog.*` query they issue on
> connect.

This arc closes that boundary. Concretely, when a GUI tool
connects to a Postgres server, it does NOT just open a socket and
ask `SELECT 1`. It runs ~5-50 introspection queries against
`pg_catalog.*` and `information_schema.*` to populate its UI tree
(databases → schemas → tables → columns → indexes → constraints).
Examples (captured from real-tool wireshark dumps + project source):

| Tool | Queries on connect |
|---|---|
| **pgAdmin 4** | ~50 queries: pg_namespace, pg_class (relkind in 'r','v','m','t','p'), pg_attribute (per table on expand), pg_index, pg_constraint, pg_proc, pg_database, pg_settings, version(), current_user, current_database(), current_schema() |
| **DBeaver** | ~30 queries: pg_database, pg_namespace, pg_class, pg_attribute, pg_index, pg_constraint, pg_type, pg_collation, pg_authid, pg_roles, current_setting('server_version') |
| **DataGrip / IntelliJ** | ~20 queries: pg_namespace, pg_class, pg_attribute, pg_constraint, pg_proc + JetBrains-specific `SELECT ... FROM information_schema.routines` |
| **Metabase** | ~5 queries: information_schema.schemata, information_schema.tables, information_schema.columns, pg_class (for table-size estimates) — the FEWEST of any tool |
| **Tableau** | ~10 queries via the ODBC PostgreSQL driver: information_schema.tables, pg_class (oid), pg_attribute (column list) |
| **Looker / Mode / Hex** | ~8 queries each: similar Metabase pattern via information_schema |
| **Superset / Redash** | ~10 queries: information_schema.tables, information_schema.columns, pg_class (oid for stats) |
| **dbt-postgres** | ~5 queries via SQLAlchemy: information_schema.tables, information_schema.columns; relies on the SQL dialect more than the catalog |
| **sqlmesh** | similar dbt pattern |
| **datadog-postgres-integration** | ~15 queries: pg_stat_database, pg_stat_user_tables, pg_class (for relation sizes) |
| **prometheus-postgres-exporter** | ~20 queries: pg_stat_*, pg_database, pg_settings, pg_locks |

The common denominator across ALL of these is `pg_catalog.pg_class`
+ `pg_catalog.pg_namespace` + `pg_catalog.pg_attribute` +
`information_schema.tables` + `information_schema.columns`. If
KesselDB stubs those six tables (real on-the-wire `RowDescription`
+ `DataRow` responses synthesized from the live catalog), the GUI
tool's connection wizard completes and the user gets a tree view
of their KesselDB tables.

The two tools that go FURTHER — pgAdmin (querying `pg_database`,
`pg_settings`, `pg_proc`) and prometheus-postgres-exporter
(querying `pg_stat_*`) — get empty-but-well-formed responses for
the tables this arc names V1-out-of-scope. pgAdmin shows
"connected" but with an empty function list + a default
postgres-style settings page; prometheus-postgres-exporter sees
zero stats and reports zero metrics (gracefully). Neither crashes;
both display the connection.

The cost-vs-value case is asymmetric in our favor: ~8 task slices
of synthesizer code unlocks the entire GUI admin / BI ecosystem.
Compare with the alternative — for each GUI tool that wants to
display a KesselDB connection, the user runs a `pg_dump`-and-
import-into-Postgres workflow; that defeats the point of
KesselDB-as-an-online-database. pg_catalog stubs let KesselDB sit
where Postgres sits in the user's tool chain.

## 2. Scope

### 2.1 V1 of this arc — what's in (~8-10 slices)

1. **Six core pg_catalog tables** stubbed with read-only synthesized
   rows from the live KesselDB catalog:
   - `pg_namespace` — three canned schemas (pg_catalog OID 11,
     public OID 2200, information_schema OID 2202) per PG canonical
     OIDs. Every KesselDB table lives in `public`; pg_catalog and
     information_schema are reserved-name stub schemas.
   - `pg_class` — one row per KesselDB table; relnamespace=2200
     (public); relkind='r' (ordinary table); reloftype=0;
     relowner=10 (canonical postgres-user OID); OID = stable hash
     of table name (so subsequent queries can join on
     `relnamespace` / `attrelid` deterministically).
   - `pg_attribute` — one row per (table × column); attrelid = the
     table's pg_class.oid; atttypid = `field_kind_to_oid(kind)`
     from the V1 type-OID map (T4); attnum = 1-based column index;
     attnotnull = `!nullable` (KesselDB column flag); attlen = the
     PG type's size from `type_size_for_oid`; atttypmod = -1
     (V1 doesn't carry per-column modifiers); attisdropped = false.
   - `pg_type` — one row per PG type OID V1 actually emits (the
     ~12 OIDs in `types.rs` — bool=16, bytea=17, int8=20, int2=21,
     int4=23, text=25, varchar=1043, timestamptz=1184, numeric=1700,
     plus oid=26 since pg_class rows reference oids). Canned from
     PG's `pg_type.dat` snapshot.
   - `pg_index` — one row per KesselDB index; indrelid = the
     table's pg_class.oid; indexrelid = stable hash of index name;
     indkey = column attnums as a packed int2vector; indisunique
     = per the KesselDB index kind; indisprimary = false (V1
     KesselDB has no PRIMARY KEY DDL — `id` is implicit).
   - `pg_constraint` — one row per UNIQUE / FOREIGN KEY / CHECK
     constraint, deriving from the catalog's `unique_fields` /
     `foreign_keys` / `checks` collections.
2. **Two information_schema views**:
   - `information_schema.tables` — three columns (`table_catalog`
     = 'kesseldb', `table_schema` = 'public', `table_name` = …,
     `table_type` = 'BASE TABLE'). Less than 1% of GUI tools need
     more columns; spec out-of-scope.
   - `information_schema.columns` — six columns (`table_catalog`,
     `table_schema`, `table_name`, `column_name`, `ordinal_position`,
     `data_type` — the PG-text type name like `bigint` / `text`).
3. **SQL helper functions** (rewriter for SELECT-of-known-function):
   - `version()` → `'PostgreSQL 14.0 (KesselDB-1.0)'` (consistent
     with V1 ParameterStatus.server_version).
   - `current_database()` → `'kesseldb'`.
   - `current_schema()` → `'public'`.
   - `current_user` / `session_user` → `'kesseldb'` (V1 has no
     per-user identity; document gap).
   - `pg_my_temp_schema()` → `0` (no temp schemas in KesselDB).
   - `pg_is_other_temp_schema(oid)` → `false`.
   - `obj_description(oid, name)` / `obj_description(oid)` → NULL
     (V1 has no object comments).
   - `pg_get_constraintdef(oid)` → `''` (V1 doesn't reconstruct
     constraint definitions).
   - `pg_get_indexdef(oid)` → `''` (V1 doesn't reconstruct index
     DDL).
   - `pg_table_is_visible(oid)` → `true` (V1 has one schema, all
     tables visible).
   - `pg_encoding_to_char(encoding)` → `'UTF8'`.
4. **SQL pattern matching** — a small dispatcher that recognizes
   ~30-50 canonical query patterns each GUI tool issues, and routes
   each to a synthesizer. Patterns are stored as a static table of
   (regex-or-string-match-predicate, synthesizer-fn) pairs. The
   recognizer is FAST (O(1) per Q message — early-reject on the SQL
   starting with a token other than `SELECT`).

### 2.2 V1 of this arc — what's out (named, deferred)

These are explicitly NOT V1 of SP-PG-CAT. Each is named so future
scoping finds the design call:

- **`pg_proc`** — function catalog. V1 stub returns an empty
  result set (zero rows + a RowDescription with the canonical
  pg_proc columns). pgAdmin's function panel will be empty;
  acceptable for V1.
- **`pg_operator`** — operator catalog. Empty stub same shape as
  pg_proc.
- **`pg_authid`** / `pg_roles` / `pg_user` — auth catalog. Empty
  stub (KesselDB V1 PG-wire has no multi-user model — every
  connection is the same Bearer-token identity).
- **`pg_database`** — multi-database catalog. V1 returns ONE row
  (oid=1, datname='kesseldb', datdba=10). When KesselDB grows
  multi-database, this expands.
- **`pg_settings`** / `pg_settings(...)` — GUC catalog. V1 returns
  a small canned set (server_version, server_encoding,
  client_encoding, TimeZone, DateStyle, integer_datetimes,
  standard_conforming_strings, application_name) matching the
  V1 ParameterStatus emit. Tools that try to SET arbitrary GUCs
  get the V1 "ignored" treatment.
- **`pg_stat_*`** (pg_stat_database, pg_stat_user_tables,
  pg_stat_activity, …) — runtime statistics catalog. V1 stub
  returns zero rows for every pg_stat_* query (prometheus-
  postgres-exporter will report zeros; not a problem until a
  Datadog-equivalent observability arc lands).
- **`pg_locks`** — lock catalog. Empty stub.
- **`pg_collation`** — collation catalog. V1 returns one row for
  'default' (oid=100, collname='default') so DBeaver's collation
  picker doesn't crash. Real collation support is V3.
- **Row-level catalog updates as DDL fires** — today the
  synthesizer re-synthesizes from the live KesselDB catalog on
  every query. For a tool that polls the catalog every 5s
  (Metabase metadata refresh), this is fine. For a workload with
  10K tables this becomes a hot path; V2 SP-PG-CAT-CACHE adds
  a per-query cache invalidated on DDL.
- **Arbitrary pg_catalog SQL** (JOIN / GROUP BY / sub-SELECT
  against pg_catalog tables) — V1 recognizes ~30-50 canonical
  query patterns. A tool issuing a novel JOIN that doesn't match
  any pattern still gets `42P01`; documented as the V1-of-this-
  arc boundary. The pattern corpus grows as we observe new
  tools' queries (each is a 5-line table entry).
- **psql `\d+` extended output** — psql's `\d+` runs a more
  detailed query joining pg_class + pg_description + pg_indexes
  + pg_stat_user_tables. V1 covers `\d` (basic table description)
  but `\d+` will partially work — relkind/columns yes, comments/
  stats no. Document the gap.
- **Cross-schema queries** — V1 only knows about the `public`
  schema. A tool that issues `SELECT * FROM other_schema.t` gets
  `42P01` (because KesselDB itself has no schemas yet). When
  KesselDB grows multi-schema (SP-NS), this auto-extends.

## 3. Architecture — intercept at the dispatch layer, NOT the engine

The cleanest seam for this arc is the existing
`kessel_pg_gateway::dispatch::dispatch_query` function:

```text
Q message arrives with SQL bytes
  ↓ split semicolons (V1 = single-statement only)
  ↓ trim leading whitespace + comments
  ↓ if empty → EmptyQueryResponse + ReadyForQuery + return
  ↓ ──── SP-PG-CAT HOOK (NEW) ──────────────────────────────
  ↓ pg_catalog::catalog_query_hook(sql, engine):
  ↓   if SQL matches a canned pg_catalog / information_schema
  ↓   pattern OR a SELECT-of-known-function → return
  ↓   Some(wire_response_bytes); the bytes are the full
  ↓   T + D* + C + Z stream the engine.apply_sql path would
  ↓   have emitted on success.
  ↓ if Some → write bytes, return
  ↓ ──────────────────────────────────────────────────────────
  ↓ if None → existing T8 path:
  ↓   engine.apply_sql(sql) → OpResult
  ↓   format_result_pg(&op_result, &mut sink)
  ↓ ReadyForQuery('I')
```

**Why intercept at the dispatch layer (NOT inside engine.apply_sql):**

1. **Zero engine changes for the wire-protocol-quirk surface.**
   pg_catalog is a Postgres-protocol-specific concept; KesselDB
   itself has nothing called "pg_catalog" in the catalog, and
   shouldn't. Adding fake virtual tables to the engine would
   require every other dispatch path (HTTP `/v1/sql`, WebSocket
   binary, native client) to either expose them (wrong — those
   surfaces don't pretend to be Postgres) or filter them out
   (every filter site is a bug surface).
2. **One implementation site.** All knowledge of pg_catalog
   shape lives in `kessel-pg-gateway::pg_catalog::*`. If a new
   GUI tool issues a query we don't recognize, we add a pattern
   here and nothing else changes.
3. **Synthesized responses bypass the engine's apply_sql path
   entirely.** This means: no engine-level catalog read (we use
   the read-only `EngineApply::list_tables` + `describe_table`
   trait methods), no journaled op, no scatter-scan, no SP-A
   shard fanout. The synthesizer is pure-CPU + a single live-
   catalog walk per query, on the gateway thread.
4. **Read-only invariant is easy to enforce.** The hook signature
   is `catalog_query_hook(sql: &str, engine: &dyn EngineApply)
   -> Option<Vec<u8>>` — it takes an immutable engine reference,
   so the type system prevents accidentally mutating the catalog.
   No DDL pretending to be a pg_catalog SELECT.
5. **Existing test surface unchanged.** Every V1 KAT in
   `dispatch.rs::tests` continues to pass because the hook
   returns `None` for non-pg_catalog SQL — the existing
   `engine.apply_sql` path runs unchanged.

**Why NOT intercept at the kessel-sql parser layer:**

- `kessel-sql` is the language layer; it has zero PG context. It
  shouldn't grow PG-catalog-specific parse rules.
- Multiple wire surfaces share `kessel-sql` (HTTP / WS / PG / native);
  PG-specific behavior in the parser would leak everywhere.
- Intercept-before-parse is faster anyway (we don't need a parser
  AST to recognize `SELECT * FROM pg_catalog.pg_namespace`).

## 4. SQL pattern matching — recognize the queries GUI tools issue

V1 of this arc does NOT attempt to be a general pg_catalog query
planner. The realistic ambition is: capture the EXACT queries
real GUI tools issue, and ship a synthesizer per query. The
matched-pattern table is the contract.

The recognizer is `catalog_query_hook(sql) -> Option<Synthesizer>`:

```rust
type Synthesizer = fn(&dyn EngineApply) -> Vec<u8>;

fn catalog_query_hook(sql: &str) -> Option<Synthesizer> {
    let normalized = normalize_for_match(sql); // lowercase, strip
                                               // leading comments,
                                               // collapse whitespace
    if !normalized.starts_with("select") {
        return None; // fast reject — only SELECTs hit pg_catalog
    }
    for (pattern, synth) in CATALOG_PATTERNS.iter() {
        if pattern.matches(&normalized) {
            return Some(*synth);
        }
    }
    None
}
```

**Tactic — pattern shape:**

- **Exact-string match** is the cheapest. `SELECT version()` →
  `synth_version()`. ~20% of GUI queries are exact-match.
- **Prefix + substring** match. `SELECT * FROM pg_catalog.pg_class
  WHERE relnamespace = 2200` matches "SELECT FROM pg_class
  WHERE relnamespace" prefix + `2200` substring. ~50% of queries
  fit this shape (the WHERE clause differs only in OID literals).
- **Regex** for the gnarly ones (DBeaver in particular issues
  parameterized JOINs with normalize-able placeholders). ~30% of
  queries.

**T2 deliverable:** capture the actual queries `psql`, `pgcli`,
`pgAdmin`, `DBeaver` issue on connect. Run each tool against a
real Postgres server with `log_statement = 'all'`, copy the
issued queries into `crates/kessel-pg-gateway/src/pg_catalog/
queries.md`. This is the corpus the pattern table covers.

**T3-T7 deliverable:** one synthesizer per pg_catalog table, with
the corresponding pattern entries. Each synthesizer is ~50-100
LoC: build a RowDescription with canonical field names + OIDs,
build a DataRow per synthesized row, emit CommandComplete +
ReadyForQuery.

## 5. Schema synthesis — turn the live KesselDB catalog into pg_catalog rows

### 5.1 `pg_namespace` (3 canned rows)

```text
| oid  | nspname            | nspowner | nspacl |
|------|--------------------|----------|--------|
| 11   | pg_catalog         | 10       | NULL   |
| 2200 | public             | 10       | NULL   |
| 2202 | information_schema | 10       | NULL   |
```

OIDs 11 / 2200 / 2202 are the PG canonical OIDs (locked vs
`src/include/catalog/pg_namespace.dat`); KATs verify the locked
values. nspowner=10 is the canonical postgres-user OID. nspacl
NULL because V1 has no per-namespace ACLs.

### 5.2 `pg_class` (one row per KesselDB table)

```text
| oid       | relname    | relnamespace | reltype | relowner | relam | reltablespace | relpages | reltuples | relallvisible | relhasindex | relkind | relpersistence | …  |
|-----------|-----------|--------------|---------|----------|-------|---------------|----------|-----------|---------------|-------------|---------|----------------|----|
| <hash>    | <table>   | 2200         | 0       | 10       | 0     | 0             | 0        | 0         | 0             | <bool>      | 'r'     | 'p'            | …  |
```

- `oid` = stable hash of table name (deterministic; identical
  across replicas; survives restarts because it's a function of
  the name alone, not a counter).
- `relnamespace = 2200` (public — every KesselDB table).
- `reltype = 0` (V1 doesn't expose the composite-row type per
  pg_class).
- `relowner = 10` (postgres-user OID).
- `relkind = 'r'` (ordinary table; V1 doesn't expose views /
  materialized views — KesselDB has none anyway).
- `relhasindex` = true iff the table has ≥1 KesselDB index
  (gateway looks at the catalog).
- `relpages` / `reltuples` / `relallvisible` = 0 (V1 doesn't
  expose page counts or row estimates; tools that depend on
  query-cost estimates get a missing data sentinel).
- `relpersistence = 'p'` (permanent — KesselDB has no temporary
  tables).

The remaining 25 pg_class columns get the PG-standard defaults
(see `src/include/catalog/pg_class.h`); locked vs the canonical
list in a KAT.

### 5.3 `pg_attribute` (one row per (table × column))

```text
| attrelid     | attname  | atttypid | attstattarget | attlen | attnum | attnotnull | attisdropped | attidentity | …  |
|--------------|----------|----------|---------------|--------|--------|------------|--------------|-------------|----|
| <table-oid>  | <colname>| <oid>    | -1            | <size> | <i>    | <bool>     | false        | ''          | …  |
```

- `attrelid` = the table's pg_class.oid (stable hash of table
  name — joined-on by clients).
- `atttypid` = `field_kind_to_oid(kind)` from V1 T4.
- `attlen` = `type_size_for_oid(atttypid)` — int8=8, text=-1,
  etc.
- `attnum` = 1-based column index in declared order.
- `attnotnull` = `!nullable` from KesselDB column flag.
- `attidentity` = `''` (V1 doesn't expose IDENTITY columns).

Note that attnum=1 is the first user column (KesselDB has no
`oid` system column to put at attnum=0; we don't emit attnum<=0
"hidden system" rows that real PG would).

### 5.4 `pg_type` (~12 canned rows for V1 type OIDs)

The full PG pg_type catalog has 600+ rows; V1 emits the ~12 we
actually use:

```text
| oid  | typname     | typnamespace | typowner | typlen | typbyval | typtype | typcategory | …  |
|------|-------------|--------------|----------|--------|----------|---------|-------------|----|
| 16   | bool        | 11           | 10       | 1      | true     | 'b'     | 'B'         | …  |
| 17   | bytea       | 11           | 10       | -1     | false    | 'b'     | 'U'         | …  |
| 20   | int8        | 11           | 10       | 8      | true     | 'b'     | 'N'         | …  |
| 21   | int2        | 11           | 10       | 2      | true     | 'b'     | 'N'         | …  |
| 23   | int4        | 11           | 10       | 4      | true     | 'b'     | 'N'         | …  |
| 25   | text        | 11           | 10       | -1     | false    | 'b'     | 'S'         | …  |
| 26   | oid         | 11           | 10       | 4      | true     | 'b'     | 'N'         | …  |
| 1043 | varchar     | 11           | 10       | -1     | false    | 'b'     | 'S'         | …  |
| 1184 | timestamptz | 11           | 10       | 8      | true     | 'b'     | 'D'         | …  |
| 1700 | numeric     | 11           | 10       | -1     | false    | 'b'     | 'N'         | …  |
```

(Plus a few more — name=19 for `pg_class.relname`-style columns,
char=18 for `pg_class.relpersistence`-style columns, oidvector=30
for `pg_index.indkey`-style columns. Full list in T5.)

Values are canned from `src/include/catalog/pg_type.dat` (KAT-
locked); tools that read pg_type just need the columns to be
present and PG-style — they don't validate every flag.

### 5.5 `pg_index` (one row per KesselDB index)

```text
| indexrelid | indrelid    | indnatts | indnkeyatts | indisunique | indisprimary | indkey    | indoption | …  |
|------------|-------------|----------|-------------|-------------|--------------|-----------|-----------|----|
| <hash>     | <table-oid> | <n>      | <n>         | <bool>      | false        | <oidv>    | <0,0,…>   | …  |
```

- `indexrelid` = stable hash of index name (deterministic across
  replicas).
- `indrelid` = the indexed table's pg_class.oid.
- `indnatts` = number of indexed columns; `indnkeyatts` = same.
- `indisunique` = true for AddUnique-style indexes, false
  otherwise.
- `indkey` = packed int2vector of the column attnums (KesselDB
  composite indexes give us the field list directly).

### 5.6 `pg_constraint` (one row per UNIQUE / FOREIGN KEY / CHECK)

```text
| oid    | conname   | connamespace | contype | condeferrable | conrelid    | confrelid    | conkey     | confkey    | …  |
|--------|-----------|--------------|---------|---------------|-------------|--------------|------------|------------|----|
| <hash> | <name>    | 2200         | 'u'/'f'/'c' | false     | <table-oid> | <ref-oid>/0  | <attnums>  | <attnums>  | …  |
```

- `contype = 'u'` for UNIQUE, `'f'` for FOREIGN KEY, `'c'` for
  CHECK.
- `conrelid` = the constraint's host table pg_class.oid.
- `confrelid` = the foreign key's referenced table (0 for non-FK).
- `conkey` = constrained column attnums.
- `confkey` = referenced column attnums (NULL for non-FK).

KesselDB doesn't carry a constraint name today; V1 of this arc
synthesizes `<table>_<col>_key` for UNIQUE, `<table>_<col>_fkey`
for FOREIGN KEY, `<table>_check_N` for CHECK — matching PG's
auto-naming convention.

### 5.7 `information_schema.tables` (one row per KesselDB table)

```text
| table_catalog | table_schema | table_name | table_type |
|---------------|--------------|------------|------------|
| 'kesseldb'    | 'public'     | <name>     | 'BASE TABLE' |
```

### 5.8 `information_schema.columns` (one row per (table × column))

```text
| table_catalog | table_schema | table_name | column_name | ordinal_position | data_type      |
|---------------|--------------|------------|-------------|------------------|----------------|
| 'kesseldb'    | 'public'     | <name>     | <col>       | <i>              | 'bigint' / …   |
```

`data_type` is the PG-text type name (`bigint` for int8, `text`
for text, `boolean` for bool, etc.) — mapped via `oid_to_pg_text_name`,
a small helper paired with V1 T4's OID map.

## 6. SQL helper functions

The dispatcher recognizes a small set of SELECT-of-known-function
patterns and synthesizes a single-row, single-column response.

```text
SELECT version()
→ RowDescription [version: text]
  DataRow ["PostgreSQL 14.0 (KesselDB-1.0)"]
  CommandComplete "SELECT 1"
  ReadyForQuery 'I'
```

```text
SELECT current_database()
→ RowDescription [current_database: text]
  DataRow ["kesseldb"]
  CommandComplete "SELECT 1"
  ReadyForQuery 'I'
```

Same shape for `current_schema()`, `current_user`, `session_user`,
`pg_my_temp_schema()`, `pg_is_other_temp_schema(...)`,
`obj_description(...)`, `pg_get_constraintdef(...)`,
`pg_get_indexdef(...)`, `pg_table_is_visible(...)`,
`pg_encoding_to_char(...)`.

Pattern entries:
- `SELECT version()` (exact)
- `SELECT current_database()` (exact)
- `SELECT current_schema()` (exact)
- `SELECT pg_catalog.version()` (exact, qualified)
- `SHOW server_version` (exact — handled by emitting a one-row
  ParameterStatus-like response)
- `SHOW <guc_name>` (prefix — emit the canned GUC value or empty)

Multi-function queries like `SELECT version(), current_database()`
match a separate pattern that emits 2-column responses. V1 of this
arc covers the single-function shape + the most common 2-function
shape pgAdmin uses; tools issuing 3+ functions in one SELECT fall
through to `42P01` (rare; the function rewriter grows in V2 if
needed).

## 7. Task decomposition (T1..T8)

| T# | Scope | KAT delta (approx) | Real-wire ship? |
|---|---|---|---|
| **T1** | This design spec + scaffold (`pg_catalog/mod.rs` module declaration + `pg_catalog/synthesize.rs` with the `pg_namespace` synthesizer + `catalog_query_hook` dispatcher returning `Some` only for `SELECT * FROM pg_catalog.pg_namespace`; `dispatch.rs` integration hook before the existing `engine.apply_sql` path; 5-8 KATs locking spec invariants — hook returns `None` for non-pg_catalog SQL, returns `Some` for the pg_namespace query, pg_namespace synthesizer emits 3 canonical rows with the locked OIDs 11/2200/2202, RowDescription has the canonical 4 columns (oid, nspname, nspowner, nspacl), end-to-end byte-coherence test, case-insensitive matching invariant) | +5-10 | YES — psql `\dn` (list namespaces) returns the 3 schemas |
| **T2** | Query corpus capture — drive psql, pgcli, pgAdmin, DBeaver against a real Postgres + capture every introspection query they issue + write `crates/kessel-pg-gateway/src/pg_catalog/queries.md` with the corpus. Each query annotated with the issuing tool + the synthesizer slot it's destined for. T2 is documentation-only — no code change. Crucially, T2 reveals which V1-out-of-scope catalogs each tool actually queries; if pgAdmin queries pg_proc on EVERY connect (vs lazily on click), we may need to expand V1 scope. | +0 | NO — corpus only |
| **T3** | `EngineApply::list_tables() -> Vec<String>` trait extension + `EngineHandle` impl that walks the live `Catalog` returning the table names. Default impl returns empty Vec for back-compat (so SP-PG-CAT can land independently of any new engine work). Plus the `pg_class` synthesizer + dispatcher entries for the ~5 canonical `pg_class` query patterns pgAdmin/DBeaver issue (`SELECT oid, relname FROM pg_class WHERE relnamespace = 2200`, `SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r'`, etc.). | +12-18 | YES — psql `\dt` returns the KesselDB tables |
| **T4** | `pg_attribute` synthesizer + the corresponding dispatcher entries (~6 canonical patterns including the `attrelid = N` filter). Plus `pg_type` synthesizer + the ~3 canonical patterns (`SELECT oid, typname FROM pg_type` + the per-OID lookup form). KAT-locked OID values against `pg_type.dat`. | +10-15 | YES — psql `\d <table>` returns the column list with PG type names |
| **T5** | `pg_index` synthesizer + `pg_constraint` synthesizer + dispatcher entries. Includes the synthetic constraint naming (`<table>_<col>_key` etc.). | +6-10 | YES — psql `\d <table>` shows indexes and constraints |
| **T6** | `information_schema.tables` + `information_schema.columns` view synthesizers + dispatcher entries for the ~4 canonical patterns (Metabase, Tableau, Looker, Hex). | +6-10 | YES — Metabase / Tableau connection wizard completes |
| **T7** | SQL helper functions — version(), current_database(), current_schema(), current_user, pg_my_temp_schema(), pg_is_other_temp_schema, obj_description, pg_get_constraintdef, pg_get_indexdef, pg_table_is_visible, pg_encoding_to_char, plus the SHOW pattern for canned GUCs. Each is a separate dispatcher entry + a tiny synthesizer. | +12-16 | YES — pgAdmin connect wizard completes |
| **T8** | Real-client smoke — manual hand-test against psql `\dt` / `\d` / pgcli tab-completion / pgAdmin "Connect to PostgreSQL" wizard / DBeaver schema browser. Document gaps as named V2 follow-up slices. Update USAGE.md §9 to remove the "GUI admin tools don't work" line. Update STATUS.md row. | +0-4 | YES — the arc closes; SP-PG-CAT V1 complete |

Estimated SP-PG-CAT V1 total: **~50-80 KATs across 8 slices**.

Post-V1 (V2 — production GUI parity):

| T# | Scope | Estimate |
|---|---|---|
| **T9 (V2)** — pg_proc real function listing | ~1 slice |
| **T10 (V2)** — pg_database multi-database when KesselDB grows that | ~1 slice |
| **T11 (V2)** — per-query cache invalidated on DDL (huge-catalog optimization) | ~1 slice |
| **T12 (V2)** — pg_stat_* runtime stats stub→real | ~2 slices |
| **T13 (V2)** — collation / locale catalog (pg_collation real) | ~1 slice |
| **T14 (V2)** — psql `\d+` extended output (pg_description + pg_indexes detail) | ~1 slice |
| **T15 (V2)** — Cross-schema queries when KesselDB grows namespaces (SP-NS) | ~1 slice |

## 8. Acceptance criteria

SP-PG-CAT V1 (T1-T8) ships when:

1. **psql `\dt`** returns the list of KesselDB tables in `public`
   (not "did not find any relations" error).
2. **psql `\d <table_name>`** returns the table's column list with
   PG-style type names (e.g. `id | bigint | not null`).
3. **psql `\dn`** returns the 3 schemas (pg_catalog, public,
   information_schema).
4. **pgcli** connects + tab-completion works for KesselDB table
   names (pgcli runs `pg_class` queries on connect to populate
   its tab-completion cache).
5. **DBeaver "Connect to PostgreSQL"** wizard completes without
   error; the connection appears in the navigator tree with the
   KesselDB tables visible.
6. **pgAdmin 4 "Add Server"** wizard completes; the server
   appears in the browser tree with the tables visible under
   public schema. (Functions/triggers/extensions panels show
   empty — acceptable V1.)
7. **Metabase "Add Database"** wizard completes; the schema is
   discoverable.
8. **No regression** — all SP-PG V1 KATs continue to pass (the
   hook returns `None` for every existing test SQL).
9. **No engine changes that affect HTTP / WebSocket / native
   protocol surfaces** — `cargo tree -p kesseldb-server -e
   normal` unchanged; default `cargo build -p kesseldb-server`
   byte-identical.
10. **10+ pentest sweep** — pg_catalog queries that ask for
    nonexistent OIDs / unknown schemas / malformed parameters
    return clean empty result sets (no panic, no SchemaError
    leakage to client).

## 9. Self-review — weak spots of this design

1. **The query-pattern-match approach is brittle.** If pgAdmin
   ships a new version that rewords its introspection SQL, our
   pattern table misses and tools regress. Mitigations: (a) T2's
   corpus capture is the contract; we add a CI smoke that runs
   each captured query through `catalog_query_hook` and asserts
   the synthesizer fires; (b) we ship the pattern table sorted
   by source-tool, so a tool maintainer wanting to add a new
   pattern can do so in one PR; (c) the fall-through case
   (pattern miss → V1 `42P01`) is at least consistent with V1
   behavior — the regression is "tool worked, now doesn't" not
   "tool crashed". V2 follow-up: a structured AST-based matcher
   (parse the SQL via kessel-sql, walk for FROM pg_catalog.x
   shapes) replaces the regex layer.

2. **Synthesizing on every query is O(catalog-size) per query.**
   For <1000 tables this is fast (microseconds). For a workload
   with 10K tables, a `pg_class` enumeration query becomes a
   bottleneck. V1 of this arc ships the naive version because
   "first make it work"; V2 SP-PG-CAT-CACHE adds a per-query
   cache keyed on (catalog_version, query_pattern) — DDL bumps
   the catalog_version, invalidating the cache. Documented gap.

3. **The canned `pg_type` rows lie about a lot of PG-specific
   fields.** PG's `pg_type` has 30+ columns; V1 fills in the
   ~10 that tools actually read (oid, typname, typlen, typbyval,
   typtype, typcategory, typnamespace, typowner). The rest are
   PG-standard defaults. If a tool reads `typdelim` for parsing
   array-literal output, it gets `,` (PG default) even though
   KesselDB has no array support. If a tool reads `typalign` to
   compute binary-format padding, it gets the PG default for the
   underlying type — irrelevant for V1 because we don't ship
   binary format anyway. Documented gap; each missing column
   becomes a 1-line addition in T4 when a real tool needs it.

4. **V1 doesn't support arbitrary pg_catalog SQL (JOIN, GROUP
   BY, sub-SELECT).** A tool issuing `SELECT c.relname FROM
   pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
   WHERE n.nspname = 'public'` is a JOIN, not one of our
   recognized patterns; today it falls through to `42P01`.
   Mitigation: capture this JOIN as a pattern in T2, ship the
   synthesizer for the exact shape (join of pg_class +
   pg_namespace where nspname literal). The 30-50 patterns we
   ship in T1-T7 cover the queries the common tools issue;
   novel JOINs from new tools become new pattern entries.
   The honest limit is: we are NOT building a pg_catalog query
   planner. We are emulating the queries real tools issue.

5. **`version()` lying as PostgreSQL 14 carries product risk.**
   SP-PG V1 §11 weak-spot #11 names this; SP-PG-CAT inherits it.
   Tools that gate features on the version string fastpath
   (`PostgreSQL 14.x`) believe they're talking to PG 14 and may
   issue queries against catalogs/operators KesselDB doesn't
   support. The `(KesselDB-1.0)` suffix is the truth-leaking
   escape hatch; tools that parse the full string see it.
   Risk: a tool that gates "supports MERGE" on "PG ≥ 15" will
   not issue MERGE (KesselDB doesn't support it either — safe);
   a tool that gates "supports CREATE INDEX CONCURRENTLY" on
   "PG ≥ 9.6" will issue it (KesselDB doesn't support — fails
   gracefully with V1 SQLSTATE `0A000`). Documented.

6. **Single-database assumption may cause some tools to query
   `pg_database` and get a surprising empty result.** V1 of this
   arc returns ONE row in pg_database (`kesseldb`). A tool
   listing the available databases sees `kesseldb` and routes
   queries there. When KesselDB grows multi-database (no current
   plan), V2 expands pg_database. Documented.

7. **The synthesizer's stable-hash for OIDs collides for similar
   table names.** SHA-256(table_name)[..4] gives a 32-bit OID
   space; birthday paradox for collisions at ~65K tables. KAT
   asserts no collision in the V1 test corpus (~10 table names);
   real-deployment risk is low but real. V2 mitigation: a
   monotonic-counter OID assigner with a stable seed (deterministic
   across replicas via VSR-ordered DDL).

8. **`information_schema` is two separate schemas in
   pg_catalog — `pg_catalog.information_schema_*` views and the
   `information_schema.*` schema namespace.** The V1 of this
   arc handles both forms via the same dispatcher (a query for
   `information_schema.tables` routes to the same synthesizer
   as `pg_catalog.information_schema_tables`). Tools that
   distinguish the two (rare) may see surprising responses.

9. **No on-the-fly catalog change visibility.** The hook reads
   the live KesselDB catalog on every query, which means a
   `CREATE TABLE` immediately appears in subsequent
   pg_catalog/pg_class results. But a tool that caches its UI
   tree client-side won't refresh until the user clicks
   "refresh"; some tools (DBeaver in particular) cache for
   minutes. Out-of-scope for this arc (it's a client problem).

10. **The pattern table grows over time.** Every new GUI tool
    we observe issues new queries. V1 of this arc ships ~30-50
    patterns; over 6 months we'll grow to 100+. The risk is
    that the pattern table becomes a maintenance burden. V2
    SP-PG-CAT-AST shifts to AST-walking which collapses the
    pattern table into ~10 shape-recognizers.

11. **No telemetry on pattern-misses.** When a tool issues a
    pg_catalog query we don't recognize, we emit `42P01` and
    move on. The operator has no signal that "tool X is asking
    for catalog Y we don't support." V1 of this arc ships a
    `KESSELDB_PG_CAT_LOG_MISSES=1` env-var that logs each
    unrecognized pg_catalog SELECT to stderr — operators can
    grep their logs for misses and file pattern-table additions.
    Cheap; documented.

## 10. Open questions

- **Does pgAdmin 4 require `pg_authid` to be queryable for the
  connection to complete, or does it tolerate a `42P01` on that
  catalog?** T2 corpus capture will answer this. If REQUIRED, we
  add `pg_authid` to V1 scope (one extra slice for the empty-
  stub).
- **What's the canonical OID for the KesselDB "database"?** PG's
  default install has `template0=1`, `template1=11`, `postgres=13`.
  V1 of this arc uses oid=1 (`kesseldb`) — collides with PG's
  `template0`. Risk: a tool that asks "is this the template
  database?" via `pg_database WHERE oid = 1` gets a confused
  answer. Cheap mitigation: use oid=16384 (the first user OID
  in PG) for the KesselDB database. T1 picks one and locks it;
  T2 audits whether any captured query queries by oid=1.
- **Should `pg_proc` be a 1-row stub (so `current_setting()` is
  introspectable) or 0-row (zero pg_proc rows)?** T2 corpus
  capture will clarify. If pgAdmin requires at least 1 row for
  the function panel to render without error, 1-row stub it is.
- **Should we lock the canonical PG version string we lie about?**
  V1 of this arc uses `'PostgreSQL 14.0 (KesselDB-1.0)'` matching
  SP-PG V1's ParameterStatus.server_version. If we ever bump
  the PG-emulation target to 15 or 16, both surfaces must move
  together. Document the invariant in `lib.rs` doc.
- **Should the pattern table be sorted by frequency-of-use (cache-
  friendly) or by source-tool (maintenance-friendly)?** V1 picks
  source-tool; V2 SP-PG-CAT-CACHE can re-sort during build.

## 11. References

- SP-PG V1 design: `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`
- SP-PG V1 progress: `docs/superpowers/specs/2026-05-27-kesseldb-subproject-sppg-progress.md`
- `crates/kessel-pg-gateway/src/dispatch.rs` — the hook integration point
- `crates/kessel-pg-gateway/src/engine.rs` — the EngineApply trait
- `crates/kessel-pg-gateway/src/types.rs` — FieldKind ↔ PG OID map
- `crates/kessel-pg-gateway/src/response.rs` — RowDescription / DataRow / CommandComplete encoders the synthesizer reuses
- `crates/kessel-catalog/src/lib.rs` — KesselDB authoritative catalog
- PostgreSQL System Catalogs (chapter 51 of the PG docs) — the
  authoritative description of every pg_catalog table this arc
  emulates
- `src/include/catalog/pg_namespace.dat` — canonical OIDs for the
  three reserved schemas (11/2200/2202)
- `src/include/catalog/pg_class.h` — pg_class column list
- `src/include/catalog/pg_attribute.h` — pg_attribute column list
- `src/include/catalog/pg_type.dat` — canonical type-OID definitions
- `docs/USAGE.md §9` — the V1-boundary line this arc removes
