# SP-PG-CAT T2 — canonical pg_catalog / information_schema query corpus

This is the contract that the T3-T7 pattern dispatcher implements
against. Every query below is the verbatim SQL a real open-source
tool issues on connect or on an interactive command, captured from
the tool's public source:

- `psql` queries from `src/bin/psql/describe.c` in postgres/postgres
  (PG 14 branch — V1's emulation target). The `\dt`, `\d`, `\dn`,
  `\du`, `\dT`, `\db` describe-commands compile into these SELECTs
  via `listTables`, `describeOneTableDetails`, `listSchemas`,
  `describeRoles`, `listTypes`, `listTablespaces`.
- `pgcli` auto-completion queries from
  `pgcli/packages/pgcli/pgcli/packages/sqlcompletion.py` and
  `pgcli/packages/pgcli/pgcli/pgexecute.py` (helpers
  `tables`, `databases`, `schemata`, `functions`, `views`).
- DBeaver introspection queries from
  `dbeaver/plugins/org.jkiss.dbeaver.ext.postgresql/src/org/jkiss/
  dbeaver/ext/postgresql/model/PostgreSchema.java` +
  `PostgreTable.java`.
- pgJDBC `getTables` / `getColumns` / `getIndexInfo` from
  `pgjdbc/pgjdbc/src/main/java/org/postgresql/jdbc/
  PgDatabaseMetaData.java`.

Each query is annotated with:

- **Tool** — the issuing tool + the user action that triggers it.
- **Hits** — the catalog tables / functions the query reads,
  cross-referenced against the synthesizer slice (T#).
- **Pattern shape** — exact / prefix / regex / JOIN — informs the
  T3-T7 dispatcher entry style.
- **Scope flag** — V1-in-scope vs V1-out-of-scope (named V2
  follow-up).

Annotations follow the slice taxonomy in `2026-05-27-kesseldb-
sppgcat-pg-catalog-design.md §7`.

---

## 1. `psql` describe-commands (`src/bin/psql/describe.c`)

### 1.1 `\dn` — list schemas

```sql
SELECT n.nspname AS "Name",
       pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
FROM pg_catalog.pg_namespace n
WHERE n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
ORDER BY 1;
```

- **Tool:** `psql \dn`
- **Hits:** `pg_namespace` (T1 done), `pg_get_userbyid` (T7 helper).
- **Pattern shape:** JOIN with builtin function — V1 strategy:
  intercept the entire canonical query as a single canned synthesizer.
- **Scope:** V1 in scope. Returns only `public` (matches the WHERE
  clause filtering `pg_*` + `information_schema`).

### 1.2 `\dt` — list tables in current schema (the canonical headline query)

```sql
SELECT n.nspname as "Schema",
       c.relname as "Name",
       CASE c.relkind WHEN 'r' THEN 'table'
                      WHEN 'v' THEN 'view'
                      WHEN 'm' THEN 'materialized view'
                      WHEN 'i' THEN 'index'
                      WHEN 'S' THEN 'sequence'
                      WHEN 's' THEN 'special'
                      WHEN 't' THEN 'TOAST table'
                      WHEN 'f' THEN 'foreign table'
                      WHEN 'p' THEN 'partitioned table'
                      WHEN 'I' THEN 'partitioned index' END as "Type",
       pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
      AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2;
```

- **Tool:** `psql \dt`
- **Hits:** `pg_class` (T3), `pg_namespace` (T1 done),
  `pg_get_userbyid` (T7), `pg_table_is_visible` (T7).
- **Pattern shape:** JOIN — V1 strategy (A) per design §3.4: intercept
  the entire canonical query as a single synthesizer producing the
  joined-result rows directly. The synthesizer fills `Schema`=`public`,
  `Type`=`table`, `Owner`=`kesseldb` for every KesselDB table.
- **Scope:** V1 in scope. Headline acceptance criterion (spec §8 #1).

### 1.3 `\d` (no args) — list tables, views, sequences

Same as `\dt` but with `c.relkind IN ('r','p','v','m','S','f','')`.
Distinct pattern entry from §1.2 because the relkind set differs.

- **Tool:** `psql \d`
- **Hits:** same as §1.2
- **Pattern shape:** JOIN, canonical canned.
- **Scope:** V1 in scope.

### 1.4 `\d <name>` — describe a table (step 1 of 3)

```sql
SELECT c.oid,
       n.nspname,
       c.relname
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname OPERATOR(pg_catalog.~) '^(<name>)$' COLLATE pg_catalog.default
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 2, 3;
```

- **Tool:** `psql \d <name>` step 1 — resolves table name to oid
- **Hits:** `pg_class` (T3), `pg_namespace` (T1 done),
  `pg_table_is_visible` (T7).
- **Pattern shape:** parameterized — V1 strategy: regex-match the
  `OPERATOR(pg_catalog.~)` shape, extract the quoted `<name>`,
  synthesize a one-row result (or zero-row if the table doesn't
  exist in the live catalog).
- **Scope:** V1 in scope.

### 1.5 `\d <name>` — describe a table (step 2 of 3: column list)

```sql
SELECT a.attname,
  pg_catalog.format_type(a.atttypid, a.atttypmod),
  (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
   FROM pg_catalog.pg_attrdef d
   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
  a.attnotnull,
  (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
  a.attidentity,
  a.attgenerated
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = '<table_oid>' AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum;
```

- **Tool:** `psql \d <name>` step 2 — column list
- **Hits:** `pg_attribute` (T4), `pg_attrdef` (V1 out-of-scope —
  empty result; column defaults not displayed), `pg_collation`
  (V1: 1-row 'default' stub), `pg_type` (T4),
  `pg_get_expr`/`format_type` (T7 helpers).
- **Pattern shape:** parameterized — V1 strategy: regex-match the
  `attrelid = '<oid>'` shape, extract the OID, synthesize the
  per-column rows from `describe_table` of the matching KesselDB
  table.
- **Scope:** V1 in scope at the `pg_attribute` level. The subselects
  on `pg_attrdef` / `pg_collation` return NULL (V1 doesn't carry
  defaults or non-default collations).

### 1.6 `\d <name>` — describe a table (step 3 of 3: indexes)

```sql
SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered,
       i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
       pg_catalog.pg_get_constraintdef(con.oid, true),
       contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace
FROM pg_catalog.pg_class c, pg_catalog.pg_class c2,
     pg_catalog.pg_index i
  LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
WHERE c.oid = '<table_oid>' AND c.oid = i.indrelid AND i.indexrelid = c2.oid
ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname;
```

- **Tool:** `psql \d <name>` step 3 — indexes (if any)
- **Hits:** `pg_class` (T3), `pg_index` (T5), `pg_constraint` (T5),
  `pg_get_indexdef`/`pg_get_constraintdef` (T7 — V1 returns empty
  string).
- **Pattern shape:** JOIN — V1 strategy: regex-match the
  `c.oid = '<oid>'` shape, synthesize from the KesselDB index +
  constraint lists for the matching table.
- **Scope:** V1 in scope (T5).

### 1.7 `\dT` — list types

```sql
SELECT n.nspname as "Schema",
       pg_catalog.format_type(t.oid, NULL) AS "Name",
       pg_catalog.obj_description(t.oid, 'pg_type') as "Description"
FROM pg_catalog.pg_type t
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
  AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
  AND n.nspname <> 'pg_catalog' AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_type_is_visible(t.oid)
ORDER BY 1, 2;
```

- **Tool:** `psql \dT`
- **Hits:** `pg_type` (T4), `pg_namespace` (T1 done),
  `pg_class` (T3), `obj_description` (T7), `format_type` (T7),
  `pg_type_is_visible` (T7).
- **Pattern shape:** JOIN — V1 strategy: canned canonical match,
  but the WHERE clause excludes `pg_catalog` so V1's response is
  empty (KesselDB has no user-defined types yet).
- **Scope:** V1 in scope (empty result is correct).

### 1.8 `\du` — list roles

```sql
SELECT r.rolname, r.rolsuper, r.rolinherit,
  r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,
  r.rolconnlimit, r.rolvaliduntil,
  ARRAY(SELECT b.rolname
        FROM pg_catalog.pg_auth_members m
        JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
        WHERE m.member = r.oid) as memberof,
  r.rolreplication,
  r.rolbypassrls
FROM pg_catalog.pg_roles r
WHERE r.rolname !~ '^pg_'
ORDER BY 1;
```

- **Tool:** `psql \du`
- **Hits:** `pg_roles` (V1 out-of-scope — see §2.2; the V1 stub
  returns the single `kesseldb` role), `pg_auth_members` (V1 out
  of scope — empty).
- **Pattern shape:** JOIN with subquery + ARRAY — V1 strategy:
  canned canonical match returning one row (`kesseldb` superuser).
- **Scope:** Stub V1 — empty array for memberof. T7 ships the
  `pg_roles` synthesizer (1 canned row) as part of the helper-fn
  slice because it's a tiny stub.

### 1.9 `\db` — list tablespaces

```sql
SELECT spcname AS "Name",
  pg_catalog.pg_get_userbyid(spcowner) AS "Owner",
  pg_catalog.pg_tablespace_location(oid) AS "Location"
FROM pg_catalog.pg_tablespace
ORDER BY 1;
```

- **Tool:** `psql \db`
- **Hits:** `pg_tablespace` (V1 out-of-scope — empty stub),
  `pg_get_userbyid` (T7), `pg_tablespace_location` (V1 out of
  scope — empty stub).
- **Pattern shape:** simple — canned 1-row stub or 0-row response.
- **Scope:** V1 returns 0 rows (KesselDB has no tablespaces).
  Acceptable — psql shows "no tablespaces" cleanly.

---

## 2. `pgcli` auto-completion queries (`pgcli/pgexecute.py`)

### 2.1 `tables()` — list tables for tab completion

```sql
SELECT n.nspname schema_name, c.relname table_name
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = ANY('{r,p,f,v,m}') AND n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
ORDER BY 1, 2;
```

- **Tool:** `pgcli` connect (auto-completion cache fill)
- **Hits:** `pg_class` (T3), `pg_namespace` (T1 done).
- **Pattern shape:** JOIN — V1 strategy: canned canonical match;
  same JOIN shape as psql `\dt` but different SELECT projection,
  so a distinct pattern entry.
- **Scope:** V1 in scope.

### 2.2 `schemata()` — list schemas

```sql
SELECT nspname FROM pg_catalog.pg_namespace ORDER BY 1;
```

- **Tool:** `pgcli` connect
- **Hits:** `pg_namespace` (T1 done — `SELECT * FROM
  pg_catalog.pg_namespace` is the V1-shipping pattern; the
  projection-narrowed `SELECT nspname` is a NEW pattern T3 adds).
- **Pattern shape:** exact (after normalization).
- **Scope:** V1 in scope. Adds 1 new pattern entry.

### 2.3 `databases()` — list databases

```sql
SELECT datname FROM pg_catalog.pg_database;
```

- **Tool:** `pgcli` connect
- **Hits:** `pg_database` (V1 out-of-scope stub — returns 1 row
  `kesseldb`).
- **Pattern shape:** exact.
- **Scope:** V1 1-row stub (T7 ships).

### 2.4 `columns()` — list columns for a table

```sql
SELECT nsp.nspname schema_name, cls.relname table_name,
       att.attname column_name, att.atttypid::regtype::text type_name,
       att.atthasdef has_default, pg_get_expr(def.adbin, def.adrelid) default
FROM pg_catalog.pg_attribute att
INNER JOIN pg_catalog.pg_class cls ON att.attrelid = cls.oid
INNER JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
LEFT OUTER JOIN pg_catalog.pg_attrdef def
  ON def.adrelid = att.attrelid AND def.adnum = att.attnum
WHERE att.attnum > 0 AND nsp.nspname !~ '^pg_'
  AND nsp.nspname <> 'information_schema' AND NOT att.attisdropped
ORDER BY 1, 2, att.attnum;
```

- **Tool:** `pgcli` connect (column-level tab completion)
- **Hits:** `pg_attribute` (T4), `pg_class` (T3), `pg_namespace`
  (T1 done), `pg_attrdef` (V1 out-of-scope — empty subselect),
  `pg_get_expr` (V1 out-of-scope — returns NULL).
- **Pattern shape:** JOIN — V1 strategy: canned canonical match;
  T4 ships the synthesizer.
- **Scope:** V1 in scope.

### 2.5 `functions()` — list user functions

```sql
SELECT n.nspname schema_name, p.proname func_name,
       pg_catalog.pg_get_function_arguments(p.oid) arg_list,
       pg_catalog.pg_get_function_result(p.oid) result,
       p.proretset is_aggregate, false is_window, p.proretset is_set_returning
FROM pg_catalog.pg_proc p
INNER JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
ORDER BY 1, 2;
```

- **Tool:** `pgcli` connect
- **Hits:** `pg_proc` (V1 out-of-scope — empty stub),
  `pg_get_function_arguments`/`pg_get_function_result` (V1 out
  of scope — empty stub).
- **Pattern shape:** JOIN — V1 strategy: 0-row response.
- **Scope:** V1 returns 0 rows. Acceptable — pgcli's tab completion
  for function names will be empty.

---

## 3. DBeaver introspection queries (PostgreSchema.java / PostgreTable.java)

### 3.1 Schema list (DBeaver "PostgreSchemaCache")

```sql
SELECT n.oid, n.* FROM pg_catalog.pg_namespace n ORDER BY nspname;
```

- **Tool:** DBeaver Connect-to-PostgreSQL wizard (left-tree schema list)
- **Hits:** `pg_namespace` (T1 done).
- **Pattern shape:** prefix `SELECT n.oid, n.* FROM
  pg_catalog.pg_namespace n`. Note this is distinct from T1's
  `SELECT * FROM pg_catalog.pg_namespace` because of the projection
  shape — T3 adds a pattern entry.
- **Scope:** V1 in scope.

### 3.2 Tables list (DBeaver "PostgreTableCache")

```sql
SELECT c.oid, c.*, d.description
FROM pg_catalog.pg_class c
LEFT OUTER JOIN pg_catalog.pg_description d
  ON d.objoid = c.oid AND d.objsubid = 0 AND d.classoid = 'pg_class'::regclass
WHERE c.relnamespace = <schema_oid>
  AND c.relkind in ('r','v','m','S','f','p')
ORDER BY c.oid;
```

- **Tool:** DBeaver schema-tree expand
- **Hits:** `pg_class` (T3), `pg_description` (V1 out-of-scope —
  empty LEFT JOIN).
- **Pattern shape:** JOIN with parameterized schema OID — V1
  strategy: regex-match the `c.relnamespace = <oid>` shape,
  extract OID, return all tables (V1 = all KesselDB tables are
  in `public`=2200; any other OID returns 0 rows).
- **Scope:** V1 in scope.

### 3.3 Columns list (DBeaver "PostgreTableColumnCache")

```sql
SELECT a.attname, a.atttypid, a.attlen, a.attnum, a.attkind,
       a.atttypmod, a.attnotnull, a.atthasdef, a.attisdropped,
       a.attidentity, a.attgenerated, a.attstorage, a.attcompression,
       a.attstattarget, d.description AS attdescr,
       pg_get_expr(ad.adbin, ad.adrelid) AS def_value
FROM pg_catalog.pg_attribute a
LEFT OUTER JOIN pg_catalog.pg_attrdef ad ON (a.attrelid = ad.adrelid AND a.attnum = ad.adnum)
LEFT OUTER JOIN pg_catalog.pg_description d ON (d.objoid = a.attrelid AND d.objsubid = a.attnum AND d.classoid = 'pg_class'::regclass)
WHERE a.attrelid = <table_oid>
ORDER BY a.attnum;
```

- **Tool:** DBeaver expand-table
- **Hits:** `pg_attribute` (T4), `pg_attrdef` (V1 stub — empty),
  `pg_description` (V1 stub — empty).
- **Pattern shape:** JOIN with parameterized table OID — V1
  strategy: regex-match the `a.attrelid = <oid>` shape, extract
  OID, return per-column rows from the matching KesselDB table.
- **Scope:** V1 in scope.

---

## 4. JDBC driver (pgJDBC `PgDatabaseMetaData`)

### 4.1 `getTables` (used by every tool that uses pgJDBC)

```sql
SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,
       CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'
            WHEN true THEN
              CASE WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN
                CASE c.relkind WHEN 'r' THEN 'SYSTEM TABLE' WHEN 'v' THEN 'SYSTEM VIEW' WHEN 'i' THEN 'SYSTEM INDEX' ELSE NULL END
              WHEN n.nspname = 'pg_toast' THEN
                CASE c.relkind WHEN 'r' THEN 'SYSTEM TOAST TABLE' WHEN 'i' THEN 'SYSTEM TOAST INDEX' ELSE NULL END
              ELSE
                CASE c.relkind WHEN 'r' THEN 'TEMPORARY TABLE' WHEN 'p' THEN 'TEMPORARY TABLE' WHEN 'i' THEN 'TEMPORARY INDEX' WHEN 'S' THEN 'TEMPORARY SEQUENCE' WHEN 'v' THEN 'TEMPORARY VIEW' ELSE NULL END
              END
            WHEN false THEN
              CASE c.relkind WHEN 'r' THEN 'TABLE' WHEN 'p' THEN 'PARTITIONED TABLE' WHEN 'i' THEN 'INDEX' WHEN 'P' THEN 'PARTITIONED INDEX' WHEN 'S' THEN 'SEQUENCE' WHEN 'v' THEN 'VIEW' WHEN 'c' THEN 'TYPE' WHEN 'f' THEN 'FOREIGN TABLE' WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE NULL END
            ELSE NULL
        END AS TABLE_TYPE,
       d.description AS REMARKS,
       '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME,
       '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION
FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c
  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0 AND d.classoid = 'pg_class'::regclass)
WHERE c.relnamespace = n.oid AND c.relkind IN ('r','p') AND n.nspname LIKE 'public'
ORDER BY TABLE_TYPE, TABLE_SCHEM, TABLE_NAME;
```

- **Tool:** every JDBC-driven tool (DataGrip, JetBrains IDEs,
  Tableau JDBC, Looker JDBC, dbt-postgres via Java, sqlmesh,
  schemaspy, Datadog `dbm`, …)
- **Hits:** `pg_namespace` (T1 done), `pg_class` (T3),
  `pg_description` (V1 stub — empty LEFT JOIN).
- **Pattern shape:** large JOIN with CASE — V1 strategy: canned
  canonical match (the entire query body), synthesize the
  joined result directly. The pgJDBC query is stable across
  driver versions (last shape change: pgjdbc 42.2.0, 2019).
- **Scope:** V1 in scope. Critical because most BI tools route
  through pgJDBC.

### 4.2 `getColumns`

```sql
SELECT * FROM (
  SELECT n.nspname,c.relname,a.attname,a.atttypid,a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod,a.attlen,t.typtypmod,
  row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum,
  nullif(a.attidentity, '') as attidentity,nullif(a.attgenerated, '') as attgenerated,pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,t.typbasetype,t.typtype
  FROM pg_catalog.pg_namespace n
  JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
  JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)
  JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
  LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)
  LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)
  LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')
  LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog')
  WHERE c.relkind in ('r','p','v','f','m') and a.attnum > 0 AND NOT a.attisdropped AND n.nspname LIKE 'public' AND c.relname LIKE '<table>'
) c WHERE true ORDER BY nspname,c.relname,attnum;
```

- **Tool:** every JDBC-driven tool, expanding a table to see columns
- **Hits:** `pg_namespace` (T1), `pg_class` (T3), `pg_attribute`
  (T4), `pg_type` (T4), `pg_attrdef` (V1 stub), `pg_description`
  (V1 stub).
- **Pattern shape:** large JOIN — V1 strategy: canned canonical
  match with parameterized table-name extraction (`c.relname LIKE
  '<table>'`).
- **Scope:** V1 in scope.

### 4.3 `getIndexInfo`

```sql
SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, ct.relname AS TABLE_NAME,
       NOT i.indisunique AS NON_UNIQUE, NULL AS INDEX_QUALIFIER, ci.relname AS INDEX_NAME,
       CASE i.indisclustered WHEN true THEN 1 ELSE CASE am.amname WHEN 'hash' THEN 2 ELSE 3 END END AS TYPE,
       (information_schema._pg_expandarray(i.indkey)).n AS ORDINAL_POSITION,
       trim(both '"' from pg_catalog.pg_get_indexdef(ci.oid, (information_schema._pg_expandarray(i.indkey)).n, false)) AS COLUMN_NAME,
       NULL AS ASC_OR_DESC, ci.reltuples AS CARDINALITY, ci.relpages AS PAGES,
       pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION
FROM pg_catalog.pg_class ct
JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)
JOIN pg_catalog.pg_index i ON (ct.oid = i.indrelid)
JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)
JOIN pg_catalog.pg_am am ON (ci.relam = am.oid)
WHERE true AND n.nspname = 'public' AND ct.relname = '<table>'
ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION;
```

- **Tool:** JDBC-driven tools, expanding a table
- **Hits:** `pg_class` (T3), `pg_namespace` (T1), `pg_index`
  (T5), `pg_am` (V1 stub — empty join returns no rows since V1
  emits no `pg_am`; consequence: getIndexInfo returns 0 rows).
- **Pattern shape:** JOIN — V1 strategy: 0-row response (V1
  doesn't emit pg_am so the JOIN excludes everything).
- **Scope:** V1 returns 0 rows. Tools that depend on index info
  see no indexes — acceptable V1 (tools degrade gracefully).

---

## 5. `information_schema` queries (Metabase / Tableau / Looker / Hex)

### 5.1 `information_schema.tables` — Metabase / Tableau

```sql
SELECT table_catalog, table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name;
```

- **Tool:** Metabase / Tableau / Looker / Hex / Superset connect-database
- **Hits:** `information_schema.tables` (T6).
- **Pattern shape:** exact-prefix — V1 strategy: T6 ships the
  synthesizer.
- **Scope:** V1 in scope (T6).

### 5.2 `information_schema.columns` — Metabase / Tableau

```sql
SELECT table_catalog, table_schema, table_name, column_name,
       ordinal_position, data_type, character_maximum_length,
       numeric_precision, numeric_scale, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = '<table>'
ORDER BY ordinal_position;
```

- **Tool:** Metabase / Tableau column introspection
- **Hits:** `information_schema.columns` (T6).
- **Pattern shape:** prefix with parameterized table-name —
  V1 strategy: T6 ships the synthesizer.
- **Scope:** V1 in scope (T6).

### 5.3 `information_schema.schemata`

```sql
SELECT schema_name FROM information_schema.schemata
WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
ORDER BY schema_name;
```

- **Tool:** Metabase / Tableau / Looker / Hex / dbt-postgres
- **Hits:** `information_schema.schemata` (T6).
- **Pattern shape:** exact-prefix.
- **Scope:** V1 in scope (T6, 1-row response: `public`).

---

## 6. SQL helper functions (T7)

### 6.1 `version()` — every tool issues this

```sql
SELECT version();
```

- **Hits:** `version()` builtin (T7).
- **Pattern shape:** exact (after normalization).
- **Scope:** V1 in scope.

### 6.2 `current_database()`, `current_schema()`, `current_user`

```sql
SELECT current_database();
SELECT current_schema();
SELECT current_user;
SELECT session_user;
```

- **Hits:** T7 helper synthesizers.
- **Pattern shape:** exact.
- **Scope:** V1 in scope.

### 6.3 pgAdmin connect-probe multi-function

```sql
SELECT version(), current_database(), current_user, current_schema();
```

- **Hits:** T7 — multi-function shape.
- **Pattern shape:** exact (after normalization).
- **Scope:** V1 in scope.

### 6.4 `SHOW <guc>` — server_version, server_encoding, etc.

```sql
SHOW server_version;
SHOW server_encoding;
SHOW client_encoding;
SHOW TimeZone;
SHOW DateStyle;
SHOW standard_conforming_strings;
SHOW integer_datetimes;
SHOW application_name;
```

- **Hits:** T7 — `SHOW` prefix synthesizer returning canned GUCs.
- **Pattern shape:** prefix `show ` after normalization.
- **Scope:** V1 in scope. Values match the V1 ParameterStatus
  emit (spec §3.2 of SP-PG V1).

### 6.5 `pg_get_userbyid(oid)` — psql `\dn`, `\dt` use

```sql
SELECT pg_catalog.pg_get_userbyid(10);
```

- **Hits:** T7 — returns `'kesseldb'` for any input OID (V1 has
  one user identity).
- **Pattern shape:** prefix `select pg_catalog.pg_get_userbyid(`
  or `select pg_get_userbyid(`.
- **Scope:** V1 in scope.

### 6.6 `pg_table_is_visible(oid)`

```sql
SELECT pg_catalog.pg_table_is_visible(<oid>);
```

- **Hits:** T7 — returns `t` (V1 has one schema, all tables
  visible).
- **Pattern shape:** prefix.
- **Scope:** V1 in scope.

---

## 7. V1-out-of-scope queries observed in tools (graceful 42P01 acceptable)

These appear in the captured corpus but are deliberately not
recognized by V1; each falls through to the existing
`42P01 undefined_table` error. Documented so future V2 slices
have a known pickup list (each is one pattern + one synthesizer
in a future T slice).

- `pg_settings` (pgAdmin connect — opens "Variables" panel) — V2
  SP-PG-CAT-GUC.
- `pg_stat_database`, `pg_stat_user_tables` (Datadog,
  prometheus-postgres-exporter) — V2 SP-PG-CAT-STATS.
- `pg_locks` (pgAdmin "Locks" panel) — V2 SP-PG-CAT-STATS.
- `pg_collation` (DBeaver collation picker) — V2.
- `pg_proc` (pgAdmin "Functions" panel) — V2 SP-PG-CAT-PROC.
- `pg_authid` (pgAdmin "Login/Group Roles" panel) — V2.
- `pg_extension`, `pg_available_extensions` (pgAdmin "Extensions"
  panel) — V2.
- `pg_event_trigger` (pgAdmin "Event Triggers" panel) — V2.
- `pg_publication`, `pg_subscription` (pgAdmin "Publications"
  panel) — V2.

---

## 8. Pattern-table sizing summary

| Slice | New patterns | Headline queries |
|---|---|---|
| T1 (DONE) | 1 (`SELECT * FROM pg_catalog.pg_namespace`) | psql `\dn` partial |
| T3 (this session) | 4 (psql `\dt`, psql `\d` no-args, pgcli `tables()`, DBeaver schema-list + tables-list) | psql `\dt` full |
| T4 | 6 (psql `\d <t>` cols, pgcli `columns()`, DBeaver `columns()`, JDBC `getColumns`, `\dT`, per-OID pg_type lookup) | psql `\d <t>` |
| T5 | 3 (`\d <t>` index step, JDBC `getIndexInfo`, JDBC `getPrimaryKeys`) | psql `\d <t>` indexes |
| T6 | 5 (info_schema.tables, info_schema.columns, info_schema.schemata, info_schema.key_column_usage, info_schema.table_constraints) | Metabase wizard |
| T7 | 10 (version, current_database, current_schema, current_user, session_user, multi-function probe, SHOW prefix, pg_get_userbyid, pg_table_is_visible, pg_get_indexdef) | pgAdmin wizard |
| **Total V1** | **~29 pattern entries** | psql `\dt`/`\d`/`\dn` + pgcli + DBeaver + pgAdmin |

Each pattern entry is ~5 lines (predicate + synthesizer call).
The growing pattern table is the **contract** between V1 of this
arc and the GUI ecosystem — every entry is locked by a KAT in the
relevant T slice.

---

## 9. Capture methodology (for adding new patterns)

When a new GUI tool is observed, capture its query corpus via:

1. Start a real PostgreSQL 14 with `log_statement = 'all'` in
   `postgresql.conf` and `log_destination = 'stderr'` +
   `logging_collector = on`.
2. Drive the tool through its connect wizard + schema-tree expand
   + expand-table + interactive query flow.
3. Copy the issued queries from `pg_log/postgresql-*.log` (filter
   out the housekeeping `SET …` statements; what we care about is
   the SELECTs against `pg_catalog.*` and `information_schema.*`).
4. Cross-reference against the tool's source code to confirm we
   captured the full sweep (some queries are issued lazily on
   user action — easy to miss if not all UI surfaces are clicked).
5. Add the captured query to this file, annotated per the §1-§6
   shape, with `Tool` / `Hits` / `Pattern shape` / `Scope flag`.
6. Add a pattern-table entry + synthesizer in the appropriate T
   slice (T3 for pg_class shape, T4 for pg_attribute/pg_type
   shape, etc.).

The capture is one-way: once a query is in this file, the
synthesizer + KAT lock its V1 behavior. Patches that change
synthesizer output must update this file + the relevant KAT.
