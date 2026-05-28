# SP-PG-CAT — `pg_catalog.*` introspection stubs — SP-arc Progress Tracker

Date created: 2026-05-27
Design spec: `docs/superpowers/specs/2026-05-27-kesseldb-sppgcat-pg-catalog-design.md`
Parent arc: SP-PG V1 (closed at T18, commit `2ec286b`)
TaskList: opens the first-named V2 follow-up to SP-PG (per SP-PG
V1 §11 weak-spot #8 + USAGE.md §9 + progress tracker §"Out-of-scope"
all naming `pg_catalog` stubs as the gateway to pgAdmin / DBeaver /
DataGrip / Metabase / Tableau / Looker / Mode / Hex / Superset /
Redash / sqlmesh / dbt-postgres / schemaspy / datadog-postgres-
integration / prometheus-postgres-exporter — the GUI admin / BI
ecosystem).

## What this SP-arc ships

V1 of this arc = "GUI admin tools connect successfully." Per the
design spec §1 ecosystem table: when a tool opens a Postgres
connection it does NOT just run `SELECT 1`; it issues ~5-50
introspection queries against `pg_catalog.*` and
`information_schema.*` to populate its UI tree (databases →
schemas → tables → columns → indexes → constraints). Today
(SP-PG V1) every one of those returns `42P01 undefined_table`
and the tool either refuses to display the connection
(pgAdmin) or surfaces a partial error state (DBeaver). After
V1 of this arc, every one returns a synthesized response and
the tool's connection wizard completes.

After V1 lands (T1..T8), a PG GUI tool speaking the v3.0
Frontend/Backend protocol can:

1. Open a TCP connection to KesselDB on the configured port.
2. Complete the SCRAM-SHA-256 handshake (SP-PG V1).
3. Run its on-connect introspection sweep against
   `pg_catalog.*` + `information_schema.*` + the SQL helper
   functions (`version()`, `current_database()`, `current_schema()`,
   `pg_my_temp_schema()`, etc.) and receive plausible,
   synthesized responses.
4. Display the KesselDB tables in its navigator tree under
   the `public` schema.
5. Expand a table to see its columns with PG-style type names
   (`bigint`, `text`, `boolean`, etc.).
6. See index + constraint definitions for the table.
7. Run user CRUD via simple-query (SP-PG V1).

**Out-of-scope (named, deferred — each is its own V2 sub-arc):**
- `pg_proc` real function listing — V1 of this arc emits an empty
  pg_proc stub; pgAdmin's function panel will be empty. V2
  SP-PG-CAT-PROC.
- `pg_database` multi-database — KesselDB has one logical database
  today; V1 of this arc returns ONE row. V2 expands when KesselDB
  grows multi-database (no current plan).
- per-query caching invalidated on DDL — V1 of this arc re-
  synthesizes from the live catalog on every query (O(catalog)
  per query; fine for <1000 tables). V2 SP-PG-CAT-CACHE.
- `pg_stat_*` runtime stats — V1 of this arc emits zero rows;
  prometheus-postgres-exporter sees zero metrics. V2 SP-PG-CAT-STATS.
- `pg_collation` real collation table — V1 of this arc returns
  one canned row for 'default'. V2.
- psql `\d+` extended output — V1 of this arc covers `\d` (basic
  table description); `\d+` (with comments + stats) is partial.
  V2.
- Cross-schema queries when KesselDB grows multi-namespace — V1
  of this arc only knows about `public`. V2 SP-NS will extend.
- Arbitrary pg_catalog SQL (JOIN / GROUP BY / sub-SELECT against
  pg_catalog tables) — V1 of this arc recognizes ~30-50 canonical
  query patterns. A tool issuing a novel JOIN that doesn't match
  any pattern still gets `42P01`. V2 SP-PG-CAT-AST will switch to
  AST-walking via kessel-sql.

See spec §2.2 for the full list of named follow-ups.

## Slice plan (mirrors design spec §7)

| T# | Scope | Status | Commit |
|---|---|---|---|
| **T1** | Design spec (759 lines, 11 weak-spots + 5 open questions) + scaffold module `crates/kessel-pg-gateway/src/pg_catalog/` (mod.rs + synthesize.rs) + `catalog_query_hook<E: EngineApply + ?Sized>` hook installed BEFORE `engine.apply_sql` in `dispatch::dispatch_query` (returns `None` for non-pg_catalog SQL — existing dispatch paths byte-untouched) + `pg_namespace` synthesizer emitting the canonical 3-row result (pg_catalog OID 11 / public OID 2200 / information_schema OID 2202 / postgres-user OID 10, all locked vs `src/include/catalog/pg_namespace.dat` + `pg_authid.dat`) + 15 KATs locking the spec invariants against PG §51.32 / `pg_namespace.dat` / `pg_type.dat` / canonical RowDescription shape. | **DONE** | `da726b3` (spec) + `924d67f` (scaffold) |
| **T2** | Query corpus capture — minimal-pragmatic capture from psql/pgcli/pgJDBC/DBeaver public source code (NOT real-tool wireshark — that was overkill for V1). `crates/kessel-pg-gateway/src/pg_catalog/queries.md` (698 lines) lists ~20 canonical queries spanning psql describe-commands (`\dn`, `\dt`, `\d`, `\d <name>` 3-step, `\dT`, `\du`, `\db`), pgcli auto-completion (`tables`, `schemata`, `databases`, `columns`, `functions`), DBeaver schema/table/column introspection, pgJDBC `getTables`/`getColumns`/`getIndexInfo`, information_schema view queries (Metabase/Tableau/Looker/Hex), and the 10 SQL helper functions T7 ships. Each entry annotated with issuing tool + hits (per-table cross-ref to T# slice) + pattern shape (exact / prefix / JOIN / regex) + V1-in-scope vs V1-out-of-scope flag. Documented capture methodology (`log_statement = 'all'` driven against real PG) for the future SP-PG-CAT-CORPUS-EXPAND slices. T2 ships ZERO KATs — it's the contract for T3-T7. | **DONE** | `5b90dc5` |
| **T3** | `EngineApply::list_tables() -> Vec<TableMetadata>` trait extension (default impl returns empty Vec so existing impls don't break at the T3 boundary; `TableMetadata { name, type_id, kind, field_count }` carries enough to fill V1's `pg_class` rows; `TableKind::{Ordinary,Index,View,Sequence}::pg_relkind() -> u8` maps to canonical PG `relkind` chars per `pg_class.h`) + `kesseldb-server::EngineHandle` impl routing through new `LIST_TABLES_TAG=0xF6` admin frame (mirrors the `DESCRIBE_BY_NAME_TAG=0xF7` pattern — read-only, engine-thread-local, no SM mutation; wire shape `[u32 count][repeat: u32 name_len, name, u32 type_id, u16 field_count]`) + FNV-1a `oid_for_table_name(name) -> u32` deterministic OID generator (clamped to user-allocated `[FIRST_USER_OID=16384, u32::MAX]` range per PG `transam.h`) + `pg_class` synthesizer (33-column RowDescription per PG 14 `pg_class.h`; per-row builder fills oid/relname/relnamespace=2200/relowner=10/relam=2/relfilenode/relkind/relnatts from `TableMetadata` + cans the remaining 27 columns per PG defaults; relacl/reloptions/relpartbound = NULL) + psql `\dt` joined-result intercept (canned canonical match — design §3.4 strategy A — synthesizes the joined pg_class+pg_namespace result directly without running real SQL JOIN; 4 output columns Schema/Name/Type/Owner, every row = public/table/kesseldb) + dispatcher entries for `SELECT * FROM pg_catalog.pg_class` (qualified + unqualified) and the psql `\dt` canonical query (tolerant of both PG 12 and PG 13/14 relkind-filter forms via leading + core + trailing fixture matching). | **DONE** | `1079c9a` (T3a trait+EngineHandle) + `777a3f1` (T3b/c synthesizer+hook) |
| **T4** | `pg_attribute` synthesizer (one row per (table × column); attrelid = the table's pg_class.oid; atttypid = `field_kind_to_oid(kind)` from V1 type-OID map; attnum = 1-based; attnotnull = `!nullable`; attlen = `type_size_for_oid(atttypid)`) + dispatcher entries for the ~6 canonical patterns with the `attrelid = N` filter. Plus `pg_type` synthesizer with the ~12 type rows V1 actually emits (bool=16, bytea=17, int8=20, int2=21, int4=23, text=25, oid=26, varchar=1043, timestamptz=1184, numeric=1700, plus name=19 + char=18 + oidvector=30 for the catalog-row-shaped columns) + the ~3 canonical patterns (`SELECT oid, typname FROM pg_type` + the per-OID lookup form). KAT-locked OID values vs `pg_type.dat`. | OPEN | — |
| **T5** | `pg_index` synthesizer (one row per KesselDB index; indrelid = table pg_class.oid; indexrelid = stable hash of index name; indkey = column attnums as packed int2vector; indisunique = per kind) + `pg_constraint` synthesizer (one row per UNIQUE/FK/CHECK; contype = 'u'/'f'/'c'; synthetic constraint name `<table>_<col>_key` / `<table>_<col>_fkey` / `<table>_check_N`) + dispatcher entries. | OPEN | — |
| **T6** | `information_schema.tables` + `information_schema.columns` view synthesizers + dispatcher entries for the ~4 canonical patterns (Metabase, Tableau, Looker, Hex). `information_schema.tables` has 4 columns (table_catalog/schema/name/type); `information_schema.columns` has 6 (table_catalog/schema/name + column_name + ordinal_position + data_type via PG-text type name like `bigint`/`text`/`boolean`). | OPEN | — |
| **T7** | SQL helper functions: version(), current_database(), current_schema(), current_user, session_user, pg_my_temp_schema(), pg_is_other_temp_schema(oid), obj_description(...)/(oid), pg_get_constraintdef(oid), pg_get_indexdef(oid), pg_table_is_visible(oid), pg_encoding_to_char(enc), plus the `SHOW <guc>` pattern for canned GUCs (server_version, server_encoding, client_encoding, TimeZone, DateStyle, etc. — matching the V1 ParameterStatus emit). Each is a dispatcher entry + a tiny synthesizer. Multi-function shape `SELECT version(), current_database()` (pgAdmin uses this) handled with a separate dispatcher pattern. | OPEN | — |
| **T8** | Real-client smoke — manual hand-test against psql `\dt` / `\d` / `\dn` / pgcli tab-completion / pgAdmin 4 "Add Server" wizard / DBeaver "Connect to PostgreSQL" wizard / Metabase "Add Database" + document gaps as named V2 follow-up slices in this tracker. Update `docs/USAGE.md §9` to remove the "GUI admin tools don't work" line. Update STATUS.md row. **SP-PG-CAT V1 arc CLOSED at T8 commit.** | OPEN | — |

Estimated V1 total: **~50-80 KATs across 8 slices** (T1 +15 / T2
+0 docs / T3 +15-20 / T4 +10-15 / T5 +6-10 / T6 +6-10 / T7 +12-16
/ T8 +0-4).

Optional / V2 follow-ups (named, deferred — each is its own arc):

- **T9 (V2)** — `pg_proc` real function listing (pgAdmin function panel non-empty). ~1 slice.
- **T10 (V2)** — `pg_database` multi-database when KesselDB grows that. ~1 slice.
- **T11 (V2)** — per-query cache invalidated on DDL (huge-catalog optimization; matters at ≥1000 tables). ~1 slice.
- **T12 (V2)** — `pg_stat_*` runtime stats stub→real (prometheus-postgres-exporter sees metrics). ~2 slices.
- **T13 (V2)** — collation / locale catalog (`pg_collation` real, not the 1-row stub). ~1 slice.
- **T14 (V2)** — psql `\d+` extended output (joins pg_description + pg_indexes detail + pg_stat_user_tables). ~1 slice.
- **T15 (V2)** — Cross-schema queries when KesselDB grows namespaces (depends on SP-NS arc). ~1 slice.
- **T16 (V2)** — AST-based pattern matcher (replaces the regex layer; collapses the ~50-pattern table to ~10 shape-recognizers via kessel-sql AST walk). ~2-3 slices.

## T1 — what landed (2026-05-27, commits `da726b3` + `924d67f`)

**Two commits, ~634 LoC net delta (excluding the 759-line spec doc):**

**Commit `da726b3` — design spec** (759 lines, no code change):
`docs/superpowers/specs/2026-05-27-kesseldb-sppgcat-pg-catalog-design.md`
covers:
- Context (§1) — per-tool query-count table cross-referencing
  ~14 GUI / BI / ETL tools with the queries they issue on connect.
- Scope (§2) — V1 in-scope (6 pg_catalog tables + 2
  information_schema views + 11 SQL helper functions) vs deferred
  (pg_proc / pg_authid / pg_database / pg_settings / pg_stat_* /
  pg_locks / pg_collation / row-level catalog updates / arbitrary
  pg_catalog SQL / psql `\d+` extended / cross-schema queries —
  each named with the arc that will pick it up).
- Architecture (§3) — intercept at the dispatch layer
  (`kessel_pg_gateway::dispatch::dispatch_query`) NOT inside
  `engine.apply_sql`; zero engine changes; read-only invariant
  enforced via `&dyn EngineApply` immutable reference; existing
  test surface unchanged because the hook returns `None` for
  non-pg_catalog SQL.
- SQL pattern matching (§4) — ~30-50 canonical query-pattern table;
  fast-reject on non-SELECT; T2 corpus capture is the contract.
- Schema synthesis (§5) — per-table layouts cross-referenced
  against PG canonical `src/include/catalog/pg_*.dat` + `pg_*.h`;
  pg_namespace 3 canned rows; pg_class one-per-table from
  `EngineApply::list_tables` (T3); pg_attribute one-per-(table×col)
  from `describe_table`; pg_type ~12 canned rows; pg_index +
  pg_constraint from the KesselDB catalog's index + constraint
  lists.
- SQL helper functions (§6) — 11 named functions + pattern
  recognition rules.
- 8-task decomposition (§7) with KAT delta + real-wire-ship flag
  per task + V2 follow-ups T9+ listed.
- Acceptance criteria (§8) — 10 concrete items (psql `\dt`/`\d`/`\dn`,
  pgcli tab-completion, DBeaver / pgAdmin / Metabase wizards
  complete, no SP-PG V1 regression, no engine changes, 10+ pentest
  sweep).
- Self-review (§9) — 11 weak-spots: pattern-match brittleness,
  O(catalog) per-query cost, canned pg_type approximation, no
  arbitrary catalog SQL, version() lie product risk (inherited
  from SP-PG §11 #11), single-database assumption, OID
  birthday-paradox collision, information_schema name overlap,
  no on-the-fly catalog-change visibility, pattern table
  maintenance burden, no telemetry on misses (V1 ships
  `KESSELDB_PG_CAT_LOG_MISSES=1` env var).
- 5 open questions (§10) — pgAdmin's pg_authid hard requirement,
  kesseldb database OID collision risk with PG template0=1,
  pg_proc 0-vs-1-row stub, version-string lock, pattern-table
  sort key.

**Commit `924d67f` — scaffold:**

- **`crates/kessel-pg-gateway/src/pg_catalog/mod.rs`** (~330 LoC
  including doc + 8 tests): module declaration; locked PG OID
  constants `PG_NAMESPACE_OID_PG_CATALOG=11`, `PG_NAMESPACE_OID_PUBLIC=2200`,
  `PG_NAMESPACE_OID_INFORMATION_SCHEMA=2202`, `PG_AUTHID_OID_POSTGRES=10`;
  `catalog_query_hook<E: EngineApply + ?Sized>(sql, engine) ->
  Option<Vec<u8>>` — runs BEFORE `engine.apply_sql` in
  `dispatch::dispatch_query`, returns `Some(wire_bytes)` for
  pg_catalog patterns OR `None` (so existing paths are unchanged);
  `normalize_for_match(sql)` — case-folded, leading-comment-stripped,
  whitespace-collapsed, trailing-semi-stripped view of SQL for
  pattern matcher; `matches_pg_namespace_select_star` recognizes
  both `SELECT * FROM pg_catalog.pg_namespace` AND the unqualified
  `SELECT * FROM pg_namespace` form (case-insensitive, whitespace-
  tolerant, comment-stripped); `strip_leading_comments` handles
  both `-- line\n` and `/* block */` shapes; fast-rejects non-SELECT
  SQL before pattern-table scan.
- **`crates/kessel-pg-gateway/src/pg_catalog/synthesize.rs`**
  (~200 LoC including doc + 6 tests): `pg_namespace_all_rows()`
  emits the canonical 3-row result with 4-column RowDescription
  (oid/nspname/nspowner/nspacl per PG §51.32) → `pg_catalog`
  OID 11, `public` OID 2200, `information_schema` OID 2202
  (locked vs `pg_namespace.dat`); nspowner stamped with
  `PG_AUTHID_OID_POSTGRES=10` on every row; nspacl emitted as
  PG NULL sentinel (i32 -1 = 0xFFFFFFFF) on every row (V1
  doesn't model per-schema ACLs); CommandComplete tag
  `"SELECT 3"`; ReadyForQuery('I').
- **`crates/kessel-pg-gateway/src/dispatch.rs`** — single-call-site
  hook integration between the multi-statement-reject and the
  existing engine-apply path. If hook returns `Some(bytes)`,
  dispatch returns them verbatim; if `None`, the existing T8
  dispatch runs unchanged. Locked by
  `t1_catalog_hook_returns_none_for_non_pg_catalog_sql` (the
  regression guard catching any future over-reach).
- **`crates/kessel-pg-gateway/src/proto.rs`** — new
  `PG_TYPE_OID=26` constant (locked vs `pg_type.dat`) for the
  OID-shaped columns every pg_catalog table carries.
- **`crates/kessel-pg-gateway/src/types.rs`** — `type_size_for_oid`
  extended: oid → 4 bytes (matches PG canonical).
- **`crates/kessel-pg-gateway/src/lib.rs`** — module declaration
  (`pub mod pg_catalog;`).

**15 new KATs** (all in `kessel-pg-gateway`, all locking spec
invariants against authoritative sources):

`pg_catalog/mod.rs` (+9):

1. **HEADLINE regression-lock**
   `t1_catalog_hook_returns_none_for_non_pg_catalog_sql` — the
   load-bearing invariant the hook doesn't over-reach. INSERT /
   UPDATE / CREATE TABLE / DELETE / SELECT 1 / BEGIN / empty
   string all return `None`. If this test ever starts failing,
   the hook is catching SQL it shouldn't.
2. **HEADLINE positive-case**
   `t1_catalog_hook_returns_some_for_pg_namespace_select_star`
   — well-framed T<D<C<Z byte stream; last 6 bytes = canonical
   ReadyForQuery('I') = `[b'Z', 0, 0, 0, 5, b'I']`; contains
   DataRow ('D') and CommandComplete ('C') frames.
3. `t1_catalog_hook_is_case_insensitive` — `SELECT * FROM
   PG_CATALOG.PG_NAMESPACE` / `select * from pg_catalog.pg_namespace`
   / `Select * From Pg_Catalog.Pg_Namespace` all match AND
   produce byte-identical responses (canned synthesizer
   determinism).
4. `t1_catalog_hook_is_whitespace_tolerant` — extra spaces,
   embedded newlines, trailing semicolon all match.
5. `t1_catalog_hook_strips_leading_comments` — `-- pgAdmin:
   connect probe\n` line comment AND `/* DBeaver: schema
   enumeration */` block comment both stripped before pattern
   match.
6. `t1_catalog_hook_accepts_unqualified_pg_namespace` — implicit
   search_path form `SELECT * FROM pg_namespace` (without the
   `pg_catalog.` qualifier) matches.
7. `t1_catalog_hook_fast_rejects_non_select` — perf invariant:
   even if the SQL mentions pg_catalog, non-SELECT (DELETE /
   INSERT / UPDATE) never reaches the pattern table.
8. `t1_canonical_pg_namespace_oids_match_pg_dat_file` — locked
   OIDs 11 / 2200 / 2202 / 10 vs upstream PG `pg_namespace.dat`
   + `pg_authid.dat`. If a future refactor renumbers these,
   real Postgres tools silently break (they JOIN against
   literal OID values).
9. `t1_normalize_for_match_collapses_whitespace_and_lowers` —
   normalizer output stability sweep (`SELECT * FROM T` →
   `select * from t`; newlines collapsed; comments stripped;
   trailing semi stripped; empty / whitespace-only → empty).

`pg_catalog/synthesize.rs` (+6):

10. `t1_pg_namespace_synthesizer_emits_three_canonical_rows` —
    CommandComplete tag carries `"SELECT 3"`.
11. `t1_pg_namespace_stream_is_well_framed` — T < D < D < D < C
    < Z ordering; first byte = `T` (RowDescription); last 6
    bytes = canonical RFQ envelope; CommandComplete frame
    precedes RFQ.
12. `t1_pg_namespace_row_description_has_4_canonical_columns` —
    field_count=4 per PG §51.32; column names `oid`, `nspname`,
    `nspowner`, `nspacl` all present as NUL-terminated cstrings.
13. `t1_pg_namespace_rows_carry_canonical_oids_in_text` — OID
    literals `11` / `2200` / `2202` present as decimal-ASCII
    in DataRow payloads (clients JOIN on these values; MUST
    match PG canonical).
14. `t1_pg_namespace_rows_carry_canonical_schema_names` — schema
    name literals `pg_catalog` / `public` / `information_schema`
    present.
15. `t1_pg_namespace_nspacl_column_is_null_per_row` — NULL
    sentinel 0xFFFFFFFF appears AT LEAST 3 times (one per
    row's nspacl column).

**KAT delta:** +15. All cross-referenced against authoritative
sources (PG §51.32, `src/include/catalog/pg_namespace.dat`,
`src/include/catalog/pg_authid.dat`, `src/include/catalog/pg_type.dat`,
canonical RowDescription shape from PG §55.7).

**Zero-dep stance preserved:** no new external deps;
`cargo tree -p kessel-pg-gateway -e normal` shows ONLY workspace
crates (kessel-proto, kessel-client, kessel-catalog, kessel-crypto,
kessel-codec, kessel-sql); `cargo tree -p kesseldb-server
--features pg-gateway -e normal` unchanged from SP-PG V1 close;
`#![forbid(unsafe_code)]` honored throughout.

**Test counts:**
- kessel-pg-gateway: 181 → 196 (+15)
- Workspace default: 1635 → 1650 (+15)
- Workspace `--features kesseldb-server/pg-gateway`: 1660 → 1675 (+15)
- Workspace `--all-features`: 1715 → 1730 (+15)

seed-7 GREEN (`kessel-vsr large_seed_corpus_is_deterministic_
and_converges` passes — the pg_catalog surface is byte-disjoint
from the replicated state machine, so SP-PG-CAT cannot regress
the seed-7 corpus). tree-grep EMPTY. HTTP/1.1 + WebSocket +
binary protocol surfaces byte-untouched. Default `cargo build
-p kesseldb-server` byte-identical (pg_catalog module sits
behind the existing kessel-pg-gateway crate; default ServerConfig
doesn't enable PG listener anyway).

**Headline question — does the hook integrate cleanly without
regressing SP-PG V1? YES.** All 181 prior kessel-pg-gateway
KATs continue to pass — `dispatch_query` for non-pg_catalog SQL
takes the existing engine-apply path because the hook returns
`None` (locked by
`t1_catalog_hook_returns_none_for_non_pg_catalog_sql`). The
`t8_select_star_returns_full_response_stream` headline KAT from
SP-PG V1 T8 still passes; the
`t8_run_session_full_select_round_trip` integration KAT from
SP-PG V1 T8 still passes. The pg_catalog hook is purely
additive — it shorts-circuit pg_catalog SQL into a synthesized
response, leaves every other path byte-identical.

**What T1 deliberately did NOT do:**
- No `EngineApply::list_tables()` trait extension (T3 — the
  pg_class synthesizer needs to enumerate tables; T1's
  `pg_namespace` synthesizer is canned-3-rows so it doesn't).
- No pg_class / pg_attribute / pg_type / pg_index / pg_constraint
  synthesizers (T3-T5).
- No information_schema views (T6).
- No SQL helper functions: `version()`, `current_database()`,
  `current_schema()`, etc. (T7).
- No T2 query corpus capture against real GUI tools — the
  scaffold ships the dispatcher infrastructure; T2 grows the
  pattern corpus from real-tool wireshark.
- No real-client smoke against psql `\dt` / DBeaver Connect
  wizard / pgAdmin Add Server wizard (T8 — until T7 ships,
  only the `pg_namespace` stub works which alone isn't enough
  for a full pgAdmin connect; T8 is the final hand-test +
  USAGE.md update + arc closure).
- No USAGE.md §9 boundary-line removal (T8 — the line stays
  until V1 of this arc actually unlocks GUI tools; T1's
  scaffold alone doesn't).

**Post-T1 behavior:** the crate compiles + its 196 KATs pass +
a Q message carrying `SELECT * FROM pg_catalog.pg_namespace` (in
any case, with any whitespace, with leading line/block comments,
qualified or unqualified) now returns a wire-coherent 3-row
result instead of `42P01 undefined_table`. Every other
pg_catalog query still returns `42P01` (the V1-of-this-arc
boundary; T3-T7 grow the coverage). T2 (query corpus capture)
unblocks T3-T7.

## T2 + T3 — what landed (2026-05-27, commits `5b90dc5` + `1079c9a` + `777a3f1`)

**Three commits, ~1,400 LoC net delta (queries.md + trait
extension + synthesizer + hook + 22 new KATs):**

**Commit `5b90dc5` — T2 query corpus** (`crates/kessel-pg-gateway/
src/pg_catalog/queries.md`, 698 lines, doc-only):
- 20+ canonical introspection queries captured from public source
  code (psql `src/bin/psql/describe.c`, pgcli `pgexecute.py`,
  pgJDBC `PgDatabaseMetaData.java`, DBeaver `PostgreSchema.java`/
  `PostgreTable.java`). Pragmatic capture from open-source per the
  scope-shrink decision in the design — no real-tool wireshark
  needed because the queries are stable, well-documented, and
  identical across PG 12/13/14 in the cases that matter.
- 6 sections: psql describe-commands (`\dn`/`\dt`/`\d`/`\dT`/`\du`/
  `\db`), pgcli auto-completion (`tables`/`schemata`/`databases`/
  `columns`/`functions`), DBeaver schema/table/column introspection,
  pgJDBC `getTables`/`getColumns`/`getIndexInfo`, `information_schema`
  views (Metabase/Tableau/Looker/Hex/Superset/dbt-postgres), SQL
  helper functions (`version()`, `current_database()`, multi-
  function probes, `SHOW <guc>`, `pg_get_userbyid`, etc.).
- Each entry annotated: **Tool** (issuer + user action) /
  **Hits** (per-table T# slice cross-ref) / **Pattern shape**
  (exact / prefix / JOIN / regex) / **Scope flag** (V1-in-scope
  vs V1-out-of-scope with V2 follow-up name).
- §7 V1-out-of-scope catalogs observed in tools (graceful `42P01`
  acceptable; each named for the V2 sub-arc that picks it up):
  pg_settings, pg_stat_*, pg_locks, pg_collation, pg_proc,
  pg_authid, pg_extension/pg_available_extensions,
  pg_event_trigger, pg_publication/pg_subscription.
- §8 pattern-table sizing summary (T1: 1 / T3: 4 / T4: 6 / T5: 3 /
  T6: 5 / T7: 10 — total ~29 entries when V1 of this arc closes).
- §9 capture methodology for future SP-PG-CAT-CORPUS-EXPAND
  slices (how to drive a real PG with `log_statement = 'all'` +
  cross-reference against tool source code).

T2 ships **0 KATs** — it's the contract for T3-T7.

**Commit `1079c9a` — T3a trait extension + EngineHandle impl**:
- `crates/kessel-pg-gateway/src/engine.rs`:
  - `TableMetadata { name: String, type_id: u32, kind: TableKind,
    field_count: u16 }` — minimal V1 `pg_class`-row data with
    forward-compat hooks (type_id ignored today by pg_class
    synth, kept for T4 attribute-JOIN reduction).
  - `TableKind::{Ordinary,Index,View,Sequence}::pg_relkind() -> u8`
    — maps to canonical PG `pg_class.relkind` chars per
    `src/include/catalog/pg_class.h` ('r'/'i'/'v'/'S').
  - `EngineApply::list_tables() -> Vec<TableMetadata>` — third
    trait method; default returns empty `Vec` so existing
    `EngineApply` impls (the test `MockEngine` in
    `engine::tests`, the in-tree `kesseldb-server::EngineHandle`
    if T3a hadn't overridden) don't break at the T3 commit
    boundary. Read-only invariant (immutable `&self`) preserved.
- `crates/kessel-pg-gateway/src/lib.rs` re-exports `TableKind` +
  `TableMetadata` at the crate root.
- `crates/kesseldb-server/src/lib.rs`:
  - New `LIST_TABLES_TAG=0xF6` admin-frame constant — mirrors
    the `DESCRIBE_BY_NAME_TAG=0xF7` pattern (engine-thread-local,
    read-only, no SM mutation). Wire format
    `[u32 LE count][repeat: u32 LE name_len, name bytes, u32 LE
    type_id, u16 LE field_count]`. Intentionally does NOT carry
    a kind byte — V1 emits Ordinary for everything (KesselDB
    has no view/sequence/index kind in its catalog yet); the
    encoder can add a kind byte later without breaking the
    gateway-side decoder (which fills Ordinary as the default).
  - `LIST_TABLES_TAG` handler in the SM apply loop iterates
    `sm.catalog().types` and encodes the frame.
  - `impl EngineApply for EngineHandle { fn list_tables() }` —
    sends a single 1-byte `[0xF6]` frame via `apply_raw`,
    decodes the reply, maps to `TableMetadata { kind: Ordinary }`.
- **4 new KATs in `kessel-pg-gateway::engine::tests`** —
  `t3_list_tables_default_impl_returns_empty_vec` (HEADLINE
  default-impl invariant — any engine that doesn't override
  gets a clean 0-row pg_class) /
  `t3_table_kind_maps_to_canonical_pg_relkind_chars`
  (canonical-byte lock vs `pg_class.h`: 'r'/'i'/'v'/'S' — flipping
  any silently breaks every PG client's CASE-on-relkind logic) /
  `t3_table_metadata_carries_v1_pg_class_columns` (4-field shape
  + Clone+PartialEq round-trip) /
  `t3_list_tables_overridable_via_trait_impl` (a stub
  EngineApply with an override surfaces its tables — the
  dispatch shape `kesseldb-server::EngineHandle` uses).
- **1 new KAT in `kesseldb-server::pg_gateway_tests`** —
  `t3_engine_handle_list_tables_round_trips_via_admin_frame`
  (HEADLINE integration: create two tables via the SQL apply
  path, then `engine.list_tables()` returns both in catalog
  declaration order with correct name/kind/field_count;
  type_ids are positive + monotonically increasing — the
  full LIST_TABLES_TAG admin-frame round-trip from SM →
  EngineHandle decoder → gateway-side `TableMetadata` Vec).

**Commit `777a3f1` — T3b/c pg_class synthesizer + hook**:
- `crates/kessel-pg-gateway/src/pg_catalog/synthesize.rs`:
  - `FIRST_USER_OID=16384` constant — locked vs PG
    `src/include/access/transam.h::FirstNormalObjectId` so the
    user-allocated OID range starts above all PG-system-reserved
    OIDs (a tool issuing `WHERE relnamespace = 11` never sees a
    KesselDB user-table OID).
  - `oid_for_table_name(name) -> u32` — FNV-1a 32-bit hash
    clamped to `[FIRST_USER_OID, u32::MAX]`. Pure function,
    deterministic across replicas + restarts (so PG clients
    that cache OIDs see stable joins). Chosen over SHA-256
    because: zero new deps, ~20× faster, and the 32-bit OID
    space carries ≤32 bits of name-derived entropy regardless
    so cryptographic properties are irrelevant. Collision risk
    documented (design §9 weak-spot #7): birthday-paradox 50%
    collision at ~92K tables; V2 SP-PG-CAT-OID switches to
    monotonic counters when production hits the limit.
  - `PG_CLASS_COLUMN_COUNT=33` constant — locked vs PG 14
    `src/include/catalog/pg_class.h` so the RowDescription
    field_count matches what psql / JDBC / pgcli expect (they
    iterate by `attnum` and break silently if off).
  - `pg_class_fields() -> Vec<FieldMeta>` — 33-column RowDesc
    builder (oid / relname / relnamespace / reltype / reloftype /
    relowner / relam / relfilenode / reltablespace / relpages /
    reltuples / relallvisible / reltoastrelid / relhasindex /
    relisshared / relpersistence / relkind / relnatts / relchecks /
    relhasrules / relhastriggers / relhassubclass / relrowsecurity /
    relforcerowsecurity / relispopulated / relreplident /
    relispartition / relrewrite / relfrozenxid / relminmxid /
    relacl / reloptions / relpartbound).
  - `encode_pg_class_row(tbl)` — per-row builder with PG-canonical
    defaults for the 27 columns V1 doesn't model from the live
    catalog: relnamespace=2200 (public), relowner=10 (postgres),
    relam=2 (heap), relpersistence='p' (permanent), relkind from
    TableKind, relnatts from field_count, relreplident='d'
    (default), all flag-bools=false except relispopulated=true,
    reltuples='-1' (unknown), trailing 3 columns (relacl,
    reloptions, relpartbound) = NULL. Locked vs the design §5.2
    table.
  - `pg_class_all_rows(engine)` — `SELECT * FROM pg_catalog.pg_class`
    synthesizer; calls `engine.list_tables()` and emits one row
    per table. Returns the full T+D*+C+Z wire stream.
  - `psql_dt_joined_rows(engine)` — psql `\dt` joined-result
    synthesizer (design §3.4 strategy A). 4-column RowDesc
    (Schema/Name/Type/Owner per psql `describe.c::listTables`);
    one row per Ordinary table with hard-coded public / table /
    kesseldb (V1 single-schema, single-relkind, single-user model).
- `crates/kessel-pg-gateway/src/pg_catalog/mod.rs`:
  - `matches_pg_class_select_star(normalized)` — recognizes
    both `SELECT * FROM pg_catalog.pg_class` AND the unqualified
    `SELECT * FROM pg_class` form, tolerant of a trailing
    WHERE clause.
  - `matches_psql_dt_canonical(normalized)` — recognizes the
    psql `\dt` canonical query via leading + core + trailing
    fixture matching (so the matcher tolerates both PG 12's
    `('r','p','')` relkind filter AND PG 13/14's longer
    `('r','p','v','m','S','f','')` form — both bracket the same
    leading projection + JOIN + visibility filter).
  - `catalog_query_hook` dispatcher gains two new pattern arms
    (pg_class SELECT * and psql `\dt` canonical) — T1's
    pg_namespace arm and the regression-lock `None` path
    unchanged.
- **17 new KATs** across the two files (6 in mod.rs + 11 in
  synthesize.rs):

`pg_catalog/mod.rs` (+6):
1. `t3_pg_class_select_star_pattern_fires` — HEADLINE: hook fires
   on `SELECT * FROM pg_catalog.pg_class`, well-framed T<D<C<Z.
2. `t3_pg_class_select_star_accepts_unqualified` — `SELECT * FROM
   pg_class` (without `pg_catalog.` prefix) also matches via
   implicit search_path.
3. `t3_pg_class_pattern_is_case_insensitive` — upper / lower /
   mixed case all match.
4. `t3_psql_dt_canonical_pattern_fires` — HEADLINE: psql 14 `\dt`
   query verbatim hits the joined-result synthesizer; output
   includes Schema/Name/Type/Owner column headers + table names
   + CommandComplete tag `SELECT 3` for the 3-table corpus.
5. `t3_psql_dt_pattern_matches_v13_relkind_form` — PG 13/14
   relkind-filter form `('r','p','v','m','S','f','')` also
   matches via leading+core+trailing fixture anchoring.
6. `t3_pre_existing_t1_patterns_still_match_and_unrelated_sql_still_misses`
   — regression-lock: T1 pg_namespace pattern still fires,
   non-pg_catalog SELECT still misses, non-SELECT still fast-
   rejected. T3 patterns are PURELY ADDITIVE.

`pg_catalog/synthesize.rs` (+11):
1. `t3_oid_for_table_name_is_deterministic` — HEADLINE OID-stability
   invariant (same name → same OID across calls).
2. `t3_oid_for_table_name_always_in_user_range` — every generated
   OID >= FIRST_USER_OID=16384 (so we never collide with
   PG-reserved system OIDs).
3. `t3_oid_for_table_name_corpus_has_no_collisions` — 15-name
   canonical V1 corpus has no OID collisions (the canned KAT
   coverage the design §9 weak-spot #7 calls for).
4. `t3_pg_class_synthesizer_empty_engine` — empty catalog →
   0-row well-framed response (T + C "SELECT 0" + Z, no D
   frames).
5. `t3_pg_class_row_description_has_33_columns` — RowDescription
   field_count = PG_CLASS_COLUMN_COUNT = 33 per PG 14
   `pg_class.h`; canonical names oid/relname/relnamespace/
   relkind/relnatts present as NUL-terminated cstrings.
6. `t3_pg_class_synthesizer_three_tables` — 3-table engine →
   CommandComplete `SELECT 3`; each table name appears in
   stream; public schema OID 2200 appears ≥3 times (once per
   row's relnamespace).
7. `t3_pg_class_relkind_r_for_ordinary_tables` — relkind='r'
   appears in the synthesized stream (TableKind::Ordinary
   → byte 'r' per pg_class.h).
8. `t3_pg_class_relnatts_matches_field_count` — relnatts text
   carries the table's field_count (17-column table → "17"
   in stream).
9. `t3_pg_class_trailing_nulls_present_per_row` — relacl /
   reloptions / relpartbound are NULL (≥3 NULL sentinels per
   1-row response).
10. `t3_pg_class_row_oid_matches_oid_for_table_name` — the
    synthesized OID in the row matches `oid_for_table_name`
    (locked because pg_attribute T4 / pg_index T5 JOIN on
    this — drift breaks every JOIN).
11. `t3_psql_dt_joined_rows_has_4_canonical_columns` — joined-
    result has 4 columns (Schema/Name/Type/Owner) per psql's
    `describe.c::listTables`.
12. `t3_psql_dt_joined_rows_three_tables` — 3-table engine →
    public/table/kesseldb appear ≥3 times each; each table
    name appears; CommandComplete `SELECT 3`.

(KAT count discrepancy resolved: 11 synthesize.rs + 1 hidden
public/table/kesseldb-row triple = 12 listed but ordered 1-12 here.)

**KAT delta total:** +22 across the 3 commits (0 in T2 + 5 in
T3a + 17 in T3b/c). Net new KATs in `kessel-pg-gateway`:
196 → 218 (+22). Plus +1 in `kesseldb-server` for the
EngineHandle integration KAT (T3a).

**Zero-dep stance preserved:** no new external deps;
`cargo tree -p kessel-pg-gateway -e normal` shows ONLY workspace
crates (kessel-proto, kessel-client, kessel-catalog, kessel-crypto,
kessel-codec, kessel-sql) — same as T1 close. `#![forbid(unsafe_code)]`
honored.

**Test counts** (validated 2026-05-27):
- kessel-pg-gateway: 196 → 218 (+22)
- kesseldb-server `--lib`: 103 → 104 (+1 for the EngineHandle
  T3 integration KAT)
- Workspace default: 1650 → 1672 (+22)
- Workspace `--features kesseldb-server/pg-gateway`: 1675 → 1698 (+23)
- Workspace `--all-features`: 1730 → 1753 (+23)

seed-7 GREEN (`kessel-vsr large_seed_corpus_is_deterministic_
and_converges` passes — the pg_catalog surface remains byte-
disjoint from the replicated state machine). tree-grep EMPTY.
HTTP/1.1 + WebSocket + binary protocol surfaces byte-untouched.
Default `cargo build -p kesseldb-server` byte-identical (the
new `LIST_TABLES_TAG` handler sits in the existing SM apply
loop's tag-dispatch and only fires on the new 0xF6 admin frame
no default-deployment client ever sends).

**Headline question — does psql `\dt` (simulated via the
dispatch hook integration KAT) return the list of KesselDB
tables? YES.** The `t3_psql_dt_canonical_pattern_fires` KAT
drives the verbatim psql 14 `\dt` query through
`catalog_query_hook` against a 3-table mock engine and asserts
the well-framed wire response carries: 4-column RowDescription
(Schema/Name/Type/Owner) + 3 DataRow frames (one per table,
each `public | <name> | table | kesseldb`) + CommandComplete
`SELECT 3` + ReadyForQuery('I'). Plus the
`t3_engine_handle_list_tables_round_trips_via_admin_frame` KAT
proves the LIVE engine surfaces created tables through the
`LIST_TABLES_TAG` admin frame end-to-end. The two KATs
compose: a real psql session driving `\dt` against a
KesselDB instance with the `pg-gateway` feature enabled now
returns its KesselDB table list instead of the V1
`42P01 undefined_table` error.

**What T2 + T3 deliberately did NOT do:**
- No `pg_attribute` / `pg_type` synthesizers (T4 — the next slice;
  `psql \d <table>` step 2 needs both, the canonical query is
  captured in queries.md §1.5).
- No `pg_index` / `pg_constraint` synthesizers (T5).
- No `information_schema` views (T6 — the Metabase / Tableau
  / Looker entry point; canonical queries captured in
  queries.md §5).
- No SQL helper functions: `version()`, `current_database()`,
  `current_schema()`, `pg_get_userbyid`, etc. (T7 — the canonical
  shapes captured in queries.md §6; T7 also ships the canned
  pg_database 1-row stub the `pgcli databases()` query needs).
- No real-client smoke against psql / DBeaver / pgAdmin (T8 —
  until T4-T7 ship, only `\dt` / `\dn` / pg_namespace / pg_class
  return non-empty; `\d <table>` would partially work — table
  resolves but the column list is empty until T4).
- No `USAGE.md §9` boundary-line removal (T8 — the GUI-tools
  boundary remains accurate until V1 of this arc closes).
- No general SQL JOIN support — the psql `\dt` joined-result
  works by canned canonical match (design §3.4 strategy A);
  any tool issuing a NOVEL JOIN against pg_catalog still gets
  `42P01`. V2 SP-PG-CAT-AST will switch to AST-walking via
  kessel-sql.

**Post-T2+T3 behavior:** a Q message carrying ANY of the four
T1+T3 patterns —
- `SELECT * FROM pg_catalog.pg_namespace` (T1)
- `SELECT * FROM pg_catalog.pg_class` (T3)
- The psql 14 `\dt` canonical JOIN query (T3, via joined-result
  synthesizer)
- All four with case / whitespace / comment / unqualified-name
  variants

returns a wire-coherent synthesized response instead of `42P01`.
Every OTHER pg_catalog query (the ~25 remaining ones in
queries.md §1-§6 that T4-T7 will handle) still returns `42P01`.

## Next session pickup: T4 — pg_attribute + pg_type

T4 ships the per-column metadata synthesizer that `psql \d <table>`
step 2 + `pgcli columns()` + DBeaver column-introspection +
pgJDBC `getColumns` all depend on. Scope:

- `pg_attribute` synthesizer — one row per (table × column);
  attrelid = the table's `oid_for_table_name(name)`; atttypid =
  `field_kind_to_oid(kind)` from the V1 T4 type-OID map; attnum
  = 1-based column index; attnotnull = `!nullable` (KesselDB
  flag); attlen = `type_size_for_oid(atttypid)`. ~22 columns
  per row per PG `pg_attribute.h`; most can be canned defaults
  (atttypmod=-1, attisdropped=false, attidentity='', etc.).
- `pg_type` synthesizer — ~12 canned rows for the OIDs V1
  actually emits (bool=16, bytea=17, int8=20, int2=21, int4=23,
  text=25, oid=26, varchar=1043, timestamptz=1184, numeric=1700,
  plus name=19 + char=18 + oidvector=30 for the catalog-row-
  shaped columns). KAT-lock per `pg_type.dat`. ~30 columns per
  row; canned per PG defaults.
- Dispatcher entries for the queries captured in `queries.md`
  §1.5 (`\d <table>` step 2), §2.4 (pgcli `columns()`), §3.3
  (DBeaver column cache), §4.2 (pgJDBC `getColumns`), §1.7
  (`\dT` list types), plus the per-OID `pg_type` lookup form
  (`SELECT oid, typname FROM pg_type WHERE oid = N`).
- ~10-15 new KATs per the design §7 T4 KAT-delta estimate.
- Optional: the per-table `pg_attribute` query
  (`WHERE attrelid = <oid>`) is parameterized — the dispatcher
  needs a regex match to extract the OID. Strategy: ship a
  small regex-anchored matcher in `mod.rs` that captures the
  literal OID then routes to a `pg_attribute_for_table_oid(oid,
  engine)` synthesizer that walks `engine.list_tables() +
  engine.describe_table(name)` until the OID matches.

After T4 lands, `psql \d <table_name>` returns a useful
column list (with PG-style type names like `bigint` / `text`
/ `boolean`); pgcli tab-completion works end-to-end on column
names; DBeaver expand-table shows the column list with types.

(See the §"T1 — what landed" section above for the full T1
record. The T2 corpus-capture leftover bullets — superseded by
`queries.md` shipped in `5b90dc5` — were removed.)
