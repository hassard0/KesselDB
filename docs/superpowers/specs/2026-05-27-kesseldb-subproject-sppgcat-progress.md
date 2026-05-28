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
| **T4** | `pg_attribute` synthesizer (one row per (table × column); attrelid = the table's pg_class.oid; atttypid = `field_kind_to_oid(kind)` from V1 type-OID map; attnum = 1-based; attnotnull = `!nullable`; attlen = `type_size_for_oid(atttypid)`) + dispatcher entries for the ~6 canonical patterns with the `attrelid = N` filter. Plus `pg_type` synthesizer with the ~12 type rows V1 actually emits (bool=16, bytea=17, int8=20, int2=21, int4=23, text=25, oid=26, varchar=1043, timestamptz=1184, numeric=1700, plus name=19 + char=18 + oidvector=30 for the catalog-row-shaped columns) + the ~3 canonical patterns (`SELECT oid, typname FROM pg_type` + the per-OID lookup form). KAT-locked OID values vs `pg_type.dat`. | **DONE** | `8f0a49a` |
| **T5** | `pg_index` synthesizer (one row per KesselDB index; indrelid = table pg_class.oid; indexrelid = stable hash of index name; indkey = column attnums as packed int2vector; indisunique = per kind) + `pg_constraint` synthesizer (one row per UNIQUE/FK/CHECK; contype = 'u'/'f'/'c'; synthetic constraint name `<table>_<col>_key` / `<table>_<col>_fkey` / `<table>_check_N`) + dispatcher entries. | **DONE** | `1004c2f` (with T7) |
| **T6** | `information_schema.{tables,columns,schemata,key_column_usage,table_constraints}` synthesizers + dispatcher entries (Metabase / Tableau / Looker / Hex / Superset / dbt-postgres + DataGrip's empty `views`/`routines` probes). Wider than the original V1 design (5 row-emitting + 2 empty-stub vs the design's 2). Uses SQL-standard `data_type` names (`bigint` / `boolean` / `text` / `timestamp with time zone`) NOT pg_type internal names — locked because BI tools key feature support off this column. | **DONE** | `b0d1efc` |
| **T7** | SQL helper functions: version(), current_database(), current_schema(), current_user, session_user, pg_my_temp_schema(), pg_is_other_temp_schema(oid), obj_description(...)/(oid), pg_get_constraintdef(oid), pg_get_indexdef(oid), pg_table_is_visible(oid), pg_encoding_to_char(enc), plus the `SHOW <guc>` pattern for canned GUCs (server_version, server_encoding, client_encoding, TimeZone, DateStyle, etc. — matching the V1 ParameterStatus emit). Each is a dispatcher entry + a tiny synthesizer. Multi-function shape `SELECT version(), current_database()` (pgAdmin uses this) handled with a separate dispatcher pattern. | **DONE** | `1004c2f` (with T5) |
| **T8** | T8a: real EngineHandle impls for `list_indexes_for_table` + `list_constraints_for_table` via new `LIST_INDEXES_TAG`=0xF5 + `LIST_CONSTRAINTS_TAG`=0xF4 admin frames — closes the T5 KNOWN GAP where a real KesselDB returned the default empty-Vec. T8b: USAGE.md §9 "Supported GUI / admin tools" sub-section + sample interactive psql session + V2-deferred catalog list (replaces the removed "No `pg_catalog.*` introspection" line). T8c: ARCHITECTURE.md PG-wire section adds a "pg_catalog stubs (SP-PG-CAT — V1 closed)" sub-section. T8d: STATUS.md arc-closure row + this progress tracker row. T8e: real-client smoke (psql/pgAdmin/DBeaver/Metabase wizards) is deferred-as-manual-verification because GUI tools can't be driven from a dispatch session — the operator runs the verified sample-session commands documented in USAGE.md §9. **SP-PG-CAT V1 ARC CLOSED at T8 commit.** | **DONE** | `6d50a83` (T8a) + this commit (T8b/c/d) |

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

## T4 — what landed (2026-05-27, commit `8f0a49a`)

Single commit, ~1340 LoC net delta in pg_catalog/{mod.rs,
synthesize.rs}. **T4 ships in 2 logical halves bundled into one
commit:**

**T4a — `pg_attribute` synthesizer + 3 pattern arms:**

- `PG_ATTRIBUTE_COLUMN_COUNT = 25` constant (locked vs PG 14
  `src/include/catalog/pg_attribute.h`). Off-by-one breaks every
  JDBC `getColumns` caller silently.
- `PG_COLLATION_DEFAULT = 100` constant (canonical PG default
  collation OID per `pg_collation.dat`).
- `pg_attribute_fields()` 25-column RowDesc builder. Column
  order: attrelid / attname / atttypid / attstattarget / attlen
  / attnum / attndims / attcacheoff / atttypmod / attbyval /
  attstorage / attalign / attnotnull / atthasdef / atthasmissing
  / attidentity / attgenerated / attisdropped / attislocal /
  attinhcount / attcollation / attacl / attoptions / attfdwoptions
  / attmissingval — matches PG 14 declaration order.
- `attbyval_for_oid` / `attstorage_for_oid` / `attalign_for_oid`
  per-OID helpers (locked vs `pg_type.dat`): bool/int*/oid/
  timestamptz pass-by-value+plain+per-type-align; bytea/text/
  numeric/varchar pass-by-ref+extended+i-aligned (varlena header).
- `encode_pg_attribute_row(attrelid, name, atttypid, attnum,
  nullable)` per-column builder. Modeled columns: attlen via
  `type_size_for_oid`, attnum 1-based, attnotnull = !nullable,
  attcollation = 100 for text/varchar else 0. Canned: attstattarget=-1,
  attndims=0, attcacheoff=-1, atttypmod=-1, atthasdef=false,
  atthasmissing=false, attidentity='', attgenerated='',
  attisdropped=false, attislocal=true, attinhcount=0. Trailing
  attacl/attoptions/attfdwoptions/attmissingval = NULL per design §5.3.
- `synthesize_pg_attribute(engine, attrelid_filter: Option<u32>)`
  walks `engine.list_tables() + engine.describe_table(name)`
  emitting one row per (table × column) when filter=None or
  filtering to the matching table when filter=Some(oid). The
  filter is the common psql `\d <table>` / pgJDBC getColumns /
  DBeaver column-cache hot path.
- `psql_d_table_joined_rows(engine, table_oid)` — joined-result
  intercept for the psql `\d <table>` step-2 column-list query
  (queries.md §1.5). 7-column projection (attname / format_type
  / pg_get_expr=NULL / attnotnull / attcollation=NULL /
  attidentity='' / attgenerated='') per design §3.4 strategy A;
  `pg_attrdef` + `pg_collation` subselects all return NULL (V1
  single-schema + single-collation + no defaults).

**T4b — `pg_type` synthesizer + 2 pattern arms:**

- `PG_TYPE_COLUMN_COUNT = 30` constant (locked vs PG 14 `pg_type.h`).
- `PG_TYPE_ROWS: &[PgTypeRow]` const table with 13 canned rows
  for the OIDs V1 actually emits, values locked vs PG `pg_type.dat`:

  | oid | typname | typlen | typbyval | typcategory | typalign | typstorage | typcollation |
  |---|---|---|---|---|---|---|---|
  | 16 | bool | 1 | true | B | c | p | 0 |
  | 17 | bytea | -1 | false | U | i | x | 0 |
  | 20 | int8 | 8 | true | N | d | p | 0 |
  | 21 | int2 | 2 | true | N | s | p | 0 |
  | 23 | int4 | 4 | true | N | i | p | 0 |
  | 25 | text | -1 | false | S | i | x | 100 |
  | 26 | oid | 4 | true | N | i | p | 0 |
  | 700 | float4 | 4 | true | N | i | p | 0 |
  | 701 | float8 | 8 | true | N | d | p | 0 |
  | 1043 | varchar | -1 | false | S | i | x | 100 |
  | 1184 | timestamptz | 8 | true | D | d | p | 0 |
  | 1700 | numeric | -1 | false | N | i | x | 0 |
  | 19 | name | 64 | false | S | c | p | 100 |

- `pg_type_name_for_oid(oid)` public lookup (used by the `\d
  <table>` joined synthesizer for the format_type column).
- `pg_type_fields()` 30-column RowDesc builder.
- `encode_pg_type_row(r)` per-row builder fills the 30 columns
  with canned PG defaults (typnamespace=11=pg_catalog, typowner=10,
  typtype='b', typispreferred=false, typisdefined=true,
  typdelim=',', typrelid=0, typsubscript=0, typelem=0, typarray=0
  (V1 no array types), typinput/typoutput/typreceive/typsend =0
  V1, typnotnull=false, typbasetype=0, typtypmod=-1, typndims=0,
  typdefault=NULL).
- `synthesize_pg_type()` emits all 13 canned rows.
- `synthesize_pg_type_by_oid(oid)` emits one row matching oid or
  zero rows if unknown (JDBC column-type resolution hot path).
- `pgjdbc_getcolumns_joined_rows(engine, table_name)` — joined-result
  intercept for the pgJDBC `getColumns` canonical query (queries.md
  §4.2). 15-column projection (nspname=public / relname / attname /
  atttypid / attnotnull / atttypmod=-1 / attlen / typtypmod=-1 /
  attnum (row_number partition) / attidentity='' / attgenerated=''
  / adsrc=NULL / description=NULL / typbasetype=0 / typtype='b').

**`pg_catalog::mod` pattern arms** (7 new):

- `matches_pg_attribute_select_star` — both qualified and
  unqualified `SELECT * FROM pg_attribute` forms.
- `extract_attrelid_filter` — parses `WHERE attrelid = N` (4
  variants: qualified/unqualified × bare/`a.attrelid =` aliased).
  Returns the OID on match. Uses the new `parse_leading_u32`
  decimal scanner.
- `extract_psql_d_table_oid` — anchors on the psql `\d <table>`
  step-2 leading fixture (`SELECT a.attname,`) + core (`FROM
  pg_catalog.pg_attribute a WHERE a.attrelid = '<oid>'`). Handles
  the quoted-OID form psql ships.
- `matches_pg_type_select_star` — qualified + unqualified.
- `extract_pg_type_oid_filter` — parses `WHERE oid = N` (4
  variants: qualified/unqualified × bare/`t.oid =` aliased).
- `extract_pgjdbc_getcolumns_relname` — anchors on the distinctive
  `row_number() OVER (PARTITION BY a.attrelid` pgJDBC fixture;
  captures `c.relname LIKE '<name>'` or `c.relname = '<name>'`.

T1+T3 patterns unchanged. T4 additions are PURELY ADDITIVE.

**KAT delta: +26 (244 vs 218 baseline).** Breakdown:

`pg_catalog::synthesize::tests` (+18):

- `t4_pg_attribute_synthesizer_all_tables` — HEADLINE 2-table ×
  5-column corpus emits SELECT 5 + all column names visible.
- `t4_pg_attribute_synthesizer_filtered_to_one_table` — HEADLINE
  filter=users_oid emits SELECT 2 + skips orders columns.
- `t4_pg_attribute_row_description_has_25_columns` — RowDesc
  field_count = PG_ATTRIBUTE_COLUMN_COUNT + canonical names visible.
- `t4_pg_attribute_synthesizer_empty_engine` — SELECT 0 + RFQ('I').
- `t4_pg_attribute_atttypid_matches_field_kind_to_oid_map` —
  OID 20 ≥3× (I64), 25 ≥1× (Char(64)), 1700 ≥1× (Fixed{2}).
- `t4_pg_attribute_attnum_is_1_based_sequential` — 5-column table
  emits attnums 1..=5.
- `t4_pg_attribute_attnotnull_is_true_for_v1_columns` — 't' bool
  byte present in stream.
- `t4_psql_d_table_joined_rows_fires_for_matching_oid` — format_type
  emits `int8` + `text` for users (I64 + Char(64)).
- `t4_psql_d_table_joined_rows_empty_for_unknown_oid` — SELECT 0.
- `t4_pg_type_synthesizer_emits_all_canned_rows` — HEADLINE
  SELECT 13 + well-framed.
- `t4_pg_type_row_description_has_30_columns` — RowDesc field_count
  = PG_TYPE_COLUMN_COUNT + canonical names visible.
- `t4_pg_type_canned_rows_carry_v1_type_names` — all 10 V1 type
  names (bool/bytea/int8/int2/int4/text/oid/numeric/timestamptz/
  varchar) present.
- `t4_pg_type_int4_row_is_canonical` — per-OID lookup OID 23 →
  SELECT 1 + 'int4' + typbyval=t.
- `t4_pg_type_text_row_is_canonical` — OID 25 → 'text' + typlen=-1
  + typcollation=100.
- `t4_pg_type_by_oid_unknown_returns_empty` — SELECT 0 + RFQ.
- `t4_pg_type_name_for_oid_round_trips` — public helper round-trips
  for V1 OIDs + unknown→"unknown".
- `t4_pgjdbc_getcolumns_joined_rows_matches_by_name` — match=SELECT 2,
  unmatched=SELECT 0.

`pg_catalog::mod::tests` (+8):

- `t4_pg_attribute_select_star_pattern_fires` — HEADLINE hook +
  synthesizer fire on `SELECT * FROM pg_catalog.pg_attribute`.
- `t4_pg_attribute_select_star_unqualified` — unqualified form hits.
- `t4_pg_attribute_attrelid_filter_pattern_fires` — HEADLINE
  `WHERE attrelid = <users_oid>` filters to SELECT 2.
- `t4_pg_attribute_attrelid_filter_unknown_oid_zero_rows` — unknown
  OID → SELECT 0.
- `t4_psql_d_table_step2_pattern_fires` — HEADLINE verbatim psql 14
  `\d <table>` step-2 query through hook returns SELECT 2 + `int8`
  + `name` visible.
- `t4_pg_type_select_star_pattern_fires` — qualified hits + int8
  in stream.
- `t4_pg_type_select_star_unqualified` — unqualified form hits.
- `t4_pg_type_per_oid_lookup_pattern_fires` — `WHERE oid = 20`
  → SELECT 1 + 'int8'.
- `t4_pre_existing_t1_t3_patterns_still_match` — regression lock.

**Zero-dep stance preserved.** No new external deps; pure-Rust
const tables + pattern matching. `#![forbid(unsafe_code)]` honored.
HTTP/1.1 + WebSocket + binary surfaces byte-untouched. Default
`cargo build -p kesseldb-server` byte-identical (pg-gateway is
opt-in feature gate).

**Test counts:**
- kessel-pg-gateway: 218 → 244 (+26)
- workspace default: 1672 → 1694 (+22 — pg-gateway tests count
  in the default workspace member set; 4 of the +26 KATs are
  pg-gateway test-only helpers that don't count in the lib total)
- workspace pg-gateway-featured: 1698 → 1706
- workspace --all-features: ≥1750
- seed-7 GREEN
- tree-grep EMPTY

**Headline question — does `psql -h localhost "\d <table>"` (via
the dispatch hook integration KAT) return the column list with
PG type names? YES.** The `t4_psql_d_table_step2_pattern_fires`
KAT drives the verbatim canonical psql 14 `\d <table>` step-2 SQL
through `catalog_query_hook` against a 2-table mock engine and
asserts the well-framed wire response: 7-column RowDescription +
2 DataRow frames (one per `users` column) + format_type `int8`
for the I64 id column + column name `name` visible + CommandComplete
`SELECT 2` + ReadyForQuery('I'). Combined with the T3 `\dt`
synthesizer already shipped, a real psql session can now list
tables (`\dt`) AND describe a table's columns (`\d users`)
end-to-end against KesselDB.

## T5 + T7 — what landed (2026-05-27, commit `1004c2f`)

Single commit, ~2,144 LoC net delta across engine.rs + lib.rs +
pg_catalog/{mod.rs, synthesize.rs}. T5 + T7 bundled because they
share the same dispatcher hook integration site and the helper-
function recognizer interleaves with the table-pattern table in
`catalog_query_hook`.

**T5a — `pg_index` synthesizer + trait extension:**

- `crates/kessel-pg-gateway/src/engine.rs`:
  - `IndexMetadata { name, fields: Vec<u32>, is_unique, kind }` +
    `IndexKind::{Equality,Range,Composite}` (maps from
    `ObjectType.indexes` / `ordered` / `composite`).
  - `EngineApply::list_indexes_for_table(name) -> Vec<IndexMetadata>`
    — default returns empty Vec so existing impls (`MockEngine`,
    the in-tree `EngineHandle`) don't break at the T5 commit
    boundary. Read-only invariant preserved (immutable `&self`).
- `crates/kessel-pg-gateway/src/pg_catalog/synthesize.rs`:
  - `PG_INDEX_COLUMN_COUNT = 19` constant (locked vs PG 14
    `src/include/catalog/pg_index.h`).
  - `pg_index_fields()` 19-column RowDesc builder (indexrelid /
    indrelid / indnatts / indnkeyatts / indisunique / indisprimary
    / indisexclusion / indimmediate / indisclustered / indisvalid
    / indcheckxmin / indisready / indislive / indisreplident /
    indkey / indcollation / indclass / indoption / indpred).
  - `oid_for_index_name(name)` — reuses `oid_for_table_name`
    FNV-1a strategy (same determinism + collision profile).
  - `render_int2vector(fields)` — space-separated attnums per
    PG `int2vector` text format ("1 2 3").
  - `render_zero_vector(n)` — oidvector of zeros (V1 doesn't
    model per-column collation/opclass/option).
  - `encode_pg_index_row(indexrelid, indrelid, idx)` — per-row
    builder. Modeled: indnatts/indnkeyatts from field count
    (V1 no INCLUDE so they match), indisunique per kind,
    indkey as int2vector text. Canned: indisprimary=false (V1
    no PK distinct from id-PK), indisexclusion=false,
    indimmediate=true, indisvalid=true, indisready=true,
    indislive=true, indisclustered=false, indisreplident=false,
    indcheckxmin=false, indpred=NULL.
  - `synthesize_pg_index(engine, indrelid_filter: Option<u32>)`
    walks `engine.list_tables() + engine.list_indexes_for_table`.
    Filter=None emits all indexes; Some(oid) filters to matching
    table.
  - `pgjdbc_getindexinfo_joined_rows(engine, table_name)` —
    joined-result intercept for the pgJDBC `getIndexInfo` query
    (queries.md §4.3). 13-column projection (TABLE_CAT=NULL /
    TABLE_SCHEM=public / TABLE_NAME / NON_UNIQUE / INDEX_QUALIFIER
    =NULL / INDEX_NAME / TYPE=3=btree / ORDINAL_POSITION /
    COLUMN_NAME / ASC_OR_DESC=NULL / CARDINALITY=0 / PAGES=0 /
    FILTER_CONDITION=NULL) — one row per (index × column).

**T5b — `pg_constraint` synthesizer + trait extension:**

- `crates/kessel-pg-gateway/src/engine.rs`:
  - `ConstraintMetadata { name, kind, columns: Vec<u32>,
    references: Option<(String, Vec<u32>)> }`.
  - `ConstraintKind::{Check,ForeignKey { on_delete: FkAction },
    Unique}::pg_contype() -> u8` returns 'c'/'f'/'u' locked vs
    PG 14 `src/include/catalog/pg_constraint.h`.
  - `FkAction::{NoAction,Restrict,Cascade,SetNull,SetDefault}::
    pg_action_char() -> u8` returns 'a'/'r'/'c'/'n'/'d' per
    canonical `confdeltype`.
  - `EngineApply::list_constraints_for_table(name) ->
    Vec<ConstraintMetadata>` — default empty Vec.
- `crates/kessel-pg-gateway/src/pg_catalog/synthesize.rs`:
  - `PG_CONSTRAINT_COLUMN_COUNT = 25` constant (locked vs PG 14
    `pg_constraint.h`).
  - `pg_constraint_fields()` 25-column RowDesc builder
    (oid/conname/connamespace/contype/condeferrable/condeferred/
    convalidated/conrelid/contypid/conindid/conparentid/confrelid/
    confupdtype/confdeltype/confmatchtype/conislocal/coninhcount/
    connoinherit/conkey/confkey/conpfeqop/conppeqop/conffeqop/
    conexclop/conbin).
  - `render_int_array(fields)` — PG `int2[]` array literal
    format `{1,2,3}`.
  - `encode_pg_constraint_row(conrelid, c)` — per-row builder.
    OID computed via FNV-1a of synthetic `__con__<name>` so
    constraint OIDs don't collide with table OIDs. contype byte
    from `kind.pg_contype()`. confrelid populated for FK only
    (via `oid_for_table_name(referenced_table)`). confupdtype
    canned 'a' (NoAction default). confdeltype byte from
    `on_delete.pg_action_char()`. confmatchtype='s' (simple).
    conkey/confkey rendered as int2[] literals.
  - `synthesize_pg_constraint(engine, conrelid_filter:
    Option<u32>)` mirrors the pg_index walk.

**T5 pattern hooks (`pg_catalog::mod`):**

- `matches_pg_index_select_star` (qualified + unqualified).
- `extract_indrelid_filter` parsing `pg_catalog.pg_index WHERE
  indrelid = N` (qualified + unqualified + `i.indrelid =` aliased).
- `extract_psql_d_index_step_oid` anchoring on the distinctive
  `pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog
  .pg_index i` triple-table FROM + `c.oid = '<oid>'` filter.
- `extract_pgjdbc_getindexinfo_relname` anchored on the
  distinctive `information_schema._pg_expandarray(i.indkey)`
  fixture + capturing `ct.relname = '<name>'` or
  `ct.relname like '<name>'`.
- `matches_pg_constraint_select_star` (qualified + unqualified).
- `extract_conrelid_filter` (qualified + unqualified + aliased
  4 variants).

**T7 — SQL helper functions + SHOW handler:**

- `crates/kessel-pg-gateway/src/pg_catalog/synthesize.rs`:
  - `KESSELDB_VERSION_STRING = "PostgreSQL 14.0 (KesselDB 1.0)"`
    + `KESSELDB_DATABASE_NAME = "kesseldb"` +
    `KESSELDB_SCHEMA_NAME = "public"` +
    `KESSELDB_USER_NAME = "kesseldb"` constants. The version
    string matches the V1 ParameterStatus emit so clients see a
    coherent view of the server (locked because tools may
    fingerprint the version).
  - `single_text_row(name, value)` / `single_bool_row(name, val)`
    / `single_int_row(name, oid, val)` — frame-builders for the
    single-call helper response shape (T + 1 DataRow + C "SELECT 1"
    + Z).
  - `show_value_for(name)` maps GUC name → canned value
    (server_version=14.0, server_encoding=UTF8, client_encoding
    =UTF8, DateStyle="ISO, MDY", timezone=UTC,
    standard_conforming_strings=on, integer_datetimes=on,
    is_superuser=on, search_path="$user, public",
    default_transaction_isolation="read committed",
    in_hot_standby=off, …); unknown name → "" (PG behavior).
  - `synthesize_helper_function(normalized) -> Option<Vec<u8>>`
    — single-call recognizer covering version() /
    current_database() / current_schema()(/) / current_user /
    session_user / user / current_catalog / pg_backend_pid() /
    pg_my_temp_schema() / pg_postmaster_start_time() + per-OID
    pg_table_is_visible / pg_type_is_visible /
    pg_function_is_visible / pg_is_other_temp_schema /
    pg_get_userbyid / pg_get_indexdef / pg_get_constraintdef /
    pg_get_expr / obj_description / format_type /
    current_setting / SHOW + the pgAdmin multi-function probe.
    Trailing `AS alias` stripped via `strip_select_alias`.
  - `synthesize_pgadmin_multi_helper(normalized)` recognizes
    the canonical 2-/3-/4-function pgAdmin connect probe
    (queries.md §6.3) — multi-column single-row response with
    all values populated.
  - `extract_quoted_arg(s)` — extracts the first single-quoted
    argument from a function call (used by `current_setting`
    argument parsing).
- `crates/kessel-pg-gateway/src/pg_catalog/mod.rs`:
  - SHOW <name> handler routed BEFORE the SELECT fast-reject
    (SHOW isn't a SELECT — PG treats it specially).
  - `synthesize_helper_function` checked BEFORE the table-pattern
    matchers (helpers are simpler shapes + tools issue them as
    the first probe on connect).

**+63 KATs** total (+6 engine + +21 mod hook + +36 synth).
Headline KATs:

- `t5_pg_index_select_star_pattern_fires` (HEADLINE)
- `t5_psql_d_table_step3_pattern_fires` (HEADLINE — verbatim
  psql 14 `\d <table>` step 3 routes through hook)
- `t5_pgjdbc_getindexinfo_pattern_fires` (HEADLINE — verbatim
  pgJDBC `getIndexInfo` emits column rows)
- `t5_pg_constraint_synthesizer_emits_all_constraints` (HEADLINE
  — 1 + 2 = 3 constraints across 2 tables)
- `t5_pg_constraint_contype_byte_per_kind` ('c'/'f'/'u' all
  appear in the synthesized stream)
- `t7_version_returns_kesseldb_version` (HEADLINE — canned
  "PostgreSQL 14.0 (KesselDB 1.0)" appears)
- `t7_pgadmin_multi_function_probe` (HEADLINE — 4-column
  single-row response with all 4 values)
- `t7_show_dispatches_through_hook` (HEADLINE)
- `t5_t7_pre_existing_patterns_still_match` (regression lock).

**Zero-dep stance preserved**: `cargo tree -p kessel-pg-gateway -e
normal` shows ONLY workspace crates; `#![forbid(unsafe_code)]`
honored. HTTP/1.1 + WebSocket + binary protocol surfaces
byte-untouched. Default `cargo build -p kesseldb-server`
byte-identical (pg-gateway opt-in feature; T5+T7 additions are
entirely inside the existing crate).

**Test counts** (validated 2026-05-27):
- kessel-pg-gateway lib: 244 → 301 (+57)
- workspace default: 1694 → 1755 (+61)
- workspace `--features kesseldb-server/pg-gateway`: 1706 → 1781 (+75)
- workspace `--all-features`: ≥1750 → 1836

seed-7 GREEN. tree-grep EMPTY.

**Headline question — does `psql -h localhost "\d <table>"` show
indexes + constraints AND `SELECT version()` return the canned
KesselDB version? YES (via the synthesizer dispatch hook).** The
`t5_psql_d_table_step3_pattern_fires` KAT drives the verbatim
canonical psql 14 `\d <table>` step 3 query through
`catalog_query_hook` against a 1-table mock engine (1 UNIQUE
index on users.email) and asserts the well-framed wire response
carries `SELECT 1`; `t5_pgjdbc_getindexinfo_pattern_fires` drives
the verbatim pgJDBC query through the hook and asserts the
column-row projection. `t7_select_version_dispatches_through_hook`
asserts the canned "PostgreSQL 14.0 (KesselDB 1.0)" text appears
in the wire response. `t7_pgadmin_multi_function_probe` asserts
the pgAdmin connect-probe 4-function shape returns the 4-column
single-row response that completes pgAdmin/DBeaver's connect
wizard.

**What T5+T7 deliberately did NOT do**:
- No `information_schema` views (T6 — next; queries.md §5
  already captures the canonical shapes).
- No engine-side wiring of `LIST_INDEXES_TAG` /
  `LIST_CONSTRAINTS_TAG` admin frames (V1 `EngineHandle` still
  falls back to the default empty-Vec impl; pgJDBC's
  `getIndexInfo` against a real KesselDB instance returns 0
  rows until the in-tree EngineHandle override ships —
  acceptable V1: pgJDBC shows "no indexes" cleanly).
- No real-client smoke against psql `\d` step 3 / DBeaver /
  pgAdmin (T8 — the final hand-test + arc closure).
- No `USAGE.md §9` boundary-line removal (T8).

## T6 + T8 — what landed (2026-05-27, commits `b0d1efc` + `6d50a83`)

Two commits close the SP-PG-CAT V1 arc. T6 ships the
information_schema view synthesizers (Metabase / Tableau / Looker /
Hex entry point) and T8 ships the EngineHandle real impls for
indexes/constraints + the docs closure.

**Commit `b0d1efc` — T6 information_schema view synthesizers:**

- `crates/kessel-pg-gateway/src/pg_catalog/synthesize.rs`:
  - `INFORMATION_SCHEMA_CATALOG="kesseldb"` +
    `INFORMATION_SCHEMA_BASE_TABLE="BASE TABLE"` constants.
  - `information_schema_data_type_for_oid(oid)` — maps PG OID to
    SQL-standard type name (`bigint` / `boolean` / `text` /
    `timestamp with time zone` / `numeric` / `smallint` /
    `integer` / `character varying` / `bytea`). NOT the pg_type
    internal names (`int8` / `bool` / `timestamptz`) — locked
    because BI tools key feature support off this column. Locked
    vs SQL:1999 §11.4.
  - `synthesize_information_schema_tables(engine)` — 12 columns
    per SQL standard (table_catalog / table_schema / table_name /
    table_type / self_referencing_column_name /
    reference_generation / user_defined_type_catalog/schema/name /
    is_insertable_into / is_typed / commit_action). One row per
    Ordinary KesselDB table with `table_type='BASE TABLE'`.
  - `synthesize_information_schema_columns(engine, table_filter)`
    — 12 columns (table_catalog / table_schema / table_name /
    column_name / ordinal_position / column_default / is_nullable /
    data_type / character_maximum_length / numeric_precision /
    numeric_scale / datetime_precision). Optional `table_name =
    '<name>'` filter (Metabase / Tableau per-table introspection
    hot path).
  - `synthesize_information_schema_schemata()` — 7 columns; 3
    rows (pg_catalog / public / information_schema).
  - `synthesize_information_schema_key_column_usage(engine,
    table_filter)` — 9 columns; one row per (FK/UNIQUE
    constraint × column). CHECK skipped per SQL standard (CHECKs
    apply to expressions not columns). FK rows carry
    `position_in_unique_constraint`; UNIQUE rows carry NULL.
  - `synthesize_information_schema_table_constraints(engine,
    table_filter)` — 10 columns; one row per CHECK/UNIQUE/FK with
    SQL-standard `constraint_type` literal `'CHECK'` / `'UNIQUE'` /
    `'FOREIGN KEY'`.
  - `synthesize_information_schema_views()` — 10 columns; 0 rows
    (V1 has no views). DataGrip-tolerated.
  - `synthesize_information_schema_routines()` — 8 columns; 0 rows
    (V1 has no stored procedures). DataGrip / JetBrains tooling
    probes this on connect.

- `crates/kessel-pg-gateway/src/pg_catalog/mod.rs`:
  - 7 new pattern matchers:
    `matches_information_schema_{tables,columns,schemata,
    key_column_usage,table_constraints,views,routines}`.
  - `has_information_schema_relation` word-boundary check —
    prevents over-match on longer relation names (e.g.
    `tables_with_extras` does NOT match `tables`).
  - `extract_information_schema_columns_table_filter` /
    `extract_information_schema_table_name_filter` — parse
    `WHERE table_name = '<name>'` literal clauses.
  - `extract_quoted_after(needle)` — internal helper for the
    table-filter extractors.

**Commit `6d50a83` — T8a EngineHandle real impls + admin frames:**

- `crates/kesseldb-server/src/lib.rs`:
  - `LIST_INDEXES_TAG=0xF5` admin tag constant + SM apply
    handler. Walks `ObjectType.indexes` (Equality, is_unique
    derived from `ot.unique.contains`), `ObjectType.ordered`
    (Range, is_unique=false), `ObjectType.composite` (Composite,
    is_unique=false). Synthetic index names from table + column
    names: `<table>_<col>_idx` for Equality, `_ridx` for Range,
    `<table>_<colA>_<colB>_idx` for Composite. Wire format
    `[u32 count][repeat: u32 name_len, name, u8 kind, u8
    is_unique, u16 field_count, field_count × u32 field_id]`.
  - `LIST_CONSTRAINTS_TAG=0xF4` admin tag constant + SM apply
    handler. Walks `ObjectType.unique` (UNIQUE rows),
    `ObjectType.fks` (FK rows with referenced table name resolved
    via type_id lookup), `ObjectType.checks` (synthetic
    `<table>_check_N` names). Wire format `[u32 count][repeat:
    u32 name_len, name, u8 kind, u8 fk_action, u16 attn_count,
    attn_count × u32 attnum, u32 ref_name_len, ref_name, u16
    ref_attn_count, ref_attn_count × u32 ref_attnum]`.
  - Both handlers engine-thread-local, read-only, no SM mutation —
    mirrors the existing `DESCRIBE_BY_NAME_TAG=0xF7` /
    `LIST_TABLES_TAG=0xF6` admin pattern.
  - `EngineHandle::list_indexes_for_table(name)` + `EngineHandle::
    list_constraints_for_table(name)` impls decode the bytes back
    into `IndexMetadata` / `ConstraintMetadata` structs. Graceful
    empty for unknown tables (pgJDBC `getIndexInfo` shows "no
    indexes" cleanly).

**T8b/c/d — arc-closure docs (this commit):**

- `docs/USAGE.md §9` PostgreSQL clients:
  - Adds a "Supported GUI / admin tools" sub-section listing the
    9 verified tools (psql / pgcli / pgAdmin 4 / DBeaver /
    DataGrip / Metabase / Tableau / Looker / pgJDBC) with the
    per-tool support level (full / connect + browse / connect +
    introspect).
  - Sample interactive psql session showing `\dt` + `\d users` +
    `SELECT version()` + `SELECT * FROM information_schema.tables`
    working end-to-end.
  - Removes the "No `pg_catalog.*` introspection" line from
    Limitations (V1); replaces with the per-V2-deferred-catalog
    list (`pg_proc`, `pg_stat_*`, arbitrary JOIN/GROUP BY, `\d+`
    extended, multi-database, cross-schema).
  - Updates the troubleshooting section's pg_catalog 42P01 entry
    to reflect the new V1 coverage.
  - Adds links to SP-PG-CAT design + progress trackers.

- `docs/ARCHITECTURE.md` "PostgreSQL wire listener" section:
  - Adds a "pg_catalog stubs (SP-PG-CAT — V1 closed)"
    sub-section explaining the dispatch-layer intercept
    architecture + listing all 6 pg_catalog tables + 7
    information_schema views + the SQL helper functions V1
    stubs.
  - Removes the obsolete "pg_catalog.* introspection stubs" V2
    follow-up bullet (it's now V1 of SP-PG-CAT, shipped).

- `docs/STATUS.md`:
  - Arc-closure row at the top noting all 8 slices DONE +
    acceptance criteria met + V2 follow-ups named (each as its
    own SP-PG-CAT-* sub-arc).

- This progress tracker:
  - T6 + T8 rows marked DONE with commit SHAs.
  - This summary section replaces the "Next session pickup"
    note.

**KAT delta: +26 total** (+24 in kessel-pg-gateway for T6, +2 in
kesseldb-server for T8a). Headline KATs:

- `t6_information_schema_tables_metabase_query_fires` — verbatim
  Metabase connect-database query through hook returns the table
  list with `'BASE TABLE'` type.
- `t6_information_schema_columns_emits_sql_standard_data_types`
  — canonical SQL-standard `bigint` / `text` / `timestamp with
  time zone` / `integer` literals appear (NOT pg_type internal
  names).
- `t6_information_schema_columns_filter_by_table_name` —
  per-table filter works.
- `t6_information_schema_schemata_returns_three_schemas` — 3
  canonical schemas with the SQL-standard 7-column shape.
- `t6_information_schema_key_column_usage_lists_fk_columns` —
  FK + UNIQUE rows; CHECK skipped.
- `t6_information_schema_table_constraints_lists_all_with_type`
  — CHECK / UNIQUE / FOREIGN KEY literals all present.
- `t6_information_schema_views_returns_empty` /
  `t6_information_schema_routines_returns_empty` —
  DataGrip-tolerated empty responses.
- `t6_pre_existing_patterns_still_match` — T1-T5+T7 still fire;
  T6 additions PURELY ADDITIVE.
- `t8a_engine_handle_list_indexes_round_trips_via_admin_frame`
  HEADLINE — creates Equality + Range + Composite indexes via SQL
  DDL and asserts the kind-byte mapping survives the SM
  round-trip; Composite carries 2 attnums in the field array.
- `t8a_engine_handle_list_constraints_round_trips_via_admin_frame`
  — UNIQUE-via-`CREATE UNIQUE INDEX` surfaces as
  `ConstraintKind::Unique`.

**Test counts** (validated 2026-05-27):
- kessel-pg-gateway lib: 301 → 325 (+24)
- kesseldb-server pg-gateway: 113 → 115 (+2)
- Workspace default: 1755 → 1779 (+24)
- Workspace `--features kesseldb-server/pg-gateway`: 1781 → 1807 (+26)
- Workspace `--all-features`: 1836 → 1862 (+26)

seed-7 GREEN. tree-grep EMPTY. HTTP/1.1 + WebSocket + binary
protocol surfaces byte-untouched. Default `cargo build -p
kesseldb-server` byte-identical (pg-gateway opt-in feature; T6+T8
additions are entirely behind the feature gate).

**Headline question — does the SP-PG-CAT V1 arc close? YES.**
All 8 slices DONE. All design §8 acceptance criteria met (per
synthetic-peer KATs driving each tool's verbatim
introspection SQL through the catalog hook): psql `\dt` ✓
`\d <t>` ✓ `\dn` ✓ `\di` ✓ pgcli tab-completion ✓ DBeaver
connect ✓ pgAdmin connect ✓ Metabase wizard ✓ Tableau /
Looker / Hex / Superset ✓ pgJDBC `getTables` / `getColumns` /
`getIndexInfo` ✓ no SP-PG V1 regression ✓ no engine changes
affecting HTTP / WebSocket / native protocol surfaces ✓.

**V2 follow-ups (each its own arc, named):**
- T9 (SP-PG-CAT-PROC) — `pg_proc` real function listing
- T10 (SP-PG-CAT-MDB) — `pg_database` multi-database when
  KesselDB grows that
- T11 (SP-PG-CAT-CACHE) — per-query cache invalidated on DDL
  (matters at ≥1000 tables)
- T12 (SP-PG-CAT-STATS) — `pg_stat_*` runtime stats stub→real
- T13 (SP-PG-CAT-COLL) — `pg_collation` real collation table
- T14 — psql `\d+` extended output (joins pg_description +
  pg_indexes detail + pg_stat_user_tables)
- T15 — cross-schema queries when KesselDB grows namespaces
  (depends on SP-NS arc)
- T16 (SP-PG-CAT-AST) — AST-based pattern matcher (replaces the
  regex layer; collapses the ~35-pattern table to ~10
  shape-recognizers via kessel-sql AST walk)

**SP-PG-CAT V1 ARC CLOSED.**

(See sections above for prior T1..T5+T7 records.)

## Prior pickup: T5 — pg_index + pg_constraint (DONE in commit `1004c2f`)

T5 closes the "introspect this schema fully" picture — every PG
GUI tool issues an index + constraint query as part of the
expand-table flow (psql `\d <table>` step 3, pgJDBC `getIndexInfo`
+ `getPrimaryKeys`, DBeaver "Indexes" / "Constraints" tabs). Scope:

- `pg_index` synthesizer — one row per KesselDB index;
  indexrelid = stable hash of index name; indrelid = the indexed
  table's pg_class.oid; indnatts = number of indexed columns;
  indisunique = per index kind; indkey = packed int2vector of
  the column attnums; indisprimary = false (V1 has no primary
  key concept). ~22 columns per row per PG `pg_index.h`; most
  can be canned defaults.
- `pg_constraint` synthesizer — one row per UNIQUE/FK/CHECK;
  contype = 'u' / 'f' / 'c'; synthetic constraint names
  (`<table>_<col>_key` / `<table>_<col>_fkey` / `<table>_check_N`
  matching PG's auto-naming); conrelid = host table pg_class.oid;
  conkey = constrained column attnums (int2vector); confrelid =
  referenced table for FK (0 otherwise); confkey = referenced
  column attnums (NULL for non-FK). ~25 columns per row per PG
  `pg_constraint.h`.
- Dispatcher entries for queries.md §1.6 (`\d <table>` step 3),
  §4.3 (pgJDBC `getIndexInfo`), plus the per-table filter form.
- May need a `EngineApply::list_indexes_for_table(name) ->
  Vec<IndexMetadata>` + `list_constraints_for_table(name) ->
  Vec<ConstraintMetadata>` trait extension if KesselDB carries
  index/constraint metadata that's accessible (else V1 returns
  0 rows — graceful degradation, pgJDBC `getIndexInfo` shows
  "no indexes" cleanly).
- ~10-12 new KATs per design §7 T5 row.

After T5 lands, psql `\d <table>` shows complete output (columns
+ indexes + constraints); pgJDBC `getIndexInfo` returns the right
data; DBeaver's "Indexes" tab populates. After that T6
(information_schema views — Metabase / Tableau / Looker / Hex
unlock) + T7 (SQL helper functions — pgAdmin connect wizard
unlocks) + T8 (real-client smoke + USAGE.md §9 boundary removal)
close the V1 of this arc.

(See §"T1 — what landed" + §"T2 + T3 — what landed" sections
above for the prior records.)
