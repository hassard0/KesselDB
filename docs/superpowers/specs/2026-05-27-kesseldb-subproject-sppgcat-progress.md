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
| **T2** | Query corpus capture — drive psql, pgcli, pgAdmin 4, DBeaver, DataGrip, Metabase, Tableau against a real Postgres server with `log_statement = 'all'` and copy every introspection query into `crates/kessel-pg-gateway/src/pg_catalog/queries.md` (annotated by issuing tool + destination synthesizer). Each captured query becomes a pattern-table entry for T3-T7. T2 is documentation-only — no code change — but defines the contract the next 5 slices implement. Crucially T2 reveals which V1-out-of-scope catalogs each tool actually queries on connect; if pgAdmin queries pg_proc on every connect (vs lazily on click), we may need to widen V1 scope. | OPEN | — |
| **T3** | `EngineApply::list_tables() -> Vec<String>` trait extension (default impl returns empty Vec so existing impls don't break at the T3 boundary) + `kesseldb-server::EngineHandle` impl walking the live `Catalog` returning the user table names + `pg_class` synthesizer (one row per KesselDB table; relnamespace=2200 = public; relkind='r' = ordinary table; OID = stable hash of table name for join-on stability across replicas) + dispatcher entries for the ~5 canonical pg_class query patterns pgAdmin and DBeaver issue (`SELECT oid, relname FROM pg_class WHERE relnamespace = 2200`, `SELECT * FROM pg_catalog.pg_class WHERE relkind = 'r'`, etc.). | OPEN | — |
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

## Next session pickup: T2 — query corpus capture

T2 is documentation-only — no code change, 0 KATs. The
deliverable is `crates/kessel-pg-gateway/src/pg_catalog/queries.md`
with the introspection queries every common GUI tool issues on
connect, annotated by:

- Issuing tool (pgAdmin 4 / DBeaver / DataGrip / Metabase / Tableau / …)
- Query SQL (verbatim)
- Destination synthesizer slot (pg_namespace / pg_class /
  pg_attribute / pg_type / pg_index / pg_constraint /
  information_schema.tables / information_schema.columns /
  helper-fn) — informs T3-T7 implementation order
- V1-out-of-scope flag (queries against pg_proc / pg_authid /
  pg_database / pg_settings / pg_stat_* go here; informs whether
  any of these tools require an out-of-scope catalog to be
  non-empty before the connect wizard completes)

Approach: drive each tool against a real Postgres server with
`log_statement = 'all'` set in `postgresql.conf`, perform the
"connect + expand schema tree + click on a table to see columns"
flow, then copy the issued queries out of the Postgres log.
Cross-reference against tool-source-code (most are open source)
to confirm we caught the full sweep.

After T2 lands, T3-T7 ship the per-table synthesizer + the
pattern-table entries to match each captured query. Each slice
is one synthesizer + ~5-10 pattern entries + +10-20 KATs
(pattern-fires positive case, synthesizer-shape KATs, OID
canonical-value locks, edge cases per the synthesized table's
column list).
