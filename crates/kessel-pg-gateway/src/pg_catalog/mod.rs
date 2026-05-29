//! SP-PG-CAT — `pg_catalog.*` + `information_schema.*` introspection
//! stubs for the PG-wire gateway.
//!
//! **T1 status (this commit):** module declaration + `pg_namespace`
//! synthesizer + `catalog_query_hook` dispatcher recognizing the
//! `SELECT * FROM pg_catalog.pg_namespace` shape. The hook returns
//! `None` for every other SQL, which means existing dispatch paths
//! are unchanged for non-pg_catalog SQL.
//!
//! ## What this module does
//!
//! - `catalog_query_hook(sql, engine) -> Option<Vec<u8>>` — runs
//!   BEFORE `engine.apply_sql` in `dispatch::dispatch_query`. If the
//!   SQL matches a known pg_catalog / information_schema / known-
//!   function pattern, returns `Some(wire_response_bytes)` (a full
//!   `T + D* + C + Z` byte stream the gateway writes to the wire
//!   verbatim). If the SQL is anything else, returns `None` and the
//!   existing `engine.apply_sql` path runs unchanged.
//! - `normalize_for_match(sql) -> String` — case-folded, leading-
//!   comment-stripped, whitespace-collapsed view of the SQL for the
//!   pattern matcher. Cheap (O(sql.len())); only invoked once per Q.
//!
//! ## What this module does NOT do (yet — named V2 follow-ups)
//!
//! - **T2** — capture the actual queries pgAdmin / DBeaver /
//!   DataGrip / Metabase issue on connect into
//!   `pg_catalog/queries.md`. T1 ships the dispatcher infrastructure;
//!   T2 grows the pattern corpus.
//! - **T3** — `EngineApply::list_tables() -> Vec<String>` trait
//!   extension + `pg_class` synthesizer + ~5 canonical pg_class
//!   query patterns. T1's synthesizer is read-only against the
//!   catalog via existing `describe_table`; T3 widens to enumerate
//!   tables.
//! - **T4** — `pg_attribute` + `pg_type` synthesizers.
//! - **T5** — `pg_index` + `pg_constraint` synthesizers.
//! - **T6** — `information_schema.tables` + `information_schema.columns`
//!   view synthesizers.
//! - **T7** — SQL helper functions: `version()`, `current_database()`,
//!   `current_schema()`, `pg_my_temp_schema()`, `pg_is_other_temp_schema(oid)`,
//!   `obj_description(oid)`, `pg_get_constraintdef(oid)`,
//!   `pg_get_indexdef(oid)`, `pg_table_is_visible(oid)`,
//!   `pg_encoding_to_char(enc)`.
//! - **T8** — real-client smoke (psql `\dt`, pgcli tab-completion,
//!   DBeaver Connect wizard, pgAdmin Add Server wizard) + USAGE.md
//!   §9 update removing the V1-boundary line + STATUS.md row.
//!
//! ## Why a dispatcher hook, not engine support
//!
//! pg_catalog is a Postgres-protocol-specific concept; KesselDB's
//! catalog has nothing called "pg_catalog" and shouldn't. Adding
//! fake virtual tables to the engine would require every other
//! wire surface (HTTP `/v1/sql`, WebSocket binary, native client)
//! to either expose them (wrong — those surfaces don't pretend to
//! be Postgres) or filter them out (every filter site is a bug
//! surface). The hook sits ABOVE `engine.apply_sql` in
//! `dispatch::dispatch_query` so it's the only site that knows
//! about pg_catalog.
//!
//! ## Read-only invariant
//!
//! The hook signature takes `&dyn EngineApply` (immutable). The
//! synthesizer can only call `describe_table` (already read-only
//! per V1 T8) and (when T3 lands) `list_tables` (also read-only).
//! No code path in this module mutates the engine, journals an op,
//! or causes a scatter-scan.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-27-kesseldb-sppgcat-pg-catalog-design.md`

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::engine::EngineApply;

pub mod synthesize;

/// Canonical PG OID for the `pg_catalog` schema (locked vs
/// `src/include/catalog/pg_namespace.dat`). Every pg_catalog row's
/// `nspname` references this OID.
pub const PG_NAMESPACE_OID_PG_CATALOG: u32 = 11;

/// Canonical PG OID for the `public` schema (locked vs
/// `src/include/catalog/pg_namespace.dat`). Every KesselDB user table
/// lives here for V1 of this arc.
pub const PG_NAMESPACE_OID_PUBLIC: u32 = 2200;

/// Canonical PG OID for the `information_schema` namespace (locked
/// vs `src/include/catalog/pg_namespace.dat`).
pub const PG_NAMESPACE_OID_INFORMATION_SCHEMA: u32 = 2202;

/// Canonical PG OID for the `postgres` (super-)user — the per-row
/// owner OID we stamp on every synthesized pg_class / pg_namespace
/// row in V1 of this arc (KesselDB doesn't have a per-user model
/// yet, so every row's owner is the same canonical OID).
pub const PG_AUTHID_OID_POSTGRES: u32 = 10;

/// Top-level hook called from `dispatch::dispatch_query` BEFORE the
/// `engine.apply_sql` path. Returns:
///
/// - `Some(bytes)` — the SQL matched a pg_catalog / information_schema
///   / known-function pattern. `bytes` is the complete wire response
///   stream (`RowDescription + DataRow* + CommandComplete + ReadyForQuery`).
///   The caller writes it to the wire verbatim.
/// - `None` — the SQL did not match any pattern. The caller falls
///   through to the existing `engine.apply_sql` path.
///
/// **Fast-path invariant:** the hook is O(1) in catalog size for
/// the pattern-match step (we early-reject SQL that doesn't start
/// with `SELECT`; the pattern table is scanned linearly but is
/// small). Only the SELECTED synthesizer touches the live catalog
/// (via `engine.describe_table`); the dispatch step itself is
/// pattern-match-only.
pub fn catalog_query_hook<E: EngineApply + ?Sized>(
    sql: &str,
    engine: &E,
) -> Option<Vec<u8>> {
    let normalized = normalize_for_match(sql);
    // SP-PG-CAT T7 — `SHOW <name>` recognizer (separate top-level SQL
    // statement, not a function call — PG treats it specially). Checked
    // BEFORE the SELECT fast-reject because SHOW isn't a SELECT.
    if normalized.starts_with("show ") || normalized == "show all" {
        return synthesize::synthesize_helper_function(&normalized);
    }
    // Fast reject — only SELECT statements hit pg_catalog. Saves us
    // from running the pattern table on every INSERT / UPDATE / DDL.
    if !normalized.starts_with("select") {
        return None;
    }
    // SP-PG-CAT T7 — single-call helper-function recognizer (version(),
    // current_database(), pg_get_userbyid(N), …). Checked BEFORE the
    // table-pattern matchers because helpers are simpler shapes and
    // tools issue them as the first probe on connect (queries.md §6).
    if let Some(bytes) = synthesize::synthesize_helper_function(&normalized) {
        return Some(bytes);
    }
    // T1: `SELECT * FROM pg_catalog.pg_namespace` (canned 3-row).
    if matches_pg_namespace_select_star(&normalized) {
        return Some(synthesize::pg_namespace_all_rows());
    }
    // T3: `SELECT * FROM pg_catalog.pg_class` (one row per live table).
    if matches_pg_class_select_star(&normalized) {
        return Some(synthesize::pg_class_all_rows(engine));
    }
    // T3: psql `\dt` (canonical JOIN — design §3.4 strategy A).
    if matches_psql_dt_canonical(&normalized) {
        return Some(synthesize::psql_dt_joined_rows(engine));
    }
    // T4: `SELECT * FROM pg_catalog.pg_attribute` (all columns).
    if matches_pg_attribute_select_star(&normalized) {
        return Some(synthesize::synthesize_pg_attribute(engine, None));
    }
    // T4: `SELECT * FROM pg_catalog.pg_attribute WHERE attrelid = N`.
    if let Some(oid) = extract_attrelid_filter(&normalized) {
        return Some(synthesize::synthesize_pg_attribute(engine, Some(oid)));
    }
    // T4: psql `\d <table>` step-2 column-list query (queries.md §1.5).
    if let Some(oid) = extract_psql_d_table_oid(&normalized) {
        return Some(synthesize::psql_d_table_joined_rows(engine, oid));
    }
    // T4: `SELECT * FROM pg_catalog.pg_type` (all canned rows).
    if matches_pg_type_select_star(&normalized) {
        return Some(synthesize::synthesize_pg_type());
    }
    // T4: `SELECT ... FROM pg_catalog.pg_type WHERE oid = N` (per-OID).
    if let Some(oid) = extract_pg_type_oid_filter(&normalized) {
        return Some(synthesize::synthesize_pg_type_by_oid(oid));
    }
    // T4: pgJDBC `getColumns` (queries.md §4.2 — large JOIN).
    if let Some(name) = extract_pgjdbc_getcolumns_relname(&normalized) {
        return Some(synthesize::pgjdbc_getcolumns_joined_rows(engine, &name));
    }
    // T5: `SELECT * FROM pg_catalog.pg_index` (all indexes).
    if matches_pg_index_select_star(&normalized) {
        return Some(synthesize::synthesize_pg_index(engine, None));
    }
    // T5: `SELECT * FROM pg_catalog.pg_index WHERE indrelid = N`
    // (psql `\d <table>` step 3 + the per-table filter form).
    if let Some(oid) = extract_indrelid_filter(&normalized) {
        return Some(synthesize::synthesize_pg_index(engine, Some(oid)));
    }
    // T5: psql `\d <table>` step 3 — canonical JOIN against pg_class
    // + pg_index + LEFT JOIN pg_constraint (queries.md §1.6). Anchors
    // on the unique fixture and extracts the table OID from the
    // `c.oid = '<oid>'` clause.
    if let Some(oid) = extract_psql_d_index_step_oid(&normalized) {
        return Some(synthesize::synthesize_pg_index(engine, Some(oid)));
    }
    // T5: pgJDBC `getIndexInfo` (queries.md §4.3 — large JOIN with
    // `ct.relname = '<table>'`).
    if let Some(name) = extract_pgjdbc_getindexinfo_relname(&normalized) {
        return Some(synthesize::pgjdbc_getindexinfo_joined_rows(engine, &name));
    }
    // T5: `SELECT * FROM pg_catalog.pg_constraint` (all constraints).
    if matches_pg_constraint_select_star(&normalized) {
        return Some(synthesize::synthesize_pg_constraint(engine, None));
    }
    // T5: `SELECT * FROM pg_catalog.pg_constraint WHERE conrelid = N`
    // (psql `\d <table>` constraint-section + per-table filter form).
    if let Some(oid) = extract_conrelid_filter(&normalized) {
        return Some(synthesize::synthesize_pg_constraint(engine, Some(oid)));
    }
    // T6: `information_schema.tables` (Metabase / Tableau / Looker
    // connect-database wizard).
    if matches_information_schema_tables(&normalized) {
        return Some(synthesize::synthesize_information_schema_tables(engine));
    }
    // T6: `information_schema.columns` — with optional `table_name =
    // '<name>'` filter (Metabase / Tableau per-table introspection
    // hot path).
    if matches_information_schema_columns(&normalized) {
        let filter = extract_information_schema_columns_table_filter(&normalized);
        return Some(synthesize::synthesize_information_schema_columns(
            engine,
            filter.as_deref(),
        ));
    }
    // T6: `information_schema.schemata` (Metabase / Tableau /
    // dbt-postgres schema-list).
    if matches_information_schema_schemata(&normalized) {
        return Some(synthesize::synthesize_information_schema_schemata());
    }
    // T6: `information_schema.key_column_usage` (PK + FK column
    // discovery).
    if matches_information_schema_key_column_usage(&normalized) {
        let filter = extract_information_schema_table_name_filter(&normalized);
        return Some(synthesize::synthesize_information_schema_key_column_usage(
            engine,
            filter.as_deref(),
        ));
    }
    // T6: `information_schema.table_constraints` (PK / FK / UNIQUE /
    // CHECK enumeration).
    if matches_information_schema_table_constraints(&normalized) {
        let filter = extract_information_schema_table_name_filter(&normalized);
        return Some(synthesize::synthesize_information_schema_table_constraints(
            engine,
            filter.as_deref(),
        ));
    }
    // T6: `information_schema.views` (well-framed empty — KesselDB
    // V1 has no views).
    if matches_information_schema_views(&normalized) {
        return Some(synthesize::synthesize_information_schema_views());
    }
    // T6: `information_schema.routines` (well-framed empty — V1 has
    // no stored procedures; DataGrip / JetBrains query this on
    // connect).
    if matches_information_schema_routines(&normalized) {
        return Some(synthesize::synthesize_information_schema_routines());
    }
    // T8 (real-psql): psql `\d <name>` STEP 1 — the table-OID lookup
    // query. Uses the regex operator `OPERATOR(pg_catalog.~) '^(<n>)$'`,
    // which KesselDB's SQL parser doesn't accept; without this matcher
    // the user gets `ERROR: sql: unexpected char '~'` on every `\d <t>`
    // invocation. Synthesizer emits (oid, nspname, relname) for the
    // table whose name matches the regex anchor exactly (psql always
    // submits `^(<exact>)$` for a simple `\d foo`). 0 rows if the name
    // doesn't exist — psql then prints "Did not find any relation named ...".
    if let Some(name) = extract_psql_d_step1_relname(&normalized) {
        return Some(synthesize::psql_d_step1_oid_lookup(engine, &name));
    }
    // T8 (real-psql): psql `\d <name>` STEP 2 — the per-OID relation
    // attribute summary. psql ships a 15-column projection from
    // `pg_class c LEFT JOIN pg_am am` filtered by `c.oid = '<oid>'`.
    // The query contains `::pg_catalog.regtype::pg_catalog.text` cast
    // syntax that KesselDB's SQL parser rejects with `unexpected char
    // ':'`. Synthesizer emits one canned row of `pg_class` defaults
    // (relchecks=0, relkind='r', relhasindex='f', etc.) — psql uses
    // these to drive the "Table" header text, the "Indexes:" /
    // "Foreign-key constraints:" section toggles, and so on.
    if let Some(oid) = extract_psql_d_step2_oid(&normalized) {
        return Some(synthesize::psql_d_step2_relsummary(engine, oid));
    }
    // T8 (real-psql): psql `\d <name>` policy-list (pg_policy). psql
    // unconditionally polls for row-level-security policies on every
    // table after the column list. KesselDB V1 has no RLS — return a
    // well-framed empty response. Without this matcher the query
    // would error with `expected FROM` (subselects in projection).
    if matches_psql_d_pg_policy(&normalized) {
        return Some(synthesize::psql_d_pg_policy_empty());
    }
    // T8 (real-psql): psql `\d <name>` inheritance-list (pg_inherits).
    // Empty for non-partitioned V1 tables.
    if matches_psql_d_pg_inherits(&normalized) {
        return Some(synthesize::psql_d_pg_inherits_empty());
    }
    // T8 (real-psql): psql `\d <name>` trigger-list (pg_trigger).
    // V1 has no engine-level triggers stored in pg_trigger.
    if matches_psql_d_pg_trigger(&normalized) {
        return Some(synthesize::psql_d_pg_trigger_empty());
    }
    // T8 (real-psql): psql `\d <name>` extended-statistics
    // (pg_statistic_ext). PG 16 added this poll. KesselDB V1 has no
    // engine-level multi-column statistics.
    if matches_psql_d_pg_statistic_ext(&normalized) {
        return Some(synthesize::psql_d_pg_statistic_ext_empty());
    }
    // T8 (real-psql): psql `\d <name>` publication-list
    // (pg_publication / pg_publication_rel). KesselDB V1 has no
    // logical replication publications.
    if matches_psql_d_pg_publication(&normalized) {
        return Some(synthesize::psql_d_pg_publication_empty());
    }
    // T8 (real-psql): psql `\d <name>` foreign-table info
    // (pg_foreign_table). KesselDB V1 has no foreign-data wrappers.
    if matches_psql_d_pg_foreign_table(&normalized) {
        return Some(synthesize::psql_d_pg_foreign_table_empty());
    }
    // T8 (real-psql): psql `\dn` schema-list query. Uses the negated
    // regex operator `n.nspname !~ '^pg_'` — same parser-rejection
    // problem as `\d <name>`. Always returns the public schema (V1
    // has no other user schemas).
    if matches_psql_dn(&normalized) {
        return Some(synthesize::psql_dn_schema_list());
    }
    None
}

/// Normalizes SQL for pattern matching:
///
/// - Strip leading whitespace.
/// - Strip leading `-- ...` line comments + `/* ... */` block comments
///   (single-pass, repeated until first non-comment token).
/// - Lowercase the entire string (PG identifiers are case-insensitive
///   unless quoted; pg_catalog tables are never quoted in tool SQL).
/// - Collapse runs of internal whitespace to single spaces.
/// - Strip trailing `;` + whitespace.
///
/// Output is cheap to compare against pattern predicates. Cost is
/// O(sql.len()); no allocations beyond the output String.
pub fn normalize_for_match(sql: &str) -> String {
    let stripped = strip_leading_comments(sql);
    let lower: String = stripped.to_ascii_lowercase();
    // Collapse whitespace.
    let mut out = String::with_capacity(lower.len());
    let mut prev_space = false;
    for c in lower.chars() {
        if c.is_whitespace() {
            if !prev_space {
                out.push(' ');
            }
            prev_space = true;
        } else {
            out.push(c);
            prev_space = false;
        }
    }
    // Trim trailing whitespace + semicolons.
    out.trim()
        .trim_end_matches(';')
        .trim()
        .to_string()
}

/// Strip leading `-- ...\n` line comments and `/* ... */` block
/// comments + whitespace. Returns the substring starting at the
/// first non-comment, non-whitespace character. Mirrors the helper
/// in `dispatch::strip_leading_comments_and_whitespace` but exposed
/// here because the pg_catalog hook needs to normalize SQL BEFORE
/// the dispatch comment-strip runs (dispatch's strip is private).
fn strip_leading_comments(sql: &str) -> &str {
    let mut s = sql;
    loop {
        let trimmed = s.trim_start();
        if trimmed.starts_with("--") {
            match trimmed.find('\n') {
                Some(p) => s = &trimmed[p + 1..],
                None => return "",
            }
            continue;
        }
        if trimmed.starts_with("/*") {
            match trimmed[2..].find("*/") {
                Some(p) => s = &trimmed[2 + p + 2..],
                None => return "",
            }
            continue;
        }
        return trimmed;
    }
}

/// Recognize the `SELECT * FROM pg_catalog.pg_namespace` shape (and
/// the unqualified `SELECT * FROM pg_namespace` form clients also
/// issue). Accepts an optional trailing `;` (already stripped by
/// `normalize_for_match`) and optional `WHERE` clauses (V1 returns
/// all 3 rows regardless — clients filter client-side if needed).
fn matches_pg_namespace_select_star(normalized: &str) -> bool {
    // The normalized SQL is lowercase + whitespace-collapsed. Match
    // BOTH the qualified (`pg_catalog.pg_namespace`) and unqualified
    // (`pg_namespace`) forms — pgcli + psql issue qualified;
    // information_schema-style tools sometimes drop the schema.
    normalized == "select * from pg_catalog.pg_namespace"
        || normalized == "select * from pg_namespace"
        || normalized.starts_with("select * from pg_catalog.pg_namespace where ")
        || normalized.starts_with("select * from pg_namespace where ")
}

/// SP-PG-CAT T3 — recognize `SELECT * FROM pg_catalog.pg_class`
/// (and the unqualified `pg_class` form clients also issue, since
/// `pg_catalog` is in every connection's implicit search_path).
/// Tolerates a trailing WHERE clause but emits the full table list
/// regardless — clients filter client-side on the returned rows.
fn matches_pg_class_select_star(normalized: &str) -> bool {
    normalized == "select * from pg_catalog.pg_class"
        || normalized == "select * from pg_class"
        || normalized.starts_with("select * from pg_catalog.pg_class where ")
        || normalized.starts_with("select * from pg_class where ")
}

/// SP-PG-CAT T3 — recognize the canonical psql `\dt` query (from
/// `src/bin/psql/describe.c::listTables`). The query is a JOIN of
/// `pg_class` + `pg_namespace` with a CASE on `relkind` and a
/// `pg_table_is_visible(oid)` filter — V1 doesn't run arbitrary
/// JOINs, so we match the canonical form byte-for-byte (after
/// `normalize_for_match` lowercases + collapses whitespace) and
/// synthesize the joined-result rows directly per design §3.4
/// strategy A. The pattern is tolerant of the two `relkind` filter
/// shapes psql ships across PG 12/13/14 (`('r','p','')` vs
/// `('r','p','v','m','s','f','')`) — both are subsumed by checking
/// only the leading + trailing fixtures.
fn matches_psql_dt_canonical(normalized: &str) -> bool {
    // Required leading fixture — the SELECT projection psql emits.
    let leading = "select n.nspname as \"schema\", c.relname as \"name\",";
    // Required core fixture — the FROM + JOIN clause shape.
    let core = "from pg_catalog.pg_class c left join pg_catalog.pg_namespace n on n.oid = c.relnamespace";
    // Required trailing fixture — the visibility filter + ORDER BY.
    let trailing_filter = "and pg_catalog.pg_table_is_visible(c.oid)";
    normalized.starts_with(leading)
        && normalized.contains(core)
        && normalized.contains(trailing_filter)
}

/// SP-PG-CAT T4 — recognize `SELECT * FROM pg_catalog.pg_attribute`
/// (and the unqualified `pg_attribute` form). Tolerates a trailing
/// `WHERE` clause that DOESN'T look like the parameterized
/// `attrelid = <oid>` form (that one's handled by
/// `extract_attrelid_filter` instead).
fn matches_pg_attribute_select_star(normalized: &str) -> bool {
    normalized == "select * from pg_catalog.pg_attribute"
        || normalized == "select * from pg_attribute"
}

/// SP-PG-CAT T4 — recognize the parameterized `SELECT * FROM
/// pg_catalog.pg_attribute WHERE attrelid = N` shape (and the
/// unqualified form). Returns the OID if matched. The common psql
/// `\d <table>` step / pgJDBC `getColumns` cases come here.
fn extract_attrelid_filter(normalized: &str) -> Option<u32> {
    let prefixes = [
        "select * from pg_catalog.pg_attribute where attrelid = ",
        "select * from pg_attribute where attrelid = ",
        "select * from pg_catalog.pg_attribute where a.attrelid = ",
        "select * from pg_attribute where a.attrelid = ",
    ];
    for p in prefixes {
        if let Some(rest) = normalized.strip_prefix(p) {
            return parse_leading_u32(rest);
        }
    }
    None
}

/// SP-PG-CAT T4 — recognize the canonical psql `\d <table>` step-2
/// column-list query (queries.md §1.5). The query SELECTs from
/// `pg_attribute` with a `WHERE a.attrelid = '<oid>'` clause (the
/// OID is quoted in psql because PG's parser accepts the literal
/// either way). Returns the extracted OID if the query matches the
/// canonical shape.
fn extract_psql_d_table_oid(normalized: &str) -> Option<u32> {
    // Anchor on the leading fixture psql emits for `\d <table>`
    // step 2: `SELECT a.attname,` then later `FROM pg_catalog.
    // pg_attribute a WHERE a.attrelid = '<oid>'`.
    let leading = "select a.attname,";
    let core = "from pg_catalog.pg_attribute a where a.attrelid = ";
    if !normalized.starts_with(leading) {
        return None;
    }
    let pos = normalized.find(core)?;
    let after = &normalized[pos + core.len()..];
    // OID may be quoted (`'16385'`) or unquoted (`16385`); psql ships
    // the quoted form per `describe.c::describeOneTableDetails`.
    let after = after.strip_prefix('\'').unwrap_or(after);
    parse_leading_u32(after)
}

/// SP-PG-CAT T4 — recognize `SELECT * FROM pg_catalog.pg_type`
/// (and the unqualified form).
fn matches_pg_type_select_star(normalized: &str) -> bool {
    normalized == "select * from pg_catalog.pg_type"
        || normalized == "select * from pg_type"
}

/// SP-PG-CAT T4 — recognize `SELECT ... FROM pg_catalog.pg_type
/// WHERE oid = N` (the per-OID lookup form used by JDBC's column-
/// type resolution path). Returns the OID. Tolerates a few projection
/// shapes (`*`, `typname`, `typname, typlen, typbyval` etc.).
fn extract_pg_type_oid_filter(normalized: &str) -> Option<u32> {
    // Anchor on `from pg_catalog.pg_type` (qualified or unqualified)
    // followed by ` where oid = N`. The projection between SELECT and
    // FROM is variable, so we substring-match.
    let from_qualified = " from pg_catalog.pg_type where oid = ";
    let from_unqualified = " from pg_type where oid = ";
    let from_qualified_aliased = " from pg_catalog.pg_type t where t.oid = ";
    let from_unqualified_aliased = " from pg_type t where t.oid = ";
    if !normalized.starts_with("select ") {
        return None;
    }
    for marker in [
        from_qualified,
        from_unqualified,
        from_qualified_aliased,
        from_unqualified_aliased,
    ] {
        if let Some(pos) = normalized.find(marker) {
            let after = &normalized[pos + marker.len()..];
            return parse_leading_u32(after);
        }
    }
    None
}

/// SP-PG-CAT T4 — recognize the pgJDBC `getColumns` canonical query
/// (queries.md §4.2). The query is a large JOIN that ends with
/// `c.relname LIKE '<table>'`. Returns the extracted table name on
/// match.
///
/// The pgJDBC query body is too long to byte-match; we anchor on
/// the canonical fixture (the `row_number() OVER (PARTITION BY
/// a.attrelid` projection, distinctive to pgJDBC), then scan for
/// `c.relname like '<name>'` and extract the name.
fn extract_pgjdbc_getcolumns_relname(normalized: &str) -> Option<String> {
    // The pgJDBC getColumns query carries the distinctive
    // `row_number() over (partition by a.attrelid` fixture. The
    // canonical SELECT starts with `select * from ( select`.
    if !normalized.contains("row_number() over (partition by a.attrelid")
        && !normalized.contains("row_number() over(partition by a.attrelid")
    {
        return None;
    }
    // The `c.relname like '<table>'` clause is the table-name
    // extraction point. Match LIKE / = (both common).
    let needles = ["c.relname like '", "c.relname = '"];
    for needle in needles {
        if let Some(pos) = normalized.find(needle) {
            let after = &normalized[pos + needle.len()..];
            // The name ends at the next `'`.
            if let Some(end) = after.find('\'') {
                return Some(after[..end].to_string());
            }
        }
    }
    None
}

/// SP-PG-CAT T5 — recognize `SELECT * FROM pg_catalog.pg_index`
/// (qualified + unqualified). Tolerates a trailing WHERE clause that
/// DOESN'T look like the parameterized `indrelid = N` form (that one
/// is handled by `extract_indrelid_filter`).
fn matches_pg_index_select_star(normalized: &str) -> bool {
    normalized == "select * from pg_catalog.pg_index"
        || normalized == "select * from pg_index"
}

/// SP-PG-CAT T5 — recognize the parameterized `SELECT * FROM
/// pg_catalog.pg_index WHERE indrelid = N` shape. Returns the OID
/// if matched. Common psql `\d <table>` step 3 + pgJDBC
/// `getIndexInfo` paths come here.
fn extract_indrelid_filter(normalized: &str) -> Option<u32> {
    let prefixes = [
        "select * from pg_catalog.pg_index where indrelid = ",
        "select * from pg_index where indrelid = ",
        "select * from pg_catalog.pg_index where i.indrelid = ",
        "select * from pg_index where i.indrelid = ",
    ];
    for p in prefixes {
        if let Some(rest) = normalized.strip_prefix(p) {
            return parse_leading_u32(rest);
        }
    }
    None
}

/// SP-PG-CAT T5 — recognize the canonical psql `\d <table>` step 3
/// index-list query (queries.md §1.6). The query has the
/// distinctive fixture `from pg_catalog.pg_class c, pg_catalog.
/// pg_class c2, pg_catalog.pg_index i` with `c.oid = '<oid>'` as
/// the table filter. Returns the OID if matched.
fn extract_psql_d_index_step_oid(normalized: &str) -> Option<u32> {
    // Anchor on the unique multi-pg_class FROM clause that distinguishes
    // the step-3 query from step-1 / step-2.
    let leading = "select c2.relname,";
    let core = "from pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i";
    if !normalized.starts_with(leading) {
        return None;
    }
    if !normalized.contains(core) {
        return None;
    }
    // Find `c.oid = '<oid>'`.
    let needle = "c.oid = '";
    if let Some(pos) = normalized.find(needle) {
        let after = &normalized[pos + needle.len()..];
        return parse_leading_u32(after);
    }
    // Unquoted form `c.oid = N`.
    let unquoted = "c.oid = ";
    if let Some(pos) = normalized.find(unquoted) {
        let after = &normalized[pos + unquoted.len()..];
        return parse_leading_u32(after);
    }
    None
}

/// SP-PG-CAT T5 — recognize the pgJDBC `getIndexInfo` canonical
/// query (queries.md §4.3). The query JOINs pg_class+pg_namespace+
/// pg_index+pg_class(idx)+pg_am with `ct.relname = '<table>'`.
/// Returns the table name on match.
fn extract_pgjdbc_getindexinfo_relname(normalized: &str) -> Option<String> {
    // Anchor on a distinctive fixture: pgJDBC uses
    // `information_schema._pg_expandarray(i.indkey)` to expand indkey.
    if !normalized.contains("information_schema._pg_expandarray(i.indkey)") {
        return None;
    }
    let needles = ["ct.relname = '", "ct.relname like '"];
    for needle in needles {
        if let Some(pos) = normalized.find(needle) {
            let after = &normalized[pos + needle.len()..];
            if let Some(end) = after.find('\'') {
                return Some(after[..end].to_string());
            }
        }
    }
    None
}

/// SP-PG-CAT T5 — recognize `SELECT * FROM pg_catalog.pg_constraint`
/// (qualified + unqualified).
fn matches_pg_constraint_select_star(normalized: &str) -> bool {
    normalized == "select * from pg_catalog.pg_constraint"
        || normalized == "select * from pg_constraint"
}

/// SP-PG-CAT T5 — recognize the parameterized `SELECT * FROM
/// pg_catalog.pg_constraint WHERE conrelid = N` shape. Returns the
/// OID. Used by psql `\d <table>` constraint-section + JDBC's
/// `getPrimaryKeys` paths.
fn extract_conrelid_filter(normalized: &str) -> Option<u32> {
    let prefixes = [
        "select * from pg_catalog.pg_constraint where conrelid = ",
        "select * from pg_constraint where conrelid = ",
        "select * from pg_catalog.pg_constraint where c.conrelid = ",
        "select * from pg_constraint where c.conrelid = ",
        "select * from pg_catalog.pg_constraint where con.conrelid = ",
    ];
    for p in prefixes {
        if let Some(rest) = normalized.strip_prefix(p) {
            return parse_leading_u32(rest);
        }
    }
    None
}

// ─── SP-PG-CAT T6 — information_schema matchers ─────────────────────────────
//
// information_schema is the SQL-standard catalog. Tools issue
// SELECTs against it instead of pg_catalog when they want to be
// vendor-neutral (Metabase, Tableau, Looker, Hex, Superset,
// dbt-postgres, sqlmesh). The matcher style mirrors the pg_catalog
// shape — fast-reject on a starting fixture, then match a small
// table of canonical patterns.
//
// All matchers below assume normalized SQL (lowercase + collapsed
// whitespace + leading-comment-stripped + trailing-semi-stripped).
// The matcher functions return `bool` for "table-name" matches
// (the synthesizer fires unconditionally) or `Option<String>` for
// extract-table-name shapes (the synthesizer filters to the named
// table).

/// SP-PG-CAT T6 — recognize `SELECT ... FROM information_schema
/// .tables ...`. The projection is variable (Metabase emits 4
/// columns, Tableau emits 2, Looker emits a custom set); we anchor
/// on the FROM clause + ignore the projection differences.
fn matches_information_schema_tables(normalized: &str) -> bool {
    has_information_schema_relation(normalized, "tables")
}

/// SP-PG-CAT T6 — recognize `SELECT ... FROM information_schema
/// .columns ...`.
fn matches_information_schema_columns(normalized: &str) -> bool {
    has_information_schema_relation(normalized, "columns")
}

/// SP-PG-CAT T6 — recognize `SELECT ... FROM information_schema
/// .schemata ...`.
fn matches_information_schema_schemata(normalized: &str) -> bool {
    has_information_schema_relation(normalized, "schemata")
}

/// SP-PG-CAT T6 — recognize `SELECT ... FROM information_schema
/// .key_column_usage ...`.
fn matches_information_schema_key_column_usage(normalized: &str) -> bool {
    has_information_schema_relation(normalized, "key_column_usage")
}

/// SP-PG-CAT T6 — recognize `SELECT ... FROM information_schema
/// .table_constraints ...`.
fn matches_information_schema_table_constraints(normalized: &str) -> bool {
    has_information_schema_relation(normalized, "table_constraints")
}

/// SP-PG-CAT T6 — recognize `SELECT ... FROM information_schema
/// .views ...`.
fn matches_information_schema_views(normalized: &str) -> bool {
    has_information_schema_relation(normalized, "views")
}

/// SP-PG-CAT T6 — recognize `SELECT ... FROM information_schema
/// .routines ...`.
fn matches_information_schema_routines(normalized: &str) -> bool {
    has_information_schema_relation(normalized, "routines")
}

/// Internal helper — true iff the normalized SQL contains
/// `from information_schema.<relation>` as a token sequence.
/// Tolerates trailing `,` / `where` / `order` / `;` / end-of-string
/// so we don't over-match on a longer relation name (e.g.
/// `tables_with_extras` would NOT match `tables`).
fn has_information_schema_relation(normalized: &str, relation: &str) -> bool {
    let needle = format!("from information_schema.{relation}");
    let Some(pos) = normalized.find(&needle) else {
        return false;
    };
    // Char after needle must be word-boundary (space, ; , end-of-string).
    let after = &normalized[pos + needle.len()..];
    after.is_empty()
        || after.starts_with(' ')
        || after.starts_with(',')
        || after.starts_with(';')
}

/// SP-PG-CAT T8 (real-psql) — recognize the canonical psql `\d <name>`
/// STEP 1 query.
///
/// The query (PG 14 `src/bin/psql/describe.c::describeOneTableDetails`)
/// looks up the table OID by matching `c.relname` against a regex
/// anchor:
///
/// ```text
/// SELECT c.oid, n.nspname, c.relname
/// FROM pg_catalog.pg_class c
///   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
/// WHERE c.relname OPERATOR(pg_catalog.~) '^(<name>)$' COLLATE pg_catalog.default
///   AND pg_catalog.pg_table_is_visible(c.oid)
/// ORDER BY 2, 3;
/// ```
///
/// V1 KesselDB's SQL parser doesn't accept `OPERATOR(...)`, so without
/// this matcher every `\d <name>` produces `ERROR: sql: unexpected
/// char '~'`. The fix anchors on the unique fixture (the canonical
/// `pg_class` + `pg_namespace` LEFT JOIN with the regex operator) and
/// extracts the bare table name from `^(<name>)$`. Returns `None` if
/// the regex is non-trivial (e.g. user typed `\d 'pattern.*'` — V2,
/// not V1).
///
/// Returns the lowercase table name (psql already lowercases unquoted
/// identifiers before sending; the `normalize_for_match` lowercase
/// happens too).
fn extract_psql_d_step1_relname(normalized: &str) -> Option<String> {
    // Required leading fixture — the exact SELECT projection psql ships.
    let leading = "select c.oid, n.nspname, c.relname";
    if !normalized.starts_with(leading) {
        return None;
    }
    // Required core fixture — the FROM + LEFT JOIN clause shape.
    let core = "from pg_catalog.pg_class c left join pg_catalog.pg_namespace n on n.oid = c.relnamespace";
    if !normalized.contains(core) {
        return None;
    }
    // Required visibility filter — distinguishes from less-related
    // queries that might share the SELECT projection.
    if !normalized.contains("and pg_catalog.pg_table_is_visible(c.oid)") {
        return None;
    }
    // Extract the name from `c.relname operator(pg_catalog.~) '^(<name>)$'`.
    // Two shapes psql emits (PG 12/13/14 vs PG 15+): the COLLATE clause
    // is sometimes absent; the anchor regex is always `^(<name>)$` for
    // a bare `\d foo`.
    let regex_marker = "c.relname operator(pg_catalog.~) '^(";
    let pos = normalized.find(regex_marker)?;
    let after = &normalized[pos + regex_marker.len()..];
    // The closing fixture is `)$'` — name ends there.
    let end = after.find(")$'")?;
    let raw_name = &after[..end];
    // V1 only handles bare identifiers (no regex metacharacters). If
    // the user types `\d 'foo.*'` psql escapes the regex specials
    // differently; punt to engine path (which will error, matching
    // real PG's behavior for non-existent objects).
    if raw_name.is_empty()
        || raw_name.chars().any(|c| {
            !(c.is_ascii_alphanumeric() || c == '_')
        })
    {
        return None;
    }
    Some(raw_name.to_string())
}

/// SP-PG-CAT T8 (real-psql) — recognize psql `\d <name>` STEP 2.
///
/// After the OID lookup (step 1), psql ships a 15-column projection
/// from `pg_class c LEFT JOIN pg_am am` filtered by `c.oid = '<oid>'`
/// to drive the table-summary header (relkind / relpersistence /
/// "Indexes:" toggle / etc.). The query body looks like:
///
/// ```text
/// SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules,
///        c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity,
///        false AS relhasoids, c.relispartition, '',
///        c.reltablespace,
///        CASE WHEN c.reloftype = 0 THEN ''
///             ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END,
///        c.relpersistence, c.relreplident, am.amname
///   FROM pg_catalog.pg_class c
///        LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
///        LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
///  WHERE c.oid = '<oid>';
/// ```
///
/// The `::pg_catalog.regtype::pg_catalog.text` cast syntax KesselDB's
/// SQL parser rejects with `unexpected char ':'`. Returns the OID if
/// the matcher fires. Anchors on the distinctive 15-column projection
/// + `from pg_catalog.pg_class c left join pg_catalog.pg_class tc`
/// fixture to avoid false matches on other `c.oid = ...` queries.
fn extract_psql_d_step2_oid(normalized: &str) -> Option<u32> {
    // Required leading fixture (the 15-column projection's first 3
    // canonical columns — psql ships them in this exact order across
    // PG 12..16).
    let leading = "select c.relchecks, c.relkind, c.relhasindex";
    if !normalized.starts_with(leading) {
        return None;
    }
    // Required JOIN fixture — the distinctive double LEFT JOIN
    // against `pg_class tc` (the toast relation) + `pg_am am`.
    let core = "from pg_catalog.pg_class c left join pg_catalog.pg_class tc";
    if !normalized.contains(core) {
        return None;
    }
    // OID lives in `where c.oid = '<oid>'` (quoted per psql).
    let oid_marker = "where c.oid = '";
    let pos = normalized.find(oid_marker)?;
    let after = &normalized[pos + oid_marker.len()..];
    let end = after.find('\'')?;
    after[..end].parse::<u32>().ok()
}

/// SP-PG-CAT T8 (real-psql) — recognize psql `\d <name>` pg_policy
/// poll. psql polls for row-level-security policies after the column
/// list; KesselDB V1 has no RLS so this is always empty. The query
/// uses subselects + `array()` / `any (...)` shapes that KesselDB's
/// SQL parser rejects with `expected FROM`, so a matcher here is
/// what makes `\d <table>` actually render.
fn matches_psql_d_pg_policy(normalized: &str) -> bool {
    // Anchor on the distinctive `select pol.polname` projection +
    // `from pg_catalog.pg_policy pol where pol.polrelid = '<n>'`.
    normalized.starts_with("select pol.polname")
        && normalized.contains("from pg_catalog.pg_policy pol")
}

/// SP-PG-CAT T8 (real-psql) — recognize psql `\d <name>` pg_inherits
/// poll. Partitioning info; KesselDB V1 has none.
fn matches_psql_d_pg_inherits(normalized: &str) -> bool {
    // psql's inherits query: `SELECT c.oid::pg_catalog.regclass FROM
    // pg_catalog.pg_class c, pg_catalog.pg_inherits i WHERE c.oid =
    // inhparent AND inhrelid = '<n>' ORDER BY inhseqno;`
    normalized.contains("from pg_catalog.pg_class c, pg_catalog.pg_inherits")
        || (normalized.starts_with("select c.oid")
            && normalized.contains("pg_catalog.pg_inherits")
            && normalized.contains("inhparent"))
}

/// SP-PG-CAT T8 (real-psql) — recognize psql `\d <name>` pg_trigger
/// poll. KesselDB V1 has no PG-wire triggers in pg_trigger.
fn matches_psql_d_pg_trigger(normalized: &str) -> bool {
    // psql's trigger query is a SELECT against `pg_catalog.pg_trigger`
    // with various per-PG-version columns. The unambiguous anchor is
    // `from pg_catalog.pg_trigger t where t.tgrelid = '<n>'`.
    normalized.contains("from pg_catalog.pg_trigger")
        && normalized.contains("tgrelid")
}

/// SP-PG-CAT T8 (real-psql) — recognize psql `\d <name>` extended-
/// statistics poll (PG 16+). Uses `::pg_catalog.regclass` cast syntax
/// that KesselDB's SQL parser rejects.
fn matches_psql_d_pg_statistic_ext(normalized: &str) -> bool {
    normalized.contains("from pg_catalog.pg_statistic_ext")
        && normalized.contains("stxrelid")
}

/// SP-PG-CAT T8 (real-psql) — recognize psql `\d <name>` publication
/// poll. PG 10+ logical-replication metadata; KesselDB V1 has none.
fn matches_psql_d_pg_publication(normalized: &str) -> bool {
    normalized.contains("from pg_catalog.pg_publication")
        || (normalized.contains("pg_publication_rel")
            && normalized.contains("prrelid"))
}

/// SP-PG-CAT T8 (real-psql) — recognize psql `\d <name>` foreign-table
/// poll. KesselDB V1 has no foreign-data wrappers.
fn matches_psql_d_pg_foreign_table(normalized: &str) -> bool {
    normalized.contains("from pg_catalog.pg_foreign_table")
        && normalized.contains("ftrelid")
}

/// SP-PG-CAT T8 (real-psql) — recognize the canonical psql `\dn`
/// schema-list query.
///
/// The query (PG 14 `src/bin/psql/describe.c::listSchemas`) uses
/// the negated regex operator `!~`:
///
/// ```text
/// SELECT n.nspname AS "Name",
///   pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
/// FROM pg_catalog.pg_namespace n
/// WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
/// ORDER BY 1;
/// ```
///
/// Without this matcher psql `\dn` errors with `unexpected char '"'`
/// (the quoted `"Name"` projection alias). Synthesizer emits one row
/// — the `public` schema — because V1 KesselDB has no other
/// user-visible schemas; pg_catalog + information_schema are filtered
/// out by the query.
fn matches_psql_dn(normalized: &str) -> bool {
    // Anchor on the distinctive `pg_get_userbyid(n.nspowner)` fixture
    // — only psql `\dn` ships this exact projection. Tolerates the
    // PG 12/13/14/15 minor variations in the WHERE clause.
    let leading = "select n.nspname as \"name\"";
    let user_marker = "pg_catalog.pg_get_userbyid(n.nspowner)";
    let from_marker = "from pg_catalog.pg_namespace n";
    normalized.starts_with(leading)
        && normalized.contains(user_marker)
        && normalized.contains(from_marker)
}

/// SP-PG-CAT T6 — extract the table-name filter from
/// `information_schema.columns` queries.
///
/// Recognizes both standalone clauses (Metabase, queries.md §5.2):
/// - `where table_name = '<name>'`
/// - `where ... and table_name = '<name>'`
///
/// Returns `None` if no `table_name = '<name>'` literal clause
/// appears (the synthesizer then emits all columns of all tables).
/// Wildcards (`LIKE`, `%`, `_`) are not unwrapped — a wildcard
/// pattern falls through to all-tables since V1 doesn't model
/// LIKE-pattern matching here.
fn extract_information_schema_columns_table_filter(normalized: &str) -> Option<String> {
    extract_quoted_after(normalized, "table_name = '")
}

/// SP-PG-CAT T6 — extract the table-name filter shared by
/// `key_column_usage` / `table_constraints`. Same shape as
/// `extract_information_schema_columns_table_filter`.
fn extract_information_schema_table_name_filter(normalized: &str) -> Option<String> {
    extract_quoted_after(normalized, "table_name = '")
}

/// Internal helper — find `needle` (which must end with `'`), then
/// scan to the next `'` and return the substring between as the
/// captured literal.
fn extract_quoted_after(normalized: &str, needle: &str) -> Option<String> {
    let pos = normalized.find(needle)?;
    let after = &normalized[pos + needle.len()..];
    let end = after.find('\'')?;
    Some(after[..end].to_string())
}

/// Parse a leading decimal u32 from `s` — used by the
/// `WHERE oid = N` / `WHERE attrelid = N` extractors. Stops at the
/// first non-digit; returns None if no digits.
fn parse_leading_u32(s: &str) -> Option<u32> {
    let mut acc: u64 = 0;
    let mut any = false;
    for c in s.chars() {
        if let Some(d) = c.to_digit(10) {
            acc = acc.checked_mul(10)?.checked_add(d as u64)?;
            if acc > u32::MAX as u64 {
                return None;
            }
            any = true;
        } else {
            break;
        }
    }
    if any {
        Some(acc as u32)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::PgColumn;
    use kessel_catalog::FieldKind;
    use kessel_proto::OpResult;

    /// Test engine that records no apply_sql calls and returns no
    /// table schemas — adequate for T1's pg_namespace synthesizer
    /// since it's fully canned (no engine queries).
    struct StubEngine;

    impl EngineApply for StubEngine {
        fn apply_sql(&self, _sql: &str) -> OpResult {
            OpResult::SchemaError("StubEngine: apply_sql should not be reached".into())
        }
        fn describe_table(&self, _name: &str) -> Option<Vec<PgColumn>> {
            None
        }
    }

    // ───────────────────────────────────────────────────────────────────
    // T1 KATs — locked invariants for the SP-PG-CAT scaffold.
    // ───────────────────────────────────────────────────────────────────

    /// **HEADLINE invariant — regression-lock:** the hook returns
    /// `None` for non-pg_catalog SQL so existing dispatch paths are
    /// unchanged. If this test starts failing, the hook is over-
    /// reaching and catching SQL it shouldn't.
    #[test]
    fn t1_catalog_hook_returns_none_for_non_pg_catalog_sql() {
        let eng = StubEngine;
        assert!(catalog_query_hook("SELECT * FROM users", &eng).is_none());
        assert!(catalog_query_hook("INSERT INTO t (id) VALUES (1)", &eng).is_none());
        assert!(catalog_query_hook("CREATE TABLE t (id i64)", &eng).is_none());
        assert!(catalog_query_hook("UPDATE t SET v = 1 WHERE id = 2", &eng).is_none());
        assert!(catalog_query_hook("DELETE FROM t WHERE id = 1", &eng).is_none());
        // NOTE: `SELECT 1` is intercepted as of SP-PG-EXTQ T7 (SQLAlchemy
        // connection-validity probe) — see
        // `t7_select_1_returns_single_int_row` in synthesize.rs tests.
        assert!(catalog_query_hook("", &eng).is_none());
        assert!(catalog_query_hook("BEGIN", &eng).is_none());
    }

    /// **HEADLINE invariant — pattern fires:** the hook returns
    /// `Some(bytes)` for the canonical `SELECT * FROM pg_catalog
    /// .pg_namespace` query the tool issues on connect.
    #[test]
    fn t1_catalog_hook_returns_some_for_pg_namespace_select_star() {
        let eng = StubEngine;
        let res = catalog_query_hook("SELECT * FROM pg_catalog.pg_namespace", &eng);
        assert!(res.is_some(), "pg_namespace SELECT * MUST hit the hook");
        let bytes = res.unwrap();
        assert!(!bytes.is_empty(), "synthesizer MUST emit a non-empty response");
        // The byte stream is well-framed: starts with 'T' (RowDescription),
        // contains 'D' (DataRow), 'C' (CommandComplete), and ends with 'Z'
        // (ReadyForQuery).
        assert_eq!(bytes[0], b'T', "first frame is RowDescription");
        assert!(bytes.iter().any(|&b| b == b'D'), "MUST contain DataRow frames");
        assert!(bytes.iter().any(|&b| b == b'C'), "MUST contain CommandComplete");
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I'],
            "MUST end with ReadyForQuery('I')");
    }

    /// **Case-insensitivity invariant:** clients send the SQL in
    /// mixed case (psql lower-cases; pgAdmin upper-cases; JDBC
    /// drivers mix). The hook MUST match regardless.
    #[test]
    fn t1_catalog_hook_is_case_insensitive() {
        let eng = StubEngine;
        let upper = catalog_query_hook("SELECT * FROM PG_CATALOG.PG_NAMESPACE", &eng);
        let lower = catalog_query_hook("select * from pg_catalog.pg_namespace", &eng);
        let mixed = catalog_query_hook("Select * From Pg_Catalog.Pg_Namespace", &eng);
        assert!(upper.is_some(), "upper-case SQL MUST match");
        assert!(lower.is_some(), "lower-case SQL MUST match");
        assert!(mixed.is_some(), "mixed-case SQL MUST match");
        // All three responses are byte-identical (canned synthesizer).
        assert_eq!(upper, lower);
        assert_eq!(lower, mixed);
    }

    /// **Whitespace-tolerance invariant:** clients include varied
    /// whitespace (newlines, multiple spaces, leading/trailing
    /// spaces). The hook normalizer collapses all of it.
    #[test]
    fn t1_catalog_hook_is_whitespace_tolerant() {
        let eng = StubEngine;
        let extra_ws = catalog_query_hook(
            "  SELECT  *   FROM    pg_catalog.pg_namespace  \n  ",
            &eng,
        );
        let newlines = catalog_query_hook(
            "SELECT *\nFROM\npg_catalog.pg_namespace",
            &eng,
        );
        let trailing_semi = catalog_query_hook(
            "SELECT * FROM pg_catalog.pg_namespace;",
            &eng,
        );
        assert!(extra_ws.is_some());
        assert!(newlines.is_some());
        assert!(trailing_semi.is_some());
    }

    /// **Comment-tolerance invariant:** ORMs and JDBC drivers
    /// prepend `-- ...` and `/* ... */` comments. The hook
    /// normalizer strips them.
    #[test]
    fn t1_catalog_hook_strips_leading_comments() {
        let eng = StubEngine;
        let line_comment = catalog_query_hook(
            "-- pgAdmin: connect probe\nSELECT * FROM pg_catalog.pg_namespace",
            &eng,
        );
        let block_comment = catalog_query_hook(
            "/* DBeaver: schema enumeration */ SELECT * FROM pg_catalog.pg_namespace",
            &eng,
        );
        assert!(line_comment.is_some(), "line comment MUST be stripped");
        assert!(block_comment.is_some(), "block comment MUST be stripped");
    }

    /// **Unqualified-name tolerance:** PG allows `pg_namespace`
    /// without the `pg_catalog.` prefix because pg_catalog is in
    /// every connection's search_path. The hook accepts both forms.
    #[test]
    fn t1_catalog_hook_accepts_unqualified_pg_namespace() {
        let eng = StubEngine;
        let unqualified = catalog_query_hook("SELECT * FROM pg_namespace", &eng);
        assert!(unqualified.is_some(),
            "unqualified pg_namespace (via implicit search_path) MUST match");
    }

    /// **Pre-normalize fast-reject invariant:** non-SELECT SQL never
    /// reaches the pattern table. Locked because any future refactor
    /// that changes the fast-reject should re-confirm the perf
    /// invariant ("SELECT-only" is the only thing pg_catalog can be).
    #[test]
    fn t1_catalog_hook_fast_rejects_non_select() {
        let eng = StubEngine;
        // Even if the SQL mentions pg_catalog, non-SELECT is rejected.
        assert!(catalog_query_hook(
            "DELETE FROM pg_catalog.pg_namespace WHERE oid = 11", &eng).is_none());
        assert!(catalog_query_hook(
            "INSERT INTO pg_catalog.pg_namespace VALUES (11, 'pg_catalog', 10, NULL)", &eng).is_none());
        assert!(catalog_query_hook(
            "UPDATE pg_catalog.pg_namespace SET nspname = 'x' WHERE oid = 11", &eng).is_none());
        // (KesselDB doesn't actually permit DML on pg_catalog from any
        // path; the fast-reject just prevents the pattern table from
        // wasting cycles on impossible queries.)
    }

    /// **PG OID canonical-values lock:** the three reserved schema
    /// OIDs match PG's `src/include/catalog/pg_namespace.dat`. If a
    /// future refactor renumbers these, real Postgres tools will
    /// silently break (they JOIN against literal OID values).
    #[test]
    fn t1_canonical_pg_namespace_oids_match_pg_dat_file() {
        assert_eq!(PG_NAMESPACE_OID_PG_CATALOG, 11,
            "pg_catalog schema OID is 11 (locked vs pg_namespace.dat)");
        assert_eq!(PG_NAMESPACE_OID_PUBLIC, 2200,
            "public schema OID is 2200 (locked vs pg_namespace.dat)");
        assert_eq!(PG_NAMESPACE_OID_INFORMATION_SCHEMA, 2202,
            "information_schema OID is 2202 (locked vs pg_namespace.dat)");
        assert_eq!(PG_AUTHID_OID_POSTGRES, 10,
            "postgres-superuser OID is 10 (locked vs pg_authid.dat)");
    }

    /// **Normalizer correctness lock:** the normalize_for_match
    /// helper produces stable lower-case + whitespace-collapsed
    /// output. Locked because the pattern table depends on this
    /// shape.
    #[test]
    fn t1_normalize_for_match_collapses_whitespace_and_lowers() {
        assert_eq!(
            normalize_for_match("  SELECT  *  FROM  T  "),
            "select * from t"
        );
        assert_eq!(
            normalize_for_match("SELECT\n\t*\nFROM\nt"),
            "select * from t"
        );
        assert_eq!(
            normalize_for_match("-- comment\nSELECT 1;"),
            "select 1"
        );
        assert_eq!(
            normalize_for_match("/* block */ SELECT 1 ;"),
            "select 1"
        );
        assert_eq!(normalize_for_match(""), "");
        assert_eq!(normalize_for_match("   "), "");
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-CAT T3 KATs — pg_class pattern + psql \dt canonical
    // joined-result intercept. Engines that override `list_tables()`
    // (the `MockListEngine` below) drive the synthesizer; engines
    // that don't (the `StubEngine` above) get an empty-result-set
    // response.
    // ───────────────────────────────────────────────────────────────────

    use crate::engine::{TableKind, TableMetadata};

    /// EngineApply that overrides `list_tables()` so the T3
    /// synthesizers see a non-empty catalog without spinning up
    /// the kesseldb-server EngineHandle (that's the
    /// `pg_gateway_tests::t3_engine_handle_list_tables_round_trips_via_admin_frame`
    /// integration KAT instead).
    struct MockListEngine {
        tables: Vec<TableMetadata>,
    }
    impl EngineApply for MockListEngine {
        fn apply_sql(&self, _sql: &str) -> OpResult {
            OpResult::SchemaError("MockListEngine: apply_sql should not be reached".into())
        }
        fn describe_table(&self, _name: &str) -> Option<Vec<PgColumn>> {
            None
        }
        fn list_tables(&self) -> Vec<TableMetadata> {
            self.tables.clone()
        }
    }

    fn three_table_engine() -> MockListEngine {
        MockListEngine {
            tables: vec![
                TableMetadata { name: "users".into(),  type_id: 1, kind: TableKind::Ordinary, field_count: 2 },
                TableMetadata { name: "orders".into(), type_id: 2, kind: TableKind::Ordinary, field_count: 3 },
                TableMetadata { name: "lineitems".into(), type_id: 3, kind: TableKind::Ordinary, field_count: 5 },
            ],
        }
    }

    /// **Pattern match:** `SELECT * FROM pg_catalog.pg_class` hits
    /// the hook AND the synthesizer fires the well-framed response.
    /// HEADLINE for the T3 dispatcher entry.
    #[test]
    fn t3_pg_class_select_star_pattern_fires() {
        let eng = three_table_engine();
        let res = catalog_query_hook("SELECT * FROM pg_catalog.pg_class", &eng);
        assert!(res.is_some(), "pg_class SELECT * MUST hit the hook");
        let bytes = res.unwrap();
        assert!(!bytes.is_empty());
        assert_eq!(bytes[0], b'T', "first frame is RowDescription");
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// **Pattern match — unqualified form:** `SELECT * FROM pg_class`
    /// (no `pg_catalog.` prefix) also hits the hook because pg_catalog
    /// is in every connection's implicit search_path.
    #[test]
    fn t3_pg_class_select_star_accepts_unqualified() {
        let eng = three_table_engine();
        let res = catalog_query_hook("SELECT * FROM pg_class", &eng);
        assert!(res.is_some(), "unqualified pg_class MUST match");
    }

    /// **Case-insensitivity invariant preserved for T3 pattern:**
    /// upper / lower / mixed all match (locked because pgAdmin
    /// upper-cases and JDBC drivers mix case).
    #[test]
    fn t3_pg_class_pattern_is_case_insensitive() {
        let eng = three_table_engine();
        let upper = catalog_query_hook("SELECT * FROM PG_CATALOG.PG_CLASS", &eng);
        let lower = catalog_query_hook("select * from pg_catalog.pg_class", &eng);
        let mixed = catalog_query_hook("Select * From Pg_Catalog.Pg_Class", &eng);
        assert!(upper.is_some());
        assert!(lower.is_some());
        assert!(mixed.is_some());
    }

    /// **Pattern match — psql `\dt` canonical query** hits the hook
    /// and the joined-result synthesizer fires. HEADLINE for the
    /// arc's primary acceptance criterion (design §8 #1).
    #[test]
    fn t3_psql_dt_canonical_pattern_fires() {
        let eng = three_table_engine();
        // The exact query psql 14 ships (from describe.c). Note the
        // `\n` to mirror the multi-line form psql sends.
        let psql_dt = "SELECT n.nspname as \"Schema\",\n\
                       c.relname as \"Name\",\n\
                       CASE c.relkind WHEN 'r' THEN 'table' END as \"Type\",\n\
                       pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\"\n\
                       FROM pg_catalog.pg_class c\n\
                       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n\
                       WHERE c.relkind IN ('r','p','')\n\
                       AND n.nspname <> 'pg_catalog'\n\
                       AND n.nspname <> 'information_schema'\n\
                       AND n.nspname !~ '^pg_toast'\n\
                       AND pg_catalog.pg_table_is_visible(c.oid)\n\
                       ORDER BY 1,2;";
        let res = catalog_query_hook(psql_dt, &eng);
        assert!(res.is_some(), "psql \\dt canonical query MUST hit the hook");
        let bytes = res.unwrap();
        assert_eq!(bytes[0], b'T');
        // The joined result includes "Schema" / "Name" / "Type" / "Owner"
        // — these names appear verbatim in the RowDescription.
        assert!(bytes.windows(b"Schema\0".len()).any(|w| w == b"Schema\0"));
        assert!(bytes.windows(b"Name\0".len()).any(|w| w == b"Name\0"));
        assert!(bytes.windows(b"Type\0".len()).any(|w| w == b"Type\0"));
        assert!(bytes.windows(b"Owner\0".len()).any(|w| w == b"Owner\0"));
        // Each table appears as `table` + `public` + `kesseldb`.
        assert!(bytes.windows(b"users".len()).any(|w| w == b"users"));
        assert!(bytes.windows(b"orders".len()).any(|w| w == b"orders"));
        assert!(bytes.windows(b"lineitems".len()).any(|w| w == b"lineitems"));
        // CommandComplete tag carries `SELECT 3` for the 3 tables.
        assert!(bytes.windows(b"SELECT 3\0".len()).any(|w| w == b"SELECT 3\0"));
    }

    /// **Pattern match — `\dt` with an extra-strict relkind filter
    /// (PG 13/14 form including views/materialized views/sequences)**
    /// also hits because the matcher anchors on the leading +
    /// trailing fixtures.
    #[test]
    fn t3_psql_dt_pattern_matches_v13_relkind_form() {
        let eng = three_table_engine();
        let psql_dt_v13 = "SELECT n.nspname as \"Schema\", c.relname as \"Name\", \
                           CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' END as \"Type\", \
                           pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\" \
                           FROM pg_catalog.pg_class c \
                           LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                           WHERE c.relkind IN ('r','p','v','m','S','f','') \
                           AND n.nspname <> 'pg_catalog' \
                           AND n.nspname <> 'information_schema' \
                           AND n.nspname !~ '^pg_toast' \
                           AND pg_catalog.pg_table_is_visible(c.oid) \
                           ORDER BY 1,2;";
        let res = catalog_query_hook(psql_dt_v13, &eng);
        assert!(res.is_some(), "psql \\dt v13 form MUST also match");
    }

    /// **Regression-lock — T1 paths unchanged:** every pg_namespace
    /// pattern from T1 still hits AND every non-pg_catalog SQL still
    /// returns None. T3 added patterns are PURELY ADDITIVE.
    #[test]
    fn t3_pre_existing_t1_patterns_still_match_and_unrelated_sql_still_misses() {
        let eng = three_table_engine();
        // T1 still fires.
        assert!(catalog_query_hook("SELECT * FROM pg_catalog.pg_namespace", &eng).is_some());
        // Unrelated SELECT still misses.
        assert!(catalog_query_hook("SELECT * FROM users", &eng).is_none());
        // Non-SELECT still fast-rejected.
        assert!(catalog_query_hook("INSERT INTO t (id) VALUES (1)", &eng).is_none());
        assert!(catalog_query_hook("UPDATE t SET v = 1 WHERE id = 2", &eng).is_none());
        assert!(catalog_query_hook("CREATE TABLE t (id i64)", &eng).is_none());
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-CAT T4 KATs — pg_attribute + pg_type patterns + the psql
    // `\d <table>` step-2 / pgJDBC `getColumns` joined-result intercepts.
    // ───────────────────────────────────────────────────────────────────

    use crate::pg_catalog::synthesize::oid_for_table_name;

    /// Engine that combines list_tables + describe_table for T4
    /// pattern-hook tests (the parameterized `attrelid = N` /
    /// `\d <table>` / pgJDBC-getColumns paths all need real
    /// describe_table data, not just the metadata).
    struct PatternHookEngine {
        tables: Vec<TableMetadata>,
        schemas: std::collections::BTreeMap<String, Vec<PgColumn>>,
    }
    impl EngineApply for PatternHookEngine {
        fn apply_sql(&self, _sql: &str) -> OpResult {
            OpResult::SchemaError("PatternHookEngine: apply_sql should not be reached".into())
        }
        fn describe_table(&self, name: &str) -> Option<Vec<PgColumn>> {
            self.schemas.get(name).cloned()
        }
        fn list_tables(&self) -> Vec<TableMetadata> {
            self.tables.clone()
        }
    }

    fn t4_test_engine() -> PatternHookEngine {
        let mut schemas = std::collections::BTreeMap::new();
        schemas.insert("users".to_string(), vec![
            PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(64), nullable: false },
        ]);
        PatternHookEngine {
            tables: vec![
                TableMetadata { name: "users".into(), type_id: 1, kind: TableKind::Ordinary, field_count: 2 },
            ],
            schemas,
        }
    }

    /// **Pattern match:** `SELECT * FROM pg_catalog.pg_attribute` hits
    /// the hook + synthesizer fires.
    #[test]
    fn t4_pg_attribute_select_star_pattern_fires() {
        let eng = t4_test_engine();
        let res = catalog_query_hook("SELECT * FROM pg_catalog.pg_attribute", &eng);
        assert!(res.is_some(), "pg_attribute SELECT * MUST hit the hook");
        let bytes = res.unwrap();
        assert_eq!(bytes[0], b'T');
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// **Pattern match — unqualified pg_attribute** also fires.
    #[test]
    fn t4_pg_attribute_select_star_unqualified() {
        let eng = t4_test_engine();
        assert!(catalog_query_hook("SELECT * FROM pg_attribute", &eng).is_some());
    }

    /// **Pattern match — `WHERE attrelid = N` extracts the OID and
    /// filters to that table.** Headline for the psql `\d <table>`
    /// / pgJDBC `getColumns` path.
    #[test]
    fn t4_pg_attribute_attrelid_filter_pattern_fires() {
        let eng = t4_test_engine();
        let users_oid = oid_for_table_name("users");
        let sql = format!("SELECT * FROM pg_catalog.pg_attribute WHERE attrelid = {}", users_oid);
        let res = catalog_query_hook(&sql, &eng);
        assert!(res.is_some(), "attrelid-filtered query MUST hit the hook");
        let bytes = res.unwrap();
        // Filtered to users (2 columns).
        assert!(bytes.windows(b"SELECT 2\0".len()).any(|w| w == b"SELECT 2\0"),
            "filtered to users MUST emit SELECT 2");
    }

    /// **Pattern match — `pg_catalog.pg_attribute WHERE attrelid = N`
    /// with an unknown OID returns 0 rows (well-framed).**
    #[test]
    fn t4_pg_attribute_attrelid_filter_unknown_oid_zero_rows() {
        let eng = t4_test_engine();
        let res = catalog_query_hook(
            "SELECT * FROM pg_catalog.pg_attribute WHERE attrelid = 99999",
            &eng,
        );
        assert!(res.is_some(), "any attrelid = N MUST hit the hook");
        let bytes = res.unwrap();
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
    }

    /// **Pattern match — psql `\d <table>` step-2 column-list query**
    /// (queries.md §1.5) extracts the OID from the quoted form and
    /// fires the joined-result synthesizer.
    #[test]
    fn t4_psql_d_table_step2_pattern_fires() {
        let eng = t4_test_engine();
        let users_oid = oid_for_table_name("users");
        // Mirror the canonical psql `\d <table>` step-2 SQL from
        // queries.md §1.5 — the OID is quoted (PG accepts it either way).
        let sql = format!(
            "SELECT a.attname,\n  \
             pg_catalog.format_type(a.atttypid, a.atttypmod),\n  \
             (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) \
              FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid \
              AND d.adnum = a.attnum AND a.atthasdef),\n  \
             a.attnotnull,\n  \
             NULL AS attcollation,\n  \
             a.attidentity,\n  \
             a.attgenerated\n\
             FROM pg_catalog.pg_attribute a\n\
             WHERE a.attrelid = '{users_oid}' AND a.attnum > 0 AND NOT a.attisdropped\n\
             ORDER BY a.attnum",
        );
        let res = catalog_query_hook(&sql, &eng);
        assert!(res.is_some(), "psql \\d <table> step-2 MUST hit the hook");
        let bytes = res.unwrap();
        // The synthesizer emitted 2 column rows for `users`.
        assert!(bytes.windows(b"SELECT 2\0".len()).any(|w| w == b"SELECT 2\0"));
        // PG type name `int8` for I64 appears.
        assert!(bytes.windows(b"int8".len()).any(|w| w == b"int8"));
        // Column names appear.
        assert!(bytes.windows(b"name".len()).any(|w| w == b"name"));
    }

    /// **Pattern match — `SELECT * FROM pg_catalog.pg_type`** hits.
    #[test]
    fn t4_pg_type_select_star_pattern_fires() {
        let eng = t4_test_engine();
        let res = catalog_query_hook("SELECT * FROM pg_catalog.pg_type", &eng);
        assert!(res.is_some(), "pg_type SELECT * MUST hit the hook");
        let bytes = res.unwrap();
        assert_eq!(bytes[0], b'T');
        // The canned `int8` type name appears.
        assert!(bytes.windows(b"int8".len()).any(|w| w == b"int8"));
    }

    /// **Pattern match — unqualified pg_type** also fires.
    #[test]
    fn t4_pg_type_select_star_unqualified() {
        let eng = t4_test_engine();
        assert!(catalog_query_hook("SELECT * FROM pg_type", &eng).is_some());
    }

    /// **Pattern match — `SELECT typname, typlen FROM pg_catalog.pg_type
    /// WHERE oid = N` extracts the OID.** Used by JDBC's column-type
    /// resolution path.
    #[test]
    fn t4_pg_type_per_oid_lookup_pattern_fires() {
        let eng = t4_test_engine();
        // Match against INT8 (20).
        let res = catalog_query_hook(
            "SELECT typname, typlen, typbyval FROM pg_catalog.pg_type WHERE oid = 20",
            &eng,
        );
        assert!(res.is_some(), "pg_type per-OID lookup MUST hit the hook");
        let bytes = res.unwrap();
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        assert!(bytes.windows(b"int8".len()).any(|w| w == b"int8"));
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-CAT T5 KATs — pg_index + pg_constraint pattern hooks + the
    // pgJDBC `getIndexInfo` / psql `\d <table>` step-3 joined-result
    // intercepts. Drive via T5IndexHookEngine (overrides list_indexes
    // + list_constraints + describe_table for the resolution paths).
    // ───────────────────────────────────────────────────────────────────

    use crate::engine::{ConstraintKind, ConstraintMetadata, FkAction, IndexKind, IndexMetadata};

    struct T5IndexHookEngine {
        tables: Vec<TableMetadata>,
        schemas: std::collections::BTreeMap<String, Vec<PgColumn>>,
        indexes: std::collections::BTreeMap<String, Vec<IndexMetadata>>,
        constraints: std::collections::BTreeMap<String, Vec<ConstraintMetadata>>,
    }
    impl EngineApply for T5IndexHookEngine {
        fn apply_sql(&self, _sql: &str) -> OpResult {
            OpResult::SchemaError("T5IndexHookEngine: apply_sql should not be reached".into())
        }
        fn describe_table(&self, name: &str) -> Option<Vec<PgColumn>> {
            self.schemas.get(name).cloned()
        }
        fn list_tables(&self) -> Vec<TableMetadata> {
            self.tables.clone()
        }
        fn list_indexes_for_table(&self, name: &str) -> Vec<IndexMetadata> {
            self.indexes.get(name).cloned().unwrap_or_default()
        }
        fn list_constraints_for_table(&self, name: &str) -> Vec<ConstraintMetadata> {
            self.constraints.get(name).cloned().unwrap_or_default()
        }
    }

    fn t5_index_hook_engine() -> T5IndexHookEngine {
        let mut schemas = std::collections::BTreeMap::new();
        schemas.insert(
            "users".to_string(),
            vec![
                PgColumn { name: "id".into(), kind: FieldKind::I64, nullable: false },
                PgColumn { name: "email".into(), kind: FieldKind::Char(128), nullable: false },
            ],
        );
        let mut indexes = std::collections::BTreeMap::new();
        indexes.insert(
            "users".to_string(),
            vec![IndexMetadata {
                name: "users_email_idx".into(),
                fields: vec![2],
                is_unique: true,
                kind: IndexKind::Equality,
            }],
        );
        let mut constraints = std::collections::BTreeMap::new();
        constraints.insert(
            "users".to_string(),
            vec![ConstraintMetadata {
                name: "users_email_key".into(),
                kind: ConstraintKind::Unique,
                columns: vec![2],
                references: None,
            }],
        );
        T5IndexHookEngine {
            tables: vec![TableMetadata {
                name: "users".into(),
                type_id: 1,
                kind: TableKind::Ordinary,
                field_count: 2,
            }],
            schemas,
            indexes,
            constraints,
        }
    }

    /// **HEADLINE — `SELECT * FROM pg_catalog.pg_index` hits hook +
    /// synthesizer fires.** pg_index doesn't carry the index name
    /// column (that's pg_class.relname); the indexrelid OID slot
    /// carries the per-index identity.
    #[test]
    fn t5_pg_index_select_star_pattern_fires() {
        let eng = t5_index_hook_engine();
        let res = catalog_query_hook("SELECT * FROM pg_catalog.pg_index", &eng);
        assert!(res.is_some(), "pg_index SELECT * MUST hit the hook");
        let bytes = res.unwrap();
        assert_eq!(bytes[0], b'T');
        // users has 1 index → SELECT 1.
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        // The synthetic indexrelid OID is present in the data row.
        let idx_oid = crate::pg_catalog::synthesize::oid_for_index_name("users_email_idx")
            .to_string();
        assert!(bytes.windows(idx_oid.len()).any(|w| w == idx_oid.as_bytes()));
    }

    /// **Pattern match — unqualified pg_index** also fires.
    #[test]
    fn t5_pg_index_select_star_unqualified() {
        let eng = t5_index_hook_engine();
        assert!(catalog_query_hook("SELECT * FROM pg_index", &eng).is_some());
    }

    /// **Pattern match — `WHERE indrelid = N` extracts OID and
    /// filters indexes to that table.**
    #[test]
    fn t5_pg_index_indrelid_filter_pattern_fires() {
        let eng = t5_index_hook_engine();
        let users_oid = crate::pg_catalog::synthesize::oid_for_table_name("users");
        let sql = format!("SELECT * FROM pg_catalog.pg_index WHERE indrelid = {}", users_oid);
        let res = catalog_query_hook(&sql, &eng);
        assert!(res.is_some(), "indrelid-filtered query MUST hit the hook");
        let bytes = res.unwrap();
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        // Unknown OID → 0 rows.
        let res2 = catalog_query_hook(
            "SELECT * FROM pg_catalog.pg_index WHERE indrelid = 999999",
            &eng,
        );
        assert!(res2.is_some());
        let bytes2 = res2.unwrap();
        assert!(bytes2.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
    }

    /// **HEADLINE — psql `\d <table>` step 3 canonical query fires.**
    #[test]
    fn t5_psql_d_table_step3_pattern_fires() {
        let eng = t5_index_hook_engine();
        let users_oid = crate::pg_catalog::synthesize::oid_for_table_name("users");
        // Verbatim psql 14 \d <table> step 3 (queries.md §1.6).
        let sql = format!(
            "SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered,\n\
                    i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),\n\
                    pg_catalog.pg_get_constraintdef(con.oid, true),\n\
                    contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace\n\
             FROM pg_catalog.pg_class c, pg_catalog.pg_class c2,\n\
                  pg_catalog.pg_index i\n\
               LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid \
                 AND conindid = i.indexrelid AND contype IN ('p','u','x'))\n\
             WHERE c.oid = '{users_oid}' AND c.oid = i.indrelid AND i.indexrelid = c2.oid\n\
             ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname"
        );
        let res = catalog_query_hook(&sql, &eng);
        assert!(res.is_some(), "psql \\d <table> step 3 MUST hit the hook");
        let bytes = res.unwrap();
        // 1 index on users → SELECT 1.
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
    }

    /// **HEADLINE — pgJDBC `getIndexInfo` canonical query fires.**
    #[test]
    fn t5_pgjdbc_getindexinfo_pattern_fires() {
        let eng = t5_index_hook_engine();
        let sql = "SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, ct.relname AS TABLE_NAME, \
                   NOT i.indisunique AS NON_UNIQUE, NULL AS INDEX_QUALIFIER, ci.relname AS INDEX_NAME, \
                   CASE i.indisclustered WHEN true THEN 1 ELSE CASE am.amname WHEN 'hash' THEN 2 ELSE 3 END END AS TYPE, \
                   (information_schema._pg_expandarray(i.indkey)).n AS ORDINAL_POSITION, \
                   trim(both '\"' from pg_catalog.pg_get_indexdef(ci.oid, (information_schema._pg_expandarray(i.indkey)).n, false)) AS COLUMN_NAME, \
                   NULL AS ASC_OR_DESC, ci.reltuples AS CARDINALITY, ci.relpages AS PAGES, \
                   pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION \
                   FROM pg_catalog.pg_class ct \
                   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) \
                   JOIN pg_catalog.pg_index i ON (ct.oid = i.indrelid) \
                   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) \
                   JOIN pg_catalog.pg_am am ON (ci.relam = am.oid) \
                   WHERE true AND n.nspname = 'public' AND ct.relname = 'users' \
                   ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION";
        let res = catalog_query_hook(sql, &eng);
        assert!(res.is_some(), "pgJDBC getIndexInfo MUST hit the hook");
        let bytes = res.unwrap();
        // 1 index × 1 column on users → SELECT 1.
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        assert!(bytes.windows(b"users_email_idx".len()).any(|w| w == b"users_email_idx"));
        assert!(bytes.windows(b"email".len()).any(|w| w == b"email"));
    }

    /// **Pattern match — `SELECT * FROM pg_catalog.pg_constraint`.**
    #[test]
    fn t5_pg_constraint_select_star_pattern_fires() {
        let eng = t5_index_hook_engine();
        let res = catalog_query_hook("SELECT * FROM pg_catalog.pg_constraint", &eng);
        assert!(res.is_some(), "pg_constraint SELECT * MUST hit the hook");
        let bytes = res.unwrap();
        // users has 1 constraint → SELECT 1.
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
        assert!(bytes.windows(b"users_email_key".len()).any(|w| w == b"users_email_key"));
    }

    /// **Pattern match — unqualified pg_constraint.**
    #[test]
    fn t5_pg_constraint_select_star_unqualified() {
        let eng = t5_index_hook_engine();
        assert!(catalog_query_hook("SELECT * FROM pg_constraint", &eng).is_some());
    }

    /// **Pattern match — `WHERE conrelid = N` extracts OID and
    /// filters constraints to that table.**
    #[test]
    fn t5_pg_constraint_conrelid_filter_pattern_fires() {
        let eng = t5_index_hook_engine();
        let users_oid = crate::pg_catalog::synthesize::oid_for_table_name("users");
        let sql = format!(
            "SELECT * FROM pg_catalog.pg_constraint WHERE conrelid = {}",
            users_oid
        );
        let res = catalog_query_hook(&sql, &eng);
        assert!(res.is_some());
        let bytes = res.unwrap();
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-CAT T7 KATs — helper-function dispatch via catalog_query_hook
    // (case-insensitive + whitespace-tolerant via normalize_for_match).
    // ───────────────────────────────────────────────────────────────────

    /// **HEADLINE — `SELECT version()` dispatches through the hook.**
    #[test]
    fn t7_select_version_dispatches_through_hook() {
        let eng = t5_index_hook_engine();
        let res = catalog_query_hook("SELECT version()", &eng);
        assert!(res.is_some(), "SELECT version() MUST hit the hook");
        let bytes = res.unwrap();
        assert_eq!(bytes[0], b'T');
        // The canonical version string appears.
        assert!(bytes.windows(b"PostgreSQL 14.0 (KesselDB 1.0)".len())
            .any(|w| w == b"PostgreSQL 14.0 (KesselDB 1.0)"));
        // Last 6 bytes are ReadyForQuery('I').
        assert_eq!(&bytes[bytes.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);
    }

    /// **Case-insensitive helper function dispatch.** Upper-/mixed-case
    /// all route through the hook.
    #[test]
    fn t7_helper_function_dispatch_is_case_insensitive() {
        let eng = t5_index_hook_engine();
        assert!(catalog_query_hook("SELECT VERSION()", &eng).is_some());
        assert!(catalog_query_hook("Select Current_Database()", &eng).is_some());
        assert!(catalog_query_hook("SELECT CURRENT_SCHEMA()", &eng).is_some());
    }

    /// **HEADLINE — `SHOW server_version` dispatches through the hook.**
    #[test]
    fn t7_show_dispatches_through_hook() {
        let eng = t5_index_hook_engine();
        let res = catalog_query_hook("SHOW server_version", &eng);
        assert!(res.is_some(), "SHOW server_version MUST hit the hook");
        let bytes = res.unwrap();
        assert!(bytes.windows(b"14.0".len()).any(|w| w == b"14.0"));
    }

    /// **`SHOW timezone` returns 'UTC'.**
    #[test]
    fn t7_show_timezone_dispatch_returns_utc() {
        let eng = t5_index_hook_engine();
        let res = catalog_query_hook("SHOW timezone", &eng);
        assert!(res.is_some());
        let bytes = res.unwrap();
        assert!(bytes.windows(b"UTC".len()).any(|w| w == b"UTC"));
    }

    /// **Helper functions tolerate trailing semicolons + extra
    /// whitespace.** Locked because pgcli ships `SELECT version();`
    /// and pgJDBC inserts newlines.
    #[test]
    fn t7_helper_pattern_tolerates_trailing_semicolon_and_whitespace() {
        let eng = t5_index_hook_engine();
        assert!(catalog_query_hook("SELECT version();", &eng).is_some());
        assert!(catalog_query_hook("  SELECT  version()  ;  ", &eng).is_some());
        assert!(catalog_query_hook("SELECT version()\n", &eng).is_some());
    }

    /// **Helper patterns checked BEFORE table-pattern matchers** —
    /// `SELECT version()` doesn't fall through to a (nonexistent)
    /// `SELECT * FROM ...` matcher.
    #[test]
    fn t7_helper_patterns_check_before_table_patterns() {
        let eng = t5_index_hook_engine();
        // version() returns a 1-row text result (helper synth), not
        // a SELECT * FROM ... unimplemented path.
        let res = catalog_query_hook("SELECT version()", &eng).expect("matches");
        // The CommandComplete tag is `SELECT 1`, not anything else.
        assert!(res.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
    }

    /// **`SELECT version() AS v` matches with the alias suffix.**
    #[test]
    fn t7_helper_pattern_with_as_alias() {
        let eng = t5_index_hook_engine();
        let res = catalog_query_hook("SELECT version() AS v", &eng);
        assert!(res.is_some(), "SELECT version() AS v MUST match");
        let bytes = res.unwrap();
        assert!(bytes.windows(b"PostgreSQL".len()).any(|w| w == b"PostgreSQL"));
    }

    /// **Regression lock — T5+T7 patterns don't break T1+T3+T4.**
    #[test]
    fn t5_t7_pre_existing_patterns_still_match() {
        let eng = t5_index_hook_engine();
        // T1
        assert!(catalog_query_hook("SELECT * FROM pg_catalog.pg_namespace", &eng).is_some());
        // T3
        assert!(catalog_query_hook("SELECT * FROM pg_catalog.pg_class", &eng).is_some());
        // T4
        assert!(catalog_query_hook("SELECT * FROM pg_catalog.pg_attribute", &eng).is_some());
        assert!(catalog_query_hook("SELECT * FROM pg_catalog.pg_type", &eng).is_some());
        // Unrelated SELECT still misses (no helper-function match + no
        // pg_catalog match).
        assert!(catalog_query_hook("SELECT id FROM users", &eng).is_none());
        // Non-SELECT non-SHOW still fast-rejected.
        assert!(catalog_query_hook(
            "DELETE FROM pg_catalog.pg_index WHERE indrelid = 16385",
            &eng).is_none());
    }

    /// **Regression lock — T1+T3 patterns still match + non-pg_catalog
    /// SQL still misses.** T4 added patterns are PURELY ADDITIVE.
    #[test]
    fn t4_pre_existing_t1_t3_patterns_still_match() {
        let eng = t4_test_engine();
        // T1
        assert!(catalog_query_hook("SELECT * FROM pg_catalog.pg_namespace", &eng).is_some());
        // T3
        assert!(catalog_query_hook("SELECT * FROM pg_catalog.pg_class", &eng).is_some());
        // Unrelated SELECT still misses.
        assert!(catalog_query_hook("SELECT * FROM users", &eng).is_none());
        // Non-SELECT still fast-rejected even if mentioning pg_attribute.
        assert!(catalog_query_hook(
            "DELETE FROM pg_catalog.pg_attribute WHERE attrelid = 16385",
            &eng).is_none());
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-CAT T6 KATs — information_schema pattern matchers + the
    // end-to-end dispatch through catalog_query_hook.
    // ───────────────────────────────────────────────────────────────────

    /// Build an InfoSchemaEngine-shaped helper here (mod.rs tests can't
    /// import the helper from synthesize::tests).
    struct T6Engine {
        tables: Vec<TableMetadata>,
        schemas: std::collections::BTreeMap<String, Vec<PgColumn>>,
    }
    impl EngineApply for T6Engine {
        fn apply_sql(&self, _sql: &str) -> OpResult {
            OpResult::SchemaError("T6Engine: apply_sql should not be reached".into())
        }
        fn describe_table(&self, name: &str) -> Option<Vec<PgColumn>> {
            self.schemas.get(name).cloned()
        }
        fn list_tables(&self) -> Vec<TableMetadata> {
            self.tables.clone()
        }
    }
    fn t6_engine() -> T6Engine {
        let mut schemas = std::collections::BTreeMap::new();
        schemas.insert("users".to_string(), vec![
            PgColumn { name: "id".into(),   kind: FieldKind::I64, nullable: false },
            PgColumn { name: "name".into(), kind: FieldKind::Char(64), nullable: false },
        ]);
        T6Engine {
            tables: vec![
                TableMetadata { name: "users".into(),  type_id: 1, kind: TableKind::Ordinary, field_count: 2 },
                TableMetadata { name: "orders".into(), type_id: 2, kind: TableKind::Ordinary, field_count: 3 },
            ],
            schemas,
        }
    }

    /// **HEADLINE — information_schema.tables canonical Metabase
    /// query hits the hook + synthesizer fires.** The Metabase
    /// connect-database wizard issues this verbatim
    /// (queries.md §5.1).
    #[test]
    fn t6_information_schema_tables_metabase_query_fires() {
        let eng = t6_engine();
        let metabase_query = "SELECT table_catalog, table_schema, table_name, table_type \
                              FROM information_schema.tables \
                              WHERE table_schema NOT IN ('pg_catalog', 'information_schema') \
                              ORDER BY table_schema, table_name;";
        let res = catalog_query_hook(metabase_query, &eng);
        assert!(res.is_some(), "Metabase info_schema.tables query MUST hit hook");
        let bytes = res.unwrap();
        assert_eq!(bytes[0], b'T');
        assert!(bytes.windows(b"users".len()).any(|w| w == b"users"));
        assert!(bytes.windows(b"orders".len()).any(|w| w == b"orders"));
        assert!(bytes.windows(b"BASE TABLE".len()).any(|w| w == b"BASE TABLE"));
    }

    /// **HEADLINE — information_schema.columns canonical Metabase
    /// per-table query hits the hook AND filters to the named
    /// table.** queries.md §5.2.
    #[test]
    fn t6_information_schema_columns_metabase_per_table_query_fires() {
        let eng = t6_engine();
        let metabase_query = "SELECT table_catalog, table_schema, table_name, column_name, \
                              ordinal_position, data_type, character_maximum_length, \
                              numeric_precision, numeric_scale, is_nullable \
                              FROM information_schema.columns \
                              WHERE table_schema = 'public' AND table_name = 'users' \
                              ORDER BY ordinal_position;";
        let res = catalog_query_hook(metabase_query, &eng);
        assert!(res.is_some(), "Metabase info_schema.columns query MUST hit hook");
        let bytes = res.unwrap();
        // Only the 2 users columns (filter to 'users' worked).
        assert!(bytes.windows(b"SELECT 2\0".len()).any(|w| w == b"SELECT 2\0"),
            "filter MUST restrict to users table");
        assert!(bytes.windows(b"bigint".len()).any(|w| w == b"bigint"));
        assert!(bytes.windows(b"text".len()).any(|w| w == b"text"));
    }

    /// **HEADLINE — information_schema.schemata canonical query
    /// returns the 3 schemas.** queries.md §5.3.
    #[test]
    fn t6_information_schema_schemata_canonical_query_fires() {
        let eng = t6_engine();
        let query = "SELECT schema_name FROM information_schema.schemata \
                     WHERE schema_name NOT IN ('pg_catalog', 'information_schema') \
                     ORDER BY schema_name;";
        let res = catalog_query_hook(query, &eng);
        assert!(res.is_some());
        let bytes = res.unwrap();
        assert!(bytes.windows(b"SELECT 3\0".len()).any(|w| w == b"SELECT 3\0"));
        assert!(bytes.windows(b"public".len()).any(|w| w == b"public"));
    }

    /// **information_schema.tables / .columns / .schemata matching is
    /// case-insensitive.** Mixed-case SQL from JDBC drivers MUST match.
    #[test]
    fn t6_information_schema_pattern_is_case_insensitive() {
        let eng = t6_engine();
        let upper = catalog_query_hook(
            "SELECT * FROM INFORMATION_SCHEMA.TABLES",
            &eng,
        );
        let mixed = catalog_query_hook(
            "SELECT * From Information_Schema.Tables",
            &eng,
        );
        assert!(upper.is_some(), "upper-case info_schema MUST match");
        assert!(mixed.is_some(), "mixed-case info_schema MUST match");
    }

    /// **information_schema.views returns empty (0 rows) via hook.**
    /// Tools probing for views see a graceful empty result.
    #[test]
    fn t6_information_schema_views_hook_returns_empty() {
        let eng = t6_engine();
        let res = catalog_query_hook(
            "SELECT * FROM information_schema.views",
            &eng,
        );
        assert!(res.is_some());
        let bytes = res.unwrap();
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
    }

    /// **information_schema.routines returns empty (0 rows) via hook.**
    /// DataGrip queries this on connect; well-framed empty per spec.
    #[test]
    fn t6_information_schema_routines_hook_returns_empty() {
        let eng = t6_engine();
        let res = catalog_query_hook(
            "SELECT * FROM information_schema.routines",
            &eng,
        );
        assert!(res.is_some());
        let bytes = res.unwrap();
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
    }

    /// **information_schema.key_column_usage canonical query hits
    /// the hook.** Used by Metabase relationship inference.
    #[test]
    fn t6_information_schema_key_column_usage_hook_fires() {
        let eng = t6_engine();
        let res = catalog_query_hook(
            "SELECT * FROM information_schema.key_column_usage",
            &eng,
        );
        assert!(res.is_some(), "info_schema.key_column_usage MUST hit hook");
        let bytes = res.unwrap();
        // No constraints in the default fixture → 0 rows.
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
    }

    /// **information_schema.table_constraints canonical query hits
    /// the hook.** Used by Schemaspy / ER diagram tools.
    #[test]
    fn t6_information_schema_table_constraints_hook_fires() {
        let eng = t6_engine();
        let res = catalog_query_hook(
            "SELECT * FROM information_schema.table_constraints",
            &eng,
        );
        assert!(res.is_some());
        let bytes = res.unwrap();
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
    }

    /// **has_information_schema_relation has proper word-boundary
    /// behaviour** — a longer relation name (e.g.
    /// `tables_with_extras`) does NOT match `tables`. Locks the
    /// over-match prevention.
    #[test]
    fn t6_information_schema_matcher_word_boundary() {
        // The hook matches `from information_schema.tables` exactly,
        // not `tables_with_extras`. We don't have a real such table,
        // but the matcher MUST reject the substring match.
        assert!(!has_information_schema_relation(
            "select * from information_schema.tables_with_extras", "tables"));
        // Trailing whitespace / where / ; / end-of-string all valid.
        assert!(has_information_schema_relation(
            "select * from information_schema.tables", "tables"));
        assert!(has_information_schema_relation(
            "select * from information_schema.tables where x = 1", "tables"));
        assert!(has_information_schema_relation(
            "select * from information_schema.tables, foo", "tables"));
    }

    /// **Regression lock — T6 additions don't break T1-T5+T7.**
    #[test]
    fn t6_pre_existing_patterns_still_match() {
        let eng = t6_engine();
        // T1
        assert!(catalog_query_hook("SELECT * FROM pg_catalog.pg_namespace", &eng).is_some());
        // T3
        assert!(catalog_query_hook("SELECT * FROM pg_catalog.pg_class", &eng).is_some());
        // T4
        assert!(catalog_query_hook("SELECT * FROM pg_catalog.pg_attribute", &eng).is_some());
        assert!(catalog_query_hook("SELECT * FROM pg_catalog.pg_type", &eng).is_some());
        // T5
        assert!(catalog_query_hook("SELECT * FROM pg_catalog.pg_index", &eng).is_some());
        assert!(catalog_query_hook("SELECT * FROM pg_catalog.pg_constraint", &eng).is_some());
        // T7
        assert!(catalog_query_hook("SELECT version()", &eng).is_some());
        // Unrelated SELECT still misses.
        assert!(catalog_query_hook("SELECT id FROM users", &eng).is_none());
        // Non-SELECT still fast-rejected.
        assert!(catalog_query_hook(
            "DELETE FROM information_schema.tables",
            &eng).is_none());
    }

    // ───────────────────────────────────────────────────────────────────
    // T8 (real-psql) KATs — locked invariants for the `\d <name>` step-1
    // OID-lookup recognizer and the `\dn` schema-list recognizer.
    // ───────────────────────────────────────────────────────────────────

    /// **HEADLINE — psql `\d <table>` step-1 query (PG 14 / 16) hits
    /// the hook.** Without this matcher the user gets
    /// `ERROR: sql: unexpected char '~'`, the V1-boundary bug captured
    /// during the 2026-05-28 real-libpq smoke on vulcan.
    #[test]
    fn t8_psql_d_step1_query_fires() {
        let eng = t6_engine();
        // The exact wire SQL psql 16.14 ships for `\d users`. Quoted
        // OID '<oid>' and COLLATE are both in PG 14+; the matcher is
        // tolerant of either being present.
        let q = "SELECT c.oid,\n  n.nspname,\n  c.relname\n\
                 FROM pg_catalog.pg_class c\n     \
                 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n\
                 WHERE c.relname OPERATOR(pg_catalog.~) '^(users)$' COLLATE pg_catalog.default\n  \
                 AND pg_catalog.pg_table_is_visible(c.oid)\n\
                 ORDER BY 2, 3;";
        let res = catalog_query_hook(q, &eng);
        assert!(res.is_some(),
            "psql \\d <name> step-1 query MUST hit the hook (was: parser-rejected)");
        let bytes = res.unwrap();
        // Well-framed response with at least one DataRow for `users`.
        assert_eq!(bytes[0], b'T');
        assert!(bytes.windows(b"users".len()).any(|w| w == b"users"),
            "response MUST include the matched table name");
        assert!(bytes.windows(b"public".len()).any(|w| w == b"public"),
            "response MUST include the public schema name");
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
    }

    /// **`\d <name>` step-1 returns 0 rows for a non-existent table.**
    /// psql then prints "Did not find any relation named X." and exits.
    #[test]
    fn t8_psql_d_step1_query_zero_rows_for_unknown_name() {
        let eng = t6_engine();
        let q = "SELECT c.oid, n.nspname, c.relname \
                 FROM pg_catalog.pg_class c \
                 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.relname OPERATOR(pg_catalog.~) '^(does_not_exist)$' COLLATE pg_catalog.default \
                 AND pg_catalog.pg_table_is_visible(c.oid) \
                 ORDER BY 2, 3;";
        let res = catalog_query_hook(q, &eng).expect("matches");
        // Empty rows, but well-framed.
        assert_eq!(res[0], b'T');
        assert!(res.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
    }

    /// **`\d <name>` step-1 rejects regex metacharacters.** V1 only
    /// supports bare identifier lookup; if psql ships a real pattern
    /// (e.g. `\d t.*`), the matcher punts so the engine path errors
    /// out the same way real PG would (no false synthetic answer).
    #[test]
    fn t8_psql_d_step1_query_punts_on_regex_metachars() {
        // Direct test of the extractor — these would normalize down
        // to forms with `.*` or `[ab]` inside the `^(...)$` capture.
        let with_dot_star = "select c.oid, n.nspname, c.relname \
            from pg_catalog.pg_class c left join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
            where c.relname operator(pg_catalog.~) '^(t.*)$' collate pg_catalog.default \
            and pg_catalog.pg_table_is_visible(c.oid) order by 2, 3";
        assert!(extract_psql_d_step1_relname(with_dot_star).is_none(),
            "regex metachars MUST cause the matcher to punt (engine errors out)");
        // Bare identifier MUST be extracted.
        let bare = "select c.oid, n.nspname, c.relname \
            from pg_catalog.pg_class c left join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
            where c.relname operator(pg_catalog.~) '^(users)$' collate pg_catalog.default \
            and pg_catalog.pg_table_is_visible(c.oid) order by 2, 3";
        assert_eq!(extract_psql_d_step1_relname(bare).as_deref(), Some("users"));
    }

    /// **HEADLINE — psql `\d <name>` STEP 2 (pg_class summary) hits
    /// the hook.** The 15-column projection includes
    /// `c.reloftype::pg_catalog.regtype::pg_catalog.text` which
    /// KesselDB's SQL parser rejects with `unexpected char ':'`.
    #[test]
    fn t8_psql_d_step2_query_fires() {
        let eng = t6_engine();
        // The exact wire SQL psql 16.14 ships for the per-OID step
        // of `\d users`. The OID is the FNV-1a hash of "users" (the
        // first table in t6_engine).
        let users_oid = synthesize::oid_for_table_name("users");
        let q = format!(
            "SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, \
             c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, \
             false AS relhasoids, c.relispartition, '', c.reltablespace, \
             CASE WHEN c.reloftype = 0 THEN '' \
             ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, \
             c.relpersistence, c.relreplident, am.amname \
             FROM pg_catalog.pg_class c \
             LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid) \
             LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid) \
             WHERE c.oid = '{users_oid}';",
        );
        let res = catalog_query_hook(&q, &eng);
        assert!(res.is_some(),
            "psql \\d <name> step-2 query MUST hit the hook (was: ':'-err)");
        let bytes = res.unwrap();
        assert_eq!(bytes[0], b'T');
        // One row for the matched users table.
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"),
            "step-2 MUST emit exactly one row for a valid OID");
        // Locked: relkind='r', amname='heap' for an ordinary table.
        assert!(bytes.windows(b"heap".len()).any(|w| w == b"heap"));
    }

    /// **`\d <name>` step-2 returns 0 rows for an unknown OID.** psql
    /// then prints "No matching relations found." and exits.
    #[test]
    fn t8_psql_d_step2_query_zero_rows_for_unknown_oid() {
        let eng = t6_engine();
        let q = "SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, \
             c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, \
             false AS relhasoids, c.relispartition, '', c.reltablespace, \
             CASE WHEN c.reloftype = 0 THEN '' \
             ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, \
             c.relpersistence, c.relreplident, am.amname \
             FROM pg_catalog.pg_class c \
             LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid) \
             LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid) \
             WHERE c.oid = '999999999';";
        let res = catalog_query_hook(q, &eng).expect("matches");
        assert_eq!(res[0], b'T');
        assert!(res.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
    }

    /// **HEADLINE — psql `\dn` schema-list query hits the hook.**
    /// Without this matcher psql `\dn` errors with
    /// `ERROR: sql: unexpected char '"'` (the quoted `"Name"` alias).
    #[test]
    fn t8_psql_dn_query_fires() {
        let eng = t6_engine();
        let q = "SELECT n.nspname AS \"Name\",\n  \
                 pg_catalog.pg_get_userbyid(n.nspowner) AS \"Owner\"\n\
                 FROM pg_catalog.pg_namespace n\n\
                 WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'\n\
                 ORDER BY 1;";
        let res = catalog_query_hook(q, &eng);
        assert!(res.is_some(), "psql \\dn schema-list MUST hit the hook");
        let bytes = res.unwrap();
        assert_eq!(bytes[0], b'T');
        // V1 single schema = public.
        assert!(bytes.windows(b"public".len()).any(|w| w == b"public"));
        assert!(bytes.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"));
    }

    /// **HEADLINE — psql `\d <name>` pg_policy poll hits the hook.**
    /// Without this matcher, `\d <table>` always aborts after the
    /// column-list with `expected FROM` (subselect rejection).
    #[test]
    fn t8_psql_d_pg_policy_poll_fires() {
        let eng = t6_engine();
        let q = "SELECT pol.polname, pol.polpermissive, \
                 CASE WHEN pol.polroles = '{0}' THEN NULL ELSE \
                 pg_catalog.array_to_string(array(select rolname from \
                 pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END, \
                 pg_catalog.pg_get_expr(pol.polqual, pol.polrelid), \
                 pg_catalog.pg_get_expr(pol.polwithcheck, pol.polrelid), \
                 CASE pol.polcmd WHEN 'r' THEN 'SELECT' WHEN 'a' THEN 'INSERT' \
                 WHEN 'w' THEN 'UPDATE' WHEN 'd' THEN 'DELETE' END AS cmd \
                 FROM pg_catalog.pg_policy pol \
                 WHERE pol.polrelid = '1611034886' ORDER BY 1;";
        let res = catalog_query_hook(q, &eng);
        assert!(res.is_some(),
            "psql \\d <name> pg_policy poll MUST hit the hook");
        let bytes = res.unwrap();
        assert_eq!(bytes[0], b'T');
        // V1 always 0 rows — KesselDB has no RLS.
        assert!(bytes.windows(b"SELECT 0\0".len()).any(|w| w == b"SELECT 0\0"));
    }

    /// **psql `\d <name>` pg_inherits poll** — well-framed empty for
    /// non-partitioned V1 tables.
    #[test]
    fn t8_psql_d_pg_inherits_poll_fires() {
        let eng = t6_engine();
        let q = "SELECT c.oid::pg_catalog.regclass FROM pg_catalog.pg_class c, \
                 pg_catalog.pg_inherits i WHERE c.oid = inhparent AND inhrelid = '1611034886' \
                 ORDER BY inhseqno;";
        // Note: this query contains `::` cast too, so even though the
        // matcher fires for the canonical shape, the IMPORTANT thing
        // is that the matcher returns Some(empty) — without it the
        // engine would error on `::`.
        let res = catalog_query_hook(q, &eng);
        assert!(res.is_some(), "psql pg_inherits poll MUST hit the hook");
    }

    /// **psql `\d <name>` pg_trigger poll** — well-framed empty.
    #[test]
    fn t8_psql_d_pg_trigger_poll_fires() {
        let eng = t6_engine();
        let q = "SELECT t.tgname, t.tgenabled, t.tgisinternal \
                 FROM pg_catalog.pg_trigger t WHERE t.tgrelid = '1611034886' \
                 ORDER BY 1;";
        let res = catalog_query_hook(q, &eng);
        assert!(res.is_some(), "psql pg_trigger poll MUST hit the hook");
    }

    /// **psql `\d <name>` pg_statistic_ext poll** (PG 16+). Without
    /// this matcher the query errors with `unexpected char ':'`
    /// (the `::pg_catalog.regclass` cast).
    #[test]
    fn t8_psql_d_pg_statistic_ext_poll_fires() {
        let eng = t6_engine();
        let q = "SELECT oid, stxrelid::pg_catalog.regclass, \
                 stxnamespace::pg_catalog.regnamespace::pg_catalog.text AS nsp, stxname, \
                 pg_catalog.pg_get_statisticsobjdef_columns(oid) AS columns, \
                 'd' = any(stxkind) AS ndist_enabled, \
                 'f' = any(stxkind) AS deps_enabled, \
                 'm' = any(stxkind) AS mcv_enabled, \
                 stxstattarget FROM pg_catalog.pg_statistic_ext \
                 WHERE stxrelid = '1611034886' ORDER BY nsp, stxname;";
        let res = catalog_query_hook(q, &eng);
        assert!(res.is_some(), "pg_statistic_ext poll MUST hit the hook");
    }

    /// **psql `\d <name>` pg_publication poll.**
    #[test]
    fn t8_psql_d_pg_publication_poll_fires() {
        let eng = t6_engine();
        let q = "SELECT pubname FROM pg_catalog.pg_publication p, \
                 pg_catalog.pg_publication_rel pr \
                 WHERE p.oid = pr.prpubid AND pr.prrelid = '1611034886' ORDER BY 1;";
        let res = catalog_query_hook(q, &eng);
        assert!(res.is_some(), "pg_publication poll MUST hit the hook");
    }

    /// **T8 additions don't break existing T1-T7 patterns.** Lock the
    /// regression boundary.
    #[test]
    fn t8_regression_lock_existing_patterns_still_match() {
        let eng = t6_engine();
        assert!(catalog_query_hook("SELECT * FROM pg_catalog.pg_namespace", &eng).is_some());
        assert!(catalog_query_hook("SELECT * FROM pg_catalog.pg_class", &eng).is_some());
        assert!(catalog_query_hook("SELECT version()", &eng).is_some());
        // SELECTs that look like \d step-1 but lack the JOIN don't match.
        assert!(catalog_query_hook("SELECT c.oid, n.nspname, c.relname FROM users", &eng).is_none());
    }
}
