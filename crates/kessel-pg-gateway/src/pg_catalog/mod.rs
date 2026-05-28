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
    // Fast reject — only SELECT statements hit pg_catalog. Saves us
    // from running the pattern table on every INSERT / UPDATE / DDL.
    if !normalized.starts_with("select") {
        return None;
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
        assert!(catalog_query_hook("SELECT 1", &eng).is_none());
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
}
