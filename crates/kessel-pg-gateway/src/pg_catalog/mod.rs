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
    // T1 ships exactly ONE pattern: `SELECT * FROM pg_catalog.pg_namespace`
    // (with optional whitespace + case-insensitive matching + tolerance
    // for the unqualified `SELECT * FROM pg_namespace` form). T2+ grow
    // the pattern table via a static `[(predicate, synthesizer)]` array.
    if matches_pg_namespace_select_star(&normalized) {
        return Some(synthesize::pg_namespace_all_rows());
    }
    let _ = engine; // T1: the pg_namespace synthesizer is canned;
                    // T3+ synthesizers will use the engine handle.
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
}
