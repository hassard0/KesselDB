//! SP-PG-EXTQ-DESCRIBE-VERSION — scalar-SELECT RowDescription synthesizer.
//!
//! The extended-query `Describe(portal|statement)` step needs to know
//! the column shape of the result set BEFORE Execute runs. For ordinary
//! `SELECT * FROM <table>` SQLs, the existing path uses
//! `kessel_sql::select_star_table` + `engine.describe_table` to derive
//! the shape. For scalar SELECTs that SP-PG-EXTQ T7 added Simple-Query
//! handlers for (`SELECT version()`, `SELECT current_user`,
//! `SELECT 1`, etc.), there is no table to describe — and the old path
//! fell through to `NoData`.
//!
//! pgJDBC treats `NoData` as authoritative ("this query returns no
//! rows"). When the subsequent `DataRow` arrived (from the Simple-Query
//! synthesizer's Execute output), pgJDBC raised
//! `IllegalStateException: Received resultset tuples, but no field
//! structure for them`. The wire transcript that proved the bug is in
//! `docs/superpowers/sppgjdbcsmoke-t2-smoke-2026-06-02.txt` §5.
//!
//! This module ships the V1 fix: a closed-set whitelist of scalar SELECT
//! patterns + a tiny per-pattern column shape, mirroring the recognition
//! table in `pg_catalog::synthesize::synthesize_helper_function` so both
//! the Simple-Query and Extended-Query Describe paths agree on the same
//! set of recognized SQLs.
//!
//! ## What this module DOES (V1)
//!
//! - `row_description_for_scalar_select(sql) -> Option<Vec<u8>>` —
//!   returns the `T` RowDescription frame bytes if `sql` matches a known
//!   scalar SELECT pattern; `None` otherwise.
//! - Recognizes the following normalized SQLs (after lowercase +
//!   whitespace-collapse + trailing `;` strip + trailing ` AS <alias>`
//!   strip + `::TYPE` cast strip):
//!   - `select version()` / `select pg_catalog.version()` → ("version", TEXT)
//!   - `select current_database()` / `select current_catalog` → ("current_database", TEXT)
//!   - `select current_schema()` / `select current_schema` → ("current_schema", TEXT)
//!   - `select current_user` / `select user` → ("current_user", TEXT)
//!   - `select session_user` → ("session_user", TEXT)
//!   - `select 1` (bare integer literal) → ("?column?", INT4)
//!   - `select true` / `select false` → ("bool", BOOL)
//!   - `select null` → ("?column?", TEXT)
//!   - `select '<literal>'` (bare string literal) → ("?column?", TEXT)
//!
//! ## What this module does NOT do (named follow-ups)
//!
//! - V2 `SP-PG-EXTQ-DESCRIBE-EXPR` — arbitrary expressions
//!   (`SELECT 1 + 2`, `SELECT length('abc')`).
//! - V2 `SP-PG-EXTQ-DESCRIBE-MULTI-PROJ` — multi-projection SELECTs
//!   without FROM (e.g. `SELECT version(), current_user`); the
//!   Simple-Query path handles these via
//!   `synthesize_pgadmin_multi_helper`, but JDBC issues them in simple
//!   mode (no Describe step), so the Extended-Query Describe gap
//!   doesn't matter in practice yet.
//! - V2 `SP-PG-EXTQ-DESCRIBE-SUBQUERY` — `SELECT col FROM (subquery)`.
//!
//! ## Locked invariants
//!
//! - The recognized pattern set is the SAME set as
//!   `pg_catalog::synthesize::synthesize_helper_function` for scalar
//!   SELECTs. Adding a new scalar SELECT handler to that function
//!   without updating this module would re-trigger the JDBC
//!   `IllegalStateException`. Locked by `t1_pattern_parity_with_simple_query`.
//! - The RowDescription bytes here are byte-equal to the T frame at the
//!   head of `pg_catalog::synthesize::single_text_row`/`single_int_row`/
//!   `single_bool_row` for the same column shape. Locked by
//!   `t1_byte_equal_to_simple_query_t_frame`.

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::cast_stripper::strip_pg_casts;
use crate::proto::{PG_TYPE_BOOL, PG_TYPE_INT4, PG_TYPE_TEXT};
use crate::response::{encode_row_description, FieldMeta};

/// If `sql` matches a known scalar SELECT pattern, return the encoded
/// `T` RowDescription frame bytes for the matching one-column shape.
/// Returns `None` if the SQL does not match any V1 pattern (caller
/// continues with `select_star_table` / `NoData` as before).
///
/// The match is case-insensitive after normalization (lowercase,
/// whitespace-collapsed, trailing `;` stripped, trailing ` AS <alias>`
/// stripped, `::TYPE[(args)]` cast operators stripped).
pub fn row_description_for_scalar_select(sql: &str) -> Option<Vec<u8>> {
    let (name, type_oid) = recognize_scalar_select(sql)?;
    let fields = vec![FieldMeta {
        name: name.to_string(),
        type_oid,
    }];
    Some(encode_row_description(&fields))
}

/// Internal helper — returns the recognized (column_name, type_oid)
/// shape for `sql` if it matches a known scalar SELECT pattern.
/// Pulled out as a separate function so the KATs can verify the
/// recognition table independently of the byte encoding.
pub fn recognize_scalar_select(sql: &str) -> Option<(&'static str, u32)> {
    let normalized = normalize_scalar(sql);
    match normalized.as_str() {
        "select version()" | "select pg_catalog.version()" => {
            Some(("version", PG_TYPE_TEXT))
        }
        "select current_database()" | "select current_catalog" => {
            Some(("current_database", PG_TYPE_TEXT))
        }
        "select current_schema()" | "select current_schema" => {
            Some(("current_schema", PG_TYPE_TEXT))
        }
        "select current_user" | "select user" => {
            Some(("current_user", PG_TYPE_TEXT))
        }
        "select session_user" => Some(("session_user", PG_TYPE_TEXT)),
        "select pg_backend_pid()" => Some(("pg_backend_pid", PG_TYPE_INT4)),
        "select 1" => Some(("?column?", PG_TYPE_INT4)),
        "select true" | "select false" => Some(("bool", PG_TYPE_BOOL)),
        "select null" => Some(("?column?", PG_TYPE_TEXT)),
        other => {
            // `select '<literal>'` — bare single-quoted string literal,
            // no internal `'` allowed (V1 keeps the matcher narrow).
            if let Some(rest) = other.strip_prefix("select '") {
                if let Some(end) = rest.find('\'') {
                    // Must be the closing quote with nothing after it.
                    if end == rest.len() - 1 {
                        return Some(("?column?", PG_TYPE_TEXT));
                    }
                }
            }
            // `select <integer>` — bare unsigned integer literal,
            // no other tokens. INT4 if it fits in i32; the JDBC client
            // does the value-type widening when needed.
            if let Some(rest) = other.strip_prefix("select ") {
                if !rest.is_empty()
                    && rest.bytes().all(|b| b.is_ascii_digit())
                {
                    return Some(("?column?", PG_TYPE_INT4));
                }
            }
            None
        }
    }
}

/// Normalize the SQL for the scalar matcher:
///
/// 1. Strip `::TYPE[(args)]` cast operators (shared with the dispatch-
///    entry stripper) — turns `SELECT 1::int8` into `SELECT 1` before
///    the matcher sees the SQL.
/// 2. Trim whitespace + trailing `;`.
/// 3. Lowercase.
/// 4. Collapse internal whitespace runs to a single space.
/// 5. Strip a trailing ` as <alias>` clause (mirrors
///    `pg_catalog::synthesize::strip_select_alias`).
fn normalize_scalar(sql: &str) -> String {
    let stripped = strip_pg_casts(sql);
    let trimmed = stripped.trim().trim_end_matches(';').trim();
    let lower: String = trimmed.to_ascii_lowercase();
    // Collapse internal whitespace.
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
    let collapsed = out.trim().to_string();
    // Strip trailing ` as <ident>` if present (mirrors
    // pg_catalog::synthesize::strip_select_alias).
    if let Some(pos) = collapsed.rfind(" as ") {
        let tail = &collapsed[pos + 4..];
        if !tail.is_empty()
            && tail.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            return collapsed[..pos].to_string();
        }
    }
    collapsed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{PG_TYPE_BOOL, PG_TYPE_INT4, PG_TYPE_TEXT};
    use crate::response::{encode_row_description, FieldMeta};

    /// **HEADLINE — `SELECT version()` Describe emits a 1-column
    /// "version" RowDescription of type TEXT.** The bytes are
    /// byte-equal to the T frame `single_text_row("version", _)` emits
    /// in the Simple-Query path.
    #[test]
    fn t1_scalar_rd_for_version_text() {
        let bytes = row_description_for_scalar_select("SELECT version()")
            .expect("matches");
        // Tag is 'T' (RowDescription).
        assert_eq!(bytes[0], b'T');
        // 1 field, name "version\0", OID 25 (TEXT).
        let expected = encode_row_description(&[FieldMeta {
            name: "version".to_string(),
            type_oid: PG_TYPE_TEXT,
        }]);
        assert_eq!(bytes, expected, "T frame must be byte-equal to Simple-Query");
        // Trailing `;` tolerated.
        let bytes_semi =
            row_description_for_scalar_select("SELECT version();").expect("matches");
        assert_eq!(bytes_semi, expected);
        // Leading whitespace tolerated.
        let bytes_ws =
            row_description_for_scalar_select("  SELECT  version()  ").expect("matches");
        assert_eq!(bytes_ws, expected);
        // pg_catalog-qualified form recognized.
        let bytes_qual =
            row_description_for_scalar_select("SELECT pg_catalog.version()").expect("matches");
        assert_eq!(bytes_qual, expected);
        // Trailing AS alias tolerated.
        let bytes_alias =
            row_description_for_scalar_select("SELECT version() AS v").expect("matches");
        assert_eq!(bytes_alias, expected);
    }

    /// **`SELECT current_user` / `SELECT user` Describe emits a
    /// "current_user" RowDescription of type TEXT.**
    #[test]
    fn t1_scalar_rd_for_current_user_text() {
        let expected = encode_row_description(&[FieldMeta {
            name: "current_user".to_string(),
            type_oid: PG_TYPE_TEXT,
        }]);
        for sql in ["SELECT current_user", "SELECT user", "select CURRENT_USER"] {
            let bytes = row_description_for_scalar_select(sql).expect("matches");
            assert_eq!(bytes, expected, "for sql {sql:?}");
        }
    }

    /// **`SELECT current_database()` Describe emits a
    /// "current_database" RowDescription of type TEXT.**
    #[test]
    fn t1_scalar_rd_for_current_database_text() {
        let expected = encode_row_description(&[FieldMeta {
            name: "current_database".to_string(),
            type_oid: PG_TYPE_TEXT,
        }]);
        for sql in ["SELECT current_database()", "SELECT current_catalog"] {
            let bytes = row_description_for_scalar_select(sql).expect("matches");
            assert_eq!(bytes, expected, "for sql {sql:?}");
        }
    }

    /// **`SELECT current_schema[()]` Describe emits a "current_schema"
    /// RowDescription of type TEXT — both paren'd and bare forms.**
    #[test]
    fn t1_scalar_rd_for_current_schema_text() {
        let expected = encode_row_description(&[FieldMeta {
            name: "current_schema".to_string(),
            type_oid: PG_TYPE_TEXT,
        }]);
        for sql in ["SELECT current_schema()", "SELECT current_schema"] {
            let bytes = row_description_for_scalar_select(sql).expect("matches");
            assert_eq!(bytes, expected, "for sql {sql:?}");
        }
    }

    /// **`SELECT session_user` Describe emits "session_user" TEXT.**
    #[test]
    fn t1_scalar_rd_for_session_user_text() {
        let expected = encode_row_description(&[FieldMeta {
            name: "session_user".to_string(),
            type_oid: PG_TYPE_TEXT,
        }]);
        let bytes = row_description_for_scalar_select("SELECT session_user").expect("matches");
        assert_eq!(bytes, expected);
    }

    /// **`SELECT 1` Describe emits a "?column?" RowDescription of type
    /// INT4 (the PG canonical for an anonymous integer literal).**
    #[test]
    fn t1_scalar_rd_for_select_1_int4() {
        let expected = encode_row_description(&[FieldMeta {
            name: "?column?".to_string(),
            type_oid: PG_TYPE_INT4,
        }]);
        let bytes = row_description_for_scalar_select("SELECT 1").expect("matches");
        assert_eq!(bytes, expected);
        // Also accept larger integers (all map to INT4 in V1 — the
        // JDBC client widens the value type at runtime).
        let bytes_42 = row_description_for_scalar_select("SELECT 42").expect("matches");
        assert_eq!(bytes_42, expected);
    }

    /// **`SELECT 'hello'` Describe emits a "?column?" RowDescription
    /// of type TEXT.**
    #[test]
    fn t1_scalar_rd_for_string_literal_text() {
        let expected = encode_row_description(&[FieldMeta {
            name: "?column?".to_string(),
            type_oid: PG_TYPE_TEXT,
        }]);
        let bytes = row_description_for_scalar_select("SELECT 'hello'").expect("matches");
        assert_eq!(bytes, expected);
        let bytes_empty = row_description_for_scalar_select("SELECT ''").expect("matches");
        assert_eq!(bytes_empty, expected);
    }

    /// **`SELECT NULL` Describe emits a "?column?" RowDescription of
    /// type TEXT (PG's default for untyped NULL).**
    #[test]
    fn t1_scalar_rd_for_null_text() {
        let expected = encode_row_description(&[FieldMeta {
            name: "?column?".to_string(),
            type_oid: PG_TYPE_TEXT,
        }]);
        let bytes = row_description_for_scalar_select("SELECT NULL").expect("matches");
        assert_eq!(bytes, expected);
        let bytes_lower = row_description_for_scalar_select("select null").expect("matches");
        assert_eq!(bytes_lower, expected);
    }

    /// **`SELECT true` / `SELECT false` Describe emits "bool" of type
    /// BOOL.**
    #[test]
    fn t1_scalar_rd_for_bool_literal_bool() {
        let expected = encode_row_description(&[FieldMeta {
            name: "bool".to_string(),
            type_oid: PG_TYPE_BOOL,
        }]);
        let bytes_true = row_description_for_scalar_select("SELECT true").expect("matches");
        assert_eq!(bytes_true, expected);
        let bytes_false = row_description_for_scalar_select("SELECT false").expect("matches");
        assert_eq!(bytes_false, expected);
    }

    /// **HEADLINE — `SELECT 1::int8` Describe (post cast-strip) emits
    /// the same "?column?" / INT4 shape as `SELECT 1`.** Covers the
    /// pgJDBC simple-mode-after-cast-strip scenario described in
    /// SP-PG-EXTQ-CAST.
    #[test]
    fn t1_scalar_rd_for_int8_cast_strip_int4() {
        let expected = encode_row_description(&[FieldMeta {
            name: "?column?".to_string(),
            type_oid: PG_TYPE_INT4,
        }]);
        for sql in [
            "SELECT 1::int8",
            "SELECT 1::int4",
            "SELECT 42::int8",
            "SELECT 1 :: int8",
        ] {
            let bytes = row_description_for_scalar_select(sql).expect("matches");
            assert_eq!(bytes, expected, "for sql {sql:?}");
        }
    }

    /// **`SELECT * FROM t` MUST fall through to the existing path
    /// (returns None here so the caller proceeds with select_star_table).**
    #[test]
    fn t1_scalar_rd_for_select_star_table_falls_through() {
        assert!(row_description_for_scalar_select("SELECT * FROM t").is_none());
        assert!(row_description_for_scalar_select("SELECT * FROM users").is_none());
        assert!(
            row_description_for_scalar_select("SELECT * FROM t WHERE id = 1").is_none()
        );
    }

    /// **Multi-statement SQL must NOT match — the dispatcher splits on
    /// `;`, but defensively we reject here too.**
    #[test]
    fn t1_scalar_rd_for_multi_statement_returns_none() {
        // After trim, `SELECT version(); SELECT 1` is not a normalized
        // single-stmt scalar pattern.
        assert!(row_description_for_scalar_select("SELECT version(); SELECT 1").is_none());
        // Multi-projection without FROM also rejected (V1 out-of-scope).
        assert!(row_description_for_scalar_select("SELECT 1, 2").is_none());
        assert!(
            row_description_for_scalar_select("SELECT version(), current_user").is_none()
        );
    }

    /// **Single-column projection from a real table is V1 out-of-scope
    /// — falls through to the `select_star_table` path (which itself
    /// returns None) and ultimately to NoData.**
    #[test]
    fn t1_scalar_rd_for_single_column_projection_returns_none() {
        assert!(row_description_for_scalar_select("SELECT col FROM t").is_none());
        assert!(row_description_for_scalar_select("SELECT id, name FROM t").is_none());
    }

    /// **Unrecognized SELECTs fall through (return None) — covers SQL
    /// that the matcher must NOT misclassify.**
    #[test]
    fn t1_scalar_rd_for_unrecognized_select_returns_none() {
        // Expressions are V2 scope.
        assert!(row_description_for_scalar_select("SELECT 1 + 2").is_none());
        // Function call with args we don't recognize.
        assert!(row_description_for_scalar_select("SELECT my_func(1)").is_none());
        // Non-SELECT.
        assert!(row_description_for_scalar_select("INSERT INTO t (id) VALUES (1)").is_none());
        assert!(row_description_for_scalar_select("CREATE TABLE t (id BIGINT)").is_none());
        // Empty.
        assert!(row_description_for_scalar_select("").is_none());
        assert!(row_description_for_scalar_select("   ").is_none());
        // Just whitespace + semicolon.
        assert!(row_description_for_scalar_select(" ; ").is_none());
    }

    /// **Locked invariant — the recognized column shape for each
    /// recognized SQL is stable across refactors.** Mirrors the
    /// Simple-Query handler table in
    /// `pg_catalog::synthesize::synthesize_helper_function`.
    #[test]
    fn t1_pattern_recognition_table_is_stable() {
        let cases = [
            ("SELECT version()", "version", PG_TYPE_TEXT),
            ("SELECT pg_catalog.version()", "version", PG_TYPE_TEXT),
            ("SELECT current_database()", "current_database", PG_TYPE_TEXT),
            ("SELECT current_catalog", "current_database", PG_TYPE_TEXT),
            ("SELECT current_schema()", "current_schema", PG_TYPE_TEXT),
            ("SELECT current_schema", "current_schema", PG_TYPE_TEXT),
            ("SELECT current_user", "current_user", PG_TYPE_TEXT),
            ("SELECT user", "current_user", PG_TYPE_TEXT),
            ("SELECT session_user", "session_user", PG_TYPE_TEXT),
            ("SELECT 1", "?column?", PG_TYPE_INT4),
            ("SELECT true", "bool", PG_TYPE_BOOL),
            ("SELECT false", "bool", PG_TYPE_BOOL),
            ("SELECT NULL", "?column?", PG_TYPE_TEXT),
            ("SELECT 'hello'", "?column?", PG_TYPE_TEXT),
            ("SELECT 1::int8", "?column?", PG_TYPE_INT4),
        ];
        for (sql, expected_name, expected_oid) in cases {
            let (name, oid) = recognize_scalar_select(sql)
                .unwrap_or_else(|| panic!("MUST recognize: {sql:?}"));
            assert_eq!(name, expected_name, "name mismatch for {sql:?}");
            assert_eq!(oid, expected_oid, "oid mismatch for {sql:?}");
        }
    }
}
