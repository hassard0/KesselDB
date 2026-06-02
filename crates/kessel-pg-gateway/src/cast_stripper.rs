//! SP-PG-EXTQ-CAST T2 — strip PostgreSQL `::TYPE[(args)]` type-cast
//! operator from SQL text before it reaches `kessel-sql`'s lexer.
//!
//! ## Why
//!
//! pgJDBC's `preferQueryMode=simple` (and a handful of PostGIS /
//! pgvector helpers) inject `::int8` / `::text` / `::numeric(15,2)`
//! type-cast operators into the SQL text. `kessel-sql`'s lexer
//! rejects `:` with `42601 syntax_error: unexpected char ':'`, so the
//! whole simple-mode JDBC path returns `PARTIAL` in the SP-PG-EXTQ T8
//! ORM compat matrix.
//!
//! This module strips the cast text BEFORE the dispatcher hits the
//! lexer. The engine's existing type-checker handles implicit type
//! coercion at INSERT / WHERE-comparison sites; the `::TYPE` text is
//! redundant under our type system because the column type already
//! gives the target type via `describe_table`.
//!
//! ## What it does NOT do
//!
//! - Validate that the cast was well-typed (V2
//!   `SP-PG-EXTQ-CAST-VALIDATE`).
//! - Handle nested casts `(a::int)::text` — V1 strips both flat
//!   passes; nested-depth tracking is V2 `SP-PG-EXTQ-CAST-NESTED`.
//! - Recognise multi-word type names `TIMESTAMP WITH TIME ZONE`,
//!   `DOUBLE PRECISION` — V1 strips only the first identifier (pgJDBC
//!   uses the spaceless aliases `timestamptz`, `float8` so this is
//!   sufficient in practice; lift via V2
//!   `SP-PG-EXTQ-CAST-MULTIWORD-TYPE`).
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-06-01-kesseldb-sppgextqcast-design.md`

#![forbid(unsafe_code)]

/// Strip every `::TYPE[(args)]` PostgreSQL type-cast operator from
/// `sql`, preserving cast-like text inside single-quoted string
/// literals, `--` line comments, and `/* ... */` block comments.
///
/// Returns an owned `String` because the rewrite is bytes-out-bytes-in
/// with possible shrinking. For SQL containing no `::`, the returned
/// string equals the input byte-for-byte (verified by K-CAST-2 +
/// `no_cast_pure_passthrough_fuzz`).
///
/// The scanner is single-pass + O(sql.len()) + zero-alloc-per-byte
/// beyond the `Vec<u8>` output buffer. Pre-sized to the input length
/// because cast stripping can only shrink (never grow) the SQL.
pub fn strip_pg_casts(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0usize;

    while i < bytes.len() {
        let b = bytes[i];

        // Single-quoted string literal — copy through to the closing
        // quote, handling the doubled-quote escape `''` per PG §4.1.2.1.
        if b == b'\'' {
            out.push(b);
            i += 1;
            while i < bytes.len() {
                let c = bytes[i];
                out.push(c);
                i += 1;
                if c == b'\'' {
                    // Doubled-quote: stay in the string.
                    if i < bytes.len() && bytes[i] == b'\'' {
                        out.push(b'\'');
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }

        // Line comment `--` — copy through to the next newline.
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() {
                let c = bytes[i];
                out.push(c);
                i += 1;
                if c == b'\n' {
                    break;
                }
            }
            continue;
        }

        // Block comment `/* ... */` — copy through to the closing
        // `*/`. PG block comments do NOT nest in the strip path (a
        // real PG parser does; we don't need to here because the
        // strip is conservative — if a nested `*/` ends us early,
        // the worst case is we strip a `::TYPE` inside a comment,
        // which is still semantically safe because the engine
        // wouldn't see comment text either way).
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            out.push(bytes[i]);
            out.push(bytes[i + 1]);
            i += 2;
            while i < bytes.len() {
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    out.push(bytes[i]);
                    out.push(bytes[i + 1]);
                    i += 2;
                    break;
                }
                out.push(bytes[i]);
                i += 1;
            }
            continue;
        }

        // The cast itself: `::IDENT[(args)]`.
        //
        // Two-byte lookahead — we already know `b == bytes[i]`; check
        // that this byte is `:` AND the next is also `:`. The double-
        // colon disambiguates a real cast from the `:NAMED` parameter
        // pattern (which the gateway doesn't see — it's substituted
        // client-side — but cheap to be defensive).
        if b == b':' && i + 1 < bytes.len() && bytes[i + 1] == b':' {
            i += 2; // skip `::`
            // Skip whitespace between `::` and the type identifier
            // (pgJDBC sometimes emits `:: int8`).
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            // Skip the type identifier (ASCII `[A-Za-z_][A-Za-z0-9_]*`).
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
            {
                i += 1;
            }
            // Skip the optional `(args)` for parameterised types
            // (`numeric(15,2)`, `varchar(255)`). One level only — V1
            // doesn't track nested parens (the `(args)` body of a
            // PG type spec doesn't contain nested parens in any of
            // the V1-supported pgJDBC emits).
            if i < bytes.len() && bytes[i] == b'(' {
                i += 1; // skip `(`
                while i < bytes.len() && bytes[i] != b')' {
                    i += 1;
                }
                if i < bytes.len() {
                    i += 1; // skip `)`
                }
            }
            continue;
        }

        // Default: copy the byte through.
        out.push(b);
        i += 1;
    }

    // The scanner only produces valid UTF-8 because it preserves every
    // multi-byte sequence intact (it only strips ASCII regions: the
    // `::` operator, ASCII identifiers, and ASCII `(...)`). Defensive
    // fallback to the input on the impossible UTF-8 error keeps the
    // signature infallible.
    String::from_utf8(out).unwrap_or_else(|_| sql.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- K-CAST-1 ----
    #[test]
    fn k_cast_1_empty_string_is_no_op() {
        assert_eq!(strip_pg_casts(""), "");
    }

    // ---- K-CAST-2 ----
    #[test]
    fn k_cast_2_no_casts_pure_passthrough() {
        let sql = "SELECT id, name FROM users WHERE id = 42";
        assert_eq!(strip_pg_casts(sql), sql);
    }

    // ---- K-CAST-3 ----
    #[test]
    fn k_cast_3_select_one_int8() {
        assert_eq!(strip_pg_casts("SELECT 1::int8"), "SELECT 1");
    }

    // ---- K-CAST-4 ----
    #[test]
    fn k_cast_4_select_col_text_from_t() {
        assert_eq!(
            strip_pg_casts("SELECT col::text FROM t"),
            "SELECT col FROM t"
        );
    }

    // ---- K-CAST-5 ----
    #[test]
    fn k_cast_5_where_param_int4() {
        assert_eq!(
            strip_pg_casts("WHERE col = $1::int4"),
            "WHERE col = $1"
        );
    }

    // ---- K-CAST-6 ----
    #[test]
    fn k_cast_6_literal_with_cast_inside_string_preserved() {
        let sql = "SELECT 'literal ::int8 inside'";
        assert_eq!(strip_pg_casts(sql), sql);
    }

    // ---- K-CAST-7 ----
    #[test]
    fn k_cast_7_line_comment_with_cast_preserved() {
        let sql = "-- comment ::int8 trailing\nSELECT 1";
        assert_eq!(strip_pg_casts(sql), sql);
    }

    // ---- K-CAST-8 ----
    #[test]
    fn k_cast_8_block_comment_with_cast_preserved() {
        let sql = "/* ::int8 block */ SELECT 1";
        assert_eq!(strip_pg_casts(sql), sql);
    }

    // ---- K-CAST-9 ----
    #[test]
    fn k_cast_9_doubled_quote_in_string_preserved() {
        let sql = "SELECT 'O''Reilly ::ok' FROM t";
        assert_eq!(strip_pg_casts(sql), sql);
    }

    // ---- K-CAST-10 ----
    #[test]
    fn k_cast_10_multiple_casts_in_one_query() {
        assert_eq!(
            strip_pg_casts("SELECT a::int, b::text FROM t"),
            "SELECT a, b FROM t"
        );
    }

    // ---- K-CAST-11 ----
    #[test]
    fn k_cast_11_parameterised_type_numeric() {
        assert_eq!(
            strip_pg_casts("NULL::numeric(15,2)"),
            "NULL"
        );
    }

    // ---- K-CAST-12 ----
    #[test]
    fn k_cast_12_cast_at_end_of_sql_no_trailing_space() {
        // No trailing whitespace after the type identifier.
        assert_eq!(strip_pg_casts("SELECT 1::int8"), "SELECT 1");
    }

    // ---- K-CAST-13 ----
    #[test]
    fn k_cast_13_lone_colon_untouched() {
        assert_eq!(strip_pg_casts(":"), ":");
        // And a single colon inside the SQL stays put.
        assert_eq!(
            strip_pg_casts("SELECT * FROM t WHERE x = ':' "),
            "SELECT * FROM t WHERE x = ':' "
        );
    }

    // ---- K-CAST-14 ----
    #[test]
    fn k_cast_14_cast_inside_string_stays_outside_strips() {
        // The `'a::b'` literal stays untouched; the trailing `::text`
        // cast (outside the string) is stripped.
        assert_eq!(
            strip_pg_casts("SELECT 'a::b'::text"),
            "SELECT 'a::b'"
        );
    }

    // ---- K-CAST-15 ----
    #[test]
    fn k_cast_15_null_timestamp_basic() {
        // V1 strips only the first identifier — `WITH TIME ZONE`
        // stays. pgJDBC simple-mode uses the spaceless alias
        // `timestamptz` so this hits in practice.
        assert_eq!(
            strip_pg_casts("NULL::timestamp WITH TIME ZONE"),
            "NULL WITH TIME ZONE"
        );
        // And the spaceless alias goes clean.
        assert_eq!(
            strip_pg_casts("NULL::timestamptz"),
            "NULL"
        );
    }

    // ---- extra coverage beyond the K-CAST table ----

    #[test]
    fn cast_with_whitespace_between_colons_and_type() {
        // pgJDBC sometimes emits a space — handle it.
        assert_eq!(strip_pg_casts("SELECT 1::  int8"), "SELECT 1");
    }

    #[test]
    fn parameterised_varchar_with_size() {
        assert_eq!(
            strip_pg_casts("SELECT col::varchar(255) FROM t"),
            "SELECT col FROM t"
        );
    }

    #[test]
    fn cast_in_select_list_and_where_combined() {
        assert_eq!(
            strip_pg_casts("SELECT id::int8 FROM t WHERE name = $1::text"),
            "SELECT id FROM t WHERE name = $1"
        );
    }

    #[test]
    fn block_comment_with_unterminated_block_safe() {
        // Unterminated `/* ...` — we read to end-of-input + emit
        // verbatim. The strip never panics.
        let sql = "SELECT 1 /* unterminated";
        assert_eq!(strip_pg_casts(sql), sql);
    }

    #[test]
    fn cast_to_uppercase_type_name() {
        // PG type names are case-insensitive; the strip matches any
        // ASCII identifier so uppercase works.
        assert_eq!(strip_pg_casts("SELECT 1::INT8"), "SELECT 1");
    }

    #[test]
    fn cast_with_underscore_type_name() {
        // `_int4` is the array-of-int4 type name.
        assert_eq!(
            strip_pg_casts("SELECT '{1,2}'::_int4"),
            "SELECT '{1,2}'"
        );
    }

    #[test]
    fn no_cast_pure_passthrough_fuzz() {
        // Spot-check that varied SQL without `::` is byte-equal.
        let inputs = [
            "",
            "SELECT 1",
            "SELECT 'a' FROM t",
            "INSERT INTO t (a, b) VALUES (1, 2)",
            "UPDATE t SET a = 'x'",
            "DELETE FROM t WHERE id = 1",
            "CREATE TABLE t (id BIGINT)",
            "-- only a comment",
            "/* block */ SELECT 1",
            "SELECT a, b, c FROM x WHERE y = $1 AND z = $2",
            "SELECT 'O''Reilly' AS name",
        ];
        for s in inputs {
            assert_eq!(strip_pg_casts(s), s, "input: {s:?}");
        }
    }

    #[test]
    fn semicolon_after_cast_is_preserved() {
        // The trailing `;` survives the strip.
        assert_eq!(strip_pg_casts("SELECT 1::int8;"), "SELECT 1;");
    }

    #[test]
    fn jdbc_simple_mode_select_id_int8_from_table() {
        // The exact shape pgJDBC simple-mode emits for a long-typed
        // SELECT column.
        assert_eq!(
            strip_pg_casts("SELECT id::int8 FROM smoke"),
            "SELECT id FROM smoke"
        );
    }
}
