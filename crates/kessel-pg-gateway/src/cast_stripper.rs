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
///
/// SP-PG-EXTQ-CAST-VALIDATE T2 — V1's signature is preserved as a
/// thin wrapper around `strip_pg_casts_tracked` that drops the
/// `Vec<(usize, u32)>` tracking vec. The byte-equality invariant
/// remains locked: every caller of `strip_pg_casts` still gets the
/// exact same `String` back.
pub fn strip_pg_casts(sql: &str) -> String {
    strip_pg_casts_tracked(sql).0
}

/// SP-PG-EXTQ-CAST-VALIDATE T2 — like `strip_pg_casts` but also
/// returns the list of `(zero_based_param_index, declared_cast_oid)`
/// pairs the scanner observed.
///
/// A pair is recorded ONLY when:
/// 1. The `::TYPE` operator is immediately preceded by a `$N`
///    placeholder (no whitespace between `$N` and `::` — pgJDBC
///    simple-mode emits the placeholder-and-cast as a single token).
/// 2. The type name is recognised by `type_name_to_oid`. Unknown
///    type names skip the tracking record (V1 decision: fall back
///    to V1's "strip + hope" behaviour for unknown types; lets a
///    future workload's PG type avoid a hard failure at the
///    validator).
/// 3. The placeholder index `N` is `>= 1` (PG `$0` is malformed and
///    the gateway rejects it elsewhere; if it slips through here we
///    just don't record).
///
/// Index returned is `N - 1` (zero-based) to match the storage
/// convention in `extq::PreparedStmt.param_oids` and
/// `extq::Portal.param_values`.
///
/// Used by `extq::dispatch_parse` to populate
/// `PreparedStmt.param_casts`. The validator at `dispatch_bind`
/// rejects any mismatch between the bound parameter OID and the
/// declared cast OID with `42846 cannot_coerce`.
pub fn strip_pg_casts_tracked(sql: &str) -> (String, Vec<(usize, u32)>) {
    let bytes = sql.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut casts: Vec<(usize, u32)> = Vec::new();
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
            // SP-PG-EXTQ-CAST-VALIDATE T2 — look backward in `out` for
            // a `$N` placeholder immediately preceding the `::`. This
            // is the "we have a bound parameter at this position with
            // a declared type" signal. If present, we'll record a
            // tracking pair after we identify the type.
            let pending_param_index: Option<usize> = look_back_for_dollar_param(&out);

            i += 2; // skip `::`
            // Skip whitespace between `::` and the type identifier
            // (pgJDBC sometimes emits `:: int8`).
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            // Capture the type identifier so we can look up its OID.
            let type_name_start = i;
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
            {
                i += 1;
            }
            let type_name = &bytes[type_name_start..i];
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
            // Record the tracking pair IFF we saw `$N` immediately
            // before AND the type name maps to a recognised OID.
            if let (Some(idx), Some(oid)) =
                (pending_param_index, type_name_to_oid(type_name))
            {
                casts.push((idx, oid));
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
    let s = String::from_utf8(out).unwrap_or_else(|_| sql.to_string());
    (s, casts)
}

/// SP-PG-EXTQ-CAST-VALIDATE T2 — inspect the tail of the strip
/// scanner's output buffer for a `$N` placeholder immediately preceding
/// the current position. Returns `Some(N - 1)` (zero-based parameter
/// index) when the tail matches `$` followed by one-or-more ASCII
/// digits forming a `N >= 1`; returns `None` otherwise.
///
/// "Immediately preceding" means: from `out.len()` walking backwards,
/// every byte read is an ASCII digit until we hit a `$`. No whitespace
/// allowed (pgJDBC simple-mode + every other client emits `$1::int8`
/// as a single token without a space).
fn look_back_for_dollar_param(out: &[u8]) -> Option<usize> {
    let mut j = out.len();
    while j > 0 && out[j - 1].is_ascii_digit() {
        j -= 1;
    }
    if j == out.len() {
        // No digits — can't be `$N`.
        return None;
    }
    if j == 0 || out[j - 1] != b'$' {
        // The byte before the digits isn't `$`.
        return None;
    }
    // Parse the digits as a `u32` (more than enough headroom for any
    // realistic $N — PG itself caps at $65535 but the V1 gateway caps
    // earlier in the substitute layer).
    let digits = &out[j..];
    // `from_utf8_unchecked` would be cheaper but we forbid unsafe; the
    // ASCII-only check above means `from_utf8` always succeeds.
    let n = std::str::from_utf8(digits).ok()?.parse::<u32>().ok()?;
    if n == 0 {
        // PG `$0` is malformed; don't record. Gateway rejects it
        // separately.
        return None;
    }
    Some((n - 1) as usize)
}

/// SP-PG-EXTQ-CAST-VALIDATE T2 — map a PG type name (the identifier
/// between `::` and `[(args)]`) to its PG `pg_type.dat` OID.
///
/// Match is case-insensitive (ASCII). Unknown names return `None` so
/// the scanner skips recording (V1 decision: fall back to V1's
/// "strip + hope" behaviour for unknown types; lets a future PG type
/// a workload starts using avoid a hard failure at the validator).
///
/// Covers every type the V1 gateway type-name table emits + the
/// canonical pgJDBC simple-mode set. Add new entries as new types
/// land in `crate::types` / `crate::proto`.
fn type_name_to_oid(name: &[u8]) -> Option<u32> {
    // Lowercase-compare without allocating beyond the small buffer.
    // Type names are short; 32 bytes covers every V1 entry.
    let mut buf = [0u8; 32];
    if name.is_empty() || name.len() > buf.len() {
        return None;
    }
    for (i, &b) in name.iter().enumerate() {
        buf[i] = b.to_ascii_lowercase();
    }
    let lower = &buf[..name.len()];
    Some(match lower {
        // Integer family.
        b"int2" | b"smallint" => crate::proto::PG_TYPE_INT2,
        b"int4" | b"int" | b"integer" => crate::proto::PG_TYPE_INT4,
        b"int8" | b"bigint" => crate::proto::PG_TYPE_INT8,
        // String family.
        b"text" => crate::proto::PG_TYPE_TEXT,
        b"varchar" => crate::proto::PG_TYPE_VARCHAR,
        // Boolean.
        b"bool" | b"boolean" => crate::proto::PG_TYPE_BOOL,
        // Byte array.
        b"bytea" => crate::proto::PG_TYPE_BYTEA,
        // Floating point.
        b"float4" | b"real" => crate::proto::PG_TYPE_FLOAT4,
        b"float8" => crate::proto::PG_TYPE_FLOAT8,
        // Numeric.
        b"numeric" | b"decimal" => crate::proto::PG_TYPE_NUMERIC,
        // Timestamps. V1 only handles the spaceless alias —
        // `timestamp with time zone` is multi-word per the parent
        // arc's K-CAST-15 boundary.
        b"timestamptz" => crate::proto::PG_TYPE_TIMESTAMPTZ,
        _ => return None,
    })
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

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-EXTQ-CAST-VALIDATE T2 KATs — `strip_pg_casts_tracked`.
    // ───────────────────────────────────────────────────────────────────

    #[test]
    fn tracked_strip_returns_pair_for_dollar_param_cast() {
        let (sql, casts) = strip_pg_casts_tracked("SELECT $1::int8");
        assert_eq!(sql, "SELECT $1");
        assert_eq!(casts, vec![(0, crate::proto::PG_TYPE_INT8)]);
    }

    #[test]
    fn tracked_strip_does_not_track_literal_cast() {
        // Literal cast (`1::int8`) — no `$N` immediately before `::`,
        // so no tracking pair recorded. The V1 strip behaviour is
        // preserved otherwise.
        let (sql, casts) = strip_pg_casts_tracked("SELECT 1::int8");
        assert_eq!(sql, "SELECT 1");
        assert!(casts.is_empty(), "literal cast must NOT track: {casts:?}");
    }

    #[test]
    fn tracked_strip_handles_multiple_params() {
        let (sql, casts) =
            strip_pg_casts_tracked("WHERE id = $1::int8 AND name = $2::text");
        assert_eq!(sql, "WHERE id = $1 AND name = $2");
        assert_eq!(
            casts,
            vec![
                (0, crate::proto::PG_TYPE_INT8),
                (1, crate::proto::PG_TYPE_TEXT),
            ]
        );
    }

    #[test]
    fn tracked_strip_handles_unknown_type_name() {
        // Unknown type name (`weirdtype`) — V1 still strips the bytes
        // (parent arc behaviour) but does NOT record a tracking pair.
        // The validator at dispatch_bind treats unknown types as
        // "fall through to strip + hope" (V1 of THIS arc decision).
        let (sql, casts) = strip_pg_casts_tracked("SELECT $1::weirdtype");
        assert_eq!(sql, "SELECT $1");
        assert!(
            casts.is_empty(),
            "unknown type must NOT track: {casts:?}"
        );
    }

    #[test]
    fn tracked_strip_unknown_param_index_no_record() {
        // `$0` is malformed per PG; we don't record. The stripper is
        // lenient and still drops the `::int8` bytes so kessel-sql's
        // downstream classifier produces the canonical error.
        let (sql, casts) = strip_pg_casts_tracked("SELECT $0::int8");
        assert_eq!(sql, "SELECT $0");
        assert!(
            casts.is_empty(),
            "PG \\$0 must NOT track: {casts:?}"
        );
    }

    #[test]
    fn tracked_strip_thin_wrapper_byte_equal_to_v1() {
        // Regression guard — `strip_pg_casts(sql)` MUST equal
        // `strip_pg_casts_tracked(sql).0` for the entire V1 K-CAST-1..15
        // set + key extras. The wrapper-vs-tracked split is invisible
        // at the original byte-equality contract.
        let inputs = [
            "",
            "SELECT id, name FROM users WHERE id = 42",
            "SELECT 1::int8",
            "SELECT col::text FROM t",
            "WHERE col = $1::int4",
            "SELECT 'literal ::int8 inside'",
            "-- comment ::int8 trailing\nSELECT 1",
            "/* ::int8 block */ SELECT 1",
            "SELECT 'O''Reilly ::ok' FROM t",
            "SELECT a::int, b::text FROM t",
            "NULL::numeric(15,2)",
            ":",
            "SELECT * FROM t WHERE x = ':' ",
            "SELECT 'a::b'::text",
            "NULL::timestamp WITH TIME ZONE",
            "NULL::timestamptz",
            "SELECT 1::  int8",
            "SELECT col::varchar(255) FROM t",
            "SELECT id::int8 FROM t WHERE name = $1::text",
            "SELECT 1 /* unterminated",
            "SELECT 1::INT8",
            "SELECT '{1,2}'::_int4",
            "SELECT 1::int8;",
            "SELECT id::int8 FROM smoke",
        ];
        for s in inputs {
            let v1 = strip_pg_casts(s);
            let tracked = strip_pg_casts_tracked(s).0;
            assert_eq!(v1, tracked, "wrapper drifted for {s:?}");
        }
    }

    #[test]
    fn tracked_strip_dollar_param_inside_string_not_tracked() {
        // `$1::int8` inside a string literal is NOT a cast — the
        // scanner skips the whole literal. Locked here because a
        // refactor that drops the string-context handling would
        // silently start tracking strings.
        let sql = "SELECT '$1::int8 inside'";
        let (out, casts) = strip_pg_casts_tracked(sql);
        assert_eq!(out, sql);
        assert!(casts.is_empty(), "string-literal cast must NOT track: {casts:?}");
    }

    #[test]
    fn tracked_strip_param_then_literal_records_only_param() {
        // Mixed shape: `$1::int8, 1::text` records the param cast
        // (`$1 -> int8`) but not the literal cast (`1::text`).
        let (sql, casts) = strip_pg_casts_tracked("SELECT $1::int8, 1::text FROM t");
        assert_eq!(sql, "SELECT $1, 1 FROM t");
        assert_eq!(casts, vec![(0, crate::proto::PG_TYPE_INT8)]);
    }

    #[test]
    fn tracked_strip_param_cast_with_parameterised_type() {
        // `$1::numeric(15,2)` — the `(15,2)` suffix shouldn't break
        // the tracking. Records `(0, PG_TYPE_NUMERIC)`.
        let (sql, casts) = strip_pg_casts_tracked("SELECT $1::numeric(15,2)");
        assert_eq!(sql, "SELECT $1");
        assert_eq!(casts, vec![(0, crate::proto::PG_TYPE_NUMERIC)]);
    }

    #[test]
    fn tracked_strip_high_param_index() {
        // `$10` — multi-digit index. Records `(9, PG_TYPE_INT8)`
        // (zero-based).
        let (sql, casts) = strip_pg_casts_tracked("SELECT $10::int8");
        assert_eq!(sql, "SELECT $10");
        assert_eq!(casts, vec![(9, crate::proto::PG_TYPE_INT8)]);
    }

    #[test]
    fn tracked_strip_type_name_oid_lookup_table_canonical() {
        // Exhaustive smoke for the type-name lookup table. Each entry
        // here MUST round-trip to the documented OID; the table is
        // the contract the validator hangs off.
        let cases: &[(&str, u32)] = &[
            ("SELECT $1::int2", crate::proto::PG_TYPE_INT2),
            ("SELECT $1::smallint", crate::proto::PG_TYPE_INT2),
            ("SELECT $1::int4", crate::proto::PG_TYPE_INT4),
            ("SELECT $1::int", crate::proto::PG_TYPE_INT4),
            ("SELECT $1::integer", crate::proto::PG_TYPE_INT4),
            ("SELECT $1::int8", crate::proto::PG_TYPE_INT8),
            ("SELECT $1::bigint", crate::proto::PG_TYPE_INT8),
            ("SELECT $1::text", crate::proto::PG_TYPE_TEXT),
            ("SELECT $1::varchar", crate::proto::PG_TYPE_VARCHAR),
            ("SELECT $1::bool", crate::proto::PG_TYPE_BOOL),
            ("SELECT $1::boolean", crate::proto::PG_TYPE_BOOL),
            ("SELECT $1::bytea", crate::proto::PG_TYPE_BYTEA),
            ("SELECT $1::float4", crate::proto::PG_TYPE_FLOAT4),
            ("SELECT $1::real", crate::proto::PG_TYPE_FLOAT4),
            ("SELECT $1::float8", crate::proto::PG_TYPE_FLOAT8),
            ("SELECT $1::numeric", crate::proto::PG_TYPE_NUMERIC),
            ("SELECT $1::decimal", crate::proto::PG_TYPE_NUMERIC),
            ("SELECT $1::timestamptz", crate::proto::PG_TYPE_TIMESTAMPTZ),
            ("SELECT $1::INT8", crate::proto::PG_TYPE_INT8), // case-insensitive
        ];
        for (sql, expected_oid) in cases {
            let (_, casts) = strip_pg_casts_tracked(sql);
            assert_eq!(
                casts,
                vec![(0, *expected_oid)],
                "type-name OID lookup failed for {sql:?}"
            );
        }
    }
}
