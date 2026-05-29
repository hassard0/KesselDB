//! Parameter substitution — text-format `$N` substitution at Execute
//! time per SP-PG-EXTQ design spec §4.
//!
//! **T5 status (this commit):** the textual `$N` → bound-value
//! substitution helper that the Execute dispatcher (also T5) calls
//! before handing the rewritten SQL string off to the existing
//! `dispatch::dispatch_query` Simple Query pipeline. The helper is
//! pure / engine-free / stateless — it takes the prepared SQL and
//! the portal's `param_values: Vec<Option<Vec<u8>>>` and returns a
//! `String` with every `$N` placeholder replaced.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-28-kesseldb-sppgextq-extended-query-design.md`
//! §4 + §11 weak-spot #1.
//!
//! ## Substitution rules (V1)
//!
//! | Bound value | Rendered SQL |
//! |---|---|
//! | `None` (PG NULL, wire length=-1) | bare `NULL` keyword |
//! | `Some([])` (empty bytes) | `''` (empty single-quoted string) |
//! | `Some(b"hello")` | `'hello'` |
//! | `Some(b"O'Brien")` | `'O''Brien'` (single-quote doubling) |
//! | `Some(b"42")` | `'42'` (text format is universally string-shaped) |
//! | `Some(b"-3.14")` | `'-3.14'` |
//!
//! Why quote everything? The libpq protocol's text format is already
//! string-shaped at the wire — a `SELECT $1::int` carries `"42"` as
//! ASCII bytes. The KesselDB SQL parser accepts `'42'` as either a
//! quoted literal or an implicit-cast integer, so wrapping every
//! text-format param in single quotes is correctness-preserving for
//! every PG type without the substitution layer needing to know the
//! column's OID. The optimisation (emit unquoted when `param_oids[i]`
//! says INT/BOOL) is a future polish — V1 ships the simplest correct
//! shape.
//!
//! ## Edge cases V1 handles
//!
//! - **`$10`, `$20`** (two-digit indices) — the scanner is greedy
//!   over the decimal digits, so `$10` resolves to the 10th param
//!   even if the SQL also contains `$1` literally.
//! - **Same `$N` used multiple times** — `WHERE x = $1 OR y = $1`
//!   with $1=42 → `WHERE x = '42' OR y = '42'`. The substitution
//!   walks the SQL left-to-right replacing every occurrence.
//! - **`$N` inside a single-quoted string literal** — NOT substituted.
//!   `'hello $1'` stays `'hello $1'` verbatim. PG itself follows the
//!   same rule.
//! - **`$N` inside a double-quoted identifier** — NOT substituted.
//!   `"col$1"` stays `"col$1"`.
//! - **`$N` inside `-- line comment`** — NOT substituted; the comment
//!   is left verbatim (the engine SQL parser ignores it).
//! - **`$N` inside `/* block comment */`** — NOT substituted; left
//!   verbatim.
//! - **`$0`** or other 0-index — V1 returns an error
//!   (`SubstituteError::ZeroParamIndex`) because PG `$N` indices are
//!   1-based.
//! - **`$N` referencing a position the portal didn't bind** — V1 returns
//!   `SubstituteError::ParamIndexOutOfBounds`.
//!
//! ## What V1 does NOT do (documented as spec §11 weak-spot #1)
//!
//! - V1 has no AST — `$N` is found by textual scanning. A future SQL
//!   extension that introduced a token using `$N` for something other
//!   than a parameter (e.g. a dollar-quoted string `$tag$body$tag$`)
//!   would need a substitution-skip rule added here. V1 detects
//!   `$tag$` dollar-quoting (the leading `$` followed by a non-digit
//!   that ends with another `$`) and skips it. PG `$$body$$` (empty
//!   tag) is the common case.
//! - V1 does no type validation — a `$1` for an `INT8` parameter
//!   bound as text bytes `"not an int"` is rendered as `'not an int'`
//!   and the engine SQL parser produces the type-mismatch error at
//!   Execute. Matches PG itself; spec §11 weak-spot #10.

#![forbid(unsafe_code)]
#![allow(dead_code)]

/// Errors the substitution can return. All map to SQLSTATE `08P01
/// protocol_violation` at the dispatcher boundary — they indicate
/// a client bug (mismatch between Parse SQL + Bind value count).
#[derive(Debug, PartialEq, Eq)]
pub enum SubstituteError {
    /// The SQL referenced `$0` — PG `$N` indices are 1-based.
    ZeroParamIndex,
    /// The SQL referenced `$N` where N > the portal's bound parameter
    /// count. Carries `index` (the requested 1-based index) and
    /// `available` (the portal's `param_values.len()`).
    ParamIndexOutOfBounds { index: usize, available: usize },
}

/// Substitute `$N` placeholders in `sql` with the corresponding
/// bound parameter values from `params` (0-indexed: `$1` → `params[0]`).
///
/// **`params` semantics:** each entry is `Option<&[u8]>` — `None`
/// means SQL NULL (the wire `length=-1` sentinel); `Some(bytes)` is
/// the raw text-format bytes the client sent at Bind. Substitution
/// rules per the module-level docs.
///
/// **Lexer state.** The scanner tracks four lexical regions where
/// `$N` is NOT substituted:
/// - inside `'single-quoted string'` (PG single-quote-doubling escape)
/// - inside `"double-quoted identifier"` (PG `""` escape)
/// - inside `-- line comment` to next `\n`
/// - inside `/* block comment */` (non-nesting; matches PG default)
/// - inside `$tag$body$tag$` PG dollar-quoted string literal
///   (detected as `$<letters/_><letters/_/digits>*$`)
///
/// Returns the rewritten SQL string. The function ALLOCATES one
/// String (the output) and pushes byte-by-byte; no regex, no extra
/// allocations beyond the unavoidable.
pub fn substitute_text_format_params(
    sql: &str,
    params: &[Option<&[u8]>],
) -> Result<String, SubstituteError> {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 16);
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        // ── Skip single-quoted strings ─────────────────────────────
        if b == b'\'' {
            out.push('\'');
            i += 1;
            while i < bytes.len() {
                let c = bytes[i];
                if c == b'\'' {
                    // Doubled '' = escaped quote (still in string).
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        out.push('\'');
                        out.push('\'');
                        i += 2;
                        continue;
                    }
                    // End of string.
                    out.push('\'');
                    i += 1;
                    break;
                }
                out.push(c as char);
                i += 1;
            }
            continue;
        }
        // ── Skip double-quoted identifiers ─────────────────────────
        if b == b'"' {
            out.push('"');
            i += 1;
            while i < bytes.len() {
                let c = bytes[i];
                if c == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        out.push('"');
                        out.push('"');
                        i += 2;
                        continue;
                    }
                    out.push('"');
                    i += 1;
                    break;
                }
                out.push(c as char);
                i += 1;
            }
            continue;
        }
        // ── Skip line comments ─────────────────────────────────────
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            // Find the newline (or end of buffer).
            let end = bytes[i..].iter().position(|&x| x == b'\n').map(|p| i + p);
            match end {
                Some(p) => {
                    out.push_str(std::str::from_utf8(&bytes[i..=p]).unwrap_or(""));
                    i = p + 1;
                }
                None => {
                    out.push_str(std::str::from_utf8(&bytes[i..]).unwrap_or(""));
                    i = bytes.len();
                }
            }
            continue;
        }
        // ── Skip block comments ────────────────────────────────────
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            let rest = &bytes[i + 2..];
            match rest.windows(2).position(|w| w == b"*/") {
                Some(p) => {
                    let end = i + 2 + p + 2;
                    out.push_str(std::str::from_utf8(&bytes[i..end]).unwrap_or(""));
                    i = end;
                }
                None => {
                    // Unterminated block comment — emit verbatim and
                    // stop.
                    out.push_str(std::str::from_utf8(&bytes[i..]).unwrap_or(""));
                    i = bytes.len();
                }
            }
            continue;
        }
        // ── `$` handling: parameter placeholder OR dollar-quoted string ──
        if b == b'$' {
            // PG dollar-quoted: `$tag$...$tag$` where tag is
            // [A-Za-z_][A-Za-z_0-9]*. The body can contain ANY chars
            // including quotes. The terminator is the same `$tag$`
            // sequence. Detect by looking at the byte AFTER the `$`:
            // if it's a letter/underscore, this is a dollar-quoted
            // string, not a `$N` placeholder.
            if i + 1 < bytes.len() && is_tag_start_byte(bytes[i + 1]) {
                // Read the tag.
                let tag_start = i + 1;
                let mut tag_end = tag_start;
                while tag_end < bytes.len() && is_tag_cont_byte(bytes[tag_end]) {
                    tag_end += 1;
                }
                if tag_end < bytes.len() && bytes[tag_end] == b'$' {
                    // Confirmed dollar-quoted string with non-empty tag.
                    let opener_end = tag_end + 1;
                    let tag = &bytes[tag_start..tag_end];
                    let mut term_idx = None;
                    // Find the matching `$tag$` terminator.
                    let mut j = opener_end;
                    while j < bytes.len() {
                        if bytes[j] == b'$' {
                            let after = j + 1;
                            if after + tag.len() <= bytes.len()
                                && &bytes[after..after + tag.len()] == tag
                                && after + tag.len() < bytes.len()
                                && bytes[after + tag.len()] == b'$'
                            {
                                term_idx = Some(after + tag.len() + 1);
                                break;
                            }
                        }
                        j += 1;
                    }
                    match term_idx {
                        Some(end) => {
                            out.push_str(std::str::from_utf8(&bytes[i..end]).unwrap_or(""));
                            i = end;
                            continue;
                        }
                        None => {
                            // Unterminated — emit verbatim and stop.
                            out.push_str(std::str::from_utf8(&bytes[i..]).unwrap_or(""));
                            i = bytes.len();
                            continue;
                        }
                    }
                }
            }
            // Empty-tag dollar-quoted string `$$body$$`.
            if i + 1 < bytes.len() && bytes[i + 1] == b'$' {
                // Find the matching `$$`.
                let opener_end = i + 2;
                let mut term_idx = None;
                let mut j = opener_end;
                while j + 1 < bytes.len() {
                    if bytes[j] == b'$' && bytes[j + 1] == b'$' {
                        term_idx = Some(j + 2);
                        break;
                    }
                    j += 1;
                }
                match term_idx {
                    Some(end) => {
                        out.push_str(std::str::from_utf8(&bytes[i..end]).unwrap_or(""));
                        i = end;
                        continue;
                    }
                    None => {
                        // Unterminated — emit verbatim and stop.
                        out.push_str(std::str::from_utf8(&bytes[i..]).unwrap_or(""));
                        i = bytes.len();
                        continue;
                    }
                }
            }
            // Otherwise: try `$N` placeholder. Greedy decimal-digit
            // scan starting after the `$`.
            let mut digit_end = i + 1;
            while digit_end < bytes.len() && bytes[digit_end].is_ascii_digit() {
                digit_end += 1;
            }
            if digit_end > i + 1 {
                let digits = std::str::from_utf8(&bytes[i + 1..digit_end])
                    .expect("ascii digits are valid utf8");
                let n: usize = digits.parse().expect("ascii digits parse to usize");
                if n == 0 {
                    return Err(SubstituteError::ZeroParamIndex);
                }
                if n > params.len() {
                    return Err(SubstituteError::ParamIndexOutOfBounds {
                        index: n,
                        available: params.len(),
                    });
                }
                render_param(&mut out, params[n - 1]);
                i = digit_end;
                continue;
            }
            // `$` with no following digit and no dollar-quote tag —
            // emit verbatim.
            out.push('$');
            i += 1;
            continue;
        }
        // Default: emit byte verbatim (UTF-8 is byte-stable for
        // our purposes — multi-byte UTF-8 bytes are all >= 0x80
        // and don't collide with any of our lexer-triggering
        // ASCII bytes).
        out.push(b as char);
        i += 1;
    }
    Ok(out)
}

/// Render one parameter value into the output string.
///
/// - `None` (PG NULL) → bare `NULL` keyword (NOT quoted).
/// - `Some(bytes)` → `'<bytes-with-single-quotes-doubled>'`.
///
/// The single-quote escaping is PG §4.1.2.1 String Constants — "to
/// include a single-quote character within a string constant, write
/// two adjacent single quotes". Locked by KATs.
fn render_param(out: &mut String, value: Option<&[u8]>) {
    match value {
        None => out.push_str("NULL"),
        Some(bytes) => {
            out.push('\'');
            for &b in bytes {
                if b == b'\'' {
                    out.push('\'');
                    out.push('\'');
                } else {
                    // Lossy UTF-8 fallback: any byte that isn't valid
                    // utf-8 still ends up as a single char via
                    // `char::from(b)` (which is a 1:1 byte→char
                    // mapping for 0x00..=0xFF and corrupts non-ASCII
                    // UTF-8). For V1 text-format params, the client
                    // sends valid UTF-8 text, so this branch is the
                    // happy path. A pathological client sending raw
                    // bytes gets garbage in the SQL but no crash.
                    out.push(b as char);
                }
            }
            out.push('\'');
        }
    }
}

fn is_tag_start_byte(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_tag_cont_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    // ───────────────────────────────────────────────────────────────────
    // T5 KATs — spec §4 substitution rules + edge cases.
    // ───────────────────────────────────────────────────────────────────

    /// Spec §4: `$1` with a text-format bound value → single-quoted
    /// literal.
    #[test]
    fn t5_substitute_dollar_one_with_text_value() {
        let sql = "SELECT $1";
        let params: Vec<Option<&[u8]>> = vec![Some(b"42")];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "SELECT '42'");
    }

    /// Spec §4: `$1` with `None` (NULL) → bare `NULL` keyword.
    #[test]
    fn t5_substitute_dollar_one_with_null_renders_bare_keyword() {
        let sql = "SELECT $1";
        let params: Vec<Option<&[u8]>> = vec![None];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "SELECT NULL");
    }

    /// Spec §4: single-quote in the value is doubled per PG §4.1.2.1.
    #[test]
    fn t5_substitute_value_containing_single_quote_doubles_it() {
        let sql = "INSERT INTO t (name) VALUES ($1)";
        let params: Vec<Option<&[u8]>> = vec![Some(b"O'Brien")];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "INSERT INTO t (name) VALUES ('O''Brien')");
    }

    /// Spec §4: a numeric text-format value is single-quoted just
    /// like any other text value — the SQL parser does the implicit
    /// cast.
    #[test]
    fn t5_substitute_numeric_value_is_still_quoted() {
        let sql = "SELECT $1::int";
        let params: Vec<Option<&[u8]>> = vec![Some(b"42")];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "SELECT '42'::int");
    }

    /// Edge: `$10` (two-digit index) is parsed as index=10, not as
    /// `$1` followed by literal `0`. Locks against the ambiguity.
    #[test]
    fn t5_substitute_two_digit_index_is_parsed_greedily() {
        // Build 10 params; the 10th is `"ten"`.
        let strings: Vec<Vec<u8>> =
            (1..=10).map(|i| format!("val{i}").into_bytes()).collect();
        let params: Vec<Option<&[u8]>> = strings.iter().map(|v| Some(v.as_slice())).collect();
        let sql = "SELECT $10";
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "SELECT 'val10'");
    }

    /// Edge: `$20` two-digit index.
    #[test]
    fn t5_substitute_two_digit_index_20() {
        let strings: Vec<Vec<u8>> =
            (1..=20).map(|i| format!("v{i}").into_bytes()).collect();
        let params: Vec<Option<&[u8]>> = strings.iter().map(|v| Some(v.as_slice())).collect();
        let sql = "SELECT $20, $1";
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "SELECT 'v20', 'v1'");
    }

    /// Edge: same `$N` referenced multiple times → all occurrences
    /// substituted with the same value.
    #[test]
    fn t5_substitute_same_param_used_multiple_times() {
        let sql = "WHERE x = $1 OR y = $1";
        let params: Vec<Option<&[u8]>> = vec![Some(b"42")];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "WHERE x = '42' OR y = '42'");
    }

    /// Edge: `$1` inside a single-quoted string is NOT substituted.
    #[test]
    fn t5_substitute_dollar_in_single_quoted_string_is_literal() {
        let sql = "SELECT 'hello $1 world', $1";
        let params: Vec<Option<&[u8]>> = vec![Some(b"X")];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "SELECT 'hello $1 world', 'X'");
    }

    /// Edge: `$1` inside a double-quoted identifier is NOT substituted.
    #[test]
    fn t5_substitute_dollar_in_double_quoted_identifier_is_literal() {
        let sql = "SELECT \"col$1\" FROM t WHERE x = $1";
        let params: Vec<Option<&[u8]>> = vec![Some(b"X")];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "SELECT \"col$1\" FROM t WHERE x = 'X'");
    }

    /// Edge: `$1` inside a `--` line comment is NOT substituted.
    #[test]
    fn t5_substitute_dollar_in_line_comment_is_literal() {
        let sql = "-- comment $1 here\nSELECT $1";
        let params: Vec<Option<&[u8]>> = vec![Some(b"X")];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "-- comment $1 here\nSELECT 'X'");
    }

    /// Edge: `$1` inside a `/* */` block comment is NOT substituted.
    #[test]
    fn t5_substitute_dollar_in_block_comment_is_literal() {
        let sql = "/* leading $1 */ SELECT $1";
        let params: Vec<Option<&[u8]>> = vec![Some(b"X")];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "/* leading $1 */ SELECT 'X'");
    }

    /// Edge: PG dollar-quoted string `$$body$$` is NOT substituted.
    #[test]
    fn t5_substitute_dollar_quoted_empty_tag_is_literal() {
        let sql = "SELECT $$hello $1 world$$, $1";
        let params: Vec<Option<&[u8]>> = vec![Some(b"X")];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "SELECT $$hello $1 world$$, 'X'");
    }

    /// Edge: PG dollar-quoted with a tag `$body$...$body$` is NOT
    /// substituted.
    #[test]
    fn t5_substitute_dollar_quoted_named_tag_is_literal() {
        let sql = "SELECT $tag$hello $1 world$tag$, $1";
        let params: Vec<Option<&[u8]>> = vec![Some(b"X")];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "SELECT $tag$hello $1 world$tag$, 'X'");
    }

    /// Empty-bytes value → `''` empty SQL string literal.
    #[test]
    fn t5_substitute_empty_value_renders_as_empty_string_literal() {
        let sql = "SELECT $1";
        let params: Vec<Option<&[u8]>> = vec![Some(b"")];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "SELECT ''");
    }

    /// `$0` rejected — PG `$N` indices are 1-based.
    #[test]
    fn t5_substitute_zero_index_rejected() {
        let sql = "SELECT $0";
        let params: Vec<Option<&[u8]>> = vec![];
        let err = substitute_text_format_params(sql, &params).unwrap_err();
        assert_eq!(err, SubstituteError::ZeroParamIndex);
    }

    /// `$N` exceeding bound count rejected.
    #[test]
    fn t5_substitute_out_of_bounds_index_rejected() {
        let sql = "SELECT $3";
        let params: Vec<Option<&[u8]>> = vec![Some(b"a"), Some(b"b")];
        let err = substitute_text_format_params(sql, &params).unwrap_err();
        assert_eq!(
            err,
            SubstituteError::ParamIndexOutOfBounds {
                index: 3,
                available: 2
            }
        );
    }

    /// Bare `$` (with no digit following and no dollar-quote tag) is
    /// emitted verbatim — defensive against pathological SQL.
    #[test]
    fn t5_substitute_bare_dollar_with_no_digit_is_literal() {
        let sql = "SELECT $";
        let params: Vec<Option<&[u8]>> = vec![];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "SELECT $");
    }

    /// SQL with NO `$N` placeholders is returned unchanged.
    #[test]
    fn t5_substitute_no_placeholders_returns_sql_verbatim() {
        let sql = "SELECT 1 + 2";
        let params: Vec<Option<&[u8]>> = vec![];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "SELECT 1 + 2");
    }

    /// Mixed: NULL `$1` + text `$2` + numeric-text `$3` in one query.
    #[test]
    fn t5_substitute_mixed_null_text_numeric() {
        let sql = "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)";
        let params: Vec<Option<&[u8]>> =
            vec![None, Some(b"hello"), Some(b"42")];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(
            out,
            "INSERT INTO t (a, b, c) VALUES (NULL, 'hello', '42')"
        );
    }

    /// Doubled-quote escaping inside a single-quoted literal does
    /// NOT confuse the scanner: `'O''Brien'` is one literal, and any
    /// `$N` after it gets substituted normally.
    #[test]
    fn t5_substitute_doubled_quote_in_existing_literal_does_not_confuse_scanner() {
        let sql = "SELECT 'O''Brien', $1";
        let params: Vec<Option<&[u8]>> = vec![Some(b"X")];
        let out = substitute_text_format_params(sql, &params).expect("ok");
        assert_eq!(out, "SELECT 'O''Brien', 'X'");
    }
}
