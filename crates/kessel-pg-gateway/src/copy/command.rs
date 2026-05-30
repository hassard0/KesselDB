//! SP-PG-COPY — `COPY ...` SQL-text recognizer for the Simple Query
//! path.
//!
//! Recognizes the V1-supported COPY command shapes:
//!
//! - `COPY <ident> FROM STDIN [WITH (FORMAT text)]`
//! - `COPY <ident> (col1, col2) FROM STDIN [WITH (FORMAT text)]`
//! - `COPY <ident> TO STDOUT [WITH (FORMAT text)]`
//! - `COPY <ident> (col1, col2) TO STDOUT [WITH (FORMAT text)]`
//!
//! V2 follow-ups (named for the error path):
//!
//! - `FORMAT csv` / `FORMAT binary` → `RejectReason::UnsupportedFormat`
//!   so the dispatcher can emit `0A000` with a precise message
//!   pointing at the V2 arc.
//! - `FROM '/path/to/file'` / `TO '/path/to/file'` →
//!   `RejectReason::FileAccess` → `0A000` (security — V2 SP-PG-COPY-
//!   FILE; permanent gate on operator opt-in).
//! - `FROM PROGRAM '...'` / `TO PROGRAM '...'` →
//!   `RejectReason::ProgramAccess` → `0A000` (permanent hard pass).
//!
//! Returns `Some(ParsedCopy::*)` for the V1-supported shapes,
//! `Some(ParsedCopy::Rejected { reason })` for the V2-only / hard-
//! pass shapes (so the dispatcher can emit a precise error), and
//! `None` for non-COPY SQL (the existing dispatch path is unchanged).
//!
//! Lenient on leading whitespace + line/block comments + trailing
//! `;` — mirrors `recognize_discard` / `recognize_tx_control` shape
//! so ORM-prepended comments don't break recognition.

#![forbid(unsafe_code)]
#![allow(dead_code)]

/// The recognized V1-supported COPY commands + the V2-only rejection
/// kinds the recognizer surfaces so the dispatcher can emit precise
/// error messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedCopy {
    /// `COPY <table> [(cols)] FROM STDIN [WITH (FORMAT text)]`.
    From {
        table: String,
        columns: Option<Vec<String>>,
    },
    /// `COPY <table> [(cols)] TO STDOUT [WITH (FORMAT text)]`.
    To {
        table: String,
        columns: Option<Vec<String>>,
    },
    /// A COPY command V1 can't serve. The dispatcher renders the
    /// reason into the canonical `0A000` ErrorResponse message.
    Rejected { reason: RejectReason },
}

/// Why a COPY command was recognized but rejected at the V1 gateway.
/// Each variant maps to a precise error message the dispatcher
/// surfaces so operators / clients know which V2 arc would lift the
/// restriction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    /// `WITH (FORMAT binary)` — V1 doesn't ship binary format
    /// (SP-PG-COPY-BIN).
    BinaryFormat,
    /// `WITH (FORMAT csv)` — V1 doesn't ship CSV format
    /// (SP-PG-COPY-CSV).
    CsvFormat,
    /// `WITH (FORMAT <unknown>)` — neither text nor a V2-named
    /// format. Carries the offending format name for diagnostics.
    UnknownFormat { format: String },
    /// `COPY ... FROM '/path/to/file'` or `... TO '/path/to/file'`
    /// — server-side file access. Hard pass without an opt-in
    /// operator surface (SP-PG-COPY-FILE).
    FileAccess,
    /// `COPY ... FROM PROGRAM '...'` or `... TO PROGRAM '...'`
    /// — shells out. Permanent hard pass.
    ProgramAccess,
    /// `COPY ... FROM <SOURCE>` where SOURCE is neither STDIN nor a
    /// file/program form V1 understands. Defensive fallback.
    UnknownSource,
}

/// Recognize a COPY-flavor SQL statement. See `ParsedCopy` for the
/// recognized variants. Returns `None` for non-COPY SQL (existing
/// dispatch path is unchanged).
///
/// Lenient on leading whitespace + comments + trailing `;`.
pub fn parse_copy_command(sql: &str) -> Option<ParsedCopy> {
    // Strip leading whitespace + line/block comments.
    let mut s = sql;
    loop {
        let trimmed = s.trim_start();
        if let Some(rest) = trimmed.strip_prefix("--") {
            match rest.find('\n') {
                Some(p) => s = &rest[p + 1..],
                None => return None,
            }
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("/*") {
            match rest.find("*/") {
                Some(p) => s = &rest[p + 2..],
                None => return None,
            }
            continue;
        }
        s = trimmed;
        break;
    }
    // Strip trailing whitespace + at most one trailing `;`.
    let mut t = s.trim_end();
    if let Some(stripped) = t.strip_suffix(';') {
        t = stripped.trim_end();
    }
    if t.is_empty() {
        return None;
    }

    // Tokenize the first word.
    let (head, rest) = split_word(t);
    if !head.eq_ignore_ascii_case("COPY") {
        return None;
    }
    let rest = rest.trim_start();

    // The table name. Strip the `(col, col, ...)` list if present.
    let (table, rest) = parse_ident(rest)?;
    let rest = rest.trim_start();

    let (columns, rest) = if rest.starts_with('(') {
        let (cols, after) = parse_column_list(rest)?;
        (Some(cols), after.trim_start())
    } else {
        (None, rest)
    };

    // Now expect FROM or TO.
    let (verb, rest) = split_word(rest);
    let rest = rest.trim_start();
    let direction = match verb.to_ascii_uppercase().as_str() {
        "FROM" => "FROM",
        "TO" => "TO",
        _ => return None,
    };

    // Source/destination — STDIN/STDOUT vs program / file.
    let (source, rest) = split_word(rest);
    let rest = rest.trim_start();
    let source_upper = source.to_ascii_uppercase();
    match source_upper.as_str() {
        "STDIN" | "STDOUT" => {}
        "PROGRAM" => return Some(ParsedCopy::Rejected { reason: RejectReason::ProgramAccess }),
        _ if source.starts_with('\'') => {
            return Some(ParsedCopy::Rejected { reason: RejectReason::FileAccess });
        }
        _ => return Some(ParsedCopy::Rejected { reason: RejectReason::UnknownSource }),
    }

    // STDIN only valid with FROM; STDOUT only valid with TO.
    let from_stdin = direction == "FROM" && source_upper == "STDIN";
    let to_stdout = direction == "TO" && source_upper == "STDOUT";
    if !from_stdin && !to_stdout {
        return Some(ParsedCopy::Rejected { reason: RejectReason::UnknownSource });
    }

    // Optional WITH (FORMAT text|csv|binary) — V1 accepts text +
    // rejects others.
    if !rest.is_empty() {
        // Tolerant: accept `WITH (...)` clause and check FORMAT.
        if let Some(format_clause) = extract_format_clause(rest) {
            match format_clause.to_ascii_uppercase().as_str() {
                "TEXT" => {} // V1 default; explicit text is fine
                "BINARY" => {
                    return Some(ParsedCopy::Rejected {
                        reason: RejectReason::BinaryFormat,
                    });
                }
                "CSV" => {
                    return Some(ParsedCopy::Rejected {
                        reason: RejectReason::CsvFormat,
                    });
                }
                other => {
                    return Some(ParsedCopy::Rejected {
                        reason: RejectReason::UnknownFormat {
                            format: other.to_string(),
                        },
                    });
                }
            }
        }
        // Other WITH options (HEADER / DELIMITER / FREEZE / etc.) —
        // V1 silently ignores anything that isn't FORMAT. A future
        // V2 SP-PG-COPY-CSV may surface these.
    }

    if from_stdin {
        Some(ParsedCopy::From { table, columns })
    } else {
        Some(ParsedCopy::To { table, columns })
    }
}

/// Split `s` into (first word, rest). The first word is everything
/// up to the first ASCII whitespace; the rest is everything after
/// (including the whitespace). If `s` is empty, returns ("", "").
fn split_word(s: &str) -> (&str, &str) {
    match s.find(|c: char| c.is_whitespace() || c == '(' || c == ';' || c == ',') {
        Some(idx) => s.split_at(idx),
        None => (s, ""),
    }
}

/// Parse a SQL identifier — a bare word `[A-Za-z_][A-Za-z0-9_]*` or
/// a double-quoted identifier `"..."` (V1: no `""` doubled-quote
/// escape; an unsupported edge case for V1, V2 would lift). Returns
/// (ident, rest). `None` if no identifier is present.
fn parse_ident(s: &str) -> Option<(String, &str)> {
    let s = s.trim_start();
    if s.is_empty() {
        return None;
    }
    let bytes = s.as_bytes();
    if bytes[0] == b'"' {
        // Quoted identifier: scan to matching closing quote.
        let mut i = 1;
        while i < bytes.len() && bytes[i] != b'"' {
            i += 1;
        }
        if i >= bytes.len() {
            return None; // unterminated quote
        }
        let ident = std::str::from_utf8(&bytes[1..i]).ok()?.to_string();
        return Some((ident, &s[i + 1..]));
    }
    // Bare ident.
    let first = bytes[0];
    if !(first.is_ascii_alphabetic() || first == b'_') {
        return None;
    }
    let mut i = 1;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    let ident = std::str::from_utf8(&bytes[..i]).ok()?.to_string();
    Some((ident, &s[i..]))
}

/// Parse a `(col1, col2, ...)` column list. Returns the column names
/// + the rest of the string after the closing `)`.
fn parse_column_list(s: &str) -> Option<(Vec<String>, &str)> {
    let s = s.trim_start();
    if !s.starts_with('(') {
        return None;
    }
    let mut cols = Vec::new();
    let mut rest = &s[1..];
    loop {
        let r = rest.trim_start();
        if r.starts_with(')') {
            return Some((cols, &r[1..]));
        }
        let (ident, after) = parse_ident(r)?;
        cols.push(ident);
        let after = after.trim_start();
        if let Some(stripped) = after.strip_prefix(',') {
            rest = stripped;
            continue;
        }
        if after.starts_with(')') {
            return Some((cols, &after[1..]));
        }
        // Malformed — bail.
        return None;
    }
}

/// Extract the FORMAT clause from a `WITH (...)` or bare-list
/// options string. Returns the format value (e.g. `"text"`) or
/// `None` if FORMAT isn't present.
///
/// V1 accepts both:
/// - `WITH (FORMAT text, HEADER)` (modern parenthesized form)
/// - `WITH FORMAT 'text'` (legacy bare form — rarely used)
/// - `WITH (FORMAT 'text')` (quoted format name)
fn extract_format_clause(s: &str) -> Option<String> {
    let upper = s.to_ascii_uppercase();
    // Find "FORMAT" word-bounded.
    let key = "FORMAT";
    let mut search = upper.as_str();
    let mut offset = 0;
    while let Some(pos) = search.find(key) {
        let abs = offset + pos;
        // Word-boundary check on the left.
        let left_ok =
            abs == 0 || !is_ident_char(upper.as_bytes()[abs - 1]);
        // Word-boundary check on the right.
        let right_idx = abs + key.len();
        let right_ok = right_idx >= upper.len()
            || !is_ident_char(upper.as_bytes()[right_idx]);
        if left_ok && right_ok {
            // Found a real FORMAT keyword. Skip past it and any
            // whitespace, then read the next word/quoted-string.
            let after = s[right_idx..].trim_start();
            return Some(read_format_value(after));
        }
        let consumed = pos + key.len();
        offset += consumed;
        search = &search[consumed..];
    }
    None
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Read the value following the FORMAT keyword. Accepts bare ident
/// (`text`), quoted (`'text'`), or stops at the first whitespace /
/// `,` / `)` / `;`.
fn read_format_value(s: &str) -> String {
    let s = s.trim_start();
    if s.is_empty() {
        return String::new();
    }
    let bytes = s.as_bytes();
    if bytes[0] == b'\'' {
        // Quoted.
        let mut i = 1;
        while i < bytes.len() && bytes[i] != b'\'' {
            i += 1;
        }
        return std::str::from_utf8(&bytes[1..i]).unwrap_or("").to_string();
    }
    // Bare ident — scan until whitespace / punctuation.
    let mut i = 0;
    while i < bytes.len()
        && !bytes[i].is_ascii_whitespace()
        && bytes[i] != b','
        && bytes[i] != b')'
        && bytes[i] != b';'
    {
        i += 1;
    }
    std::str::from_utf8(&bytes[..i]).unwrap_or("").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── parse_copy_command — happy paths ───────────────────────────────

    /// SP-PG-COPY T1: `COPY t FROM STDIN` recognized as the basic
    /// FROM form.
    #[test]
    fn t1_parse_copy_t_from_stdin() {
        match parse_copy_command("COPY t FROM STDIN") {
            Some(ParsedCopy::From { table, columns }) => {
                assert_eq!(table, "t");
                assert_eq!(columns, None);
            }
            other => panic!("expected From, got {other:?}"),
        }
    }

    /// SP-PG-COPY T1: `COPY t TO STDOUT` recognized as the basic TO
    /// form.
    #[test]
    fn t1_parse_copy_t_to_stdout() {
        match parse_copy_command("COPY t TO STDOUT") {
            Some(ParsedCopy::To { table, columns }) => {
                assert_eq!(table, "t");
                assert_eq!(columns, None);
            }
            other => panic!("expected To, got {other:?}"),
        }
    }

    /// SP-PG-COPY T1: column list parsed correctly.
    #[test]
    fn t1_parse_copy_t_columns_from_stdin() {
        match parse_copy_command("COPY users (id, name, email) FROM STDIN") {
            Some(ParsedCopy::From { table, columns }) => {
                assert_eq!(table, "users");
                assert_eq!(
                    columns,
                    Some(vec!["id".to_string(), "name".to_string(), "email".to_string()])
                );
            }
            other => panic!("expected From with columns, got {other:?}"),
        }
    }

    /// SP-PG-COPY T1: case-insensitive on the verbs (lowercase copy /
    /// from / stdin / to / stdout all recognized).
    #[test]
    fn t1_parse_copy_case_insensitive() {
        assert!(matches!(
            parse_copy_command("copy t from stdin"),
            Some(ParsedCopy::From { .. })
        ));
        assert!(matches!(
            parse_copy_command("Copy T To StdOut"),
            Some(ParsedCopy::To { .. })
        ));
    }

    /// SP-PG-COPY T1: trailing semicolon tolerated.
    #[test]
    fn t1_parse_copy_with_trailing_semicolon() {
        assert!(matches!(
            parse_copy_command("COPY t FROM STDIN;"),
            Some(ParsedCopy::From { .. })
        ));
        assert!(matches!(
            parse_copy_command("  COPY t TO STDOUT  ;  "),
            Some(ParsedCopy::To { .. })
        ));
    }

    /// SP-PG-COPY T1: leading line + block comments tolerated.
    #[test]
    fn t1_parse_copy_with_leading_comments() {
        assert!(matches!(
            parse_copy_command("-- pg_dump line\nCOPY t FROM STDIN"),
            Some(ParsedCopy::From { .. })
        ));
        assert!(matches!(
            parse_copy_command("/* dump header */ COPY t TO STDOUT"),
            Some(ParsedCopy::To { .. })
        ));
    }

    /// SP-PG-COPY T1: `WITH (FORMAT text)` accepted explicitly (V1
    /// default).
    #[test]
    fn t1_parse_copy_explicit_text_format_accepted() {
        match parse_copy_command("COPY t FROM STDIN WITH (FORMAT text)") {
            Some(ParsedCopy::From { table, .. }) => assert_eq!(table, "t"),
            other => panic!("expected From, got {other:?}"),
        }
    }

    /// SP-PG-COPY T1: `WITH (FORMAT binary)` → `RejectReason::BinaryFormat`
    /// so the dispatcher can emit a precise V2-pointing error.
    #[test]
    fn t1_parse_copy_binary_format_rejected() {
        match parse_copy_command("COPY t FROM STDIN WITH (FORMAT binary)") {
            Some(ParsedCopy::Rejected { reason: RejectReason::BinaryFormat }) => {}
            other => panic!("expected Rejected(BinaryFormat), got {other:?}"),
        }
    }

    /// SP-PG-COPY T1: `WITH (FORMAT csv)` → `RejectReason::CsvFormat`.
    #[test]
    fn t1_parse_copy_csv_format_rejected() {
        match parse_copy_command("COPY t FROM STDIN WITH (FORMAT csv)") {
            Some(ParsedCopy::Rejected { reason: RejectReason::CsvFormat }) => {}
            other => panic!("expected Rejected(CsvFormat), got {other:?}"),
        }
    }

    /// SP-PG-COPY T1: `COPY t FROM '/path/to/file'` → file access
    /// rejected for security (V2 SP-PG-COPY-FILE).
    #[test]
    fn t1_parse_copy_file_source_rejected() {
        match parse_copy_command("COPY t FROM '/etc/passwd'") {
            Some(ParsedCopy::Rejected { reason: RejectReason::FileAccess }) => {}
            other => panic!("expected Rejected(FileAccess), got {other:?}"),
        }
    }

    /// SP-PG-COPY T1: `COPY t FROM PROGRAM 'cat /tmp/d'` → program
    /// access rejected permanently.
    #[test]
    fn t1_parse_copy_program_source_rejected() {
        match parse_copy_command("COPY t FROM PROGRAM 'cat /tmp/d'") {
            Some(ParsedCopy::Rejected { reason: RejectReason::ProgramAccess }) => {}
            other => panic!("expected Rejected(ProgramAccess), got {other:?}"),
        }
    }

    /// SP-PG-COPY T1: non-COPY SQL returns None — the dispatch
    /// fallthrough invariant.
    #[test]
    fn t1_parse_copy_returns_none_for_non_copy_sql() {
        assert_eq!(parse_copy_command("SELECT 1"), None);
        assert_eq!(parse_copy_command("INSERT INTO t VALUES (1)"), None);
        assert_eq!(parse_copy_command("BEGIN"), None);
        assert_eq!(parse_copy_command("DISCARD ALL"), None);
        assert_eq!(parse_copy_command(""), None);
        assert_eq!(parse_copy_command("   "), None);
    }

    /// SP-PG-COPY T1: `COPY` substring inside a string literal /
    /// other-keyword position is NOT recognized.
    #[test]
    fn t1_parse_copy_does_not_match_string_literal() {
        assert_eq!(parse_copy_command("SELECT 'COPY t FROM STDIN'"), None);
        assert_eq!(parse_copy_command("INSERT INTO logs VALUES ('COPY')"), None);
    }

    /// SP-PG-COPY T1: quoted identifier as the table name parses
    /// cleanly. (V1 doesn't yet support double-quote-escaping inside
    /// the identifier — `""` would round-trip to a literal `"`.)
    #[test]
    fn t1_parse_copy_quoted_table_name() {
        match parse_copy_command(r#"COPY "weird_table" FROM STDIN"#) {
            Some(ParsedCopy::From { table, .. }) => {
                assert_eq!(table, "weird_table");
            }
            other => panic!("expected From with quoted ident, got {other:?}"),
        }
    }

    /// SP-PG-COPY T1: WITH clause with FORMAT alongside other options
    /// — V1 picks up FORMAT, silently ignores the others.
    #[test]
    fn t1_parse_copy_with_format_among_other_options() {
        // CSV-shaped WITH clause — V1 should still detect CSV format
        // and reject it precisely.
        match parse_copy_command("COPY t FROM STDIN WITH (FORMAT csv, HEADER true)") {
            Some(ParsedCopy::Rejected { reason: RejectReason::CsvFormat }) => {}
            other => panic!("expected Rejected(CsvFormat), got {other:?}"),
        }
    }

    /// SP-PG-COPY T1: WITH clause that doesn't mention FORMAT is
    /// accepted (V1 default = text).
    #[test]
    fn t1_parse_copy_with_freeze_only_accepted() {
        match parse_copy_command("COPY t FROM STDIN WITH (FREEZE)") {
            Some(ParsedCopy::From { .. }) => {} // V1 silently ignores FREEZE
            other => panic!("expected From, got {other:?}"),
        }
    }

    /// SP-PG-COPY T1: `FROM` without `STDIN` (e.g. an unsupported
    /// shape `FROM t2`) → UnknownSource rejection.
    #[test]
    fn t1_parse_copy_unknown_from_target_rejected() {
        match parse_copy_command("COPY t FROM otherthing") {
            Some(ParsedCopy::Rejected { reason: RejectReason::UnknownSource }) => {}
            other => panic!("expected Rejected(UnknownSource), got {other:?}"),
        }
    }

    /// SP-PG-COPY T1: TO without STDOUT (e.g. swapped FROM STDOUT,
    /// not a valid PG shape) → rejected.
    #[test]
    fn t1_parse_copy_to_stdin_is_invalid() {
        // TO STDIN is not a valid combination per PG. We reject as
        // UnknownSource.
        match parse_copy_command("COPY t TO STDIN") {
            Some(ParsedCopy::Rejected { reason: RejectReason::UnknownSource }) => {}
            other => panic!("expected Rejected(UnknownSource), got {other:?}"),
        }
    }
}
