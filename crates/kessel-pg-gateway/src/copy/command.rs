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

use crate::copy::csv::{self, CsvOptions};
use crate::copy::CopyFormat;

/// The recognized V1-supported COPY commands + the V2-only rejection
/// kinds the recognizer surfaces so the dispatcher can emit precise
/// error messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedCopy {
    /// `COPY <table> [(cols)] FROM STDIN [WITH (FORMAT text|csv, ...)]`.
    ///
    /// **SP-PG-COPY-CSV V1** — `format` carries the wire format. Text
    /// is the SP-PG-COPY V1 default; Csv engages the CSV codec with
    /// the resolved options (DELIMITER / QUOTE / ESCAPE / NULL /
    /// HEADER).
    From {
        table: String,
        columns: Option<Vec<String>>,
        format: CopyFormat,
    },
    /// `COPY <table> [(cols)] TO STDOUT [WITH (FORMAT text|csv, ...)]`.
    To {
        table: String,
        columns: Option<Vec<String>>,
        format: CopyFormat,
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
    /// `WITH (FORMAT csv)` — V1 SP-PG-COPY shipped text only; the CSV
    /// codec lands in SP-PG-COPY-CSV (this arc). Reserved for any
    /// future restriction where CSV is unavailable; SP-PG-COPY-CSV V1
    /// accepts CSV directly via `ParsedCopy::From/To { format:
    /// CopyFormat::Csv(...) }`.
    CsvFormat,
    /// `WITH (FORMAT <unknown>)` — neither text nor a V2-named
    /// format. Carries the offending format name for diagnostics.
    UnknownFormat { format: String },
    /// **SP-PG-COPY-CSV V1** — `WITH (FORMAT csv, FORCE_QUOTE ...)` /
    /// `FORCE_NOT_NULL ...` / `FORCE_NULL ...`. These are column-
    /// scoped modifiers V1 doesn't yet implement; V2
    /// `SP-PG-COPY-CSV-FORCEQUOTE` lifts.
    UnsupportedCsvOption { option: String },
    /// **SP-PG-COPY-CSV V1** — `WITH (FORMAT csv, DELIMITER '||')`
    /// or any single-byte option with a multi-byte value. Carries the
    /// option name for diagnostics.
    InvalidCsvOptionValue { option: String, value: String },
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

    // Optional WITH (FORMAT text|csv|binary, ...) — SP-PG-COPY V1
    // accepts text; SP-PG-COPY-CSV V1 lifts CSV; binary stays V2.
    let format = if !rest.is_empty() {
        match parse_with_options(rest) {
            Ok(f) => f,
            Err(reason) => return Some(ParsedCopy::Rejected { reason }),
        }
    } else {
        CopyFormat::Text
    };

    if from_stdin {
        Some(ParsedCopy::From {
            table,
            columns,
            format,
        })
    } else {
        Some(ParsedCopy::To {
            table,
            columns,
            format,
        })
    }
}

/// **SP-PG-COPY-CSV V1** — parse the `WITH (...)` option clause into a
/// `CopyFormat`. Accepts:
///
/// - `WITH (FORMAT text)` → `CopyFormat::Text`
/// - `WITH (FORMAT csv [, DELIMITER 'X'] [, QUOTE 'X'] [, ESCAPE 'X']
///   [, NULL 'string'] [, HEADER [true|false]])` → `CopyFormat::Csv(...)`
/// - `WITH (FORMAT binary)` → `Err(BinaryFormat)`
/// - `WITH (FORMAT <unknown>)` → `Err(UnknownFormat)`
/// - No FORMAT key OR no WITH clause → `CopyFormat::Text` (V1 default)
///
/// Other recognized CSV options:
/// - `FORCE_QUOTE ...` → `Err(UnsupportedCsvOption)` (V2)
/// - `FORCE_NOT_NULL ...` → `Err(UnsupportedCsvOption)` (V2)
/// - `FORCE_NULL ...` → `Err(UnsupportedCsvOption)` (V2)
/// - `ENCODING 'utf16'` → `Err(UnsupportedCsvOption)` (V2)
/// - `FREEZE` → silently accepted (no-op, KesselDB has no
///   visibility map)
///
/// Unknown options outside CSV: silently dropped (matches V1
/// SP-PG-COPY tolerant stance).
pub(crate) fn parse_with_options(s: &str) -> Result<CopyFormat, RejectReason> {
    let opts = tokenize_options(s);
    // First pass: find FORMAT to decide the codec.
    let mut format_name: Option<String> = None;
    for (k, v) in &opts {
        if k.eq_ignore_ascii_case("FORMAT") {
            format_name = Some(v.clone().unwrap_or_default());
        }
    }
    let fmt = format_name.unwrap_or_else(|| "text".to_string());
    match fmt.to_ascii_uppercase().as_str() {
        "TEXT" => Ok(CopyFormat::Text),
        "BINARY" => Err(RejectReason::BinaryFormat),
        "CSV" => {
            let mut csv_opts = CsvOptions::default();
            for (k, v) in &opts {
                let key = k.to_ascii_uppercase();
                match key.as_str() {
                    "FORMAT" => {}
                    "FREEZE" => {} // silent no-op
                    "DELIMITER" => {
                        let value = v.clone().unwrap_or_default();
                        csv_opts.delimiter = csv::validate_single_byte("DELIMITER", &value)
                            .map_err(|_| RejectReason::InvalidCsvOptionValue {
                                option: "DELIMITER".to_string(),
                                value,
                            })?;
                    }
                    "QUOTE" => {
                        let value = v.clone().unwrap_or_default();
                        csv_opts.quote = csv::validate_single_byte("QUOTE", &value).map_err(
                            |_| RejectReason::InvalidCsvOptionValue {
                                option: "QUOTE".to_string(),
                                value,
                            },
                        )?;
                        // Default escape tracks quote unless explicitly set.
                        csv_opts.escape = csv_opts.quote;
                    }
                    "ESCAPE" => {
                        let value = v.clone().unwrap_or_default();
                        csv_opts.escape = csv::validate_single_byte("ESCAPE", &value).map_err(
                            |_| RejectReason::InvalidCsvOptionValue {
                                option: "ESCAPE".to_string(),
                                value,
                            },
                        )?;
                    }
                    "NULL" => {
                        csv_opts.null_marker = v.clone().unwrap_or_default();
                    }
                    "HEADER" => {
                        // PG: HEADER [boolean]. Bare HEADER = true.
                        // Accept: HEADER, HEADER true, HEADER false,
                        // HEADER on, HEADER off.
                        csv_opts.header = match v.as_deref() {
                            None => true,
                            Some(val) => {
                                let u = val.to_ascii_uppercase();
                                matches!(u.as_str(), "TRUE" | "ON" | "1" | "YES" | "MATCH")
                            }
                        };
                    }
                    "FORCE_QUOTE" | "FORCE_NOT_NULL" | "FORCE_NULL" => {
                        return Err(RejectReason::UnsupportedCsvOption {
                            option: key,
                        });
                    }
                    "ENCODING" => {
                        let value = v.clone().unwrap_or_default();
                        // V1 accepts only UTF-8 / unicode aliases.
                        let u = value.to_ascii_uppercase();
                        if !matches!(u.as_str(), "UTF8" | "UTF-8" | "UNICODE" | "") {
                            return Err(RejectReason::UnsupportedCsvOption {
                                option: format!("ENCODING '{value}'"),
                            });
                        }
                    }
                    _ => {
                        // Unknown option — silently ignored (matches
                        // V1 SP-PG-COPY tolerant stance).
                    }
                }
            }
            Ok(CopyFormat::Csv(csv_opts))
        }
        other => Err(RejectReason::UnknownFormat {
            format: other.to_string(),
        }),
    }
}

/// Tokenize the `WITH (...)` body (or bare option list) into
/// `(key, optional_value)` pairs. Lenient — accepts:
///
/// - `WITH (FORMAT csv, HEADER, DELIMITER '|')`
/// - `WITH FORMAT csv` (legacy bare form)
/// - `FORMAT csv HEADER true` (whitespace-separated)
/// - `CSV HEADER` (psql's classic `\copy` form — V1 doesn't recognize
///   the bare `CSV` keyword as FORMAT csv since the dispatcher already
///   sees the explicit `WITH (FORMAT ...)` shape from psql `\copy` →
///   server-side `COPY` translation; left for V2 if a client needs it).
fn tokenize_options(s: &str) -> Vec<(String, Option<String>)> {
    // Strip a leading WITH (case-insensitive).
    let mut t = s.trim_start();
    if t.len() >= 4 && t[..4].eq_ignore_ascii_case("WITH") {
        let after = &t[4..];
        if after
            .chars()
            .next()
            .map(|c| c.is_whitespace() || c == '(')
            .unwrap_or(false)
        {
            t = after.trim_start();
        }
    }
    // If wrapped in parens, strip them.
    let body = if let Some(inside) = t.strip_prefix('(') {
        match inside.rfind(')') {
            Some(end) => &inside[..end],
            None => inside,
        }
    } else {
        t
    };

    // Split on commas at top level (no nested parens in CSV options
    // V1 — FORCE_QUOTE (col1, col2) shape rejected before we get here,
    // but be defensive against nested parens by tracking depth).
    let mut pairs: Vec<(String, Option<String>)> = Vec::new();
    let mut buf = String::new();
    let mut depth = 0i32;
    let mut in_quote = false;
    for c in body.chars() {
        if in_quote {
            buf.push(c);
            if c == '\'' {
                in_quote = false;
            }
            continue;
        }
        match c {
            '\'' => {
                in_quote = true;
                buf.push(c);
            }
            '(' => {
                depth += 1;
                buf.push(c);
            }
            ')' => {
                depth -= 1;
                buf.push(c);
            }
            ',' if depth == 0 => {
                if !buf.trim().is_empty() {
                    pairs.push(split_key_value(buf.trim()));
                }
                buf.clear();
            }
            _ => buf.push(c),
        }
    }
    if !buf.trim().is_empty() {
        pairs.push(split_key_value(buf.trim()));
    }
    pairs
}

/// Split a single option's text into `(key, optional_value)`. The key
/// is the first word; the value is everything after (with surrounding
/// quotes stripped if present).
fn split_key_value(s: &str) -> (String, Option<String>) {
    let s = s.trim();
    // Find the first whitespace OR start of `'`.
    let mut split_at = s.len();
    for (i, c) in s.char_indices() {
        if c.is_whitespace() {
            split_at = i;
            break;
        }
    }
    let key = s[..split_at].to_string();
    let rest = s[split_at..].trim();
    if rest.is_empty() {
        return (key, None);
    }
    // Strip surrounding single quotes if present.
    let value = if let Some(stripped) = rest.strip_prefix('\'') {
        if let Some(end) = stripped.find('\'') {
            stripped[..end].to_string()
        } else {
            stripped.to_string()
        }
    } else {
        // Bare token — value is up to next whitespace / comma / ).
        let mut end = rest.len();
        for (i, c) in rest.char_indices() {
            if c.is_whitespace() || c == ',' || c == ')' {
                end = i;
                break;
            }
        }
        rest[..end].to_string()
    };
    (key, Some(value))
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
            Some(ParsedCopy::From { table, columns, format }) => {
                assert_eq!(table, "t");
                assert_eq!(columns, None);
                assert_eq!(format, CopyFormat::Text);
            }
            other => panic!("expected From, got {other:?}"),
        }
    }

    /// SP-PG-COPY T1: `COPY t TO STDOUT` recognized as the basic TO
    /// form.
    #[test]
    fn t1_parse_copy_t_to_stdout() {
        match parse_copy_command("COPY t TO STDOUT") {
            Some(ParsedCopy::To { table, columns, format }) => {
                assert_eq!(table, "t");
                assert_eq!(columns, None);
                assert_eq!(format, CopyFormat::Text);
            }
            other => panic!("expected To, got {other:?}"),
        }
    }

    /// SP-PG-COPY T1: column list parsed correctly.
    #[test]
    fn t1_parse_copy_t_columns_from_stdin() {
        match parse_copy_command("COPY users (id, name, email) FROM STDIN") {
            Some(ParsedCopy::From { table, columns, .. }) => {
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
            Some(ParsedCopy::From { table, format, .. }) => {
                assert_eq!(table, "t");
                assert_eq!(format, CopyFormat::Text);
            }
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

    /// SP-PG-COPY-CSV T1: `WITH (FORMAT csv)` → `ParsedCopy::From {
    /// format: Csv(default options) }` (no longer rejected — V1 lifts).
    #[test]
    fn csv_t1_parse_copy_csv_format_accepted_with_defaults() {
        match parse_copy_command("COPY t FROM STDIN WITH (FORMAT csv)") {
            Some(ParsedCopy::From { format: CopyFormat::Csv(opts), .. }) => {
                assert_eq!(opts.delimiter, b',');
                assert_eq!(opts.quote, b'"');
                assert_eq!(opts.escape, b'"');
                assert!(opts.null_marker.is_empty());
                assert!(!opts.header);
            }
            other => panic!("expected From with Csv format, got {other:?}"),
        }
    }

    /// SP-PG-COPY-CSV T1: HEADER flag honored.
    #[test]
    fn csv_t1_parse_csv_header_flag() {
        match parse_copy_command("COPY t FROM STDIN WITH (FORMAT csv, HEADER)") {
            Some(ParsedCopy::From { format: CopyFormat::Csv(opts), .. }) => {
                assert!(opts.header);
            }
            other => panic!("expected From with Csv+HEADER, got {other:?}"),
        }
    }

    /// SP-PG-COPY-CSV T1: custom DELIMITER, QUOTE, NULL.
    #[test]
    fn csv_t1_parse_csv_custom_options() {
        match parse_copy_command(
            "COPY t TO STDOUT WITH (FORMAT csv, DELIMITER ';', QUOTE '\"', NULL 'NULL', HEADER true)",
        ) {
            Some(ParsedCopy::To { format: CopyFormat::Csv(opts), .. }) => {
                assert_eq!(opts.delimiter, b';');
                assert_eq!(opts.quote, b'"');
                assert_eq!(opts.null_marker, "NULL");
                assert!(opts.header);
            }
            other => panic!("expected To with Csv+custom, got {other:?}"),
        }
    }

    /// SP-PG-COPY-CSV T1: invalid DELIMITER (multi-byte) rejected.
    #[test]
    fn csv_t1_parse_invalid_delimiter_rejected() {
        match parse_copy_command("COPY t FROM STDIN WITH (FORMAT csv, DELIMITER '||')") {
            Some(ParsedCopy::Rejected {
                reason: RejectReason::InvalidCsvOptionValue { option, .. },
            }) => {
                assert_eq!(option, "DELIMITER");
            }
            other => panic!("expected Rejected(InvalidCsvOptionValue), got {other:?}"),
        }
    }

    /// SP-PG-COPY-CSV T1: FORCE_QUOTE option rejected (V2).
    #[test]
    fn csv_t1_parse_force_quote_rejected_v2() {
        match parse_copy_command("COPY t TO STDOUT WITH (FORMAT csv, FORCE_QUOTE (id, name))") {
            Some(ParsedCopy::Rejected {
                reason: RejectReason::UnsupportedCsvOption { option },
            }) => {
                assert_eq!(option, "FORCE_QUOTE");
            }
            other => panic!("expected Rejected(UnsupportedCsvOption FORCE_QUOTE), got {other:?}"),
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

    /// SP-PG-COPY-CSV T1: a WITH clause with CSV format + HEADER true
    /// is fully accepted (was rejected pre-CSV V1).
    #[test]
    fn t1_parse_copy_with_format_among_other_options() {
        match parse_copy_command("COPY t FROM STDIN WITH (FORMAT csv, HEADER true)") {
            Some(ParsedCopy::From { format: CopyFormat::Csv(opts), .. }) => {
                assert!(opts.header);
            }
            other => panic!("expected From with Csv+HEADER, got {other:?}"),
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
