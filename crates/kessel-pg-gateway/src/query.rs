//! Simple Query (`Q`) message parser. PG §55.7 / §55.2.3.
//!
//! Wire shape (PG §55.7 "Query"):
//!
//! ```text
//! Q [length:4 BE — includes itself but NOT type] [sql_text\0]
//! ```
//!
//! `length` covers itself + the SQL text + the trailing NUL. The SQL
//! text is a NUL-terminated C-string of valid UTF-8 (PG accepts any
//! `client_encoding` for the SQL text, but our V1 advertises
//! `client_encoding=UTF8` in ParameterStatus so we enforce UTF-8 at
//! the gate per spec §3.3 / §4 — a libpq client violating that
//! contract is a real bug we want to surface, not silently mis-render).
//!
//! **What this module does:** parses the BODY of the Q message (i.e.
//! the bytes AFTER the type tag and length prefix have been stripped
//! by `server::read_message`) and returns the SQL text slice with the
//! trailing NUL removed. The caller copies if the slice needs to
//! out-live the inbound buffer.
//!
//! **What this module does NOT do:**
//! - It does NOT dispatch into `EngineApply::apply_sql` — that's T8.
//! - It does NOT detect multi-statement Q (separate hook
//!   `contains_multiple_statements` is exposed for T8 to use; per
//!   spec §11 weak-spot #5, multi-statement Q → SQLSTATE `42601`
//!   syntax_error at the response layer).
//! - It does NOT detect empty / whitespace-only queries — T8 calls
//!   `is_effectively_empty` on the parsed text and emits
//!   `EmptyQueryResponse` ('I') if so.
//! - It does NOT enforce a maximum SQL length — the framing layer
//!   (`server::read_message`) already capped the whole frame at
//!   `PG_MAX_MESSAGE_SIZE = 16 MiB` per spec §3.1.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`

#![forbid(unsafe_code)]
#![allow(dead_code)]

/// Errors `parse_query_body` can return. All map to SQLSTATE `08P01`
/// protocol_violation per spec §6.2 + §7 (the Q frame itself is
/// malformed; T7's ErrorResponse encoder is responsible for
/// translation when the dispatcher wires this in T8).
#[derive(Debug, PartialEq, Eq)]
pub enum QueryParseError {
    /// The body did not end with a NUL byte. PG §55.7 requires the
    /// SQL text be a C-string (NUL-terminated). A client that omits
    /// the terminator is sending a malformed frame.
    MissingNulTerminator,
    /// The body had a NUL embedded inside the SQL text (i.e. before
    /// the terminator). Even with `standard_conforming_strings=on`,
    /// embedded NULs are a protocol violation in the simple-query
    /// path — PG would reject this at the parser layer with `22021`
    /// character_not_in_repertoire, but we surface it at the wire
    /// layer because the C-string framing is unambiguous.
    EmbeddedNul,
    /// The body bytes (minus the trailing NUL) were not valid UTF-8.
    /// V1 advertises `client_encoding=UTF8` in ParameterStatus, so a
    /// client sending non-UTF-8 SQL has violated the advertised
    /// session encoding.
    NotUtf8,
    /// The body was zero bytes (no trailing NUL even). Distinct from
    /// MissingNulTerminator because a zero-length body is a length-
    /// field mismatch, not a forgot-the-NUL bug.
    EmptyBody,
}

/// Parses the BODY of a Q (Simple Query) message — i.e. the bytes
/// AFTER `server::read_message` has stripped the 1-byte type tag and
/// 4-byte length prefix.
///
/// On success, returns the SQL text as a borrowed `&str` (the
/// trailing NUL is removed; embedded NULs are rejected). The returned
/// slice borrows from `body`; the caller MUST copy it if it needs to
/// out-live the inbound buffer (which T8 will, because dispatch into
/// the engine consumes an owned `String`).
///
/// The returned text MAY be empty (the client sent `Q [length=5]
/// \0`). Per PG §55.2.3, an empty Q triggers `EmptyQueryResponse`
/// ('I') instead of `RowDescription`/`DataRow`/`CommandComplete`.
/// T8 uses `is_effectively_empty` on the returned `&str` to decide.
pub fn parse_query_body(body: &[u8]) -> Result<&str, QueryParseError> {
    if body.is_empty() {
        return Err(QueryParseError::EmptyBody);
    }
    let last = *body.last().expect("non-empty checked above");
    if last != 0 {
        return Err(QueryParseError::MissingNulTerminator);
    }
    // Strip the trailing NUL.
    let sql_bytes = &body[..body.len() - 1];
    // Reject embedded NULs — every byte before the terminator must
    // be non-zero. (`memchr` would be faster but we're zero-dep and
    // a linear scan over a 16 MiB cap is fine for V1.)
    if sql_bytes.iter().any(|&b| b == 0) {
        return Err(QueryParseError::EmbeddedNul);
    }
    std::str::from_utf8(sql_bytes).map_err(|_| QueryParseError::NotUtf8)
}

/// Returns true if the SQL text is effectively empty (zero-length OR
/// only whitespace OR only SQL `--` line comments OR only `/* ... */`
/// block comments). T8 calls this on the parsed text and emits
/// `EmptyQueryResponse` if true, instead of routing into the engine.
///
/// PG §55.2.3: "If the query string contains no SQL command, the
/// server returns EmptyQueryResponse instead of CommandComplete."
/// Most clients treat this as a successful no-op; some (older
/// pgcli) display "(no query)".
///
/// V1 recognizes:
/// - ASCII whitespace (`' '`, `'\t'`, `'\r'`, `'\n'`)
/// - `--` line comment (to end-of-line or end-of-input)
/// - `/* ... */` block comment (non-nested; PG technically supports
///   nesting but V1 doesn't — nested block comments will be passed
///   through to `kessel-sql` which will parse them as it sees fit;
///   "effectively empty" detection is best-effort here).
pub fn is_effectively_empty(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b' ' || b == b'\t' || b == b'\r' || b == b'\n' {
            i += 1;
            continue;
        }
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            // Line comment — skip to next \n or end.
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            // Block comment — skip to matching */ or end.
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            // Skip past the closing */ if present; if unterminated,
            // treat the rest as comment (best-effort).
            if i + 1 < bytes.len() {
                i += 2;
            } else {
                i = bytes.len();
            }
            continue;
        }
        // Non-whitespace, non-comment byte found → not empty.
        return false;
    }
    true
}

/// Returns true if the SQL text appears to contain MULTIPLE
/// statements separated by `;`. V1 rejects multi-statement Q with
/// SQLSTATE `42601` syntax_error per spec §11 weak-spot #5 — KesselDB
/// SQL is single-statement only, and a Q like `SELECT 1; DROP TABLE
/// users` should NOT be silently mis-routed.
///
/// Detection rules (per PG §4.1.5 string-literal lexing rules
/// approximated for the V1 simple-query path):
/// - A `;` outside any string literal / quoted identifier counts as
///   a statement separator. A trailing-only `;` (single statement
///   with a terminator) does NOT count — `SELECT 1;` is one
///   statement, `SELECT 1; SELECT 2` is two.
/// - Single-quoted strings (`'...'`) with `''` escapes are
///   recognized.
/// - Double-quoted identifiers (`"..."`) with `""` escapes are
///   recognized.
/// - Dollar-quoted strings (`$tag$...$tag$`) are NOT recognized in
///   V1 (zero-dep best-effort; KesselDB SQL doesn't emit them).
/// - Line / block comments are recognized (statements inside them
///   don't count).
///
/// Returns true if MORE than one statement is detected. A single
/// statement (with or without trailing `;`) returns false.
pub fn contains_multiple_statements(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut semicolon_count = 0;
    while i < bytes.len() {
        let b = bytes[i];
        // Whitespace
        if b == b' ' || b == b'\t' || b == b'\r' || b == b'\n' {
            i += 1;
            continue;
        }
        // Line comment
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Block comment
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < bytes.len() { i += 2; } else { i = bytes.len(); }
            continue;
        }
        // Single-quoted string literal
        if b == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    // Doubled '' is an escaped quote — skip both
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Double-quoted identifier
        if b == b'"' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Statement separator
        if b == b';' {
            semicolon_count += 1;
            i += 1;
            continue;
        }
        // Any other content byte — once we see SQL content AFTER a
        // semicolon, we have a multi-statement Q. The flag latches
        // (`seen_non_ws_after_semicolon` is monotonic) so that a
        // trailing `;` after the second statement (e.g. `A; B;`)
        // doesn't accidentally unset it.
        if semicolon_count > 0 {
            return true;
        }
        i += 1;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    // ───────────────────────────────────────────────────────────────────
    // T3 KATs — lock the Simple Query message body parser per PG §55.7
    // / §55.2.3.
    // ───────────────────────────────────────────────────────────────────

    /// Happy path — a well-formed `Q` body returns the SQL text with
    /// the trailing NUL stripped.
    #[test]
    fn t3_parse_select_1_strips_trailing_nul() {
        let body = b"SELECT 1\0";
        assert_eq!(parse_query_body(body), Ok("SELECT 1"));
    }

    /// PG §55.2.3 empty-query path: `Q [length=5] \0` → body is just
    /// `\0` → parses to empty string. T8 will then call
    /// `is_effectively_empty` and emit `EmptyQueryResponse`.
    #[test]
    fn t3_parse_empty_query_returns_empty_string() {
        let body = b"\0";
        assert_eq!(parse_query_body(body), Ok(""));
        assert!(is_effectively_empty(""));
    }

    /// A body with NO trailing NUL is malformed per PG §55.7
    /// (C-string requirement).
    #[test]
    fn t3_rejects_body_without_trailing_nul() {
        let body = b"SELECT 1";
        assert_eq!(
            parse_query_body(body),
            Err(QueryParseError::MissingNulTerminator)
        );
    }

    /// Zero-length body — the length field claimed 4 (header only)
    /// without even a terminator. Distinct error variant from
    /// MissingNulTerminator so the dispatcher can log differently.
    #[test]
    fn t3_rejects_zero_length_body() {
        assert_eq!(parse_query_body(b""), Err(QueryParseError::EmptyBody));
    }

    /// Embedded NUL inside the SQL text → rejected. A client sending
    /// `SELECT 1\0 DROP\0` is either confused or hostile; either way
    /// V1 rejects rather than truncating the SQL silently.
    #[test]
    fn t3_rejects_embedded_nul_in_sql_text() {
        let body = b"SELECT 1\0 DROP\0";
        assert_eq!(
            parse_query_body(body),
            Err(QueryParseError::EmbeddedNul)
        );
    }

    /// Non-UTF-8 body — V1 advertised `client_encoding=UTF8` in
    /// ParameterStatus; an invalid-UTF-8 SQL text is a session-
    /// encoding contract violation. Use `0xFF` (a never-valid UTF-8
    /// start byte) to construct an unambiguous bad sequence.
    #[test]
    fn t3_rejects_non_utf8_sql_text() {
        let body: &[u8] = &[b'S', b'E', b'L', 0xFF, 0xFE, 0];
        assert_eq!(parse_query_body(body), Err(QueryParseError::NotUtf8));
    }

    /// Multi-byte UTF-8 round-trips cleanly. PG accepts UTF-8 SQL
    /// (e.g. `SELECT 'café'`). Locks no double-decoding bug.
    #[test]
    fn t3_parses_utf8_multibyte_text() {
        let body = "SELECT 'café'\0".as_bytes();
        assert_eq!(parse_query_body(body), Ok("SELECT 'café'"));
    }

    /// Whitespace-only SQL is `is_effectively_empty == true`. Locks
    /// the T8 EmptyQueryResponse hook.
    #[test]
    fn t3_whitespace_only_is_effectively_empty() {
        assert!(is_effectively_empty(""));
        assert!(is_effectively_empty(" "));
        assert!(is_effectively_empty("\t\r\n  "));
    }

    /// Line / block comment-only SQL is also effectively empty per
    /// PG §55.2.3 (no actual SQL command).
    #[test]
    fn t3_comment_only_is_effectively_empty() {
        assert!(is_effectively_empty("-- just a comment"));
        assert!(is_effectively_empty("-- one\n-- two\n"));
        assert!(is_effectively_empty("/* block comment */"));
        assert!(is_effectively_empty("  /* foo */ \n  -- bar"));
    }

    /// A real SQL statement is NOT effectively empty (negative-control).
    #[test]
    fn t3_real_sql_is_not_effectively_empty() {
        assert!(!is_effectively_empty("SELECT 1"));
        assert!(!is_effectively_empty("-- comment\nSELECT 1"));
        assert!(!is_effectively_empty("/* foo */ INSERT INTO t VALUES (1)"));
    }

    /// Single statement with trailing `;` is NOT multi-statement
    /// per spec §11 weak-spot #5 detection rule.
    #[test]
    fn t3_single_statement_with_trailing_semicolon_is_not_multi() {
        assert!(!contains_multiple_statements("SELECT 1"));
        assert!(!contains_multiple_statements("SELECT 1;"));
        assert!(!contains_multiple_statements("SELECT 1 ;  "));
        assert!(!contains_multiple_statements("SELECT 1;\n-- trailing comment"));
    }

    /// Two statements separated by `;` → IS multi-statement (V1
    /// rejects with `42601`).
    #[test]
    fn t3_two_statements_detected_as_multi() {
        assert!(contains_multiple_statements("SELECT 1; SELECT 2"));
        assert!(contains_multiple_statements("SELECT 1; SELECT 2;"));
        assert!(contains_multiple_statements(
            "INSERT INTO t VALUES (1); DELETE FROM t WHERE x = 2"
        ));
    }

    /// Semicolons INSIDE string literals don't count as separators.
    /// Locks the lexer-aware multi-statement detector against false
    /// positives that would break legitimate single-statement SQL.
    #[test]
    fn t3_semicolon_inside_string_literal_is_not_a_separator() {
        // 'a;b' is a single literal — the ; is part of it.
        assert!(!contains_multiple_statements("SELECT 'a;b'"));
        // Doubled '' escape inside the literal: 'it''s; ok'
        assert!(!contains_multiple_statements("SELECT 'it''s; ok'"));
    }

    /// Semicolons inside double-quoted identifiers don't count either.
    #[test]
    fn t3_semicolon_inside_quoted_identifier_is_not_a_separator() {
        assert!(!contains_multiple_statements(r#"SELECT "weird;col" FROM t"#));
    }

    /// Semicolons inside comments don't count.
    #[test]
    fn t3_semicolon_inside_comment_is_not_a_separator() {
        assert!(!contains_multiple_statements("SELECT 1 -- ; this is a comment\n"));
        assert!(!contains_multiple_statements("SELECT 1 /* ; ; ; */"));
    }
}
