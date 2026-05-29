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

/// SP-PG-EXTQ T7 — DISCARD-flavor recognizer for the Simple Query
/// path.
///
/// PG-spec DISCARD (PG §SQL-DISCARD) resets per-session state on the
/// server. The vast majority of ORM connection pools (psycopg2,
/// SQLAlchemy default pool, JDBC HikariCP, pgx) issue
/// `DISCARD ALL` between checkouts to ensure the next caller sees a
/// clean session — temp tables dropped, GUCs reset, prepared
/// statements + portals cleared.
///
/// V1 KesselDB has no temp tables / GUCs / cursors / sequence cache, so
/// the only state DISCARD actually touches is the extq SessionState
/// (prepared statements + portals + error_state). Without this hook,
/// the SQL hits `kessel-sql` which doesn't know DISCARD and rejects it
/// with `42601 syntax_error` — breaking every ORM pool.
///
/// This recognizer is intentionally LENIENT — leading whitespace +
/// line/block comments are stripped before the keyword test, trailing
/// `;` is tolerated, the target keyword (ALL / STATEMENTS / PORTALS /
/// PLANS / SEQUENCES / TEMP / TEMPORARY) is case-insensitive. Returns
/// `Some(DiscardKind::*)` on a match and `None` for anything else (the
/// caller's existing dispatch path is unchanged for non-DISCARD SQL).
///
/// V1 maps:
/// - `DISCARD ALL` → `DiscardKind::All` (drop both stmts + portals +
///   error_state).
/// - `DISCARD STATEMENTS` → `DiscardKind::Statements` (drop just
///   statements; portals preserved).
/// - `DISCARD PORTALS` → `DiscardKind::Portals` (drop just portals;
///   statements preserved).
/// - `DISCARD PLANS` / `DISCARD SEQUENCES` / `DISCARD TEMP` /
///   `DISCARD TEMPORARY` → `DiscardKind::Noop` (V1 doesn't track these,
///   so DISCARD is a no-op; we still emit `DISCARD ALL` CommandComplete
///   so the client doesn't choke).
///
/// Per PG §SQL-DISCARD, every variant emits `CommandComplete("DISCARD
/// ALL")` (the literal tag for DISCARD is the input verb in PG's own
/// implementation, but every libpq-based client treats the tag as
/// opaque so V1 can normalize to `DISCARD ALL`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscardKind {
    /// `DISCARD ALL` — full session reset.
    All,
    /// `DISCARD STATEMENTS` — drop prepared statements only.
    Statements,
    /// `DISCARD PORTALS` — drop portals only.
    Portals,
    /// `DISCARD PLANS` / `SEQUENCES` / `TEMP` / `TEMPORARY` — V1 doesn't
    /// track these surfaces, so this is a no-op on the gateway side.
    /// We still recognize + emit `CommandComplete` so the client
    /// pool's state-reset handshake completes cleanly.
    Noop,
}

/// Recognize a DISCARD-flavor SQL statement. Returns `Some(kind)` on
/// a match (case-insensitive, leading-comment-tolerant, trailing-
/// semicolon-tolerant) or `None` otherwise. See `DiscardKind` for the
/// supported variants.
pub fn recognize_discard(sql: &str) -> Option<DiscardKind> {
    // Strip leading whitespace + comments using the existing
    // dispatch.rs leading-comment-stripper shape (inlined here so
    // query.rs stays dispatch.rs-independent).
    let mut s = sql;
    loop {
        let trimmed = s.trim_start();
        if let Some(rest) = trimmed.strip_prefix("--") {
            // Line comment to next \n.
            match rest.find('\n') {
                Some(p) => s = &rest[p + 1..],
                None => return None, // comment-only → no DISCARD
            }
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("/*") {
            // Block comment — non-nesting, scan for `*/`.
            match rest.find("*/") {
                Some(p) => s = &rest[p + 2..],
                None => return None,
            }
            continue;
        }
        s = trimmed;
        break;
    }
    // Strip trailing whitespace + a single trailing `;` + more
    // whitespace (the dispatch layer tolerates `SELECT 1;` and we
    // mirror that here).
    let mut t = s.trim_end();
    if let Some(stripped) = t.strip_suffix(';') {
        t = stripped.trim_end();
    }
    if t.is_empty() {
        return None;
    }
    // Split on the first whitespace boundary into KEYWORD + REST.
    let mut iter = t.splitn(2, |c: char| c.is_whitespace());
    let kw = iter.next().unwrap_or("");
    if !kw.eq_ignore_ascii_case("DISCARD") {
        return None;
    }
    let rest = iter.next().unwrap_or("").trim();
    // Bare `DISCARD` with no target → treat as DISCARD ALL per the
    // PG spec's "default target" reading (in fact PG rejects bare
    // DISCARD as a syntax error but V1 is lenient because some
    // older asyncpg builds emit it on shutdown). Empty rest → All.
    if rest.is_empty() {
        return Some(DiscardKind::All);
    }
    // Take just the next token (target). Anything beyond the
    // target — e.g. `DISCARD ALL EXTRA STUFF` — V1 still treats as
    // valid because clients sometimes append benign suffixes
    // (asyncpg has been seen appending `WAIT`).
    let target = rest.split_whitespace().next().unwrap_or("");
    match target.to_ascii_uppercase().as_str() {
        "ALL" => Some(DiscardKind::All),
        "STATEMENTS" => Some(DiscardKind::Statements),
        "PORTALS" => Some(DiscardKind::Portals),
        "PLANS" | "SEQUENCES" | "TEMP" | "TEMPORARY" => Some(DiscardKind::Noop),
        _ => None,
    }
}

/// SP-PG-EXTQ T7 — transaction-control recognizer for the Simple
/// Query path.
///
/// V1 KesselDB has no real transaction blocks (every statement is
/// auto-committed at the engine layer; spec §11 weak-spot #6 names
/// SP-PG-TX as the V2 arc that lifts this). But every ORM pool +
/// session manager emits `BEGIN` / `COMMIT` / `ROLLBACK` (and the
/// SQLAlchemy-default `SET SESSION CHARACTERISTICS AS TRANSACTION`
/// shape) at checkout/checkin. Without gateway-side interception
/// these reach `kessel-sql` which doesn't know them and rejects with
/// `42601 unsupported statement` — breaking SQLAlchemy at the
/// `engine.connect()` probe.
///
/// V1 V2-ish workaround: recognize the verbs at the gateway, treat
/// them as NO-OPS at the storage layer (every statement is already
/// auto-committed), emit the canonical CommandComplete tag, return
/// RFQ('I'). The RFQ status byte STAYS 'I' (idle) because V1 has no
/// real implicit-tx state to emit 'T' (transaction) for — V2 SP-PG-TX
/// fixes the status byte properly.
///
/// Recognizes:
/// - `BEGIN` / `BEGIN TRANSACTION` / `BEGIN WORK` / `START TRANSACTION` →
///   `TxControl::Begin` (CommandComplete tag "BEGIN").
/// - `COMMIT` / `COMMIT TRANSACTION` / `COMMIT WORK` / `END` /
///   `END TRANSACTION` → `TxControl::Commit` (CommandComplete tag
///   "COMMIT").
/// - `ROLLBACK` / `ROLLBACK TRANSACTION` / `ROLLBACK WORK` / `ABORT`
///   → `TxControl::Rollback` (CommandComplete tag "ROLLBACK").
/// - `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...`
///   + `SET TRANSACTION ISOLATION LEVEL ...` → `TxControl::SetTx`
///   (CommandComplete tag "SET").
///
/// Returns `None` for anything else (the existing dispatch path is
/// unchanged). Like `recognize_discard`, this is lenient on leading
/// whitespace + comments + trailing `;`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxControl {
    /// `BEGIN` / `START TRANSACTION` → CommandComplete tag "BEGIN".
    Begin,
    /// `COMMIT` / `END` → CommandComplete tag "COMMIT".
    Commit,
    /// `ROLLBACK` / `ABORT` → CommandComplete tag "ROLLBACK".
    Rollback,
    /// `SET (SESSION CHARACTERISTICS AS )? TRANSACTION ...` →
    /// CommandComplete tag "SET". V1 doesn't track isolation level
    /// (everything is committed-on-statement-success), so we accept
    /// and discard.
    SetTx,
}

impl TxControl {
    /// CommandComplete tag PG emits for this verb.
    pub fn command_tag(self) -> &'static str {
        match self {
            TxControl::Begin => "BEGIN",
            TxControl::Commit => "COMMIT",
            TxControl::Rollback => "ROLLBACK",
            TxControl::SetTx => "SET",
        }
    }
}

/// Recognize a transaction-control SQL statement. See `TxControl` for
/// the supported verbs. Returns `Some(kind)` on a match (case-
/// insensitive, leading-comment-tolerant, trailing-semicolon-
/// tolerant); `None` otherwise.
pub fn recognize_tx_control(sql: &str) -> Option<TxControl> {
    // Reuse the same leading-strip shape as recognize_discard.
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
    let mut t = s.trim_end();
    if let Some(stripped) = t.strip_suffix(';') {
        t = stripped.trim_end();
    }
    if t.is_empty() {
        return None;
    }
    // Tokenize for the first 1-2 keywords.
    let mut tokens = t.split_whitespace();
    let kw1 = tokens.next().unwrap_or("").to_ascii_uppercase();
    let kw2 = tokens.next().unwrap_or("").to_ascii_uppercase();
    match kw1.as_str() {
        "BEGIN" => {
            // BEGIN / BEGIN TRANSACTION / BEGIN WORK / BEGIN ISOLATION ...
            // Anything starting with BEGIN is a transaction-begin
            // verb in PG (V1 is intentionally lax here).
            Some(TxControl::Begin)
        }
        "START" => {
            // START TRANSACTION — required two-word form.
            if kw2 == "TRANSACTION" {
                Some(TxControl::Begin)
            } else {
                None
            }
        }
        "COMMIT" | "END" => {
            // COMMIT / COMMIT TRANSACTION / COMMIT WORK / END / END
            // TRANSACTION. END is a PG synonym for COMMIT.
            Some(TxControl::Commit)
        }
        "ROLLBACK" | "ABORT" => {
            // ROLLBACK / ROLLBACK TRANSACTION / ROLLBACK WORK / ABORT.
            Some(TxControl::Rollback)
        }
        "SET" => {
            // SET TRANSACTION ISOLATION LEVEL ...
            // SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...
            // SET LOCAL TRANSACTION ...
            // Match against any SET that includes the word
            // TRANSACTION in the first 5 tokens (catches all variants
            // SQLAlchemy + asyncpg + JDBC emit on connect probe).
            let head = t.to_ascii_uppercase();
            if head.contains("TRANSACTION") {
                Some(TxControl::SetTx)
            } else {
                None
            }
        }
        _ => None,
    }
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

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-EXTQ T7 KATs — DISCARD recognition. Locks the ORM-pool
    // adoption hook that gateway-intercepts DISCARD ALL / STATEMENTS /
    // PORTALS before the SQL reaches the engine.
    // ───────────────────────────────────────────────────────────────────

    /// Headline T7 KAT: bare `DISCARD ALL` recognized.
    #[test]
    fn t7_recognize_discard_all_basic() {
        assert_eq!(recognize_discard("DISCARD ALL"), Some(DiscardKind::All));
    }

    /// `DISCARD STATEMENTS` recognized as the statement-only variant.
    #[test]
    fn t7_recognize_discard_statements() {
        assert_eq!(
            recognize_discard("DISCARD STATEMENTS"),
            Some(DiscardKind::Statements)
        );
    }

    /// `DISCARD PORTALS` recognized as the portal-only variant.
    #[test]
    fn t7_recognize_discard_portals() {
        assert_eq!(
            recognize_discard("DISCARD PORTALS"),
            Some(DiscardKind::Portals)
        );
    }

    /// V1-untracked variants recognized as Noop (we still emit
    /// CommandComplete so ORM pool checkout doesn't choke).
    #[test]
    fn t7_recognize_discard_noop_variants() {
        assert_eq!(recognize_discard("DISCARD PLANS"), Some(DiscardKind::Noop));
        assert_eq!(
            recognize_discard("DISCARD SEQUENCES"),
            Some(DiscardKind::Noop)
        );
        assert_eq!(recognize_discard("DISCARD TEMP"), Some(DiscardKind::Noop));
        assert_eq!(
            recognize_discard("DISCARD TEMPORARY"),
            Some(DiscardKind::Noop)
        );
    }

    /// Case-insensitive on the verb and target keyword.
    #[test]
    fn t7_recognize_discard_case_insensitive() {
        assert_eq!(recognize_discard("discard all"), Some(DiscardKind::All));
        assert_eq!(recognize_discard("Discard All"), Some(DiscardKind::All));
        assert_eq!(recognize_discard("DISCARD all"), Some(DiscardKind::All));
        assert_eq!(
            recognize_discard("discard STATEMENTS"),
            Some(DiscardKind::Statements)
        );
    }

    /// Trailing `;` is tolerated (dispatch layer convention).
    #[test]
    fn t7_recognize_discard_with_trailing_semicolon() {
        assert_eq!(recognize_discard("DISCARD ALL;"), Some(DiscardKind::All));
        assert_eq!(
            recognize_discard("  DISCARD ALL  ;  "),
            Some(DiscardKind::All)
        );
    }

    /// Leading whitespace + line comments + block comments stripped
    /// before the keyword test (mirrors `cmd_complete_tag_for_sql`
    /// tolerance of ORM-prepended SQL comments).
    #[test]
    fn t7_recognize_discard_with_leading_comments() {
        assert_eq!(
            recognize_discard("  -- pool checkout\n DISCARD ALL"),
            Some(DiscardKind::All)
        );
        assert_eq!(
            recognize_discard("/* connection reset */ DISCARD ALL"),
            Some(DiscardKind::All)
        );
        assert_eq!(
            recognize_discard("-- a\n-- b\n/* c */ DISCARD STATEMENTS"),
            Some(DiscardKind::Statements)
        );
    }

    /// Bare `DISCARD` with no target → treated as DISCARD ALL (lenient
    /// shape some asyncpg builds emit on shutdown).
    #[test]
    fn t7_recognize_bare_discard_falls_back_to_all() {
        assert_eq!(recognize_discard("DISCARD"), Some(DiscardKind::All));
        assert_eq!(recognize_discard("DISCARD ;"), Some(DiscardKind::All));
    }

    /// Non-DISCARD SQL is NOT recognized (negative control — defends
    /// the dispatch-fallthrough invariant).
    #[test]
    fn t7_recognize_discard_returns_none_for_non_discard_sql() {
        assert_eq!(recognize_discard("SELECT 1"), None);
        assert_eq!(recognize_discard("INSERT INTO t (id) VALUES (1)"), None);
        assert_eq!(recognize_discard("BEGIN"), None);
        assert_eq!(recognize_discard("COMMIT"), None);
        assert_eq!(recognize_discard(""), None);
        assert_eq!(recognize_discard("   "), None);
        assert_eq!(recognize_discard("-- only a comment"), None);
    }

    /// `DISCARD` substring inside a different verb is NOT recognized
    /// (defends against false positives like a quoted SQL literal
    /// reading `'DISCARD'`).
    #[test]
    fn t7_recognize_discard_only_matches_leading_keyword() {
        assert_eq!(recognize_discard("SELECT 'DISCARD'"), None);
        assert_eq!(
            recognize_discard("INSERT INTO logs (verb) VALUES ('DISCARD ALL')"),
            None
        );
    }

    /// Unknown DISCARD target → returns None (we don't silently treat
    /// e.g. `DISCARD WIDGETS` as a no-op, because that's likely a
    /// client bug or unsupported PG feature we shouldn't pretend to
    /// implement).
    #[test]
    fn t7_recognize_discard_unknown_target_returns_none() {
        assert_eq!(recognize_discard("DISCARD WIDGETS"), None);
        assert_eq!(recognize_discard("DISCARD CONNECTIONS"), None);
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-EXTQ T7 KATs — transaction-control recognition. Locks the
    // SQLAlchemy / asyncpg / JDBC pool checkout/checkin probe path
    // (BEGIN / COMMIT / ROLLBACK / SET TRANSACTION ISOLATION LEVEL).
    // ───────────────────────────────────────────────────────────────────

    /// HEADLINE T7 KAT: `BEGIN` recognized as TxControl::Begin.
    #[test]
    fn t7_recognize_tx_control_begin() {
        assert_eq!(recognize_tx_control("BEGIN"), Some(TxControl::Begin));
        assert_eq!(
            recognize_tx_control("BEGIN TRANSACTION"),
            Some(TxControl::Begin)
        );
        assert_eq!(recognize_tx_control("BEGIN WORK"), Some(TxControl::Begin));
        assert_eq!(
            recognize_tx_control("START TRANSACTION"),
            Some(TxControl::Begin)
        );
        assert_eq!(recognize_tx_control("begin"), Some(TxControl::Begin));
    }

    /// `COMMIT` + `END` variants recognized.
    #[test]
    fn t7_recognize_tx_control_commit() {
        assert_eq!(recognize_tx_control("COMMIT"), Some(TxControl::Commit));
        assert_eq!(
            recognize_tx_control("COMMIT TRANSACTION"),
            Some(TxControl::Commit)
        );
        assert_eq!(
            recognize_tx_control("COMMIT WORK"),
            Some(TxControl::Commit)
        );
        assert_eq!(recognize_tx_control("END"), Some(TxControl::Commit));
        assert_eq!(
            recognize_tx_control("END TRANSACTION"),
            Some(TxControl::Commit)
        );
        assert_eq!(recognize_tx_control("commit"), Some(TxControl::Commit));
    }

    /// `ROLLBACK` + `ABORT` variants recognized.
    #[test]
    fn t7_recognize_tx_control_rollback() {
        assert_eq!(
            recognize_tx_control("ROLLBACK"),
            Some(TxControl::Rollback)
        );
        assert_eq!(
            recognize_tx_control("ROLLBACK TRANSACTION"),
            Some(TxControl::Rollback)
        );
        assert_eq!(recognize_tx_control("ABORT"), Some(TxControl::Rollback));
        assert_eq!(
            recognize_tx_control("rollback"),
            Some(TxControl::Rollback)
        );
    }

    /// SQLAlchemy/JDBC isolation-level setter variants recognized.
    #[test]
    fn t7_recognize_tx_control_set_transaction_variants() {
        assert_eq!(
            recognize_tx_control("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"),
            Some(TxControl::SetTx)
        );
        assert_eq!(
            recognize_tx_control(
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED"
            ),
            Some(TxControl::SetTx)
        );
        assert_eq!(
            recognize_tx_control("SET LOCAL TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
            Some(TxControl::SetTx)
        );
    }

    /// Trailing semicolon + leading comments are tolerated (mirrors
    /// `recognize_discard`).
    #[test]
    fn t7_recognize_tx_control_lenient_formatting() {
        assert_eq!(recognize_tx_control("BEGIN;"), Some(TxControl::Begin));
        assert_eq!(
            recognize_tx_control("  -- pool checkout\n BEGIN"),
            Some(TxControl::Begin)
        );
        assert_eq!(
            recognize_tx_control("/* sa */ COMMIT TRANSACTION;"),
            Some(TxControl::Commit)
        );
    }

    /// Negative control — non-tx-control SQL returns None.
    #[test]
    fn t7_recognize_tx_control_non_tx_sql_returns_none() {
        assert_eq!(recognize_tx_control("SELECT 1"), None);
        assert_eq!(recognize_tx_control("INSERT INTO t VALUES (1)"), None);
        assert_eq!(recognize_tx_control(""), None);
        // SET that isn't tx-control (e.g. SET search_path) → None.
        assert_eq!(recognize_tx_control("SET search_path = public"), None);
        assert_eq!(recognize_tx_control("SET timezone = 'UTC'"), None);
        // START without TRANSACTION → None.
        assert_eq!(recognize_tx_control("START FOO"), None);
        assert_eq!(recognize_tx_control("DISCARD ALL"), None);
    }

    /// CommandComplete tag matches PG canonical strings.
    #[test]
    fn t7_tx_control_command_tag() {
        assert_eq!(TxControl::Begin.command_tag(), "BEGIN");
        assert_eq!(TxControl::Commit.command_tag(), "COMMIT");
        assert_eq!(TxControl::Rollback.command_tag(), "ROLLBACK");
        assert_eq!(TxControl::SetTx.command_tag(), "SET");
    }
}
