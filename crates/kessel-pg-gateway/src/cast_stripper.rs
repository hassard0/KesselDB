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

        // SP-PG-SQL-QUOTED-IDENT — double-quoted delimited identifier.
        // Copy through to the closing `"`, honouring the doubled-`""`
        // escape (PG §4.1.1). A cast operator can never appear INSIDE a
        // delimited identifier, but a `'` or `::` CAN (e.g. a column
        // literally named `"a::b"` or `"O'Brien"`); skipping the region
        // keeps the single-quote scanner from mis-pairing on an embedded
        // `'` and keeps a literal `::` inside the identifier from being
        // stripped. Django double-quotes every identifier, so this path
        // runs on every Django statement.
        if b == b'"' {
            out.push(b);
            i += 1;
            while i < bytes.len() {
                let c = bytes[i];
                out.push(c);
                i += 1;
                if c == b'"' {
                    if i < bytes.len() && bytes[i] == b'"' {
                        out.push(b'"');
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

/// SP-PG-EXTQ-CAST-VALIDATE-LITERAL — describes a single cross-category
/// literal cast detected by `find_literal_cast_mismatch`. The same data
/// shape feeds both the simple-query dispatcher's `42846 cannot_coerce`
/// renderer and the extended-query `ExtqError::LiteralCastMismatch`
/// variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiteralCastMismatch {
    /// Inferred natural PG OID of the literal preceding the `::`
    /// operator. Picked by `classify_literal_natural_oid` from the
    /// literal's shape (digits / quoted string / bool keyword / etc.).
    pub literal_oid: u32,
    /// PG OID the SQL cast declared (after the `::TYPE` identifier).
    pub cast_oid: u32,
    /// PG `typcategory` byte of the literal (see `types::oid_category`).
    pub literal_category: char,
    /// PG `typcategory` byte of the cast type.
    pub cast_category: char,
}

/// SP-PG-EXTQ-CAST-VALIDATE-LITERAL — scan `sql` for any
/// `LITERAL::TYPE` patterns whose literal natural category disagrees
/// with the cast type's category. Returns the FIRST mismatch (first-
/// mismatch-wins ordering — same as the `$N` validator) or `None` if
/// every literal cast is within-category, the literal is `NULL`, or
/// there are no literal casts at all.
///
/// V1 simplifications:
/// - Only the bytes IMMEDIATELY before `::` are classified (not
///   arbitrary expressions). `(1+2)::int8` falls through to V1's
///   "strip + hope" because the `)` token has no natural type.
/// - `NULL::TYPE` is always accepted (PG `NULL` is the canonical
///   typed-NULL idiom; the literal has no natural type).
/// - `$N::TYPE` is NEVER classified as a literal — the V1 + COMPAT
///   `$N` validator covers it at Bind time.
/// - String → date / string → numeric conversions PG accepts via its
///   input-function machinery (e.g. `'42'::int8`, `'2024-01-01'::date`)
///   are conservatively rejected here as cross-category. Lift in
///   `SP-PG-EXTQ-CAST-VALIDATE-LITERAL-NUMSTR` /
///   `SP-PG-EXTQ-CAST-VALIDATE-LITERAL-DATEPARSE`.
///
/// Called by `dispatch::dispatch_query`, `dispatch::dispatch_query_
/// with_params`, and `extq::dispatch_parse` BEFORE running the strip;
/// a `Some` result short-circuits the dispatch with a `42846
/// cannot_coerce` ErrorResponse.
pub fn find_literal_cast_mismatch(sql: &str) -> Option<LiteralCastMismatch> {
    let bytes = sql.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    // `just_closed_string` tracks whether the byte we just appended to
    // `out` closes a single-quoted string literal. We need this because
    // by the time the scanner sees `::`, the closing `'` is already in
    // `out` and the lookback at that byte sees a single `'` (which
    // could be the open OR close of a string — only the state tells us).
    let mut just_closed_string = false;
    let mut i = 0usize;

    while i < bytes.len() {
        let b = bytes[i];

        // Single-quoted string literal — copy through to the closing
        // quote, handling the doubled-quote escape `''` per PG §4.1.2.1.
        // After the closing quote, set `just_closed_string = true` so
        // the next `::` detection knows the preceding byte ends a TEXT
        // literal (natural OID 25).
        if b == b'\'' {
            out.push(b);
            i += 1;
            let mut closed = false;
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
                        closed = true;
                        break;
                    }
                }
            }
            just_closed_string = closed;
            continue;
        }

        // SP-PG-SQL-QUOTED-IDENT — double-quoted delimited identifier.
        // Copy through to the closing `"` (doubled-`""` escape). The
        // closing `"` is NOT a string close, so reset the
        // `just_closed_string` flag — a `::` immediately after a
        // delimited identifier casts the COLUMN VALUE, not a literal, so
        // the literal classifier must not treat it as a TEXT literal.
        if b == b'"' {
            out.push(b);
            i += 1;
            while i < bytes.len() {
                let c = bytes[i];
                out.push(c);
                i += 1;
                if c == b'"' {
                    if i < bytes.len() && bytes[i] == b'"' {
                        out.push(b'"');
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            just_closed_string = false;
            continue;
        }

        // Line comment `--` — copy through to the next newline. Inside
        // a comment we can't be classifying a literal so `just_closed_string`
        // is reset on any byte we emit here.
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            just_closed_string = false;
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

        // Block comment `/* ... */` — copy through.
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            just_closed_string = false;
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
        if b == b':' && i + 1 < bytes.len() && bytes[i + 1] == b':' {
            // SP-PG-EXTQ-CAST-VALIDATE-LITERAL — classify the bytes
            // immediately before the `::`. `just_closed_string` carries
            // the only context the lookback needs that `out` alone can't
            // give (since `'` is symmetric).
            let literal_oid =
                classify_literal_natural_oid(&out, just_closed_string);
            // Reset the string-just-closed flag now that we've consumed
            // it for the classification. Any further `'` will re-arm it.
            just_closed_string = false;

            i += 2; // skip `::`
            while i < bytes.len() && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            let type_name_start = i;
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
            {
                i += 1;
            }
            let type_name = &bytes[type_name_start..i];
            // Skip the optional `(args)` for parameterised types.
            if i < bytes.len() && bytes[i] == b'(' {
                i += 1;
                while i < bytes.len() && bytes[i] != b')' {
                    i += 1;
                }
                if i < bytes.len() {
                    i += 1;
                }
            }
            // Now check for a literal-cast mismatch.
            if let (Some(lit_oid), Some(cast_oid)) =
                (literal_oid, type_name_to_oid(type_name))
            {
                // `lit_oid == 0` is the NULL sentinel — always accept.
                if lit_oid != 0 {
                    let lit_cat = crate::types::oid_category(lit_oid);
                    let cast_cat = crate::types::oid_category(cast_oid);
                    if lit_cat != cast_cat {
                        return Some(LiteralCastMismatch {
                            literal_oid: lit_oid,
                            cast_oid,
                            literal_category: lit_cat,
                            cast_category: cast_cat,
                        });
                    }
                }
            }
            continue;
        }

        // Default: copy the byte through. Any non-`'` byte resets the
        // string-just-closed flag — only an IMMEDIATELY-following `::`
        // counts as a literal-cast on a string.
        if !b.is_ascii_whitespace() {
            just_closed_string = false;
        }
        out.push(b);
        i += 1;
    }
    None
}

/// SP-PG-EXTQ-CAST-VALIDATE-LITERAL — classify the natural PG type
/// OID of the literal IMMEDIATELY before the `::` cast operator,
/// using only the bytes already emitted to the strip scanner's output
/// buffer (`out`) plus the `just_closed_string` flag.
///
/// Returns:
/// - `Some(0)` if the literal is `NULL` (case-insensitive). Caller
///   special-cases zero as "anytype" and accepts unconditionally.
/// - `Some(PG_TYPE_BOOL)` for `true` / `false` (case-insensitive).
/// - `Some(PG_TYPE_TEXT)` if the byte preceding the `::` closes a
///   single-quoted string literal (signalled by the caller via
///   `just_closed_string`).
/// - `Some(PG_TYPE_INT4)` for a bare integer fitting in i32.
/// - `Some(PG_TYPE_INT8)` for a bare integer that overflows i32.
/// - `Some(PG_TYPE_FLOAT8)` for a numeric literal containing a `.`.
/// - `None` for anything else (identifier, `)`, `$N` placeholder, etc.
///   `$N` returns `None` here — the V1 + COMPAT validator covers it
///   at Bind time, and classifying it as a literal would double-record.
fn classify_literal_natural_oid(
    out: &[u8],
    just_closed_string: bool,
) -> Option<u32> {
    // 1. String literal — the previous byte closed a single-quoted
    //    string. We can short-circuit because the closing `'` is the
    //    last byte in `out` and it can't be anything else.
    if just_closed_string {
        return Some(crate::proto::PG_TYPE_TEXT);
    }

    if out.is_empty() {
        return None;
    }

    let last = out[out.len() - 1];

    // 2. Identifier / keyword — last byte is an ASCII letter, digit, or
    //    underscore, AND walking backwards we find a non-identifier
    //    byte. Read it case-insensitively and match `NULL` / `TRUE` /
    //    `FALSE`. (Digit-only suffix is handled below by the numeric
    //    branch.)
    if last.is_ascii_alphabetic() || last == b'_' {
        // Walk back to the start of the identifier.
        let mut j = out.len();
        while j > 0
            && (out[j - 1].is_ascii_alphanumeric() || out[j - 1] == b'_')
        {
            j -= 1;
        }
        let ident = &out[j..];
        // Match case-insensitive against the known literal keywords.
        if eq_ignore_ascii_case(ident, b"null") {
            return Some(0); // anytype sentinel
        }
        if eq_ignore_ascii_case(ident, b"true")
            || eq_ignore_ascii_case(ident, b"false")
        {
            return Some(crate::proto::PG_TYPE_BOOL);
        }
        // Other identifiers (column names, function names) are NOT
        // classifiable literals — fall through to "no classification".
        return None;
    }

    // 3. Bare integer / float — last byte is an ASCII digit. Walk back
    //    over digits + optional single `.`. If we hit `$` first, it's
    //    a `$N` placeholder — return None so the `$N` validator owns
    //    it (V1 + COMPAT path).
    if last.is_ascii_digit() {
        let mut j = out.len();
        let mut has_dot = false;
        while j > 0 {
            let c = out[j - 1];
            if c.is_ascii_digit() {
                j -= 1;
            } else if c == b'.' && !has_dot {
                has_dot = true;
                j -= 1;
            } else {
                break;
            }
        }
        // If the byte BEFORE the digits is `$`, it's a placeholder.
        if j > 0 && out[j - 1] == b'$' {
            return None;
        }
        // Bare numeric literal. Float if it contains a `.`.
        if has_dot {
            return Some(crate::proto::PG_TYPE_FLOAT8);
        }
        // Integer — INT4 if it fits in i32, INT8 otherwise. Use a
        // signed parse so we don't accidentally over-allocate for
        // i32::MAX + 1 etc.
        let digits = &out[j..];
        match std::str::from_utf8(digits).ok().and_then(|s| s.parse::<i64>().ok())
        {
            Some(n) if (i32::MIN as i64..=i32::MAX as i64).contains(&n) => {
                Some(crate::proto::PG_TYPE_INT4)
            }
            Some(_) => Some(crate::proto::PG_TYPE_INT8),
            // Overflow (very large multi-digit number) → INT8 still
            // works as a category-only signal; we just bucket it as
            // 'N' (numeric). pick INT8 so the OID is in the table.
            None => Some(crate::proto::PG_TYPE_INT8),
        }
    } else {
        None
    }
}

/// ASCII case-insensitive byte slice equality. Tiny helper so we don't
/// pull `std::ascii::AsciiExt` flows or allocate for case folding.
fn eq_ignore_ascii_case(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for (x, y) in a.iter().zip(b.iter()) {
        if x.to_ascii_lowercase() != y.to_ascii_lowercase() {
            return false;
        }
    }
    true
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

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-EXTQ-CAST-VALIDATE-LITERAL KATs — `find_literal_cast_mismatch`
    // ───────────────────────────────────────────────────────────────────

    /// Within-category numeric literal cast accepts. `1` is INT4 ('N'),
    /// `int8` is INT8 ('N') — same category, no mismatch.
    #[test]
    fn literal_int_cast_accepts_within_numeric_category() {
        assert_eq!(find_literal_cast_mismatch("SELECT 1::int8"), None);
        assert_eq!(find_literal_cast_mismatch("SELECT 42::int4"), None);
        assert_eq!(find_literal_cast_mismatch("SELECT 100::numeric"), None);
    }

    /// HEADLINE: cross-category 'S' string literal cast into 'N' numeric
    /// rejects. This is the V1 silent-strip hole this arc closes —
    /// previously `SELECT 'hello'::int8` stripped to `SELECT 'hello'`
    /// and the gateway didn't notice the bogus declaration.
    #[test]
    fn literal_string_cast_into_numeric_rejects() {
        let m = find_literal_cast_mismatch("SELECT 'hello'::int8")
            .expect("expected literal mismatch for 'hello'::int8");
        assert_eq!(m.literal_oid, crate::proto::PG_TYPE_TEXT);
        assert_eq!(m.cast_oid, crate::proto::PG_TYPE_INT8);
        assert_eq!(m.literal_category, 'S');
        assert_eq!(m.cast_category, 'N');
    }

    /// Float literal `1.5` is FLOAT8 ('N'); casting to INT8 ('N') is
    /// same-category narrowing — accept at the gateway. The engine
    /// type-checker handles any runtime overflow.
    #[test]
    fn literal_float_cast_into_numeric_accepts() {
        assert_eq!(find_literal_cast_mismatch("SELECT 1.5::int8"), None);
        assert_eq!(find_literal_cast_mismatch("SELECT 3.14::float4"), None);
    }

    /// `'hello'::text` is the canonical string-typed string literal —
    /// 'S' ↔ 'S' accepts.
    #[test]
    fn literal_string_to_text_accepts() {
        assert_eq!(find_literal_cast_mismatch("SELECT 'hello'::text"), None);
        assert_eq!(find_literal_cast_mismatch("SELECT 'x'::varchar"), None);
    }

    /// Bool literals (`true` / `false`) cast to BOOL accept. Case-
    /// insensitive on the keyword.
    #[test]
    fn literal_bool_to_bool_accepts() {
        assert_eq!(find_literal_cast_mismatch("SELECT true::bool"), None);
        assert_eq!(find_literal_cast_mismatch("SELECT false::boolean"), None);
        assert_eq!(find_literal_cast_mismatch("SELECT TRUE::bool"), None);
    }

    /// `NULL::TYPE` accepts unconditionally regardless of cast type.
    /// PG `NULL` is the canonical typed-NULL idiom (e.g. RowDescription
    /// emit, COALESCE shape). Case-insensitive on the keyword.
    #[test]
    fn literal_null_cast_always_accepts() {
        for sql in [
            "SELECT NULL::int8",
            "SELECT NULL::text",
            "SELECT NULL::bytea",
            "SELECT NULL::bool",
            "SELECT NULL::timestamptz",
            "SELECT null::int4",      // lowercase
            "SELECT Null::numeric",   // mixed case
        ] {
            assert_eq!(
                find_literal_cast_mismatch(sql),
                None,
                "NULL cast must always accept: {sql:?}"
            );
        }
    }

    /// `true::int8` is 'B' vs 'N' — cross-category mismatch.
    #[test]
    fn literal_bool_to_int_rejects() {
        let m = find_literal_cast_mismatch("SELECT true::int8")
            .expect("expected literal mismatch for true::int8");
        assert_eq!(m.literal_oid, crate::proto::PG_TYPE_BOOL);
        assert_eq!(m.cast_oid, crate::proto::PG_TYPE_INT8);
        assert_eq!(m.literal_category, 'B');
        assert_eq!(m.cast_category, 'N');
    }

    /// SQL has no `::` — no mismatch. Pure passthrough.
    #[test]
    fn literal_pure_passthrough_no_casts() {
        assert_eq!(find_literal_cast_mismatch(""), None);
        assert_eq!(
            find_literal_cast_mismatch("SELECT 1 FROM t WHERE id = 42"),
            None
        );
        assert_eq!(
            find_literal_cast_mismatch("SELECT 'hello' FROM t"),
            None
        );
    }

    /// `$N::TYPE` is a placeholder cast — NOT classified as a literal.
    /// The V1 + COMPAT `$N` validator owns this case at Bind time;
    /// classifying it here would double-report.
    #[test]
    fn literal_dollar_param_cast_not_classified_as_literal() {
        assert_eq!(find_literal_cast_mismatch("SELECT $1::int8"), None);
        assert_eq!(
            find_literal_cast_mismatch("WHERE id = $1::int8 AND n = $2::text"),
            None
        );
        // Even a "would-be-mismatch" param ($1 declared param OID is
        // unknown to this helper) doesn't fire — the lookback returns
        // None because `$` is the byte before the digits.
        assert_eq!(
            find_literal_cast_mismatch("SELECT $1::bool"),
            None
        );
    }

    /// Mixed shape — `'hello'::int8 AND $1::text`. The string-literal
    /// mismatch fires FIRST (before the `$N` validator runs at Bind
    /// time). First-mismatch-wins ordering matches the V1 + COMPAT
    /// `$N` validator's behaviour.
    #[test]
    fn literal_mismatch_first_wins_over_dollar_param() {
        let m = find_literal_cast_mismatch(
            "SELECT * FROM t WHERE n = 'hello'::int8 AND id = $1::text",
        )
        .expect("expected literal mismatch to fire before $N");
        assert_eq!(m.literal_oid, crate::proto::PG_TYPE_TEXT);
        assert_eq!(m.cast_oid, crate::proto::PG_TYPE_INT8);
    }

    /// String with PG-doubled-quote escape (`'O''Reilly'`) — still
    /// classifies as TEXT. The `''` doesn't terminate the string
    /// scanner per PG §4.1.2.1.
    #[test]
    fn literal_string_with_doubled_quote_classified_as_text() {
        let m = find_literal_cast_mismatch("SELECT 'O''Reilly'::int8")
            .expect("doubled-quote string is still TEXT");
        assert_eq!(m.literal_oid, crate::proto::PG_TYPE_TEXT);
        assert_eq!(m.cast_oid, crate::proto::PG_TYPE_INT8);
    }

    /// String literal cast INSIDE a string is NOT a real cast — the
    /// scanner skips the entire literal. Locked here because a refactor
    /// that drops the string-context handling would silently start
    /// rejecting safe queries.
    #[test]
    fn literal_cast_inside_string_not_detected() {
        // The entire `SELECT '$1::int8 inside'` is one TEXT literal —
        // no `::` appears outside the string.
        assert_eq!(
            find_literal_cast_mismatch("SELECT '$1::int8 inside'"),
            None
        );
        // And cast text inside a string literal followed by a real
        // outside cast: the outside one classifies the closing `'`
        // as a TEXT literal natural type.
        let m = find_literal_cast_mismatch("SELECT 'a::b'::int8")
            .expect("outside cast on a TEXT literal");
        assert_eq!(m.literal_oid, crate::proto::PG_TYPE_TEXT);
        assert_eq!(m.cast_oid, crate::proto::PG_TYPE_INT8);
    }

    /// Big integer (> i32::MAX) classifies as INT8 — same 'N' category
    /// as INT8 cast, so no mismatch.
    #[test]
    fn literal_big_integer_classifies_as_int8_and_accepts_int8_cast() {
        // 3_000_000_000 > i32::MAX = 2_147_483_647.
        assert_eq!(
            find_literal_cast_mismatch("SELECT 3000000000::int8"),
            None
        );
        // And big-int into TEXT cast still rejects (cross-category).
        let m =
            find_literal_cast_mismatch("SELECT 3000000000::text")
                .expect("big int into text rejects");
        assert_eq!(m.literal_oid, crate::proto::PG_TYPE_INT8);
        assert_eq!(m.cast_oid, crate::proto::PG_TYPE_TEXT);
    }

    /// Unknown type name (e.g. `weirdtype`) yields no mismatch — same
    /// V1 "fall back to strip + hope for unknown types" decision the
    /// `$N` validator made.
    #[test]
    fn literal_unknown_type_name_falls_through() {
        assert_eq!(
            find_literal_cast_mismatch("SELECT 'hello'::weirdtype"),
            None
        );
    }

    /// Comment context — cast-like text inside `--` or `/* */` does
    /// NOT trigger the validator.
    #[test]
    fn literal_cast_inside_comment_not_detected() {
        assert_eq!(
            find_literal_cast_mismatch("-- 'hello'::int8 in line comment\nSELECT 1"),
            None
        );
        assert_eq!(
            find_literal_cast_mismatch("/* 'hello'::int8 in block */ SELECT 1"),
            None
        );
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-SQL-QUOTED-IDENT — the cast scanners must skip double-quoted
    // delimited identifier regions (Django double-quotes every
    // identifier). A `::` or `'` INSIDE a quoted identifier must not be
    // mis-parsed as a cast operator / string-literal boundary.
    // ───────────────────────────────────────────────────────────────────

    /// The Django statement shape is pure passthrough (no real casts) —
    /// the quoted identifiers survive byte-for-byte.
    #[test]
    fn strip_quoted_django_select_passthrough() {
        let sql = r#"SELECT "smokeapp_author"."id", "smokeapp_author"."name" FROM "smokeapp_author""#;
        assert_eq!(strip_pg_casts(sql), sql);
    }

    /// A `::cast` INSIDE a quoted identifier is NOT stripped (the
    /// identifier `"a::b"` is a single delimited name).
    #[test]
    fn strip_cast_text_inside_quoted_ident_preserved() {
        let sql = r#"SELECT "a::b" FROM t"#;
        assert_eq!(strip_pg_casts(sql), sql);
    }

    /// A real `::cast` on a quoted COLUMN reference still strips, leaving
    /// the quoted identifier intact.
    #[test]
    fn strip_real_cast_on_quoted_column() {
        assert_eq!(
            strip_pg_casts(r#"SELECT "id"::int8 FROM "t""#),
            r#"SELECT "id" FROM "t""#
        );
    }

    /// A single-quote INSIDE a quoted identifier (`"O'Brien"`) does NOT
    /// open a string literal — without quote-skipping the scanner would
    /// mis-pair on the `'` and swallow the rest of the SQL.
    #[test]
    fn strip_single_quote_inside_quoted_ident_safe() {
        let sql = r#"SELECT "O'Brien" FROM t WHERE x = 1"#;
        assert_eq!(strip_pg_casts(sql), sql);
    }

    /// Doubled `""` escape inside a quoted identifier is handled (the
    /// region scanner stays inside the identifier across the `""`).
    #[test]
    fn strip_doubled_quote_escape_in_quoted_ident() {
        let sql = r#"SELECT "a""b"::int8 FROM t"#;
        assert_eq!(strip_pg_casts(sql), r#"SELECT "a""b" FROM t"#);
    }

    /// The literal-cast validator must NOT classify a `::` immediately
    /// after a quoted identifier as a TEXT-literal cast — a quoted column
    /// ref carries the column's type, not a literal.
    #[test]
    fn literal_validator_skips_quoted_ident_cast() {
        // `"id"::int8` casts a column ref — no literal mismatch.
        assert_eq!(
            find_literal_cast_mismatch(r#"SELECT "id"::int8 FROM "t""#),
            None
        );
        // A `'` inside a quoted identifier doesn't open a string literal.
        assert_eq!(
            find_literal_cast_mismatch(r#"SELECT "O'Brien"::text FROM t"#),
            None
        );
    }
}
