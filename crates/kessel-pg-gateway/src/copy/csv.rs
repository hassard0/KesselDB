//! SP-PG-COPY-CSV — CSV-format COPY row codec.
//!
//! PG §SQL-COPY "CSV Format" + `src/backend/commands/copyfromparse.c`
//! + RFC 4180:
//!
//! - Default delimiter `,`, default quote `"`, default escape = quote.
//! - Empty unquoted field = NULL (unless a custom `NULL '...'` marker
//!   was provided, in which case ONLY a field byte-equal to the marker
//!   is NULL).
//! - Empty quoted field (`""`) = empty string — DISTINCT from NULL.
//! - Embedded delimiter / quote / newline inside a field → the field
//!   must be quoted.
//! - Inside a quoted field, the quote char is escaped by DOUBLING
//!   (`""`), or by the configured `ESCAPE` char if it differs from
//!   `QUOTE`.
//! - Newlines (`\n` or `\r\n`) inside a quoted field are part of the
//!   value — a single CSV record can span multiple physical lines.
//! - Backslashes are NOT escape characters in CSV (unlike text
//!   format). A literal backslash is just a backslash.
//!
//! Two surfaces:
//!
//! - `parse_csv_record(bytes, pos, options)` — try to parse one CSV
//!   record starting at `pos`. Returns `Ok(Some((fields, next_pos)))`
//!   on a complete record, `Ok(None)` if `bytes` contains only a
//!   partial record (need more data → save to carry buffer),
//!   `Err(...)` on a malformed record.
//! - `encode_csv_record(values, options)` — encode a row into bytes
//!   INCLUDING the trailing `\n`.
//!
//! Hand-rolled — no `csv` crate dependency (preserves the SP-PG-COPY
//! no-extra-deps invariant).

#![forbid(unsafe_code)]
#![allow(dead_code)]

/// CSV codec options — PG-canonical defaults match the
/// `WITH (FORMAT csv)` no-other-options shape.
///
/// Spec §2.3 defaults table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvOptions {
    /// Field separator. PG default `,`. Single byte (PG enforces).
    pub delimiter: u8,
    /// Quote character. PG default `"`. Single byte.
    pub quote: u8,
    /// Escape character inside a quoted field. PG default = quote
    /// (i.e. `""` doubled-quote escape). Single byte.
    pub escape: u8,
    /// NULL marker string. PG default is empty (unquoted empty field
    /// = NULL). Custom marker (e.g. `"NULL"`) means ONLY a field
    /// byte-equal to the marker decodes as NULL.
    pub null_marker: String,
    /// HEADER mode. On input: first record consumed as header (and
    /// discarded by the dispatcher). On output: first record emitted
    /// contains column names.
    pub header: bool,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: b'"',
            escape: b'"',
            null_marker: String::new(),
            header: false,
        }
    }
}

/// Errors `parse_csv_record` can return. All map at the caller to
/// PG SQLSTATE `22023 invalid_parameter_value`.
#[derive(Debug, PartialEq, Eq)]
pub enum CsvParseError {
    /// A quoted field opened with the quote char never closed before
    /// end-of-input. Distinct from "need more data" — the caller
    /// detects need-more-data by `Ok(None)` (when the bytes might
    /// still contain a closing quote in the next CopyData frame).
    /// `UnterminatedQuote` is the hard-error variant returned at the
    /// finalize boundary (CopyDone) when the carry buffer still has
    /// an open quote.
    UnterminatedQuote,
    /// The field count in this record didn't match the expected
    /// column count.
    FieldCountMismatch { expected: usize, actual: usize },
    /// A trailing escape char with nothing following inside a quoted
    /// field (e.g. `"foo\` if backslash is the configured escape).
    /// V1 only emits this when `ESCAPE != QUOTE` since the default
    /// `""` escape can't trail.
    TrailingEscape,
    /// Field bytes weren't valid UTF-8. PG only enforces this when
    /// the connection's `client_encoding` is UTF-8; V1 always enforces
    /// (V2 `SP-PG-COPY-CSV-ENCODING` widens).
    NotUtf8,
}

/// Try to parse one CSV record starting at `pos`. Returns:
///
/// - `Ok(Some((fields, next_pos)))` if a complete record was parsed.
///   `next_pos` is the byte offset of the START of the next record
///   (one past the trailing `\n` or `\r\n` — or `bytes.len()` if the
///   record ended at end-of-input without a trailing newline).
/// - `Ok(None)` if the bytes contain only a partial record (need
///   more data — caller saves trailing bytes from `pos` onward into
///   the carry buffer).
/// - `Err(...)` on a hard-malformed record.
///
/// `expected_cols`: enforce a field count. `0` disables the check
/// (used by the header-row parse).
pub fn parse_csv_record(
    bytes: &[u8],
    pos: usize,
    options: &CsvOptions,
    expected_cols: usize,
) -> Result<Option<(Vec<Option<String>>, usize)>, CsvParseError> {
    let mut fields: Vec<Option<String>> = Vec::with_capacity(expected_cols.max(1));
    let mut current: Vec<u8> = Vec::new();
    let mut i = pos;
    let mut at_field_start = true;
    let mut is_quoted = false;
    let len = bytes.len();
    // True iff the current field has SEEN any quote chars (used to
    // distinguish empty-quoted "" from empty-unquoted "").
    let mut quoted_started = false;

    if i >= len {
        // Nothing to parse.
        return Ok(None);
    }

    loop {
        if i >= len {
            // End of input. If we're inside a quoted field, this is
            // a partial record — caller must wait for more bytes.
            if is_quoted {
                return Ok(None);
            }
            // Unterminated last record (no trailing \n). Emit it as
            // the final record (PG accepts no-trailing-newline on the
            // last row).
            push_field(&mut fields, &mut current, at_field_start, quoted_started, options);
            return finalize_record(fields, i, expected_cols);
        }

        let b = bytes[i];

        if is_quoted {
            // Inside a quoted field.
            if b == options.escape && options.escape == options.quote {
                // Standard case: ESCAPE == QUOTE. `""` is the doubled
                // escape. Peek the next byte.
                if i + 1 < len && bytes[i + 1] == options.quote {
                    current.push(options.quote);
                    i += 2;
                    continue;
                }
                // Lone quote = end of quoted segment.
                is_quoted = false;
                i += 1;
                continue;
            }
            if b == options.escape && options.escape != options.quote {
                // Distinct escape char. The next byte is consumed
                // verbatim into the field.
                if i + 1 >= len {
                    return Ok(None); // need more data
                }
                current.push(bytes[i + 1]);
                i += 2;
                continue;
            }
            if b == options.quote {
                // Closing quote (the escape != quote case above
                // already handled doubled-quote).
                is_quoted = false;
                i += 1;
                continue;
            }
            // Any other byte (including \n, \r, delimiter) is part of
            // the field value when inside a quoted segment.
            current.push(b);
            i += 1;
            continue;
        }

        // Not inside a quoted field.
        if b == options.delimiter {
            push_field(&mut fields, &mut current, at_field_start, quoted_started, options);
            at_field_start = true;
            quoted_started = false;
            i += 1;
            continue;
        }
        if b == b'\n' {
            push_field(&mut fields, &mut current, at_field_start, quoted_started, options);
            return finalize_record(fields, i + 1, expected_cols);
        }
        if b == b'\r' {
            // Handle \r\n by skipping the \r and treating the \n as
            // the record terminator. A bare \r (no following \n) is
            // also a record terminator per RFC 4180 — PG accepts both.
            push_field(&mut fields, &mut current, at_field_start, quoted_started, options);
            let next_pos = if i + 1 < len && bytes[i + 1] == b'\n' {
                i + 2
            } else {
                i + 1
            };
            return finalize_record(fields, next_pos, expected_cols);
        }
        if b == options.quote && at_field_start {
            // Opening quote of a quoted field. Discard any leading
            // whitespace bytes between delimiter and quote — PG is
            // strict here; V1 follows.
            is_quoted = true;
            quoted_started = true;
            at_field_start = false;
            i += 1;
            continue;
        }
        // Regular byte — append to field.
        current.push(b);
        at_field_start = false;
        i += 1;
    }
}

/// Push the current field bytes into `fields`. Distinguishes NULL
/// (unquoted empty + matching null_marker) from empty-string (quoted
/// empty).
fn push_field(
    fields: &mut Vec<Option<String>>,
    current: &mut Vec<u8>,
    at_field_start: bool,
    quoted_started: bool,
    options: &CsvOptions,
) {
    let bytes = std::mem::take(current);
    if !quoted_started {
        // Unquoted field. Empty = NULL when null_marker is empty;
        // otherwise check the marker.
        if options.null_marker.is_empty() {
            if bytes.is_empty() && at_field_start {
                fields.push(None);
                return;
            }
        } else if bytes == options.null_marker.as_bytes() {
            fields.push(None);
            return;
        }
        // Non-NULL — push as Some(String) (lossy on non-UTF-8; the
        // caller can detect via a follow-up check if needed).
        let s = String::from_utf8(bytes).unwrap_or_else(|e| {
            // Replace invalid bytes with U+FFFD — V1 stance. A future
            // V2 might surface NotUtf8 as a hard error.
            String::from_utf8_lossy(e.as_bytes()).into_owned()
        });
        fields.push(Some(s));
    } else {
        // Quoted field. Always Some(String); empty quoted = "".
        let s = String::from_utf8(bytes).unwrap_or_else(|e| {
            String::from_utf8_lossy(e.as_bytes()).into_owned()
        });
        fields.push(Some(s));
    }
}

fn finalize_record(
    fields: Vec<Option<String>>,
    next_pos: usize,
    expected_cols: usize,
) -> Result<Option<(Vec<Option<String>>, usize)>, CsvParseError> {
    if expected_cols > 0 && fields.len() != expected_cols {
        return Err(CsvParseError::FieldCountMismatch {
            expected: expected_cols,
            actual: fields.len(),
        });
    }
    Ok(Some((fields, next_pos)))
}

/// Encode one CSV record. Returns bytes INCLUDING the trailing `\n`.
///
/// Per PG semantics, a value is quoted iff it contains the delimiter,
/// the quote char, `\n`, or `\r`. NULL renders as the null_marker
/// (default empty). A non-NULL value byte-equal to the null_marker is
/// FORCE-quoted so it doesn't round-trip as NULL.
pub fn encode_csv_record(values: &[Option<&str>], options: &CsvOptions) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::with_capacity(64);
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(options.delimiter);
        }
        match v {
            None => {
                // NULL → null_marker (default empty unquoted).
                out.extend_from_slice(options.null_marker.as_bytes());
            }
            Some(s) => {
                let needs_quote = field_needs_quoting(s.as_bytes(), options);
                if needs_quote {
                    out.push(options.quote);
                    for &b in s.as_bytes() {
                        if b == options.quote {
                            // Doubled escape (or custom ESCAPE char).
                            out.push(options.escape);
                            out.push(options.quote);
                        } else if b == options.escape && options.escape != options.quote {
                            // When escape != quote, the escape char
                            // itself must be escaped inside the field.
                            out.push(options.escape);
                            out.push(b);
                        } else {
                            out.push(b);
                        }
                    }
                    out.push(options.quote);
                } else {
                    out.extend_from_slice(s.as_bytes());
                }
            }
        }
    }
    out.push(b'\n');
    out
}

fn field_needs_quoting(bytes: &[u8], options: &CsvOptions) -> bool {
    if bytes.is_empty() {
        // Empty Some("") — must be quoted to distinguish from NULL
        // when null_marker is empty (the default). When null_marker
        // is non-empty (e.g. "NULL"), an empty Some("") is unambiguous
        // unquoted, so no quoting needed.
        return options.null_marker.is_empty();
    }
    // Force-quote if value matches the null_marker (so it doesn't
    // round-trip as NULL on the other side).
    if !options.null_marker.is_empty() && bytes == options.null_marker.as_bytes() {
        return true;
    }
    for &b in bytes {
        if b == options.delimiter || b == options.quote || b == b'\n' || b == b'\r' {
            return true;
        }
    }
    false
}

// ─── SP-PG-COPY-CSV-NUMERIC (2026-06-02) ────────────────────────────
//
// Canonical-form validator for the text/CSV NUMERIC column type
// (PG OID 1700). PG's text/CSV NUMERIC representation is the bare
// decimal string the `numeric_out` function emits — `42`,
// `12345.6789`, `-3.14`, `0.0001`. PG 14+ also accepts the special
// values `NaN`, `Infinity`, `-Infinity` case-insensitively, and
// normalises to the canonical mixed-case form. This validator
// covers both surfaces so text + CSV COPY into a NUMERIC column
// accepts the full grammar without dropping garbage through to the
// kessel-sql layer (which would surface a confusing generic
// parse_error).
//
// Companion design spec:
// `docs/superpowers/specs/2026-06-02-kesseldb-sppgcopycsvnumeric-design.md`
//
// Scope: V1 covers finite decimals + NaN + ±Infinity. V2
// SP-PG-COPY-CSV-NUMERIC-SCI (2026-06-02) lifts the
// scientific-notation rejection by parsing the mantissa+exponent
// and expanding into the canonical decimal text. Arbitrary-
// precision values beyond the kessel-sql i128 cap surface at INSERT
// time (`SP-PG-COPY-NUMERIC-BIGNUM`).
//
// Companion SCI design spec:
// `docs/superpowers/specs/2026-06-02-kesseldb-sppgcopycsvnumericsci-design.md`

/// Errors `validate_numeric_text` can return. All map at the caller
/// to PG SQLSTATE `22P02 invalid_text_representation`.
#[derive(Debug, PartialEq, Eq)]
pub enum CsvNumericError {
    /// Empty string (or all-whitespace after trim).
    Empty,
    /// Non-decimal byte at the given (0-based) byte position.
    BadByte { position: usize, byte: u8 },
    /// Structural malformation (multiple decimal points, multiple
    /// signs, sign-without-digits, …). `reason` is a static phrase
    /// suitable for inclusion in the user-facing message.
    Malformed { reason: &'static str },
    /// Scientific notation rejected — V1 SP-PG-COPY-CSV-NUMERIC
    /// reserved this variant for V2 SP-PG-COPY-CSV-NUMERIC-SCI to
    /// surface in the rejection message. After SCI V1 landed
    /// (2026-06-02), well-formed scientific notation expands to
    /// canonical decimal text and this variant is unreachable from
    /// `validate_numeric_text` — preserved for back-compat with any
    /// downstream pattern-match. Malformed scientific input surfaces
    /// as `Malformed { reason }` (out-of-range exponent, missing
    /// exponent, etc.) or `BadByte` (non-digit in exponent).
    ScientificNotation,
}

/// SP-PG-COPY-CSV-NUMERIC T1 — validate a text/CSV NUMERIC field's
/// contents and return the canonical PG form.
///
/// **Accepted shapes:**
/// - Finite decimals: `[+-]?(\d+(\.\d*)?|\.\d+)`. Sign normalised
///   (`+42` → `42`); leading-dot (`.5`) and trailing-dot (`5.`)
///   tolerated per PG; leading zeros preserved verbatim (the
///   downstream kessel-sql parser normalises).
/// - Case-insensitive specials: `NaN` / `Infinity` / `-Infinity` —
///   accepted in any case (`nan`, `INF`, `+inf`, `-Infinity`, …) and
///   returned in the canonical PG mixed-case form.
/// - Scientific notation (SP-PG-COPY-CSV-NUMERIC-SCI V1, 2026-06-02):
///   `[+-]?(\d+(\.\d+)?|\.\d+)[eE][+-]?\d+` — mantissa + signed
///   integer exponent (`1e10`, `1.5E-3`, `6.022e+23`, `-3.14e2`,
///   `.5e2`). The exponent is expanded into the canonical decimal
///   text by shifting the mantissa's decimal point. Exponents with
///   `|exp| > 100` reject as Malformed("exponent out of range") to
///   avoid pathological digit-string allocation.
///
/// **Returns:** the canonical-form string. For finite values
/// (decimal or scientific): the sign-normalised expanded decimal
/// text. For specials: one of the three canonical strings.
pub fn validate_numeric_text(s: &str) -> Result<String, CsvNumericError> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(CsvNumericError::Empty);
    }
    // ── Special-string preamble (case-insensitive) ──────────────
    let lower = trimmed.to_ascii_lowercase();
    match lower.as_str() {
        "nan" => return Ok("NaN".to_string()),
        "infinity" | "+infinity" | "inf" | "+inf" => {
            return Ok("Infinity".to_string());
        }
        "-infinity" | "-inf" => return Ok("-Infinity".to_string()),
        _ => {}
    }
    // ── SP-PG-COPY-CSV-NUMERIC-SCI V1 (2026-06-02) ──────────────
    // Try the scientific-notation branch first. Returns:
    //  - Ok(Some(canonical)) if the input matches the scientific
    //    grammar and the exponent shift produced a valid decimal.
    //  - Ok(None) if the input doesn't contain e/E at all (fall
    //    through to the canonical-decimal grammar below).
    //  - Err(...) if the input contains e/E but is malformed.
    if let Some(canonical) = parse_scientific_notation(trimmed)? {
        return Ok(canonical);
    }
    // ── Finite decimal grammar ───────────────────────────────────
    let bytes = trimmed.as_bytes();
    let mut i = 0usize;
    let mut sign = b'+';
    if bytes[0] == b'+' || bytes[0] == b'-' {
        sign = bytes[0];
        i += 1;
        if i >= bytes.len() {
            return Err(CsvNumericError::Malformed {
                reason: "sign without digits",
            });
        }
    }
    let digits_start = i;
    let mut saw_int_digit = false;
    let mut saw_dot = false;
    let mut saw_frac_digit = false;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'0'..=b'9' => {
                if saw_dot {
                    saw_frac_digit = true;
                } else {
                    saw_int_digit = true;
                }
                i += 1;
            }
            b'.' => {
                if saw_dot {
                    return Err(CsvNumericError::Malformed {
                        reason: "multiple decimal points",
                    });
                }
                saw_dot = true;
                i += 1;
            }
            b'+' | b'-' => {
                return Err(CsvNumericError::Malformed {
                    reason: "multiple signs",
                });
            }
            b'e' | b'E' => {
                // Unreachable in practice — parse_scientific_notation
                // above swallows every e/E-bearing input. Kept as a
                // defensive default.
                return Err(CsvNumericError::ScientificNotation);
            }
            other => {
                return Err(CsvNumericError::BadByte {
                    position: i,
                    byte: other,
                });
            }
        }
    }
    if !saw_int_digit && !saw_frac_digit {
        return Err(CsvNumericError::Malformed {
            reason: "no digits",
        });
    }
    // Canonical form: drop a leading '+'; keep a leading '-' iff the
    // value isn't all-zeros (so -0 becomes 0 — PG's numeric_out
    // canonical form has no negative zero).
    let digits_str = &trimmed[digits_start..];
    let is_all_zero = digits_str
        .as_bytes()
        .iter()
        .all(|&b| b == b'0' || b == b'.');
    let prefix = if sign == b'-' && !is_all_zero { "-" } else { "" };
    Ok(format!("{prefix}{digits_str}"))
}

/// SP-PG-COPY-CSV-NUMERIC-SCI V1 (2026-06-02) — try to parse `s` as
/// scientific notation and return the canonical decimal form.
///
/// Returns:
/// - `Ok(Some(canonical))` if `s` matches the scientific grammar
///   AND the exponent shift produced a valid decimal.
/// - `Ok(None)` if `s` doesn't contain `e`/`E` at all (caller falls
///   through to the canonical-decimal grammar).
/// - `Err(CsvNumericError)` if `s` contains `e`/`E` but the mantissa
///   or exponent is malformed.
///
/// Grammar (after the special-string preamble has already handled
/// NaN/Inf):
///   mantissa ::= [+-]?(\d+(\.\d+)?|\.\d+)
///   exponent ::= [+-]?\d+
///   sci      ::= mantissa [eE] exponent
///
/// The expansion algorithm shifts the mantissa's implicit decimal
/// point by `exp - (mantissa_frac_digits)`. See design spec §4.
fn parse_scientific_notation(s: &str) -> Result<Option<String>, CsvNumericError> {
    // Fast-path: no e/E means "not scientific" — fall through.
    let e_pos = match s.bytes().position(|b| b == b'e' || b == b'E') {
        Some(p) => p,
        None => return Ok(None),
    };
    // Hard-reject a second `e`/`E` anywhere in the tail.
    if s.bytes().skip(e_pos + 1).any(|b| b == b'e' || b == b'E') {
        return Err(CsvNumericError::Malformed {
            reason: "multiple exponent markers",
        });
    }

    let mantissa = &s[..e_pos];
    let exp_str = &s[e_pos + 1..];

    if mantissa.is_empty() {
        // Bare `e10` — no mantissa.
        return Err(CsvNumericError::BadByte {
            position: 0,
            byte: s.as_bytes()[0],
        });
    }
    if exp_str.is_empty() {
        return Err(CsvNumericError::Malformed {
            reason: "missing exponent",
        });
    }

    // ── Parse exponent ────────────────────────────────────────
    // Allow optional leading sign + ASCII digits only.
    let exp_bytes = exp_str.as_bytes();
    let (exp_sign, exp_digits_start) = match exp_bytes[0] {
        b'+' => (1i32, 1usize),
        b'-' => (-1i32, 1usize),
        b'0'..=b'9' => (1i32, 0usize),
        _ => {
            return Err(CsvNumericError::Malformed {
                reason: "malformed exponent",
            });
        }
    };
    if exp_digits_start >= exp_bytes.len() {
        return Err(CsvNumericError::Malformed {
            reason: "malformed exponent",
        });
    }
    let mut exp_val: i32 = 0;
    for &b in &exp_bytes[exp_digits_start..] {
        match b {
            b'0'..=b'9' => {
                let d = (b - b'0') as i32;
                // Saturate-detect by comparison against the cap so a
                // pathological `1e9999999999` rejects cleanly.
                if exp_val > 1_000 {
                    return Err(CsvNumericError::Malformed {
                        reason: "exponent out of range",
                    });
                }
                exp_val = exp_val * 10 + d;
            }
            b'+' | b'-' => {
                return Err(CsvNumericError::Malformed {
                    reason: "malformed exponent",
                });
            }
            b'.' => {
                return Err(CsvNumericError::Malformed {
                    reason: "non-integer exponent",
                });
            }
            _ => {
                return Err(CsvNumericError::Malformed {
                    reason: "malformed exponent",
                });
            }
        }
    }
    let exp_signed = exp_sign * exp_val;
    if exp_signed.abs() > 100 {
        return Err(CsvNumericError::Malformed {
            reason: "exponent out of range",
        });
    }

    // ── Parse mantissa ────────────────────────────────────────
    // Grammar: [+-]?(\d+(\.\d+)?|\.\d+).
    // Trailing-dot (`5.`) is out-of-scope (SP-PG-COPY-CSV-NUMERIC-SCI-TRAILDOT).
    let mbytes = mantissa.as_bytes();
    let (mant_sign, mant_body_start) = match mbytes[0] {
        b'+' => ('+', 1usize),
        b'-' => ('-', 1usize),
        _ => ('+', 0usize),
    };
    if mant_body_start >= mbytes.len() {
        return Err(CsvNumericError::Malformed {
            reason: "sign without digits",
        });
    }
    let body = &mbytes[mant_body_start..];

    // Walk the body — count int digits + frac digits + position of dot.
    let mut int_digits: Vec<u8> = Vec::new();
    let mut frac_digits: Vec<u8> = Vec::new();
    let mut saw_dot = false;
    for (off, &b) in body.iter().enumerate() {
        match b {
            b'0'..=b'9' => {
                if saw_dot {
                    frac_digits.push(b);
                } else {
                    int_digits.push(b);
                }
            }
            b'.' => {
                if saw_dot {
                    return Err(CsvNumericError::Malformed {
                        reason: "multiple decimal points",
                    });
                }
                saw_dot = true;
            }
            b'+' | b'-' => {
                return Err(CsvNumericError::Malformed {
                    reason: "multiple signs",
                });
            }
            other => {
                return Err(CsvNumericError::BadByte {
                    position: mant_body_start + off,
                    byte: other,
                });
            }
        }
    }
    if int_digits.is_empty() && frac_digits.is_empty() {
        return Err(CsvNumericError::Malformed {
            reason: "no digits",
        });
    }
    // Trailing-dot mantissa (`5.` with no fractional digits) is the
    // V2 SP-PG-COPY-CSV-NUMERIC-SCI-TRAILDOT shape — out of scope.
    if saw_dot && frac_digits.is_empty() {
        return Err(CsvNumericError::Malformed {
            reason:
                "trailing-dot mantissa in scientific notation not supported in V1 (SP-PG-COPY-CSV-NUMERIC-SCI-TRAILDOT)",
        });
    }

    // ── Expand ───────────────────────────────────────────────
    // D = concat(int_digits, frac_digits) — the integer value of
    // the mantissa scaled by 10^frac_digits.len(). The decimal point
    // in the final value is exp_signed - frac_digits.len() places to
    // the right of D's tail. Equivalently: shift D's implicit
    // decimal point K places where K = exp_signed - frac_digits.len().
    let mut all_digits: Vec<u8> = Vec::with_capacity(int_digits.len() + frac_digits.len());
    all_digits.extend_from_slice(&int_digits);
    all_digits.extend_from_slice(&frac_digits);
    let k: i32 = exp_signed - (frac_digits.len() as i32);

    // Build canonical digit string with the dot in the right place.
    // body_canonical = render(all_digits, k).
    let body_canonical = render_shifted(&all_digits, k);

    // ── Sign canonicalisation ────────────────────────────────
    // Drop a leading '+'; keep '-' iff value isn't all-zeros (so
    // `-0e0` canonicalises to `0`, matching V1).
    let all_zero = body_canonical
        .as_bytes()
        .iter()
        .all(|&b| b == b'0' || b == b'.');
    let prefix = if mant_sign == '-' && !all_zero {
        "-"
    } else {
        ""
    };
    Ok(Some(format!("{prefix}{body_canonical}")))
}

/// Render `digits` as a decimal string with the implicit decimal
/// point shifted `k` places to the right (positive `k` appends zeros;
/// negative `k` inserts a dot, padding with leading zeros if needed).
///
/// Leading-zero suppression: the integer part is trimmed to its
/// canonical form (`0` for a pure-zero integer; otherwise no leading
/// zeros). The fractional part is preserved verbatim.
fn render_shifted(digits: &[u8], k: i32) -> String {
    if digits.is_empty() {
        return "0".to_string();
    }
    if k >= 0 {
        // Append k zeros — pure integer result.
        let mut out: Vec<u8> = Vec::with_capacity(digits.len() + k as usize);
        out.extend_from_slice(digits);
        for _ in 0..k {
            out.push(b'0');
        }
        // Strip leading zeros (preserve a single 0).
        let trimmed = strip_leading_zeros(&out);
        return String::from_utf8(trimmed.to_vec()).expect("ASCII digits");
    }
    // k < 0: insert a dot |k| places from the right.
    let shift = (-k) as usize;
    if shift < digits.len() {
        let split = digits.len() - shift;
        let int_part = strip_leading_zeros(&digits[..split]);
        let frac_part = &digits[split..];
        let mut out: Vec<u8> = Vec::with_capacity(int_part.len() + 1 + frac_part.len());
        out.extend_from_slice(int_part);
        out.push(b'.');
        out.extend_from_slice(frac_part);
        return String::from_utf8(out).expect("ASCII digits");
    }
    // shift >= digits.len(): result is 0.<pad><digits>
    let pad = shift - digits.len();
    let mut out: Vec<u8> = Vec::with_capacity(2 + pad + digits.len());
    out.extend_from_slice(b"0.");
    for _ in 0..pad {
        out.push(b'0');
    }
    out.extend_from_slice(digits);
    String::from_utf8(out).expect("ASCII digits")
}

/// Strip leading ASCII `0` bytes from the slice; preserve a single
/// `0` if the result would otherwise be empty.
fn strip_leading_zeros(d: &[u8]) -> &[u8] {
    let mut i = 0;
    while i + 1 < d.len() && d[i] == b'0' {
        i += 1;
    }
    &d[i..]
}

/// Validate a CSV option's value is exactly one byte. Used by the
/// `command::parse_with_options` extension to surface a clean error
/// when the operator wrote e.g. `DELIMITER '||'`.
pub fn validate_single_byte(name: &str, value: &str) -> Result<u8, String> {
    let bytes = value.as_bytes();
    if bytes.len() != 1 {
        return Err(format!(
            "COPY csv {name} must be a single character (got {} bytes)",
            bytes.len()
        ));
    }
    Ok(bytes[0])
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── parse_csv_record ───────────────────────────────────────────────

    /// SP-PG-COPY-CSV T1: a basic three-field unquoted record.
    #[test]
    fn t1_csv_parse_basic_three_unquoted_fields() {
        let opt = CsvOptions::default();
        let bytes = b"1,hello,world\n";
        let (fields, next_pos) = parse_csv_record(bytes, 0, &opt, 3).unwrap().unwrap();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0], Some("1".to_string()));
        assert_eq!(fields[1], Some("hello".to_string()));
        assert_eq!(fields[2], Some("world".to_string()));
        assert_eq!(next_pos, bytes.len());
    }

    /// SP-PG-COPY-CSV T1: empty unquoted field = NULL.
    #[test]
    fn t1_csv_parse_empty_unquoted_is_null() {
        let opt = CsvOptions::default();
        let bytes = b"1,,world\n";
        let (fields, _) = parse_csv_record(bytes, 0, &opt, 3).unwrap().unwrap();
        assert_eq!(fields[0], Some("1".to_string()));
        assert_eq!(fields[1], None);
        assert_eq!(fields[2], Some("world".to_string()));
    }

    /// SP-PG-COPY-CSV T1: empty QUOTED field = empty string (distinct
    /// from NULL).
    #[test]
    fn t1_csv_parse_empty_quoted_is_empty_string() {
        let opt = CsvOptions::default();
        let bytes = b"1,\"\",world\n";
        let (fields, _) = parse_csv_record(bytes, 0, &opt, 3).unwrap().unwrap();
        assert_eq!(fields[0], Some("1".to_string()));
        assert_eq!(fields[1], Some("".to_string()));
        assert_eq!(fields[2], Some("world".to_string()));
    }

    /// SP-PG-COPY-CSV T1: quoted field containing the delimiter.
    #[test]
    fn t1_csv_parse_quoted_field_with_delimiter() {
        let opt = CsvOptions::default();
        let bytes = b"1,\"hello, world\",3\n";
        let (fields, _) = parse_csv_record(bytes, 0, &opt, 3).unwrap().unwrap();
        assert_eq!(fields[1], Some("hello, world".to_string()));
    }

    /// SP-PG-COPY-CSV T1: doubled-quote escape inside a quoted field.
    /// `"embedded ""quote"""` → `embedded "quote"`.
    #[test]
    fn t1_csv_parse_doubled_quote_escape() {
        let opt = CsvOptions::default();
        let bytes = b"1,\"embedded \"\"quote\"\"\",3\n";
        let (fields, _) = parse_csv_record(bytes, 0, &opt, 3).unwrap().unwrap();
        assert_eq!(fields[1], Some(r#"embedded "quote""#.to_string()));
    }

    /// SP-PG-COPY-CSV T1: newline inside a quoted field is part of the
    /// value — the record spans multiple physical lines.
    #[test]
    fn t1_csv_parse_newline_in_quoted_field() {
        let opt = CsvOptions::default();
        let bytes = b"1,\"line1\nline2\",3\n";
        let (fields, _) = parse_csv_record(bytes, 0, &opt, 3).unwrap().unwrap();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[1], Some("line1\nline2".to_string()));
    }

    /// SP-PG-COPY-CSV T1: bare newline outside quotes ends the record.
    #[test]
    fn t1_csv_parse_bare_newline_ends_record() {
        let opt = CsvOptions::default();
        let bytes = b"a,b\nc,d\n";
        let (fields, next_pos) = parse_csv_record(bytes, 0, &opt, 2).unwrap().unwrap();
        assert_eq!(fields[0], Some("a".to_string()));
        assert_eq!(fields[1], Some("b".to_string()));
        assert_eq!(next_pos, 4);
        // Parse the second record from next_pos.
        let (fields2, _) = parse_csv_record(bytes, next_pos, &opt, 2).unwrap().unwrap();
        assert_eq!(fields2[0], Some("c".to_string()));
        assert_eq!(fields2[1], Some("d".to_string()));
    }

    /// SP-PG-COPY-CSV T1: \r\n line endings tolerated.
    #[test]
    fn t1_csv_parse_crlf_line_endings() {
        let opt = CsvOptions::default();
        let bytes = b"a,b\r\nc,d\r\n";
        let (fields, next_pos) = parse_csv_record(bytes, 0, &opt, 2).unwrap().unwrap();
        assert_eq!(fields[0], Some("a".to_string()));
        assert_eq!(next_pos, 5);
        let (fields2, _) = parse_csv_record(bytes, next_pos, &opt, 2).unwrap().unwrap();
        assert_eq!(fields2[1], Some("d".to_string()));
    }

    /// SP-PG-COPY-CSV T1: partial record (mid-quoted-field, no closing
    /// quote yet) returns `Ok(None)` so the caller can carry-buffer.
    #[test]
    fn t1_csv_parse_partial_quoted_returns_none() {
        let opt = CsvOptions::default();
        let bytes = b"1,\"hello, wor"; // no closing quote
        let got = parse_csv_record(bytes, 0, &opt, 3).unwrap();
        assert!(got.is_none(), "partial quoted field must return None");
    }

    /// SP-PG-COPY-CSV T1: custom delimiter `;` works.
    #[test]
    fn t1_csv_parse_custom_delimiter_semicolon() {
        let opt = CsvOptions {
            delimiter: b';',
            ..CsvOptions::default()
        };
        let bytes = b"1;hello;world\n";
        let (fields, _) = parse_csv_record(bytes, 0, &opt, 3).unwrap().unwrap();
        assert_eq!(fields[0], Some("1".to_string()));
        assert_eq!(fields[1], Some("hello".to_string()));
        assert_eq!(fields[2], Some("world".to_string()));
    }

    /// SP-PG-COPY-CSV T1: custom NULL marker `NULL` decodes that
    /// literal as None and treats empty unquoted as "" (not NULL).
    #[test]
    fn t1_csv_parse_custom_null_marker() {
        let opt = CsvOptions {
            null_marker: "NULL".to_string(),
            ..CsvOptions::default()
        };
        let bytes = b"1,NULL,3\n";
        let (fields, _) = parse_csv_record(bytes, 0, &opt, 3).unwrap().unwrap();
        assert_eq!(fields[0], Some("1".to_string()));
        assert_eq!(fields[1], None);
        assert_eq!(fields[2], Some("3".to_string()));
    }

    /// SP-PG-COPY-CSV T1: record without trailing newline (last row of
    /// a file) parses cleanly with next_pos == bytes.len().
    #[test]
    fn t1_csv_parse_no_trailing_newline() {
        let opt = CsvOptions::default();
        let bytes = b"a,b,c";
        let (fields, next_pos) = parse_csv_record(bytes, 0, &opt, 3).unwrap().unwrap();
        assert_eq!(fields[0], Some("a".to_string()));
        assert_eq!(next_pos, bytes.len());
    }

    // ─── encode_csv_record ──────────────────────────────────────────────

    /// SP-PG-COPY-CSV T1: encode a basic three-field unquoted record.
    #[test]
    fn t1_csv_encode_basic_three_fields() {
        let opt = CsvOptions::default();
        let values: Vec<Option<&str>> = vec![Some("1"), Some("hello"), Some("world")];
        let bytes = encode_csv_record(&values, &opt);
        assert_eq!(bytes, b"1,hello,world\n");
    }

    /// SP-PG-COPY-CSV T1: encode a value containing a comma forces
    /// quoting.
    #[test]
    fn t1_csv_encode_comma_in_value_quoted() {
        let opt = CsvOptions::default();
        let values: Vec<Option<&str>> = vec![Some("1"), Some("hello, world"), Some("3")];
        let bytes = encode_csv_record(&values, &opt);
        assert_eq!(bytes, b"1,\"hello, world\",3\n");
    }

    /// SP-PG-COPY-CSV T1: encode a value containing the quote char
    /// forces quoting + doubles the embedded quote.
    #[test]
    fn t1_csv_encode_quote_in_value_doubled() {
        let opt = CsvOptions::default();
        let values: Vec<Option<&str>> = vec![Some(r#"embedded "quote""#)];
        let bytes = encode_csv_record(&values, &opt);
        assert_eq!(bytes, b"\"embedded \"\"quote\"\"\"\n");
    }

    /// SP-PG-COPY-CSV T1: embedded newline forces quoting.
    #[test]
    fn t1_csv_encode_newline_forces_quoting() {
        let opt = CsvOptions::default();
        let values: Vec<Option<&str>> = vec![Some("line1\nline2")];
        let bytes = encode_csv_record(&values, &opt);
        assert_eq!(bytes, b"\"line1\nline2\"\n");
    }

    /// SP-PG-COPY-CSV T1: NULL → empty unquoted (default null_marker).
    #[test]
    fn t1_csv_encode_null_default_is_empty_unquoted() {
        let opt = CsvOptions::default();
        let values: Vec<Option<&str>> = vec![Some("a"), None, Some("c")];
        let bytes = encode_csv_record(&values, &opt);
        assert_eq!(bytes, b"a,,c\n");
    }

    /// SP-PG-COPY-CSV T1: empty Some("") encodes as `""` (quoted) so
    /// it round-trips as empty-string, not NULL.
    #[test]
    fn t1_csv_encode_empty_string_is_quoted_empty() {
        let opt = CsvOptions::default();
        let values: Vec<Option<&str>> = vec![Some("a"), Some(""), Some("c")];
        let bytes = encode_csv_record(&values, &opt);
        assert_eq!(bytes, b"a,\"\",c\n");
    }

    /// SP-PG-COPY-CSV T1: custom null_marker `NULL` — NULL renders as
    /// the literal, and a real "NULL" string is force-quoted to
    /// disambiguate.
    #[test]
    fn t1_csv_encode_custom_null_marker_disambiguates() {
        let opt = CsvOptions {
            null_marker: "NULL".to_string(),
            ..CsvOptions::default()
        };
        // NULL → "NULL" (unquoted marker text).
        let v1: Vec<Option<&str>> = vec![Some("a"), None, Some("c")];
        assert_eq!(encode_csv_record(&v1, &opt), b"a,NULL,c\n");
        // A real "NULL" string is force-quoted so it doesn't decode
        // as NULL on the other side.
        let v2: Vec<Option<&str>> = vec![Some("a"), Some("NULL"), Some("c")];
        assert_eq!(encode_csv_record(&v2, &opt), b"a,\"NULL\",c\n");
    }

    // ─── round-trip property ────────────────────────────────────────────

    /// SP-PG-COPY-CSV T1 — round-trip property: a record encoded then
    /// parsed back returns the original fields.
    #[test]
    fn t1_csv_round_trip_property() {
        let opt = CsvOptions::default();
        let originals: Vec<Option<String>> = vec![
            Some("plain".to_string()),
            Some("".to_string()),
            Some("with,comma".to_string()),
            Some(r#"with "quote""#.to_string()),
            Some("with\nnewline".to_string()),
            None,
            Some("unicode café".to_string()),
        ];
        let refs: Vec<Option<&str>> = originals.iter().map(|v| v.as_deref()).collect();
        let encoded = encode_csv_record(&refs, &opt);
        let (parsed, _) = parse_csv_record(&encoded, 0, &opt, originals.len()).unwrap().unwrap();
        assert_eq!(parsed, originals);
    }

    /// SP-PG-COPY-CSV T1 — round-trip with custom delimiter +
    /// custom null marker.
    #[test]
    fn t1_csv_round_trip_custom_options() {
        let opt = CsvOptions {
            delimiter: b';',
            null_marker: "<NA>".to_string(),
            ..CsvOptions::default()
        };
        let originals: Vec<Option<String>> = vec![
            Some("plain".to_string()),
            None,
            Some("has;semicolon".to_string()),
            Some("<NA>".to_string()), // real value matching marker — must round-trip as Some
        ];
        let refs: Vec<Option<&str>> = originals.iter().map(|v| v.as_deref()).collect();
        let encoded = encode_csv_record(&refs, &opt);
        let (parsed, _) = parse_csv_record(&encoded, 0, &opt, originals.len()).unwrap().unwrap();
        assert_eq!(parsed, originals);
    }

    // ─── field-count mismatch ───────────────────────────────────────────

    /// SP-PG-COPY-CSV T1: field-count mismatch surfaces.
    #[test]
    fn t1_csv_parse_field_count_mismatch() {
        let opt = CsvOptions::default();
        let bytes = b"a,b\n";
        match parse_csv_record(bytes, 0, &opt, 3) {
            Err(CsvParseError::FieldCountMismatch { expected, actual }) => {
                assert_eq!(expected, 3);
                assert_eq!(actual, 2);
            }
            other => panic!("expected FieldCountMismatch, got {other:?}"),
        }
    }

    /// SP-PG-COPY-CSV T1: expected_cols=0 disables the check (used by
    /// header parse).
    #[test]
    fn t1_csv_parse_header_row_no_field_count_check() {
        let opt = CsvOptions::default();
        let bytes = b"id,name,email\n";
        let (fields, _) = parse_csv_record(bytes, 0, &opt, 0).unwrap().unwrap();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0], Some("id".to_string()));
    }

    // ─── validate_single_byte ───────────────────────────────────────────

    /// SP-PG-COPY-CSV T1: `validate_single_byte` accepts a 1-byte
    /// string + rejects multi-byte.
    #[test]
    fn t1_csv_validate_single_byte() {
        assert_eq!(validate_single_byte("DELIMITER", "|").unwrap(), b'|');
        assert!(validate_single_byte("DELIMITER", "||").is_err());
        assert!(validate_single_byte("QUOTE", "").is_err());
    }

    // ─── SP-PG-COPY-CSV-NUMERIC validator KATs ──────────────────────────
    //
    // Companion design spec:
    // `docs/superpowers/specs/2026-06-02-kesseldb-sppgcopycsvnumeric-design.md`

    /// SP-PG-COPY-CSV-NUMERIC T1: bare integer.
    #[test]
    fn t1_numeric_validate_bare_integer() {
        assert_eq!(validate_numeric_text("42").unwrap(), "42");
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: signed finite decimal.
    #[test]
    fn t1_numeric_validate_signed_decimal() {
        assert_eq!(validate_numeric_text("-3.14").unwrap(), "-3.14");
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: large finite decimal.
    #[test]
    fn t1_numeric_validate_large_decimal() {
        assert_eq!(validate_numeric_text("12345.6789").unwrap(), "12345.6789");
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: small fractional.
    #[test]
    fn t1_numeric_validate_small_fractional() {
        assert_eq!(validate_numeric_text("0.0001").unwrap(), "0.0001");
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: leading `+` is stripped (sign
    /// normalised to canonical PG form).
    #[test]
    fn t1_numeric_validate_strips_leading_plus() {
        assert_eq!(validate_numeric_text("+42").unwrap(), "42");
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: leading-dot fractional accepted
    /// (PG tolerates this — `.5` ≡ `0.5`).
    #[test]
    fn t1_numeric_validate_leading_dot_fractional() {
        assert_eq!(validate_numeric_text(".5").unwrap(), ".5");
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: trailing-dot accepted (PG tolerates
    /// `5.` as `5.0`).
    #[test]
    fn t1_numeric_validate_trailing_dot() {
        assert_eq!(validate_numeric_text("5.").unwrap(), "5.");
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: NaN case-insensitive accepted
    /// (lowercase form).
    #[test]
    fn t1_numeric_validate_nan_lowercase() {
        assert_eq!(validate_numeric_text("nan").unwrap(), "NaN");
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: NaN canonical mixed-case
    /// round-trips identity.
    #[test]
    fn t1_numeric_validate_nan_canonical() {
        assert_eq!(validate_numeric_text("NaN").unwrap(), "NaN");
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: NaN UPPERCASE accepted.
    #[test]
    fn t1_numeric_validate_nan_uppercase() {
        assert_eq!(validate_numeric_text("NAN").unwrap(), "NaN");
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: Infinity case-insensitive
    /// (lowercase + alias `inf` + `+inf`).
    #[test]
    fn t1_numeric_validate_infinity_variants() {
        assert_eq!(validate_numeric_text("infinity").unwrap(), "Infinity");
        assert_eq!(validate_numeric_text("INFINITY").unwrap(), "Infinity");
        assert_eq!(validate_numeric_text("Infinity").unwrap(), "Infinity");
        assert_eq!(validate_numeric_text("+Infinity").unwrap(), "Infinity");
        assert_eq!(validate_numeric_text("inf").unwrap(), "Infinity");
        assert_eq!(validate_numeric_text("+inf").unwrap(), "Infinity");
        assert_eq!(validate_numeric_text("INF").unwrap(), "Infinity");
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: -Infinity case-insensitive
    /// (lowercase + alias `-inf`).
    #[test]
    fn t1_numeric_validate_neg_infinity_variants() {
        assert_eq!(validate_numeric_text("-infinity").unwrap(), "-Infinity");
        assert_eq!(validate_numeric_text("-Infinity").unwrap(), "-Infinity");
        assert_eq!(validate_numeric_text("-INFINITY").unwrap(), "-Infinity");
        assert_eq!(validate_numeric_text("-inf").unwrap(), "-Infinity");
        assert_eq!(validate_numeric_text("-INF").unwrap(), "-Infinity");
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: garbage rejects.
    ///
    /// Note: post-SP-PG-COPY-CSV-NUMERIC-SCI (2026-06-02), inputs
    /// containing `e`/`E` route through the scientific-notation
    /// branch first. `"hello"` has `e` at position 1, so the SCI
    /// parser interprets `h` as the mantissa and `llo` as a
    /// malformed exponent — surfacing `Malformed { reason:
    /// "malformed exponent" }` instead of the pre-SCI V1 BadByte at
    /// position 0. Both forms map to `22P02
    /// invalid_text_representation` at the dispatcher; the rejection
    /// is still surfaced cleanly. We also assert a pure-digits-with-
    /// garbage input (`"42x"`) still surfaces the original BadByte.
    #[test]
    fn t1_numeric_validate_garbage_rejects() {
        // `hello` now flows through the SCI branch (because `e` is
        // present); SCI surfaces a Malformed exponent error.
        match validate_numeric_text("hello") {
            Err(CsvNumericError::Malformed { reason }) => {
                assert!(
                    reason.contains("exponent") || reason.contains("mantissa"),
                    "reason = {reason}"
                );
            }
            Err(CsvNumericError::BadByte { .. }) => {
                // Acceptable too — equivalent 22P02 at the dispatcher.
            }
            other => panic!("expected rejection for 'hello', got {other:?}"),
        }
        // A pure-non-e garbage input still hits the canonical-decimal
        // BadByte at the original position.
        match validate_numeric_text("42x") {
            Err(CsvNumericError::BadByte { position, byte }) => {
                assert_eq!(position, 2);
                assert_eq!(byte, b'x');
            }
            other => panic!("expected BadByte at 2 for '42x', got {other:?}"),
        }
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: multiple decimal points reject as
    /// Malformed.
    #[test]
    fn t1_numeric_validate_multi_dot_rejects() {
        match validate_numeric_text("1.2.3") {
            Err(CsvNumericError::Malformed { reason }) => {
                assert!(reason.contains("decimal"));
            }
            other => panic!("expected Malformed for multi-dot, got {other:?}"),
        }
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: empty string rejects with Empty.
    #[test]
    fn t1_numeric_validate_empty_rejects() {
        assert_eq!(validate_numeric_text(""), Err(CsvNumericError::Empty));
        // Whitespace-only also rejects as Empty (trim).
        assert_eq!(validate_numeric_text("   "), Err(CsvNumericError::Empty));
    }

    /// SP-PG-COPY-CSV-NUMERIC-SCI T1 (2026-06-02) — V1 rejected
    /// scientific notation; the SCI V2 arc lifted that to canonical
    /// decimal expansion. The original V1 inputs now expand cleanly.
    /// See KATs `t1_sci_*` below for the full grammar coverage.
    #[test]
    fn t1_numeric_validate_scientific_notation_v1_inputs_now_expand() {
        assert_eq!(validate_numeric_text("1e10").unwrap(), "10000000000");
        assert_eq!(validate_numeric_text("2E-3").unwrap(), "0.002");
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: multiple signs (e.g. `--5`) reject
    /// as Malformed.
    #[test]
    fn t1_numeric_validate_multi_sign_rejects() {
        match validate_numeric_text("--5") {
            Err(CsvNumericError::Malformed { reason }) => {
                assert!(reason.contains("sign"));
            }
            other => panic!("expected Malformed for multi-sign, got {other:?}"),
        }
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: sign-without-digits rejects.
    #[test]
    fn t1_numeric_validate_lone_sign_rejects() {
        match validate_numeric_text("+") {
            Err(CsvNumericError::Malformed { reason }) => {
                assert!(reason.contains("digits"));
            }
            other => panic!("expected Malformed for lone sign, got {other:?}"),
        }
        match validate_numeric_text("-") {
            Err(CsvNumericError::Malformed { reason }) => {
                assert!(reason.contains("digits"));
            }
            other => panic!("expected Malformed for lone sign, got {other:?}"),
        }
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: lone dot rejects as Malformed
    /// (no digits on either side).
    #[test]
    fn t1_numeric_validate_lone_dot_rejects() {
        match validate_numeric_text(".") {
            Err(CsvNumericError::Malformed { reason }) => {
                assert!(reason.contains("digits"));
            }
            other => panic!("expected Malformed for lone dot, got {other:?}"),
        }
    }

    /// SP-PG-COPY-CSV-NUMERIC T1: negative zero canonicalises to
    /// `0` (PG's numeric_out canonical form has no negative zero).
    #[test]
    fn t1_numeric_validate_negative_zero_canonicalises() {
        assert_eq!(validate_numeric_text("-0").unwrap(), "0");
        assert_eq!(validate_numeric_text("-0.00").unwrap(), "0.00");
    }

    // ─── SP-PG-COPY-CSV-NUMERIC-SCI V1 KATs (2026-06-02) ────────────────
    //
    // Companion design spec:
    // `docs/superpowers/specs/2026-06-02-kesseldb-sppgcopycsvnumericsci-design.md`
    //
    // Scientific notation parsed + expanded into canonical decimal text.
    // V1 grammar: [+-]?(\d+(\.\d+)?|\.\d+)[eE][+-]?\d+ with |exp|<=100.

    /// SCI T1: `1e10` expands to `"10000000000"` (Avogadro-style integer).
    #[test]
    fn t1_sci_integer_mantissa_positive_exp() {
        assert_eq!(validate_numeric_text("1e10").unwrap(), "10000000000");
    }

    /// SCI T1: `1e-3` expands to `"0.001"` (negative exponent, leading-
    /// zero pad).
    #[test]
    fn t1_sci_integer_mantissa_negative_exp() {
        assert_eq!(validate_numeric_text("1e-3").unwrap(), "0.001");
    }

    /// SCI T1: `1.5e2` expands to `"150"` (fractional mantissa, decimal
    /// shifted out).
    #[test]
    fn t1_sci_fractional_mantissa_positive_exp() {
        assert_eq!(validate_numeric_text("1.5e2").unwrap(), "150");
    }

    /// SCI T1: `1.5e-2` expands to `"0.015"` (fractional mantissa,
    /// negative exponent, leading-zero pad).
    #[test]
    fn t1_sci_fractional_mantissa_negative_exp() {
        assert_eq!(validate_numeric_text("1.5e-2").unwrap(), "0.015");
    }

    /// SCI T1: `6.022e23` expands to the 24-digit Avogadro number
    /// representation (canonical PG `numeric_out` form).
    #[test]
    fn t1_sci_avogadro_number_expands() {
        assert_eq!(
            validate_numeric_text("6.022e23").unwrap(),
            "602200000000000000000000"
        );
    }

    /// SCI T1: `-3.14e2` expands to `"-314"` (signed mantissa, decimal
    /// fully shifted out).
    #[test]
    fn t1_sci_signed_mantissa_expands() {
        assert_eq!(validate_numeric_text("-3.14e2").unwrap(), "-314");
    }

    /// SCI T1: `+1.5e+3` expands to `"1500"` (explicit positive sign on
    /// both mantissa and exponent — both stripped).
    #[test]
    fn t1_sci_explicit_positive_signs_stripped() {
        assert_eq!(validate_numeric_text("+1.5e+3").unwrap(), "1500");
    }

    /// SCI T1: uppercase `E` exponent marker accepted (case-insensitive).
    #[test]
    fn t1_sci_uppercase_exponent_marker() {
        assert_eq!(validate_numeric_text("1E10").unwrap(), "10000000000");
    }

    /// SCI T1: mixed-case `1.5E-3` expands to `"0.0015"`.
    #[test]
    fn t1_sci_mixed_case_uppercase_e() {
        assert_eq!(validate_numeric_text("1.5E-3").unwrap(), "0.0015");
    }

    /// SCI T1: `1e0` expands to `"1"` (exp=0 is a no-op shift).
    #[test]
    fn t1_sci_exp_zero_is_identity() {
        assert_eq!(validate_numeric_text("1e0").unwrap(), "1");
    }

    /// SCI T1: `0e0` expands to `"0"` (zero mantissa + zero exp).
    #[test]
    fn t1_sci_zero_mantissa_zero_exp() {
        assert_eq!(validate_numeric_text("0e0").unwrap(), "0");
    }

    /// SCI T1: leading-dot mantissa `.5e2` expands to `"50"`.
    #[test]
    fn t1_sci_leading_dot_mantissa() {
        assert_eq!(validate_numeric_text(".5e2").unwrap(), "50");
    }

    /// SCI T1: out-of-range exponent (|exp| > 100) rejects as Malformed
    /// with `"exponent out of range"`.
    #[test]
    fn t1_sci_exponent_out_of_range_rejects() {
        match validate_numeric_text("1e1000") {
            Err(CsvNumericError::Malformed { reason }) => {
                assert!(
                    reason.contains("exponent out of range"),
                    "reason = {reason}"
                );
            }
            other => panic!("expected Malformed for 1e1000, got {other:?}"),
        }
        // Negative side too.
        match validate_numeric_text("1e-200") {
            Err(CsvNumericError::Malformed { reason }) => {
                assert!(reason.contains("exponent out of range"));
            }
            other => panic!("expected Malformed for 1e-200, got {other:?}"),
        }
    }

    /// SCI T1: bare `e10` (no mantissa) rejects (BadByte at position 0
    /// — the `e` is illegal as a first byte).
    #[test]
    fn t1_sci_bare_e_no_mantissa_rejects() {
        match validate_numeric_text("e10") {
            Err(CsvNumericError::BadByte { position, byte }) => {
                assert_eq!(position, 0);
                assert_eq!(byte, b'e');
            }
            other => panic!("expected BadByte at 0, got {other:?}"),
        }
    }

    /// SCI T1: `1e` (no exponent digits) rejects as Malformed with
    /// `"missing exponent"`.
    #[test]
    fn t1_sci_missing_exponent_digits_rejects() {
        match validate_numeric_text("1e") {
            Err(CsvNumericError::Malformed { reason }) => {
                assert!(reason.contains("missing exponent"), "reason = {reason}");
            }
            other => panic!("expected Malformed for 1e, got {other:?}"),
        }
    }

    /// SCI T1: multiple exponent markers `1ee2` reject as Malformed.
    #[test]
    fn t1_sci_multiple_exponent_markers_reject() {
        match validate_numeric_text("1ee2") {
            Err(CsvNumericError::Malformed { reason }) => {
                assert!(reason.contains("multiple exponent markers"));
            }
            other => panic!("expected Malformed for 1ee2, got {other:?}"),
        }
    }

    /// SCI T1: malformed exponent sign `1e+-3` rejects as Malformed.
    #[test]
    fn t1_sci_malformed_exponent_sign_rejects() {
        match validate_numeric_text("1e+-3") {
            Err(CsvNumericError::Malformed { reason }) => {
                assert!(reason.contains("malformed exponent"));
            }
            other => panic!("expected Malformed for 1e+-3, got {other:?}"),
        }
    }

    /// SCI T1: non-integer exponent `1e1.5` rejects as Malformed.
    #[test]
    fn t1_sci_non_integer_exponent_rejects() {
        match validate_numeric_text("1e1.5") {
            Err(CsvNumericError::Malformed { reason }) => {
                assert!(reason.contains("non-integer exponent"));
            }
            other => panic!("expected Malformed for 1e1.5, got {other:?}"),
        }
    }

    /// SCI T1: trailing-dot mantissa `5.e2` is the named follow-up arc
    /// `SP-PG-COPY-CSV-NUMERIC-SCI-TRAILDOT` — rejected with that arc
    /// name in the message.
    #[test]
    fn t1_sci_trailing_dot_mantissa_named_followup() {
        match validate_numeric_text("5.e2") {
            Err(CsvNumericError::Malformed { reason }) => {
                assert!(
                    reason.contains("SP-PG-COPY-CSV-NUMERIC-SCI-TRAILDOT"),
                    "reason = {reason}"
                );
            }
            other => panic!("expected Malformed for 5.e2, got {other:?}"),
        }
    }

    /// SCI T1: negative zero with a scientific suffix canonicalises to
    /// `"0"` (matches V1 `-0` → `0` semantics).
    #[test]
    fn t1_sci_negative_zero_canonicalises() {
        assert_eq!(validate_numeric_text("-0e5").unwrap(), "0");
    }
}
