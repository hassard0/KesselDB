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

use crate::proto::{
    PG_TYPE_BOOL, PG_TYPE_BYTEA, PG_TYPE_FLOAT4, PG_TYPE_FLOAT8, PG_TYPE_INT2, PG_TYPE_INT4,
    PG_TYPE_INT8, PG_TYPE_NUMERIC, PG_TYPE_TEXT, PG_TYPE_TIMESTAMPTZ, PG_TYPE_VARCHAR,
};

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
    /// SP-PG-EXTQ-BIN T2 — preprocessing a binary parameter into its
    /// SQL-literal form failed. Carries position (1-based for client
    /// readability) and a human-readable reason from
    /// `BinaryDecodeError`. Maps to SQLSTATE `08P01 protocol_
    /// violation` at the dispatcher.
    BinaryDecode { position: usize, reason: String },
}

/// SP-PG-EXTQ-BIN T1 — errors from `decode_binary_param`. Each carries
/// enough context for the dispatcher to render a precise client-facing
/// SQLSTATE + message (the unsupported variants name the V2 follow-up
/// arc so operators can grep for the gap).
#[derive(Debug, PartialEq, Eq)]
pub enum BinaryDecodeError {
    /// Wire byte count doesn't match the declared type's fixed-size
    /// requirement. Maps to SQLSTATE `08P01 protocol_violation` —
    /// the client mismatched its type OID with the bound bytes.
    /// Carries `type_oid`, the expected byte count, and the actual.
    WrongLength {
        type_oid: u32,
        expected: usize,
        actual: usize,
    },
    /// The bytes decode as a value that isn't valid for the declared
    /// type — e.g. BOOL byte that's neither 0x00 nor 0x01, or a TEXT
    /// payload that isn't valid UTF-8. Maps to SQLSTATE `22P02
    /// invalid_text_representation` (same code PG uses).
    BadValue { type_oid: u32, reason: &'static str },
    /// V1 doesn't support this type in binary format yet. Carries
    /// `type_oid` + the V2 follow-up arc name. Maps to SQLSTATE
    /// `0A000 feature_not_supported` so clients fall back gracefully.
    Unsupported { type_oid: u32, arc: &'static str },
}

/// SP-PG-EXTQ-BIN T1 — decode a binary-format parameter into the SQL-
/// literal representation the substitute helper will splice into the
/// rewritten SQL.
///
/// **Returns:** the BARE literal text (NOT single-quoted). The caller
/// is responsible for wrapping in `'...'` and applying single-quote
/// doubling for text-shaped output. The caller also adds the
/// `::bytea` / `::timestamptz` SQL-cast suffix for the binary-only
/// types whose literal text doesn't parse without a cast hint.
///
/// **Per-type contracts** (per PG §55.8 binary representations):
///
/// | type | OID | input bytes | output literal |
/// |---|---|---|---|
/// | BOOL | 16 | 1 byte 0x00/0x01 | `false` / `true` |
/// | BYTEA | 17 | raw bytes | `\xHEX` (lowercase hex; caller wraps `'\\xHEX'::bytea`) |
/// | INT2 | 21 | 2 bytes BE i16 | decimal string |
/// | INT4 | 23 | 4 bytes BE i32 | decimal string |
/// | INT8 | 20 | 8 bytes BE i64 | decimal string |
/// | TEXT | 25 | UTF-8 | bare string (caller wraps `'...'` + escapes `'`) |
/// | VARCHAR | 1043 | UTF-8 | bare string |
/// | FLOAT4 | 700 | 4 bytes IEEE-754 BE | `{:?}` (round-trip-precise) |
/// | FLOAT8 | 701 | 8 bytes IEEE-754 BE | `{:?}` (round-trip-precise) |
/// | TIMESTAMPTZ | 1184 | 8 bytes BE i64 µs since 2000-01-01 UTC | ISO `YYYY-MM-DD HH:MM:SS.ffffff+00` (caller wraps `'...'::timestamptz`) |
/// | NUMERIC | 1700 | varlena base-10000 | `BinaryDecodeError::Unsupported { arc: "SP-PG-EXTQ-BIN-NUMERIC" }` |
/// | other | any | any | `BinaryDecodeError::Unsupported { arc: "SP-PG-EXTQ-BIN-EXTRA" }` |
///
/// **Why bare literals?** The caller already runs the resulting SQL
/// through KesselDB's SQL parser, which accepts unquoted integer +
/// float + bool literals natively. For TEXT/VARCHAR/BYTEA/TIMESTAMPTZ
/// the literal text needs quoting (and for BYTEA/TIMESTAMPTZ a cast
/// hint), but that's the SUBSTITUTE layer's responsibility — keeping
/// the decoder pure (bytes → string, no SQL formatting) makes it
/// trivially testable.
pub fn decode_binary_param(bytes: &[u8], type_oid: u32) -> Result<String, BinaryDecodeError> {
    match type_oid {
        PG_TYPE_BOOL => decode_bool(bytes),
        PG_TYPE_INT2 => decode_int2(bytes),
        PG_TYPE_INT4 => decode_int4(bytes),
        PG_TYPE_INT8 => decode_int8(bytes),
        PG_TYPE_FLOAT4 => decode_float4(bytes),
        PG_TYPE_FLOAT8 => decode_float8(bytes),
        PG_TYPE_TEXT | PG_TYPE_VARCHAR => decode_text(bytes, type_oid),
        PG_TYPE_BYTEA => Ok(decode_bytea(bytes)),
        PG_TYPE_TIMESTAMPTZ => decode_timestamptz(bytes),
        PG_TYPE_NUMERIC => decode_numeric(bytes),
        _ => Err(BinaryDecodeError::Unsupported {
            type_oid,
            arc: "SP-PG-EXTQ-BIN-EXTRA",
        }),
    }
}

/// SP-PG-EXTQ-BIN T2 — scan a SQL string for `$N` placeholders and
/// return the maximum `N` found (0 if none). Honors the same lexical-
/// skip rules as `substitute_inner` (single-quoted strings, double-
/// quoted identifiers, line comments, block comments, dollar-quoted
/// strings).
///
/// Used by `dispatch_describe` for the `Describe('S')` synthesis path:
/// when Parse provided no OID hints, V1 falls back to emitting a
/// `ParameterDescription` of `[PG_TYPE_TEXT; max_n]` so drivers like
/// asyncpg (which rely on the server's parameter-count answer before
/// they'll Bind) know how many params the SQL takes. PG itself does
/// real type inference here; V1 does the simpler-but-correct shape
/// (text encoding works for every supported binary type because the
/// `text-format` wire bytes are valid UTF-8 the gateway can
/// substitute via the existing text path).
pub fn count_placeholders(sql: &str) -> usize {
    let mut max_n = 0usize;
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        // Skip single-quoted strings.
        if b == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
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
        // Skip double-quoted identifiers.
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
        // Skip line comments.
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            let end = bytes[i..].iter().position(|&x| x == b'\n').map(|p| i + p);
            i = end.map(|p| p + 1).unwrap_or(bytes.len());
            continue;
        }
        // Skip block comments.
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            let rest = &bytes[i + 2..];
            let end = rest
                .windows(2)
                .position(|w| w == b"*/")
                .map(|p| i + 2 + p + 2)
                .unwrap_or(bytes.len());
            i = end;
            continue;
        }
        // `$` handling.
        if b == b'$' {
            // Dollar-quoted (tagged or empty-tag) — skip entirely.
            if i + 1 < bytes.len() && (is_tag_start_byte(bytes[i + 1]) || bytes[i + 1] == b'$') {
                // Skip ahead past the dollar-quoted region. Re-use
                // substitute_inner's logic via a stub.
                // For simplicity: find the next `$` and check if the
                // close tag matches.
                if bytes[i + 1] == b'$' {
                    // Empty-tag — find next `$$`.
                    let mut j = i + 2;
                    let mut found = false;
                    while j + 1 < bytes.len() {
                        if bytes[j] == b'$' && bytes[j + 1] == b'$' {
                            i = j + 2;
                            found = true;
                            break;
                        }
                        j += 1;
                    }
                    if !found {
                        i = bytes.len();
                    }
                    continue;
                } else {
                    // Tagged — find matching close tag.
                    let tag_start = i + 1;
                    let mut tag_end = tag_start;
                    while tag_end < bytes.len() && is_tag_cont_byte(bytes[tag_end]) {
                        tag_end += 1;
                    }
                    if tag_end < bytes.len() && bytes[tag_end] == b'$' {
                        let tag = &bytes[tag_start..tag_end];
                        let opener_end = tag_end + 1;
                        let mut j = opener_end;
                        let mut found = false;
                        while j < bytes.len() {
                            if bytes[j] == b'$' {
                                let after = j + 1;
                                if after + tag.len() <= bytes.len()
                                    && &bytes[after..after + tag.len()] == tag
                                    && after + tag.len() < bytes.len()
                                    && bytes[after + tag.len()] == b'$'
                                {
                                    i = after + tag.len() + 1;
                                    found = true;
                                    break;
                                }
                            }
                            j += 1;
                        }
                        if !found {
                            i = bytes.len();
                        }
                        continue;
                    }
                }
            }
            // `$N` — greedy decimal scan.
            let mut digit_end = i + 1;
            while digit_end < bytes.len() && bytes[digit_end].is_ascii_digit() {
                digit_end += 1;
            }
            if digit_end > i + 1 {
                let digits = std::str::from_utf8(&bytes[i + 1..digit_end])
                    .expect("ascii digits are utf8");
                if let Ok(n) = digits.parse::<usize>() {
                    if n > max_n {
                        max_n = n;
                    }
                }
                i = digit_end;
                continue;
            }
        }
        i += 1;
    }
    max_n
}

/// True iff `type_oid` is one V1's `decode_binary_param` accepts
/// (used by `dispatch_bind` to admit binary params for supported
/// OIDs and reject the rest at Bind time, BEFORE the portal stores).
pub fn binary_format_supported_for_oid(type_oid: u32) -> bool {
    matches!(
        type_oid,
        PG_TYPE_BOOL
            | PG_TYPE_INT2
            | PG_TYPE_INT4
            | PG_TYPE_INT8
            | PG_TYPE_FLOAT4
            | PG_TYPE_FLOAT8
            | PG_TYPE_TEXT
            | PG_TYPE_VARCHAR
            | PG_TYPE_BYTEA
            | PG_TYPE_TIMESTAMPTZ
            | PG_TYPE_NUMERIC
    )
}

/// V2 follow-up arc name for a given unsupported binary type OID.
/// Used in dispatcher error messages so clients (and operators
/// reading logs) see exactly which arc unlocks the gap.
pub fn unsupported_binary_arc_for_oid(type_oid: u32) -> &'static str {
    if type_oid == PG_TYPE_NUMERIC {
        "SP-PG-EXTQ-BIN-NUMERIC"
    } else {
        "SP-PG-EXTQ-BIN-EXTRA"
    }
}

fn decode_bool(bytes: &[u8]) -> Result<String, BinaryDecodeError> {
    if bytes.len() != 1 {
        return Err(BinaryDecodeError::WrongLength {
            type_oid: PG_TYPE_BOOL,
            expected: 1,
            actual: bytes.len(),
        });
    }
    match bytes[0] {
        0x00 => Ok("false".to_string()),
        0x01 => Ok("true".to_string()),
        _ => Err(BinaryDecodeError::BadValue {
            type_oid: PG_TYPE_BOOL,
            reason: "BOOL binary byte must be 0x00 or 0x01",
        }),
    }
}

fn decode_int2(bytes: &[u8]) -> Result<String, BinaryDecodeError> {
    if bytes.len() != 2 {
        return Err(BinaryDecodeError::WrongLength {
            type_oid: PG_TYPE_INT2,
            expected: 2,
            actual: bytes.len(),
        });
    }
    let n = i16::from_be_bytes([bytes[0], bytes[1]]);
    Ok(n.to_string())
}

fn decode_int4(bytes: &[u8]) -> Result<String, BinaryDecodeError> {
    if bytes.len() != 4 {
        return Err(BinaryDecodeError::WrongLength {
            type_oid: PG_TYPE_INT4,
            expected: 4,
            actual: bytes.len(),
        });
    }
    let n = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    Ok(n.to_string())
}

fn decode_int8(bytes: &[u8]) -> Result<String, BinaryDecodeError> {
    if bytes.len() != 8 {
        return Err(BinaryDecodeError::WrongLength {
            type_oid: PG_TYPE_INT8,
            expected: 8,
            actual: bytes.len(),
        });
    }
    let n = i64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    Ok(n.to_string())
}

fn decode_float4(bytes: &[u8]) -> Result<String, BinaryDecodeError> {
    if bytes.len() != 4 {
        return Err(BinaryDecodeError::WrongLength {
            type_oid: PG_TYPE_FLOAT4,
            expected: 4,
            actual: bytes.len(),
        });
    }
    let f = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    // {:?} on f32 gives the shortest round-trip-precise decimal —
    // matches Postgres's extra_float_digits=3 behavior closely enough
    // for the SQL parser to accept and re-encode losslessly.
    Ok(format!("{f:?}"))
}

fn decode_float8(bytes: &[u8]) -> Result<String, BinaryDecodeError> {
    if bytes.len() != 8 {
        return Err(BinaryDecodeError::WrongLength {
            type_oid: PG_TYPE_FLOAT8,
            expected: 8,
            actual: bytes.len(),
        });
    }
    let f = f64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    Ok(format!("{f:?}"))
}

fn decode_text(bytes: &[u8], type_oid: u32) -> Result<String, BinaryDecodeError> {
    std::str::from_utf8(bytes)
        .map(|s| s.to_string())
        .map_err(|_| BinaryDecodeError::BadValue {
            type_oid,
            reason: "TEXT/VARCHAR binary bytes must be valid UTF-8",
        })
}

fn decode_bytea(bytes: &[u8]) -> String {
    // PG bytea text format: `\xHEX` (lowercase). The caller wraps in
    // single quotes + `::bytea` cast suffix.
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push('\\');
    s.push('x');
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// SP-PG-EXTQ-BIN-NUMERIC T3 — decode the PG NUMERIC binary wire frame
/// via the pure-Rust codec in `extq::binary_numeric`. Maps the codec's
/// error enum into the dispatcher's `BinaryDecodeError` shape so the
/// existing error-propagation paths in `preprocess_params` +
/// `dispatch_execute` work without per-variant churn.
fn decode_numeric(bytes: &[u8]) -> Result<String, BinaryDecodeError> {
    use crate::extq::binary_numeric::{decode_numeric_binary, BinaryNumericError};
    decode_numeric_binary(bytes).map_err(|e| match e {
        // SP-PG-EXTQ-BIN-NUMERIC-NAN-INF (2026-06-02): the codec no
        // longer constructs `NaN` — the special sign codes (NAN /
        // PINF / NINF) now decode to canonical strings. Arm kept as
        // a defensive fallback for source compatibility.
        BinaryNumericError::NaN => BinaryDecodeError::Unsupported {
            type_oid: PG_TYPE_NUMERIC,
            arc: "SP-PG-EXTQ-BIN-NUMERIC-NAN",
        },
        BinaryNumericError::OutOfRange { arc, .. } => BinaryDecodeError::Unsupported {
            type_oid: PG_TYPE_NUMERIC,
            arc,
        },
        BinaryNumericError::WrongLength { actual } => BinaryDecodeError::WrongLength {
            type_oid: PG_TYPE_NUMERIC,
            expected: 8,
            actual,
        },
        BinaryNumericError::Truncated { .. } => BinaryDecodeError::BadValue {
            type_oid: PG_TYPE_NUMERIC,
            reason: "NUMERIC binary digit array truncated",
        },
        BinaryNumericError::BadSign { .. } => BinaryDecodeError::BadValue {
            type_oid: PG_TYPE_NUMERIC,
            reason: "NUMERIC binary unknown sign code",
        },
        BinaryNumericError::BadDigit { .. } => BinaryDecodeError::BadValue {
            type_oid: PG_TYPE_NUMERIC,
            reason: "NUMERIC binary digit out of [0, 9999]",
        },
        BinaryNumericError::BadDecimalString { .. } => BinaryDecodeError::BadValue {
            type_oid: PG_TYPE_NUMERIC,
            reason: "NUMERIC binary decoder returned invalid string (unreachable)",
        },
    })
}

/// Convert PG binary TIMESTAMPTZ (i64 microseconds since
/// 2000-01-01 00:00:00 UTC) to an ISO-8601 timestamp string with
/// `+00` timezone suffix. PG's binary epoch is 30 years later than
/// the Unix epoch (946684800 seconds), so we offset before formatting.
///
/// V1 ships a pure-Rust implementation (no chrono dep) to honor the
/// zero-dep stance. The algorithm: split microseconds → seconds +
/// subsecond microseconds; offset to Unix epoch; civil-from-days
/// algorithm (Howard Hinnant date.h algorithm — patent-free, in
/// public domain) to extract Y/M/D from the day count; format.
fn decode_timestamptz(bytes: &[u8]) -> Result<String, BinaryDecodeError> {
    if bytes.len() != 8 {
        return Err(BinaryDecodeError::WrongLength {
            type_oid: PG_TYPE_TIMESTAMPTZ,
            expected: 8,
            actual: bytes.len(),
        });
    }
    let pg_micros = i64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    // PG epoch = 2000-01-01 00:00:00 UTC. Unix epoch = 1970-01-01.
    // Difference = 946684800 seconds = 946684800_000_000 µs.
    const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;
    let unix_micros = pg_micros.checked_add(PG_EPOCH_OFFSET_MICROS).ok_or(
        BinaryDecodeError::BadValue {
            type_oid: PG_TYPE_TIMESTAMPTZ,
            reason: "TIMESTAMPTZ binary overflows i64 microseconds",
        },
    )?;
    // Split into seconds + microsecond remainder, normalizing for
    // negative values (pre-1970 timestamps).
    let (sec, micro) = if unix_micros >= 0 {
        (unix_micros / 1_000_000, (unix_micros % 1_000_000) as u32)
    } else {
        // Floor division so the micro remainder is non-negative.
        let q = unix_micros / 1_000_000;
        let r = unix_micros % 1_000_000;
        if r == 0 {
            (q, 0u32)
        } else {
            (q - 1, (r + 1_000_000) as u32)
        }
    };
    // Days since Unix epoch (1970-01-01).
    let days = sec.div_euclid(86_400);
    let sec_of_day = sec.rem_euclid(86_400) as u32;
    let hh = sec_of_day / 3600;
    let mm = (sec_of_day % 3600) / 60;
    let ss = sec_of_day % 60;
    let (y, m, d) = civil_from_days(days);
    Ok(format!(
        "{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}.{micro:06}+00"
    ))
}

/// Howard Hinnant "civil_from_days" — convert days-since-1970-01-01
/// to (year, month, day). Public domain. Correct for all i64 days
/// (covers years -5,879,610 .. +5,879,611 — wildly beyond any real
/// timestamp). Source: https://howardhinnant.github.io/date_algorithms.html
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    // Algorithm assumes the proleptic Gregorian calendar; March-based
    // year so the leap day is at the end of the year.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let y_adj = if m <= 2 { y + 1 } else { y };
    (y_adj, m, d)
}

/// SP-PG-EXTQ-BIN T2 — one prepared parameter value, ready for
/// substitution. The variants represent the three distinct flavors:
/// - `Null` — wire length=-1 sentinel; renders as bare `NULL`.
/// - `Text(bytes)` — text-format wire bytes; substitute wraps in
///   single quotes + applies `'`→`''` doubling.
/// - `Raw(sql)` — a pre-rendered SQL fragment (e.g. an INT8 binary
///   decoded to `100` or a BYTEA binary decoded + wrapped as
///   `'\xDEAD'::bytea`). Substitute splices verbatim — NO quoting,
///   NO escaping. The caller is responsible for producing valid SQL.
///
/// This variant exists so the binary-format path can route through
/// the same `$N` scanner as the text path without the scanner
/// needing to know about per-param format codes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparedParam {
    Null,
    Text(Vec<u8>),
    Raw(String),
}

/// SP-PG-EXTQ-BIN T2 — substitute `$N` placeholders with the format-
/// aware rendered parameters. Each entry of `params` carries its own
/// rendering rule (Null / Text / Raw). The scanner is shared with
/// the text-only path; only the per-param renderer differs.
pub fn substitute_params(
    sql: &str,
    params: &[PreparedParam],
) -> Result<String, SubstituteError> {
    substitute_inner(sql, params.len(), |out, i| {
        render_prepared_param(out, &params[i])
    })
}

/// SP-PG-EXTQ-BIN T2 — preprocess raw wire params + per-position
/// format codes + Parse-time type OIDs into the discriminated
/// `Vec<PreparedParam>` the new `substitute_params` consumes.
///
/// **Inputs:**
/// - `params: Option<&[u8]>` per position (None = SQL NULL).
/// - `formats: &[u16]` per PG length conventions (0 codes = all text,
///   1 code = all-same, N codes = per-position).
/// - `type_oids: &[u32]` from `PreparedStmt.param_oids` (may be
///   shorter than params; missing positions default to OID 0).
///
/// **Per-position rule:**
/// - format text + value None → `Null`.
/// - format text + value Some(b) → `Text(b)` (substitute wraps in
///   quotes + escapes).
/// - format binary + value None → `Null` (PG semantics: NULL is
///   format-agnostic — `length=-1` sentinel overrides the format
///   code).
/// - format binary + value Some(b) → `decode_binary_param(b, oid)`
///   into a SQL literal, wrapped per type rules:
///   - integer/float/bool: `Raw(literal)` (no quotes, no cast).
///   - text/varchar: `Text(literal-bytes)` (substitute handles
///     quoting + escaping).
///   - bytea: `Raw('\xHEX'::bytea)`.
///   - timestamptz: `Raw('ISO+00'::timestamptz)`.
///
/// Returns a `Vec<PreparedParam>` ready for `substitute_params`.
/// `BinaryDecodeError` propagates as `SubstituteError::BinaryDecode`.
pub fn preprocess_params(
    params: &[Option<&[u8]>],
    formats: &[u16],
    type_oids: &[u32],
) -> Result<Vec<PreparedParam>, SubstituteError> {
    let mut out = Vec::with_capacity(params.len());
    for (i, value) in params.iter().enumerate() {
        let format = effective_format_code(formats, i);
        let bytes = match value {
            None => {
                out.push(PreparedParam::Null);
                continue;
            }
            Some(b) => *b,
        };
        if format == crate::proto::FORMAT_CODE_TEXT {
            out.push(PreparedParam::Text(bytes.to_vec()));
            continue;
        }
        // Binary path.
        let type_oid = type_oids.get(i).copied().unwrap_or(0);
        let literal = decode_binary_param(bytes, type_oid).map_err(|e| {
            SubstituteError::BinaryDecode {
                position: i,
                reason: render_binary_error(&e),
            }
        })?;
        let rendered = render_binary_decoded(type_oid, &literal);
        out.push(rendered);
    }
    Ok(out)
}

/// SP-PG-EXTQ-PARSED T3 — preprocess wire params into typed
/// `Option<kessel_codec::Value>` slots ready for
/// `kessel_sql::compile_with_params`. Returns `Some(Vec<...>)` only
/// when EVERY parameter can be represented cleanly as a typed
/// `Value`; otherwise returns `None` so the caller falls back to
/// the existing text-substitution path.
///
/// Per design spec §3.3, the typed-path-eligible cases for V1 are:
///
/// - NULL (length=-1) → `Some(None)`. Format-agnostic.
/// - text format + INT2/INT4/INT8 OID → `Value::Int(n)` if the
///   text bytes parse as i128, else `Value::Blob` (the parser's
///   SP-PG-SQL-PAREN-VALUES coercion will route the bytes back to
///   an int for the numeric column).
/// - text format + BOOL OID → `Value::Uint(0|1)` for `"true"`/
///   `"t"`/`"f"`/`"false"`/`"1"`/`"0"`. Anything else → fallback.
/// - text format + TEXT/VARCHAR OID → `Value::Blob(bytes.to_vec())`.
/// - text format + no OID (oid==0) or unknown OID → `Value::Blob`
///   (let the parser route by column context).
/// - binary format + INT2/INT4/INT8 OID → decode → `Value::Int(n)`.
/// - binary format + BOOL OID → `Value::Uint(0|1)`.
/// - binary format + BYTEA OID → `Value::Blob(bytes.to_vec())`.
/// - binary format + TEXT/VARCHAR OID → `Value::Blob(bytes.to_vec())`
///   (the binary path's text decoder is a UTF-8 validate + clone;
///   we skip the validate here because kessel-sql accepts arbitrary
///   bytes in Blob and would error at insert time if the column
///   needs UTF-8).
/// - binary format + FLOAT4/FLOAT8/TIMESTAMPTZ/NUMERIC → return
///   `None` overall. The text-substitution path's binary-decoder +
///   cast-wrapper shape (`'ISO'::timestamptz`, etc.) is the only
///   one that compiles cleanly today; V1 doesn't widen `Value` to
///   carry float / timestamp types yet.
///
/// **Returning `None` is a graceful fallback signal — it means the
/// gateway should keep using the existing `preprocess_params` text-
/// substitution path for this Bind. The default V1 disposition is
/// to keep the text path as the default and ONLY route through the
/// typed path when an env knob opts in (so we don't risk a silent
/// compat regression).
pub fn preprocess_typed_params(
    params: &[Option<&[u8]>],
    formats: &[u16],
    type_oids: &[u32],
) -> Option<Vec<Option<kessel_codec::Value>>> {
    let mut out: Vec<Option<kessel_codec::Value>> =
        Vec::with_capacity(params.len());
    for (i, value) in params.iter().enumerate() {
        let format = effective_format_code(formats, i);
        let bytes = match value {
            None => {
                out.push(None);
                continue;
            }
            Some(b) => *b,
        };
        let type_oid = type_oids.get(i).copied().unwrap_or(0);
        let v = if format == crate::proto::FORMAT_CODE_TEXT {
            preprocess_text_value(bytes, type_oid)?
        } else {
            preprocess_binary_value(bytes, type_oid)?
        };
        out.push(Some(v));
    }
    Some(out)
}

/// SP-PG-EXTQ-PARSED T3 — text-format → typed `Value` routing.
/// Returns `None` (so the caller falls back to text-substitution)
/// for OIDs the typed path doesn't cover cleanly yet.
fn preprocess_text_value(bytes: &[u8], type_oid: u32) -> Option<kessel_codec::Value> {
    use kessel_codec::Value;
    match type_oid {
        PG_TYPE_INT2 | PG_TYPE_INT4 | PG_TYPE_INT8 => {
            // Try parsing as an int; on failure fall back to Blob so
            // SP-PG-SQL-PAREN-VALUES's text→int coercion still runs
            // at the parser layer. NEVER falls back to None: the
            // typed path can express either flavor.
            let s = std::str::from_utf8(bytes).ok()?;
            if let Ok(n) = s.parse::<i128>() {
                Some(Value::Int(n))
            } else {
                Some(Value::Blob(bytes.to_vec()))
            }
        }
        PG_TYPE_BOOL => {
            // PG's text-format BOOL accepts t/f/true/false/1/0 (case-
            // insensitive). Other shapes → fall back.
            let s = std::str::from_utf8(bytes).ok()?;
            match s.trim().to_ascii_lowercase().as_str() {
                "t" | "true" | "1" => Some(Value::Uint(1)),
                "f" | "false" | "0" => Some(Value::Uint(0)),
                _ => None,
            }
        }
        PG_TYPE_TEXT | PG_TYPE_VARCHAR | PG_TYPE_BYTEA => {
            // Text/varchar/bytea text format is already string-shaped
            // bytes — Value::Blob carries them through to the parser
            // which (post SP-PG-EXTQ-PARSED-BYTEA-TYPED) routes via
            // `Tok::Bytes` → `Lit::Bytes` → `lit_to_value` for the
            // target column kind. Non-UTF8 bytes preserved verbatim.
            Some(Value::Blob(bytes.to_vec()))
        }
        // FLOAT4/FLOAT8/TIMESTAMPTZ/NUMERIC text-format SHAPES need a
        // SQL-cast wrapper (`'1.5'::float8`, `'ISO'::timestamptz`)
        // that only the text-substitution path emits. Fall back.
        PG_TYPE_FLOAT4 | PG_TYPE_FLOAT8 | PG_TYPE_TIMESTAMPTZ | PG_TYPE_NUMERIC => None,
        // Unknown OID — the parser's lit_to_value will route by
        // column kind. Pass through as Blob.
        _ => Some(Value::Blob(bytes.to_vec())),
    }
}

/// SP-PG-EXTQ-PARSED T3 — binary-format → typed `Value` routing.
/// Reuses the canonical PG-binary-format decoders from
/// `decode_binary_param` but only for the OIDs whose decoded form
/// can be expressed cleanly as a `Value`. FLOAT/TIMESTAMPTZ/NUMERIC
/// stay on the text-substitution path.
fn preprocess_binary_value(bytes: &[u8], type_oid: u32) -> Option<kessel_codec::Value> {
    use kessel_codec::Value;
    match type_oid {
        PG_TYPE_INT2 => {
            if bytes.len() != 2 { return None; }
            Some(Value::Int(i16::from_be_bytes([bytes[0], bytes[1]]) as i128))
        }
        PG_TYPE_INT4 => {
            if bytes.len() != 4 { return None; }
            Some(Value::Int(i32::from_be_bytes(
                [bytes[0], bytes[1], bytes[2], bytes[3]],
            ) as i128))
        }
        PG_TYPE_INT8 => {
            if bytes.len() != 8 { return None; }
            Some(Value::Int(i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]) as i128))
        }
        PG_TYPE_BOOL => {
            if bytes.len() != 1 { return None; }
            match bytes[0] {
                0x00 => Some(Value::Uint(0)),
                0x01 => Some(Value::Uint(1)),
                _ => None,
            }
        }
        // SP-PG-EXTQ-PARSED-BYTEA-TYPED T2 — BYTEA binary now flows
        // through the typed path. kessel-sql's `rewrite_param_tokens`
        // emits `Tok::Bytes(b)` for the `Value::Blob` shape (no
        // UTF-8 round-trip) and value-position parsers route it
        // through `Lit::Bytes` → `lit_to_value` → `Value::Blob` for
        // CHAR/BYTES/Ref columns. The bytes never enter SQL text
        // and arbitrary byte sequences (including 0x00, 0xFF, and
        // isolated UTF-8 continuation bytes) round-trip byte-equal.
        PG_TYPE_BYTEA => Some(Value::Blob(bytes.to_vec())),
        PG_TYPE_TEXT | PG_TYPE_VARCHAR => {
            // Validate UTF-8 to match the text-substitution path's
            // error shape — if invalid, fall back so the existing
            // text path's `BinaryDecode` error fires with its full
            // SQLSTATE context.
            std::str::from_utf8(bytes).ok()?;
            Some(Value::Blob(bytes.to_vec()))
        }
        // FLOAT/TIMESTAMPTZ/NUMERIC binary forms need text-substitution
        // path's cast wrappers. Fall back.
        PG_TYPE_FLOAT4 | PG_TYPE_FLOAT8 | PG_TYPE_TIMESTAMPTZ | PG_TYPE_NUMERIC => None,
        // Unknown OID — fall back.
        _ => None,
    }
}

/// SP-PG-EXTQ-BIN T2 — compute the effective format code for position
/// `i` per the PG length conventions:
/// - 0 codes  = all text  → 0
/// - 1 code   = all-same  → formats[0]
/// - N codes  = per-pos   → formats[i] (out-of-range falls back to 0)
///
/// Shared between `dispatch_bind` (binary-format admission check at
/// Bind time) and `preprocess_params` (per-position decode dispatch
/// at Execute time). Single-sourcing the convention here means a
/// future PG length-convention change updates BOTH dispatch paths in
/// one place.
pub fn effective_format_code(formats: &[u16], i: usize) -> u16 {
    match formats.len() {
        0 => crate::proto::FORMAT_CODE_TEXT,
        1 => formats[0],
        _ => formats.get(i).copied().unwrap_or(crate::proto::FORMAT_CODE_TEXT),
    }
}

/// SP-PG-EXTQ-BIN T2 — pick the right wire-SQL wrapper for a
/// decoded binary literal based on its type OID.
fn render_binary_decoded(type_oid: u32, literal: &str) -> PreparedParam {
    match type_oid {
        // Integers, floats, bool — bare unquoted literal.
        PG_TYPE_INT2 | PG_TYPE_INT4 | PG_TYPE_INT8 | PG_TYPE_FLOAT4 | PG_TYPE_FLOAT8
        | PG_TYPE_BOOL => PreparedParam::Raw(literal.to_string()),
        // Text / Varchar — single-quoted with `'`→`''` doubling
        // (route through the existing Text renderer so the escape
        // logic isn't duplicated).
        PG_TYPE_TEXT | PG_TYPE_VARCHAR => PreparedParam::Text(literal.as_bytes().to_vec()),
        // Bytea — needs explicit cast so the SQL parser accepts the
        // `\xHEX` shape.
        PG_TYPE_BYTEA => PreparedParam::Raw(format!("'{literal}'::bytea")),
        // Timestamptz — same shape with `::timestamptz` cast.
        PG_TYPE_TIMESTAMPTZ => PreparedParam::Raw(format!("'{literal}'::timestamptz")),
        // Numeric — single-quoted (kessel-sql accepts a quoted decimal
        // literal the same way it accepts text-format NUMERIC params).
        // The decoder already emitted a bare canonical decimal string
        // (no `'`), so the Text variant's `'`→`''` escape is a no-op.
        PG_TYPE_NUMERIC => PreparedParam::Text(literal.as_bytes().to_vec()),
        // Should never reach here — the Bind-time admission check
        // already rejected unsupported OIDs. Defensive: emit Raw.
        _ => PreparedParam::Raw(literal.to_string()),
    }
}

fn render_binary_error(e: &BinaryDecodeError) -> String {
    match e {
        BinaryDecodeError::WrongLength {
            type_oid,
            expected,
            actual,
        } => format!(
            "binary parameter of type OID {type_oid}: expected {expected} bytes, got {actual}"
        ),
        BinaryDecodeError::BadValue { type_oid, reason } => {
            format!("binary parameter of type OID {type_oid}: {reason}")
        }
        BinaryDecodeError::Unsupported { type_oid, arc } => {
            format!("binary parameter of type OID {type_oid} not supported in V1: {arc}")
        }
    }
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
    substitute_inner(sql, params.len(), |out, idx| render_param(out, params[idx]))
}

/// SP-PG-EXTQ-BIN T2 — shared scanner used by both
/// `substitute_text_format_params` (text-only path) and
/// `substitute_params` (format-aware path). The scanner walks the
/// SQL text exactly as before, skipping single-quoted strings, double-
/// quoted identifiers, line comments, block comments, and dollar-
/// quoted strings. On `$N` (1-based) placeholder, it calls back into
/// the supplied `render` closure with the output buffer + 0-based
/// index — the caller is responsible for emitting the parameter's
/// SQL representation.
///
/// Splitting the scanner from the renderer is what lets the binary-
/// format path reuse 100% of the substitution logic without
/// duplicating any of the lexer state.
fn substitute_inner<F>(
    sql: &str,
    param_count: usize,
    mut render: F,
) -> Result<String, SubstituteError>
where
    F: FnMut(&mut String, usize),
{
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
                if n > param_count {
                    return Err(SubstituteError::ParamIndexOutOfBounds {
                        index: n,
                        available: param_count,
                    });
                }
                render(&mut out, n - 1);
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

/// SP-PG-EXTQ-BIN T2 — emit one `PreparedParam` into the output
/// string. Mirrors `render_param` for the Text/Null variants but adds
/// the Raw variant that splices a pre-rendered SQL fragment verbatim.
fn render_prepared_param(out: &mut String, value: &PreparedParam) {
    match value {
        PreparedParam::Null => out.push_str("NULL"),
        PreparedParam::Text(bytes) => {
            out.push('\'');
            for &b in bytes {
                if b == b'\'' {
                    out.push('\'');
                    out.push('\'');
                } else {
                    out.push(b as char);
                }
            }
            out.push('\'');
        }
        PreparedParam::Raw(sql) => out.push_str(sql),
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

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-EXTQ-BIN T1 KATs — `decode_binary_param` per PG §55.8
    // binary representations. Each KAT pins one supported PG type's
    // decode shape against a canonical wire byte pattern.
    // ───────────────────────────────────────────────────────────────────

    /// INT8 binary `[0x00 .. 0x00 0x64]` → SQL literal `100`.
    #[test]
    fn t1bin_decode_int8_binary_positive() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64];
        let out = decode_binary_param(&bytes, PG_TYPE_INT8).expect("ok");
        assert_eq!(out, "100");
    }

    /// INT8 binary all-ones (-1 as i64) → SQL literal `-1`.
    #[test]
    fn t1bin_decode_int8_binary_negative() {
        let bytes = [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        let out = decode_binary_param(&bytes, PG_TYPE_INT8).expect("ok");
        assert_eq!(out, "-1");
    }

    /// INT4 binary `0xFFFFFFFF` → SQL literal `-1`.
    #[test]
    fn t1bin_decode_int4_binary_negative() {
        let bytes = [0xFF, 0xFF, 0xFF, 0xFF];
        let out = decode_binary_param(&bytes, PG_TYPE_INT4).expect("ok");
        assert_eq!(out, "-1");
    }

    /// INT2 binary 42 (`0x002A`) → SQL literal `42`.
    #[test]
    fn t1bin_decode_int2_binary() {
        let bytes = [0x00, 0x2A];
        let out = decode_binary_param(&bytes, PG_TYPE_INT2).expect("ok");
        assert_eq!(out, "42");
    }

    /// BOOL binary `0x01` → SQL literal `true`.
    #[test]
    fn t1bin_decode_bool_true() {
        let out = decode_binary_param(&[0x01], PG_TYPE_BOOL).expect("ok");
        assert_eq!(out, "true");
    }

    /// BOOL binary `0x00` → SQL literal `false`.
    #[test]
    fn t1bin_decode_bool_false() {
        let out = decode_binary_param(&[0x00], PG_TYPE_BOOL).expect("ok");
        assert_eq!(out, "false");
    }

    /// BOOL binary with an invalid byte rejects with `BadValue`.
    #[test]
    fn t1bin_decode_bool_invalid_byte_rejects() {
        let err = decode_binary_param(&[0x02], PG_TYPE_BOOL).unwrap_err();
        match err {
            BinaryDecodeError::BadValue { type_oid, .. } => {
                assert_eq!(type_oid, PG_TYPE_BOOL);
            }
            other => panic!("expected BadValue, got {other:?}"),
        }
    }

    /// FLOAT8 binary of π → SQL literal `3.141592653589793` (the
    /// round-trip-precise shortest decimal Rust's `{:?}` produces).
    #[test]
    fn t1bin_decode_float8_pi() {
        let bytes = std::f64::consts::PI.to_be_bytes();
        let out = decode_binary_param(&bytes, PG_TYPE_FLOAT8).expect("ok");
        assert_eq!(out, "3.141592653589793");
    }

    /// FLOAT4 binary of 1.5 → SQL literal `1.5`.
    #[test]
    fn t1bin_decode_float4() {
        let bytes = 1.5f32.to_be_bytes();
        let out = decode_binary_param(&bytes, PG_TYPE_FLOAT4).expect("ok");
        assert_eq!(out, "1.5");
    }

    /// TEXT binary UTF-8 → bare UTF-8 string (caller wraps in quotes
    /// + escapes single-quotes — decoder returns the raw string).
    #[test]
    fn t1bin_decode_text_utf8() {
        let bytes = b"hello";
        let out = decode_binary_param(bytes, PG_TYPE_TEXT).expect("ok");
        assert_eq!(out, "hello");
    }

    /// TEXT binary with an embedded `'` → bare UTF-8 (caller does
    /// the `'`→`''` doubling; decoder is escape-agnostic).
    #[test]
    fn t1bin_decode_text_with_quote_returns_raw() {
        let bytes = b"hello'world";
        let out = decode_binary_param(bytes, PG_TYPE_TEXT).expect("ok");
        assert_eq!(out, "hello'world");
    }

    /// VARCHAR binary is the same shape as TEXT.
    #[test]
    fn t1bin_decode_varchar_utf8() {
        let bytes = b"vcr";
        let out = decode_binary_param(bytes, PG_TYPE_VARCHAR).expect("ok");
        assert_eq!(out, "vcr");
    }

    /// TEXT binary with invalid UTF-8 bytes rejects with `BadValue`.
    #[test]
    fn t1bin_decode_text_invalid_utf8_rejects() {
        let bytes = [0xFF, 0xFE, 0xFD];
        let err = decode_binary_param(&bytes, PG_TYPE_TEXT).unwrap_err();
        match err {
            BinaryDecodeError::BadValue { type_oid, .. } => {
                assert_eq!(type_oid, PG_TYPE_TEXT);
            }
            other => panic!("expected BadValue, got {other:?}"),
        }
    }

    /// BYTEA binary `[0xDE, 0xAD]` → PG bytea text literal `\xdead`.
    #[test]
    fn t1bin_decode_bytea_hex() {
        let out = decode_binary_param(&[0xDE, 0xAD], PG_TYPE_BYTEA).expect("ok");
        assert_eq!(out, "\\xdead");
    }

    /// BYTEA empty bytes → bare `\x`.
    #[test]
    fn t1bin_decode_bytea_empty() {
        let out = decode_binary_param(&[], PG_TYPE_BYTEA).expect("ok");
        assert_eq!(out, "\\x");
    }

    /// TIMESTAMPTZ binary microseconds since PG epoch (2000-01-01 UTC)
    /// → ISO timestamp string with `+00` suffix. The reference value
    /// `2026-06-01 12:34:56.789012+00` is 832077296789012 µs after the
    /// PG epoch (precomputed from the same algorithm).
    #[test]
    fn t1bin_decode_timestamptz_iso() {
        // Compute the expected micros: seconds since PG epoch
        // (2000-01-01 00:00:00 UTC) for 2026-06-01 12:34:56 UTC, then
        // add 789012 µs of fractional seconds.
        let days_2000_to_2026_06_01 = days_between_2000_and(2026, 6, 1);
        let secs = days_2000_to_2026_06_01 as i64 * 86_400 + 12 * 3600 + 34 * 60 + 56;
        let micros = secs * 1_000_000 + 789_012;
        let bytes = micros.to_be_bytes();
        let out = decode_binary_param(&bytes, PG_TYPE_TIMESTAMPTZ).expect("ok");
        assert_eq!(out, "2026-06-01 12:34:56.789012+00");
    }

    /// TIMESTAMPTZ binary 0 µs since PG epoch → `2000-01-01 00:00:00.000000+00`.
    #[test]
    fn t1bin_decode_timestamptz_zero_is_pg_epoch() {
        let bytes = 0i64.to_be_bytes();
        let out = decode_binary_param(&bytes, PG_TYPE_TIMESTAMPTZ).expect("ok");
        assert_eq!(out, "2000-01-01 00:00:00.000000+00");
    }

    /// TIMESTAMPTZ binary wrong length rejects with `WrongLength`.
    #[test]
    fn t1bin_decode_timestamptz_wrong_length_rejects() {
        let err = decode_binary_param(&[0x00], PG_TYPE_TIMESTAMPTZ).unwrap_err();
        match err {
            BinaryDecodeError::WrongLength {
                type_oid,
                expected,
                actual,
            } => {
                assert_eq!(type_oid, PG_TYPE_TIMESTAMPTZ);
                assert_eq!(expected, 8);
                assert_eq!(actual, 1);
            }
            other => panic!("expected WrongLength, got {other:?}"),
        }
    }

    /// SP-PG-EXTQ-BIN-NUMERIC T3 — NUMERIC binary now decodes via the
    /// `extq::binary_numeric` codec. The all-zero header (no digits, no
    /// dscale, sign=POS) decodes to "0".
    #[test]
    fn t3num_decode_numeric_zero_through_codec() {
        let out = decode_binary_param(&[0x00; 8], PG_TYPE_NUMERIC).expect("ok");
        assert_eq!(out, "0");
    }

    /// SP-PG-EXTQ-BIN-NUMERIC T3 — NUMERIC binary `42` decodes through
    /// the codec.
    #[test]
    fn t3num_decode_numeric_42_through_codec() {
        let bytes = [
            0x00, 0x01, // ndigits=1
            0x00, 0x00, // weight=0
            0x00, 0x00, // sign=POS
            0x00, 0x00, // dscale=0
            0x00, 0x2A, // digit[0]=42
        ];
        let out = decode_binary_param(&bytes, PG_TYPE_NUMERIC).expect("ok");
        assert_eq!(out, "42");
    }

    /// SP-PG-EXTQ-BIN-NUMERIC-NAN-INF (2026-06-02) — NaN binary sign
    /// decodes to canonical `"NaN"` string via the dispatcher boundary.
    /// V1 rejected this; the NAN-INF arc lifts the rejection at both
    /// the codec layer AND this dispatcher boundary.
    #[test]
    fn t3num_decode_numeric_nan_returns_nan_string_through_codec() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00];
        let out = decode_binary_param(&bytes, PG_TYPE_NUMERIC).unwrap();
        assert_eq!(out, "NaN");
    }

    /// SP-PG-EXTQ-BIN-NUMERIC-NAN-INF: +Infinity binary sign decodes to
    /// canonical `"Infinity"` string via the dispatcher boundary.
    #[test]
    fn t3num_decode_numeric_pos_infinity_returns_infinity_string_through_codec() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0xD0, 0x00, 0x00, 0x00];
        let out = decode_binary_param(&bytes, PG_TYPE_NUMERIC).unwrap();
        assert_eq!(out, "Infinity");
    }

    /// SP-PG-EXTQ-BIN-NUMERIC-NAN-INF: -Infinity binary sign decodes to
    /// canonical `"-Infinity"` string via the dispatcher boundary.
    #[test]
    fn t3num_decode_numeric_neg_infinity_returns_minus_infinity_string_through_codec() {
        let bytes = [0x00, 0x00, 0x00, 0x00, 0xF0, 0x00, 0x00, 0x00];
        let out = decode_binary_param(&bytes, PG_TYPE_NUMERIC).unwrap();
        assert_eq!(out, "-Infinity");
    }

    /// SP-PG-EXTQ-BIN-NUMERIC T3 — out-of-range NUMERIC (≥10^18 integer
    /// part) rejects with `SP-PG-EXTQ-BIN-NUMERIC-BIGNUM` so operators
    /// can identify the bignum carve-out.
    #[test]
    fn t3num_decode_numeric_out_of_range_rejects_with_bignum_arc() {
        // Build a 5-base-10000-digit NUMERIC (10^16-ish * 10000 > 10^18).
        // ndigits=5, weight=4 (so digit[0] is at base-10000^4 = 10^16),
        // sign=POS, dscale=0, digits=[9999, 9999, 9999, 9999, 9999].
        let mut bytes = vec![
            0x00, 0x05, // ndigits=5
            0x00, 0x04, // weight=4
            0x00, 0x00, // sign=POS
            0x00, 0x00, // dscale=0
        ];
        for _ in 0..5 {
            bytes.extend_from_slice(&9999i16.to_be_bytes());
        }
        let err = decode_binary_param(&bytes, PG_TYPE_NUMERIC).unwrap_err();
        match err {
            BinaryDecodeError::Unsupported { type_oid, arc } => {
                assert_eq!(type_oid, PG_TYPE_NUMERIC);
                assert_eq!(arc, "SP-PG-EXTQ-BIN-NUMERIC-BIGNUM");
            }
            other => panic!("expected Unsupported (bignum), got {other:?}"),
        }
    }

    /// Unknown OIDs (e.g. JSONB 3802, UUID 2950) reject with the
    /// generic follow-up arc `SP-PG-EXTQ-BIN-EXTRA`.
    #[test]
    fn t1bin_decode_unknown_oid_returns_unsupported() {
        let err = decode_binary_param(&[0x00; 4], 3802 /* JSONB */).unwrap_err();
        match err {
            BinaryDecodeError::Unsupported { type_oid, arc } => {
                assert_eq!(type_oid, 3802);
                assert_eq!(arc, "SP-PG-EXTQ-BIN-EXTRA");
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    /// INT8 binary with wrong length (7 bytes) rejects with `WrongLength`.
    #[test]
    fn t1bin_decode_int8_wrong_length_rejects() {
        let err = decode_binary_param(&[0x00; 7], PG_TYPE_INT8).unwrap_err();
        match err {
            BinaryDecodeError::WrongLength {
                type_oid,
                expected,
                actual,
            } => {
                assert_eq!(type_oid, PG_TYPE_INT8);
                assert_eq!(expected, 8);
                assert_eq!(actual, 7);
            }
            other => panic!("expected WrongLength, got {other:?}"),
        }
    }

    /// `binary_format_supported_for_oid` accepts the V1 supported set
    /// (post SP-PG-EXTQ-BIN-NUMERIC T3 this now includes NUMERIC) and
    /// rejects everything else. Locked so a future drift (e.g.
    /// forgetting TIMESTAMPTZ in the helper) can't silently accept-
    /// then-fail-at-decode.
    #[test]
    fn t1bin_binary_format_supported_for_oid_matches_decoder() {
        // Supported (V1 BIN + V1 BIN-RESULTS + V2 BIN-NUMERIC T3).
        for oid in [
            PG_TYPE_BOOL,
            PG_TYPE_INT2,
            PG_TYPE_INT4,
            PG_TYPE_INT8,
            PG_TYPE_FLOAT4,
            PG_TYPE_FLOAT8,
            PG_TYPE_TEXT,
            PG_TYPE_VARCHAR,
            PG_TYPE_BYTEA,
            PG_TYPE_TIMESTAMPTZ,
            PG_TYPE_NUMERIC,
        ] {
            assert!(
                binary_format_supported_for_oid(oid),
                "OID {oid} should be supported",
            );
        }
        // Unsupported.
        assert!(!binary_format_supported_for_oid(3802 /* JSONB */));
        assert!(!binary_format_supported_for_oid(2950 /* UUID */));
        assert!(!binary_format_supported_for_oid(0));
    }

    /// `unsupported_binary_arc_for_oid` still names a follow-up arc for
    /// each unsupported OID. NUMERIC's `SP-PG-EXTQ-BIN-NUMERIC` arc
    /// remains as the naming helper since the OID hash hasn't moved,
    /// but the param decoder now accepts NUMERIC — operators reaching
    /// the helper now would be on the COPY-BIN-NUMERIC follow-up path.
    /// (Each call site has its own admission check; this helper still
    /// names the arc per OID.)
    #[test]
    fn t1bin_unsupported_binary_arc_naming() {
        assert_eq!(
            unsupported_binary_arc_for_oid(PG_TYPE_NUMERIC),
            "SP-PG-EXTQ-BIN-NUMERIC",
        );
        assert_eq!(
            unsupported_binary_arc_for_oid(3802),
            "SP-PG-EXTQ-BIN-EXTRA",
        );
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-EXTQ-BIN T2 KATs — `substitute_params` + `preprocess_params`
    // unified per-format dispatch. The text path is regression-locked
    // against the existing `substitute_text_format_params` to make
    // sure the refactor doesn't drift; the binary paths exercise each
    // V1-supported type's full pipeline (preprocess + render).
    // ───────────────────────────────────────────────────────────────────

    /// Backward-compat lock: `substitute_params` on an all-Text input
    /// produces byte-equal output to `substitute_text_format_params`
    /// on the same params. Locks against post-refactor drift.
    #[test]
    fn t2bin_substitute_params_text_path_byte_equal_to_text_only() {
        let sql = "INSERT INTO t (a, b, c) VALUES ($1, $2, $3)";
        let raw: Vec<Option<&[u8]>> =
            vec![Some(b"hi"), None, Some(b"O'Brien")];
        let via_text = substitute_text_format_params(sql, &raw).expect("ok");
        let prepared = vec![
            PreparedParam::Text(b"hi".to_vec()),
            PreparedParam::Null,
            PreparedParam::Text(b"O'Brien".to_vec()),
        ];
        let via_unified = substitute_params(sql, &prepared).expect("ok");
        assert_eq!(via_text, via_unified);
    }

    /// `Raw` variant splices verbatim — no quoting, no escaping. The
    /// caller (preprocess_params) is responsible for the SQL shape.
    #[test]
    fn t2bin_substitute_params_raw_splices_verbatim() {
        let sql = "SELECT $1, $2";
        let prepared = vec![
            PreparedParam::Raw("42".to_string()),
            PreparedParam::Raw("'\\xdead'::bytea".to_string()),
        ];
        let out = substitute_params(sql, &prepared).expect("ok");
        assert_eq!(out, "SELECT 42, '\\xdead'::bytea");
    }

    /// `preprocess_params` of a single INT8 binary value with the
    /// correct OID hint produces a `Raw("100")` (bare integer literal,
    /// no quotes).
    #[test]
    fn t2bin_preprocess_int8_binary_renders_as_raw_integer() {
        let raw_bytes = 100i64.to_be_bytes();
        let params: Vec<Option<&[u8]>> = vec![Some(&raw_bytes[..])];
        let formats = vec![crate::proto::FORMAT_CODE_BINARY];
        let oids = vec![PG_TYPE_INT8];
        let prepared = preprocess_params(&params, &formats, &oids).expect("ok");
        assert_eq!(prepared, vec![PreparedParam::Raw("100".to_string())]);
    }

    /// `preprocess_params` of a TEXT binary value with an embedded `'`
    /// routes through the Text variant so the substitute layer's
    /// `'`→`''` doubling applies.
    #[test]
    fn t2bin_preprocess_text_binary_routes_through_text_variant_for_escape() {
        let payload = b"O'Brien";
        let params: Vec<Option<&[u8]>> = vec![Some(payload)];
        let formats = vec![crate::proto::FORMAT_CODE_BINARY];
        let oids = vec![PG_TYPE_TEXT];
        let prepared = preprocess_params(&params, &formats, &oids).expect("ok");
        assert_eq!(prepared, vec![PreparedParam::Text(b"O'Brien".to_vec())]);
        // Confirm the substitute layer doubles the embedded quote.
        let out = substitute_params("SELECT $1", &prepared).expect("ok");
        assert_eq!(out, "SELECT 'O''Brien'");
    }

    /// `preprocess_params` of a BYTEA binary value wraps the
    /// `\xHEX` literal with the `::bytea` cast suffix so the SQL
    /// parser sees a properly-typed literal.
    #[test]
    fn t2bin_preprocess_bytea_binary_wraps_with_cast_suffix() {
        let payload = [0xCAu8, 0xFE];
        let params: Vec<Option<&[u8]>> = vec![Some(&payload[..])];
        let formats = vec![crate::proto::FORMAT_CODE_BINARY];
        let oids = vec![PG_TYPE_BYTEA];
        let prepared = preprocess_params(&params, &formats, &oids).expect("ok");
        assert_eq!(
            prepared,
            vec![PreparedParam::Raw("'\\xcafe'::bytea".to_string())]
        );
    }

    /// `preprocess_params` of a TIMESTAMPTZ binary value (0 µs since
    /// PG epoch) wraps the ISO literal with `::timestamptz`.
    #[test]
    fn t2bin_preprocess_timestamptz_binary_wraps_with_cast_suffix() {
        let payload = 0i64.to_be_bytes();
        let params: Vec<Option<&[u8]>> = vec![Some(&payload[..])];
        let formats = vec![crate::proto::FORMAT_CODE_BINARY];
        let oids = vec![PG_TYPE_TIMESTAMPTZ];
        let prepared = preprocess_params(&params, &formats, &oids).expect("ok");
        assert_eq!(
            prepared,
            vec![PreparedParam::Raw(
                "'2000-01-01 00:00:00.000000+00'::timestamptz".to_string()
            )]
        );
    }

    /// `preprocess_params` of a NULL value at a binary-format
    /// position renders as `PreparedParam::Null` regardless of the
    /// declared type OID (PG semantics: length=-1 sentinel is format-
    /// agnostic).
    #[test]
    fn t2bin_preprocess_null_binary_renders_as_null_regardless_of_oid() {
        let params: Vec<Option<&[u8]>> = vec![None];
        let formats = vec![crate::proto::FORMAT_CODE_BINARY];
        let oids = vec![PG_TYPE_INT8];
        let prepared = preprocess_params(&params, &formats, &oids).expect("ok");
        assert_eq!(prepared, vec![PreparedParam::Null]);
    }

    /// `preprocess_params` of an invalid binary payload (BOOL byte
    /// neither 0x00 nor 0x01) propagates as `SubstituteError::
    /// BinaryDecode { position: 0, reason: ... }`.
    #[test]
    fn t2bin_preprocess_invalid_binary_payload_returns_substitute_error() {
        let params: Vec<Option<&[u8]>> = vec![Some(&[0x02u8][..])];
        let formats = vec![crate::proto::FORMAT_CODE_BINARY];
        let oids = vec![PG_TYPE_BOOL];
        let err = preprocess_params(&params, &formats, &oids).unwrap_err();
        match err {
            SubstituteError::BinaryDecode { position, .. } => {
                assert_eq!(position, 0);
            }
            other => panic!("expected BinaryDecode, got {other:?}"),
        }
    }

    /// `effective_format_code` honors PG length conventions:
    /// 0 codes → text, 1 code → all-same, N codes → per-position.
    #[test]
    fn t2bin_effective_format_code_length_conventions() {
        // 0 codes — every position is text.
        assert_eq!(
            effective_format_code(&[], 0),
            crate::proto::FORMAT_CODE_TEXT
        );
        assert_eq!(
            effective_format_code(&[], 99),
            crate::proto::FORMAT_CODE_TEXT
        );
        // 1 code — same code applies everywhere.
        assert_eq!(
            effective_format_code(&[crate::proto::FORMAT_CODE_BINARY], 0),
            crate::proto::FORMAT_CODE_BINARY
        );
        assert_eq!(
            effective_format_code(&[crate::proto::FORMAT_CODE_BINARY], 5),
            crate::proto::FORMAT_CODE_BINARY
        );
        // N codes — per-position.
        let formats = vec![
            crate::proto::FORMAT_CODE_TEXT,
            crate::proto::FORMAT_CODE_BINARY,
            crate::proto::FORMAT_CODE_TEXT,
        ];
        assert_eq!(
            effective_format_code(&formats, 0),
            crate::proto::FORMAT_CODE_TEXT
        );
        assert_eq!(
            effective_format_code(&formats, 1),
            crate::proto::FORMAT_CODE_BINARY
        );
        assert_eq!(
            effective_format_code(&formats, 2),
            crate::proto::FORMAT_CODE_TEXT
        );
        // Out-of-range falls back to text.
        assert_eq!(
            effective_format_code(&formats, 99),
            crate::proto::FORMAT_CODE_TEXT
        );
    }

    // ───────────────────────────────────────────────────────────────────
    // Test helpers — used by TIMESTAMPTZ KATs to compute days-between
    // dates without pulling in chrono.
    // ───────────────────────────────────────────────────────────────────

    /// Days between PG epoch (2000-01-01) and the given (y, m, d) in
    /// the proleptic Gregorian calendar. Uses the inverse of
    /// `civil_from_days` (Howard Hinnant's `days_from_civil`).
    fn days_between_2000_and(y: i64, m: u32, d: u32) -> i64 {
        days_from_civil(y, m, d) - days_from_civil(2000, 1, 1)
    }

    /// Howard Hinnant `days_from_civil`: convert (y, m, d) → days
    /// since 1970-01-01 in the proleptic Gregorian calendar.
    fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
        let y = if m <= 2 { y - 1 } else { y };
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = (y - era * 400) as u64; // [0, 399]
        let m = m as u64;
        let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + (d as u64) - 1; // [0, 365]
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
        era * 146_097 + doe as i64 - 719_468
    }

    // ───────────────────────────────────────────────────────────────────
    // SP-PG-EXTQ-PARSED T3 KATs — `preprocess_typed_params` classifies
    // wire params into `Option<Value>` slots ready for kessel-sql's
    // `compile_with_params`, OR returns `None` to signal fallback to
    // the text-substitution path. The default V1 disposition keeps the
    // text path as default; these KATs lock the classifier's per-OID
    // routing so a future flip can rely on the contract.
    // ───────────────────────────────────────────────────────────────────

    /// pgJDBC's `setInt(1, 42)` arrives as text-format bytes `b"42"`
    /// with INT8 OID. Classifier returns `Some(Value::Int(42))`.
    #[test]
    fn t3parsed_preprocess_typed_text_int_returns_value_int() {
        let params: Vec<Option<&[u8]>> = vec![Some(b"42")];
        let formats: Vec<u16> = vec![]; // 0 codes = all text
        let oids: Vec<u32> = vec![PG_TYPE_INT8];
        let typed = preprocess_typed_params(&params, &formats, &oids)
            .expect("typed path accepts the int param");
        assert_eq!(
            typed,
            vec![Some(kessel_codec::Value::Int(42))]
        );
    }

    /// psycopg2's `cursor.execute("...", (b"hello",))` arrives as
    /// text-format bytes with TEXT OID. Classifier returns
    /// `Some(Value::Blob(b"hello"))`.
    #[test]
    fn t3parsed_preprocess_typed_text_blob_returns_value_blob() {
        let params: Vec<Option<&[u8]>> = vec![Some(b"hello")];
        let formats: Vec<u16> = vec![];
        let oids: Vec<u32> = vec![PG_TYPE_TEXT];
        let typed = preprocess_typed_params(&params, &formats, &oids)
            .expect("typed path accepts the text param");
        assert_eq!(
            typed,
            vec![Some(kessel_codec::Value::Blob(b"hello".to_vec()))]
        );
    }

    /// NULL (length=-1) → `Some(None)`. Format-agnostic.
    #[test]
    fn t3parsed_preprocess_typed_null_returns_some_none() {
        let params: Vec<Option<&[u8]>> = vec![None];
        let formats: Vec<u16> = vec![crate::proto::FORMAT_CODE_BINARY];
        let oids: Vec<u32> = vec![PG_TYPE_INT8];
        let typed = preprocess_typed_params(&params, &formats, &oids)
            .expect("typed path accepts NULL");
        assert_eq!(typed, vec![None]);
    }

    /// Binary-format INT8 decodes to a typed `Value::Int`.
    #[test]
    fn t3parsed_preprocess_typed_binary_int8_returns_value_int() {
        let raw = 100i64.to_be_bytes();
        let params: Vec<Option<&[u8]>> = vec![Some(&raw[..])];
        let formats: Vec<u16> = vec![crate::proto::FORMAT_CODE_BINARY];
        let oids: Vec<u32> = vec![PG_TYPE_INT8];
        let typed = preprocess_typed_params(&params, &formats, &oids)
            .expect("typed path accepts binary INT8");
        assert_eq!(typed, vec![Some(kessel_codec::Value::Int(100))]);
    }

    /// FLOAT8 falls back to the text-substitution path because the
    /// kessel-codec `Value` enum doesn't carry a float variant in V1.
    /// `preprocess_typed_params` returns `None` overall so the
    /// dispatcher routes through `preprocess_params` (the text path).
    #[test]
    fn t3parsed_preprocess_typed_float8_falls_back_to_text_path() {
        let raw = 1.5f64.to_be_bytes();
        let params: Vec<Option<&[u8]>> = vec![Some(&raw[..])];
        let formats: Vec<u16> = vec![crate::proto::FORMAT_CODE_BINARY];
        let oids: Vec<u32> = vec![PG_TYPE_FLOAT8];
        let typed = preprocess_typed_params(&params, &formats, &oids);
        assert!(
            typed.is_none(),
            "FLOAT8 should fall back to text path; got {typed:?}"
        );
    }

    /// TIMESTAMPTZ falls back too (cast-wrapper is text-path-only in
    /// V1).
    #[test]
    fn t3parsed_preprocess_typed_timestamptz_falls_back_to_text_path() {
        let raw = 0i64.to_be_bytes();
        let params: Vec<Option<&[u8]>> = vec![Some(&raw[..])];
        let formats: Vec<u16> = vec![crate::proto::FORMAT_CODE_BINARY];
        let oids: Vec<u32> = vec![PG_TYPE_TIMESTAMPTZ];
        let typed = preprocess_typed_params(&params, &formats, &oids);
        assert!(typed.is_none());
    }

    /// Mixed-types Bind where ONE param is typed-path-eligible (INT8)
    /// and another isn't (FLOAT8): classifier returns `None` overall
    /// so the WHOLE Bind routes through the text path. Prevents the
    /// dispatcher from running a half-typed half-text shape that
    /// would need two code paths.
    #[test]
    fn t3parsed_preprocess_typed_mixed_one_unsupported_returns_none() {
        let int8 = 42i64.to_be_bytes();
        let f8 = 1.5f64.to_be_bytes();
        let params: Vec<Option<&[u8]>> = vec![Some(&int8[..]), Some(&f8[..])];
        let formats: Vec<u16> = vec![
            crate::proto::FORMAT_CODE_BINARY,
            crate::proto::FORMAT_CODE_BINARY,
        ];
        let oids: Vec<u32> = vec![PG_TYPE_INT8, PG_TYPE_FLOAT8];
        let typed = preprocess_typed_params(&params, &formats, &oids);
        assert!(typed.is_none(), "mixed-types should fall back");
    }

    /// HEADLINE SECURITY KAT — the quote-injection attempt at the
    /// gateway boundary. `preprocess_typed_params` accepts the
    /// payload as a `Value::Blob`; downstream `kessel_sql::
    /// compile_with_params` will pass it through to the program as a
    /// typed operand. The DROP TABLE bytes never enter the SQL text.
    #[test]
    fn t3parsed_preprocess_typed_quote_injection_payload_becomes_value_blob() {
        let payload = b"'; DROP TABLE t; --";
        let params: Vec<Option<&[u8]>> = vec![Some(payload)];
        let formats: Vec<u16> = vec![];
        let oids: Vec<u32> = vec![PG_TYPE_TEXT];
        let typed = preprocess_typed_params(&params, &formats, &oids)
            .expect("typed path accepts the text param");
        assert_eq!(
            typed,
            vec![Some(kessel_codec::Value::Blob(payload.to_vec()))]
        );
        // End-to-end: feed through kessel-sql's compile_with_params
        // and verify the program carries the payload bytes verbatim
        // (the same security shape as the kessel-sql T2 KAT, but
        // routed through the gateway's classifier).
        use kessel_catalog::Catalog;
        use kessel_proto::Op;
        // Build a minimal catalog with a `name CHAR(64)` column so
        // the SELECT compiles. The KesselDB CREATE TABLE compiler
        // populates the catalog directly; here we hand-assemble a
        // Catalog with one ObjectType via `from_def` (the public
        // helper for synthetic schema construction).
        let mut ot = kessel_catalog::ObjectType::from_def(
            "t".to_string(),
            vec![
                kessel_catalog::Field {
                    field_id: 1,
                    name: "id".to_string(),
                    kind: kessel_catalog::FieldKind::I64,
                    nullable: false,
                },
                kessel_catalog::Field {
                    field_id: 2,
                    name: "name".to_string(),
                    kind: kessel_catalog::FieldKind::Char(64),
                    nullable: false,
                },
            ],
        );
        ot.type_id = 1;
        let mut cat = Catalog::default();
        cat.types.push(ot);
        let op = kessel_sql::compile_with_params(
            "SELECT * FROM t WHERE name = $1",
            &cat,
            &typed,
        )
        .expect("compile_with_params ok");
        match op {
            Op::QueryRows { program, .. } => {
                let prog_has_payload =
                    program.windows(payload.len()).any(|w| w == payload);
                assert!(
                    prog_has_payload,
                    "the gateway-classified Value::Blob payload must \
                     reach the program operand verbatim; got program \
                     bytes = {program:?}"
                );
            }
            other => panic!(
                "expected Op::QueryRows; got {other:?} (SECURITY \
                 REGRESSION — bound bytes may have been re-parsed as \
                 SQL)"
            ),
        }
    }

    /// Empty params input → empty typed slice. Defensive against the
    /// no-`$N` case.
    #[test]
    fn t3parsed_preprocess_typed_empty_params_returns_empty_vec() {
        let typed = preprocess_typed_params(&[], &[], &[])
            .expect("empty params is a trivial typed Bind");
        assert!(typed.is_empty());
    }

    /// pgJDBC `setBoolean(1, true)` text-format `"t"` with BOOL OID
    /// → `Value::Uint(1)`.
    #[test]
    fn t3parsed_preprocess_typed_text_bool_t_returns_value_uint_one() {
        let params: Vec<Option<&[u8]>> = vec![Some(b"t")];
        let formats: Vec<u16> = vec![];
        let oids: Vec<u32> = vec![PG_TYPE_BOOL];
        let typed = preprocess_typed_params(&params, &formats, &oids)
            .expect("BOOL text 't' is typed-eligible");
        assert_eq!(typed, vec![Some(kessel_codec::Value::Uint(1))]);
    }

    /// pgJDBC `setBoolean(1, false)` text-format `"false"` → `Value::Uint(0)`.
    #[test]
    fn t3parsed_preprocess_typed_text_bool_false_returns_value_uint_zero() {
        let params: Vec<Option<&[u8]>> = vec![Some(b"false")];
        let formats: Vec<u16> = vec![];
        let oids: Vec<u32> = vec![PG_TYPE_BOOL];
        let typed = preprocess_typed_params(&params, &formats, &oids)
            .expect("BOOL text 'false' is typed-eligible");
        assert_eq!(typed, vec![Some(kessel_codec::Value::Uint(0))]);
    }

    /// NUMERIC falls back (the text-substitution path's quoted-decimal
    /// shape is the only one that compiles cleanly today).
    #[test]
    fn t3parsed_preprocess_typed_numeric_falls_back_to_text_path() {
        let params: Vec<Option<&[u8]>> = vec![Some(b"3.14")];
        let formats: Vec<u16> = vec![];
        let oids: Vec<u32> = vec![PG_TYPE_NUMERIC];
        let typed = preprocess_typed_params(&params, &formats, &oids);
        assert!(typed.is_none());
    }

    // ─────────────────────────────────────────────────────────────────
    // SP-PG-EXTQ-PARSED-BYTEA-TYPED T2 KATs — BYTEA-binary route admits
    // arbitrary raw bytes through the typed path (post-bug-fix shape).
    // The prior V1 disposition returned `None` and forced the text-
    // substitution fallback because `kessel_sql::rewrite_param_tokens`
    // corrupted non-UTF8 bytes through `String::from_utf8_lossy`. With
    // the fix in `rewrite_param_tokens` (`Tok::Bytes` + `Lit::Bytes`),
    // BYTEA-binary is now uniformly typed.
    // ─────────────────────────────────────────────────────────────────

    /// BYTEA binary `[0x00, 0xFF]` → `Some(Value::Blob([0x00, 0xFF]))`.
    /// The prior shape returned `None` (typed-path carve-out); now the
    /// typed path accepts BYTEA verbatim.
    #[test]
    fn t2byteatyped_preprocess_binary_bytea_returns_value_blob() {
        let raw: Vec<u8> = vec![0x00, 0xFF];
        let params: Vec<Option<&[u8]>> = vec![Some(&raw[..])];
        let formats: Vec<u16> = vec![crate::proto::FORMAT_CODE_BINARY];
        let oids: Vec<u32> = vec![PG_TYPE_BYTEA];
        let typed = preprocess_typed_params(&params, &formats, &oids)
            .expect("BYTEA binary now flows through typed path");
        assert_eq!(typed, vec![Some(kessel_codec::Value::Blob(raw))]);
    }

    /// BYTEA binary `[0xDE, 0xAD, 0xBE, 0xEF]` (a common non-UTF8
    /// payload) round-trips as `Value::Blob` byte-equal.
    #[test]
    fn t2byteatyped_preprocess_binary_bytea_non_utf8_payload_preserved() {
        let raw: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let params: Vec<Option<&[u8]>> = vec![Some(&raw[..])];
        let formats: Vec<u16> = vec![crate::proto::FORMAT_CODE_BINARY];
        let oids: Vec<u32> = vec![PG_TYPE_BYTEA];
        let typed = preprocess_typed_params(&params, &formats, &oids)
            .expect("BYTEA binary now typed-eligible");
        assert_eq!(
            typed,
            vec![Some(kessel_codec::Value::Blob(raw))],
        );
    }

    /// BYTEA binary with empty bytes `[]` → `Some(Value::Blob([]))`.
    /// Edge case: the empty-payload shape still routes through the
    /// typed path (previously fell back to text-substitute path).
    #[test]
    fn t2byteatyped_preprocess_binary_bytea_empty_payload() {
        let raw: Vec<u8> = vec![];
        let params: Vec<Option<&[u8]>> = vec![Some(&raw[..])];
        let formats: Vec<u16> = vec![crate::proto::FORMAT_CODE_BINARY];
        let oids: Vec<u32> = vec![PG_TYPE_BYTEA];
        let typed = preprocess_typed_params(&params, &formats, &oids)
            .expect("empty BYTEA accepted");
        assert_eq!(
            typed,
            vec![Some(kessel_codec::Value::Blob(raw))],
        );
    }

    /// End-to-end: gateway classifier + kessel-sql `compile_with_params`
    /// round-trips arbitrary non-UTF8 bytes through to the program
    /// operand. This is the gateway-layer regression-lock for the
    /// `from_utf8_lossy` bug fix.
    #[test]
    fn t2byteatyped_gateway_bytea_binary_non_utf8_round_trip_to_program() {
        let raw: Vec<u8> = vec![0xFF, 0xFE, 0xFD, 0x00, 0x80];
        let params: Vec<Option<&[u8]>> = vec![Some(&raw[..])];
        let formats: Vec<u16> = vec![crate::proto::FORMAT_CODE_BINARY];
        let oids: Vec<u32> = vec![PG_TYPE_BYTEA];
        let typed = preprocess_typed_params(&params, &formats, &oids)
            .expect("BYTEA binary typed-eligible");
        // End-to-end via kessel-sql compile_with_params.
        use kessel_catalog::Catalog;
        use kessel_proto::Op;
        let mut ot = kessel_catalog::ObjectType::from_def(
            "b".to_string(),
            vec![
                kessel_catalog::Field {
                    field_id: 1,
                    name: "id".to_string(),
                    kind: kessel_catalog::FieldKind::I64,
                    nullable: false,
                },
                kessel_catalog::Field {
                    field_id: 2,
                    name: "data".to_string(),
                    kind: kessel_catalog::FieldKind::Bytes(8),
                    nullable: false,
                },
            ],
        );
        ot.type_id = 1;
        let mut cat = Catalog::default();
        cat.types.push(ot);
        let op = kessel_sql::compile_with_params(
            "SELECT * FROM b WHERE data = $1",
            &cat,
            &typed,
        )
        .expect("compile_with_params ok");
        match op {
            Op::QueryRows { program, .. } => {
                let has = program.windows(raw.len()).any(|w| w == raw.as_slice());
                assert!(
                    has,
                    "expected non-UTF8 BYTEA bytes {raw:?} to appear \
                     verbatim in the program operand; got {program:?}",
                );
                // The UTF-8 replacement bytes (lossy regression) must
                // NOT appear.
                let lossy = program.windows(3).any(|w| w == [0xEF, 0xBF, 0xBD]);
                assert!(
                    !lossy,
                    "UTF-8 replacement bytes 0xEF 0xBF 0xBD appeared in \
                     program — indicates the lossy-UTF8 regression took \
                     effect; got {program:?}",
                );
            }
            other => panic!("expected QueryRows; got {other:?}"),
        }
    }
}
