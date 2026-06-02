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
        PG_TYPE_NUMERIC => Err(BinaryDecodeError::Unsupported {
            type_oid,
            arc: "SP-PG-EXTQ-BIN-NUMERIC",
        }),
        _ => Err(BinaryDecodeError::Unsupported {
            type_oid,
            arc: "SP-PG-EXTQ-BIN-EXTRA",
        }),
    }
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

    /// NUMERIC binary explicitly rejects with the named follow-up arc
    /// `SP-PG-EXTQ-BIN-NUMERIC` so operators can grep for the gap.
    #[test]
    fn t1bin_decode_numeric_returns_unsupported_with_followup_arc() {
        let err = decode_binary_param(&[0x00; 8], PG_TYPE_NUMERIC).unwrap_err();
        match err {
            BinaryDecodeError::Unsupported { type_oid, arc } => {
                assert_eq!(type_oid, PG_TYPE_NUMERIC);
                assert_eq!(arc, "SP-PG-EXTQ-BIN-NUMERIC");
            }
            other => panic!("expected Unsupported, got {other:?}"),
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
    /// and rejects NUMERIC + unknown OIDs. Locked so a future drift
    /// (e.g. forgetting TIMESTAMPTZ in the helper) can't silently
    /// accept-then-fail-at-decode.
    #[test]
    fn t1bin_binary_format_supported_for_oid_matches_decoder() {
        // Supported.
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
        ] {
            assert!(
                binary_format_supported_for_oid(oid),
                "OID {oid} should be supported",
            );
        }
        // Unsupported.
        assert!(!binary_format_supported_for_oid(PG_TYPE_NUMERIC));
        assert!(!binary_format_supported_for_oid(3802 /* JSONB */));
        assert!(!binary_format_supported_for_oid(2950 /* UUID */));
        assert!(!binary_format_supported_for_oid(0));
    }

    /// `unsupported_binary_arc_for_oid` names the right follow-up arc
    /// for both the NUMERIC carve-out and the generic "extra types"
    /// bucket.
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
}
