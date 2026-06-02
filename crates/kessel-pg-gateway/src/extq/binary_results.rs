//! SP-PG-EXTQ-BIN-RESULTS — binary-format DataRow + RowDescription
//! post-processing per spec §3 (this arc — `docs/superpowers/specs/
//! 2026-06-01-kesseldb-sppgextq-bin-results-design.md`).
//!
//! **T1 status (this commit):** the pure helpers — `encode_binary_value`
//! per-OID encoder, `rewrite_data_row_with_formats` row rewriter, and
//! `rewrite_row_description_with_formats` field-format-code rewriter
//! + the `extract_type_oids_from_row_description` parser the dispatcher
//! uses to thread the column OIDs from the existing RowDescription bytes
//! into the rewrite step. No dispatcher / Execute changes yet — T2
//! wires the rewrite into `dispatch_execute`.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-06-01-kesseldb-sppgextq-bin-results-design.md`
//!
//! ## What this module does
//!
//! - `encode_binary_value(text, type_oid) -> Result<Vec<u8>, BinaryEncodeError>`
//!   — takes the text-format wire bytes the existing `render_pg_text`
//!   would emit, plus the PG type OID, and produces the PG binary
//!   wire bytes per PG §55.8.
//! - `rewrite_data_row_with_formats(frame, formats, type_oids)` — parses
//!   a complete `D` DataRow wire frame, re-encodes per-column per the
//!   format codes, and emits a fresh `D` frame.
//! - `rewrite_row_description_with_formats(frame, formats)` — flips
//!   per-field `format_code` slot in a `T` RowDescription wire frame
//!   to match the portal's per-column requested format.
//! - `extract_type_oids_from_row_description(frame)` — parses a `T`
//!   RowDescription frame and returns the per-field `type_oid` slot
//!   value as a `Vec<u32>`. The Execute dispatcher uses this to thread
//!   the OIDs into `rewrite_data_row_with_formats` without re-consulting
//!   the engine.
//!
//! ## What this module does NOT do (T2+ / V2)
//!
//! - It does NOT call into `dispatch_execute` (T2 wires that).
//! - It does NOT encode NUMERIC binary (V2 SP-PG-EXTQ-BIN-NUMERIC).
//! - It does NOT encode JSONB / UUID / ARRAY (V2 SP-PG-EXTQ-BIN-EXTRA).
//! - It does NOT touch the simple-query `dispatch_query` path — that
//!   stays text-only forever (matches PG itself; simple-query has no
//!   `result_formats`).

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::proto::{
    BE_DATA_ROW, BE_ROW_DESCRIPTION, FORMAT_CODE_TEXT, PG_TYPE_BOOL, PG_TYPE_BYTEA,
    PG_TYPE_FLOAT4, PG_TYPE_FLOAT8, PG_TYPE_INT2, PG_TYPE_INT4, PG_TYPE_INT8, PG_TYPE_NUMERIC,
    PG_TYPE_TEXT, PG_TYPE_TIMESTAMPTZ, PG_TYPE_VARCHAR,
};

/// SP-PG-EXTQ-BIN-RESULTS T1 — errors from `encode_binary_value`. Each
/// carries enough context for the dispatcher to render a precise
/// client-facing SQLSTATE + message (the unsupported variants name the
/// V2 follow-up arc).
#[derive(Debug, PartialEq, Eq)]
pub enum BinaryEncodeError {
    /// The text bytes couldn't be parsed back into the declared type
    /// (e.g. INT4 text that isn't an integer, BOOL text that isn't
    /// `t`/`f`). Maps to SQLSTATE `22P02 invalid_text_representation`.
    BadValue { type_oid: u32, reason: String },
    /// V1 doesn't support this type in binary format yet. Carries the
    /// OID + the V2 follow-up arc name. Maps to SQLSTATE `0A000
    /// feature_not_supported`.
    Unsupported {
        type_oid: u32,
        arc: &'static str,
    },
}

/// SP-PG-EXTQ-BIN-RESULTS T1 — encode a text-rendered column value as
/// PG binary per §55.8.
///
/// **Inputs:** `text` is the same bytes `render_pg_text` would emit
/// for the column's text format (e.g. `"42"` for INT4, `"t"`/`"f"` for
/// BOOL, `"\\x<hex>"` for BYTEA, `"2026-06-01 12:00:00.000000+00"` for
/// TIMESTAMPTZ). `type_oid` is the column's PG type OID from
/// RowDescription.
///
/// **Per-type contracts** (per PG §55.8 binary representations — the
/// SYMMETRIC inverse of `extq::substitute::decode_binary_param`):
///
/// | type | OID | input text | output bytes |
/// |---|---|---|---|
/// | BOOL | 16 | `t` / `f` | 1 byte 0x01 / 0x00 |
/// | BYTEA | 17 | `\xHEX` | raw bytes (HEX decoded) |
/// | INT2 | 21 | decimal | 2 bytes BE i16 |
/// | INT4 | 23 | decimal | 4 bytes BE i32 |
/// | INT8 | 20 | decimal | 8 bytes BE i64 |
/// | TEXT | 25 | UTF-8 | UTF-8 (pass-through) |
/// | VARCHAR | 1043 | UTF-8 | UTF-8 (pass-through) |
/// | FLOAT4 | 700 | decimal | 4 bytes IEEE-754 BE |
/// | FLOAT8 | 701 | decimal | 8 bytes IEEE-754 BE |
/// | TIMESTAMPTZ | 1184 | ISO `YYYY-MM-DD HH:MM:SS.ffffff+00` | 8 bytes BE i64 µs since 2000-01-01 UTC |
/// | NUMERIC | 1700 | decimal | `BinaryEncodeError::Unsupported { arc: "SP-PG-EXTQ-BIN-NUMERIC" }` |
/// | other | any | any | `BinaryEncodeError::Unsupported { arc: "SP-PG-EXTQ-BIN-EXTRA" }` |
///
/// **Why text input?** The text bytes are what KesselDB's
/// `render_pg_text` (the canonical text-format renderer the existing
/// DataRow path uses) emits. Re-using them keeps the post-processor a
/// pure text→binary transform per column, sidesteps any need to plumb
/// the original `Value` through the dispatch pipeline, and locks the
/// invariant that `decode_binary_param(encode_binary_value(text)) ==
/// text` round-trips for every supported scalar.
pub fn encode_binary_value(
    text: &[u8],
    type_oid: u32,
) -> Result<Vec<u8>, BinaryEncodeError> {
    match type_oid {
        PG_TYPE_BOOL => encode_bool(text),
        PG_TYPE_INT2 => encode_int::<2>(text, type_oid),
        PG_TYPE_INT4 => encode_int::<4>(text, type_oid),
        PG_TYPE_INT8 => encode_int::<8>(text, type_oid),
        PG_TYPE_FLOAT4 => encode_float4(text),
        PG_TYPE_FLOAT8 => encode_float8(text),
        PG_TYPE_TEXT | PG_TYPE_VARCHAR => Ok(text.to_vec()),
        PG_TYPE_BYTEA => encode_bytea(text),
        PG_TYPE_TIMESTAMPTZ => encode_timestamptz(text),
        PG_TYPE_NUMERIC => Err(BinaryEncodeError::Unsupported {
            type_oid,
            arc: "SP-PG-EXTQ-BIN-NUMERIC",
        }),
        _ => Err(BinaryEncodeError::Unsupported {
            type_oid,
            arc: "SP-PG-EXTQ-BIN-EXTRA",
        }),
    }
}

/// True iff `type_oid` is one V1's `encode_binary_value` accepts. Same
/// set as `substitute::binary_format_supported_for_oid` — the param +
/// result paths support the same OIDs by construction.
pub fn binary_result_supported_for_oid(type_oid: u32) -> bool {
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

/// V2 follow-up arc name for a given unsupported binary result OID.
pub fn unsupported_binary_result_arc(type_oid: u32) -> &'static str {
    if type_oid == PG_TYPE_NUMERIC {
        "SP-PG-EXTQ-BIN-NUMERIC"
    } else {
        "SP-PG-EXTQ-BIN-EXTRA"
    }
}

fn encode_bool(text: &[u8]) -> Result<Vec<u8>, BinaryEncodeError> {
    match text {
        b"t" => Ok(vec![0x01]),
        b"f" => Ok(vec![0x00]),
        // Tolerate the spelled-out form (the param decoder emits
        // `true`/`false` literally) so the round-trip identity holds.
        b"true" => Ok(vec![0x01]),
        b"false" => Ok(vec![0x00]),
        _ => Err(BinaryEncodeError::BadValue {
            type_oid: PG_TYPE_BOOL,
            reason: format!(
                "BOOL text must be 't'/'f' or 'true'/'false' (got {:?})",
                String::from_utf8_lossy(text)
            ),
        }),
    }
}

/// Generic signed-integer encoder. `BYTES` is the wire width (2/4/8).
fn encode_int<const BYTES: usize>(
    text: &[u8],
    type_oid: u32,
) -> Result<Vec<u8>, BinaryEncodeError> {
    let s = std::str::from_utf8(text).map_err(|_| BinaryEncodeError::BadValue {
        type_oid,
        reason: format!(
            "integer text must be valid UTF-8 (got {:?})",
            String::from_utf8_lossy(text)
        ),
    })?;
    // Parse as i64 then narrow — covers all three widths.
    let n: i64 = s.parse().map_err(|e: std::num::ParseIntError| {
        BinaryEncodeError::BadValue {
            type_oid,
            reason: format!("integer text {s:?}: {e}"),
        }
    })?;
    match BYTES {
        2 => {
            let v = i16::try_from(n).map_err(|_| BinaryEncodeError::BadValue {
                type_oid,
                reason: format!("INT2 value {n} out of range [i16::MIN, i16::MAX]"),
            })?;
            Ok(v.to_be_bytes().to_vec())
        }
        4 => {
            let v = i32::try_from(n).map_err(|_| BinaryEncodeError::BadValue {
                type_oid,
                reason: format!("INT4 value {n} out of range [i32::MIN, i32::MAX]"),
            })?;
            Ok(v.to_be_bytes().to_vec())
        }
        8 => Ok(n.to_be_bytes().to_vec()),
        _ => unreachable!("encode_int<const BYTES> only callable for 2/4/8"),
    }
}

fn encode_float4(text: &[u8]) -> Result<Vec<u8>, BinaryEncodeError> {
    let s = std::str::from_utf8(text).map_err(|_| BinaryEncodeError::BadValue {
        type_oid: PG_TYPE_FLOAT4,
        reason: "FLOAT4 text must be valid UTF-8".to_string(),
    })?;
    let f: f32 = s.parse().map_err(|e: std::num::ParseFloatError| {
        BinaryEncodeError::BadValue {
            type_oid: PG_TYPE_FLOAT4,
            reason: format!("FLOAT4 text {s:?}: {e}"),
        }
    })?;
    Ok(f.to_be_bytes().to_vec())
}

fn encode_float8(text: &[u8]) -> Result<Vec<u8>, BinaryEncodeError> {
    let s = std::str::from_utf8(text).map_err(|_| BinaryEncodeError::BadValue {
        type_oid: PG_TYPE_FLOAT8,
        reason: "FLOAT8 text must be valid UTF-8".to_string(),
    })?;
    let f: f64 = s.parse().map_err(|e: std::num::ParseFloatError| {
        BinaryEncodeError::BadValue {
            type_oid: PG_TYPE_FLOAT8,
            reason: format!("FLOAT8 text {s:?}: {e}"),
        }
    })?;
    Ok(f.to_be_bytes().to_vec())
}

/// Decode the PG bytea text representation `\xHEX` back to raw bytes.
/// Lowercase + uppercase hex both accepted. The single backslash + `x`
/// prefix is mandatory (PG's "hex" output mode; KesselDB's
/// `render_pg_text` always emits this shape for `FieldKind::Bytes`).
fn encode_bytea(text: &[u8]) -> Result<Vec<u8>, BinaryEncodeError> {
    // Empty bytea text is `\x` (just the prefix). Permit.
    if text.len() < 2 || text[0] != b'\\' || text[1] != b'x' {
        return Err(BinaryEncodeError::BadValue {
            type_oid: PG_TYPE_BYTEA,
            reason: format!(
                "BYTEA text must start with '\\x' (got {:?})",
                String::from_utf8_lossy(text)
            ),
        });
    }
    let hex = &text[2..];
    if hex.len() % 2 != 0 {
        return Err(BinaryEncodeError::BadValue {
            type_oid: PG_TYPE_BYTEA,
            reason: format!("BYTEA hex must have even length (got {} chars)", hex.len()),
        });
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    for pair in hex.chunks_exact(2) {
        let hi = decode_hex_nibble(pair[0])?;
        let lo = decode_hex_nibble(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn decode_hex_nibble(b: u8) -> Result<u8, BinaryEncodeError> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(BinaryEncodeError::BadValue {
            type_oid: PG_TYPE_BYTEA,
            reason: format!("BYTEA hex contains non-hex byte 0x{b:02X}"),
        }),
    }
}

/// Encode an ISO timestamp string `YYYY-MM-DD HH:MM:SS.ffffff+00` as
/// the PG binary TIMESTAMPTZ wire format: i64 microseconds since
/// 2000-01-01 00:00:00 UTC, big-endian.
///
/// The INVERSE of `extq::substitute::decode_timestamptz`. Howard
/// Hinnant's `days_from_civil` algorithm — public-domain, pure-Rust.
///
/// V1 accepts the exact shape `render_pg_text` produces; minor
/// variations (no microsecond suffix, no timezone, etc.) are accepted
/// best-effort.
fn encode_timestamptz(text: &[u8]) -> Result<Vec<u8>, BinaryEncodeError> {
    let s = std::str::from_utf8(text).map_err(|_| BinaryEncodeError::BadValue {
        type_oid: PG_TYPE_TIMESTAMPTZ,
        reason: "TIMESTAMPTZ text must be valid UTF-8".to_string(),
    })?;
    // Split off optional timezone suffix.
    let (datetime, _tz) = split_timezone(s);
    // Split into date + time on the first space or `T`.
    let (date_part, time_part) = datetime
        .split_once(' ')
        .or_else(|| datetime.split_once('T'))
        .ok_or_else(|| BinaryEncodeError::BadValue {
            type_oid: PG_TYPE_TIMESTAMPTZ,
            reason: format!("TIMESTAMPTZ missing date/time separator in {s:?}"),
        })?;
    // Date: YYYY-MM-DD.
    let mut date_iter = date_part.splitn(3, '-');
    let y_str = date_iter.next().ok_or_else(|| bad_ts(s, "missing year"))?;
    // Negative years are written as `-YYYY-MM-DD`; the leading '-' is
    // attached to the year token by the splitn iterator when the
    // string starts with '-'. Handle that.
    let (y_sign, y_digits): (i64, &str) = if y_str.starts_with('-') {
        (-1, &y_str[1..])
    } else {
        (1, y_str)
    };
    let y_abs: i64 = y_digits
        .parse()
        .map_err(|_| bad_ts(s, "year not numeric"))?;
    let y = y_sign * y_abs;
    let m: u32 = date_iter
        .next()
        .ok_or_else(|| bad_ts(s, "missing month"))?
        .parse()
        .map_err(|_| bad_ts(s, "month not numeric"))?;
    let d: u32 = date_iter
        .next()
        .ok_or_else(|| bad_ts(s, "missing day"))?
        .parse()
        .map_err(|_| bad_ts(s, "day not numeric"))?;
    // Time: HH:MM:SS[.ffffff].
    let (hms_part, frac_part) = time_part.split_once('.').unwrap_or((time_part, ""));
    let mut hms_iter = hms_part.splitn(3, ':');
    let hh: u32 = hms_iter
        .next()
        .ok_or_else(|| bad_ts(s, "missing hour"))?
        .parse()
        .map_err(|_| bad_ts(s, "hour not numeric"))?;
    let mm: u32 = hms_iter
        .next()
        .ok_or_else(|| bad_ts(s, "missing minute"))?
        .parse()
        .map_err(|_| bad_ts(s, "minute not numeric"))?;
    let ss: u32 = hms_iter
        .next()
        .ok_or_else(|| bad_ts(s, "missing second"))?
        .parse()
        .map_err(|_| bad_ts(s, "second not numeric"))?;
    // Microseconds: pad/truncate to 6 digits.
    let micros_part: u32 = if frac_part.is_empty() {
        0
    } else {
        // Pad to 6 digits (truncate if longer).
        let mut buf = String::with_capacity(6);
        for c in frac_part.chars().take(6) {
            buf.push(c);
        }
        while buf.len() < 6 {
            buf.push('0');
        }
        buf.parse().map_err(|_| bad_ts(s, "subseconds not numeric"))?
    };
    // Compose to days since 1970-01-01, then microseconds since the
    // PG epoch (2000-01-01).
    let days = days_from_civil(y, m, d);
    let sec_of_day = (hh as i64) * 3600 + (mm as i64) * 60 + (ss as i64);
    let unix_sec = days * 86_400 + sec_of_day;
    let unix_micros = unix_sec
        .checked_mul(1_000_000)
        .and_then(|v| v.checked_add(micros_part as i64))
        .ok_or_else(|| bad_ts(s, "timestamp overflows i64 microseconds"))?;
    // PG epoch is 30 years AFTER Unix epoch.
    const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;
    let pg_micros = unix_micros.checked_sub(PG_EPOCH_OFFSET_MICROS).ok_or_else(
        || bad_ts(s, "timestamp underflows PG epoch offset"),
    )?;
    Ok(pg_micros.to_be_bytes().to_vec())
}

fn bad_ts(text: &str, reason: &str) -> BinaryEncodeError {
    BinaryEncodeError::BadValue {
        type_oid: PG_TYPE_TIMESTAMPTZ,
        reason: format!("TIMESTAMPTZ {text:?}: {reason}"),
    }
}

/// Strip a trailing PG timezone suffix `+00` / `+0000` / `-04:30` /
/// `Z`. V1 ignores the offset (PG always normalizes to UTC at
/// storage; the wire text already reflects UTC). Returns (datetime,
/// offset_str). The offset is currently discarded.
fn split_timezone(s: &str) -> (&str, &str) {
    // Look for trailing `Z` first.
    if let Some(prefix) = s.strip_suffix('Z') {
        return (prefix, "Z");
    }
    // Scan backwards for `+` or `-` after the seconds (last 6 chars
    // worth — the offset is up to 6 chars `+HH:MM`).
    let bytes = s.as_bytes();
    // Find the LAST `+`/`-` in the string (the date part also has
    // `-`, so we need to scan from the end and stop when we hit a
    // space or `T` or the start).
    let mut i = bytes.len();
    while i > 0 {
        i -= 1;
        let b = bytes[i];
        if b == b' ' || b == b'T' {
            return (s, "");
        }
        if b == b'+' || b == b'-' {
            // Verify the character before isn't part of the date.
            // The date format is YYYY-MM-DD which has its `-` chars
            // BEFORE the space/`T` separator; any `-` AFTER the space
            // is a timezone offset.
            return (&s[..i], &s[i..]);
        }
    }
    (s, "")
}

/// Howard Hinnant `days_from_civil` — inverse of `civil_from_days`.
/// Returns days since 1970-01-01 for a proleptic Gregorian (y, m, d).
/// Public domain. Source: https://howardhinnant.github.io/date_algorithms.html
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64; // [0, 399]
    let mp = if m > 2 { m - 3 } else { m + 9 } as u64; // [0, 11]
    let doy = (153 * mp + 2) / 5 + (d as u64) - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe as i64 - 719_468
}

/// SP-PG-EXTQ-BIN-RESULTS T1 — parse a complete `D` DataRow wire frame
/// into per-column `Option<Vec<u8>>` (None = NULL sentinel `-1`). The
/// inverse of `crate::response::encode_data_row` for the parse side.
///
/// Returns `None` on a malformed frame (tag != 'D', length mismatch,
/// truncated bytes). The dispatcher treats malformed as a passthrough
/// (defensive — never observed in production because the frames come
/// from our own encoder).
pub fn parse_data_row(frame: &[u8]) -> Option<Vec<Option<Vec<u8>>>> {
    if frame.len() < 7 || frame[0] != BE_DATA_ROW {
        return None;
    }
    let frame_len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
    if frame.len() != 1 + frame_len {
        return None;
    }
    let col_count = u16::from_be_bytes([frame[5], frame[6]]) as usize;
    let mut cols = Vec::with_capacity(col_count);
    let mut i = 7;
    for _ in 0..col_count {
        if i + 4 > frame.len() {
            return None;
        }
        let len = i32::from_be_bytes([frame[i], frame[i + 1], frame[i + 2], frame[i + 3]]);
        i += 4;
        if len < 0 {
            // NULL sentinel.
            cols.push(None);
            continue;
        }
        let len = len as usize;
        if i + len > frame.len() {
            return None;
        }
        cols.push(Some(frame[i..i + len].to_vec()));
        i += len;
    }
    Some(cols)
}

/// SP-PG-EXTQ-BIN-RESULTS T1 — re-encode a DataRow frame per the
/// portal's per-column `format_codes`. Columns whose effective format
/// is binary AND whose type OID is V1-supported are routed through
/// `encode_binary_value`; columns whose effective format is text pass
/// through unchanged; NULL columns pass through unchanged regardless
/// of format (NULL is format-agnostic per PG §55.2.3).
///
/// **Inputs:**
/// - `text_frame: &[u8]` — a complete `D` wire frame as produced by
///   `crate::response::encode_data_row` (caller usually gets this by
///   running `dispatch::dispatch_query` then splitting via
///   `split_dispatch_query_bytes` in `extq::mod`).
/// - `formats: &[u16]` — per PG length conventions (0 codes = all
///   text, 1 code = all-same, N codes = per-column). The same
///   `effective_format_code` helper from `extq::substitute` would
///   apply, but to avoid the dependency we inline the rule here.
/// - `type_oids: &[u32]` — per-column PG type OIDs from RowDescription.
///   Use `extract_type_oids_from_row_description` to get this from the
///   RowDescription frame the prelude already carries.
///
/// **Returns:** a fresh `D` wire frame. The text-pass-through path is
/// effectively a parse-then-re-encode round trip — produces the same
/// bytes as `text_frame` by `encode_data_row`'s contract, so the
/// existing byte-locked KATs in `response.rs` continue to hold.
///
/// **Per-column dispatch:**
/// - effective_format == 0 (text) → pass-through.
/// - effective_format == 1 (binary) + NULL → pass-through (NULL).
/// - effective_format == 1 (binary) + value → `encode_binary_value`.
pub fn rewrite_data_row_with_formats(
    text_frame: &[u8],
    formats: &[u16],
    type_oids: &[u32],
) -> Result<Vec<u8>, BinaryRewriteError> {
    let cols = parse_data_row(text_frame).ok_or(BinaryRewriteError::MalformedDataRow)?;
    let mut new_cols: Vec<Option<Vec<u8>>> = Vec::with_capacity(cols.len());
    for (i, col) in cols.into_iter().enumerate() {
        let format = effective_format_code(formats, i);
        match col {
            None => new_cols.push(None),
            Some(text) if format == FORMAT_CODE_TEXT => new_cols.push(Some(text)),
            Some(text) => {
                let oid = type_oids.get(i).copied().unwrap_or(0);
                let binary =
                    encode_binary_value(&text, oid).map_err(|e| BinaryRewriteError::Encode {
                        position: i,
                        error: e,
                    })?;
                new_cols.push(Some(binary));
            }
        }
    }
    let borrowed: Vec<Option<&[u8]>> = new_cols.iter().map(|c| c.as_deref()).collect();
    Ok(crate::response::encode_data_row(&borrowed))
}

/// SP-PG-EXTQ-BIN-RESULTS T1 — rewrite the per-field `format_code` slot
/// in a RowDescription frame to match the portal's per-column requested
/// format. Walks each field's sub-frame (name cstring + 18 fixed
/// bytes) and flips the last 2 bytes (format_code slot).
///
/// Returns the rewritten frame as a fresh `Vec<u8>` — exact same shape
/// + size as the input (only the format_code slots change). If `formats`
/// is empty or every code is 0, returns the input unchanged.
///
/// Defensive: if the frame is malformed (truncated, wrong tag, length
/// mismatch), returns the input unchanged (the dispatcher treats this
/// as the text-passthrough case).
pub fn rewrite_row_description_with_formats(
    rd_frame: &[u8],
    formats: &[u16],
) -> Vec<u8> {
    // Zero-cost early-out for the text-only path.
    if formats.iter().all(|&f| f == FORMAT_CODE_TEXT) {
        return rd_frame.to_vec();
    }
    if rd_frame.len() < 7 || rd_frame[0] != BE_ROW_DESCRIPTION {
        return rd_frame.to_vec();
    }
    let frame_len = u32::from_be_bytes([rd_frame[1], rd_frame[2], rd_frame[3], rd_frame[4]])
        as usize;
    if rd_frame.len() != 1 + frame_len {
        return rd_frame.to_vec();
    }
    let field_count = u16::from_be_bytes([rd_frame[5], rd_frame[6]]) as usize;
    let mut out = rd_frame.to_vec();
    let mut i = 7;
    for field_idx in 0..field_count {
        // Skip the name cstring.
        let mut name_end = i;
        while name_end < out.len() && out[name_end] != 0 {
            name_end += 1;
        }
        if name_end >= out.len() {
            return rd_frame.to_vec();
        }
        // Skip past NUL terminator.
        i = name_end + 1;
        // Fixed: table_oid(4) + column_attr(2) + type_oid(4)
        // + type_size(2) + type_modifier(4) + format_code(2) = 18.
        if i + 18 > out.len() {
            return rd_frame.to_vec();
        }
        // format_code slot is the last 2 bytes of this 18-byte section.
        let format_slot = i + 16;
        let effective = effective_format_code(formats, field_idx);
        out[format_slot..format_slot + 2].copy_from_slice(&effective.to_be_bytes());
        i += 18;
    }
    out
}

/// SP-PG-EXTQ-BIN-RESULTS T1 — parse a RowDescription frame and return
/// the per-field `type_oid` slot values in declaration order. The
/// inverse of `crate::response::encode_row_description` for just the
/// type_oid slots (the rewriter doesn't need the names / sizes).
///
/// Returns `None` on a malformed frame.
pub fn extract_type_oids_from_row_description(rd_frame: &[u8]) -> Option<Vec<u32>> {
    if rd_frame.len() < 7 || rd_frame[0] != BE_ROW_DESCRIPTION {
        return None;
    }
    let frame_len = u32::from_be_bytes([rd_frame[1], rd_frame[2], rd_frame[3], rd_frame[4]])
        as usize;
    if rd_frame.len() != 1 + frame_len {
        return None;
    }
    let field_count = u16::from_be_bytes([rd_frame[5], rd_frame[6]]) as usize;
    let mut oids = Vec::with_capacity(field_count);
    let mut i = 7;
    for _ in 0..field_count {
        // Skip name cstring.
        let mut name_end = i;
        while name_end < rd_frame.len() && rd_frame[name_end] != 0 {
            name_end += 1;
        }
        if name_end >= rd_frame.len() {
            return None;
        }
        i = name_end + 1;
        // table_oid:4, column_attr:2, then type_oid:4.
        if i + 18 > rd_frame.len() {
            return None;
        }
        let oid_slot = i + 6;
        let oid = u32::from_be_bytes([
            rd_frame[oid_slot],
            rd_frame[oid_slot + 1],
            rd_frame[oid_slot + 2],
            rd_frame[oid_slot + 3],
        ]);
        oids.push(oid);
        i += 18;
    }
    Some(oids)
}

/// SP-PG-EXTQ-BIN-RESULTS T1 — errors from `rewrite_data_row_with_formats`.
#[derive(Debug, PartialEq, Eq)]
pub enum BinaryRewriteError {
    /// The input frame's wire bytes didn't parse as a `D` DataRow.
    /// Defensive — shouldn't happen because the frames come from our
    /// own encoder.
    MalformedDataRow,
    /// A per-column `encode_binary_value` failed. Carries the 0-based
    /// position + the underlying `BinaryEncodeError`. The dispatcher
    /// wraps this in a SQLSTATE `0A000` ErrorResponse.
    Encode {
        position: usize,
        error: BinaryEncodeError,
    },
}

/// SP-PG-EXTQ-BIN-RESULTS T1 — same convention as
/// `substitute::effective_format_code` (single-sourced into this module
/// to avoid a cyclic import-tree, and to keep the result-path's PG
/// length-convention rule colocated with the result rewriter). The two
/// helpers MUST stay byte-equivalent — a future PG length-convention
/// change updates both call sites.
pub fn effective_format_code(formats: &[u16], i: usize) -> u16 {
    match formats.len() {
        0 => FORMAT_CODE_TEXT,
        1 => formats[0],
        _ => formats.get(i).copied().unwrap_or(FORMAT_CODE_TEXT),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::FORMAT_CODE_BINARY;
    use crate::response::{encode_data_row, encode_row_description, FieldMeta};

    // ───────────────────────────────────────────────────────────────
    // T1 KATs — encode_binary_value per-type byte-correctness. Each
    // test locks ONE per-type encoder against canonical PG §55.8
    // wire bytes. Drift here would silently corrupt every binary
    // result on the wire.
    // ───────────────────────────────────────────────────────────────

    /// BOOL true → 0x01.
    #[test]
    fn t1binr_encode_bool_true_byte_correct() {
        assert_eq!(encode_binary_value(b"t", PG_TYPE_BOOL).unwrap(), vec![0x01]);
        assert_eq!(
            encode_binary_value(b"true", PG_TYPE_BOOL).unwrap(),
            vec![0x01]
        );
    }

    /// BOOL false → 0x00.
    #[test]
    fn t1binr_encode_bool_false_byte_correct() {
        assert_eq!(encode_binary_value(b"f", PG_TYPE_BOOL).unwrap(), vec![0x00]);
        assert_eq!(
            encode_binary_value(b"false", PG_TYPE_BOOL).unwrap(),
            vec![0x00]
        );
    }

    /// BOOL invalid text rejects.
    #[test]
    fn t1binr_encode_bool_invalid_rejects() {
        let err = encode_binary_value(b"yes", PG_TYPE_BOOL).unwrap_err();
        assert!(matches!(err, BinaryEncodeError::BadValue { .. }));
    }

    /// INT2 42 → 0x00 0x2A.
    #[test]
    fn t1binr_encode_int2_be_byte_correct() {
        assert_eq!(
            encode_binary_value(b"42", PG_TYPE_INT2).unwrap(),
            vec![0x00, 0x2A]
        );
        assert_eq!(
            encode_binary_value(b"-1", PG_TYPE_INT2).unwrap(),
            vec![0xFF, 0xFF]
        );
        assert_eq!(
            encode_binary_value(b"32767", PG_TYPE_INT2).unwrap(),
            vec![0x7F, 0xFF]
        );
    }

    /// INT2 overflow rejects.
    #[test]
    fn t1binr_encode_int2_overflow_rejects() {
        let err = encode_binary_value(b"40000", PG_TYPE_INT2).unwrap_err();
        assert!(matches!(err, BinaryEncodeError::BadValue { .. }));
    }

    /// INT4 -1 → 0xFFFFFFFF (the canonical sign-extended BE pattern).
    #[test]
    fn t1binr_encode_int4_be_byte_correct() {
        assert_eq!(
            encode_binary_value(b"-1", PG_TYPE_INT4).unwrap(),
            vec![0xFF, 0xFF, 0xFF, 0xFF]
        );
        assert_eq!(
            encode_binary_value(b"100", PG_TYPE_INT4).unwrap(),
            vec![0x00, 0x00, 0x00, 0x64]
        );
    }

    /// INT8 100 → big-endian 8 bytes.
    #[test]
    fn t1binr_encode_int8_be_byte_correct() {
        assert_eq!(
            encode_binary_value(b"100", PG_TYPE_INT8).unwrap(),
            vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64]
        );
        assert_eq!(
            encode_binary_value(b"-1", PG_TYPE_INT8).unwrap(),
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        );
    }

    /// INT8 max → 0x7FFFFFFFFFFFFFFF.
    #[test]
    fn t1binr_encode_int8_max_byte_correct() {
        let max_str = i64::MAX.to_string();
        assert_eq!(
            encode_binary_value(max_str.as_bytes(), PG_TYPE_INT8).unwrap(),
            vec![0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        );
    }

    /// FLOAT8 π → canonical IEEE 754 BE bytes.
    #[test]
    fn t1binr_encode_float8_pi_be_byte_correct() {
        // π = 3.141592653589793. The exact IEEE 754 BE encoding.
        let out = encode_binary_value(b"3.141592653589793", PG_TYPE_FLOAT8).unwrap();
        assert_eq!(
            out,
            vec![0x40, 0x09, 0x21, 0xFB, 0x54, 0x44, 0x2D, 0x18]
        );
    }

    /// FLOAT4 1.5 → IEEE 754 single BE.
    #[test]
    fn t1binr_encode_float4_be_byte_correct() {
        let out = encode_binary_value(b"1.5", PG_TYPE_FLOAT4).unwrap();
        // 1.5 = 0x3FC00000.
        assert_eq!(out, vec![0x3F, 0xC0, 0x00, 0x00]);
    }

    /// TEXT bytes pass through verbatim (UTF-8 → UTF-8 binary).
    #[test]
    fn t1binr_encode_text_utf8_pass_through() {
        assert_eq!(
            encode_binary_value(b"hello", PG_TYPE_TEXT).unwrap(),
            b"hello".to_vec()
        );
        // Multi-byte UTF-8 passes too.
        assert_eq!(
            encode_binary_value("héllo".as_bytes(), PG_TYPE_TEXT).unwrap(),
            "héllo".as_bytes().to_vec()
        );
        // VARCHAR mirrors TEXT.
        assert_eq!(
            encode_binary_value(b"world", PG_TYPE_VARCHAR).unwrap(),
            b"world".to_vec()
        );
    }

    /// BYTEA `\xdead` text → raw 0xDE 0xAD bytes.
    #[test]
    fn t1binr_encode_bytea_hex_to_raw() {
        assert_eq!(
            encode_binary_value(b"\\xdead", PG_TYPE_BYTEA).unwrap(),
            vec![0xDE, 0xAD]
        );
        assert_eq!(
            encode_binary_value(b"\\xdeadbeef", PG_TYPE_BYTEA).unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
        // Empty bytea.
        assert_eq!(
            encode_binary_value(b"\\x", PG_TYPE_BYTEA).unwrap(),
            Vec::<u8>::new()
        );
        // Uppercase hex accepted.
        assert_eq!(
            encode_binary_value(b"\\xDEAD", PG_TYPE_BYTEA).unwrap(),
            vec![0xDE, 0xAD]
        );
    }

    /// BYTEA without `\x` prefix rejects.
    #[test]
    fn t1binr_encode_bytea_missing_prefix_rejects() {
        let err = encode_binary_value(b"dead", PG_TYPE_BYTEA).unwrap_err();
        assert!(matches!(err, BinaryEncodeError::BadValue { .. }));
    }

    /// TIMESTAMPTZ ISO 2000-01-01 00:00:00.000000+00 → 0 micros.
    #[test]
    fn t1binr_encode_timestamptz_epoch_is_zero() {
        let out =
            encode_binary_value(b"2000-01-01 00:00:00.000000+00", PG_TYPE_TIMESTAMPTZ).unwrap();
        assert_eq!(out, vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    }

    /// TIMESTAMPTZ ISO 2000-01-01 00:00:01.000000+00 → 1_000_000 micros.
    #[test]
    fn t1binr_encode_timestamptz_one_second_after_epoch() {
        let out =
            encode_binary_value(b"2000-01-01 00:00:01.000000+00", PG_TYPE_TIMESTAMPTZ).unwrap();
        assert_eq!(i64::from_be_bytes(out.try_into().unwrap()), 1_000_000);
    }

    /// NUMERIC binary requested → Unsupported with V2 arc name.
    #[test]
    fn t1binr_encode_numeric_returns_unsupported_with_arc() {
        let err = encode_binary_value(b"3.14", PG_TYPE_NUMERIC).unwrap_err();
        match err {
            BinaryEncodeError::Unsupported { arc, type_oid } => {
                assert_eq!(arc, "SP-PG-EXTQ-BIN-NUMERIC");
                assert_eq!(type_oid, PG_TYPE_NUMERIC);
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    /// Unknown OID → Unsupported with V2 arc name SP-PG-EXTQ-BIN-EXTRA.
    #[test]
    fn t1binr_encode_unknown_oid_returns_unsupported_with_arc() {
        // OID 114 = json (PG canonical).
        let err = encode_binary_value(b"{}", 114).unwrap_err();
        match err {
            BinaryEncodeError::Unsupported { arc, type_oid } => {
                assert_eq!(arc, "SP-PG-EXTQ-BIN-EXTRA");
                assert_eq!(type_oid, 114);
            }
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    /// Round-trip: decode (V1 param) then encode (V1 result) is
    /// identity for INT8. Locks the symmetry invariant across SP-PG-
    /// EXTQ-BIN and SP-PG-EXTQ-BIN-RESULTS.
    #[test]
    fn t1binr_round_trip_decode_encode_int8() {
        let original_bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64];
        let text = crate::extq::substitute::decode_binary_param(&original_bytes, PG_TYPE_INT8)
            .unwrap();
        assert_eq!(text, "100");
        let re_encoded = encode_binary_value(text.as_bytes(), PG_TYPE_INT8).unwrap();
        assert_eq!(re_encoded, original_bytes);
    }

    /// Round-trip identity for FLOAT8.
    #[test]
    fn t1binr_round_trip_decode_encode_float8() {
        let original_bytes = vec![0x40, 0x09, 0x21, 0xFB, 0x54, 0x44, 0x2D, 0x18];
        let text = crate::extq::substitute::decode_binary_param(&original_bytes, PG_TYPE_FLOAT8)
            .unwrap();
        let re_encoded = encode_binary_value(text.as_bytes(), PG_TYPE_FLOAT8).unwrap();
        assert_eq!(re_encoded, original_bytes);
    }

    /// Round-trip identity for BOOL true.
    #[test]
    fn t1binr_round_trip_decode_encode_bool() {
        let original_bytes = vec![0x01];
        let text = crate::extq::substitute::decode_binary_param(&original_bytes, PG_TYPE_BOOL)
            .unwrap();
        assert_eq!(text, "true");
        let re_encoded = encode_binary_value(text.as_bytes(), PG_TYPE_BOOL).unwrap();
        assert_eq!(re_encoded, original_bytes);
    }

    /// Round-trip identity for BYTEA.
    #[test]
    fn t1binr_round_trip_decode_encode_bytea() {
        let original_bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let text = crate::extq::substitute::decode_binary_param(&original_bytes, PG_TYPE_BYTEA)
            .unwrap();
        assert_eq!(text, "\\xdeadbeef");
        let re_encoded = encode_binary_value(text.as_bytes(), PG_TYPE_BYTEA).unwrap();
        assert_eq!(re_encoded, original_bytes);
    }

    /// Round-trip identity for TIMESTAMPTZ.
    #[test]
    fn t1binr_round_trip_decode_encode_timestamptz() {
        // 2026-06-01 00:00:00 UTC = 26 years after PG epoch.
        // 26 years = ~819_936_000 sec (varies with leap years).
        // Easier: pick µs since PG epoch = 0 (the epoch itself).
        let original_bytes = vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        let text =
            crate::extq::substitute::decode_binary_param(&original_bytes, PG_TYPE_TIMESTAMPTZ)
                .unwrap();
        assert_eq!(text, "2000-01-01 00:00:00.000000+00");
        let re_encoded = encode_binary_value(text.as_bytes(), PG_TYPE_TIMESTAMPTZ).unwrap();
        assert_eq!(re_encoded, original_bytes);
    }

    // ───────────────────────────────────────────────────────────────
    // T1 KATs — parse_data_row + rewrite_data_row_with_formats.
    // ───────────────────────────────────────────────────────────────

    /// `parse_data_row` round-trips a single-column DataRow.
    #[test]
    fn t1binr_parse_data_row_single_column_round_trip() {
        let frame = encode_data_row(&[Some(b"42")]);
        let cols = parse_data_row(&frame).expect("parse");
        assert_eq!(cols, vec![Some(b"42".to_vec())]);
    }

    /// `parse_data_row` handles NULL columns + mixed.
    #[test]
    fn t1binr_parse_data_row_with_null_and_value() {
        let frame = encode_data_row(&[Some(b"1"), None, Some(b"t")]);
        let cols = parse_data_row(&frame).expect("parse");
        assert_eq!(
            cols,
            vec![Some(b"1".to_vec()), None, Some(b"t".to_vec())]
        );
    }

    /// `parse_data_row` returns None on a non-DataRow tag.
    #[test]
    fn t1binr_parse_data_row_rejects_wrong_tag() {
        let mut bad = encode_data_row(&[Some(b"x")]);
        bad[0] = b'T'; // change tag to RowDescription
        assert_eq!(parse_data_row(&bad), None);
    }

    /// `rewrite_data_row_with_formats` with empty formats is a pass-
    /// through (byte-equal output).
    #[test]
    fn t1binr_rewrite_data_row_empty_formats_passthrough() {
        let frame = encode_data_row(&[Some(b"42"), Some(b"t")]);
        let out = rewrite_data_row_with_formats(&frame, &[], &[PG_TYPE_INT8, PG_TYPE_BOOL])
            .expect("ok");
        assert_eq!(out, frame);
    }

    /// `rewrite_data_row_with_formats` with all-text `[0]` is a
    /// pass-through.
    #[test]
    fn t1binr_rewrite_data_row_all_text_passthrough() {
        let frame = encode_data_row(&[Some(b"42")]);
        let out = rewrite_data_row_with_formats(&frame, &[0], &[PG_TYPE_INT8]).expect("ok");
        assert_eq!(out, frame);
    }

    /// `rewrite_data_row_with_formats` with all-binary `[1]` flips
    /// every column to binary. INT8 "100" → 8 bytes BE.
    #[test]
    fn t1binr_rewrite_data_row_all_binary_int8_byte_correct() {
        let frame = encode_data_row(&[Some(b"100")]);
        let out = rewrite_data_row_with_formats(&frame, &[1], &[PG_TYPE_INT8]).expect("ok");
        // Expected wire: 'D' + length + col_count=1 + col_length=8 + 8 BE bytes
        // length = 4 + 2 + 4 + 8 = 18
        let mut expected = Vec::new();
        expected.push(b'D');
        expected.extend_from_slice(&18u32.to_be_bytes());
        expected.extend_from_slice(&1u16.to_be_bytes());
        expected.extend_from_slice(&8i32.to_be_bytes());
        expected.extend_from_slice(&100i64.to_be_bytes());
        assert_eq!(out, expected);
    }

    /// Mixed text + binary per-column formats — INT8 binary at pos 0,
    /// TEXT text at pos 1.
    #[test]
    fn t1binr_rewrite_data_row_mixed_text_and_binary() {
        let frame = encode_data_row(&[Some(b"42"), Some(b"hello")]);
        let out = rewrite_data_row_with_formats(
            &frame,
            &[FORMAT_CODE_BINARY, FORMAT_CODE_TEXT],
            &[PG_TYPE_INT8, PG_TYPE_TEXT],
        )
        .expect("ok");
        let cols = parse_data_row(&out).expect("parse");
        // INT8 binary: 8 bytes BE for 42.
        assert_eq!(cols[0], Some(42i64.to_be_bytes().to_vec()));
        // TEXT pass-through: "hello".
        assert_eq!(cols[1], Some(b"hello".to_vec()));
    }

    /// NULL columns stay NULL regardless of format code.
    #[test]
    fn t1binr_rewrite_data_row_null_column_stays_null() {
        let frame = encode_data_row(&[Some(b"42"), None]);
        let out = rewrite_data_row_with_formats(
            &frame,
            &[FORMAT_CODE_BINARY],
            &[PG_TYPE_INT8, PG_TYPE_INT8],
        )
        .expect("ok");
        let cols = parse_data_row(&out).expect("parse");
        assert_eq!(cols[0], Some(42i64.to_be_bytes().to_vec()));
        assert_eq!(cols[1], None);
    }

    /// NUMERIC binary requested → Encode error with the V2 arc.
    #[test]
    fn t1binr_rewrite_data_row_numeric_binary_rejects() {
        let frame = encode_data_row(&[Some(b"3.14")]);
        let err = rewrite_data_row_with_formats(&frame, &[1], &[PG_TYPE_NUMERIC]).unwrap_err();
        match err {
            BinaryRewriteError::Encode {
                position,
                error: BinaryEncodeError::Unsupported { arc, .. },
            } => {
                assert_eq!(position, 0);
                assert_eq!(arc, "SP-PG-EXTQ-BIN-NUMERIC");
            }
            other => panic!("expected Encode::Unsupported, got {other:?}"),
        }
    }

    // ───────────────────────────────────────────────────────────────
    // T1 KATs — rewrite_row_description_with_formats.
    // ───────────────────────────────────────────────────────────────

    /// Text formats `[0]` → RowDescription pass-through (byte-equal).
    #[test]
    fn t1binr_rewrite_row_description_text_passthrough() {
        let rd = encode_row_description(&[FieldMeta {
            name: "id".to_string(),
            type_oid: PG_TYPE_INT8,
        }]);
        let out = rewrite_row_description_with_formats(&rd, &[0]);
        assert_eq!(out, rd);
    }

    /// Binary formats `[1]` → flips per-field format_code slot to 1.
    #[test]
    fn t1binr_rewrite_row_description_flips_format_codes() {
        let rd = encode_row_description(&[
            FieldMeta {
                name: "id".to_string(),
                type_oid: PG_TYPE_INT8,
            },
            FieldMeta {
                name: "name".to_string(),
                type_oid: PG_TYPE_TEXT,
            },
        ]);
        let out = rewrite_row_description_with_formats(&rd, &[1]);
        // Confirm same length (only 2-byte slots change).
        assert_eq!(out.len(), rd.len());
        // Tag + length + field_count are byte-equal.
        assert_eq!(&out[..7], &rd[..7]);
        // Confirm the format_code slot per field is 1 (BE u16).
        // Field 0: "id\0" (3 bytes) + table_oid(4) + col_attr(2)
        //   + type_oid(4) + type_size(2) + type_modifier(4) = 19 bytes
        //   from start of field; format_code at offset 7 + 19 = 26.
        let f0_format_slot = 7 + 3 + 16;
        assert_eq!(out[f0_format_slot], 0x00);
        assert_eq!(out[f0_format_slot + 1], 0x01);
    }

    /// Per-column formats `[1, 0]` → only field 0 flips.
    #[test]
    fn t1binr_rewrite_row_description_per_column_format() {
        let rd = encode_row_description(&[
            FieldMeta {
                name: "id".to_string(),
                type_oid: PG_TYPE_INT8,
            },
            FieldMeta {
                name: "name".to_string(),
                type_oid: PG_TYPE_TEXT,
            },
        ]);
        let out =
            rewrite_row_description_with_formats(&rd, &[FORMAT_CODE_BINARY, FORMAT_CODE_TEXT]);
        // Field 0 format_code → 1.
        let f0_format_slot = 7 + 3 + 16;
        assert_eq!(out[f0_format_slot..f0_format_slot + 2], [0x00, 0x01]);
        // Field 1 starts at f0_format_slot + 2 = 28.
        // "name\0" = 5 bytes, then 18 bytes of fixed fields.
        let f1_start = f0_format_slot + 2;
        let f1_format_slot = f1_start + 5 + 16;
        assert_eq!(out[f1_format_slot..f1_format_slot + 2], [0x00, 0x00]);
    }

    /// `extract_type_oids_from_row_description` round-trips the OID
    /// slot values.
    #[test]
    fn t1binr_extract_type_oids_round_trip() {
        let rd = encode_row_description(&[
            FieldMeta {
                name: "id".to_string(),
                type_oid: PG_TYPE_INT8,
            },
            FieldMeta {
                name: "flag".to_string(),
                type_oid: PG_TYPE_BOOL,
            },
            FieldMeta {
                name: "name".to_string(),
                type_oid: PG_TYPE_TEXT,
            },
        ]);
        let oids = extract_type_oids_from_row_description(&rd).expect("parse");
        assert_eq!(oids, vec![PG_TYPE_INT8, PG_TYPE_BOOL, PG_TYPE_TEXT]);
    }

    /// Malformed RowDescription returns None.
    #[test]
    fn t1binr_extract_type_oids_rejects_wrong_tag() {
        let dr = encode_data_row(&[Some(b"x")]);
        assert_eq!(extract_type_oids_from_row_description(&dr), None);
    }

    /// `effective_format_code` mirrors `substitute::effective_format_code`.
    #[test]
    fn t1binr_effective_format_code_pg_length_conventions() {
        // 0 codes = all text → 0
        assert_eq!(effective_format_code(&[], 0), FORMAT_CODE_TEXT);
        assert_eq!(effective_format_code(&[], 99), FORMAT_CODE_TEXT);
        // 1 code = all-same → formats[0]
        assert_eq!(
            effective_format_code(&[FORMAT_CODE_BINARY], 0),
            FORMAT_CODE_BINARY
        );
        assert_eq!(
            effective_format_code(&[FORMAT_CODE_BINARY], 99),
            FORMAT_CODE_BINARY
        );
        // N codes = per-position; out-of-range falls back to text.
        let f = vec![FORMAT_CODE_TEXT, FORMAT_CODE_BINARY, FORMAT_CODE_TEXT];
        assert_eq!(effective_format_code(&f, 0), FORMAT_CODE_TEXT);
        assert_eq!(effective_format_code(&f, 1), FORMAT_CODE_BINARY);
        assert_eq!(effective_format_code(&f, 2), FORMAT_CODE_TEXT);
        assert_eq!(effective_format_code(&f, 5), FORMAT_CODE_TEXT);
    }

    // ───────────────────────────────────────────────────────────────
    // T1 KATs — supported-OID + arc-name helpers.
    // ───────────────────────────────────────────────────────────────

    /// `binary_result_supported_for_oid` matches the same set as
    /// `substitute::binary_format_supported_for_oid`.
    #[test]
    fn t1binr_supported_oid_set_matches_param_side() {
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
            assert!(binary_result_supported_for_oid(oid), "oid {oid} should be supported");
            assert!(
                crate::extq::substitute::binary_format_supported_for_oid(oid),
                "param side should also support oid {oid}"
            );
        }
        for oid in [PG_TYPE_NUMERIC, 114, 2950, 1009] {
            assert!(!binary_result_supported_for_oid(oid), "oid {oid} should NOT be supported");
        }
    }

    /// `unsupported_binary_result_arc` returns the right V2 follow-up.
    #[test]
    fn t1binr_unsupported_arc_names_v2_followup() {
        assert_eq!(
            unsupported_binary_result_arc(PG_TYPE_NUMERIC),
            "SP-PG-EXTQ-BIN-NUMERIC"
        );
        assert_eq!(unsupported_binary_result_arc(114), "SP-PG-EXTQ-BIN-EXTRA");
        assert_eq!(unsupported_binary_result_arc(2950), "SP-PG-EXTQ-BIN-EXTRA");
    }
}
