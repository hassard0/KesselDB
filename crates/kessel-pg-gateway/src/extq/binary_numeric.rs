//! SP-PG-EXTQ-BIN-NUMERIC — PostgreSQL NUMERIC binary-format codec.
//!
//! **T2 status (this commit):** the pure-Rust NUMERIC binary
//! codec — `decode_numeric_binary` parses the PG `numeric_send` wire
//! shape into the canonical decimal string PG's `numeric_out` emits;
//! `encode_numeric_binary` does the inverse. The codec is engine-free
//! / stateless / `#![forbid(unsafe_code)]` and re-uses an `i128`
//! accumulator (no bignum dep).
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-06-02-kesseldb-sppgextqbinnumeric-design.md`
//!
//! ## What this module does
//!
//! - `decode_numeric_binary(bytes) -> Result<String, BinaryNumericError>`
//!   — parse the PG NUMERIC wire frame:
//!     `ndigits:i16 weight:i16 sign:u16 dscale:i16 [digit:i16]*`
//!   reconstruct the canonical decimal string `[-]?int_part[.frac_part]`.
//! - `encode_numeric_binary(decimal_str) -> Result<Vec<u8>, BinaryNumericError>`
//!   — parse the canonical decimal string and produce the PG wire bytes.
//!
//! ## V1 supported range
//!
//! `|value| < 10^18` with up to 18 fractional digits — covers the
//! typical ORM Decimal/BigDecimal shape (i64-shaped amounts, currency,
//! percentages, fractional rates). Wider values reject with
//! `BinaryNumericError::OutOfRange { arc: "SP-PG-EXTQ-BIN-NUMERIC-BIGNUM" }`
//! → SQLSTATE `22003 numeric_value_out_of_range`. The bignum support is
//! a future arc named in §2.2 of the design spec.
//!
//! ## What this module does NOT do (V2+ / out-of-scope)
//!
//! - NaN (`sign == 0xC000`) decodes reject with `BinaryNumericError::NaN`
//!   → `SP-PG-EXTQ-BIN-NUMERIC-NAN`.
//! - `+Infinity` / `-Infinity` (PG 14+ sign codes 0xD000 / 0xF000)
//!   reject with `BinaryNumericError::BadSign` →
//!   `SP-PG-EXTQ-BIN-NUMERIC-INF`.
//! - The codec is NOT used by COPY BIN's per-row encoder. COPY BIN
//!   keeps its independent `SP-PG-COPY-BIN-NUMERIC` deferral.

#![forbid(unsafe_code)]
#![allow(dead_code)]

/// PG NUMERIC sign code for positive values.
pub(crate) const NUMERIC_POS: u16 = 0x0000;
/// PG NUMERIC sign code for negative values.
pub(crate) const NUMERIC_NEG: u16 = 0x4000;
/// PG NUMERIC sign code for NaN.
pub(crate) const NUMERIC_NAN: u16 = 0xC000;

/// V1 cap: integer magnitude < 10^18 (`i64::MAX` is ~9.22e18; we
/// constrain to 10^18 so the i128 accumulator can safely combine
/// integer + ≤18 fractional digits without overflow).
const V1_INT_MAGNITUDE_CAP: i128 = 1_000_000_000_000_000_000_i128;

/// V1 cap: at most 18 fractional decimal digits.
const V1_MAX_FRAC_DIGITS: usize = 18;

/// V1 cap: at most 5 base-10000 digits in the wire array (covers
/// 10^18 - 1 integer + 4 fractional groups). Decoder rejects beyond
/// this so the i128 accumulator never overflows.
const V1_MAX_NDIGITS: usize = 32;

/// Errors from the NUMERIC binary codec. Shared between decoder + encoder.
#[derive(Debug, PartialEq, Eq)]
pub enum BinaryNumericError {
    /// Wire byte count smaller than the 8-byte header, or not aligned
    /// to a 2-byte digit boundary. Maps to SQLSTATE `08P01
    /// protocol_violation`.
    WrongLength { actual: usize },
    /// Wire bytes declared `ndigits=N` but the buffer only carried
    /// `M < 2*N` digit bytes. Maps to SQLSTATE `08P01`.
    Truncated { ndigits: usize, available: usize },
    /// `sign=0xC000` (NaN). Maps to SQLSTATE `22023` with the
    /// `SP-PG-EXTQ-BIN-NUMERIC-NAN` follow-up arc name.
    NaN,
    /// Unknown sign code (not POS/NEG/NAN). Includes the PG 14+
    /// `+Inf`/`-Inf` codes which V1 doesn't support. Maps to SQLSTATE
    /// `08P01` (or `0A000` at the caller boundary, with the
    /// `SP-PG-EXTQ-BIN-NUMERIC-INF` follow-up arc name for the
    /// infinity codes specifically).
    BadSign { sign: u16 },
    /// Per-digit value out of [0, 9999]. Maps to SQLSTATE `08P01`.
    BadDigit { position: usize, value: i16 },
    /// V1 only supports `|value| < 10^18` + ≤18 fractional digits.
    /// Maps to SQLSTATE `22003 numeric_value_out_of_range` at the
    /// caller boundary; the `arc` field names the follow-up.
    OutOfRange {
        reason: String,
        arc: &'static str,
    },
    /// Encoder: input string isn't a valid decimal literal. Maps to
    /// SQLSTATE `22P02 invalid_text_representation`.
    BadDecimalString { input: String, reason: String },
}

/// SP-PG-EXTQ-BIN-NUMERIC T2 — decode a PG NUMERIC binary wire frame
/// into the canonical decimal string PG's `numeric_out` emits.
///
/// **Returns:** the bare decimal text (NOT single-quoted) — same shape
/// the existing PG numeric-text rendering produces (`"42"`, `"-3.14"`,
/// `"0.0001"`, `"12345.6789"`). The caller wraps in `'...'` for
/// substitute-time SQL splicing.
///
/// **Wire layout** (per `src/backend/utils/adt/numeric.c::numeric_send`):
///
/// ```text
/// [ndigits:i16 BE]
/// [weight: i16 BE]
/// [sign:   u16 BE]
/// [dscale: i16 BE]
/// [digit:  i16 BE] * ndigits  // each in [0, 9999]
/// ```
///
/// Total wire size = `8 + 2*ndigits` bytes.
pub fn decode_numeric_binary(bytes: &[u8]) -> Result<String, BinaryNumericError> {
    if bytes.len() < 8 {
        return Err(BinaryNumericError::WrongLength { actual: bytes.len() });
    }
    let ndigits = i16::from_be_bytes([bytes[0], bytes[1]]);
    let weight = i16::from_be_bytes([bytes[2], bytes[3]]);
    let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
    let dscale = i16::from_be_bytes([bytes[6], bytes[7]]);
    // Sign dispatch.
    if sign == NUMERIC_NAN {
        return Err(BinaryNumericError::NaN);
    }
    if sign != NUMERIC_POS && sign != NUMERIC_NEG {
        return Err(BinaryNumericError::BadSign { sign });
    }
    if ndigits < 0 {
        return Err(BinaryNumericError::WrongLength { actual: bytes.len() });
    }
    let ndigits_usize = ndigits as usize;
    if ndigits_usize > V1_MAX_NDIGITS {
        return Err(BinaryNumericError::OutOfRange {
            reason: format!("ndigits={ndigits_usize} exceeds V1 cap {V1_MAX_NDIGITS}"),
            arc: "SP-PG-EXTQ-BIN-NUMERIC-BIGNUM",
        });
    }
    let expected_len = 8 + 2 * ndigits_usize;
    if bytes.len() < expected_len {
        return Err(BinaryNumericError::Truncated {
            ndigits: ndigits_usize,
            available: bytes.len().saturating_sub(8),
        });
    }
    // Parse digits.
    let mut digits: Vec<i16> = Vec::with_capacity(ndigits_usize);
    for k in 0..ndigits_usize {
        let off = 8 + 2 * k;
        let d = i16::from_be_bytes([bytes[off], bytes[off + 1]]);
        if !(0..=9999).contains(&d) {
            return Err(BinaryNumericError::BadDigit {
                position: k,
                value: d,
            });
        }
        digits.push(d);
    }

    // Special case: zero with no digits.
    if ndigits_usize == 0 {
        // dscale > 0 means "0.000..." with dscale digits after the point.
        return Ok(format_zero(dscale));
    }

    // Convert base-10000 digits + weight + dscale into an integer
    // accumulator + a "decimal point" position. The accumulator
    // represents `value * 10^dscale_used` as an integer; we then
    // split off the fractional part of dscale digits.

    // We want the "value scaled to dscale" — i.e. value * 10^dscale =
    // sum(digit[k] * 10000^(weight - k)) * 10^dscale.
    // = sum(digit[k] * 10^(4*(weight - k) + dscale))
    // For each digit, its exponent is 4*(weight - k) + dscale. We
    // sum into an i128 accumulator, rejecting overflow.
    //
    // Alternative simpler implementation: build the integer part by
    // walking digits with non-negative weight; build the fractional
    // part by walking digits with negative weight (taking dscale into
    // account). We use this clearer form.

    let mut int_part: i128 = 0;
    let mut frac_str = String::with_capacity(dscale as usize);
    // dscale: number of decimal digits AFTER the point in the final
    // output. Total decimal-point position is between digits whose
    // weight is >= 0 and digits whose weight is < 0.
    //
    // The integer portion is the SUM of digit[k] * 10000^(weight - k)
    // for k where weight - k >= 0.
    // The fractional portion is positionally: for each base-10000
    // group with weight w < 0, it contributes 4 fractional digits
    // starting at position -4*w - 4 + 1 = -4*w - 3 from the point
    // ... actually easier:
    //
    // Build the fractional string left-to-right by laying out base-
    // 10000 digits at their positions:
    //  - For weight w = -1, the 4 digits occupy fractional positions 1..4.
    //  - For weight w = -2, they occupy 5..8.
    //  - etc.
    // Any "gap" between groups gets filled with zeros.

    // ── Integer side ─────────────────────────────────────────────────
    for k in 0..ndigits_usize {
        let w = (weight as i32) - (k as i32);
        if w < 0 {
            break;
        }
        // digit[k] * 10000^w. Overflow check.
        let pow = pow10000(w as u32).ok_or_else(|| BinaryNumericError::OutOfRange {
            reason: format!("integer weight 10000^{w} exceeds V1 i128 cap"),
            arc: "SP-PG-EXTQ-BIN-NUMERIC-BIGNUM",
        })?;
        let added = (digits[k] as i128)
            .checked_mul(pow)
            .ok_or_else(|| BinaryNumericError::OutOfRange {
                reason: format!("digit*pow overflow at position {k}"),
                arc: "SP-PG-EXTQ-BIN-NUMERIC-BIGNUM",
            })?;
        int_part = int_part
            .checked_add(added)
            .ok_or_else(|| BinaryNumericError::OutOfRange {
                reason: format!("integer accumulator overflow at position {k}"),
                arc: "SP-PG-EXTQ-BIN-NUMERIC-BIGNUM",
            })?;
        if int_part.abs() >= V1_INT_MAGNITUDE_CAP {
            return Err(BinaryNumericError::OutOfRange {
                reason: format!(
                    "integer magnitude {int_part} >= V1 cap {V1_INT_MAGNITUDE_CAP}"
                ),
                arc: "SP-PG-EXTQ-BIN-NUMERIC-BIGNUM",
            });
        }
    }

    // ── Fractional side ──────────────────────────────────────────────
    // Lay out base-10000 digits at their positions in a String of
    // length dscale.
    let dscale_usize = dscale.max(0) as usize;
    if dscale_usize > V1_MAX_FRAC_DIGITS {
        return Err(BinaryNumericError::OutOfRange {
            reason: format!(
                "dscale={dscale_usize} exceeds V1 cap {V1_MAX_FRAC_DIGITS}"
            ),
            arc: "SP-PG-EXTQ-BIN-NUMERIC-BIGNUM",
        });
    }
    if dscale_usize > 0 {
        // Initialize a zero-filled fractional buffer.
        let mut frac_buf = vec![b'0'; dscale_usize];
        for k in 0..ndigits_usize {
            let w = (weight as i32) - (k as i32);
            if w >= 0 {
                continue;
            }
            // This digit occupies fractional positions (-w-1)*4 .. (-w-1)*4+4.
            let start = ((-w - 1) as usize) * 4;
            // Write 4-digit base-10 representation of digit[k].
            let mut d = digits[k] as u32;
            for j in (0..4).rev() {
                let pos = start + j;
                if pos < dscale_usize {
                    frac_buf[pos] = b'0' + (d % 10) as u8;
                }
                d /= 10;
            }
            // The fractional contribution beyond dscale is truncated
            // (PG's numeric_out rounds, but for V1 we accept the
            // canonical encoder's output which doesn't include digits
            // beyond dscale).
        }
        frac_str = String::from_utf8(frac_buf).expect("ascii digits");
    }

    // Compose the output string.
    let sign_char = if sign == NUMERIC_NEG && (int_part != 0 || !is_all_zero(&frac_str)) {
        "-"
    } else {
        ""
    };
    let int_str = int_part.to_string();
    let out = if frac_str.is_empty() {
        format!("{sign_char}{int_str}")
    } else {
        format!("{sign_char}{int_str}.{frac_str}")
    };
    Ok(out)
}

/// SP-PG-EXTQ-BIN-NUMERIC T2 — encode the canonical PG-style decimal
/// string into the PG NUMERIC binary wire format.
///
/// **Accepts:** `[-]?\d+(\.\d+)?` — e.g. `"42"`, `"-3.14"`, `"0.0001"`,
/// `"12345.6789"`. Leading `+` is also tolerated. Empty string and
/// non-decimal text reject with `BadDecimalString`.
pub fn encode_numeric_binary(decimal_str: &str) -> Result<Vec<u8>, BinaryNumericError> {
    let s = decimal_str.trim();
    if s.is_empty() {
        return Err(BinaryNumericError::BadDecimalString {
            input: decimal_str.to_string(),
            reason: "empty string".into(),
        });
    }
    // Lex sign.
    let (sign, rest) = match s.as_bytes()[0] {
        b'-' => (NUMERIC_NEG, &s[1..]),
        b'+' => (NUMERIC_POS, &s[1..]),
        _ => (NUMERIC_POS, s),
    };
    if rest.is_empty() {
        return Err(BinaryNumericError::BadDecimalString {
            input: decimal_str.to_string(),
            reason: "sign without digits".into(),
        });
    }
    // Split integer + fractional parts.
    let (int_str_raw, frac_str_raw) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    // Validate all digits are ASCII decimal.
    for b in int_str_raw.bytes().chain(frac_str_raw.bytes()) {
        if !b.is_ascii_digit() {
            return Err(BinaryNumericError::BadDecimalString {
                input: decimal_str.to_string(),
                reason: format!("non-digit byte 0x{b:02X}"),
            });
        }
    }
    if int_str_raw.is_empty() && frac_str_raw.is_empty() {
        return Err(BinaryNumericError::BadDecimalString {
            input: decimal_str.to_string(),
            reason: "no digits".into(),
        });
    }
    // Strip leading zeros from integer part (keep one if all zeros).
    let int_str_trimmed = int_str_raw.trim_start_matches('0');
    let int_str = if int_str_trimmed.is_empty() && !int_str_raw.is_empty() {
        "0"
    } else if int_str_trimmed.is_empty() {
        "0"
    } else {
        int_str_trimmed
    };
    // Validate V1 range.
    if int_str.len() > 18 {
        return Err(BinaryNumericError::OutOfRange {
            reason: format!(
                "integer part has {} digits, exceeds V1 cap of 18",
                int_str.len()
            ),
            arc: "SP-PG-EXTQ-BIN-NUMERIC-BIGNUM",
        });
    }
    if frac_str_raw.len() > V1_MAX_FRAC_DIGITS {
        return Err(BinaryNumericError::OutOfRange {
            reason: format!(
                "fractional part has {} digits, exceeds V1 cap {V1_MAX_FRAC_DIGITS}",
                frac_str_raw.len()
            ),
            arc: "SP-PG-EXTQ-BIN-NUMERIC-BIGNUM",
        });
    }
    let int_val: i128 = if int_str == "0" {
        0
    } else {
        int_str
            .parse::<i128>()
            .map_err(|e| BinaryNumericError::BadDecimalString {
                input: decimal_str.to_string(),
                reason: format!("integer parse: {e}"),
            })?
    };
    if int_val.abs() >= V1_INT_MAGNITUDE_CAP {
        return Err(BinaryNumericError::OutOfRange {
            reason: format!(
                "integer value {int_val} >= V1 cap {V1_INT_MAGNITUDE_CAP}"
            ),
            arc: "SP-PG-EXTQ-BIN-NUMERIC-BIGNUM",
        });
    }
    // dscale = number of fractional digits.
    let dscale: i16 = frac_str_raw.len() as i16;

    // Special case: zero (sign=POS regardless of input sign for true zero).
    let frac_all_zero = frac_str_raw.bytes().all(|b| b == b'0');
    let int_all_zero = int_val == 0;
    if int_all_zero && frac_all_zero {
        // All-zero header except dscale (preserved so the wire matches
        // PG's `numeric_send` for "0.000...000").
        let mut out = Vec::with_capacity(8);
        out.extend_from_slice(&0i16.to_be_bytes()); // ndigits
        out.extend_from_slice(&0i16.to_be_bytes()); // weight
        out.extend_from_slice(&NUMERIC_POS.to_be_bytes()); // sign
        out.extend_from_slice(&dscale.to_be_bytes()); // dscale
        return Ok(out);
    }

    // Build base-10000 digit array. The strategy: lay out the digit
    // string (integer part + fractional part concatenated) with
    // padding on the fractional end so the total length is a multiple
    // of 4. Then chunk left-to-right into base-10000 digits.
    //
    // Weight of the leftmost base-10000 digit:
    //   - integer part has `int_str.len()` decimal digits
    //   - leftmost base-10000 digit covers the top 1..4 decimal digits
    //   - weight = ceil(int_str.len() / 4) - 1
    // Special: if integer part is "0", weight is determined by the
    // position of the first non-zero fractional digit.

    let (digits_buf, leftmost_weight) =
        compose_digits_for_encode(int_str, frac_str_raw)?;

    let ndigits: i16 = digits_buf.len() as i16;
    let mut out = Vec::with_capacity(8 + 2 * digits_buf.len());
    out.extend_from_slice(&ndigits.to_be_bytes());
    out.extend_from_slice(&leftmost_weight.to_be_bytes());
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&dscale.to_be_bytes());
    for d in digits_buf {
        out.extend_from_slice(&d.to_be_bytes());
    }
    Ok(out)
}

/// Lay out `int_str` (no leading zeros, possibly `"0"`) + `frac_str`
/// (raw, possibly empty) into a base-10000 digit array. Returns the
/// digits (most-significant first) + the weight of `digits[0]`.
///
/// Strategy: concatenate `int_str` + `frac_str`. Determine how many
/// trailing zeros to add to `frac_str` so the fractional segment is a
/// multiple of 4. Chunk into base-10000 groups of 4 decimal digits
/// (right-aligned in the integer side, left-aligned in the fractional
/// side). Strip trailing zero base-10000 groups (PG's canonical form).
fn compose_digits_for_encode(
    int_str: &str,
    frac_str: &str,
) -> Result<(Vec<i16>, i16), BinaryNumericError> {
    // INTEGER SIDE: pad on the LEFT with zeros so total length is a
    // multiple of 4. Each 4-digit group becomes one base-10000 digit.
    let int_len = int_str.len();
    let int_pad = (4 - int_len % 4) % 4;
    let int_padded = format!("{}{}", "0".repeat(int_pad), int_str);
    let int_groups = int_padded.len() / 4;

    // FRACTIONAL SIDE: pad on the RIGHT with zeros so total length is
    // a multiple of 4.
    let frac_len = frac_str.len();
    let frac_pad = (4 - frac_len % 4) % 4;
    let frac_padded = format!("{}{}", frac_str, "0".repeat(frac_pad));
    let frac_groups = frac_padded.len() / 4;

    // Build digits.
    let mut digits: Vec<i16> = Vec::with_capacity(int_groups + frac_groups);
    for g in 0..int_groups {
        let s = &int_padded[g * 4..g * 4 + 4];
        let n: i16 = s
            .parse()
            .map_err(|e| BinaryNumericError::BadDecimalString {
                input: int_padded.clone(),
                reason: format!("base-10000 parse: {e}"),
            })?;
        digits.push(n);
    }
    for g in 0..frac_groups {
        let s = &frac_padded[g * 4..g * 4 + 4];
        let n: i16 = s
            .parse()
            .map_err(|e| BinaryNumericError::BadDecimalString {
                input: frac_padded.clone(),
                reason: format!("base-10000 parse: {e}"),
            })?;
        digits.push(n);
    }

    // Determine leftmost weight.
    // After integer-padding, weight of the leftmost digit is
    // `int_groups - 1`. (If `int_str` was "0", int_groups will be 1
    // with digit value 0.)
    let leftmost_weight = (int_groups as i32) - 1;

    // Strip leading zero base-10000 digits (PG canonical form): if
    // digit[0] == 0 AND there are more digits, drop it and shift the
    // leftmost weight down. Repeat.
    let mut weight = leftmost_weight;
    while digits.len() > 1 && digits[0] == 0 {
        digits.remove(0);
        weight -= 1;
    }
    // Strip trailing zero base-10000 digits ONLY from the fractional
    // side (i.e. when weight - (len-1) < 0). PG canonical form has no
    // trailing zero groups that are entirely in the fractional region.
    while digits.len() > 1 {
        let last_idx = digits.len() - 1;
        let last_weight = weight - (last_idx as i32);
        if last_weight < 0 && digits[last_idx] == 0 {
            digits.pop();
        } else {
            break;
        }
    }
    // After all stripping, if we have just one zero digit and weight=0,
    // the value is effectively zero — handled by the encoder's
    // earlier `int_all_zero && frac_all_zero` short-circuit. If we
    // somehow arrive here with digits == [0], emit it.

    // Cap weight in i16 range. With our V1 caps this is always safe.
    let weight_i16: i16 = i16::try_from(weight).map_err(|_| BinaryNumericError::OutOfRange {
        reason: format!("weight {weight} doesn't fit in i16"),
        arc: "SP-PG-EXTQ-BIN-NUMERIC-BIGNUM",
    })?;
    Ok((digits, weight_i16))
}

/// Compute `10000^exp` as i128, returning None on overflow. V1 caps
/// `exp` at 4 (since `10000^5 = 10^20 > i128::MAX / 1000`); the
/// `V1_INT_MAGNITUDE_CAP < 10^18 < 10000^5` guard makes higher
/// exponents unreachable.
fn pow10000(exp: u32) -> Option<i128> {
    let mut acc: i128 = 1;
    for _ in 0..exp {
        acc = acc.checked_mul(10_000)?;
    }
    Some(acc)
}

fn format_zero(dscale: i16) -> String {
    if dscale <= 0 {
        "0".to_string()
    } else {
        let mut s = String::with_capacity(2 + dscale as usize);
        s.push('0');
        s.push('.');
        for _ in 0..dscale {
            s.push('0');
        }
        s
    }
}

fn is_all_zero(s: &str) -> bool {
    s.bytes().all(|b| b == b'0')
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── decode KATs ──────────────────────────────────────────────────

    /// Decode the all-zero header → "0".
    #[test]
    fn t2_decode_zero_returns_zero_string() {
        let bytes = [0u8; 8];
        assert_eq!(decode_numeric_binary(&bytes).unwrap(), "0");
    }

    /// Decode (ndigits=1, weight=0, sign=POS, dscale=0, digit=42) → "42".
    #[test]
    fn t2_decode_one_digit_42() {
        let bytes = [
            0x00, 0x01, // ndigits=1
            0x00, 0x00, // weight=0
            0x00, 0x00, // sign=POS
            0x00, 0x00, // dscale=0
            0x00, 0x2A, // digit[0]=42
        ];
        assert_eq!(decode_numeric_binary(&bytes).unwrap(), "42");
    }

    /// Decode (ndigits=2, weight=0, sign=POS, dscale=1, [1, 5000]) → "1.5".
    #[test]
    fn t2_decode_one_and_a_half() {
        let bytes = [
            0x00, 0x02, // ndigits=2
            0x00, 0x00, // weight=0
            0x00, 0x00, // sign=POS
            0x00, 0x01, // dscale=1
            0x00, 0x01, // digit[0]=1
            0x13, 0x88, // digit[1]=5000
        ];
        assert_eq!(decode_numeric_binary(&bytes).unwrap(), "1.5");
    }

    /// Decode (ndigits=3, weight=1, sign=POS, dscale=4, [1, 2345, 6789])
    /// → "12345.6789".
    #[test]
    fn t2_decode_pi_ish_12345_6789() {
        let bytes = [
            0x00, 0x03, // ndigits=3
            0x00, 0x01, // weight=1
            0x00, 0x00, // sign=POS
            0x00, 0x04, // dscale=4
            0x00, 0x01, // digit[0]=1
            0x09, 0x29, // digit[1]=2345
            0x1A, 0x85, // digit[2]=6789
        ];
        assert_eq!(decode_numeric_binary(&bytes).unwrap(), "12345.6789");
    }

    /// Decode (ndigits=2, weight=0, sign=NEG, dscale=2, [3, 1400])
    /// → "-3.14".
    #[test]
    fn t2_decode_negative_3_14() {
        let bytes = [
            0x00, 0x02, // ndigits=2
            0x00, 0x00, // weight=0
            0x40, 0x00, // sign=NEG
            0x00, 0x02, // dscale=2
            0x00, 0x03, // digit[0]=3
            0x05, 0x78, // digit[1]=1400
        ];
        assert_eq!(decode_numeric_binary(&bytes).unwrap(), "-3.14");
    }

    /// Decode (ndigits=1, weight=-1, sign=POS, dscale=4, [1]) → "0.0001".
    #[test]
    fn t2_decode_small_fraction_0_0001() {
        let bytes = [
            0x00, 0x01, // ndigits=1
            0xFF, 0xFF, // weight=-1
            0x00, 0x00, // sign=POS
            0x00, 0x04, // dscale=4
            0x00, 0x01, // digit[0]=1
        ];
        assert_eq!(decode_numeric_binary(&bytes).unwrap(), "0.0001");
    }

    /// Decode NaN sign rejected.
    #[test]
    fn t2_decode_nan_rejected() {
        let bytes = [
            0x00, 0x00, // ndigits=0
            0x00, 0x00, // weight=0
            0xC0, 0x00, // sign=NAN
            0x00, 0x00, // dscale=0
        ];
        assert_eq!(decode_numeric_binary(&bytes), Err(BinaryNumericError::NaN));
    }

    /// Decode bad sign rejected.
    #[test]
    fn t2_decode_bad_sign_rejected() {
        let bytes = [
            0x00, 0x00, // ndigits=0
            0x00, 0x00, // weight=0
            0x12, 0x34, // sign=invalid
            0x00, 0x00, // dscale=0
        ];
        match decode_numeric_binary(&bytes) {
            Err(BinaryNumericError::BadSign { sign }) => assert_eq!(sign, 0x1234),
            other => panic!("expected BadSign, got {other:?}"),
        }
    }

    /// Decode truncated digit array rejected.
    #[test]
    fn t2_decode_truncated_rejected() {
        let bytes = [
            0x00, 0x02, // ndigits=2 (so 4 bytes of digits expected)
            0x00, 0x00, // weight=0
            0x00, 0x00, // sign=POS
            0x00, 0x00, // dscale=0
            0x00, 0x01, // only digit[0]
                        // digit[1] missing
        ];
        assert!(matches!(
            decode_numeric_binary(&bytes),
            Err(BinaryNumericError::Truncated { .. })
        ));
    }

    /// Decode <8-byte header rejected.
    #[test]
    fn t2_decode_wrong_header_length_rejected() {
        let bytes = [0u8; 7];
        assert!(matches!(
            decode_numeric_binary(&bytes),
            Err(BinaryNumericError::WrongLength { .. })
        ));
    }

    /// Decode bad per-digit value rejected.
    #[test]
    fn t2_decode_bad_digit_rejected() {
        let bytes = [
            0x00, 0x01, // ndigits=1
            0x00, 0x00, // weight=0
            0x00, 0x00, // sign=POS
            0x00, 0x00, // dscale=0
            0x27, 0x11, // digit[0]=10001 (out of range 0..9999)
        ];
        match decode_numeric_binary(&bytes) {
            Err(BinaryNumericError::BadDigit { position, value }) => {
                assert_eq!(position, 0);
                assert_eq!(value, 0x2711);
            }
            other => panic!("expected BadDigit, got {other:?}"),
        }
    }

    // ── encode KATs ──────────────────────────────────────────────────

    /// Encode "0" → all-zero header.
    #[test]
    fn t2_encode_zero() {
        assert_eq!(encode_numeric_binary("0").unwrap(), vec![0u8; 8]);
    }

    /// Encode "42" → (ndigits=1, weight=0, sign=POS, dscale=0, digit=42).
    #[test]
    fn t2_encode_42() {
        let out = encode_numeric_binary("42").unwrap();
        assert_eq!(
            out,
            vec![
                0x00, 0x01, // ndigits
                0x00, 0x00, // weight
                0x00, 0x00, // sign
                0x00, 0x00, // dscale
                0x00, 0x2A, // digit[0]
            ]
        );
    }

    /// Encode "1.5" → (ndigits=2, weight=0, sign=POS, dscale=1, [1, 5000]).
    #[test]
    fn t2_encode_one_and_a_half() {
        let out = encode_numeric_binary("1.5").unwrap();
        assert_eq!(
            out,
            vec![
                0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // header
                0x00, 0x01, // digit[0]=1
                0x13, 0x88, // digit[1]=5000
            ]
        );
    }

    /// Encode "12345.6789" → (ndigits=3, weight=1, sign=POS, dscale=4,
    /// [1, 2345, 6789]).
    #[test]
    fn t2_encode_pi_ish_12345_6789() {
        let out = encode_numeric_binary("12345.6789").unwrap();
        assert_eq!(
            out,
            vec![
                0x00, 0x03, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, // header
                0x00, 0x01, // digit[0]=1
                0x09, 0x29, // digit[1]=2345
                0x1A, 0x85, // digit[2]=6789
            ]
        );
    }

    /// Encode "-3.14" → (ndigits=2, weight=0, sign=NEG, dscale=2, [3, 1400]).
    #[test]
    fn t2_encode_negative_3_14() {
        let out = encode_numeric_binary("-3.14").unwrap();
        assert_eq!(
            out,
            vec![
                0x00, 0x02, 0x00, 0x00, 0x40, 0x00, 0x00, 0x02, // header
                0x00, 0x03, // digit[0]=3
                0x05, 0x78, // digit[1]=1400
            ]
        );
    }

    /// Encode "0.0001" → (ndigits=1, weight=-1, sign=POS, dscale=4, [1]).
    #[test]
    fn t2_encode_small_fraction_0_0001() {
        let out = encode_numeric_binary("0.0001").unwrap();
        assert_eq!(
            out,
            vec![
                0x00, 0x01, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x04, // header
                0x00, 0x01, // digit[0]=1
            ]
        );
    }

    /// Encode bad decimal string rejected.
    #[test]
    fn t2_encode_bad_decimal_string_rejected() {
        assert!(matches!(
            encode_numeric_binary("abc"),
            Err(BinaryNumericError::BadDecimalString { .. })
        ));
        assert!(matches!(
            encode_numeric_binary(""),
            Err(BinaryNumericError::BadDecimalString { .. })
        ));
        assert!(matches!(
            encode_numeric_binary("-"),
            Err(BinaryNumericError::BadDecimalString { .. })
        ));
        assert!(matches!(
            encode_numeric_binary("3.14.15"),
            Err(BinaryNumericError::BadDecimalString { .. })
        ));
    }

    /// Encode out-of-range (≥10^18 integer or >18 frac digits) rejected.
    #[test]
    fn t2_encode_out_of_range_rejected() {
        // 19-digit integer.
        let big = "1".to_string() + &"0".repeat(18); // 10^18
        match encode_numeric_binary(&big) {
            Err(BinaryNumericError::OutOfRange { arc, .. }) => {
                assert_eq!(arc, "SP-PG-EXTQ-BIN-NUMERIC-BIGNUM");
            }
            other => panic!("expected OutOfRange for 10^18, got {other:?}"),
        }
        // 19-digit fractional part.
        let long_frac = "0.".to_string() + &"1".repeat(19);
        match encode_numeric_binary(&long_frac) {
            Err(BinaryNumericError::OutOfRange { arc, .. }) => {
                assert_eq!(arc, "SP-PG-EXTQ-BIN-NUMERIC-BIGNUM");
            }
            other => panic!("expected OutOfRange for 19 frac digits, got {other:?}"),
        }
    }

    // ── round-trip identity KATs ────────────────────────────────────

    /// decode(encode(s)) == s for every canonical example.
    #[test]
    fn t2_round_trip_encode_decode_identity() {
        let cases = [
            "0", "42", "1.5", "12345.6789", "-3.14", "0.0001",
            // Additional shapes
            "100", "-1", "999999999999999999", // 10^18 - 1
            "0.1", "0.5", "0.123456789012345678", // 18 frac digits
            "-999999999999999.999",
        ];
        for s in cases {
            let bytes = encode_numeric_binary(s).expect(s);
            let decoded = decode_numeric_binary(&bytes).expect(s);
            assert_eq!(decoded, s, "round-trip mismatch for {s:?}");
        }
    }

    /// encode(decode(bytes)) == bytes for every canonical example.
    #[test]
    fn t2_round_trip_decode_encode_identity() {
        let cases: &[&[u8]] = &[
            // 0 (all-zero header)
            &[0u8; 8],
            // 42
            &[0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A],
            // 1.5
            &[
                0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x13, 0x88,
            ],
            // 12345.6789
            &[
                0x00, 0x03, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x01, 0x09, 0x29, 0x1A, 0x85,
            ],
            // -3.14
            &[
                0x00, 0x02, 0x00, 0x00, 0x40, 0x00, 0x00, 0x02, 0x00, 0x03, 0x05, 0x78,
            ],
            // 0.0001
            &[0x00, 0x01, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x04, 0x00, 0x01],
        ];
        for bytes in cases {
            let s = decode_numeric_binary(bytes).expect("decode");
            let re_encoded = encode_numeric_binary(&s).expect("encode");
            assert_eq!(
                &re_encoded[..],
                *bytes,
                "round-trip mismatch decoded={s:?} for bytes={bytes:?}"
            );
        }
    }

    /// 1000 random rationals in the V1 range round-trip cleanly.
    #[test]
    fn t2_round_trip_random_rationals() {
        // Simple LCG for determinism (no rand dep).
        let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
        let next = |s: &mut u64| -> u64 {
            *s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            *s
        };
        for _ in 0..1000 {
            let int_digits = (next(&mut state) % 16) as usize; // 0..15 digits
            let frac_digits = (next(&mut state) % 9) as usize; // 0..8 digits
            let neg = (next(&mut state) % 2) == 0;

            // Build a random integer + fractional string.
            let mut int_str = String::new();
            for _ in 0..int_digits {
                int_str.push((b'0' + (next(&mut state) % 10) as u8) as char);
            }
            let int_str = int_str.trim_start_matches('0').to_string();
            let int_str = if int_str.is_empty() { "0".to_string() } else { int_str };
            let mut frac_str = String::new();
            for _ in 0..frac_digits {
                frac_str.push((b'0' + (next(&mut state) % 10) as u8) as char);
            }
            // Drop trailing zeros in the fractional part for canonical form.
            // Actually, PG NUMERIC preserves trailing zeros via dscale, so
            // we keep them — the round-trip identity requires it.
            let composed = if frac_str.is_empty() {
                int_str.clone()
            } else {
                format!("{int_str}.{frac_str}")
            };
            // Apply sign — but "0" + neg = "0" (no negative zero in canonical form).
            let composed_signed = if neg
                && !(int_str == "0" && frac_str.bytes().all(|b| b == b'0'))
            {
                format!("-{composed}")
            } else {
                composed
            };
            let bytes = match encode_numeric_binary(&composed_signed) {
                Ok(b) => b,
                Err(BinaryNumericError::OutOfRange { .. }) => continue,
                Err(e) => panic!("encode({composed_signed:?}) failed: {e:?}"),
            };
            let decoded = decode_numeric_binary(&bytes).expect("decode");
            assert_eq!(decoded, composed_signed, "round-trip mismatch");
        }
    }

    /// dscale preserved on zero: "0.00" round-trips with dscale=2.
    #[test]
    fn t2_round_trip_zero_with_dscale() {
        let bytes = encode_numeric_binary("0.00").unwrap();
        // ndigits=0, weight=0, sign=POS, dscale=2
        assert_eq!(
            bytes,
            vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02]
        );
        assert_eq!(decode_numeric_binary(&bytes).unwrap(), "0.00");
    }
}
