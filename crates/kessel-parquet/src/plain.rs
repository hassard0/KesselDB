//! PLAIN-encoding page decode (Apache Parquet "Data Pages"/PLAIN).
//! INT32/INT64 LE; FLOAT/DOUBLE IEEE-754 LE; BOOLEAN bit-packed
//! LSB-first; BYTE_ARRAY = [u32 LE len][bytes]. INT96 = 8-byte LE
//! `nanos_of_day` u64 + 4-byte LE `julian_day` u32 → ns since the
//! Unix epoch (Julian day 2440588 == 1970-01-01 UTC). FLBA =
//! `type_length` raw bytes per value (no length prefix). DECIMAL
//! over INT32/INT64/FLBA/BYTE_ARRAY → `PqValue::Decimal` with
//! sign-extended i128 unscaled value. All bounds-checked.
#![allow(dead_code)]

use crate::meta::Type;
use crate::{PqError, PqValue};

fn bad(s: &str) -> PqError {
    PqError::Bad(s.to_string())
}

/// DECIMAL logical-type parameters carried alongside a physical type.
/// `precision` is in `1..=38` and `scale` is in `0..=precision` (validated
/// by `build_plain_spec` before any decode).
#[derive(Clone, Copy, Debug)]
pub(crate) struct DecimalSpec {
    pub(crate) precision: u32,
    pub(crate) scale: u32,
}

/// Per-leaf decode spec built once at the file-level gate. The hot
/// `decode_plain` loop branches on `ptype` + `decimal` only; all
/// metadata (FLBA width, precision/scale ranges, physical-vs-logical
/// agreement) is pre-validated.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PlainSpec {
    pub(crate) ptype: Type,
    /// Some(n) iff ptype == FixedLenByteArray; n is the byte width
    /// (validated 1..=16 when DECIMAL, 1..=65_536 otherwise).
    pub(crate) flba_len: Option<usize>,
    /// Some iff this leaf carries a DECIMAL logical type.
    pub(crate) decimal: Option<DecimalSpec>,
}

impl PlainSpec {
    /// Non-DECIMAL plain leaf. Arbitrary-FLBA must use `flba`.
    /// This constructor is for the 7 plain physical types
    /// (Bool/Int32/Int64/Float/Double/ByteArray/Int96).
    /// FLBA must specify a width via `flba` instead.
    pub(crate) fn plain(ptype: Type) -> Self {
        Self {
            ptype,
            flba_len: None,
            decimal: None,
        }
    }
    pub(crate) fn flba(n: usize) -> Self {
        Self {
            ptype: Type::FixedLenByteArray,
            flba_len: Some(n),
            decimal: None,
        }
    }
    pub(crate) fn flba_decimal(n: usize, precision: u32, scale: u32) -> Self {
        Self {
            ptype: Type::FixedLenByteArray,
            flba_len: Some(n),
            decimal: Some(DecimalSpec { precision, scale }),
        }
    }
    pub(crate) fn int_decimal(ptype: Type, precision: u32, scale: u32) -> Self {
        // ptype must be Int32 or Int64; caller's responsibility.
        Self {
            ptype,
            flba_len: None,
            decimal: Some(DecimalSpec { precision, scale }),
        }
    }
    pub(crate) fn byte_array_decimal(precision: u32, scale: u32) -> Self {
        Self {
            ptype: Type::ByteArray,
            flba_len: None,
            decimal: Some(DecimalSpec { precision, scale }),
        }
    }
}

// INT96 → Unix-epoch conversion constants. Julian day 2_440_588 is
// 1970-01-01 UTC (parquet.thrift INT96 is a legacy nanosecond-timestamp
// physical type whose 12 bytes are `nanos_of_day` (u64 LE) + `julian_day`
// (u32 LE)). NS_PER_DAY = 86_400 * 1_000_000_000 = 86_400_000_000_000.
const JULIAN_UNIX_EPOCH: i64 = 2_440_588;
const NS_PER_DAY: i64 = 86_400 * 1_000_000_000;

/// Sign-extend a big-endian two's-complement byte slice of length `n`
/// (where `1 <= n <= 16`) into an `i128`.
///
/// Safety contract (triple-gated by callers; `debug_assert` enforces
/// in tests; unreachable in release if all three gates hold):
///   1. `build_plain_spec`: rejects FLBA DECIMAL `n == 0 || n > 16`.
///   2. `decode_plain` FLBA arm: runtime guard `if n > 16 { return Err }`.
///   3. `decode_plain` BYTE_ARRAY arm: runtime guard
///      `if len == 0 || len > 16 { return Err }`.
/// Do NOT remove or weaken any of these without auditing the others.
fn flba_be_to_i128(bytes: &[u8]) -> i128 {
    debug_assert!(!bytes.is_empty() && bytes.len() <= 16);
    let sign_byte = if bytes[0] & 0x80 != 0 { 0xFFu8 } else { 0x00u8 };
    let mut be16 = [sign_byte; 16];
    // `16 - bytes.len()` is in 0..=15 since 1 <= bytes.len() <= 16.
    be16[16 - bytes.len()..].copy_from_slice(bytes);
    i128::from_be_bytes(be16)
}

pub(crate) fn decode_plain(
    data: &[u8],
    spec: PlainSpec,
    count: usize,
) -> Result<Vec<PqValue>, PqError> {
    // PENTEST HARDENING (Task 12): `count` is the attacker-controlled
    // `dp_num_values` from the page header (up to i32::MAX). Sizing the
    // output `Vec` from the raw `count` lets a lying header
    // (`dp_num_values = i32::MAX`) pre-reserve tens of GB and OOM-abort
    // the process BEFORE the per-type `data.get(..need)?` bounds check
    // can reject it. Bound the reservation by what the page bytes can
    // possibly hold: every PLAIN value consumes >= 1 byte (BOOLEAN is
    // >= 1 bit, so `data.len()` is still a safe non-OOM upper bound and
    // never under-reserves harmfully — the real values are still
    // `push`ed and the per-type `.get(..need)?` still returns
    // `PqError::Bad` for genuinely short data). This caps the eager
    // allocation without altering correct-decode behavior.
    // NOTE: for BOOLEAN (bit-packed, >=1 value per BIT) data.len() may
    // under-reserve by up to 8x the decoded count — harmless: the loop
    // still push()es every value (reallocating as needed). The point of
    // the .min is purely the OOM bound vs a lying huge `count`; do NOT
    // revert to with_capacity(count).
    let mut out = Vec::with_capacity(count.min(data.len()));
    match spec.ptype {
        Type::Int32 => {
            let need = count.checked_mul(4).ok_or_else(|| bad("int32 ovf"))?;
            let s = data.get(..need).ok_or_else(|| bad("int32 truncated"))?;
            if let Some(dec) = spec.decimal {
                let scale = dec.scale as i32;
                for ch in s.chunks_exact(4) {
                    let v = i32::from_le_bytes(ch.try_into().unwrap()) as i128;
                    out.push(PqValue::Decimal { unscaled: v, scale });
                }
            } else {
                for ch in s.chunks_exact(4) {
                    out.push(PqValue::I64(
                        i32::from_le_bytes(ch.try_into().unwrap()) as i64,
                    ));
                }
            }
        }
        Type::Int64 => {
            let need = count.checked_mul(8).ok_or_else(|| bad("int64 ovf"))?;
            let s = data.get(..need).ok_or_else(|| bad("int64 truncated"))?;
            if let Some(dec) = spec.decimal {
                let scale = dec.scale as i32;
                for ch in s.chunks_exact(8) {
                    let v = i64::from_le_bytes(ch.try_into().unwrap()) as i128;
                    out.push(PqValue::Decimal { unscaled: v, scale });
                }
            } else {
                for ch in s.chunks_exact(8) {
                    out.push(PqValue::I64(i64::from_le_bytes(
                        ch.try_into().unwrap(),
                    )));
                }
            }
        }
        Type::Float => {
            if spec.decimal.is_some() {
                return Err(bad("DECIMAL on incompatible physical type"));
            }
            let need = count.checked_mul(4).ok_or_else(|| bad("f32 ovf"))?;
            let s = data.get(..need).ok_or_else(|| bad("f32 truncated"))?;
            for ch in s.chunks_exact(4) {
                out.push(PqValue::F64(
                    f32::from_le_bytes(ch.try_into().unwrap()) as f64,
                ));
            }
        }
        Type::Double => {
            if spec.decimal.is_some() {
                return Err(bad("DECIMAL on incompatible physical type"));
            }
            let need = count.checked_mul(8).ok_or_else(|| bad("f64 ovf"))?;
            let s = data.get(..need).ok_or_else(|| bad("f64 truncated"))?;
            for ch in s.chunks_exact(8) {
                out.push(PqValue::F64(f64::from_le_bytes(
                    ch.try_into().unwrap(),
                )));
            }
        }
        Type::Boolean => {
            if spec.decimal.is_some() {
                return Err(bad("DECIMAL on incompatible physical type"));
            }
            let need = count.div_ceil(8);
            let s = data.get(..need).ok_or_else(|| bad("bool truncated"))?;
            for i in 0..count {
                let byte = s[i / 8];
                out.push(PqValue::Bool((byte >> (i % 8)) & 1 == 1));
            }
        }
        Type::ByteArray => {
            let mut p = 0usize;
            if let Some(dec) = spec.decimal {
                let scale = dec.scale as i32;
                for _ in 0..count {
                    let lb = data
                        .get(p..p + 4)
                        .ok_or_else(|| bad("byte_array len truncated"))?;
                    let len = u32::from_le_bytes(lb.try_into().unwrap())
                        as usize;
                    p += 4;
                    // BYTE_ARRAY DECIMAL: unscaled byte-width must be
                    // 1..=16 (else can't sign-extend into i128 / zero
                    // bytes is malformed). Pentest: lying huge `len`
                    // is caught here BEFORE the get() bounds check.
                    if len == 0 || len > 16 {
                        return Err(bad(
                            "BYTE_ARRAY DECIMAL width out of range (1..=16)",
                        ));
                    }
                    let v = data
                        .get(p..p.checked_add(len)
                            .ok_or_else(|| bad("byte_array len ovf"))?)
                        .ok_or_else(|| bad("byte_array data truncated"))?;
                    let unscaled = flba_be_to_i128(v);
                    out.push(PqValue::Decimal { unscaled, scale });
                    p += len;
                }
            } else {
                for _ in 0..count {
                    let lb = data
                        .get(p..p + 4)
                        .ok_or_else(|| bad("byte_array len truncated"))?;
                    let len = u32::from_le_bytes(lb.try_into().unwrap())
                        as usize;
                    p += 4;
                    let v = data
                        .get(p..p.checked_add(len)
                            .ok_or_else(|| bad("byte_array len ovf"))?)
                        .ok_or_else(|| bad("byte_array data truncated"))?;
                    out.push(PqValue::Bytes(v.to_vec()));
                    p += len;
                }
            }
        }
        Type::Int96 => {
            // INT96 carries no DECIMAL metadata in the SP108 scope
            // (rejected upstream in build_plain_spec). 12 bytes per
            // value: 8 bytes LE u64 nanos_of_day + 4 bytes LE u32
            // julian_day. Convert to ns since the Unix epoch via the
            // checked-arithmetic recipe — every step rejects hostile
            // values as typed Bad, never panic.
            let need = count.checked_mul(12).ok_or_else(|| bad("int96 ovf"))?;
            let s = data.get(..need).ok_or_else(|| bad("int96 truncated"))?;
            for ch in s.chunks_exact(12) {
                let nod_bytes: [u8; 8] = ch[..8].try_into().unwrap();
                let jd_bytes: [u8; 4] = ch[8..12].try_into().unwrap();
                let nod = u64::from_le_bytes(nod_bytes);
                let jd = u32::from_le_bytes(jd_bytes);
                // parquet spec: nanos_of_day ∈ [0, 86_400_000_000_000).
                if nod >= 86_400_000_000_000 {
                    return Err(bad("int96 nanos-of-day out of range"));
                }
                let day_offset = i64::from(jd)
                    .checked_sub(JULIAN_UNIX_EPOCH)
                    .ok_or_else(|| bad("int96 julian day range"))?;
                let day_ns = day_offset
                    .checked_mul(NS_PER_DAY)
                    .ok_or_else(|| bad("int96 ns overflow"))?;
                // nod < 86_400_000_000_000 fits in i64 trivially.
                let nod_i64 = i64::try_from(nod)
                    .map_err(|_| bad("int96 nanos-of-day too large"))?;
                let ns = day_ns
                    .checked_add(nod_i64)
                    .ok_or_else(|| bad("int96 ns overflow"))?;
                out.push(PqValue::Timestamp(ns));
            }
        }
        Type::FixedLenByteArray => {
            let n = spec
                .flba_len
                .ok_or_else(|| bad("FLBA missing type_length"))?;
            if n == 0 {
                return Err(bad("FLBA zero type_length"));
            }
            let need = count.checked_mul(n).ok_or_else(|| bad("flba ovf"))?;
            let s = data.get(..need).ok_or_else(|| bad("flba truncated"))?;
            if let Some(dec) = spec.decimal {
                // build_plain_spec guarantees n in 1..=16 when DECIMAL,
                // but defense-in-depth: a future code path passing a
                // hand-built spec with n>16 would corrupt the i128
                // sign-extend. Reject as typed Bad.
                if n > 16 {
                    return Err(bad(
                        "DECIMAL FLBA byte width > 16 (overflows i128)",
                    ));
                }
                let scale = dec.scale as i32;
                for ch in s.chunks_exact(n) {
                    let unscaled = flba_be_to_i128(ch);
                    out.push(PqValue::Decimal { unscaled, scale });
                }
            } else {
                for ch in s.chunks_exact(n) {
                    out.push(PqValue::Bytes(ch.to_vec()));
                }
            }
        }
        Type::Other(_) => {
            return Err(PqError::Unsupported(format!(
                "physical type {:?} (OBJ-2c)",
                spec.ptype
            )))
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::Type;
    use crate::PqValue;

    #[test]
    fn plain_decode_spec_kat() {
        // INT64: values 7, -2  -> 7i64 LE, (-2)i64 LE
        let mut b = Vec::new();
        b.extend_from_slice(&7i64.to_le_bytes());
        b.extend_from_slice(&(-2i64).to_le_bytes());
        assert_eq!(
            decode_plain(&b, PlainSpec::plain(Type::Int64), 2).unwrap(),
            vec![PqValue::I64(7), PqValue::I64(-2)]
        );
        // INT32: 1, 1000
        let mut c = Vec::new();
        c.extend_from_slice(&1i32.to_le_bytes());
        c.extend_from_slice(&1000i32.to_le_bytes());
        assert_eq!(
            decode_plain(&c, PlainSpec::plain(Type::Int32), 2).unwrap(),
            vec![PqValue::I64(1), PqValue::I64(1000)]
        );
        // DOUBLE: 1.5, -0.25 (exact in f64)
        let mut d = Vec::new();
        d.extend_from_slice(&1.5f64.to_le_bytes());
        d.extend_from_slice(&(-0.25f64).to_le_bytes());
        assert_eq!(
            decode_plain(&d, PlainSpec::plain(Type::Double), 2).unwrap(),
            vec![PqValue::F64(1.5), PqValue::F64(-0.25)]
        );
        // FLOAT: 2.0
        let e = 2.0f32.to_le_bytes().to_vec();
        assert_eq!(
            decode_plain(&e, PlainSpec::plain(Type::Float), 1).unwrap(),
            vec![PqValue::F64(2.0)]
        );
        // BOOLEAN bit-packed LSB-first: true,false,true -> 0b00000101
        assert_eq!(
            decode_plain(&[0b0000_0101], PlainSpec::plain(Type::Boolean), 3).unwrap(),
            vec![PqValue::Bool(true), PqValue::Bool(false), PqValue::Bool(true)]
        );
        // BYTE_ARRAY: "hi","x" -> [2,0,0,0]"hi"[1,0,0,0]"x"
        let mut g = Vec::new();
        g.extend_from_slice(&2u32.to_le_bytes()); g.extend_from_slice(b"hi");
        g.extend_from_slice(&1u32.to_le_bytes()); g.extend_from_slice(b"x");
        assert_eq!(
            decode_plain(&g, PlainSpec::plain(Type::ByteArray), 2).unwrap(),
            vec![PqValue::Bytes(b"hi".to_vec()), PqValue::Bytes(b"x".to_vec())]
        );
    }

    #[test]
    fn plain_truncated_is_typed_error() {
        assert!(decode_plain(&[0u8; 3], PlainSpec::plain(Type::Int64), 1).is_err());
        let mut g = Vec::new();
        g.extend_from_slice(&99u32.to_le_bytes()); // lying length
        g.extend_from_slice(b"hi");
        assert!(decode_plain(&g, PlainSpec::plain(Type::ByteArray), 1).is_err());
    }

    // ── SP108 T3 PLAIN INT96/FLBA/DECIMAL KATs ──────────────────────────
    //
    // INT96 PLAIN decode: 12 bytes per value, LE.
    // Hand-derived: julian_day=2_440_588 is 1970-01-01 UTC (parquet.thrift);
    // NS_PER_DAY = 86_400 * 1_000_000_000 = 86_400_000_000_000.
    //   row 0: nod=0, jd=2_440_588 → 0 ns since epoch
    //   row 1: nod=0, jd=2_440_589 → +1 day → +86_400_000_000_000 ns
    //   row 2: nod=0, jd=2_440_587 → -1 day → -86_400_000_000_000 ns

    #[test]
    fn plain_decode_int96_to_timestamp() {
        let mut b = Vec::new();
        for &(nod, jd) in &[(0u64, 2_440_588u32), (0, 2_440_589), (0, 2_440_587)] {
            b.extend_from_slice(&nod.to_le_bytes());
            b.extend_from_slice(&jd.to_le_bytes());
        }
        let spec = PlainSpec::plain(Type::Int96);
        let got = decode_plain(&b, spec, 3).unwrap();
        assert_eq!(
            got,
            vec![
                PqValue::Timestamp(0),
                PqValue::Timestamp(86_400_000_000_000),
                PqValue::Timestamp(-86_400_000_000_000),
            ]
        );
    }

    #[test]
    fn plain_decode_int96_with_nanos_of_day() {
        // nod=1_500_000_000 (1.5 seconds) + jd=2_440_588 (epoch day)
        // → ns since epoch = 1_500_000_000.
        let mut b = Vec::new();
        b.extend_from_slice(&1_500_000_000u64.to_le_bytes());
        b.extend_from_slice(&2_440_588u32.to_le_bytes());
        assert_eq!(
            decode_plain(&b, PlainSpec::plain(Type::Int96), 1).unwrap(),
            vec![PqValue::Timestamp(1_500_000_000)]
        );
    }

    #[test]
    fn plain_decode_int96_rejects_nanos_of_day_out_of_range() {
        // nod = 86_400_000_000_000 (the upper bound, EXCLUSIVE) → Bad.
        let mut b = Vec::new();
        b.extend_from_slice(&86_400_000_000_000u64.to_le_bytes());
        b.extend_from_slice(&2_440_588u32.to_le_bytes());
        let err = decode_plain(&b, PlainSpec::plain(Type::Int96), 1).unwrap_err();
        assert!(matches!(err, PqError::Bad(_)));
    }

    #[test]
    fn plain_decode_int96_truncated_is_bad() {
        // 11 bytes (one byte short of a single INT96 value) → Bad, no panic.
        let err = decode_plain(&[0u8; 11], PlainSpec::plain(Type::Int96), 1).unwrap_err();
        assert!(matches!(err, PqError::Bad(_)));
    }

    #[test]
    fn plain_decode_int96_rejects_huge_julian_day_overflow() {
        // jd = u32::MAX → (u32::MAX - 2_440_588) as i64 = 4_292_526_707
        // → * NS_PER_DAY = 4_292_526_707 * 86_400_000_000_000.
        // 4_292_526_707 * 86_400_000_000_000 ≈ 3.7e23 > i64::MAX (9.2e18)
        // → checked_mul overflow → Bad (no panic).
        let mut b = Vec::new();
        b.extend_from_slice(&0u64.to_le_bytes());
        b.extend_from_slice(&u32::MAX.to_le_bytes());
        let err = decode_plain(&b, PlainSpec::plain(Type::Int96), 1).unwrap_err();
        assert!(matches!(err, PqError::Bad(_)));
    }

    // FLBA non-DECIMAL: 16 raw bytes per value → PqValue::Bytes.
    #[test]
    fn plain_decode_flba_uuid_to_bytes() {
        let mut b = Vec::new();
        b.extend_from_slice(&[0xAA; 16]);
        b.extend_from_slice(&[0xBB; 16]);
        let spec = PlainSpec::flba(16);
        let got = decode_plain(&b, spec, 2).unwrap();
        assert_eq!(
            got,
            vec![PqValue::Bytes(vec![0xAA; 16]), PqValue::Bytes(vec![0xBB; 16])]
        );
    }

    #[test]
    fn plain_decode_flba_truncated_is_bad() {
        // Spec n=16, payload only 15 bytes → Bad, no panic.
        let err = decode_plain(&[0u8; 15], PlainSpec::flba(16), 1).unwrap_err();
        assert!(matches!(err, PqError::Bad(_)));
    }

    // FLBA(8) DECIMAL(15,3): big-endian signed 8-byte values, sign-extended.
    // Value 1: 12345 (positive)  → bytes 00 00 00 00 00 00 30 39
    // Value 2: -456  (negative)  → bytes FF FF FF FF FF FF FE 38
    // Hand-check the negative case:
    //   -456 as i64 in two's complement = 0xFFFFFFFFFFFFFE38 (BE bytes).
    //   Sign-extend to i128: 16 bytes all 0xFF in upper 8, lower 8 unchanged.
    //   i128::from_be_bytes([0xFF;16 with low 8 = ..FE38]) = -456.
    #[test]
    fn plain_decode_flba_decimal_sign_extends_to_i128() {
        let mut b = Vec::new();
        b.extend_from_slice(&12345i64.to_be_bytes());
        b.extend_from_slice(&(-456i64).to_be_bytes());
        let spec = PlainSpec::flba_decimal(8, 15, 3);
        let got = decode_plain(&b, spec, 2).unwrap();
        assert_eq!(
            got,
            vec![
                PqValue::Decimal { unscaled: 12345, scale: 3 },
                PqValue::Decimal { unscaled: -456, scale: 3 },
            ]
        );
    }

    // INT32 DECIMAL: 4-byte LE i32 → widen (sign-extending) to i128.
    #[test]
    fn plain_decode_int32_decimal_widens_to_i128() {
        let mut b = Vec::new();
        b.extend_from_slice(&12345i32.to_le_bytes());
        b.extend_from_slice(&(-456i32).to_le_bytes());
        let spec = PlainSpec::int_decimal(Type::Int32, 5, 2);
        let got = decode_plain(&b, spec, 2).unwrap();
        assert_eq!(
            got,
            vec![
                PqValue::Decimal { unscaled: 12345, scale: 2 },
                PqValue::Decimal { unscaled: -456, scale: 2 },
            ]
        );
    }

    // INT64 DECIMAL: 8-byte LE i64 → widen (sign-extending) to i128.
    #[test]
    fn plain_decode_int64_decimal_widens_to_i128() {
        let mut b = Vec::new();
        b.extend_from_slice(&12345i64.to_le_bytes());
        b.extend_from_slice(&(-456i64).to_le_bytes());
        let spec = PlainSpec::int_decimal(Type::Int64, 15, 3);
        let got = decode_plain(&b, spec, 2).unwrap();
        assert_eq!(
            got,
            vec![
                PqValue::Decimal { unscaled: 12345, scale: 3 },
                PqValue::Decimal { unscaled: -456, scale: 3 },
            ]
        );
    }

    // BYTE_ARRAY DECIMAL: [u32 LE len][BE-bytes] per value.
    // Hand-derived: 123 in 2 BE bytes = 0x00 0x7B (since 123 = 0x7B,
    // top bit clear → positive, sign-extend with 0x00 in upper bytes).
    #[test]
    fn plain_decode_byte_array_decimal() {
        let mut b = Vec::new();
        b.extend_from_slice(&2u32.to_le_bytes());
        b.extend_from_slice(&[0x00, 0x7B]); // 123
        let spec = PlainSpec::byte_array_decimal(5, 2);
        let got = decode_plain(&b, spec, 1).unwrap();
        assert_eq!(
            got,
            vec![PqValue::Decimal { unscaled: 123, scale: 2 }]
        );
    }

    // BYTE_ARRAY DECIMAL must reject zero-length or >16 widths.
    #[test]
    fn plain_decode_byte_array_decimal_rejects_zero_length() {
        let mut b = Vec::new();
        b.extend_from_slice(&0u32.to_le_bytes());
        let err = decode_plain(&b, PlainSpec::byte_array_decimal(5, 2), 1).unwrap_err();
        assert!(matches!(err, PqError::Bad(_)));
    }

    #[test]
    fn plain_decode_byte_array_decimal_rejects_oversized() {
        let mut b = Vec::new();
        b.extend_from_slice(&17u32.to_le_bytes());
        b.extend_from_slice(&[0u8; 17]);
        let err = decode_plain(&b, PlainSpec::byte_array_decimal(38, 2), 1).unwrap_err();
        assert!(matches!(err, PqError::Bad(_)));
    }

    // PlainSpec refactor regression: every existing PLAIN INT64 test still
    // passes byte-identically through the new spec form.
    #[test]
    fn plain_decode_int64_byte_identity_after_plainspec_refactor() {
        let mut b = Vec::new();
        b.extend_from_slice(&7i64.to_le_bytes());
        b.extend_from_slice(&(-2i64).to_le_bytes());
        assert_eq!(
            decode_plain(&b, PlainSpec::plain(Type::Int64), 2).unwrap(),
            vec![PqValue::I64(7), PqValue::I64(-2)]
        );
    }

    #[test]
    fn plain_decode_float_double_decimal_rejected() {
        // DECIMAL on Float/Double is malformed; reject as Bad.
        let spec = PlainSpec {
            ptype: Type::Float,
            flba_len: None,
            decimal: Some(DecimalSpec { precision: 5, scale: 2 }),
        };
        let err = decode_plain(&[0u8; 4], spec, 1).unwrap_err();
        assert!(matches!(err, PqError::Bad(_)));
    }
}
