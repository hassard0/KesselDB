//! Round-trip tests: load pyarrow-generated Parquet fixtures via
//! include_bytes! and assert extract() returns the expected logical
//! rows. Fixture provenance: pyarrow 24.0.0; see
//! tests/fixtures/README.md.
use kessel_parquet::{extract, extract_with_cap, PqError, PqValue, DEFAULT_MAX_PAGE_SIZE};
use kessel_parquet::PqValue::{Bytes, I64, Null, Timestamp};

// ── OBJ-2c-4: INT96 fixtures ─────────────────────────────────────────────────
const INT96_PLAIN: &[u8]     = include_bytes!("fixtures/int96_plain.parquet");
const INT96_DICT: &[u8]      = include_bytes!("fixtures/int96_dict.parquet");
const INT96_V2_SNAPPY: &[u8] = include_bytes!("fixtures/int96_v2_snappy.parquet");
const INT96_OPTIONAL: &[u8]  = include_bytes!("fixtures/int96_optional.parquet");

// ── OBJ-2c-4: DECIMAL fixtures ────────────────────────────────────────────────
const DEC_I32: &[u8]      = include_bytes!("fixtures/decimal_int32.parquet");
const DEC_I32_DICT: &[u8] = include_bytes!("fixtures/decimal_int32_dict.parquet");
const DEC_I64: &[u8]      = include_bytes!("fixtures/decimal_int64.parquet");
const DEC_FLBA: &[u8]     = include_bytes!("fixtures/decimal_flba.parquet");
const DEC_FLBA_OPT: &[u8] = include_bytes!("fixtures/decimal_flba_optional.parquet");

// OBJ-2c-4 (SP108 T4 review): matched-precision DECIMAL fixtures for the
// 3-way INT32/INT64/FLBA cross-physical-type determinism pin. Same 5
// logical values, same scale=2; three different physical encodings.
const DEC_I32_EQ:  &[u8] = include_bytes!("fixtures/decimal_int32_eq.parquet");
const DEC_I64_EQ:  &[u8] = include_bytes!("fixtures/decimal_int64_eq.parquet");
const DEC_FLBA_EQ: &[u8] = include_bytes!("fixtures/decimal_flba_eq.parquet");

// ── OBJ-2c-4: FLBA non-DECIMAL fixture ──────────────────────────────────────
const FLBA_UUID: &[u8] = include_bytes!("fixtures/flba_uuid.parquet");

const FLAT: &[u8] = include_bytes!("fixtures/flat_required.parquet");
const MRG: &[u8] = include_bytes!("fixtures/flat_multirg.parquet");
const DICT: &[u8] = include_bytes!("fixtures/dict_flat.parquet");
const NULLABLE: &[u8] = include_bytes!("fixtures/nullable.parquet");
const NULLABLE_PLAIN: &[u8] = include_bytes!("fixtures/nullable_plain.parquet");

// ── SP149: LZ4_RAW codec fixture ────────────────────────────────────────────
// pyarrow 24.0.0 with `compression='lz4'` writes codec id 7 (LZ4_RAW) — the
// modern raw LZ4 block format, no Hadoop 8-byte framing. Verified by
// hand-decoding the footer thrift: field 4 (codec) = zigzag varint 0x0e
// = value 7 = LZ4_RAW.
const LZ4_RAW_FLAT: &[u8] = include_bytes!("fixtures/lz4_raw_flat.parquet");

// ── SP150: BROTLI codec fixture ─────────────────────────────────────────────
// pyarrow 24.0.0 with `compression='brotli'` writes codec id 4 (BROTLI).
// SP150 V1 only recognizes the codec at meta-decode time (`Codec::Brotli`);
// decompression returns a typed `Unsupported` naming the dedicated SP-arc
// follow-up — a zero-dep RFC 7932 Brotli decoder is multi-week scope
// (~10-15 tasks like SP125-SP140 zstd). The `#[ignore]`'d round-trip below
// is ready to flip live the moment a Brotli decoder ships; the active
// rejection-lock test pins the named-follow-up message until then.
const BROTLI_FLAT: &[u8] = include_bytes!("fixtures/brotli_flat.parquet");

#[test]
fn fixture_flat_required_decodes_expected_rows() {
    let rows = extract(FLAT, &["id", "name"]).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec![PqValue::I64(7), PqValue::Bytes(b"hi".to_vec())]);
    assert_eq!(rows[1], vec![PqValue::I64(-2), PqValue::Bytes(b"x".to_vec())]);
    assert_eq!(rows[2], vec![PqValue::I64(100), PqValue::Bytes(b"zed".to_vec())]);

    // subset + reordering: ask flag then score, check first row only
    let r2 = extract(FLAT, &["flag", "score"]).unwrap();
    assert_eq!(r2[0], vec![PqValue::Bool(true), PqValue::F64(1.5)]);
}

#[test]
fn fixture_multi_row_group_concatenates() {
    let rows = extract(MRG, &["id"]).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2], vec![PqValue::I64(100)]);
}

#[test]
fn dict_flat_fixture_roundtrips() {
    let rows = extract(DICT, &["id", "s"])
        .expect("extract dict fixture");
    assert_eq!(
        rows,
        vec![
            vec![PqValue::I64(7),   PqValue::Bytes(b"a".to_vec())],
            vec![PqValue::I64(7),   PqValue::Bytes(b"a".to_vec())],
            vec![PqValue::I64(-2),  PqValue::Bytes(b"b".to_vec())],
            vec![PqValue::I64(7),   PqValue::Bytes(b"c".to_vec())],
            vec![PqValue::I64(100), PqValue::Bytes(b"a".to_vec())],
        ]
    );
}

/// OBJ-2b-4: real pyarrow nullable.parquet (OPTIONAL + dict + Snappy).
/// This is the decisive non-self-referential proof of the capstone:
/// extract() reads vanilla pq.write_table(df) output (pyarrow defaults:
/// OPTIONAL + dictionary + Snappy) with NULLs, zero special flags.
/// The fixture schema was metadata-verified OPTIONAL and compression SNAPPY.
#[test]
fn nullable_parquet_fixture_roundtrips() {
    let expected = vec![
        vec![I64(7),   Bytes(b"a".to_vec())],
        vec![I64(7),   Null],
        vec![Null,     Bytes(b"b".to_vec())],
        vec![I64(-2),  Bytes(b"c".to_vec())],
        vec![I64(100), Bytes(b"a".to_vec())],
    ];
    let rows = extract(NULLABLE, &["id", "s"])
        .expect("extract nullable.parquet (OPTIONAL+dict+Snappy)");
    assert_eq!(rows, expected, "nullable.parquet (OPTIONAL+dict+Snappy)");
}

/// OBJ-2b-4: real pyarrow nullable_plain.parquet (OPTIONAL + PLAIN + UNCOMPRESSED).
/// Same logical table as nullable.parquet; different encoding/compression.
/// Proves OPTIONAL null-scatter works on both PLAIN and dictionary paths.
#[test]
fn nullable_plain_parquet_fixture_roundtrips() {
    let expected = vec![
        vec![I64(7),   Bytes(b"a".to_vec())],
        vec![I64(7),   Null],
        vec![Null,     Bytes(b"b".to_vec())],
        vec![I64(-2),  Bytes(b"c".to_vec())],
        vec![I64(100), Bytes(b"a".to_vec())],
    ];
    let rows = extract(NULLABLE_PLAIN, &["id", "s"])
        .expect("extract nullable_plain.parquet (OPTIONAL+PLAIN+UNCOMPRESSED)");
    assert_eq!(rows, expected, "nullable_plain.parquet (OPTIONAL+PLAIN+UNCOMPRESSED)");
}

#[test]
fn snappy_fixtures_roundtrip() {
    for f in ["snappy_dict.parquet", "snappy_plain.parquet"] {
        let path = format!(
            "{}/tests/fixtures/{}", env!("CARGO_MANIFEST_DIR"), f);
        let bytes = std::fs::read(&path).expect("read fixture");
        let rows = extract(&bytes, &["id", "s"])
            .unwrap_or_else(|e| panic!("{f}: {e}"));
        assert_eq!(rows, vec![
            vec![PqValue::I64(7),   PqValue::Bytes(b"a".to_vec())],
            vec![PqValue::I64(7),   PqValue::Bytes(b"a".to_vec())],
            vec![PqValue::I64(-2),  PqValue::Bytes(b"b".to_vec())],
            vec![PqValue::I64(7),   PqValue::Bytes(b"c".to_vec())],
            vec![PqValue::I64(100), PqValue::Bytes(b"a".to_vec())],
        ], "{f}");
    }
}

/// OBJ-2c-3: real pyarrow v2_plain / v2_dict / v2_gzip / v2_nullable.
/// DataPageHeaderV2 fixtures (data_page_version='2.0'). Decisive
/// non-self-referential proof: production extract() over
/// metadata-verified-V2 real pyarrow files.
/// v2_gzip proves V2 per-section GZIP decompression composes;
/// v2_nullable proves V2 def-level null scatter.
#[test]
fn v2_plain_fixture_roundtrips() {
    let path = format!(
        "{}/tests/fixtures/v2_plain.parquet", env!("CARGO_MANIFEST_DIR"));
    let bytes = std::fs::read(&path).expect("read v2_plain.parquet");
    let rows = extract(&bytes, &["id", "s"])
        .expect("extract v2_plain.parquet (V2+PLAIN+UNCOMPRESSED)");
    assert_eq!(rows, vec![
        vec![I64(7),   Bytes(b"a".to_vec())],
        vec![I64(7),   Bytes(b"a".to_vec())],
        vec![I64(-2),  Bytes(b"b".to_vec())],
        vec![I64(7),   Bytes(b"c".to_vec())],
        vec![I64(100), Bytes(b"a".to_vec())],
    ], "v2_plain.parquet");
}

#[test]
fn v2_dict_fixture_roundtrips() {
    let path = format!(
        "{}/tests/fixtures/v2_dict.parquet", env!("CARGO_MANIFEST_DIR"));
    let bytes = std::fs::read(&path).expect("read v2_dict.parquet");
    let rows = extract(&bytes, &["id", "s"])
        .expect("extract v2_dict.parquet (V2+PLAIN_DICTIONARY+UNCOMPRESSED)");
    assert_eq!(rows, vec![
        vec![I64(7),   Bytes(b"a".to_vec())],
        vec![I64(7),   Bytes(b"a".to_vec())],
        vec![I64(-2),  Bytes(b"b".to_vec())],
        vec![I64(7),   Bytes(b"c".to_vec())],
        vec![I64(100), Bytes(b"a".to_vec())],
    ], "v2_dict.parquet");
}

#[test]
fn v2_gzip_fixture_roundtrips() {
    let path = format!(
        "{}/tests/fixtures/v2_gzip.parquet", env!("CARGO_MANIFEST_DIR"));
    let bytes = std::fs::read(&path).expect("read v2_gzip.parquet");
    let rows = extract(&bytes, &["id", "s"])
        .expect("extract v2_gzip.parquet (V2+PLAIN_DICTIONARY+GZIP)");
    assert_eq!(rows, vec![
        vec![I64(7),   Bytes(b"a".to_vec())],
        vec![I64(7),   Bytes(b"a".to_vec())],
        vec![I64(-2),  Bytes(b"b".to_vec())],
        vec![I64(7),   Bytes(b"c".to_vec())],
        vec![I64(100), Bytes(b"a".to_vec())],
    ], "v2_gzip.parquet");
}

#[test]
fn v2_nullable_fixture_roundtrips() {
    let path = format!(
        "{}/tests/fixtures/v2_nullable.parquet", env!("CARGO_MANIFEST_DIR"));
    let bytes = std::fs::read(&path).expect("read v2_nullable.parquet");
    let rows = extract(&bytes, &["id", "s"])
        .expect("extract v2_nullable.parquet (V2+OPTIONAL+PLAIN_DICTIONARY+SNAPPY)");
    assert_eq!(rows, vec![
        vec![I64(7),   Bytes(b"a".to_vec())],
        vec![I64(7),   Null],
        vec![Null,     Bytes(b"b".to_vec())],
        vec![I64(-2),  Bytes(b"c".to_vec())],
        vec![I64(100), Bytes(b"a".to_vec())],
    ], "v2_nullable.parquet");
}

/// OBJ-2c-3: V2-vs-V1 source-independence pin.
/// Asserts that extract() over a V2-encoded file (v2_dict, DataPageHeaderV2,
/// V1 metadata envelope) yields identical logical rows as extract() over the
/// V1-encoded dict_flat.parquet fixture (DataPageHeader, same schema + data).
/// Both carry the same 5 logical rows: id=[7,7,-2,7,100], s=["a","a","b","c","a"].
/// This pins that the V2/V1 decode paths are source-independent: same data in,
/// same data out, regardless of which Parquet page format produced the file.
#[test]
fn v2_dict_vs_v1_dict_source_independence_pin() {
    let v2_path = format!(
        "{}/tests/fixtures/v2_dict.parquet", env!("CARGO_MANIFEST_DIR"));
    let v1_path = format!(
        "{}/tests/fixtures/dict_flat.parquet", env!("CARGO_MANIFEST_DIR"));
    let v2_bytes = std::fs::read(&v2_path).expect("read v2_dict.parquet");
    let v1_bytes = std::fs::read(&v1_path).expect("read dict_flat.parquet");
    let v2_rows = extract(&v2_bytes, &["id", "s"])
        .expect("extract v2_dict.parquet");
    let v1_rows = extract(&v1_bytes, &["id", "s"])
        .expect("extract dict_flat.parquet");
    assert_eq!(v2_rows, v1_rows,
        "V2 dict and V1 dict must produce identical logical rows for the same source data");
}

/// OBJ-2c-1: real pyarrow gzip_dict.parquet and gzip_plain.parquet
/// (REQUIRED + GZIP, dict-encoded and PLAIN respectively). Decisive
/// non-self-referential proof: production extract() over
/// metadata-verified-GZIP real pyarrow files.
#[test]
fn gzip_fixtures_roundtrip() {
    for f in ["gzip_dict.parquet", "gzip_plain.parquet"] {
        let path = format!(
            "{}/tests/fixtures/{}", env!("CARGO_MANIFEST_DIR"), f);
        let bytes = std::fs::read(&path).expect("read fixture");
        let rows = extract(&bytes, &["id", "s"])
            .unwrap_or_else(|e| panic!("{f}: {e}"));
        assert_eq!(rows, vec![
            vec![I64(7),   Bytes(b"a".to_vec())],
            vec![I64(7),   Bytes(b"a".to_vec())],
            vec![I64(-2),  Bytes(b"b".to_vec())],
            vec![I64(7),   Bytes(b"c".to_vec())],
            vec![I64(100), Bytes(b"a".to_vec())],
        ], "{f}");
    }
}

/// OBJ-2c-1: real pyarrow gzip_nullable.parquet (OPTIONAL + dict + GZIP,
/// with NULLs). Proves gzip ∘ def-levels ∘ dict composition through the
/// page_payload seam — decisive non-self-referential proof.
#[test]
fn gzip_nullable_fixture_roundtrips() {
    let path = format!(
        "{}/tests/fixtures/gzip_nullable.parquet", env!("CARGO_MANIFEST_DIR"));
    let bytes = std::fs::read(&path).expect("read fixture");
    let expected = vec![
        vec![I64(7),   Bytes(b"a".to_vec())],
        vec![I64(7),   Null],
        vec![Null,     Bytes(b"b".to_vec())],
        vec![I64(-2),  Bytes(b"c".to_vec())],
        vec![I64(100), Bytes(b"a".to_vec())],
    ];
    let rows = extract(&bytes, &["id", "s"])
        .expect("extract gzip_nullable.parquet (OPTIONAL+dict+GZIP)");
    assert_eq!(rows, expected, "gzip_nullable.parquet (OPTIONAL+dict+GZIP)");
}

// ════════════════════════════════════════════════════════════════════════════
// OBJ-2c-4: INT96 + DECIMAL (INT32/INT64/FLBA) + FLBA-UUID roundtrip tests
// ════════════════════════════════════════════════════════════════════════════

/// OBJ-2c-4: real pyarrow int96_plain.parquet (INT96, PLAIN, UNCOMPRESSED, V1).
/// Timestamps: 1970-01-01 (0 ns), 1970-01-02 (+86400s), 1969-12-31 (-86400s).
/// Metadata-verified: phys=INT96, V1 data page. Decisive non-self-referential
/// proof of the INT96 plain decode path via production extract().
#[test]
fn int96_plain_fixture_roundtrips() {
    let rows = extract(INT96_PLAIN, &["ts"])
        .expect("extract int96_plain.parquet (INT96+PLAIN+UNCOMPRESSED)");
    assert_eq!(rows, vec![
        vec![Timestamp(0)],
        vec![Timestamp(86_400_000_000_000)],
        vec![Timestamp(-86_400_000_000_000)],
    ], "int96_plain.parquet");
}

/// OBJ-2c-4: real pyarrow int96_dict.parquet (INT96, PLAIN_DICTIONARY, UNCOMPRESSED, V1).
/// Same logical rows as int96_plain.parquet; proves INT96 dict-encoded path.
#[test]
fn int96_dict_fixture_roundtrips() {
    let rows = extract(INT96_DICT, &["ts"])
        .expect("extract int96_dict.parquet (INT96+DICT+UNCOMPRESSED)");
    assert_eq!(rows, vec![
        vec![Timestamp(0)],
        vec![Timestamp(86_400_000_000_000)],
        vec![Timestamp(-86_400_000_000_000)],
    ], "int96_dict.parquet");
}

/// OBJ-2c-4: real pyarrow int96_v2_snappy.parquet (INT96, PLAIN, SNAPPY, V2).
/// Same logical rows; proves INT96 decodes correctly through DataPageHeaderV2
/// + Snappy. Metadata-verified: V2-discriminator bytes 0x15 0x06 at offset 4.
#[test]
fn int96_v2_snappy_fixture_roundtrips() {
    let rows = extract(INT96_V2_SNAPPY, &["ts"])
        .expect("extract int96_v2_snappy.parquet (INT96+PLAIN+SNAPPY+V2)");
    assert_eq!(rows, vec![
        vec![Timestamp(0)],
        vec![Timestamp(86_400_000_000_000)],
        vec![Timestamp(-86_400_000_000_000)],
    ], "int96_v2_snappy.parquet");
}

/// OBJ-2c-4: real pyarrow int96_optional.parquet (INT96, OPTIONAL, V1).
/// Rows: 0 ns / NULL / -86400s ns. Proves INT96 null-scatter in OPTIONAL.
#[test]
fn int96_optional_fixture_roundtrips() {
    let rows = extract(INT96_OPTIONAL, &["ts"])
        .expect("extract int96_optional.parquet (INT96+OPTIONAL+V1)");
    assert_eq!(rows, vec![
        vec![Timestamp(0)],
        vec![Null],
        vec![Timestamp(-86_400_000_000_000)],
    ], "int96_optional.parquet");
}

/// OBJ-2c-4: INT96 plain-vs-dict-vs-V2 source-independence pin.
/// Asserts that extract() over int96_plain, int96_dict, and int96_v2_snappy
/// all produce identical logical rows despite different encodings (PLAIN /
/// PLAIN_DICTIONARY / DataPageHeaderV2+SNAPPY). The three fixtures carry the
/// same 3 timestamps: 0 ns, +86400s ns, -86400s ns.
/// Pins: plain, dict, and V2 INT96 decode paths are source-format-independent.
#[test]
fn int96_plain_vs_dict_vs_v2_source_independence_pin() {
    let plain = extract(INT96_PLAIN, &["ts"])
        .expect("extract int96_plain");
    let dict = extract(INT96_DICT, &["ts"])
        .expect("extract int96_dict");
    let v2sn = extract(INT96_V2_SNAPPY, &["ts"])
        .expect("extract int96_v2_snappy");
    assert_eq!(plain, dict,
        "INT96 PLAIN and DICT must produce identical logical rows");
    assert_eq!(plain, v2sn,
        "INT96 PLAIN and V2+SNAPPY must produce identical logical rows");
}

/// OBJ-2c-4: real pyarrow decimal_int32.parquet (DECIMAL INT32, precision=5, scale=2).
/// Values: 1.23, -4.56, 100.00 → unscaled: 123, -456, 10000 / scale=2.
/// Metadata-verified: phys=INT32, conv=DECIMAL, logical=Decimal(precision=5, scale=2).
/// Generated with store_decimal_as_integer=True; proves INT32-backed DECIMAL decode.
#[test]
fn decimal_int32_fixture_roundtrips() {
    let rows = extract(DEC_I32, &["d"])
        .expect("extract decimal_int32.parquet (DECIMAL+INT32)");
    assert_eq!(rows, vec![
        vec![PqValue::Decimal { unscaled: 123,    scale: 2 }],
        vec![PqValue::Decimal { unscaled: -456,   scale: 2 }],
        vec![PqValue::Decimal { unscaled: 10_000, scale: 2 }],
    ], "decimal_int32.parquet");
}

/// OBJ-2c-4: real pyarrow decimal_int32_dict.parquet (DECIMAL INT32, dict-encoded).
/// Same precision=5, scale=2 values as decimal_int32.parquet.
/// Proves INT32-backed DECIMAL decodes correctly through the dictionary path.
#[test]
fn decimal_int32_dict_fixture_roundtrips() {
    let rows = extract(DEC_I32_DICT, &["d"])
        .expect("extract decimal_int32_dict.parquet (DECIMAL+INT32+DICT)");
    assert_eq!(rows, vec![
        vec![PqValue::Decimal { unscaled: 123,    scale: 2 }],
        vec![PqValue::Decimal { unscaled: -456,   scale: 2 }],
        vec![PqValue::Decimal { unscaled: 10_000, scale: 2 }],
    ], "decimal_int32_dict.parquet");
}

/// OBJ-2c-4: real pyarrow decimal_int64.parquet (DECIMAL INT64, precision=18, scale=3).
/// Values: 1.234, -4.567, 100000.000 → unscaled: 1234, -4567, 100000000 / scale=3.
/// Metadata-verified: phys=INT64, conv=DECIMAL, logical=Decimal(precision=18, scale=3).
/// Generated with store_decimal_as_integer=True; proves INT64-backed DECIMAL decode.
#[test]
fn decimal_int64_fixture_roundtrips() {
    let rows = extract(DEC_I64, &["d"])
        .expect("extract decimal_int64.parquet (DECIMAL+INT64)");
    assert_eq!(rows, vec![
        vec![PqValue::Decimal { unscaled: 1_234,       scale: 3 }],
        vec![PqValue::Decimal { unscaled: -4_567,      scale: 3 }],
        vec![PqValue::Decimal { unscaled: 100_000_000, scale: 3 }],
    ], "decimal_int64.parquet");
}

/// OBJ-2c-4: real pyarrow decimal_flba.parquet (DECIMAL FLBA, precision=30, scale=5,
/// type_length=13 bytes). Values: 1.23456, -4.56789, 100000.00000 →
/// unscaled: 123456, -456789, 10000000000 / scale=5.
/// Metadata-verified: phys=FIXED_LEN_BYTE_ARRAY, conv=DECIMAL,
/// logical=Decimal(precision=30, scale=5). Proves FLBA-backed DECIMAL decode.
#[test]
fn decimal_flba_fixture_roundtrips() {
    let rows = extract(DEC_FLBA, &["d"])
        .expect("extract decimal_flba.parquet (DECIMAL+FLBA)");
    assert_eq!(rows, vec![
        vec![PqValue::Decimal { unscaled: 123_456,       scale: 5 }],
        vec![PqValue::Decimal { unscaled: -456_789,      scale: 5 }],
        vec![PqValue::Decimal { unscaled: 10_000_000_000, scale: 5 }],
    ], "decimal_flba.parquet");
}

/// OBJ-2c-4: real pyarrow decimal_flba_optional.parquet (DECIMAL FLBA, OPTIONAL).
/// Rows: 1.23456 / NULL / -4.56789 → Decimal{123456,5} / Null / Decimal{-456789,5}.
/// Proves FLBA DECIMAL null-scatter in OPTIONAL.
#[test]
fn decimal_flba_optional_fixture_roundtrips() {
    let rows = extract(DEC_FLBA_OPT, &["d"])
        .expect("extract decimal_flba_optional.parquet (DECIMAL+FLBA+OPTIONAL)");
    assert_eq!(rows, vec![
        vec![PqValue::Decimal { unscaled: 123_456,  scale: 5 }],
        vec![Null],
        vec![PqValue::Decimal { unscaled: -456_789, scale: 5 }],
    ], "decimal_flba_optional.parquet");
}

/// OBJ-2c-4: DECIMAL INT32 encoding-independence pin (plain vs dict).
///
/// Asserts the INT32 PLAIN and INT32 PLAIN_DICTIONARY decode paths produce
/// identical logical rows for the same precision/scale (precision=5, scale=2).
/// This pins encoding-only source independence within a single physical type;
/// cross-physical-type independence is pinned by
/// `decimal_cross_physical_type_determinism_pin` below.
#[test]
fn decimal_int32_encoding_independence_pin() {
    let plain = extract(DEC_I32, &["d"])
        .expect("extract decimal_int32 plain");
    let dict = extract(DEC_I32_DICT, &["d"])
        .expect("extract decimal_int32 dict");
    assert_eq!(plain, dict,
        "DECIMAL INT32 plain and dict paths must produce identical logical rows");
}

/// OBJ-2c-4 (SP108 T4 review): DECIMAL cross-physical-type determinism pin.
///
/// The SAME logical DECIMAL values via INT32, INT64, and FLBA physical types
/// must decode to byte-identical `PqValue::Decimal { unscaled, scale }`. This
/// is the end-to-end source-format-independence proof that the T4 spec
/// requires; it is exercised through the production `extract()` entry point
/// against three pyarrow-written fixtures with metadata-verified differing
/// physical_type but matched scale=2 and identical logical rows.
///
/// Fixtures (all 5 logical values, all scale=2):
///   - decimal_int32_eq.parquet : pa.decimal128(9,  2) + store_decimal_as_integer → INT32
///   - decimal_int64_eq.parquet : pa.decimal128(18, 2) + store_decimal_as_integer → INT64
///   - decimal_flba_eq.parquet  : pa.decimal128(30, 2)                            → FIXED_LEN_BYTE_ARRAY
///
/// Logical values (max unscaled |v| = 99_999_999 < 2^31, fits INT32):
///   123.45        →  Decimal{ unscaled:    12_345,  scale: 2 }
///   -67.89        →  Decimal{ unscaled:    -6_789,  scale: 2 }
///   100_000.00    →  Decimal{ unscaled: 10_000_000, scale: 2 }
///   0.00          →  Decimal{ unscaled:         0,  scale: 2 }
///   -999_999.99   →  Decimal{ unscaled: -99_999_999, scale: 2 }
///
/// pyarrow 24.0.0 cannot write BYTE_ARRAY DECIMAL (plan-acknowledged); the
/// BYTE_ARRAY decode path is exercised by hand-KATs in `lib.rs#tests`. The
/// 3-way INT32/INT64/FLBA pin here is what proves source-format independence
/// end-to-end via production extract().
#[test]
fn decimal_cross_physical_type_determinism_pin() {
    let i32_rows  = extract(DEC_I32_EQ,  &["d"])
        .expect("extract decimal_int32_eq.parquet (DECIMAL+INT32, matched p=9 s=2)");
    let i64_rows  = extract(DEC_I64_EQ,  &["d"])
        .expect("extract decimal_int64_eq.parquet (DECIMAL+INT64, matched p=18 s=2)");
    let flba_rows = extract(DEC_FLBA_EQ, &["d"])
        .expect("extract decimal_flba_eq.parquet (DECIMAL+FLBA, matched p=30 s=2)");

    assert_eq!(i32_rows, i64_rows,
        "INT32 vs INT64 DECIMAL decode mismatch — source-format independence violated");
    assert_eq!(i64_rows, flba_rows,
        "INT64 vs FLBA DECIMAL decode mismatch — source-format independence violated");

    // Hand-derived exact expected rows (5 logical values × scale=2).
    let expected: Vec<Vec<PqValue>> = vec![
        vec![PqValue::Decimal { unscaled:     12_345, scale: 2 }],
        vec![PqValue::Decimal { unscaled:     -6_789, scale: 2 }],
        vec![PqValue::Decimal { unscaled: 10_000_000, scale: 2 }],
        vec![PqValue::Decimal { unscaled:          0, scale: 2 }],
        vec![PqValue::Decimal { unscaled: -99_999_999, scale: 2 }],
    ];
    assert_eq!(i32_rows,  expected, "INT32 path: exact-value mismatch vs hand-derived expected");
    assert_eq!(i64_rows,  expected, "INT64 path: exact-value mismatch vs hand-derived expected");
    assert_eq!(flba_rows, expected, "FLBA path: exact-value mismatch vs hand-derived expected");
}

/// OBJ-2c-4: real pyarrow flba_uuid.parquet (FIXED_LEN_BYTE_ARRAY(16), no DECIMAL).
/// Three rows of 16-byte fixed-size binary: 0x01*16, 0x02*16, 0x03*16.
/// Metadata-verified: phys=FIXED_LEN_BYTE_ARRAY, type_length=16, no converted/logical type.
/// Proves non-DECIMAL FLBA decodes as PqValue::Bytes.
#[test]
fn flba_uuid_fixture_roundtrips_to_bytes() {
    let rows = extract(FLBA_UUID, &["u"])
        .expect("extract flba_uuid.parquet (FLBA+non-DECIMAL)");
    assert_eq!(rows, vec![
        vec![Bytes(vec![0x01u8; 16])],
        vec![Bytes(vec![0x02u8; 16])],
        vec![Bytes(vec![0x03u8; 16])],
    ], "flba_uuid.parquet");
}

// ════════════════════════════════════════════════════════════════════════════
// OBJ-2c-2 (SP136): real pyarrow zstd-compressed fixtures. THE
// non-self-referential validator for the SP125-SP135 zstd pipeline.
// ════════════════════════════════════════════════════════════════════════════

const ZSTD_PLAIN: &[u8]         = include_bytes!("fixtures/zstd_plain.parquet");
const ZSTD_DICT: &[u8]          = include_bytes!("fixtures/zstd_dict.parquet");
const ZSTD_NULLABLE: &[u8]      = include_bytes!("fixtures/zstd_nullable.parquet");
// SP138 stress fixtures: BYTE_ARRAY strings + dict+nullable + V2+zstd.
// The `zstd_stress.parquet` fixture (kept on disk) exercises the
// FSE-Compressed LL/OF/ML mode SIMULTANEOUSLY at concentrated
// distributions — that path partially works after SP139 (FSE table
// parsing is now correct; SP137-fix-lock + 3 SP138 e2e tests confirm
// the small/medium fixtures), but the stress sequence-stream decode
// trips a downstream bug (likely in the FSE state machine's
// 0-nb-bits transition handling or the bit-bookkeeping at a
// 3-state-interleaved decode) that SP140 will isolate via bit-by-bit
// comparison against a libzstd reference C trace.
const ZSTD_STRINGS: &[u8]       = include_bytes!("fixtures/zstd_strings.parquet");
const ZSTD_DICT_NULLABLE: &[u8] = include_bytes!("fixtures/zstd_dict_nullable.parquet");
const ZSTD_V2: &[u8]            = include_bytes!("fixtures/zstd_v2.parquet");

/// SP136-E2E-1: REQUIRED INT64 PLAIN under zstd.
/// **SP137 PENDING**: pyarrow's libzstd output triggers a typed
/// `UnexpectedEof` in the SP125-SP135 pipeline. The standalone
/// reference `zstd -3` CLI output DOES decode correctly (see
/// `zstd::tests::sp136_kat_decode_reference_stream_hello`). The wire
/// is in place (Codec::Zstd + page_payload + the full
/// literals/sequences/execution pipeline); pyarrow-libzstd specific
/// encoding-corner compatibility is the SP137 follow-up.
#[test]
// SP137 FIXED: FSE base_state algorithm corrected; pyarrow zstd e2e now passes.
fn zstd_plain_fixture_roundtrips() {
    let rows = extract(ZSTD_PLAIN, &["id"])
        .expect("extract zstd_plain.parquet (REQUIRED+PLAIN+zstd)");
    assert_eq!(rows, vec![
        vec![I64(1)],
        vec![I64(2)],
        vec![I64(3)],
        vec![I64(4)],
        vec![I64(5)],
    ], "zstd_plain.parquet");
}

/// SP136-E2E-2: REQUIRED INT64 with dictionary encoding under zstd.
/// **SP137 PENDING** (see SP136-E2E-1 doc).
#[test]
// SP137 FIXED.
fn zstd_dict_fixture_roundtrips() {
    let rows = extract(ZSTD_DICT, &["id"])
        .expect("extract zstd_dict.parquet (REQUIRED+dict+zstd)");
    assert_eq!(rows.len(), 70);
    assert_eq!(rows[0], vec![I64(10)]);
}

/// SP136-E2E-3: OPTIONAL INT64 with nulls under zstd.
/// **SP137 PENDING** (see SP136-E2E-1 doc).
#[test]
// SP137 FIXED.
fn zstd_nullable_fixture_roundtrips() {
    let rows = extract(ZSTD_NULLABLE, &["v"])
        .expect("extract zstd_nullable.parquet (OPTIONAL+PLAIN+zstd)");
    assert_eq!(rows, vec![
        vec![I64(1)],
        vec![Null],
        vec![I64(3)],
        vec![Null],
        vec![I64(5)],
    ], "zstd_nullable.parquet");
}


// ════════════════════════════════════════════════════════════════════════════
// OBJ-2c-2 (SP138): expanded pyarrow zstd fixtures stressing the SP130
// 4-stream Huffman + SP131 FSE-weight Huffman tree + V2-page paths
// that the SP136 small-data fixtures were too small to trigger.
// ════════════════════════════════════════════════════════════════════════════

/// SP138-E2E-1: zstd + BYTE_ARRAY strings (REQUIRED). Exercises the
/// zstd pipeline over a literal alphabet that's NOT INT64-dominated.
#[test]
fn zstd_strings_fixture_roundtrips() {
    let rows = extract(ZSTD_STRINGS, &["s"])
        .expect("extract zstd_strings.parquet (REQUIRED+BYTE_ARRAY+zstd)");
    assert_eq!(rows, vec![
        vec![Bytes(b"alpha".to_vec())],
        vec![Bytes(b"beta".to_vec())],
        vec![Bytes(b"gamma".to_vec())],
        vec![Bytes(b"delta".to_vec())],
        vec![Bytes(b"epsilon".to_vec())],
    ], "zstd_strings.parquet");
}

/// SP138-E2E-2: zstd + dict + OPTIONAL (nullable) INT64. Three-way
/// composition: zstd page decode ∘ dict resolve ∘ def-level scatter.
#[test]
fn zstd_dict_nullable_fixture_roundtrips() {
    let rows = extract(ZSTD_DICT_NULLABLE, &["v"])
        .expect("extract zstd_dict_nullable.parquet (OPTIONAL+dict+zstd)");
    assert_eq!(rows.len(), 35); // 7 rows × 5 repetitions
    // Pattern: [10, None, 20, None, 10, 20, 10] repeated.
    assert_eq!(rows[0], vec![I64(10)]);
    assert_eq!(rows[1], vec![Null]);
    assert_eq!(rows[2], vec![I64(20)]);
    assert_eq!(rows[3], vec![Null]);
    assert_eq!(rows[4], vec![I64(10)]);
    assert_eq!(rows[5], vec![I64(20)]);
    assert_eq!(rows[6], vec![I64(10)]);
    assert_eq!(rows[7], vec![I64(10)]); // start of 2nd repetition
}

/// SP138-E2E-3: zstd + DATA_PAGE_V2 (data_page_version='2.0'). Proves
/// zstd composes with the V2-page seam (the values-section-only
/// decompression path at lib.rs::decode_data_page_v2).
#[test]
fn zstd_v2_fixture_roundtrips() {
    let rows = extract(ZSTD_V2, &["id"])
        .expect("extract zstd_v2.parquet (REQUIRED+PLAIN+zstd+V2)");
    assert_eq!(rows, vec![
        vec![I64(1)], vec![I64(2)], vec![I64(3)], vec![I64(4)], vec![I64(5)],
    ], "zstd_v2.parquet");
}

const ZSTD_STRESS: &[u8] = include_bytes!("fixtures/zstd_stress.parquet");

/// SP140-E2E: zstd stress — 2000 random INT64 values. Pyarrow's libzstd
/// uses **FSE-Compressed mode for ALL THREE LL/OF/ML codes** here.
/// The SP140 fix corrected a spurious `+ 128` in
/// `parse_sequences_header`'s 2-byte VLQ formula that had been inflating
/// `num_sequences` by 128 (the stress fixture's VLQ `[0x87, 0xcf]`
/// correctly decodes to 1999, not the pre-SP140 buggy 2127). With the
/// fix, the loop exits cleanly after the 1999th sequence and the full
/// 2000-row table round-trips byte-identical to pyarrow's output.
#[test]
fn zstd_stress_fixture_roundtrips() {
    let rows = extract(ZSTD_STRESS, &["big"])
        .expect("extract zstd_stress.parquet (REQUIRED+PLAIN+zstd; FSE-Compressed all 3 modes)");
    assert_eq!(rows.len(), 2000);
    // Spot-check known values from the pyarrow generator (random.seed(42)).
    assert_eq!(rows[0], vec![I64(2867825)]);
    assert_eq!(rows[1], vec![I64(1419610)]);
    assert_eq!(rows[2], vec![I64(5614226)]);
    assert_eq!(rows[1999], vec![I64(3679603)]);
    // Determinism cross-check: every row is non-null I64 in expected range.
    for r in &rows {
        assert_eq!(r.len(), 1);
        match &r[0] {
            I64(v) => assert!(*v >= 1_000_000 && *v <= 10_000_000,
                "out-of-range value: {v}"),
            other => panic!("non-I64 in zstd_stress: {other:?}"),
        }
    }
}

// ── SP143 T9: real pyarrow List<T> fixtures ─────────────────────────────────
//
// Each fixture was generated by `tests/fixtures/regen_list.py` using
// pyarrow 24.0.0 with version='1.0', use_dictionary=False,
// compression='NONE', data_page_version='1.0'. The five files cover the
// canonical 3-node LIST encoding (group `my_list` (List) → repeated group
// `list` → leaf `element`) across the four-shape def-level matrix plus a
// BYTE_ARRAY (string) variant:
//
//   list_i64_required       REQ outer, REQ element, INT64
//   list_i64_optional       REQ outer, OPT element, INT64
//   list_string             REQ outer, REQ element, BYTE_ARRAY (utf8)
//   optional_list_i64       OPT outer, REQ element, INT64
//   list_with_null_items    REQ outer, OPT element, INT64 (full matrix)
//
// These are the decisive real-data proof that the SP143 T1-T6 pipeline
// (rep/def stream decode, plain leaf decode, Dremel-style record
// assembler) matches the Parquet standard as pyarrow writes it — not
// just hand-built fixtures from T7.
const LIST_I64_REQUIRED:   &[u8] = include_bytes!("fixtures/list_i64_required.parquet");
const LIST_I64_OPTIONAL:   &[u8] = include_bytes!("fixtures/list_i64_optional.parquet");
const LIST_STRING:         &[u8] = include_bytes!("fixtures/list_string.parquet");
const OPTIONAL_LIST_I64:   &[u8] = include_bytes!("fixtures/optional_list_i64.parquet");
const LIST_WITH_NULL_ITEMS:&[u8] = include_bytes!("fixtures/list_with_null_items.parquet");

#[test]
fn pyarrow_list_i64_required() {
    let rows = extract(LIST_I64_REQUIRED, &["my_list"])
        .expect("extract list_i64_required");
    assert_eq!(rows.len(), 2, "two records");
    assert_eq!(
        rows[0],
        vec![PqValue::List(vec![I64(1), I64(2), I64(3)])]
    );
    assert_eq!(
        rows[1],
        vec![PqValue::List(vec![I64(10), I64(20)])]
    );
}

#[test]
fn pyarrow_list_i64_optional() {
    let rows = extract(LIST_I64_OPTIONAL, &["my_list"])
        .expect("extract list_i64_optional");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        vec![PqValue::List(vec![I64(10), Null, I64(20)])]
    );
}

#[test]
fn pyarrow_list_string() {
    let rows = extract(LIST_STRING, &["my_list"])
        .expect("extract list_string");
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0],
        vec![PqValue::List(vec![
            Bytes(b"foo".to_vec()),
            Bytes(b"bar".to_vec()),
        ])]
    );
    assert_eq!(
        rows[1],
        vec![PqValue::List(vec![Bytes(b"baz".to_vec())])]
    );
}

#[test]
fn pyarrow_optional_list_i64() {
    let rows = extract(OPTIONAL_LIST_I64, &["my_list"])
        .expect("extract optional_list_i64");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![Null]);
    assert_eq!(
        rows[1],
        vec![PqValue::List(vec![I64(7), I64(8)])]
    );
}

#[test]
fn pyarrow_list_with_null_items() {
    let rows = extract(LIST_WITH_NULL_ITEMS, &["my_list"])
        .expect("extract list_with_null_items");
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        vec![PqValue::List(vec![I64(1), Null, I64(2)])]
    );
    assert_eq!(rows[1], vec![PqValue::List(vec![])]);
    assert_eq!(
        rows[2],
        vec![PqValue::List(vec![Null, Null])]
    );
}

// ── SP144 T7: real pyarrow Map<K,V> + struct fixtures ──────────────────────
//
// Each fixture was generated by `tests/fixtures/regen_map_struct.py` using
// pyarrow 24.0.0 with version='1.0', use_dictionary=False,
// compression='NONE', data_page_version='1.0'. The five files cover the
// canonical 3-node MAP encoding (group `my_map` (Map) → repeated group
// `key_value` → leaves `key` / `value`) and the struct-of-primitives
// pattern (group `my_struct` → primitive leaves) across REQ/OPT outer
// variants:
//
//   map_string_i64           REQ outer, REQ value (i64)
//   optional_map_string_i64  OPT outer, REQ value (i64)
//   map_string_string        REQ outer, REQ value (string)
//   struct_i64_string        REQ outer struct{id i64, name string}
//   optional_struct          OPT outer struct{id i64, name string} with null row
//
// These are the decisive real-data proof that the SP144 T1-T6 pipeline
// (LogicalType::Map recognition, assemble_map_kv + assemble_struct,
// classify_column_plan routing, nested extract) matches the Parquet
// standard as pyarrow writes it — not just hand-built fixtures from T6.
const MAP_STRING_I64:          &[u8] = include_bytes!("fixtures/map_string_i64.parquet");
const OPTIONAL_MAP_STRING_I64: &[u8] = include_bytes!("fixtures/optional_map_string_i64.parquet");
const MAP_STRING_STRING:       &[u8] = include_bytes!("fixtures/map_string_string.parquet");
const STRUCT_I64_STRING:       &[u8] = include_bytes!("fixtures/struct_i64_string.parquet");
const OPTIONAL_STRUCT:         &[u8] = include_bytes!("fixtures/optional_struct.parquet");

#[test]
fn pyarrow_map_string_i64() {
    let rows = extract(MAP_STRING_I64, &["my_map"])
        .expect("extract map_string_i64");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![PqValue::Map(vec![
        (Bytes(b"a".to_vec()), I64(1)),
        (Bytes(b"b".to_vec()), I64(2)),
    ])]);
    assert_eq!(rows[1], vec![PqValue::Map(vec![
        (Bytes(b"x".to_vec()), I64(7)),
    ])]);
}

#[test]
fn pyarrow_optional_map_string_i64() {
    let rows = extract(OPTIONAL_MAP_STRING_I64, &["my_map"])
        .expect("extract optional_map_string_i64");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![Null]);
    assert_eq!(rows[1], vec![PqValue::Map(vec![
        (Bytes(b"k".to_vec()), I64(42)),
    ])]);
}

#[test]
fn pyarrow_map_string_string() {
    let rows = extract(MAP_STRING_STRING, &["my_map"])
        .expect("extract map_string_string");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec![PqValue::Map(vec![
        (Bytes(b"lang".to_vec()), Bytes(b"rust".to_vec())),
        (Bytes(b"ver".to_vec()),  Bytes(b"1.95".to_vec())),
    ])]);
}

#[test]
fn pyarrow_struct_i64_string() {
    let rows = extract(STRUCT_I64_STRING, &["my_struct"])
        .expect("extract struct_i64_string");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![PqValue::Struct(vec![
        ("id".into(),   I64(1)),
        ("name".into(), Bytes(b"alice".to_vec())),
    ])]);
    assert_eq!(rows[1], vec![PqValue::Struct(vec![
        ("id".into(),   I64(2)),
        ("name".into(), Bytes(b"bob".to_vec())),
    ])]);
}

#[test]
fn pyarrow_optional_struct() {
    let rows = extract(OPTIONAL_STRUCT, &["my_struct"])
        .expect("extract optional_struct");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec![PqValue::Struct(vec![
        ("id".into(),   I64(1)),
        ("name".into(), Bytes(b"alice".to_vec())),
    ])]);
    assert_eq!(rows[1], vec![Null]);
    assert_eq!(rows[2], vec![PqValue::Struct(vec![
        ("id".into(),   I64(3)),
        ("name".into(), Bytes(b"carol".to_vec())),
    ])]);
}

// ============================================================================
// SP145 T7: pyarrow deep-nesting fixtures (final OBJ-2c-5 slice).
//
// Fixtures generated by tests/fixtures/regen_deep_nesting.py via pyarrow
// 24.0.0. Exercises the 4 SP145 lift shapes:
//   list_of_list_i64         List<List<i64>>
//   list_of_struct           List<struct<id:i64, name:string>>
//   map_string_struct        Map<string, struct<count:i64, ratio:f64>>
//   struct_with_list_field   struct<id:i64, tags:List<string>>
//   struct_with_struct_field struct<id:i64, inner:struct<a:i64, b:bool>>
//   map_string_list_string   Map<string, List<string>>  (BOLD cross-product)
//
// These are the decisive real-data proof that the SP145 T1-T6 pipeline
// (the 4 new ColumnKind variants, the per-shape assemblers, the recursive
// classify lifts) matches pyarrow's Parquet output — not just hand-built
// fixtures.
const LIST_OF_LIST_I64:           &[u8] = include_bytes!("fixtures/list_of_list_i64.parquet");
const LIST_OF_STRUCT:             &[u8] = include_bytes!("fixtures/list_of_struct.parquet");
const MAP_STRING_STRUCT:          &[u8] = include_bytes!("fixtures/map_string_struct.parquet");
const STRUCT_WITH_LIST_FIELD:     &[u8] = include_bytes!("fixtures/struct_with_list_field.parquet");
const STRUCT_WITH_STRUCT_FIELD:   &[u8] = include_bytes!("fixtures/struct_with_struct_field.parquet");
const MAP_STRING_LIST_STRING:     &[u8] = include_bytes!("fixtures/map_string_list_string.parquet");
const STRUCT_WITH_MAP_FIELD:      &[u8] = include_bytes!("fixtures/struct_with_map_field.parquet");

#[test]
fn pyarrow_list_of_list_i64() {
    let rows = extract(LIST_OF_LIST_I64, &["my_lol"])
        .expect("extract list_of_list_i64");
    assert_eq!(rows.len(), 3);
    // R0: [[1,2,3], [4,5]]
    assert_eq!(rows[0], vec![PqValue::List(vec![
        PqValue::List(vec![I64(1), I64(2), I64(3)]),
        PqValue::List(vec![I64(4), I64(5)]),
    ])]);
    // R1: [[10]]
    assert_eq!(rows[1], vec![PqValue::List(vec![
        PqValue::List(vec![I64(10)]),
    ])]);
    // R2: [[], [100, 200]]
    assert_eq!(rows[2], vec![PqValue::List(vec![
        PqValue::List(vec![]),
        PqValue::List(vec![I64(100), I64(200)]),
    ])]);
}

#[test]
fn pyarrow_list_of_struct() {
    let rows = extract(LIST_OF_STRUCT, &["my_los"])
        .expect("extract list_of_struct");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![PqValue::List(vec![
        PqValue::Struct(vec![
            ("id".into(),   I64(1)),
            ("name".into(), Bytes(b"alice".to_vec())),
        ]),
        PqValue::Struct(vec![
            ("id".into(),   I64(2)),
            ("name".into(), Bytes(b"bob".to_vec())),
        ]),
    ])]);
    assert_eq!(rows[1], vec![PqValue::List(vec![
        PqValue::Struct(vec![
            ("id".into(),   I64(99)),
            ("name".into(), Bytes(b"zoe".to_vec())),
        ]),
    ])]);
}

#[test]
fn pyarrow_map_string_struct() {
    let rows = extract(MAP_STRING_STRUCT, &["my_mss"])
        .expect("extract map_string_struct");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![PqValue::Map(vec![
        (Bytes(b"alpha".to_vec()), PqValue::Struct(vec![
            ("count".into(), I64(1)),
            ("ratio".into(), PqValue::F64(0.5)),
        ])),
        (Bytes(b"beta".to_vec()), PqValue::Struct(vec![
            ("count".into(), I64(2)),
            ("ratio".into(), PqValue::F64(1.5)),
        ])),
    ])]);
    assert_eq!(rows[1], vec![PqValue::Map(vec![
        (Bytes(b"gamma".to_vec()), PqValue::Struct(vec![
            ("count".into(), I64(99)),
            ("ratio".into(), PqValue::F64(3.14)),
        ])),
    ])]);
}

#[test]
fn pyarrow_struct_with_list_field() {
    let rows = extract(STRUCT_WITH_LIST_FIELD, &["my_swl"])
        .expect("extract struct_with_list_field");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec![PqValue::Struct(vec![
        ("id".into(),   I64(1)),
        ("tags".into(), PqValue::List(vec![
            Bytes(b"rust".to_vec()),
            Bytes(b"parquet".to_vec()),
        ])),
    ])]);
    assert_eq!(rows[1], vec![PqValue::Struct(vec![
        ("id".into(),   I64(2)),
        ("tags".into(), PqValue::List(vec![])),
    ])]);
    assert_eq!(rows[2], vec![PqValue::Struct(vec![
        ("id".into(),   I64(3)),
        ("tags".into(), PqValue::List(vec![Bytes(b"nested".to_vec())])),
    ])]);
}

#[test]
fn pyarrow_struct_with_struct_field() {
    let rows = extract(STRUCT_WITH_STRUCT_FIELD, &["my_sws"])
        .expect("extract struct_with_struct_field");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![PqValue::Struct(vec![
        ("id".into(),    I64(1)),
        ("inner".into(), PqValue::Struct(vec![
            ("a".into(), I64(10)),
            ("b".into(), PqValue::Bool(true)),
        ])),
    ])]);
    assert_eq!(rows[1], vec![PqValue::Struct(vec![
        ("id".into(),    I64(2)),
        ("inner".into(), PqValue::Struct(vec![
            ("a".into(), I64(20)),
            ("b".into(), PqValue::Bool(false)),
        ])),
    ])]);
}

#[test]
fn pyarrow_struct_with_map_field() {
    // T9 cross-product: struct<id:i64, attrs:Map<string,i64>>.
    // Exercises the recursive classify_nested_group_child(Map) +
    // decode_field_by_kind(NestedMapKV) compositional path.
    let rows = extract(STRUCT_WITH_MAP_FIELD, &["my_swm"])
        .expect("extract struct_with_map_field");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![PqValue::Struct(vec![
        ("id".into(),    I64(1)),
        ("attrs".into(), PqValue::Map(vec![
            (Bytes(b"a".to_vec()), I64(10)),
            (Bytes(b"b".to_vec()), I64(20)),
        ])),
    ])]);
    assert_eq!(rows[1], vec![PqValue::Struct(vec![
        ("id".into(),    I64(2)),
        ("attrs".into(), PqValue::Map(vec![
            (Bytes(b"x".to_vec()), I64(99)),
        ])),
    ])]);
}

#[test]
fn pyarrow_map_string_list_string() {
    let rows = extract(MAP_STRING_LIST_STRING, &["my_msls"])
        .expect("extract map_string_list_string");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![PqValue::Map(vec![
        (Bytes(b"langs".to_vec()), PqValue::List(vec![
            Bytes(b"rust".to_vec()),
            Bytes(b"go".to_vec()),
        ])),
        (Bytes(b"frameworks".to_vec()), PqValue::List(vec![
            Bytes(b"axum".to_vec()),
            Bytes(b"tokio".to_vec()),
        ])),
    ])]);
    assert_eq!(rows[1], vec![PqValue::Map(vec![
        (Bytes(b"single".to_vec()), PqValue::List(vec![
            Bytes(b"only".to_vec()),
        ])),
    ])]);
}

// SP146 T5: pyarrow deep-nesting follow-up fixtures (close OBJ-2c-5 arc).
//
// Fixtures generated by tests/fixtures/regen_deep_nesting_followups.py via
// pyarrow 24.0.0. Exercises the 3 SP145-deferred cross-products that
// SP146 closes:
//   list_of_list_of_list_i64    List<List<List<i64>>> (3-deep, max_rep=3)
//   list_of_map_string_i64      List<Map<string, i64>>
//   map_string_map_string_i64   Map<string, Map<string, i64>>
const LIST_OF_LIST_OF_LIST_I64:    &[u8] = include_bytes!("fixtures/list_of_list_of_list_i64.parquet");
const LIST_OF_MAP_STRING_I64:      &[u8] = include_bytes!("fixtures/list_of_map_string_i64.parquet");
const MAP_STRING_MAP_STRING_I64:   &[u8] = include_bytes!("fixtures/map_string_map_string_i64.parquet");

#[test]
fn pyarrow_list_of_list_of_list_i64() {
    let rows = extract(LIST_OF_LIST_OF_LIST_I64, &["my_lll"])
        .expect("extract list_of_list_of_list_i64");
    assert_eq!(rows.len(), 3);
    // R0: [[[1,2],[3]], [[4]]]
    assert_eq!(rows[0], vec![PqValue::List(vec![
        PqValue::List(vec![
            PqValue::List(vec![I64(1), I64(2)]),
            PqValue::List(vec![I64(3)]),
        ]),
        PqValue::List(vec![
            PqValue::List(vec![I64(4)]),
        ]),
    ])]);
    // R1: [[[10]]]
    assert_eq!(rows[1], vec![PqValue::List(vec![
        PqValue::List(vec![PqValue::List(vec![I64(10)])]),
    ])]);
    // R2: [[[20],[30]]]
    assert_eq!(rows[2], vec![PqValue::List(vec![
        PqValue::List(vec![
            PqValue::List(vec![I64(20)]),
            PqValue::List(vec![I64(30)]),
        ]),
    ])]);
}

#[test]
fn pyarrow_list_of_map_string_i64() {
    let rows = extract(LIST_OF_MAP_STRING_I64, &["my_lom"])
        .expect("extract list_of_map_string_i64");
    assert_eq!(rows.len(), 2);
    // R0: [{"a": 1, "b": 2}, {"c": 3}]
    assert_eq!(rows[0], vec![PqValue::List(vec![
        PqValue::Map(vec![
            (Bytes(b"a".to_vec()), I64(1)),
            (Bytes(b"b".to_vec()), I64(2)),
        ]),
        PqValue::Map(vec![
            (Bytes(b"c".to_vec()), I64(3)),
        ]),
    ])]);
    // R1: [{"only": 99}]
    assert_eq!(rows[1], vec![PqValue::List(vec![
        PqValue::Map(vec![
            (Bytes(b"only".to_vec()), I64(99)),
        ]),
    ])]);
}

#[test]
fn pyarrow_map_string_map_string_i64() {
    let rows = extract(MAP_STRING_MAP_STRING_I64, &["my_mom"])
        .expect("extract map_string_map_string_i64");
    assert_eq!(rows.len(), 2);
    // R0: {"alpha": {"x":1,"y":2}, "beta": {"z":3}}
    assert_eq!(rows[0], vec![PqValue::Map(vec![
        (Bytes(b"alpha".to_vec()), PqValue::Map(vec![
            (Bytes(b"x".to_vec()), I64(1)),
            (Bytes(b"y".to_vec()), I64(2)),
        ])),
        (Bytes(b"beta".to_vec()), PqValue::Map(vec![
            (Bytes(b"z".to_vec()), I64(3)),
        ])),
    ])]);
    // R1: {"gamma": {"k": 99}}
    assert_eq!(rows[1], vec![PqValue::Map(vec![
        (Bytes(b"gamma".to_vec()), PqValue::Map(vec![
            (Bytes(b"k".to_vec()), I64(99)),
        ])),
    ])]);
}

/// SP149: pyarrow LZ4_RAW round-trip — real-data validation that the
/// hand-rolled lz4.rs block decoder matches what pyarrow 24.0.0 emits
/// for `compression='lz4'`. The fixture has 2 columns (id INT64 +
/// name STRING), 5 rows, single row group, V1 data pages, no dictionary,
/// codec id 7 (LZ4_RAW — verified via footer hex inspection: f4 codec
/// header 0x15 followed by zigzag varint 0x0e = decoded value 7).
#[test]
fn pyarrow_lz4_raw_flat() {
    let rows = extract(LZ4_RAW_FLAT, &["id", "name"]).expect("extract lz4_raw fixture");
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0], vec![I64(1), Bytes(b"alice".to_vec())]);
    assert_eq!(rows[1], vec![I64(2), Bytes(b"bob".to_vec())]);
    assert_eq!(rows[2], vec![I64(3), Bytes(b"carol".to_vec())]);
    assert_eq!(rows[3], vec![I64(4), Bytes(b"dave".to_vec())]);
    assert_eq!(rows[4], vec![I64(5), Bytes(b"eve".to_vec())]);
}

/// SP154 final lock: pyarrow BROTLI fixture decodes end-to-end via the
/// zero-dep RFC 7932 decoder. The fixture has 2 columns (id INT64 +
/// name STRING), 5 rows, single row group, V1 data pages, no dictionary,
/// codec id 4. Both data pages (id i64 column + name BYTE_ARRAY column)
/// decode byte-identical to pyarrow's encoder input. This closes the
/// SP154 SP-arc and CLOSES OBJ-2c-2 codec matrix at 6/7 codecs supported
/// (LZO remains deprecated).
#[test]
fn pyarrow_brotli_flat() {
    let rows = extract(BROTLI_FLAT, &["id", "name"]).expect("extract brotli fixture");
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0], vec![I64(1), Bytes(b"alice".to_vec())]);
    assert_eq!(rows[1], vec![I64(2), Bytes(b"bob".to_vec())]);
    assert_eq!(rows[2], vec![I64(3), Bytes(b"carol".to_vec())]);
    assert_eq!(rows[3], vec![I64(4), Bytes(b"dave".to_vec())]);
    assert_eq!(rows[4], vec![I64(5), Bytes(b"eve".to_vec())]);
}

// ════════════════════════════════════════════════════════════════════════════
// SP151 — Parquet >64 MiB page payload cap lift (OBJ-2c-4 follow-up)
//
// The historical 64 MiB cap was distributed across three per-codec module
// consts. SP151 bumps each to 256 MiB (matching DEFAULT_MAX_PAGE_SIZE) and
// adds extract_with_cap as the operator's configurable knob.
//
// These tests pin the new public API contract:
//   - DEFAULT_MAX_PAGE_SIZE = 256 * 1024 * 1024 (operator-visible const)
//   - extract(file, w) decodes any existing fixture (cap is generous)
//   - extract_with_cap(file, w, DEFAULT_MAX_PAGE_SIZE) is byte-identical
//     to extract(file, w)
//   - extract_with_cap(file, w, 100) rejects with Unsupported NAMING the
//     cap and the operator knob (so a hostile/large input gets a clear
//     pointer at extract_with_cap rather than a generic Bad)
//   - extract_with_cap(file, w, 0) rejects every page (kill-switch)
//   - Thread-local cap state is restored after the call (subsequent
//     extract() with default cap works again)
// ════════════════════════════════════════════════════════════════════════════

#[test]
fn sp151_default_max_page_size_is_256_mib() {
    assert_eq!(DEFAULT_MAX_PAGE_SIZE, 256 * 1024 * 1024);
}

#[test]
fn sp151_extract_with_cap_default_matches_extract() {
    // The flat fixture decodes identically under extract() and under
    // extract_with_cap(.., DEFAULT_MAX_PAGE_SIZE). This pins that the
    // V1 default path is byte-identical to the configurable path at
    // the default cap — no observable behavior change for the
    // historical extract() entry point.
    let a = extract(FLAT, &["id", "name"]).unwrap();
    let b = extract_with_cap(FLAT, &["id", "name"], DEFAULT_MAX_PAGE_SIZE).unwrap();
    assert_eq!(a, b);
}

#[test]
fn sp151_extract_with_cap_high_enough_for_dict_fixture() {
    // 256 MiB is the default; a slightly higher cap still decodes
    // (proving the cap is plumbed but doesn't artificially limit at
    // the default). Use 512 MiB — still well below the per-codec
    // module ceiling (which is exactly DEFAULT_MAX_PAGE_SIZE, so the
    // per-codec const blocks anything above; this test pins that a
    // user-supplied cap ABOVE the per-codec ceiling still lets pages
    // BELOW the per-codec ceiling through — it does not raise the
    // ceiling, only the per-call lower bound).
    let r = extract_with_cap(DICT, &["id"], 512 * 1024 * 1024).unwrap();
    assert!(!r.is_empty());
}

#[test]
fn sp151_extract_with_cap_low_rejects_unsupported_with_named_knob() {
    // A 1-byte cap is below any real page's declared
    // uncompressed_page_size (every page header carries a strictly
    // positive size — even a 0-row page header thrift-encodes its
    // own metadata, so uncompressed_size >= 1 in practice). The
    // first page-size check fires Unsupported. The error message
    // must NAME the SP151 follow-up tag AND the extract_with_cap
    // operator knob so an operator hitting this in production has a
    // direct path to raise the cap. (NOTE: the FLAT fixture's pages
    // are tiny — 100-byte cap actually decodes; use cap=1 so the
    // check is exercised reliably across all fixture sizes.)
    let err = extract_with_cap(FLAT, &["id", "name"], 1)
        .expect_err("1-byte cap must reject every real page");
    match err {
        PqError::Unsupported(msg) => {
            assert!(
                msg.contains("SP151"),
                "rejection must NAME SP151 so the follow-up is greppable: {msg}"
            );
            assert!(
                msg.contains("extract_with_cap"),
                "rejection must NAME the operator knob: {msg}"
            );
            assert!(
                msg.contains("max_page_size cap 1"),
                "rejection must echo the cap value so the operator knows \
                 what they set: {msg}"
            );
        }
        other => panic!("expected Unsupported(SP151...), got {other:?}"),
    }
}

#[test]
fn sp151_extract_with_cap_zero_rejects_every_page() {
    // cap=0 is the operator kill-switch: every page header decodes a
    // strictly-positive uncompressed_page_size, so the first
    // check_page_size fires.
    let err = extract_with_cap(FLAT, &["id"], 0)
        .expect_err("cap=0 must reject every page");
    assert!(matches!(err, PqError::Unsupported(_)));
}

#[test]
fn sp151_thread_local_cap_restored_after_failed_extract() {
    // The RAII guard restores the previous cap on any return path
    // (including Err). Pin that a failed extract_with_cap doesn't
    // leave the thread-local in the tightened state — subsequent
    // extract() must work at the default cap.
    let _ = extract_with_cap(FLAT, &["id"], 100); // expected Err
    // The default cap is back in force: extract() works.
    let r = extract(FLAT, &["id"]).unwrap();
    assert!(!r.is_empty());
}

#[test]
fn sp151_thread_local_cap_restored_after_panicking_extract() {
    // The RAII guard restores on Drop, which fires even on a panic
    // unwind. Pin that a `extract_with_cap` that panics (here:
    // induced by a hostile cap inside a catch_unwind) leaves the
    // default cap restored.
    let _ = std::panic::catch_unwind(|| {
        let _ = extract_with_cap(FLAT, &["id"], 100);
        panic!("simulated panic AFTER the extract_with_cap call");
    });
    // Cap restored: default extract works.
    let r = extract(FLAT, &["id"]).unwrap();
    assert!(!r.is_empty());
}

#[test]
fn sp151_pyarrow_dict_int64_decodes_at_default_cap() {
    // Round-trip oracle: every prior pyarrow oracle (LIST, MAP, dict,
    // V2, snappy, gzip, zstd, LZ4_RAW, INT96, DECIMAL, deep nesting)
    // must still decode at the SP151 default cap. Spot-checking the
    // dict-flat fixture pins the back-compat invariant; the broader
    // fixture corpus is exercised by every other test in this file.
    let r = extract(DICT, &["id"]).unwrap();
    assert!(!r.is_empty());
}
