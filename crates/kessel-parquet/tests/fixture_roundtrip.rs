//! Round-trip tests: load pyarrow-generated Parquet fixtures via
//! include_bytes! and assert extract() returns the expected logical
//! rows. Fixture provenance: pyarrow 24.0.0; see
//! tests/fixtures/README.md.
use kessel_parquet::{extract, PqValue};
use kessel_parquet::PqValue::{Bytes, I64, Null};

const FLAT: &[u8] = include_bytes!("fixtures/flat_required.parquet");
const MRG: &[u8] = include_bytes!("fixtures/flat_multirg.parquet");
const DICT: &[u8] = include_bytes!("fixtures/dict_flat.parquet");
const NULLABLE: &[u8] = include_bytes!("fixtures/nullable.parquet");
const NULLABLE_PLAIN: &[u8] = include_bytes!("fixtures/nullable_plain.parquet");

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
