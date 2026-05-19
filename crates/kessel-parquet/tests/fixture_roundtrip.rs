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
