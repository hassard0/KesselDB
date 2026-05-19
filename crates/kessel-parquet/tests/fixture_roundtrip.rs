//! Round-trip tests: load pyarrow-generated Parquet fixtures via
//! include_bytes! and assert extract() returns the expected logical
//! rows. Fixture provenance: pyarrow 24.0.0; see
//! tests/fixtures/README.md.
use kessel_parquet::{extract, PqValue};

const FLAT: &[u8] = include_bytes!("fixtures/flat_required.parquet");
const MRG: &[u8] = include_bytes!("fixtures/flat_multirg.parquet");

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
