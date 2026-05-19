//! rows_from_body(Format::Parquet) over the kessel-parquet fixture.
//! Only compiled with --features object-store.
#![cfg(feature = "object-store")]

use kessel_catalog::FieldKind;
use kessel_fetch::{rows_from_body_for_test, ColumnMap, Format};

const FLAT: &[u8] =
    include_bytes!("../../kessel-parquet/tests/fixtures/flat_required.parquet");

#[test]
fn parquet_rows_coerce_through_existing_path() {
    // id column is INT64 in the fixture (rows include -2); use I64.
    // 7i64.to_le_bytes() == 7u64.to_le_bytes() so the plan's assertion holds.
    let cols = vec![
        ColumnMap { name: "id".into(), kind: FieldKind::I64, source: "id".into() },
        ColumnMap { name: "name".into(), kind: FieldKind::Char(8), source: "name".into() },
    ];
    let rows = rows_from_body_for_test(FLAT, Format::Parquet, &cols, None).unwrap();
    assert_eq!(rows[0][0], 7u64.to_le_bytes().to_vec());
    assert_eq!(&rows[0][1][..2], b"hi");
}
