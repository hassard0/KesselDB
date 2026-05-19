//! rows_from_body(Format::Parquet) over the kessel-parquet fixture.
//! Only compiled with --features object-store.
#![cfg(feature = "object-store")]

use kessel_catalog::FieldKind;
use kessel_fetch::{rows_from_body_for_test, ColumnMap, FetchError, Format};

const FLAT: &[u8] =
    include_bytes!("../../kessel-parquet/tests/fixtures/flat_required.parquet");

const NULLABLE: &[u8] =
    include_bytes!("../../kessel-parquet/tests/fixtures/nullable.parquet");

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

/// Source-format-independence pin (OBJ-2b-4).
///
/// Both a Parquet OPTIONAL null (def-level==0) and a JSON `null` value
/// converge on the same internal `json::Cell::Null` before reaching
/// `coerce::to_field_bytes`. This test verifies that convergence by
/// feeding each source through `rows_from_body_for_test` with the same
/// non-nullable `FieldKind::I64` column declaration and asserting that
/// BOTH return an identical `FetchError::Type` containing "null".
///
/// Why this is a valid independence pin: `coerce::to_field_bytes` has a
/// single `Cell::Null` arm that returns the "null in a non-nullable
/// external column" error regardless of how the null arrived — the
/// typed error string is the observable footprint of `Cell::Null`.
/// If the Parquet null had been mis-mapped to any other `Cell` variant
/// (e.g. `Cell::Text("null")`) the error would differ or not appear
/// at all, so identical typed errors prove both sources reach the same
/// `Cell::Null` internal representation.
#[test]
fn parquet_null_and_json_null_produce_identical_coerce_error() {
    // Single column: I64, non-nullable (as all current external columns are).
    let col_id = ColumnMap {
        name: "id".into(),
        kind: FieldKind::I64,
        source: "id".into(),
    };

    // Parquet path: nullable.parquet has a null in the `id` column at row 2
    // (id=[7,7,null,-2,100]). rows_from_body_for_test must return
    // FetchError::Type("null in a non-nullable external column").
    let parquet_err = rows_from_body_for_test(
        NULLABLE, Format::Parquet, &[col_id.clone()], None,
    )
    .expect_err("nullable parquet with null id must error at coerce");

    // JSON path: a JSON array with an explicit null value for the id field.
    // The JSON parser produces Cell::Null for `null`, which coerce rejects
    // identically.
    let json_body = br#"[{"id": null}]"#;
    let json_err = rows_from_body_for_test(
        json_body, Format::Json, &[col_id], None,
    )
    .expect_err("json null id must error at coerce");

    // Both must be FetchError::Type (not Http/Parse/Auth) and contain "null".
    match &parquet_err {
        FetchError::Type(msg) => assert!(
            msg.contains("null"),
            "parquet null coerce error must mention null, got: {msg}"
        ),
        other => panic!("expected FetchError::Type for parquet null, got {other:?}"),
    }
    match &json_err {
        FetchError::Type(msg) => assert!(
            msg.contains("null"),
            "json null coerce error must mention null, got: {msg}"
        ),
        other => panic!("expected FetchError::Type for json null, got {other:?}"),
    }

    // The decisive assertion: both paths produce the SAME typed error string,
    // proving they converge on the same Cell::Null internal representation.
    assert_eq!(
        parquet_err, json_err,
        "Parquet OPTIONAL null and JSON null must produce byte-identical FetchError"
    );
}
