//! PG backend response-message encoders.
//!
//! **T5 (this commit):** `RowDescription` ('T') + `DataRow` ('D')
//! encoders.
//! **T6 (next commit):** `CommandComplete` ('C') + `ReadyForQuery`
//! ('Z') + `EmptyQueryResponse` ('I') encoders + canonical tag
//! builders.
//!
//! V1 emits all columns in PG TEXT format (format code 0). Binary
//! format is V2. The per-type text-format value renderer lives at
//! T8 — this module emits the wire-frame envelopes around byte
//! payloads the caller supplies.
//!
//! ## Wire shapes (PG §55.7)
//!
//! **RowDescription 'T':**
//! ```text
//! T [length:4 BE] [field_count:2 BE]
//!   for each field:
//!     [name:cstring] [table_oid:4 BE] [column_attr:2 BE]
//!     [type_oid:4 BE] [type_size:2 BE] [type_modifier:4 BE]
//!     [format_code:2 BE]
//! ```
//! V1: table_oid=0 (no table-identity tracking), column_attr=0,
//! type_modifier=-1 (no per-column modifier), format_code=0 (text).
//!
//! **DataRow 'D':**
//! ```text
//! D [length:4 BE] [column_count:2 BE]
//!   for each column:
//!     [length:4 BE]  // -1 for NULL; otherwise byte length
//!     [bytes:length]  // omitted entirely if NULL
//! ```
//!
//! ## What this module does NOT do
//!
//! - It does NOT render a `kessel-proto::Row` to per-column text-format
//!   bytes (T8). The `encode_data_row` here takes already-encoded
//!   `Option<&[u8]>` per column (None = NULL).
//! - It does NOT pick a `CommandComplete` tag from a SQL statement
//!   (T8/T9 inspect the leading keyword to dispatch).
//! - It does NOT emit `ErrorResponse` ('E') — that's T7 with the
//!   SQLSTATE catalog.
//!
//! Companion design spec:
//! `docs/superpowers/specs/2026-05-27-kesseldb-sppg-postgres-wire-design.md`

#![forbid(unsafe_code)]
#![allow(dead_code)]

use crate::proto::{
    BE_DATA_ROW, BE_ROW_DESCRIPTION, FORMAT_CODE_TEXT, PG_DATA_ROW_COL_NULL_SENTINEL,
};
use crate::types::type_size_for_oid;

/// Per-field metadata for `RowDescription`. Caller builds one of
/// these per column (typically by looking up the table's schema via
/// `EngineApply::describe_table` — T8 adds that trait method).
///
/// V1 emits text format only; `type_modifier` is always -1 (no per-
/// column modifier — e.g. `VARCHAR(N)` would encode N here, but V1
/// uses PG `text` which is unbounded so no modifier needed).
#[derive(Debug, Clone)]
pub struct FieldMeta {
    /// Column name as it appears in the result set. Encoded as a
    /// NUL-terminated string in the wire frame.
    pub name: String,
    /// PG type OID from the `types::field_kind_to_oid` table.
    pub type_oid: u32,
}

/// Encodes a `RowDescription` ('T') message. Returns the full wire
/// frame including the type byte and length prefix.
///
/// `fields` may be empty (a query like `SELECT FROM t` would return
/// `RowDescription` with field_count=0 before `DataRow`s with no
/// columns and `CommandComplete`).
///
/// V1 emits table_oid=0 + column_attr=0 + type_modifier=-1 +
/// format_code=0 (text) for every column. `type_size` comes from the
/// `types::type_size_for_oid` table (8 for int8/timestamptz, -1 for
/// bytea/text/numeric, etc.).
pub fn encode_row_description(fields: &[FieldMeta]) -> Vec<u8> {
    // Compute the payload length first so we can write the length
    // prefix without a second pass.
    let mut payload_len = 2; // u16 field_count
    for f in fields {
        // name + NUL + 4 (table_oid) + 2 (column_attr) + 4 (type_oid)
        //  + 2 (type_size) + 4 (type_modifier) + 2 (format_code)
        payload_len += f.name.len() + 1 + 4 + 2 + 4 + 2 + 4 + 2;
    }
    let total_length = (4 + payload_len) as u32;
    let mut frame = Vec::with_capacity(1 + total_length as usize);
    frame.push(BE_ROW_DESCRIPTION);
    frame.extend_from_slice(&total_length.to_be_bytes());
    frame.extend_from_slice(&(fields.len() as u16).to_be_bytes());
    for f in fields {
        frame.extend_from_slice(f.name.as_bytes());
        frame.push(0);
        // table_oid = 0 (we don't carry a table identity)
        frame.extend_from_slice(&0u32.to_be_bytes());
        // column_attr = 0 (per-column attribute number; V1 doesn't track)
        frame.extend_from_slice(&0u16.to_be_bytes());
        // type_oid
        frame.extend_from_slice(&f.type_oid.to_be_bytes());
        // type_size (i16; -1 for var-length)
        let ts = type_size_for_oid(f.type_oid);
        frame.extend_from_slice(&ts.to_be_bytes());
        // type_modifier = -1 (no per-column modifier in V1)
        frame.extend_from_slice(&(-1i32).to_be_bytes());
        // format_code = 0 (text)
        frame.extend_from_slice(&FORMAT_CODE_TEXT.to_be_bytes());
    }
    frame
}

/// Encodes a `DataRow` ('D') message. Each entry in `columns` is
/// either `Some(bytes)` (the pre-encoded PG text-format bytes for the
/// column) or `None` (NULL — wire sentinel is i32 -1).
///
/// The bytes in `Some(bytes)` are emitted verbatim — V1 has already
/// rendered them per the type's text format at the caller (T8). For
/// example, a `Bool::true` is `b"t"`, a `bytea` of `0xDEADBEEF` is
/// `b"\\xdeadbeef"`, a NULL is `None`.
pub fn encode_data_row(columns: &[Option<&[u8]>]) -> Vec<u8> {
    let mut payload_len = 2u32; // u16 col_count
    for c in columns {
        payload_len += 4; // i32 col_length (always present, even for NULL)
        if let Some(bytes) = c {
            payload_len += bytes.len() as u32;
        }
    }
    let total_length = 4 + payload_len;
    let mut frame = Vec::with_capacity(1 + total_length as usize);
    frame.push(BE_DATA_ROW);
    frame.extend_from_slice(&total_length.to_be_bytes());
    frame.extend_from_slice(&(columns.len() as u16).to_be_bytes());
    for c in columns {
        match c {
            None => {
                // PG NULL sentinel: i32 -1.
                frame.extend_from_slice(&PG_DATA_ROW_COL_NULL_SENTINEL.to_be_bytes());
            }
            Some(bytes) => {
                let len = bytes.len() as i32;
                frame.extend_from_slice(&len.to_be_bytes());
                frame.extend_from_slice(bytes);
            }
        }
    }
    frame
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{PG_TYPE_BOOL, PG_TYPE_INT8, PG_TYPE_TEXT};

    // ───────────────────────────────────────────────────────────────────
    // T5 KATs — RowDescription + DataRow encoders.
    // Cross-referenced against PG §55.7 sample wire dumps.
    // ───────────────────────────────────────────────────────────────────

    /// Empty RowDescription (0 columns) is a valid wire frame.
    /// PG accepts this for `SELECT FROM t` (zero projected columns).
    #[test]
    fn t5_empty_row_description() {
        let frame = encode_row_description(&[]);
        // 'T' + length=6 + field_count=0
        assert_eq!(frame, vec![b'T', 0, 0, 0, 6, 0, 0]);
    }

    /// Single-column RowDescription is byte-locked vs spec §5.
    /// Locks every offset: type byte, length prefix, field_count,
    /// name+NUL, table_oid=0, column_attr=0, type_oid, type_size,
    /// type_modifier=-1, format_code=0.
    #[test]
    fn t5_single_column_row_description_byte_locked() {
        let fields = vec![FieldMeta {
            name: "id".to_string(),
            type_oid: PG_TYPE_INT8,
        }];
        let frame = encode_row_description(&fields);
        // Expected:
        //   'T'                     (1)
        //   length=4 + 2 + per-field = 4 + 2 + (2+1)+4+2+4+2+4+2 = 27 (4)
        //   field_count=1           (2)
        //   "id\0"                  (3)
        //   table_oid=0             (4)
        //   column_attr=0           (2)
        //   type_oid=20             (4)
        //   type_size=8             (2)
        //   type_modifier=-1        (4)
        //   format_code=0           (2)
        let mut expected = Vec::new();
        expected.push(b'T');
        expected.extend_from_slice(&27u32.to_be_bytes());
        expected.extend_from_slice(&1u16.to_be_bytes());
        expected.extend_from_slice(b"id\0");
        expected.extend_from_slice(&0u32.to_be_bytes());
        expected.extend_from_slice(&0u16.to_be_bytes());
        expected.extend_from_slice(&20u32.to_be_bytes());
        expected.extend_from_slice(&8i16.to_be_bytes());
        expected.extend_from_slice(&(-1i32).to_be_bytes());
        expected.extend_from_slice(&0u16.to_be_bytes());
        assert_eq!(frame, expected);
    }

    /// Three-column RowDescription with mixed types — id INT8, name
    /// TEXT, flag BOOL. Wire-pattern locked.
    #[test]
    fn t5_three_column_row_description_mixed_types() {
        let fields = vec![
            FieldMeta { name: "id".to_string(), type_oid: PG_TYPE_INT8 },
            FieldMeta { name: "name".to_string(), type_oid: PG_TYPE_TEXT },
            FieldMeta { name: "flag".to_string(), type_oid: PG_TYPE_BOOL },
        ];
        let frame = encode_row_description(&fields);
        // Confirms type byte + field count
        assert_eq!(frame[0], b'T');
        assert_eq!(&frame[5..7], &3u16.to_be_bytes()); // count=3
        // Confirms three names present
        assert!(frame.windows(2).any(|w| w == b"id"));
        assert!(frame.windows(4).any(|w| w == b"name"));
        assert!(frame.windows(4).any(|w| w == b"flag"));
        // Confirms type sizes match the types::type_size_for_oid table
        // int8 → 8, text → -1, bool → 1
        assert!(frame.windows(2).any(|w| w == 8i16.to_be_bytes()));
        assert!(frame.windows(2).any(|w| w == (-1i16).to_be_bytes()));
        assert!(frame.windows(2).any(|w| w == 1i16.to_be_bytes()));
    }

    /// DataRow with a single i64 column 42 → text-format `"42"`.
    /// Wire-pattern locked.
    #[test]
    fn t5_data_row_single_int8_column() {
        let v = b"42";
        let frame = encode_data_row(&[Some(v)]);
        // 'D' + length + col_count=1 + col_length=2 + "42"
        // length = 4 + 2 + 4 + 2 = 12
        let mut expected = Vec::new();
        expected.push(b'D');
        expected.extend_from_slice(&12u32.to_be_bytes());
        expected.extend_from_slice(&1u16.to_be_bytes());
        expected.extend_from_slice(&2i32.to_be_bytes());
        expected.extend_from_slice(b"42");
        assert_eq!(frame, expected);
    }

    /// DataRow with three columns including a NULL middle column.
    /// Wire-pattern locked — NULL sentinel is i32 -1 (0xFFFFFFFF).
    #[test]
    fn t5_data_row_with_null_column() {
        let id = b"1";
        let flag = b"t";
        let frame = encode_data_row(&[Some(id), None, Some(flag)]);
        // 'D' + length + col_count=3
        // col 0: length=1 + "1"
        // col 1: length=-1 (NULL marker, no bytes)
        // col 2: length=1 + "t"
        // total payload = 2 + (4+1) + 4 + (4+1) = 16
        // total length = 4 + 16 = 20
        let mut expected = Vec::new();
        expected.push(b'D');
        expected.extend_from_slice(&20u32.to_be_bytes());
        expected.extend_from_slice(&3u16.to_be_bytes());
        expected.extend_from_slice(&1i32.to_be_bytes());
        expected.extend_from_slice(b"1");
        expected.extend_from_slice(&(-1i32).to_be_bytes());
        expected.extend_from_slice(&1i32.to_be_bytes());
        expected.extend_from_slice(b"t");
        assert_eq!(frame, expected);
        // Confirm the 0xFFFFFFFF unsigned representation is in the frame
        assert!(frame.windows(4).any(|w| w == [0xFF, 0xFF, 0xFF, 0xFF]));
    }

    /// DataRow with an empty (zero-length but NOT NULL) column.
    /// Wire: length=0 + no bytes. Distinct from NULL (length=-1).
    #[test]
    fn t5_data_row_with_empty_non_null_column() {
        let empty: &[u8] = &[];
        let frame = encode_data_row(&[Some(empty)]);
        // length=4+2+4 = 10
        let mut expected = Vec::new();
        expected.push(b'D');
        expected.extend_from_slice(&10u32.to_be_bytes());
        expected.extend_from_slice(&1u16.to_be_bytes());
        expected.extend_from_slice(&0i32.to_be_bytes());
        assert_eq!(frame, expected);
    }

    /// DataRow with bytea text-format value `\\xDEADBEEF` — locks
    /// that no extra escaping happens at the wire layer (T8 already
    /// rendered the bytea text representation).
    #[test]
    fn t5_data_row_bytea_text_format_value_passes_through() {
        let v = b"\\xdeadbeef";
        let frame = encode_data_row(&[Some(v)]);
        // Confirm the bytes are present verbatim
        assert!(frame.windows(10).any(|w| w == b"\\xdeadbeef"));
        // Confirm column length is 10
        assert!(frame.windows(4).any(|w| w == 10i32.to_be_bytes()));
    }

    /// `RowDescription` with no fields + `DataRow` with no columns
    /// — the wire frames are byte-coherent (count=0 in both). Sanity
    /// check for the empty-result-set path.
    #[test]
    fn t5_empty_result_set_frames_are_coherent() {
        let rd = encode_row_description(&[]);
        let dr = encode_data_row(&[]);
        // RowDescription: 'T' + length=6 + count=0
        assert_eq!(rd, vec![b'T', 0, 0, 0, 6, 0, 0]);
        // DataRow: 'D' + length=6 + count=0
        assert_eq!(dr, vec![b'D', 0, 0, 0, 6, 0, 0]);
    }

    /// DataRow with a multi-row stream of i64 values — locks the
    /// pattern T8 will use to stream large SELECT results.
    #[test]
    fn t5_data_row_multiple_int8_values_round_trip() {
        let r1 = encode_data_row(&[Some(b"1"), Some(b"2"), Some(b"3")]);
        let r2 = encode_data_row(&[Some(b"4"), Some(b"5"), Some(b"6")]);
        // Both rows have the same shape:
        // 'D' + length + col_count=3 + (4+1)*3 = 17
        // length = 4 + 2 + 5*3 = 21
        for (frame, vals) in [(r1, [b"1", b"2", b"3"]), (r2, [b"4", b"5", b"6"])].iter() {
            assert_eq!(frame[0], b'D');
            assert_eq!(&frame[5..7], &3u16.to_be_bytes());
            for v in vals.iter() {
                assert!(frame.windows(v.len()).any(|w| w == v.as_slice()));
            }
        }
    }
}
