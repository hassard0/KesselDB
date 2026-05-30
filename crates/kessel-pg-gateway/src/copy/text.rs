//! SP-PG-COPY — text-format COPY row codec.
//!
//! PG §COPY-FORMATS / `src/backend/commands/copyfromparse.c`:
//!
//! - Rows separated by `\n` (LF). `\r\n` is also accepted on input;
//!   V1 emits just `\n` on output.
//! - Fields separated by `\t` (tab).
//! - NULL is the literal `\N` (backslash + uppercase N). Distinct
//!   from empty-string (which is just an empty field between two
//!   tabs).
//! - Backslash escapes inside field values: `\b` `\f` `\n` `\r` `\t`
//!   `\v` `\\` (and `\N` only at the whole-field-byte boundary).
//! - End-of-data marker (single line `\.\n`) is OPTIONAL in v3
//!   protocol; V1 tolerates the marker on input and skips it.
//!
//! Two surfaces:
//!
//! - `parse_text_row_bytes(line, ncols)` — decode one row of bytes
//!   (WITHOUT the trailing `\n`) into a `Vec<Option<Vec<u8>>>`.
//! - `encode_text_row(values)` — encode a row into bytes including
//!   the trailing `\n`. Used by COPY TO.
//!
//! Both surfaces honor the §4 7-edge corpus in the spec.

#![forbid(unsafe_code)]
#![allow(dead_code)]

/// Errors `parse_text_row_bytes` can return. All map at the caller
/// to PG SQLSTATE `22023 invalid_parameter_value` (the canonical
/// "COPY data does not match table column count / value" SQLSTATE).
#[derive(Debug, PartialEq, Eq)]
pub enum CopyTextParseError {
    /// The number of tab-separated fields in the row does NOT match
    /// the expected column count. PG terminology: "extra data after
    /// last expected column" or "missing data for column N".
    FieldCountMismatch { expected: usize, actual: usize },
    /// A backslash escape sequence that isn't one of the 8 PG-
    /// recognized ones (`\b \f \n \r \t \v \\ \N`). PG would accept
    /// `\<digit>` as octal too — V2 SP-PG-COPY-CSV may add; V1
    /// rejects.
    UnknownEscape { byte: u8 },
    /// A trailing backslash with no following byte. PG rejects this
    /// as malformed.
    TrailingBackslash,
}

/// Parse one row of text-format COPY data (without the trailing `\n`)
/// into a per-field `Option<Vec<u8>>` (None = NULL sentinel `\N`).
///
/// `ncols` is the expected number of columns. The row MUST have
/// exactly that many tab-separated fields — a mismatch is a
/// `FieldCountMismatch` error.
///
/// **Special-case: the `\.` end-of-data marker.** A line consisting
/// of EXACTLY `\.` (two bytes) is the legacy v2-protocol end-of-data
/// marker. PG v3 protocol uses `CopyDone` framing instead, so the
/// marker is redundant — but psql still emits it via `\copy`. V1's
/// caller (the COPY FROM dispatcher in T2) detects this and skips
/// the line BEFORE calling `parse_text_row_bytes`; this function
/// itself only handles real-row decode. If a caller passes a `\.`
/// line in, the function returns Ok with one field (`Some(b".".to_vec())`)
/// from the byte-level decode — the caller is responsible for the
/// special-case handling.
pub fn parse_text_row_bytes(
    line: &[u8],
    ncols: usize,
) -> Result<Vec<Option<Vec<u8>>>, CopyTextParseError> {
    let mut fields: Vec<Option<Vec<u8>>> = Vec::with_capacity(ncols);
    let mut current: Vec<u8> = Vec::new();
    let mut i = 0usize;
    let mut field_started = false;
    let mut is_null = false;

    // Track whether we're at the START of a field (so a leading `\N`
    // is the NULL sentinel; an embedded `\N` mid-field is just a
    // literal `N`).
    let mut at_field_start = true;

    while i < line.len() {
        let b = line[i];
        if b == b'\t' {
            // Field separator — push current field, reset.
            if is_null {
                fields.push(None);
            } else {
                fields.push(Some(std::mem::take(&mut current)));
            }
            current.clear();
            is_null = false;
            at_field_start = true;
            field_started = false;
            i += 1;
            continue;
        }
        if b == b'\\' {
            // Escape sequence — needs at least one more byte.
            if i + 1 >= line.len() {
                return Err(CopyTextParseError::TrailingBackslash);
            }
            let next = line[i + 1];
            if at_field_start && next == b'N' && (i + 2 == line.len() || line[i + 2] == b'\t') {
                // NULL sentinel `\N` (must be the WHOLE field).
                is_null = true;
                i += 2;
                at_field_start = false;
                field_started = true;
                continue;
            }
            match next {
                b'b' => current.push(b'\x08'),
                b'f' => current.push(b'\x0c'),
                b'n' => current.push(b'\n'),
                b'r' => current.push(b'\r'),
                b't' => current.push(b'\t'),
                b'v' => current.push(b'\x0b'),
                b'\\' => current.push(b'\\'),
                b'N' => current.push(b'N'), // `\N` mid-field is literal N
                other => return Err(CopyTextParseError::UnknownEscape { byte: other }),
            }
            i += 2;
            at_field_start = false;
            field_started = true;
            continue;
        }
        current.push(b);
        i += 1;
        at_field_start = false;
        field_started = true;
    }
    // Push the trailing field. Even if the input is empty (zero-byte
    // line), the canonical PG behavior is to emit ONE empty field —
    // PG treats `\n` alone as "one row with one empty column."
    // EXCEPT if ncols is 0 (a zero-column table) — defensive guard.
    if field_started || !line.is_empty() || ncols > 0 {
        if is_null {
            fields.push(None);
        } else {
            fields.push(Some(current));
        }
    }
    if fields.len() != ncols {
        return Err(CopyTextParseError::FieldCountMismatch {
            expected: ncols,
            actual: fields.len(),
        });
    }
    Ok(fields)
}

/// Encode one row of text-format COPY data. Returns the bytes
/// INCLUDING the trailing `\n` (so the caller can stream multiple
/// rows by concatenating).
///
/// Each field is either `Some(bytes)` (the raw bytes; the encoder
/// applies the 7 backslash escapes) or `None` (NULL → wire `\N`).
/// Fields are separated by `\t`.
pub fn encode_text_row(values: &[Option<&[u8]>]) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::with_capacity(64); // typical row size
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        match v {
            None => out.extend_from_slice(b"\\N"),
            Some(bytes) => {
                for &b in *bytes {
                    match b {
                        b'\\' => out.extend_from_slice(b"\\\\"),
                        b'\n' => out.extend_from_slice(b"\\n"),
                        b'\r' => out.extend_from_slice(b"\\r"),
                        b'\t' => out.extend_from_slice(b"\\t"),
                        b'\x08' => out.extend_from_slice(b"\\b"),
                        b'\x0c' => out.extend_from_slice(b"\\f"),
                        b'\x0b' => out.extend_from_slice(b"\\v"),
                        other => out.push(other),
                    }
                }
            }
        }
    }
    out.push(b'\n');
    out
}

/// True iff the given line is the v2-protocol end-of-data marker
/// (a single line consisting of exactly the two bytes `\.`). The
/// COPY FROM dispatcher consults this before parsing the line as
/// a data row.
pub fn is_end_of_data_marker(line: &[u8]) -> bool {
    line == b"\\."
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── parse_text_row_bytes — happy paths ─────────────────────────────

    /// SP-PG-COPY T1: a two-field tab-separated row decodes cleanly.
    #[test]
    fn t1_parse_two_fields_round_trip() {
        let line = b"1\thello";
        let fields = parse_text_row_bytes(line, 2).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], Some(b"1".to_vec()));
        assert_eq!(fields[1], Some(b"hello".to_vec()));
    }

    /// SP-PG-COPY T1: a NULL field renders as the `\N` sentinel
    /// (uppercase N, not lowercase).
    #[test]
    fn t1_parse_null_field_returns_none() {
        let line = b"1\t\\N";
        let fields = parse_text_row_bytes(line, 2).unwrap();
        assert_eq!(fields[0], Some(b"1".to_vec()));
        assert_eq!(fields[1], None);
    }

    /// SP-PG-COPY T1: empty-string field (`\t\t`) is distinct from
    /// NULL — empties to `Some(b"".to_vec())`.
    #[test]
    fn t1_parse_empty_field_is_distinct_from_null() {
        let line = b"a\t\tc";
        let fields = parse_text_row_bytes(line, 3).unwrap();
        assert_eq!(fields[0], Some(b"a".to_vec()));
        assert_eq!(fields[1], Some(b"".to_vec()));
        assert_eq!(fields[2], Some(b"c".to_vec()));
    }

    /// SP-PG-COPY T1: every backslash escape decodes to the right
    /// byte. PG §COPY-FORMATS canonical 7-escape list.
    #[test]
    fn t1_parse_backslash_escapes_decode_correctly() {
        let line = b"\\b\\f\\n\\r\\t\\v\\\\";
        let fields = parse_text_row_bytes(line, 1).unwrap();
        assert_eq!(
            fields[0],
            Some(vec![0x08, 0x0c, b'\n', b'\r', b'\t', 0x0b, b'\\'])
        );
    }

    /// SP-PG-COPY T1: a `\N` mid-field (not at the field start) is
    /// the literal `N` (per PG: `\N` is the NULL sentinel ONLY when
    /// the entire field is `\N`).
    #[test]
    fn t1_parse_n_mid_field_is_literal_n() {
        let line = b"foo\\Nbar";
        let fields = parse_text_row_bytes(line, 1).unwrap();
        assert_eq!(fields[0], Some(b"fooNbar".to_vec()));
    }

    /// SP-PG-COPY T1: a row with embedded `\t` (escaped) in a field
    /// value decodes correctly.
    #[test]
    fn t1_parse_field_with_embedded_tab_escape() {
        let line = b"col1\tcol\\t2\tcol3";
        let fields = parse_text_row_bytes(line, 3).unwrap();
        assert_eq!(fields[0], Some(b"col1".to_vec()));
        assert_eq!(fields[1], Some(b"col\t2".to_vec()));
        assert_eq!(fields[2], Some(b"col3".to_vec()));
    }

    /// SP-PG-COPY T1: field count mismatch surfaces.
    #[test]
    fn t1_parse_field_count_mismatch() {
        let line = b"a\tb\tc";
        match parse_text_row_bytes(line, 2) {
            Err(CopyTextParseError::FieldCountMismatch { expected, actual }) => {
                assert_eq!(expected, 2);
                assert_eq!(actual, 3);
            }
            other => panic!("expected FieldCountMismatch, got {other:?}"),
        }
    }

    /// SP-PG-COPY T1: trailing backslash with nothing after → error.
    #[test]
    fn t1_parse_trailing_backslash_rejected() {
        let line = b"foo\\";
        assert!(matches!(
            parse_text_row_bytes(line, 1),
            Err(CopyTextParseError::TrailingBackslash)
        ));
    }

    /// SP-PG-COPY T1: unknown escape sequence → error.
    #[test]
    fn t1_parse_unknown_escape_rejected() {
        let line = b"foo\\xbar";
        assert!(matches!(
            parse_text_row_bytes(line, 1),
            Err(CopyTextParseError::UnknownEscape { byte: b'x' })
        ));
    }

    // ─── encode_text_row — happy paths ──────────────────────────────────

    /// SP-PG-COPY T1: encoding a two-field row matches the wire shape
    /// PG produces (`field1\tfield2\n`).
    #[test]
    fn t1_encode_two_fields_matches_wire_shape() {
        let values: Vec<Option<&[u8]>> = vec![Some(b"1"), Some(b"hello")];
        let bytes = encode_text_row(&values);
        assert_eq!(bytes, b"1\thello\n");
    }

    /// SP-PG-COPY T1: encoding a NULL field emits the `\N` sentinel.
    #[test]
    fn t1_encode_null_field_emits_backslash_n() {
        let values: Vec<Option<&[u8]>> = vec![Some(b"1"), None, Some(b"3")];
        let bytes = encode_text_row(&values);
        assert_eq!(bytes, b"1\t\\N\t3\n");
    }

    /// SP-PG-COPY T1: encoding a field containing the 7 escapable
    /// characters produces the canonical escaped form.
    #[test]
    fn t1_encode_special_chars_escaped() {
        let values: Vec<Option<&[u8]>> =
            vec![Some(&[b'\\', b'\n', b'\r', b'\t', 0x08, 0x0c, 0x0b])];
        let bytes = encode_text_row(&values);
        // Each char produces a 2-byte `\X` escape, then `\n` row
        // terminator → 14 + 1 = 15 bytes.
        assert_eq!(bytes, b"\\\\\\n\\r\\t\\b\\f\\v\n");
        assert_eq!(bytes.len(), 15);
    }

    /// SP-PG-COPY T1 — round-trip property: an arbitrary byte vector
    /// encoded then parsed back returns the original bytes (modulo
    /// the trailing `\n` that encode adds + parse strips). Lock
    /// against escape/unescape drift.
    #[test]
    fn t1_text_row_round_trip_property() {
        let originals: Vec<Vec<u8>> = vec![
            b"plain".to_vec(),
            b"".to_vec(),
            b"with\tembedded\ttab".to_vec(),
            b"with\nembedded\nnewline".to_vec(),
            b"with\\embedded\\backslash".to_vec(),
            vec![0x08, 0x0c, 0x0b, b'\r'],
            b"unicode caf\xc3\xa9".to_vec(), // UTF-8 "café"
        ];
        let refs: Vec<Option<&[u8]>> = originals.iter().map(|v| Some(v.as_slice())).collect();
        let encoded = encode_text_row(&refs);
        // Strip the trailing \n that encode_text_row added before
        // feeding into parse.
        assert_eq!(encoded.last(), Some(&b'\n'));
        let line = &encoded[..encoded.len() - 1];
        let parsed = parse_text_row_bytes(line, originals.len()).unwrap();
        for (i, (orig, got)) in originals.iter().zip(parsed.iter()).enumerate() {
            assert_eq!(got.as_ref().unwrap(), orig, "field {i} round-trip");
        }
    }

    /// SP-PG-COPY T1 — round-trip property with a NULL field mixed
    /// in. None → `\N` → None.
    #[test]
    fn t1_text_row_round_trip_with_null() {
        let fields: Vec<Option<&[u8]>> = vec![Some(b"hello"), None, Some(b"world")];
        let encoded = encode_text_row(&fields);
        let line = &encoded[..encoded.len() - 1];
        let parsed = parse_text_row_bytes(line, 3).unwrap();
        assert_eq!(parsed[0], Some(b"hello".to_vec()));
        assert_eq!(parsed[1], None);
        assert_eq!(parsed[2], Some(b"world".to_vec()));
    }

    /// SP-PG-COPY T1: `is_end_of_data_marker` recognizes the v2
    /// `\.` line and nothing else.
    #[test]
    fn t1_end_of_data_marker_recognized() {
        assert!(is_end_of_data_marker(b"\\."));
        assert!(!is_end_of_data_marker(b"\\.foo"));
        assert!(!is_end_of_data_marker(b"foo\\."));
        assert!(!is_end_of_data_marker(b""));
        assert!(!is_end_of_data_marker(b"\\"));
        assert!(!is_end_of_data_marker(b"."));
    }
}
