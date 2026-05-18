//! NDJSON: one JSON object per line; blank lines skipped.
use crate::json::{path_get, parse, Cell};
use crate::{ColumnMap, FetchError};

pub fn extract(
    body: &[u8],
    cols: &[ColumnMap],
) -> Result<Vec<Vec<Cell>>, FetchError> {
    let text = std::str::from_utf8(body)
        .map_err(|_| FetchError::Parse("NDJSON not UTF-8".into()))?;
    let mut rows = Vec::new();
    for (lineno, line) in text.split('\n').enumerate() {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        let v = parse(t).map_err(|e| {
            FetchError::Parse(format!("NDJSON line {}: {e}", lineno + 1))
        })?;
        let mut row = Vec::with_capacity(cols.len());
        for c in cols {
            row.push(path_get(&v, &c.source)?);
        }
        rows.push(row);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_catalog::FieldKind;
    fn cm(n: &str, s: &str) -> ColumnMap {
        ColumnMap { name: n.into(), kind: FieldKind::U64, source: s.into() }
    }

    #[test]
    fn objects_per_line_blanks_skipped() {
        let body = b"{\"id\":1,\"u\":{\"n\":\"a\"}}\n\n{\"id\":2,\"u\":{\"n\":\"b\"}}\n";
        let cols = vec![cm("id", "id"), cm("nm", "u.n")];
        assert_eq!(
            extract(body, &cols).unwrap(),
            vec![
                vec![Cell::Text("1".into()), Cell::Text("a".into())],
                vec![Cell::Text("2".into()), Cell::Text("b".into())],
            ]
        );
    }

    #[test]
    fn malformed_line_is_typed_error() {
        let body = b"{\"id\":1}\n{not json}\n";
        assert!(matches!(
            extract(body, &[cm("id", "id")]),
            Err(FetchError::Parse(_))
        ));
    }

    #[test]
    fn no_trailing_newline_ok() {
        let body = b"{\"id\":7}";
        assert_eq!(
            extract(body, &[cm("id", "id")]).unwrap(),
            vec![vec![Cell::Text("7".into())]]
        );
    }

    #[test]
    fn whitespace_only_line_skipped() {
        let body = b"{\"id\":1}\n   \n{\"id\":2}";
        let rows = extract(body, &[cm("id", "id")]).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn non_utf8_body_is_parse_error() {
        let body = b"\xff\xfe{\"id\":1}";
        assert!(matches!(
            extract(body, &[cm("id", "id")]),
            Err(FetchError::Parse(_))
        ));
    }

    #[test]
    fn error_includes_line_number() {
        let body = b"{\"id\":1}\n{bad}\n{\"id\":3}";
        let err = extract(body, &[cm("id", "id")]).unwrap_err();
        let msg = match err {
            FetchError::Parse(s) => s,
            _ => panic!("wrong variant"),
        };
        assert!(msg.contains("line 2"), "got: {msg}");
    }
}
