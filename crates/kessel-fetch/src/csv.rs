//! RFC 4180 CSV: first row is the header; columns selected by name.
use crate::json::Cell;
use crate::{ColumnMap, FetchError};

pub fn extract(
    body: &[u8],
    cols: &[ColumnMap],
) -> Result<Vec<Vec<Cell>>, FetchError> {
    let text = std::str::from_utf8(body)
        .map_err(|_| FetchError::Parse("CSV not UTF-8".into()))?;
    let mut records = parse_records(text)?;
    if records.is_empty() {
        return Ok(Vec::new());
    }
    let header = records.remove(0);
    let mut idx = Vec::with_capacity(cols.len());
    for c in cols {
        let p = header.iter().position(|h| h == &c.source).ok_or_else(|| {
            FetchError::Parse(format!("CSV header has no column `{}`", c.source))
        })?;
        idx.push(p);
    }
    let mut rows = Vec::with_capacity(records.len());
    for rec in records {
        let mut row = Vec::with_capacity(cols.len());
        for &p in &idx {
            let v = rec.get(p).cloned().ok_or_else(|| {
                FetchError::Parse("CSV row shorter than header".into())
            })?;
            row.push(if v.is_empty() { Cell::Null } else { Cell::Text(v) });
        }
        rows.push(row);
    }
    Ok(rows)
}

// Slice-1: non-ASCII bytes in CSV are pushed byte-wise; UTF-8 CSV text is a documented follow-on (see EXT design non-goals).
/// Split RFC 4180 text into records of fields. Quotes allow `,`,
/// CR, LF; `""` is an escaped quote. Trailing newline ignored.
fn parse_records(s: &str) -> Result<Vec<Vec<String>>, FetchError> {
    let b = s.as_bytes();
    let mut recs: Vec<Vec<String>> = Vec::new();
    let mut rec: Vec<String> = Vec::new();
    let mut field = String::new();
    let mut i = 0;
    let mut in_q = false;
    let mut any = false;
    while i < b.len() {
        let c = b[i];
        if in_q {
            if c == b'"' {
                if i + 1 < b.len() && b[i + 1] == b'"' {
                    field.push('"');
                    i += 2;
                    continue;
                }
                in_q = false;
                i += 1;
                continue;
            }
            field.push(c as char);
            i += 1;
            continue;
        }
        match c {
            b'"' => {
                in_q = true;
                any = true;
                i += 1;
            }
            b',' => {
                rec.push(std::mem::take(&mut field));
                any = true;
                i += 1;
            }
            b'\r' => {
                i += 1;
            }
            b'\n' => {
                rec.push(std::mem::take(&mut field));
                recs.push(std::mem::take(&mut rec));
                any = false;
                i += 1;
            }
            _ => {
                field.push(c as char);
                any = true;
                i += 1;
            }
        }
    }
    if in_q {
        return Err(FetchError::Parse("unterminated CSV quote".into()));
    }
    if any || !field.is_empty() {
        rec.push(field);
        recs.push(rec);
    }
    Ok(recs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_catalog::FieldKind;

    fn cm(name: &str, src: &str) -> ColumnMap {
        ColumnMap { name: name.into(), kind: FieldKind::U64, source: src.into() }
    }

    #[test]
    fn header_selected_by_name_with_quotes_and_newlines() {
        let body = b"id,note\r\n1,\"a,b\"\r\n2,\"line\r\nbreak\"\r\n";
        let cols = vec![cm("n", "note"), cm("i", "id")];
        let rows = extract(body, &cols).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Cell::Text("a,b".into()), Cell::Text("1".into())],
                vec![Cell::Text("line\r\nbreak".into()), Cell::Text("2".into())],
            ]
        );
    }

    #[test]
    fn empty_field_is_null_and_escaped_quote() {
        let body = b"a,b\n,\"x\"\"y\"\n";
        let cols = vec![cm("a", "a"), cm("b", "b")];
        let rows = extract(body, &cols).unwrap();
        assert_eq!(rows, vec![vec![Cell::Null, Cell::Text("x\"y".into())]]);
    }

    #[test]
    fn missing_header_column_is_parse_error() {
        let body = b"a\n1\n";
        assert!(matches!(
            extract(body, &[cm("x", "zzz")]),
            Err(FetchError::Parse(_))
        ));
    }
}
