//! Minimal JSON: array of objects, scalar dotted-path extraction.
use crate::{ColumnMap, FetchError};

/// A JSON scalar rendered canonically for coercion.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Cell {
    Null,
    Bool(bool),
    /// number or string, kept as its source text (numbers un-reformatted)
    Text(String),
}

pub fn extract(
    body: &[u8],
    cols: &[ColumnMap],
) -> Result<Vec<Vec<Cell>>, FetchError> {
    let v = parse(std::str::from_utf8(body).map_err(|_| {
        FetchError::Parse("body is not UTF-8".into())
    })?)?;
    let arr = match v {
        Json::Array(a) => a,
        _ => return Err(FetchError::Parse("top level must be an array".into())),
    };
    let mut rows = Vec::with_capacity(arr.len());
    for el in &arr {
        let mut row = Vec::with_capacity(cols.len());
        for c in cols {
            row.push(path_get(el, &c.source)?);
        }
        rows.push(row);
    }
    Ok(rows)
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Json {
    Null,
    Bool(bool),
    Num(String),
    Str(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

pub(crate) fn path_get(v: &Json, path: &str) -> Result<Cell, FetchError> {
    let mut cur = v;
    for seg in path.split('.') {
        match cur {
            Json::Object(m) => {
                cur = m
                    .iter()
                    .find(|(k, _)| k == seg)
                    .map(|(_, vv)| vv)
                    .ok_or_else(|| {
                        FetchError::Parse(format!("path `{path}`: no key `{seg}`"))
                    })?;
            }
            _ => {
                return Err(FetchError::Parse(format!(
                    "path `{path}`: `{seg}` is not an object"
                )))
            }
        }
    }
    match cur {
        Json::Null => Ok(Cell::Null),
        Json::Bool(b) => Ok(Cell::Bool(*b)),
        Json::Num(n) => Ok(Cell::Text(n.clone())),
        Json::Str(s) => Ok(Cell::Text(s.clone())),
        _ => Err(FetchError::Parse(format!(
            "path `{path}` is not a scalar"
        ))),
    }
}

struct P<'a> {
    b: &'a [u8],
    s: &'a str,
    i: usize,
}

pub(crate) fn parse(s: &str) -> Result<Json, FetchError> {
    let mut p = P { b: s.as_bytes(), s, i: 0 };
    p.ws();
    let v = p.value()?;
    p.ws();
    if p.i != p.b.len() {
        return Err(FetchError::Parse("trailing data after JSON".into()));
    }
    Ok(v)
}

impl<'a> P<'a> {
    fn ws(&mut self) {
        while self.i < self.b.len()
            && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r')
        {
            self.i += 1;
        }
    }
    fn byte(&self) -> Result<u8, FetchError> {
        self.b
            .get(self.i)
            .copied()
            .ok_or_else(|| FetchError::Parse("unexpected end of JSON".into()))
    }
    fn value(&mut self) -> Result<Json, FetchError> {
        self.ws();
        match self.byte()? {
            b'{' => self.object(),
            b'[' => self.array(),
            b'"' => Ok(Json::Str(self.string()?)),
            b't' => self.lit("true", Json::Bool(true)),
            b'f' => self.lit("false", Json::Bool(false)),
            b'n' => self.lit("null", Json::Null),
            _ => self.number(),
        }
    }
    fn lit(&mut self, kw: &str, j: Json) -> Result<Json, FetchError> {
        if self.b[self.i..].starts_with(kw.as_bytes()) {
            self.i += kw.len();
            Ok(j)
        } else {
            Err(FetchError::Parse(format!("expected `{kw}`")))
        }
    }
    fn number(&mut self) -> Result<Json, FetchError> {
        let start = self.i;
        while self.i < self.b.len()
            && matches!(self.b[self.i],
                b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
        {
            self.i += 1;
        }
        if self.i == start {
            return Err(FetchError::Parse("expected value".into()));
        }
        Ok(Json::Num(
            std::str::from_utf8(&self.b[start..self.i])
                .map_err(|_| FetchError::Parse("invalid number bytes".into()))?
                .to_string(),
        ))
    }
    fn string(&mut self) -> Result<String, FetchError> {
        // Slice-1 limitation: lone/paired UTF-16 surrogate escapes (\uD800-\uDFFF) are replaced with U+FFFD; astral pairs are not recombined.
        self.i += 1; // opening quote
        let mut s = String::new();
        loop {
            let c = self.byte()?;
            self.i += 1;
            match c {
                b'"' => return Ok(s),
                b'\\' => {
                    let e = self.byte()?;
                    self.i += 1;
                    match e {
                        b'"' => s.push('"'),
                        b'\\' => s.push('\\'),
                        b'/' => s.push('/'),
                        b'n' => s.push('\n'),
                        b't' => s.push('\t'),
                        b'r' => s.push('\r'),
                        b'b' => s.push('\u{0008}'),
                        b'f' => s.push('\u{000C}'),
                        b'u' => {
                            let hex = self
                                .b
                                .get(self.i..self.i + 4)
                                .ok_or_else(|| {
                                    FetchError::Parse("bad \\u escape".into())
                                })?;
                            let cp = u32::from_str_radix(
                                std::str::from_utf8(hex).map_err(|_| {
                                    FetchError::Parse("bad \\u".into())
                                })?,
                                16,
                            )
                            .map_err(|_| FetchError::Parse("bad \\u".into()))?;
                            self.i += 4;
                            s.push(
                                char::from_u32(cp).unwrap_or('\u{FFFD}'),
                            );
                        }
                        _ => {
                            return Err(FetchError::Parse(
                                "bad escape".into(),
                            ))
                        }
                    }
                }
                _ => {
                    self.i -= 1; // undo the byte() advance
                    let ch = self.s[self.i..]
                        .chars()
                        .next()
                        .ok_or_else(|| {
                            FetchError::Parse("invalid UTF-8 in string".into())
                        })?;
                    self.i += ch.len_utf8();
                    s.push(ch);
                }
            }
        }
    }
    fn array(&mut self) -> Result<Json, FetchError> {
        self.i += 1;
        let mut out = Vec::new();
        self.ws();
        if self.byte()? == b']' {
            self.i += 1;
            return Ok(Json::Array(out));
        }
        loop {
            out.push(self.value()?);
            self.ws();
            match self.byte()? {
                b',' => {
                    self.i += 1;
                }
                b']' => {
                    self.i += 1;
                    return Ok(Json::Array(out));
                }
                _ => return Err(FetchError::Parse("expected `,` or `]`".into())),
            }
        }
    }
    fn object(&mut self) -> Result<Json, FetchError> {
        self.i += 1;
        let mut out = Vec::new();
        self.ws();
        if self.byte()? == b'}' {
            self.i += 1;
            return Ok(Json::Object(out));
        }
        loop {
            self.ws();
            if self.byte()? != b'"' {
                return Err(FetchError::Parse("expected object key".into()));
            }
            let k = self.string()?;
            self.ws();
            if self.byte()? != b':' {
                return Err(FetchError::Parse("expected `:`".into()));
            }
            self.i += 1;
            let v = self.value()?;
            out.push((k, v));
            self.ws();
            match self.byte()? {
                b',' => {
                    self.i += 1;
                }
                b'}' => {
                    self.i += 1;
                    return Ok(Json::Object(out));
                }
                _ => return Err(FetchError::Parse("expected `,` or `}`".into())),
            }
        }
    }
}

/// Navigate `rows_path` (if any) to a JSON array, then extract `cols`
/// from each element (same scalar dotted-path rule as `extract`).
/// `None` rows_path == today's top-level-array behavior.
#[allow(dead_code)] // used by fetch_rows_paginated in the next task
pub(crate) fn rows_at(
    body: &[u8],
    cols: &[ColumnMap],
    rows_path: Option<&str>,
) -> Result<Vec<Vec<Cell>>, FetchError> {
    let rp = match rows_path {
        None => return extract(body, cols),
        Some(p) => p,
    };
    let v = parse(std::str::from_utf8(body).map_err(|_| {
        FetchError::Parse("body is not UTF-8".into())
    })?)?;
    let mut cur = &v;
    for seg in rp.split('.') {
        match cur {
            Json::Object(m) => {
                cur = m.iter().find(|(k, _)| k == seg).map(|(_, x)| x)
                    .ok_or_else(|| FetchError::Parse(format!(
                        "ROWS `{rp}`: no key `{seg}`")))?;
            }
            _ => return Err(FetchError::Parse(format!(
                "ROWS `{rp}`: `{seg}` is not an object"))),
        }
    }
    let arr = match cur {
        Json::Array(a) => a,
        _ => return Err(FetchError::Parse(format!(
            "ROWS `{rp}` is not an array"))),
    };
    let mut rows = Vec::with_capacity(arr.len());
    for el in arr {
        let mut row = Vec::with_capacity(cols.len());
        for c in cols { row.push(path_get(el, &c.source)?); }
        rows.push(row);
    }
    Ok(rows)
}

/// Read the scalar at `path`. `None` if absent / JSON null / empty
/// string (the pagination "stop" signals) or if the node is a
/// non-scalar (array/object). Numbers render as their source text.
#[allow(dead_code)] // used by fetch_rows_paginated in the next task
pub(crate) fn opt_string_at(
    body: &[u8],
    path: &str,
) -> Result<Option<String>, FetchError> {
    let v = parse(std::str::from_utf8(body).map_err(|_| {
        FetchError::Parse("body is not UTF-8".into())
    })?)?;
    let mut cur = &v;
    for seg in path.split('.') {
        match cur {
            Json::Object(m) => match m.iter().find(|(k, _)| k == seg) {
                Some((_, x)) => cur = x,
                None => return Ok(None), // absent => stop
            },
            _ => return Ok(None),
        }
    }
    Ok(match cur {
        Json::Null => None,
        Json::Str(s) if s.is_empty() => None,
        Json::Str(s) => Some(s.clone()),
        Json::Num(n) => Some(n.clone()),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_catalog::FieldKind;

    fn cm(name: &str, src: &str) -> ColumnMap {
        ColumnMap { name: name.into(), kind: FieldKind::U64, source: src.into() }
    }

    #[test]
    fn extracts_flat_and_nested_scalars() {
        let body = br#"[{"id":1,"u":{"name":"ann"}},{"id":2,"u":{"name":"bo"}}]"#;
        let cols = vec![cm("id", "id"), cm("nm", "u.name")];
        let rows = extract(body, &cols).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Cell::Text("1".into()), Cell::Text("ann".into())],
                vec![Cell::Text("2".into()), Cell::Text("bo".into())],
            ]
        );
    }

    #[test]
    fn null_and_bool_and_missing_path() {
        let body = br#"[{"a":null,"b":true}]"#;
        let cols = vec![cm("a", "a"), cm("b", "b")];
        let rows = extract(body, &cols).unwrap();
        assert_eq!(rows, vec![vec![Cell::Null, Cell::Bool(true)]]);
        let bad = vec![cm("x", "nope")];
        assert!(matches!(extract(body, &bad), Err(FetchError::Parse(_))));
    }

    #[test]
    fn rejects_non_array_top_level_and_bad_json() {
        assert!(matches!(extract(b"{}", &[]), Err(FetchError::Parse(_))));
        assert!(matches!(extract(b"[", &[]), Err(FetchError::Parse(_))));
    }

    #[test]
    fn handles_strings_with_escapes_and_numbers() {
        let body = br#"[{"s":"a\"b\n","n":-12.5}]"#;
        let cols = vec![cm("s", "s"), cm("n", "n")];
        let rows = extract(body, &cols).unwrap();
        assert_eq!(
            rows,
            vec![vec![Cell::Text("a\"b\n".into()), Cell::Text("-12.5".into())]]
        );
    }

    #[test]
    fn preserves_multibyte_utf8_in_strings() {
        let body = "[{\"s\":\"caf\u{00e9} \u{65e5}\u{672c} \u{1f600}\"}]".as_bytes();
        let cols = vec![cm("s", "s")];
        let rows = extract(body, &cols).unwrap();
        assert_eq!(rows, vec![vec![Cell::Text("caf\u{00e9} \u{65e5}\u{672c} \u{1f600}".into())]]);
    }

    #[test]
    fn rows_at_navigates_envelope_then_extracts() {
        let body = br#"{"data":{"items":[{"id":1},{"id":2}]},"p":{"next":"http://x/2"}}"#;
        let cols = vec![ColumnMap{name:"id".into(),kind:FieldKind::U64,source:"id".into()}];
        let rows = rows_at(body, &cols, Some("data.items")).unwrap();
        assert_eq!(rows, vec![vec![Cell::Text("1".into())], vec![Cell::Text("2".into())]]);
        // None rows_path == top-level array (delegates to extract())
        let arr = br#"[{"id":9}]"#;
        assert_eq!(rows_at(arr, &cols, None).unwrap(), vec![vec![Cell::Text("9".into())]]);
        // path that is not an array => Parse error
        assert!(matches!(rows_at(body, &cols, Some("p")), Err(FetchError::Parse(_))));
        // path with a missing key => Parse error
        assert!(matches!(rows_at(body, &cols, Some("data.nope")), Err(FetchError::Parse(_))));
    }

    #[test]
    fn opt_string_at_reads_cursor_or_none() {
        let b = br#"{"p":{"next":"http://x/2"},"empty":"","nul":null}"#;
        assert_eq!(opt_string_at(b, "p.next").unwrap(), Some("http://x/2".to_string()));
        assert_eq!(opt_string_at(b, "empty").unwrap(), None);   // empty string => stop
        assert_eq!(opt_string_at(b, "nul").unwrap(), None);     // null => stop
        assert_eq!(opt_string_at(b, "missing").unwrap(), None); // absent => stop
        assert_eq!(opt_string_at(b, "p.missing").unwrap(), None); // absent nested => stop
        // a number cursor renders as its source text
        let n = br#"{"c":42}"#;
        assert_eq!(opt_string_at(n, "c").unwrap(), Some("42".to_string()));
        // a non-scalar (array/object) at the path => None (stop, not error)
        let o = br#"{"c":[1,2]}"#;
        assert_eq!(opt_string_at(o, "c").unwrap(), None);
    }
}
