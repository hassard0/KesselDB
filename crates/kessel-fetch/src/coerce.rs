//! Cell -> declared FieldKind -> raw little-endian field bytes
//! (exactly what kessel-codec stores for that kind).
use crate::json::Cell;
use crate::FetchError;
use kessel_catalog::FieldKind;

pub fn to_field_bytes(
    kind: &FieldKind,
    cell: Cell,
) -> Result<Vec<u8>, FetchError> {
    use FieldKind::*;
    let txt = match (&cell, kind) {
        (Cell::Null, _) => {
            return Err(FetchError::Type(
                "null in a non-nullable external column".into(),
            ))
        }
        (Cell::Bool(b), Bool) => return Ok(vec![*b as u8]),
        (Cell::Bool(b), _) => (if *b { "1" } else { "0" }).to_string(),
        (Cell::Text(s), _) => s.clone(),
    };
    let int = |signed: bool, w: usize| -> Result<Vec<u8>, FetchError> {
        if signed {
            let n: i128 = txt.parse().map_err(|_| {
                FetchError::Type(format!("`{txt}` is not an integer"))
            })?;
            Ok(n.to_le_bytes()[..w].to_vec())
        } else {
            let n: u128 = txt.parse().map_err(|_| {
                FetchError::Type(format!("`{txt}` is not an unsigned integer"))
            })?;
            Ok(n.to_le_bytes()[..w].to_vec())
        }
    };
    match kind {
        U8 => int(false, 1),
        U16 => int(false, 2),
        U32 => int(false, 4),
        U64 => int(false, 8),
        U128 => int(false, 16),
        I8 => int(true, 1),
        I16 => int(true, 2),
        I32 => int(true, 4),
        I64 => int(true, 8),
        I128 => int(true, 16),
        Bool => Ok(vec![
            if txt == "1" || txt.eq_ignore_ascii_case("true") { 1 } else { 0 },
        ]),
        Timestamp => int(false, 8),
        Char(w) | Bytes(w) => {
            let raw = txt.as_bytes();
            let w = *w as usize;
            if raw.len() > w {
                return Err(FetchError::Type(format!(
                    "value of {} bytes exceeds CHAR/BYTES({w})",
                    raw.len()
                )));
            }
            let mut out = vec![0u8; w];
            out[..raw.len()].copy_from_slice(raw);
            Ok(out)
        }
        other => Err(FetchError::Type(format!(
            "external column kind {other:?} unsupported in slice 1"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_little_endian_by_width() {
        assert_eq!(
            to_field_bytes(&FieldKind::U32, Cell::Text("258".into())).unwrap(),
            vec![2, 1, 0, 0]
        );
        assert_eq!(
            to_field_bytes(&FieldKind::I64, Cell::Text("-1".into())).unwrap(),
            vec![0xFF; 8]
        );
        assert_eq!(
            to_field_bytes(&FieldKind::U128, Cell::Text("1".into()))
                .unwrap()
                .len(),
            16
        );
    }

    #[test]
    fn bool_and_char_and_null_and_bad() {
        assert_eq!(
            to_field_bytes(&FieldKind::Bool, Cell::Bool(true)).unwrap(),
            vec![1]
        );
        assert_eq!(
            to_field_bytes(&FieldKind::Char(4), Cell::Text("hi".into())).unwrap(),
            vec![b'h', b'i', 0, 0]
        );
        assert!(matches!(
            to_field_bytes(&FieldKind::U32, Cell::Null),
            Err(FetchError::Type(_))
        ));
        assert!(matches!(
            to_field_bytes(&FieldKind::U32, Cell::Text("abc".into())),
            Err(FetchError::Type(_))
        ));
        assert!(matches!(
            to_field_bytes(&FieldKind::Char(1), Cell::Text("toolong".into())),
            Err(FetchError::Type(_))
        ));
    }
}
