//! PLAIN-encoding page decode (Apache Parquet "Data Pages"/PLAIN).
//! INT32/INT64 LE; FLOAT/DOUBLE IEEE-754 LE; BOOLEAN bit-packed
//! LSB-first; BYTE_ARRAY = [u32 LE len][bytes]. Bounds-checked.
#![allow(dead_code)]

use crate::meta::Type;
use crate::{PqError, PqValue};

fn bad(s: &str) -> PqError {
    PqError::Bad(s.to_string())
}

pub fn decode_plain(
    data: &[u8],
    ptype: Type,
    count: usize,
) -> Result<Vec<PqValue>, PqError> {
    let mut out = Vec::with_capacity(count);
    match ptype {
        Type::Int32 => {
            let need = count.checked_mul(4).ok_or_else(|| bad("int32 ovf"))?;
            let s = data.get(..need).ok_or_else(|| bad("int32 truncated"))?;
            for ch in s.chunks_exact(4) {
                out.push(PqValue::I64(
                    i32::from_le_bytes(ch.try_into().unwrap()) as i64,
                ));
            }
        }
        Type::Int64 => {
            let need = count.checked_mul(8).ok_or_else(|| bad("int64 ovf"))?;
            let s = data.get(..need).ok_or_else(|| bad("int64 truncated"))?;
            for ch in s.chunks_exact(8) {
                out.push(PqValue::I64(i64::from_le_bytes(
                    ch.try_into().unwrap(),
                )));
            }
        }
        Type::Float => {
            let need = count.checked_mul(4).ok_or_else(|| bad("f32 ovf"))?;
            let s = data.get(..need).ok_or_else(|| bad("f32 truncated"))?;
            for ch in s.chunks_exact(4) {
                out.push(PqValue::F64(
                    f32::from_le_bytes(ch.try_into().unwrap()) as f64,
                ));
            }
        }
        Type::Double => {
            let need = count.checked_mul(8).ok_or_else(|| bad("f64 ovf"))?;
            let s = data.get(..need).ok_or_else(|| bad("f64 truncated"))?;
            for ch in s.chunks_exact(8) {
                out.push(PqValue::F64(f64::from_le_bytes(
                    ch.try_into().unwrap(),
                )));
            }
        }
        Type::Boolean => {
            let need = count.div_ceil(8);
            let s = data.get(..need).ok_or_else(|| bad("bool truncated"))?;
            for i in 0..count {
                let byte = s[i / 8];
                out.push(PqValue::Bool((byte >> (i % 8)) & 1 == 1));
            }
        }
        Type::ByteArray => {
            let mut p = 0usize;
            for _ in 0..count {
                let lb = data
                    .get(p..p + 4)
                    .ok_or_else(|| bad("byte_array len truncated"))?;
                let len = u32::from_le_bytes(lb.try_into().unwrap())
                    as usize;
                p += 4;
                let v = data
                    .get(p..p.checked_add(len)
                        .ok_or_else(|| bad("byte_array len ovf"))?)
                    .ok_or_else(|| bad("byte_array data truncated"))?;
                out.push(PqValue::Bytes(v.to_vec()));
                p += len;
            }
        }
        other => {
            return Err(PqError::Unsupported(format!(
                "physical type {other:?} (OBJ-2c)"
            )))
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::Type;
    use crate::PqValue;

    #[test]
    fn plain_decode_spec_kat() {
        // INT64: values 7, -2  -> 7i64 LE, (-2)i64 LE
        let mut b = Vec::new();
        b.extend_from_slice(&7i64.to_le_bytes());
        b.extend_from_slice(&(-2i64).to_le_bytes());
        assert_eq!(
            decode_plain(&b, Type::Int64, 2).unwrap(),
            vec![PqValue::I64(7), PqValue::I64(-2)]
        );
        // INT32: 1, 1000
        let mut c = Vec::new();
        c.extend_from_slice(&1i32.to_le_bytes());
        c.extend_from_slice(&1000i32.to_le_bytes());
        assert_eq!(
            decode_plain(&c, Type::Int32, 2).unwrap(),
            vec![PqValue::I64(1), PqValue::I64(1000)]
        );
        // DOUBLE: 1.5, -0.25 (exact in f64)
        let mut d = Vec::new();
        d.extend_from_slice(&1.5f64.to_le_bytes());
        d.extend_from_slice(&(-0.25f64).to_le_bytes());
        assert_eq!(
            decode_plain(&d, Type::Double, 2).unwrap(),
            vec![PqValue::F64(1.5), PqValue::F64(-0.25)]
        );
        // FLOAT: 2.0
        let e = 2.0f32.to_le_bytes().to_vec();
        assert_eq!(
            decode_plain(&e, Type::Float, 1).unwrap(),
            vec![PqValue::F64(2.0)]
        );
        // BOOLEAN bit-packed LSB-first: true,false,true -> 0b00000101
        assert_eq!(
            decode_plain(&[0b0000_0101], Type::Boolean, 3).unwrap(),
            vec![PqValue::Bool(true), PqValue::Bool(false), PqValue::Bool(true)]
        );
        // BYTE_ARRAY: "hi","x" -> [2,0,0,0]"hi"[1,0,0,0]"x"
        let mut g = Vec::new();
        g.extend_from_slice(&2u32.to_le_bytes()); g.extend_from_slice(b"hi");
        g.extend_from_slice(&1u32.to_le_bytes()); g.extend_from_slice(b"x");
        assert_eq!(
            decode_plain(&g, Type::ByteArray, 2).unwrap(),
            vec![PqValue::Bytes(b"hi".to_vec()), PqValue::Bytes(b"x".to_vec())]
        );
    }

    #[test]
    fn plain_truncated_is_typed_error() {
        assert!(decode_plain(&[0u8; 3], Type::Int64, 1).is_err());
        let mut g = Vec::new();
        g.extend_from_slice(&99u32.to_le_bytes()); // lying length
        g.extend_from_slice(b"hi");
        assert!(decode_plain(&g, Type::ByteArray, 1).is_err());
    }
}
