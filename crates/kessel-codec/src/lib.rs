//! kessel-codec: encode/decode a fixed-width record from an `ObjectType`.
//!
//! PURE: no I/O, clock, or RNG. Record layout:
//!   [schema_ver u32 LE] [field_count u16 LE] [null bitmap 8B] [field data...]
//! Up-projection: decoding a record written under an older schema yields NULL
//! for any field beyond its `field_count` (so added nullable fields read NULL
//! on old rows without rewriting them).

#![forbid(unsafe_code)]

use kessel_catalog::{FieldKind, ObjectType, HEADER_BYTES, NULL_BITMAP_BYTES, SCHEMA_VER_BYTES};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Null,
    /// Unsigned ints, Bool (0/1), Timestamp.
    Uint(u128),
    /// Signed ints, Fixed (raw scaled integer).
    Int(i128),
    /// Char/Bytes/Ref/OverflowRef — exactly `width` bytes after encode.
    Blob(Vec<u8>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum CodecError {
    ArityMismatch { expected: usize, got: usize },
    NullInNonNullable(usize),
    BlobTooLong { field: usize, max: usize, got: usize },
    ShortRecord,
    BadSchemaTag,
}

fn fc_off() -> usize {
    SCHEMA_VER_BYTES
}
fn bitmap_off() -> usize {
    SCHEMA_VER_BYTES + 2
}

fn set_null(bitmap: &mut [u8], i: usize) {
    bitmap[i / 8] |= 1 << (i % 8);
}
fn is_null(bitmap: &[u8], i: usize) -> bool {
    bitmap.get(i / 8).map(|b| b & (1 << (i % 8)) != 0).unwrap_or(false)
}

/// Encode `values` (parallel to `ot.fields`) into a fixed-size record.
pub fn encode(ot: &ObjectType, values: &[Value]) -> Result<Vec<u8>, CodecError> {
    if values.len() != ot.fields.len() {
        return Err(CodecError::ArityMismatch {
            expected: ot.fields.len(),
            got: values.len(),
        });
    }
    let layout = ot.compute_layout();
    let mut buf = vec![0u8; layout.record_size];
    buf[0..4].copy_from_slice(&ot.schema_ver.to_le_bytes());
    buf[fc_off()..fc_off() + 2].copy_from_slice(&(ot.fields.len() as u16).to_le_bytes());

    let mut bitmap = [0u8; NULL_BITMAP_BYTES];
    for (i, (field, val)) in ot.fields.iter().zip(values).enumerate() {
        let off = layout.offsets[i];
        let w = field.kind.width() as usize;
        match val {
            Value::Null => {
                if !field.nullable {
                    return Err(CodecError::NullInNonNullable(i));
                }
                set_null(&mut bitmap, i);
            }
            Value::Uint(u) => {
                let le = u.to_le_bytes();
                buf[off..off + w].copy_from_slice(&le[..w]);
            }
            Value::Int(v) => {
                let le = v.to_le_bytes();
                buf[off..off + w].copy_from_slice(&le[..w]);
            }
            Value::Blob(b) => {
                if b.len() > w {
                    return Err(CodecError::BlobTooLong {
                        field: i,
                        max: w,
                        got: b.len(),
                    });
                }
                buf[off..off + b.len()].copy_from_slice(b);
            }
        }
    }
    buf[bitmap_off()..bitmap_off() + NULL_BITMAP_BYTES].copy_from_slice(&bitmap);
    Ok(buf)
}

/// Decode a record into values parallel to the CURRENT `ot.fields`. Fields
/// beyond the record's stored `field_count` (older schema) decode as NULL.
pub fn decode(ot: &ObjectType, rec: &[u8]) -> Result<Vec<Value>, CodecError> {
    if rec.len() < HEADER_BYTES {
        return Err(CodecError::ShortRecord);
    }
    let stored_fc =
        u16::from_le_bytes(rec[fc_off()..fc_off() + 2].try_into().unwrap()) as usize;
    let bitmap = &rec[bitmap_off()..bitmap_off() + NULL_BITMAP_BYTES];
    let layout = ot.compute_layout();
    let mut out = Vec::with_capacity(ot.fields.len());
    for (i, field) in ot.fields.iter().enumerate() {
        if i >= stored_fc {
            out.push(Value::Null); // up-projected (didn't exist when written)
            continue;
        }
        if is_null(bitmap, i) {
            out.push(Value::Null);
            continue;
        }
        let off = layout.offsets[i];
        let w = field.kind.width() as usize;
        if off + w > rec.len() {
            return Err(CodecError::ShortRecord);
        }
        let raw = &rec[off..off + w];
        let v = match field.kind {
            FieldKind::I8
            | FieldKind::I16
            | FieldKind::I32
            | FieldKind::I64
            | FieldKind::I128
            | FieldKind::Fixed { .. } => {
                let mut le = [0u8; 16];
                le[..w].copy_from_slice(raw);
                // sign-extend
                if w < 16 && raw[w - 1] & 0x80 != 0 {
                    for b in le.iter_mut().skip(w) {
                        *b = 0xFF;
                    }
                }
                Value::Int(i128::from_le_bytes(le))
            }
            FieldKind::Char(_)
            | FieldKind::Bytes(_)
            | FieldKind::Ref
            | FieldKind::OverflowRef => Value::Blob(raw.to_vec()),
            _ => {
                let mut le = [0u8; 16];
                le[..w].copy_from_slice(raw);
                Value::Uint(u128::from_le_bytes(le))
            }
        };
        out.push(v);
    }
    Ok(out)
}

/// The `schema_ver` a record was written under (for diagnostics/migration).
pub fn record_schema_ver(rec: &[u8]) -> Option<u32> {
    rec.get(0..4).map(|b| u32::from_le_bytes(b.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_catalog::Field;
    use kessel_proto::Rng;

    fn ot(fields: Vec<Field>, ver: u32) -> ObjectType {
        ObjectType {
            type_id: 1,
            name: "t".into(),
            schema_ver: ver,
            fields,
            indexes: vec![],
        }
    }

    #[test]
    fn roundtrip_mixed_kinds() {
        let t = ot(
            vec![
                Field { field_id: 1, name: "a".into(), kind: FieldKind::U128, nullable: false },
                Field { field_id: 2, name: "b".into(), kind: FieldKind::I32, nullable: false },
                Field { field_id: 3, name: "c".into(), kind: FieldKind::Char(8), nullable: true },
                Field { field_id: 4, name: "d".into(), kind: FieldKind::Bool, nullable: false },
            ],
            1,
        );
        let vals = vec![
            Value::Uint(340282366920938463463374607431768211455),
            Value::Int(-12345),
            Value::Blob(b"hi".to_vec()),
            Value::Uint(1),
        ];
        let enc = encode(&t, &vals).unwrap();
        let dec = decode(&t, &enc).unwrap();
        assert_eq!(dec[0], Value::Uint(u128::MAX));
        assert_eq!(dec[1], Value::Int(-12345));
        assert_eq!(dec[3], Value::Uint(1));
        if let Value::Blob(b) = &dec[2] {
            assert_eq!(&b[..2], b"hi");
            assert_eq!(b.len(), 8, "char field is fixed width");
        } else {
            panic!("expected blob");
        }
    }

    #[test]
    fn null_in_nonnullable_rejected() {
        let t = ot(
            vec![Field { field_id: 1, name: "a".into(), kind: FieldKind::U32, nullable: false }],
            1,
        );
        assert_eq!(encode(&t, &[Value::Null]), Err(CodecError::NullInNonNullable(0)));
    }

    #[test]
    fn up_projection_added_nullable_field_reads_null() {
        let v1 = ot(
            vec![Field { field_id: 1, name: "a".into(), kind: FieldKind::U64, nullable: false }],
            1,
        );
        let rec = encode(&v1, &[Value::Uint(42)]).unwrap();
        // schema evolves: append a nullable field
        let mut v2 = v1.clone();
        v2.fields.push(Field { field_id: 2, name: "b".into(), kind: FieldKind::U32, nullable: true });
        v2.schema_ver = 2;
        let dec = decode(&v2, &rec).unwrap();
        assert_eq!(dec[0], Value::Uint(42));
        assert_eq!(dec[1], Value::Null, "old record lacks the new field");
        assert_eq!(record_schema_ver(&rec), Some(1));
    }

    #[test]
    fn property_roundtrip_random_schemas() {
        let mut rng = Rng::new(0xABCDEF);
        for _ in 0..300 {
            let nf = 1 + rng.below(10) as usize;
            let mut fields = Vec::new();
            for fid in 0..nf {
                let kind = match rng.below(6) {
                    0 => FieldKind::U64,
                    1 => FieldKind::I32,
                    2 => FieldKind::U128,
                    3 => FieldKind::Char(16),
                    4 => FieldKind::Bool,
                    _ => FieldKind::Timestamp,
                };
                fields.push(Field {
                    field_id: fid as u16,
                    name: format!("f{fid}"),
                    kind,
                    nullable: true,
                });
            }
            let t = ot(fields.clone(), 1);
            let vals: Vec<Value> = fields
                .iter()
                .map(|f| match f.kind {
                    FieldKind::I32 => Value::Int(-(rng.below(1000) as i128)),
                    FieldKind::Char(_) => Value::Blob(vec![7u8; rng.below(16) as usize]),
                    FieldKind::Bool => Value::Uint(rng.below(2) as u128),
                    // U64/Timestamp are 8 bytes; U128 is 16 — a u64 fits both.
                    _ => Value::Uint(rng.next_u64() as u128),
                })
                .collect();
            let enc = encode(&t, &vals).unwrap();
            let dec = decode(&t, &enc).unwrap();
            for (i, f) in fields.iter().enumerate() {
                match (&vals[i], &dec[i], f.kind) {
                    (Value::Blob(a), Value::Blob(b), FieldKind::Char(w)) => {
                        assert_eq!(&b[..a.len()], &a[..]);
                        assert_eq!(b.len(), w as usize);
                    }
                    (a, b, _) => assert_eq!(a, b, "field {i}"),
                }
            }
        }
    }
}
