//! Minimal Thrift Compact Protocol reader — only the subset Parquet
//! `FileMetaData` uses. Spec: Apache Thrift "compact protocol".
//! Every read is bounds-checked; malformed input ⇒ `Err`, no panic.
#![allow(dead_code)] // consumed by meta.rs / lib.rs

pub type TResult<T> = Result<T, crate::PqError>;

fn err(s: &str) -> crate::PqError {
    crate::PqError::Bad(s.to_string())
}

/// Compact field types (Thrift compact spec).
pub mod ctype {
    pub const BOOL_TRUE: u8 = 1;
    pub const BOOL_FALSE: u8 = 2;
    pub const I8: u8 = 3;
    pub const I16: u8 = 4;
    pub const I32: u8 = 5;
    pub const I64: u8 = 6;
    pub const DOUBLE: u8 = 7;
    pub const BINARY: u8 = 8;
    pub const LIST: u8 = 9;
    pub const SET: u8 = 10;
    pub const MAP: u8 = 11;
    pub const STRUCT: u8 = 12;
}

pub fn zigzag_decode(u: u64) -> i64 {
    ((u >> 1) as i64) ^ -((u & 1) as i64)
}

pub struct Reader<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> Reader<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Reader { b, p: 0 }
    }
    pub fn pos(&self) -> usize {
        self.p
    }
    pub fn at_end(&self) -> bool {
        self.p >= self.b.len()
    }
    pub fn byte(&mut self) -> TResult<u8> {
        let v = *self.b.get(self.p).ok_or_else(|| err("eof: byte"))?;
        self.p += 1;
        Ok(v)
    }
    pub fn uvarint(&mut self) -> TResult<u64> {
        let mut shift = 0u32;
        let mut out = 0u64;
        loop {
            let b = self.byte()?;
            if shift >= 64 {
                return Err(err("varint overflow"));
            }
            out |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(out);
            }
            shift += 7;
        }
    }
    pub fn ivarint(&mut self) -> TResult<i64> {
        Ok(zigzag_decode(self.uvarint()?))
    }
    pub fn take(&mut self, n: usize) -> TResult<&'a [u8]> {
        let end = self.p.checked_add(n).ok_or_else(|| err("len overflow"))?;
        let s = self.b.get(self.p..end).ok_or_else(|| err("eof: take"))?;
        self.p = end;
        Ok(s)
    }
}

#[derive(Clone, Debug)]
pub struct Field {
    pub id: i16,
    pub ctype: u8,
    /// For BOOL_TRUE/BOOL_FALSE the value is in the header itself.
    pub bool_val: Option<bool>,
}

/// Reads one Thrift-compact struct (delta-encoded field headers).
pub struct StructReader<'a> {
    r: Reader<'a>,
    last_id: i16,
}

impl<'a> StructReader<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        StructReader { r: Reader::new(b), last_id: 0 }
    }
    pub fn reader(&mut self) -> &mut Reader<'a> {
        &mut self.r
    }
    pub fn reader_pos(&self) -> usize {
        self.r.pos()
    }
    /// Reset the delta-ID accumulator to 0. Call this before reading
    /// each nested struct that is decoded inline via the same
    /// StructReader (Thrift compact protocol: each struct's field-ID
    /// deltas are independent of the enclosing struct's counter).
    pub fn reset_last_id(&mut self) {
        self.last_id = 0;
    }
    /// Save the current last-field-ID so the caller can restore it
    /// after processing an inline nested struct.
    pub fn save_last_id(&self) -> i16 {
        self.last_id
    }
    /// Restore the last-field-ID saved before entering an inline
    /// nested struct (so the outer struct's delta chain resumes).
    pub fn restore_last_id(&mut self, saved: i16) {
        self.last_id = saved;
    }
    /// `Ok(None)` = STOP field (end of struct).
    pub fn next_field(&mut self) -> TResult<Option<Field>> {
        let h = self.r.byte()?;
        if h == 0 {
            return Ok(None);
        }
        let ctype = h & 0x0f;
        let delta = (h >> 4) & 0x0f;
        let id = if delta == 0 {
            let z = self.r.ivarint()?;
            i16::try_from(z).map_err(|_| err("field id range"))?
        } else {
            self.last_id
                .checked_add(delta as i16)
                .ok_or_else(|| err("field id overflow"))?
        };
        self.last_id = id;
        let bool_val = match ctype {
            ctype::BOOL_TRUE => Some(true),
            ctype::BOOL_FALSE => Some(false),
            _ => None,
        };
        Ok(Some(Field { id, ctype, bool_val }))
    }
    pub fn read_i32(&mut self, f: &Field) -> TResult<i32> {
        if f.ctype != ctype::I32 && f.ctype != ctype::I8
            && f.ctype != ctype::I16
        {
            return Err(err("expected i32"));
        }
        i32::try_from(self.r.ivarint()?).map_err(|_| err("i32 range"))
    }
    pub fn read_i64(&mut self, f: &Field) -> TResult<i64> {
        if f.ctype != ctype::I64 {
            return Err(err("expected i64"));
        }
        self.r.ivarint()
    }
    pub fn read_bool(&mut self, f: &Field) -> TResult<bool> {
        f.bool_val.ok_or_else(|| err("expected bool"))
    }
    pub fn read_binary(&mut self, f: &Field) -> TResult<&'a [u8]> {
        if f.ctype != ctype::BINARY {
            return Err(err("expected binary"));
        }
        let n = usize::try_from(self.r.uvarint()?)
            .map_err(|_| err("binary len range"))?;
        self.r.take(n)
    }
    /// List header: returns (element_ctype, count). Spec: size byte
    /// `(size<<4)|etype`; if size==15 a uvarint count follows.
    pub fn list_header(&mut self) -> TResult<(u8, usize)> {
        let h = self.r.byte()?;
        let etype = h & 0x0f;
        let mut size = (h >> 4) as usize;
        if size == 15 {
            size = usize::try_from(self.r.uvarint()?)
                .map_err(|_| err("list size range"))?;
        }
        Ok((etype, size))
    }
    /// Skip one field's value of the given ctype. Recursion is
    /// depth-capped (attacker-facing: a crafted deeply-nested struct
    /// must not smash the stack — real Parquet metadata never nests
    /// anywhere near this).
    pub fn skip(&mut self, ctype: u8) -> TResult<()> {
        self.skip_depth(ctype, 0)
    }

    fn skip_depth(&mut self, ctype: u8, depth: u32) -> TResult<()> {
        if depth > MAX_SKIP_DEPTH {
            return Err(err("skip: nesting too deep"));
        }
        match ctype {
            ctype::BOOL_TRUE | ctype::BOOL_FALSE => {}
            ctype::I8 | ctype::I16 | ctype::I32 | ctype::I64 => {
                self.r.uvarint()?;
            }
            ctype::DOUBLE => {
                self.r.take(8)?;
            }
            ctype::BINARY => {
                let n = usize::try_from(self.r.uvarint()?)
                    .map_err(|_| err("skip bin len"))?;
                self.r.take(n)?;
            }
            ctype::LIST | ctype::SET => {
                let (et, count) = self.list_header()?;
                for _ in 0..count {
                    self.skip_depth(et, depth + 1)?;
                }
            }
            ctype::STRUCT => {
                // A skipped struct has its OWN delta-encoded field-id
                // sequence (Thrift compact resets last-field-id per
                // struct). Restore the outer struct's last_id afterward
                // so the caller's next delta decodes against the correct
                // base — otherwise a crafted skipped struct can shift the
                // outer field-id and silently mis-decode a known field.
                let saved = self.last_id;
                while let Some(f) = self.next_field()? {
                    let ct = f.ctype;
                    self.skip_depth(ct, depth + 1)?;
                }
                self.last_id = saved;
            }
            // MAP (ctype 11) intentionally not implemented: not used by
            // the OBJ-2a Parquet metadata schema subset. Returns a typed
            // error (never a panic) if ever encountered.
            _ => return Err(err("skip: unsupported/unknown ctype")),
        }
        Ok(())
    }
}

const MAX_SKIP_DEPTH: u32 = 64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_and_zigzag_spec_kat() {
        for (n, bytes) in [
            (0u64, &[0x00u8][..]),
            (1, &[0x01]),
            (127, &[0x7f]),
            (128, &[0x80, 0x01]),
            (300, &[0xac, 0x02]),
        ] {
            let mut c = Reader::new(bytes);
            assert_eq!(c.uvarint().unwrap(), n, "uvarint {n}");
            assert!(c.at_end());
        }
        for (v, z) in [
            (0i64, 0u64),
            (-1, 1),
            (1, 2),
            (-2, 3),
            (2, 4),
            (2147483647, 4294967294),
        ] {
            assert_eq!(zigzag_decode(z), v, "zigzag {z}->{v}");
        }
    }

    #[test]
    fn struct_field_header_and_types_spec_kat() {
        // { f1 i32=7, f2 i64=-2, f3 binary="hi", STOP } delta-encoded:
        // 0x15=(d1,i32) zz(7)=14=0x0e ; 0x16=(d1,i64) zz(-2)=3=0x03 ;
        // 0x18=(d1,binary) len2 "hi" ; 0x00 STOP
        let bytes = [0x15, 0x0e, 0x16, 0x03, 0x18, 0x02, b'h', b'i', 0x00];
        let mut s = StructReader::new(&bytes);
        let f1 = s.next_field().unwrap().unwrap();
        assert_eq!(f1.id, 1);
        assert_eq!(s.read_i32(&f1).unwrap(), 7);
        let f2 = s.next_field().unwrap().unwrap();
        assert_eq!(f2.id, 2);
        assert_eq!(s.read_i64(&f2).unwrap(), -2);
        let f3 = s.next_field().unwrap().unwrap();
        assert_eq!(f3.id, 3);
        assert_eq!(s.read_binary(&f3).unwrap(), b"hi");
        assert!(s.next_field().unwrap().is_none());
    }

    #[test]
    fn truncated_input_is_typed_error_not_panic() {
        let mut c = Reader::new(&[0x80]);
        assert!(c.uvarint().is_err());
        let mut s = StructReader::new(&[0x18, 0x05]);
        let f = s.next_field().unwrap().unwrap();
        assert!(s.read_binary(&f).is_err());
    }

    #[test]
    fn skip_struct_does_not_corrupt_outer_last_id() {
        // Outer struct: f1 i32=11, f2 STRUCT{ f50 i32=99 }, f3 i32=33, STOP.
        // f2's inner field-id 50 must NOT leak into the outer chain;
        // f3 (delta 1 from f2's id 2) must decode as id 3 -> value 33.
        let mut b = Vec::new();
        // f1: (delta1<<4)|I32(5)=0x15 ; zz(11)=22 -> uvarint 0x16
        b.push(0x15); b.push(0x16);
        // f2: (delta1<<4)|STRUCT(12)=0x1c
        b.push(0x1c);
        //   inner f50: long form (delta 0) -> 0x0|I32(5)=0x05 ; zz(50)=100 -> 0x64 ; value zz(99)=198 -> 0xc6 0x01
        b.push(0x05); b.push(0x64); b.push(0xc6); b.push(0x01);
        b.push(0x00); // inner STOP
        // f3: (delta1<<4)|I32(5)=0x15 ; zz(33)=66 -> 0x42
        b.push(0x15); b.push(0x42);
        b.push(0x00); // outer STOP
        let mut s = StructReader::new(&b);
        let f1 = s.next_field().unwrap().unwrap();
        assert_eq!(f1.id, 1);
        assert_eq!(s.read_i32(&f1).unwrap(), 11);
        let f2 = s.next_field().unwrap().unwrap();
        assert_eq!(f2.id, 2);
        assert_eq!(f2.ctype, ctype::STRUCT);
        s.skip(f2.ctype).unwrap(); // skip the unknown struct
        let f3 = s.next_field().unwrap().unwrap();
        assert_eq!(f3.id, 3, "outer last_id must be restored after skip");
        assert_eq!(s.read_i32(&f3).unwrap(), 33);
        assert!(s.next_field().unwrap().is_none());
    }

    #[test]
    fn deeply_nested_skip_is_typed_error_not_stack_overflow() {
        // 200 nested STRUCT headers (each byte 0x1c = delta1|STRUCT),
        // then STOPs. ~200 bytes, but nesting depth 200 > the cap.
        // Must return Err (typed), never recurse to a stack smash.
        let mut bytes = vec![0x1cu8; 200];
        bytes.extend(std::iter::repeat(0x00u8).take(200));
        let mut s = StructReader::new(&bytes);
        // The outermost field is a STRUCT (0x1c): next_field then
        // skip its value -> recursion. Drive it via skip directly:
        let f = s.next_field().unwrap().unwrap();
        let ct = f.ctype;
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || s.skip(ct),
        ));
        assert!(r.is_ok(), "must not panic/stack-overflow");
        assert!(r.unwrap().is_err(), "deep nesting must be a typed Err");
    }
}
