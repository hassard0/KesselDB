//! kessel-catalog: object-type schemas and their fixed-width memory layout.
//!
//! PURE: no I/O, clock, or RNG. Layout is a deterministic function of the
//! field list, identical on every replica and platform.
//!
//! Layout stability rule (enables cheap online DDL + up-projection):
//!   record = [schema_ver u32] [null bitmap: fixed 8 bytes] [field data...]
//! The null bitmap is a FIXED 8 bytes (=> max 64 fields), so appending a
//! nullable field never moves an existing field's offset. An old, shorter
//! record simply lacks the newer trailing fields, which decode as NULL.

#![forbid(unsafe_code)]

pub const MAX_FIELDS: usize = 64;
pub const NULL_BITMAP_BYTES: usize = 8;
pub const SCHEMA_VER_BYTES: usize = 4;
pub const FIELD_COUNT_BYTES: usize = 2;
/// `[schema_ver u32] [field_count u16] [null bitmap 8B]`. `field_count` makes
/// up-projection unambiguous even when a new field fits inside an old
/// record's power-of-two padding (it would otherwise read as zeros, not NULL).
pub const HEADER_BYTES: usize = SCHEMA_VER_BYTES + FIELD_COUNT_BYTES + NULL_BITMAP_BYTES; // 14

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldKind {
    U8,
    U16,
    U32,
    U64,
    U128,
    I8,
    I16,
    I32,
    I64,
    I128,
    Bool,
    Fixed { scale: u8 }, // decimal stored as i64 * 10^-scale
    Char(u16),           // fixed-length text, zero-padded
    Bytes(u16),          // fixed-length raw bytes
    Timestamp,           // u64 nanos
    Ref,                 // 16-byte ObjectId
    OverflowRef,         // RESERVED (var-length, Sub-project 2): 8-byte handle
}

impl FieldKind {
    pub fn width(&self) -> u16 {
        match self {
            FieldKind::U8 | FieldKind::I8 | FieldKind::Bool => 1,
            FieldKind::U16 | FieldKind::I16 => 2,
            FieldKind::U32 | FieldKind::I32 => 4,
            FieldKind::U64 | FieldKind::I64 | FieldKind::Timestamp => 8,
            FieldKind::Fixed { .. } => 8,
            FieldKind::U128 | FieldKind::I128 => 16,
            FieldKind::Ref => 16,
            FieldKind::OverflowRef => 8,
            FieldKind::Char(n) | FieldKind::Bytes(n) => *n,
        }
    }
    fn tag(&self) -> u8 {
        match self {
            FieldKind::U8 => 1,
            FieldKind::U16 => 2,
            FieldKind::U32 => 3,
            FieldKind::U64 => 4,
            FieldKind::U128 => 5,
            FieldKind::I8 => 6,
            FieldKind::I16 => 7,
            FieldKind::I32 => 8,
            FieldKind::I64 => 9,
            FieldKind::I128 => 10,
            FieldKind::Bool => 11,
            FieldKind::Fixed { .. } => 12,
            FieldKind::Char(_) => 13,
            FieldKind::Bytes(_) => 14,
            FieldKind::Timestamp => 15,
            FieldKind::Ref => 16,
            FieldKind::OverflowRef => 17,
        }
    }
    fn from_tag(tag: u8, arg: u16) -> Option<FieldKind> {
        Some(match tag {
            1 => FieldKind::U8,
            2 => FieldKind::U16,
            3 => FieldKind::U32,
            4 => FieldKind::U64,
            5 => FieldKind::U128,
            6 => FieldKind::I8,
            7 => FieldKind::I16,
            8 => FieldKind::I32,
            9 => FieldKind::I64,
            10 => FieldKind::I128,
            11 => FieldKind::Bool,
            12 => FieldKind::Fixed { scale: arg as u8 },
            13 => FieldKind::Char(arg),
            14 => FieldKind::Bytes(arg),
            15 => FieldKind::Timestamp,
            16 => FieldKind::Ref,
            17 => FieldKind::OverflowRef,
            _ => return None,
        })
    }
    fn arg(&self) -> u16 {
        match self {
            FieldKind::Fixed { scale } => *scale as u16,
            FieldKind::Char(n) | FieldKind::Bytes(n) => *n,
            _ => 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub field_id: u16,
    pub name: String,
    pub kind: FieldKind,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectType {
    pub type_id: u32,
    pub name: String,
    pub schema_ver: u32,
    pub fields: Vec<Field>,
    /// `field_id`s with an equality secondary index (Sub-project 3).
    pub indexes: Vec<u16>,
    /// `field_id`s with a UNIQUE constraint (Sub-project 4). Always a subset
    /// of `indexes` (UNIQUE implies an index).
    pub unique: Vec<u16>,
    /// Foreign keys: `(field_id, referenced_type_id, on_delete)` where
    /// `on_delete` is 0=NoAction (SP6: only checked on child write),
    /// 1=Restrict, 2=Cascade (SP11: enforced when a parent is deleted).
    pub fks: Vec<(u16, u32, u8)>,
    /// CHECK constraints (Sub-project 7): compiled kessel-expr programs that
    /// must evaluate true for every written row.
    pub checks: Vec<Vec<u8>>,
    /// Before-write triggers (Sub-project 8): kessel-expr programs run in
    /// order on each Create/Update; may mutate the record or reject it.
    pub triggers: Vec<Vec<u8>>,
    /// `field_id`s with an order-preserving range index (Sub-project 15),
    /// enabling sub-linear `FindRange`.
    pub ordered: Vec<u16>,
    /// Composite (multi-field) equality indexes (Sub-project 27): each entry
    /// is the ordered list of `field_id`s forming one composite index.
    pub composite: Vec<Vec<u16>>,
    /// Per-column defaults (SP86): `(field_id, raw fixed-width bytes)`.
    /// Applied at INSERT to omitted columns and by `ON DELETE SET
    /// DEFAULT`. Serialized in a backward-compatible trailer of the
    /// length-delimited type-def blob (old blobs simply have no
    /// trailer ⇒ no defaults).
    pub defaults: Vec<(u16, Vec<u8>)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Layout {
    pub record_size: usize,
    /// Byte offset of each field's data, parallel to `ObjectType.fields`.
    pub offsets: Vec<usize>,
}

fn next_pow2(n: usize) -> usize {
    let mut p = 16;
    while p < n {
        p <<= 1;
    }
    p
}

impl ObjectType {
    /// Build a minimal `ObjectType` from a decoded type definition
    /// (`decode_type_def`). Enough for `kessel_codec::decode` (which only
    /// needs `fields`/`compute_layout`); index/constraint metadata is left
    /// empty. Used by clients to decode `SELECT` rows from the wire schema.
    pub fn from_def(name: String, fields: Vec<Field>) -> Self {
        ObjectType {
            type_id: 0,
            name,
            schema_ver: 1,
            fields,
            indexes: vec![],
            unique: vec![],
            fks: vec![],
            checks: vec![],
            triggers: vec![],
            ordered: vec![],
            composite: vec![],
            defaults: vec![],
        }
    }

    /// Pure layout computation. Offsets of existing fields are invariant under
    /// appending new fields (fixed header + fixed null bitmap).
    pub fn compute_layout(&self) -> Layout {
        let mut offsets = Vec::with_capacity(self.fields.len());
        let mut cur = HEADER_BYTES;
        for f in &self.fields {
            offsets.push(cur);
            cur += f.kind.width() as usize;
        }
        Layout {
            record_size: next_pow2(cur),
            offsets,
        }
    }

    pub fn field_index(&self, field_id: u16) -> Option<usize> {
        self.fields.iter().position(|f| f.field_id == field_id)
    }
}

// ---- serialization (the opaque `def`/`field` payloads in proto Ops) --------

fn put_str(b: &mut Vec<u8>, s: &str) {
    b.extend_from_slice(&(s.len() as u16).to_le_bytes());
    b.extend_from_slice(s.as_bytes());
}
fn get_str(b: &[u8], p: &mut usize) -> Option<String> {
    let n = u16::from_le_bytes(b.get(*p..*p + 2)?.try_into().ok()?) as usize;
    *p += 2;
    let s = String::from_utf8_lossy(b.get(*p..*p + n)?).into_owned();
    *p += n;
    Some(s)
}

pub fn encode_field(f: &Field) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&f.field_id.to_le_bytes());
    b.push(f.nullable as u8);
    b.push(f.kind.tag());
    b.extend_from_slice(&f.kind.arg().to_le_bytes());
    put_str(&mut b, &f.name);
    b
}

pub fn decode_field(b: &[u8]) -> Option<Field> {
    let mut p = 0;
    let field_id = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?);
    p += 2;
    let nullable = *b.get(p)? != 0;
    p += 1;
    let tag = *b.get(p)?;
    p += 1;
    let arg = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?);
    p += 2;
    let kind = FieldKind::from_tag(tag, arg)?;
    let name = get_str(b, &mut p)?;
    Some(Field {
        field_id,
        name,
        kind,
        nullable,
    })
}

/// Encode a CreateType payload: just name + fields (the SM assigns
/// `type_id`/`schema_ver` deterministically).
pub fn encode_type_def(name: &str, fields: &[Field]) -> Vec<u8> {
    let mut b = Vec::new();
    put_str(&mut b, name);
    b.extend_from_slice(&(fields.len() as u16).to_le_bytes());
    for f in fields {
        let fb = encode_field(f);
        b.extend_from_slice(&(fb.len() as u16).to_le_bytes());
        b.extend_from_slice(&fb);
    }
    b
}

pub fn decode_type_def(b: &[u8]) -> Option<(String, Vec<Field>)> {
    let mut p = 0;
    let name = get_str(b, &mut p)?;
    let n = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?) as usize;
    p += 2;
    let mut fields = Vec::with_capacity(n);
    for _ in 0..n {
        let fl = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?) as usize;
        p += 2;
        fields.push(decode_field(b.get(p..p + fl)?)?);
        p += fl;
    }
    Some((name, fields))
}

/// Like [`encode_type_def`] but appends a backward-compatible defaults
/// trailer `[u16 ndef] ndef×([u16 fid][u16 len][bytes])` (only when
/// non-empty). Old decoders ignore trailing bytes, so a blob written
/// this way still decodes name+fields identically (SP86).
pub fn encode_type_def_with_defaults(
    name: &str,
    fields: &[Field],
    defaults: &[(u16, Vec<u8>)],
) -> Vec<u8> {
    let mut b = encode_type_def(name, fields);
    if !defaults.is_empty() {
        b.extend_from_slice(&(defaults.len() as u16).to_le_bytes());
        for (fid, raw) in defaults {
            b.extend_from_slice(&fid.to_le_bytes());
            b.extend_from_slice(&(raw.len() as u16).to_le_bytes());
            b.extend_from_slice(raw);
        }
    }
    b
}

/// Parse the SP86 defaults trailer from a type-def blob (the bytes
/// after `name` + the `n` length-delimited fields). Empty if there is
/// no trailer (an old blob, or no defaults) or on any short read —
/// defaults are a pure accelerator/convenience, never load-bearing.
pub fn decode_type_defaults(b: &[u8]) -> Vec<(u16, Vec<u8>)> {
    let parse = || -> Option<Vec<(u16, Vec<u8>)>> {
        let mut p = 0usize;
        let _ = get_str(b, &mut p)?; // name
        let n = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?) as usize;
        p += 2;
        for _ in 0..n {
            let fl =
                u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?) as usize;
            p += 2 + fl;
        }
        if p >= b.len() {
            return Some(Vec::new()); // no trailer (old blob / no defaults)
        }
        let nd = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?) as usize;
        p += 2;
        let mut out = Vec::with_capacity(nd);
        for _ in 0..nd {
            let fid = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?);
            p += 2;
            let l =
                u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?) as usize;
            p += 2;
            out.push((fid, b.get(p..p + l)?.to_vec()));
            p += l;
        }
        Some(out)
    };
    parse().unwrap_or_default()
}

/// The whole catalog, persisted by the state machine as object type 0.
#[derive(Clone, Debug, Default)]
pub struct Catalog {
    pub types: Vec<ObjectType>,
    pub next_type_id: u32,
}

impl Catalog {
    pub fn get(&self, type_id: u32) -> Option<&ObjectType> {
        self.types.iter().find(|t| t.type_id == type_id)
    }
    pub fn get_mut(&mut self, type_id: u32) -> Option<&mut ObjectType> {
        self.types.iter_mut().find(|t| t.type_id == type_id)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&self.next_type_id.to_le_bytes());
        b.extend_from_slice(&(self.types.len() as u32).to_le_bytes());
        for t in &self.types {
            b.extend_from_slice(&t.type_id.to_le_bytes());
            b.extend_from_slice(&t.schema_ver.to_le_bytes());
            let def = encode_type_def_with_defaults(
                &t.name,
                &t.fields,
                &t.defaults,
            );
            b.extend_from_slice(&(def.len() as u32).to_le_bytes());
            b.extend_from_slice(&def);
            b.extend_from_slice(&(t.indexes.len() as u16).to_le_bytes());
            for fid in &t.indexes {
                b.extend_from_slice(&fid.to_le_bytes());
            }
            b.extend_from_slice(&(t.unique.len() as u16).to_le_bytes());
            for fid in &t.unique {
                b.extend_from_slice(&fid.to_le_bytes());
            }
            b.extend_from_slice(&(t.fks.len() as u16).to_le_bytes());
            for (fid, rt, od) in &t.fks {
                b.extend_from_slice(&fid.to_le_bytes());
                b.extend_from_slice(&rt.to_le_bytes());
                b.push(*od);
            }
            b.extend_from_slice(&(t.checks.len() as u16).to_le_bytes());
            for prog in &t.checks {
                b.extend_from_slice(&(prog.len() as u32).to_le_bytes());
                b.extend_from_slice(prog);
            }
            b.extend_from_slice(&(t.triggers.len() as u16).to_le_bytes());
            for prog in &t.triggers {
                b.extend_from_slice(&(prog.len() as u32).to_le_bytes());
                b.extend_from_slice(prog);
            }
            b.extend_from_slice(&(t.ordered.len() as u16).to_le_bytes());
            for fid in &t.ordered {
                b.extend_from_slice(&fid.to_le_bytes());
            }
            b.extend_from_slice(&(t.composite.len() as u16).to_le_bytes());
            for ci in &t.composite {
                b.extend_from_slice(&(ci.len() as u16).to_le_bytes());
                for fid in ci {
                    b.extend_from_slice(&fid.to_le_bytes());
                }
            }
        }
        b
    }

    pub fn decode(b: &[u8]) -> Option<Catalog> {
        if b.len() < 8 {
            return Some(Catalog::default());
        }
        let next_type_id = u32::from_le_bytes(b[0..4].try_into().ok()?);
        let n = u32::from_le_bytes(b[4..8].try_into().ok()?) as usize;
        let mut p = 8;
        let mut types = Vec::with_capacity(n);
        for _ in 0..n {
            let type_id = u32::from_le_bytes(b.get(p..p + 4)?.try_into().ok()?);
            p += 4;
            let schema_ver = u32::from_le_bytes(b.get(p..p + 4)?.try_into().ok()?);
            p += 4;
            let dl = u32::from_le_bytes(b.get(p..p + 4)?.try_into().ok()?) as usize;
            p += 4;
            let def_slice = b.get(p..p + dl)?;
            let (name, fields) = decode_type_def(def_slice)?;
            let defaults = decode_type_defaults(def_slice);
            p += dl;
            let ni = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?) as usize;
            p += 2;
            let mut indexes = Vec::with_capacity(ni);
            for _ in 0..ni {
                indexes.push(u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?));
                p += 2;
            }
            let nu = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?) as usize;
            p += 2;
            let mut unique = Vec::with_capacity(nu);
            for _ in 0..nu {
                unique.push(u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?));
                p += 2;
            }
            let nf = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?) as usize;
            p += 2;
            let mut fks = Vec::with_capacity(nf);
            for _ in 0..nf {
                let fid = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?);
                p += 2;
                let rt = u32::from_le_bytes(b.get(p..p + 4)?.try_into().ok()?);
                p += 4;
                let od = *b.get(p)?;
                p += 1;
                fks.push((fid, rt, od));
            }
            let nc = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?) as usize;
            p += 2;
            let mut checks = Vec::with_capacity(nc);
            for _ in 0..nc {
                let cl = u32::from_le_bytes(b.get(p..p + 4)?.try_into().ok()?) as usize;
                p += 4;
                checks.push(b.get(p..p + cl)?.to_vec());
                p += cl;
            }
            let nt = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?) as usize;
            p += 2;
            let mut triggers = Vec::with_capacity(nt);
            for _ in 0..nt {
                let tl = u32::from_le_bytes(b.get(p..p + 4)?.try_into().ok()?) as usize;
                p += 4;
                triggers.push(b.get(p..p + tl)?.to_vec());
                p += tl;
            }
            let no = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?) as usize;
            p += 2;
            let mut ordered = Vec::with_capacity(no);
            for _ in 0..no {
                ordered.push(u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?));
                p += 2;
            }
            let ncomp = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?) as usize;
            p += 2;
            let mut composite = Vec::with_capacity(ncomp);
            for _ in 0..ncomp {
                let nf = u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?) as usize;
                p += 2;
                let mut ci = Vec::with_capacity(nf);
                for _ in 0..nf {
                    ci.push(u16::from_le_bytes(b.get(p..p + 2)?.try_into().ok()?));
                    p += 2;
                }
                composite.push(ci);
            }
            types.push(ObjectType {
                type_id,
                name,
                schema_ver,
                fields,
                indexes,
                unique,
                fks,
                checks,
                triggers,
                ordered,
                composite,
                defaults,
            });
        }
        Some(Catalog {
            types,
            next_type_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_fields() -> Vec<Field> {
        vec![
            Field { field_id: 1, name: "debit".into(), kind: FieldKind::U128, nullable: false },
            Field { field_id: 2, name: "credit".into(), kind: FieldKind::U128, nullable: false },
            Field { field_id: 3, name: "amount".into(), kind: FieldKind::U64, nullable: false },
            Field { field_id: 4, name: "code".into(), kind: FieldKind::U16, nullable: false },
        ]
    }

    #[test]
    fn layout_is_pure_and_deterministic() {
        let t = ObjectType { type_id: 1, name: "transfer".into(), schema_ver: 1, fields: sample_fields(), indexes: vec![], unique: vec![], fks: vec![], checks: vec![], triggers: vec![], ordered: vec![], composite: vec![], defaults: vec![] };
        let a = t.compute_layout();
        let b = t.compute_layout();
        assert_eq!(a, b);
        assert_eq!(a.offsets[0], HEADER_BYTES);
        assert_eq!(a.offsets[1], HEADER_BYTES + 16);
        assert_eq!(a.offsets[2], HEADER_BYTES + 32);
        assert_eq!(a.offsets[3], HEADER_BYTES + 40);
        // 12 + 16+16+8+2 = 54 -> next pow2 = 64
        assert_eq!(a.record_size, 64);
    }

    #[test]
    fn appending_nullable_field_keeps_existing_offsets() {
        let mut t = ObjectType { type_id: 1, name: "t".into(), schema_ver: 1, fields: sample_fields(), indexes: vec![], unique: vec![], fks: vec![], checks: vec![], triggers: vec![], ordered: vec![], composite: vec![], defaults: vec![] };
        let before = t.compute_layout();
        t.fields.push(Field { field_id: 5, name: "memo".into(), kind: FieldKind::Char(32), nullable: true });
        t.schema_ver += 1;
        let after = t.compute_layout();
        assert_eq!(&after.offsets[..4], &before.offsets[..4], "old fields must not move");
        assert!(after.record_size >= before.record_size);
    }

    #[test]
    fn type_def_roundtrip() {
        let fields = sample_fields();
        let enc = encode_type_def("transfer", &fields);
        let (name, dec) = decode_type_def(&enc).unwrap();
        assert_eq!(name, "transfer");
        assert_eq!(dec, fields);
    }

    #[test]
    fn catalog_roundtrip() {
        let mut c = Catalog::default();
        c.next_type_id = 3;
        c.types.push(ObjectType { type_id: 1, name: "a".into(), schema_ver: 2, fields: sample_fields(), indexes: vec![3], unique: vec![3], fks: vec![(3, 9, 2)], checks: vec![vec![1, 2, 3]], triggers: vec![vec![7, 7]], ordered: vec![2], composite: vec![vec![1, 2]], defaults: vec![(1, vec![9, 0, 0, 0])] });
        c.types.push(ObjectType { type_id: 2, name: "b".into(), schema_ver: 1, fields: vec![], indexes: vec![], unique: vec![], fks: vec![], checks: vec![], triggers: vec![], ordered: vec![], composite: vec![], defaults: vec![] });
        let enc = c.encode();
        let dec = Catalog::decode(&enc).unwrap();
        assert_eq!(dec.next_type_id, 3);
        assert_eq!(dec.types.len(), 2);
        assert_eq!(dec.types[0].name, "a");
        assert_eq!(dec.types[0].fields, sample_fields());
        assert_eq!(dec.types[0].indexes, vec![3], "indexes survive roundtrip");
        assert_eq!(dec.types[0].unique, vec![3], "unique survives roundtrip");
        assert_eq!(dec.types[0].fks, vec![(3, 9, 2)], "fks survive roundtrip");
        assert_eq!(dec.types[0].checks, vec![vec![1u8, 2, 3]], "checks survive roundtrip");
        assert_eq!(dec.types[0].triggers, vec![vec![7u8, 7]], "triggers survive roundtrip");
        assert_eq!(dec.types[0].ordered, vec![2], "ordered survives roundtrip");
        assert_eq!(dec.types[0].composite, vec![vec![1u16, 2]], "composite survives roundtrip");
        assert_eq!(dec.types[1].indexes, Vec::<u16>::new());
        assert_eq!(dec.types[1].unique, Vec::<u16>::new());
        assert_eq!(dec.types[1].fks, Vec::<(u16, u32, u8)>::new());
        assert_eq!(Catalog::decode(&[]).unwrap().types.len(), 0);
    }
}
