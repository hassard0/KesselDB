//! kessel-proto: wire/log types, little-endian codec primitives, and a
//! deterministic PRNG. Dependency-free on purpose — determinism is a feature.

#![forbid(unsafe_code)]

pub type TypeId = u32;
pub type OpNumber = u64;
pub type ClientId = u128;

/// 128-bit caller-supplied object identifier. The engine never generates ids
/// (that would introduce nondeterminism into the state machine).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct ObjectId(pub [u8; 16]);

impl ObjectId {
    pub fn from_u128(v: u128) -> Self {
        ObjectId(v.to_le_bytes())
    }
    pub fn as_u128(&self) -> u128 {
        u128::from_le_bytes(self.0)
    }
}

/// One query predicate (Sub-project 5). `op`: 0 = Eq, 1 = Ge (>=),
/// 2 = Le (<=). `value` is the field value (width-normalized by the engine).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pred {
    pub field_id: u16,
    pub op: u8,
    pub value: Vec<u8>,
}

/// Operations applied by the deterministic state machine. Payloads are opaque
/// bytes here so `kessel-proto` stays schema-agnostic; `kessel-catalog` /
/// `kessel-codec` give them meaning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    CreateType { def: Vec<u8> },
    AlterTypeAddField { type_id: TypeId, field: Vec<u8> },
    Create { type_id: TypeId, id: ObjectId, record: Vec<u8> },
    Update { type_id: TypeId, id: ObjectId, record: Vec<u8> },
    Delete { type_id: TypeId, id: ObjectId },
    GetById { type_id: TypeId, id: ObjectId },
    /// Read a variable-length overflow blob by its deterministic handle
    /// (Sub-project 2). Write side rides inside `Create`/`Update` records.
    GetBlob { handle: u64 },
    /// Declare an equality secondary index on a field; backfills existing
    /// rows deterministically (Sub-project 3).
    CreateIndex { type_id: TypeId, field_id: u16 },
    /// Equality lookup: returns concatenated 16-byte object ids of every row
    /// whose indexed field equals `value` (Sub-project 3).
    FindBy { type_id: TypeId, field_id: u16, value: Vec<u8> },
    /// Add a UNIQUE constraint on a field (Sub-project 4): ensures/creates an
    /// index, validates current data, then enforces on future writes.
    AddUnique { type_id: TypeId, field_id: u16 },
    /// Conjunctive query (Sub-project 5): returns concatenated 16-byte object
    /// ids of rows matching ALL predicates. The planner intersects indexed
    /// equality predicates and filter-scans the rest.
    Query { type_id: TypeId, preds: Vec<Pred> },
    /// Add a foreign-key constraint (Sub-project 6): `field_id`'s value
    /// (padded to 16 bytes) must be an existing object id of
    /// `ref_type_id`. Validates current data before enabling.
    AddForeignKey { type_id: TypeId, field_id: u16, ref_type_id: TypeId },
    /// Add a CHECK constraint (Sub-project 7): a compiled kessel-expr program
    /// that must evaluate true for every written row. Validates current data.
    AddCheck { type_id: TypeId, program: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpResult {
    Ok,
    Got(Vec<u8>),
    Exists,
    NotFound,
    TypeCreated(TypeId),
    SchemaError(String),
    /// A built-in constraint (NOT NULL / UNIQUE) rejected the write
    /// (Sub-project 4). Deterministic — counts as a committed op result.
    Constraint(String),
}

impl Op {
    /// Discriminant tag used in WAL frames and the wire protocol.
    pub fn kind(&self) -> u8 {
        match self {
            Op::CreateType { .. } => 1,
            Op::AlterTypeAddField { .. } => 2,
            Op::Create { .. } => 3,
            Op::Update { .. } => 4,
            Op::Delete { .. } => 5,
            Op::GetById { .. } => 6,
            Op::GetBlob { .. } => 7,
            Op::CreateIndex { .. } => 8,
            Op::FindBy { .. } => 9,
            Op::AddUnique { .. } => 10,
            Op::Query { .. } => 11,
            Op::AddForeignKey { .. } => 12,
            Op::AddCheck { .. } => 13,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        b.push(self.kind());
        match self {
            Op::CreateType { def } => codec::put_bytes(&mut b, def),
            Op::AlterTypeAddField { type_id, field } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_bytes(&mut b, field);
            }
            Op::Create { type_id, id, record }
            | Op::Update { type_id, id, record } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&id.0);
                codec::put_bytes(&mut b, record);
            }
            Op::Delete { type_id, id } | Op::GetById { type_id, id } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&id.0);
            }
            Op::GetBlob { handle } => codec::put_u64(&mut b, *handle),
            Op::CreateIndex { type_id, field_id } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&field_id.to_le_bytes());
            }
            Op::FindBy { type_id, field_id, value } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&field_id.to_le_bytes());
                codec::put_bytes(&mut b, value);
            }
            Op::AddUnique { type_id, field_id } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&field_id.to_le_bytes());
            }
            Op::Query { type_id, preds } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_u32(&mut b, preds.len() as u32);
                for p in preds {
                    b.extend_from_slice(&p.field_id.to_le_bytes());
                    b.push(p.op);
                    codec::put_bytes(&mut b, &p.value);
                }
            }
            Op::AddForeignKey { type_id, field_id, ref_type_id } => {
                codec::put_u32(&mut b, *type_id);
                b.extend_from_slice(&field_id.to_le_bytes());
                codec::put_u32(&mut b, *ref_type_id);
            }
            Op::AddCheck { type_id, program } => {
                codec::put_u32(&mut b, *type_id);
                codec::put_bytes(&mut b, program);
            }
        }
        b
    }

    pub fn decode(buf: &[u8]) -> Option<Op> {
        let mut c = codec::Cursor::new(buf);
        let kind = c.u8()?;
        let op = match kind {
            1 => Op::CreateType { def: c.bytes()? },
            2 => Op::AlterTypeAddField { type_id: c.u32()?, field: c.bytes()? },
            3 => Op::Create { type_id: c.u32()?, id: c.object_id()?, record: c.bytes()? },
            4 => Op::Update { type_id: c.u32()?, id: c.object_id()?, record: c.bytes()? },
            5 => Op::Delete { type_id: c.u32()?, id: c.object_id()? },
            6 => Op::GetById { type_id: c.u32()?, id: c.object_id()? },
            7 => Op::GetBlob { handle: c.u64()? },
            8 => Op::CreateIndex { type_id: c.u32()?, field_id: c.u16()? },
            9 => Op::FindBy { type_id: c.u32()?, field_id: c.u16()?, value: c.bytes()? },
            10 => Op::AddUnique { type_id: c.u32()?, field_id: c.u16()? },
            11 => {
                let type_id = c.u32()?;
                let n = c.u32()? as usize;
                let mut preds = Vec::with_capacity(n);
                for _ in 0..n {
                    preds.push(Pred {
                        field_id: c.u16()?,
                        op: c.u8()?,
                        value: c.bytes()?,
                    });
                }
                Op::Query { type_id, preds }
            }
            12 => Op::AddForeignKey {
                type_id: c.u32()?,
                field_id: c.u16()?,
                ref_type_id: c.u32()?,
            },
            13 => Op::AddCheck { type_id: c.u32()?, program: c.bytes()? },
            _ => return None,
        };
        Some(op)
    }
}

/// Little-endian primitives, length-prefixed byte fields, CRC-32C (Castagnoli).
pub mod codec {
    use crate::ObjectId;

    pub fn put_u8(b: &mut Vec<u8>, v: u8) {
        b.push(v);
    }
    pub fn put_u32(b: &mut Vec<u8>, v: u32) {
        b.extend_from_slice(&v.to_le_bytes());
    }
    pub fn put_u64(b: &mut Vec<u8>, v: u64) {
        b.extend_from_slice(&v.to_le_bytes());
    }
    pub fn put_bytes(b: &mut Vec<u8>, v: &[u8]) {
        put_u32(b, v.len() as u32);
        b.extend_from_slice(v);
    }

    pub struct Cursor<'a> {
        buf: &'a [u8],
        pos: usize,
    }

    impl<'a> Cursor<'a> {
        pub fn new(buf: &'a [u8]) -> Self {
            Cursor { buf, pos: 0 }
        }
        pub fn u8(&mut self) -> Option<u8> {
            let v = *self.buf.get(self.pos)?;
            self.pos += 1;
            Some(v)
        }
        pub fn u16(&mut self) -> Option<u16> {
            let s = self.buf.get(self.pos..self.pos + 2)?;
            self.pos += 2;
            Some(u16::from_le_bytes(s.try_into().ok()?))
        }
        pub fn u32(&mut self) -> Option<u32> {
            let s = self.buf.get(self.pos..self.pos + 4)?;
            self.pos += 4;
            Some(u32::from_le_bytes(s.try_into().ok()?))
        }
        pub fn u64(&mut self) -> Option<u64> {
            let s = self.buf.get(self.pos..self.pos + 8)?;
            self.pos += 8;
            Some(u64::from_le_bytes(s.try_into().ok()?))
        }
        pub fn object_id(&mut self) -> Option<ObjectId> {
            let s = self.buf.get(self.pos..self.pos + 16)?;
            self.pos += 16;
            Some(ObjectId(s.try_into().ok()?))
        }
        pub fn bytes(&mut self) -> Option<Vec<u8>> {
            let n = self.u32()? as usize;
            let s = self.buf.get(self.pos..self.pos + n)?;
            self.pos += n;
            Some(s.to_vec())
        }
        pub fn remaining(&self) -> usize {
            self.buf.len() - self.pos
        }
    }

    const CRC32C_POLY: u32 = 0x82F6_3B78;

    /// CRC-32C (Castagnoli). Software table-free implementation — slow but
    /// dependency-free and bit-identical everywhere (determinism > speed for
    /// the integrity check; hot paths can swap a table later).
    pub fn crc32c(data: &[u8]) -> u32 {
        let mut crc: u32 = 0xFFFF_FFFF;
        for &byte in data {
            crc ^= byte as u32;
            for _ in 0..8 {
                let mask = (crc & 1).wrapping_neg();
                crc = (crc >> 1) ^ (CRC32C_POLY & mask);
            }
        }
        !crc
    }
}

/// Deterministic splitmix64 PRNG. Used by tests and the simulator so a single
/// `u64` seed reproduces an entire run bit-for-bit.
#[derive(Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// Uniform in `[0, n)`. `n == 0` returns 0.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next_u64() % n
        }
    }
    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let r = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&r[..chunk.len()]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_roundtrip_all_variants() {
        let id = ObjectId::from_u128(0xDEAD_BEEF_CAFE);
        let ops = vec![
            Op::CreateType { def: vec![1, 2, 3] },
            Op::AlterTypeAddField { type_id: 7, field: vec![9, 9] },
            Op::Create { type_id: 4, id, record: vec![0xAA; 130] },
            Op::Update { type_id: 4, id, record: vec![] },
            Op::Delete { type_id: 4, id },
            Op::GetById { type_id: 4, id },
            Op::GetBlob { handle: 0xABCD_1234_5678 },
            Op::CreateIndex { type_id: 4, field_id: 2 },
            Op::FindBy { type_id: 4, field_id: 2, value: vec![1, 2, 3, 4] },
            Op::AddUnique { type_id: 4, field_id: 2 },
            Op::Query {
                type_id: 4,
                preds: vec![
                    Pred { field_id: 1, op: 0, value: vec![9, 9] },
                    Pred { field_id: 2, op: 1, value: vec![] },
                ],
            },
            Op::AddForeignKey { type_id: 4, field_id: 1, ref_type_id: 2 },
            Op::AddCheck { type_id: 4, program: vec![0, 1, 2, 3] },
        ];
        for op in ops {
            let enc = op.encode();
            let dec = Op::decode(&enc).expect("decode");
            assert_eq!(op, dec);
            assert_eq!(op.kind(), enc[0]);
        }
    }

    #[test]
    fn object_id_u128_roundtrip() {
        for v in [0u128, 1, u128::MAX, 0x1234_5678_9ABC] {
            assert_eq!(ObjectId::from_u128(v).as_u128(), v);
        }
    }

    #[test]
    fn crc32c_known_vectors() {
        // CRC-32C check value for ASCII "123456789" is 0xE3069283.
        assert_eq!(codec::crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(codec::crc32c(b""), 0x0000_0000);
        // Bit-flip changes the CRC.
        assert_ne!(codec::crc32c(b"abc"), codec::crc32c(b"abd"));
    }

    #[test]
    fn rng_is_deterministic_per_seed() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        let mut c = Rng::new(43);
        let sa: Vec<u64> = (0..16).map(|_| a.next_u64()).collect();
        let sb: Vec<u64> = (0..16).map(|_| b.next_u64()).collect();
        let sc: Vec<u64> = (0..16).map(|_| c.next_u64()).collect();
        assert_eq!(sa, sb, "same seed must reproduce");
        assert_ne!(sa, sc, "different seed must diverge");
    }

    #[test]
    fn decode_rejects_truncated() {
        assert!(Op::decode(&[3, 4, 0, 0]).is_none());
        assert!(Op::decode(&[]).is_none());
        assert!(Op::decode(&[99]).is_none());
    }
}
