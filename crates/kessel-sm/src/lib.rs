//! kessel-sm: the deterministic state machine.
//!
//! `apply(op_number, op) -> OpResult` is a pure function of (committed state,
//! op). NO clock, NO RNG: ids/field-ids/schema-versions derive only from
//! catalog state, timestamps arrive inside the op (from the VSR primary).
//! The only side effect is the injected `Storage`, which is itself
//! deterministic. This is what lets the simulator replay a run bit-for-bit.

#![forbid(unsafe_code)]

use kessel_catalog::{decode_field, decode_type_def, Catalog, Field, ObjectType};
use kessel_io::Vfs;
use kessel_proto::{Op, OpResult};
use kessel_storage::{make_key, Key, Storage};

/// Catalog persisted as object type 0, single well-known key.
fn catalog_key() -> Key {
    make_key(0, &[0u8; 16])
}

/// Reserved keyspace for variable-length overflow blobs (Sub-project 2).
const OVERFLOW_TYPE: u32 = 0xFFFF_FFFF;

fn handle_key(handle: u64) -> Key {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&handle.to_le_bytes());
    make_key(OVERFLOW_TYPE, &id)
}

/// Build a `Create`/`Update` record with an overflow trailer:
/// `[fixed][u16 n]( [u16 field_idx][u32 len][bytes] )*`. `fixed` must be the
/// codec-encoded fixed-width record (handles will be patched in by the SM).
pub fn encode_overflow_record(fixed: &[u8], entries: &[(u16, Vec<u8>)]) -> Vec<u8> {
    let mut b = fixed.to_vec();
    b.extend_from_slice(&(entries.len() as u16).to_le_bytes());
    for (idx, bytes) in entries {
        b.extend_from_slice(&idx.to_le_bytes());
        b.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        b.extend_from_slice(bytes);
    }
    b
}

pub struct StateMachine<V: Vfs> {
    catalog: Catalog,
    storage: Storage<V>,
    /// Optional read cache. NEVER consulted to compute committed state or the
    /// digest — only to short-circuit `GetById`. Off => zero core-path effect.
    cache: Option<kessel_cache::ReadCache>,
}

impl<V: Vfs> StateMachine<V> {
    pub fn open(vfs: V) -> std::io::Result<Self> {
        let storage = Storage::open(vfs)?;
        let catalog = storage
            .get(&catalog_key())
            .and_then(|b| Catalog::decode(&b))
            .unwrap_or_default();
        let mut catalog = catalog;
        if catalog.next_type_id == 0 {
            catalog.next_type_id = 1; // 0 reserved for the catalog itself
        }
        Ok(StateMachine {
            catalog,
            storage,
            cache: None,
        })
    }

    /// Enable a bounded read cache (M4). Purely a `GetById` accelerator.
    pub fn with_cache(mut self, capacity: usize) -> Self {
        self.cache = Some(kessel_cache::ReadCache::new(capacity));
        self
    }

    pub fn cache_hit_rate(&self) -> Option<f64> {
        self.cache.as_ref().map(|c| c.hit_rate())
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    fn persist_catalog(&mut self, op_number: u64) -> OpResult {
        let bytes = self.catalog.encode();
        match self.storage.put(op_number, catalog_key(), bytes) {
            Ok(()) => OpResult::Ok,
            Err(e) => OpResult::SchemaError(format!("catalog persist: {e}")),
        }
    }

    /// Split an optional overflow trailer off `record`, persist each blob
    /// under a deterministic op-derived handle, and patch the handle into the
    /// fixed record's `OverflowRef` slot. Returns the now-truly-fixed record.
    /// Deterministic: handle = (op_number << 20) | field_idx — identical on
    /// every replica because `op_number` is replicated.
    fn materialize_overflow(
        &mut self,
        op_number: u64,
        type_id: u32,
        record: Vec<u8>,
    ) -> Result<Vec<u8>, OpResult> {
        let layout = match self.catalog.get(type_id) {
            Some(t) => t.compute_layout(),
            None => return Err(OpResult::SchemaError(format!("no type {type_id}"))),
        };
        let fixed_size = layout.record_size;
        if record.len() <= fixed_size {
            return Ok(record); // no trailer (back-compatible with SP1 records)
        }
        let mut fixed = record[..fixed_size].to_vec();
        let tr = &record[fixed_size..];
        if tr.len() < 2 {
            return Err(OpResult::SchemaError("bad overflow trailer".into()));
        }
        let n = u16::from_le_bytes([tr[0], tr[1]]) as usize;
        let mut p = 2usize;
        for _ in 0..n {
            if p + 6 > tr.len() {
                return Err(OpResult::SchemaError("truncated overflow entry".into()));
            }
            let field_idx = u16::from_le_bytes([tr[p], tr[p + 1]]) as usize;
            let len = u32::from_le_bytes([tr[p + 2], tr[p + 3], tr[p + 4], tr[p + 5]]) as usize;
            p += 6;
            if p + len > tr.len() {
                return Err(OpResult::SchemaError("overflow blob overruns".into()));
            }
            let blob = tr[p..p + len].to_vec();
            p += len;
            let off = match layout.offsets.get(field_idx) {
                Some(&o) if o + 8 <= fixed_size => o,
                _ => return Err(OpResult::SchemaError("bad overflow field_idx".into())),
            };
            let handle: u64 = (op_number << 20) | (field_idx as u64);
            if let Err(e) = self.storage.put(op_number, handle_key(handle), blob) {
                return Err(OpResult::SchemaError(format!("overflow store: {e}")));
            }
            fixed[off..off + 8].copy_from_slice(&handle.to_le_bytes());
        }
        Ok(fixed)
    }

    /// Apply one committed op. Deterministic.
    pub fn apply(&mut self, op_number: u64, op: Op) -> OpResult {
        match op {
            Op::CreateType { def } => {
                let (name, raw_fields) = match decode_type_def(&def) {
                    Some(x) => x,
                    None => return OpResult::SchemaError("bad type def".into()),
                };
                if raw_fields.len() > kessel_catalog::MAX_FIELDS {
                    return OpResult::SchemaError("too many fields".into());
                }
                if self.catalog.types.iter().any(|t| t.name == name) {
                    return OpResult::SchemaError(format!("type '{name}' exists"));
                }
                let type_id = self.catalog.next_type_id;
                // Deterministically (re)assign field ids 1..=n.
                let fields: Vec<Field> = raw_fields
                    .into_iter()
                    .enumerate()
                    .map(|(i, f)| Field {
                        field_id: (i + 1) as u16,
                        ..f
                    })
                    .collect();
                self.catalog.types.push(ObjectType {
                    type_id,
                    name,
                    schema_ver: 1,
                    fields,
                });
                self.catalog.next_type_id += 1;
                match self.persist_catalog(op_number) {
                    OpResult::Ok => OpResult::TypeCreated(type_id),
                    e => e,
                }
            }

            Op::AlterTypeAddField { type_id, field } => {
                let new_field = match decode_field(&field) {
                    Some(f) => f,
                    None => return OpResult::SchemaError("bad field".into()),
                };
                if !new_field.nullable {
                    return OpResult::SchemaError(
                        "Sub-project 1: added fields must be nullable".into(),
                    );
                }
                let next_fid = match self.catalog.get(type_id) {
                    Some(t) => t.fields.iter().map(|f| f.field_id).max().unwrap_or(0) + 1,
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                if let Some(t) = self.catalog.get_mut(type_id) {
                    if t.fields.len() + 1 > kessel_catalog::MAX_FIELDS {
                        return OpResult::SchemaError("too many fields".into());
                    }
                    t.fields.push(Field {
                        field_id: next_fid,
                        ..new_field
                    });
                    t.schema_ver += 1;
                }
                self.persist_catalog(op_number)
            }

            Op::Create { type_id, id, record } => {
                if self.catalog.get(type_id).is_none() {
                    return OpResult::SchemaError(format!("no type {type_id}"));
                }
                let key = make_key(type_id, &id.0);
                if self.storage.get(&key).is_some() {
                    return OpResult::Exists;
                }
                let record = match self.materialize_overflow(op_number, type_id, record) {
                    Ok(r) => r,
                    Err(e) => return e,
                };
                let cached = self.cache.as_mut().map(|_| record.clone());
                match self.storage.put(op_number, key, record) {
                    Ok(()) => {
                        if let (Some(c), Some(v)) = (self.cache.as_mut(), cached) {
                            c.insert(key, v);
                        }
                        OpResult::Ok
                    }
                    Err(e) => OpResult::SchemaError(format!("store: {e}")),
                }
            }

            Op::Update { type_id, id, record } => {
                if self.catalog.get(type_id).is_none() {
                    return OpResult::SchemaError(format!("no type {type_id}"));
                }
                let key = make_key(type_id, &id.0);
                if self.storage.get(&key).is_none() {
                    return OpResult::NotFound;
                }
                let record = match self.materialize_overflow(op_number, type_id, record) {
                    Ok(r) => r,
                    Err(e) => return e,
                };
                let cached = self.cache.as_mut().map(|_| record.clone());
                match self.storage.put(op_number, key, record) {
                    Ok(()) => {
                        if let Some(c) = self.cache.as_mut() {
                            match cached {
                                Some(v) => c.insert(key, v),
                                None => c.invalidate(&key),
                            }
                        }
                        OpResult::Ok
                    }
                    Err(e) => OpResult::SchemaError(format!("store: {e}")),
                }
            }

            Op::Delete { type_id, id } => {
                let key = make_key(type_id, &id.0);
                if self.storage.get(&key).is_none() {
                    return OpResult::NotFound;
                }
                match self.storage.delete(op_number, key) {
                    Ok(()) => {
                        if let Some(c) = self.cache.as_mut() {
                            c.invalidate(&key); // never serve a deleted row
                        }
                        OpResult::Ok
                    }
                    Err(e) => OpResult::SchemaError(format!("store: {e}")),
                }
            }

            Op::GetById { type_id, id } => {
                let key = make_key(type_id, &id.0);
                if let Some(c) = self.cache.as_mut() {
                    if let Some(v) = c.get(&key) {
                        return OpResult::Got(v);
                    }
                }
                match self.storage.get(&key) {
                    Some(b) => {
                        if let Some(c) = self.cache.as_mut() {
                            c.insert(key, b.clone());
                        }
                        OpResult::Got(b)
                    }
                    None => OpResult::NotFound,
                }
            }

            Op::GetBlob { handle } => match self.storage.get(&handle_key(handle)) {
                Some(b) => OpResult::Got(b),
                None => OpResult::NotFound,
            },
        }
    }

    /// Apply a batch of committed ops with a SINGLE fsync at the end
    /// (group commit). The batch is durable only once this returns Ok.
    /// Mirrors how a VSR primary will hand a committed batch to the SM.
    pub fn apply_batch(&mut self, ops: Vec<(u64, Op)>) -> std::io::Result<Vec<OpResult>> {
        self.storage.set_autosync(false);
        let mut out = Vec::with_capacity(ops.len());
        for (n, op) in ops {
            out.push(self.apply(n, op));
        }
        self.storage.sync()?;
        self.storage.set_autosync(true);
        Ok(out)
    }

    /// Deterministic digest of the whole replicated state (data + catalog).
    /// Two replicas that have applied the same committed prefix MUST match.
    pub fn digest(&self) -> u32 {
        self.storage.digest()
    }

    /// Flush the underlying storage memtable (used at checkpoints/benchmarks).
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.storage.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_catalog::{encode_field, encode_type_def, Field, FieldKind, ObjectType};
    use kessel_io::MemVfs;
    use kessel_proto::{ObjectId, Rng};
    use std::collections::HashMap;

    fn transfer_def() -> Vec<u8> {
        encode_type_def(
            "transfer",
            &[
                Field { field_id: 0, name: "debit".into(), kind: FieldKind::U128, nullable: false },
                Field { field_id: 0, name: "amount".into(), kind: FieldKind::U64, nullable: false },
            ],
        )
    }

    #[test]
    fn create_type_assigns_deterministic_ids() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        assert_eq!(sm.apply(1, Op::CreateType { def: transfer_def() }), OpResult::TypeCreated(1));
        assert_eq!(sm.apply(2, Op::CreateType { def: encode_type_def("account", &[]) }), OpResult::TypeCreated(2));
        let t = sm.catalog().get(1).unwrap();
        assert_eq!(t.fields[0].field_id, 1);
        assert_eq!(t.fields[1].field_id, 2);
        // duplicate name rejected
        assert!(matches!(
            sm.apply(3, Op::CreateType { def: transfer_def() }),
            OpResult::SchemaError(_)
        ));
    }

    #[test]
    fn crud_lifecycle_and_error_results_are_deterministic() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: transfer_def() });
        let id = ObjectId::from_u128(7);
        assert_eq!(sm.apply(2, Op::Create { type_id: 1, id, record: vec![1, 2, 3] }), OpResult::Ok);
        assert_eq!(sm.apply(3, Op::Create { type_id: 1, id, record: vec![9] }), OpResult::Exists);
        assert_eq!(sm.apply(4, Op::GetById { type_id: 1, id }), OpResult::Got(vec![1, 2, 3]));
        assert_eq!(sm.apply(5, Op::Update { type_id: 1, id, record: vec![4, 5] }), OpResult::Ok);
        assert_eq!(sm.apply(6, Op::GetById { type_id: 1, id }), OpResult::Got(vec![4, 5]));
        assert_eq!(sm.apply(7, Op::Update { type_id: 1, id: ObjectId::from_u128(99), record: vec![] }), OpResult::NotFound);
        assert_eq!(sm.apply(8, Op::Delete { type_id: 1, id }), OpResult::Ok);
        assert_eq!(sm.apply(9, Op::Delete { type_id: 1, id }), OpResult::NotFound);
        assert_eq!(sm.apply(10, Op::GetById { type_id: 1, id }), OpResult::NotFound);
        // unknown type
        assert!(matches!(
            sm.apply(11, Op::Create { type_id: 42, id, record: vec![] }),
            OpResult::SchemaError(_)
        ));
    }

    #[test]
    fn online_ddl_add_field_must_be_nullable() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: transfer_def() });
        let bad = encode_field(&Field { field_id: 0, name: "x".into(), kind: FieldKind::U32, nullable: false });
        assert!(matches!(
            sm.apply(2, Op::AlterTypeAddField { type_id: 1, field: bad }),
            OpResult::SchemaError(_)
        ));
        let good = encode_field(&Field { field_id: 0, name: "memo".into(), kind: FieldKind::Char(16), nullable: true });
        assert_eq!(sm.apply(3, Op::AlterTypeAddField { type_id: 1, field: good }), OpResult::Ok);
        let t = sm.catalog().get(1).unwrap();
        assert_eq!(t.schema_ver, 2);
        assert_eq!(t.fields.len(), 3);
        assert_eq!(t.fields[2].field_id, 3);
    }

    #[test]
    fn recovery_reloads_catalog_and_data() {
        let vfs = MemVfs::new();
        {
            let mut sm = StateMachine::open(vfs.clone()).unwrap();
            sm.apply(1, Op::CreateType { def: transfer_def() });
            sm.apply(2, Op::Create { type_id: 1, id: ObjectId::from_u128(5), record: vec![0xAA] });
            sm.flush().unwrap();
        }
        let mut sm = StateMachine::open(vfs).unwrap();
        assert!(sm.catalog().get(1).is_some(), "catalog survived restart");
        assert_eq!(
            sm.apply(3, Op::GetById { type_id: 1, id: ObjectId::from_u128(5) }),
            OpResult::Got(vec![0xAA])
        );
    }

    #[test]
    fn apply_batch_group_commit_is_durable() {
        let vfs = MemVfs::new();
        {
            let mut sm = StateMachine::open(vfs.clone()).unwrap();
            sm.apply(1, Op::CreateType { def: transfer_def() });
            let ops: Vec<(u64, Op)> = (0..100u64)
                .map(|i| {
                    (
                        10 + i,
                        Op::Create {
                            type_id: 1,
                            id: ObjectId::from_u128(i as u128),
                            record: vec![i as u8],
                        },
                    )
                })
                .collect();
            let res = sm.apply_batch(ops).unwrap();
            assert_eq!(res.len(), 100);
            assert!(res.iter().all(|r| *r == OpResult::Ok));
            sm.flush().unwrap();
        }
        let mut sm = StateMachine::open(vfs).unwrap();
        assert_eq!(
            sm.apply(999, Op::GetById { type_id: 1, id: ObjectId::from_u128(73) }),
            OpResult::Got(vec![73])
        );
    }

    #[test]
    fn cache_on_equals_cache_off() {
        // The read cache must be observationally invisible: identical op
        // results and identical state digest with cache on vs off.
        let run = |cache: bool| {
            let mut sm = if cache {
                StateMachine::open(MemVfs::new()).unwrap().with_cache(256)
            } else {
                StateMachine::open(MemVfs::new()).unwrap()
            };
            sm.apply(1, Op::CreateType { def: transfer_def() });
            let mut rng = Rng::new(0xBEEF);
            let mut results = Vec::new();
            for op in 2..3000u64 {
                let id = ObjectId::from_u128(rng.below(40) as u128);
                let o = match rng.below(5) {
                    0 => Op::Create { type_id: 1, id, record: vec![(op & 0xFF) as u8; 4] },
                    1 => Op::Update { type_id: 1, id, record: vec![0x77; 2] },
                    2 => Op::Delete { type_id: 1, id },
                    _ => Op::GetById { type_id: 1, id },
                };
                results.push(sm.apply(op, o));
            }
            (results, sm.digest())
        };
        let (r_off, d_off) = run(false);
        let (r_on, d_on) = run(true);
        assert_eq!(r_off, r_on, "cache changed observable op results");
        assert_eq!(d_off, d_on, "cache changed the state digest");
    }

    fn overflow_type_def() -> Vec<u8> {
        encode_type_def(
            "blobby",
            &[
                Field { field_id: 0, name: "body".into(), kind: FieldKind::OverflowRef, nullable: false },
                Field { field_id: 0, name: "n".into(), kind: FieldKind::U64, nullable: false },
            ],
        )
    }

    fn fixed_zeros() -> Vec<u8> {
        let t = ObjectType {
            type_id: 1,
            name: "blobby".into(),
            schema_ver: 1,
            fields: vec![
                Field { field_id: 1, name: "body".into(), kind: FieldKind::OverflowRef, nullable: false },
                Field { field_id: 2, name: "n".into(), kind: FieldKind::U64, nullable: false },
            ],
        };
        vec![0u8; t.compute_layout().record_size]
    }

    #[test]
    fn overflow_roundtrip_large_blob() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: overflow_type_def() });
        let blob = vec![0x5Au8; 100 * 1024]; // 100 KB
        let rec = super::encode_overflow_record(&fixed_zeros(), &[(0, blob.clone())]);
        let id = ObjectId::from_u128(1);
        assert_eq!(sm.apply(2, Op::Create { type_id: 1, id, record: rec }), OpResult::Ok);

        // GetById returns the FIXED record with a non-zero handle patched in.
        let handle = (2u64 << 20) | 0;
        match sm.apply(3, Op::GetById { type_id: 1, id }) {
            OpResult::Got(fixed) => {
                assert_eq!(fixed.len(), fixed_zeros().len(), "record stays fixed-width");
                let h = u64::from_le_bytes(fixed[14..22].try_into().unwrap());
                assert_eq!(h, handle, "OverflowRef slot holds the handle");
            }
            o => panic!("unexpected {o:?}"),
        }
        assert_eq!(sm.apply(4, Op::GetBlob { handle }), OpResult::Got(blob));
        assert_eq!(sm.apply(5, Op::GetBlob { handle: 999 }), OpResult::NotFound);
    }

    #[test]
    fn overflow_handles_are_deterministic_across_replicas() {
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: overflow_type_def() });
            for i in 0..50u64 {
                let rec = super::encode_overflow_record(
                    &fixed_zeros(),
                    &[(0, format!("payload-{i}").into_bytes())],
                );
                sm.apply(2 + i, Op::Create { type_id: 1, id: ObjectId::from_u128(i as u128), record: rec });
            }
            sm.digest()
        };
        assert_eq!(build(), build(), "overflow must not break determinism");
    }

    #[test]
    fn update_orphans_old_blob_no_gc_documented() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: overflow_type_def() });
        let id = ObjectId::from_u128(7);
        let r1 = super::encode_overflow_record(&fixed_zeros(), &[(0, b"old".to_vec())]);
        sm.apply(2, Op::Create { type_id: 1, id, record: r1 });
        let h_old = (2u64 << 20) | 0;
        let r2 = super::encode_overflow_record(&fixed_zeros(), &[(0, b"new".to_vec())]);
        sm.apply(3, Op::Update { type_id: 1, id, record: r2 });
        let h_new = (3u64 << 20) | 0;
        // New value readable; the record points at the new handle.
        assert_eq!(sm.apply(4, Op::GetBlob { handle: h_new }), OpResult::Got(b"new".to_vec()));
        // Old blob is ORPHANED but still resolvable (no GC yet — documented).
        assert_eq!(sm.apply(5, Op::GetBlob { handle: h_old }), OpResult::Got(b"old".to_vec()));
    }

    /// Linearizability vs. an in-memory reference model under a random op
    /// stream (the M2 correctness oracle).
    #[test]
    fn linearizable_vs_reference_model() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: transfer_def() });
        let mut model: HashMap<u128, Vec<u8>> = HashMap::new();
        let mut rng = Rng::new(0x5151);
        for op in 2..4000u64 {
            let id = ObjectId::from_u128(rng.below(50) as u128);
            let k = id.as_u128();
            match rng.below(5) {
                0 => {
                    let rec = vec![(op & 0xFF) as u8; 1 + rng.below(20) as usize];
                    let r = sm.apply(op, Op::Create { type_id: 1, id, record: rec.clone() });
                    if model.contains_key(&k) {
                        assert_eq!(r, OpResult::Exists);
                    } else {
                        assert_eq!(r, OpResult::Ok);
                        model.insert(k, rec);
                    }
                }
                1 => {
                    let rec = vec![0x55; 1 + rng.below(10) as usize];
                    let r = sm.apply(op, Op::Update { type_id: 1, id, record: rec.clone() });
                    if model.contains_key(&k) {
                        assert_eq!(r, OpResult::Ok);
                        model.insert(k, rec);
                    } else {
                        assert_eq!(r, OpResult::NotFound);
                    }
                }
                2 => {
                    let r = sm.apply(op, Op::Delete { type_id: 1, id });
                    if model.remove(&k).is_some() {
                        assert_eq!(r, OpResult::Ok);
                    } else {
                        assert_eq!(r, OpResult::NotFound);
                    }
                }
                _ => {
                    let r = sm.apply(op, Op::GetById { type_id: 1, id });
                    match model.get(&k) {
                        Some(v) => assert_eq!(r, OpResult::Got(v.clone())),
                        None => assert_eq!(r, OpResult::NotFound),
                    }
                }
            }
            if rng.below(200) == 0 {
                sm.flush().unwrap();
            }
        }
    }
}
