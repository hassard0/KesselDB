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

pub struct StateMachine<V: Vfs> {
    catalog: Catalog,
    storage: Storage<V>,
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
        Ok(StateMachine { catalog, storage })
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
                match self.storage.put(op_number, key, record) {
                    Ok(()) => OpResult::Ok,
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
                match self.storage.put(op_number, key, record) {
                    Ok(()) => OpResult::Ok,
                    Err(e) => OpResult::SchemaError(format!("store: {e}")),
                }
            }

            Op::Delete { type_id, id } => {
                let key = make_key(type_id, &id.0);
                if self.storage.get(&key).is_none() {
                    return OpResult::NotFound;
                }
                match self.storage.delete(op_number, key) {
                    Ok(()) => OpResult::Ok,
                    Err(e) => OpResult::SchemaError(format!("store: {e}")),
                }
            }

            Op::GetById { type_id, id } => {
                let key = make_key(type_id, &id.0);
                match self.storage.get(&key) {
                    Some(b) => OpResult::Got(b),
                    None => OpResult::NotFound,
                }
            }
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

    /// Flush the underlying storage memtable (used at checkpoints/benchmarks).
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.storage.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_catalog::{encode_field, encode_type_def, FieldKind};
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
