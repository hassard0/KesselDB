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

    // ---- equality secondary index (Sub-project 3) ----
    //
    // Index keyspace: storage type-slot = IDX_TYPE_BASE | (user_type & 0xFFFF).
    // Key id16 = field_id(2) ++ value_digest8 ++ [0;6]. The entry value holds
    // digest-collision-safe buckets: per distinct full value, a sorted set of
    // 16-byte object ids. All index keys/bytes are content-derived, so two
    // replicas applying the same ops build a byte-identical index keyspace
    // (covered by the state digest). Index maintenance is read-modify-write
    // (correct; throughput optimization is future perf work, documented).

    fn idx_value_digest(v: &[u8]) -> [u8; 8] {
        let a = kessel_proto::codec::crc32c(v) as u64;
        let mut salted = Vec::with_capacity(v.len() + 1);
        salted.push(0xA5);
        salted.extend_from_slice(v);
        let b = kessel_proto::codec::crc32c(&salted) as u64;
        ((a << 32) | b).to_le_bytes()
    }

    fn idx_key(user_type: u32, field_id: u16, v: &[u8]) -> Key {
        let mut id = [0u8; 16];
        id[..2].copy_from_slice(&field_id.to_le_bytes());
        id[2..10].copy_from_slice(&Self::idx_value_digest(v));
        make_key(0xFFFE_0000 | (user_type & 0xFFFF), &id)
    }

    /// buckets := [u16 n] then n × ( [u32 vlen][value] [u32 m] m×[16 id] )
    fn idx_decode(buf: &[u8]) -> Vec<(Vec<u8>, Vec<[u8; 16]>)> {
        let mut out = Vec::new();
        if buf.len() < 2 {
            return out;
        }
        let n = u16::from_le_bytes([buf[0], buf[1]]) as usize;
        let mut p = 2;
        for _ in 0..n {
            if p + 4 > buf.len() {
                break;
            }
            let vl = u32::from_le_bytes(buf[p..p + 4].try_into().unwrap()) as usize;
            p += 4;
            let val = buf[p..p + vl].to_vec();
            p += vl;
            let m = u32::from_le_bytes(buf[p..p + 4].try_into().unwrap()) as usize;
            p += 4;
            let mut ids = Vec::with_capacity(m);
            for _ in 0..m {
                let mut id = [0u8; 16];
                id.copy_from_slice(&buf[p..p + 16]);
                ids.push(id);
                p += 16;
            }
            out.push((val, ids));
        }
        out
    }

    fn idx_encode(buckets: &[(Vec<u8>, Vec<[u8; 16]>)]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&(buckets.len() as u16).to_le_bytes());
        for (val, ids) in buckets {
            b.extend_from_slice(&(val.len() as u32).to_le_bytes());
            b.extend_from_slice(val);
            b.extend_from_slice(&(ids.len() as u32).to_le_bytes());
            for id in ids {
                b.extend_from_slice(id);
            }
        }
        b
    }

    fn idx_add(&mut self, op_number: u64, ut: u32, fid: u16, v: &[u8], obj: [u8; 16]) {
        let key = Self::idx_key(ut, fid, v);
        let mut buckets = self
            .storage
            .get(&key)
            .map(|b| Self::idx_decode(&b))
            .unwrap_or_default();
        match buckets.iter_mut().find(|(val, _)| val == v) {
            Some((_, ids)) => {
                if let Err(i) = ids.binary_search(&obj) {
                    ids.insert(i, obj); // sorted set => deterministic bytes
                }
            }
            None => {
                buckets.push((v.to_vec(), vec![obj]));
                buckets.sort_by(|a, b| a.0.cmp(&b.0));
            }
        }
        let _ = self.storage.put(op_number, key, Self::idx_encode(&buckets));
    }

    fn idx_remove(&mut self, op_number: u64, ut: u32, fid: u16, v: &[u8], obj: [u8; 16]) {
        let key = Self::idx_key(ut, fid, v);
        let mut buckets = match self.storage.get(&key) {
            Some(b) => Self::idx_decode(&b),
            None => return,
        };
        if let Some((_, ids)) = buckets.iter_mut().find(|(val, _)| val == v) {
            if let Ok(i) = ids.binary_search(&obj) {
                ids.remove(i);
            }
        }
        buckets.retain(|(_, ids)| !ids.is_empty());
        if buckets.is_empty() {
            let _ = self.storage.delete(op_number, key);
        } else {
            let _ = self.storage.put(op_number, key, Self::idx_encode(&buckets));
        }
    }

    /// (offset,width) of an indexed field; None if absent or OverflowRef.
    fn idx_field_pos(ot: &kessel_catalog::ObjectType, fid: u16) -> Option<(usize, usize)> {
        let i = ot.fields.iter().position(|f| f.field_id == fid)?;
        if matches!(ot.fields[i].kind, kessel_catalog::FieldKind::OverflowRef) {
            return None;
        }
        let layout = ot.compute_layout();
        Some((layout.offsets[i], ot.fields[i].kind.width() as usize))
    }

    /// Maintain every index of `type_id` for one row mutation.
    fn idx_maintain(
        &mut self,
        op_number: u64,
        type_id: u32,
        obj: [u8; 16],
        old: Option<&[u8]>,
        new: Option<&[u8]>,
    ) {
        let ot = match self.catalog.get(type_id) {
            Some(t) => t.clone(),
            None => return,
        };
        for fid in ot.indexes.clone() {
            let (off, w) = match Self::idx_field_pos(&ot, fid) {
                Some(p) => p,
                None => continue,
            };
            let ov = old.and_then(|r| r.get(off..off + w));
            let nv = new.and_then(|r| r.get(off..off + w));
            if ov == nv {
                continue;
            }
            if let Some(o) = ov {
                let o = o.to_vec();
                self.idx_remove(op_number, type_id, fid, &o, obj);
            }
            if let Some(n) = nv {
                let n = n.to_vec();
                self.idx_add(op_number, type_id, fid, &n, obj);
            }
        }
    }

    // ---- built-in constraints (Sub-project 4) ----

    /// Field `i` is NULL if absent (beyond the record's stored field_count)
    /// or its null-bitmap bit is set. Reads only the codec header constants
    /// (no full codec dependency).
    fn field_is_null(rec: &[u8], i: usize) -> bool {
        use kessel_catalog::{NULL_BITMAP_BYTES, SCHEMA_VER_BYTES};
        if rec.len() < kessel_catalog::HEADER_BYTES {
            return true;
        }
        let fc = u16::from_le_bytes(
            rec[SCHEMA_VER_BYTES..SCHEMA_VER_BYTES + 2].try_into().unwrap(),
        ) as usize;
        if i >= fc {
            return true;
        }
        let bm = &rec[SCHEMA_VER_BYTES + 2..SCHEMA_VER_BYTES + 2 + NULL_BITMAP_BYTES];
        bm.get(i / 8).map(|b| b & (1 << (i % 8)) != 0).unwrap_or(true)
    }

    /// NOT NULL is enforced only for well-formed codec records — those whose
    /// length is exactly the type's computed `record_size`. Opaque/raw byte
    /// writes (any other length) carry no codec null information and are not
    /// constrained: a deliberate, documented kernel scoping (constraints ride
    /// the codec contract; raw byte writers opt out).
    fn check_not_null(ot: &kessel_catalog::ObjectType, rec: &[u8]) -> Result<(), String> {
        use kessel_catalog::SCHEMA_VER_BYTES;
        // Codec contract: exact record_size AND field_count == #fields. Any
        // other shape is an opaque/raw write and opts out of NOT NULL.
        if rec.len() != ot.compute_layout().record_size {
            return Ok(());
        }
        let fc = u16::from_le_bytes(
            rec[SCHEMA_VER_BYTES..SCHEMA_VER_BYTES + 2].try_into().unwrap(),
        ) as usize;
        if fc != ot.fields.len() {
            return Ok(());
        }
        for (i, f) in ot.fields.iter().enumerate() {
            if !f.nullable && Self::field_is_null(rec, i) {
                return Err(format!("NOT NULL violated on field '{}'", f.name));
            }
        }
        Ok(())
    }

    /// True if some OTHER object already holds `v` for this unique field.
    fn unique_conflict(&self, ut: u32, fid: u16, v: &[u8], self_obj: [u8; 16]) -> bool {
        let key = Self::idx_key(ut, fid, v);
        match self.storage.get(&key) {
            Some(b) => Self::idx_decode(&b)
                .into_iter()
                .find(|(val, _)| val == v)
                .map(|(_, ids)| ids.iter().any(|id| *id != self_obj))
                .unwrap_or(false),
            None => false,
        }
    }

    /// Enforce all UNIQUE constraints of `type_id` for one row write.
    fn check_unique(
        &self,
        type_id: u32,
        rec: &[u8],
        self_obj: [u8; 16],
    ) -> Result<(), String> {
        let ot = match self.catalog.get(type_id) {
            Some(t) => t,
            None => return Ok(()),
        };
        for fid in &ot.unique {
            if let Some((off, w)) = Self::idx_field_pos(ot, *fid) {
                if let Some(v) = rec.get(off..off + w) {
                    if self.unique_conflict(type_id, *fid, v, self_obj) {
                        let name = ot
                            .fields
                            .iter()
                            .find(|f| f.field_id == *fid)
                            .map(|f| f.name.as_str())
                            .unwrap_or("?");
                        return Err(format!("UNIQUE violated on field '{name}'"));
                    }
                }
            }
        }
        Ok(())
    }

    // ---- query planner (Sub-project 5) ----

    /// Width-normalize `v` to exactly `w` bytes (zero-pad / truncate).
    fn norm(v: &[u8], w: usize) -> Vec<u8> {
        let mut o = vec![0u8; w];
        let n = v.len().min(w);
        o[..n].copy_from_slice(&v[..n]);
        o
    }

    /// Compare two width-`w` field encodings per kind (numeric where it
    /// matters; lexicographic for byte kinds).
    fn cmp_field(
        kind: kessel_catalog::FieldKind,
        a: &[u8],
        b: &[u8],
    ) -> std::cmp::Ordering {
        use kessel_catalog::FieldKind::*;
        let w = kind.width() as usize;
        let a = Self::norm(a, w);
        let b = Self::norm(b, w);
        let load_u = |x: &[u8]| {
            let mut le = [0u8; 16];
            le[..w.min(16)].copy_from_slice(&x[..w.min(16)]);
            u128::from_le_bytes(le)
        };
        let load_i = |x: &[u8]| {
            let mut le = [0u8; 16];
            le[..w.min(16)].copy_from_slice(&x[..w.min(16)]);
            if w < 16 && x[w - 1] & 0x80 != 0 {
                for byte in le.iter_mut().skip(w) {
                    *byte = 0xFF;
                }
            }
            i128::from_le_bytes(le)
        };
        match kind {
            U8 | U16 | U32 | U64 | U128 | Bool | Timestamp => load_u(&a).cmp(&load_u(&b)),
            I8 | I16 | I32 | I64 | I128 | Fixed { .. } => load_i(&a).cmp(&load_i(&b)),
            Char(_) | Bytes(_) | Ref | OverflowRef => a.cmp(&b),
        }
    }

    /// Sorted object-id set for an indexed field value (equality).
    fn idx_lookup(&self, ut: u32, fid: u16, v: &[u8]) -> Vec<[u8; 16]> {
        let key = Self::idx_key(ut, fid, v);
        self.storage
            .get(&key)
            .map(|b| {
                Self::idx_decode(&b)
                    .into_iter()
                    .find(|(val, _)| val == v)
                    .map(|(_, ids)| ids)
                    .unwrap_or_default()
            })
            .unwrap_or_default()
    }

    /// True if `rec` follows the codec contract (exact size + field_count).
    /// FK / NOT NULL enforcement is scoped to such records (raw writers opt
    /// out by construction — documented).
    fn is_codec_record(ot: &kessel_catalog::ObjectType, rec: &[u8]) -> bool {
        use kessel_catalog::SCHEMA_VER_BYTES;
        if rec.len() != ot.compute_layout().record_size {
            return false;
        }
        let fc = u16::from_le_bytes(
            rec[SCHEMA_VER_BYTES..SCHEMA_VER_BYTES + 2].try_into().unwrap(),
        ) as usize;
        fc == ot.fields.len()
    }

    /// Enforce all foreign keys of `type_id` for one row write. The field
    /// value, padded to 16 bytes, must be an existing object id of the
    /// referenced type. Codec-record scoped; NULL fk fields are skipped
    /// (SQL-like). Read-only check against committed state ⇒ deterministic.
    fn check_fk(&self, type_id: u32, rec: &[u8]) -> Result<(), String> {
        let ot = match self.catalog.get(type_id) {
            Some(t) => t,
            None => return Ok(()),
        };
        if ot.fks.is_empty() || !Self::is_codec_record(ot, rec) {
            return Ok(());
        }
        let layout = ot.compute_layout();
        for (fid, rt) in &ot.fks {
            let i = match ot.fields.iter().position(|f| f.field_id == *fid) {
                Some(i) => i,
                None => continue,
            };
            if Self::field_is_null(rec, i) {
                continue; // NULL foreign key allowed
            }
            let off = layout.offsets[i];
            let w = ot.fields[i].kind.width() as usize;
            let fv = match rec.get(off..off + w) {
                Some(v) => v,
                None => continue,
            };
            let mut id16 = [0u8; 16];
            let n = fv.len().min(16);
            id16[..n].copy_from_slice(&fv[..n]);
            if self.storage.get(&make_key(*rt, &id16)).is_none() {
                let name = ot
                    .fields
                    .iter()
                    .find(|f| f.field_id == *fid)
                    .map(|f| f.name.as_str())
                    .unwrap_or("?");
                return Err(format!(
                    "FOREIGN KEY violated on field '{name}' -> type {rt}"
                ));
            }
        }
        Ok(())
    }

    /// Run every before-write trigger of `type_id` in order. Each may mutate
    /// the record or reject the write. Deterministic (pure gas-bounded VM).
    fn run_triggers(&self, type_id: u32, mut rec: Vec<u8>) -> Result<Vec<u8>, String> {
        let ot = match self.catalog.get(type_id) {
            Some(t) => t,
            None => return Ok(rec),
        };
        if ot.triggers.is_empty() {
            return Ok(rec);
        }
        for (n, prog) in ot.triggers.iter().enumerate() {
            match kessel_expr::eval_trigger(prog, ot, &rec) {
                Ok(Some(next)) => rec = next,
                Ok(None) => return Err(format!("trigger #{n} rejected the write")),
                Err(e) => return Err(format!("trigger #{n} error: {e:?}")),
            }
        }
        Ok(rec)
    }

    /// Run every CHECK program of `type_id` against `rec`. A program that
    /// returns false OR errors (div-by-zero, bad program, out-of-gas) rejects
    /// the write. Deterministic: the VM is pure and gas-bounded.
    fn check_checks(&self, type_id: u32, rec: &[u8]) -> Result<(), String> {
        let ot = match self.catalog.get(type_id) {
            Some(t) => t,
            None => return Ok(()),
        };
        for (n, prog) in ot.checks.iter().enumerate() {
            match kessel_expr::eval(prog, ot, rec) {
                Ok(true) => {}
                Ok(false) => return Err(format!("CHECK #{n} failed")),
                Err(e) => return Err(format!("CHECK #{n} error: {e:?}")),
            }
        }
        Ok(())
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
                    indexes: Vec::new(),
                    unique: Vec::new(),
                    fks: Vec::new(),
                    checks: Vec::new(),
                    triggers: Vec::new(),
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
                let need_idx = self
                    .catalog
                    .get(type_id)
                    .map(|t| !t.indexes.is_empty())
                    .unwrap_or(false);
                let record = match self.run_triggers(type_id, record) {
                    Ok(r) => r,
                    Err(e) => return OpResult::Constraint(e),
                };
                if let Some(t) = self.catalog.get(type_id) {
                    if let Err(e) = Self::check_not_null(t, &record) {
                        return OpResult::Constraint(e);
                    }
                }
                if let Err(e) = self.check_unique(type_id, &record, id.0) {
                    return OpResult::Constraint(e);
                }
                if let Err(e) = self.check_fk(type_id, &record) {
                    return OpResult::Constraint(e);
                }
                if let Err(e) = self.check_checks(type_id, &record) {
                    return OpResult::Constraint(e);
                }
                let rec_idx = if need_idx { Some(record.clone()) } else { None };
                let cached = self.cache.as_mut().map(|_| record.clone());
                match self.storage.put(op_number, key, record) {
                    Ok(()) => {
                        if let (Some(c), Some(v)) = (self.cache.as_mut(), cached) {
                            c.insert(key, v);
                        }
                        if let Some(r) = rec_idx {
                            self.idx_maintain(op_number, type_id, id.0, None, Some(&r));
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
                let old = self.storage.get(&key);
                if old.is_none() {
                    return OpResult::NotFound;
                }
                let record = match self.materialize_overflow(op_number, type_id, record) {
                    Ok(r) => r,
                    Err(e) => return e,
                };
                let need_idx = self
                    .catalog
                    .get(type_id)
                    .map(|t| !t.indexes.is_empty())
                    .unwrap_or(false);
                let record = match self.run_triggers(type_id, record) {
                    Ok(r) => r,
                    Err(e) => return OpResult::Constraint(e),
                };
                if let Some(t) = self.catalog.get(type_id) {
                    if let Err(e) = Self::check_not_null(t, &record) {
                        return OpResult::Constraint(e);
                    }
                }
                if let Err(e) = self.check_unique(type_id, &record, id.0) {
                    return OpResult::Constraint(e);
                }
                if let Err(e) = self.check_fk(type_id, &record) {
                    return OpResult::Constraint(e);
                }
                if let Err(e) = self.check_checks(type_id, &record) {
                    return OpResult::Constraint(e);
                }
                let rec_idx = if need_idx { Some(record.clone()) } else { None };
                let cached = self.cache.as_mut().map(|_| record.clone());
                match self.storage.put(op_number, key, record) {
                    Ok(()) => {
                        if let Some(c) = self.cache.as_mut() {
                            match cached {
                                Some(v) => c.insert(key, v),
                                None => c.invalidate(&key),
                            }
                        }
                        if let Some(r) = rec_idx {
                            self.idx_maintain(
                                op_number,
                                type_id,
                                id.0,
                                old.as_deref(),
                                Some(&r),
                            );
                        }
                        OpResult::Ok
                    }
                    Err(e) => OpResult::SchemaError(format!("store: {e}")),
                }
            }

            Op::Delete { type_id, id } => {
                let key = make_key(type_id, &id.0);
                let old = self.storage.get(&key);
                if old.is_none() {
                    return OpResult::NotFound;
                }
                match self.storage.delete(op_number, key) {
                    Ok(()) => {
                        if let Some(c) = self.cache.as_mut() {
                            c.invalidate(&key); // never serve a deleted row
                        }
                        self.idx_maintain(op_number, type_id, id.0, old.as_deref(), None);
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

            Op::CreateIndex { type_id, field_id } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                if ot.fields.iter().all(|f| f.field_id != field_id) {
                    return OpResult::SchemaError(format!("no field {field_id}"));
                }
                if Self::idx_field_pos(&ot, field_id).is_none() {
                    return OpResult::SchemaError("cannot index an OverflowRef field".into());
                }
                if ot.indexes.contains(&field_id) {
                    return OpResult::Ok; // idempotent
                }
                if let Some(t) = self.catalog.get_mut(type_id) {
                    t.indexes.push(field_id);
                }
                if let OpResult::SchemaError(e) = self.persist_catalog(op_number) {
                    return OpResult::SchemaError(e);
                }
                // Deterministic backfill of existing rows.
                let lo = make_key(type_id, &[0u8; 16]);
                let hi = make_key(type_id, &[0xFFu8; 16]);
                let rows = self.storage.scan_range(&lo, &hi);
                let (off, w) = Self::idx_field_pos(&ot, field_id).unwrap();
                for (k, rec) in rows {
                    if let Some(v) = rec.get(off..off + w) {
                        let mut obj = [0u8; 16];
                        obj.copy_from_slice(&k[4..20]);
                        let v = v.to_vec();
                        self.idx_add(op_number, type_id, field_id, &v, obj);
                    }
                }
                OpResult::Ok
            }

            Op::FindBy { type_id, field_id, value } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                let (_, w) = match Self::idx_field_pos(&ot, field_id) {
                    Some(p) => p,
                    None => return OpResult::SchemaError("field not indexable".into()),
                };
                if !ot.indexes.contains(&field_id) {
                    return OpResult::SchemaError("field is not indexed".into());
                }
                // Normalize query value to the field's fixed width.
                let mut v = value;
                v.resize(w, 0);
                let key = Self::idx_key(type_id, field_id, &v);
                let ids = match self.storage.get(&key) {
                    Some(b) => Self::idx_decode(&b)
                        .into_iter()
                        .find(|(val, _)| *val == v)
                        .map(|(_, ids)| ids)
                        .unwrap_or_default(),
                    None => Vec::new(),
                };
                let mut out = Vec::with_capacity(ids.len() * 16);
                for id in ids {
                    out.extend_from_slice(&id);
                }
                OpResult::Got(out)
            }

            Op::AddUnique { type_id, field_id } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                let (off, w) = match Self::idx_field_pos(&ot, field_id) {
                    Some(p) => p,
                    None => return OpResult::SchemaError("field not indexable".into()),
                };
                if ot.unique.contains(&field_id) {
                    return OpResult::Ok; // idempotent
                }
                // Ensure the backing index exists (build it if needed).
                if !ot.indexes.contains(&field_id) {
                    if let Some(t) = self.catalog.get_mut(type_id) {
                        t.indexes.push(field_id);
                    }
                    if let OpResult::SchemaError(e) = self.persist_catalog(op_number) {
                        return OpResult::SchemaError(e);
                    }
                    let lo = make_key(type_id, &[0u8; 16]);
                    let hi = make_key(type_id, &[0xFFu8; 16]);
                    for (k, rec) in self.storage.scan_range(&lo, &hi) {
                        if let Some(v) = rec.get(off..off + w) {
                            let mut obj = [0u8; 16];
                            obj.copy_from_slice(&k[4..20]);
                            let v = v.to_vec();
                            self.idx_add(op_number, type_id, field_id, &v, obj);
                        }
                    }
                }
                // Validate current data has no duplicate for this field.
                let idxtype = 0xFFFE_0000 | (type_id & 0xFFFF);
                let mut lo = [0u8; 16];
                lo[..2].copy_from_slice(&field_id.to_le_bytes());
                let mut hi = lo;
                hi[2..].copy_from_slice(&[0xFFu8; 14]);
                let lo = make_key(idxtype, &lo);
                let hi = make_key(idxtype, &hi);
                for (_, entry) in self.storage.scan_range(&lo, &hi) {
                    for (_, ids) in Self::idx_decode(&entry) {
                        if ids.len() > 1 {
                            return OpResult::Constraint(
                                "AddUnique: existing duplicate values".into(),
                            );
                        }
                    }
                }
                if let Some(t) = self.catalog.get_mut(type_id) {
                    t.unique.push(field_id);
                }
                self.persist_catalog(op_number)
            }

            Op::Query { type_id, preds } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                let layout = ot.compute_layout();
                // Resolve every predicate against the schema.
                struct P {
                    off: usize,
                    w: usize,
                    kind: kessel_catalog::FieldKind,
                    fid: u16,
                    op: u8,
                    val: Vec<u8>,
                    indexed: bool,
                }
                let mut plan: Vec<P> = Vec::with_capacity(preds.len());
                for pr in &preds {
                    let i = match ot.fields.iter().position(|f| f.field_id == pr.field_id) {
                        Some(i) => i,
                        None => {
                            return OpResult::SchemaError(format!("no field {}", pr.field_id))
                        }
                    };
                    if pr.op > 2 {
                        return OpResult::SchemaError("bad predicate op".into());
                    }
                    let w = ot.fields[i].kind.width() as usize;
                    plan.push(P {
                        off: layout.offsets[i],
                        w,
                        kind: ot.fields[i].kind,
                        fid: pr.field_id,
                        op: pr.op,
                        val: Self::norm(&pr.value, w),
                        indexed: ot.indexes.contains(&pr.field_id)
                            && Self::idx_field_pos(&ot, pr.field_id).is_some(),
                    });
                }

                let row_ok = |rec: &[u8], p: &P| -> bool {
                    match rec.get(p.off..p.off + p.w) {
                        Some(fv) => {
                            let c = Self::cmp_field(p.kind, fv, &p.val);
                            match p.op {
                                0 => c == std::cmp::Ordering::Equal,
                                1 => c != std::cmp::Ordering::Less, // >=
                                _ => c != std::cmp::Ordering::Greater, // <=
                            }
                        }
                        None => false,
                    }
                };

                // Planner: intersect indexed-equality predicates' id sets.
                let mut cand: Option<Vec<[u8; 16]>> = None;
                for p in plan.iter().filter(|p| p.op == 0 && p.indexed) {
                    let ids = self.idx_lookup(type_id, p.fid, &p.val);
                    cand = Some(match cand {
                        None => ids,
                        Some(prev) => {
                            let s: std::collections::BTreeSet<_> = ids.into_iter().collect();
                            prev.into_iter().filter(|i| s.contains(i)).collect()
                        }
                    });
                }

                let mut matched: Vec<[u8; 16]> = Vec::new();
                match cand {
                    Some(ids) => {
                        // Index-driven: verify ALL predicates on each candidate.
                        for id in ids {
                            if let Some(rec) = self.storage.get(&make_key(type_id, &id)) {
                                if plan.iter().all(|p| row_ok(&rec, p)) {
                                    matched.push(id);
                                }
                            }
                        }
                    }
                    None => {
                        // Filtered scan over the type's contiguous key range.
                        let lo = make_key(type_id, &[0u8; 16]);
                        let hi = make_key(type_id, &[0xFFu8; 16]);
                        for (k, rec) in self.storage.scan_range(&lo, &hi) {
                            if plan.iter().all(|p| row_ok(&rec, p)) {
                                let mut id = [0u8; 16];
                                id.copy_from_slice(&k[4..20]);
                                matched.push(id);
                            }
                        }
                    }
                }
                matched.sort_unstable();
                let mut out = Vec::with_capacity(matched.len() * 16);
                for id in matched {
                    out.extend_from_slice(&id);
                }
                OpResult::Got(out)
            }

            Op::AddForeignKey { type_id, field_id, ref_type_id } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                if self.catalog.get(ref_type_id).is_none() {
                    return OpResult::SchemaError(format!("no ref type {ref_type_id}"));
                }
                let i = match ot.fields.iter().position(|f| f.field_id == field_id) {
                    Some(i) => i,
                    None => return OpResult::SchemaError(format!("no field {field_id}")),
                };
                if matches!(ot.fields[i].kind, kessel_catalog::FieldKind::OverflowRef) {
                    return OpResult::SchemaError("cannot FK an OverflowRef field".into());
                }
                if ot.fks.contains(&(field_id, ref_type_id)) {
                    return OpResult::Ok; // idempotent
                }
                // Validate existing rows (same scope as enforcement).
                let layout = ot.compute_layout();
                let off = layout.offsets[i];
                let w = ot.fields[i].kind.width() as usize;
                let lo = make_key(type_id, &[0u8; 16]);
                let hi = make_key(type_id, &[0xFFu8; 16]);
                for (_, rec) in self.storage.scan_range(&lo, &hi) {
                    if !Self::is_codec_record(&ot, &rec) || Self::field_is_null(&rec, i) {
                        continue;
                    }
                    if let Some(fv) = rec.get(off..off + w) {
                        let mut id16 = [0u8; 16];
                        let n = fv.len().min(16);
                        id16[..n].copy_from_slice(&fv[..n]);
                        if self.storage.get(&make_key(ref_type_id, &id16)).is_none() {
                            return OpResult::Constraint(
                                "AddForeignKey: existing dangling reference".into(),
                            );
                        }
                    }
                }
                if let Some(t) = self.catalog.get_mut(type_id) {
                    t.fks.push((field_id, ref_type_id));
                }
                self.persist_catalog(op_number)
            }

            Op::AddCheck { type_id, program } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                // Structural validation against a zero record (catches a
                // malformed program even when the type has no rows yet).
                let zero = vec![0u8; ot.compute_layout().record_size];
                if let Err(e) = kessel_expr::eval(&program, &ot, &zero) {
                    if matches!(
                        e,
                        kessel_expr::ExprError::BadProgram
                            | kessel_expr::ExprError::StackUnderflow
                            | kessel_expr::ExprError::EmptyResult
                    ) {
                        return OpResult::SchemaError(format!("bad CHECK program: {e:?}"));
                    }
                }
                // Validate every existing row satisfies the new CHECK.
                let lo = make_key(type_id, &[0u8; 16]);
                let hi = make_key(type_id, &[0xFFu8; 16]);
                for (_, rec) in self.storage.scan_range(&lo, &hi) {
                    match kessel_expr::eval(&program, &ot, &rec) {
                        Ok(true) => {}
                        _ => {
                            return OpResult::Constraint(
                                "AddCheck: existing row violates CHECK".into(),
                            )
                        }
                    }
                }
                if let Some(t) = self.catalog.get_mut(type_id) {
                    t.checks.push(program);
                }
                self.persist_catalog(op_number)
            }

            Op::AddTrigger { type_id, program } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                // Structural validation against a zero record.
                let zero = vec![0u8; ot.compute_layout().record_size];
                if let Err(e) = kessel_expr::eval_trigger(&program, &ot, &zero) {
                    if matches!(
                        e,
                        kessel_expr::ExprError::BadProgram
                            | kessel_expr::ExprError::StackUnderflow
                    ) {
                        return OpResult::SchemaError(format!("bad trigger program: {e:?}"));
                    }
                }
                if let Some(t) = self.catalog.get_mut(type_id) {
                    t.triggers.push(program);
                }
                self.persist_catalog(op_number)
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
    use kessel_catalog::{encode_field, encode_type_def, Field, FieldKind, Layout, ObjectType};
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
            indexes: vec![],
            unique: vec![],
            fks: vec![],
            checks: vec![],
            triggers: vec![],
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

    // ---- Sub-project 3: equality secondary index ----

    fn indexed_type_def() -> Vec<u8> {
        // field 1 = u32 "owner" (indexable), field 2 = u32 "v"
        encode_type_def(
            "rec",
            &[
                Field { field_id: 0, name: "owner".into(), kind: FieldKind::U32, nullable: false },
                Field { field_id: 0, name: "v".into(), kind: FieldKind::U32, nullable: false },
            ],
        )
    }
    fn rec_bytes(owner: u32, v: u32) -> Vec<u8> {
        let t = ObjectType {
            type_id: 1,
            name: "rec".into(),
            schema_ver: 1,
            fields: vec![
                Field { field_id: 1, name: "owner".into(), kind: FieldKind::U32, nullable: false },
                Field { field_id: 2, name: "v".into(), kind: FieldKind::U32, nullable: false },
            ],
            indexes: vec![],
            unique: vec![],
            fks: vec![],
            checks: vec![],
            triggers: vec![],
        };
        let mut b = vec![0u8; t.compute_layout().record_size];
        let o0 = t.compute_layout().offsets[0];
        let o1 = t.compute_layout().offsets[1];
        b[o0..o0 + 4].copy_from_slice(&owner.to_le_bytes());
        b[o1..o1 + 4].copy_from_slice(&v.to_le_bytes());
        b
    }
    fn ids_of(r: OpResult) -> Vec<u128> {
        match r {
            OpResult::Got(b) => b
                .chunks(16)
                .map(|c| u128::from_le_bytes(c.try_into().unwrap()))
                .collect::<Vec<_>>(),
            o => panic!("expected Got, got {o:?}"),
        }
    }

    #[test]
    fn equality_index_find_by_after_create_and_backfill() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: indexed_type_def() });
        // rows BEFORE index exists -> exercises deterministic backfill
        for i in 0..6u128 {
            let owner = if i < 4 { 100 } else { 200 };
            sm.apply(2 + i as u64, Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(i),
                record: rec_bytes(owner, i as u32),
            });
        }
        assert_eq!(sm.apply(20, Op::CreateIndex { type_id: 1, field_id: 1 }), OpResult::Ok);
        assert_eq!(sm.apply(21, Op::CreateIndex { type_id: 1, field_id: 1 }), OpResult::Ok); // idempotent

        let mut got = ids_of(sm.apply(22, Op::FindBy { type_id: 1, field_id: 1, value: 100u32.to_le_bytes().to_vec() }));
        got.sort();
        assert_eq!(got, vec![0, 1, 2, 3]);
        let mut g2 = ids_of(sm.apply(23, Op::FindBy { type_id: 1, field_id: 1, value: 200u32.to_le_bytes().to_vec() }));
        g2.sort();
        assert_eq!(g2, vec![4, 5]);
        assert_eq!(ids_of(sm.apply(24, Op::FindBy { type_id: 1, field_id: 1, value: 999u32.to_le_bytes().to_vec() })), Vec::<u128>::new());
    }

    #[test]
    fn index_maintained_on_update_and_delete() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: indexed_type_def() });
        sm.apply(2, Op::CreateIndex { type_id: 1, field_id: 1 });
        let id = ObjectId::from_u128(42);
        sm.apply(3, Op::Create { type_id: 1, id, record: rec_bytes(7, 1) });
        assert_eq!(ids_of(sm.apply(4, Op::FindBy { type_id: 1, field_id: 1, value: 7u32.to_le_bytes().to_vec() })), vec![42]);
        // update moves it from owner=7 bucket to owner=9 bucket
        sm.apply(5, Op::Update { type_id: 1, id, record: rec_bytes(9, 1) });
        assert_eq!(ids_of(sm.apply(6, Op::FindBy { type_id: 1, field_id: 1, value: 7u32.to_le_bytes().to_vec() })), Vec::<u128>::new());
        assert_eq!(ids_of(sm.apply(7, Op::FindBy { type_id: 1, field_id: 1, value: 9u32.to_le_bytes().to_vec() })), vec![42]);
        // delete removes it entirely
        sm.apply(8, Op::Delete { type_id: 1, id });
        assert_eq!(ids_of(sm.apply(9, Op::FindBy { type_id: 1, field_id: 1, value: 9u32.to_le_bytes().to_vec() })), Vec::<u128>::new());
    }

    #[test]
    fn index_is_deterministic_across_instances() {
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: indexed_type_def() });
            sm.apply(2, Op::CreateIndex { type_id: 1, field_id: 1 });
            let mut rng = Rng::new(0xF00D);
            for op in 3..600u64 {
                let id = ObjectId::from_u128(rng.below(60) as u128);
                match rng.below(4) {
                    0 => { sm.apply(op, Op::Delete { type_id: 1, id }); }
                    1 => { sm.apply(op, Op::Update { type_id: 1, id, record: rec_bytes((rng.below(5)) as u32, op as u32) }); }
                    _ => { sm.apply(op, Op::Create { type_id: 1, id, record: rec_bytes((rng.below(5)) as u32, op as u32) }); }
                }
            }
            sm.digest()
        };
        assert_eq!(build(), build(), "index broke replica determinism");
    }

    // ---- Sub-project 4: UNIQUE + NOT NULL constraints ----

    #[test]
    fn not_null_enforced_for_codec_records() {
        use kessel_codec::{encode, Value};
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        // type: a = U32 NOT NULL, b = U32 nullable
        sm.apply(1, Op::CreateType {
            def: encode_type_def(
                "r",
                &[
                    Field { field_id: 0, name: "a".into(), kind: FieldKind::U32, nullable: false },
                    Field { field_id: 0, name: "b".into(), kind: FieldKind::U32, nullable: true },
                ],
            ),
        });
        let ot = sm.catalog().get(1).unwrap().clone();
        let id = ObjectId::from_u128(1);
        // Valid codec record (both set). a set, b NULL is also valid (b is
        // nullable). codec itself refuses Null in a non-nullable field, so to
        // exercise the SM-level NOT NULL guard we hand-set field 0's null bit.
        let good = encode(&ot, &[Value::Uint(7), Value::Null]).unwrap();
        assert_eq!(sm.apply(3, Op::Create { type_id: 1, id, record: good.clone() }), OpResult::Ok);
        let mut bad = good.clone();
        // null bitmap starts after schema_ver(4)+field_count(2) = byte 6;
        // set bit 0 => field 0 ('a', NOT NULL) is null.
        bad[6] |= 1;
        assert!(matches!(
            sm.apply(4, Op::Create { type_id: 1, id: ObjectId::from_u128(2), record: bad }),
            OpResult::Constraint(_)
        ));
    }

    #[test]
    fn unique_rejects_duplicate_on_create_and_update() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: indexed_type_def() });
        assert_eq!(sm.apply(2, Op::AddUnique { type_id: 1, field_id: 1 }), OpResult::Ok);
        assert_eq!(sm.apply(3, Op::AddUnique { type_id: 1, field_id: 1 }), OpResult::Ok); // idempotent
        sm.apply(4, Op::Create { type_id: 1, id: ObjectId::from_u128(1), record: rec_bytes(100, 1) });
        // second row with same owner=100 -> UNIQUE violation
        assert!(matches!(
            sm.apply(5, Op::Create { type_id: 1, id: ObjectId::from_u128(2), record: rec_bytes(100, 2) }),
            OpResult::Constraint(_)
        ));
        // different value is fine
        assert_eq!(sm.apply(6, Op::Create { type_id: 1, id: ObjectId::from_u128(2), record: rec_bytes(101, 2) }), OpResult::Ok);
        // updating row 2 to collide with row 1's value -> rejected
        assert!(matches!(
            sm.apply(7, Op::Update { type_id: 1, id: ObjectId::from_u128(2), record: rec_bytes(100, 9) }),
            OpResult::Constraint(_)
        ));
        // updating a row to its own same value is fine (self excluded)
        assert_eq!(sm.apply(8, Op::Update { type_id: 1, id: ObjectId::from_u128(1), record: rec_bytes(100, 42) }), OpResult::Ok);
    }

    #[test]
    fn add_unique_validates_existing_data() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: indexed_type_def() });
        sm.apply(2, Op::Create { type_id: 1, id: ObjectId::from_u128(1), record: rec_bytes(5, 1) });
        sm.apply(3, Op::Create { type_id: 1, id: ObjectId::from_u128(2), record: rec_bytes(5, 2) });
        // existing dup on field 1 -> AddUnique must refuse
        assert!(matches!(
            sm.apply(4, Op::AddUnique { type_id: 1, field_id: 1 }),
            OpResult::Constraint(_)
        ));
        // fix the dup, then it succeeds
        sm.apply(5, Op::Update { type_id: 1, id: ObjectId::from_u128(2), record: rec_bytes(6, 2) });
        assert_eq!(sm.apply(6, Op::AddUnique { type_id: 1, field_id: 1 }), OpResult::Ok);
        // and is now enforced
        assert!(matches!(
            sm.apply(7, Op::Create { type_id: 1, id: ObjectId::from_u128(3), record: rec_bytes(5, 3) }),
            OpResult::Constraint(_)
        ));
    }

    // ---- Sub-project 5: query planner ----

    fn q_type_def() -> Vec<u8> {
        encode_type_def(
            "q",
            &[
                Field { field_id: 0, name: "owner".into(), kind: FieldKind::U32, nullable: false },
                Field { field_id: 0, name: "kind".into(), kind: FieldKind::U16, nullable: false },
                Field { field_id: 0, name: "v".into(), kind: FieldKind::U32, nullable: false },
            ],
        )
    }
    fn q_layout() -> Layout {
        ObjectType {
            type_id: 1,
            name: "q".into(),
            schema_ver: 1,
            fields: vec![
                Field { field_id: 1, name: "owner".into(), kind: FieldKind::U32, nullable: false },
                Field { field_id: 2, name: "kind".into(), kind: FieldKind::U16, nullable: false },
                Field { field_id: 3, name: "v".into(), kind: FieldKind::U32, nullable: false },
            ],
            indexes: vec![],
            unique: vec![],
            fks: vec![],
            checks: vec![],
            triggers: vec![],
        }
        .compute_layout()
    }
    fn qrec(owner: u32, kind: u16, v: u32) -> Vec<u8> {
        let l = q_layout();
        let mut b = vec![0u8; l.record_size];
        b[l.offsets[0]..l.offsets[0] + 4].copy_from_slice(&owner.to_le_bytes());
        b[l.offsets[1]..l.offsets[1] + 2].copy_from_slice(&kind.to_le_bytes());
        b[l.offsets[2]..l.offsets[2] + 4].copy_from_slice(&v.to_le_bytes());
        b
    }
    fn qids(r: OpResult) -> Vec<u128> {
        match r {
            OpResult::Got(b) => b.chunks(16).map(|c| u128::from_le_bytes(c.try_into().unwrap())).collect(),
            o => panic!("expected Got, got {o:?}"),
        }
    }
    fn pred(field_id: u16, op: u8, value: Vec<u8>) -> kessel_proto::Pred {
        kessel_proto::Pred { field_id, op, value }
    }

    #[test]
    fn query_multi_index_intersection() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: q_type_def() });
        sm.apply(2, Op::CreateIndex { type_id: 1, field_id: 1 }); // owner
        sm.apply(3, Op::CreateIndex { type_id: 1, field_id: 2 }); // kind
        // rows: (owner, kind)
        let rows = [(100, 2), (100, 9), (100, 2), (200, 2), (100, 2)];
        for (i, (o, k)) in rows.iter().enumerate() {
            sm.apply(10 + i as u64, Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(i as u128),
                record: qrec(*o, *k, i as u32),
            });
        }
        // owner=100 AND kind=2  -> ids 0, 2, 4
        let mut got = qids(sm.apply(20, Op::Query {
            type_id: 1,
            preds: vec![
                pred(1, 0, 100u32.to_le_bytes().to_vec()),
                pred(2, 0, 2u16.to_le_bytes().to_vec()),
            ],
        }));
        got.sort();
        assert_eq!(got, vec![0, 2, 4]);
    }

    #[test]
    fn query_range_filtered_scan_no_index() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: q_type_def() });
        for i in 0..20u64 {
            sm.apply(2 + i, Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(i as u128),
                record: qrec(1, 0, i as u32),
            });
        }
        // 5 <= v <= 9  (no index on v -> filtered scan, numeric LE compare)
        let mut got = qids(sm.apply(40, Op::Query {
            type_id: 1,
            preds: vec![
                pred(3, 1, 5u32.to_le_bytes().to_vec()), // >= 5
                pred(3, 2, 9u32.to_le_bytes().to_vec()), // <= 9
            ],
        }));
        got.sort();
        assert_eq!(got, vec![5, 6, 7, 8, 9]);
    }

    #[test]
    fn query_indexed_eq_plus_unindexed_range() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: q_type_def() });
        sm.apply(2, Op::CreateIndex { type_id: 1, field_id: 1 }); // owner indexed
        for i in 0..30u64 {
            sm.apply(3 + i, Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(i as u128),
                record: qrec(if i % 2 == 0 { 7 } else { 8 }, 0, i as u32),
            });
        }
        // owner=7 (indexed) AND v >= 20  -> even i in [20,29] => 20,22,24,26,28
        let mut got = qids(sm.apply(50, Op::Query {
            type_id: 1,
            preds: vec![
                pred(1, 0, 7u32.to_le_bytes().to_vec()),
                pred(3, 1, 20u32.to_le_bytes().to_vec()),
            ],
        }));
        got.sort();
        assert_eq!(got, vec![20, 22, 24, 26, 28]);
        // empty result is well-formed
        assert_eq!(qids(sm.apply(51, Op::Query {
            type_id: 1,
            preds: vec![pred(1, 0, 999u32.to_le_bytes().to_vec())],
        })), Vec::<u128>::new());
    }

    #[test]
    fn query_is_readonly_and_deterministic() {
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: q_type_def() });
            sm.apply(2, Op::CreateIndex { type_id: 1, field_id: 1 });
            for i in 0..40u64 {
                sm.apply(3 + i, Op::Create {
                    type_id: 1,
                    id: ObjectId::from_u128(i as u128),
                    record: qrec((i % 3) as u32, 0, i as u32),
                });
            }
            let d0 = sm.digest();
            let q = sm.apply(99, Op::Query {
                type_id: 1,
                preds: vec![pred(1, 0, 1u32.to_le_bytes().to_vec())],
            });
            (qids(q), d0, sm.digest())
        };
        let (ids_a, before, after) = build();
        let (ids_b, _, _) = build();
        assert_eq!(before, after, "Query must not mutate state");
        assert_eq!(ids_a, ids_b, "Query must be deterministic");
        assert!(!ids_a.is_empty());
    }

    // ---- Sub-project 8: deterministic mutating triggers ----

    #[test]
    fn trigger_derives_field_on_write() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: chk_type() }); // fields age(I32) f1, bal(I64) f2
        // trigger: bal := age * 10
        let prog = kessel_expr::Program::new()
            .load(1).push_int(10).mul().set_field(2)
            .bytes();
        assert_eq!(sm.apply(2, Op::AddTrigger { type_id: 1, program: prog }), OpResult::Ok);
        assert_eq!(
            sm.apply(3, Op::Create { type_id: 1, id: ObjectId::from_u128(1), record: chk_rec(7, 0) }),
            OpResult::Ok
        );
        // read back: bal must have been set to 70 by the trigger
        match sm.apply(4, Op::GetById { type_id: 1, id: ObjectId::from_u128(1) }) {
            OpResult::Got(r) => {
                let l = q_like_layout();
                let bal = i64::from_le_bytes(r[l.1..l.1 + 8].try_into().unwrap());
                assert_eq!(bal, 70, "trigger derived bal = age*10");
            }
            o => panic!("unexpected {o:?}"),
        }
        // update also re-derives
        sm.apply(5, Op::Update { type_id: 1, id: ObjectId::from_u128(1), record: chk_rec(3, 999) });
        match sm.apply(6, Op::GetById { type_id: 1, id: ObjectId::from_u128(1) }) {
            OpResult::Got(r) => {
                let l = q_like_layout();
                let bal = i64::from_le_bytes(r[l.1..l.1 + 8].try_into().unwrap());
                assert_eq!(bal, 30, "trigger re-derived on update (ignored client 999)");
            }
            o => panic!("unexpected {o:?}"),
        }
    }

    // offsets of (age, bal) for the chk_type layout
    fn q_like_layout() -> (usize, usize) {
        let ot = ObjectType {
            type_id: 1, name: "acct".into(), schema_ver: 1,
            fields: vec![
                Field { field_id: 1, name: "age".into(), kind: FieldKind::I32, nullable: false },
                Field { field_id: 2, name: "bal".into(), kind: FieldKind::I64, nullable: false },
            ],
            indexes: vec![], unique: vec![], fks: vec![], checks: vec![], triggers: vec![],
        };
        let l = ot.compute_layout();
        (l.offsets[0], l.offsets[1])
    }

    #[test]
    fn trigger_can_reject_write() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: chk_type() });
        let prog = kessel_expr::Program::new().reject().bytes();
        assert_eq!(sm.apply(2, Op::AddTrigger { type_id: 1, program: prog }), OpResult::Ok);
        assert!(matches!(
            sm.apply(3, Op::Create { type_id: 1, id: ObjectId::from_u128(1), record: chk_rec(1, 1) }),
            OpResult::Constraint(_)
        ));
        // malformed trigger -> SchemaError not panic
        assert!(matches!(
            sm.apply(4, Op::AddTrigger { type_id: 1, program: vec![3] }),
            OpResult::SchemaError(_)
        ));
    }

    #[test]
    fn trigger_then_check_compose_deterministically() {
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: chk_type() });
            // trigger: bal := age * 2 ; CHECK: bal >= 0  (rejects negative age)
            sm.apply(2, Op::AddTrigger {
                type_id: 1,
                program: kessel_expr::Program::new().load(1).push_int(2).mul().set_field(2).bytes(),
            });
            sm.apply(3, Op::AddCheck {
                type_id: 1,
                program: kessel_expr::Program::new().load(2).push_int(0).ge().bytes(),
            });
            for i in 0..40i64 {
                let age = (i as i32) - 15;
                sm.apply(4 + i as u64, Op::Create {
                    type_id: 1,
                    id: ObjectId::from_u128(i as u128),
                    record: chk_rec(age, 7777),
                });
            }
            sm.digest()
        };
        assert_eq!(build(), build(), "trigger+check pipeline must be deterministic");
    }

    // ---- Sub-project 7: CHECK via deterministic expression VM ----

    fn chk_type() -> Vec<u8> {
        encode_type_def("acct", &[
            Field { field_id: 0, name: "age".into(), kind: FieldKind::I32, nullable: false },
            Field { field_id: 0, name: "bal".into(), kind: FieldKind::I64, nullable: false },
        ])
    }
    fn chk_rec(age: i32, bal: i64) -> Vec<u8> {
        let ot = ObjectType {
            type_id: 1, name: "acct".into(), schema_ver: 1,
            fields: vec![
                Field { field_id: 1, name: "age".into(), kind: FieldKind::I32, nullable: false },
                Field { field_id: 2, name: "bal".into(), kind: FieldKind::I64, nullable: false },
            ],
            indexes: vec![], unique: vec![], fks: vec![], checks: vec![], triggers: vec![],
        };
        let l = ot.compute_layout();
        let mut b = vec![0u8; l.record_size];
        b[l.offsets[0]..l.offsets[0] + 4].copy_from_slice(&age.to_le_bytes());
        b[l.offsets[1]..l.offsets[1] + 8].copy_from_slice(&bal.to_le_bytes());
        b
    }

    #[test]
    fn check_constraint_enforced_via_vm() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: chk_type() });
        // CHECK: age >= 18 AND bal >= 0
        let prog = kessel_expr::Program::new()
            .load(1).push_int(18).ge()
            .load(2).push_int(0).ge()
            .and()
            .bytes();
        assert_eq!(sm.apply(2, Op::AddCheck { type_id: 1, program: prog }), OpResult::Ok);
        assert_eq!(
            sm.apply(3, Op::Create { type_id: 1, id: ObjectId::from_u128(1), record: chk_rec(25, 100) }),
            OpResult::Ok
        );
        assert!(matches!(
            sm.apply(4, Op::Create { type_id: 1, id: ObjectId::from_u128(2), record: chk_rec(16, 100) }),
            OpResult::Constraint(_)
        ));
        assert!(matches!(
            sm.apply(5, Op::Create { type_id: 1, id: ObjectId::from_u128(3), record: chk_rec(30, -5) }),
            OpResult::Constraint(_)
        ));
        // update violating CHECK is rejected too
        assert!(matches!(
            sm.apply(6, Op::Update { type_id: 1, id: ObjectId::from_u128(1), record: chk_rec(25, -1) }),
            OpResult::Constraint(_)
        ));
    }

    #[test]
    fn add_check_validates_existing_and_rejects_bad_program() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: chk_type() });
        sm.apply(2, Op::Create { type_id: 1, id: ObjectId::from_u128(1), record: chk_rec(40, 0) });
        sm.apply(3, Op::Create { type_id: 1, id: ObjectId::from_u128(2), record: chk_rec(10, 0) });
        let prog = kessel_expr::Program::new().load(1).push_int(18).ge().bytes();
        // existing row age=10 violates -> AddCheck refused
        assert!(matches!(
            sm.apply(4, Op::AddCheck { type_id: 1, program: prog.clone() }),
            OpResult::Constraint(_)
        ));
        // malformed program -> SchemaError, not a panic
        assert!(matches!(
            sm.apply(5, Op::AddCheck { type_id: 1, program: vec![3] }),
            OpResult::SchemaError(_)
        ));
        // fix the row, then AddCheck succeeds and enforces
        sm.apply(6, Op::Update { type_id: 1, id: ObjectId::from_u128(2), record: chk_rec(22, 0) });
        assert_eq!(sm.apply(7, Op::AddCheck { type_id: 1, program: prog }), OpResult::Ok);
        assert!(matches!(
            sm.apply(8, Op::Create { type_id: 1, id: ObjectId::from_u128(9), record: chk_rec(5, 0) }),
            OpResult::Constraint(_)
        ));
    }

    #[test]
    fn check_is_deterministic() {
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: chk_type() });
            let prog = kessel_expr::Program::new().load(1).push_int(0).gt().bytes();
            sm.apply(2, Op::AddCheck { type_id: 1, program: prog });
            for i in 0..50u64 {
                let age = (i as i32) - 10; // some negative -> rejected uniformly
                sm.apply(3 + i, Op::Create {
                    type_id: 1,
                    id: ObjectId::from_u128(i as u128),
                    record: chk_rec(age, 0),
                });
            }
            sm.digest()
        };
        assert_eq!(build(), build(), "CHECK VM must be deterministic");
    }

    // ---- Sub-project 6: foreign keys ----

    #[test]
    fn foreign_key_enforced_and_validates_existing() {
        use kessel_codec::{encode, Value};
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        // parent type 1, child type 2 with pref(U128) -> parent, v(U32)
        sm.apply(1, Op::CreateType {
            def: encode_type_def("parent", &[
                Field { field_id: 0, name: "a".into(), kind: FieldKind::U64, nullable: false },
            ]),
        });
        sm.apply(2, Op::CreateType {
            def: encode_type_def("child", &[
                Field { field_id: 0, name: "pref".into(), kind: FieldKind::U128, nullable: false },
                Field { field_id: 0, name: "v".into(), kind: FieldKind::U32, nullable: false },
            ]),
        });
        let child_ot = sm.catalog().get(2).unwrap().clone();
        let child = |p: u128, v: u32| encode(&child_ot, &[Value::Uint(p), Value::Uint(v as u128)]).unwrap();

        // parent id=5 exists
        sm.apply(3, Op::Create { type_id: 1, id: ObjectId::from_u128(5), record: vec![1] });

        // Add FK: pref -> type 1 (no children yet -> validates clean)
        assert_eq!(
            sm.apply(4, Op::AddForeignKey { type_id: 2, field_id: 1, ref_type_id: 1 }),
            OpResult::Ok
        );
        assert_eq!(
            sm.apply(5, Op::AddForeignKey { type_id: 2, field_id: 1, ref_type_id: 1 }),
            OpResult::Ok,
            "idempotent"
        );
        // child referencing existing parent 5 -> Ok
        assert_eq!(
            sm.apply(6, Op::Create { type_id: 2, id: ObjectId::from_u128(1), record: child(5, 1) }),
            OpResult::Ok
        );
        // child referencing missing parent 999 -> Constraint
        assert!(matches!(
            sm.apply(7, Op::Create { type_id: 2, id: ObjectId::from_u128(2), record: child(999, 1) }),
            OpResult::Constraint(_)
        ));
        // update child to dangling ref -> Constraint
        assert!(matches!(
            sm.apply(8, Op::Update { type_id: 2, id: ObjectId::from_u128(1), record: child(404, 2) }),
            OpResult::Constraint(_)
        ));
    }

    #[test]
    fn add_foreign_key_rejects_existing_dangling() {
        use kessel_codec::{encode, Value};
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType {
            def: encode_type_def("parent", &[
                Field { field_id: 0, name: "a".into(), kind: FieldKind::U64, nullable: false },
            ]),
        });
        sm.apply(2, Op::CreateType {
            def: encode_type_def("child", &[
                Field { field_id: 0, name: "pref".into(), kind: FieldKind::U128, nullable: false },
            ]),
        });
        let cot = sm.catalog().get(2).unwrap().clone();
        sm.apply(3, Op::Create { type_id: 1, id: ObjectId::from_u128(5), record: vec![1] });
        // child references parent 5 (exists) and 7 (does NOT) before FK added
        sm.apply(4, Op::Create { type_id: 2, id: ObjectId::from_u128(1), record: encode(&cot, &[Value::Uint(5)]).unwrap() });
        sm.apply(5, Op::Create { type_id: 2, id: ObjectId::from_u128(2), record: encode(&cot, &[Value::Uint(7)]).unwrap() });
        // AddForeignKey must refuse (id=2 is dangling) and NOT enable
        assert!(matches!(
            sm.apply(6, Op::AddForeignKey { type_id: 2, field_id: 1, ref_type_id: 1 }),
            OpResult::Constraint(_)
        ));
        // fix the dangling row, then it succeeds and is enforced
        sm.apply(7, Op::Update { type_id: 2, id: ObjectId::from_u128(2), record: encode(&cot, &[Value::Uint(5)]).unwrap() });
        assert_eq!(sm.apply(8, Op::AddForeignKey { type_id: 2, field_id: 1, ref_type_id: 1 }), OpResult::Ok);
        assert!(matches!(
            sm.apply(9, Op::Create { type_id: 2, id: ObjectId::from_u128(3), record: encode(&cot, &[Value::Uint(123)]).unwrap() }),
            OpResult::Constraint(_)
        ));
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
