//! kessel-sm: the deterministic state machine.
//!
//! `apply(op_number, op) -> OpResult` is a pure function of (committed state,
//! op). NO clock, NO RNG: ids/field-ids/schema-versions derive only from
//! catalog state, timestamps arrive inside the op (from the VSR primary).
//! The only side effect is the injected `Storage`, which is itself
//! deterministic. This is what lets the simulator replay a run bit-for-bit.

#![forbid(unsafe_code)]

use kessel_catalog::{
    decode_field, decode_type_def, encode_type_def, Catalog, Field, ObjectType,
};
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

/// Reserved keyspace for the global cross-shard sequencer (SP79).
/// Object id 0 = the next-sequence counter; ids ≥ 1 = descriptor log
/// entries keyed by **big-endian** seq so a range scan is in order.
const SEQ_TYPE: u32 = 0xFFFF_FFF0;

fn seq_counter_key() -> Key {
    make_key(SEQ_TYPE, &[0u8; 16])
}

fn seq_entry_key(seq: u64) -> Key {
    let mut id = [0u8; 16];
    id[8..16].copy_from_slice(&seq.to_be_bytes()); // big-endian ⇒ sorted
    make_key(SEQ_TYPE, &id)
}

/// Reserved keyspace for a shard's cross-shard apply cursor (SP80):
/// the highest global sequence number this shard has processed.
const XSHARD_TYPE: u32 = 0xFFFF_FFF1;

fn xshard_cursor_key() -> Key {
    make_key(XSHARD_TYPE, &[0u8; 16])
}

/// Reserved keyspaces (SP81): the sequencer dedup map (exactly-once
/// append) and the per-seq cross-shard verdict store (deterministic
/// abort agreement). Both are in the digest, so a router restart
/// re-derives identical decisions from durable state — no coordinator.
const SEQ_DEDUP_TYPE: u32 = 0xFFFF_FFF2;
const XVOTE_TYPE: u32 = 0xFFFF_FFF3;

/// 128-bit FNV-1a — a wide, deterministic id for an arbitrary dedup key.
fn fnv16(b: &[u8]) -> [u8; 16] {
    let mut h: u128 = 0x6c62272e07bb014262b821756295c58d;
    for &x in b {
        h ^= x as u128;
        h = h.wrapping_mul(0x0000000001000000000000000000013B);
    }
    h.to_le_bytes()
}

fn seq_dedup_key(k: &[u8]) -> Key {
    make_key(SEQ_DEDUP_TYPE, &fnv16(k))
}

fn xvote_key(seq: u64) -> Key {
    let mut id = [0u8; 16];
    id[8..16].copy_from_slice(&seq.to_be_bytes());
    make_key(XVOTE_TYPE, &id)
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

/// Default read-cache capacity (entries) for `StateMachine::open` (SP50).
/// Bounded LRU; digest-invisible; `with_cache` overrides it.
const DEFAULT_READ_CACHE: usize = 8192;

pub struct StateMachine<V: Vfs> {
    catalog: Catalog,
    storage: Storage<V>,
    /// Optional read cache. NEVER consulted to compute committed state or the
    /// digest — only to short-circuit `GetById`. Off => zero core-path effect.
    cache: Option<kessel_cache::ReadCache>,
    /// Engine-local schema epoch (SP51). Not part of the digest; bumped by
    /// `persist_catalog` on every catalog change.
    catalog_epoch: u64,
}

impl<V: Vfs> StateMachine<V> {
    pub fn open(vfs: V) -> std::io::Result<Self> {
        let mut storage = Storage::open(vfs)?;
        // SP49: bound point-read fan-out for the product (raw `Storage`
        // stays unbounded by default for the primitive tests). 8 segments
        // keeps reads ≈O(1) in total data while amortising write cost.
        storage.set_compact_threshold(8);
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
            // SP50: the product runs with the read cache ON by default.
            // It is digest-invisible (never consulted for committed state)
            // and is deterministically invalidated on every write, so this
            // changes nothing observable or replicated — it only
            // short-circuits hot `GetById`s. `with_cache` overrides the
            // capacity; cache-off remains available for the raw primitive.
            cache: Some(kessel_cache::ReadCache::new(DEFAULT_READ_CACHE)),
            catalog_epoch: 0,
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
            Ok(()) => {
                // SP51: every catalog mutation flows through here, so this
                // is the one place to bump the schema epoch. It is engine-
                // local metadata (NOT part of the digest / replicated
                // state) but is deterministic — same op stream ⇒ same
                // epoch — so a compiled-statement cache keyed by it is
                // never served against a changed schema.
                self.catalog_epoch += 1;
                OpResult::Ok
            }
            Err(e) => OpResult::SchemaError(format!("catalog persist: {e}")),
        }
    }

    /// Monotonic counter bumped on every catalog (schema) change. A SQL
    /// compile cache keyed by `(sql, catalog_epoch)` stays correct across
    /// online DDL without recompiling on the hot path (SP51).
    pub fn catalog_epoch(&self) -> u64 {
        self.catalog_epoch
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

    /// SP76: the overflow-blob handles a record references — the 8-byte
    /// handle stored at each `OverflowRef` field slot (0 = none). Used
    /// to reclaim blobs when the referencing row is updated or deleted.
    fn overflow_handles(&self, type_id: u32, rec: &[u8]) -> Vec<u64> {
        let ot = match self.catalog.get(type_id) {
            Some(t) => t,
            None => return Vec::new(),
        };
        let layout = ot.compute_layout();
        let mut hs = Vec::new();
        for (i, f) in ot.fields.iter().enumerate() {
            if matches!(f.kind, kessel_catalog::FieldKind::OverflowRef) {
                if let Some(b) = rec.get(layout.offsets[i]..layout.offsets[i] + 8)
                {
                    let h = u64::from_le_bytes(b.try_into().unwrap());
                    if h != 0 {
                        hs.push(h);
                    }
                }
            }
        }
        hs
    }

    /// Delete the given overflow blobs (deterministic: handles are
    /// op-number-derived, so every replica frees the same keys).
    fn reclaim_overflow(&mut self, op: u64, freed: &[u64]) {
        for &h in freed {
            let _ = self.storage.delete(op, handle_key(h));
        }
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

    // SP25: ONE LSM entry per (value, object) — no read-modify-write.
    // Variable-length key = ns(4 LE) ++ field_id(2 LE) ++ value(w) ++
    // object_id(16). Lexicographic order groups all entries for a given
    // (type,field,value) contiguously, so add = 1 put, remove = 1 delete,
    // lookup = a prefix scan. Empty value bytes (the entry payload carries
    // nothing; the object id lives in the key).

    fn idx_prefix(ut: u32, fid: u16, v: &[u8]) -> Vec<u8> {
        let mut p = Vec::with_capacity(6 + v.len());
        p.extend_from_slice(&(0xFFFE_0000 | (ut & 0xFFFF)).to_le_bytes());
        p.extend_from_slice(&fid.to_le_bytes());
        p.extend_from_slice(v);
        p
    }

    fn idx_entry_key(ut: u32, fid: u16, v: &[u8], obj: &[u8; 16]) -> Key {
        let mut k = Self::idx_prefix(ut, fid, v);
        k.extend_from_slice(obj);
        k
    }

    fn idx_add(&mut self, op_number: u64, ut: u32, fid: u16, v: &[u8], obj: [u8; 16]) {
        let key = Self::idx_entry_key(ut, fid, v, &obj);
        let _ = self.storage.put(op_number, key, Vec::new()); // O(1), no RMW
    }

    fn idx_remove(&mut self, op_number: u64, ut: u32, fid: u16, v: &[u8], obj: [u8; 16]) {
        let key = Self::idx_entry_key(ut, fid, v, &obj);
        let _ = self.storage.delete(op_number, key); // O(1), no RMW
    }

    /// Synthetic field-id for composite index number `n` (real field_ids are
    /// small; 0xC000+ is reserved for composites).
    fn composite_fid(n: usize) -> u16 {
        0xC000 | (n as u16 & 0x3FFF)
    }

    /// Concatenate the member fields' bytes from `rec` (in `flist` order).
    /// None if any member is missing/short.
    fn composite_concat(
        ot: &kessel_catalog::ObjectType,
        flist: &[u16],
        rec: &[u8],
    ) -> Option<Vec<u8>> {
        let layout = ot.compute_layout();
        let mut out = Vec::new();
        for fid in flist {
            let i = ot.fields.iter().position(|f| f.field_id == *fid)?;
            let off = layout.offsets[i];
            let w = ot.fields[i].kind.width() as usize;
            out.extend_from_slice(rec.get(off..off + w)?);
        }
        Some(out)
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
        // SP15: order-preserving range indexes.
        for fid in ot.ordered.clone() {
            let (off, w, kind) = match Self::ord_field_pos(&ot, fid) {
                Some(p) => p,
                None => continue,
            };
            let ov = old.and_then(|r| r.get(off..off + w));
            let nv = new.and_then(|r| r.get(off..off + w));
            if ov == nv {
                continue;
            }
            if let Some(o) = ov.and_then(|b| Self::order_key(kind, b)) {
                self.oidx_remove(op_number, type_id, fid, o, obj);
            }
            if let Some(n) = nv.and_then(|b| Self::order_key(kind, b)) {
                self.oidx_add(op_number, type_id, fid, n, obj);
            }
        }
        // SP87: variable-length (CHAR/BYTES) ordered indexes — the
        // numeric loop above skipped these (ord_field_pos → None).
        for fid in ot.ordered.clone() {
            let (off, w, k) = match Self::vord_field_pos(&ot, fid) {
                Some(p) => p,
                None => continue,
            };
            let ov = old.and_then(|r| r.get(off..off + w));
            let nv = new.and_then(|r| r.get(off..off + w));
            if ov == nv {
                continue;
            }
            if let Some(o) = ov {
                let o = Self::vorder_key(k, o, w);
                self.voidx_remove(op_number, type_id, fid, &o, obj);
            }
            if let Some(n) = nv {
                let n = Self::vorder_key(k, n, w);
                self.voidx_add(op_number, type_id, fid, &n, obj);
            }
        }
        // SP27: composite (multi-field) equality indexes.
        for (ci_no, flist) in ot.composite.clone().iter().enumerate() {
            if flist.is_empty() {
                continue; // SP74: a dropped composite — slot kept, inert
            }
            let oc = old.and_then(|r| Self::composite_concat(&ot, flist, r));
            let nc = new.and_then(|r| Self::composite_concat(&ot, flist, r));
            if oc == nc {
                continue;
            }
            let cfid = Self::composite_fid(ci_no);
            if let Some(o) = &oc {
                self.idx_remove(op_number, type_id, cfid, o, obj);
            }
            if let Some(n) = &nc {
                self.idx_add(op_number, type_id, cfid, n, obj);
            }
        }
    }

    // ---- SP15: order-preserving range index ----

    /// (offset, width, kind) if the field supports an ordered index
    /// (fixed-width numeric/bool/timestamp ≤ 8 bytes). None otherwise.
    fn ord_field_pos(
        ot: &kessel_catalog::ObjectType,
        fid: u16,
    ) -> Option<(usize, usize, kessel_catalog::FieldKind)> {
        use kessel_catalog::FieldKind::*;
        let i = ot.fields.iter().position(|f| f.field_id == fid)?;
        let kind = ot.fields[i].kind;
        let ok = matches!(
            kind,
            U8 | U16 | U32 | U64 | I8 | I16 | I32 | I64 | Bool | Timestamp | Fixed { .. }
        );
        if !ok {
            return None;
        }
        let layout = ot.compute_layout();
        Some((layout.offsets[i], ot.fields[i].kind.width() as usize, kind))
    }

    /// Order-preserving 8-byte big-endian encoding: unsigned as-is,
    /// signed/Fixed with the sign bit flipped so lexicographic byte order
    /// equals numeric order.
    fn order_key(kind: kessel_catalog::FieldKind, raw: &[u8]) -> Option<[u8; 8]> {
        use kessel_catalog::FieldKind::*;
        let w = kind.width() as usize;
        if w > 8 || raw.len() < w {
            return None;
        }
        let signed = matches!(kind, I8 | I16 | I32 | I64 | Fixed { .. });
        let mut le = [0u8; 8];
        le[..w].copy_from_slice(&raw[..w]);
        if signed && w < 8 && raw[w - 1] & 0x80 != 0 {
            for b in le.iter_mut().skip(w) {
                *b = 0xFF;
            }
        }
        let mut v = u64::from_le_bytes(le);
        if signed {
            v ^= 0x8000_0000_0000_0000;
        }
        Some(v.to_be_bytes())
    }

    fn oidx_key(ut: u32, fid: u16, ok: [u8; 8]) -> Key {
        let mut id = [0u8; 16];
        id[..2].copy_from_slice(&fid.to_le_bytes());
        id[2..10].copy_from_slice(&ok);
        make_key(0xFFFD_0000 | (ut & 0xFFFF), &id)
    }

    // ---- SP87: variable-length (CHAR/BYTES) ordered index ----
    //
    // A SEPARATE keyspace (tag 0xFFFC) so the numeric ≤8B path (0xFFFD)
    // is byte-identical and untouched (zero migration / digest risk).
    // CHAR/BYTES are fixed-width and memcmp-ordered as stored
    // (zero-padded), so the order key is just the raw `w` bytes — no
    // transform. Key = tag(4) ++ field_id(2) ++ ok(w); a bucket value
    // is the sorted set of 16-byte object ids, exactly like `oidx`.

    /// (offset, width, kind) if the field is a fixed-width CHAR/BYTES
    /// column (variable-length ordered-index path). None otherwise.
    fn vord_field_pos(
        ot: &kessel_catalog::ObjectType,
        fid: u16,
    ) -> Option<(usize, usize, kessel_catalog::FieldKind)> {
        use kessel_catalog::FieldKind::*;
        let i = ot.fields.iter().position(|f| f.field_id == fid)?;
        let kind = ot.fields[i].kind;
        // CHAR/BYTES are memcmp-ordered as stored; U128/I128 (SP91)
        // exceed the 8-byte numeric path and ride the same 0xFFFC
        // keyspace via an order-preserving 16-byte transform
        // (`vorder_key`).
        if !matches!(kind, Char(_) | Bytes(_) | U128 | I128) {
            return None;
        }
        let layout = ot.compute_layout();
        Some((layout.offsets[i], kind.width() as usize, kind))
    }

    /// The order-preserving variable-length key for the `0xFFFC`
    /// keyspace. CHAR/BYTES: the raw width-`w` bytes, unchanged (so
    /// every pre-SP91 string index is byte-identical — zero migration
    /// / digest risk). U128: 16-byte big-endian (memcmp == numeric).
    /// I128 (SP91): 16-byte big-endian with the sign bit flipped so
    /// negatives sort below positives. `raw` is the field's stored
    /// little-endian bytes (or a `norm`-padded bound).
    fn vorder_key(
        kind: kessel_catalog::FieldKind,
        raw: &[u8],
        w: usize,
    ) -> Vec<u8> {
        use kessel_catalog::FieldKind::*;
        match kind {
            U128 | I128 => {
                let mut le = [0u8; 16];
                let n = raw.len().min(16);
                le[..n].copy_from_slice(&raw[..n]);
                // sign-extend a short negative I128 bound (codec-stored
                // fields are always full width; SQL/Op bounds may be
                // minimal) — mirrors the codec's load path.
                if matches!(kind, I128)
                    && n > 0
                    && n < 16
                    && raw[n - 1] & 0x80 != 0
                {
                    for b in le.iter_mut().skip(n) {
                        *b = 0xFF;
                    }
                }
                let mut v = u128::from_le_bytes(le);
                if matches!(kind, I128) {
                    v ^= 1u128 << 127;
                }
                v.to_be_bytes().to_vec()
            }
            _ => Self::norm(raw, w),
        }
    }

    fn voidx_key(ut: u32, fid: u16, ok: &[u8]) -> Key {
        let mut k = Vec::with_capacity(6 + ok.len());
        k.extend_from_slice(&(0xFFFC_0000 | (ut & 0xFFFF)).to_le_bytes());
        k.extend_from_slice(&fid.to_le_bytes());
        k.extend_from_slice(ok);
        k
    }

    fn voidx_add(&mut self, op: u64, ut: u32, fid: u16, ok: &[u8], obj: [u8; 16]) {
        let key = Self::voidx_key(ut, fid, ok);
        let mut ids: Vec<[u8; 16]> = self
            .storage
            .get(&key)
            .map(|b| {
                b.chunks(16)
                    .filter(|c| c.len() == 16)
                    .map(|c| {
                        let mut a = [0u8; 16];
                        a.copy_from_slice(c);
                        a
                    })
                    .collect()
            })
            .unwrap_or_default();
        if let Err(i) = ids.binary_search(&obj) {
            ids.insert(i, obj);
        }
        let mut out = Vec::with_capacity(ids.len() * 16);
        for x in &ids {
            out.extend_from_slice(x);
        }
        let _ = self.storage.put(op, key, out);
    }

    fn voidx_remove(&mut self, op: u64, ut: u32, fid: u16, ok: &[u8], obj: [u8; 16]) {
        let key = Self::voidx_key(ut, fid, ok);
        let mut ids: Vec<[u8; 16]> = match self.storage.get(&key) {
            Some(b) => b
                .chunks(16)
                .filter(|c| c.len() == 16)
                .map(|c| {
                    let mut a = [0u8; 16];
                    a.copy_from_slice(c);
                    a
                })
                .collect(),
            None => return,
        };
        if let Ok(i) = ids.binary_search(&obj) {
            ids.remove(i);
        }
        if ids.is_empty() {
            let _ = self.storage.delete(op, key);
        } else {
            let mut out = Vec::with_capacity(ids.len() * 16);
            for x in &ids {
                out.extend_from_slice(x);
            }
            let _ = self.storage.put(op, key, out);
        }
    }

    fn oidx_add(&mut self, op: u64, ut: u32, fid: u16, ok: [u8; 8], obj: [u8; 16]) {
        let key = Self::oidx_key(ut, fid, ok);
        let mut ids: Vec<[u8; 16]> = self
            .storage
            .get(&key)
            .map(|b| b.chunks(16).filter(|c| c.len() == 16).map(|c| {
                let mut a = [0u8; 16];
                a.copy_from_slice(c);
                a
            }).collect())
            .unwrap_or_default();
        if let Err(i) = ids.binary_search(&obj) {
            ids.insert(i, obj);
        }
        let mut out = Vec::with_capacity(ids.len() * 16);
        for x in &ids {
            out.extend_from_slice(x);
        }
        let _ = self.storage.put(op, key, out);
    }

    fn oidx_remove(&mut self, op: u64, ut: u32, fid: u16, ok: [u8; 8], obj: [u8; 16]) {
        let key = Self::oidx_key(ut, fid, ok);
        let mut ids: Vec<[u8; 16]> = match self.storage.get(&key) {
            Some(b) => b.chunks(16).filter(|c| c.len() == 16).map(|c| {
                let mut a = [0u8; 16];
                a.copy_from_slice(c);
                a
            }).collect(),
            None => return,
        };
        if let Ok(i) = ids.binary_search(&obj) {
            ids.remove(i);
        }
        if ids.is_empty() {
            let _ = self.storage.delete(op, key);
        } else {
            let mut out = Vec::with_capacity(ids.len() * 16);
            for x in &ids {
                out.extend_from_slice(x);
            }
            let _ = self.storage.put(op, key, out);
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
        self.idx_lookup(ut, fid, v).iter().any(|id| *id != self_obj)
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

    /// SP73 (columnar fast-path): the MIN (`want_max=false`) or MAX
    /// (`want_max=true`) raw bytes of an order-indexed column, read
    /// straight from the extreme of the order index — O(scan of the
    /// matching segment) instead of a full table scan. The order index
    /// is sorted by an order-preserving key, so its first/last entry is
    /// the global min/max; we fetch one row under that entry and return
    /// the column's raw bytes. `None` if the table is empty, the field
    /// is not order-indexed, or the index/row is (transiently)
    /// unavailable — the caller then falls back to the full scan, so
    /// this is purely an accelerator and never changes the answer.
    fn agg_extreme(
        &self,
        type_id: u32,
        field_id: u16,
        off: usize,
        w: usize,
        want_max: bool,
    ) -> Option<Vec<u8>> {
        let idxt = 0xFFFD_0000 | (type_id & 0xFFFF);
        let mut klo = [0u8; 16];
        klo[..2].copy_from_slice(&field_id.to_le_bytes());
        let mut khi = [0u8; 16];
        khi[..2].copy_from_slice(&field_id.to_le_bytes());
        khi[2..].copy_from_slice(&[0xFFu8; 14]);
        // Early-stopping boundary lookup — does NOT materialise the
        // whole order-index range (that would be O(n) and pointless).
        let (_, entry) = self.storage.bound_in(
            &make_key(idxt, &klo),
            &make_key(idxt, &khi),
            want_max,
        )?;
        let oid: [u8; 16] = entry.get(0..16)?.try_into().ok()?;
        let rec = self.storage.get(&make_key(type_id, &oid))?;
        Some(rec.get(off..off + w)?.to_vec())
    }

    /// #73: the same index-extreme accelerator for the SP87/SP91
    /// `0xFFFC` keyspace (CHAR/BYTES + U128/I128). Its order key is
    /// order-preserving (`vorder_key`), so the first / last bucket is
    /// the global MIN / MAX; we fetch one row under it and return the
    /// column's raw width-`w` bytes. `None` ⇒ empty / not indexed /
    /// transiently unavailable ⇒ caller falls back to the full scan,
    /// so this never changes the answer.
    fn agg_extreme_var(
        &self,
        type_id: u32,
        field_id: u16,
        off: usize,
        w: usize,
        want_max: bool,
    ) -> Option<Vec<u8>> {
        // `tag++fid` (no ok) is shorter than — hence < — every real
        // `tag++fid++ok` bucket; `tag++fid++[0xFF; w]` is ≥ all of them.
        let lo = Self::voidx_key(type_id, field_id, &[]);
        let hi = Self::voidx_key(type_id, field_id, &vec![0xFFu8; w]);
        let (_, entry) = self.storage.bound_in(&lo, &hi, want_max)?;
        let oid: [u8; 16] = entry.get(0..16)?.try_into().ok()?;
        let rec = self.storage.get(&make_key(type_id, &oid))?;
        Some(rec.get(off..off + w)?.to_vec())
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
        // Prefix scan over all (ns,field,value,*) entries: sub-linear in the
        // matching set, no fan-out, no read-modify-write.
        let prefix = Self::idx_prefix(ut, fid, v);
        let mut lo = prefix.clone();
        lo.extend_from_slice(&[0u8; 16]);
        let mut hi = prefix.clone();
        hi.extend_from_slice(&[0xFFu8; 16]);
        let mut ids = Vec::new();
        for k in self.storage.scan_prefix(&lo, &hi) {
            if k.len() == prefix.len() + 16 && k.starts_with(&prefix) {
                let mut id = [0u8; 16];
                id.copy_from_slice(&k[prefix.len()..]);
                ids.push(id);
            }
        }
        ids.sort_unstable();
        ids.dedup();
        ids
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
        for (fid, rt, _od) in &ot.fks {
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

    /// DFS the ON DELETE closure rooted at `(target_type, target_id)`.
    /// Pushes every object that must be deleted (root + CASCADE descendants)
    /// into `out`; returns `Err` if a RESTRICT edge still has children or the
    /// budget is exceeded. Pure read over committed state ⇒ deterministic.
    /// For every object being deleted, find children (not themselves being
    /// deleted) whose FK with `on_delete = SET NULL (3)` references it.
    /// Returns `(child_type, child_id, field_idx, offset, width)`, deduped.
    #[allow(clippy::type_complexity)]
    fn collect_set_null(
        &self,
        closure: &[(u32, [u8; 16])],
    ) -> Vec<(u32, [u8; 16], usize, usize, usize, Option<Vec<u8>>)> {
        let in_closure: std::collections::HashSet<(u32, [u8; 16])> =
            closure.iter().copied().collect();
        let mut seen: std::collections::HashSet<(u32, [u8; 16], usize)> =
            std::collections::HashSet::new();
        let mut out = Vec::new();
        for (dt, did) in closure {
            for ct in &self.catalog.types {
                let layout = ct.compute_layout();
                for (fid, rt, od) in &ct.fks {
                    // 3 = SET NULL, 4 = SET DEFAULT (SP86).
                    if *rt != *dt || (*od != 3 && *od != 4) {
                        continue;
                    }
                    let fi = match ct.fields.iter().position(|f| f.field_id == *fid) {
                        Some(i) => i,
                        None => continue,
                    };
                    let w = ct.fields[fi].kind.width() as usize;
                    let off = layout.offsets[fi];
                    // SET DEFAULT writes the child column's declared
                    // default (width-normalised); if there is none it
                    // degrades to SET NULL — documented, deterministic.
                    let dflt = if *od == 4 {
                        ct.defaults
                            .iter()
                            .find(|(f, _)| f == fid)
                            .map(|(_, d)| Self::norm(d, w))
                    } else {
                        None
                    };
                    let val = Self::norm(did, w);
                    for cid in self.idx_lookup(ct.type_id, *fid, &val) {
                        if in_closure.contains(&(ct.type_id, cid)) {
                            continue;
                        }
                        if seen.insert((ct.type_id, cid, fi)) {
                            out.push((
                                ct.type_id,
                                cid,
                                fi,
                                off,
                                w,
                                dflt.clone(),
                            ));
                        }
                    }
                }
            }
        }
        out
    }

    fn cascade_collect(
        &self,
        target_type: u32,
        target_id: [u8; 16],
        out: &mut Vec<(u32, [u8; 16])>,
        visited: &mut std::collections::HashSet<(u32, [u8; 16])>,
        budget: &mut u32,
    ) -> Result<(), String> {
        if !visited.insert((target_type, target_id)) {
            return Ok(()); // already scheduled (handles diamonds/cycles)
        }
        if *budget == 0 {
            return Err("ON DELETE cascade budget exceeded".into());
        }
        *budget -= 1;
        out.push((target_type, target_id));
        for ct in &self.catalog.types {
            for (fid, rt, od) in &ct.fks {
                if *rt != target_type || *od == 0 {
                    continue;
                }
                let fi = match ct.fields.iter().position(|f| f.field_id == *fid) {
                    Some(i) => i,
                    None => continue,
                };
                let w = ct.fields[fi].kind.width() as usize;
                let val = Self::norm(&target_id, w);
                let child_ids = self.idx_lookup(ct.type_id, *fid, &val);
                match *od {
                    1 => {
                        if !child_ids.is_empty() {
                            return Err(format!(
                                "ON DELETE RESTRICT: type {} field {} still references type {}",
                                ct.type_id, fid, target_type
                            ));
                        }
                    }
                    2 => {
                        for cid in child_ids {
                            self.cascade_collect(ct.type_id, cid, out, visited, budget)?;
                        }
                    }
                    // 3 = SET NULL: handled separately (a mutation, not a
                    // delete) by `collect_set_null`; skip here.
                    _ => {}
                }
            }
        }
        Ok(())
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
            Op::SeqAppend { payload } => {
                // SP79: atomic assign-next + store, ONE replicated op.
                // Counter advances strictly by VSR-replicated op order,
                // so every replica of the sequencer group assigns the
                // identical gap-free seq and converges bit-for-bit.
                let cur = self
                    .storage
                    .get(&seq_counter_key())
                    .and_then(|b| b.get(..8).map(|s| {
                        u64::from_le_bytes(s.try_into().unwrap())
                    }))
                    .unwrap_or(0);
                let s = cur + 1;
                if let Err(e) =
                    self.storage.put(op_number, seq_entry_key(s), payload)
                {
                    return OpResult::SchemaError(format!("seq store: {e}"));
                }
                if let Err(e) = self.storage.put(
                    op_number,
                    seq_counter_key(),
                    s.to_le_bytes().to_vec(),
                ) {
                    return OpResult::SchemaError(format!("seq counter: {e}"));
                }
                OpResult::Got(s.to_le_bytes().to_vec())
            }
            Op::SeqRead { from, limit } => {
                let lo = seq_entry_key(from.max(1));
                let hi = seq_entry_key(u64::MAX);
                let mut out = Vec::new();
                let mut n = 0u32;
                for (k, v) in self.storage.scan_range(&lo, &hi) {
                    if limit != 0 && n >= limit {
                        break;
                    }
                    let seq = k
                        .get(12..20)
                        .map(|b| u64::from_be_bytes(b.try_into().unwrap()))
                        .unwrap_or(0);
                    out.extend_from_slice(&seq.to_le_bytes());
                    out.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    out.extend_from_slice(&v);
                    n += 1;
                }
                OpResult::Got(out)
            }
            Op::XshardApply { seq, ops } => {
                // SP80: apply this shard's slice of the cross-shard txn
                // at global `seq`. Strictly in-order and idempotent: the
                // shard processes every global seq exactly once (its
                // slice, or empty to just advance), tracking a cursor in
                // a reserved keyspace (part of the digest ⇒ every replica
                // of this shard group advances identically). The ordered
                // sequencer log is the commit point.
                let c = self
                    .storage
                    .get(&xshard_cursor_key())
                    .and_then(|b| b.get(..8).map(|s| {
                        u64::from_le_bytes(s.try_into().unwrap())
                    }))
                    .unwrap_or(0);
                if seq <= c {
                    return OpResult::Ok; // already applied (safe re-drive)
                }
                if seq != c + 1 {
                    return OpResult::SchemaError(format!(
                        "xshard out of order: have {c}, got {seq}"
                    ));
                }
                for o in &ops {
                    let ok = matches!(
                        o,
                        Op::Create { .. }
                            | Op::Update { .. }
                            | Op::Delete { .. }
                    );
                    if !ok {
                        return OpResult::SchemaError(
                            "xshard slice: only Create/Update/Delete \
                             allowed"
                                .into(),
                        );
                    }
                }
                let own = !self.storage.in_txn();
                if own {
                    self.storage.begin_txn();
                }
                for (i, o) in ops.into_iter().enumerate() {
                    let r = self.apply(op_number + i as u64, o);
                    // NOTE (slice-3 boundary): a slice op failing here is
                    // a per-shard abort. Deterministic cross-shard abort
                    // *agreement* (so all shards abort iff any would) is
                    // slice 4; this slice's tests use non-conflicting
                    // slices. We still roll this shard's slice back
                    // atomically rather than apply it half-way.
                    if matches!(
                        r,
                        OpResult::Exists
                            | OpResult::NotFound
                            | OpResult::SchemaError(_)
                            | OpResult::Constraint(_)
                    ) {
                        if own {
                            self.storage.abort_txn();
                            if let Some(cc) = self.cache.as_mut() {
                                cc.clear();
                            }
                        }
                        return r;
                    }
                }
                // Advance the cursor atomically with the slice.
                if let Err(e) = self.storage.put(
                    op_number,
                    xshard_cursor_key(),
                    seq.to_le_bytes().to_vec(),
                ) {
                    if own {
                        self.storage.abort_txn();
                    }
                    return OpResult::SchemaError(format!("xshard cursor: {e}"));
                }
                if own {
                    match self.storage.commit_txn() {
                        Ok(()) => OpResult::Ok,
                        Err(e) => {
                            OpResult::SchemaError(format!("xshard commit: {e}"))
                        }
                    }
                } else {
                    OpResult::Ok
                }
            }
            Op::SeqAppendOnce { key, payload } => {
                // SP81: exactly-once append. The map verifies the FULL
                // key, so a 128-bit-hash collision can only ever cause a
                // (astronomically unlikely) MISSED dedup, never a FALSE
                // one — correctness-safe.
                let dk = seq_dedup_key(&key);
                if let Some(v) = self.storage.get(&dk) {
                    if v.len() >= 8 && v.get(8..) == Some(key.as_slice()) {
                        return OpResult::Got(v[..8].to_vec());
                    }
                }
                let cur = self
                    .storage
                    .get(&seq_counter_key())
                    .and_then(|b| b.get(..8).map(|s| {
                        u64::from_le_bytes(s.try_into().unwrap())
                    }))
                    .unwrap_or(0);
                let s = cur + 1;
                if let Err(e) =
                    self.storage.put(op_number, seq_entry_key(s), payload)
                {
                    return OpResult::SchemaError(format!("seq store: {e}"));
                }
                let _ = self.storage.put(
                    op_number,
                    seq_counter_key(),
                    s.to_le_bytes().to_vec(),
                );
                let mut rec = s.to_le_bytes().to_vec();
                rec.extend_from_slice(&key);
                let _ = self.storage.put(op_number, dk, rec);
                OpResult::Got(s.to_le_bytes().to_vec())
            }

            Op::XshardDecide { seq, ops } => {
                // SP81 phase 1: persist a STABLE verdict for `seq` (a
                // pure function of committed state at decide time), and
                // apply NOTHING. Idempotent — a re-decide returns the
                // recorded verdict even if state later changed, so every
                // router re-derives the same global decision.
                let vk = xvote_key(seq);
                if let Some(v) = self.storage.get(&vk) {
                    return OpResult::Got(v);
                }
                for o in &ops {
                    if !matches!(
                        o,
                        Op::Create { .. } | Op::Update { .. } | Op::Delete { .. }
                    ) {
                        return OpResult::SchemaError(
                            "xshard decide: only Create/Update/Delete".into(),
                        );
                    }
                }
                let own = !self.storage.in_txn();
                if own {
                    self.storage.begin_txn();
                }
                let mut pass = true;
                for (i, o) in ops.into_iter().enumerate() {
                    let r = self.apply(op_number + i as u64, o);
                    if matches!(
                        r,
                        OpResult::Exists
                            | OpResult::NotFound
                            | OpResult::SchemaError(_)
                            | OpResult::Constraint(_)
                    ) {
                        pass = false;
                        break;
                    }
                }
                if own {
                    self.storage.abort_txn(); // dry-run: never apply
                    if let Some(c) = self.cache.as_mut() {
                        c.clear();
                    }
                }
                let verdict = vec![pass as u8];
                let _ = self.storage.put(op_number, vk, verdict.clone());
                OpResult::Got(verdict)
            }

            Op::XshardCommit { seq, ops, commit } => {
                // SP81 phase 2: same in-order/idempotent cursor as
                // XshardApply, but gated by the deterministic global
                // decision. `commit=false` ⇒ deterministic atomic SKIP
                // (advance the cursor, apply nothing) so all shards stay
                // lockstep and the txn is all-or-none.
                let c = self
                    .storage
                    .get(&xshard_cursor_key())
                    .and_then(|b| b.get(..8).map(|s| {
                        u64::from_le_bytes(s.try_into().unwrap())
                    }))
                    .unwrap_or(0);
                if seq <= c {
                    return OpResult::Ok;
                }
                if seq != c + 1 {
                    return OpResult::SchemaError(format!(
                        "xshard out of order: have {c}, got {seq}"
                    ));
                }
                for o in &ops {
                    if !matches!(
                        o,
                        Op::Create { .. } | Op::Update { .. } | Op::Delete { .. }
                    ) {
                        return OpResult::SchemaError(
                            "xshard commit: only Create/Update/Delete".into(),
                        );
                    }
                }
                let own = !self.storage.in_txn();
                if own {
                    self.storage.begin_txn();
                }
                if commit {
                    for (i, o) in ops.into_iter().enumerate() {
                        let r = self.apply(op_number + i as u64, o);
                        if matches!(
                            r,
                            OpResult::Exists
                                | OpResult::NotFound
                                | OpResult::SchemaError(_)
                                | OpResult::Constraint(_)
                        ) {
                            if own {
                                self.storage.abort_txn();
                                if let Some(cc) = self.cache.as_mut() {
                                    cc.clear();
                                }
                            }
                            return r;
                        }
                    }
                }
                if let Err(e) = self.storage.put(
                    op_number,
                    xshard_cursor_key(),
                    seq.to_le_bytes().to_vec(),
                ) {
                    if own {
                        self.storage.abort_txn();
                    }
                    return OpResult::SchemaError(format!("xshard cursor: {e}"));
                }
                if own {
                    match self.storage.commit_txn() {
                        Ok(()) => OpResult::Ok,
                        Err(e) => {
                            OpResult::SchemaError(format!("xshard commit: {e}"))
                        }
                    }
                } else {
                    OpResult::Ok
                }
            }

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
                    ordered: Vec::new(),
                    composite: Vec::new(),
                    // SP86: per-column defaults ride a backward-compat
                    // trailer in the type-def blob; the parser keyed
                    // them by positional (1-based) id, which is exactly
                    // the field id assigned above.
                    defaults: kessel_catalog::decode_type_defaults(&def),
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
                    .map(|t| {
                        !t.indexes.is_empty()
                            || !t.ordered.is_empty()
                            || !t.composite.is_empty()
                    })
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
                match self.storage.put(op_number, key.clone(), record) {
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
                    .map(|t| {
                        !t.indexes.is_empty()
                            || !t.ordered.is_empty()
                            || !t.composite.is_empty()
                    })
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
                // SP76: blobs the OLD record referenced but the NEW one
                // no longer does are now unreachable — reclaim them.
                let old_h = old
                    .as_deref()
                    .map(|r| self.overflow_handles(type_id, r))
                    .unwrap_or_default();
                let new_h = self.overflow_handles(type_id, &record);
                let freed: Vec<u64> =
                    old_h.into_iter().filter(|h| !new_h.contains(h)).collect();
                let rec_idx = if need_idx { Some(record.clone()) } else { None };
                let cached = self.cache.as_mut().map(|_| record.clone());
                match self.storage.put(op_number, key.clone(), record) {
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
                        self.reclaim_overflow(op_number, &freed);
                        OpResult::Ok
                    }
                    Err(e) => OpResult::SchemaError(format!("store: {e}")),
                }
            }

            Op::UpdateSet { type_id, id, sets } => {
                // SP84: deterministic server-side RMW as ONE replicated
                // op, so SQL UPDATE composes inside Op::Txn (the read is
                // overlay-aware ⇒ read-your-writes within the batch).
                // We decode the *current* record (overlay-aware get),
                // splice the set fields, re-encode, and delegate to the
                // proven Op::Update path (triggers / NOT NULL / UNIQUE /
                // FK / CHECK / balance / indexes / overflow GC).
                let key = make_key(type_id, &id.0);
                let old = match self.storage.get(&key) {
                    Some(r) => r,
                    None => return OpResult::NotFound,
                };
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => {
                        return OpResult::SchemaError(format!("no type {type_id}"))
                    }
                };
                let mut vals = match kessel_codec::decode(&ot, &old) {
                    Ok(v) => v,
                    Err(e) => {
                        return OpResult::SchemaError(format!(
                            "updateset decode: {e:?}"
                        ))
                    }
                };
                for (fid, raw) in &sets {
                    let i = match ot.fields.iter().position(|f| f.field_id == *fid)
                    {
                        Some(i) => i,
                        None => {
                            return OpResult::SchemaError(format!(
                                "updateset: no field {fid}"
                            ))
                        }
                    };
                    let w = ot.fields[i].kind.width() as usize;
                    vals[i] = kessel_codec::value_from_raw(
                        ot.fields[i].kind,
                        &Self::norm(raw, w),
                    );
                }
                let record = match kessel_codec::encode(&ot, &vals) {
                    Ok(r) => r,
                    Err(e) => {
                        return OpResult::SchemaError(format!(
                            "updateset encode: {e:?}"
                        ))
                    }
                };
                self.apply(op_number, Op::Update { type_id, id, record })
            }

            Op::Delete { type_id, id } => {
                let key = make_key(type_id, &id.0);
                if self.storage.get(&key).is_none() {
                    return OpResult::NotFound;
                }
                // Compute the full ON DELETE closure (root + cascade
                // descendants); RESTRICT anywhere aborts with zero effect.
                let mut closure: Vec<(u32, [u8; 16])> = Vec::new();
                let mut visited: std::collections::HashSet<(u32, [u8; 16])> =
                    std::collections::HashSet::new();
                let mut budget: u32 = 200_000;
                if let Err(e) =
                    self.cascade_collect(type_id, id.0, &mut closure, &mut visited, &mut budget)
                {
                    return OpResult::Constraint(e);
                }
                // ON DELETE SET NULL targets: children (not themselves being
                // deleted) whose FK references something in the closure.
                let set_null = self.collect_set_null(&closure);
                // Atomic: if not already inside a Txn, wrap the whole effect.
                let own_txn = !self.storage.in_txn();
                if own_txn {
                    self.storage.begin_txn();
                }
                for (ct, cid, fi, off, w, dflt) in &set_null {
                    let k = make_key(*ct, cid);
                    let old = match self.storage.get(&k) {
                        Some(r) => r,
                        None => continue,
                    };
                    let mut new = old.clone();
                    if let Some(d) = dflt {
                        // SET DEFAULT: write the default value bytes; it
                        // is a present value, so do NOT set the null bit.
                        if let Some(slot) = new.get_mut(*off..*off + *w) {
                            slot.copy_from_slice(&Self::norm(d, *w));
                        }
                    } else {
                        // SET NULL: zero the field and set the null bit
                        // (codec-shaped records).
                        for b in
                            new.get_mut(*off..*off + *w).into_iter().flatten()
                        {
                            *b = 0;
                        }
                        if let Some(t) = self.catalog.get(*ct) {
                            if Self::is_codec_record(t, &new) {
                                let bit =
                                    kessel_catalog::SCHEMA_VER_BYTES + 2 + fi / 8;
                                if bit < new.len() {
                                    new[bit] |= 1 << (fi % 8);
                                }
                            }
                        }
                    }
                    if let Err(e) = self.storage.put(op_number, k.clone(), new.clone()) {
                        if own_txn {
                            self.storage.abort_txn();
                        }
                        return OpResult::SchemaError(format!("set-null store: {e}"));
                    }
                    if let Some(c) = self.cache.as_mut() {
                        c.invalidate(&k);
                    }
                    self.idx_maintain(op_number, *ct, *cid, Some(&old), Some(&new));
                }
                for (t, oid) in &closure {
                    let k = make_key(*t, oid);
                    let oldr = self.storage.get(&k);
                    // SP76: the deleted row's overflow blobs become
                    // unreachable — reclaim them (same op, atomic in the
                    // delete's own txn; handles are deterministic).
                    let freed = oldr
                        .as_deref()
                        .map(|r| self.overflow_handles(*t, r))
                        .unwrap_or_default();
                    if let Err(e) = self.storage.delete(op_number, k.clone()) {
                        if own_txn {
                            self.storage.abort_txn();
                        }
                        return OpResult::SchemaError(format!("store: {e}"));
                    }
                    if let Some(c) = self.cache.as_mut() {
                        c.invalidate(&k);
                    }
                    self.idx_maintain(op_number, *t, *oid, oldr.as_deref(), None);
                    self.reclaim_overflow(op_number, &freed);
                }
                if own_txn {
                    if let Err(e) = self.storage.commit_txn() {
                        return OpResult::SchemaError(format!("txn commit: {e}"));
                    }
                }
                OpResult::Ok
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

            Op::Describe { type_id } => match self.catalog.get(type_id) {
                Some(t) => OpResult::Got(encode_type_def(&t.name, &t.fields)),
                None => OpResult::NotFound,
            },

            Op::DropType { type_id } => {
                if self.catalog.get(type_id).is_none() {
                    return OpResult::NotFound;
                }
                // Referential integrity: refuse to drop a table another
                // table still points at via a foreign key (no effect).
                if let Some(t) = self.catalog.types.iter().find(|t| {
                    t.type_id != type_id
                        && t.fks.iter().any(|(_, rt, _)| *rt == type_id)
                }) {
                    return OpResult::Constraint(format!(
                        "DROP TABLE: type {type_id} is referenced by a foreign \
                         key from \"{}\"",
                        t.name
                    ));
                }
                let own_txn = !self.storage.in_txn();
                if own_txn {
                    self.storage.begin_txn();
                }
                // Remove every row together with its index entries (the
                // type is still in the catalog so `idx_maintain` resolves
                // the schema; we drop it afterwards).
                let lo = make_key(type_id, &[0u8; 16]);
                let hi = make_key(type_id, &[0xFFu8; 16]);
                for (k, rec) in self.storage.scan_range(&lo, &hi) {
                    let mut obj = [0u8; 16];
                    if k.len() >= 20 {
                        obj.copy_from_slice(&k[4..20]);
                    }
                    self.idx_maintain(op_number, type_id, obj, Some(&rec), None);
                    if let Err(e) = self.storage.delete(op_number, k) {
                        if own_txn {
                            self.storage.abort_txn();
                        }
                        return OpResult::SchemaError(format!("drop: {e}"));
                    }
                }
                self.catalog.types.retain(|t| t.type_id != type_id);
                let pc = self.persist_catalog(op_number);
                if !matches!(pc, OpResult::Ok) {
                    if own_txn {
                        self.storage.abort_txn();
                    }
                    return pc;
                }
                if own_txn {
                    if let Err(e) = self.storage.commit_txn() {
                        return OpResult::SchemaError(format!("drop commit: {e}"));
                    }
                }
                OpResult::Ok
            }

            Op::DropIndex { type_id, fields } => {
                // SP74: drop the secondary index(es) on exactly `fields`
                // and delete their LSM entries. Query results are
                // unaffected — the planner falls back to a verified scan
                // (the QueryRows program-verify invariant guarantees the
                // same answer), only without acceleration.
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                // Delete every index entry whose key has the raw prefix
                // `tag` (eq/composite live in the 0xFFFE keyspace,
                // ordered in 0xFFFD). Keys are a contiguous range, so a
                // prefix .. prefix++0xFF… scan covers exactly them.
                let mut drop_prefix = |me: &mut Self, prefix: Vec<u8>| {
                    let mut hi = prefix.clone();
                    hi.extend_from_slice(&[0xFFu8; 64]);
                    for (k, _) in me.storage.scan_range(&prefix, &hi) {
                        let _ = me.storage.delete(op_number, k);
                    }
                };
                let mut found = false;
                if fields.len() == 1 {
                    let fid = fields[0];
                    if ot.indexes.contains(&fid) {
                        let pre = Self::idx_prefix(type_id, fid, &[]);
                        drop_prefix(self, pre);
                        found = true;
                    }
                    if ot.ordered.contains(&fid) {
                        // Numeric 0xFFFD entries (prefix = tag(4)++fid(2)).
                        let idxt = 0xFFFD_0000 | (type_id & 0xFFFF);
                        let mut id = [0u8; 16];
                        id[..2].copy_from_slice(&fid.to_le_bytes());
                        let pre = make_key(idxt, &id);
                        drop_prefix(self, pre[..6].to_vec());
                        // SP90: CHAR/BYTES 0xFFFC entries, if any.
                        let mut vpre =
                            (0xFFFC_0000 | (type_id & 0xFFFF)).to_le_bytes().to_vec();
                        vpre.extend_from_slice(&fid.to_le_bytes());
                        drop_prefix(self, vpre);
                        found = true;
                    }
                    if found {
                        if let Some(t) = self.catalog.get_mut(type_id) {
                            t.indexes.retain(|f| *f != fid);
                            t.unique.retain(|f| *f != fid);
                            t.ordered.retain(|f| *f != fid);
                        }
                    }
                } else if let Some(ci) =
                    ot.composite.iter().position(|c| *c == fields)
                {
                    let cfid = Self::composite_fid(ci);
                    let pre = Self::idx_prefix(type_id, cfid, &[]);
                    drop_prefix(self, pre);
                    // Empty the slot rather than removing it — composite
                    // entries are keyed by slot index, so removing would
                    // renumber later composites and orphan their keys.
                    if let Some(t) = self.catalog.get_mut(type_id) {
                        t.composite[ci].clear();
                    }
                    found = true;
                }
                if !found {
                    return OpResult::NotFound;
                }
                self.persist_catalog(op_number)
            }

            Op::RenameField { type_id, field_id, name } => {
                // SP75: catalog-only. Indexes are keyed by field id, so
                // data and index entries are untouched.
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t,
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                if !ot.fields.iter().any(|f| f.field_id == field_id) {
                    return OpResult::SchemaError(format!("no field {field_id}"));
                }
                if ot.fields.iter().any(|f| f.name == name && f.field_id != field_id) {
                    return OpResult::Constraint(format!(
                        "RENAME COLUMN: name \"{name}\" already in use"
                    ));
                }
                if let Some(t) = self.catalog.get_mut(type_id) {
                    if let Some(f) =
                        t.fields.iter_mut().find(|f| f.field_id == field_id)
                    {
                        f.name = name;
                    }
                }
                self.persist_catalog(op_number)
            }

            Op::DropField { type_id, field_id } => {
                // SP75: physically remove a column — re-encode every row
                // without it and shrink the schema, so nothing
                // downstream needs a "dropped" special case. Conservative
                // guards keep it correct rather than clever.
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                let fi = match ot.fields.iter().position(|f| f.field_id == field_id)
                {
                    Some(i) => i,
                    None => return OpResult::SchemaError(format!("no field {field_id}")),
                };
                if ot.fields.len() == 1 {
                    return OpResult::SchemaError(
                        "DROP COLUMN: a table must keep at least one column \
                         (use DROP TABLE)"
                            .into(),
                    );
                }
                if matches!(
                    ot.fields[fi].kind,
                    kessel_catalog::FieldKind::OverflowRef
                ) {
                    return OpResult::SchemaError(
                        "DROP COLUMN: overflow columns are not supported".into(),
                    );
                }
                if ot.fks.iter().any(|(f, _, _)| *f == field_id) {
                    return OpResult::Constraint(
                        "DROP COLUMN: column backs a foreign key".into(),
                    );
                }
                if !ot.checks.is_empty() || !ot.triggers.is_empty() {
                    return OpResult::SchemaError(
                        "DROP COLUMN: not supported on a table with CHECK \
                         constraints or triggers (their programs are \
                         position-encoded)"
                            .into(),
                    );
                }
                let own_txn = !self.storage.in_txn();
                if own_txn {
                    self.storage.begin_txn();
                }
                // Drop the column's own index entries + catalog membership
                // (surviving fields' index entries are keyed by
                // (field_id,value) and their values do not change, so they
                // stay valid — no rebuild needed).
                let mut prefixes: Vec<Vec<u8>> = Vec::new();
                if ot.indexes.contains(&field_id) {
                    prefixes.push(Self::idx_prefix(type_id, field_id, &[]));
                }
                if ot.ordered.contains(&field_id) {
                    let idxt = 0xFFFD_0000 | (type_id & 0xFFFF);
                    let mut id = [0u8; 16];
                    id[..2].copy_from_slice(&field_id.to_le_bytes());
                    prefixes.push(make_key(idxt, &id)[..6].to_vec());
                    // SP90: CHAR/BYTES 0xFFFC ordered-index entries.
                    let mut vpre =
                        (0xFFFC_0000 | (type_id & 0xFFFF)).to_le_bytes().to_vec();
                    vpre.extend_from_slice(&field_id.to_le_bytes());
                    prefixes.push(vpre);
                }
                for (ci, members) in ot.composite.iter().enumerate() {
                    if members.contains(&field_id) {
                        prefixes.push(Self::idx_prefix(
                            type_id,
                            Self::composite_fid(ci),
                            &[],
                        ));
                    }
                }
                for pre in prefixes {
                    let mut hi = pre.clone();
                    hi.extend_from_slice(&[0xFFu8; 64]);
                    for (k, _) in self.storage.scan_range(&pre, &hi) {
                        let _ = self.storage.delete(op_number, k);
                    }
                }
                // Shrunk schema used to re-encode every row.
                let mut new_ot = ot.clone();
                new_ot.fields.remove(fi);
                let lo = make_key(type_id, &[0u8; 16]);
                let hi = make_key(type_id, &[0xFFu8; 16]);
                for (k, rec) in self.storage.scan_range(&lo, &hi) {
                    let mut vals = match kessel_codec::decode(&ot, &rec) {
                        Ok(v) => v,
                        Err(e) => {
                            if own_txn {
                                self.storage.abort_txn();
                            }
                            return OpResult::SchemaError(format!(
                                "DROP COLUMN decode: {e:?}"
                            ));
                        }
                    };
                    vals.remove(fi);
                    let nr = match kessel_codec::encode(&new_ot, &vals) {
                        Ok(r) => r,
                        Err(e) => {
                            if own_txn {
                                self.storage.abort_txn();
                            }
                            return OpResult::SchemaError(format!(
                                "DROP COLUMN encode: {e:?}"
                            ));
                        }
                    };
                    if let Err(e) = self.storage.put(op_number, k, nr) {
                        if own_txn {
                            self.storage.abort_txn();
                        }
                        return OpResult::SchemaError(format!("DROP COLUMN: {e}"));
                    }
                }
                if let Some(t) = self.catalog.get_mut(type_id) {
                    t.fields.remove(fi);
                    t.indexes.retain(|f| *f != field_id);
                    t.unique.retain(|f| *f != field_id);
                    t.ordered.retain(|f| *f != field_id);
                    for c in t.composite.iter_mut() {
                        if c.contains(&field_id) {
                            c.clear();
                        }
                    }
                    t.schema_ver += 1;
                }
                let pc = self.persist_catalog(op_number);
                if !matches!(pc, OpResult::Ok) {
                    if own_txn {
                        self.storage.abort_txn();
                    }
                    return pc;
                }
                if own_txn {
                    if let Err(e) = self.storage.commit_txn() {
                        return OpResult::SchemaError(format!(
                            "DROP COLUMN commit: {e}"
                        ));
                    }
                }
                OpResult::Ok
            }

            Op::AddBalanceGuard { type_id, field_id } => {
                // SP77: a named non-negative invariant. It IS a CHECK
                // (`field >= 0`); we validate the column is signed
                // numeric (so "negative" is meaningful — a guard on an
                // unsigned column would be vacuous, almost always a
                // mistake) and then reuse the proven AddCheck path
                // (existing-row validation, persistence, per-write
                // enforcement incl. inside Txn). No new catalog format.
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t,
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                let fk = match ot.fields.iter().find(|f| f.field_id == field_id) {
                    Some(f) => f.kind,
                    None => return OpResult::SchemaError(format!("no field {field_id}")),
                };
                use kessel_catalog::FieldKind::*;
                if !matches!(fk, I8 | I16 | I32 | I64 | I128 | Fixed { .. }) {
                    return OpResult::SchemaError(
                        "balance guard requires a signed numeric column \
                         (a guard on an unsigned column is always true)"
                            .into(),
                    );
                }
                let program = kessel_expr::Program::new()
                    .load(field_id)
                    .push_int(0)
                    .ge()
                    .bytes();
                // Delegate: identical effect to ADD CHECK (col >= 0),
                // including rejecting the add if a current row is
                // already negative.
                self.apply(op_number, Op::AddCheck { type_id, program })
            }

            Op::Join { left_type, right_type, left_field, right_field, limit } => {
                let lt = match self.catalog.get(left_type) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {left_type}")),
                };
                let rt = match self.catalog.get(right_type) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {right_type}")),
                };
                let pos = |ot: &kessel_catalog::ObjectType, fid: u16| {
                    ot.fields.iter().position(|f| f.field_id == fid).map(|i| {
                        (ot.compute_layout().offsets[i], ot.fields[i].kind.width() as usize)
                    })
                };
                let (loff, lw) = match pos(&lt, left_field) {
                    Some(p) => p,
                    None => return OpResult::SchemaError("no left join field".into()),
                };
                let (roff, rw) = match pos(&rt, right_field) {
                    Some(p) => p,
                    None => return OpResult::SchemaError("no right join field".into()),
                };
                if lw != rw {
                    return OpResult::SchemaError(
                        "join fields must have equal width".into(),
                    );
                }
                // Build the right side keyed by the join value (scan order =>
                // deterministic per-key ordering).
                let mut map: std::collections::HashMap<Vec<u8>, Vec<Vec<u8>>> =
                    std::collections::HashMap::new();
                let rlo = make_key(right_type, &[0u8; 16]);
                let rhi = make_key(right_type, &[0xFFu8; 16]);
                for (_, rr) in self.storage.scan_range(&rlo, &rhi) {
                    if let Some(k) = rr.get(roff..roff + rw) {
                        map.entry(k.to_vec()).or_default().push(rr);
                    }
                }
                // SP72 — typed (self-describing) result. The joined output
                // carries its own column schema so the client decodes it
                // generically (no DESCRIBE, no per-shape special-casing).
                // The schema is a synthetic type def: every left column as
                // `<lt>.<col>` then every right column as `<rt>.<col>`,
                // same kinds/order. A joined row is exactly the left record
                // bytes followed by the right record bytes — which, because
                // record layout is sequential by field, is precisely a
                // valid record of that combined type. Payload:
                //   [b"KTR1"][u32 deflen][type def][ [u32 reclen][rec] ]*
                let mut combined: Vec<kessel_catalog::Field> =
                    Vec::with_capacity(lt.fields.len() + rt.fields.len());
                let mut fid: u16 = 0;
                for (src, f) in lt
                    .fields
                    .iter()
                    .map(|f| (&lt.name, f))
                    .chain(rt.fields.iter().map(|f| (&rt.name, f)))
                {
                    combined.push(kessel_catalog::Field {
                        field_id: fid,
                        name: format!("{src}.{}", f.name),
                        kind: f.kind,
                        nullable: f.nullable,
                    });
                    fid += 1;
                }
                let jname = format!("{}+{}", lt.name, rt.name);
                let typedef =
                    kessel_catalog::encode_type_def(&jname, &combined);
                let cot = kessel_catalog::ObjectType::from_def(
                    jname,
                    combined.clone(),
                );
                let mut out = Vec::with_capacity(64 + typedef.len());
                out.extend_from_slice(b"KTR1");
                out.extend_from_slice(&(typedef.len() as u32).to_le_bytes());
                out.extend_from_slice(&typedef);
                // Probe with the left side in key order. A joined row is
                // built by DECODING each side against its own type and
                // re-ENCODING the concatenated values against the combined
                // type — raw byte concat would be wrong, since every record
                // carries its own header + null bitmap.
                let llo = make_key(left_type, &[0u8; 16]);
                let lhi = make_key(left_type, &[0xFFu8; 16]);
                let mut n = 0u32;
                'outer: for (_, lr) in self.storage.scan_range(&llo, &lhi) {
                    let k = match lr.get(loff..loff + lw) {
                        Some(k) => k,
                        None => continue,
                    };
                    if let Some(rs) = map.get(k) {
                        let lv = match kessel_codec::decode(&lt, &lr) {
                            Ok(v) => v,
                            Err(e) => {
                                return OpResult::SchemaError(format!(
                                    "join decode left: {e:?}"
                                ))
                            }
                        };
                        for rr in rs {
                            if limit != 0 && n >= limit {
                                break 'outer;
                            }
                            let rv = match kessel_codec::decode(&rt, rr) {
                                Ok(v) => v,
                                Err(e) => {
                                    return OpResult::SchemaError(format!(
                                        "join decode right: {e:?}"
                                    ))
                                }
                            };
                            let mut row = lv.clone();
                            row.extend(rv);
                            let rec = match kessel_codec::encode(&cot, &row) {
                                Ok(r) => r,
                                Err(e) => {
                                    return OpResult::SchemaError(format!(
                                        "join encode: {e:?}"
                                    ))
                                }
                            };
                            out.extend_from_slice(
                                &(rec.len() as u32).to_le_bytes(),
                            );
                            out.extend_from_slice(&rec);
                            n += 1;
                        }
                    }
                }
                OpResult::Got(out)
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
                let ids = self.idx_lookup(type_id, field_id, &v);
                let mut out = Vec::with_capacity(ids.len() * 16);
                for id in ids {
                    out.extend_from_slice(&id);
                }
                OpResult::Got(out)
            }

            Op::AddCompositeIndex { type_id, fields } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                if fields.is_empty() {
                    return OpResult::SchemaError("composite index needs ≥1 field".into());
                }
                for fid in &fields {
                    match ot.fields.iter().position(|f| f.field_id == *fid) {
                        Some(i) if !matches!(
                            ot.fields[i].kind,
                            kessel_catalog::FieldKind::OverflowRef
                        ) => {}
                        Some(_) => {
                            return OpResult::SchemaError(
                                "cannot composite-index an OverflowRef field".into(),
                            )
                        }
                        None => return OpResult::SchemaError(format!("no field {fid}")),
                    }
                }
                if ot.composite.iter().any(|c| *c == fields) {
                    return OpResult::Ok; // idempotent
                }
                let ci_no = ot.composite.len();
                if let Some(t) = self.catalog.get_mut(type_id) {
                    t.composite.push(fields.clone());
                }
                if let OpResult::SchemaError(e) = self.persist_catalog(op_number) {
                    return OpResult::SchemaError(e);
                }
                let cfid = Self::composite_fid(ci_no);
                let lo = make_key(type_id, &[0u8; 16]);
                let hi = make_key(type_id, &[0xFFu8; 16]);
                for (k, rec) in self.storage.scan_range(&lo, &hi) {
                    if let Some(c) = Self::composite_concat(&ot, &fields, &rec) {
                        let mut obj = [0u8; 16];
                        obj.copy_from_slice(&k[4..20]);
                        self.idx_add(op_number, type_id, cfid, &c, obj);
                    }
                }
                OpResult::Ok
            }

            Op::FindByComposite { type_id, fields, values } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                let ci_no = match ot.composite.iter().position(|c| *c == fields) {
                    Some(n) => n,
                    None => {
                        return OpResult::SchemaError("no such composite index".into())
                    }
                };
                if values.len() != fields.len() {
                    return OpResult::SchemaError("values/fields arity mismatch".into());
                }
                // Normalize each value to its field width, concatenated.
                let layout = ot.compute_layout();
                let mut concat = Vec::new();
                for (fid, val) in fields.iter().zip(&values) {
                    let i = match ot.fields.iter().position(|f| f.field_id == *fid) {
                        Some(i) => i,
                        None => return OpResult::SchemaError(format!("no field {fid}")),
                    };
                    let w = ot.fields[i].kind.width() as usize;
                    let _ = layout.offsets[i];
                    let mut v = val.clone();
                    v.resize(w, 0);
                    concat.extend_from_slice(&v);
                }
                let ids = self.idx_lookup(type_id, Self::composite_fid(ci_no), &concat);
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
                // Validate current data has no duplicate value for this
                // field, directly over the data rows (representation-
                // independent and correct).
                let dlo = make_key(type_id, &[0u8; 16]);
                let dhi = make_key(type_id, &[0xFFu8; 16]);
                let mut seen: std::collections::HashSet<Vec<u8>> =
                    std::collections::HashSet::new();
                for (_, rec) in self.storage.scan_range(&dlo, &dhi) {
                    if let Some(v) = rec.get(off..off + w) {
                        if !seen.insert(v.to_vec()) {
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

            Op::AddForeignKey { type_id, field_id, ref_type_id, on_delete } => {
                if on_delete > 4 {
                    return OpResult::SchemaError(
                        "on_delete must be 0|1|2|3|4 (0=NoAction 1=Restrict \
                         2=Cascade 3=SetNull 4=SetDefault)"
                            .into(),
                    );
                }
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
                if ot.fks.iter().any(|(f, r, _)| *f == field_id && *r == ref_type_id) {
                    return OpResult::Ok; // idempotent
                }
                let layout = ot.compute_layout();
                let off = layout.offsets[i];
                let w = ot.fields[i].kind.width() as usize;
                let lo = make_key(type_id, &[0u8; 16]);
                let hi = make_key(type_id, &[0xFFu8; 16]);
                // Validate existing rows (same scope as enforcement).
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
                // RESTRICT/CASCADE need a reverse lookup -> ensure an index
                // on the FK field (build + backfill if absent).
                if on_delete != 0 && !ot.indexes.contains(&field_id) {
                    if let Some(t) = self.catalog.get_mut(type_id) {
                        t.indexes.push(field_id);
                    }
                    if let OpResult::SchemaError(e) = self.persist_catalog(op_number) {
                        return OpResult::SchemaError(e);
                    }
                    for (k, rec) in self.storage.scan_range(&lo, &hi) {
                        if let Some(v) = rec.get(off..off + w) {
                            let mut obj = [0u8; 16];
                            obj.copy_from_slice(&k[4..20]);
                            let v = v.to_vec();
                            self.idx_add(op_number, type_id, field_id, &v, obj);
                        }
                    }
                }
                if let Some(t) = self.catalog.get_mut(type_id) {
                    t.fks.push((field_id, ref_type_id, on_delete));
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

            Op::QueryExpr { type_id, program } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                // Arbitrary boolean filter via the deterministic VM. Filtered
                // scan over the type's contiguous key range; read-only.
                let lo = make_key(type_id, &[0u8; 16]);
                let hi = make_key(type_id, &[0xFFu8; 16]);
                let mut matched: Vec<[u8; 16]> = Vec::new();
                for (k, rec) in self.storage.scan_range(&lo, &hi) {
                    match kessel_expr::eval(&program, &ot, &rec) {
                        Ok(true) => {
                            let mut id = [0u8; 16];
                            id.copy_from_slice(&k[4..20]);
                            matched.push(id);
                        }
                        Ok(false) => {}
                        Err(e) => {
                            return OpResult::SchemaError(format!("query program: {e:?}"))
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

            Op::Select { type_id, program, limit } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                // Filtered scan returning up to `limit` rows as
                // length-prefixed record blobs (sorted by key for
                // deterministic output). Read-only.
                let lo = make_key(type_id, &[0u8; 16]);
                let hi = make_key(type_id, &[0xFFu8; 16]);
                let mut out = Vec::new();
                let mut n = 0u32;
                for (_, rec) in self.storage.scan_range(&lo, &hi) {
                    if limit != 0 && n >= limit {
                        break;
                    }
                    match kessel_expr::eval(&program, &ot, &rec) {
                        Ok(true) => {
                            out.extend_from_slice(&(rec.len() as u32).to_le_bytes());
                            out.extend_from_slice(&rec);
                            n += 1;
                        }
                        Ok(false) => {}
                        Err(e) => {
                            return OpResult::SchemaError(format!("select program: {e:?}"))
                        }
                    }
                }
                OpResult::Got(out)
            }

            Op::QueryRows { type_id, eq_preds, program, limit, range_preds } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                // Planner: intersect indexed equality predicates to narrow
                // candidates; the full `program` still verifies every row so
                // the result is identical to Select (index only accelerates).
                let mut cand: Option<std::collections::BTreeSet<[u8; 16]>> = None;
                for (fid, val) in &eq_preds {
                    let fi = match ot.fields.iter().position(|f| f.field_id == *fid) {
                        Some(i) => i,
                        None => continue,
                    };
                    if !ot.indexes.contains(fid) {
                        continue;
                    }
                    let w = ot.fields[fi].kind.width() as usize;
                    let mut v = val.clone();
                    v.resize(w, 0);
                    let ids: std::collections::BTreeSet<[u8; 16]> =
                        self.idx_lookup(type_id, *fid, &v).into_iter().collect();
                    cand = Some(match cand {
                        None => ids,
                        Some(prev) => prev.intersection(&ids).copied().collect(),
                    });
                }
                // SP63: if the equality predicates EXACTLY cover a
                // composite index (same field set), narrow via that single
                // composite lookup too. It is the exact id set for the
                // full equality tuple (a superset of true matches), so
                // intersecting it in keeps the result identical (the
                // program still verifies every candidate) while making
                // multi-column equality sub-linear even when the
                // individual columns are not separately indexed.
                if !eq_preds.is_empty() {
                    let qfields: std::collections::BTreeSet<u16> =
                        eq_preds.iter().map(|(f, _)| *f).collect();
                    let ci = ot.composite.iter().position(|c| {
                        c.len() == qfields.len()
                            && c.iter().collect::<std::collections::BTreeSet<_>>()
                                == qfields.iter().collect()
                    });
                    if let Some(ci_no) = ci {
                        // Build the concat key in the composite index's
                        // declared field order.
                        let mut concat = Vec::new();
                        let mut ok = true;
                        for fid in &ot.composite[ci_no] {
                            let (val, fi) = match (
                                eq_preds.iter().find(|(f, _)| f == fid),
                                ot.fields.iter().position(|f| f.field_id == *fid),
                            ) {
                                (Some((_, v)), Some(i)) => (v, i),
                                _ => {
                                    ok = false;
                                    break;
                                }
                            };
                            let w = ot.fields[fi].kind.width() as usize;
                            let mut v = val.clone();
                            v.resize(w, 0);
                            concat.extend_from_slice(&v);
                        }
                        if ok {
                            let cfid = Self::composite_fid(ci_no);
                            let ids: std::collections::BTreeSet<[u8; 16]> = self
                                .idx_lookup(type_id, cfid, &concat)
                                .into_iter()
                                .collect();
                            cand = Some(match cand {
                                None => ids,
                                Some(prev) => {
                                    prev.intersection(&ids).copied().collect()
                                }
                            });
                        }
                    }
                }
                // SP70: range narrowing via the order index. All hints on
                // ONE field are combined into a single tight interval
                // [max of lower bounds, min of upper bounds] and scanned
                // once — so `v >= a AND v <= b` is one narrow slice, not
                // two huge half-open scans. The slice is taken inclusively
                // (`>`/`<` strictness is enforced by `program`), so it is
                // a SUPERSET of matches: intersecting it in only narrows,
                // and the full program still verifies every candidate ⇒
                // result identical to a scan (the SP62/63 invariant).
                let rfields: std::collections::BTreeSet<u16> =
                    range_preds.iter().map(|(f, _, _)| *f).collect();
                for fid in rfields {
                    if !ot.ordered.contains(&fid) {
                        continue;
                    }
                    // Build the order-index [klo, khi] bounds for this
                    // field's range hints — numeric (0xFFFD, 8-byte
                    // sign-flipped) or CHAR/BYTES (0xFFFC, raw width-w
                    // bytes, SP90). The bound is taken inclusively; `>`
                    // / `<` strictness is enforced by `program`, so the
                    // slice is a SUPERSET (the SP62/63/70 invariant).
                    let (klo, khi) = if let Some((_, w, kind)) =
                        Self::ord_field_pos(&ot, fid)
                    {
                        let mut lo_ok = [0u8; 8];
                        let mut hi_ok = [0xFFu8; 8];
                        let mut usable = false;
                        for (f, rop, val) in &range_preds {
                            if *f != fid {
                                continue;
                            }
                            let vk = match Self::order_key(
                                kind,
                                &Self::norm(val, w),
                            ) {
                                Some(k) => k,
                                None => continue,
                            };
                            match *rop {
                                0 | 1 if vk > lo_ok => lo_ok = vk,
                                2 | 3 if vk < hi_ok => hi_ok = vk,
                                0..=3 => {}
                                _ => continue,
                            }
                            usable = true;
                        }
                        if !usable {
                            continue;
                        }
                        let idxt = 0xFFFD_0000 | (type_id & 0xFFFF);
                        let mut a = [0u8; 16];
                        a[..2].copy_from_slice(&fid.to_le_bytes());
                        a[2..10].copy_from_slice(&lo_ok);
                        let mut b = [0u8; 16];
                        b[..2].copy_from_slice(&fid.to_le_bytes());
                        b[2..10].copy_from_slice(&hi_ok);
                        b[10..].copy_from_slice(&[0xFFu8; 6]);
                        (make_key(idxt, &a), make_key(idxt, &b))
                    } else if let Some((_, w, k)) =
                        Self::vord_field_pos(&ot, fid)
                    {
                        // CHAR/BYTES raw bytes — and U128/I128 via the
                        // SP91 order-preserving transform — are
                        // memcmp-ordered; combine hints into one tight
                        // [lo, hi]. The `[0; w]`..`[0xFF; w]` defaults
                        // are full-range in the *transformed* space too
                        // (sign-flip maps all of i128 onto it).
                        let mut lo_v = vec![0u8; w];
                        let mut hi_v = vec![0xFFu8; w];
                        let mut usable = false;
                        for (f, rop, val) in &range_preds {
                            if *f != fid {
                                continue;
                            }
                            let vk = Self::vorder_key(k, val, w);
                            match *rop {
                                0 | 1 if vk > lo_v => lo_v = vk,
                                2 | 3 if vk < hi_v => hi_v = vk,
                                0..=3 => {}
                                _ => continue,
                            }
                            usable = true;
                        }
                        if !usable {
                            continue;
                        }
                        (
                            Self::voidx_key(type_id, fid, &lo_v),
                            Self::voidx_key(type_id, fid, &hi_v),
                        )
                    } else {
                        continue;
                    };
                    let mut ids: std::collections::BTreeSet<[u8; 16]> =
                        std::collections::BTreeSet::new();
                    for (_, entry) in self.storage.scan_range(&klo, &khi) {
                        for ch in entry.chunks(16) {
                            if ch.len() == 16 {
                                let mut a = [0u8; 16];
                                a.copy_from_slice(ch);
                                ids.insert(a);
                            }
                        }
                    }
                    cand = Some(match cand {
                        None => ids,
                        Some(prev) => {
                            prev.intersection(&ids).copied().collect()
                        }
                    });
                }
                let mut out = Vec::new();
                let mut n = 0u32;
                let mut emit = |rec: &[u8], n: &mut u32, out: &mut Vec<u8>| {
                    out.extend_from_slice(&(rec.len() as u32).to_le_bytes());
                    out.extend_from_slice(rec);
                    *n += 1;
                };
                match cand {
                    Some(ids) => {
                        // index-driven: fetch only candidates (sorted set =>
                        // deterministic order), verify the full predicate.
                        for id in ids {
                            if limit != 0 && n >= limit {
                                break;
                            }
                            let rec = match self.storage.get(&make_key(type_id, &id)) {
                                Some(r) => r,
                                None => continue,
                            };
                            match kessel_expr::eval(&program, &ot, &rec) {
                                Ok(true) => emit(&rec, &mut n, &mut out),
                                Ok(false) => {}
                                Err(e) => {
                                    return OpResult::SchemaError(format!(
                                        "query program: {e:?}"
                                    ))
                                }
                            }
                        }
                    }
                    None => {
                        let lo = make_key(type_id, &[0u8; 16]);
                        let hi = make_key(type_id, &[0xFFu8; 16]);
                        for (_, rec) in self.storage.scan_range(&lo, &hi) {
                            if limit != 0 && n >= limit {
                                break;
                            }
                            match kessel_expr::eval(&program, &ot, &rec) {
                                Ok(true) => emit(&rec, &mut n, &mut out),
                                Ok(false) => {}
                                Err(e) => {
                                    return OpResult::SchemaError(format!(
                                        "query program: {e:?}"
                                    ))
                                }
                            }
                        }
                    }
                }
                OpResult::Got(out)
            }

            Op::SelectFields { type_id, program, fields, limit } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                let layout = ot.compute_layout();
                // Resolve projected fields to (offset,width); reject unknown.
                let mut cols: Vec<(usize, usize)> = Vec::with_capacity(fields.len());
                for fid in &fields {
                    match ot.fields.iter().position(|f| f.field_id == *fid) {
                        Some(i) => cols.push((layout.offsets[i], ot.fields[i].kind.width() as usize)),
                        None => return OpResult::SchemaError(format!("no field {fid}")),
                    }
                }
                let lo = make_key(type_id, &[0u8; 16]);
                let hi = make_key(type_id, &[0xFFu8; 16]);
                let mut out = Vec::new();
                let mut n = 0u32;
                for (_, rec) in self.storage.scan_range(&lo, &hi) {
                    if limit != 0 && n >= limit {
                        break;
                    }
                    match kessel_expr::eval(&program, &ot, &rec) {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(e) => {
                            return OpResult::SchemaError(format!("project program: {e:?}"))
                        }
                    }
                    let mut row = Vec::new();
                    for (off, w) in &cols {
                        match rec.get(*off..*off + *w) {
                            Some(b) => row.extend_from_slice(b),
                            None => row.extend(std::iter::repeat(0u8).take(*w)),
                        }
                    }
                    out.extend_from_slice(&(row.len() as u32).to_le_bytes());
                    out.extend_from_slice(&row);
                    n += 1;
                }
                OpResult::Got(out)
            }

            Op::Aggregate { type_id, program, kind, field_id } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                // #73: MIN/MAX over a CHAR/BYTES/U128/I128 column (the
                // SP87/SP91 `0xFFFC` keyspace). Self-contained early
                // return; the numeric ≤8B path below is unchanged.
                // SUM/AVG stay numeric-only (handled by the path below).
                if (kind == 2 || kind == 3)
                    && Self::ord_field_pos(&ot, field_id).is_none()
                {
                    let (off, w, fk) = match Self::vord_field_pos(&ot, field_id)
                    {
                        Some(p) => p,
                        None => {
                            return OpResult::SchemaError(
                                "Aggregate MIN/MAX field must be numeric \
                                 ≤8B, CHAR/BYTES, or U128/I128"
                                    .into(),
                            )
                        }
                    };
                    let uncond = program
                        == kessel_expr::Program::new()
                            .push_int(1)
                            .bytes()
                            .as_slice();
                    // Fast path: no filter + an ordered index ⇒ read
                    // the index extreme (never changes the answer).
                    if uncond && ot.ordered.contains(&field_id) {
                        return match self.agg_extreme_var(
                            type_id,
                            field_id,
                            off,
                            w,
                            kind == 3,
                        ) {
                            Some(raw) => OpResult::Got(raw),
                            None => OpResult::Got(Vec::new()), // empty
                        };
                    }
                    // Slow path (the oracle): scan + filter, track the
                    // extreme raw bytes via the kind-correct comparator.
                    let lo = make_key(type_id, &[0u8; 16]);
                    let hi = make_key(type_id, &[0xFFu8; 16]);
                    let mut best: Option<Vec<u8>> = None;
                    for (_, rec) in self.storage.scan_range(&lo, &hi) {
                        if !uncond {
                            match kessel_expr::eval(&program, &ot, &rec) {
                                Ok(true) => {}
                                Ok(false) => continue,
                                Err(e) => {
                                    return OpResult::SchemaError(format!(
                                        "agg program: {e:?}"
                                    ))
                                }
                            }
                        }
                        if let Some(raw) = rec.get(off..off + w) {
                            best = Some(match best {
                                None => raw.to_vec(),
                                Some(b) => {
                                    let ord = Self::cmp_field(fk, raw, &b);
                                    let take = if kind == 3 {
                                        ord == std::cmp::Ordering::Greater
                                    } else {
                                        ord == std::cmp::Ordering::Less
                                    };
                                    if take { raw.to_vec() } else { b }
                                }
                            });
                        }
                    }
                    return OpResult::Got(best.unwrap_or_default());
                }
                // COUNT needs no field; SUM/MIN/MAX need a numeric ≤8B field.
                let fpos = if kind == 0 {
                    None
                } else {
                    match Self::ord_field_pos(&ot, field_id) {
                        Some((off, w, fk)) => Some((off, w, fk)),
                        None => {
                            return OpResult::SchemaError(
                                "Aggregate field must be numeric ≤8B".into(),
                            )
                        }
                    }
                };
                let decode_i128 = |raw: &[u8], w: usize, signed: bool| -> i128 {
                    let mut le = [0u8; 16];
                    le[..w.min(16)].copy_from_slice(&raw[..w.min(16)]);
                    if signed && w < 16 && raw[w - 1] & 0x80 != 0 {
                        for b in le.iter_mut().skip(w) {
                            *b = 0xFF;
                        }
                    }
                    i128::from_le_bytes(le)
                };
                // SP73 columnar fast-path. `uncond` = the planner's
                // canonical always-true program (no WHERE): the per-row
                // expr-VM filter is then pure overhead, so skip it and
                // fold only the aggregated column. For MIN/MAX of an
                // order-indexed column with no filter, skip the scan
                // entirely and read the index extreme. Both are pure
                // accelerators — the slow path below is the oracle.
                let uncond = program
                    == kessel_expr::Program::new().push_int(1).bytes().as_slice();
                if uncond {
                    if let (Some((off, w, fk)), true) =
                        (fpos, kind == 2 || kind == 3)
                    {
                        use kessel_catalog::FieldKind::*;
                        let signed =
                            matches!(fk, I8 | I16 | I32 | I64 | Fixed { .. });
                        if ot.ordered.contains(&field_id) {
                            let r = self
                                .agg_extreme(type_id, field_id, off, w, kind == 3)
                                .map(|raw| decode_i128(&raw, w, signed))
                                .unwrap_or(0);
                            return OpResult::Got(r.to_le_bytes().to_vec());
                        }
                    }
                }
                let lo = make_key(type_id, &[0u8; 16]);
                let hi = make_key(type_id, &[0xFFu8; 16]);
                let mut count: i128 = 0;
                let mut sum: i128 = 0;
                let mut mn: Option<i128> = None;
                let mut mx: Option<i128> = None;
                for (_, rec) in self.storage.scan_range(&lo, &hi) {
                    if !uncond {
                        match kessel_expr::eval(&program, &ot, &rec) {
                            Ok(true) => {}
                            Ok(false) => continue,
                            Err(e) => {
                                return OpResult::SchemaError(format!(
                                    "agg program: {e:?}"
                                ))
                            }
                        }
                    }
                    count += 1;
                    if let Some((off, w, fk)) = fpos {
                        if let Some(raw) = rec.get(off..off + w) {
                            use kessel_catalog::FieldKind::*;
                            let signed = matches!(fk, I8 | I16 | I32 | I64 | Fixed { .. });
                            let v = decode_i128(raw, w, signed);
                            sum = sum.wrapping_add(v);
                            mn = Some(mn.map_or(v, |m| m.min(v)));
                            mx = Some(mx.map_or(v, |m| m.max(v)));
                        }
                    }
                }
                let result: i128 = match kind {
                    0 => count,
                    1 => sum,
                    2 => mn.unwrap_or(0),
                    3 => mx.unwrap_or(0),
                    4 => {
                        if count == 0 {
                            0
                        } else {
                            sum / count // integer AVG (floor toward zero)
                        }
                    }
                    _ => {
                        return OpResult::SchemaError(
                            "agg kind must be 0|1|2|3|4".into(),
                        )
                    }
                };
                OpResult::Got(result.to_le_bytes().to_vec())
            }

            Op::SelectSorted { type_id, program, sort_field, desc, offset, limit } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                let layout = ot.compute_layout();
                let (soff, sw, skind) = match ot.fields.iter().position(|f| f.field_id == sort_field) {
                    Some(i) => (
                        layout.offsets[i],
                        ot.fields[i].kind.width() as usize,
                        ot.fields[i].kind,
                    ),
                    None => return OpResult::SchemaError(format!("no sort field {sort_field}")),
                };
                let lo = make_key(type_id, &[0u8; 16]);
                let hi = make_key(type_id, &[0xFFu8; 16]);
                // Buffer matches with their sort-key bytes + object id
                // (tiebreak by id => total deterministic order).
                let mut rows: Vec<(Vec<u8>, [u8; 16], Vec<u8>)> = Vec::new();
                for (k, rec) in self.storage.scan_range(&lo, &hi) {
                    match kessel_expr::eval(&program, &ot, &rec) {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(e) => {
                            return OpResult::SchemaError(format!("sort program: {e:?}"))
                        }
                    }
                    let sk = rec.get(soff..soff + sw).map(|b| b.to_vec()).unwrap_or_default();
                    let mut oid = [0u8; 16];
                    oid.copy_from_slice(&k[4..20]);
                    rows.push((sk, oid, rec));
                }
                rows.sort_by(|a, b| {
                    Self::cmp_field(skind, &a.0, &b.0).then(a.1.cmp(&b.1))
                });
                if desc {
                    rows.reverse();
                }
                let mut out = Vec::new();
                let mut emitted = 0u32;
                for (_, _, rec) in rows.into_iter().skip(offset as usize) {
                    if limit != 0 && emitted >= limit {
                        break;
                    }
                    out.extend_from_slice(&(rec.len() as u32).to_le_bytes());
                    out.extend_from_slice(&rec);
                    emitted += 1;
                }
                OpResult::Got(out)
            }

            Op::GroupAggregate { type_id, program, group_field, kind, agg_field } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                let layout = ot.compute_layout();
                let gpos = match ot.fields.iter().position(|f| f.field_id == group_field) {
                    Some(i) => (layout.offsets[i], ot.fields[i].kind.width() as usize),
                    None => return OpResult::SchemaError(format!("no group field {group_field}")),
                };
                let apos = if kind == 0 {
                    None
                } else {
                    match Self::ord_field_pos(&ot, agg_field) {
                        Some(p) => Some(p),
                        None => {
                            return OpResult::SchemaError(
                                "GroupAggregate agg field must be numeric ≤8B".into(),
                            )
                        }
                    }
                };
                let dec = |raw: &[u8], w: usize, signed: bool| -> i128 {
                    let mut le = [0u8; 16];
                    le[..w.min(16)].copy_from_slice(&raw[..w.min(16)]);
                    if signed && w < 16 && raw[w - 1] & 0x80 != 0 {
                        for b in le.iter_mut().skip(w) {
                            *b = 0xFF;
                        }
                    }
                    i128::from_le_bytes(le)
                };
                // BTreeMap => groups emitted in ascending key order
                // (deterministic). Per group: (count, sum, min, max).
                let mut groups: std::collections::BTreeMap<
                    Vec<u8>,
                    (i128, i128, Option<i128>, Option<i128>),
                > = std::collections::BTreeMap::new();
                // SP73: skip the per-row expr-VM when the program is the
                // planner's canonical always-true (no WHERE) — same
                // accelerator as scalar Aggregate; group/agg columns are
                // still read by offset, result is identical.
                let uncond = program
                    == kessel_expr::Program::new().push_int(1).bytes().as_slice();
                let lo = make_key(type_id, &[0u8; 16]);
                let hi = make_key(type_id, &[0xFFu8; 16]);
                for (_, rec) in self.storage.scan_range(&lo, &hi) {
                    if !uncond {
                        match kessel_expr::eval(&program, &ot, &rec) {
                            Ok(true) => {}
                            Ok(false) => continue,
                            Err(e) => {
                                return OpResult::SchemaError(format!(
                                    "group program: {e:?}"
                                ))
                            }
                        }
                    }
                    let gkey = match rec.get(gpos.0..gpos.0 + gpos.1) {
                        Some(b) => b.to_vec(),
                        None => continue,
                    };
                    let e = groups.entry(gkey).or_insert((0, 0, None, None));
                    e.0 += 1;
                    if let Some((off, w, fk)) = apos {
                        if let Some(raw) = rec.get(off..off + w) {
                            use kessel_catalog::FieldKind::*;
                            let signed = matches!(fk, I8 | I16 | I32 | I64 | Fixed { .. });
                            let v = dec(raw, w, signed);
                            e.1 = e.1.wrapping_add(v);
                            e.2 = Some(e.2.map_or(v, |m| m.min(v)));
                            e.3 = Some(e.3.map_or(v, |m| m.max(v)));
                        }
                    }
                }
                let mut out = Vec::new();
                out.extend_from_slice(&(groups.len() as u32).to_le_bytes());
                for (k, (cnt, sum, mn, mx)) in groups {
                    let res: i128 = match kind {
                        0 => cnt,
                        1 => sum,
                        2 => mn.unwrap_or(0),
                        3 => mx.unwrap_or(0),
                        4 => {
                            if cnt == 0 {
                                0
                            } else {
                                sum / cnt
                            }
                        }
                        _ => {
                            return OpResult::SchemaError(
                                "agg kind must be 0|1|2|3|4".into(),
                            )
                        }
                    };
                    out.extend_from_slice(&(k.len() as u32).to_le_bytes());
                    out.extend_from_slice(&k);
                    out.extend_from_slice(&res.to_le_bytes());
                }
                OpResult::Got(out)
            }

            Op::AddOrderedIndex { type_id, field_id } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                // Numeric ≤8B (0xFFFD) or CHAR/BYTES variable-length
                // (0xFFFC, SP87) — pick the path by field kind.
                let num = Self::ord_field_pos(&ot, field_id);
                let var = if num.is_none() {
                    Self::vord_field_pos(&ot, field_id)
                } else {
                    None
                };
                if num.is_none() && var.is_none() {
                    return OpResult::SchemaError(
                        "field kind not supported for ordered index (need \
                         fixed-width ≤8B numeric/bool/ts, U128/I128, or \
                         CHAR/BYTES)"
                            .into(),
                    );
                }
                if ot.ordered.contains(&field_id) {
                    return OpResult::Ok; // idempotent
                }
                if let Some(t) = self.catalog.get_mut(type_id) {
                    t.ordered.push(field_id);
                }
                if let OpResult::SchemaError(e) = self.persist_catalog(op_number) {
                    return OpResult::SchemaError(e);
                }
                let lo = make_key(type_id, &[0u8; 16]);
                let hi = make_key(type_id, &[0xFFu8; 16]);
                for (k, rec) in self.storage.scan_range(&lo, &hi) {
                    let mut obj = [0u8; 16];
                    obj.copy_from_slice(&k[4..20]);
                    if let Some((off, w, kind)) = num {
                        if let Some(ok) = rec
                            .get(off..off + w)
                            .and_then(|b| Self::order_key(kind, b))
                        {
                            self.oidx_add(op_number, type_id, field_id, ok, obj);
                        }
                    } else if let Some((off, w, k)) = var {
                        if let Some(b) = rec.get(off..off + w) {
                            let b = Self::vorder_key(k, b, w);
                            self.voidx_add(op_number, type_id, field_id, &b, obj);
                        }
                    }
                }
                OpResult::Ok
            }

            Op::FindRange { type_id, field_id, lo, hi } => {
                let ot = match self.catalog.get(type_id) {
                    Some(t) => t.clone(),
                    None => return OpResult::SchemaError(format!("no type {type_id}")),
                };
                if !ot.ordered.contains(&field_id) {
                    return OpResult::SchemaError("field is not range-indexed".into());
                }
                // Numeric ≤8B (0xFFFD) or CHAR/BYTES (0xFFFC, SP87).
                let (klo, khi) = if let Some((_, w, kind)) =
                    Self::ord_field_pos(&ot, field_id)
                {
                    let lo_ok = match Self::order_key(kind, &Self::norm(&lo, w)) {
                        Some(k) => k,
                        None => {
                            return OpResult::SchemaError("bad range lo".into())
                        }
                    };
                    let hi_ok = match Self::order_key(kind, &Self::norm(&hi, w)) {
                        Some(k) => k,
                        None => {
                            return OpResult::SchemaError("bad range hi".into())
                        }
                    };
                    let idxt = 0xFFFD_0000 | (type_id & 0xFFFF);
                    let mut a = [0u8; 16];
                    a[..2].copy_from_slice(&field_id.to_le_bytes());
                    a[2..10].copy_from_slice(&lo_ok);
                    let mut b = [0u8; 16];
                    b[..2].copy_from_slice(&field_id.to_le_bytes());
                    b[2..10].copy_from_slice(&hi_ok);
                    b[10..].copy_from_slice(&[0xFFu8; 6]);
                    (make_key(idxt, &a), make_key(idxt, &b))
                } else if let Some((_, w, k)) =
                    Self::vord_field_pos(&ot, field_id)
                {
                    // CHAR/BYTES: order key = raw width-`w` bytes
                    // (memcmp order). U128/I128 (SP91): order-preserving
                    // 16-byte BE / sign-flipped key. Bucket keys are
                    // exactly tag++fid++ok, so the inclusive [lo,hi]
                    // scan needs no padding slot.
                    (
                        Self::voidx_key(
                            type_id,
                            field_id,
                            &Self::vorder_key(k, &lo, w),
                        ),
                        Self::voidx_key(
                            type_id,
                            field_id,
                            &Self::vorder_key(k, &hi, w),
                        ),
                    )
                } else {
                    return OpResult::SchemaError(
                        "field not range-indexable".into(),
                    );
                };
                let mut ids: Vec<[u8; 16]> = Vec::new();
                for (_, entry) in self.storage.scan_range(&klo, &khi) {
                    for c in entry.chunks(16) {
                        if c.len() == 16 {
                            let mut a = [0u8; 16];
                            a.copy_from_slice(c);
                            ids.push(a);
                        }
                    }
                }
                ids.sort_unstable();
                ids.dedup();
                let mut out = Vec::with_capacity(ids.len() * 16);
                for id in ids {
                    out.extend_from_slice(&id);
                }
                OpResult::Got(out)
            }

            Op::Txn { ops } => {
                // Only data ops (and reads) may participate; schema/DDL and
                // nested txns are rejected up-front so the overlay model
                // stays correct (the overlay does not cover catalog/scan).
                for o in &ops {
                    let ok = matches!(
                        o,
                        Op::Create { .. }
                            | Op::Update { .. }
                            | Op::UpdateSet { .. }
                            | Op::Delete { .. }
                            | Op::GetById { .. }
                            | Op::Describe { .. }
                            | Op::Join { .. }
                            | Op::GetBlob { .. }
                            | Op::FindBy { .. }
                            | Op::Query { .. }
                            | Op::QueryExpr { .. }
                            | Op::FindRange { .. }
                            | Op::FindByComposite { .. }
                            | Op::Select { .. }
                            | Op::QueryRows { .. }
                            | Op::Aggregate { .. }
                            | Op::SelectFields { .. }
                            | Op::GroupAggregate { .. }
                            | Op::SelectSorted { .. }
                    );
                    if !ok {
                        return OpResult::SchemaError(
                            "Txn: only data ops allowed (no DDL / nested txn)".into(),
                        );
                    }
                }
                self.storage.begin_txn();
                for (i, o) in ops.into_iter().enumerate() {
                    let r = self.apply(op_number + i as u64, o);
                    let failed = matches!(
                        r,
                        OpResult::Exists
                            | OpResult::NotFound
                            | OpResult::SchemaError(_)
                            | OpResult::Constraint(_)
                    );
                    if failed {
                        self.storage.abort_txn();
                        if let Some(c) = self.cache.as_mut() {
                            c.clear(); // purge any overlay-derived entries
                        }
                        return r; // whole batch rolled back
                    }
                }
                match self.storage.commit_txn() {
                    Ok(()) => OpResult::Ok,
                    Err(e) => OpResult::SchemaError(format!("txn commit: {e}")),
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

    /// Deterministic digest of the whole replicated state (data + catalog).
    /// Two replicas that have applied the same committed prefix MUST match.
    pub fn digest(&self) -> u32 {
        self.storage.digest()
    }

    /// Flush the underlying storage memtable (used at checkpoints/benchmarks).
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.storage.flush()
    }

    /// Durability control for server-side group commit (SP68): turn off
    /// the per-op WAL fsync, then `sync()` once per drained request batch.
    /// Pure durability *timing* — ordering/state/digest are unchanged.
    pub fn set_autosync(&mut self, on: bool) {
        self.storage.set_autosync(on);
    }
    /// fsync the WAL now (one call durably commits the whole batch).
    pub fn sync(&mut self) -> std::io::Result<()> {
        self.storage.sync()
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
    fn read_cache_on_by_default_and_correct_under_mutation() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        // Cache is enabled by default (SP50) — not None.
        assert!(sm.cache_hit_rate().is_some(), "read cache should be on by default");

        sm.apply(1, Op::CreateType { def: transfer_def() });
        let id = ObjectId::from_u128(7);
        assert_eq!(
            sm.apply(2, Op::Create { type_id: 1, id, record: vec![1, 2, 3] }),
            OpResult::Ok
        );

        // Repeated point reads hit the cache.
        for op in 3..8 {
            assert_eq!(
                sm.apply(op, Op::GetById { type_id: 1, id }),
                OpResult::Got(vec![1, 2, 3])
            );
        }
        assert!(
            sm.cache_hit_rate().unwrap() > 0.0,
            "repeated GetById must register cache hits"
        );

        // A write must invalidate the cached entry (correctness, not stale).
        assert_eq!(
            sm.apply(8, Op::Update { type_id: 1, id, record: vec![4, 5] }),
            OpResult::Ok
        );
        assert_eq!(
            sm.apply(9, Op::GetById { type_id: 1, id }),
            OpResult::Got(vec![4, 5]),
            "read after write must see the new value, not a stale cache entry"
        );

        // Delete also invalidates.
        assert_eq!(sm.apply(10, Op::Delete { type_id: 1, id }), OpResult::Ok);
        assert_eq!(
            sm.apply(11, Op::GetById { type_id: 1, id }),
            OpResult::NotFound,
            "read after delete must not return a cached value"
        );
    }

    #[test]
    fn drop_table_removes_rows_and_type_and_guards_fks() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        assert_eq!(
            sm.apply(1, Op::CreateType { def: encode_type_def("acct", &[]) }),
            OpResult::TypeCreated(1)
        );
        let id1 = ObjectId::from_u128(1);
        let id2 = ObjectId::from_u128(2);
        sm.apply(2, Op::Create { type_id: 1, id: id1, record: vec![1] });
        sm.apply(3, Op::Create { type_id: 1, id: id2, record: vec![2] });
        assert_eq!(sm.apply(4, Op::GetById { type_id: 1, id: id1 }), OpResult::Got(vec![1]));

        // Drop it. The type is gone from the catalog and Describe fails;
        // (a query against the dropped type now errors at the type check,
        // which is the externally-observable "table is gone").
        assert_eq!(sm.apply(5, Op::DropType { type_id: 1 }), OpResult::Ok);
        assert_eq!(sm.apply(8, Op::Describe { type_id: 1 }), OpResult::NotFound);
        assert!(sm.catalog().get(1).is_none(), "type must be gone from catalog");
        // Dropping a non-existent type is a clean NotFound (idempotent-ish).
        assert_eq!(sm.apply(9, Op::DropType { type_id: 99 }), OpResult::NotFound);
        // The name is free again (catalog entry truly removed).
        assert!(matches!(
            sm.apply(10, Op::CreateType { def: encode_type_def("acct", &[]) }),
            OpResult::TypeCreated(_)
        ));

        // Foreign-key guard: cannot drop a table another table references.
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: encode_type_def("parent", &[]) });
        let child = encode_type_def(
            "child",
            &[Field { field_id: 0, name: "p".into(), kind: FieldKind::Ref, nullable: false }],
        );
        sm.apply(2, Op::CreateType { def: child });
        assert_eq!(
            sm.apply(
                3,
                Op::AddForeignKey { type_id: 2, field_id: 1, ref_type_id: 1, on_delete: 0 }
            ),
            OpResult::Ok
        );
        // parent is referenced → refused, parent still present.
        assert!(matches!(
            sm.apply(4, Op::DropType { type_id: 1 }),
            OpResult::Constraint(_)
        ));
        assert!(sm.catalog().get(1).is_some(), "refused drop must not remove the type");
        // Drop the child first, then the parent succeeds.
        assert_eq!(sm.apply(5, Op::DropType { type_id: 2 }), OpResult::Ok);
        assert_eq!(sm.apply(6, Op::DropType { type_id: 1 }), OpResult::Ok);
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
            ordered: vec![],
            composite: vec![],
            defaults: vec![],
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

    /// SP79 (cross-shard slice 2): the global sequencer assigns a
    /// gap-free, monotonic, 1-based total order in ONE op each; the
    /// ordered log reads back exactly (with from/limit); and an
    /// identical op stream yields an identical digest (so every replica
    /// of the sequencer VSR group converges bit-for-bit).
    #[test]
    fn sequencer_is_gap_free_monotonic_and_deterministic() {
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            let mut seqs = Vec::new();
            for (i, p) in
                [b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()].iter().enumerate()
            {
                match sm.apply(10 + i as u64, Op::SeqAppend { payload: p.clone() }) {
                    OpResult::Got(b) => {
                        seqs.push(u64::from_le_bytes(b.try_into().unwrap()))
                    }
                    o => panic!("unexpected {o:?}"),
                }
            }
            (sm, seqs)
        };
        let (mut sm, seqs) = build();
        assert_eq!(seqs, vec![1, 2, 3], "gap-free, monotonic, 1-based");

        // Full ordered log from 1.
        let parse = |b: Vec<u8>| -> Vec<(u64, Vec<u8>)> {
            let mut out = Vec::new();
            let mut p = 0;
            while p + 12 <= b.len() {
                let s = u64::from_le_bytes(b[p..p + 8].try_into().unwrap());
                let l = u32::from_le_bytes(b[p + 8..p + 12].try_into().unwrap())
                    as usize;
                p += 12;
                out.push((s, b[p..p + l].to_vec()));
                p += l;
            }
            out
        };
        match sm.apply(100, Op::SeqRead { from: 1, limit: 0 }) {
            OpResult::Got(b) => assert_eq!(
                parse(b),
                vec![
                    (1, b"a".to_vec()),
                    (2, b"bb".to_vec()),
                    (3, b"ccc".to_vec())
                ]
            ),
            o => panic!("unexpected {o:?}"),
        }
        // from/limit window.
        match sm.apply(101, Op::SeqRead { from: 2, limit: 1 }) {
            OpResult::Got(b) => {
                assert_eq!(parse(b), vec![(2, b"bb".to_vec())])
            }
            o => panic!("unexpected {o:?}"),
        }
        // Reading past the end is empty, not an error.
        assert_eq!(
            sm.apply(102, Op::SeqRead { from: 99, limit: 0 }),
            OpResult::Got(vec![])
        );

        // Deterministic: identical op stream ⇒ identical digest ⇒ every
        // replica of the sequencer group converges.
        let (a, sa) = build();
        let (b2, sb) = build();
        assert_eq!(sa, sb);
        assert_eq!(a.digest(), b2.digest(), "sequencer must be deterministic");
    }

    /// SP80 (cross-shard slice 3): a shard applies cross-shard slices
    /// strictly in global-seq order, exactly once (idempotent re-drive),
    /// atomically (a failing slice rolls back and does NOT advance the
    /// cursor), with empty slices just advancing — and deterministically.
    #[test]
    fn xshard_apply_is_in_order_idempotent_and_atomic() {
        let mk = |v: u128| Op::Create {
            type_id: 1,
            id: ObjectId::from_u128(v),
            record: vec![v as u8, 0, 0, 0, 0, 0, 0, 0],
        };
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: q_type_def() });
            sm
        };
        let present = |sm: &mut StateMachine<MemVfs>, op, v: u128| {
            matches!(
                sm.apply(op, Op::GetById { type_id: 1, id: ObjectId::from_u128(v) }),
                OpResult::Got(_)
            )
        };
        let mut sm = build();

        // seq 1 applies.
        assert_eq!(
            sm.apply(10, Op::XshardApply { seq: 1, ops: vec![mk(1)] }),
            OpResult::Ok
        );
        assert!(present(&mut sm, 11, 1));
        // Re-drive of seq 1 is a no-op even with different ops (the
        // cursor already passed it) — exactly-once.
        assert_eq!(
            sm.apply(12, Op::XshardApply { seq: 1, ops: vec![mk(999)] }),
            OpResult::Ok
        );
        assert!(!present(&mut sm, 13, 999), "re-drive must not re-apply");
        // Out of order is refused (no gaps).
        assert!(matches!(
            sm.apply(14, Op::XshardApply { seq: 3, ops: vec![mk(3)] }),
            OpResult::SchemaError(_)
        ));
        assert!(!present(&mut sm, 15, 3));
        // Empty slice just advances the cursor (non-participant shard).
        assert_eq!(
            sm.apply(16, Op::XshardApply { seq: 2, ops: vec![] }),
            OpResult::Ok
        );
        // Now seq 3 with a real op applies.
        assert_eq!(
            sm.apply(17, Op::XshardApply { seq: 3, ops: vec![mk(3)] }),
            OpResult::Ok
        );
        assert!(present(&mut sm, 18, 3));
        // Atomic: a slice whose 2nd op fails rolls back the 1st too and
        // does NOT advance the cursor.
        assert!(matches!(
            sm.apply(
                19,
                Op::XshardApply { seq: 4, ops: vec![mk(4), mk(1)] } // mk(1) dup → Exists
            ),
            OpResult::Exists | OpResult::Constraint(_) | OpResult::SchemaError(_)
        ));
        assert!(!present(&mut sm, 20, 4), "failed slice must roll back");
        // Cursor unchanged ⇒ seq 4 can still be retried cleanly.
        assert_eq!(
            sm.apply(21, Op::XshardApply { seq: 4, ops: vec![mk(4)] }),
            OpResult::Ok
        );
        assert!(present(&mut sm, 22, 4));

        // Deterministic: identical slice stream ⇒ identical digest.
        let drive = |sm: &mut StateMachine<MemVfs>| {
            sm.apply(30, Op::XshardApply { seq: 1, ops: vec![mk(7)] });
            sm.apply(31, Op::XshardApply { seq: 2, ops: vec![] });
            sm.apply(32, Op::XshardApply { seq: 3, ops: vec![mk(8)] });
        };
        let mut a = build();
        let mut b2 = build();
        drive(&mut a);
        drive(&mut b2);
        assert_eq!(a.digest(), b2.digest(), "xshard apply must be deterministic");
    }

    /// SP81 (cross-shard slice 4): exactly-once append; decide is a
    /// stable, side-effect-free verdict; commit is gated, atomic, and
    /// cursor-idempotent — the primitives the deterministic abort
    /// agreement and recovery are built on.
    #[test]
    fn xshard_two_phase_and_exactly_once_primitives() {
        let mk = |v: u128| Op::Create {
            type_id: 1,
            id: ObjectId::from_u128(v),
            record: vec![v as u8, 0, 0, 0, 0, 0, 0, 0],
        };
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: q_type_def() });
        let present = |sm: &mut StateMachine<MemVfs>, op, v: u128| {
            matches!(
                sm.apply(op, Op::GetById { type_id: 1, id: ObjectId::from_u128(v) }),
                OpResult::Got(_)
            )
        };
        let seq_of = |r: OpResult| match r {
            OpResult::Got(b) => u64::from_le_bytes(b.try_into().unwrap()),
            o => panic!("{o:?}"),
        };

        // Exactly-once append: same key ⇒ same seq (no new entry);
        // different key ⇒ next seq.
        let s1 = seq_of(sm.apply(
            10,
            Op::SeqAppendOnce { key: b"tx-1".to_vec(), payload: vec![9] },
        ));
        let s1b = seq_of(sm.apply(
            11,
            Op::SeqAppendOnce { key: b"tx-1".to_vec(), payload: vec![7] },
        ));
        assert_eq!(s1, s1b, "retry must return the same seq");
        let s2 = seq_of(sm.apply(
            12,
            Op::SeqAppendOnce { key: b"tx-2".to_vec(), payload: vec![1] },
        ));
        assert_eq!(s2, s1 + 1, "a new key gets the next seq");

        // Decide: a slice that WOULD succeed ⇒ verdict 1, applies
        // nothing; stable across re-decide; a slice that WOULD fail
        // (dup) ⇒ verdict 0.
        sm.apply(20, Op::XshardApply { seq: 1, ops: vec![mk(5)] }); // cursor=1, id5 exists
        assert_eq!(
            sm.apply(21, Op::XshardDecide { seq: 2, ops: vec![mk(6)] }),
            OpResult::Got(vec![1])
        );
        assert!(!present(&mut sm, 22, 6), "decide must not apply");
        // Re-decide is stable even though we now (separately) create 6.
        sm.apply(23, Op::XshardApply { seq: 2, ops: vec![mk(6)] }); // cursor=2
        assert_eq!(
            sm.apply(24, Op::XshardDecide { seq: 2, ops: vec![mk(6)] }),
            OpResult::Got(vec![1]),
            "verdict for a seq is stable once recorded"
        );
        // A would-fail slice (dup id 5) ⇒ verdict 0.
        assert_eq!(
            sm.apply(25, Op::XshardDecide { seq: 3, ops: vec![mk(5)] }),
            OpResult::Got(vec![0])
        );

        // Commit gating: commit=false ⇒ skip (advance cursor, apply
        // nothing); commit=true ⇒ apply; idempotent re-drive.
        assert_eq!(
            sm.apply(26, Op::XshardCommit { seq: 3, ops: vec![mk(7)], commit: false }),
            OpResult::Ok
        );
        assert!(!present(&mut sm, 27, 7), "commit=false must skip atomically");
        assert_eq!(
            sm.apply(28, Op::XshardCommit { seq: 4, ops: vec![mk(8)], commit: true }),
            OpResult::Ok
        );
        assert!(present(&mut sm, 29, 8));
        assert_eq!(
            sm.apply(30, Op::XshardCommit { seq: 4, ops: vec![mk(99)], commit: true }),
            OpResult::Ok,
            "re-drive past cursor is an idempotent no-op"
        );
        assert!(!present(&mut sm, 31, 99));

        // Deterministic.
        let drive = |s: &mut StateMachine<MemVfs>| {
            s.apply(40, Op::CreateType { def: q_type_def() });
            s.apply(41, Op::SeqAppendOnce { key: b"k".to_vec(), payload: vec![1] });
            s.apply(42, Op::XshardDecide { seq: 1, ops: vec![mk(3)] });
            s.apply(43, Op::XshardCommit { seq: 1, ops: vec![mk(3)], commit: true });
        };
        let mut a = StateMachine::open(MemVfs::new()).unwrap();
        let mut b2 = StateMachine::open(MemVfs::new()).unwrap();
        drive(&mut a);
        drive(&mut b2);
        assert_eq!(a.digest(), b2.digest(), "two-phase must be deterministic");
    }

    /// SP82 (cross-shard slice 5): the deterministic cross-shard
    /// protocol is atomic and convergent under an ADVERSARIAL drive
    /// schedule — partial decide, "router crash", duplicate
    /// SeqAppendOnce retries, repeated full-log recovery, reordering —
    /// the final per-shard state is identical to a clean run and the
    /// whole chaotic schedule is itself bit-for-bit deterministic.
    /// (Per-group partition tolerance is the seed-7 corpus; this proves
    /// the cross-shard layer composed on top of it.)
    #[test]
    fn xshard_protocol_atomic_and_deterministic_under_adversarial_drive() {
        const K: usize = 3;
        let mk = |v: u128| Op::Create {
            type_id: 1,
            id: ObjectId::from_u128(v),
            record: vec![v as u8, 0, 0, 0, 0, 0, 0, 0],
        };
        // (dedup key, per-shard slices). T2's shard-1 slice dups a row
        // pre-seeded below ⇒ that whole cross-shard txn must abort
        // everywhere; T1/T3 must commit everywhere.
        let txns: Vec<(Vec<u8>, [Vec<Op>; K])> = vec![
            (b"t1".to_vec(), [vec![mk(10)], vec![mk(11)], vec![]]),
            (b"t2".to_vec(), [vec![], vec![mk(99)], vec![mk(12)]]), // mk(99) dup ⇒ abort
            (b"t3".to_vec(), [vec![mk(13)], vec![], vec![mk(14)]]),
        ];

        // Build K shard SMs (+ pre-seed the dup) and a sequencer SM.
        let setup = || {
            let mut shards: Vec<StateMachine<MemVfs>> =
                (0..K).map(|_| StateMachine::open(MemVfs::new()).unwrap()).collect();
            for s in shards.iter_mut() {
                s.apply(1, Op::CreateType { def: q_type_def() });
            }
            // Pre-existing row on shard 1 so T2's slice deterministically
            // fails (Exists) ⇒ deterministic global abort.
            shards[1].apply(2, mk(99));
            let seqr = StateMachine::open(MemVfs::new()).unwrap();
            (shards, seqr, std::cell::Cell::new(100u64))
        };
        let assign = |seqr: &mut StateMachine<MemVfs>,
                      n: &std::cell::Cell<u64>,
                      key: &[u8]|
         -> u64 {
            n.set(n.get() + 1);
            match seqr.apply(
                n.get(),
                Op::SeqAppendOnce { key: key.to_vec(), payload: vec![] },
            ) {
                OpResult::Got(b) => u64::from_le_bytes(b.try_into().unwrap()),
                o => panic!("{o:?}"),
            }
        };
        let drive = |shards: &mut Vec<StateMachine<MemVfs>>,
                     n: &std::cell::Cell<u64>,
                     seq: u64,
                     slices: &[Vec<Op>; K]| {
            let mut decision = true;
            for (i, sl) in slices.iter().enumerate() {
                if sl.is_empty() {
                    continue;
                }
                n.set(n.get() + 1);
                if let OpResult::Got(v) = shards[i].apply(
                    n.get(),
                    Op::XshardDecide { seq, ops: sl.clone() },
                ) {
                    if v == vec![0] {
                        decision = false;
                    }
                }
            }
            for (i, sl) in slices.iter().enumerate() {
                n.set(n.get() + 1);
                shards[i].apply(
                    n.get(),
                    Op::XshardCommit { seq, ops: sl.clone(), commit: decision },
                );
            }
        };
        let digests =
            |sh: &mut Vec<StateMachine<MemVfs>>| -> Vec<u32> {
                sh.iter_mut().map(|s| s.digest()).collect()
            };

        // --- Clean reference run ---
        let (mut cs, mut cseq, cn) = setup();
        let mut log: Vec<(u64, [Vec<Op>; K])> = Vec::new();
        for (k, sl) in &txns {
            let seq = assign(&mut cseq, &cn, k);
            log.push((seq, sl.clone()));
        }
        for (seq, sl) in &log {
            drive(&mut cs, &cn, *seq, sl);
        }
        let reference = digests(&mut cs);
        // Sanity: T1/T3 committed (rows present), T2 aborted everywhere.
        let present = |s: &mut StateMachine<MemVfs>, v: u128| {
            matches!(
                s.apply(9_000_000, Op::GetById { type_id: 1, id: ObjectId::from_u128(v) }),
                OpResult::Got(_)
            )
        };
        assert!(present(&mut cs[0], 10) && present(&mut cs[1], 11), "T1 committed");
        assert!(present(&mut cs[0], 13) && present(&mut cs[2], 14), "T3 committed");
        assert!(
            !present(&mut cs[2], 12),
            "T2 must abort on shard 2 too (deterministic agreement)"
        );

        // --- Adversarial run (fresh state) ---
        let adversarial = || {
            let (mut s, mut seqr, n) = setup();
            // Duplicate, out-of-order SeqAppendOnce retries: same key ⇒
            // same seq, no extra entries.
            let mut alog: Vec<(u64, [Vec<Op>; K])> = Vec::new();
            for (k, sl) in &txns {
                let q1 = assign(&mut seqr, &n, k);
                let q2 = assign(&mut seqr, &n, k); // retry
                assert_eq!(q1, q2, "exactly-once seq under retry");
                alog.push((q1, sl.clone()));
            }
            let rec = |s: &mut Vec<StateMachine<MemVfs>>,
                       n: &std::cell::Cell<u64>| {
                for (seq, sl) in &alog {
                    drive(s, n, *seq, sl);
                }
            };
            // Chaos: fully drive T1; partially decide T2 on shard 1
            // only; then "router crash" → full recovery; recover AGAIN
            // (idempotent); then a stray duplicate commit of T1.
            drive(&mut s, &n, alog[0].0, &alog[0].1);
            n.set(n.get() + 1);
            let _ = s[1].apply(
                n.get(),
                Op::XshardDecide { seq: alog[1].0, ops: alog[1].1[1].clone() },
            );
            rec(&mut s, &n); // recover from the durable log
            rec(&mut s, &n); // again — must be a no-op
            n.set(n.get() + 1);
            let _ = s[0].apply(
                n.get(),
                Op::XshardCommit { seq: alog[0].0, ops: alog[0].1[0].clone(), commit: true },
            );
            rec(&mut s, &n); // and once more
            digests(&mut s)
        };

        let a1 = adversarial();
        let a2 = adversarial();
        assert_eq!(
            a1, reference,
            "adversarial drive must converge to the clean-run state"
        );
        assert_eq!(
            a1, a2,
            "the whole adversarial schedule is itself deterministic"
        );
    }

    /// SP84: `Op::UpdateSet` is a deterministic server-side RMW that
    /// composes inside `Op::Txn` (read-your-writes via the overlay) and
    /// reuses the proven Op::Update enforcement path.
    #[test]
    fn update_set_rmw_composes_in_txn_and_is_deterministic() {
        // Codec-encoded record (what a SQL INSERT actually stores —
        // qrec() is header-less and not representative here).
        let crec = |sm: &StateMachine<MemVfs>, o: u32, k: u16, v: u32| {
            let ot = sm.catalog().get(1).unwrap().clone();
            kessel_codec::encode(
                &ot,
                &[
                    kessel_codec::Value::Uint(o as u128),
                    kessel_codec::Value::Uint(k as u128),
                    kessel_codec::Value::Uint(v as u128),
                ],
            )
            .unwrap()
        };
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: q_type_def() }); // owner f1,kind f2,v f3
            let r = crec(&sm, 7, 0, 100);
            sm.apply(
                2,
                Op::Create { type_id: 1, id: ObjectId::from_u128(1), record: r },
            );
            sm
        };
        let v_of = |sm: &mut StateMachine<MemVfs>, op, id: u128| -> u32 {
            match sm.apply(op, Op::GetById { type_id: 1, id: ObjectId::from_u128(id) }) {
                OpResult::Got(r) => {
                    let ot = sm.catalog().get(1).unwrap().clone();
                    match kessel_codec::decode(&ot, &r).unwrap()[2] {
                        kessel_codec::Value::Uint(u) => u as u32,
                        _ => panic!(),
                    }
                }
                o => panic!("{o:?}"),
            }
        };
        let mut sm = build();
        // Standalone RMW: set field 3 (v) = 500.
        assert_eq!(
            sm.apply(
                10,
                Op::UpdateSet {
                    type_id: 1,
                    id: ObjectId::from_u128(1),
                    sets: vec![(3, 500u32.to_le_bytes().to_vec())],
                },
            ),
            OpResult::Ok
        );
        assert_eq!(v_of(&mut sm, 11, 1), 500);
        // Missing row ⇒ NotFound (no effect).
        assert_eq!(
            sm.apply(
                12,
                Op::UpdateSet {
                    type_id: 1,
                    id: ObjectId::from_u128(99),
                    sets: vec![(3, 1u32.to_le_bytes().to_vec())],
                },
            ),
            OpResult::NotFound
        );
        // Composes in a txn: Create id 2 then UpdateSet id 2 in the SAME
        // batch (read-your-writes via the overlay); atomic.
        assert_eq!(
            sm.apply(
                13,
                Op::Txn {
                    ops: vec![
                        Op::Create { type_id: 1, id: ObjectId::from_u128(2), record: crec(&sm, 1, 0, 10) },
                        Op::UpdateSet { type_id: 1, id: ObjectId::from_u128(2), sets: vec![(3, 42u32.to_le_bytes().to_vec())] },
                    ],
                },
            ),
            OpResult::Ok
        );
        assert_eq!(v_of(&mut sm, 14, 2), 42, "RMW saw the in-txn create");
        // A failing member rolls the whole txn back (UpdateSet on a
        // missing row ⇒ NotFound ⇒ abort; the create must not persist).
        assert_ne!(
            sm.apply(
                15,
                Op::Txn {
                    ops: vec![
                        Op::Create { type_id: 1, id: ObjectId::from_u128(3), record: crec(&sm, 1, 0, 1) },
                        Op::UpdateSet { type_id: 1, id: ObjectId::from_u128(404), sets: vec![(3, 9u32.to_le_bytes().to_vec())] },
                    ],
                },
            ),
            OpResult::Ok
        );
        assert_eq!(
            sm.apply(16, Op::GetById { type_id: 1, id: ObjectId::from_u128(3) }),
            OpResult::NotFound,
            "failed txn must roll back the create too"
        );
        // Deterministic.
        let drive = |sm: &mut StateMachine<MemVfs>| {
            sm.apply(20, Op::UpdateSet { type_id: 1, id: ObjectId::from_u128(1), sets: vec![(3, 7u32.to_le_bytes().to_vec())] });
            sm.apply(21, Op::Txn { ops: vec![
                Op::Create { type_id: 1, id: ObjectId::from_u128(5), record: crec(sm, 2, 0, 2) },
                Op::UpdateSet { type_id: 1, id: ObjectId::from_u128(5), sets: vec![(1, 9u32.to_le_bytes().to_vec())] },
            ]});
        };
        let mut a = build();
        let mut b2 = build();
        drive(&mut a);
        drive(&mut b2);
        assert_eq!(a.digest(), b2.digest(), "UpdateSet must be deterministic");
    }

    #[test]
    fn overflow_blobs_are_reclaimed_on_update_and_delete() {
        // SP76: an UPDATE that replaces an overflow value frees the old
        // blob; a DELETE frees the row's blobs. Deterministic — handles
        // are op-number-derived, so every replica reclaims the same keys.
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: overflow_type_def() });
            let id = ObjectId::from_u128(7);
            let r1 = super::encode_overflow_record(&fixed_zeros(), &[(0, b"old".to_vec())]);
            sm.apply(2, Op::Create { type_id: 1, id, record: r1 });
            let r2 = super::encode_overflow_record(&fixed_zeros(), &[(0, b"new".to_vec())]);
            sm.apply(3, Op::Update { type_id: 1, id, record: r2 });
            sm
        };
        let mut sm = build();
        let h_old = (2u64 << 20) | 0;
        let h_new = (3u64 << 20) | 0;
        // New value readable; OLD blob reclaimed (was the documented leak).
        assert_eq!(
            sm.apply(4, Op::GetBlob { handle: h_new }),
            OpResult::Got(b"new".to_vec())
        );
        assert_eq!(
            sm.apply(5, Op::GetBlob { handle: h_old }),
            OpResult::NotFound,
            "UPDATE must reclaim the superseded blob"
        );
        // DELETE reclaims the row's current blob too.
        sm.apply(6, Op::Delete { type_id: 1, id: ObjectId::from_u128(7) });
        assert_eq!(
            sm.apply(7, Op::GetBlob { handle: h_new }),
            OpResult::NotFound,
            "DELETE must reclaim the row's blob"
        );
        // Deterministic across identical histories.
        let a = build();
        let b = build();
        assert_eq!(a.digest(), b.digest(), "overflow GC must be deterministic");
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
            ordered: vec![],
            composite: vec![],
            defaults: vec![],
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

    /// SP87 oracle: a `RANGE INDEX` on a `CHAR` column makes
    /// `Op::FindRange` return EXACTLY the lexicographic-range rows
    /// (== an independent brute-force filter), stays correct under
    /// UPDATE/DELETE maintenance, and is deterministic.
    #[test]
    fn string_range_index_equals_brute_force_and_is_maintained() {
        use kessel_codec::{encode, Value};
        use kessel_proto::Rng;
        // type: s CHAR(8) (field 1, range-indexed), n U32 (field 2).
        let tdef = encode_type_def(
            "t",
            &[
                Field { field_id: 0, name: "s".into(), kind: FieldKind::Char(8), nullable: false },
                Field { field_id: 0, name: "n".into(), kind: FieldKind::U32, nullable: false },
            ],
        );
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: tdef.clone() });
            let cot = sm.catalog().get(1).unwrap().clone();
            let mut rng = Rng::new(0x5712_AB);
            let mut model: Vec<(u128, [u8; 8])> = Vec::new();
            for id in 1..=120u128 {
                // short random lowercase strings (0..4 chars), zero-padded.
                let len = (rng.below(5)) as usize;
                let mut s = [0u8; 8];
                for c in s.iter_mut().take(len) {
                    *c = b'a' + (rng.below(6) as u8); // a..f
                }
                let rec = encode(
                    &cot,
                    &[Value::Blob(s.to_vec()), Value::Uint(id)],
                )
                .unwrap();
                sm.apply(
                    10 + id as u64,
                    Op::Create { type_id: 1, id: ObjectId::from_u128(id), record: rec },
                );
                model.push((id, s));
            }
            sm.apply(900, Op::AddOrderedIndex { type_id: 1, field_id: 1 });
            (sm, model)
        };
        let (mut sm, mut model) = build();
        let norm8 = |b: &[u8]| {
            let mut o = [0u8; 8];
            let k = b.len().min(8);
            o[..k].copy_from_slice(&b[..k]);
            o
        };
        let mut op = 2000u64;
        let mut rng = Rng::new(0x9911);
        for _ in 0..40 {
            let mut mk = |rng: &mut kessel_proto::Rng| {
                let len = rng.below(4) as usize;
                let mut v = Vec::new();
                for _ in 0..len {
                    v.push(b'a' + (rng.below(6) as u8));
                }
                v
            };
            let (mut lo, mut hi) = (mk(&mut rng), mk(&mut rng));
            if norm8(&hi) < norm8(&lo) {
                std::mem::swap(&mut lo, &mut hi);
            }
            op += 1;
            let mut got = ids_of(sm.apply(
                op,
                Op::FindRange { type_id: 1, field_id: 1, lo: lo.clone(), hi: hi.clone() },
            ));
            got.sort_unstable();
            let (l8, h8) = (norm8(&lo), norm8(&hi));
            let mut want: Vec<u128> = model
                .iter()
                .filter(|(_, s)| *s >= l8 && *s <= h8)
                .map(|(id, _)| *id)
                .collect();
            want.sort_unstable();
            assert_eq!(got, want, "FindRange(\"{lo:?}\"..=\"{hi:?}\") != brute force");
        }
        // UPDATE maintenance: move row 1's string to "zzzz".
        let cot = sm.catalog().get(1).unwrap().clone();
        let z = encode(&cot, &[Value::Blob(b"zzzz".to_vec()), Value::Uint(1)]).unwrap();
        sm.apply(5000, Op::Update { type_id: 1, id: ObjectId::from_u128(1), record: z });
        model.iter_mut().find(|(id, _)| *id == 1).unwrap().1 = norm8(b"zzzz");
        assert_eq!(
            ids_of(sm.apply(5001, Op::FindRange { type_id: 1, field_id: 1, lo: b"zz".to_vec(), hi: b"zzzzzzzz".to_vec() })),
            vec![1],
            "UPDATE re-indexed the row under its new value"
        );
        // DELETE maintenance: row 1 leaves the index.
        sm.apply(5002, Op::Delete { type_id: 1, id: ObjectId::from_u128(1) });
        assert!(
            !ids_of(sm.apply(5003, Op::FindRange { type_id: 1, field_id: 1, lo: b"zz".to_vec(), hi: b"zzzzzzzz".to_vec() })).contains(&1),
            "DELETE removed the row from the index"
        );
        // Deterministic.
        let (a, _) = build();
        let (b2, _) = build();
        assert_eq!(a.digest(), b2.digest(), "string range index must be deterministic");
    }

    /// SP91 oracle: a `RANGE INDEX` on a 16-byte `U128` / `I128`
    /// column makes `Op::FindRange` return EXACTLY the numeric-range
    /// rows (== an independent brute-force filter) — *including
    /// negative I128 values, which must sort below the positives* —
    /// stays correct under UPDATE/DELETE, and is deterministic. These
    /// exceed the 8-byte numeric `0xFFFD` path; SP91 routes them
    /// through the SP87 `0xFFFC` variable-length keyspace with an
    /// order-preserving 16-byte big-endian (sign-flipped for I128) key.
    #[test]
    fn u128_i128_range_index_equals_brute_force_and_is_maintained() {
        use kessel_codec::{encode, Value};
        use kessel_proto::Rng;

        // ---- U128 ----
        let udef = encode_type_def(
            "u",
            &[
                Field { field_id: 0, name: "v".into(), kind: FieldKind::U128, nullable: false },
                Field { field_id: 0, name: "n".into(), kind: FieldKind::U32, nullable: false },
            ],
        );
        let ubuild = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: udef.clone() });
            let cot = sm.catalog().get(1).unwrap().clone();
            let mut rng = Rng::new(0x91_A1);
            let mut model: Vec<(u128, u128)> = Vec::new();
            for id in 1..=120u128 {
                // wide values spanning the whole u128 range.
                let v = (rng.below(u64::MAX) as u128) << 64
                    | rng.below(u64::MAX) as u128;
                let rec = encode(
                    &cot,
                    &[Value::Uint(v), Value::Uint(id)],
                )
                .unwrap();
                sm.apply(
                    10 + id as u64,
                    Op::Create { type_id: 1, id: ObjectId::from_u128(id), record: rec },
                );
                model.push((id, v));
            }
            sm.apply(900, Op::AddOrderedIndex { type_id: 1, field_id: 1 });
            (sm, model)
        };
        let (mut sm, mut model) = ubuild();
        let mut rng = Rng::new(0x77_22);
        let mut op = 2000u64;
        for _ in 0..40 {
            let mut mk = |r: &mut Rng| {
                (r.below(u64::MAX) as u128) << 64 | r.below(u64::MAX) as u128
            };
            let (mut lo, mut hi) = (mk(&mut rng), mk(&mut rng));
            if hi < lo {
                std::mem::swap(&mut lo, &mut hi);
            }
            op += 1;
            let mut got = ids_of(sm.apply(
                op,
                Op::FindRange {
                    type_id: 1,
                    field_id: 1,
                    lo: lo.to_le_bytes().to_vec(),
                    hi: hi.to_le_bytes().to_vec(),
                },
            ));
            got.sort_unstable();
            let mut want: Vec<u128> = model
                .iter()
                .filter(|(_, v)| *v >= lo && *v <= hi)
                .map(|(id, _)| *id)
                .collect();
            want.sort_unstable();
            assert_eq!(got, want, "U128 FindRange({lo}..={hi}) != brute force");
        }
        // UPDATE maintenance: move row 1 to u128::MAX.
        let cot = sm.catalog().get(1).unwrap().clone();
        let z = encode(&cot, &[Value::Uint(u128::MAX), Value::Uint(1)]).unwrap();
        sm.apply(5000, Op::Update { type_id: 1, id: ObjectId::from_u128(1), record: z });
        model.iter_mut().find(|(id, _)| *id == 1).unwrap().1 = u128::MAX;
        assert!(
            ids_of(sm.apply(5001, Op::FindRange {
                type_id: 1,
                field_id: 1,
                lo: (u128::MAX - 1).to_le_bytes().to_vec(),
                hi: u128::MAX.to_le_bytes().to_vec(),
            }))
            .contains(&1),
            "UPDATE re-indexed the U128 row under its new value"
        );
        // DELETE maintenance.
        sm.apply(5002, Op::Delete { type_id: 1, id: ObjectId::from_u128(1) });
        assert!(
            !ids_of(sm.apply(5003, Op::FindRange {
                type_id: 1,
                field_id: 1,
                lo: (u128::MAX - 1).to_le_bytes().to_vec(),
                hi: u128::MAX.to_le_bytes().to_vec(),
            }))
            .contains(&1),
            "DELETE removed the U128 row from the index"
        );
        let (a, _) = ubuild();
        let (b, _) = ubuild();
        assert_eq!(a.digest(), b.digest(), "U128 range index must be deterministic");

        // ---- I128 (signed: negatives must sort below positives) ----
        let idef = encode_type_def(
            "i",
            &[
                Field { field_id: 0, name: "v".into(), kind: FieldKind::I128, nullable: false },
                Field { field_id: 0, name: "n".into(), kind: FieldKind::U32, nullable: false },
            ],
        );
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: idef.clone() });
        let cot = sm.catalog().get(1).unwrap().clone();
        let mut rng = Rng::new(0x5151);
        let mut model: Vec<(u128, i128)> = Vec::new();
        for id in 1..=120u128 {
            let mag = (rng.below(u64::MAX) as i128) << 32 | rng.below(u64::MAX) as i128;
            let v = if rng.below(2) == 0 { -mag } else { mag };
            let rec = encode(&cot, &[Value::Int(v), Value::Uint(id)]).unwrap();
            sm.apply(
                10 + id as u64,
                Op::Create { type_id: 1, id: ObjectId::from_u128(id), record: rec },
            );
            model.push((id, v));
        }
        sm.apply(900, Op::AddOrderedIndex { type_id: 1, field_id: 1 });
        let mut op = 3000u64;
        for _ in 0..40 {
            let mut mk = |r: &mut Rng| {
                let mag = (r.below(u64::MAX) as i128) << 32 | r.below(u64::MAX) as i128;
                if r.below(2) == 0 { -mag } else { mag }
            };
            let (mut lo, mut hi) = (mk(&mut rng), mk(&mut rng));
            if hi < lo {
                std::mem::swap(&mut lo, &mut hi);
            }
            op += 1;
            let mut got = ids_of(sm.apply(
                op,
                Op::FindRange {
                    type_id: 1,
                    field_id: 1,
                    lo: lo.to_le_bytes().to_vec(),
                    hi: hi.to_le_bytes().to_vec(),
                },
            ));
            got.sort_unstable();
            let mut want: Vec<u128> = model
                .iter()
                .filter(|(_, v)| *v >= lo && *v <= hi)
                .map(|(id, _)| *id)
                .collect();
            want.sort_unstable();
            assert_eq!(got, want, "I128 FindRange({lo}..={hi}) != brute force");
        }
        // Spanning bound that straddles zero must include both signs.
        let mut got = ids_of(sm.apply(9000, Op::FindRange {
            type_id: 1,
            field_id: 1,
            lo: i128::MIN.to_le_bytes().to_vec(),
            hi: i128::MAX.to_le_bytes().to_vec(),
        }));
        got.sort_unstable();
        let mut all: Vec<u128> = model.iter().map(|(id, _)| *id).collect();
        all.sort_unstable();
        assert_eq!(got, all, "I128 full-range must return every row");
    }

    fn rng_below(r: &mut kessel_proto::Rng, n: u64) -> u64 {
        r.below(n)
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
            ordered: vec![],
            composite: vec![],
            defaults: vec![],
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

    // ---- Sub-project 9: atomic multi-op transactions ----

    #[test]
    fn txn_commits_all_or_nothing() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: indexed_type_def() });
        // successful txn: two creates both land
        let r = sm.apply(2, Op::Txn {
            ops: vec![
                Op::Create { type_id: 1, id: ObjectId::from_u128(1), record: rec_bytes(10, 1) },
                Op::Create { type_id: 1, id: ObjectId::from_u128(2), record: rec_bytes(20, 2) },
            ],
        });
        assert_eq!(r, OpResult::Ok);
        assert!(matches!(sm.apply(3, Op::GetById { type_id: 1, id: ObjectId::from_u128(1) }), OpResult::Got(_)));
        assert!(matches!(sm.apply(4, Op::GetById { type_id: 1, id: ObjectId::from_u128(2) }), OpResult::Got(_)));
    }

    #[test]
    fn txn_rolls_back_on_midbatch_failure() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: indexed_type_def() });
        sm.apply(2, Op::AddUnique { type_id: 1, field_id: 1 });
        // txn: create A(owner=5) OK, then create B(owner=5) -> UNIQUE fail.
        // Whole txn must roll back: A must NOT exist afterwards.
        let r = sm.apply(3, Op::Txn {
            ops: vec![
                Op::Create { type_id: 1, id: ObjectId::from_u128(1), record: rec_bytes(5, 1) },
                Op::Create { type_id: 1, id: ObjectId::from_u128(2), record: rec_bytes(5, 2) },
            ],
        });
        assert!(matches!(r, OpResult::Constraint(_)), "got {r:?}");
        assert_eq!(
            sm.apply(4, Op::GetById { type_id: 1, id: ObjectId::from_u128(1) }),
            OpResult::NotFound,
            "first op rolled back"
        );
        // index must also be clean (no phantom from rolled-back op)
        assert_eq!(
            ids_of(sm.apply(5, Op::FindBy { type_id: 1, field_id: 1, value: 5u32.to_le_bytes().to_vec() })),
            Vec::<u128>::new()
        );
        // a subsequent good single create still works
        assert_eq!(sm.apply(6, Op::Create { type_id: 1, id: ObjectId::from_u128(9), record: rec_bytes(5, 9) }), OpResult::Ok);
    }

    #[test]
    fn txn_rejects_ddl_and_nested() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: indexed_type_def() });
        assert!(matches!(
            sm.apply(2, Op::Txn { ops: vec![Op::CreateType { def: indexed_type_def() }] }),
            OpResult::SchemaError(_)
        ));
        // type/data untouched by the rejected txn
        assert_eq!(sm.apply(3, Op::Create { type_id: 1, id: ObjectId::from_u128(1), record: rec_bytes(1, 1) }), OpResult::Ok);
    }

    #[test]
    fn txn_is_deterministic() {
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: indexed_type_def() });
            sm.apply(2, Op::AddUnique { type_id: 1, field_id: 1 });
            let mut rng = Rng::new(0xDEFA17);
            for op in 3..400u64 {
                let a = ObjectId::from_u128(rng.below(30) as u128);
                let b = ObjectId::from_u128(rng.below(30) as u128);
                let _ = sm.apply(op, Op::Txn {
                    ops: vec![
                        Op::Create { type_id: 1, id: a, record: rec_bytes(rng.below(20) as u32, op as u32) },
                        Op::Create { type_id: 1, id: b, record: rec_bytes(rng.below(20) as u32, op as u32) },
                    ],
                });
            }
            sm.digest()
        };
        assert_eq!(build(), build(), "atomic txn pipeline must be deterministic");
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
            indexes: vec![], unique: vec![], fks: vec![], checks: vec![], triggers: vec![], ordered: vec![], composite: vec![],
            defaults: vec![],
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

    // ---- Sub-project 11: ON DELETE RESTRICT / CASCADE ----

    fn pc_setup() -> (StateMachine<MemVfs>, kessel_catalog::ObjectType) {
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
        (sm, cot)
    }

    #[test]
    fn on_delete_restrict_blocks_parent_delete() {
        use kessel_codec::{encode, Value};
        let (mut sm, cot) = pc_setup();
        sm.apply(3, Op::Create { type_id: 1, id: ObjectId::from_u128(5), record: vec![1] });
        assert_eq!(
            sm.apply(4, Op::AddForeignKey { type_id: 2, field_id: 1, ref_type_id: 1, on_delete: 1 }),
            OpResult::Ok
        );
        sm.apply(5, Op::Create { type_id: 2, id: ObjectId::from_u128(50), record: encode(&cot, &[Value::Uint(5)]).unwrap() });
        // RESTRICT: parent delete refused while a child references it
        assert!(matches!(
            sm.apply(6, Op::Delete { type_id: 1, id: ObjectId::from_u128(5) }),
            OpResult::Constraint(_)
        ));
        assert!(matches!(sm.apply(7, Op::GetById { type_id: 1, id: ObjectId::from_u128(5) }), OpResult::Got(_)), "parent untouched");
        // remove child, then parent delete succeeds
        sm.apply(8, Op::Delete { type_id: 2, id: ObjectId::from_u128(50) });
        assert_eq!(sm.apply(9, Op::Delete { type_id: 1, id: ObjectId::from_u128(5) }), OpResult::Ok);
    }

    #[test]
    fn on_delete_cascade_removes_children_recursively() {
        use kessel_codec::{encode, Value};
        let (mut sm, cot) = pc_setup();
        // grandchild type 3: gref(U128) -> child(type 2), CASCADE
        sm.apply(3, Op::CreateType {
            def: encode_type_def("gc", &[
                Field { field_id: 0, name: "gref".into(), kind: FieldKind::U128, nullable: false },
            ]),
        });
        let got = sm.catalog().get(3).unwrap().clone();
        sm.apply(4, Op::Create { type_id: 1, id: ObjectId::from_u128(7), record: vec![1] });
        sm.apply(5, Op::AddForeignKey { type_id: 2, field_id: 1, ref_type_id: 1, on_delete: 2 });
        sm.apply(6, Op::AddForeignKey { type_id: 3, field_id: 1, ref_type_id: 2, on_delete: 2 });
        // children 50,51 -> parent 7 ; grandchild 500 -> child 50
        sm.apply(7, Op::Create { type_id: 2, id: ObjectId::from_u128(50), record: encode(&cot, &[Value::Uint(7)]).unwrap() });
        sm.apply(8, Op::Create { type_id: 2, id: ObjectId::from_u128(51), record: encode(&cot, &[Value::Uint(7)]).unwrap() });
        sm.apply(9, Op::Create { type_id: 3, id: ObjectId::from_u128(500), record: encode(&got, &[Value::Uint(50)]).unwrap() });
        // delete the parent -> cascades through child -> grandchild
        assert_eq!(sm.apply(10, Op::Delete { type_id: 1, id: ObjectId::from_u128(7) }), OpResult::Ok);
        assert_eq!(sm.apply(11, Op::GetById { type_id: 1, id: ObjectId::from_u128(7) }), OpResult::NotFound);
        assert_eq!(sm.apply(12, Op::GetById { type_id: 2, id: ObjectId::from_u128(50) }), OpResult::NotFound);
        assert_eq!(sm.apply(13, Op::GetById { type_id: 2, id: ObjectId::from_u128(51) }), OpResult::NotFound);
        assert_eq!(sm.apply(14, Op::GetById { type_id: 3, id: ObjectId::from_u128(500) }), OpResult::NotFound);
    }

    #[test]
    fn on_delete_is_deterministic() {
        use kessel_codec::{encode, Value};
        let build = || {
            let (mut sm, cot) = pc_setup();
            sm.apply(3, Op::AddForeignKey { type_id: 2, field_id: 1, ref_type_id: 1, on_delete: 2 });
            for p in 0..10u128 {
                sm.apply(10 + p as u64, Op::Create { type_id: 1, id: ObjectId::from_u128(p), record: vec![1] });
            }
            for ch in 0..40u128 {
                let parent = ch % 10;
                sm.apply(100 + ch as u64, Op::Create {
                    type_id: 2,
                    id: ObjectId::from_u128(1000 + ch),
                    record: encode(&cot, &[Value::Uint(parent)]).unwrap(),
                });
            }
            for p in 0..10u128 {
                if p % 2 == 0 {
                    sm.apply(500 + p as u64, Op::Delete { type_id: 1, id: ObjectId::from_u128(p) });
                }
            }
            sm.digest()
        };
        assert_eq!(build(), build(), "ON DELETE cascade must be deterministic");
    }

    // ---- Sub-project 19: ON DELETE SET NULL ----

    #[test]
    fn on_delete_set_null_nulls_referencing_fk() {
        use kessel_codec::{decode, encode, Value};
        let (mut sm, cot) = pc_setup(); // parent type1 (a U64), child type2 (pref U128)
        sm.apply(3, Op::Create { type_id: 1, id: ObjectId::from_u128(5), record: vec![1] });
        assert_eq!(
            sm.apply(4, Op::AddForeignKey { type_id: 2, field_id: 1, ref_type_id: 1, on_delete: 3 }),
            OpResult::Ok
        );
        // child 50 references parent 5 (codec record so the null bit is real)
        sm.apply(5, Op::Create {
            type_id: 2,
            id: ObjectId::from_u128(50),
            record: encode(&cot, &[Value::Uint(5)]).unwrap(),
        });
        // sanity: child currently references 5
        assert_eq!(
            ids_of(sm.apply(6, Op::FindBy { type_id: 2, field_id: 1, value: 5u128.to_le_bytes().to_vec() })),
            vec![50]
        );
        // delete the parent -> child's FK is SET NULL, child still exists
        assert_eq!(sm.apply(7, Op::Delete { type_id: 1, id: ObjectId::from_u128(5) }), OpResult::Ok);
        match sm.apply(8, Op::GetById { type_id: 2, id: ObjectId::from_u128(50) }) {
            OpResult::Got(rec) => {
                let vals = decode(&cot, &rec).unwrap();
                assert_eq!(vals[0], Value::Null, "FK field is now NULL");
            }
            o => panic!("child should still exist, got {o:?}"),
        }
        // and it no longer indexes under parent 5
        assert_eq!(
            ids_of(sm.apply(9, Op::FindBy { type_id: 2, field_id: 1, value: 5u128.to_le_bytes().to_vec() })),
            Vec::<u128>::new()
        );
    }

    /// SP86: `ON DELETE SET DEFAULT` (action 4) writes the child FK
    /// column's declared default (a present value, no null bit) when
    /// the parent is deleted; with no declared default it deterministically
    /// degrades to SET NULL.
    #[test]
    fn on_delete_set_default_writes_column_default() {
        use kessel_codec::{decode, encode, Value};
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType {
            def: encode_type_def("parent", &[
                Field { field_id: 0, name: "a".into(), kind: FieldKind::U64, nullable: false },
            ]),
        });
        // child.pref U128 with a declared DEFAULT of 777.
        sm.apply(2, Op::CreateType {
            def: kessel_catalog::encode_type_def_with_defaults(
                "child",
                &[Field { field_id: 0, name: "pref".into(), kind: FieldKind::U128, nullable: true }],
                &[(1u16, 777u128.to_le_bytes().to_vec())],
            ),
        });
        let cot = sm.catalog().get(2).unwrap().clone();
        assert_eq!(
            sm.catalog().get(2).unwrap().defaults,
            vec![(1u16, 777u128.to_le_bytes().to_vec())],
            "child default loaded from the type-def trailer"
        );
        sm.apply(3, Op::Create { type_id: 1, id: ObjectId::from_u128(5), record: vec![1] });
        assert_eq!(
            sm.apply(4, Op::AddForeignKey { type_id: 2, field_id: 1, ref_type_id: 1, on_delete: 4 }),
            OpResult::Ok
        );
        sm.apply(5, Op::Create {
            type_id: 2,
            id: ObjectId::from_u128(50),
            record: encode(&cot, &[Value::Uint(5)]).unwrap(),
        });
        // Delete the parent → child.pref becomes the DEFAULT (777),
        // NOT NULL; child still exists and re-indexes under 777.
        assert_eq!(
            sm.apply(6, Op::Delete { type_id: 1, id: ObjectId::from_u128(5) }),
            OpResult::Ok
        );
        match sm.apply(7, Op::GetById { type_id: 2, id: ObjectId::from_u128(50) }) {
            OpResult::Got(rec) => {
                assert_eq!(
                    decode(&cot, &rec).unwrap()[0],
                    Value::Uint(777),
                    "SET DEFAULT writes the column default, not NULL"
                );
            }
            o => panic!("child should still exist, got {o:?}"),
        }
        assert_eq!(
            ids_of(sm.apply(8, Op::FindBy { type_id: 2, field_id: 1, value: 777u128.to_le_bytes().to_vec() })),
            vec![50],
            "child re-indexed under the default value"
        );

        // No-default child + on_delete=4 ⇒ deterministic SET NULL.
        sm.apply(10, Op::CreateType {
            def: encode_type_def("c2", &[
                Field { field_id: 0, name: "pref".into(), kind: FieldKind::U128, nullable: true },
            ]),
        });
        let c2 = sm.catalog().get(3).unwrap().clone();
        sm.apply(11, Op::Create { type_id: 1, id: ObjectId::from_u128(6), record: vec![2] });
        sm.apply(12, Op::AddForeignKey { type_id: 3, field_id: 1, ref_type_id: 1, on_delete: 4 });
        sm.apply(13, Op::Create {
            type_id: 3,
            id: ObjectId::from_u128(60),
            record: encode(&c2, &[Value::Uint(6)]).unwrap(),
        });
        assert_eq!(
            sm.apply(14, Op::Delete { type_id: 1, id: ObjectId::from_u128(6) }),
            OpResult::Ok
        );
        match sm.apply(15, Op::GetById { type_id: 3, id: ObjectId::from_u128(60) }) {
            OpResult::Got(rec) => assert_eq!(
                decode(&c2, &rec).unwrap()[0],
                Value::Null,
                "no declared default ⇒ SET DEFAULT degrades to SET NULL"
            ),
            o => panic!("child should still exist, got {o:?}"),
        }
    }

    #[test]
    fn on_delete_set_null_is_deterministic() {
        use kessel_codec::{encode, Value};
        let build = || {
            let (mut sm, cot) = pc_setup();
            sm.apply(3, Op::AddForeignKey { type_id: 2, field_id: 1, ref_type_id: 1, on_delete: 3 });
            for p in 0..8u128 {
                sm.apply(10 + p as u64, Op::Create { type_id: 1, id: ObjectId::from_u128(p), record: vec![1] });
            }
            for ch in 0..30u128 {
                sm.apply(100 + ch as u64, Op::Create {
                    type_id: 2,
                    id: ObjectId::from_u128(1000 + ch),
                    record: encode(&cot, &[Value::Uint(ch % 8)]).unwrap(),
                });
            }
            for p in (0..8u128).step_by(2) {
                sm.apply(500 + p as u64, Op::Delete { type_id: 1, id: ObjectId::from_u128(p) });
            }
            sm.digest()
        };
        assert_eq!(build(), build(), "ON DELETE SET NULL must be deterministic");
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
            indexes: vec![], unique: vec![], fks: vec![], checks: vec![], triggers: vec![], ordered: vec![], composite: vec![],
            defaults: vec![],
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

    // ---- Sub-project 14: OR/NOT boolean queries via the expr VM ----

    #[test]
    fn query_expr_or_not_and_combined() {
        use kessel_expr::Program;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: q_type_def() }); // owner u32 f1, kind u16 f2, v u32 f3
        // rows: (owner, kind, v)
        let rows = [(100, 1, 5), (200, 2, 6), (300, 9, 7), (100, 9, 8), (400, 1, 99)];
        for (i, (o, k, v)) in rows.iter().enumerate() {
            sm.apply(10 + i as u64, Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(i as u128),
                record: qrec(*o, *k, *v),
            });
        }
        // OR: owner==100 OR owner==200  -> rows 0,1,3
        let p_or = Program::new()
            .load(1).push_int(100).eq()
            .load(1).push_int(200).eq()
            .or()
            .bytes();
        let mut g = qids(sm.apply(20, Op::QueryExpr { type_id: 1, program: p_or }));
        g.sort();
        assert_eq!(g, vec![0, 1, 3]);
        // NOT: NOT(kind==9)  -> rows 0,1,4
        let p_not = Program::new().load(2).push_int(9).eq().not().bytes();
        let mut g = qids(sm.apply(21, Op::QueryExpr { type_id: 1, program: p_not }));
        g.sort();
        assert_eq!(g, vec![0, 1, 4]);
        // combined: (owner==100 AND v>=8) OR kind==2  -> row 3 (100,_,8), row 1 (kind 2)
        let p_c = Program::new()
            .load(1).push_int(100).eq()
            .load(3).push_int(8).ge()
            .and()
            .load(2).push_int(2).eq()
            .or()
            .bytes();
        let mut g = qids(sm.apply(22, Op::QueryExpr { type_id: 1, program: p_c }));
        g.sort();
        assert_eq!(g, vec![1, 3]);
        // empty result is well-formed
        let p_none = Program::new().load(1).push_int(99999).eq().bytes();
        assert_eq!(qids(sm.apply(23, Op::QueryExpr { type_id: 1, program: p_none })), Vec::<u128>::new());
    }

    #[test]
    fn query_expr_is_readonly_and_deterministic() {
        use kessel_expr::Program;
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: q_type_def() });
            for i in 0..40u64 {
                sm.apply(2 + i, Op::Create {
                    type_id: 1,
                    id: ObjectId::from_u128(i as u128),
                    record: qrec((i % 5) as u32, 0, i as u32),
                });
            }
            let d0 = sm.digest();
            let p = Program::new()
                .load(1).push_int(2).eq()
                .load(1).push_int(4).eq()
                .or()
                .bytes();
            let r = qids(sm.apply(99, Op::QueryExpr { type_id: 1, program: p }));
            (r, d0, sm.digest())
        };
        let (ids_a, before, after) = build();
        let (ids_b, _, _) = build();
        assert_eq!(before, after, "QueryExpr must not mutate state");
        assert_eq!(ids_a, ids_b, "QueryExpr must be deterministic");
        assert!(!ids_a.is_empty());
    }

    // ---- Sub-project 15: order-preserving range index ----

    fn rng_type() -> Vec<u8> {
        encode_type_def("rng", &[
            Field { field_id: 0, name: "score".into(), kind: FieldKind::I32, nullable: false },
            Field { field_id: 0, name: "big".into(), kind: FieldKind::U64, nullable: false },
        ])
    }
    fn rng_off() -> (usize, usize) {
        let ot = ObjectType {
            type_id: 1, name: "rng".into(), schema_ver: 1,
            fields: vec![
                Field { field_id: 1, name: "score".into(), kind: FieldKind::I32, nullable: false },
                Field { field_id: 2, name: "big".into(), kind: FieldKind::U64, nullable: false },
            ],
            indexes: vec![], unique: vec![], fks: vec![], checks: vec![], triggers: vec![], ordered: vec![], composite: vec![],
            defaults: vec![],
        };
        let l = ot.compute_layout();
        (l.offsets[0], l.offsets[1])
    }
    fn rrec(score: i32, big: u64) -> Vec<u8> {
        let (o0, o1) = rng_off();
        let mut b = vec![0u8; {
            let ot = ObjectType { type_id:1,name:"rng".into(),schema_ver:1,fields:vec![
                Field{field_id:1,name:"score".into(),kind:FieldKind::I32,nullable:false},
                Field{field_id:2,name:"big".into(),kind:FieldKind::U64,nullable:false}],
                indexes:vec![],unique:vec![],fks:vec![],checks:vec![],triggers:vec![],ordered:vec![],composite:vec![],defaults:vec![] };
            ot.compute_layout().record_size
        }];
        b[o0..o0 + 4].copy_from_slice(&score.to_le_bytes());
        b[o1..o1 + 8].copy_from_slice(&big.to_le_bytes());
        b
    }

    #[test]
    fn range_index_signed_ordering_and_maintenance() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: rng_type() });
        // rows id=i with score s
        let scores = [(-5i32), -1, 0, 3, 10];
        for (i, s) in scores.iter().enumerate() {
            sm.apply(2 + i as u64, Op::Create {
                type_id: 1, id: ObjectId::from_u128(i as u128), record: rrec(*s, 0),
            });
        }
        assert_eq!(sm.apply(10, Op::AddOrderedIndex { type_id: 1, field_id: 1 }), OpResult::Ok);
        assert_eq!(sm.apply(11, Op::AddOrderedIndex { type_id: 1, field_id: 1 }), OpResult::Ok); // idempotent
        // range [-2, 5] across the sign boundary -> scores -1(id1),0(id2),3(id3)
        let mut g = ids_of(sm.apply(12, Op::FindRange {
            type_id: 1, field_id: 1,
            lo: (-2i32).to_le_bytes().to_vec(), hi: 5i32.to_le_bytes().to_vec(),
        }));
        g.sort();
        assert_eq!(g, vec![1, 2, 3]);
        // full negative range
        let mut g = ids_of(sm.apply(13, Op::FindRange {
            type_id: 1, field_id: 1,
            lo: (-100i32).to_le_bytes().to_vec(), hi: (-1i32).to_le_bytes().to_vec(),
        }));
        g.sort();
        assert_eq!(g, vec![0, 1]); // -5, -1
        // update id0 score -5 -> 7, then it leaves the negative range and
        // enters [6,8]
        sm.apply(14, Op::Update { type_id: 1, id: ObjectId::from_u128(0), record: rrec(7, 0) });
        assert_eq!(ids_of(sm.apply(15, Op::FindRange {
            type_id: 1, field_id: 1,
            lo: (-100i32).to_le_bytes().to_vec(), hi: (-1i32).to_le_bytes().to_vec(),
        })), vec![1]); // only -1 (id1) left negative
        assert_eq!(ids_of(sm.apply(16, Op::FindRange {
            type_id: 1, field_id: 1,
            lo: 6i32.to_le_bytes().to_vec(), hi: 8i32.to_le_bytes().to_vec(),
        })), vec![0]);
        // delete id3 (score 3) removes it from range
        sm.apply(17, Op::Delete { type_id: 1, id: ObjectId::from_u128(3) });
        assert_eq!(ids_of(sm.apply(18, Op::FindRange {
            type_id: 1, field_id: 1,
            lo: 2i32.to_le_bytes().to_vec(), hi: 4i32.to_le_bytes().to_vec(),
        })), Vec::<u128>::new());
        // unsupported field kind (u128/char) rejected
        assert!(matches!(
            sm.apply(19, Op::AddOrderedIndex { type_id: 1, field_id: 99 }),
            OpResult::SchemaError(_)
        ));
    }

    #[test]
    fn range_index_is_deterministic() {
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: rng_type() });
            sm.apply(2, Op::AddOrderedIndex { type_id: 1, field_id: 1 });
            let mut rng = Rng::new(0x5AFE);
            for op in 3..400u64 {
                let id = ObjectId::from_u128(rng.below(40) as u128);
                match rng.below(4) {
                    0 => { sm.apply(op, Op::Delete { type_id: 1, id }); }
                    _ => {
                        let s = (rng.below(200) as i32) - 100;
                        sm.apply(op, Op::Create { type_id: 1, id, record: rrec(s, op) });
                        sm.apply(op + 100000, Op::Update { type_id: 1, id, record: rrec(s / 2, op) });
                    }
                }
            }
            sm.digest()
        };
        assert_eq!(build(), build(), "range index must be deterministic");
    }

    // ---- Sub-project 18: Select (rows + filter + LIMIT) ----

    fn sel_rows(r: OpResult) -> Vec<Vec<u8>> {
        match r {
            OpResult::Got(b) => {
                let mut out = Vec::new();
                let mut p = 0;
                while p + 4 <= b.len() {
                    let l = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                    p += 4;
                    out.push(b[p..p + l].to_vec());
                    p += l;
                }
                out
            }
            o => panic!("expected Got, got {o:?}"),
        }
    }

    #[test]
    fn select_returns_filtered_rows_with_limit() {
        use kessel_expr::Program;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: q_type_def() }); // owner u32 f1, kind u16 f2, v u32 f3
        for i in 0..20u64 {
            sm.apply(2 + i, Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(i as u128),
                record: qrec(if i < 8 { 100 } else { 200 }, 0, i as u32),
            });
        }
        // filter owner==100 -> 8 rows; unlimited
        let prog = Program::new().load(1).push_int(100).eq().bytes();
        let rows = sel_rows(sm.apply(30, Op::Select { type_id: 1, program: prog.clone(), limit: 0 }));
        assert_eq!(rows.len(), 8);
        // each returned blob is a full fixed record
        let rsz = qrec(0, 0, 0).len();
        assert!(rows.iter().all(|r| r.len() == rsz));
        // LIMIT 3 caps it
        let rows = sel_rows(sm.apply(31, Op::Select { type_id: 1, program: prog, limit: 3 }));
        assert_eq!(rows.len(), 3);
        // filter matching nothing -> empty
        let none = Program::new().load(1).push_int(99999).eq().bytes();
        assert_eq!(sel_rows(sm.apply(32, Op::Select { type_id: 1, program: none, limit: 0 })).len(), 0);
    }

    #[test]
    fn select_is_readonly_and_deterministic() {
        use kessel_expr::Program;
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: q_type_def() });
            for i in 0..30u64 {
                sm.apply(2 + i, Op::Create {
                    type_id: 1, id: ObjectId::from_u128(i as u128),
                    record: qrec((i % 4) as u32, 0, i as u32),
                });
            }
            let d0 = sm.digest();
            let prog = Program::new().load(1).push_int(2).lt().bytes(); // owner<2
            let rows = sel_rows(sm.apply(99, Op::Select { type_id: 1, program: prog, limit: 0 }));
            (rows, d0, sm.digest())
        };
        let (a, before, after) = build();
        let (b, _, _) = build();
        assert_eq!(before, after, "Select must not mutate state");
        assert_eq!(a, b, "Select must be deterministic");
        assert!(!a.is_empty());
    }

    // ---- Sub-project 20: aggregates ----

    /// #73 oracle: `MIN`/`MAX` over a `CHAR` / `U128` / `I128`
    /// column (the SP87/SP91 `0xFFFC` keyspace) returns EXACTLY the
    /// brute-force extreme — fast path (no filter + ordered index →
    /// `agg_extreme_var`) AND slow path (filtered, or no index) AND
    /// empty — including `U128 > i128::MAX` and negative `I128`, and
    /// is deterministic. Numeric ≤8B is a separate (unchanged) path.
    #[test]
    fn agg_minmax_over_0xfffc_equals_bruteforce() {
        use kessel_codec::{encode, Value};
        use kessel_expr::Program;
        use kessel_proto::Rng;

        let tdef = encode_type_def(
            "t",
            &[
                Field { field_id: 0, name: "owner".into(), kind: FieldKind::U32, nullable: false },
                Field { field_id: 0, name: "s".into(), kind: FieldKind::Char(8), nullable: false },
                Field { field_id: 0, name: "u".into(), kind: FieldKind::U128, nullable: false },
                Field { field_id: 0, name: "i".into(), kind: FieldKind::I128, nullable: false },
            ],
        );
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: tdef.clone() });
            // Fields are 1-based: owner=1, s=2, u=3, i=4. RANGE INDEX
            // on s(2) and u(3) → fast path; i(4) has NO index → forces
            // the slow path even with no filter.
            sm.apply(2, Op::AddOrderedIndex { type_id: 1, field_id: 2 });
            sm.apply(3, Op::AddOrderedIndex { type_id: 1, field_id: 3 });
            let cot = sm.catalog().get(1).unwrap().clone();
            let mut rng = Rng::new(0x7333_AA);
            let mut model: Vec<(u32, [u8; 8], u128, i128)> = Vec::new();
            for id in 1..=160u128 {
                let owner = rng.below(4) as u32;
                let len = rng.below(5) as usize;
                let mut s = [0u8; 8];
                for c in s.iter_mut().take(len) {
                    *c = b'a' + rng.below(6) as u8;
                }
                // u spans past i128::MAX; i spans negatives.
                let u = (rng.below(u64::MAX) as u128) << 64
                    | rng.below(u64::MAX) as u128;
                let mag =
                    (rng.below(u64::MAX) as i128) << 20 | rng.below(u64::MAX) as i128;
                let i = if rng.below(2) == 0 { -mag } else { mag };
                let rec = encode(
                    &cot,
                    &[
                        Value::Uint(owner as u128),
                        Value::Blob(s.to_vec()),
                        Value::Uint(u),
                        Value::Int(i),
                    ],
                )
                .unwrap();
                sm.apply(
                    10 + id as u64,
                    Op::Create { type_id: 1, id: ObjectId::from_u128(id), record: rec },
                );
                model.push((owner, s, u, i));
            }
            (sm, model)
        };
        let (mut sm, model) = build();
        let agg = |sm: &mut StateMachine<MemVfs>,
                   op: u64,
                   k: u8,
                   fid: u16,
                   p: Vec<u8>|
         -> Vec<u8> {
            match sm.apply(op, Op::Aggregate { type_id: 1, program: p, kind: k, field_id: fid })
            {
                OpResult::Got(b) => b,
                o => panic!("expected Got, got {o:?}"),
            }
        };
        let all = Program::new().push_int(1).bytes();
        let mut op = 2000u64;
        for filter in [None, Some(1u32), Some(99u32)] {
            let (prog, sel): (Vec<u8>, Box<dyn Fn(u32) -> bool>) = match filter {
                None => (all.clone(), Box::new(|_| true)),
                Some(kk) => (
                    Program::new().load(1).push_int(kk as i128).eq().bytes(),
                    Box::new(move |o| o == kk),
                ),
            };
            let rows: Vec<&(u32, [u8; 8], u128, i128)> =
                model.iter().filter(|r| sel(r.0)).collect();
            // ---- CHAR s (f2) ----
            op += 1;
            let got_min = agg(&mut sm, op, 2, 2, prog.clone());
            op += 1;
            let got_max = agg(&mut sm, op, 3, 2, prog.clone());
            if let Some(exp) = rows.iter().map(|r| r.1).min() {
                assert_eq!(got_min, exp.to_vec(), "MIN(s) f={filter:?}");
                let exp = rows.iter().map(|r| r.1).max().unwrap();
                assert_eq!(got_max, exp.to_vec(), "MAX(s) f={filter:?}");
            } else {
                assert!(got_min.is_empty() && got_max.is_empty(), "empty MIN/MAX(s)");
            }
            // ---- U128 u (f3) ----
            op += 1;
            let gmin = agg(&mut sm, op, 2, 3, prog.clone());
            op += 1;
            let gmax = agg(&mut sm, op, 3, 3, prog.clone());
            if let Some(exp) = rows.iter().map(|r| r.2).min() {
                assert_eq!(gmin, exp.to_le_bytes().to_vec(), "MIN(u) f={filter:?}");
                let exp = rows.iter().map(|r| r.2).max().unwrap();
                assert_eq!(gmax, exp.to_le_bytes().to_vec(), "MAX(u) f={filter:?}");
            } else {
                assert!(gmin.is_empty() && gmax.is_empty());
            }
            // ---- I128 i (f4) — NO index ⇒ slow path even unfiltered ----
            op += 1;
            let imin = agg(&mut sm, op, 2, 4, prog.clone());
            op += 1;
            let imax = agg(&mut sm, op, 3, 4, prog.clone());
            if let Some(exp) = rows.iter().map(|r| r.3).min() {
                assert_eq!(imin, exp.to_le_bytes().to_vec(), "MIN(i) f={filter:?}");
                let exp = rows.iter().map(|r| r.3).max().unwrap();
                assert_eq!(imax, exp.to_le_bytes().to_vec(), "MAX(i) f={filter:?}");
            } else {
                assert!(imin.is_empty() && imax.is_empty());
            }
        }
        // SUM/AVG over a CHAR column stay an honest SchemaError.
        assert!(matches!(
            sm.apply(9001, Op::Aggregate { type_id: 1, program: all.clone(), kind: 1, field_id: 2 }),
            OpResult::SchemaError(_)
        ));
        // Deterministic.
        let (a, _) = build();
        let (b, _) = build();
        assert_eq!(a.digest(), b.digest(), "0xFFFC MIN/MAX path must be deterministic");
    }

    #[test]
    fn aggregate_count_sum_min_max() {
        use kessel_expr::Program;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: q_type_def() }); // owner u32 f1, kind u16 f2, v u32 f3
        // rows: owner, v   (v in field 3)
        let data = [(1u32, 10i64), (1, 20), (1, 5), (2, 100), (2, 7)];
        for (i, (o, v)) in data.iter().enumerate() {
            sm.apply(2 + i as u64, Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(i as u128),
                record: qrec(*o, 0, *v as u32),
            });
        }
        let agg = |sm: &mut StateMachine<MemVfs>, op, k, prog: Vec<u8>| -> i128 {
            match sm.apply(op, Op::Aggregate { type_id: 1, program: prog, kind: k, field_id: 3 }) {
                OpResult::Got(b) => i128::from_le_bytes(b.try_into().unwrap()),
                o => panic!("expected Got, got {o:?}"),
            }
        };
        let all = Program::new().push_int(1).bytes(); // always true
        let owner1 = Program::new().load(1).push_int(1).eq().bytes();
        assert_eq!(agg(&mut sm, 20, 0, all.clone()), 5, "COUNT all");
        assert_eq!(agg(&mut sm, 21, 0, owner1.clone()), 3, "COUNT owner=1");
        assert_eq!(agg(&mut sm, 22, 1, owner1.clone()), 35, "SUM v owner=1 (10+20+5)");
        assert_eq!(agg(&mut sm, 23, 2, owner1.clone()), 5, "MIN v owner=1");
        assert_eq!(agg(&mut sm, 24, 3, owner1), 20, "MAX v owner=1");
        assert_eq!(agg(&mut sm, 25, 1, all), 142, "SUM v all (10+20+5+100+7)");
        // no match -> COUNT 0, SUM 0
        let none = Program::new().load(1).push_int(999).eq().bytes();
        assert_eq!(agg(&mut sm, 26, 0, none.clone()), 0);
        assert_eq!(agg(&mut sm, 27, 1, none), 0);
    }

    /// SP73 oracle: the columnar aggregate fast-path (no-WHERE skips the
    /// expr-VM; MIN/MAX of an order-indexed column reads the index
    /// extreme) must return EXACTLY what an independent brute-force model
    /// computes — for every kind, with and without a filter, including
    /// the empty case. This guards that the accelerator never changes
    /// the answer (the only way it could be unsafe).
    #[test]
    fn aggregate_columnar_fastpath_equals_scan_oracle() {
        use kessel_expr::Program;
        use kessel_proto::Rng;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: q_type_def() });
        sm.apply(2, Op::AddOrderedIndex { type_id: 1, field_id: 3 }); // RANGE idx on v
        let mut rng = Rng::new(0xC0FF_EE99);
        let mut model: Vec<(u32, u32)> = Vec::new(); // (owner, v)
        for id in 0..220u128 {
            let o = rng.below(5) as u32;
            let v = rng.below(1000) as u32;
            sm.apply(
                10 + id as u64,
                Op::Create {
                    type_id: 1,
                    id: ObjectId::from_u128(id),
                    record: qrec(o, 0, v),
                },
            );
            model.push((o, v));
        }
        let agg = |sm: &mut StateMachine<MemVfs>, op: u64, k: u8, p: Vec<u8>| -> i128 {
            match sm.apply(op, Op::Aggregate { type_id: 1, program: p, kind: k, field_id: 3 })
            {
                OpResult::Got(b) => i128::from_le_bytes(b.try_into().unwrap()),
                o => panic!("expected Got, got {o:?}"),
            }
        };
        let mut op = 1000u64;
        for filter in [None, Some(0u32), Some(2u32), Some(99u32)] {
            // None ⇒ canonical always-true (fast path, incl. index
            // MIN/MAX). Some(k) ⇒ owner==k (slow path, expr-VM runs).
            let (prog, sel): (Vec<u8>, Box<dyn Fn(u32) -> bool>) = match filter {
                None => (
                    Program::new().push_int(1).bytes(),
                    Box::new(|_| true),
                ),
                Some(kf) => (
                    Program::new().load(1).push_int(kf as i128).eq().bytes(),
                    Box::new(move |o| o == kf),
                ),
            };
            let vs: Vec<i128> = model
                .iter()
                .filter(|(o, _)| sel(*o))
                .map(|(_, v)| *v as i128)
                .collect();
            let cnt = vs.len() as i128;
            let sum: i128 = vs.iter().sum();
            let mn = vs.iter().min().copied().unwrap_or(0);
            let mx = vs.iter().max().copied().unwrap_or(0);
            let avg = if cnt == 0 { 0 } else { sum / cnt };
            op += 1; assert_eq!(agg(&mut sm, op, 0, prog.clone()), cnt, "COUNT {filter:?}");
            op += 1; assert_eq!(agg(&mut sm, op, 1, prog.clone()), sum, "SUM {filter:?}");
            op += 1; assert_eq!(agg(&mut sm, op, 2, prog.clone()), mn, "MIN {filter:?}");
            op += 1; assert_eq!(agg(&mut sm, op, 3, prog.clone()), mx, "MAX {filter:?}");
            op += 1; assert_eq!(agg(&mut sm, op, 4, prog.clone()), avg, "AVG {filter:?}");
        }
    }

    /// SP73: MIN/MAX of an order-indexed column with no filter must
    /// answer from the index extreme — materially faster than the
    /// equivalent full scan — and return the identical value.
    #[test]
    fn min_max_via_index_skips_the_scan() {
        use kessel_expr::Program;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: q_type_def() });
        sm.apply(2, Op::AddOrderedIndex { type_id: 1, field_id: 3 });
        let n = 40_000u128;
        for id in 0..n {
            let v = ((id * 2654435761) % 1_000_000) as u32;
            sm.apply(
                10 + id as u64,
                Op::Create {
                    type_id: 1,
                    id: ObjectId::from_u128(id),
                    record: qrec(0, 0, v),
                },
            );
        }
        // Canonical always-true ⇒ index-extreme fast path.
        let canon = Program::new().push_int(1).bytes();
        // Always-true but NOT the canonical constant ⇒ forces the full
        // scan + per-row expr-VM (the honest baseline to beat).
        let scan = Program::new().push_int(7).push_int(7).eq().bytes();
        let run = |sm: &mut StateMachine<MemVfs>, op, k, p: Vec<u8>| -> (i128, u128) {
            let t = std::time::Instant::now();
            let r = match sm.apply(
                op,
                Op::Aggregate { type_id: 1, program: p, kind: k, field_id: 3 },
            ) {
                OpResult::Got(b) => i128::from_le_bytes(b.try_into().unwrap()),
                o => panic!("{o:?}"),
            };
            (r, t.elapsed().as_micros())
        };
        let (mn_fast, tf) = run(&mut sm, 90, 2, canon.clone());
        let (mn_scan, ts) = run(&mut sm, 91, 2, scan.clone());
        let (mx_fast, _) = run(&mut sm, 92, 3, canon);
        let (mx_scan, _) = run(&mut sm, 93, 3, scan);
        assert_eq!(mn_fast, mn_scan, "index MIN == scan MIN");
        assert_eq!(mx_fast, mx_scan, "index MAX == scan MAX");
        println!(
            "[agg-fastpath] MIN over {n} rows: index {tf}µs vs full-scan \
             {ts}µs  (~{:.0}x)",
            ts as f64 / tf.max(1) as f64
        );
        assert!(
            tf * 3 < ts,
            "MIN via order index must skip the scan (index {tf}µs vs \
             scan {ts}µs)"
        );
    }

    #[test]
    fn aggregate_avg_integer() {
        use kessel_expr::Program;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: q_type_def() });
        for (i, v) in [10u32, 20, 5, 100, 7].iter().enumerate() {
            sm.apply(2 + i as u64, Op::Create {
                type_id: 1, id: ObjectId::from_u128(i as u128),
                record: qrec(if i < 3 { 1 } else { 2 }, 0, *v),
            });
        }
        let avg = |sm: &mut StateMachine<MemVfs>, op, prog: Vec<u8>| -> i128 {
            match sm.apply(op, Op::Aggregate { type_id: 1, program: prog, kind: 4, field_id: 3 }) {
                OpResult::Got(b) => i128::from_le_bytes(b.try_into().unwrap()),
                o => panic!("{o:?}"),
            }
        };
        let owner1 = Program::new().load(1).push_int(1).eq().bytes();
        // owner=1: v {10,20,5} -> 35/3 = 11 (integer floor)
        assert_eq!(avg(&mut sm, 20, owner1), 11);
        // all: 142/5 = 28
        assert_eq!(avg(&mut sm, 21, Program::new().push_int(1).bytes()), 28);
        // empty -> 0 (no div-by-zero)
        let none = Program::new().load(1).push_int(999).eq().bytes();
        assert_eq!(avg(&mut sm, 22, none), 0);
    }

    #[test]
    fn aggregate_is_readonly_and_deterministic() {
        use kessel_expr::Program;
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: q_type_def() });
            for i in 0..40u64 {
                sm.apply(2 + i, Op::Create {
                    type_id: 1, id: ObjectId::from_u128(i as u128),
                    record: qrec((i % 4) as u32, 0, i as u32),
                });
            }
            let d0 = sm.digest();
            let prog = Program::new().push_int(1).bytes();
            let s = match sm.apply(99, Op::Aggregate { type_id: 1, program: prog, kind: 1, field_id: 3 }) {
                OpResult::Got(b) => i128::from_le_bytes(b.try_into().unwrap()),
                o => panic!("{o:?}"),
            };
            (s, d0, sm.digest())
        };
        let (a, before, after) = build();
        let (b, _, _) = build();
        assert_eq!(before, after, "Aggregate must not mutate state");
        assert_eq!(a, b, "Aggregate must be deterministic");
        assert_eq!(a, (0..40).sum::<i128>(), "SUM 0..40");
    }

    // ---- Sub-project 21: projection (SelectFields) ----

    #[test]
    fn select_fields_projects_chosen_columns() {
        use kessel_expr::Program;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: q_type_def() }); // owner u32 f1, kind u16 f2, v u32 f3
        for i in 0..10u64 {
            sm.apply(2 + i, Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(i as u128),
                record: qrec(100 + i as u32, 7, 1000 + i as u32),
            });
        }
        // project [owner(f1,4B), v(f3,4B)] for all rows -> rows of 8 bytes
        let prog = Program::new().push_int(1).bytes();
        match sm.apply(20, Op::SelectFields { type_id: 1, program: prog, fields: vec![1, 3], limit: 0 }) {
            OpResult::Got(b) => {
                let mut p = 0;
                let mut rows = Vec::new();
                while p + 4 <= b.len() {
                    let l = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                    p += 4;
                    rows.push(b[p..p + l].to_vec());
                    p += l;
                }
                assert_eq!(rows.len(), 10);
                assert!(rows.iter().all(|r| r.len() == 8), "owner(4)+v(4)");
                // row 0: owner=100, v=1000
                assert_eq!(u32::from_le_bytes(rows[0][0..4].try_into().unwrap()), 100);
                assert_eq!(u32::from_le_bytes(rows[0][4..8].try_into().unwrap()), 1000);
            }
            o => panic!("{o:?}"),
        }
        // unknown field rejected
        assert!(matches!(
            sm.apply(21, Op::SelectFields { type_id: 1, program: Program::new().push_int(1).bytes(), fields: vec![999], limit: 0 }),
            OpResult::SchemaError(_)
        ));
    }

    #[test]
    fn select_fields_is_readonly_and_deterministic() {
        use kessel_expr::Program;
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: q_type_def() });
            for i in 0..30u64 {
                sm.apply(2 + i, Op::Create {
                    type_id: 1, id: ObjectId::from_u128(i as u128),
                    record: qrec((i % 3) as u32, 0, i as u32),
                });
            }
            let d0 = sm.digest();
            let prog = Program::new().load(1).push_int(1).eq().bytes();
            let r = match sm.apply(99, Op::SelectFields { type_id: 1, program: prog, fields: vec![3], limit: 0 }) {
                OpResult::Got(b) => b,
                o => panic!("{o:?}"),
            };
            (r, d0, sm.digest())
        };
        let (a, before, after) = build();
        let (b, _, _) = build();
        assert_eq!(before, after, "SelectFields must not mutate state");
        assert_eq!(a, b, "SelectFields must be deterministic");
        assert!(!a.is_empty());
    }

    // ---- Sub-project 22: GROUP BY aggregation ----

    #[test]
    fn group_aggregate_sum_and_count_per_group() {
        use kessel_expr::Program;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: q_type_def() }); // owner u32 f1, kind u16 f2, v u32 f3
        // (owner, v): owner is group key (4 bytes), v aggregated
        let data = [(1u32, 10u32), (1, 20), (2, 5), (2, 7), (2, 8), (3, 100)];
        for (i, (o, v)) in data.iter().enumerate() {
            sm.apply(2 + i as u64, Op::Create {
                type_id: 1, id: ObjectId::from_u128(i as u128), record: qrec(*o, 0, *v),
            });
        }
        let parse = |b: Vec<u8>| -> Vec<(u32, i128)> {
            let n = u32::from_le_bytes(b[0..4].try_into().unwrap()) as usize;
            let mut p = 4;
            let mut g = Vec::new();
            for _ in 0..n {
                let kl = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                p += 4;
                let key = u32::from_le_bytes(b[p..p + 4].try_into().unwrap());
                p += kl;
                let val = i128::from_le_bytes(b[p..p + 16].try_into().unwrap());
                p += 16;
                g.push((key, val));
            }
            g
        };
        let all = Program::new().push_int(1).bytes();
        // SUM(v) GROUP BY owner -> {1:30, 2:20, 3:100} ascending key order
        match sm.apply(20, Op::GroupAggregate { type_id: 1, program: all.clone(), group_field: 1, kind: 1, agg_field: 3 }) {
            OpResult::Got(b) => assert_eq!(parse(b), vec![(1, 30), (2, 20), (3, 100)]),
            o => panic!("{o:?}"),
        }
        // COUNT GROUP BY owner -> {1:2, 2:3, 3:1}
        match sm.apply(21, Op::GroupAggregate { type_id: 1, program: all.clone(), group_field: 1, kind: 0, agg_field: 0 }) {
            OpResult::Got(b) => assert_eq!(parse(b), vec![(1, 2), (2, 3), (3, 1)]),
            o => panic!("{o:?}"),
        }
        // MAX(v) GROUP BY owner -> {1:20, 2:8, 3:100}
        match sm.apply(22, Op::GroupAggregate { type_id: 1, program: all, group_field: 1, kind: 3, agg_field: 3 }) {
            OpResult::Got(b) => assert_eq!(parse(b), vec![(1, 20), (2, 8), (3, 100)]),
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn group_aggregate_is_readonly_and_deterministic() {
        use kessel_expr::Program;
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: q_type_def() });
            for i in 0..40u64 {
                sm.apply(2 + i, Op::Create {
                    type_id: 1, id: ObjectId::from_u128(i as u128),
                    record: qrec((i % 5) as u32, 0, i as u32),
                });
            }
            let d0 = sm.digest();
            let r = match sm.apply(99, Op::GroupAggregate {
                type_id: 1, program: Program::new().push_int(1).bytes(),
                group_field: 1, kind: 1, agg_field: 3,
            }) {
                OpResult::Got(b) => b,
                o => panic!("{o:?}"),
            };
            (r, d0, sm.digest())
        };
        let (a, before, after) = build();
        let (b, _, _) = build();
        assert_eq!(before, after, "GroupAggregate must not mutate state");
        assert_eq!(a, b, "GroupAggregate must be deterministic");
    }

    // ---- Sub-project 23: ORDER BY + OFFSET/LIMIT ----

    #[test]
    fn select_sorted_orders_and_paginates() {
        use kessel_expr::Program;
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: q_type_def() }); // owner f1, kind f2, v(u32) f3
        // insert in scrambled v order
        let vs = [50u32, 10, 90, 30, 70, 20, 80, 40, 60, 0];
        for (i, v) in vs.iter().enumerate() {
            sm.apply(2 + i as u64, Op::Create {
                type_id: 1, id: ObjectId::from_u128(i as u128), record: qrec(1, 0, *v),
            });
        }
        let read_v = |b: &[u8]| -> Vec<u32> {
            let mut p = 0;
            let mut out = Vec::new();
            let (o0, _) = q_v_off();
            while p + 4 <= b.len() {
                let l = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                p += 4;
                let rec = &b[p..p + l];
                out.push(u32::from_le_bytes(rec[o0..o0 + 4].try_into().unwrap()));
                p += l;
            }
            out
        };
        let all = Program::new().push_int(1).bytes();
        // ascending by v, all
        match sm.apply(20, Op::SelectSorted { type_id: 1, program: all.clone(), sort_field: 3, desc: false, offset: 0, limit: 0 }) {
            OpResult::Got(b) => assert_eq!(read_v(&b), vec![0,10,20,30,40,50,60,70,80,90]),
            o => panic!("{o:?}"),
        }
        // descending, offset 2 limit 3 -> skip 90,80 -> [70,60,50]
        match sm.apply(21, Op::SelectSorted { type_id: 1, program: all, sort_field: 3, desc: true, offset: 2, limit: 3 }) {
            OpResult::Got(b) => assert_eq!(read_v(&b), vec![70, 60, 50]),
            o => panic!("{o:?}"),
        }
    }

    fn q_v_off() -> (usize, usize) {
        // offset of field 3 (v) in the q_type layout
        let ot = ObjectType {
            type_id: 1, name: "q".into(), schema_ver: 1,
            fields: vec![
                Field { field_id: 1, name: "owner".into(), kind: FieldKind::U32, nullable: false },
                Field { field_id: 2, name: "kind".into(), kind: FieldKind::U16, nullable: false },
                Field { field_id: 3, name: "v".into(), kind: FieldKind::U32, nullable: false },
            ],
            indexes: vec![], unique: vec![], fks: vec![], checks: vec![], triggers: vec![], ordered: vec![], composite: vec![],
            defaults: vec![],
        };
        let l = ot.compute_layout();
        (l.offsets[2], 4)
    }

    #[test]
    fn select_sorted_is_deterministic() {
        use kessel_expr::Program;
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: q_type_def() });
            for i in 0..30u64 {
                sm.apply(2 + i, Op::Create {
                    type_id: 1, id: ObjectId::from_u128(i as u128),
                    record: qrec(0, 0, (i * 7 % 11) as u32),
                });
            }
            let d0 = sm.digest();
            let r = match sm.apply(99, Op::SelectSorted {
                type_id: 1, program: Program::new().push_int(1).bytes(),
                sort_field: 3, desc: false, offset: 1, limit: 10,
            }) { OpResult::Got(b) => b, o => panic!("{o:?}") };
            (r, d0, sm.digest())
        };
        let (a, before, after) = build();
        let (b, _, _) = build();
        assert_eq!(before, after);
        assert_eq!(a, b, "SelectSorted must be deterministic");
    }

    // ---- Sub-project 27: composite (multi-field) indexes ----

    #[test]
    fn composite_index_find_and_maintenance() {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: q_type_def() }); // owner u32 f1, kind u16 f2, v u32 f3
        // rows (owner, kind)
        let rows = [(100u32, 1u16), (100, 2), (100, 1), (200, 1), (100, 2)];
        for (i, (o, k)) in rows.iter().enumerate() {
            sm.apply(2 + i as u64, Op::Create {
                type_id: 1, id: ObjectId::from_u128(i as u128), record: qrec(*o, *k, i as u32),
            });
        }
        // composite index on (owner, kind) — backfill
        assert_eq!(sm.apply(20, Op::AddCompositeIndex { type_id: 1, fields: vec![1, 2] }), OpResult::Ok);
        assert_eq!(sm.apply(21, Op::AddCompositeIndex { type_id: 1, fields: vec![1, 2] }), OpResult::Ok); // idempotent
        // (owner=100, kind=1) -> ids 0,2
        let q = |sm: &mut StateMachine<MemVfs>, op, o: u32, k: u16| -> Vec<u128> {
            ids_of(sm.apply(op, Op::FindByComposite {
                type_id: 1, fields: vec![1, 2],
                values: vec![o.to_le_bytes().to_vec(), k.to_le_bytes().to_vec()],
            }))
        };
        let mut g = q(&mut sm, 22, 100, 1); g.sort();
        assert_eq!(g, vec![0, 2]);
        let mut g = q(&mut sm, 23, 100, 2); g.sort();
        assert_eq!(g, vec![1, 4]);
        assert_eq!(q(&mut sm, 24, 999, 9), Vec::<u128>::new());
        // update row 0 -> (100,2): leaves (100,1), joins (100,2)
        sm.apply(25, Op::Update { type_id: 1, id: ObjectId::from_u128(0), record: qrec(100, 2, 0) });
        assert_eq!(q(&mut sm, 26, 100, 1), vec![2]);
        let mut g = q(&mut sm, 27, 100, 2); g.sort();
        assert_eq!(g, vec![0, 1, 4]);
        // delete row 4 -> drops from (100,2)
        sm.apply(28, Op::Delete { type_id: 1, id: ObjectId::from_u128(4) });
        let mut g = q(&mut sm, 29, 100, 2); g.sort();
        assert_eq!(g, vec![0, 1]);
    }

    #[test]
    fn composite_index_is_deterministic() {
        let build = || {
            let mut sm = StateMachine::open(MemVfs::new()).unwrap();
            sm.apply(1, Op::CreateType { def: q_type_def() });
            sm.apply(2, Op::AddCompositeIndex { type_id: 1, fields: vec![1, 2] });
            let mut rng = Rng::new(0xC0FFEE);
            for op in 3..400u64 {
                let id = ObjectId::from_u128(rng.below(40) as u128);
                match rng.below(4) {
                    0 => { sm.apply(op, Op::Delete { type_id: 1, id }); }
                    _ => {
                        sm.apply(op, Op::Create {
                            type_id: 1, id,
                            record: qrec((rng.below(5)) as u32, (rng.below(3)) as u16, op as u32),
                        });
                    }
                }
            }
            sm.digest()
        };
        assert_eq!(build(), build(), "composite index must be deterministic");
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
            sm.apply(4, Op::AddForeignKey { type_id: 2, field_id: 1, ref_type_id: 1, on_delete: 0 }),
            OpResult::Ok
        );
        assert_eq!(
            sm.apply(5, Op::AddForeignKey { type_id: 2, field_id: 1, ref_type_id: 1, on_delete: 0 }),
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
            sm.apply(6, Op::AddForeignKey { type_id: 2, field_id: 1, ref_type_id: 1, on_delete: 0 }),
            OpResult::Constraint(_)
        ));
        // fix the dangling row, then it succeeds and is enforced
        sm.apply(7, Op::Update { type_id: 2, id: ObjectId::from_u128(2), record: encode(&cot, &[Value::Uint(5)]).unwrap() });
        assert_eq!(sm.apply(8, Op::AddForeignKey { type_id: 2, field_id: 1, ref_type_id: 1, on_delete: 0 }), OpResult::Ok);
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
