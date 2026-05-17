//! kessel-storage: a small LSM (memtable + immutable SSTables + compaction)
//! on a write-ahead log, all over the `kessel-io` VFS so the simulator can
//! crash it deterministically.
//!
//! Crash-safety ordering on flush:
//!   1. write new SSTable blob, `sync`
//!   2. write MANIFEST referencing it, `sync`
//!   3. only then reset the WAL
//! A crash between any two steps recovers to a consistent state because
//! re-applying a WAL entry is idempotent (latest-write-wins, same bytes).

#![forbid(unsafe_code)]

use kessel_io::{Disk, Vfs};
use kessel_proto::codec::crc32c;
use std::collections::BTreeMap;
use std::io;

/// Variable-length, lexicographically-ordered key (SP24). Data rows still
/// use `type_id (4, LE) ++ object_id (16)` = 20 bytes (a type's rows remain
/// a contiguous range), but the key type is now `Vec<u8>` so indexes can use
/// per-(value,object) keys without a read-modify-write bucket.
pub type Key = Vec<u8>;

pub fn make_key(type_id: u32, object_id: &[u8; 16]) -> Key {
    let mut k = Vec::with_capacity(20);
    k.extend_from_slice(&type_id.to_le_bytes());
    k.extend_from_slice(object_id);
    k
}

const WAL_NAME: &str = "wal";
const MANIFEST_NAME: &str = "MANIFEST";
const SST_MAGIC: u32 = 0x4B53_5354; // "KSST"
const MAN_MAGIC: u32 = 0x4B4D_414E; // "KMAN"

// ----------------------------------------------------------------------------
// WAL
// ----------------------------------------------------------------------------

/// One logical mutation. `None` value = tombstone (delete).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub op_number: u64,
    pub key: Key,
    pub value: Option<Vec<u8>>,
}

fn encode_entry(e: &Entry) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&e.op_number.to_le_bytes());
    p.extend_from_slice(&(e.key.len() as u16).to_le_bytes());
    p.extend_from_slice(&e.key);
    match &e.value {
        Some(v) => {
            p.push(0);
            p.extend_from_slice(&(v.len() as u32).to_le_bytes());
            p.extend_from_slice(v);
        }
        None => p.push(1),
    }
    p
}

fn decode_entry(p: &[u8]) -> Option<Entry> {
    if p.len() < 8 + 2 + 1 {
        return None;
    }
    let op_number = u64::from_le_bytes(p[0..8].try_into().ok()?);
    let kl = u16::from_le_bytes(p[8..10].try_into().ok()?) as usize;
    let key = p.get(10..10 + kl)?.to_vec();
    let mut q = 10 + kl;
    let value = match *p.get(q)? {
        0 => {
            q += 1;
            let vl = u32::from_le_bytes(p.get(q..q + 4)?.try_into().ok()?) as usize;
            q += 4;
            Some(p.get(q..q + vl)?.to_vec())
        }
        1 => None,
        _ => return None,
    };
    Some(Entry {
        op_number,
        key,
        value,
    })
}

/// Append-only log. Frame = `u32 payload_len ++ u32 crc32c(payload) ++ payload`.
struct Wal {
    disk: Box<dyn Disk>,
    end: u64,
}

impl Wal {
    fn open(vfs: &dyn Vfs) -> io::Result<Self> {
        let disk = vfs.open(WAL_NAME)?;
        let end = disk.len();
        Ok(Wal { disk, end })
    }

    fn append(&mut self, e: &Entry) -> io::Result<()> {
        let payload = encode_entry(e);
        let mut frame = Vec::with_capacity(8 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc32c(&payload).to_le_bytes());
        frame.extend_from_slice(&payload);
        self.disk.write_at(self.end, &frame)?;
        self.end += frame.len() as u64;
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.disk.sync()
    }

    /// Replay every intact frame, stopping at the first short/corrupt one
    /// (the unsynced tail after a crash).
    fn replay(&self) -> Vec<Entry> {
        let mut out = Vec::new();
        let total = self.disk.len();
        let mut off = 0u64;
        while off + 8 <= total {
            let mut hdr = [0u8; 8];
            if self.disk.read_at(off, &mut hdr).unwrap_or(0) < 8 {
                break;
            }
            let plen = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as u64;
            let crc = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
            if off + 8 + plen > total {
                break; // torn tail
            }
            let mut payload = vec![0u8; plen as usize];
            if self.disk.read_at(off + 8, &mut payload).unwrap_or(0) < plen as usize {
                break;
            }
            if crc32c(&payload) != crc {
                break; // corrupt tail
            }
            match decode_entry(&payload) {
                Some(e) => out.push(e),
                None => break,
            }
            off += 8 + plen;
        }
        out
    }
}

// ----------------------------------------------------------------------------
// SSTable
// ----------------------------------------------------------------------------

fn write_sstable(
    vfs: &dyn Vfs,
    name: &str,
    entries: &BTreeMap<Key, Option<Vec<u8>>>,
) -> io::Result<()> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&SST_MAGIC.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (k, v) in entries {
        buf.extend_from_slice(&(k.len() as u16).to_le_bytes());
        buf.extend_from_slice(k);
        match v {
            Some(val) => {
                buf.push(0);
                buf.extend_from_slice(&(val.len() as u32).to_le_bytes());
                buf.extend_from_slice(val);
            }
            None => buf.push(1),
        }
    }
    let crc = crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&SST_MAGIC.to_le_bytes());
    let mut disk = vfs.open(name)?;
    disk.write_at(0, &buf)?;
    disk.sync()?;
    Ok(())
}

/// Fully-loaded, sorted, integrity-checked SSTable (M1: simple & correct;
/// block index / bloom filter is a later perf concern).
/// Zero-dep Bloom filter (SP48). Built once per SSTable from its keys so a
/// point `get` can skip a table that *definitely* does not hold the key in
/// O(1) instead of binary-searching it — turning the long-standing
/// O(#sstables) read path into O(1) expected. No false negatives (so
/// tombstone/shadow correctness is preserved); ~1% false positives only
/// cost one needless binary search. In-memory only — rebuilt from entries
/// at open, so the on-disk format is unchanged.
struct Bloom {
    bits: Vec<u64>,
    mask: u64,
    k: u32,
}

impl Bloom {
    /// FNV-1a 64 of a key.
    #[inline]
    fn hash(key: &[u8]) -> u64 {
        let mut h = 0xcbf29ce484222325u64;
        for &b in key {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h
    }

    fn build<'a, I: Iterator<Item = &'a Key>>(n: usize, keys: I) -> Self {
        // ~10 bits/key, power-of-two word count → ~1% FPR at k=7.
        let bits_target = (n.max(1) as u64) * 10;
        let words = (bits_target / 64 + 1).next_power_of_two() as usize;
        let mut b = Bloom {
            bits: vec![0u64; words],
            mask: (words as u64 * 64) - 1,
            k: 7,
        };
        for key in keys {
            b.insert(key);
        }
        b
    }

    #[inline]
    fn positions(&self, key: &[u8]) -> (u64, u64) {
        let h = Bloom::hash(key);
        // double hashing: bit_i = h1 + i*h2 (h2 forced odd, nonzero)
        (h, (h >> 32) | 1)
    }

    #[inline]
    fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.positions(key);
        for i in 0..self.k as u64 {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) & self.mask;
            self.bits[(bit >> 6) as usize] |= 1u64 << (bit & 63);
        }
    }

    #[inline]
    fn maybe_contains(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.positions(key);
        for i in 0..self.k as u64 {
            let bit = h1.wrapping_add(i.wrapping_mul(h2)) & self.mask;
            if self.bits[(bit >> 6) as usize] & (1u64 << (bit & 63)) == 0 {
                return false; // definitely absent
            }
        }
        true // probably present (or a ~1% false positive)
    }
}

struct SsTable {
    entries: Vec<(Key, Option<Vec<u8>>)>,
    bloom: Bloom,
}

impl SsTable {
    /// Cheap O(1) pruning: does this table's `[min,max]` key span intersect
    /// the query range `[lo,hi]`? `entries` is sorted, so the bounds are the
    /// first/last keys. Lets a prefix/range scan skip whole SSTables that
    /// cannot contribute — the SP45 point-read win in a large LSM (turns
    /// O(#sstables · log n) into O(#overlapping · log n)).
    #[inline]
    fn overlaps(&self, lo: &Key, hi: &Key) -> bool {
        match (self.entries.first(), self.entries.last()) {
            (Some((min, _)), Some((max, _))) => min <= hi && max >= lo,
            _ => false, // empty table contributes nothing
        }
    }
}

impl SsTable {
    fn open(vfs: &dyn Vfs, name: &str) -> io::Result<Self> {
        let disk = vfs.open(name)?;
        let len = disk.len() as usize;
        let mut buf = vec![0u8; len];
        disk.read_at(0, &mut buf)?;
        if len < 16 || u32::from_le_bytes(buf[0..4].try_into().unwrap()) != SST_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad sstable magic"));
        }
        let body_crc = u32::from_le_bytes(buf[len - 8..len - 4].try_into().unwrap());
        if crc32c(&buf[..len - 8]) != body_crc {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "sstable crc"));
        }
        let count = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        let mut entries = Vec::with_capacity(count);
        let mut p = 8usize;
        for _ in 0..count {
            let kl = u16::from_le_bytes(buf[p..p + 2].try_into().unwrap()) as usize;
            p += 2;
            let key = buf[p..p + kl].to_vec();
            p += kl;
            let tag = buf[p];
            p += 1;
            let val = if tag == 0 {
                let vl = u32::from_le_bytes(buf[p..p + 4].try_into().unwrap()) as usize;
                p += 4;
                let v = buf[p..p + vl].to_vec();
                p += vl;
                Some(v)
            } else {
                None
            };
            entries.push((key, val));
        }
        let bloom = Bloom::build(entries.len(), entries.iter().map(|(k, _)| k));
        Ok(SsTable { entries, bloom })
    }

    fn get(&self, key: &Key) -> Option<&Option<Vec<u8>>> {
        // O(1) reject of a definite miss before the binary search — the
        // SP48 point-read fast path. No false negatives, so this never
        // skips a table that actually holds the key (shadow/tombstone
        // semantics preserved by the newest-first caller).
        if !self.bloom.maybe_contains(key) {
            return None;
        }
        self.entries
            .binary_search_by(|(k, _)| k.cmp(key))
            .ok()
            .map(|i| &self.entries[i].1)
    }
}

// ----------------------------------------------------------------------------
// Manifest
// ----------------------------------------------------------------------------

#[derive(Default, Clone)]
struct Manifest {
    /// Oldest -> newest. Newer SSTables shadow older ones.
    sstables: Vec<String>,
    next_sst: u64,
}

fn read_manifest(vfs: &dyn Vfs) -> Manifest {
    if !vfs.exists(MANIFEST_NAME) {
        return Manifest::default();
    }
    let disk = match vfs.open(MANIFEST_NAME) {
        Ok(d) => d,
        Err(_) => return Manifest::default(),
    };
    let len = disk.len() as usize;
    if len < 12 {
        return Manifest::default();
    }
    let mut buf = vec![0u8; len];
    disk.read_at(0, &mut buf).ok();
    if u32::from_le_bytes(buf[0..4].try_into().unwrap()) != MAN_MAGIC {
        return Manifest::default();
    }
    let stored_crc = u32::from_le_bytes(buf[len - 4..len].try_into().unwrap());
    if crc32c(&buf[..len - 4]) != stored_crc {
        return Manifest::default(); // torn manifest write -> rebuild from WAL
    }
    let next_sst = u64::from_le_bytes(buf[4..12].try_into().unwrap());
    let n = u32::from_le_bytes(buf[12..16].try_into().unwrap()) as usize;
    let mut p = 16usize;
    let mut sstables = Vec::with_capacity(n);
    for _ in 0..n {
        let nl = u32::from_le_bytes(buf[p..p + 4].try_into().unwrap()) as usize;
        p += 4;
        sstables.push(String::from_utf8_lossy(&buf[p..p + nl]).into_owned());
        p += nl;
    }
    Manifest { sstables, next_sst }
}

fn write_manifest(vfs: &dyn Vfs, m: &Manifest) -> io::Result<()> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&MAN_MAGIC.to_le_bytes());
    buf.extend_from_slice(&m.next_sst.to_le_bytes());
    buf.extend_from_slice(&(m.sstables.len() as u32).to_le_bytes());
    for s in &m.sstables {
        buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
        buf.extend_from_slice(s.as_bytes());
    }
    let crc = crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    let mut disk = vfs.open(MANIFEST_NAME)?;
    disk.write_at(0, &buf)?;
    disk.sync()?;
    Ok(())
}

// ----------------------------------------------------------------------------
// LSM storage engine
// ----------------------------------------------------------------------------

pub struct Storage<V: Vfs> {
    vfs: V,
    memtable: BTreeMap<Key, Option<Vec<u8>>>,
    sstables: Vec<SsTable>,
    manifest: Manifest,
    wal: Wal,
    /// When false, `commit` appends to the WAL without fsync; the caller
    /// must call `sync()` to make the group durable (TB-style group commit).
    autosync: bool,
    /// Active transaction overlay (Sub-project 9). While `Some`, writes are
    /// buffered here (NOT in WAL/memtable) and reads see it first, so a
    /// transaction is all-or-nothing: `commit_txn` flushes it atomically,
    /// `abort_txn` discards it leaving zero trace.
    txn: Option<BTreeMap<Key, (u64, Option<Vec<u8>>)>>,
    /// SP49: if > 0, `flush` auto-`compact`s once the live segment count
    /// reaches this, so the point-read fan-out stays bounded (≈ O(1) in
    /// total data instead of O(#flushes)). 0 = off (raw primitive
    /// behaviour, unchanged — every existing storage test relies on it).
    compact_threshold: usize,
}

impl<V: Vfs> Storage<V> {
    /// Open or recover. Loads the manifest's SSTables, then replays the WAL
    /// tail into the memtable (idempotent, so double-apply after a mid-flush
    /// crash is safe).
    pub fn open(vfs: V) -> io::Result<Self> {
        let manifest = read_manifest(&vfs);
        let mut sstables = Vec::new();
        for name in &manifest.sstables {
            sstables.push(SsTable::open(&vfs, name)?);
        }
        let wal = Wal::open(&vfs)?;
        let mut memtable = BTreeMap::new();
        for e in wal.replay() {
            memtable.insert(e.key, e.value);
        }
        Ok(Storage {
            vfs,
            memtable,
            sstables,
            manifest,
            wal,
            autosync: true,
            txn: None,
            compact_threshold: 0,
        })
    }

    /// Enable bounded-segment auto-compaction (SP49): once `flush` produces
    /// a `k`-th live segment it compacts back to one, keeping point-read
    /// fan-out ≤ `k`. Deterministic (driven only by the op/flush stream),
    /// so replicas stay identical. `0` disables it (the default).
    pub fn set_compact_threshold(&mut self, k: usize) {
        self.compact_threshold = k;
    }

    /// Begin an atomic transaction: subsequent put/delete buffer in an
    /// overlay until `commit_txn` (atomic flush) or `abort_txn` (discard).
    pub fn begin_txn(&mut self) {
        self.txn = Some(BTreeMap::new());
    }

    /// Atomically flush the transaction overlay: append every buffered entry
    /// to the WAL, ONE fsync, then make them visible. Crash-consistent (WAL
    /// replay rebuilds the memtable; a torn tail loses the whole batch).
    pub fn commit_txn(&mut self) -> io::Result<()> {
        let ov = match self.txn.take() {
            Some(o) => o,
            None => return Ok(()),
        };
        for (k, (n, v)) in &ov {
            self.wal.append(&Entry {
                op_number: *n,
                key: k.clone(),
                value: v.clone(),
            })?;
        }
        self.wal.sync()?;
        for (k, (_, v)) in ov {
            self.memtable.insert(k, v);
        }
        Ok(())
    }

    /// Discard the transaction overlay. Nothing reached the WAL/memtable, so
    /// there is literally nothing to undo.
    pub fn abort_txn(&mut self) {
        self.txn = None;
    }

    pub fn in_txn(&self) -> bool {
        self.txn.is_some()
    }

    /// Group-commit control. `false` => `commit` skips the per-op fsync;
    /// durability is reached only on the next `sync()`. This is the single
    /// biggest throughput lever (matches TigerBeetle's batched-fsync design).
    pub fn set_autosync(&mut self, on: bool) {
        self.autosync = on;
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.wal.sync()
    }

    pub fn put(&mut self, op_number: u64, key: Key, value: Vec<u8>) -> io::Result<()> {
        self.commit(Entry {
            op_number,
            key,
            value: Some(value),
        })
    }

    pub fn delete(&mut self, op_number: u64, key: Key) -> io::Result<()> {
        self.commit(Entry {
            op_number,
            key,
            value: None,
        })
    }

    fn commit(&mut self, e: Entry) -> io::Result<()> {
        if let Some(ov) = self.txn.as_mut() {
            ov.insert(e.key, (e.op_number, e.value)); // buffered, not durable
            return Ok(());
        }
        self.wal.append(&e)?;
        if self.autosync {
            self.wal.sync()?;
        }
        self.memtable.insert(e.key, e.value);
        Ok(())
    }

    pub fn get(&self, key: &Key) -> Option<Vec<u8>> {
        if let Some(ov) = &self.txn {
            if let Some((_, v)) = ov.get(key) {
                return v.clone(); // read-your-writes within the txn
            }
        }
        if let Some(v) = self.memtable.get(key) {
            return v.clone();
        }
        for sst in self.sstables.iter().rev() {
            if let Some(v) = sst.get(key) {
                return v.clone();
            }
        }
        None
    }

    /// Persist the memtable as a new SSTable and reset the WAL.
    pub fn flush(&mut self) -> io::Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }
        let name = format!("sst-{:08}", self.manifest.next_sst);
        write_sstable(&self.vfs, &name, &self.memtable)?; // 1. sstable durable
        let mut next = self.manifest.clone();
        next.sstables.push(name.clone());
        next.next_sst += 1;
        write_manifest(&self.vfs, &next)?; // 2. manifest durable
        self.manifest = next;
        self.sstables.push(SsTable::open(&self.vfs, &name)?);
        // 3. reset WAL only after the above are durable
        self.vfs.remove(WAL_NAME)?;
        self.wal = Wal::open(&self.vfs)?;
        self.memtable.clear();
        // SP49: keep read fan-out bounded. Deterministic — same op/flush
        // stream ⇒ same compaction points ⇒ identical state on every
        // replica (compaction preserves live keys, drops shadowed/
        // tombstoned, so the digest is unchanged).
        if self.compact_threshold > 0 && self.sstables.len() >= self.compact_threshold {
            self.compact()?;
        }
        Ok(())
    }

    /// Merge all SSTables into one, dropping shadowed keys and tombstones.
    pub fn compact(&mut self) -> io::Result<()> {
        if self.sstables.len() < 2 {
            return Ok(());
        }
        let mut merged: BTreeMap<Key, Option<Vec<u8>>> = BTreeMap::new();
        for sst in &self.sstables {
            for (k, v) in &sst.entries {
                merged.insert(k.clone(), v.clone()); // later (newer) wins
            }
        }
        merged.retain(|_, v| v.is_some()); // base level: drop tombstones
        let name = format!("sst-{:08}", self.manifest.next_sst);
        write_sstable(&self.vfs, &name, &merged)?;
        let old: Vec<String> = self.manifest.sstables.clone();
        let mut next = Manifest {
            sstables: vec![name.clone()],
            next_sst: self.manifest.next_sst + 1,
        };
        write_manifest(&self.vfs, &next)?;
        for o in old {
            self.vfs.remove(&o).ok();
        }
        next.sstables = vec![name.clone()];
        self.manifest = next;
        self.sstables = vec![SsTable::open(&self.vfs, &name)?];
        Ok(())
    }

    pub fn sstable_count(&self) -> usize {
        self.sstables.len()
    }

    /// Merged live view (oldest SSTable -> newest -> memtable, latest wins,
    /// tombstones dropped). Sorted by key. Used for digests / convergence
    /// checks; O(total) — not a hot path.
    pub fn scan_all(&self) -> Vec<(Key, Vec<u8>)> {
        let mut merged: BTreeMap<Key, Option<Vec<u8>>> = BTreeMap::new();
        for sst in &self.sstables {
            for (k, v) in &sst.entries {
                merged.insert(k.clone(), v.clone());
            }
        }
        for (k, v) in &self.memtable {
            merged.insert(k.clone(), v.clone());
        }
        merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect()
    }

    /// Sorted live entries whose key is in `[lo, hi]` (inclusive). Merges
    /// memtable over SSTables, tombstones dropped. Backs index lookups and
    /// type-range backfill. O(matching + #sstables·log n).
    pub fn scan_range(&self, lo: &Key, hi: &Key) -> Vec<(Key, Vec<u8>)> {
        let mut merged: BTreeMap<Key, Option<Vec<u8>>> = BTreeMap::new();
        for sst in &self.sstables {
            if !sst.overlaps(lo, hi) {
                continue;
            }
            let s = sst.entries.partition_point(|(k, _)| k < lo);
            for (k, v) in &sst.entries[s..] {
                if k > hi {
                    break;
                }
                merged.insert(k.clone(), v.clone());
            }
        }
        for (k, v) in self.memtable.range(lo.clone()..=hi.clone()) {
            merged.insert(k.clone(), v.clone());
        }
        // Overlay-aware (SP25): an in-flight transaction's buffered writes
        // must be visible to range scans too — index lookups, FK reverse
        // lookups and cascade now prefix-scan, so read-your-writes has to
        // hold for scans, not just point gets.
        if let Some(ov) = &self.txn {
            for (k, (_, v)) in ov.range(lo.clone()..=hi.clone()) {
                merged.insert(k.clone(), v.clone());
            }
        }
        merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect()
    }

    /// Keys present in `[lo, hi]` (live, tombstone-aware), nothing else.
    /// Lightweight: avoids the merged-BTreeMap + value clones of
    /// `scan_range`. Fast path when there are no SSTables and no active txn
    /// (the common case for index prefix scans) — a direct memtable walk.
    pub fn scan_prefix(&self, lo: &Key, hi: &Key) -> Vec<Key> {
        if self.sstables.is_empty() && self.txn.is_none() {
            return self
                .memtable
                .range(lo.clone()..=hi.clone())
                .filter(|(_, v)| v.is_some())
                .map(|(k, _)| k.clone())
                .collect();
        }
        let mut present: BTreeMap<Key, bool> = BTreeMap::new();
        for sst in &self.sstables {
            if !sst.overlaps(lo, hi) {
                continue;
            }
            let s = sst.entries.partition_point(|(k, _)| k < lo);
            for (k, v) in &sst.entries[s..] {
                if k > hi {
                    break;
                }
                present.insert(k.clone(), v.is_some());
            }
        }
        for (k, v) in self.memtable.range(lo.clone()..=hi.clone()) {
            present.insert(k.clone(), v.is_some());
        }
        if let Some(ov) = &self.txn {
            for (k, (_, v)) in ov.range(lo.clone()..=hi.clone()) {
                present.insert(k.clone(), v.is_some());
            }
        }
        present
            .into_iter()
            .filter_map(|(k, alive)| alive.then_some(k))
            .collect()
    }

    /// Order-independent CRC digest of the entire live keyspace.
    pub fn digest(&self) -> u32 {
        let mut acc: u32 = 0xFFFF_FFFF;
        for (k, v) in self.scan_all() {
            let mut rec = Vec::with_capacity(28 + v.len());
            rec.extend_from_slice(&k);
            rec.extend_from_slice(&(v.len() as u32).to_le_bytes());
            rec.extend_from_slice(&v);
            // commutative-ish fold (XOR of per-record CRCs) so insertion
            // order of the scan never matters.
            acc ^= crc32c(&rec);
        }
        acc
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_io::MemVfs;
    use kessel_proto::Rng;

    fn k(n: u128) -> Key {
        make_key((n >> 96) as u32, &(n as u128).to_le_bytes())
    }

    #[test]
    fn wal_roundtrip_and_torn_tail() {
        let vfs = MemVfs::new();
        {
            let mut w = Wal::open(&vfs).unwrap();
            for i in 0..10u64 {
                w.append(&Entry {
                    op_number: i,
                    key: k(i as u128),
                    value: Some(vec![i as u8; 4]),
                })
                .unwrap();
            }
            w.sync().unwrap();
            // an unsynced, half-written frame
            let mut d = vfs.open(WAL_NAME).unwrap();
            d.write_at(d.len(), &[9, 9, 9]).unwrap();
        }
        vfs.crash();
        let w = Wal::open(&vfs).unwrap();
        let replayed = w.replay();
        assert_eq!(replayed.len(), 10, "torn tail must be ignored");
        assert_eq!(replayed[7].op_number, 7);
    }

    #[test]
    fn lsm_get_spans_memtable_and_sstables() {
        let vfs = MemVfs::new();
        let mut s = Storage::open(vfs).unwrap();
        s.put(1, k(1), b"v1".to_vec()).unwrap();
        s.put(2, k(2), b"v2".to_vec()).unwrap();
        s.flush().unwrap();
        s.put(3, k(2), b"v2b".to_vec()).unwrap(); // shadow in memtable
        s.put(4, k(3), b"v3".to_vec()).unwrap();
        assert_eq!(s.get(&k(1)), Some(b"v1".to_vec()));
        assert_eq!(s.get(&k(2)), Some(b"v2b".to_vec()), "newer wins");
        assert_eq!(s.get(&k(3)), Some(b"v3".to_vec()));
        assert_eq!(s.get(&k(99)), None);
        s.delete(5, k(1)).unwrap();
        assert_eq!(s.get(&k(1)), None, "tombstone hides older value");
    }

    #[test]
    fn compaction_drops_tombstones_and_shadowed() {
        let vfs = MemVfs::new();
        let mut s = Storage::open(vfs).unwrap();
        s.put(1, k(1), b"a".to_vec()).unwrap();
        s.put(2, k(2), b"b".to_vec()).unwrap();
        s.flush().unwrap();
        s.put(3, k(1), b"a2".to_vec()).unwrap();
        s.delete(4, k(2)).unwrap();
        s.flush().unwrap();
        assert_eq!(s.sstable_count(), 2);
        s.compact().unwrap();
        assert_eq!(s.sstable_count(), 1);
        assert_eq!(s.get(&k(1)), Some(b"a2".to_vec()));
        assert_eq!(s.get(&k(2)), None);
    }

    #[test]
    fn scan_prunes_disjoint_sstables_without_changing_results() {
        let vfs = MemVfs::new();
        let mut s = Storage::open(vfs).unwrap();
        // 40 SSTables, each holding one distinct key — a many-segment LSM.
        let m = 40u128;
        for i in 0..m {
            s.put((i + 1) as u64, k(i * 1000), vec![i as u8]).unwrap();
            s.flush().unwrap();
        }
        assert_eq!(s.sstable_count() as u128, m);

        // A single-key point scan must return exactly that key — the
        // overlap-skip means 39 of 40 SSTables are pruned in O(1) each,
        // but the result is unchanged.
        let target = k(17 * 1000);
        assert_eq!(
            s.scan_prefix(&target, &target),
            vec![target.clone()],
            "point scan over a many-SSTable LSM must be exact"
        );
        assert_eq!(
            s.scan_range(&target, &target),
            vec![(target.clone(), vec![17u8])],
        );
        // A miss is still a clean empty (all SSTables pruned).
        let absent = k(999_999);
        assert!(s.scan_prefix(&absent, &absent).is_empty());

        // Oracle: scanning the full [min,max] key span (Keys order
        // lexicographically over the encoded bytes) must still return every
        // key — pruning skips disjoint tables but drops nothing in range.
        let mut want: Vec<Key> = (0..m).map(|i| k(i * 1000)).collect();
        want.sort();
        let lo = want.first().unwrap().clone();
        let hi = want.last().unwrap().clone();
        let mut got: Vec<Key> = s.scan_prefix(&lo, &hi);
        got.sort();
        assert_eq!(got, want, "wide scan still returns the full set");
    }

    #[test]
    fn bloom_has_no_false_negatives() {
        // The only correctness requirement of a Bloom filter: every key
        // that was inserted must test positive (false positives are fine).
        let keys: Vec<Key> = (0..500u128).map(k).collect();
        let b = Bloom::build(keys.len(), keys.iter());
        for key in &keys {
            assert!(b.maybe_contains(key), "false negative — would lose data");
        }
        // Sanity: a clearly-absent key is *usually* rejected (not asserted
        // per-key since FP is allowed, but the rate must be low).
        let fps = (1_000_000u128..1_000_500)
            .filter(|n| b.maybe_contains(&k(*n)))
            .count();
        assert!(fps < 25, "false-positive rate too high: {fps}/500");
    }

    #[test]
    fn point_get_correct_with_bloom_across_many_sstables() {
        let vfs = MemVfs::new();
        let mut s = Storage::open(vfs).unwrap();
        // 60 SSTables of distinct keys; then a shadow + a tombstone in
        // later tables. The bloom must never hide a real key.
        for i in 0..60u128 {
            s.put((i + 1) as u64, k(i), vec![i as u8]).unwrap();
            s.flush().unwrap();
        }
        s.put(100, k(17), b"shadow".to_vec()).unwrap(); // newer value
        s.flush().unwrap();
        s.delete(101, k(42)).unwrap(); // tombstone
        s.flush().unwrap();

        for i in 0..60u128 {
            let want = match i {
                17 => Some(b"shadow".to_vec()),
                42 => None,
                _ => Some(vec![i as u8]),
            };
            assert_eq!(s.get(&k(i)), want, "key {i} wrong with bloom path");
        }
        // Absent keys: correct (and mostly bloom-rejected, but correctness
        // holds regardless of FP).
        assert_eq!(s.get(&k(9999)), None);
        assert_eq!(s.sstable_count(), 62);
    }

    #[test]
    fn bounded_compaction_caps_segments_and_stays_correct() {
        let vfs = MemVfs::new();
        let mut s = Storage::open(vfs).unwrap();
        s.set_compact_threshold(4);
        let mut op = 0u64;
        // 30 flushes worth of distinct keys — without bounding this would
        // be 30 segments; with threshold 4 it must stay ≤ 4.
        for i in 0..30u128 {
            op += 1;
            s.put(op, k(i), vec![i as u8]).unwrap();
            s.flush().unwrap();
            assert!(
                s.sstable_count() <= 4,
                "segment count {} exceeded the cap after flush {i}",
                s.sstable_count()
            );
        }
        // Shadow + tombstone after the cap is active.
        op += 1;
        s.put(op, k(10), b"new".to_vec()).unwrap();
        s.flush().unwrap();
        op += 1;
        s.delete(op, k(20)).unwrap();
        s.flush().unwrap();
        assert!(s.sstable_count() <= 4);

        for i in 0..30u128 {
            let want = match i {
                10 => Some(b"new".to_vec()),
                20 => None,
                _ => Some(vec![i as u8]),
            };
            assert_eq!(s.get(&k(i)), want, "key {i} wrong after bounded compaction");
        }
        assert_eq!(s.get(&k(9999)), None);
    }

    #[test]
    fn property_vs_btreemap_oracle() {
        let vfs = MemVfs::new();
        let mut s = Storage::open(vfs).unwrap();
        let mut oracle: BTreeMap<Key, Vec<u8>> = BTreeMap::new();
        let mut rng = Rng::new(0xC0FFEE);
        for op in 0..2000u64 {
            let key = k(rng.below(40) as u128);
            if rng.below(4) == 0 {
                s.delete(op, key.clone()).unwrap();
                oracle.remove(&key);
            } else {
                let val = vec![(op & 0xFF) as u8; 1 + rng.below(8) as usize];
                s.put(op, key.clone(), val.clone()).unwrap();
                oracle.insert(key, val);
            }
            if rng.below(50) == 0 {
                s.flush().unwrap();
            }
            if rng.below(120) == 0 {
                s.compact().unwrap();
            }
        }
        for n in 0..40u128 {
            assert_eq!(s.get(&k(n)), oracle.get(&k(n)).cloned(), "key {n}");
        }
    }

    #[test]
    fn scan_range_is_sorted_correct_across_levels() {
        let mut s = Storage::open(MemVfs::new()).unwrap();
        for n in 0..20u128 {
            s.put(n as u64, k(n), vec![n as u8]).unwrap();
            if n == 9 {
                s.flush().unwrap(); // 0..=9 in sstable, 10..=19 in memtable
            }
        }
        s.delete(99, k(5)).unwrap();
        let got = s.scan_range(&k(3), &k(12));
        let keys: Vec<u128> = got
            .iter()
            .map(|(kk, _)| u128::from_le_bytes(kk[4..].try_into().unwrap()))
            .collect();
        assert_eq!(keys, vec![3, 4, 6, 7, 8, 9, 10, 11, 12], "range, sorted, 5 tombstoned");
        assert!(got.windows(2).all(|w| w[0].0 < w[1].0));
    }

    #[test]
    fn digest_is_path_independent() {
        // Same logical state must hash identically whether data sits in the
        // memtable or has been flushed/compacted into SSTables.
        let build = |flush_at: &[u64]| {
            let mut s = Storage::open(MemVfs::new()).unwrap();
            for op in 0..30u64 {
                s.put(op, k(op as u128), vec![op as u8; 3]).unwrap();
                if flush_at.contains(&op) {
                    s.flush().unwrap();
                }
            }
            s.delete(99, k(5)).unwrap();
            s.digest()
        };
        assert_eq!(build(&[]), build(&[3, 10, 20]));
        assert_eq!(build(&[3, 10, 20]), build(&[0, 1, 2, 29]));
    }

    #[test]
    fn txn_is_atomic_commit_and_abort() {
        let vfs = MemVfs::new();
        let mut s = Storage::open(vfs.clone()).unwrap();
        s.put(1, k(1), b"base".to_vec()).unwrap();

        // aborted txn leaves zero trace
        s.begin_txn();
        s.put(2, k(2), b"x".to_vec()).unwrap();
        s.delete(3, k(1)).unwrap();
        assert_eq!(s.get(&k(2)), Some(b"x".to_vec()), "read-your-writes in txn");
        assert_eq!(s.get(&k(1)), None, "tombstone visible in txn");
        s.abort_txn();
        assert_eq!(s.get(&k(1)), Some(b"base".to_vec()), "abort rolled back");
        assert_eq!(s.get(&k(2)), None, "abort discarded insert");

        // committed txn is atomic + durable across reopen
        s.begin_txn();
        s.put(4, k(2), b"y".to_vec()).unwrap();
        s.put(5, k(3), b"z".to_vec()).unwrap();
        s.commit_txn().unwrap();
        s.flush().unwrap();
        let s2 = Storage::open(vfs).unwrap();
        assert_eq!(s2.get(&k(2)), Some(b"y".to_vec()));
        assert_eq!(s2.get(&k(3)), Some(b"z".to_vec()));
        assert_eq!(s2.get(&k(1)), Some(b"base".to_vec()));
    }

    #[test]
    fn recovery_after_crash_keeps_synced_state() {
        let vfs = MemVfs::new();
        {
            let mut s = Storage::open(vfs.clone()).unwrap();
            s.put(1, k(1), b"durable".to_vec()).unwrap();
            s.put(2, k(2), b"also".to_vec()).unwrap();
            s.flush().unwrap();
            s.put(3, k(3), b"post-flush-synced".to_vec()).unwrap();
            // simulate an in-flight unsynced write by bypassing sync
            let mut d = vfs.open(WAL_NAME).unwrap();
            d.write_at(d.len(), &[0xAB; 16]).unwrap(); // never synced
        }
        vfs.crash();
        let s = Storage::open(vfs).unwrap();
        assert_eq!(s.get(&k(1)), Some(b"durable".to_vec()));
        assert_eq!(s.get(&k(2)), Some(b"also".to_vec()));
        assert_eq!(s.get(&k(3)), Some(b"post-flush-synced".to_vec()));
    }
}
