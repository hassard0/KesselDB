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

/// `type_id (4, LE) ++ object_id (16)` — a type's rows form a contiguous range.
pub type Key = [u8; 20];

pub fn make_key(type_id: u32, object_id: &[u8; 16]) -> Key {
    let mut k = [0u8; 20];
    k[..4].copy_from_slice(&type_id.to_le_bytes());
    k[4..].copy_from_slice(object_id);
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
    if p.len() < 8 + 20 + 1 {
        return None;
    }
    let op_number = u64::from_le_bytes(p[0..8].try_into().ok()?);
    let mut key = [0u8; 20];
    key.copy_from_slice(&p[8..28]);
    let value = match p[28] {
        0 => {
            let vl = u32::from_le_bytes(p[29..33].try_into().ok()?) as usize;
            Some(p.get(33..33 + vl)?.to_vec())
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
struct SsTable {
    entries: Vec<(Key, Option<Vec<u8>>)>,
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
            let mut key = [0u8; 20];
            key.copy_from_slice(&buf[p..p + 20]);
            p += 20;
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
        Ok(SsTable { entries })
    }

    fn get(&self, key: &Key) -> Option<&Option<Vec<u8>>> {
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
        })
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
        self.wal.append(&e)?;
        if self.autosync {
            self.wal.sync()?;
        }
        self.memtable.insert(e.key, e.value);
        Ok(())
    }

    pub fn get(&self, key: &Key) -> Option<Vec<u8>> {
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
                merged.insert(*k, v.clone()); // later (newer) wins
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
                merged.insert(*k, v.clone());
            }
        }
        for (k, v) in &self.memtable {
            merged.insert(*k, v.clone());
        }
        merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
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
    fn property_vs_btreemap_oracle() {
        let vfs = MemVfs::new();
        let mut s = Storage::open(vfs).unwrap();
        let mut oracle: BTreeMap<Key, Vec<u8>> = BTreeMap::new();
        let mut rng = Rng::new(0xC0FFEE);
        for op in 0..2000u64 {
            let key = k(rng.below(40) as u128);
            if rng.below(4) == 0 {
                s.delete(op, key).unwrap();
                oracle.remove(&key);
            } else {
                let val = vec![(op & 0xFF) as u8; 1 + rng.below(8) as usize];
                s.put(op, key, val.clone()).unwrap();
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
