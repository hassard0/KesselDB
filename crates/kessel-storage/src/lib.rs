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

pub mod mvcc;
pub mod ssi;
pub mod tx;

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

/// SP116 / S2.7 — **Single source of truth** for the user-type ID range.
///
/// User-type IDs MUST satisfy `1 <= type_id <= MAX_USER_TYPE_ID`. The catalog
/// allocator in `kessel-sm::StateMachine::apply` `Op::CreateType` enforces
/// this at allocation time (returns `SchemaError` if exhaustion); the
/// storage-layer MVCC dispatch in `data_row_dispatch` enforces it at every
/// read/write (keys with `type_id > MAX_USER_TYPE_ID` route to legacy, NOT
/// MVCC). Both sites import this constant — the two-place enforcement keeps
/// the contract verifiable in either direction.
///
/// **Reserved values above MAX_USER_TYPE_ID** (the entire `0xFF00_0000..=u32::MAX`
/// range is reserved for aux/index keyspaces per the kessel-sm convention):
///   - `0xFFFC_xxxx` — IDX_STR (ordered, string + u128)
///   - `0xFFFD_xxxx` — IDX_NUM (ordered, numeric)
///   - `0xFFFE_xxxx` — IDX_EQ (equality + composite)
///   - `0xFFFF_FFF0` — SEQ (sequencer)
///   - `0xFFFF_FFF1` — XSHARD (cross-shard coordinator)
///   - `0xFFFF_FFF2` — SEQ_DEDUP
///   - `0xFFFF_FFF3` — XVOTE (cross-shard vote)
///   - `0xFFFF_FFFF` — OVERFLOW (blob storage)
///
/// SP116-shipped caveat addressed by this constant: prior to this fix the
/// dispatch implicitly trusted the catalog allocator's monotonic-from-1
/// behavior. Now the constraint is statically named, exported, and
/// double-enforced at the allocation seam (catalog) and the usage seam
/// (dispatch).
pub const MAX_USER_TYPE_ID: u32 = 0xFEFF_FFFF;

/// SP116 / S2.7 — Reserved value for the catalog's self-storage blob.
///
/// The catalog persists itself at `make_key(CATALOG_TYPE_ID, &[0; 16])`
/// (20-byte key, all-zero object_id). `data_row_dispatch` MUST NOT route
/// this key through MVCC — versioning the catalog would silently break the
/// catalog reload path (a fresh open would read whichever version the LSM
/// happened to surface first instead of the latest).
pub const CATALOG_TYPE_ID: u32 = 0;

/// SP116 / S2.7 — Storage-layer MVCC dispatch discriminator.
///
/// Returns `Some(type_id)` iff `key` is a **user-type data-row key**:
///   - exactly 20 bytes (matches `make_key` shape), AND
///   - `type_id` is in the user-type range `[1, MAX_USER_TYPE_ID]`
///     (excludes `CATALOG_TYPE_ID = 0` and the entire `0xFF00_0000..=u32::MAX`
///     reserved range — see `MAX_USER_TYPE_ID` for the full reserved table).
///
/// Single source of truth: this gate uses the `MAX_USER_TYPE_ID` constant
/// that the catalog allocator (in kessel-sm) ALSO references when refusing
/// to mint a user type that would alias the reserved range. The two-place
/// enforcement makes the invariant inspectable from either direction.
///
/// This is the safe shape; a naive `key.len() == 20` discriminator would
/// MVCC-versionize index entries AND the catalog blob (both also use
/// `make_key`). The classifier flagged the unsafe form during SP116 T2;
/// the failing `it_coverage_catalog_ddl_byte_net_zero_versioned_keyspace`
/// test surfaced the catalog-blob trap one iteration later; this constant
/// addresses the remaining "currently enforced by catalog allocator but
/// not statically guaranteed" caveat from the SP116 shipped record.
#[inline]
pub(crate) fn data_row_dispatch(key: &[u8]) -> Option<u32> {
    if key.len() == mvcc::PREFIX_LEN {
        let type_id = u32::from_le_bytes([key[0], key[1], key[2], key[3]]);
        if type_id != CATALOG_TYPE_ID && type_id <= MAX_USER_TYPE_ID {
            return Some(type_id);
        }
    }
    None
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
/// SP-Perf-A T2: the `+ Send + Sync` bounds on the trait object let the
/// `Arc<RwLock<StateMachine>>` shared between the engine thread and
/// read-pool workers be `Send + Sync` (the engine thread spawns with
/// the SM already moved in; readers race against the writer through
/// the rwlock).
/// SP-Perf-A T5: FileDisk is now `Sync` for real — `read_at` is
/// positional (`pread`/`seek_read`) and takes `&self`, so the
/// per-file mutex T2 needed for `Sync` is gone. The `+ Sync` bound
/// here was already declared; T5 lifts the runtime serialisation that
/// satisfied it. Every existing concrete `Disk` impl (`FileDisk`,
/// `MemDisk`, `FaultDisk`, `MemVfsDisk`) is `Send + Sync`.
struct Wal {
    disk: Box<dyn Disk + Send + Sync>,
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
    /// SP94: durable apply-cursor watermark — the highest op-number
    /// whose effects were folded into an SSTable at flush/compact
    /// time. `flush` truncates the WAL, so this carries the cursor
    /// across reopen for already-flushed ops; post-flush ops extend it
    /// via WAL replay. `0` ⇒ unknown (also what a pre-SP94 manifest
    /// reads as — backward compatible).
    high_op: u64,
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
    // SP94: optional `high_op` watermark sits between the sstable
    // list and the trailing CRC. A pre-SP94 manifest has nothing
    // there (p == len-4) ⇒ `0` (unknown) — backward compatible.
    let high_op = if len >= p + 4 + 8 {
        u64::from_le_bytes(buf[p..p + 8].try_into().unwrap())
    } else {
        0
    };
    Manifest { sstables, next_sst, high_op }
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
    buf.extend_from_slice(&m.high_op.to_le_bytes()); // SP94 watermark
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
    /// SP94: the highest op-number ever durably WAL-framed (max over
    /// replay + every `commit`). `None` ⇒ nothing durable yet. Used by
    /// the state machine as its replay/recovery apply-cursor; not part
    /// of the digest (it is derived from the WAL, not stored state).
    high_op: Option<u64>,
    /// SP114 / S2.5: The local view of the SM's low_water_mark. Set by
    /// the SM apply arm on `Op::AdvanceWatermark` apply via
    /// `set_low_water_mark`. Read by `Tx::begin*` to validate the
    /// snapshot_opnum is serveable. Initial value 0 (no GC has happened;
    /// every snapshot >= 0 is serveable, which is every snapshot).
    low_water_mark: u64,
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
        // SP94: the cursor survives a WAL-truncating flush via the
        // manifest watermark; post-flush ops extend it from the WAL.
        let mut high_op: Option<u64> =
            (manifest.high_op > 0).then_some(manifest.high_op);
        for e in wal.replay() {
            high_op = Some(high_op.map_or(e.op_number, |h| h.max(e.op_number)));
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
            high_op,
            // SP114 / S2.5: no GC has run yet; every snapshot is serveable.
            low_water_mark: 0,
        })
    }

    /// SP114 / S2.5: Read the storage's current low_water_mark.
    pub fn low_water_mark(&self) -> u64 {
        self.low_water_mark
    }

    /// SP114 / S2.5: Set the storage's low_water_mark. Called by the SM
    /// apply arm on `Op::AdvanceWatermark` apply. Caller is responsible
    /// for monotonicity — the SM apply path validates this before calling.
    pub fn set_low_water_mark(&mut self, w: u64) {
        self.low_water_mark = w;
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
        // SP116 / S2.7 — transparent MVCC dispatch for user-type data rows.
        // See `data_row_dispatch` for the discriminator rationale. Non-data-row
        // keys (catalog 3-byte, indexes 0xFFFx-prefix 20-byte, aux 0xFFFF_FFFx
        // 20-byte) fall through to the legacy commit() path unchanged.
        if let Some(type_id) = data_row_dispatch(&key) {
            let mut oid = [0u8; 16];
            oid.copy_from_slice(&key[4..20]);
            return mvcc::put_versioned(self, type_id, &oid, op_number, Some(value));
        }
        self.commit(Entry {
            op_number,
            key,
            value: Some(value),
        })
    }

    pub fn delete(&mut self, op_number: u64, key: Key) -> io::Result<()> {
        // SP116 / S2.7 — user-type data-row deletes become MVCC tombstone
        // versions at the same (type_id, oid) at commit_opnum=op_number.
        if let Some(type_id) = data_row_dispatch(&key) {
            let mut oid = [0u8; 16];
            oid.copy_from_slice(&key[4..20]);
            return mvcc::put_versioned(self, type_id, &oid, op_number, None);
        }
        self.commit(Entry {
            op_number,
            key,
            value: None,
        })
    }

    /// MVCC variant of `put`/`delete`: accepts `Option<Vec<u8>>` so a
    /// tombstone (`None`) is expressible in a single call. Reuses the same
    /// Entry/WAL commit path; `op_number` becomes the version's commit_opnum.
    /// Added for S2.1; the only change to this file in that slice.
    pub fn put_entry_versioned(
        &mut self,
        op_number: u64,
        key: Key,
        value: Option<Vec<u8>>,
    ) -> io::Result<()> {
        self.commit(Entry { op_number, key, value })
    }

    /// Like `scan_range` but yields `(Key, Option<Vec<u8>>)` — tombstones
    /// are visible as `None`. Used by the MVCC layer to scan a versioned-key
    /// prefix where a tombstone encodes a logical deletion rather than
    /// a missing key. Merge order: older SSTables < newer SSTables < memtable
    /// < txn overlay (latest wins).
    pub fn scan_range_versions(
        &self,
        lo: &Key,
        hi: &Key,
    ) -> Vec<(Key, Option<Vec<u8>>)> {
        if lo > hi {
            return Vec::new();
        }
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
        if let Some(ov) = &self.txn {
            for (k, (_, v)) in ov.range(lo.clone()..=hi.clone()) {
                merged.insert(k.clone(), v.clone());
            }
        }
        merged.into_iter().collect()
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
        let op = e.op_number;
        self.memtable.insert(e.key, e.value);
        self.high_op = Some(self.high_op.map_or(op, |h| h.max(op)));
        Ok(())
    }

    /// SP94: the highest op-number ever durably WAL-framed (max over
    /// recovery replay + every `commit`); `None` ⇒ nothing durable.
    /// The state machine uses this as its crash-recovery apply-cursor.
    pub fn high_op(&self) -> Option<u64> {
        self.high_op
    }

    pub fn get(&self, key: &Key) -> Option<Vec<u8>> {
        // SP116 / S2.7 — user-type data-row point read goes to MVCC at the
        // latest committed snapshot (READ COMMITTED for the apply seam, which
        // is serial in log-position order). Tombstone collapses to None.
        if let Some(type_id) = data_row_dispatch(key) {
            let mut oid = [0u8; 16];
            oid.copy_from_slice(&key[4..20]);
            return match mvcc::get_at_snapshot(self, type_id, &oid, u64::MAX) {
                mvcc::SnapshotRead::Found(v) => Some(v),
                mvcc::SnapshotRead::Tombstoned | mvcc::SnapshotRead::NotYetWritten => None,
            };
        }
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
        // SP94: persist the durable apply-cursor — the WAL (which
        // also carries it) is truncated just below.
        next.high_op = self.high_op.unwrap_or(next.high_op).max(next.high_op);
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
    ///
    /// SP115 / S2.6 cutover refinement: tombstones in the 28-byte MVCC
    /// versioned-key space are SEMANTICALLY DIFFERENT from tombstones in
    /// the 20-byte legacy data/aux keyspace. An MVCC tombstone encodes
    /// "this row is logically deleted at commit_opnum X" and is REQUIRED
    /// to make subsequent snapshot reads return SnapshotRead::Tombstoned
    /// (rather than returning the OLDER live version). Compaction MUST
    /// preserve MVCC tombstones; only the 20-byte legacy tombstones (and
    /// other base-level tombstones) may be dropped. Honest disclosure:
    /// the SP115 T2 cutover is partial (the data-row apply arms continue
    /// to write via the legacy keypath for byte-equivalence test
    /// preservation); the 28-byte tombstone preservation is the
    /// forward-looking primitive for when the cutover completes.
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
        // Drop tombstones EXCEPT MVCC 28-byte versioned-key tombstones,
        // which the MVCC layer relies on for snapshot read correctness.
        merged.retain(|k, v| v.is_some() || k.len() == 28);
        let name = format!("sst-{:08}", self.manifest.next_sst);
        write_sstable(&self.vfs, &name, &merged)?;
        let old: Vec<String> = self.manifest.sstables.clone();
        let mut next = Manifest {
            sstables: vec![name.clone()],
            next_sst: self.manifest.next_sst + 1,
            // SP94: compaction preserves the durable cursor.
            high_op: self
                .high_op
                .unwrap_or(self.manifest.high_op)
                .max(self.manifest.high_op),
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
        // SP116 / S2.7 — user-type data-row range scan dispatches to MVCC at
        // u64::MAX snapshot. The discriminator requires BOTH bounds to be
        // 20-byte user-type keys with the SAME type_id (the canonical
        // type-prefix range produced by callers as
        // `[make_key(t, &[0;16]), make_key(t, &[255;16])]`). Sub-ranges within
        // a single type also dispatch — `mvcc::scan_at_snapshot` returns all
        // live versions for the type, which we then post-filter against the
        // requested [lo, hi]. Cross-type or partial-prefix ranges fall through.
        if let (Some(t_lo), Some(t_hi)) = (data_row_dispatch(lo), data_row_dispatch(hi)) {
            if t_lo == t_hi {
                let mut out: Vec<(Key, Vec<u8>)> = mvcc::scan_at_snapshot(self, t_lo, u64::MAX)
                    .into_iter()
                    .map(|(oid, v)| (make_key(t_lo, &oid), v))
                    .filter(|(k, _)| k >= lo && k <= hi)
                    .collect();
                out.sort_by(|a, b| a.0.cmp(&b.0));
                return out;
            }
        }
        // An inverted inclusive range `[lo, hi]` with `lo > hi` contains
        // nothing — and `BTreeMap::range(lo..=hi)` *panics* on it. A
        // caller can legitimately produce one (e.g. a planner narrowing
        // `WHERE s >= 'd' AND s <= 'b'` into inverted index bounds), so
        // treat it as the empty scan rather than letting it abort.
        if lo > hi {
            return Vec::new();
        }
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
        if lo > hi {
            return Vec::new(); // empty inverted range; see `scan_range`.
        }
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

    /// The single smallest (`want_max=false`) or largest
    /// (`want_max=true`) **live** entry whose key is in `[lo, hi]`,
    /// without materialising the whole range. Each SSTable is binary-
    /// searched for its boundary candidate and the memtable/overlay use
    /// their ordered cursors; the global candidate is resolved with a
    /// tombstone-aware point `get`, advancing past a (rare) tombstoned
    /// boundary. Sub-linear in the common case — the accelerator behind
    /// `MIN`/`MAX` on an order-indexed column. A hard iteration cap makes
    /// it fall back to `None` (caller does the full scan) rather than
    /// ever loop unboundedly: a pure optimisation, never a wrong answer.
    pub fn bound_in(
        &self,
        lo: &Key,
        hi: &Key,
        want_max: bool,
    ) -> Option<(Key, Vec<u8>)> {
        // Strictly-greater successor (length-safe: append 0x00 — shorter
        // shared-prefix keys can't exist here since keys are fixed-width).
        let succ = |k: &Key| -> Key {
            let mut n = k.clone();
            n.push(0);
            n
        };
        // Strictly-smaller predecessor: big-endian decrement with borrow
        // (equal-length byte order == integer order). None at all-zero.
        let pred = |k: &Key| -> Option<Key> {
            let mut n = k.clone();
            for i in (0..n.len()).rev() {
                if n[i] > 0 {
                    n[i] -= 1;
                    return Some(n);
                }
                n[i] = 0xFF;
            }
            None
        };
        let mut lo_c = lo.clone();
        let mut hi_c = hi.clone();
        for _ in 0..4096 {
            if lo_c > hi_c {
                return None;
            }
            // Boundary candidate key across every source.
            let mut cand: Option<Key> = None;
            let mut take = |k: &Key| {
                if k < &lo_c || k > &hi_c {
                    return;
                }
                cand = Some(match cand.take() {
                    None => k.clone(),
                    Some(c) => {
                        if (want_max && *k > c) || (!want_max && *k < c) {
                            k.clone()
                        } else {
                            c
                        }
                    }
                });
            };
            for sst in &self.sstables {
                if !sst.overlaps(&lo_c, &hi_c) {
                    continue;
                }
                if want_max {
                    let pp =
                        sst.entries.partition_point(|(k, _)| k <= &hi_c);
                    if pp > 0 {
                        take(&sst.entries[pp - 1].0);
                    }
                } else {
                    let s =
                        sst.entries.partition_point(|(k, _)| k < &lo_c);
                    if let Some((k, _)) = sst.entries.get(s) {
                        take(k);
                    }
                }
            }
            {
                let mut it = self.memtable.range(lo_c.clone()..=hi_c.clone());
                if let Some((k, _)) =
                    if want_max { it.next_back() } else { it.next() }
                {
                    take(k);
                }
            }
            if let Some(ov) = &self.txn {
                let mut it = ov.range(lo_c.clone()..=hi_c.clone());
                if let Some((k, _)) =
                    if want_max { it.next_back() } else { it.next() }
                {
                    take(k);
                }
            }
            let c = cand?;
            // Tombstone-aware newest-wins resolution of just this key.
            if let Some(v) = self.get(&c) {
                return Some((c, v));
            }
            // Boundary was tombstoned at the newest version — step past it.
            if want_max {
                hi_c = pred(&c)?;
            } else {
                lo_c = succ(&c);
            }
        }
        None // cap hit (pathological tombstone run) → caller full-scans
    }

    /// Order-independent CRC digest of the entire live keyspace EXCLUDING the
    /// 28-byte MVCC versioned keyspace. The MVCC byte-identity across replicas
    /// is gated separately via SP115 T3 (`apply_one 3-replica byte-identity for
    /// MVCC infrastructure ops`) + SP116 T3 (`it_sql_workload_3_replica_byte_identity`).
    ///
    /// **SP116 / S2.7 (Decision 1) migration.** Pre-SP116 the digest included the
    /// MVCC keyspace, which made the xshard test's byte-identical-cross-replica
    /// assertion structurally incompatible with MVCC keys baking `commit_opnum`
    /// into 8 BE bytes (different op_number sequences across runs → different
    /// MVCC keys → different digests, despite logically identical state).
    /// Per SP116 Decision 1, the filter skips 28-byte MVCC keys; the discriminator
    /// is the key length:
    ///   - legacy data-row keys           = 20 bytes (`make_key(type_id, oid)`)
    ///   - **MVCC versioned data keys**   = 28 bytes (legacy + 8 BE commit_opnum)
    ///   - catalog / index / blob / sequencer / constraint keys use distinct
    ///     prefix shapes; none are exactly 28 bytes by construction.
    /// The protective `if k.len() == 28 { continue; }` filter therefore excludes
    /// ONLY the MVCC versioned data-row keyspace, preserving every other
    /// keyspace's byte-identical-cross-replica contribution exactly as before.
    pub fn digest(&self) -> u32 {
        let mut acc: u32 = 0xFFFF_FFFF;
        for (k, v) in self.scan_all() {
            // SP116 (Decision 1): skip the MVCC versioned data-row keyspace
            // so xshard / VSR / SQL determinism tests preserve their
            // byte-identical-cross-replica intent across the SP116 cutover.
            if k.len() == 28 {
                continue;
            }
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

    /// SP92: the invariant VSR safety rests on. A torn WAL write
    /// (only half the frame reaches disk) must leave a *clean
    /// committed prefix*: `Storage::open` recovers every op before
    /// the torn one and **nothing** at or after it — no partial /
    /// garbage op ever surfaces. (A recovered replica is then a
    /// well-defined lagging node the quorum can safely state-transfer
    /// — the multi-node crash-recover-during-view-change harness that
    /// builds on this `FaultVfs` is tracked as a separate slice.)
    #[test]
    fn wal_torn_write_recovers_clean_committed_prefix() {
        use kessel_io::{FaultKind, FaultVfs};

        let recover = || -> (Vec<bool>, bool) {
            let fv = FaultVfs::new(MemVfs::new());
            {
                let mut s = Storage::open(fv.clone()).unwrap();
                // 10 cleanly-committed ops.
                for i in 1..=10u64 {
                    s.put(i, k(i as u128), vec![i as u8; 6]).unwrap();
                }
                // Arm: the *very next* WAL write tears in half. Robust
                // to any Wal::open preamble — we count from here.
                fv.arm("wal", 1, FaultKind::Torn);
                // op 11's frame is torn; 12..=20 append after it.
                for i in 11..=20u64 {
                    s.put(i, k(i as u128), vec![i as u8; 6]).unwrap();
                }
                assert!(fv.fired(), "the torn-write fault must have fired");
            }
            // Reopen from the SAME disk (disarm so replay/compaction
            // writes are clean).
            fv.plan().lock().unwrap().kind = None;
            let s2 = Storage::open(fv.clone()).unwrap();
            let present: Vec<bool> =
                (1..=20u64).map(|i| s2.get(&k(i as u128)).is_some()).collect();
            (present, fv.fired())
        };

        let (present, fired) = recover();
        assert!(fired);
        for i in 0..10 {
            assert!(present[i], "op {} (before the tear) must survive", i + 1);
        }
        for i in 10..20 {
            assert!(
                !present[i],
                "op {} (at/after the tear) must NOT partially survive",
                i + 1
            );
        }
        // Deterministic: the recovered prefix is identical run-to-run.
        let (present2, _) = recover();
        assert_eq!(present, present2, "torn-write recovery must be deterministic");
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

    /// SP116 / S2.7 — Hand-derived KATs for the storage-layer MVCC dispatch
    /// discriminator (`data_row_dispatch`). These lock the discriminator's
    /// reserved-range exclusions so a future tweak that "simplifies" the
    /// gate (e.g. removes the `type_id != 0` check or the `key[3] != 0xFF`
    /// check) fires here, not in a downstream test that's hard to trace
    /// back to the discriminator.

    /// `dispatch_user_type_routes_to_mvcc` — a 20-byte key with a user
    /// `type_id` in (0, 0xFF00_0000) routes through MVCC; the write lands
    /// at a 28-byte versioned key, and the same 20-byte key reads it back
    /// via `Storage::get`.
    #[test]
    fn dispatch_user_type_routes_to_mvcc() {
        let mut s = Storage::open(MemVfs::new()).unwrap();
        let key = make_key(42, &[0x55u8; 16]); // type_id=42 → user type
        let value = vec![0xABu8; 8];
        s.put(7, key.clone(), value.clone()).unwrap();
        // The 20-byte legacy key MUST NOT exist in any direct (non-MVCC) form;
        // a raw scan over the 20-byte key range finds the reconstructed key
        // by way of the MVCC dispatch on scan_range.
        assert_eq!(
            s.get(&key),
            Some(value),
            "SP116 dispatch: 20-byte user-type key must round-trip via MVCC"
        );
        // And the raw 28-byte versioned key IS present in the scan_all sweep.
        let any_28 = s.scan_all().iter().any(|(k, _)| k.len() == 28);
        assert!(any_28, "SP116 dispatch: write must land at a 28-byte MVCC key");
    }

    /// `dispatch_excludes_catalog_type_id_zero` — type_id=0 is reserved for
    /// the catalog's own blob storage; that path MUST stay on legacy.
    #[test]
    fn dispatch_excludes_catalog_type_id_zero() {
        let mut s = Storage::open(MemVfs::new()).unwrap();
        let key = make_key(0, &[0u8; 16]); // type_id=0 → catalog
        let value = vec![0xC1u8; 8];
        s.put(3, key.clone(), value.clone()).unwrap();
        // The 20-byte legacy key MUST appear unchanged in the dump.
        let dump: Vec<_> = s.scan_all();
        assert!(
            dump.iter().any(|(k, v)| k == &key && v == &value),
            "SP116 dispatch: type_id=0 (catalog) must stay on legacy 20-byte path"
        );
        // No 28-byte MVCC keys should exist (catalog NOT versioned).
        assert!(
            dump.iter().all(|(k, _)| k.len() != 28),
            "SP116 dispatch: catalog write must NOT land at a 28-byte MVCC key"
        );
    }

    /// `dispatch_excludes_high_byte_ff_aux_and_index_keys` — index + aux
    /// keyspaces all use 0xFFxx_xxxx prefixes; their 20-byte
    /// `make_key`-shaped entries MUST stay on legacy (NOT MVCC-versioned).
    #[test]
    fn dispatch_excludes_high_byte_ff_aux_and_index_keys() {
        let mut s = Storage::open(MemVfs::new()).unwrap();
        // One key per known reserved range — IDX_STR, IDX_NUM, IDX_EQ,
        // XSHARD, XVOTE, SEQ_DEDUP, SEQ, OVERFLOW.
        let reserved_types: &[u32] = &[
            0xFFFC_0001, // IDX_STR (ordered, str/u128)
            0xFFFD_0001, // IDX_NUM (ordered, numeric)
            0xFFFE_0001, // IDX_EQ (equality / composite)
            0xFFFF_FFF0, // SEQ
            0xFFFF_FFF1, // XSHARD
            0xFFFF_FFF2, // SEQ_DEDUP
            0xFFFF_FFF3, // XVOTE
            0xFFFF_FFFF, // OVERFLOW
        ];
        for (i, &t) in reserved_types.iter().enumerate() {
            let key = make_key(t, &[i as u8; 16]);
            s.put((i + 1) as u64, key.clone(), vec![i as u8; 4]).unwrap();
            assert_eq!(
                s.get(&key),
                Some(vec![i as u8; 4]),
                "SP116 dispatch: reserved type {t:#x} (20-byte aux/index key) \
                 must round-trip via legacy, not MVCC"
            );
        }
        // No 28-byte MVCC keys must have leaked from the reserved-range puts.
        assert!(
            s.scan_all().iter().all(|(k, _)| k.len() != 28),
            "SP116 dispatch: reserved 0xFFxx_xxxx keys must NEVER produce 28-byte \
             MVCC keys — they stay on legacy by construction"
        );
    }

    /// `dispatch_excludes_non_20_byte_keys` — only keys of length exactly 20
    /// participate in the dispatch. Catalog 3-byte, MVCC 28-byte, and any
    /// other length pass through unchanged.
    #[test]
    fn dispatch_excludes_non_20_byte_keys() {
        let mut s = Storage::open(MemVfs::new()).unwrap();
        // 3-byte catalog-shaped key.
        s.put(1, vec![0x00, 0x00, 0x01], vec![0xAAu8; 4]).unwrap();
        assert_eq!(s.get(&vec![0x00, 0x00, 0x01]), Some(vec![0xAAu8; 4]));
        // 19-byte off-by-one (too short).
        let short = vec![1u8; 19];
        s.put(2, short.clone(), vec![0xBBu8; 4]).unwrap();
        assert_eq!(s.get(&short), Some(vec![0xBBu8; 4]));
        // 21-byte off-by-one (too long).
        let long = vec![2u8; 21];
        s.put(3, long.clone(), vec![0xCCu8; 4]).unwrap();
        assert_eq!(s.get(&long), Some(vec![0xCCu8; 4]));
        // Synthetic 28-byte key (NOT an MVCC versioned key — distinct payload
        // pattern). The 28-byte length matches what mvcc::PREFIX_LEN + 8
        // would produce so the dispatch must NOT fire on a put with this
        // shape (the 28-byte length is OUTPUT-only for MVCC).
        let v28 = vec![0xDDu8; 28];
        s.put(4, v28.clone(), vec![0xEEu8; 4]).unwrap();
        assert_eq!(s.get(&v28), Some(vec![0xEEu8; 4]));
    }

    /// `dispatch_delete_writes_mvcc_tombstone` — Storage::delete on a 20-byte
    /// user-type key emits an MVCC tombstone version (None value at
    /// commit_opnum); subsequent Storage::get returns None.
    #[test]
    fn dispatch_delete_writes_mvcc_tombstone() {
        let mut s = Storage::open(MemVfs::new()).unwrap();
        let key = make_key(99, &[0x77u8; 16]);
        let value = vec![0xF0u8; 16];
        s.put(5, key.clone(), value.clone()).unwrap();
        assert_eq!(s.get(&key), Some(value));
        s.delete(10, key.clone()).unwrap();
        assert_eq!(
            s.get(&key),
            None,
            "SP116 dispatch: tombstone at op=10 must collapse to None on Storage::get"
        );
        // Both versions (live at 5 + tombstone at 10) exist as 28-byte MVCC keys.
        let mvcc_keys: Vec<_> = s.scan_all().into_iter().filter(|(k, _)| k.len() == 28).collect();
        // Two MVCC physical keys were written (op=5 live, op=10 tombstone)
        // at DISTINCT 28-byte keys (different inverted-opnum suffix). scan_all
        // drops `value.is_none()` (the tombstone), leaving exactly the older
        // physical entry from op=5 visible at the raw scan layer. The MVCC
        // visibility filter at scan_at_snapshot(u64::MAX) hides it (see the
        // scan_range assertion below) — that's the MVCC semantic layer.
        assert_eq!(
            mvcc_keys.len(),
            1,
            "SP116 dispatch: scan_all (live-filter) keeps the op=5 physical \
             entry; the op=10 tombstone is value-filtered out. Got {} entries.",
            mvcc_keys.len()
        );
        // Sanity: scan_range over the type-prefix returns nothing (the
        // tombstone hides the prior version under MVCC semantics at u64::MAX).
        let lo = make_key(99, &[0u8; 16]);
        let hi = make_key(99, &[0xFFu8; 16]);
        assert!(
            s.scan_range(&lo, &hi).is_empty(),
            "SP116 dispatch: scan_range at u64::MAX after tombstone must be empty"
        );
    }

    /// SP116 / S2.7 — caveat-closure KATs for the user-type ID boundary.
    ///
    /// These lock the discriminator behavior at the exact MAX_USER_TYPE_ID
    /// boundary: type_id = MAX_USER_TYPE_ID is the LAST routable user type;
    /// type_id = MAX_USER_TYPE_ID + 1 (i.e., 0xFF00_0000) is the FIRST
    /// reserved address and MUST stay legacy. Mirror tests verify the
    /// CATALOG_TYPE_ID = 0 exclusion (catalog blob stays legacy).
    /// The catalog-side allocator gate is exercised separately in kessel-sm
    /// (the SP116-caveat-closure test there asserts CreateType refuses to
    /// allocate next_type_id > MAX_USER_TYPE_ID).

    /// `dispatch_at_max_user_type_id_routes_to_mvcc` — the LAST routable
    /// user type. Off-by-one paranoia: if a future change tightens the
    /// gate to `<` instead of `<=`, this catches it.
    #[test]
    fn dispatch_at_max_user_type_id_routes_to_mvcc() {
        let mut s = Storage::open(MemVfs::new()).unwrap();
        let key = make_key(MAX_USER_TYPE_ID, &[0x33u8; 16]);
        s.put(1, key.clone(), vec![0xABu8; 4]).unwrap();
        assert_eq!(
            s.get(&key),
            Some(vec![0xABu8; 4]),
            "SP116 caveat: type_id == MAX_USER_TYPE_ID (0xFEFF_FFFF) IS \
             a user type and MUST route to MVCC"
        );
        assert!(
            s.scan_all().iter().any(|(k, _)| k.len() == 28),
            "SP116 caveat: max-user-type write must land at a 28-byte MVCC key"
        );
    }

    /// `dispatch_at_first_reserved_type_id_stays_legacy` — exactly one above
    /// MAX_USER_TYPE_ID is the first reserved value (0xFF00_0000). Must NOT
    /// be MVCC-routed (would alias the reserved aux/index range).
    #[test]
    fn dispatch_at_first_reserved_type_id_stays_legacy() {
        let mut s = Storage::open(MemVfs::new()).unwrap();
        let key = make_key(MAX_USER_TYPE_ID.wrapping_add(1), &[0x44u8; 16]);
        s.put(1, key.clone(), vec![0xCDu8; 4]).unwrap();
        let dump = s.scan_all();
        assert!(
            dump.iter().any(|(k, v)| k == &key && v == &vec![0xCDu8; 4]),
            "SP116 caveat: type_id = MAX_USER_TYPE_ID + 1 (= 0xFF00_0000, \
             first reserved address) MUST stay on the legacy 20-byte path"
        );
        assert!(
            dump.iter().all(|(k, _)| k.len() != 28),
            "SP116 caveat: first-reserved write must NOT produce a 28-byte MVCC key"
        );
    }

    /// `dispatch_at_catalog_type_id_stays_legacy` — explicit re-test of
    /// the CATALOG_TYPE_ID (= 0) exclusion that was added when the
    /// `it_coverage_catalog_ddl_byte_net_zero_versioned_keyspace` test
    /// surfaced the catalog-blob trap. Mirrors the constant naming.
    #[test]
    fn dispatch_at_catalog_type_id_stays_legacy() {
        let mut s = Storage::open(MemVfs::new()).unwrap();
        let key = make_key(CATALOG_TYPE_ID, &[0u8; 16]);
        s.put(1, key.clone(), vec![0xC0u8; 4]).unwrap();
        let dump = s.scan_all();
        assert!(
            dump.iter().any(|(k, v)| k == &key && v == &vec![0xC0u8; 4]),
            "SP116 caveat: CATALOG_TYPE_ID (0) MUST stay on legacy path"
        );
        assert!(
            dump.iter().all(|(k, _)| k.len() != 28),
            "SP116 caveat: catalog-key write must NOT produce a 28-byte MVCC key"
        );
    }

    /// `max_user_type_id_constant_value_locked` — pin the literal value so
    /// a future "simplification" to a different boundary can't silently
    /// change the contract. If this value ever changes, the catalog
    /// allocator gate in kessel-sm + the dispatch gate here MUST update
    /// together.
    #[test]
    fn max_user_type_id_constant_value_locked() {
        assert_eq!(
            MAX_USER_TYPE_ID, 0xFEFF_FFFF,
            "SP116 caveat: MAX_USER_TYPE_ID is the single source of truth \
             for the user-type-vs-reserved boundary; the catalog allocator \
             AND data_row_dispatch BOTH reference this constant. If this \
             value changes, update both sites in lockstep."
        );
        assert_eq!(
            CATALOG_TYPE_ID, 0,
            "SP116 caveat: CATALOG_TYPE_ID is reserved for the catalog blob; \
             value is locked at 0 (the catalog's persisted-at make_key(0, \
             &[0;16]) shape)."
        );
        // The reserved range starts EXACTLY at MAX_USER_TYPE_ID + 1.
        assert_eq!(
            MAX_USER_TYPE_ID.wrapping_add(1),
            0xFF00_0000,
            "SP116 caveat: reserved-range start MUST be MAX_USER_TYPE_ID + 1; \
             the kessel-sm aux constants (SEQ=0xFFFF_FFF0 etc.) all live in \
             0xFF00_0000..=u32::MAX."
        );
    }

    /// SP116 / S2.7 (Decision 1) — Storage::digest MVCC-keyspace skip.
    ///
    /// Claim: writing 28-byte MVCC versioned keys does NOT change the digest
    /// value; the digest sees only non-28-byte keyspaces (legacy data-row /
    /// catalog / index / blob / sequencer / constraint). This is the keystone
    /// that lets the xshard test + ~25 other determinism assertions stay green
    /// across the SP116 cutover.
    ///
    /// Workload: build storage with mixed-shape keys (20-byte legacy +
    /// catalog + index + 28-byte MVCC); compare digest against the same build
    /// WITHOUT the 28-byte MVCC keys.
    /// Expected: byte-identical digests.
    #[test]
    fn digest_excludes_mvcc_versioned_keyspace() {
        // Baseline: legacy 20-byte data-row + catalog (3-byte) + index
        // (typically prefixed; >20 < 28 bytes) — none are 28 bytes.
        let mut baseline = Storage::open(MemVfs::new()).unwrap();
        baseline.put(1, make_key(1, &[0u8; 16]), vec![0xAAu8; 8]).unwrap();
        baseline.put(2, make_key(1, &[1u8; 16]), vec![0xBBu8; 8]).unwrap();
        // Catalog-shape key (3 bytes — distinct from 20 / 28).
        baseline.put(3, vec![0x00, 0x00, 0x01], vec![0xCCu8; 8]).unwrap();
        let baseline_digest = baseline.digest();

        // With-MVCC: same as baseline PLUS some 28-byte MVCC versioned keys.
        let mut with_mvcc = Storage::open(MemVfs::new()).unwrap();
        with_mvcc.put(1, make_key(1, &[0u8; 16]), vec![0xAAu8; 8]).unwrap();
        with_mvcc.put(2, make_key(1, &[1u8; 16]), vec![0xBBu8; 8]).unwrap();
        with_mvcc.put(3, vec![0x00, 0x00, 0x01], vec![0xCCu8; 8]).unwrap();
        // Add an MVCC key: 20-byte make_key prefix + 8 BE (u64::MAX - 5).
        let mut mvcc_key = make_key(1, &[0u8; 16]);
        mvcc_key.extend_from_slice(&(u64::MAX - 5u64).to_be_bytes());
        assert_eq!(mvcc_key.len(), 28, "constructed key must be 28 bytes");
        with_mvcc.put(4, mvcc_key.clone(), vec![0xDDu8; 16]).unwrap();
        let mut mvcc_key2 = make_key(1, &[2u8; 16]);
        mvcc_key2.extend_from_slice(&(u64::MAX - 10u64).to_be_bytes());
        with_mvcc.put(5, mvcc_key2, vec![0xEEu8; 24]).unwrap();
        let with_mvcc_digest = with_mvcc.digest();

        assert_eq!(
            baseline_digest, with_mvcc_digest,
            "SP116 Decision 1: 28-byte MVCC keys must NOT contribute to digest \
             (baseline digest {baseline_digest:#010x} != with-MVCC digest \
             {with_mvcc_digest:#010x} — Decision 1 filter is broken)"
        );

        // Negative control: adding a 27-byte key (NOT MVCC shape) DOES change the digest.
        let mut almost_mvcc = Storage::open(MemVfs::new()).unwrap();
        almost_mvcc.put(1, make_key(1, &[0u8; 16]), vec![0xAAu8; 8]).unwrap();
        almost_mvcc.put(2, make_key(1, &[1u8; 16]), vec![0xBBu8; 8]).unwrap();
        almost_mvcc.put(3, vec![0x00, 0x00, 0x01], vec![0xCCu8; 8]).unwrap();
        let mut wrong = make_key(1, &[0u8; 16]);
        wrong.extend_from_slice(&[0xFFu8; 7]); // 20 + 7 = 27 bytes, NOT MVCC
        almost_mvcc.put(4, wrong, vec![0xDDu8; 16]).unwrap();
        assert_ne!(
            baseline_digest,
            almost_mvcc.digest(),
            "SP116 Decision 1: non-28-byte keys (here 27) must still contribute to digest"
        );
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
