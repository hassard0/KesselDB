//! kessel-io: the determinism seam.
//!
//! Everything above storage talks to the outside world ONLY through these
//! three traits. Production injects the real impls; `kessel-sim` injects
//! seeded fakes so the whole database is reproducible from one `u64`.

#![forbid(unsafe_code)]

use kessel_proto::Rng;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Monotonic-ish time source. The state machine never calls this; only the
/// VSR primary does, then replicates the value.
pub trait Clock {
    fn now_nanos(&self) -> u64;
}

/// Block-addressable durable store. Offsets/lengths are byte-granular; the
/// storage engine imposes its own block structure on top.
pub trait Disk {
    fn write_at(&mut self, off: u64, buf: &[u8]) -> io::Result<()>;
    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn sync(&mut self) -> io::Result<()>;
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Message transport between replicas. Addressing is by replica index.
pub trait Net {
    fn send(&mut self, to: usize, msg: Vec<u8>);
    /// Non-blocking receive: `(from_replica, bytes)` or `None`.
    fn recv(&mut self) -> Option<(usize, Vec<u8>)>;
}

// ----------------------------------------------------------------------------
// Real implementations (production)
// ----------------------------------------------------------------------------

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_nanos(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }
}

/// File-backed disk. `read_at`/`write_at` seek explicitly; `sync` is a real
/// `fsync` so durability claims are honest.
pub struct FileDisk {
    file: RefCell<File>,
}

impl FileDisk {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        Ok(FileDisk {
            file: RefCell::new(file),
        })
    }
}

impl Disk for FileDisk {
    fn write_at(&mut self, off: u64, buf: &[u8]) -> io::Result<()> {
        let mut f = self.file.borrow_mut();
        f.seek(SeekFrom::Start(off))?;
        f.write_all(buf)
    }
    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        let mut f = self.file.borrow_mut();
        f.seek(SeekFrom::Start(off))?;
        let mut read = 0;
        while read < buf.len() {
            match f.read(&mut buf[read..]) {
                Ok(0) => break,
                Ok(n) => read += n,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(read)
    }
    fn sync(&mut self) -> io::Result<()> {
        self.file.borrow_mut().sync_all()
    }
    fn len(&self) -> u64 {
        self.file
            .borrow()
            .metadata()
            .map(|m| m.len())
            .unwrap_or(0)
    }
}

// ----------------------------------------------------------------------------
// Simulated implementations (deterministic, seeded)
// ----------------------------------------------------------------------------

/// Logical clock advanced explicitly by the simulator. Shared by clone.
#[derive(Clone, Default)]
pub struct SimClock {
    nanos: Rc<RefCell<u64>>,
}

impl SimClock {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn advance(&self, by: u64) {
        *self.nanos.borrow_mut() += by;
    }
    pub fn set(&self, v: u64) {
        *self.nanos.borrow_mut() = v;
    }
}

impl Clock for SimClock {
    fn now_nanos(&self) -> u64 {
        *self.nanos.borrow()
    }
}

/// In-memory disk with fault hooks. M0 uses it clean; M1 turns on torn-write
/// and corruption injection (the hooks are present so the format/API doesn't
/// change later).
pub struct MemDisk {
    data: Vec<u8>,
    /// Bytes written since the last `sync`, used by M1 torn-write injection.
    unsynced_from: Option<u64>,
}

impl MemDisk {
    pub fn new() -> Self {
        MemDisk {
            data: Vec::new(),
            unsynced_from: None,
        }
    }
    pub fn snapshot(&self) -> Vec<u8> {
        self.data.clone()
    }
    pub fn from_snapshot(data: Vec<u8>) -> Self {
        MemDisk {
            data,
            unsynced_from: None,
        }
    }
}

impl Default for MemDisk {
    fn default() -> Self {
        Self::new()
    }
}

impl Disk for MemDisk {
    fn write_at(&mut self, off: u64, buf: &[u8]) -> io::Result<()> {
        let end = off as usize + buf.len();
        if end > self.data.len() {
            self.data.resize(end, 0);
        }
        self.data[off as usize..end].copy_from_slice(buf);
        self.unsynced_from = Some(match self.unsynced_from {
            Some(p) => p.min(off),
            None => off,
        });
        Ok(())
    }
    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        let off = off as usize;
        if off >= self.data.len() {
            return Ok(0);
        }
        let n = buf.len().min(self.data.len() - off);
        buf[..n].copy_from_slice(&self.data[off..off + n]);
        Ok(n)
    }
    fn sync(&mut self) -> io::Result<()> {
        self.unsynced_from = None;
        Ok(())
    }
    fn len(&self) -> u64 {
        self.data.len() as u64
    }
}

/// Deterministic in-process message bus for a fixed set of replicas. M0/M2
/// use it FIFO; M3 layers seeded drop/dup/reorder/delay on top of `step`.
pub struct SimNet {
    /// Per-destination inbox.
    inboxes: Vec<VecDeque<(usize, Vec<u8>)>>,
    rng: Rng,
}

impl SimNet {
    pub fn new(replicas: usize, seed: u64) -> Self {
        SimNet {
            inboxes: (0..replicas).map(|_| VecDeque::new()).collect(),
            rng: Rng::new(seed),
        }
    }
    /// A per-replica handle implementing `Net`, routed through this bus.
    pub fn handle(net: Rc<RefCell<SimNet>>, replica: usize) -> SimNetHandle {
        SimNetHandle { net, replica }
    }
    pub fn deliver(&mut self, from: usize, to: usize, msg: Vec<u8>) {
        // Fault injection seam (no faults in M0; consumes rng so the seed
        // schedule is stable when faults are enabled in M3).
        let _ = self.rng.next_u64();
        if let Some(inbox) = self.inboxes.get_mut(to) {
            inbox.push_back((from, msg));
        }
    }
    pub fn pending(&self) -> usize {
        self.inboxes.iter().map(|q| q.len()).sum()
    }
}

pub struct SimNetHandle {
    net: Rc<RefCell<SimNet>>,
    replica: usize,
}

impl Net for SimNetHandle {
    fn send(&mut self, to: usize, msg: Vec<u8>) {
        self.net.borrow_mut().deliver(self.replica, to, msg);
    }
    fn recv(&mut self) -> Option<(usize, Vec<u8>)> {
        self.net
            .borrow_mut()
            .inboxes
            .get_mut(self.replica)
            .and_then(|q| q.pop_front())
    }
}

// ----------------------------------------------------------------------------
// Vfs: a namespace of named durable blobs (so the storage engine can have
// WAL + manifest + many SSTables while ALL I/O still flows through the seam).
// ----------------------------------------------------------------------------

pub trait Vfs {
    fn open(&self, name: &str) -> io::Result<Box<dyn Disk>>;
    fn exists(&self, name: &str) -> bool;
    fn remove(&self, name: &str) -> io::Result<()>;
    fn list(&self) -> Vec<String>;
}

/// Real directory-backed VFS (production).
pub struct DirVfs {
    root: std::path::PathBuf,
}

impl DirVfs {
    pub fn new(root: impl AsRef<Path>) -> io::Result<Self> {
        std::fs::create_dir_all(root.as_ref())?;
        Ok(DirVfs {
            root: root.as_ref().to_path_buf(),
        })
    }
}

impl Vfs for DirVfs {
    fn open(&self, name: &str) -> io::Result<Box<dyn Disk>> {
        Ok(Box::new(FileDisk::open(self.root.join(name))?))
    }
    fn exists(&self, name: &str) -> bool {
        self.root.join(name).exists()
    }
    fn remove(&self, name: &str) -> io::Result<()> {
        let p = self.root.join(name);
        if p.exists() {
            std::fs::remove_file(p)?;
        }
        Ok(())
    }
    fn list(&self) -> Vec<String> {
        std::fs::read_dir(&self.root)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter_map(|e| e.file_name().into_string().ok())
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[derive(Clone, Default)]
struct MemBlob {
    /// All bytes ever written.
    data: Rc<RefCell<Vec<u8>>>,
    /// Length known durable as of the last `sync` (torn-tail model).
    synced_len: Rc<RefCell<u64>>,
}

/// Deterministic in-memory VFS (simulator). Models crash by discarding any
/// bytes written after the last `sync` on each blob (the common "unsynced
/// tail is not durable" failure — enough for a meaningful recovery test).
#[derive(Clone, Default)]
pub struct MemVfs {
    blobs: Rc<RefCell<std::collections::BTreeMap<String, MemBlob>>>,
}

impl MemVfs {
    pub fn new() -> Self {
        Self::default()
    }
    /// Simulate a crash: every blob loses anything past its last sync point.
    pub fn crash(&self) {
        for blob in self.blobs.borrow().values() {
            let keep = *blob.synced_len.borrow() as usize;
            blob.data.borrow_mut().truncate(keep);
        }
    }
}

struct MemVfsDisk {
    blob: MemBlob,
}

impl Vfs for MemVfs {
    fn open(&self, name: &str) -> io::Result<Box<dyn Disk>> {
        let blob = self
            .blobs
            .borrow_mut()
            .entry(name.to_string())
            .or_default()
            .clone();
        Ok(Box::new(MemVfsDisk { blob }))
    }
    fn exists(&self, name: &str) -> bool {
        self.blobs.borrow().contains_key(name)
    }
    fn remove(&self, name: &str) -> io::Result<()> {
        self.blobs.borrow_mut().remove(name);
        Ok(())
    }
    fn list(&self) -> Vec<String> {
        self.blobs.borrow().keys().cloned().collect()
    }
}

impl Disk for MemVfsDisk {
    fn write_at(&mut self, off: u64, buf: &[u8]) -> io::Result<()> {
        let mut d = self.blob.data.borrow_mut();
        let end = off as usize + buf.len();
        if end > d.len() {
            d.resize(end, 0);
        }
        d[off as usize..end].copy_from_slice(buf);
        Ok(())
    }
    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        let d = self.blob.data.borrow();
        let off = off as usize;
        if off >= d.len() {
            return Ok(0);
        }
        let n = buf.len().min(d.len() - off);
        buf[..n].copy_from_slice(&d[off..off + n]);
        Ok(n)
    }
    fn sync(&mut self) -> io::Result<()> {
        let len = self.blob.data.borrow().len() as u64;
        *self.blob.synced_len.borrow_mut() = len;
        Ok(())
    }
    fn len(&self) -> u64 {
        self.blob.data.borrow().len() as u64
    }
}

// ----------------------------------------------------------------------------
// FaultVfs — deterministic disk-fault injection (SP92)
// ----------------------------------------------------------------------------

/// What a single armed fault does to one `write_at`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FaultKind {
    /// Persist only the first half of the buffer (a torn write): the
    /// frame on disk is short, so a length/CRC-checked replay must stop
    /// cleanly at it and recover the intact prefix.
    Torn,
    /// Fail the write with an I/O error (the caller's `?` propagates).
    Err,
}

/// Deterministic, externally-controlled fault schedule shared by every
/// `FaultDisk` a `FaultVfs` hands out. Unarmed (`kind == None`) it is a
/// pure pass-through, so wrapping any VFS in `FaultVfs` changes nothing
/// until a test explicitly `arm`s it.
#[derive(Clone, Default)]
pub struct FaultPlan {
    /// Substring matched against the opened file name; only writes to a
    /// matching file are counted/affected.
    pub target: String,
    /// 1-based index of the matching write to hit. `0` = disarmed.
    pub at_write: u32,
    pub kind: Option<FaultKind>,
    /// Matching writes observed so far (deterministic counter).
    pub writes_seen: u32,
    /// Set once the fault has actually fired (lets a test assert it did).
    pub fired: bool,
}

/// A VFS wrapper that injects one deterministic disk fault. `inner` is
/// any real VFS (typically `MemVfs`); the plan is shared by clone so a
/// test holds one handle and every disk the cluster opens obeys it.
#[derive(Clone)]
pub struct FaultVfs<V: Vfs> {
    inner: V,
    plan: Rc<RefCell<FaultPlan>>,
}

impl<V: Vfs> FaultVfs<V> {
    pub fn new(inner: V) -> Self {
        FaultVfs { inner, plan: Rc::new(RefCell::new(FaultPlan::default())) }
    }
    /// Shared plan handle (clone-cheap; same `RefCell` as every disk).
    pub fn plan(&self) -> Rc<RefCell<FaultPlan>> {
        Rc::clone(&self.plan)
    }
    /// Arm the fault: hit the `nth` (1-based) write to a file whose name
    /// contains `target`, with `kind`. Resets the counter.
    pub fn arm(&self, target: &str, nth: u32, kind: FaultKind) {
        let mut p = self.plan.borrow_mut();
        p.target = target.to_string();
        p.at_write = nth;
        p.kind = Some(kind);
        p.writes_seen = 0;
        p.fired = false;
    }
    pub fn fired(&self) -> bool {
        self.plan.borrow().fired
    }
    pub fn inner(&self) -> &V {
        &self.inner
    }
}

impl<V: Vfs> Vfs for FaultVfs<V> {
    fn open(&self, name: &str) -> io::Result<Box<dyn Disk>> {
        Ok(Box::new(FaultDisk {
            inner: self.inner.open(name)?,
            name: name.to_string(),
            plan: Rc::clone(&self.plan),
        }))
    }
    fn exists(&self, name: &str) -> bool {
        self.inner.exists(name)
    }
    fn remove(&self, name: &str) -> io::Result<()> {
        self.inner.remove(name)
    }
    fn list(&self) -> Vec<String> {
        self.inner.list()
    }
}

struct FaultDisk {
    inner: Box<dyn Disk>,
    name: String,
    plan: Rc<RefCell<FaultPlan>>,
}

impl Disk for FaultDisk {
    fn write_at(&mut self, off: u64, buf: &[u8]) -> io::Result<()> {
        // Decide under the borrow, act after it (don't hold it across
        // the inner I/O call).
        let action = {
            let mut p = self.plan.borrow_mut();
            if p.kind.is_some()
                && p.at_write > 0
                && self.name.contains(&p.target)
            {
                p.writes_seen += 1;
                if p.writes_seen == p.at_write {
                    p.fired = true;
                    p.kind
                } else {
                    None
                }
            } else {
                None
            }
        };
        match action {
            Some(FaultKind::Err) => Err(io::Error::new(
                io::ErrorKind::Other,
                "injected disk write fault",
            )),
            Some(FaultKind::Torn) => {
                // Persist only the first half — a short/torn frame.
                let keep = buf.len() / 2;
                self.inner.write_at(off, &buf[..keep])
            }
            None => self.inner.write_at(off, buf),
        }
    }
    fn read_at(&self, off: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read_at(off, buf)
    }
    fn sync(&mut self) -> io::Result<()> {
        self.inner.sync()
    }
    fn len(&self) -> u64 {
        self.inner.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memdisk_write_read_roundtrip() {
        let mut d = MemDisk::new();
        d.write_at(8, b"kessel").unwrap();
        let mut buf = [0u8; 6];
        let n = d.read_at(8, &mut buf).unwrap();
        assert_eq!(n, 6);
        assert_eq!(&buf, b"kessel");
        assert_eq!(d.len(), 14);
        assert_eq!(d.read_at(100, &mut buf).unwrap(), 0);
    }

    #[test]
    fn memdisk_snapshot_restore() {
        let mut d = MemDisk::new();
        d.write_at(0, b"abcd").unwrap();
        let snap = d.snapshot();
        let d2 = MemDisk::from_snapshot(snap);
        let mut buf = [0u8; 4];
        d2.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf, b"abcd");
    }

    #[test]
    fn simclock_is_explicit_and_shared() {
        let c = SimClock::new();
        assert_eq!(c.now_nanos(), 0);
        let c2 = c.clone();
        c.advance(100);
        assert_eq!(c2.now_nanos(), 100, "clones share the same logical time");
    }

    #[test]
    fn simnet_fifo_delivery_between_handles() {
        let net = Rc::new(RefCell::new(SimNet::new(2, 1)));
        let mut a = SimNet::handle(net.clone(), 0);
        let mut b = SimNet::handle(net.clone(), 1);
        a.send(1, b"ping".to_vec());
        a.send(1, b"pong".to_vec());
        assert_eq!(b.recv(), Some((0, b"ping".to_vec())));
        assert_eq!(b.recv(), Some((0, b"pong".to_vec())));
        assert_eq!(b.recv(), None);
    }

    #[test]
    fn simnet_schedule_is_seed_deterministic() {
        let run = || {
            let net = Rc::new(RefCell::new(SimNet::new(2, 7)));
            let mut a = SimNet::handle(net.clone(), 0);
            for i in 0..5u8 {
                a.send(1, vec![i]);
            }
            let v = net.borrow().rng.clone().next_u64();
            v
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn memvfs_persists_across_reopen() {
        let vfs = MemVfs::new();
        {
            let mut d = vfs.open("wal").unwrap();
            d.write_at(0, b"durable").unwrap();
            d.sync().unwrap();
        }
        let d2 = vfs.open("wal").unwrap();
        let mut buf = [0u8; 7];
        d2.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf, b"durable");
        assert!(vfs.exists("wal"));
        assert_eq!(vfs.list(), vec!["wal".to_string()]);
    }

    #[test]
    fn memvfs_crash_discards_unsynced_tail() {
        let vfs = MemVfs::new();
        {
            let mut d = vfs.open("seg").unwrap();
            d.write_at(0, b"keep").unwrap();
            d.sync().unwrap();
            d.write_at(4, b"LOSTLOST").unwrap(); // never synced
        }
        vfs.crash();
        let d = vfs.open("seg").unwrap();
        assert_eq!(d.len(), 4, "unsynced tail must be gone after crash");
        let mut buf = [0u8; 4];
        d.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf, b"keep");
    }
}
