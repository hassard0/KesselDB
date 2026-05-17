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
}
