//! kessel-sim: the seeded deterministic simulator.
//!
//! M0 establishes the determinism backbone: boot N nodes over the simulated
//! clock/disk/net from a single `u64` seed, run the world forward, and fold
//! every observable into one trace hash. Same seed -> identical hash, always.
//! Later milestones drive real ops and fault injection through this same loop.

#![forbid(unsafe_code)]

use kessel_io::{Clock, MemDisk, SimClock, SimNet};
use kessel_proto::{codec::crc32c, Rng};
use std::cell::RefCell;
use std::rc::Rc;

pub struct Sim {
    pub seed: u64,
    pub nodes: usize,
    clock: SimClock,
    net: Rc<RefCell<SimNet>>,
    disks: Vec<MemDisk>,
    rng: Rng,
    /// Rolling trace digest folded from every observable each step.
    trace: u32,
}

impl Sim {
    pub fn new(seed: u64, nodes: usize) -> Self {
        Sim {
            seed,
            nodes,
            clock: SimClock::new(),
            net: Rc::new(RefCell::new(SimNet::new(nodes, seed))),
            disks: (0..nodes).map(|_| MemDisk::new()).collect(),
            rng: Rng::new(seed),
            trace: 0xFFFF_FFFF,
        }
    }

    fn fold(&mut self, tag: u8, value: u64) {
        let mut buf = [0u8; 9];
        buf[0] = tag;
        buf[1..].copy_from_slice(&value.to_le_bytes());
        // Chain the digest: new = crc32c(prev_le ++ record).
        let mut chained = self.trace.to_le_bytes().to_vec();
        chained.extend_from_slice(&buf);
        self.trace = crc32c(&chained);
    }

    /// Run `steps` idle ticks. Each tick advances the clock by a seeded
    /// amount and folds the world state into the trace. No ops yet (M0).
    pub fn run_idle(&mut self, steps: usize) -> u64 {
        for step in 0..steps {
            let jitter = 1 + self.rng.below(1000);
            self.clock.advance(jitter);
            let now = self.clock.now_nanos();
            let rngv = self.rng.next_u64();
            let pending = self.net.borrow().pending() as u64;
            let disk_lens: Vec<u64> = self
                .disks
                .iter()
                .enumerate()
                .map(|(i, d)| ((i as u64) << 32) | kessel_io::Disk::len(d))
                .collect();
            self.fold(0x01, step as u64);
            self.fold(0x02, now);
            self.fold(0x03, rngv);
            self.fold(0x04, pending);
            for v in disk_lens {
                self.fold(0x05, v);
            }
        }
        self.trace as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// M0 EXIT GATE: 100 seeds, each run twice, identical trace hash.
    #[test]
    fn m0_determinism_gate() {
        for seed in 0..100u64 {
            let h1 = Sim::new(seed, 3).run_idle(50);
            let h2 = Sim::new(seed, 3).run_idle(50);
            assert_eq!(h1, h2, "seed {seed} must reproduce bit-for-bit");
        }
    }

    #[test]
    fn distinct_seeds_generally_diverge() {
        let mut seen = std::collections::HashSet::new();
        for seed in 0..100u64 {
            seen.insert(Sim::new(seed, 3).run_idle(50));
        }
        // Not a hard guarantee, but a healthy simulator should not collapse
        // 100 seeds into a handful of traces.
        assert!(seen.len() > 90, "only {} distinct traces", seen.len());
    }

    #[test]
    fn node_count_affects_trace() {
        assert_ne!(
            Sim::new(5, 3).run_idle(20),
            Sim::new(5, 5).run_idle(20),
            "cluster size is part of the observable world"
        );
    }
}
