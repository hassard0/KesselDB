//! kessel-shard: deterministic key -> shard routing (groundwork).
//!
//! Sub-project 1 ships a SINGLE shard; this is the routing seam so the
//! multi-shard step is mechanical. Mapping uses rendezvous (highest-random-
//! weight) hashing on the storage key, so adding/removing a shard only
//! remaps the minimal key fraction (~1/shards) — no global reshuffle.
//!
//! KNOWN LIMITATION (documented, not hidden): with >1 shard, a transaction
//! spanning keys in different shards is a cross-shard transaction. Each shard
//! is its own VSR group; cross-shard atomicity (2PC / deterministic
//! coordinator) is OUT OF SCOPE for Sub-project 1 and tracked for a later
//! spec. Until then a multi-shard deployment offers per-shard atomicity only.

#![forbid(unsafe_code)]

use kessel_proto::codec::crc32c;
use kessel_storage::Key;

#[derive(Clone)]
pub struct ShardMap {
    shards: u32,
}

impl ShardMap {
    pub fn new(shards: u32) -> Self {
        ShardMap {
            shards: shards.max(1),
        }
    }

    pub fn shards(&self) -> u32 {
        self.shards
    }

    /// Rendezvous hashing: shard with the max weight for this key wins.
    pub fn shard_of(&self, key: &Key) -> u32 {
        let mut best = (0u32, 0u32);
        for s in 0..self.shards {
            let mut buf = [0u8; 24];
            buf[..20].copy_from_slice(key);
            buf[20..].copy_from_slice(&s.to_le_bytes());
            let w = crc32c(&buf);
            if w > best.1 || (w == best.1 && s < best.0) {
                best = (s, w);
            }
        }
        best.0
    }

    /// Cross-shard iff the two keys route to different shards.
    pub fn is_cross_shard(&self, a: &Key, b: &Key) -> bool {
        self.shard_of(a) != self.shard_of(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_proto::Rng;
    use kessel_storage::make_key;

    fn rand_key(rng: &mut Rng) -> Key {
        let mut id = [0u8; 16];
        rng.fill(&mut id);
        make_key((rng.next_u64() % 8) as u32, &id)
    }

    #[test]
    fn single_shard_is_trivial() {
        let m = ShardMap::new(1);
        let mut rng = Rng::new(1);
        for _ in 0..1000 {
            assert_eq!(m.shard_of(&rand_key(&mut rng)), 0);
        }
    }

    #[test]
    fn mapping_is_stable_and_deterministic() {
        let m = ShardMap::new(8);
        let mut rng = Rng::new(7);
        let keys: Vec<Key> = (0..500).map(|_| rand_key(&mut rng)).collect();
        let a: Vec<u32> = keys.iter().map(|k| m.shard_of(k)).collect();
        let b: Vec<u32> = keys.iter().map(|k| m.shard_of(k)).collect();
        assert_eq!(a, b, "routing must be a pure function of the key");
    }

    #[test]
    fn distribution_is_reasonably_balanced() {
        let shards = 8u32;
        let m = ShardMap::new(shards);
        let mut rng = Rng::new(99);
        let n = 80_000;
        let mut hist = vec![0u32; shards as usize];
        for _ in 0..n {
            hist[m.shard_of(&rand_key(&mut rng)) as usize] += 1;
        }
        let ideal = n as f64 / shards as f64;
        for (s, &c) in hist.iter().enumerate() {
            let dev = (c as f64 - ideal).abs() / ideal;
            assert!(dev < 0.15, "shard {s} skew {:.2}% (count {c})", dev * 100.0);
        }
    }

    #[test]
    fn rendezvous_minimizes_remap_on_resize() {
        // Growing 4 -> 5 shards should remap only ~1/5 of keys, not ~all.
        let m4 = ShardMap::new(4);
        let m5 = ShardMap::new(5);
        let mut rng = Rng::new(2024);
        let n = 20_000;
        let mut moved = 0;
        for _ in 0..n {
            let k = rand_key(&mut rng);
            if m4.shard_of(&k) != m5.shard_of(&k) {
                moved += 1;
            }
        }
        let frac = moved as f64 / n as f64;
        assert!(frac < 0.30, "remapped {:.1}% (expected ~20%)", frac * 100.0);
    }
}
