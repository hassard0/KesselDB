//! kessel-cache: a bounded read cache.
//!
//! Architectural rule (see ARCHITECTURE.md): the cache is a side index off
//! ALREADY-committed state. It is NEVER consulted inside the deterministic
//! `apply` write path, so it cannot affect replication determinism or the
//! state digest. It only memoizes point reads and is invalidated by the
//! state machine on Update/Delete. Off => zero effect on the core path.

#![forbid(unsafe_code)]

use kessel_storage::Key;
use std::collections::HashMap;

pub struct ReadCache {
    cap: usize,
    /// key -> (value, last_used logical tick)
    map: HashMap<Key, (Vec<u8>, u64)>,
    clock: u64,
    pub hits: u64,
    pub misses: u64,
}

impl ReadCache {
    pub fn new(cap: usize) -> Self {
        ReadCache {
            cap: cap.max(1),
            map: HashMap::new(),
            clock: 0,
            hits: 0,
            misses: 0,
        }
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    pub fn get(&mut self, key: &Key) -> Option<Vec<u8>> {
        let t = self.tick();
        if let Some(e) = self.map.get_mut(key) {
            e.1 = t;
            self.hits += 1;
            Some(e.0.clone())
        } else {
            self.misses += 1;
            None
        }
    }

    /// Populate (read-fill or write-through with fresh bytes).
    pub fn insert(&mut self, key: Key, val: Vec<u8>) {
        let t = self.tick();
        if !self.map.contains_key(&key) && self.map.len() >= self.cap {
            // Evict least-recently-used. Deterministic tiebreak: smallest key.
            if let Some((&victim, _)) = self
                .map
                .iter()
                .min_by(|a, b| a.1 .1.cmp(&b.1 .1).then(a.0.cmp(b.0)))
            {
                self.map.remove(&victim);
            }
        }
        self.map.insert(key, (val, t));
    }

    /// MUST be called by the state machine on every Update/Delete so a stale
    /// value can never be served.
    pub fn invalidate(&mut self, key: &Key) {
        self.map.remove(key);
    }

    /// Drop every entry (used when a transaction aborts — any entries it
    /// wrote referenced uncommitted overlay values).
    pub fn clear(&mut self) {
        self.map.clear();
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    pub fn hit_rate(&self) -> f64 {
        let tot = self.hits + self.misses;
        if tot == 0 {
            0.0
        } else {
            self.hits as f64 / tot as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_storage::make_key;

    fn k(n: u128) -> Key {
        make_key(1, &n.to_le_bytes())
    }

    #[test]
    fn never_serves_stale_after_invalidate() {
        let mut c = ReadCache::new(8);
        c.insert(k(1), b"old".to_vec());
        assert_eq!(c.get(&k(1)), Some(b"old".to_vec()));
        c.invalidate(&k(1)); // simulates an Update/Delete
        assert_eq!(c.get(&k(1)), None, "stale value must not be served");
        c.insert(k(1), b"new".to_vec());
        assert_eq!(c.get(&k(1)), Some(b"new".to_vec()));
    }

    #[test]
    fn lru_eviction_is_bounded_and_deterministic() {
        let mut c = ReadCache::new(3);
        c.insert(k(1), vec![1]);
        c.insert(k(2), vec![2]);
        c.insert(k(3), vec![3]);
        c.get(&k(1)); // touch 1 -> 2 is now LRU
        c.get(&k(3));
        c.insert(k(4), vec![4]); // evicts k(2)
        assert_eq!(c.len(), 3);
        assert_eq!(c.get(&k(2)), None, "LRU victim evicted");
        assert_eq!(c.get(&k(1)), Some(vec![1]));
        assert_eq!(c.get(&k(4)), Some(vec![4]));
    }

    #[test]
    fn metrics_track_hits_and_misses() {
        let mut c = ReadCache::new(4);
        c.get(&k(9)); // miss
        c.insert(k(9), vec![9]);
        c.get(&k(9)); // hit
        c.get(&k(9)); // hit
        assert_eq!(c.hits, 2);
        assert_eq!(c.misses, 1);
        assert!((c.hit_rate() - 2.0 / 3.0).abs() < 1e-9);
    }
}
