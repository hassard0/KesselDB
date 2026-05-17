//! Standalone storage micro-benchmark (localhost, MemVfs — no real fsync,
//! so this is an in-memory upper bound, honestly labelled as such).
//! Run: `cargo run -p kessel-storage --release --example bench_storage`

use kessel_io::MemVfs;
use kessel_proto::Rng;
use kessel_storage::{make_key, Storage};
use std::time::Instant;

fn main() {
    let n: u64 = 1_000_000;
    let mut s = Storage::open(MemVfs::new()).unwrap();
    let mut rng = Rng::new(1);
    let val = vec![0xABu8; 128]; // ~TigerBeetle-record-sized payload

    let t = Instant::now();
    for op in 0..n {
        let id = (rng.next_u64() as u128).to_le_bytes();
        s.put(op, make_key(1, &id), val.clone()).unwrap();
        if op % 50_000 == 49_999 {
            s.flush().unwrap();
        }
    }
    let secs = t.elapsed().as_secs_f64();
    println!(
        "PUT  {} ops in {:.3}s = {:.0} ops/s (128B records, MemVfs in-mem)",
        n,
        secs,
        n as f64 / secs
    );

    let mut rng2 = Rng::new(1);
    let t = Instant::now();
    let mut hits = 0u64;
    for _ in 0..n {
        let id = (rng2.next_u64() as u128).to_le_bytes();
        if s.get(&make_key(1, &id)).is_some() {
            hits += 1;
        }
    }
    let secs = t.elapsed().as_secs_f64();
    println!(
        "GET  {} ops in {:.3}s = {:.0} ops/s ({} hits)",
        n,
        secs,
        n as f64 / secs,
        hits
    );
}
