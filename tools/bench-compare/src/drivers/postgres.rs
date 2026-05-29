//! Postgres driver via the `postgres` crate (sync), one connection per worker
//! thread. T1 ships YCSB-C; T2+ extends with write paths.
//!
//! Configuration:
//! - `synchronous_commit = on` (default) → matches KesselDB's
//!   AutosyncMode::EveryCommit durability promise.
//! - Connection: sync `postgres::Client`, one per worker thread.

use crate::workloads::Workload;
use crate::{pct_us, BenchResult, Cli};
use postgres::{Client, NoTls};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use std::time::{Duration, Instant};

pub fn run(
    workload: &Workload,
    n: usize,
    trial: u32,
    cli: &Cli,
) -> anyhow::Result<BenchResult> {
    match workload {
        Workload::YcsbC => run_ycsb_c(n, trial, cli),
    }
}

fn schema_sql() -> &'static str {
    // YCSB schema: id PK + 10 random 100-byte fields. We use a single
    // bytea payload column for the field bundle — point-read throughput
    // is the measured property, not column-projection cost.
    "CREATE UNLOGGED TABLE IF NOT EXISTS ycsb (
        id BIGINT PRIMARY KEY,
        payload BYTEA NOT NULL
     )"
}

fn run_ycsb_c(n: usize, trial: u32, cli: &Cli) -> anyhow::Result<BenchResult> {
    let rows = cli.rows;
    let duration = Duration::from_secs(cli.duration);

    // --- setup: drop + recreate + load on a single connection ---
    let mut setup = Client::connect(&cli.pg_url, NoTls)?;
    setup.batch_execute("DROP TABLE IF EXISTS ycsb; ")?;
    setup.batch_execute(schema_sql())?;
    // Match KesselDB durability promise: synchronous_commit=on is the
    // default on Postgres 16; we set it explicitly to lock the contract.
    setup.batch_execute("SET synchronous_commit = on;")?;

    // Load with COPY for speed (we are NOT measuring load throughput here).
    let mut rng = SmallRng::seed_from_u64(0xA5A5_5A5A ^ trial as u64);
    {
        let mut tx = setup.transaction()?;
        let mut writer = tx.copy_in("COPY ycsb (id, payload) FROM STDIN (FORMAT BINARY)")?;
        use postgres::types::Type;
        let mut buf = vec![0u8; 1024];
        // BINARY COPY header.
        writer.write_all(b"PGCOPY\n\xff\r\n\0")?;
        writer.write_all(&0u32.to_be_bytes())?; // flags
        writer.write_all(&0u32.to_be_bytes())?; // extension area len
        let _ = Type::INT8; // keep import live
        for i in 0..rows {
            rng.fill(&mut buf[..]);
            buf[..8].copy_from_slice(&(i as i64).to_be_bytes());
            // Tuple: u16 ncols=2, then for each col: i32 len + bytes.
            writer.write_all(&2i16.to_be_bytes())?;
            // col 0: id BIGINT (8B BE)
            writer.write_all(&8i32.to_be_bytes())?;
            writer.write_all(&(i as i64).to_be_bytes())?;
            // col 1: payload BYTEA
            writer.write_all(&(buf.len() as i32).to_be_bytes())?;
            writer.write_all(&buf)?;
        }
        // Trailer: ncols = -1
        writer.write_all(&(-1i16).to_be_bytes())?;
        writer.finish()?;
        tx.commit()?;
    }
    setup.batch_execute("ANALYZE ycsb;")?;
    drop(setup);

    // --- steady-state: N worker threads, one connection each ---
    let started = Instant::now();
    let stop_at = started + duration;
    let mut handles = Vec::with_capacity(n);
    for tid in 0..n {
        let url = cli.pg_url.clone();
        let h = std::thread::spawn(move || -> anyhow::Result<(u64, Vec<u64>)> {
            let mut c = Client::connect(&url, NoTls)?;
            let stmt = c.prepare("SELECT payload FROM ycsb WHERE id = $1")?;
            let mut rng = SmallRng::seed_from_u64(0xDEAD_BEEF ^ (tid as u64) ^ (trial as u64));
            let mut count = 0u64;
            let mut lat = Vec::with_capacity(1 << 16);
            loop {
                if Instant::now() >= stop_at {
                    break;
                }
                let key = rng.gen_range(0..rows as i64);
                let s = Instant::now();
                let rows_back = c.query(&stmt, &[&key])?;
                lat.push(s.elapsed().as_nanos() as u64);
                debug_assert!(!rows_back.is_empty());
                count += 1;
            }
            Ok((count, lat))
        });
        handles.push(h);
    }
    let mut total_ops = 0u64;
    let mut lat_ns: Vec<u64> = Vec::new();
    for h in handles {
        let (ops, l) = h.join().expect("postgres worker panicked")?;
        total_ops += ops;
        lat_ns.extend(l);
    }
    let elapsed = started.elapsed().as_secs_f64();
    lat_ns.sort_unstable();

    Ok(BenchResult {
        db: "postgres".into(),
        workload: "ycsb-c".into(),
        n,
        trial,
        ops_per_sec: total_ops as f64 / elapsed,
        p50_us: pct_us(&lat_ns, 0.50),
        p99_us: pct_us(&lat_ns, 0.99),
        p99_99_us: pct_us(&lat_ns, 0.9999),
        runtime_secs: elapsed,
        rows,
        note: Some(
            "postgres 16.x; UNLOGGED table for symmetry with mem-VFS KesselDB; \
             synchronous_commit=on; loopback TCP via postgres crate"
                .into(),
        ),
    })
}

// Bring the trait `Write` into scope for `writer.write_all(...)`.
use std::io::Write;
