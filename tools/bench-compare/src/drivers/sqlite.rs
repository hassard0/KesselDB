//! SQLite driver via rusqlite (bundled SQLite — hermetic builds).
//!
//! Configuration to match KesselDB MemVfs / Postgres UNLOGGED:
//!   journal_mode = MEMORY (no WAL flush to disk)
//!   synchronous  = OFF (no fsync on commit)
//! This is the "in-memory engine" parity tier. T2 will add a "durable" tier
//! that exercises WAL + synchronous=FULL on all DBs.
//!
//! SQLite is single-writer by design. journal_mode=MEMORY uses a rollback
//! journal — there is exactly one writer at a time even with multiple
//! connections. For YCSB-A (50% writes), N>1 workers will contend on the
//! shared write lock; this is an honest property of SQLite, not a bench
//! artifact. We report the resulting numbers and call it out in the
//! BENCHMARKS.md "SQLite write-concurrency note".

use crate::workloads::Workload;
use crate::{pct_us, BenchResult, Cli};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rusqlite::{params, Connection, OpenFlags};
use std::time::{Duration, Instant};

pub fn run(
    workload: &Workload,
    n: usize,
    trial: u32,
    cli: &Cli,
) -> anyhow::Result<BenchResult> {
    run_ycsb_mixed(*workload, n, trial, cli)
}

fn pragmas(c: &Connection) -> anyhow::Result<()> {
    // Match KesselDB MemVfs durability tier (in-memory; no fsync).
    c.execute_batch(
        "PRAGMA journal_mode = MEMORY;
         PRAGMA synchronous = OFF;
         PRAGMA temp_store = MEMORY;
         PRAGMA cache_size = -262144;", // 256 MiB page cache
    )?;
    Ok(())
}

fn run_ycsb_mixed(
    workload: Workload,
    n: usize,
    trial: u32,
    cli: &Cli,
) -> anyhow::Result<BenchResult> {
    let rows = cli.rows;
    let duration = Duration::from_secs(cli.duration);
    let write_ratio = workload.write_ratio();
    let needs_write = workload.has_writes();

    // Use a file-backed DB (deleted per trial). file::memory: cache=shared
    // would also work but has more cross-platform footguns; on-disk + journal_mode=MEMORY
    // gives the same write-side durability and avoids them.
    let path = cli.sqlite_path.clone();
    let _ = std::fs::remove_file(&path);

    // Setup connection.
    let setup = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    )?;
    pragmas(&setup)?;
    setup.execute_batch(
        "CREATE TABLE IF NOT EXISTS ycsb (id INTEGER PRIMARY KEY, payload BLOB NOT NULL)",
    )?;

    // Load.
    let mut rng = SmallRng::seed_from_u64(0xA5A5_5A5A ^ trial as u64);
    setup.execute("BEGIN", [])?;
    {
        let mut stmt = setup.prepare("INSERT INTO ycsb (id, payload) VALUES (?1, ?2)")?;
        let mut buf = vec![0u8; 1024];
        for i in 0..rows {
            rng.fill(&mut buf[..]);
            buf[..8].copy_from_slice(&(i as i64).to_le_bytes());
            stmt.execute(params![i as i64, &buf[..]])?;
        }
    }
    setup.execute("COMMIT", [])?;
    setup.execute_batch("ANALYZE")?;
    drop(setup);

    // Steady-state: N worker threads, one connection each.
    // - Read-only workload (YCSB-C): each worker opens read-only.
    // - Mixed workload (YCSB-A/B): each worker opens read-write; the SQLite
    //   engine enforces single-writer-at-a-time via the rollback journal lock.
    let started = Instant::now();
    let stop_at = started + duration;
    let mut handles = Vec::with_capacity(n);
    for tid in 0..n {
        let path = path.clone();
        let h = std::thread::spawn(move || -> anyhow::Result<(u64, u64, Vec<u64>)> {
            let flags = if needs_write {
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX
            } else {
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX
            };
            let c = Connection::open_with_flags(&path, flags)?;
            pragmas(&c)?;
            // Bump the busy timeout so contended writers retry instead of
            // failing with SQLITE_BUSY when another worker holds the lock.
            c.busy_timeout(Duration::from_secs(10))?;
            let mut sel = c.prepare("SELECT payload FROM ycsb WHERE id = ?1")?;
            let mut upd = c.prepare("UPDATE ycsb SET payload = ?2 WHERE id = ?1")?;
            let mut rng = SmallRng::seed_from_u64(0xDEAD_BEEF ^ (tid as u64) ^ (trial as u64));
            let mut count_total = 0u64;
            let mut count_writes = 0u64;
            let mut lat = Vec::with_capacity(1 << 16);
            let mut payload = vec![0u8; 1024];
            loop {
                if Instant::now() >= stop_at {
                    break;
                }
                let key = rng.gen_range(0..rows as i64);
                let is_write = write_ratio > 0.0 && rng.gen::<f64>() < write_ratio;
                if is_write {
                    rng.fill(&mut payload[..]);
                    payload[..8].copy_from_slice(&key.to_le_bytes());
                    let s = Instant::now();
                    let n_rows = upd.execute(params![key, &payload[..]])?;
                    lat.push(s.elapsed().as_nanos() as u64);
                    debug_assert!(n_rows == 1);
                    count_writes += 1;
                } else {
                    let s = Instant::now();
                    let _v: Vec<u8> = sel.query_row(params![key], |row| row.get(0))?;
                    lat.push(s.elapsed().as_nanos() as u64);
                }
                count_total += 1;
            }
            Ok((count_total, count_writes, lat))
        });
        handles.push(h);
    }
    let mut total_ops = 0u64;
    let mut total_writes = 0u64;
    let mut lat_ns: Vec<u64> = Vec::new();
    for h in handles {
        let (ops, w, l) = h.join().expect("sqlite worker panicked")?;
        total_ops += ops;
        total_writes += w;
        lat_ns.extend(l);
    }
    let elapsed = started.elapsed().as_secs_f64();
    lat_ns.sort_unstable();

    // Best-effort cleanup; ignore failures.
    let _ = std::fs::remove_file(&path);

    let actual_wr = if total_ops > 0 {
        total_writes as f64 / total_ops as f64
    } else {
        0.0
    };

    Ok(BenchResult {
        db: "sqlite".into(),
        workload: workload.name().to_string(),
        n,
        trial,
        ops_per_sec: total_ops as f64 / elapsed,
        p50_us: pct_us(&lat_ns, 0.50),
        p99_us: pct_us(&lat_ns, 0.99),
        p99_99_us: pct_us(&lat_ns, 0.9999),
        runtime_secs: elapsed,
        rows,
        note: Some(format!(
            "sqlite via rusqlite-bundled; journal_mode=MEMORY synchronous=OFF; \
             target write_ratio={:.2} actual={:.3}; single-writer-at-a-time \
             (rollback journal lock; N>1 writers serialize via busy_timeout)",
            write_ratio, actual_wr
        )),
    })
}
