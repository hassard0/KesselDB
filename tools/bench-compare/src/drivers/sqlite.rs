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

use crate::workloads::{sysbench, Workload};
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
    if workload.is_sysbench() {
        run_sysbench_oltp(*workload, n, trial, cli)
    } else if workload.is_tpch() {
        super::sqlite_tpch::run_tpch(*workload, n, trial, cli)
    } else {
        run_ycsb_mixed(*workload, n, trial, cli)
    }
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

// ---------------------------------------------------------------------------
// sysbench OLTP (T3)
// ---------------------------------------------------------------------------

fn run_sysbench_oltp(
    workload: Workload,
    n: usize,
    trial: u32,
    cli: &Cli,
) -> anyhow::Result<BenchResult> {
    let tables = cli.tables;
    let rows_per_table = cli.rows_per_table;
    let duration = Duration::from_secs(cli.duration);
    let needs_write = workload.sysbench_has_writes();
    if tables == 0 || rows_per_table == 0 {
        anyhow::bail!("sqlite sysbench: --tables and --rows-per-table must be >0");
    }

    let path = cli.sqlite_path.clone();
    let _ = std::fs::remove_file(&path);

    let setup = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    )?;
    pragmas(&setup)?;

    // Create N tables. Each gets a secondary index on `k`.
    let mut ddl = String::new();
    for t in 1..=tables {
        ddl.push_str(&format!(
            "CREATE TABLE IF NOT EXISTS sbtest{t} (
                id INTEGER PRIMARY KEY,
                k INTEGER NOT NULL DEFAULT 0,
                c BLOB NOT NULL,
                pad BLOB NOT NULL
             );
             CREATE INDEX IF NOT EXISTS sbtest{t}_k ON sbtest{t}(k);"
        ));
    }
    setup.execute_batch(&ddl)?;

    // Load.
    let mut rng = SmallRng::seed_from_u64(0xA5A5_5A5A ^ trial as u64);
    setup.execute("BEGIN", [])?;
    for t in 1..=tables {
        let stmt_sql = format!("INSERT INTO sbtest{t} (id, k, c, pad) VALUES (?1, ?2, ?3, ?4)");
        let mut stmt = setup.prepare(&stmt_sql)?;
        let mut c_buf = vec![0u8; sysbench::C_WIDTH];
        let mut pad_buf = vec![0u8; sysbench::PAD_WIDTH];
        for i in 0..rows_per_table {
            rng.fill(&mut c_buf[..]);
            rng.fill(&mut pad_buf[..]);
            let k = rng.gen::<i32>();
            stmt.execute(params![i as i64, k, &c_buf[..], &pad_buf[..]])?;
        }
    }
    setup.execute("COMMIT", [])?;
    setup.execute_batch("ANALYZE")?;
    drop(setup);

    let started = Instant::now();
    let stop_at = started + duration;
    let mut handles = Vec::with_capacity(n);
    let has_reads = workload.sysbench_has_reads();
    let has_writes = workload.sysbench_has_writes();
    for tid in 0..n {
        let path = path.clone();
        let h = std::thread::spawn(move || -> anyhow::Result<(u64, u64, u64, Vec<u64>)> {
            let flags = if needs_write {
                OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX
            } else {
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX
            };
            let c = Connection::open_with_flags(&path, flags)?;
            pragmas(&c)?;
            // Long busy_timeout — sysbench WO at N=8/16 has high contention
            // on the rollback-journal exclusive lock. 60s lets the slowest
            // writer wait through the contention window without surfacing
            // SQLITE_BUSY to user code. (The contention itself is honest;
            // crashing the bench is not.)
            c.busy_timeout(Duration::from_secs(60))?;
            // Prepare per-table statements.
            #[allow(clippy::type_complexity)]
            struct PerTable<'a> {
                point: rusqlite::Statement<'a>,
                simple_range: rusqlite::Statement<'a>,
                sum_range: rusqlite::Statement<'a>,
                order_range: rusqlite::Statement<'a>,
                distinct_range: rusqlite::Statement<'a>,
                upd_idx: rusqlite::Statement<'a>,
                upd_nix: rusqlite::Statement<'a>,
                del: rusqlite::Statement<'a>,
                ins: rusqlite::Statement<'a>,
            }
            // rusqlite Statements borrow Connection — keep them on the stack
            // via a Vec<PerTable<'a>> with the same lifetime as c.
            let mut tables_v: Vec<PerTable> = Vec::with_capacity(tables);
            for t in 1..=tables {
                tables_v.push(PerTable {
                    point: c.prepare(&format!("SELECT c FROM sbtest{t} WHERE id = ?1"))?,
                    simple_range: c.prepare(&format!(
                        "SELECT c FROM sbtest{t} WHERE id BETWEEN ?1 AND ?2"
                    ))?,
                    sum_range: c.prepare(&format!(
                        "SELECT SUM(k) FROM sbtest{t} WHERE id BETWEEN ?1 AND ?2"
                    ))?,
                    order_range: c.prepare(&format!(
                        "SELECT c FROM sbtest{t} WHERE id BETWEEN ?1 AND ?2 ORDER BY c"
                    ))?,
                    distinct_range: c.prepare(&format!(
                        "SELECT DISTINCT c FROM sbtest{t} WHERE id BETWEEN ?1 AND ?2 ORDER BY c"
                    ))?,
                    upd_idx: c.prepare(&format!("UPDATE sbtest{t} SET k = k + 1 WHERE id = ?1"))?,
                    upd_nix: c.prepare(&format!("UPDATE sbtest{t} SET c = ?2 WHERE id = ?1"))?,
                    del: c.prepare(&format!("DELETE FROM sbtest{t} WHERE id = ?1"))?,
                    ins: c.prepare(&format!(
                        "INSERT INTO sbtest{t} (id, k, c, pad) VALUES (?1, ?2, ?3, ?4)"
                    ))?,
                });
            }

            let mut rng = SmallRng::seed_from_u64(0xDEAD_BEEF ^ (tid as u64) ^ (trial as u64));
            let mut count_txns = 0u64;
            let mut count_inner = 0u64;
            let mut count_aborts = 0u64;
            let mut lat = Vec::with_capacity(1 << 16);
            let mut c_buf = vec![0u8; sysbench::C_WIDTH];
            let mut pad_buf = vec![0u8; sysbench::PAD_WIDTH];
            // Helper: is_busy_err — recognise SQLITE_BUSY / SQLITE_LOCKED
            // from any rusqlite error chain.
            fn is_busy(e: &rusqlite::Error) -> bool {
                use rusqlite::ffi::ErrorCode;
                if let rusqlite::Error::SqliteFailure(ffi_err, _) = e {
                    matches!(
                        ffi_err.code,
                        ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
                    )
                } else {
                    false
                }
            }
            loop {
                if Instant::now() >= stop_at {
                    break;
                }
                let table_idx = rng.gen_range(0..tables);
                let p = &mut tables_v[table_idx];
                let inner_count: u64; // assigned in the Ok arm below
                let s = Instant::now();
                // SQLite uses BEGIN IMMEDIATE for writers (locks the DB
                // for writes immediately, avoiding upgrade-from-shared
                // deadlocks). RO transactions use plain BEGIN.
                let begin_sql = if has_writes { "BEGIN IMMEDIATE" } else { "BEGIN" };
                let begin_res = c.execute_batch(begin_sql);
                if let Err(e) = &begin_res {
                    if is_busy(e) {
                        // Busy on BEGIN — count abort, skip this iteration.
                        // The bench keeps running; the abort is reported in
                        // the note so reviewers see the honest rate.
                        count_aborts += 1;
                        continue;
                    } else {
                        return Err(begin_res.unwrap_err().into());
                    }
                }

                // Run the inner-op block. Each inner op may return
                // SQLITE_BUSY when another worker holds an incompatible
                // lock; treat that as an aborted txn (ROLLBACK + count abort
                // + continue) rather than crashing the bench. This matches
                // sysbench upstream's "ignored / reconnected" report shape.
                let txn_res: rusqlite::Result<u64> = (|| {
                    let mut local_inner = 0u64;
                    if has_reads {
                        let pk = rng.gen_range(0..rows_per_table as i64);
                        let _: rusqlite::Result<Vec<u8>> =
                            p.point.query_row(params![pk], |row| row.get(0));
                        local_inner += 1;
                        for which in 0..4 {
                            let lo = rng.gen_range(
                                0..(rows_per_table.saturating_sub(sysbench::RANGE_WIDTH)) as i64,
                            );
                            let hi = lo + sysbench::RANGE_WIDTH as i64 - 1;
                            let st = match which {
                                0 => &mut p.simple_range,
                                1 => &mut p.sum_range,
                                2 => &mut p.order_range,
                                _ => &mut p.distinct_range,
                            };
                            let mut rows = st.query(params![lo, hi])?;
                            while let Some(_r) = rows.next()? {}
                            local_inner += 1;
                        }
                        for _ in 0..sysbench::POINT_SELECTS {
                            let pk = rng.gen_range(0..rows_per_table as i64);
                            let _: rusqlite::Result<Vec<u8>> =
                                p.point.query_row(params![pk], |row| row.get(0));
                            local_inner += 1;
                        }
                    }
                    if has_writes {
                        let pk = rng.gen_range(0..rows_per_table as i64);
                        p.upd_idx.execute(params![pk])?;
                        local_inner += 1;
                        let pk = rng.gen_range(0..rows_per_table as i64);
                        rng.fill(&mut c_buf[..]);
                        p.upd_nix.execute(params![pk, &c_buf[..]])?;
                        local_inner += 1;
                        let shadow_id: i64 = (rows_per_table as i64)
                            + (tid as i64) * 65_536
                            + ((count_txns % 65_536) as i64);
                        p.del.execute(params![shadow_id])?;
                        local_inner += 1;
                        let k = rng.gen::<i32>();
                        rng.fill(&mut c_buf[..]);
                        rng.fill(&mut pad_buf[..]);
                        p.ins.execute(params![shadow_id, k, &c_buf[..], &pad_buf[..]])?;
                        local_inner += 1;
                    }
                    Ok(local_inner)
                })();

                match txn_res {
                    Ok(li) => {
                        inner_count = li;
                        let commit_res = c.execute_batch("COMMIT");
                        match commit_res {
                            Ok(()) => {
                                lat.push(s.elapsed().as_nanos() as u64);
                                count_txns += 1;
                                count_inner += inner_count;
                            }
                            Err(e) if is_busy(&e) => {
                                let _ = c.execute_batch("ROLLBACK");
                                count_aborts += 1;
                            }
                            Err(e) => return Err(e.into()),
                        }
                    }
                    Err(e) if is_busy(&e) => {
                        let _ = c.execute_batch("ROLLBACK");
                        count_aborts += 1;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Ok((count_txns, count_inner, count_aborts, lat))
        });
        handles.push(h);
    }
    let mut total_txns = 0u64;
    let mut total_inner = 0u64;
    let mut total_aborts = 0u64;
    let mut lat_ns: Vec<u64> = Vec::new();
    for h in handles {
        let (txns, inner, aborts, l) = h.join().expect("sqlite worker panicked")?;
        total_txns += txns;
        total_inner += inner;
        total_aborts += aborts;
        lat_ns.extend(l);
    }
    let elapsed = started.elapsed().as_secs_f64();
    lat_ns.sort_unstable();

    let _ = std::fs::remove_file(&path);

    let abort_pct = if total_txns + total_aborts > 0 {
        (total_aborts as f64) * 100.0 / ((total_txns + total_aborts) as f64)
    } else {
        0.0
    };

    Ok(BenchResult {
        db: "sqlite".into(),
        workload: workload.name().to_string(),
        n,
        trial,
        ops_per_sec: total_txns as f64 / elapsed,
        p50_us: pct_us(&lat_ns, 0.50),
        p99_us: pct_us(&lat_ns, 0.99),
        p99_99_us: pct_us(&lat_ns, 0.9999),
        runtime_secs: elapsed,
        rows: tables * rows_per_table,
        note: Some(format!(
            "sqlite via rusqlite-bundled; journal_mode=MEMORY synchronous=OFF; \
             BEGIN{} / COMMIT brackets each txn; SERIALIZABLE isolation \
             (SQLite's default — single-writer-at-a-time via the rollback journal lock); \
             tables={}, rows/tbl={}; inner-ops/txn ≈ {:.1}; reported ops/sec = \
             committed transactions/sec; aborts (SQLITE_BUSY rolled back) = {} ({:.1}% of attempted)",
            if has_writes { " IMMEDIATE" } else { "" },
            tables,
            rows_per_table,
            if total_txns > 0 {
                total_inner as f64 / total_txns as f64
            } else {
                0.0
            },
            total_aborts,
            abort_pct,
        )),
    })
}
