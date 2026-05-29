//! Postgres driver via the `postgres` crate (sync), one connection per worker
//! thread. T1 shipped YCSB-C; T2 adds YCSB-A (50/50) and YCSB-B (95/5).
//! T3 adds the sysbench OLTP RO / WO / RW transactions (BEGIN / COMMIT
//! bracketed via the rust-postgres `Client::transaction()` API).
//!
//! Configuration:
//! - `synchronous_commit = on` (default) → matches KesselDB's
//!   AutosyncMode::EveryCommit durability promise.
//! - YCSB tables are UNLOGGED for symmetry with KesselDB MemVfs / SQLite
//!   journal_mode=MEMORY (the "in-memory engine" parity tier).
//! - sysbench OLTP tables are also UNLOGGED for the same reason.
//! - Connection: sync `postgres::Client`, one per worker thread.
//! - Isolation: READ COMMITTED (Postgres 16 default; documented in
//!   BENCHMARKS.md §3c table footnotes).

use crate::workloads::{sysbench, Workload};
use crate::{pct_us, BenchResult, Cli};
use postgres::{Client, NoTls};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::time::{Duration, Instant};

pub fn run(
    workload: &Workload,
    n: usize,
    trial: u32,
    cli: &Cli,
) -> anyhow::Result<BenchResult> {
    if workload.is_sysbench() {
        run_sysbench_oltp(*workload, n, trial, cli)
    } else {
        run_ycsb_mixed(*workload, n, trial, cli)
    }
}

fn schema_sql() -> &'static str {
    // YCSB schema: id PK + 10 random 100-byte fields. We use a single
    // bytea payload column for the field bundle — point-read throughput
    // is the measured property, not column-projection cost. UPDATE replaces
    // the whole payload, matching the KesselDB Op::Update semantics.
    "CREATE UNLOGGED TABLE IF NOT EXISTS ycsb (
        id BIGINT PRIMARY KEY,
        payload BYTEA NOT NULL
     )"
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
        let h = std::thread::spawn(move || -> anyhow::Result<(u64, u64, Vec<u64>)> {
            let mut c = Client::connect(&url, NoTls)?;
            let sel = c.prepare("SELECT payload FROM ycsb WHERE id = $1")?;
            let upd = c.prepare("UPDATE ycsb SET payload = $2 WHERE id = $1")?;
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
                    payload[..8].copy_from_slice(&key.to_be_bytes());
                    let s = Instant::now();
                    let n_rows = c.execute(&upd, &[&key, &&payload[..]])?;
                    lat.push(s.elapsed().as_nanos() as u64);
                    debug_assert!(n_rows == 1);
                    count_writes += 1;
                } else {
                    let s = Instant::now();
                    let rows_back = c.query(&sel, &[&key])?;
                    lat.push(s.elapsed().as_nanos() as u64);
                    debug_assert!(!rows_back.is_empty());
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
        let (ops, w, l) = h.join().expect("postgres worker panicked")?;
        total_ops += ops;
        total_writes += w;
        lat_ns.extend(l);
    }
    let elapsed = started.elapsed().as_secs_f64();
    lat_ns.sort_unstable();

    let actual_wr = if total_ops > 0 {
        total_writes as f64 / total_ops as f64
    } else {
        0.0
    };

    Ok(BenchResult {
        db: "postgres".into(),
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
            "postgres 16.x; UNLOGGED table; synchronous_commit=on; loopback TCP via postgres crate; \
             target write_ratio={:.2} actual={:.3}",
            write_ratio, actual_wr
        )),
    })
}

// Bring the trait `Write` into scope for `writer.write_all(...)`.
use std::io::Write;

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
    if tables == 0 || rows_per_table == 0 {
        anyhow::bail!("postgres sysbench: --tables and --rows-per-table must be >0");
    }

    // --- setup: drop + recreate + load ---
    let mut setup = Client::connect(&cli.pg_url, NoTls)?;
    setup.batch_execute("SET synchronous_commit = on;")?;
    let mut drops = String::new();
    let mut creates = String::new();
    for t in 1..=tables {
        drops.push_str(&format!("DROP TABLE IF EXISTS sbtest{t};"));
        creates.push_str(&format!(
            "CREATE UNLOGGED TABLE sbtest{t} (
                id BIGINT PRIMARY KEY,
                k INTEGER NOT NULL DEFAULT 0,
                c CHAR({c}) NOT NULL DEFAULT '',
                pad CHAR({p}) NOT NULL DEFAULT ''
             );
             CREATE INDEX IF NOT EXISTS sbtest{t}_k ON sbtest{t} (k);",
            c = sysbench::C_WIDTH,
            p = sysbench::PAD_WIDTH,
        ));
    }
    setup.batch_execute(&drops)?;
    setup.batch_execute(&creates)?;

    let mut rng = SmallRng::seed_from_u64(0xA5A5_5A5A ^ trial as u64);
    // Bulk-load each table via COPY BINARY (we are NOT measuring load throughput).
    for t in 1..=tables {
        let mut tx = setup.transaction()?;
        let stmt = format!("COPY sbtest{t} (id, k, c, pad) FROM STDIN (FORMAT BINARY)");
        let mut writer = tx.copy_in(&stmt)?;
        // BINARY COPY header.
        writer.write_all(b"PGCOPY\n\xff\r\n\0")?;
        writer.write_all(&0u32.to_be_bytes())?; // flags
        writer.write_all(&0u32.to_be_bytes())?; // extension area len
        let mut c_buf = vec![0u8; sysbench::C_WIDTH];
        let mut pad_buf = vec![0u8; sysbench::PAD_WIDTH];
        for i in 0..rows_per_table {
            rng.fill(&mut c_buf[..]);
            rng.fill(&mut pad_buf[..]);
            // Replace any 0-bytes with 'a' so CHAR text columns don't choke.
            for b in c_buf.iter_mut() {
                if *b == 0 {
                    *b = b'a';
                }
            }
            for b in pad_buf.iter_mut() {
                if *b == 0 {
                    *b = b'a';
                }
            }
            let k = rng.gen::<i32>();
            // Tuple: u16 ncols=4.
            writer.write_all(&4i16.to_be_bytes())?;
            // col 0: id BIGINT.
            writer.write_all(&8i32.to_be_bytes())?;
            writer.write_all(&(i as i64).to_be_bytes())?;
            // col 1: k INT4.
            writer.write_all(&4i32.to_be_bytes())?;
            writer.write_all(&k.to_be_bytes())?;
            // col 2: c CHAR(120) — text-as-bytea over binary COPY: pass
            // as the CHAR text; Postgres CHAR binary format is the raw
            // bytes of the text.
            writer.write_all(&(c_buf.len() as i32).to_be_bytes())?;
            writer.write_all(&c_buf)?;
            // col 3: pad CHAR(60).
            writer.write_all(&(pad_buf.len() as i32).to_be_bytes())?;
            writer.write_all(&pad_buf)?;
        }
        // Trailer.
        writer.write_all(&(-1i16).to_be_bytes())?;
        writer.finish()?;
        tx.commit()?;
    }
    for t in 1..=tables {
        setup.batch_execute(&format!("ANALYZE sbtest{t};"))?;
    }
    drop(setup);

    // --- steady-state: N worker threads ---
    let started = Instant::now();
    let stop_at = started + duration;
    let mut handles = Vec::with_capacity(n);
    let has_reads = workload.sysbench_has_reads();
    let has_writes = workload.sysbench_has_writes();
    for tid in 0..n {
        let url = cli.pg_url.clone();
        let h = std::thread::spawn(move || -> anyhow::Result<(u64, u64, Vec<u64>)> {
            let mut c = Client::connect(&url, NoTls)?;
            // Prepare per-table statements once.
            #[allow(clippy::type_complexity)]
            let mut prepared: Vec<(
                postgres::Statement,
                postgres::Statement,
                postgres::Statement,
                postgres::Statement,
                postgres::Statement,
                postgres::Statement,
                postgres::Statement,
                postgres::Statement,
            )> = Vec::with_capacity(tables);
            for t in 1..=tables {
                let point = c.prepare(&format!("SELECT c FROM sbtest{t} WHERE id = $1"))?;
                let simple_range = c.prepare(&format!(
                    "SELECT c FROM sbtest{t} WHERE id BETWEEN $1 AND $2"
                ))?;
                let sum_range = c.prepare(&format!(
                    "SELECT SUM(k) FROM sbtest{t} WHERE id BETWEEN $1 AND $2"
                ))?;
                let order_range = c.prepare(&format!(
                    "SELECT c FROM sbtest{t} WHERE id BETWEEN $1 AND $2 ORDER BY c"
                ))?;
                let distinct_range = c.prepare(&format!(
                    "SELECT DISTINCT c FROM sbtest{t} WHERE id BETWEEN $1 AND $2 ORDER BY c"
                ))?;
                let upd_idx = c.prepare(&format!("UPDATE sbtest{t} SET k = k + 1 WHERE id = $1"))?;
                let upd_nix = c.prepare(&format!("UPDATE sbtest{t} SET c = $2 WHERE id = $1"))?;
                let del = c.prepare(&format!("DELETE FROM sbtest{t} WHERE id = $1"))?;
                let _ins_will_be_inlined = (); // INSERT prepared below
                prepared.push((point, simple_range, sum_range, order_range, distinct_range, upd_idx, upd_nix, del));
            }
            // INSERT statements per table (4-column shape).
            let mut inserts: Vec<postgres::Statement> = Vec::with_capacity(tables);
            for t in 1..=tables {
                inserts.push(c.prepare(&format!(
                    "INSERT INTO sbtest{t} (id, k, c, pad) VALUES ($1, $2, $3, $4)"
                ))?);
            }

            let mut rng = SmallRng::seed_from_u64(0xDEAD_BEEF ^ (tid as u64) ^ (trial as u64));
            let mut count_txns = 0u64;
            let mut count_inner = 0u64;
            let mut lat = Vec::with_capacity(1 << 16);
            let mut c_buf = vec![0u8; sysbench::C_WIDTH];
            let mut pad_buf = vec![0u8; sysbench::PAD_WIDTH];
            loop {
                if Instant::now() >= stop_at {
                    break;
                }
                let table_idx = rng.gen_range(0..tables);
                let stmts = &prepared[table_idx];
                let ins_stmt = &inserts[table_idx];
                let s = Instant::now();
                let mut tx = c.transaction()?;
                let mut inner_count = 0u64;

                if has_reads {
                    // 1× POINT
                    let pk = rng.gen_range(0..rows_per_table as i64);
                    let _ = tx.query(&stmts.0, &[&pk])?;
                    inner_count += 1;
                    // 4× *_RANGE
                    for which in 0..4 {
                        let lo = rng.gen_range(
                            0..(rows_per_table.saturating_sub(sysbench::RANGE_WIDTH)) as i64,
                        );
                        let hi = lo + sysbench::RANGE_WIDTH as i64 - 1;
                        let stmt = match which {
                            0 => &stmts.1, // SIMPLE_RANGE
                            1 => &stmts.2, // SUM_RANGE
                            2 => &stmts.3, // ORDER_RANGE
                            _ => &stmts.4, // DISTINCT_RANGE
                        };
                        let _ = tx.query(stmt, &[&lo, &hi])?;
                        inner_count += 1;
                    }
                    // 5× POINT_SELECT
                    for _ in 0..sysbench::POINT_SELECTS {
                        let pk = rng.gen_range(0..rows_per_table as i64);
                        let _ = tx.query(&stmts.0, &[&pk])?;
                        inner_count += 1;
                    }
                }

                if has_writes {
                    // (a) UPDATE_INDEX
                    let pk = rng.gen_range(0..rows_per_table as i64);
                    let _ = tx.execute(&stmts.5, &[&pk])?;
                    inner_count += 1;
                    // (b) UPDATE_NON_INDEX
                    let pk = rng.gen_range(0..rows_per_table as i64);
                    rng.fill(&mut c_buf[..]);
                    for b in c_buf.iter_mut() {
                        if *b == 0 {
                            *b = b'a';
                        }
                    }
                    let _ = tx.execute(&stmts.6, &[&pk, &&c_buf[..]])?;
                    inner_count += 1;
                    // (c) DELETE + (d) INSERT — paired so dataset size is invariant
                    let shadow_id: i64 = (rows_per_table as i64)
                        + (tid as i64) * 65_536
                        + ((count_txns % 65_536) as i64);
                    let _ = tx.execute(&stmts.7, &[&shadow_id])?;
                    inner_count += 1;
                    let k = rng.gen::<i32>();
                    rng.fill(&mut c_buf[..]);
                    rng.fill(&mut pad_buf[..]);
                    for b in c_buf.iter_mut() {
                        if *b == 0 {
                            *b = b'a';
                        }
                    }
                    for b in pad_buf.iter_mut() {
                        if *b == 0 {
                            *b = b'a';
                        }
                    }
                    let _ = tx.execute(
                        ins_stmt,
                        &[&shadow_id, &k, &&c_buf[..], &&pad_buf[..]],
                    )?;
                    inner_count += 1;
                }

                tx.commit()?;
                lat.push(s.elapsed().as_nanos() as u64);
                count_txns += 1;
                count_inner += inner_count;
            }
            Ok((count_txns, count_inner, lat))
        });
        handles.push(h);
    }

    let mut total_txns = 0u64;
    let mut total_inner = 0u64;
    let mut lat_ns: Vec<u64> = Vec::new();
    for h in handles {
        let (txns, inner, l) = h.join().expect("postgres worker panicked")?;
        total_txns += txns;
        total_inner += inner;
        lat_ns.extend(l);
    }
    let elapsed = started.elapsed().as_secs_f64();
    lat_ns.sort_unstable();

    Ok(BenchResult {
        db: "postgres".into(),
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
            "postgres 16.x; UNLOGGED tables; synchronous_commit=on; \
             BEGIN/COMMIT via Client::transaction(); READ COMMITTED isolation; \
             tables={}, rows/tbl={}; inner-ops/txn ≈ {:.1}; reported ops/sec = transactions/sec",
            tables,
            rows_per_table,
            if total_txns > 0 {
                total_inner as f64 / total_txns as f64
            } else {
                0.0
            },
        )),
    })
}
