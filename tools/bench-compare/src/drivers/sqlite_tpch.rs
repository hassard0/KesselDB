//! SQLite TPC-H Q1 + Q6 paths.
//!
//! Schema parity with Postgres: scale-2 raw integer columns + INT
//! shipdates. SQLite has dynamic typing — INTEGER columns hold the same
//! raw values the Postgres BIGINT/INT columns hold, byte-identical
//! across drivers via the shared `tpch::gen_lineitem` generator.

use crate::tpch::{self, LineItem};
use crate::workloads::{tpch_const, Workload};
use crate::{pct_us, BenchResult, Cli};
use rusqlite::{params, Connection, OpenFlags};
use std::time::{Duration, Instant};

pub fn run_tpch(
    workload: Workload,
    n: usize,
    trial: u32,
    cli: &Cli,
) -> anyhow::Result<BenchResult> {
    let sf = workload.tpch_sf();
    let rows = tpch_const::rows_for_sf(sf).max(1000);
    let duration = Duration::from_secs(cli.duration);

    let path = cli.sqlite_path.clone();
    let _ = std::fs::remove_file(&path);

    let setup = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
    )?;
    setup.execute_batch(
        "PRAGMA journal_mode = MEMORY;
         PRAGMA synchronous = OFF;
         PRAGMA temp_store = MEMORY;
         PRAGMA cache_size = -262144;",
    )?;
    setup.execute_batch(
        "CREATE TABLE IF NOT EXISTS lineitem (
            l_orderkey INTEGER NOT NULL,
            l_partkey INTEGER NOT NULL,
            l_suppkey INTEGER NOT NULL,
            l_linenumber INTEGER NOT NULL,
            l_quantity INTEGER NOT NULL,
            l_extendedprice INTEGER NOT NULL,
            l_discount INTEGER NOT NULL,
            l_tax INTEGER NOT NULL,
            l_returnflag TEXT NOT NULL,
            l_linestatus TEXT NOT NULL,
            l_shipdate INTEGER NOT NULL,
            l_commitdate INTEGER NOT NULL,
            l_receiptdate INTEGER NOT NULL,
            l_shipinstruct BLOB NOT NULL,
            l_shipmode BLOB NOT NULL,
            l_comment BLOB NOT NULL
         );
         CREATE INDEX IF NOT EXISTS lineitem_shipdate ON lineitem(l_shipdate);",
    )?;

    // Bulk load in one transaction.
    let seed = super::kesseldb_tpch::tpch_seed(trial);
    let items = tpch::gen_lineitem(rows, seed);
    setup.execute("BEGIN", [])?;
    {
        let mut stmt = setup.prepare(
            "INSERT INTO lineitem (l_orderkey, l_partkey, l_suppkey, l_linenumber, \
             l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, \
             l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
        )?;
        for li in &items {
            insert_li(&mut stmt, li)?;
        }
    }
    setup.execute("COMMIT", [])?;
    setup.execute_batch("ANALYZE")?;
    drop(setup);

    // Steady-state workers — SQLite read-only opens for analytics.
    let started = Instant::now();
    let stop_at = started + duration;
    let mut handles = Vec::with_capacity(n);
    let is_q1 = matches!(workload, Workload::TpchQ1 { .. });
    for tid in 0..n {
        let path = path.clone();
        let h = std::thread::spawn(move || -> anyhow::Result<(u64, Vec<u64>)> {
            let c = Connection::open_with_flags(
                &path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?;
            c.execute_batch(
                "PRAGMA journal_mode = MEMORY;
                 PRAGMA synchronous = OFF;
                 PRAGMA temp_store = MEMORY;
                 PRAGMA cache_size = -262144;",
            )?;
            let q1_sql = "SELECT l_returnflag, l_linestatus, \
                                 SUM(l_quantity), SUM(l_extendedprice), \
                                 AVG(l_quantity), AVG(l_extendedprice), \
                                 AVG(l_discount), COUNT(*) \
                          FROM lineitem WHERE l_shipdate <= ?1 \
                          GROUP BY l_returnflag, l_linestatus \
                          ORDER BY l_returnflag, l_linestatus";
            let q6_sql = "SELECT SUM(l_extendedprice * l_discount) \
                          FROM lineitem \
                          WHERE l_shipdate >= ?1 AND l_shipdate < ?2 \
                            AND l_discount BETWEEN ?3 AND ?4 \
                            AND l_quantity < ?5";
            let mut q1_stmt = c.prepare(q1_sql)?;
            let mut q6_stmt = c.prepare(q6_sql)?;
            let mut count = 0u64;
            let mut lat = Vec::with_capacity(4096);
            let _ = tid;
            loop {
                if Instant::now() >= stop_at { break; }
                let s = Instant::now();
                if is_q1 {
                    let mut rows = q1_stmt.query(params![tpch_const::Q1_SHIPDATE_HI])?;
                    while let Some(_r) = rows.next()? {}
                } else {
                    let mut rows = q6_stmt.query(params![
                        tpch_const::Q6_SHIPDATE_LO,
                        tpch_const::Q6_SHIPDATE_HI,
                        tpch_const::Q6_DISCOUNT_LO_RAW,
                        tpch_const::Q6_DISCOUNT_HI_RAW,
                        tpch_const::Q6_QUANTITY_HI_RAW,
                    ])?;
                    while let Some(_r) = rows.next()? {}
                }
                lat.push(s.elapsed().as_nanos() as u64);
                count += 1;
            }
            Ok((count, lat))
        });
        handles.push(h);
    }
    let mut total_q = 0u64;
    let mut lat_ns: Vec<u64> = Vec::new();
    for h in handles {
        let (c, l) = h.join().expect("sqlite tpch worker panicked")?;
        total_q += c;
        lat_ns.extend(l);
    }
    let elapsed = started.elapsed().as_secs_f64();
    lat_ns.sort_unstable();
    let _ = std::fs::remove_file(&path);

    Ok(BenchResult {
        db: "sqlite".into(),
        workload: workload.name().to_string(),
        n,
        trial,
        ops_per_sec: total_q as f64 / elapsed,
        p50_us: pct_us(&lat_ns, 0.50),
        p99_us: pct_us(&lat_ns, 0.99),
        p99_99_us: pct_us(&lat_ns, 0.9999),
        runtime_secs: elapsed,
        rows,
        note: Some(if is_q1 {
            format!(
                "sqlite via rusqlite-bundled; journal_mode=MEMORY \
                 synchronous=OFF; SF={} ({} rows); Q1 = SELECT \
                 l_returnflag, l_linestatus, SUM/AVG aggregates + COUNT(*) \
                 FROM lineitem WHERE l_shipdate <= 19980901 GROUP BY \
                 l_returnflag, l_linestatus ORDER BY ... (numeric columns \
                 as scale-2 raw integers — see driver header). idx on \
                 l_shipdate.",
                sf, rows
            )
        } else {
            format!(
                "sqlite via rusqlite-bundled; journal_mode=MEMORY \
                 synchronous=OFF; SF={} ({} rows); Q6 = SELECT \
                 SUM(l_extendedprice * l_discount) FROM lineitem WHERE \
                 l_shipdate IN [19940101, 19950101) AND l_discount BETWEEN \
                 5 AND 7 AND l_quantity < 2400 (scale-2 raw integer \
                 constants).",
                sf, rows
            )
        }),
    })
}

fn insert_li(stmt: &mut rusqlite::Statement<'_>, li: &LineItem) -> rusqlite::Result<()> {
    stmt.execute(params![
        li.l_orderkey,
        li.l_partkey,
        li.l_suppkey,
        li.l_linenumber,
        li.l_quantity_raw,
        li.l_extendedprice_raw,
        li.l_discount_raw,
        li.l_tax_raw,
        std::str::from_utf8(&[li.l_returnflag]).unwrap_or("?").to_string(),
        std::str::from_utf8(&[li.l_linestatus]).unwrap_or("?").to_string(),
        li.l_shipdate,
        li.l_commitdate,
        li.l_receiptdate,
        &li.l_shipinstruct[..],
        &li.l_shipmode[..],
        &li.l_comment[..],
    ])?;
    Ok(())
}
