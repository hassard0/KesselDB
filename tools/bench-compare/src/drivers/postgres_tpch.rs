//! Postgres TPC-H Q1 + Q6 paths.
//!
//! Schema (mirrors `docs/superpowers/specs/2026-05-28-kesseldb-bench-suite-design.md` §3):
//! ```sql
//! CREATE UNLOGGED TABLE lineitem (
//!   l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber INT,
//!   l_quantity INT, l_extendedprice BIGINT, l_discount INT, l_tax INT,
//!   l_returnflag CHAR(1), l_linestatus CHAR(1),
//!   l_shipdate INT, l_commitdate INT, l_receiptdate INT,
//!   l_shipinstruct BYTEA, l_shipmode BYTEA, l_comment BYTEA
//! );
//! ```
//!
//! The fixed-point numeric columns (`l_quantity` scale-2 raw,
//! `l_extendedprice` scale-2 raw, `l_discount` scale-2 raw, `l_tax`
//! scale-2 raw) are stored as integers so all three DBs hold byte-
//! identical numeric data. The Q1 and Q6 SQL below operates on the raw
//! integer columns and scales constants accordingly (e.g.
//! `l_quantity < 2400` for Q6 instead of `l_quantity < 24.0`). This
//! produces the same SUM as the canonical TPC-H queries up to a constant
//! scale factor, which is what the bench measures (engine throughput,
//! not human-readable revenue figures).

use crate::tpch::{self, LineItem};
use crate::workloads::{tpch_const, Workload};
use crate::{pct_us, BenchResult, Cli};
use postgres::{Client, NoTls};
use std::io::Write;
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

    // --- setup + load ---
    let mut setup = Client::connect(&cli.pg_url, NoTls)?;
    setup.batch_execute("SET synchronous_commit = on;")?;
    setup.batch_execute("DROP TABLE IF EXISTS lineitem;")?;
    setup.batch_execute(
        "CREATE UNLOGGED TABLE lineitem (
            l_orderkey BIGINT NOT NULL,
            l_partkey BIGINT NOT NULL,
            l_suppkey BIGINT NOT NULL,
            l_linenumber INTEGER NOT NULL,
            l_quantity INTEGER NOT NULL,
            l_extendedprice BIGINT NOT NULL,
            l_discount INTEGER NOT NULL,
            l_tax INTEGER NOT NULL,
            l_returnflag CHAR(1) NOT NULL,
            l_linestatus CHAR(1) NOT NULL,
            l_shipdate INTEGER NOT NULL,
            l_commitdate INTEGER NOT NULL,
            l_receiptdate INTEGER NOT NULL,
            l_shipinstruct BYTEA NOT NULL,
            l_shipmode BYTEA NOT NULL,
            l_comment BYTEA NOT NULL
         );",
    )?;

    let seed = super::kesseldb_tpch::tpch_seed(trial);
    let items = tpch::gen_lineitem(rows, seed);

    // Bulk-load via COPY BINARY for speed.
    {
        let mut tx = setup.transaction()?;
        let stmt = "COPY lineitem (l_orderkey, l_partkey, l_suppkey, \
            l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, \
            l_returnflag, l_linestatus, l_shipdate, l_commitdate, \
            l_receiptdate, l_shipinstruct, l_shipmode, l_comment) \
            FROM STDIN (FORMAT BINARY)";
        let mut writer = tx.copy_in(stmt)?;
        // BINARY COPY header.
        writer.write_all(b"PGCOPY\n\xff\r\n\0")?;
        writer.write_all(&0u32.to_be_bytes())?; // flags
        writer.write_all(&0u32.to_be_bytes())?; // extension area len
        for li in &items {
            write_li_row(&mut writer, li)?;
        }
        writer.write_all(&(-1i16).to_be_bytes())?; // trailer
        writer.finish()?;
        tx.commit()?;
    }
    setup.batch_execute("ANALYZE lineitem;")?;
    // CREATE INDEX on l_shipdate — both Q1 and Q6 filter on it.
    setup.batch_execute("CREATE INDEX IF NOT EXISTS lineitem_shipdate ON lineitem (l_shipdate);")?;
    drop(setup);

    // --- steady-state: N workers, each connection runs queries in a loop ---
    let started = Instant::now();
    let stop_at = started + duration;
    let mut handles = Vec::with_capacity(n);
    let is_q1 = matches!(workload, Workload::TpchQ1 { .. });
    for tid in 0..n {
        let url = cli.pg_url.clone();
        let h = std::thread::spawn(move || -> anyhow::Result<(u64, Vec<u64>)> {
            let mut c = Client::connect(&url, NoTls)?;
            let q1_sql = "SELECT l_returnflag, l_linestatus, \
                                 SUM(l_quantity), SUM(l_extendedprice), \
                                 AVG(l_quantity), AVG(l_extendedprice), \
                                 AVG(l_discount), COUNT(*) \
                          FROM lineitem WHERE l_shipdate <= $1 \
                          GROUP BY l_returnflag, l_linestatus \
                          ORDER BY l_returnflag, l_linestatus";
            let q6_sql = "SELECT SUM(l_extendedprice * l_discount) \
                          FROM lineitem \
                          WHERE l_shipdate >= $1 AND l_shipdate < $2 \
                            AND l_discount BETWEEN $3 AND $4 \
                            AND l_quantity < $5";
            let q1_stmt = c.prepare(q1_sql)?;
            let q6_stmt = c.prepare(q6_sql)?;
            let mut count = 0u64;
            let mut lat = Vec::with_capacity(4096);
            let _ = tid;
            loop {
                if Instant::now() >= stop_at { break; }
                let s = Instant::now();
                if is_q1 {
                    let _ = c.query(&q1_stmt, &[&tpch_const::Q1_SHIPDATE_HI])?;
                } else {
                    let _ = c.query(
                        &q6_stmt,
                        &[
                            &tpch_const::Q6_SHIPDATE_LO,
                            &tpch_const::Q6_SHIPDATE_HI,
                            &tpch_const::Q6_DISCOUNT_LO_RAW,
                            &tpch_const::Q6_DISCOUNT_HI_RAW,
                            &tpch_const::Q6_QUANTITY_HI_RAW,
                        ],
                    )?;
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
        let (c, l) = h.join().expect("postgres tpch worker panicked")?;
        total_q += c;
        lat_ns.extend(l);
    }
    let elapsed = started.elapsed().as_secs_f64();
    lat_ns.sort_unstable();

    Ok(BenchResult {
        db: "postgres".into(),
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
                "postgres 16.x; UNLOGGED lineitem; SF={} ({} rows); Q1 = \
                 SELECT l_returnflag, l_linestatus, SUM/AVG aggregates + \
                 COUNT(*) FROM lineitem WHERE l_shipdate <= 19980901 GROUP \
                 BY l_returnflag, l_linestatus ORDER BY ... (numeric \
                 columns stored as scale-2 raw integers — see driver \
                 header). idx on l_shipdate.",
                sf, rows
            )
        } else {
            format!(
                "postgres 16.x; UNLOGGED lineitem; SF={} ({} rows); Q6 = \
                 SELECT SUM(l_extendedprice * l_discount) FROM lineitem \
                 WHERE l_shipdate IN [19940101, 19950101) AND l_discount \
                 BETWEEN 5 AND 7 AND l_quantity < 2400 (scale-2 raw \
                 integer constants).",
                sf, rows
            )
        }),
    })
}

fn write_li_row(writer: &mut postgres::CopyInWriter<'_>, li: &LineItem) -> anyhow::Result<()> {
    // 16 columns total.
    writer.write_all(&16i16.to_be_bytes())?;
    // col 0: l_orderkey BIGINT (8 bytes BE).
    writer.write_all(&8i32.to_be_bytes())?;
    writer.write_all(&li.l_orderkey.to_be_bytes())?;
    // col 1: l_partkey BIGINT.
    writer.write_all(&8i32.to_be_bytes())?;
    writer.write_all(&li.l_partkey.to_be_bytes())?;
    // col 2: l_suppkey BIGINT.
    writer.write_all(&8i32.to_be_bytes())?;
    writer.write_all(&li.l_suppkey.to_be_bytes())?;
    // col 3: l_linenumber INT (4 bytes BE).
    writer.write_all(&4i32.to_be_bytes())?;
    writer.write_all(&li.l_linenumber.to_be_bytes())?;
    // col 4: l_quantity INT.
    writer.write_all(&4i32.to_be_bytes())?;
    writer.write_all(&li.l_quantity_raw.to_be_bytes())?;
    // col 5: l_extendedprice BIGINT.
    writer.write_all(&8i32.to_be_bytes())?;
    writer.write_all(&li.l_extendedprice_raw.to_be_bytes())?;
    // col 6: l_discount INT.
    writer.write_all(&4i32.to_be_bytes())?;
    writer.write_all(&li.l_discount_raw.to_be_bytes())?;
    // col 7: l_tax INT.
    writer.write_all(&4i32.to_be_bytes())?;
    writer.write_all(&li.l_tax_raw.to_be_bytes())?;
    // col 8: l_returnflag CHAR(1) — text representation.
    writer.write_all(&1i32.to_be_bytes())?;
    writer.write_all(&[li.l_returnflag])?;
    // col 9: l_linestatus CHAR(1).
    writer.write_all(&1i32.to_be_bytes())?;
    writer.write_all(&[li.l_linestatus])?;
    // col 10: l_shipdate INT.
    writer.write_all(&4i32.to_be_bytes())?;
    writer.write_all(&li.l_shipdate.to_be_bytes())?;
    // col 11: l_commitdate INT.
    writer.write_all(&4i32.to_be_bytes())?;
    writer.write_all(&li.l_commitdate.to_be_bytes())?;
    // col 12: l_receiptdate INT.
    writer.write_all(&4i32.to_be_bytes())?;
    writer.write_all(&li.l_receiptdate.to_be_bytes())?;
    // col 13: l_shipinstruct BYTEA(25).
    writer.write_all(&(li.l_shipinstruct.len() as i32).to_be_bytes())?;
    writer.write_all(&li.l_shipinstruct)?;
    // col 14: l_shipmode BYTEA(10).
    writer.write_all(&(li.l_shipmode.len() as i32).to_be_bytes())?;
    writer.write_all(&li.l_shipmode)?;
    // col 15: l_comment BYTEA(44).
    writer.write_all(&(li.l_comment.len() as i32).to_be_bytes())?;
    writer.write_all(&li.l_comment)?;
    Ok(())
}
