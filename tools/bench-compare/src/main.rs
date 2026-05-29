//! bench-compare: cross-DB benchmark harness.
//!
//! See `docs/superpowers/specs/2026-05-28-kesseldb-bench-suite-design.md`
//! for the design + honest-reporting commitments.
//!
//! Usage:
//!   bench-compare \
//!     --db kesseldb,postgres,sqlite,tigerbeetle \
//!     --workload ycsb-c \
//!     --connections 1,8,16 \
//!     --duration 10 \
//!     --rows 100000 \
//!     --output /tmp/bench-results.json
//!
//! Output: one JSON line per (db, workload, connections, trial). The
//! BENCHMARKS.md generator (T5) reads these JSON lines.

#![forbid(unsafe_code)]

mod drivers;
mod workloads;

use clap::Parser;
use serde::Serialize;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(version, about = "KesselDB cross-DB benchmark harness")]
struct Cli {
    /// Comma-separated DB list: kesseldb, postgres, sqlite, tigerbeetle
    #[arg(long, default_value = "kesseldb,postgres,sqlite")]
    db: String,

    /// Workload to run (T1 ships ycsb-c only; T2-T4 add the rest).
    #[arg(long, default_value = "ycsb-c")]
    workload: String,

    /// Comma-separated concurrency levels.
    #[arg(long, default_value = "1,8,16")]
    connections: String,

    /// Duration of the steady-state phase in seconds.
    #[arg(long, default_value_t = 10)]
    duration: u64,

    /// Rows to load before the steady-state phase.
    #[arg(long, default_value_t = 100_000)]
    rows: usize,

    /// Trials per (db, workload, connections) — median reported.
    #[arg(long, default_value_t = 3)]
    trials: u32,

    /// JSON output file (newline-delimited).
    #[arg(long, default_value = "/tmp/bench-results.json")]
    output: PathBuf,

    /// Postgres connection URL.
    #[arg(long, default_value = "host=127.0.0.1 port=5533 user=bench password=admin dbname=bench")]
    pg_url: String,

    /// SQLite database file path.
    #[arg(long, default_value = "/tmp/bench-compare.sqlite")]
    sqlite_path: PathBuf,

    /// TigerBeetle cluster address (T1 stub; real wiring in T2).
    #[arg(long, default_value = "127.0.0.1:3001")]
    tb_address: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct BenchResult {
    pub db: String,
    pub workload: String,
    #[serde(rename = "N")]
    pub n: usize,
    pub trial: u32,
    pub ops_per_sec: f64,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p99_99_us: u64,
    pub runtime_secs: f64,
    pub rows: usize,
    /// Optional honest note (e.g. "stub — see T2", "fsync=off").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

fn parse_csv_usize(s: &str) -> Vec<usize> {
    s.split(',').filter_map(|x| x.trim().parse().ok()).collect()
}

fn parse_csv_string(s: &str) -> Vec<String> {
    s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect()
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let dbs = parse_csv_string(&cli.db);
    let connections = parse_csv_usize(&cli.connections);
    if connections.is_empty() {
        anyhow::bail!("--connections must contain at least one integer");
    }
    if dbs.is_empty() {
        anyhow::bail!("--db must contain at least one driver name");
    }

    eprintln!(
        "bench-compare: workload={} rows={} duration={}s trials={} dbs={:?} N={:?}",
        cli.workload, cli.rows, cli.duration, cli.trials, dbs, connections
    );

    let out_file = File::create(&cli.output)?;
    let mut out = BufWriter::new(out_file);

    let workload = workloads::parse_workload(&cli.workload)?;

    let started = Instant::now();
    let mut total_runs = 0usize;
    for db_name in &dbs {
        for &n in &connections {
            for trial in 1..=cli.trials {
                eprintln!(
                    "[{:>3.0}s] db={} workload={} N={} trial={}/{}",
                    started.elapsed().as_secs_f64(),
                    db_name,
                    cli.workload,
                    n,
                    trial,
                    cli.trials
                );
                let res = run_one(db_name, &workload, n, trial, &cli)?;
                let line = serde_json::to_string(&res)?;
                writeln!(out, "{line}")?;
                out.flush()?;
                println!("{line}");
                total_runs += 1;
            }
        }
    }
    eprintln!(
        "bench-compare: done. {} runs in {:.1}s. wrote {}",
        total_runs,
        started.elapsed().as_secs_f64(),
        cli.output.display()
    );
    Ok(())
}

fn run_one(
    db_name: &str,
    workload: &workloads::Workload,
    n: usize,
    trial: u32,
    cli: &Cli,
) -> anyhow::Result<BenchResult> {
    match db_name {
        "kesseldb" => drivers::kesseldb::run(workload, n, trial, cli),
        "postgres" => drivers::postgres::run(workload, n, trial, cli),
        "sqlite" => drivers::sqlite::run(workload, n, trial, cli),
        "tigerbeetle" => drivers::tigerbeetle::run(workload, n, trial, cli),
        other => anyhow::bail!("unknown --db driver: {other}"),
    }
}

/// Compute percentile in microseconds from a sorted ns vector.
pub fn pct_us(sorted_ns: &[u64], p: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let idx = ((sorted_ns.len() as f64 - 1.0) * p).round() as usize;
    sorted_ns[idx] / 1000
}
