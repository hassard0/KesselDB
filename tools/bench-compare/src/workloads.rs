//! Workload descriptors.
//!
//! T1 shipped YCSB-C (100% point reads, uniform random key).
//! T2 (this slice) adds YCSB-A (50/50 read/update) and YCSB-B (95/5 read/update).
//! T3 adds sysbench OLTP; T4 adds TPC-H Q1/Q6.
//!
//! Each workload is defined SQL-agnostically; the driver translates to the
//! target DB's native operation (KesselDB `Op::*`, Postgres SQL, SQLite SQL,
//! TigerBeetle account/transfer ops). See the design spec §3.

use anyhow::bail;

#[derive(Clone, Copy, Debug)]
pub enum Workload {
    /// YCSB-C: 100% read, uniform random key over a primary-key keyspace.
    YcsbC,
    /// YCSB-A: 50% read / 50% update, uniform random key.
    ///
    /// Canonical YCSB-A uses a Zipfian key distribution; we ship uniform in
    /// V1 for parity with YCSB-C and to keep the comparison apples-to-apples
    /// across DBs (Zipf hits page cache asymmetrically and would mask the
    /// engine-level differences we are trying to measure). A future T7 may
    /// add a zipf flag; the workload definition records this honestly.
    YcsbA,
    /// YCSB-B: 95% read / 5% update, uniform random key.
    YcsbB,
}

pub fn parse_workload(name: &str) -> anyhow::Result<Workload> {
    match name {
        "ycsb-c" => Ok(Workload::YcsbC),
        "ycsb-a" => Ok(Workload::YcsbA),
        "ycsb-b" => Ok(Workload::YcsbB),
        // T3..T4 placeholders for forward compatibility:
        "oltp-ro" | "oltp-wo" | "oltp-mix" => bail!("workload {name} ships in T3"),
        "tpch-q1" | "tpch-q6" => bail!("workload {name} ships in T4"),
        other => bail!("unknown --workload {other}"),
    }
}

impl Workload {
    pub fn name(&self) -> &'static str {
        match self {
            Workload::YcsbC => "ycsb-c",
            Workload::YcsbA => "ycsb-a",
            Workload::YcsbB => "ycsb-b",
        }
    }

    /// Probability (in [0, 1]) that any single op in the steady-state phase
    /// is a write (UPDATE). Reads = 1 - write_ratio.
    pub fn write_ratio(&self) -> f64 {
        match self {
            Workload::YcsbC => 0.00,
            Workload::YcsbA => 0.50,
            Workload::YcsbB => 0.05,
        }
    }

    /// True if this workload performs UPDATE ops (any write_ratio > 0).
    /// Used by drivers to short-circuit setup of write-side machinery.
    pub fn has_writes(&self) -> bool {
        self.write_ratio() > 0.0
    }
}
