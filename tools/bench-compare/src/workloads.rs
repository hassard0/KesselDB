//! Workload descriptors.
//!
//! T1 shipped YCSB-C (100% point reads, uniform random key).
//! T2 added YCSB-A (50/50 read/update) and YCSB-B (95/5 read/update).
//! T3 (this slice) adds sysbench OLTP read-only / write-only / read-write
//!   — the transaction-bracket workload class.
//! T4 adds TPC-H Q1/Q6.
//!
//! Each workload is defined SQL-agnostically; the driver translates to the
//! target DB's native operation (KesselDB `Op::*` and `Op::Txn`, Postgres
//! SQL `BEGIN; …; COMMIT;`, SQLite `BEGIN IMMEDIATE; …; COMMIT;`,
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
    /// sysbench OLTP read-only: 10 SELECT-class ops bracketed by BEGIN / COMMIT.
    /// See `SysbenchOp` for the 6 sub-shapes (1 POINT + 1 SIMPLE_RANGE +
    /// 1 SUM_RANGE + 1 ORDER_RANGE + 1 DISTINCT_RANGE + 5 POINT_SELECT).
    OltpRO,
    /// sysbench OLTP write-only: 4 write ops (UPDATE_INDEX, UPDATE_NON_INDEX,
    /// DELETE, INSERT) bracketed by BEGIN / COMMIT. The DELETE id and the
    /// INSERT id are paired so the row count stays constant across the run.
    OltpWO,
    /// sysbench OLTP read-write (the default sysbench OLTP profile):
    /// 10 reads + 4 writes per transaction, same shape as above.
    OltpRW,
}

pub fn parse_workload(name: &str) -> anyhow::Result<Workload> {
    match name {
        "ycsb-c" => Ok(Workload::YcsbC),
        "ycsb-a" => Ok(Workload::YcsbA),
        "ycsb-b" => Ok(Workload::YcsbB),
        "oltp-read-only" | "oltp-ro" => Ok(Workload::OltpRO),
        "oltp-write-only" | "oltp-wo" => Ok(Workload::OltpWO),
        "oltp-read-write" | "oltp-mix" | "oltp-rw" => Ok(Workload::OltpRW),
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
            Workload::OltpRO => "oltp-read-only",
            Workload::OltpWO => "oltp-write-only",
            Workload::OltpRW => "oltp-read-write",
        }
    }

    /// True for YCSB-family workloads (single point-op per measurement).
    pub fn is_ycsb(&self) -> bool {
        matches!(self, Workload::YcsbC | Workload::YcsbA | Workload::YcsbB)
    }

    /// True for sysbench-OLTP-family workloads (multi-op transaction per measurement).
    pub fn is_sysbench(&self) -> bool {
        matches!(self, Workload::OltpRO | Workload::OltpWO | Workload::OltpRW)
    }

    /// Probability (in [0, 1]) that any single op in the steady-state phase
    /// is a write (UPDATE). Reads = 1 - write_ratio. YCSB-family only;
    /// sysbench workloads bracket multi-op transactions and ignore this.
    pub fn write_ratio(&self) -> f64 {
        match self {
            Workload::YcsbC => 0.00,
            Workload::YcsbA => 0.50,
            Workload::YcsbB => 0.05,
            // Not used by sysbench drivers (they enumerate inner ops directly),
            // but provide a sensible value for code paths that ask.
            Workload::OltpRO => 0.00,
            Workload::OltpWO => 1.00,
            Workload::OltpRW => 4.0 / 14.0, // 4 writes / 14 ops per tx
        }
    }

    /// True if this workload performs UPDATE ops (any write_ratio > 0).
    /// Used by drivers to short-circuit setup of write-side machinery.
    pub fn has_writes(&self) -> bool {
        self.write_ratio() > 0.0
    }

    /// Whether the sysbench-OLTP variant runs the 10-read block (POINT +
    /// SIMPLE_RANGE + SUM_RANGE + ORDER_RANGE + DISTINCT_RANGE + 5×POINT_SELECT).
    pub fn sysbench_has_reads(&self) -> bool {
        matches!(self, Workload::OltpRO | Workload::OltpRW)
    }

    /// Whether the sysbench-OLTP variant runs the 4-write block (UPDATE_INDEX
    /// + UPDATE_NON_INDEX + DELETE + INSERT).
    pub fn sysbench_has_writes(&self) -> bool {
        matches!(self, Workload::OltpWO | Workload::OltpRW)
    }
}

/// sysbench OLTP constants (mirror upstream `oltp_common.lua` shape).
pub mod sysbench {
    /// Number of tables in the dataset. Upstream sysbench default is 10.
    pub const TABLE_COUNT: usize = 10;
    /// Range scan width: SELECT … WHERE id BETWEEN ? AND ?+RANGE_WIDTH-1.
    /// Upstream sysbench `--range_size` default is 100.
    pub const RANGE_WIDTH: usize = 100;
    /// Number of extra POINT_SELECT queries per read block (upstream default).
    pub const POINT_SELECTS: usize = 5;
    /// `c` column width (CHAR(120)) — upstream `--table_size` width is 120.
    pub const C_WIDTH: usize = 120;
    /// `pad` column width (CHAR(60)) — upstream `--table_size` width is 60.
    pub const PAD_WIDTH: usize = 60;
}
