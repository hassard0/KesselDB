//! Workload descriptors.
//!
//! T1 shipped YCSB-C (100% point reads, uniform random key).
//! T2 added YCSB-A (50/50 read/update) and YCSB-B (95/5 read/update).
//! T3 added sysbench OLTP read-only / write-only / read-write
//!   — the transaction-bracket workload class.
//! T4 (this slice) adds TPC-H Q1 (multi-aggregate GROUP BY) + Q6 (single
//!   SUM with multi-predicate WHERE) — single-table analytical workloads
//!   over `lineitem` at SF=0.01 (~60K rows).
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
    /// TPC-H Q1 — pricing summary report. Multi-aggregate GROUP BY over the
    /// `lineitem` table at the given scale factor (SF). The canonical Q1 is:
    /// ```sql
    /// SELECT l_returnflag, l_linestatus,
    ///        SUM(l_quantity), SUM(l_extendedprice),
    ///        AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount),
    ///        COUNT(*)
    /// FROM lineitem WHERE l_shipdate <= 19980901
    /// GROUP BY l_returnflag, l_linestatus
    /// ORDER BY l_returnflag, l_linestatus;
    /// ```
    /// SF=0.01 -> ~60K rows. Each "op" in the bench is one full Q1 execution
    /// (one ops_per_sec sample = one full query). Reported throughput is
    /// queries/sec, latency = per-query wall time.
    TpchQ1 { sf: f64 },
    /// TPC-H Q6 — forecasting revenue change. Single-table SUM with three
    /// WHERE predicates and no GROUP BY. The canonical Q6 is:
    /// ```sql
    /// SELECT SUM(l_extendedprice * l_discount) AS revenue
    /// FROM lineitem
    /// WHERE l_shipdate >= 19940101 AND l_shipdate < 19950101
    ///   AND l_discount BETWEEN 0.05 AND 0.07
    ///   AND l_quantity < 24;
    /// ```
    /// Same data shape and SF as Q1. Reported throughput is queries/sec.
    TpchQ6 { sf: f64 },
}

pub fn parse_workload(name: &str) -> anyhow::Result<Workload> {
    match name {
        "ycsb-c" => Ok(Workload::YcsbC),
        "ycsb-a" => Ok(Workload::YcsbA),
        "ycsb-b" => Ok(Workload::YcsbB),
        "oltp-read-only" | "oltp-ro" => Ok(Workload::OltpRO),
        "oltp-write-only" | "oltp-wo" => Ok(Workload::OltpWO),
        "oltp-read-write" | "oltp-mix" | "oltp-rw" => Ok(Workload::OltpRW),
        // SF is wired from CLI in main.rs; the parse step picks the default
        // (0.01 = ~60K rows) so an older invocation like `--workload tpch-q1`
        // (no --sf) keeps working.
        "tpch-q1" => Ok(Workload::TpchQ1 { sf: 0.01 }),
        "tpch-q6" => Ok(Workload::TpchQ6 { sf: 0.01 }),
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
            Workload::TpchQ1 { .. } => "tpch-q1",
            Workload::TpchQ6 { .. } => "tpch-q6",
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

    /// True for TPC-H analytical workloads (single full-table aggregate per
    /// measurement).
    pub fn is_tpch(&self) -> bool {
        matches!(self, Workload::TpchQ1 { .. } | Workload::TpchQ6 { .. })
    }

    /// Scale factor for TPC-H workloads (1.0 = full SF=1 ≈ 6M rows; 0.01 =
    /// SF=0.01 ≈ 60K rows). Returns 0.0 for non-TPC-H workloads.
    pub fn tpch_sf(&self) -> f64 {
        match self {
            Workload::TpchQ1 { sf } | Workload::TpchQ6 { sf } => *sf,
            _ => 0.0,
        }
    }

    /// Override the TPC-H scale factor (no-op for non-TPC-H workloads).
    /// Used by the CLI to propagate `--sf` into the workload constructor.
    pub fn with_tpch_sf(self, new_sf: f64) -> Self {
        match self {
            Workload::TpchQ1 { .. } => Workload::TpchQ1 { sf: new_sf },
            Workload::TpchQ6 { .. } => Workload::TpchQ6 { sf: new_sf },
            other => other,
        }
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
            // TPC-H workloads are pure analytical reads — every query
            // is a full-scan + aggregate. The "write_ratio" abstraction
            // does not apply (one query is the bench unit, not one op).
            Workload::TpchQ1 { .. } | Workload::TpchQ6 { .. } => 0.00,
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

/// TPC-H constants for the `lineitem`-only V1 we ship in T4.
///
/// SF=1 in the canonical TPC-H spec yields ~6,001,215 `lineitem` rows; V1
/// targets SF=0.01 (~60,000 rows) so all three measured DBs can complete
/// the analytical query in the bench's 30-second window without paging or
/// disk I/O on vulcan. The data generator (`tpch::gen_lineitem`) uses a
/// deterministic per-trial seed so KesselDB, Postgres, and SQLite all see
/// byte-identical row payloads. The full TPC-H `dbgen` mixes column
/// distributions in ways the spec defines precisely; our generator stays
/// faithful to the row count + the per-column value distributions for
/// `l_shipdate`, `l_discount`, `l_quantity`, `l_returnflag`, and
/// `l_linestatus` (the columns Q1 + Q6 actually touch), and uses
/// `SmallRng` for the rest (e.g. `l_extendedprice`, `l_partkey`) to keep
/// the workload's per-row cost realistic without depending on the full
/// dbgen text dictionary.
pub mod tpch_const {
    /// Reference row count at SF=1. SF=0.01 → 60,012 rows (rounded to
    /// 60,000 in the generator for code clarity).
    pub const ROWS_AT_SF_1: usize = 6_001_215;

    /// Q6 lower-bound shipdate filter (1994-01-01 as YYYYMMDD).
    pub const Q6_SHIPDATE_LO: i32 = 19940101;
    /// Q6 upper-bound shipdate filter — exclusive (1995-01-01 as YYYYMMDD).
    pub const Q6_SHIPDATE_HI: i32 = 19950101;
    /// Q6 discount lower bound (raw scale-2 integer: 0.05 -> 5).
    pub const Q6_DISCOUNT_LO_RAW: i32 = 5;
    /// Q6 discount upper bound, inclusive (raw scale-2 integer: 0.07 -> 7).
    pub const Q6_DISCOUNT_HI_RAW: i32 = 7;
    /// Q6 quantity upper bound, exclusive (raw scale-2 integer: 24.00 -> 2400).
    /// Stored as `l_quantity` raw scaled int; query expressed as < 24
    /// is equivalent to < 2400 raw.
    pub const Q6_QUANTITY_HI_RAW: i32 = 2400;

    /// Q1 shipdate filter upper bound (1998-09-01 as YYYYMMDD).
    pub const Q1_SHIPDATE_HI: i32 = 19980901;

    /// Compute the row count for a given scale factor (rounded to nearest
    /// 1000 for code clarity; canonical SF=0.01 yields ~60,000 rows).
    pub fn rows_for_sf(sf: f64) -> usize {
        let exact = (ROWS_AT_SF_1 as f64) * sf;
        ((exact / 1000.0).round() as usize) * 1000
    }
}
