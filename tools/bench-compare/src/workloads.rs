//! Workload descriptors. T1 ships YCSB-C only (100% point reads, uniform random).
//! T2 adds YCSB-A and YCSB-B; T3 adds sysbench OLTP; T4 adds TPC-H Q1/Q6.

use anyhow::bail;

#[derive(Clone, Debug)]
pub enum Workload {
    /// YCSB-C: 100% read, uniform random key over a primary-key keyspace.
    YcsbC,
}

pub fn parse_workload(name: &str) -> anyhow::Result<Workload> {
    match name {
        "ycsb-c" => Ok(Workload::YcsbC),
        // T2..T4 placeholders for forward compatibility:
        "ycsb-a" | "ycsb-b" => bail!("workload {name} ships in T2"),
        "oltp-ro" | "oltp-wo" | "oltp-mix" => bail!("workload {name} ships in T3"),
        "tpch-q1" | "tpch-q6" => bail!("workload {name} ships in T4"),
        other => bail!("unknown --workload {other}"),
    }
}

impl Workload {
    pub fn name(&self) -> &'static str {
        match self {
            Workload::YcsbC => "ycsb-c",
        }
    }
}
