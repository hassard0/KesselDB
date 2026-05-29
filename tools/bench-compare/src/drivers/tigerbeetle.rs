//! TigerBeetle driver — T1 stub.
//!
//! T1 ships the install (~/bench/bin/tigerbeetle 0.17.4 verified on vulcan)
//! and the harness routing, but NOT the workload translation. TigerBeetle's
//! API is account/transfer-shaped (lookup_accounts / create_accounts /
//! create_transfers); there is no generic key→value point read.
//!
//! Honest mapping for YCSB-C:
//!   each YCSB row → one TigerBeetle Account (id = row id, ledger = 1,
//!     code = 1, flags = 0, debits/credits = 0, user_data fields hold the
//!     row payload's first 16 bytes).
//!   each YCSB read → lookup_accounts([id]) RPC.
//!
//! This translation is documented in the BENCHMARKS.md "TigerBeetle
//! caveats" section. It lands in T2 alongside the YCSB-A/B work, where
//! we'll also document the YCSB-A/B (50%/95% writes via UPDATE) shape
//! that does NOT map cleanly to TigerBeetle's append-only ledger model.
//!
//! T1 stub behaviour: return a BenchResult with ops_per_sec=0 and a `note`
//! flagging the deferred-to-T2 status. The output JSON still records the
//! row so post-processing scripts see "tigerbeetle was tried, was honestly
//! reported as unsupported, here's why".

use crate::workloads::Workload;
use crate::{BenchResult, Cli};

pub fn run(
    workload: &Workload,
    n: usize,
    trial: u32,
    _cli: &Cli,
) -> anyhow::Result<BenchResult> {
    Ok(BenchResult {
        db: "tigerbeetle".into(),
        workload: workload.name().to_string(),
        n,
        trial,
        ops_per_sec: 0.0,
        p50_us: 0,
        p99_us: 0,
        p99_99_us: 0,
        runtime_secs: 0.0,
        rows: 0,
        note: Some(
            "T1 stub: TigerBeetle 0.17.4 installed on vulcan but workload \
             translation (Account/lookup_accounts mapping for YCSB-C) lands \
             in T2 — see docs/superpowers/specs/2026-05-28-kesseldb-bench-suite-design.md §3"
                .into(),
        ),
    })
}
