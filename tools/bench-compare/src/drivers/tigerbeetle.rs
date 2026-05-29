//! TigerBeetle driver — honest-stub edition.
//!
//! TigerBeetle's API is account/transfer-shaped (`lookup_accounts`,
//! `create_accounts`, `create_transfers`); it has no generic key→value
//! point read and no row-shape update primitive.
//!
//! ### YCSB workload mappings — honest assessment
//!
//! - **YCSB-C (100% read)** — *could* be mapped: each YCSB row becomes one
//!   TigerBeetle `Account` (id = row id, ledger = 1, code = 1,
//!   debits_pending/posted/credits = 0, user_data_128 holds the row payload's
//!   first 16 bytes). Each YCSB read becomes `lookup_accounts([id])`. The
//!   asymmetry footnote: TB Accounts are 128-byte fixed records with a
//!   16-byte user-data slot — *not* the 1-KiB YCSB row our other drivers
//!   carry. The number measures TB's lookup_accounts throughput, NOT
//!   "TB serving a 1-KiB row workload."
//!
//! - **YCSB-A (50% read / 50% update)** — *cannot be honestly mapped.*
//!   TigerBeetle Accounts are append-only after creation. There is no
//!   `update_account` op. The closest analog (`create_transfers` between two
//!   fixed accounts) measures double-entry transfer throughput, which is
//!   NOT a row-update workload. We refuse to translate.
//!
//! - **YCSB-B (95% read / 5% update)** — same as YCSB-A: writes don't map.
//!   We refuse to translate.
//!
//! ### What this driver does
//!
//! T2 ships the honest-stub form: emit a BenchResult with `ops_per_sec=0`
//! and a `note` documenting why. For YCSB-A/B the note says "TB has no
//! row-update primitive". For YCSB-C the note says "lookup_accounts mapping
//! deferred — see T2 follow-up".
//!
//! ### Why we haven't wired the real lookup_accounts client yet
//!
//! Available crates.io clients (`tigerbeetle-unofficial`, `enfipy-tigerbeetle`)
//! both target TigerBeetle 0.16.x. The TigerBeetle binary installed on
//! vulcan is 0.17.4. The wire protocol changed between 0.16 and 0.17, and
//! `tigerbeetle-unofficial` builds the TB C client from source at the 0.16
//! revision — connecting to a 0.17 server returns protocol errors. The
//! fixes available are:
//!   1. Downgrade the vulcan binary to 0.16.x (acceptable; preserves the
//!      0.17 install elsewhere if needed).
//!   2. Wait for an updated crates.io client matching 0.17.x.
//!   3. Hand-port the TB protocol to a small Rust client (~1-2 days).
//! T2 documents the blocker and continues; the alternative TB-only number
//! (TB binary's internal benchmark mode does not exist as of 0.17.4) is
//! also captured in the BENCHMARKS.md notes.

use crate::workloads::Workload;
use crate::{BenchResult, Cli};

pub fn run(
    workload: &Workload,
    n: usize,
    trial: u32,
    _cli: &Cli,
) -> anyhow::Result<BenchResult> {
    let note = match workload {
        Workload::YcsbC => {
            "T2 stub: TigerBeetle 0.17.4 installed on vulcan; lookup_accounts \
             mapping for YCSB-C deferred because available Rust client crates \
             (tigerbeetle-unofficial / enfipy-tigerbeetle) target TB 0.16.x \
             and the wire protocol differs from 0.17.4. Options: downgrade \
             vulcan to 0.16.x, await updated crate, or hand-port the client. \
             See drivers/tigerbeetle.rs header for full asymmetry notes."
        }
        Workload::YcsbA | Workload::YcsbB => {
            "TigerBeetle has no row-update primitive — Accounts are \
             append-only after creation; create_transfers is double-entry \
             ledger movement, not row UPDATE. We refuse to translate \
             YCSB-A/B writes to a misleading TB operation. See \
             drivers/tigerbeetle.rs header for full reasoning."
        }
    };
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
        note: Some(note.into()),
    })
}
