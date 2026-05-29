//! TigerBeetle driver.
//!
//! TigerBeetle's API is account/transfer-shaped (`lookup_accounts`,
//! `create_accounts`, `create_transfers`); it has no generic key→value
//! point read and no row-shape update primitive.
//!
//! ### YCSB workload mappings — honest assessment
//!
//! - **YCSB-C (100% read)** — *can* be mapped: each YCSB row becomes one
//!   TigerBeetle `Account` (id = row id, ledger = 1, code = 1,
//!   debits/credits = 0). Each YCSB read becomes `lookup_accounts([id])`.
//!   **Asymmetry footnote**: TB Accounts are 128-byte fixed records — *not*
//!   the 1-KiB YCSB row our other drivers carry. The number measures TB's
//!   lookup_accounts throughput, NOT "TB serving a 1-KiB row workload."
//!   We report this honestly in BENCHMARKS.md.
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
//! ### Compile-time gating
//!
//! The real TigerBeetle client (`tigerbeetle-unofficial`) is gated behind
//! the `tigerbeetle-real` cargo feature because:
//!   1. It pulls a Zig toolchain (~50 MiB) at build time.
//!   2. It requires `bindgen` + `clang` headers; on vulcan the build needs
//!      `BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include'`.
//!   3. The crate targets TigerBeetle 0.16.x; the headline vulcan binary is
//!      0.17.4, so to actually run we use the 0.16.78 binary at
//!      `/tmp/tb016/tigerbeetle` (downloaded fresh in T2 — see
//!      BENCHMARKS.md "TigerBeetle setup" note).
//!
//! Without the feature, all workloads return an honest-stub BenchResult
//! with `ops_per_sec=0` and an explanatory `note`.

use crate::workloads::Workload;
use crate::{BenchResult, Cli};

#[cfg(feature = "tigerbeetle-real")]
mod real {
    use super::*;
    use crate::pct_us;
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tigerbeetle_unofficial as tb;

    pub fn run_ycsb_c(n: usize, trial: u32, cli: &Cli) -> anyhow::Result<BenchResult> {
        let rows = cli.rows;
        let duration = Duration::from_secs(cli.duration);

        // --- setup: connect + create rows accounts ---
        let client = tb::Client::new(0, &cli.tb_address)
            .map_err(|e| anyhow::anyhow!("tb client: {e:?}"))?;
        let client = Arc::new(client);

        // Batch size — TB accepts a hard max per batch defined by message size.
        // Use 8K per batch, well under the limit.
        const BATCH: usize = 8 * 1024;
        let mut next_id: u128 = 1; // TB rejects id=0
        while (next_id as usize) <= rows {
            let mut batch = Vec::with_capacity(BATCH);
            for _ in 0..BATCH {
                if (next_id as usize) > rows { break; }
                batch.push(
                    tb::Account::new(next_id, 1, 1) // id, ledger, code
                );
                next_id += 1;
            }
            if batch.is_empty() { break; }
            let c = Arc::clone(&client);
            let res = pollster::block_on(async move { c.create_accounts(batch).await });
            // create_accounts returns Vec<CreateAccountsError> for per-row errors.
            // We tolerate `Exists` errors on re-runs of the same trial.
            match res {
                Ok(errs) => {
                    for e in errs {
                        let s = format!("{e:?}");
                        if !s.contains("Exists") {
                            anyhow::bail!("tb create_accounts row-error: {s}");
                        }
                    }
                }
                Err(e) => anyhow::bail!("tb create_accounts: {e:?}"),
            }
        }

        // --- steady-state: N worker threads, each owns a tokio-blocking pollster loop ---
        let started = Instant::now();
        let stop_at = started + duration;
        let mut handles = Vec::with_capacity(n);
        for tid in 0..n {
            let client = Arc::clone(&client);
            let h = std::thread::spawn(move || -> (u64, Vec<u64>) {
                let mut rng = SmallRng::seed_from_u64(0xDEAD_BEEF ^ (tid as u64) ^ (trial as u64));
                let mut count = 0u64;
                let mut lat = Vec::with_capacity(1 << 16);
                loop {
                    if Instant::now() >= stop_at {
                        break;
                    }
                    // TB rejects id=0 → key range is [1, rows].
                    let key = (rng.gen_range(0..rows as u128)) + 1;
                    let c = Arc::clone(&client);
                    let s = Instant::now();
                    let r = pollster::block_on(async move {
                        c.lookup_accounts(vec![key]).await
                    });
                    lat.push(s.elapsed().as_nanos() as u64);
                    debug_assert!(r.is_ok() && !r.as_ref().unwrap().is_empty());
                    count += 1;
                }
                (count, lat)
            });
            handles.push(h);
        }
        let mut total_ops = 0u64;
        let mut lat_ns: Vec<u64> = Vec::new();
        for h in handles {
            let (ops, l) = h.join().expect("tb worker panicked");
            total_ops += ops;
            lat_ns.extend(l);
        }
        let elapsed = started.elapsed().as_secs_f64();
        lat_ns.sort_unstable();

        Ok(BenchResult {
            db: "tigerbeetle".into(),
            workload: "ycsb-c".into(),
            n,
            trial,
            ops_per_sec: total_ops as f64 / elapsed,
            p50_us: pct_us(&lat_ns, 0.50),
            p99_us: pct_us(&lat_ns, 0.99),
            p99_99_us: pct_us(&lat_ns, 0.9999),
            runtime_secs: elapsed,
            rows,
            note: Some(
                "TigerBeetle 0.16.78 via tigerbeetle-unofficial 0.14.28+0.16.78; \
                 YCSB rows → Accounts (128B fixed, not 1KiB); reads → \
                 lookup_accounts([id]); ASYMMETRY: measures TB's account-lookup \
                 throughput, NOT a 1KiB-row workload — see drivers/tigerbeetle.rs"
                    .into(),
            ),
        })
    }
}

pub fn run(
    workload: &Workload,
    n: usize,
    trial: u32,
    _cli: &Cli,
) -> anyhow::Result<BenchResult> {
    #[cfg(feature = "tigerbeetle-real")]
    {
        if matches!(workload, Workload::YcsbC) {
            return real::run_ycsb_c(n, trial, _cli);
        }
    }
    let note = match workload {
        Workload::YcsbC => {
            #[cfg(feature = "tigerbeetle-real")]
            {
                unreachable!("handled by real::run_ycsb_c above")
            }
            #[cfg(not(feature = "tigerbeetle-real"))]
            {
                "Stub: TigerBeetle real client gated behind `tigerbeetle-real` \
                 cargo feature. Build with --features tigerbeetle-real on vulcan \
                 with BINDGEN_EXTRA_CLANG_ARGS='-I/usr/lib/gcc/x86_64-linux-gnu/13/include'. \
                 See drivers/tigerbeetle.rs header for full notes."
            }
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
