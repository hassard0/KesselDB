//! KesselDB in-process driver.
//!
//! Drives `kessel-sm::StateMachine` directly via `MemVfs` (memory-backed VFS).
//! This matches the kessel-bench `mem` upper-bound — no real fsync — which
//! is the apples-to-apples comparison against SQLite WAL_MEMORY and Postgres
//! fsync=off variants. T2 will add a `file` mode for honest durable comparison.
//!
//! Workload paths:
//! - YCSB-C (T1): 100% read. N reader threads call `read_only_op(Op::GetById)`
//!   on `Arc<RwLock<StateMachine>>` — `read_only_op(&self, …)` is `&self`, so
//!   reads share the lock.
//! - YCSB-A / YCSB-B (T2): mixed read+update. UPDATE goes through
//!   `StateMachine::apply(Op::Update { type_id, id, record })` which takes
//!   `&mut self` — writers acquire the RwLock exclusively. This matches
//!   KesselDB's actual single-apply-thread architecture. The Perf-A T2
//!   read-pool optimization helps reads only; writes still serialize
//!   through the apply path.

use crate::workloads::Workload;
use crate::{pct_us, BenchResult, Cli};
use kessel_catalog::{encode_type_def, Field, FieldKind};
use kessel_codec::{encode, Value};
use kessel_io::MemVfs;
use kessel_proto::{ObjectId, Op, OpResult};
use kessel_sm::StateMachine;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// YCSB row payload width per field (10 × 100 bytes ≈ canonical YCSB row
/// — record_size after header + null-bitmap ≈ 1 KiB).
const YCSB_FIELD_BYTES: u16 = 100;

/// YCSB schema: id BIGINT + 10 × Char(100). encode() against this produces
/// a fixed-width record of ~1 KiB matching the canonical YCSB row.
fn ycsb_type_def() -> Vec<u8> {
    let mut fields = vec![Field {
        field_id: 0,
        name: "id".into(),
        kind: FieldKind::U64,
        nullable: false,
    }];
    for i in 0..10 {
        fields.push(Field {
            field_id: 0,
            name: format!("f{i}"),
            kind: FieldKind::Char(YCSB_FIELD_BYTES),
            nullable: false,
        });
    }
    encode_type_def("ycsb", &fields)
}

fn build_values(id_lo: u64, rng: &mut SmallRng) -> Vec<Value> {
    let mut values = Vec::with_capacity(11);
    values.push(Value::Uint(id_lo as u128));
    for _ in 0..10 {
        let mut buf = vec![0u8; YCSB_FIELD_BYTES as usize];
        rng.fill(&mut buf[..]);
        values.push(Value::Blob(buf));
    }
    values
}

pub fn run(
    workload: &Workload,
    n: usize,
    trial: u32,
    cli: &Cli,
) -> anyhow::Result<BenchResult> {
    run_ycsb_mixed(*workload, n, trial, cli)
}

fn run_ycsb_mixed(
    workload: Workload,
    n: usize,
    trial: u32,
    cli: &Cli,
) -> anyhow::Result<BenchResult> {
    let rows = cli.rows;
    let duration = Duration::from_secs(cli.duration);
    let write_ratio = workload.write_ratio();

    // --- setup + load ---
    let mut sm = StateMachine::open(MemVfs::new())
        .map_err(|e| anyhow::anyhow!("StateMachine::open: {e}"))?;

    // YCSB schema: id BIGINT + 10 × Char(100). Record ≈ 1 KiB fixed-width.
    let tdef_bytes = ycsb_type_def();
    match sm.apply(1, Op::CreateType { def: tdef_bytes }) {
        OpResult::TypeCreated(_) => {}
        other => anyhow::bail!("kesseldb: CreateType failed: {:?}", other),
    }
    let ot = sm.catalog().get(1).expect("just-created type").clone();

    let mut rng = SmallRng::seed_from_u64(0xA5A5_5A5A ^ trial as u64);
    for i in 0..rows {
        let id = ObjectId::from_u128(i as u128);
        let values = build_values(i as u64, &mut rng);
        let rec = encode(&ot, &values)
            .map_err(|e| anyhow::anyhow!("kesseldb: encode row {i}: {:?}", e))?;
        let r = sm.apply(
            (i + 2) as u64,
            Op::Create {
                type_id: 1,
                id,
                record: rec,
            },
        );
        if !matches!(r, OpResult::Ok) {
            anyhow::bail!("kesseldb: Create row {i} failed: {:?}", r);
        }
    }

    // Monotonic op-number generator shared across workers — Op::Update goes
    // through StateMachine::apply which requires strictly-increasing op_number.
    // The first `rows + 2` op_numbers are consumed by setup; start workers at
    // rows + 2 to keep the contract.
    let op_seq = Arc::new(AtomicU64::new((rows as u64) + 2));

    // --- steady-state: N worker threads ---
    let sm = Arc::new(RwLock::new(sm));
    let mut handles = Vec::with_capacity(n);
    let started = Instant::now();
    let stop_at = started + duration;
    let workload_name = workload.name().to_string();
    for tid in 0..n {
        let sm = Arc::clone(&sm);
        let op_seq = Arc::clone(&op_seq);
        let workload_name = workload_name.clone();
        let h = std::thread::spawn(move || -> (u64, u64, Vec<u64>) {
            // Per-thread RNG; seeded distinctly per worker for fairness.
            let mut rng = SmallRng::seed_from_u64(0xDEAD_BEEF ^ (tid as u64) ^ (trial as u64));
            let mut count_total = 0u64;
            let mut count_writes = 0u64;
            let mut lat = Vec::with_capacity(1 << 16);
            // Reusable Update payload buffer + cached typed Value vector — we
            // re-roll the bytes per iter but avoid the per-iter Vec alloc.
            let mut update_buf = vec![0u8; YCSB_FIELD_BYTES as usize];
            let _ = workload_name; // present for debug builds if traced
            loop {
                let now = Instant::now();
                if now >= stop_at {
                    break;
                }
                let key = rng.gen_range(0..rows as u128);
                let is_write = write_ratio > 0.0 && rng.gen::<f64>() < write_ratio;
                if is_write {
                    // YCSB-A/B UPDATE: rewrite all 10 fields with fresh random
                    // bytes (canonical YCSB updates a single random field; we
                    // re-encode the full record because Op::Update replaces
                    // the whole record — the cost difference is the encode of
                    // 10 × 100 bytes which dominates the field-pick choice).
                    let mut values = Vec::with_capacity(11);
                    values.push(Value::Uint(key));
                    for _ in 0..10 {
                        rng.fill(&mut update_buf[..]);
                        values.push(Value::Blob(update_buf.clone()));
                    }
                    // We cannot share `ot` across threads without a clone per
                    // worker; encode against a thread-local copy of the
                    // type def via the catalog read.
                    let s = Instant::now();
                    let rec = {
                        let g = sm.read().unwrap();
                        let ot = g.catalog().get(1).expect("ycsb type").clone();
                        drop(g);
                        match encode(&ot, &values) {
                            Ok(b) => b,
                            Err(_) => {
                                // Skip op on encode error — shouldn't happen
                                // unless schema mismatch.
                                continue;
                            }
                        }
                    };
                    let op_no = op_seq.fetch_add(1, Ordering::Relaxed);
                    let r = sm.write().unwrap().apply(
                        op_no,
                        Op::Update {
                            type_id: 1,
                            id: ObjectId::from_u128(key),
                            record: rec,
                        },
                    );
                    lat.push(s.elapsed().as_nanos() as u64);
                    debug_assert!(matches!(r, OpResult::Ok));
                    count_writes += 1;
                } else {
                    let op = Op::GetById {
                        type_id: 1,
                        id: ObjectId::from_u128(key),
                    };
                    let s = Instant::now();
                    let r = sm.read().unwrap().read_only_op(op);
                    lat.push(s.elapsed().as_nanos() as u64);
                    debug_assert!(matches!(r, OpResult::Got(_)));
                }
                count_total += 1;
            }
            (count_total, count_writes, lat)
        });
        handles.push(h);
    }

    let mut total_ops = 0u64;
    let mut total_writes = 0u64;
    let mut lat_ns: Vec<u64> = Vec::new();
    for h in handles {
        let (ops, w, l) = h.join().expect("worker panicked");
        total_ops += ops;
        total_writes += w;
        lat_ns.extend(l);
    }
    let elapsed = started.elapsed().as_secs_f64();
    lat_ns.sort_unstable();

    let actual_wr = if total_ops > 0 {
        total_writes as f64 / total_ops as f64
    } else {
        0.0
    };

    Ok(BenchResult {
        db: "kesseldb".into(),
        workload: workload.name().to_string(),
        n,
        trial,
        ops_per_sec: total_ops as f64 / elapsed,
        p50_us: pct_us(&lat_ns, 0.50),
        p99_us: pct_us(&lat_ns, 0.99),
        p99_99_us: pct_us(&lat_ns, 0.9999),
        runtime_secs: elapsed,
        rows,
        note: Some(format!(
            "MemVfs in-process; no fsync; target write_ratio={:.2} actual={:.3}; \
             writes go through Op::Update on the serial apply path (no Perf-A read-pool benefit)",
            write_ratio, actual_wr
        )),
    })
}
