//! KesselDB in-process driver.
//!
//! Drives `kessel-sm::StateMachine` directly via `MemVfs` (memory-backed VFS).
//! This matches the kessel-bench `mem` upper-bound — no real fsync — which
//! is the apples-to-apples comparison against SQLite WAL_MEMORY and Postgres
//! fsync=off variants. T2 will add a `file` mode for honest durable comparison.
//!
//! YCSB-C path:
//! 1. CreateType + load `rows` records via `apply(Op::Create)`.
//! 2. Wrap StateMachine in `Arc<RwLock<>>` so N reader threads can call
//!    `read_only_op(&self, Op::GetById)` concurrently — `read_only_op` is
//!    `&self`, so this is read-shared (SP-Perf-A T2 pattern).
//! 3. Each thread loops `duration` seconds with uniform-random keys,
//!    recording per-op latency.

use crate::workloads::Workload;
use crate::{pct_us, BenchResult, Cli};
use kessel_catalog::{decode_type_def, encode_type_def, Field, FieldKind};
use kessel_codec::{encode, Value};
use kessel_io::MemVfs;
use kessel_proto::{ObjectId, Op, OpResult};
use kessel_sm::StateMachine;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
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
    match workload {
        Workload::YcsbC => run_ycsb_c(n, trial, cli),
    }
}

fn run_ycsb_c(n: usize, trial: u32, cli: &Cli) -> anyhow::Result<BenchResult> {
    let rows = cli.rows;
    let duration = Duration::from_secs(cli.duration);

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
    // Pre-decode is unused; consume to quiet warning.
    let _ = decode_type_def;

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

    // --- steady-state: N concurrent reader threads ---
    let sm = Arc::new(RwLock::new(sm));
    let mut handles = Vec::with_capacity(n);
    let started = Instant::now();
    let stop_at = started + duration;
    for tid in 0..n {
        let sm = Arc::clone(&sm);
        let h = std::thread::spawn(move || -> (u64, Vec<u64>) {
            // Per-thread RNG; seeded distinctly per worker for fairness.
            let mut rng = SmallRng::seed_from_u64(0xDEAD_BEEF ^ (tid as u64) ^ (trial as u64));
            let mut count = 0u64;
            let mut lat = Vec::with_capacity(1 << 16);
            loop {
                let now = Instant::now();
                if now >= stop_at {
                    break;
                }
                let key = rng.gen_range(0..rows as u128);
                let op = Op::GetById {
                    type_id: 1,
                    id: ObjectId::from_u128(key),
                };
                let s = Instant::now();
                let r = sm.read().unwrap().read_only_op(op);
                lat.push(s.elapsed().as_nanos() as u64);
                debug_assert!(matches!(r, OpResult::Got(_)));
                count += 1;
            }
            (count, lat)
        });
        handles.push(h);
    }

    let mut total_ops = 0u64;
    let mut lat_ns: Vec<u64> = Vec::new();
    for h in handles {
        let (ops, l) = h.join().expect("worker panicked");
        total_ops += ops;
        lat_ns.extend(l);
    }
    let elapsed = started.elapsed().as_secs_f64();
    lat_ns.sort_unstable();

    Ok(BenchResult {
        db: "kesseldb".into(),
        workload: "ycsb-c".into(),
        n,
        trial,
        ops_per_sec: total_ops as f64 / elapsed,
        p50_us: pct_us(&lat_ns, 0.50),
        p99_us: pct_us(&lat_ns, 0.99),
        p99_99_us: pct_us(&lat_ns, 0.9999),
        runtime_secs: elapsed,
        rows,
        note: Some("MemVfs in-process; no fsync (matches kessel-bench 'mem' upper-bound)".into()),
    })
}
