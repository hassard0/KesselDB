//! KesselDB M2 single-node benchmark — the early go/no-go thesis read.
//!
//! Localhost only. Reports throughput + latency percentiles for the
//! deterministic state machine on two workloads:
//!   - TB-equivalent: one fixed ~128B type (closest analogue to a
//!     TigerBeetle transfer record)
//!   - generalized:   a multi-field schema encoded via kessel-codec
//!
//! `mem` VFS = in-memory upper bound (no real fsync). `file` VFS = real
//! directory + real fsync per committed op (honest durable lower bound).
//!
//! Usage: kessel-bench [N] [mem|file]

use kessel_catalog::{encode_type_def, Field, FieldKind};
use kessel_codec::{encode, Value};
use kessel_io::{DirVfs, MemVfs, Vfs};
use kessel_proto::{ObjectId, Op, OpResult, Rng};
use kessel_sm::StateMachine;
use std::time::Instant;

fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn report(label: &str, n: usize, total_secs: f64, mut lat_ns: Vec<u64>) {
    lat_ns.sort_unstable();
    println!(
        "  {label:<22} {:>10.0} ops/s | p50 {:>6}us  p99 {:>7}us  p99.99 {:>8}us",
        n as f64 / total_secs,
        pct(&lat_ns, 0.50) / 1000,
        pct(&lat_ns, 0.99) / 1000,
        pct(&lat_ns, 0.9999) / 1000,
    );
}

fn run<V: Vfs>(tag: &str, vfs: V, n: usize, batch: usize) {
    let mut sm = StateMachine::open(vfs).unwrap();

    // --- workload A: TB-equivalent fixed ~128B type ---
    let tb_def = encode_type_def(
        "transfer",
        &[
            Field { field_id: 0, name: "debit".into(), kind: FieldKind::U128, nullable: false },
            Field { field_id: 0, name: "credit".into(), kind: FieldKind::U128, nullable: false },
            Field { field_id: 0, name: "amount".into(), kind: FieldKind::U128, nullable: false },
            Field { field_id: 0, name: "pending".into(), kind: FieldKind::U128, nullable: false },
            Field { field_id: 0, name: "user".into(), kind: FieldKind::U64, nullable: false },
            Field { field_id: 0, name: "ledger".into(), kind: FieldKind::U32, nullable: false },
            Field { field_id: 0, name: "code".into(), kind: FieldKind::U16, nullable: false },
            Field { field_id: 0, name: "flags".into(), kind: FieldKind::U16, nullable: false },
            Field { field_id: 0, name: "ts".into(), kind: FieldKind::Timestamp, nullable: false },
        ],
    );
    assert_eq!(sm.apply(1, Op::CreateType { def: tb_def }), OpResult::TypeCreated(1));
    let tb_type = sm.catalog().get(1).unwrap().clone();
    let tb_rec = encode(
        &tb_type,
        &[
            Value::Uint(1), Value::Uint(2), Value::Uint(1000), Value::Uint(0),
            Value::Uint(42), Value::Uint(7), Value::Uint(100), Value::Uint(0),
            Value::Uint(1_700_000_000_000_000_000),
        ],
    )
    .unwrap();
    println!(
        "[{tag}] TB-equivalent record = {} bytes ({} fields)",
        tb_rec.len(),
        tb_type.fields.len()
    );

    let mut op = 100u64;
    let mut lat = Vec::with_capacity(n / batch.max(1) + 1);
    let t = Instant::now();
    if batch <= 1 {
        for i in 0..n {
            let id = ObjectId::from_u128(i as u128);
            let s = Instant::now();
            let r = sm.apply(op, Op::Create { type_id: 1, id, record: tb_rec.clone() });
            lat.push(s.elapsed().as_nanos() as u64);
            debug_assert_eq!(r, OpResult::Ok);
            op += 1;
            if i % 50_000 == 49_999 {
                sm.flush().unwrap();
            }
        }
    } else {
        let mut i = 0usize;
        while i < n {
            let m = batch.min(n - i);
            let mut ops = Vec::with_capacity(m);
            for j in 0..m {
                ops.push((
                    op + j as u64,
                    Op::Create {
                        type_id: 1,
                        id: ObjectId::from_u128((i + j) as u128),
                        record: tb_rec.clone(),
                    },
                ));
            }
            let s = Instant::now(); // latency = per-batch durable commit
            sm.apply_batch(ops).unwrap();
            lat.push(s.elapsed().as_nanos() as u64);
            op += m as u64;
            i += m;
            if i % 50_000 < batch {
                sm.flush().unwrap();
            }
        }
    }
    report(
        &format!("TB-equiv CREATE x{batch}"),
        n,
        t.elapsed().as_secs_f64(),
        lat,
    );

    let mut rng = Rng::new(99);
    let mut lat = Vec::with_capacity(n);
    let t = Instant::now();
    for _ in 0..n {
        let id = ObjectId::from_u128(rng.below(n as u64) as u128);
        let s = Instant::now();
        let _ = sm.apply(op, Op::GetById { type_id: 1, id });
        lat.push(s.elapsed().as_nanos() as u64);
        op += 1;
    }
    report("TB-equiv GET", n, t.elapsed().as_secs_f64(), lat);

    // --- workload B: generalized multi-field schema ---
    let gen_def = encode_type_def(
        "doc",
        &[
            Field { field_id: 0, name: "owner".into(), kind: FieldKind::U64, nullable: false },
            Field { field_id: 0, name: "kind".into(), kind: FieldKind::U16, nullable: false },
            Field { field_id: 0, name: "score".into(), kind: FieldKind::I64, nullable: false },
            Field { field_id: 0, name: "title".into(), kind: FieldKind::Char(48), nullable: true },
            Field { field_id: 0, name: "active".into(), kind: FieldKind::Bool, nullable: false },
        ],
    );
    assert_eq!(sm.apply(op, Op::CreateType { def: gen_def }), OpResult::TypeCreated(2));
    op += 1;
    let gt = sm.catalog().get(2).unwrap().clone();

    let mut lat = Vec::with_capacity(n);
    let t = Instant::now();
    for i in 0..n {
        let rec = encode(
            &gt,
            &[
                Value::Uint(i as u128 & 0xFFFF),
                Value::Uint(3),
                Value::Int(-(i as i128 % 1000)),
                Value::Blob(format!("doc {i}").into_bytes()),
                Value::Uint((i % 2) as u128),
            ],
        )
        .unwrap();
        let id = ObjectId::from_u128(i as u128);
        let s = Instant::now();
        let r = sm.apply(op, Op::Create { type_id: 2, id, record: rec });
        lat.push(s.elapsed().as_nanos() as u64);
        debug_assert_eq!(r, OpResult::Ok);
        op += 1;
        if i % 50_000 == 49_999 {
            sm.flush().unwrap();
        }
    }
    report("generalized CREATE", n, t.elapsed().as_secs_f64(), lat);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(200_000);
    let vfs = args.get(2).map(|s| s.as_str()).unwrap_or("mem");
    let batch: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);

    println!("KesselDB M2 single-node benchmark — N={n}, vfs={vfs}, batch={batch} (localhost)");
    match vfs {
        "file" => {
            let dir = std::env::temp_dir().join(format!("kesseldb-bench-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            run("file", DirVfs::new(&dir).unwrap(), n, batch);
            let _ = std::fs::remove_dir_all(&dir);
        }
        _ => run("mem", MemVfs::new(), n, batch),
    }
}
