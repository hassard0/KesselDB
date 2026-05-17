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

/// 3-node replicated throughput via the deterministic VSR cluster (in-process
/// bus — measures protocol/commit overhead, NOT network; see scaling notes).
fn run_replicated(n: usize) {
    use kessel_vsr::sim::Cluster;
    let mut c = Cluster::new(3, 1, 0);
    let mut reqs: Vec<(u128, u64, Op)> = vec![(1u128, 1u64, Op::CreateType {
        def: encode_type_def(
            "t",
            &[Field { field_id: 0, name: "v".into(), kind: FieldKind::U64, nullable: false }],
        ),
    })];
    for i in 0..n as u64 {
        reqs.push((1, i + 2, Op::Create {
            type_id: 1,
            id: ObjectId::from_u128(i as u128),
            record: (i as u64).to_le_bytes().to_vec(),
        }));
    }
    let t = Instant::now();
    let steps = c.run(&reqs, n * 50 + 5000);
    let secs = t.elapsed().as_secs_f64();
    let d = c.live_digests();
    let converged = d.iter().all(|x| *x == d[0]);
    println!(
        "  3-node REPL CREATE     {:>10.0} ops/s | {} steps | {} replicas converged={}",
        n as f64 / secs,
        steps,
        c.replica_count(),
        converged
    );
}

/// SP16: how much do the flexibility layers cost vs a plain create?
/// In-memory (MemVfs) so this isolates CPU overhead, not fsync. Honest
/// relative numbers feeding the "Postgres flexibility at TB speed" thesis.
fn run_flex(n: usize) {
    use kessel_expr::Program;
    let tdef = || {
        encode_type_def("t", &[
            Field { field_id: 0, name: "owner".into(), kind: FieldKind::U32, nullable: false },
            Field { field_id: 0, name: "score".into(), kind: FieldKind::I32, nullable: false },
        ])
    };
    let rec = |owner: u32, score: i32| {
        // record_size for two 4B fields = next_pow2(14+8)=32
        let mut b = vec![0u8; 32];
        b[14..18].copy_from_slice(&owner.to_le_bytes());
        b[18..22].copy_from_slice(&score.to_le_bytes());
        b
    };
    let time = |label: &str, n: usize, f: &mut dyn FnMut()| {
        let t = Instant::now();
        f();
        let s = t.elapsed().as_secs_f64();
        println!("  {label:<28} {:>10.0} ops/s", n as f64 / s);
    };

    println!("[flex] in-memory; relative CPU cost of the flexibility layers");

    // baseline: plain create, no indexes/constraints
    {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: tdef() });
        let mut op = 2u64;
        time("plain CREATE", n, &mut || {
            for i in 0..n {
                sm.apply(op, Op::Create { type_id: 1, id: ObjectId::from_u128(i as u128), record: rec(i as u32, i as i32) });
                op += 1;
            }
        });
    }
    // + equality index
    {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: tdef() });
        sm.apply(2, Op::CreateIndex { type_id: 1, field_id: 1 });
        let mut op = 3u64;
        time("CREATE +eq-index", n, &mut || {
            for i in 0..n {
                sm.apply(op, Op::Create { type_id: 1, id: ObjectId::from_u128(i as u128), record: rec((i % 1000) as u32, i as i32) });
                op += 1;
            }
        });
        let mut op2 = op;
        time("FindBy (indexed eq)", n, &mut || {
            for i in 0..n {
                sm.apply(op2, Op::FindBy { type_id: 1, field_id: 1, value: ((i % 1000) as u32).to_le_bytes().to_vec() });
                op2 += 1;
            }
        });
    }
    // + ordered index, then range/scan reads
    {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: tdef() });
        sm.apply(2, Op::AddOrderedIndex { type_id: 1, field_id: 2 });
        let mut op = 3u64;
        time("CREATE +ordered-index", n, &mut || {
            for i in 0..n {
                sm.apply(op, Op::Create { type_id: 1, id: ObjectId::from_u128(i as u128), record: rec(i as u32, (i % 5000) as i32) });
                op += 1;
            }
        });
        let mut op2 = op;
        time("FindRange (1% window)", n, &mut || {
            for _ in 0..n {
                sm.apply(op2, Op::FindRange { type_id: 1, field_id: 2, lo: 0i32.to_le_bytes().to_vec(), hi: 50i32.to_le_bytes().to_vec() });
                op2 += 1;
            }
        });
        let prog = Program::new().load(2).push_int(2500).ge().bytes();
        let mut op3 = op2;
        let m = (n / 20).max(1); // QueryExpr is a full scan; fewer iters
        time("QueryExpr (full scan)", m, &mut || {
            for _ in 0..m {
                sm.apply(op3, Op::QueryExpr { type_id: 1, program: prog.clone() });
                op3 += 1;
            }
        });
    }
    // + CHECK constraint
    {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: tdef() });
        sm.apply(2, Op::AddCheck { type_id: 1, program: Program::new().load(2).push_int(0).ge().bytes() });
        let mut op = 3u64;
        time("CREATE +CHECK", n, &mut || {
            for i in 0..n {
                sm.apply(op, Op::Create { type_id: 1, id: ObjectId::from_u128(i as u128), record: rec(i as u32, (i % 1000) as i32) });
                op += 1;
            }
        });
    }
    // + trigger (derived field)
    {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: tdef() });
        sm.apply(2, Op::AddTrigger { type_id: 1, program: Program::new().load(1).push_int(2).mul().set_field(2).bytes() });
        let mut op = 3u64;
        time("CREATE +trigger", n, &mut || {
            for i in 0..n {
                sm.apply(op, Op::Create { type_id: 1, id: ObjectId::from_u128(i as u128), record: rec(i as u32, 0) });
                op += 1;
            }
        });
    }
}

/// SP47: quantify the prepared-statement cache. Measures repeated SQL
/// compilation cost vs. a cached compiled-statement clone (the exact work
/// the engine cache removes from the single-threaded hot path).
fn run_sqlcache(n: usize) {
    use std::collections::HashMap;
    let mut sm = StateMachine::open(MemVfs::new()).unwrap();
    sm.apply(
        1,
        Op::CreateType {
            def: encode_type_def(
                "acct",
                &[
                    Field { field_id: 0, name: "owner".into(), kind: FieldKind::U32, nullable: false },
                    Field { field_id: 1, name: "bal".into(), kind: FieldKind::I64, nullable: false },
                ],
            ),
        },
    );
    let q = "SELECT SUM(bal) FROM acct WHERE owner = 100";
    let cat = sm.catalog();

    // Cold: recompile every request (today's cost without the cache).
    let t0 = Instant::now();
    for _ in 0..n {
        let _ = kessel_sql::compile_stmt(q, cat).unwrap();
    }
    let cold = t0.elapsed();

    // Warm: compile once, then serve from the cache (clone) — what the
    // engine now does for a repeated statement.
    let mut cache: HashMap<String, kessel_sql::Stmt> = HashMap::new();
    let t1 = Instant::now();
    for _ in 0..n {
        if let Some(s) = cache.get(q) {
            std::hint::black_box(s.clone());
        } else {
            let s = kessel_sql::compile_stmt(q, cat).unwrap();
            cache.insert(q.to_string(), s);
        }
    }
    let warm = t1.elapsed();

    let cps = n as f64 / cold.as_secs_f64();
    let wps = n as f64 / warm.as_secs_f64();
    println!("SQL compile (cold)  : {cps:>12.0} stmt/s  ({cold:?} for {n})");
    println!("SQL compile (cached): {wps:>12.0} stmt/s  ({warm:?} for {n})");
    println!("speedup             : {:>11.1}x", wps / cps);
}

/// SP48: per-SSTable bloom filter. A point `get` of an absent key still
/// visits every SSTable (the read path is a flat newest-first scan, so it
/// stays O(#sstables) until leveled compaction — the named next step), but
/// each visit is now an O(1) bloom reject (a handful of bit tests) instead
/// of a binary search over the segment's sorted keys. This measures the
/// resulting absent-key throughput at 1 vs 64 segments — an honest
/// constant-factor number, NOT an O(1) claim.
fn run_bloomget(n: usize) {
    use kessel_storage::{make_key, Storage};
    let bench = |segments: usize| -> f64 {
        let mut s = Storage::open(MemVfs::new()).unwrap();
        let mut op = 0u64;
        for seg in 0..segments {
            for j in 0..200u128 {
                op += 1;
                let id = ((seg as u128) << 32) | j;
                s.put(op, make_key(0, &id.to_le_bytes()), vec![1]).unwrap();
            }
            s.flush().unwrap();
        }
        // All-miss workload (the bloom's job): keys never inserted.
        let t = Instant::now();
        for i in 0..n as u128 {
            let miss = make_key(0, &(0xDEAD_0000u128 + i).to_le_bytes());
            std::hint::black_box(s.get(&miss));
        }
        n as f64 / t.elapsed().as_secs_f64()
    };
    let one = bench(1);
    let many = bench(64);
    println!("absent-key GET, 1  segment : {one:>12.0} ops/s");
    println!("absent-key GET, 64 segments: {many:>12.0} ops/s");
    println!(
        "per-segment miss cost      : ~{:.0} ns  (bloom bit-tests, not a binary search)",
        (1.0 / many - 1.0 / one) * 1e9 / 63.0
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(200_000);
    let vfs = args.get(2).map(|s| s.as_str()).unwrap_or("mem");
    let batch: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);

    println!("KesselDB M2 single-node benchmark — N={n}, vfs={vfs}, batch={batch} (localhost)");
    match vfs {
        "flex" => run_flex(n),
        "sqlcache" => run_sqlcache(n),
        "bloomget" => run_bloomget(n),
        "repl" => run_replicated(n),
        "file" => {
            let dir = std::env::temp_dir().join(format!("kesseldb-bench-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            run("file", DirVfs::new(&dir).unwrap(), n, batch);
            let _ = std::fs::remove_dir_all(&dir);
        }
        _ => run("mem", MemVfs::new(), n, batch),
    }
}
