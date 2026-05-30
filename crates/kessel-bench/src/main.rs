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

/// SP67 #6: isolate the write-path phases so we know exactly where the
/// per-op time goes (perf is locked down on the target host). Pure,
/// deterministic, dependency-free; reports ns/op per phase.
fn run_profile(n: usize) {
    use kessel_catalog::ObjectType;
    use std::collections::BTreeMap;

    let fields = vec![
        Field { field_id: 0, name: "debit".into(), kind: FieldKind::U128, nullable: false },
        Field { field_id: 0, name: "credit".into(), kind: FieldKind::U128, nullable: false },
        Field { field_id: 0, name: "amount".into(), kind: FieldKind::U128, nullable: false },
        Field { field_id: 0, name: "pending".into(), kind: FieldKind::U128, nullable: false },
        Field { field_id: 0, name: "user".into(), kind: FieldKind::U64, nullable: false },
        Field { field_id: 0, name: "ledger".into(), kind: FieldKind::U32, nullable: false },
        Field { field_id: 0, name: "code".into(), kind: FieldKind::U16, nullable: false },
        Field { field_id: 0, name: "flags".into(), kind: FieldKind::U16, nullable: false },
        Field { field_id: 0, name: "ts".into(), kind: FieldKind::Timestamp, nullable: false },
    ];
    let vals = vec![
        Value::Uint(1), Value::Uint(2), Value::Uint(1000), Value::Uint(0),
        Value::Uint(42), Value::Uint(7), Value::Uint(100), Value::Uint(0),
        Value::Uint(1_700_000_000_000_000_000),
    ];
    // Build an ObjectType for direct codec timing (field_ids 1..=9).
    let mut ot_fields = fields.clone();
    for (i, f) in ot_fields.iter_mut().enumerate() {
        f.field_id = (i + 1) as u16;
    }
    let ot = ObjectType::from_def("transfer".into(), ot_fields);
    let rec = encode(&ot, &vals).unwrap();

    let bench = |label: &str, reps: usize, mut f: Box<dyn FnMut(usize)>| {
        let t = Instant::now();
        for i in 0..reps {
            f(i);
        }
        let ns = t.elapsed().as_nanos() as f64 / reps as f64;
        println!("  {label:<34} {ns:>9.0} ns/op");
    };

    println!("[profile] {}-byte record, {} fields", rec.len(), ot.fields.len());

    // 1. the per-op Vec clone the write loop itself does
    {
        let r = rec.clone();
        bench("Vec<u8> record clone", n, Box::new(move |_| {
            std::hint::black_box(r.clone());
        }));
    }
    // 2. codec encode (Values -> record bytes)
    {
        let ot2 = ot.clone();
        let v2 = vals.clone();
        bench("codec::encode (9 fields)", n, Box::new(move |_| {
            std::hint::black_box(encode(&ot2, &v2).unwrap());
        }));
    }
    // 3. codec decode (record bytes -> Values)
    {
        let ot3 = ot.clone();
        let r3 = rec.clone();
        bench("codec::decode (9 fields)", n, Box::new(move |_| {
            std::hint::black_box(kessel_codec::decode(&ot3, &r3).unwrap());
        }));
    }
    // 4. sm.apply(Create) — type with NO index (WAL+memtable+codec)
    {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: encode_type_def("t", &fields) });
        let mut op = 10u64;
        let r = rec.clone();
        bench("sm.apply Create (no index)", n, Box::new(move |i| {
            sm.apply(op, Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(i as u128),
                record: r.clone(),
            });
            op += 1;
        }));
    }
    // 5. sm.apply(Create) — type WITH an eq index on `user` (field 5)
    {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: encode_type_def("t", &fields) });
        sm.apply(2, Op::CreateIndex { type_id: 1, field_id: 5 });
        let mut op = 10u64;
        let r = rec.clone();
        bench("sm.apply Create (1 eq index)", n, Box::new(move |i| {
            sm.apply(op, Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(i as u128),
                record: r.clone(),
            });
            op += 1;
        }));
    }
    // 6. sm.apply(GetById) — read path (cache on by default)
    {
        let mut sm = StateMachine::open(MemVfs::new()).unwrap();
        sm.apply(1, Op::CreateType { def: encode_type_def("t", &fields) });
        for i in 0..1000u128 {
            sm.apply(10 + i as u64, Op::Create {
                type_id: 1,
                id: ObjectId::from_u128(i),
                record: rec.clone(),
            });
        }
        let mut op = 5000u64;
        bench("sm.apply GetById (cached)", n, Box::new(move |i| {
            std::hint::black_box(sm.apply(op, Op::GetById {
                type_id: 1,
                id: ObjectId::from_u128((i % 1000) as u128),
            }));
            op += 1;
        }));
    }
    // 7. raw Storage::put (WAL append + autosync + memtable) — isolates
    //    storage from the StateMachine apply wrapper
    {
        let mut st = kessel_storage::Storage::open(MemVfs::new()).unwrap();
        let r = rec.clone();
        bench("Storage::put (WAL+sync+memtable)", n, Box::new(move |i| {
            let k = kessel_storage::make_key(1, &(i as u128).to_le_bytes());
            st.put(i as u64, k, r.clone()).unwrap();
        }));
    }
    // 8. raw Storage::put with autosync OFF (group-commit model)
    {
        let mut st = kessel_storage::Storage::open(MemVfs::new()).unwrap();
        st.set_autosync(false);
        let r = rec.clone();
        bench("Storage::put (autosync OFF)", n, Box::new(move |i| {
            let k = kessel_storage::make_key(1, &(i as u128).to_le_bytes());
            st.put(i as u64, k, r.clone()).unwrap();
        }));
    }
    // 9. baseline: raw BTreeMap insert (20B key, 128B val) — lower bound
    {
        let mut m: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let r = rec.clone();
        bench("baseline BTreeMap insert", n, Box::new(move |i| {
            let mut k = (i as u128).to_le_bytes().to_vec();
            k.extend_from_slice(&[0u8; 4]);
            m.insert(k, r.clone());
        }));
    }
}

/// SP-Perf-A T1: parallel-reads benchmark. Spawns one in-process
/// kesseldb-server engine (the single-writer apply thread), inserts
/// `rows` records, then runs `workers` client threads each doing
/// random GetById against the seeded ids for `duration` seconds.
///
/// Goal at T1: lock the BASELINE number. T1 has shipped the read-pool
/// scaffold + `is_read_only` classifier + `ServerConfig.read_workers`
/// plumbing, but the bypass dispatch (`Arc<RwLock<StateMachine>>` +
/// `apply_read_op_raw`) is T2 scope. So at T1, runs with N=1 and
/// N=8 are expected to be ~equal (both bottlenecked on the engine
/// thread). T2's bench will show the gap. This shape lets us
/// commit T1 with a real baseline measurement and have T2 directly
/// re-run the same bench harness for an apples-to-apples PRE/POST.
///
/// Args (positional after mode):
///   workers (default num_cpus or 8)
///   rows    (default 100_000)
///   duration_secs (default 10)
///   pool_workers (default = workers; pass 0 to disable; T1 routes
///     either path through the engine queue, so this is a no-op
///     until T2 — recorded here so the same bench command works
///     unchanged across slices).
/// SP-Perf-A T4: multi-workload benchmark sweep. Extends T2's
/// point-read bench (`parallel-reads`) with 4 additional read shapes
/// so the publishable headline isn't a single-workload artifact:
///   - `get-by-id`:    point read (T2 baseline; matches the 4.75M peak)
///   - `select-limit`: full-table-scan `Op::Select` with LIMIT 10
///   - `select-sorted`: top-10 sorted by an indexed numeric column
///   - `aggregate-sum`: COUNT/SUM scan over a numeric column
///   - `find-by`:      indexed equality lookup (eq-secondary-index)
#[derive(Clone, Copy)]
enum BenchWorkload {
    GetById,
    SelectLimit,
    SelectSorted,
    AggregateSum,
    FindBy,
}

impl BenchWorkload {
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "get-by-id" => Self::GetById,
            "select-limit" => Self::SelectLimit,
            "select-sorted" => Self::SelectSorted,
            "aggregate-sum" => Self::AggregateSum,
            "find-by" => Self::FindBy,
            _ => return None,
        })
    }
    fn label(self) -> &'static str {
        match self {
            Self::GetById => "get-by-id",
            Self::SelectLimit => "select-limit",
            Self::SelectSorted => "select-sorted",
            Self::AggregateSum => "aggregate-sum",
            Self::FindBy => "find-by",
        }
    }
}

fn run_parallel_reads(
    workers: usize,
    rows: usize,
    duration_secs: u64,
    _pool_workers: Option<usize>,
    workload: BenchWorkload,
) {
    use kesseldb_server::{spawn_engine_cfg, ServerConfig};
    let dir = std::env::temp_dir()
        .join(format!("kesseldb-bench-parreads-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = ServerConfig {
        read_workers: _pool_workers,
        ..ServerConfig::default()
    };
    let engine = spawn_engine_cfg(&dir, &cfg).expect("engine open");

    // Seed: one richer table so multi-workload bench has fields to scan,
    // sort, aggregate, and equality-index.
    //   field 1: v U64 (no index)
    //   field 2: score I32 (eq + ordered index — sort/range/agg target)
    //   field 3: group U16 (eq index — find-by target)
    // Workload `get-by-id` only ever touches the primary key so the
    // richer schema's row footprint is amortized across all workloads
    // (rather than two distinct datasets with different memory pressure).
    let def = encode_type_def(
        "row",
        &[
            Field { field_id: 0, name: "v".into(), kind: FieldKind::U64, nullable: false },
            Field { field_id: 0, name: "score".into(), kind: FieldKind::I32, nullable: false },
            Field { field_id: 0, name: "group".into(), kind: FieldKind::U16, nullable: false },
        ],
    );
    assert!(matches!(
        engine.apply(Op::CreateType { def }),
        OpResult::TypeCreated(_)
    ));
    // Eq index on score (field 2)
    let _ = engine.apply(Op::CreateIndex { type_id: 1, field_id: 2 });
    // Ordered index on score (field 2) — enables FindRange + cheap MIN/MAX
    let _ = engine.apply(Op::AddOrderedIndex { type_id: 1, field_id: 2 });
    // Eq index on group (field 3) — the FindBy target
    let _ = engine.apply(Op::CreateIndex { type_id: 1, field_id: 3 });

    // Build ObjectType with the post-CreateType reassigned field ids so
    // we can encode rows. Field ids are 1..=n by SM convention.
    use kessel_catalog::ObjectType;
    let ot = ObjectType::from_def(
        "row".into(),
        vec![
            Field { field_id: 1, name: "v".into(), kind: FieldKind::U64, nullable: false },
            Field { field_id: 2, name: "score".into(), kind: FieldKind::I32, nullable: false },
            Field { field_id: 3, name: "group".into(), kind: FieldKind::U16, nullable: false },
        ],
    );

    for i in 0..rows {
        let id = ObjectId::from_u128(i as u128);
        let rec = kessel_codec::encode(
            &ot,
            &[
                kessel_codec::Value::Uint(i as u128),
                kessel_codec::Value::Int(((i as i128) % 1000) - 500),
                kessel_codec::Value::Uint((i as u128) % 100),
            ],
        )
        .expect("encode seed row");
        let _ = engine.apply(Op::Create {
            type_id: 1,
            id,
            record: rec,
        });
    }

    // Pre-build the "uncond program" once per workload — kessel-expr is
    // cheap to clone but pre-building keeps the hot loop tight.
    use kessel_expr::Program;
    let uncond_program = Program::new().push_int(1).bytes();

    let engine_arc = std::sync::Arc::new(engine);
    let stop_at = Instant::now() + std::time::Duration::from_secs(duration_secs);
    let mut handles = Vec::with_capacity(workers);
    for w in 0..workers {
        let engine = engine_arc.clone();
        let prog = uncond_program.clone();
        let h = std::thread::spawn(move || {
            let mut rng = kessel_proto::Rng::new(0xC0FFEE + w as u64);
            let mut ops: u64 = 0;
            let mut lat_ns: Vec<u64> = Vec::with_capacity(1024 * 1024);
            while Instant::now() < stop_at {
                let s = Instant::now();
                let r = match workload {
                    BenchWorkload::GetById => {
                        let i = rng.below(rows as u64);
                        let id = ObjectId::from_u128(i as u128);
                        engine.apply(Op::GetById { type_id: 1, id })
                    }
                    BenchWorkload::SelectLimit => engine.apply(Op::Select {
                        type_id: 1,
                        program: prog.clone(),
                        limit: 10,
                    }),
                    BenchWorkload::SelectSorted => engine.apply(Op::SelectSorted {
                        type_id: 1,
                        program: prog.clone(),
                        sort_field: 2,
                        desc: false,
                        offset: 0,
                        limit: 10,
                    }),
                    BenchWorkload::AggregateSum => engine.apply(Op::Aggregate {
                        type_id: 1,
                        program: prog.clone(),
                        kind: 1, // SUM
                        field_id: 2,
                        range_preds: vec![],
                    }),
                    BenchWorkload::FindBy => {
                        let v = ((rng.below(100) as u16).to_le_bytes()).to_vec();
                        engine.apply(Op::FindBy {
                            type_id: 1,
                            field_id: 3,
                            value: v,
                        })
                    }
                };
                lat_ns.push(s.elapsed().as_nanos() as u64);
                debug_assert!(matches!(
                    r,
                    OpResult::Got(_) | OpResult::NotFound
                ));
                ops += 1;
            }
            (ops, lat_ns)
        });
        handles.push(h);
    }

    let mut total_ops: u64 = 0;
    let mut lat_ns_all: Vec<u64> = Vec::with_capacity(workers * 1024 * 1024);
    for h in handles {
        let (ops, mut lat) = h.join().expect("worker join");
        total_ops += ops;
        lat_ns_all.append(&mut lat);
    }
    lat_ns_all.sort_unstable();
    println!(
        "parallel-reads workload={} workers={workers} rows={rows} \
         duration={duration_secs}s pool_workers={:?}",
        workload.label(),
        cfg.read_workers
    );
    println!(
        "  total = {total_ops:>10} ops | {:.0} ops/sec | p50 {}us  p99 {}us  p99.99 {}us",
        total_ops as f64 / duration_secs as f64,
        pct(&lat_ns_all, 0.50) / 1000,
        pct(&lat_ns_all, 0.99) / 1000,
        pct(&lat_ns_all, 0.9999) / 1000,
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // SP-Perf-A T1: spec-style `parallel-reads --workers N --rows R
    // --duration D` mode. Parsed separately because the legacy
    // positional CLI (`kessel-bench [N] [mem|file]`) is preserved
    // verbatim for every other mode.
    if args.get(1).map(|s| s.as_str()) == Some("parallel-reads") {
        // Defaults from spec §9 step 3. T4: `--workload <kind>` selects
        // among `get-by-id` (T2 baseline), `select-limit`, `select-sorted`,
        // `aggregate-sum`, `find-by`.
        let mut workers: usize = 8;
        let mut rows: usize = 100_000;
        let mut duration: u64 = 10;
        let mut pool_workers: Option<usize> = None;
        let mut workload: BenchWorkload = BenchWorkload::GetById;
        let mut i = 2;
        while i < args.len() {
            match args[i].as_str() {
                "--workers" => {
                    workers = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(workers);
                    i += 2;
                }
                "--rows" => {
                    rows = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(rows);
                    i += 2;
                }
                "--duration" => {
                    duration =
                        args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or(duration);
                    i += 2;
                }
                "--pool-workers" => {
                    pool_workers = args.get(i + 1).and_then(|s| s.parse().ok());
                    i += 2;
                }
                "--workload" => {
                    workload = args
                        .get(i + 1)
                        .and_then(|s| BenchWorkload::parse(s))
                        .unwrap_or(workload);
                    i += 2;
                }
                _ => i += 1,
            }
        }
        run_parallel_reads(workers, rows, duration, pool_workers, workload);
        return;
    }

    let n: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(200_000);
    let vfs = args.get(2).map(|s| s.as_str()).unwrap_or("mem");
    let batch: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);

    println!("KesselDB M2 single-node benchmark — N={n}, vfs={vfs}, batch={batch} (localhost)");
    match vfs {
        "flex" => run_flex(n),
        "sqlcache" => run_sqlcache(n),
        "bloomget" => run_bloomget(n),
        "profile" => run_profile(n),
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
