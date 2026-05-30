//! SP-Perf-A T3 — multi-op-kind mixed-reads determinism oracle.
//!
//! See `docs/superpowers/specs/2026-05-28-kesseldb-perf-a-parallel-reads-design.md`
//! (§6 determinism preservation, §10 acceptance criterion #4).
//!
//! What this test ships
//! ====================
//! T2 shipped a determinism oracle (`determinism_oracle_100_random_workloads`
//! in `read_pool.rs::tests`) that exercises 100 × 10 GetById ops. That
//! covers ONE read variant. T3 expands the oracle to all 16 spec §4
//! read variants, seeded against a real engine populated with multiple
//! user tables (varied schemas — primitive types + nullable + Char +
//! Bytes), 2 secondary indexes + 1 composite index + an ordered/range
//! index, plus a self-FK to exercise that path.
//!
//! For each of N=100 random workloads × 1000 ops each (= 100,000 reads
//! total), we run the workload twice:
//!   - on a "parallel" engine spawned with `read_workers = Some(8)` so
//!     every read takes the SP-Perf-A T2 bypass (`Arc<RwLock<SM>>`
//!     read guard → `StateMachine::read_only_op`); AND
//!   - on a "serial" engine spawned with `read_workers = None` so every
//!     read goes through the original single-writer apply queue.
//!
//! Then we assert byte-for-byte equality on every read's `OpResult`. A
//! divergence reproducer is the workload index + op index pair (the
//! random workload generation is `seed = workload_idx * 1000`, fully
//! deterministic).
//!
//! Why this matters
//! ================
//! If a parallel-only divergence surfaces in any of the 16 read variants
//! (e.g. an iteration order that depends on `&mut` state on the writer's
//! hot path; a cache that's threaded across workers; a hash-map seed
//! that's process-global), we want it caught here before a production
//! query returns a different answer from one connection to the next.
//!
//! Coverage map (variants × scenarios):
//!   - GetById (random oid, hit + miss)
//!   - GetBlob (overflow-handle lookup, miss is the common case)
//!   - Describe (deterministic — schema introspection by type_id)
//!   - FindBy (eq-indexed column, random value)
//!   - FindByComposite (composite-index tuple, random value)
//!   - FindRange (ordered-index lo/hi, random window)
//!   - Query (random AND-of-(Eq/Ge/Le) predicates)
//!   - QueryRows (same + return rows)
//!   - QueryExpr (a kessel-expr program: load(field) >= constant)
//!   - Select (full scan + LIMIT)
//!   - SelectFields (projection scan)
//!   - SelectSorted (sort + page)
//!   - Aggregate (COUNT/SUM/MIN/MAX over a numeric column)
//!   - GroupAggregate (group-by + agg)
//!   - SeqRead (sequencer log scan; seeded with a few entries)
//!   - Join (self-join on the FK column)
//!
//! Each random workload picks a variant uniformly + a random
//! argument; the SAME workload runs against both engines so the test
//! is correctness-locked.
#![cfg(not(miri))]

use kessel_catalog::{encode_type_def, Field, FieldKind, ObjectType};
use kessel_codec::{encode, Value};
use kessel_expr::Program;
use kessel_proto::{ObjectId, Op, OpResult, Pred, Rng};
use kesseldb_server::{spawn_engine_cfg, EngineHandle, ServerConfig};
use std::path::PathBuf;

// Headline oracle parameters. Chosen so the full 100K-read sweep
// (100 workloads × 1000 ops each, uniform over 16 variants) completes
// in <5 min on the SP-Perf-A reference vulcan box. With N_ROWS=2000
// the heavy O(N²) Join variant pulls ~4M comparisons per call × ~6250
// random calls = ~25B comparisons, which release-build kessel-sm
// chews through in ~150s.
const N_WORKLOADS: usize = 100;
const OPS_PER_WORKLOAD: usize = 1000;
const N_ROWS: u32 = 2_000;
const N_TABLES: u32 = 3;

/// 3-table schema. NOTE: kessel-sm CreateType deterministically reassigns
/// field_ids to 1..=n at create-time, so we use 1-based indexing below.
///   - type 1 "user":  field 1 v U64, field 2 score I32 (eq + ordered),
///                     field 3 group U16 (eq), field 4 name Char(16) nullable
///   - type 2 "post":  field 1 user_id Ref (eq), field 2 kind U16 (eq),
///                     field 3 bytes Bytes(8); composite index on (1, 2)
///   - type 3 "tag":   field 1 key Char(8), field 2 val U64 (eq)
///
/// Seeded with N_ROWS rows in type 1, N_ROWS/2 in type 2 (with FK to type 1),
/// N_ROWS/10 in type 3. A handful of SeqAppend entries are also written.
fn build_schemas(engine: &EngineHandle) {
    // type 1 — user
    let user_def = encode_type_def(
        "user",
        &[
            Field { field_id: 0, name: "v".into(), kind: FieldKind::U64, nullable: false },
            Field { field_id: 0, name: "score".into(), kind: FieldKind::I32, nullable: false },
            Field { field_id: 0, name: "group".into(), kind: FieldKind::U16, nullable: false },
            Field { field_id: 0, name: "name".into(), kind: FieldKind::Char(16), nullable: true },
        ],
    );
    let r = engine.apply(Op::CreateType { def: user_def });
    assert!(matches!(r, OpResult::TypeCreated(1)), "user create: {r:?}");
    // eq index on score (field 2)
    let r = engine.apply(Op::CreateIndex { type_id: 1, field_id: 2 });
    assert!(matches!(r, OpResult::Ok), "user eq-index score: {r:?}");
    // ordered (range) index on score (field 2)
    let r = engine.apply(Op::AddOrderedIndex { type_id: 1, field_id: 2 });
    assert!(matches!(r, OpResult::Ok), "user ordered score: {r:?}");
    // eq index on group (field 3)
    let r = engine.apply(Op::CreateIndex { type_id: 1, field_id: 3 });
    assert!(matches!(r, OpResult::Ok), "user eq-index group: {r:?}");

    // type 2 — post
    let post_def = encode_type_def(
        "post",
        &[
            Field { field_id: 0, name: "user_id".into(), kind: FieldKind::Ref, nullable: false },
            Field { field_id: 0, name: "kind".into(), kind: FieldKind::U16, nullable: false },
            Field { field_id: 0, name: "bytes".into(), kind: FieldKind::Bytes(8), nullable: false },
        ],
    );
    let r = engine.apply(Op::CreateType { def: post_def });
    assert!(matches!(r, OpResult::TypeCreated(2)), "post create: {r:?}");
    // field 1 is user_id (Ref)
    let r = engine.apply(Op::CreateIndex { type_id: 2, field_id: 1 });
    assert!(matches!(r, OpResult::Ok), "post eq-index field 1 (Ref): {r:?}");
    // field 2 is kind (U16)
    let r = engine.apply(Op::CreateIndex { type_id: 2, field_id: 2 });
    assert!(matches!(r, OpResult::Ok), "post eq-index field 2 (U16): {r:?}");
    // composite index on (user_id, kind) = (1, 2)
    let r = engine.apply(Op::AddCompositeIndex { type_id: 2, fields: vec![1, 2] });
    assert!(matches!(r, OpResult::Ok), "post composite (1,2): {r:?}");

    // type 3 — tag
    let tag_def = encode_type_def(
        "tag",
        &[
            Field { field_id: 0, name: "key".into(), kind: FieldKind::Char(8), nullable: false },
            Field { field_id: 0, name: "val".into(), kind: FieldKind::U64, nullable: false },
        ],
    );
    let r = engine.apply(Op::CreateType { def: tag_def });
    assert!(matches!(r, OpResult::TypeCreated(3)), "tag create: {r:?}");
    // field 2 is val (U64)
    let r = engine.apply(Op::CreateIndex { type_id: 3, field_id: 2 });
    assert!(matches!(r, OpResult::Ok), "tag eq-index val: {r:?}");
}

fn seed_data(engine: &EngineHandle) {
    // Build ObjectType locally with the same shape kessel-sm's
    // CreateType deterministically assigns: field_ids 1..=n.
    let user_ot = ObjectType::from_def(
        "user".into(),
        vec![
            Field { field_id: 1, name: "v".into(), kind: FieldKind::U64, nullable: false },
            Field { field_id: 2, name: "score".into(), kind: FieldKind::I32, nullable: false },
            Field { field_id: 3, name: "group".into(), kind: FieldKind::U16, nullable: false },
            Field { field_id: 4, name: "name".into(), kind: FieldKind::Char(16), nullable: true },
        ],
    );
    let post_ot = ObjectType::from_def(
        "post".into(),
        vec![
            Field { field_id: 1, name: "user_id".into(), kind: FieldKind::Ref, nullable: false },
            Field { field_id: 2, name: "kind".into(), kind: FieldKind::U16, nullable: false },
            Field { field_id: 3, name: "bytes".into(), kind: FieldKind::Bytes(8), nullable: false },
        ],
    );
    let tag_ot = ObjectType::from_def(
        "tag".into(),
        vec![
            Field { field_id: 1, name: "key".into(), kind: FieldKind::Char(8), nullable: false },
            Field { field_id: 2, name: "val".into(), kind: FieldKind::U64, nullable: false },
        ],
    );

    // type 1 — user: N_ROWS records
    for i in 0..N_ROWS {
        let id = ObjectId::from_u128(i as u128);
        let rec = encode(
            &user_ot,
            &[
                Value::Uint((i as u128).wrapping_mul(7)),
                Value::Int(((i as i128) % 1000) - 500), // score in [-500, 500)
                Value::Uint((i as u128) % 50),          // group in [0, 50)
                if i % 8 == 0 {
                    Value::Null
                } else {
                    let mut b = vec![0u8; 16];
                    let s = format!("user{i}");
                    let len = s.len().min(16);
                    b[..len].copy_from_slice(&s.as_bytes()[..len]);
                    Value::Blob(b)
                },
            ],
        )
        .unwrap();
        let r = engine.apply(Op::Create { type_id: 1, id, record: rec });
        assert!(matches!(r, OpResult::Ok), "user seed at {i}: {r:?}");
    }

    // type 2 — post: N_ROWS/2 records, FK to type 1
    for i in 0..N_ROWS / 2 {
        // High bit pattern keeps post ids disjoint from user ids
        let id = ObjectId::from_u128((1u128 << 100) | i as u128);
        let parent_oid_bytes =
            ObjectId::from_u128((i as u128) % (N_ROWS as u128)).0;
        let mut byts = vec![0u8; 8];
        byts[..4].copy_from_slice(&i.to_le_bytes());
        let rec = encode(
            &post_ot,
            &[
                Value::Blob(parent_oid_bytes.to_vec()),
                Value::Uint((i as u128) % 10), // kind in [0, 10)
                Value::Blob(byts),
            ],
        )
        .unwrap();
        let r = engine.apply(Op::Create { type_id: 2, id, record: rec });
        assert!(matches!(r, OpResult::Ok), "post seed at {i}: {r:?}");
    }

    // type 3 — tag: N_ROWS/10 records
    for i in 0..N_ROWS / 10 {
        let id = ObjectId::from_u128((2u128 << 100) | i as u128);
        let mut key = vec![0u8; 8];
        let s = format!("t{i}");
        let len = s.len().min(8);
        key[..len].copy_from_slice(&s.as_bytes()[..len]);
        let rec = encode(
            &tag_ot,
            &[Value::Blob(key), Value::Uint(i as u128)],
        )
        .unwrap();
        let r = engine.apply(Op::Create { type_id: 3, id, record: rec });
        assert!(matches!(r, OpResult::Ok), "tag seed at {i}: {r:?}");
    }

    // Sequencer log — a few entries so SeqRead has something.
    for i in 0..32u64 {
        let r = engine.apply(Op::SeqAppend {
            payload: i.to_le_bytes().to_vec(),
        });
        assert!(matches!(r, OpResult::Got(_)), "seq seed at {i}: {r:?}");
    }
}

fn spawn(read_workers: Option<usize>, tag: &str) -> (EngineHandle, PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "kesseldb-t3-oracle-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = ServerConfig {
        read_workers,
        ..ServerConfig::default()
    };
    let engine = spawn_engine_cfg(&dir, &cfg).expect("engine open");
    build_schemas(&engine);
    seed_data(&engine);
    (engine, dir)
}

/// 16 read-op generator dispatch. Returns `(variant_label, Op)` for the
/// caller to apply against both engines.
///
/// Heavy O(N²) Join is artificially under-sampled (1% probability vs ~6%
/// for the cheap variants) because the headline oracle's 100K-read
/// sweep would otherwise be Join-dominated. Per-variant smoke tests
/// give Join a deeper individual sweep.
fn gen_random_read_op(rng: &mut Rng) -> (&'static str, Op) {
    // Skewed roulette. Variants 0..14 (15 cheap-or-moderate) get equal
    // share of 98% (~6.5% each); Join (15) is under-sampled at 2% so
    // the 100K-read sweep finishes in <5 min — the O(N²) Join scales
    // as rows². The per-variant smoke test gives Join a deeper sweep.
    let dice = rng.below(98 * 15 + 2);  // total denom = 1472
    let kind = if dice < 98 * 15 {
        // 0..1469 maps uniformly to variants 0..14
        (dice / 98) as u64
    } else {
        15
    };
    match kind {
        0 => ("GetById", Op::GetById {
            type_id: 1 + rng.below(N_TABLES as u64) as u32,
            id: ObjectId::from_u128(rng.below(N_ROWS as u64 * 2) as u128),
        }),
        1 => ("GetBlob", Op::GetBlob {
            // Mostly NotFound (overflow handles are rare); the oracle
            // is about byte-equality whether hit or miss.
            handle: rng.next_u64(),
        }),
        2 => ("Describe", Op::Describe {
            type_id: 1 + rng.below(N_TABLES as u64) as u32,
        }),
        3 => {
            // FindBy on type 1 score (i32, field 2) — eq-indexed
            let v = ((rng.below(1000) as i32) - 500).to_le_bytes().to_vec();
            ("FindBy", Op::FindBy { type_id: 1, field_id: 2, value: v })
        }
        4 => {
            // FindByComposite on type 2 (user_id field 1, kind field 2)
            let parent = ObjectId::from_u128(rng.below(N_ROWS as u64) as u128).0;
            let kind = ((rng.below(10) as u16).to_le_bytes()).to_vec();
            ("FindByComposite", Op::FindByComposite {
                type_id: 2,
                fields: vec![1, 2],
                values: vec![parent.to_vec(), kind],
            })
        }
        5 => {
            // FindRange on type 1 score (ordered index, field 2)
            let lo = ((rng.below(500) as i32) - 500).to_le_bytes().to_vec();
            let hi = ((rng.below(500) as i32)).to_le_bytes().to_vec();
            ("FindRange", Op::FindRange {
                type_id: 1,
                field_id: 2,
                lo,
                hi,
            })
        }
        6 => {
            // Query: AND-of-(Eq/Ge/Le) — 1 to 2 predicates over type 1.
            // field 3 is group (U16, eq-indexed); field 2 is score (I32, range-indexed)
            let n_preds = 1 + rng.below(2) as usize;
            let mut preds = Vec::with_capacity(n_preds);
            for _ in 0..n_preds {
                let p = rng.below(3);
                match p {
                    0 => preds.push(Pred {
                        field_id: 3,
                        op: 0,
                        value: ((rng.below(50) as u16).to_le_bytes()).to_vec(),
                    }),
                    1 => preds.push(Pred {
                        field_id: 2,
                        op: 1,
                        value: ((rng.below(500) as i32) - 500).to_le_bytes().to_vec(),
                    }),
                    _ => preds.push(Pred {
                        field_id: 2,
                        op: 2,
                        value: ((rng.below(500) as i32)).to_le_bytes().to_vec(),
                    }),
                }
            }
            ("Query", Op::Query { type_id: 1, preds })
        }
        7 => {
            // QueryRows: eq + range preds + a uncond program
            let eq_preds = if rng.below(2) == 0 {
                vec![(
                    3u16,
                    ((rng.below(50) as u16).to_le_bytes()).to_vec(),
                )]
            } else {
                vec![]
            };
            let range_preds = if rng.below(2) == 0 {
                vec![
                    (2u16, 1u8, ((rng.below(500) as i32) - 500).to_le_bytes().to_vec()),
                    (2u16, 2u8, ((rng.below(500) as i32)).to_le_bytes().to_vec()),
                ]
            } else {
                vec![]
            };
            ("QueryRows", Op::QueryRows {
                type_id: 1,
                eq_preds,
                program: Program::new().push_int(1).bytes(),
                limit: rng.below(50) as u32,
                range_preds,
            })
        }
        8 => {
            // QueryExpr: load(score, field 2) >= K
            let k = (rng.below(1000) as i128) - 500;
            let prog = Program::new().load(2).push_int(k).ge().bytes();
            ("QueryExpr", Op::QueryExpr { type_id: 1, program: prog })
        }
        9 => {
            // Select with LIMIT (uncond program)
            ("Select", Op::Select {
                type_id: 1 + rng.below(N_TABLES as u64) as u32,
                program: Program::new().push_int(1).bytes(),
                limit: 1 + rng.below(20) as u32,
            })
        }
        10 => {
            // SelectFields: project score (2) + group (3) from type 1
            ("SelectFields", Op::SelectFields {
                type_id: 1,
                program: Program::new().push_int(1).bytes(),
                fields: vec![2, 3],
                limit: rng.below(30) as u32,
            })
        }
        11 => {
            // SelectSorted: sort by score (field 2), pages
            ("SelectSorted", Op::SelectSorted {
                type_id: 1,
                program: Program::new().push_int(1).bytes(),
                sort_field: 2,
                desc: rng.below(2) == 0,
                offset: rng.below(20) as u32,
                limit: 1 + rng.below(20) as u32,
            })
        }
        12 => {
            // Aggregate: COUNT/SUM/MIN/MAX over score (field 2)
            let kind = rng.below(4) as u8;
            ("Aggregate", Op::Aggregate {
                type_id: 1,
                program: Program::new().push_int(1).bytes(),
                kind,
                field_id: 2,
                range_preds: vec![],
            })
        }
        13 => {
            // GroupAggregate: COUNT/SUM over score (field 2), grouped by group (field 3)
            let kind = rng.below(2) as u8;
            ("GroupAggregate", Op::GroupAggregate {
                type_id: 1,
                program: Program::new().push_int(1).bytes(),
                group_field: 3,
                kind,
                agg_field: 2,
                range_preds: vec![],
            })
        }
        14 => {
            // SeqRead — scan the sequencer log
            ("SeqRead", Op::SeqRead {
                from: rng.below(32),
                limit: 1 + rng.below(16) as u32,
            })
        }
        _ => {
            // Join: self-join on user.score (field 2). `Op::Join`
            // requires equal-width fields; both sides are I32 so
            // this always matches. Cap limit at small number — Join
            // builds a hashmap of right + scans left × map; the work
            // is O(rows + matches). Limit only caps the output.
            ("Join", Op::Join {
                left_type: 1,
                right_type: 1,
                left_field: 2,
                right_field: 2,
                limit: 1 + rng.below(4) as u32,
            })
        }
    }
}

/// HEADLINE T3 oracle. 100 random workloads × 1000 reads = 100K total
/// reads, every read across all 16 variants, byte-equal between parallel
/// and serial engines.
#[test]
fn t3_oracle_100_workloads_x_1000_reads_all_16_variants() {
    let (engine_p, dir_p) = spawn(Some(8), "p");
    let (engine_s, dir_s) = spawn(None, "s");

    let mut total_reads = 0usize;
    let mut variant_counts: std::collections::BTreeMap<&'static str, usize> =
        std::collections::BTreeMap::new();
    let mut diffs: Vec<(usize, usize, &'static str)> = Vec::new();

    for workload_idx in 0..N_WORKLOADS {
        let mut rng = Rng::new((workload_idx as u64).wrapping_mul(1000).wrapping_add(0xC0FFEE));
        for op_idx in 0..OPS_PER_WORKLOAD {
            let (label, op) = gen_random_read_op(&mut rng);
            *variant_counts.entry(label).or_insert(0) += 1;
            let p = engine_p.apply(op.clone());
            let s = engine_s.apply(op);
            total_reads += 1;
            if p != s {
                diffs.push((workload_idx, op_idx, label));
                // Capture first 3 divergences with rich detail.
                if diffs.len() <= 3 {
                    eprintln!(
                        "DIVERGENCE workload={} op={} variant={} parallel={:?} serial={:?}",
                        workload_idx, op_idx, label, p, s
                    );
                }
            }
        }
    }

    drop(engine_p);
    drop(engine_s);
    let _ = std::fs::remove_dir_all(&dir_p);
    let _ = std::fs::remove_dir_all(&dir_s);

    // Coverage sanity: every variant got exercised at least once across
    // 100K random picks (each is uniformly 1/16, so this is essentially
    // certain — but lock it).
    let expected_variants: &[&str] = &[
        "GetById", "GetBlob", "Describe",
        "FindBy", "FindByComposite", "FindRange",
        "Query", "QueryRows", "QueryExpr",
        "Select", "SelectFields", "SelectSorted",
        "Aggregate", "GroupAggregate", "SeqRead", "Join",
    ];
    for v in expected_variants {
        // Join is intentionally under-sampled (~2% = 2000 hits over 100K
        // reads); the other 15 each get ~6.5% = ~6600 hits. Floor 50 is
        // ~150× safety margin against random luck.
        assert!(
            variant_counts.get(v).copied().unwrap_or(0) > 50,
            "variant {v} undersampled: {} hits over {total_reads} reads",
            variant_counts.get(v).copied().unwrap_or(0)
        );
    }

    assert_eq!(
        total_reads,
        N_WORKLOADS * OPS_PER_WORKLOAD,
        "expected {} reads, ran {}",
        N_WORKLOADS * OPS_PER_WORKLOAD,
        total_reads
    );
    assert!(
        diffs.is_empty(),
        "oracle FAILED: {} divergences across {} reads. First 10: {:?}",
        diffs.len(),
        total_reads,
        diffs.iter().take(10).collect::<Vec<_>>()
    );
}

// -------------------------------------------------------------------------
// 16 per-variant "smoke" tests — each picks ONE variant and runs 1K random
// reads against parallel + serial engines. These give us a fast,
// fine-grained failure surface so the headline oracle's failure mode tells
// us WHICH variant broke (the headline oracle's stderr also prints the
// first 3 divergences but a per-variant test is the bisection target).
// -------------------------------------------------------------------------

fn run_per_variant_smoke(seed: u64, n: usize, label: &str, mut gen: impl FnMut(&mut Rng) -> Op) {
    let (engine_p, dir_p) = spawn(Some(8), &format!("smoke-{label}"));
    let (engine_s, dir_s) = spawn(None, &format!("smoke-{label}-s"));
    let mut rng = Rng::new(seed);
    let mut diffs = 0usize;
    for i in 0..n {
        let op = gen(&mut rng);
        let p = engine_p.apply(op.clone());
        let s = engine_s.apply(op);
        if p != s {
            diffs += 1;
            if diffs <= 3 {
                eprintln!("smoke[{label}] divergence at {i}: parallel={p:?} serial={s:?}");
            }
        }
    }
    drop(engine_p);
    drop(engine_s);
    let _ = std::fs::remove_dir_all(&dir_p);
    let _ = std::fs::remove_dir_all(&dir_s);
    assert_eq!(diffs, 0, "{label}: {diffs} divergences");
}

#[test]
fn t3_smoke_get_by_id() {
    run_per_variant_smoke(1, 1000, "GetById", |r| Op::GetById {
        type_id: 1 + r.below(N_TABLES as u64) as u32,
        id: ObjectId::from_u128(r.below(N_ROWS as u64 * 2) as u128),
    });
}

#[test]
fn t3_smoke_get_blob() {
    run_per_variant_smoke(2, 200, "GetBlob", |r| Op::GetBlob { handle: r.next_u64() });
}

#[test]
fn t3_smoke_describe() {
    run_per_variant_smoke(3, 200, "Describe", |r| Op::Describe {
        type_id: 1 + r.below(N_TABLES as u64) as u32,
    });
}

#[test]
fn t3_smoke_find_by() {
    run_per_variant_smoke(4, 500, "FindBy", |r| Op::FindBy {
        type_id: 1,
        field_id: 2,
        value: ((r.below(1000) as i32) - 500).to_le_bytes().to_vec(),
    });
}

#[test]
fn t3_smoke_find_by_composite() {
    run_per_variant_smoke(5, 500, "FindByComposite", |r| {
        let parent = ObjectId::from_u128(r.below(N_ROWS as u64) as u128).0;
        let kind = ((r.below(10) as u16).to_le_bytes()).to_vec();
        Op::FindByComposite {
            type_id: 2,
            fields: vec![1, 2],
            values: vec![parent.to_vec(), kind],
        }
    });
}

#[test]
fn t3_smoke_find_range() {
    run_per_variant_smoke(6, 500, "FindRange", |r| Op::FindRange {
        type_id: 1,
        field_id: 2,
        lo: ((r.below(500) as i32) - 500).to_le_bytes().to_vec(),
        hi: ((r.below(500) as i32)).to_le_bytes().to_vec(),
    });
}

#[test]
fn t3_smoke_query() {
    run_per_variant_smoke(7, 200, "Query", |r| Op::Query {
        type_id: 1,
        preds: vec![Pred {
            field_id: 3,
            op: 0,
            value: ((r.below(50) as u16).to_le_bytes()).to_vec(),
        }],
    });
}

#[test]
fn t3_smoke_query_rows() {
    run_per_variant_smoke(8, 200, "QueryRows", |r| Op::QueryRows {
        type_id: 1,
        eq_preds: vec![(3u16, ((r.below(50) as u16).to_le_bytes()).to_vec())],
        program: Program::new().push_int(1).bytes(),
        limit: r.below(50) as u32,
        range_preds: vec![],
    });
}

#[test]
fn t3_smoke_query_expr() {
    run_per_variant_smoke(9, 100, "QueryExpr", |r| Op::QueryExpr {
        type_id: 1,
        program: Program::new()
            .load(2)
            .push_int((r.below(1000) as i128) - 500)
            .ge()
            .bytes(),
    });
}

#[test]
fn t3_smoke_select() {
    run_per_variant_smoke(10, 100, "Select", |r| Op::Select {
        type_id: 1 + r.below(N_TABLES as u64) as u32,
        program: Program::new().push_int(1).bytes(),
        limit: 1 + r.below(20) as u32,
    });
}

#[test]
fn t3_smoke_select_fields() {
    run_per_variant_smoke(11, 100, "SelectFields", |r| Op::SelectFields {
        type_id: 1,
        program: Program::new().push_int(1).bytes(),
        fields: vec![2, 3],
        limit: r.below(30) as u32,
    });
}

#[test]
fn t3_smoke_select_sorted() {
    run_per_variant_smoke(12, 100, "SelectSorted", |r| Op::SelectSorted {
        type_id: 1,
        program: Program::new().push_int(1).bytes(),
        sort_field: 2,
        desc: r.below(2) == 0,
        offset: r.below(20) as u32,
        limit: 1 + r.below(20) as u32,
    });
}

#[test]
fn t3_smoke_aggregate() {
    run_per_variant_smoke(13, 100, "Aggregate", |r| Op::Aggregate {
        type_id: 1,
        program: Program::new().push_int(1).bytes(),
        kind: r.below(4) as u8,
        field_id: 2,
        range_preds: vec![],
    });
}

#[test]
fn t3_smoke_group_aggregate() {
    run_per_variant_smoke(14, 50, "GroupAggregate", |r| Op::GroupAggregate {
        type_id: 1,
        program: Program::new().push_int(1).bytes(),
        group_field: 3,
        kind: r.below(2) as u8,
        agg_field: 2,
        range_preds: vec![],
    });
}

#[test]
fn t3_smoke_seq_read() {
    run_per_variant_smoke(15, 100, "SeqRead", |r| Op::SeqRead {
        from: r.below(32),
        limit: 1 + r.below(16) as u32,
    });
}

#[test]
fn t3_smoke_join() {
    run_per_variant_smoke(16, 20, "Join", |r| Op::Join {
        left_type: 1,
        right_type: 1,
        left_field: 2,
        right_field: 2,
        limit: 1 + r.below(8) as u32,
    });
}

// -------------------------------------------------------------------------
// SP-Perf-A-TXN-RO determinism oracle — all-RO Op::Txn{ops} parallel vs
// serial byte-equality. Same harness as T3 but every read is wrapped in
// an Op::Txn with a random inner-op count, so the bypass exercises its
// SP-Perf-A-TXN-RO Op::Txn arm (StateMachine::read_only_op) end-to-end
// against the same engine that processed the inner ops in T3 directly.
//
// Acceptance lock: 100K all-RO Op::Txn calls × parallel vs serial
// byte-equal. Plus 6 per-shape smoke tests (empty Txn, single inner,
// 16-inner mixed-variant Txn, 410-inner sysbench-shape Txn, 1-write
// poisoned-Txn falls through to apply path symmetry, mixed-RW Txn
// returns identical results on both engines).
// -------------------------------------------------------------------------

/// TXN-RO oracle headline: 100 random workloads × 1000 Op::Txn calls,
/// each Txn wraps 1-20 random RO inner ops. ~100K Op::Txn calls,
/// ~1M inner reads total. Byte-equal between parallel + serial engines.
/// SeqRead is excluded from inner-op generation because apply-Txn
/// rejects it; the bypass mirrors that rejection; both paths agree
/// (SchemaError), but the oracle is about the SUCCESS-path equivalence
/// of the 15 Txn-permitted reads.
#[test]
fn txn_ro_oracle_100_workloads_x_1000_txns_byte_equal() {
    let (engine_p, dir_p) = spawn(Some(8), "txnro-p");
    let (engine_s, dir_s) = spawn(None, "txnro-s");

    let mut total_txns = 0usize;
    let mut total_inner_ops = 0usize;
    let mut diffs: Vec<(usize, usize, usize)> = Vec::new();

    for workload_idx in 0..N_WORKLOADS {
        let mut rng = Rng::new(
            (workload_idx as u64)
                .wrapping_mul(1000)
                .wrapping_add(0xDEC0_DECF),
        );
        for op_idx in 0..OPS_PER_WORKLOAD {
            // Random inner-op count 1..=20. Avoids the empty-Txn corner
            // (which has its own per-shape KAT); the bulk shape mirrors
            // realistic Lambda-style multi-read patterns. Rejects
            // SeqRead inner ops since apply-Txn rejects them (the
            // bypass mirrors that rejection — byte-equal — but the
            // success-path oracle wants Ok-return equivalence).
            let n_inner = 1 + (rng.below(20) as usize);
            let mut inner = Vec::with_capacity(n_inner);
            while inner.len() < n_inner {
                let (label, op) = gen_random_read_op(&mut rng);
                if label == "SeqRead" {
                    continue;
                }
                inner.push(op);
            }
            let txn = Op::Txn { ops: inner };
            let p = engine_p.apply(txn.clone());
            let s = engine_s.apply(txn);
            total_txns += 1;
            total_inner_ops += n_inner;
            if p != s {
                diffs.push((workload_idx, op_idx, n_inner));
                if diffs.len() <= 3 {
                    eprintln!(
                        "TXN-RO DIVERGENCE workload={} op={} n_inner={} \
                         parallel={:?} serial={:?}",
                        workload_idx, op_idx, n_inner, p, s
                    );
                }
            }
        }
    }

    drop(engine_p);
    drop(engine_s);
    let _ = std::fs::remove_dir_all(&dir_p);
    let _ = std::fs::remove_dir_all(&dir_s);

    assert_eq!(
        total_txns,
        N_WORKLOADS * OPS_PER_WORKLOAD,
        "expected {} txns, ran {}",
        N_WORKLOADS * OPS_PER_WORKLOAD,
        total_txns
    );
    assert!(
        total_inner_ops > N_WORKLOADS * OPS_PER_WORKLOAD,
        "inner-ops count {total_inner_ops} should exceed txn count"
    );
    assert!(
        diffs.is_empty(),
        "TXN-RO oracle FAILED: {} divergences across {} all-RO Op::Txn calls \
         ({} inner reads). First 10: {:?}",
        diffs.len(),
        total_txns,
        total_inner_ops,
        diffs.iter().take(10).collect::<Vec<_>>()
    );
}

/// TXN-RO smoke 1: empty Op::Txn{ops:[]} returns Ok on both engines.
#[test]
fn txn_ro_smoke_empty_txn_returns_ok() {
    let (engine_p, dir_p) = spawn(Some(4), "txnro-empty-p");
    let (engine_s, dir_s) = spawn(None, "txnro-empty-s");
    let p = engine_p.apply(Op::Txn { ops: vec![] });
    let s = engine_s.apply(Op::Txn { ops: vec![] });
    assert_eq!(p, s, "empty Txn diverged: p={p:?} s={s:?}");
    assert_eq!(p, OpResult::Ok, "empty Txn should be Ok, got {p:?}");
    drop(engine_p);
    drop(engine_s);
    let _ = std::fs::remove_dir_all(&dir_p);
    let _ = std::fs::remove_dir_all(&dir_s);
}

/// TXN-RO smoke 2: single-inner-op Txn matches engine-level single-op.
/// Locks the invariant that a 1-inner-op Txn produces Ok (success) even
/// though the bare single-op would return Got(...) — the apply-Txn
/// contract discards inner payloads.
#[test]
fn txn_ro_smoke_single_inner_op_returns_ok() {
    let (engine_p, dir_p) = spawn(Some(4), "txnro-single-p");
    let (engine_s, dir_s) = spawn(None, "txnro-single-s");
    let op = Op::GetById {
        type_id: 1,
        id: ObjectId::from_u128(42),
    };
    let p = engine_p.apply(Op::Txn { ops: vec![op.clone()] });
    let s = engine_s.apply(Op::Txn { ops: vec![op] });
    assert_eq!(p, s, "single-inner-op Txn diverged: p={p:?} s={s:?}");
    assert_eq!(p, OpResult::Ok, "1-inner RO Txn should be Ok, got {p:?}");
    drop(engine_p);
    drop(engine_s);
    let _ = std::fs::remove_dir_all(&dir_p);
    let _ = std::fs::remove_dir_all(&dir_s);
}

/// TXN-RO smoke 3: 410-inner-op Txn (sysbench-RO shape) returns Ok and
/// matches the serial engine byte-for-byte.
#[test]
fn txn_ro_smoke_sysbench_shape_returns_ok() {
    let (engine_p, dir_p) = spawn(Some(8), "txnro-sysbench-p");
    let (engine_s, dir_s) = spawn(None, "txnro-sysbench-s");
    let mut inner = Vec::with_capacity(410);
    // 1 POINT + 4×100 SUM_RANGE expansion + 5 POINT_SELECTS = 406, +4 padding = 410
    for i in 0..410u128 {
        inner.push(Op::GetById {
            type_id: 1,
            id: ObjectId::from_u128(i % (N_ROWS as u128)),
        });
    }
    let p = engine_p.apply(Op::Txn { ops: inner.clone() });
    let s = engine_s.apply(Op::Txn { ops: inner });
    assert_eq!(p, s, "sysbench-shape Txn diverged: p={p:?} s={s:?}");
    assert_eq!(p, OpResult::Ok);
    drop(engine_p);
    drop(engine_s);
    let _ = std::fs::remove_dir_all(&dir_p);
    let _ = std::fs::remove_dir_all(&dir_s);
}

/// TXN-RO smoke 4: every Txn-permitted read variant inside one Txn.
/// Locks the 15 read variants apply-Txn permits compose correctly
/// inside an Op::Txn under the bypass. (SeqRead is NOT included —
/// apply-Txn rejects it; the bypass mirrors that rejection.)
#[test]
fn txn_ro_smoke_all_txn_permitted_variants_one_txn_returns_ok() {
    let (engine_p, dir_p) = spawn(Some(4), "txnro-allvariants-p");
    let (engine_s, dir_s) = spawn(None, "txnro-allvariants-s");
    // GetBlob{handle:0} is deliberately omitted — apply-Txn fail-fasts
    // on the NotFound (no blob with handle 0 was seeded), so the Txn
    // would return NotFound instead of Ok on both paths. The bypass
    // matches that contract; the symmetry is locked separately by
    // txn_ro_oracle_100_workloads_x_1000_txns_byte_equal.
    let inner = vec![
        Op::GetById { type_id: 1, id: ObjectId::from_u128(7) },
        Op::Describe { type_id: 1 },
        Op::FindBy {
            type_id: 1,
            field_id: 2,
            value: 0i32.to_le_bytes().to_vec(),
        },
        Op::FindByComposite {
            type_id: 2,
            fields: vec![1, 2],
            values: vec![
                ObjectId::from_u128(7).0.to_vec(),
                0u16.to_le_bytes().to_vec(),
            ],
        },
        Op::FindRange {
            type_id: 1,
            field_id: 2,
            lo: (-500i32).to_le_bytes().to_vec(),
            hi: 500i32.to_le_bytes().to_vec(),
        },
        Op::Query {
            type_id: 1,
            preds: vec![Pred {
                field_id: 3,
                op: 0,
                value: 0u16.to_le_bytes().to_vec(),
            }],
        },
        Op::QueryRows {
            type_id: 1,
            eq_preds: vec![],
            program: Program::new().push_int(1).bytes(),
            limit: 10,
            range_preds: vec![],
        },
        Op::QueryExpr {
            type_id: 1,
            program: Program::new().load(2).push_int(0).ge().bytes(),
        },
        Op::Select {
            type_id: 1,
            program: Program::new().push_int(1).bytes(),
            limit: 5,
        },
        Op::SelectFields {
            type_id: 1,
            program: Program::new().push_int(1).bytes(),
            fields: vec![2, 3],
            limit: 5,
        },
        Op::SelectSorted {
            type_id: 1,
            program: Program::new().push_int(1).bytes(),
            sort_field: 2,
            desc: false,
            offset: 0,
            limit: 5,
        },
        Op::Aggregate {
            type_id: 1,
            program: Program::new().push_int(1).bytes(),
            kind: 0,
            field_id: 2,
            range_preds: vec![],
        },
        Op::GroupAggregate {
            type_id: 1,
            program: Program::new().push_int(1).bytes(),
            group_field: 3,
            kind: 0,
            agg_field: 2,
            range_preds: vec![],
        },
        Op::Join {
            left_type: 1,
            right_type: 1,
            left_field: 2,
            right_field: 2,
            limit: 4,
        },
    ];
    // Bisect on divergence: run each inner op as a 1-op Txn on both
    // engines and report which one diverges. This gives the post-mortem
    // the specific arm to fix in the bypass validator.
    for (idx, single) in inner.iter().enumerate() {
        let p_one = engine_p.apply(Op::Txn { ops: vec![single.clone()] });
        let s_one = engine_s.apply(Op::Txn { ops: vec![single.clone()] });
        assert_eq!(p_one, s_one,
            "15-variant Txn — inner op {idx} ({:?}) diverged: p={p_one:?} s={s_one:?}",
            single.kind());
    }
    let p = engine_p.apply(Op::Txn { ops: inner.clone() });
    let s = engine_s.apply(Op::Txn { ops: inner });
    assert_eq!(p, s, "15-variant Txn diverged: p={p:?} s={s:?}");
    assert_eq!(p, OpResult::Ok);
    drop(engine_p);
    drop(engine_s);
    let _ = std::fs::remove_dir_all(&dir_p);
    let _ = std::fs::remove_dir_all(&dir_s);
}

/// TXN-RO smoke 7: Op::Txn{[SeqRead]} — SeqRead is permitted standalone
/// but rejected inside Op::Txn by apply-Txn. The bypass mirrors that
/// rejection so both engines return the same SchemaError.
#[test]
fn txn_ro_smoke_seqread_inside_txn_rejected_symmetrically() {
    let (engine_p, dir_p) = spawn(Some(4), "txnro-seqread-p");
    let (engine_s, dir_s) = spawn(None, "txnro-seqread-s");
    let txn = Op::Txn {
        ops: vec![Op::SeqRead { from: 0, limit: 4 }],
    };
    let p = engine_p.apply(txn.clone());
    let s = engine_s.apply(txn);
    assert_eq!(p, s, "SeqRead-in-Txn diverged: p={p:?} s={s:?}");
    assert!(matches!(p, OpResult::SchemaError(_)),
        "SeqRead inside Txn must SchemaError on both paths; got {p:?}");
    drop(engine_p);
    drop(engine_s);
    let _ = std::fs::remove_dir_all(&dir_p);
    let _ = std::fs::remove_dir_all(&dir_s);
}

/// TXN-RO smoke 5: mixed-RW Op::Txn (10 reads + 1 write) — both engines
/// agree on the result via the apply path. The bypass classifier rejects
/// this Txn so it falls through to apply on the parallel engine too;
/// parallel and serial must produce the same outcome (apply-Txn is
/// deterministic). Locks the apply-path symmetry under the new
/// classifier dispatch logic.
#[test]
fn txn_ro_smoke_mixed_rw_txn_falls_through_to_apply() {
    use kessel_codec::Value;
    let (engine_p, dir_p) = spawn(Some(4), "txnro-mixedrw-p");
    let (engine_s, dir_s) = spawn(None, "txnro-mixedrw-s");
    // Build a record for a new (post-seed-data) user row.
    let user_ot = ObjectType::from_def(
        "user".into(),
        vec![
            Field { field_id: 1, name: "v".into(), kind: FieldKind::U64, nullable: false },
            Field { field_id: 2, name: "score".into(), kind: FieldKind::I32, nullable: false },
            Field { field_id: 3, name: "group".into(), kind: FieldKind::U16, nullable: false },
            Field { field_id: 4, name: "name".into(), kind: FieldKind::Char(16), nullable: true },
        ],
    );
    let new_id = ObjectId::from_u128(99_999);
    let new_rec = encode(
        &user_ot,
        &[Value::Uint(7), Value::Int(0), Value::Uint(0), Value::Null],
    )
    .unwrap();
    let mut inner: Vec<Op> = Vec::with_capacity(11);
    for i in 0..10u128 {
        inner.push(Op::GetById {
            type_id: 1,
            id: ObjectId::from_u128(i),
        });
    }
    inner.push(Op::Create { type_id: 1, id: new_id, record: new_rec });
    let p = engine_p.apply(Op::Txn { ops: inner.clone() });
    let s = engine_s.apply(Op::Txn { ops: inner });
    assert_eq!(p, s, "mixed-RW Txn diverged: p={p:?} s={s:?}");
    // The write committed → Op::Ok
    assert_eq!(p, OpResult::Ok);
    // Verify the write actually persisted on both engines.
    let p_get = engine_p.apply(Op::GetById { type_id: 1, id: new_id });
    let s_get = engine_s.apply(Op::GetById { type_id: 1, id: new_id });
    assert_eq!(p_get, s_get, "post-Txn GetById diverged: p={p_get:?} s={s_get:?}");
    assert!(matches!(p_get, OpResult::Got(_)),
        "mixed-RW Txn write should have persisted on parallel engine; got {p_get:?}");
    drop(engine_p);
    drop(engine_s);
    let _ = std::fs::remove_dir_all(&dir_p);
    let _ = std::fs::remove_dir_all(&dir_s);
}

/// TXN-RO smoke 6: a Txn containing a write at position 0 (instead of
/// the end) also falls through to apply correctly, on both engines.
/// Locks the .all short-circuit doesn't accidentally let a write slip
/// through the bypass.
#[test]
fn txn_ro_smoke_write_at_front_txn_falls_through_to_apply() {
    use kessel_codec::Value;
    let (engine_p, dir_p) = spawn(Some(4), "txnro-writefront-p");
    let (engine_s, dir_s) = spawn(None, "txnro-writefront-s");
    let user_ot = ObjectType::from_def(
        "user".into(),
        vec![
            Field { field_id: 1, name: "v".into(), kind: FieldKind::U64, nullable: false },
            Field { field_id: 2, name: "score".into(), kind: FieldKind::I32, nullable: false },
            Field { field_id: 3, name: "group".into(), kind: FieldKind::U16, nullable: false },
            Field { field_id: 4, name: "name".into(), kind: FieldKind::Char(16), nullable: true },
        ],
    );
    let new_id = ObjectId::from_u128(88_888);
    let new_rec = encode(
        &user_ot,
        &[Value::Uint(1), Value::Int(0), Value::Uint(0), Value::Null],
    )
    .unwrap();
    let mut inner: Vec<Op> = Vec::with_capacity(11);
    inner.push(Op::Create { type_id: 1, id: new_id, record: new_rec });
    for i in 0..10u128 {
        inner.push(Op::GetById {
            type_id: 1,
            id: ObjectId::from_u128(i),
        });
    }
    let p = engine_p.apply(Op::Txn { ops: inner.clone() });
    let s = engine_s.apply(Op::Txn { ops: inner });
    assert_eq!(p, s, "write-at-front Txn diverged: p={p:?} s={s:?}");
    assert_eq!(p, OpResult::Ok);
    drop(engine_p);
    drop(engine_s);
    let _ = std::fs::remove_dir_all(&dir_p);
    let _ = std::fs::remove_dir_all(&dir_s);
}

// ----------------------------------------------------------------------
// SP-Perf-A-TXN-RW oracle — split-phase byte-equivalence for (R*, W*) shape.
// ----------------------------------------------------------------------
//
// V1 ships driver-level split-phase execution of mixed-RW Op::Txn{ops}
// where reads precede writes. The split is byte-equivalent to unified
// apply ONLY for this shape; read-after-write Txns fall through to
// apply unchanged. These tests lock the byte-equivalence claim.

use kesseldb_server::read_pool::{is_split_safe, read_prefix_length};

/// Helper: dispatch a mixed-RW Op::Txn via the split-phase path
/// (mirrors the bench driver's logic). Returns the verdict that
/// matches unified apply's contract for (R*, W*) shapes.
fn dispatch_split_phase(
    engine: &EngineHandle,
    ops: Vec<Op>,
) -> OpResult {
    let prefix = read_prefix_length(&ops);
    let total = ops.len();
    if prefix > 0 && prefix < total && is_split_safe(&ops[prefix..]) {
        // Split: reads then writes.
        let mut ops = ops;
        let writes = ops.split_off(prefix);
        let reads = ops;
        let read_r = engine.apply(Op::Txn { ops: reads });
        match read_r {
            OpResult::Ok => engine.apply(Op::Txn { ops: writes }),
            failed => failed,
        }
    } else {
        // No split: dispatch unified.
        engine.apply(Op::Txn { ops })
    }
}

/// TXN-RW oracle 1: 1000 random (R*, W*) Txns × unified-vs-split
/// byte-equivalent verdict + final state.
///
/// Each Txn shape:
///   - 5..15 random GetById reads (disjoint from the writes' id range)
///   - 1..4 random writes (Update on EXISTING user rows; the writes
///     pick ids from a per-Txn-disjoint "scratch" slot so the post-
///     state is deterministic across the 1000 Txns)
///
/// Engines A and B start identical. A applies via unified apply;
/// B applies via split-phase. After all 1000 Txns, every per-Txn
/// verdict matches AND every user row 0..N_ROWS Select returns
/// byte-identical data.
#[test]
fn txn_rw_split_oracle_1000_random_read_then_write_txns_byte_equivalent() {
    // Engine A — unified apply path.
    let (engine_a, dir_a) = spawn(None, "txnrw-unified");
    // Engine B — split-phase dispatch path.
    let (engine_b, dir_b) = spawn(None, "txnrw-split");

    // Same RNG seed → same Txn sequence for both engines.
    let mut rng = Rng::new(0xC0DE_FACE);
    let mut diff_verdicts: Vec<(usize, OpResult, OpResult)> = Vec::new();

    for txn_idx in 0..1000usize {
        // Build a random (R*, W*) Txn shape.
        let n_reads = 5 + (rng.below(11) as usize); // 5..=15
        let n_writes = 1 + (rng.below(4) as usize); // 1..=4
        let mut ops = Vec::with_capacity(n_reads + n_writes);
        // Random reads over existing rows.
        for _ in 0..n_reads {
            ops.push(Op::GetById {
                type_id: 1,
                id: ObjectId::from_u128((rng.below(N_ROWS as u64) as u128)),
            });
        }
        // Writes on per-Txn-disjoint id range so the two engines'
        // post-states converge deterministically. Each Txn uses ids in
        // [txn_idx * 100, txn_idx * 100 + n_writes) — far from the
        // seeded 0..N_ROWS range so we won't collide with reads.
        //
        // The write op is Update on a NEW id (Op::Update returns
        // NotFound if the id doesn't exist). To make writes succeed
        // we first Create the id at the start of each engine's loop —
        // but that complicates the test. Instead: use Update on an
        // EXISTING id (within N_ROWS) and ensure the new record is
        // valid (encoded user schema). Both engines see the same
        // sequence of (target_id, record_bytes) writes ⇒ deterministic
        // convergence.
        let user_ot = ObjectType::from_def(
            "user".into(),
            vec![
                Field { field_id: 1, name: "v".into(), kind: FieldKind::U64, nullable: false },
                Field { field_id: 2, name: "score".into(), kind: FieldKind::I32, nullable: false },
                Field { field_id: 3, name: "group".into(), kind: FieldKind::U16, nullable: false },
                Field { field_id: 4, name: "name".into(), kind: FieldKind::Char(16), nullable: true },
            ],
        );
        for w in 0..n_writes {
            let target = (((txn_idx * 7 + w) as u64) % (N_ROWS as u64)) as u128;
            let new_score = ((rng.below(1000) as i32) - 500) as i128;
            let new_group = (rng.below(50) as u128);
            let mut name_b = vec![0u8; 16];
            let s = format!("upd{txn_idx}_{w}");
            let len = s.len().min(16);
            name_b[..len].copy_from_slice(&s.as_bytes()[..len]);
            let rec = encode(
                &user_ot,
                &[
                    Value::Uint((target).wrapping_mul(7)),
                    Value::Int(new_score),
                    Value::Uint(new_group),
                    Value::Blob(name_b),
                ],
            )
            .unwrap();
            ops.push(Op::Update {
                type_id: 1,
                id: ObjectId::from_u128(target),
                record: rec,
            });
        }

        // Sanity: this Txn IS split-eligible.
        let prefix = read_prefix_length(&ops);
        assert_eq!(prefix, n_reads, "txn {txn_idx}: prefix={prefix}, expected {n_reads}");
        assert!(is_split_safe(&ops[prefix..]), "txn {txn_idx}: suffix has trailing read");

        // Apply on A (unified) and B (split). Verdicts must match.
        let verdict_a = engine_a.apply(Op::Txn { ops: ops.clone() });
        let verdict_b = dispatch_split_phase(&engine_b, ops);

        if verdict_a != verdict_b {
            diff_verdicts.push((txn_idx, verdict_a, verdict_b));
            if diff_verdicts.len() <= 3 {
                eprintln!(
                    "TXN-RW SPLIT DIVERGENCE txn={} unified={:?} split={:?}",
                    diff_verdicts.last().unwrap().0,
                    diff_verdicts.last().unwrap().1,
                    diff_verdicts.last().unwrap().2
                );
            }
        }
    }

    // Final-state byte-equivalence: Select every user row from both
    // engines and compare.
    let sel_a = engine_a.apply(Op::Select { type_id: 1, program: vec![], limit: 0 });
    let sel_b = engine_b.apply(Op::Select { type_id: 1, program: vec![], limit: 0 });

    drop(engine_a);
    drop(engine_b);
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);

    assert!(
        diff_verdicts.is_empty(),
        "TXN-RW split oracle FAILED: {} verdict divergences across 1000 Txns. First 3: {:?}",
        diff_verdicts.len(),
        diff_verdicts.iter().take(3).collect::<Vec<_>>()
    );
    assert_eq!(
        sel_a, sel_b,
        "TXN-RW split oracle: final-state Select diverged between unified and split engines"
    );
}

/// TXN-RW smoke: a single sysbench-shape Txn (10 reads, 4 writes) via
/// unified-vs-split is byte-equivalent. This is the headline-workload
/// smoke complementing the bulk oracle above.
#[test]
fn txn_rw_split_smoke_sysbench_shape_byte_equivalent() {
    let (engine_a, dir_a) = spawn(None, "txnrw-sm-unified");
    let (engine_b, dir_b) = spawn(None, "txnrw-sm-split");

    let user_ot = ObjectType::from_def(
        "user".into(),
        vec![
            Field { field_id: 1, name: "v".into(), kind: FieldKind::U64, nullable: false },
            Field { field_id: 2, name: "score".into(), kind: FieldKind::I32, nullable: false },
            Field { field_id: 3, name: "group".into(), kind: FieldKind::U16, nullable: false },
            Field { field_id: 4, name: "name".into(), kind: FieldKind::Char(16), nullable: true },
        ],
    );

    // 10 reads + 4 writes (Update on existing ids).
    let mut ops = Vec::with_capacity(14);
    for i in 0..10u128 {
        ops.push(Op::GetById { type_id: 1, id: ObjectId::from_u128(i) });
    }
    for w in 0..4u128 {
        let id = ObjectId::from_u128(100 + w);
        let rec = encode(
            &user_ot,
            &[
                Value::Uint((100 + w).wrapping_mul(7)),
                Value::Int(42),
                Value::Uint(7),
                Value::Null,
            ],
        )
        .unwrap();
        ops.push(Op::Update { type_id: 1, id, record: rec });
    }

    let verdict_a = engine_a.apply(Op::Txn { ops: ops.clone() });
    let verdict_b = dispatch_split_phase(&engine_b, ops);

    // Both should return Ok.
    assert_eq!(verdict_a, OpResult::Ok, "unified verdict {:?}", verdict_a);
    assert_eq!(verdict_b, OpResult::Ok, "split verdict {:?}", verdict_b);

    // Post-state: ids 100..104 must be byte-identical on both engines.
    for i in 100..104u128 {
        let a = engine_a.apply(Op::GetById { type_id: 1, id: ObjectId::from_u128(i) });
        let b = engine_b.apply(Op::GetById { type_id: 1, id: ObjectId::from_u128(i) });
        assert_eq!(a, b, "post-state diverged at id {i}: a={a:?} b={b:?}");
    }

    drop(engine_a);
    drop(engine_b);
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}

/// TXN-RW smoke: read-after-write Txn falls through to unified apply
/// (the dispatcher's is_split_safe guard catches it). Verdict matches
/// unified-vs-"split-but-fell-through" because both go through the
/// same path.
#[test]
fn txn_rw_split_smoke_read_after_write_falls_through_to_unified() {
    let (engine_a, dir_a) = spawn(None, "txnrw-raw-unified");
    let (engine_b, dir_b) = spawn(None, "txnrw-raw-fallthrough");

    let user_ot = ObjectType::from_def(
        "user".into(),
        vec![
            Field { field_id: 1, name: "v".into(), kind: FieldKind::U64, nullable: false },
            Field { field_id: 2, name: "score".into(), kind: FieldKind::I32, nullable: false },
            Field { field_id: 3, name: "group".into(), kind: FieldKind::U16, nullable: false },
            Field { field_id: 4, name: "name".into(), kind: FieldKind::Char(16), nullable: true },
        ],
    );
    let id = ObjectId::from_u128(42);
    let new_rec = encode(
        &user_ot,
        &[Value::Uint(999), Value::Int(1), Value::Uint(2), Value::Null],
    )
    .unwrap();
    let ops = vec![
        Op::GetById { type_id: 1, id },
        Op::Update { type_id: 1, id, record: new_rec },
        Op::GetById { type_id: 1, id },
    ];

    // The dispatcher should NOT split (suffix has a trailing read).
    let prefix = read_prefix_length(&ops);
    let should_split =
        prefix > 0 && prefix < ops.len() && is_split_safe(&ops[prefix..]);
    assert!(!should_split, "R-W-R must NOT split");

    let verdict_a = engine_a.apply(Op::Txn { ops: ops.clone() });
    let verdict_b = dispatch_split_phase(&engine_b, ops);
    assert_eq!(verdict_a, verdict_b, "R-W-R unified vs fall-through diverged");

    drop(engine_a);
    drop(engine_b);
    let _ = std::fs::remove_dir_all(&dir_a);
    let _ = std::fs::remove_dir_all(&dir_b);
}
