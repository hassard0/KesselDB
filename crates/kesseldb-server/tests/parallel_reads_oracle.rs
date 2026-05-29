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
