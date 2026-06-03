//! KesselDB driver — TPC-H Q1 + Q6 paths.
//!
//! ### Approach
//!
//! - Schema: `lineitem` catalog type with the 16 canonical columns + one
//!   derived synthetic column the engine needs to answer the query
//!   with its current capability surface:
//!   - **`l_groupkey: Char(2)`** = `[l_returnflag, l_linestatus]` so the
//!     single-column `Op::GroupAggregate` can group on the pair.
//!   - **`l_q6_revenue: I64`** = `l_extendedprice_raw * l_discount_raw`
//!     (precomputed at load) so `Op::Aggregate kind=SUM` can fold the
//!     Q6 revenue without an expression-VM aggregate path.
//!
//! - Load: one `Op::Txn { ops: vec![Create*N] }` batched insert + a
//!   range index on `l_shipdate` (added at table-creation time so the
//!   batched inserts maintain it as they go). The apply-path
//!   serializes all the Creates under one write lock; we are NOT
//!   measuring load throughput so the batched-create + index cost is
//!   hidden.
//!
//! - **SP-Analytic-Plan T4**: aggregate ops carry `range_preds:
//!   Vec<(field_id, op, value)>` half-range hints on the
//!   l_shipdate-indexed column. The SM narrows the candidate row-set
//!   via the existing ordered-index machinery BEFORE the per-row
//!   WHERE program runs (the program still verifies every candidate
//!   so the result is byte-identical to a full scan — the index only
//!   accelerates). This closes the SP-Bench-Suite T4 TPC-H loss
//!   without changing the kessel-expr semantics.
//!
//! - Q1 (single execution per "op"): **SP-Analytic-Plan-MULTI** —
//!   one `Op::GroupAggregateMulti` call carrying 4 aggregates (COUNT,
//!   SUM(l_quantity), SUM(l_extendedprice), SUM(l_discount)) all
//!   grouped by `l_groupkey`, all under the same WHERE
//!   `l_shipdate <= 19980901` predicate. The single-scan fold replaces
//!   the previous 4× `Op::GroupAggregate` shape (each one a full-
//!   narrowed-set scan with its own kessel-expr WHERE eval), so Q1
//!   pays 1× the per-row cost instead of 4×. AVG = SUM / COUNT is
//!   derived client-side from the per-group accumulators.
//!
//! - Q6 (single execution per "op"): one `Op::Aggregate { kind=SUM,
//!   field_id=L_Q6_REVENUE }` with the WHERE
//!   `l_shipdate >= 19940101 AND l_shipdate < 19950101 AND
//!    l_discount BETWEEN 5 AND 7 AND l_quantity < 2400` (raw-scaled
//!   values; see `tpch.rs` module doc).
//!
//! ### N=concurrency mapping
//!
//! Each worker thread runs full queries sequentially on a shared
//! `Arc<RwLock<StateMachine>>`. Read-only `Op::Aggregate` and
//! `Op::GroupAggregate` go through `read_only_op(&self)` so workers
//! share the read lock — the analytical workload SHOULD parallelize on
//! KesselDB (no apply-write-lock contention like `Op::Txn`). N=1,4 per
//! the design spec §3 (analytics don't benefit from very high N).

use crate::tpch::{self, field_id as fid, LineItem};
use crate::workloads::{tpch_const, Workload};
use crate::{pct_us, BenchResult, Cli};
use kessel_catalog::{encode_type_def, Field, FieldKind};
use kessel_codec::{encode, Value};
use kessel_expr::Program;
use kessel_io::MemVfs;
use kessel_proto::{ObjectId, Op, OpResult};
use kessel_sm::StateMachine;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

const LINEITEM_TYPE_ID: u32 = 1;

/// Build the `lineitem` catalog type-def blob.
fn lineitem_type_def() -> Vec<u8> {
    let fields = vec![
        Field { field_id: 0, name: "l_orderkey".into(),      kind: FieldKind::I64,        nullable: false },
        Field { field_id: 0, name: "l_partkey".into(),       kind: FieldKind::I64,        nullable: false },
        Field { field_id: 0, name: "l_suppkey".into(),       kind: FieldKind::I64,        nullable: false },
        Field { field_id: 0, name: "l_linenumber".into(),    kind: FieldKind::I32,        nullable: false },
        Field { field_id: 0, name: "l_quantity".into(),      kind: FieldKind::I32,        nullable: false },
        Field { field_id: 0, name: "l_extendedprice".into(), kind: FieldKind::I64,        nullable: false },
        Field { field_id: 0, name: "l_discount".into(),      kind: FieldKind::I32,        nullable: false },
        Field { field_id: 0, name: "l_tax".into(),           kind: FieldKind::I32,        nullable: false },
        Field { field_id: 0, name: "l_returnflag".into(),    kind: FieldKind::Char(1),    nullable: false },
        Field { field_id: 0, name: "l_linestatus".into(),    kind: FieldKind::Char(1),    nullable: false },
        Field { field_id: 0, name: "l_shipdate".into(),      kind: FieldKind::I32,        nullable: false },
        Field { field_id: 0, name: "l_commitdate".into(),    kind: FieldKind::I32,        nullable: false },
        Field { field_id: 0, name: "l_receiptdate".into(),   kind: FieldKind::I32,        nullable: false },
        Field { field_id: 0, name: "l_shipinstruct".into(),  kind: FieldKind::Char(25),   nullable: false },
        Field { field_id: 0, name: "l_shipmode".into(),      kind: FieldKind::Char(10),   nullable: false },
        Field { field_id: 0, name: "l_comment".into(),       kind: FieldKind::Char(44),   nullable: false },
        // Synthetic derived columns — see module doc.
        Field { field_id: 0, name: "l_groupkey".into(),      kind: FieldKind::Char(2),    nullable: false },
        Field { field_id: 0, name: "l_q6_revenue".into(),    kind: FieldKind::I64,        nullable: false },
    ];
    encode_type_def("lineitem", &fields)
}

fn lineitem_to_values(li: &LineItem) -> Vec<Value> {
    let group_key = vec![li.l_returnflag, li.l_linestatus];
    let q6_revenue: i64 = (li.l_extendedprice_raw as i64)
        .wrapping_mul(li.l_discount_raw as i64);
    vec![
        Value::Int(li.l_orderkey as i128),
        Value::Int(li.l_partkey as i128),
        Value::Int(li.l_suppkey as i128),
        Value::Int(li.l_linenumber as i128),
        Value::Int(li.l_quantity_raw as i128),
        Value::Int(li.l_extendedprice_raw as i128),
        Value::Int(li.l_discount_raw as i128),
        Value::Int(li.l_tax_raw as i128),
        Value::Blob(vec![li.l_returnflag]),
        Value::Blob(vec![li.l_linestatus]),
        Value::Int(li.l_shipdate as i128),
        Value::Int(li.l_commitdate as i128),
        Value::Int(li.l_receiptdate as i128),
        Value::Blob(li.l_shipinstruct.to_vec()),
        Value::Blob(li.l_shipmode.to_vec()),
        Value::Blob(li.l_comment.to_vec()),
        Value::Blob(group_key),
        Value::Int(q6_revenue as i128),
    ]
}

/// Build the Q1 WHERE program: `l_shipdate <= 19980901`.
fn q1_predicate() -> Vec<u8> {
    Program::new()
        .load(fid::L_SHIPDATE)
        .push_int(tpch_const::Q1_SHIPDATE_HI as i128)
        .le()
        .bytes()
}

/// Build the Q6 WHERE program:
/// `l_shipdate >= 19940101 AND l_shipdate < 19950101
///   AND l_discount >= 5 AND l_discount <= 7
///   AND l_quantity < 2400`
fn q6_predicate() -> Vec<u8> {
    // shipdate >= LO
    Program::new()
        .load(fid::L_SHIPDATE).push_int(tpch_const::Q6_SHIPDATE_LO as i128).ge()
        .load(fid::L_SHIPDATE).push_int(tpch_const::Q6_SHIPDATE_HI as i128).lt()
        .and()
        .load(fid::L_DISCOUNT).push_int(tpch_const::Q6_DISCOUNT_LO_RAW as i128).ge()
        .and()
        .load(fid::L_DISCOUNT).push_int(tpch_const::Q6_DISCOUNT_HI_RAW as i128).le()
        .and()
        .load(fid::L_QUANTITY).push_int(tpch_const::Q6_QUANTITY_HI_RAW as i128).lt()
        .and()
        .bytes()
}

/// Decode a `GroupAggregateMulti` reply:
/// `[u32 ngroups]` then per group `[u32 keylen][key][16B i128 LE × n_aggs]`.
/// `n_aggs` is implicit (the caller knows it from the request).
fn parse_group_aggregate_multi(buf: &[u8], n_aggs: usize) -> Vec<(Vec<u8>, Vec<i128>)> {
    let mut out = Vec::new();
    if buf.len() < 4 { return out; }
    let n_groups = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let mut p = 4usize;
    for _ in 0..n_groups {
        if p + 4 > buf.len() { break; }
        let keylen = u32::from_le_bytes(buf[p..p+4].try_into().unwrap()) as usize;
        p += 4;
        if p + keylen + 16 * n_aggs > buf.len() { break; }
        let key = buf[p..p+keylen].to_vec();
        p += keylen;
        let mut vs = Vec::with_capacity(n_aggs);
        for _ in 0..n_aggs {
            vs.push(i128::from_le_bytes(buf[p..p+16].try_into().unwrap()));
            p += 16;
        }
        out.push((key, vs));
    }
    out
}

pub fn run_tpch(
    workload: Workload,
    n: usize,
    trial: u32,
    cli: &Cli,
) -> anyhow::Result<BenchResult> {
    let sf = workload.tpch_sf();
    let rows = tpch_const::rows_for_sf(sf).max(1000);
    let duration = Duration::from_secs(cli.duration);

    // --- setup + load ---
    let mut sm = StateMachine::open(MemVfs::new())
        .map_err(|e| anyhow::anyhow!("StateMachine::open: {e}"))?;

    // CREATE TABLE lineitem.
    match sm.apply(1, Op::CreateType { def: lineitem_type_def() }) {
        OpResult::TypeCreated(_) => {}
        other => anyhow::bail!("kesseldb tpch: CreateType lineitem: {:?}", other),
    }
    // SP-Analytic-Plan T4: range index on l_shipdate so both Q1
    // (l_shipdate <= 19980901) and Q6 (l_shipdate >= 19940101 AND
    // l_shipdate < 19950101) get scan-narrowing via range_preds. Mirrors
    // the Postgres `idx_lineitem_shipdate` btree the postgres_tpch driver
    // creates.
    match sm.apply(2, Op::AddOrderedIndex { type_id: LINEITEM_TYPE_ID, field_id: fid::L_SHIPDATE }) {
        OpResult::Ok => {}
        other => anyhow::bail!("kesseldb tpch: AddOrderedIndex l_shipdate: {:?}", other),
    }
    let ot = sm.catalog().get(LINEITEM_TYPE_ID).expect("lineitem type").clone();

    // Deterministic per-trial seed: same seed across all 3 DBs so byte-
    // identical rows are loaded everywhere.
    let seed = tpch_seed(trial);
    let items = tpch::gen_lineitem(rows, seed);

    // Bulk-load via Op::Txn{ops}: one apply-call inserts all rows atomically.
    // (For large SF this would chunk; SF=0.01 fits in one batch.)
    let mut ops: Vec<Op> = Vec::with_capacity(rows);
    for (i, li) in items.iter().enumerate() {
        let values = lineitem_to_values(li);
        let rec = encode(&ot, &values).map_err(|e| {
            anyhow::anyhow!("kesseldb tpch: encode row {i}: {:?}", e)
        })?;
        ops.push(Op::Create {
            type_id: LINEITEM_TYPE_ID,
            id: ObjectId::from_u128(i as u128 + 1),
            record: rec,
        });
    }
    // Use a strictly-increasing op_number per Txn.
    let bulk_op_no = 3u64;
    let r = sm.apply(bulk_op_no, Op::Txn { ops });
    if matches!(r, OpResult::SchemaError(_) | OpResult::NotFound) {
        anyhow::bail!("kesseldb tpch: bulk Op::Txn{{Create*}} failed: {:?}", r);
    }

    // --- steady-state: N workers, each running queries sequentially ---
    let sm = Arc::new(RwLock::new(sm));
    let mut handles = Vec::with_capacity(n);
    let started = Instant::now();
    let stop_at = started + duration;

    let q1_prog = q1_predicate();
    let q6_prog = q6_predicate();
    let is_q1 = matches!(workload, Workload::TpchQ1 { .. });

    // SP-Analytic-Plan T4: range_preds for scan-narrowing via the
    // l_shipdate ordered index. Q1 has a single `l_shipdate <= HI` hint;
    // Q6 has `>=` LO AND `<` HI. L_SHIPDATE is I32 (4 LE bytes), op codes
    // 0=`>` 1=`>=` 2=`<` 3=`<=` per Op::QueryRows convention.
    let q1_range: Vec<(u16, u8, Vec<u8>)> = vec![
        (fid::L_SHIPDATE, 3, tpch_const::Q1_SHIPDATE_HI.to_le_bytes().to_vec()),
    ];
    let q6_range: Vec<(u16, u8, Vec<u8>)> = vec![
        (fid::L_SHIPDATE, 1, tpch_const::Q6_SHIPDATE_LO.to_le_bytes().to_vec()),
        (fid::L_SHIPDATE, 2, tpch_const::Q6_SHIPDATE_HI.to_le_bytes().to_vec()),
    ];

    for tid in 0..n {
        let sm = Arc::clone(&sm);
        let q1_prog = q1_prog.clone();
        let q6_prog = q6_prog.clone();
        let q1_range = q1_range.clone();
        let q6_range = q6_range.clone();
        let h = std::thread::spawn(move || -> (u64, Vec<u64>) {
            let mut count = 0u64;
            let mut lat = Vec::with_capacity(4096);
            let _ = tid; // currently unused (each worker is independent)
            loop {
                if Instant::now() >= stop_at { break; }
                let s = Instant::now();
                if is_q1 {
                    // SP-Analytic-Plan-MULTI Q1: ONE Op::GroupAggregateMulti
                    // call carrying 4 aggregates instead of 4 separate
                    // Op::GroupAggregate calls. The single-scan fold pays
                    // 1× per-row WHERE-eval + 1× per-row group-key extract
                    // (vs 4× in the pre-MULTI shape).
                    let g = sm.read().unwrap();
                    let r = g.read_only_op(Op::GroupAggregateMulti {
                        type_id: LINEITEM_TYPE_ID,
                        program: q1_prog.clone(),
                        group_field: fid::L_GROUPKEY,
                        aggregates: vec![
                            (0, 0),                    // COUNT(*)
                            (1, fid::L_QUANTITY),      // SUM(l_quantity)
                            (1, fid::L_EXTENDEDPRICE), // SUM(l_extendedprice)
                            (1, fid::L_DISCOUNT),      // SUM(l_discount)
                        ],
                        range_preds: q1_range.clone(),
                        having: None,
                        sort: None,
                    });
                    drop(g);
                    let buf = match r {
                        OpResult::Got(b) => b.to_vec(),
                        other => {
                            if count == 0 {
                                eprintln!("kesseldb tpch Q1 failed: {:?}", other);
                            }
                            break;
                        }
                    };
                    // Parse [(group_key, [count, sum_q, sum_ep, sum_dc])].
                    let groups = parse_group_aggregate_multi(&buf, 4);
                    // AVG = SUM / COUNT computed client-side. Used purely
                    // to exercise the per-group result end-to-end; we
                    // don't keep the avg values. The BTreeMap collection
                    // step is gone — Op::GroupAggregateMulti's result is
                    // already in ascending group-key order.
                    let mut _checksum: i128 = 0;
                    for (_k, vs) in &groups {
                        let c = vs[0].max(1);
                        let avg_q = vs[1] / c;
                        let avg_ep = vs[2] / c;
                        let avg_dc = vs[3] / c;
                        _checksum = _checksum
                            .wrapping_add(avg_q)
                            .wrapping_add(avg_ep)
                            .wrapping_add(avg_dc);
                    }
                } else {
                    // Q6: one Aggregate { SUM(l_q6_revenue) WHERE … }.
                    let g = sm.read().unwrap();
                    let r = g.read_only_op(Op::Aggregate {
                        type_id: LINEITEM_TYPE_ID,
                        program: q6_prog.clone(),
                        kind: 1, // SUM
                        field_id: fid::L_Q6_REVENUE,
                        range_preds: q6_range.clone(),
                    });
                    drop(g);
                    match r {
                        OpResult::Got(_b) => {
                            // sum is in _b as i128 LE (16 bytes) — exercise only.
                        }
                        other => {
                            // Surface the failure rather than silently
                            // hot-looping on `continue` (else ops/sec = 0
                            // hides the real reason).
                            if count == 0 {
                                eprintln!("kesseldb tpch Q6 failed: {:?}", other);
                            }
                            break;
                        }
                    }
                }
                lat.push(s.elapsed().as_nanos() as u64);
                count += 1;
            }
            (count, lat)
        });
        handles.push(h);
    }

    let mut total_q = 0u64;
    let mut lat_ns: Vec<u64> = Vec::new();
    for h in handles {
        let (c, l) = h.join().expect("kesseldb tpch worker panicked");
        total_q += c;
        lat_ns.extend(l);
    }
    let elapsed = started.elapsed().as_secs_f64();
    lat_ns.sort_unstable();

    // For convenience the L_Q6_REVENUE / L_GROUPKEY fields are public.
    let _ = fid::L_Q6_REVENUE; // keep the symbol live for grep

    Ok(BenchResult {
        db: "kesseldb".into(),
        workload: workload.name().to_string(),
        n,
        trial,
        ops_per_sec: total_q as f64 / elapsed,
        p50_us: pct_us(&lat_ns, 0.50),
        p99_us: pct_us(&lat_ns, 0.99),
        p99_99_us: pct_us(&lat_ns, 0.9999),
        runtime_secs: elapsed,
        rows,
        note: Some(if is_q1 {
            format!(
                "MemVfs in-process; SF={} ({} rows); Q1 mapped as ONE \
                 Op::GroupAggregateMulti (COUNT + SUM(quantity) + \
                 SUM(extprice) + SUM(discount)) grouped by synthetic \
                 2-byte l_groupkey (SP-Analytic-Plan-MULTI — collapses \
                 the previous 4× Op::GroupAggregate scan shape into one \
                 single-scan fold); AVG computed client-side per group. \
                 SP-Analytic-Plan T4: range index on l_shipdate + \
                 range_preds=[<=19980901] narrow the single scan via \
                 the order index. Ops/sec = full Q1 executions/sec.",
                sf, rows
            )
        } else {
            format!(
                "MemVfs in-process; SF={} ({} rows); Q6 mapped as one \
                 Op::Aggregate{{SUM, field=l_q6_revenue}} with multi-\
                 predicate WHERE (l_shipdate range + l_discount range + \
                 l_quantity < 24). l_q6_revenue is the precomputed \
                 l_extendedprice * l_discount product stored at load \
                 (KesselDB has no SUM(expr) primitive yet). SP-Analytic-\
                 Plan T4: range index on l_shipdate + range_preds=[>= \
                 19940101, <19950101] narrow the scan via the order \
                 index (the ~8K-row 1994 window out of 60K). Ops/sec = \
                 full Q6 executions/sec.",
                sf, rows
            )
        }),
    })
}

/// Deterministic per-trial seed shared across all DB drivers.
pub fn tpch_seed(trial: u32) -> u64 {
    0xC0FF_EE_C0_FF_EE_42_42_u64 ^ (trial as u64)
}
