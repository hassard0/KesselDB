//! `embedded` — KesselDB as a library inside your Rust program.
//!
//! Demonstrates the in-process API: no network, no socket, no auth. You
//! get an `EngineHandle` and apply SQL or `Op`s directly. Read paths
//! take the SP-Perf-A bypass straight to the shared state machine
//! under a `RwLock`, so latency is the same ~sub-µs the in-process
//! bench measures — there is no network round-trip to amortise.
//!
//! Run from the repo root:
//!     cargo run --release --example embedded -p kesseldb-server
//!
//! What it shows:
//!   1. Open a fresh data dir under a tempdir (auto-cleaned).
//!   2. Spawn the engine with `read_workers = Some(0)` so the
//!      Perf-A read-bypass is enabled — read ops dispatch on the
//!      submitting thread under an `RwLock::read()`, writes still
//!      go through the engine queue's serial-apply.
//!   3. Run SQL DDL + DML via `EngineHandle::sql`.
//!   4. Run a typed `Op` via `EngineHandle::apply` (the path bench
//!      uses; skips the SQL compiler entirely).
//!   5. Take a consistent on-disk snapshot via `EngineHandle::snapshot`.
//!
//! The example uses ONLY public APIs of `kesseldb-server` + the
//! workspace's other public crates (`kessel-proto`, `kessel-codec`,
//! `kessel-catalog`). No private re-exports, no `pub(crate)` shortcuts.

use kesseldb_server::{spawn_engine_cfg, EngineHandle, ServerConfig};
use kessel_catalog::{encode_type_def, Field, FieldKind};
use kessel_codec::Value;
use kessel_proto::{ObjectId, Op, OpResult};

fn main() {
    // Fresh data dir — auto-cleaned on process exit by the tempdir
    // pattern (we manually remove the dir at the end so the example
    // doesn't litter `/tmp`).
    let data_dir = std::env::temp_dir().join(format!(
        "kesseldb-embedded-example-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    println!("→ data dir: {}", data_dir.display());

    // Spawn the engine. `read_workers = Some(0)` opts into the
    // SP-Perf-A read-bypass (reads run on the submitting thread under
    // an `RwLock::read()`; writes still serialise through the engine
    // thread's queue). `Some(N>0)` would also spawn a pool — useful
    // for fairness under bursty workloads.
    let cfg = ServerConfig { read_workers: Some(0), ..Default::default() };
    let engine: EngineHandle =
        spawn_engine_cfg(&data_dir, &cfg).expect("engine open");

    // ── 1) SQL DDL + DML via the in-process SQL fast path ──────────
    println!("→ creating table via SQL …");
    let r = engine.sql("CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)");
    assert!(matches!(r, OpResult::TypeCreated(_)), "create acct: {r:?}");

    let r = engine.sql("INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)");
    assert!(matches!(r, OpResult::Ok), "insert 1: {r:?}");
    let r = engine.sql("INSERT INTO acct ID 2 (owner, bal) VALUES (100, 999)");
    assert!(matches!(r, OpResult::Ok), "insert 2: {r:?}");

    let r = engine.sql("SELECT SUM(bal) FROM acct WHERE owner = 100");
    let sum = match r {
        OpResult::Got(b) => i128::from_le_bytes(b[..16].try_into().unwrap()),
        o => panic!("sum got: {o:?}"),
    };
    assert_eq!(sum, 1049);
    println!("   SUM(bal) WHERE owner=100 = {sum}");

    // ── 2) Op fast path — bypass the SQL compiler entirely ─────────
    //
    // This is what `kessel-bench` uses to drive 4.8M ops/sec at
    // sub-µs p50. We build the typed Op directly. The Op kind table
    // is documented in `kessel-proto`; here we do a single
    // `Op::GetById` against acct ID 1.
    println!("→ direct Op::GetById …");
    let r = engine.apply(Op::GetById { type_id: 1, id: ObjectId::from_u128(1) });
    match r {
        OpResult::Got(rec) => println!("   raw row bytes: {} bytes", rec.len()),
        o => panic!("get_by_id: {o:?}"),
    }

    // ── 3) Build a row via the typed codec, INSERT it, read it back ─
    //
    // Demonstrates that embedded code can craft records exactly the
    // way the wire path does — same `kessel_codec::encode`, same
    // `Op::Create`. No "embedded shortcut" magic: it is the same SM
    // apply path the network surface drives.
    println!("→ typed Op::Create via the codec …");
    let typedef = encode_type_def(
        "kv",
        &[
            Field { field_id: 0, name: "k".into(), kind: FieldKind::U64, nullable: false },
            Field { field_id: 0, name: "v".into(), kind: FieldKind::U64, nullable: false },
        ],
    );
    let r = engine.apply(Op::CreateType { def: typedef.clone() });
    let kv_type_id = match r {
        OpResult::TypeCreated(t) => t,
        o => panic!("create kv: {o:?}"),
    };
    // Build one record using the catalog the engine just created.
    let kv_ot = kessel_catalog::ObjectType::from_def(
        "kv".into(),
        vec![
            Field { field_id: 1, name: "k".into(), kind: FieldKind::U64, nullable: false },
            Field { field_id: 2, name: "v".into(), kind: FieldKind::U64, nullable: false },
        ],
    );
    let rec = kessel_codec::encode(&kv_ot, &[Value::Uint(7), Value::Uint(42)])
        .expect("encode");
    let r = engine.apply(Op::Create {
        type_id: kv_type_id,
        id: ObjectId::from_u128(7),
        record: rec,
    });
    assert!(matches!(r, OpResult::Ok), "insert kv: {r:?}");

    let r = engine.apply(Op::GetById {
        type_id: kv_type_id,
        id: ObjectId::from_u128(7),
    });
    let bytes = match r {
        OpResult::Got(b) => b,
        o => panic!("get kv: {o:?}"),
    };
    let vals = kessel_codec::decode(&kv_ot, &bytes).expect("decode");
    println!("   round-tripped kv row → {vals:?}");

    // ── 4) Hot snapshot ────────────────────────────────────────────
    println!("→ taking on-disk snapshot …");
    let snap = data_dir.with_extension("snapshot");
    let _ = std::fs::remove_dir_all(&snap);
    engine.snapshot(&snap).expect("snapshot");
    let entries = std::fs::read_dir(&snap).expect("read snap").count();
    println!("   snapshot dir contains {entries} files at {}", snap.display());

    // ── 5) Stats — proof we drove real work through the engine ─────
    let stats = engine.stats();
    println!(
        "→ stats: applied_ops={}  digest=0x{:08x}  uptime={}s  read_pool={}",
        stats.applied_ops,
        stats.digest,
        stats.uptime_secs,
        engine.read_pool_workers()
    );

    // Cleanup.
    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_dir_all(&snap);
    println!("✓ embedded example complete");
}
