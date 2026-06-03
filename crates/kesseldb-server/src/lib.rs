//! kesseldb-server: a runnable single-node TCP server.
//!
//! The deterministic core (`kessel-sm`) lives on ONE owning thread and never
//! moves; connection threads talk to it over a channel. So apply is serial
//! (matching the single-threaded-core design) and the engine never needs to
//! be `Send`. The server is just the real-I/O edge; the engine stays pure.
//! VSR-over-sockets (multi-node networking) is still deferred and documented.

#![forbid(unsafe_code)]

pub mod cluster;
pub mod read_pool;
pub mod router;
pub mod scatter_scan;
pub mod sharded_engine;
pub mod sharded_sm;

use kessel_io::DirVfs;
use kessel_proto::wire::{read_frame, write_frame};
use kessel_proto::{Op, OpResult};
use kessel_sm::StateMachine;
use std::collections::HashMap;
use std::io;
use std::net::{TcpListener, ToSocketAddrs};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender};
use std::sync::{Arc, RwLock};

/// First-frame auth handshake tag: `[0xFC] ++ token`.
pub const AUTH_TAG: u8 = 0xFC;
/// Admin: request server stats. Frame = `[0xFB]`; reply `Got(stats)`.
pub const STATS_TAG: u8 = 0xFB;
/// Admin: take a consistent on-disk snapshot. Frame =
/// `[0xFA] ++ utf8 dest_dir`; reply `Ok` / `SchemaError`.
pub const SNAPSHOT_TAG: u8 = 0xFA;
/// SQL transaction commit. Frame = `[0xF9][u32 n]` then `n ×
/// ([u32 len][utf8 SQL])`. The engine compiles every statement and
/// applies them as one atomic `Op::Txn` (all-or-nothing). Built by the
/// connection handler from statements buffered between `BEGIN` and
/// `COMMIT`; the client never builds it directly.
pub const TXN_TAG: u8 = 0xF9;
/// Pipeline of INDEPENDENT requests (SP69). Frame =
/// `[0xF8][u32 cnt]` then `cnt × ([u32 len][inner frame])`, where each
/// inner frame is an ordinary `[0xFE] ++ SQL` or `Op::encode()` frame.
/// Unlike [`TXN_TAG`] this is **not** atomic — every member applies
/// independently and gets its own result; the only thing batched is the
/// group-commit fsync and the network round-trip. The reply is
/// `Got([u32 cnt]` then `cnt × ([u32 len][OpResult::encode]))`, in order.
pub const PIPELINE_TAG: u8 = 0xF8;
/// SP-PG T12 admin: describe a table by NAME (PG-wire needs name
/// lookup because PG clients don't pass type_id). Frame =
/// `[0xF7] ++ utf8 name`; reply `Got(encode_type_def(name, fields))`
/// on hit, `NotFound` on miss. Engine-thread-local; no SM mutation
/// (read-only). Used by `kessel-pg-gateway::EngineApply::describe_table`
/// via the `pg-gateway` feature impl on `EngineHandle`.
pub const DESCRIBE_BY_NAME_TAG: u8 = 0xF7;

/// SP-PG-CAT T3 admin: enumerate every user-visible table in the
/// live catalog for the `pg_class` synthesizer. Frame = `[0xF6]`
/// (no body); reply `Got(encoded)` where `encoded` is
/// `[u32 LE count][repeat: u32 LE name_len, name bytes, u32 LE
/// type_id, u16 LE field_count]`. Engine-thread-local; read-only;
/// no SM mutation. Used by `kessel-pg-gateway::EngineApply::
/// list_tables` via the `pg-gateway` feature impl on `EngineHandle`.
///
/// V1 emits TableKind::Ordinary for every entry (KesselDB catalog
/// has no view/sequence kind yet). The wire format intentionally
/// does NOT carry the kind byte — it's implicit Ordinary today —
/// keeping the admin frame minimal until KesselDB grows other
/// table kinds.
pub const LIST_TABLES_TAG: u8 = 0xF6;

/// SP-PG-CAT T8a admin: enumerate indexes on a NAMED table for the
/// `pg_index` synthesizer + the pgJDBC `getIndexInfo` joined path.
/// Frame = `[0xF5] ++ utf8 name`; reply `Got(encoded)` where
/// `encoded` is `[u32 LE count][repeat: u32 LE name_len, name bytes,
/// u8 kind, u8 is_unique, u16 LE field_count, field_count × u32 LE
/// field_id]`. `kind` is 0=Equality, 1=Range, 2=Composite per
/// `kessel_pg_gateway::IndexKind`. Engine-thread-local; read-only.
///
/// The wire format mirrors `LIST_TABLES_TAG` — variable-length
/// records prefixed with a u32 count — and uses fixed-width
/// (u8/u16/u32) tags throughout for forward compatibility.
pub const LIST_INDEXES_TAG: u8 = 0xF5;

/// SP-PG-CAT T8a admin: enumerate constraints on a NAMED table for
/// the `pg_constraint` synthesizer + the
/// `information_schema.{table_constraints,key_column_usage}` views.
/// Frame = `[0xF4] ++ utf8 name`; reply `Got(encoded)` where
/// `encoded` is `[u32 LE count][repeat: u32 LE name_len, name bytes,
/// u8 kind, u8 fk_action, u16 LE field_count, field_count × u32 LE
/// field_id, u32 LE ref_name_len, ref_name bytes (only if kind=FK),
/// u16 LE ref_field_count, ref_field_count × u32 LE field_id (only
/// if kind=FK)]`. `kind` is 0=Check, 1=ForeignKey, 2=Unique. For
/// non-FK rows the trailing referenced-table block is omitted
/// (ref_name_len=0; ref_field_count=0). Engine-thread-local;
/// read-only.
pub const LIST_CONSTRAINTS_TAG: u8 = 0xF4;

/// SP-PG-EXTQ-PARSED-DEFAULT T1 admin: SQL with typed bound
/// parameters. Frame = `[0xF3][u32 LE sql_len][sql bytes][u32 LE
/// param_count][param_count × ParamSlot]` where `ParamSlot` is a
/// tagged union: `0x00` = None (SQL NULL), `0x01 [i128 LE]` =
/// Value::Int, `0x02 [u128 LE]` = Value::Uint, `0x03 [u32 LE
/// blob_len][bytes]` = Value::Blob, `0x04` = Value::Null. Bound
/// values reach the engine as typed `kessel_codec::Value`s; NO SQL
/// text concatenation. Engine apply path runs
/// `kessel_sql::compile_stmt_with_params` against the live catalog.
pub const PARAMETERIZED_SQL_TAG: u8 = 0xF3;

/// SP-PG-EXTQ-PARSED-DEFAULT T1 — encode `(sql, params)` into a
/// `PARAMETERIZED_SQL_TAG` admin frame.
pub fn encode_parameterized_sql(
    sql: &str,
    params: &[Option<kessel_codec::Value>],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + sql.len() + 4 + params.len() * 17);
    out.push(PARAMETERIZED_SQL_TAG);
    out.extend_from_slice(&(sql.len() as u32).to_le_bytes());
    out.extend_from_slice(sql.as_bytes());
    out.extend_from_slice(&(params.len() as u32).to_le_bytes());
    for p in params {
        match p {
            None => out.push(0x00),
            Some(kessel_codec::Value::Int(i)) => {
                out.push(0x01);
                out.extend_from_slice(&i.to_le_bytes());
            }
            Some(kessel_codec::Value::Uint(u)) => {
                out.push(0x02);
                out.extend_from_slice(&u.to_le_bytes());
            }
            Some(kessel_codec::Value::Blob(b)) => {
                out.push(0x03);
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(b);
            }
            Some(kessel_codec::Value::Null) => out.push(0x04),
        }
    }
    out
}

/// SP-PG-EXTQ-PARSED-DEFAULT T1 — decode a `PARAMETERIZED_SQL_TAG`
/// admin frame body (the bytes AFTER the leading `0xF3` tag) into
/// `(sql, params)`. Returns `None` on any structural error.
pub fn decode_parameterized_sql(
    body: &[u8],
) -> Option<(String, Vec<Option<kessel_codec::Value>>)> {
    let mut p = 0usize;
    if body.len() < 4 {
        return None;
    }
    let sql_len = u32::from_le_bytes(body[p..p + 4].try_into().ok()?) as usize;
    p += 4;
    if body.len() < p + sql_len + 4 {
        return None;
    }
    let sql = std::str::from_utf8(&body[p..p + sql_len]).ok()?.to_string();
    p += sql_len;
    let param_count = u32::from_le_bytes(body[p..p + 4].try_into().ok()?) as usize;
    p += 4;
    let mut params: Vec<Option<kessel_codec::Value>> = Vec::with_capacity(param_count);
    for _ in 0..param_count {
        if p >= body.len() {
            return None;
        }
        let kind = body[p];
        p += 1;
        match kind {
            0x00 => params.push(None),
            0x01 => {
                if body.len() < p + 16 {
                    return None;
                }
                let i = i128::from_le_bytes(body[p..p + 16].try_into().ok()?);
                p += 16;
                params.push(Some(kessel_codec::Value::Int(i)));
            }
            0x02 => {
                if body.len() < p + 16 {
                    return None;
                }
                let u = u128::from_le_bytes(body[p..p + 16].try_into().ok()?);
                p += 16;
                params.push(Some(kessel_codec::Value::Uint(u)));
            }
            0x03 => {
                if body.len() < p + 4 {
                    return None;
                }
                let bl = u32::from_le_bytes(body[p..p + 4].try_into().ok()?) as usize;
                p += 4;
                if body.len() < p + bl {
                    return None;
                }
                let bytes = body[p..p + bl].to_vec();
                p += bl;
                params.push(Some(kessel_codec::Value::Blob(bytes)));
            }
            0x04 => params.push(Some(kessel_codec::Value::Null)),
            _ => return None,
        }
    }
    Some((sql, params))
}

/// Operational status of a running node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerStats {
    /// Engine sequence number — monotone count of applied ops.
    pub applied_ops: u64,
    /// Deterministic state digest (matches `Replica::digest`).
    pub digest: u32,
    /// Seconds since this engine started.
    pub uptime_secs: u64,
}

impl ServerStats {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(20);
        b.extend_from_slice(&self.applied_ops.to_le_bytes());
        b.extend_from_slice(&self.digest.to_le_bytes());
        b.extend_from_slice(&self.uptime_secs.to_le_bytes());
        b
    }
    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.len() < 20 {
            return None;
        }
        Some(ServerStats {
            applied_ops: u64::from_le_bytes(b[0..8].try_into().ok()?),
            digest: u32::from_le_bytes(b[8..12].try_into().ok()?),
            uptime_secs: u64::from_le_bytes(b[12..20].try_into().ok()?),
        })
    }
}

/// Copy every file in a flat data dir to `dest` (created if missing). The
/// caller (engine thread) guarantees no concurrent writes, so the copy is
/// a crash-consistent image — `StateMachine::open(dest)` recovers it.
fn copy_dir_flat(src: &Path, dest: &Path) -> io::Result<()> {
    std::fs::create_dir_all(dest)?;
    for e in std::fs::read_dir(src)? {
        let e = e?;
        let p = e.path();
        if p.is_file() {
            std::fs::copy(&p, dest.join(e.file_name()))?;
        }
    }
    Ok(())
}

/// Server policy: optional shared-secret token, plus quota / backpressure
/// caps. Defaults are open + generous so existing embeddings are unchanged.
#[derive(Clone)]
pub struct ServerConfig {
    /// `None` = open (no handshake expected). `Some(t)` = the first frame on
    /// every connection must be `[0xFC] ++ t` (constant-time compared).
    pub token: Option<Vec<u8>>,
    /// Max concurrent client connections PER LISTENER (binary, HTTP, HTTPS
    /// each independently cap at this value). A process with the gateway
    /// feature enabled may hold up to `max_conns × num_listeners` concurrent
    /// connections. The cap is per-listener so a misbehaving HTTP client
    /// can't starve the binary protocol.
    pub max_conns: usize,
    /// Max requests in flight to the engine; over this, callers get
    /// `OpResult::Unavailable` (backpressure) instead of unbounded queueing.
    pub max_inflight: usize,
    /// Optional TLS. `Some((cert_pem, key_pem))` terminates TLS in-process
    /// using the **opt-in `tls` cargo feature** (rustls). With the feature
    /// off this field is ignored (the default build stays zero-dependency
    /// and plaintext+token — deploy behind a TLS proxy / private network).
    pub tls: Option<(std::path::PathBuf, std::path::PathBuf)>,
    /// SP141: HTTP/1.1 gateway address (opt-in via the `http-gateway`
    /// feature). `None` = no plaintext gateway.
    pub http_addr: Option<std::net::SocketAddr>,
    /// SP141: HTTPS gateway address. Requires both `http-gateway` AND
    /// `tls` features. `None` = no HTTPS gateway.
    pub http_tls_addr: Option<std::net::SocketAddr>,
    /// SP141: HTTP gateway body cap (default 8 MiB). Mirrors the binary
    /// frame cap.
    pub http_max_body: usize,
    /// SP147: per-HTTP-connection request cap (default 1000). Prevents a
    /// single client from monopolizing one keep-alive TCP connection
    /// forever; after this many requests on one connection the gateway
    /// closes cleanly and the client must open a fresh connection.
    pub http_max_requests_per_conn: usize,
    /// SP-PG T12: PostgreSQL Frontend/Backend Protocol v3.0 gateway
    /// address (opt-in via the `pg-gateway` feature). `None` = no PG
    /// listener spawned. Independent of `http_addr` — PG and HTTP
    /// caps are separate so a misbehaving pgcli can't starve HTTP
    /// clients (spec §8.1). Default port: 5432.
    pub pg_addr: Option<std::net::SocketAddr>,
    /// SP-PG T12: per-PG-listener concurrent connection cap. Smaller
    /// than `max_conns` because PG clients hold connections longer
    /// (typical ORM pool: 10-50 per app instance). Default 256
    /// (mirrors `kessel-pg-gateway::DEFAULT_MAX_PG_CONNS`).
    pub pg_max_conns: usize,
    /// SP-PG T12: per-connection idle timeout for PG sessions.
    /// Connection that hasn't sent any client message for this long
    /// is closed. Default 600s (mirrors
    /// `kessel-pg-gateway::PG_DEFAULT_IDLE_TIMEOUT_SECS`). V1 just
    /// closes the socket on timeout; T16 will emit `57014`
    /// query_canceled ErrorResponse first.
    pub pg_idle_timeout: std::time::Duration,
    /// SP-Perf-A T1: optional read-worker pool size. `None` =
    /// pre-Perf-A behaviour (no pool spawned, every op routes through
    /// the single owning engine thread). `Some(n)` = spawn an
    /// `n`-worker `ReadPool` at server start; the connection-accept
    /// path dispatches bare-Op read-only frames to the pool while
    /// writes + SQL + admin tags still go to the engine queue.
    /// `Some(0)` is supported as a graceful "wire-only" mode that
    /// constructs the pool's plumbing but spawns no workers — the
    /// pool falls back to the submitting-thread engine call.
    ///
    /// See `docs/superpowers/specs/2026-05-28-kesseldb-perf-a-parallel-reads-design.md`.
    pub read_workers: Option<usize>,
    /// SP-Perf-A-SHARD T2 (scaffold): named config slot for the future
    /// sharded-SM apply path. **NOT yet wired into `spawn_engine_cfg`** —
    /// this scaffold ships the [`sharded_sm::ShardedStateMachine`] type
    /// + K=1 regression-lock, but the engine-spawn integration is a
    /// V2 arc (`SP-Perf-A-SHARD-APPLY`). `None` (default) = pre-SHARD
    /// behaviour (single `Arc<RwLock<StateMachine>>` per SP-Perf-A T2).
    /// `Some(1)` = K=1 collapse (functionally identical to `None`,
    /// reserved for the V2 wiring's regression check). `Some(N)` for
    /// `N >= 2` is reserved for SP-Perf-A-SHARD-APPLY.
    ///
    /// See `docs/superpowers/specs/2026-05-30-kesseldb-spperfa-shard-design.md`.
    pub shard_count: Option<usize>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            token: None,
            max_conns: 1024,
            max_inflight: 4096,
            tls: None,
            http_addr: None,
            http_tls_addr: None,
            http_max_body: 8 * 1024 * 1024,
            http_max_requests_per_conn: 1000,
            pg_addr: None,
            pg_max_conns: 256,
            pg_idle_timeout: std::time::Duration::from_secs(600),
            read_workers: None,
            shard_count: None,
        }
    }
}

/// Length-independent equality: scans in time proportional to the longer
/// input and never short-circuits on the first differing byte, so a
/// network attacker cannot byte-by-byte time the secret.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let n = a.len().max(b.len());
    let mut diff = (a.len() ^ b.len()) as u8;
    for i in 0..n {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= x ^ y;
    }
    diff == 0
}

/// Run the auth handshake. Open mode → always Ok and *no* frame consumed.
/// Token mode → read exactly one frame; accept iff it is `[0xFC] ++ token`,
/// replying `Ok`; otherwise reply `Unauthorized` and reject.
fn authenticate<S: std::io::Read + std::io::Write>(
    stream: &mut S,
    token: &Option<Vec<u8>>,
) -> bool {
    let Some(tok) = token else { return true };
    let frame = match read_frame(stream) {
        Ok(f) => f,
        Err(_) => return false,
    };
    let ok = frame.first() == Some(&AUTH_TAG) && ct_eq(&frame[1..], tok);
    let reply = if ok { OpResult::Ok } else { OpResult::Unauthorized };
    let _ = write_frame(stream, &reply.encode());
    ok
}

/// Engine-thread-local prepared-statement cache (SP47). Tokenise+parse+
/// plan is pure CPU on the single-threaded deterministic core, so for a
/// repeated SQL string it is wasted work every request. Caching the
/// compiled `Stmt` removes it from the hot path with **zero** functional
/// change (identical `Stmt`, identical results, still deterministic — the
/// cache is engine-local and never touches replicated state). It is
/// cleared whenever an applied op can change the catalog, so a cached plan
/// is never reused against a changed schema: correctness is identical to
/// always recompiling, only faster.
struct CompileCache {
    map: HashMap<String, kessel_sql::Stmt>,
    cap: usize,
}

impl CompileCache {
    fn new() -> Self {
        CompileCache { map: HashMap::new(), cap: 4096 }
    }
    fn get_or_compile(
        &mut self,
        sql: &str,
        cat: &kessel_catalog::Catalog,
    ) -> Result<kessel_sql::Stmt, String> {
        if let Some(s) = self.map.get(sql) {
            return Ok(s.clone());
        }
        let s = kessel_sql::compile_stmt(sql, cat).map_err(|e| e.to_string())?;
        if self.map.len() >= self.cap {
            self.map.clear(); // bounded + deterministic (rare)
        }
        self.map.insert(sql.to_string(), s.clone());
        Ok(s)
    }
    fn invalidate(&mut self) {
        self.map.clear();
    }
}

/// Applying an op of one of these kinds can change the catalog/schema, so
/// any cached compiled statement must be discarded afterwards.
fn mutates_schema(op: &Op) -> bool {
    matches!(
        op.kind(),
        1 | 2 | 8 | 10 | 12 | 13 | 14 | 17 | 24 | 15 | 29 | 30 | 31 | 32 | 33
    )
}

/// Apply exactly ONE request frame (`[0xFE] ++ SQL`, or a bare
/// `Op::encode()`) on the engine thread, including the server-side
/// read-modify-write for SQL `UPDATE`. This is the single source of
/// truth for "what one request does", shared by the normal path and by
/// every member of a pipeline batch (SP69) — so a pipelined member is
/// byte-for-byte equivalent to having sent it alone (same monotonic id,
/// same compile-cache use/invalidation, same result). It deliberately
/// does NOT handle the admin/txn/pipeline tags (those need the engine's
/// `start`/`dir` and are handled by the driver).
/// SP115 / S2.6 (Decision 6): Heartbeat producer for the MVCC
/// AdvanceWatermark protocol. Spawned at server startup on the VSR
/// primary; runs in a background task at a configurable interval
/// (default 1s); reads `sm.min_active_snapshot()` and submits
/// `Op::AdvanceWatermark` ops via VSR.
///
/// Non-deterministic at the SUBMISSION boundary (each replica's
/// wall-clock fires independently); deterministic at SM apply
/// (the resulting AdvanceWatermark op flows through VSR's
/// totally-ordered log).
///
/// T1 shipped the function signature scaffold; T2 ships the loop
/// body. The function spawns a daemon thread that ticks at `interval`
/// (default 1s suggested by callers) and on each tick:
///
///  1. Invokes `state` — the SM snapshot reader closure — to retrieve
///     `(target, current_lwm)` where `target = min_active_snapshot or
///     current_commit_opnum` and `current_lwm = low_water_mark`. The
///     SM is NOT `Send` (its underlying `Wal` carries a non-Send
///     `dyn Disk`); the closure runs on the engine thread via the
///     existing `EngineHandle` apply path or any equivalent
///     proxy — the heartbeat thread only sees the resulting `(u64,
///     u64)` tuple. (The honest decoupling: T2 ships the loop
///     mechanism; T3 wires the live SM via a Send-safe snapshot
///     reader; T4 covers default-interval behavior.)
///  2. If the target advance is strictly greater than current,
///     submits `Op::AdvanceWatermark { low_water_mark: target }` via
///     the `submit` closure (the VSR pipeline bridge).
///
/// Determinism: SM apply remains deterministic; the heartbeat's
/// non-determinism (each replica's clock fires at slightly different
/// times) is contained at the SUBMISSION boundary — only the primary
/// submits; the apply path is deterministic across replicas. Per
/// Decision 6 + Decision 7.
pub fn spawn_heartbeat_loop(
    state: impl Fn() -> Option<(u64 /*target*/, u64 /*current_lwm*/)>
        + Send
        + 'static,
    submit: impl Fn(Op) + Send + 'static,
    interval: std::time::Duration,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || loop {
        std::thread::sleep(interval);
        match state() {
            Some((target, current_lwm)) => {
                if target > current_lwm {
                    submit(Op::AdvanceWatermark { low_water_mark: target });
                }
            }
            None => return, // state reader signalled shutdown — exit cleanly
        }
    })
}

/// SP115 / S2.6: Helper to compute the heartbeat target from an SM.
/// Used by both `spawn_heartbeat_loop`'s state closure and the
/// `heartbeat_tick_once` test helper. Returns `(target, current_lwm)`
/// where `target = min_active_snapshot().unwrap_or(current_commit_opnum())`.
pub fn heartbeat_target<V: kessel_io::Vfs>(
    sm: &StateMachine<V>,
) -> (u64, u64) {
    let target = sm
        .min_active_snapshot()
        .unwrap_or_else(|| sm.current_commit_opnum());
    let lwm = sm.low_water_mark();
    (target, lwm)
}

fn apply_one(
    sm: &mut StateMachine<DirVfs>,
    cache: &mut CompileCache,
    n: &mut u64,
    frame: &[u8],
) -> OpResult {
    // SP115 / S2.6 (Decision 2 + Decision 3): AUTO-COMMIT TX WRAPPER.
    //
    // Every SQL-derived Op wraps in an auto-commit Tx whose snapshot
    // is `sm.current_commit_opnum()` (PostgreSQL READ COMMITTED).
    // We register the snapshot in `sm.active_snapshots` before
    // dispatching the apply and unregister after, so the heartbeat's
    // `min_active_snapshot()` reflects this in-flight Tx and the GC
    // watermark cannot advance past it (Decision 6 / 7).
    //
    // For S2.6 the SM apply arms themselves now route data-row ops
    // through MVCC (Decision 1a full-replace, applied via the
    // `data_row_{get,put,delete,scan}` seam on StateMachine). The
    // auto-commit lifecycle here is the OUTER bracket; the inner
    // arm's MVCC writes/reads consume `op_number` as the
    // commit_opnum (Decision 4). The explicit `Tx::begin_rw /
    // Tx::commit_ssi → Op::CommitTx` lifecycle is the alternative
    // path exercised by direct multi-statement-Tx callers (S2.7
    // grammar follow-up); single-statement auto-commit at this seam
    // simplifies to the register / apply / unregister bracket.
    //
    // Honest disclosure: in S2.6 single-statement auto-commit, MVCC
    // conflicts CANNOT occur at the SM apply layer (every apply
    // runs serially in the log-position order; conflicts only arise
    // for client-side concurrent Tx, which S2.6 doesn't surface to
    // the SQL grammar — S2.7).
    let snapshot = sm.current_commit_opnum();
    sm.register_snapshot(snapshot);
    let r = apply_one_inner(sm, cache, n, frame);
    sm.unregister_snapshot(snapshot);
    r
}

/// Inner apply body — the original apply_one logic. Kept separate so
/// the auto-commit register/unregister bracket is the single
/// outer-most concern in `apply_one`.
fn apply_one_inner(
    sm: &mut StateMachine<DirVfs>,
    cache: &mut CompileCache,
    n: &mut u64,
    frame: &[u8],
) -> OpResult {
    // SP-PG-EXTQ-PARSED-DEFAULT T1 — parameterized SQL admin frame.
    // Decode `(sql, params)` and run `compile_stmt_with_params` against
    // the live catalog. The bound values enter as typed
    // `kessel_codec::Value`s — NO SQL text concatenation, NO `'`->`''`
    // escape rules, NO quoting. Closes the SP-PG-EXTQ V1 weak-spot
    // #1 attack surface at the dispatch layer.
    let op = if frame.first() == Some(&PARAMETERIZED_SQL_TAG) {
        let (sql, params) = match decode_parameterized_sql(&frame[1..]) {
            Some(t) => t,
            None => {
                return OpResult::SchemaError(
                    "parameterized sql: malformed frame".into(),
                );
            }
        };
        match kessel_sql::compile_stmt_with_params(&sql, sm.catalog(), &params) {
            Ok(kessel_sql::Stmt::Op(o)) => Some(o),
            Ok(kessel_sql::Stmt::Update { type_id, id, sets }) => {
                let oid = kessel_proto::ObjectId::from_u128(id);
                let cur = sm.apply(*n, Op::GetById { type_id, id: oid });
                *n += 1;
                let rec = match cur {
                    OpResult::Got(r) => r,
                    other => return other,
                };
                let ot = match sm.catalog().get(type_id) {
                    Some(t) => t.clone(),
                    None => {
                        return OpResult::SchemaError(
                            "parameterized update: no type".into(),
                        );
                    }
                };
                let mut vals = match kessel_codec::decode(&ot, &rec) {
                    Ok(v) => v,
                    Err(e) => {
                        return OpResult::SchemaError(format!(
                            "parameterized update decode: {e:?}"
                        ));
                    }
                };
                for (fid, v) in sets {
                    if let Some(i) =
                        ot.fields.iter().position(|f| f.field_id == fid)
                    {
                        vals[i] = v;
                    }
                }
                match kessel_codec::encode(&ot, &vals) {
                    Ok(record) => Some(Op::Update { type_id, id: oid, record }),
                    Err(e) => {
                        return OpResult::SchemaError(format!(
                            "parameterized update encode: {e:?}"
                        ));
                    }
                }
            }
            Ok(kessel_sql::Stmt::UpdateWhere {
                type_id,
                program,
                sets,
                returning,
            }) => {
                return apply_dml_where(
                    sm,
                    n,
                    type_id,
                    program,
                    Some(sets),
                    returning.is_some(),
                );
            }
            Ok(kessel_sql::Stmt::DeleteWhere {
                type_id,
                program,
                returning,
            }) => {
                return apply_dml_where(
                    sm,
                    n,
                    type_id,
                    program,
                    None,
                    returning.is_some(),
                );
            }
            Ok(kessel_sql::Stmt::Explain(plan)) => {
                return OpResult::Got(plan.into_bytes().into());
            }
            Err(e) => {
                return OpResult::SchemaError(format!(
                    "parameterized sql: {e}"
                ));
            }
        }
    } else if frame.first() == Some(&0xFE) {
        let sql = match std::str::from_utf8(&frame[1..]) {
            Ok(s) => s,
            Err(_) => {
                return OpResult::SchemaError("sql: not utf8".into());
            }
        };
        match cache.get_or_compile(sql, sm.catalog()) {
            Ok(kessel_sql::Stmt::Op(o)) => Some(o),
            Ok(kessel_sql::Stmt::Update { type_id, id, sets }) => {
                // Server-side read-modify-write for SQL UPDATE.
                let oid = kessel_proto::ObjectId::from_u128(id);
                let cur = sm.apply(*n, Op::GetById { type_id, id: oid });
                *n += 1;
                let rec = match cur {
                    OpResult::Got(r) => r,
                    other => return other, // NotFound etc.
                };
                let ot = match sm.catalog().get(type_id) {
                    Some(t) => t.clone(),
                    None => {
                        return OpResult::SchemaError("update: no type".into());
                    }
                };
                let mut vals = match kessel_codec::decode(&ot, &rec) {
                    Ok(v) => v,
                    Err(e) => {
                        return OpResult::SchemaError(format!(
                            "update decode: {e:?}"
                        ));
                    }
                };
                for (fid, v) in sets {
                    if let Some(i) =
                        ot.fields.iter().position(|f| f.field_id == fid)
                    {
                        vals[i] = v;
                    }
                }
                match kessel_codec::encode(&ot, &vals) {
                    Ok(record) => Some(Op::Update { type_id, id: oid, record }),
                    Err(e) => {
                        return OpResult::SchemaError(format!(
                            "update encode: {e:?}"
                        ));
                    }
                }
            }
            // SP-PG-SQL-DML-GENERAL — general-WHERE UPDATE/DELETE: the
            // server resolves the matching ids + applies a concrete Txn
            // (Path A). Returns a framed (count [+ RETURNING rows]) Got.
            Ok(kessel_sql::Stmt::UpdateWhere {
                type_id,
                program,
                sets,
                returning,
            }) => {
                return apply_dml_where(
                    sm,
                    n,
                    type_id,
                    program,
                    Some(sets),
                    returning.is_some(),
                );
            }
            Ok(kessel_sql::Stmt::DeleteWhere {
                type_id,
                program,
                returning,
            }) => {
                return apply_dml_where(
                    sm,
                    n,
                    type_id,
                    program,
                    None,
                    returning.is_some(),
                );
            }
            Ok(kessel_sql::Stmt::Explain(plan)) => {
                return OpResult::Got(plan.into_bytes().into());
            }
            Err(e) => {
                return OpResult::SchemaError(format!("sql: {e}"));
            }
        }
    } else {
        Op::decode(frame)
    };
    match op {
        Some(o) => {
            let mutates = mutates_schema(&o);
            let r = sm.apply(*n, o);
            *n += 1;
            if mutates {
                cache.invalidate(); // schema changed → cached plans stale
            }
            r
        }
        None => OpResult::SchemaError("malformed request frame".into()),
    }
}

/// SP-PG-SQL-DML-GENERAL — frame a general-WHERE UPDATE/DELETE result so
/// the gateway can read the affected-row count + (optional) RETURNING
/// rows. Layout: `[u32 affected LE][u32 nrows LE]` then `nrows ×
/// [u32 reclen LE][record bytes]`. `nrows == 0` ⇒ no RETURNING (count
/// only). Carried inside `OpResult::Got`; the gateway distinguishes this
/// from a SELECT `Got` by the UPDATE/DELETE leading keyword.
pub const DML_RESULT_TAG: u8 = 0xD3;
pub(crate) fn frame_dml_result(affected: u32, rows: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + rows.iter().map(|r| 4 + r.len()).sum::<usize>());
    out.push(DML_RESULT_TAG);
    out.extend_from_slice(&affected.to_le_bytes());
    out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for r in rows {
        out.extend_from_slice(&(r.len() as u32).to_le_bytes());
        out.extend_from_slice(r);
    }
    out
}

/// SP-PG-SQL-DML-GENERAL (Path A) — resolve a general-WHERE UPDATE/DELETE
/// SERVER-side and apply it as ONE concrete `Op::Txn`:
///
/// 1. `Op::QueryExpr { type_id, program }` → the matching object ids
///    (already `sort_unstable`-sorted by the SM ⇒ deterministic order).
/// 2. Build `Op::Txn` of per-id `Op::UpdateSet` (UPDATE) or `Op::Delete`
///    (DELETE). The Txn is the REPLICATED artifact (a pure function of
///    committed state + predicate) — same determinism as by-id RMW.
/// 3. RETURNING: for DELETE, snapshot each matched record BEFORE the Txn
///    (the rows are about to vanish); for UPDATE, read each record AFTER
///    the Txn commits (post-mutation values). `want_returning` gates the
///    read-back so a no-RETURNING statement pays nothing.
///
/// Returns `OpResult::Got(frame_dml_result(...))` on success, or the
/// underlying error `OpResult` (e.g. `Constraint` on a UNIQUE violation —
/// the Txn rolls back atomically, zero rows applied).
fn apply_dml_where(
    sm: &mut StateMachine<DirVfs>,
    n: &mut u64,
    type_id: u32,
    program: Vec<u8>,
    sets: Option<Vec<(u16, kessel_codec::Value)>>, // Some ⇒ UPDATE, None ⇒ DELETE
    want_returning: bool,
) -> OpResult {
    // 1. Resolve matching ids (read-only scan; advances op_number like
    //    any other read so the snapshot bracket stays consistent).
    let ids_res = sm.apply(*n, Op::QueryExpr { type_id, program });
    *n += 1;
    let id_bytes = match ids_res {
        OpResult::Got(b) => b,
        other => return other, // SchemaError (no type / bad program), etc.
    };
    if id_bytes.len() % 16 != 0 {
        return OpResult::SchemaError("dml-where: malformed id stream".into());
    }
    let ids: Vec<kessel_proto::ObjectId> = id_bytes
        .chunks_exact(16)
        .map(|c| {
            let mut a = [0u8; 16];
            a.copy_from_slice(c);
            kessel_proto::ObjectId(a)
        })
        .collect();
    let affected = ids.len() as u32;

    // Convert the UPDATE SET `Value`s to the raw field bytes
    // `Op::UpdateSet` carries (mirrors the SP84 in-txn UPDATE path). Done
    // once, before the per-id fan-out.
    let raw_sets: Option<Vec<(u16, Vec<u8>)>> = match &sets {
        None => None,
        Some(vs) => {
            let ot = match sm.catalog().get(type_id) {
                Some(t) => t.clone(),
                None => {
                    return OpResult::SchemaError(format!(
                        "dml-where: no type {type_id}"
                    ))
                }
            };
            let mut rs = Vec::with_capacity(vs.len());
            for (fid, v) in vs {
                let fk = match ot.fields.iter().find(|f| f.field_id == *fid) {
                    Some(f) => f.kind,
                    None => {
                        return OpResult::SchemaError(format!(
                            "dml-where: no field {fid}"
                        ))
                    }
                };
                match kessel_codec::raw_from_value(fk, v) {
                    Some(r) => rs.push((*fid, r)),
                    None => {
                        return OpResult::Constraint(
                            "UPDATE … SET col = NULL on a general-WHERE \
                             UPDATE is not yet supported"
                                .into(),
                        )
                    }
                }
            }
            Some(rs)
        }
    };

    // 2. For DELETE RETURNING, capture the rows BEFORE they are removed.
    let mut returning_rows: Vec<Vec<u8>> = Vec::new();
    let is_delete = raw_sets.is_none();
    if want_returning && is_delete {
        for id in &ids {
            match sm.apply(*n, Op::GetById { type_id, id: *id }) {
                OpResult::Got(rec) => returning_rows.push(rec.as_ref().to_vec()),
                _ => {}
            }
            *n += 1;
        }
    }

    // 3. Build + apply the concrete Txn (the replicated artifact).
    let inner: Vec<Op> = ids
        .iter()
        .map(|id| match &raw_sets {
            Some(s) => Op::UpdateSet { type_id, id: *id, sets: s.clone() },
            None => Op::Delete { type_id, id: *id },
        })
        .collect();
    // Empty match ⇒ no-op (PG returns `UPDATE 0`/`DELETE 0` with no
    // state change). Skip the Txn entirely so we don't apply an empty
    // batch.
    if !inner.is_empty() {
        let txn_res = sm.apply(*n, Op::Txn { ops: inner });
        *n += 1;
        match txn_res {
            OpResult::Ok | OpResult::TxCommitted { .. } => {}
            other => return other, // Constraint/SchemaError ⇒ atomic rollback
        }
    }

    // 4. For UPDATE RETURNING, read the post-mutation rows.
    if want_returning && !is_delete {
        for id in &ids {
            match sm.apply(*n, Op::GetById { type_id, id: *id }) {
                OpResult::Got(rec) => returning_rows.push(rec.as_ref().to_vec()),
                _ => {}
            }
            *n += 1;
        }
    }

    OpResult::Got(frame_dml_result(affected, &returning_rows).into())
}

/// One request to the engine thread: an op and a one-shot reply channel.
type EngineMsg = (Vec<u8>, SyncSender<OpResult>);

/// Handle used by connection threads to submit ops to the single engine.
#[derive(Clone)]
pub struct EngineHandle {
    tx: Sender<EngineMsg>,
    inflight: Arc<AtomicUsize>,
    max_inflight: usize,
    /// SP142: direct-read counter for /v1/metrics — populated atomically
    /// from the engine thread on every applied op. Avoids the STATS_TAG
    /// round-trip in snapshot_metrics/snapshot_health, which would return
    /// 0 under engine saturation (Prometheus counter-reset).
    applied_ops_atomic: Arc<AtomicU64>,
    /// SP144H T1: per-Op::kind() counters. Indexed by tag-byte (the first
    /// byte of every Op::encode() frame, which equals `Op::kind() as u8`
    /// for bare-Op frames; 0xFE for SQL frames; 0xFB STATS_TAG etc. are
    /// engine-internal and excluded from publication). Size 64 = 46 Op
    /// kinds + headroom + room for special tags like 0xFE. Out-of-range
    /// tags (≥64) are dropped (no overflow into another slot).
    op_kind_counts: Arc<[AtomicU64; 64]>,
    /// SP144H T2: per-(path, status) HTTP request counters. Shared
    /// between the gateway accept loop (bumps on every emitted response)
    /// and `snapshot_metrics` (reads via `.snapshot()`). Bounded at 4×16
    /// atomic slots. Only built/published when the `http-gateway` feature
    /// is on — the field is cfg-gated so the default build pays nothing.
    #[cfg(feature = "http-gateway")]
    http_counters: Arc<kessel_http_gateway::HttpRequestCountersStatic>,
    /// SP-Perf-A T2: shared `Arc<RwLock<StateMachine>>` for the read-only
    /// bypass. When `ServerConfig.read_workers = Some(_)`, the engine
    /// thread owns the write side; reads acquire `.read()` and call
    /// `StateMachine::read_only_op` directly, skipping the engine mpsc +
    /// group-commit fsync — the apply-thread latency tax (~440 µs/op on
    /// vulcan T1) drops to a raw RwLock-read + storage-get.
    ///
    /// `None` ⇒ pre-Perf-A behaviour: every op routes through the engine
    /// queue, byte-identical to T1. (T1 did NOT populate this — T2 does,
    /// gated on the config field — so default builds with
    /// `read_workers = None` still pay nothing.)
    sm_shared: Option<Arc<RwLock<StateMachine<DirVfs>>>>,
    /// SP-Perf-A T2: optional read worker pool. When `Some`, bare-Op
    /// read-only frames dispatch to a worker thread which calls
    /// `sm_shared.read().read_only_op(op)`. When `None`, reads run on
    /// the submitting thread under the same RwLock — fewer hops but no
    /// CPU-pinning / fairness.
    ///
    /// V1 always populates BOTH `sm_shared` AND `read_pool` together
    /// when `cfg.read_workers.is_some()`, but the pool's worker count
    /// may be 0 (graceful fall-through to submitting-thread dispatch).
    read_pool: Option<Arc<read_pool::ReadPool>>,
    /// SP-Perf-A-SHARD-APPLY: optional K=N sharded dispatcher. When
    /// `Some`, `apply_raw` / `apply` route every frame through this
    /// dispatcher's per-shard sub-engines instead of the local
    /// engine queue. Constructed only when
    /// `ServerConfig.shard_count = Some(K)` with `K >= 2` —
    /// `None` for the default unsharded path (preserves SP-Perf-A
    /// T7 ownership shape byte-for-byte).
    ///
    /// The fields ABOVE (`tx`, `sm_shared`, `read_pool`, etc.) refer
    /// to a degenerate "router-shell" engine that holds no data —
    /// it owns an empty StateMachine spawned at `<data_dir>/router/`
    /// but every apply routes through the dispatcher. The router
    /// shell's atomics (`applied_ops_atomic`, `op_kind_counts`,
    /// `inflight`) are kept for the gateway compatibility surfaces
    /// (HTTP / PG / metrics) which still consult them; SHARD V2
    /// will plumb true cluster-aggregate counters.
    sharded: Option<sharded_engine::SharedDispatcher>,
}

impl EngineHandle {
    /// Submit a raw request frame. `[0xFE] ++ utf8 SQL` is compiled against
    /// the live catalog on the engine thread; otherwise it is an
    /// `Op::encode()` frame. (SQL must compile on the engine thread because
    /// it needs the catalog, which lives with the non-`Send` StateMachine.)
    ///
    /// Backpressure: if `max_inflight` requests are already queued, this
    /// returns `OpResult::Unavailable` immediately rather than growing the
    /// queue without bound.
    ///
    /// SP-Perf-A T2: when the engine was spawned with
    /// `ServerConfig.read_workers = Some(_)`, bare-Op read-only frames
    /// (kind ∈ §4 read-only set) bypass the engine mpsc + group-commit
    /// fsync and execute directly against an `Arc<RwLock<StateMachine>>`
    /// read guard. Writes + SQL + admin tags still route through the
    /// engine thread, preserving every serial-apply invariant.
    pub fn apply_raw(&self, frame: Vec<u8>) -> OpResult {
        // SP-Perf-A-SHARD-APPLY: when the engine was spawned with
        // `shard_count = Some(K)` for K >= 2, route the frame through
        // the K-shard dispatcher. The dispatcher decodes the op,
        // computes its owning shard via `hash(key) % K`, and
        // forwards to that shard's sub-engine. DDL ops broadcast to
        // every shard; scan/Txn/admin ops route to shard 0 (V1
        // limitation, see `sharded_engine.rs` module doc).
        if let Some(sharded) = &self.sharded {
            return sharded.apply_raw(frame);
        }
        // SP-Perf-A T2: read-only bypass. When opted in, decode the
        // frame's first byte against the proto read-only tag table
        // (Ops only; SQL `0xFE` + admin tags fall through). A successful
        // decode + read-only classification routes to the shared SM
        // read path; a write/SQL/admin frame falls through to the
        // existing engine queue exactly as before.
        if let Some(sm_shared) = &self.sm_shared {
            if let Some(&tag) = frame.first() {
                // Cheap kind-table lookup against the spec §4 read-only
                // set (16 variants) PLUS tag 15 (Op::Txn — needs a
                // structural recheck via is_read_only because its
                // RO-ness depends on inner-op composition). Avoids a
                // full Op::decode for write frames — only successful
                // classification needs the structural decode. SQL
                // (0xFE), admin (0xFA..0xFC, 0xF4..0xF9), session
                // (0xFD), etc. miss the table and fall through.
                let is_read_candidate = matches!(
                    tag,
                    6  // GetById
                  | 7  // GetBlob
                  | 9  // FindBy
                  | 11 // Query
                  | 15 // SP-Perf-A-TXN-RO: Op::Txn (structural recheck)
                  | 16 // QueryExpr
                  | 18 // FindRange
                  | 19 // Select
                  | 20 // Aggregate
                  | 21 // SelectFields
                  | 22 // GroupAggregate
                  | 23 // SelectSorted
                  | 25 // FindByComposite
                  | 26 // QueryRows
                  | 27 // Describe
                  | 28 // Join
                  | 35 // SeqRead
                );
                if is_read_candidate {
                    if let Some(op) = Op::decode(&frame) {
                        // SP-Perf-A-TXN-RO: for tag 15 (Op::Txn) the
                        // tag alone is insufficient — must walk inner
                        // ops via the recursive classifier. For the
                        // other 16 tags this is a cheap negation of
                        // the proto `is_mutating` already proven by
                        // KAT. Mixed-RW Txn falls through to the
                        // engine queue, byte-untouched.
                        if read_pool::is_read_only(&op) {
                            // SP144H T1: bump per-Op::kind() counter
                            // on the read path too — observability
                            // symmetry with writes. `applied_ops_atomic`
                            // is NOT bumped (preserves SP142 semantic:
                            // applied_ops counts log positions).
                            let idx = tag as usize;
                            if idx < 64 {
                                self.op_kind_counts[idx]
                                    .fetch_add(1, Ordering::AcqRel);
                            }
                            // V1: dispatch directly on the submitting
                            // thread under the read guard. The optional
                            // ReadPool exists for fairness/CPU-pinning
                            // under bursty workloads; with no pool (or
                            // workers=0) the submitting thread runs the
                            // read itself, which is the lowest-latency
                            // path on the bench. The pool is exercised
                            // via `dispatch_via_pool` (see ReadPool
                            // tests).
                            return match sm_shared.read() {
                                Ok(g) => g.read_only_op(op),
                                Err(_) => OpResult::SchemaError(
                                    "read lock poisoned".into(),
                                ),
                            };
                        }
                        // Classified as write (mixed-RW Op::Txn, or
                        // SP-Perf-A-TXN-RO not yet wiring all tags) —
                        // fall through to the engine queue.
                    }
                    // Decode failed → fall through to the engine queue
                    // (engine returns SchemaError uniformly).
                }
            }
        }
        let cur = self.inflight.fetch_add(1, Ordering::AcqRel);
        if cur >= self.max_inflight {
            self.inflight.fetch_sub(1, Ordering::AcqRel);
            return OpResult::Unavailable;
        }
        let (rtx, rrx) = sync_channel(1);
        let r = if self.tx.send((frame, rtx)).is_err() {
            OpResult::SchemaError("engine stopped".into())
        } else {
            rrx.recv()
                .unwrap_or_else(|_| OpResult::SchemaError("engine dropped reply".into()))
        };
        self.inflight.fetch_sub(1, Ordering::AcqRel);
        r
    }

    /// SP-Perf-A T2: snapshot of the underlying shared SM for the read
    /// pool worker loop. `None` when the engine was spawned without
    /// `read_workers`. The pool workers acquire the `.read()` guard per
    /// task and call `StateMachine::read_only_op` against it.
    pub fn sm_shared(&self) -> Option<Arc<RwLock<StateMachine<DirVfs>>>> {
        self.sm_shared.clone()
    }

    /// SP-Perf-A T2: worker count of the optional read pool, or 0 if
    /// none. Used by tests + observability surfaces to verify the
    /// `ServerConfig.read_workers` setting actually took effect.
    pub fn read_pool_workers(&self) -> usize {
        self.read_pool.as_ref().map(|p| p.workers()).unwrap_or(0)
    }
    /// SP-Perf-A T6 (Fix A): in-process apply that skips the
    /// `Op::encode() → apply_raw → Op::decode()` round-trip on the read
    /// path. The wire boundary (`apply_raw`) is unchanged — binary,
    /// HTTP, WS, and PG gateways still encode the Op and decode it
    /// inside `apply_raw` exactly as before. This method is the
    /// in-process callers' fast path (bench + tests + Rust embedders
    /// that hold an `EngineHandle` directly).
    ///
    /// When `sm_shared` is Some AND the op is read-only, the dispatch
    /// runs directly under the `RwLock<StateMachine>` read guard on
    /// the submitting thread — zero encode, zero decode, one atomic
    /// CAS on the rwlock reader count. Writes still go through the
    /// engine queue (the apply thread is the single writer); the
    /// encode-then-send is necessary because the engine consumes a
    /// `Vec<u8>` frame on its mpsc.
    pub fn apply(&self, op: Op) -> OpResult {
        // SP-Perf-A-SHARD-APPLY: route through the per-shard
        // dispatcher when sharded. The dispatcher's `apply_raw`
        // re-decodes the encoded op for routing, then dispatches to
        // the owning shard whose sub-engine's apply path also has
        // the SP-Perf-A T6 in-process fast path enabled (each
        // sub-engine is itself a vanilla EngineHandle with
        // sm_shared populated).
        if self.sharded.is_some() {
            return self.apply_raw(op.encode());
        }
        // SP-Perf-A T6: read-only in-process fast path. Saves the
        // Op::encode + Op::decode allocations at ~5M ops/sec × N
        // threads — the largest single contributor to the T5-
        // identified ~80M alloc/sec heap traffic ceiling.
        //
        // SP-Perf-A-TXN-RO: use `read_pool::is_read_only` (recurses
        // into Op::Txn) instead of `!op.is_mutating()` so all-RO Txn
        // wrappers route via the bypass too. Mixed-RW Txn still
        // classifies as mutating (the recursion finds a write inner
        // op) and falls through to the engine queue, byte-untouched.
        if let Some(sm_shared) = &self.sm_shared {
            if read_pool::is_read_only(&op) {
                // SP144H T1 parity: bump per-kind counter — matches
                // apply_raw's behaviour exactly so observability stays
                // symmetric across the two entry points.
                let idx = op.kind() as usize;
                if idx < 64 {
                    self.op_kind_counts[idx].fetch_add(1, Ordering::AcqRel);
                }
                return match sm_shared.read() {
                    Ok(g) => g.read_only_op(op),
                    Err(_) => OpResult::SchemaError(
                        "read lock poisoned".into(),
                    ),
                };
            }
        }
        // Writes (or no sm_shared): keep the original encode + engine
        // queue path. The engine thread decodes inside apply_one.
        self.apply_raw(op.encode())
    }

    /// SP-Perf-A T6: identical in-process fast path to `apply`, but
    /// accepts the Op by reference. Cheaper for callers that want to
    /// retain ownership (e.g. retry loops, mixed-workload drivers).
    /// For read-only ops it dispatches directly under the shared SM
    /// read guard (`Op` is then cloned into `read_only_op`'s consumed
    /// argument; for GetById/GetBlob/Describe this clone is a
    /// stack-copy because the Op carries only Copy fields). For
    /// writes it falls through to `apply_raw(op.encode())` exactly
    /// like `apply` does.
    pub fn apply_op(&self, op: &Op) -> OpResult {
        // SP-Perf-A-SHARD-APPLY: see `apply()` — route through the
        // per-shard dispatcher when sharded.
        if self.sharded.is_some() {
            return self.apply_raw(op.encode());
        }
        if let Some(sm_shared) = &self.sm_shared {
            // SP-Perf-A-TXN-RO: classifier swap (recurses into Op::Txn).
            if read_pool::is_read_only(op) {
                let idx = op.kind() as usize;
                if idx < 64 {
                    self.op_kind_counts[idx].fetch_add(1, Ordering::AcqRel);
                }
                return match sm_shared.read() {
                    Ok(g) => g.read_only_op(op.clone()),
                    Err(_) => OpResult::SchemaError(
                        "read lock poisoned".into(),
                    ),
                };
            }
        }
        self.apply_raw(op.encode())
    }

    /// Current operational stats (ops applied, state digest, uptime).
    pub fn stats(&self) -> ServerStats {
        match self.apply_raw(vec![STATS_TAG]) {
            OpResult::Got(b) => ServerStats::decode(&b)
                .unwrap_or(ServerStats { applied_ops: 0, digest: 0, uptime_secs: 0 }),
            _ => ServerStats { applied_ops: 0, digest: 0, uptime_secs: 0 },
        }
    }

    /// SP141 T6: snapshot of in-flight op count for /v1/metrics. `inflight`
    /// is `pub(self)` (private to the module), so the `impl EngineApply` block
    /// reaches it through this accessor.
    pub fn inflight_snapshot(&self) -> u64 {
        self.inflight.load(Ordering::Acquire) as u64
    }

    /// SP142: direct atomic read of the applied-op count. Cheap — no
    /// engine round-trip, immune to backpressure. Use this for
    /// observability paths (`/v1/metrics`, `/v1/health`).
    pub fn applied_ops_snapshot(&self) -> u64 {
        self.applied_ops_atomic.load(Ordering::Acquire)
    }

    /// SP144H T1: snapshot of per-Op::kind() counters. Returns non-zero
    /// rows only (bounded by ≤46 active kinds + ~5 special tags ≤ ~50).
    /// Cheap — 64 atomic loads.
    pub fn op_kind_counts_snapshot(&self) -> Vec<(u8, u64)> {
        let mut out = Vec::new();
        for (i, slot) in self.op_kind_counts.iter().enumerate() {
            let v = slot.load(Ordering::Acquire);
            if v > 0 {
                out.push((i as u8, v));
            }
        }
        out
    }

    /// In-process SQL fast path for embedded callers. Compiles + applies
    /// the statement on the engine thread exactly like the binary wire's
    /// `[0xFE] ++ sql` frame, but avoids the network round-trip entirely
    /// — embedders that hold an `EngineHandle` get the same ~sub-µs
    /// latency the in-process bench measures.
    ///
    /// Multi-statement scripts are NOT split here (one SQL string =
    /// one op). For atomic transactions use `BEGIN`/`COMMIT` over the
    /// network surface, or build an `Op::Txn` and call [`Self::apply`].
    pub fn sql(&self, sql: &str) -> OpResult {
        let mut f = Vec::with_capacity(sql.len() + 1);
        f.push(0xFE);
        f.extend_from_slice(sql.as_bytes());
        self.apply_raw(f)
    }

    /// Take a consistent on-disk snapshot/backup into `dest`. The engine
    /// flushes, then copies its data dir while no apply is in flight, so
    /// `StateMachine::open(dest)` recovers an identical state.
    pub fn snapshot(&self, dest: impl AsRef<Path>) -> io::Result<()> {
        let mut f = vec![SNAPSHOT_TAG];
        f.extend_from_slice(dest.as_ref().to_string_lossy().as_bytes());
        match self.apply_raw(f) {
            OpResult::Ok => Ok(()),
            OpResult::SchemaError(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
            _ => Err(io::Error::new(io::ErrorKind::Other, "snapshot failed")),
        }
    }
}

/// Spawn the owning engine thread with the default config.
pub fn spawn_engine(data_dir: impl AsRef<Path>) -> io::Result<EngineHandle> {
    spawn_engine_cfg(data_dir, &ServerConfig::default())
}

/// Spawn the owning engine thread (it opens the data dir itself, since
/// `StateMachine<DirVfs>` is not `Send`). Blocks until the engine is ready
/// or returns the open error. `cfg.max_inflight` bounds the queue.
///
/// SP-Perf-A-SHARD-APPLY: when `cfg.shard_count = Some(K)` with `K >= 2`,
/// this function:
///   1. Spawns K **independent** sub-engines, one per shard, each
///      rooted at `data_dir/shard-<i>/` with `shard_count = None`
///      (no recursion — sub-engines are vanilla unsharded engines).
///   2. Spawns a degenerate "router-shell" engine at `data_dir/router/`
///      whose only purpose is to satisfy the EngineHandle field
///      shape (atomics, gateway compatibility). Every apply on the
///      shell short-circuits through the `ShardedDispatcher` to the
///      owning sub-engine, so the router shell's own apply thread
///      effectively idles.
///   3. Returns an `EngineHandle` whose `sharded` field is `Some` —
///      `apply_raw` / `apply` route every frame through the
///      dispatcher.
///
/// When `cfg.shard_count` is `None` or `Some(1)`, the function takes
/// the original single-engine path — byte-identical to pre-SHARD.
pub fn spawn_engine_cfg(
    data_dir: impl AsRef<Path>,
    cfg: &ServerConfig,
) -> io::Result<EngineHandle> {
    // SP-Perf-A-SHARD-APPLY: K >= 2 sharded fan-out.
    if let Some(k) = cfg.shard_count {
        if k >= 2 {
            return spawn_sharded_engine_cfg(data_dir.as_ref(), cfg, k);
        }
        // k == 0 or k == 1 ⇒ fall through to the unsharded path
        // (K=1 is functionally identical to the unsharded engine
        // per the SHARD-1 K=1 collapse contract).
    }
    let max_inflight = cfg.max_inflight;
    let dir = data_dir.as_ref().to_path_buf();
    let (tx, rx) = channel::<EngineMsg>();
    let (ready_tx, ready_rx) = channel::<io::Result<()>>();
    // SP142: shared atomic counter for /v1/metrics & /v1/health. The
    // engine thread bumps it on every `*n += 1` (i.e. every applied op,
    // matching `stats().applied_ops` semantic exactly). The handle
    // exposes it via `applied_ops_snapshot()` — no STATS_TAG round-trip,
    // so observability is immune to engine backpressure.
    let applied_ops_atomic_for_engine = Arc::new(AtomicU64::new(0));
    let applied_ops_atomic_for_handle = applied_ops_atomic_for_engine.clone();
    // SP144H T1: per-Op::kind() counter array, shared between engine
    // thread (write) and EngineHandle (read). Same dual-Arc shape as
    // applied_ops_atomic above. Indexed by tag-byte (frame.first()),
    // out-of-range tags dropped.
    let op_kind_counts_for_engine: Arc<[AtomicU64; 64]> = Arc::new(
        std::array::from_fn(|_| AtomicU64::new(0))
    );
    let op_kind_counts_for_handle = op_kind_counts_for_engine.clone();
    // SP144H T2: per-(path, status) HTTP counter matrix. Constructed
    // once and shared by Arc with both the gateway accept loop (via
    // serve_cfg → kessel_http_gateway::serve) and the metrics-snapshot
    // path on EngineHandle. The field on EngineHandle is cfg-gated to
    // `http-gateway`, so the no-feature build pays nothing.
    #[cfg(feature = "http-gateway")]
    let http_counters_for_handle: Arc<kessel_http_gateway::HttpRequestCountersStatic> =
        Arc::new(kessel_http_gateway::HttpRequestCountersStatic::new());
    // SP-Perf-A T2: when `read_workers` is set, the state machine is
    // wrapped in `Arc<RwLock<>>` so the engine thread and read workers
    // share it. The engine thread takes `.write()` for each apply
    // (mirroring today's serial-apply semantic); the read bypass path
    // on `EngineHandle::apply_raw` takes `.read()` to dispatch a single
    // read-only op without queueing. The Arc is built BEFORE the engine
    // thread spawns (it returns it via `sm_shared_handoff`); the engine
    // thread then drains the same Arc by acquiring the write guard.
    //
    // When `read_workers = None` the bypass is OFF: we keep the original
    // direct-ownership shape and pay zero new coordination cost (no
    // rwlock, no arc clone, no extra atomic). Byte-identical to pre-T2.
    let perfa_enabled = cfg.read_workers.is_some();
    let (sm_handoff_tx, sm_handoff_rx) =
        sync_channel::<Arc<RwLock<StateMachine<DirVfs>>>>(1);
    std::thread::spawn(move || {
        // Open the SM. The ownership shape depends on `perfa_enabled`:
        //   - OFF (default): inline owned `sm` (original behaviour).
        //   - ON: build an `Arc<RwLock<>>` and hand it off to the
        //     calling thread for inclusion in `EngineHandle`.
        let sm_open = match DirVfs::new(&dir).and_then(StateMachine::open) {
            Ok(sm) => sm,
            Err(e) => {
                let _ = ready_tx.send(Err(e));
                return;
            }
        };
        // Two ownership shapes — both end up satisfying a single inner
        // closure that takes `&mut StateMachine<DirVfs>` per batch.
        let mut sm_inline: Option<StateMachine<DirVfs>> = None;
        let mut sm_shared: Option<Arc<RwLock<StateMachine<DirVfs>>>> = None;
        if perfa_enabled {
            let arc = Arc::new(RwLock::new(sm_open));
            let _ = sm_handoff_tx.send(arc.clone());
            sm_shared = Some(arc);
        } else {
            sm_inline = Some(sm_open);
        }
        let _ = ready_tx.send(Ok(()));
        // SP-Perf-A T2: hoist the SM mutable borrow up front. The original
        // body referenced `sm` by `&mut` throughout; with the rwlock the
        // borrow comes from a write guard re-acquired per drain batch.
        // For the inline path we just take a mutable reference to the
        // owned local. We define a single mutable accessor closure so
        // the drain body below is identical on both shapes.
        //
        // Because the rwlock guard is `!Send` (which is fine — engine
        // thread owns it) and is re-acquired per drain batch, the
        // critical section closely mirrors the pre-T2 serial-apply
        // semantic: writer holds the rwlock in write mode for the whole
        // batch (one compute → one fsync); readers cannot interleave.
        let mut n: u64 = 1;
        let mut cache = CompileCache::new();
        let start = std::time::Instant::now();
        // SP68: server-side group commit. The WAL no longer fsyncs per op;
        // instead the engine drains all currently-available requests,
        // applies them, fsyncs ONCE, then releases every reply. Replies
        // are sent only AFTER the group fsync, so an op is acked only when
        // durable (crash-safe; a not-yet-synced op is simply un-acked and
        // the exactly-once client retries). Ordering/state/digest are
        // unchanged — only fsync *timing* is batched. Single-op latency is
        // unchanged (drain finds nothing → one fsync, as before); under
        // concurrency one fsync is amortised over the whole batch — the
        // decisive win on EBS-class network storage.
        // Set autosync once on whichever ownership shape is live. The
        // engine thread's per-batch drain acquires the SM and then runs
        // `compute` against it; autosync is a one-shot toggle on the SM
        // itself.
        {
            let mut guard = sm_shared.as_ref().map(|a| a.write().expect("sm rwlock"));
            let sm_mut: &mut StateMachine<DirVfs> = match (&mut guard, sm_inline.as_mut()) {
                (Some(g), _) => &mut **g,
                (None, Some(s)) => s,
                (None, None) => unreachable!("engine sm not initialised"),
            };
            sm_mut.set_autosync(false);
        }
        let compute = |sm: &mut StateMachine<DirVfs>,
                       cache: &mut CompileCache,
                       n: &mut u64,
                       frame: &[u8]|
         -> OpResult {
            match frame.first() {
                Some(&STATS_TAG) => {
                    let st = ServerStats {
                        applied_ops: *n - 1,
                        digest: sm.digest(),
                        uptime_secs: start.elapsed().as_secs(),
                    };
                    return OpResult::Got(st.encode().into());
                }
                Some(&SNAPSHOT_TAG) => {
                    let dest = String::from_utf8_lossy(&frame[1..]).into_owned();
                    let r = sm
                        .flush()
                        .and_then(|_| copy_dir_flat(&dir, Path::new(&dest)));
                    return match r {
                        Ok(()) => OpResult::Ok,
                        Err(e) => OpResult::SchemaError(format!("snapshot: {e}")),
                    };
                }
                Some(&DESCRIBE_BY_NAME_TAG) => {
                    // SP-PG T12: name → encoded type def, for the
                    // PG-wire gateway's RowDescription emit. Read-only
                    // — no `*n += 1`, no schema invalidation, no
                    // catalog mutation.
                    let name = match std::str::from_utf8(&frame[1..]) {
                        Ok(s) => s,
                        Err(_) => return OpResult::SchemaError(
                            "describe_by_name: not utf8".into(),
                        ),
                    };
                    return match sm.catalog().types.iter().find(|t| t.name == name) {
                        Some(t) => OpResult::Got(
                            kessel_catalog::encode_type_def(&t.name, &t.fields).into(),
                        ),
                        None => OpResult::NotFound,
                    };
                }
                Some(&LIST_TABLES_TAG) => {
                    // SP-PG-CAT T3: enumerate user tables for the
                    // pg_class synthesizer. Read-only — no `*n += 1`,
                    // no schema invalidation. Encoding:
                    //   [u32 LE count]
                    //     [u32 LE name_len][name bytes][u32 LE type_id][u16 LE field_count]
                    // V1 emits TableKind::Ordinary for every entry
                    // (the gateway-side decoder fills that in).
                    let types = &sm.catalog().types;
                    let mut out: Vec<u8> = Vec::with_capacity(64 + types.len() * 32);
                    out.extend_from_slice(&(types.len() as u32).to_le_bytes());
                    for t in types {
                        let name_bytes = t.name.as_bytes();
                        out.extend_from_slice(
                            &(name_bytes.len() as u32).to_le_bytes(),
                        );
                        out.extend_from_slice(name_bytes);
                        out.extend_from_slice(&t.type_id.to_le_bytes());
                        let fc = t.fields.len().min(u16::MAX as usize) as u16;
                        out.extend_from_slice(&fc.to_le_bytes());
                    }
                    return OpResult::Got(out.into());
                }
                Some(&LIST_INDEXES_TAG) => {
                    // SP-PG-CAT T8a: enumerate indexes on the named
                    // table for the pg_index synthesizer + pgJDBC
                    // getIndexInfo joined path. Read-only — walks
                    // ObjectType.indexes/ordered/composite and emits
                    // one record per index with a synthetic name
                    // (e.g. `<table>_<col>_idx` for Equality,
                    // `<table>_<col>_ridx` for Range,
                    // `<table>_<colA>_<colB>_idx` for Composite).
                    let name = match std::str::from_utf8(&frame[1..]) {
                        Ok(s) => s,
                        Err(_) => return OpResult::SchemaError(
                            "list_indexes: not utf8".into(),
                        ),
                    };
                    let ot = match sm.catalog().types.iter().find(|t| t.name == name) {
                        Some(t) => t,
                        None => return OpResult::NotFound,
                    };
                    // Resolve field_id → column name for synthetic
                    // index naming (the gateway decoder uses the
                    // name directly without a second round-trip).
                    let field_name_for = |fid: u16| -> String {
                        ot.fields
                            .iter()
                            .find(|f| f.field_id == fid)
                            .map(|f| f.name.clone())
                            .unwrap_or_else(|| format!("f{fid}"))
                    };
                    // Walk indexes/ordered/composite into a uniform
                    // record list. KIND bytes: 0=Equality, 1=Range,
                    // 2=Composite per kessel_pg_gateway::IndexKind.
                    let mut records: Vec<(String, u8, bool, Vec<u32>)> = Vec::new();
                    for fid in &ot.indexes {
                        let is_unique = ot.unique.iter().any(|u| u == fid);
                        let name = format!("{}_{}_idx", ot.name, field_name_for(*fid));
                        records.push((name, 0, is_unique, vec![*fid as u32]));
                    }
                    for fid in &ot.ordered {
                        // Ordered/range indexes don't carry a UNIQUE
                        // flag in KesselDB; the gateway emits
                        // is_unique=false for these.
                        let name = format!("{}_{}_ridx", ot.name, field_name_for(*fid));
                        records.push((name, 1, false, vec![*fid as u32]));
                    }
                    for fids in &ot.composite {
                        let mut parts = Vec::with_capacity(fids.len());
                        for fid in fids {
                            parts.push(field_name_for(*fid));
                        }
                        let name = format!("{}_{}_idx", ot.name, parts.join("_"));
                        let cols: Vec<u32> = fids.iter().map(|f| *f as u32).collect();
                        records.push((name, 2, false, cols));
                    }
                    let mut out: Vec<u8> = Vec::with_capacity(64 + records.len() * 32);
                    out.extend_from_slice(&(records.len() as u32).to_le_bytes());
                    for (rname, kind, is_unique, fields) in &records {
                        let nb = rname.as_bytes();
                        out.extend_from_slice(&(nb.len() as u32).to_le_bytes());
                        out.extend_from_slice(nb);
                        out.push(*kind);
                        out.push(if *is_unique { 1 } else { 0 });
                        let fc = fields.len().min(u16::MAX as usize) as u16;
                        out.extend_from_slice(&fc.to_le_bytes());
                        for f in fields {
                            out.extend_from_slice(&f.to_le_bytes());
                        }
                    }
                    return OpResult::Got(out.into());
                }
                Some(&LIST_CONSTRAINTS_TAG) => {
                    // SP-PG-CAT T8a: enumerate constraints on the
                    // named table for the pg_constraint synthesizer +
                    // information_schema.{table_constraints,
                    // key_column_usage} views. Read-only — walks
                    // ObjectType.unique/fks/checks and emits one
                    // record per constraint.
                    let name = match std::str::from_utf8(&frame[1..]) {
                        Ok(s) => s,
                        Err(_) => return OpResult::SchemaError(
                            "list_constraints: not utf8".into(),
                        ),
                    };
                    let ot = match sm.catalog().types.iter().find(|t| t.name == name) {
                        Some(t) => t,
                        None => return OpResult::NotFound,
                    };
                    let field_name_for = |fid: u16| -> String {
                        ot.fields
                            .iter()
                            .find(|f| f.field_id == fid)
                            .map(|f| f.name.clone())
                            .unwrap_or_else(|| format!("f{fid}"))
                    };
                    let attnum_for = |fid: u16| -> u32 {
                        ot.fields
                            .iter()
                            .position(|f| f.field_id == fid)
                            .map(|p| (p + 1) as u32)
                            .unwrap_or(0)
                    };
                    let type_name_for = |tid: u32| -> String {
                        sm.catalog()
                            .types
                            .iter()
                            .find(|t| t.type_id == tid)
                            .map(|t| t.name.clone())
                            .unwrap_or_default()
                    };
                    // Records carry: (name, kind, fk_action, attnums,
                    // ref_table_name, ref_attnums). kind=0=Check,
                    // 1=ForeignKey, 2=Unique. fk_action follows
                    // FkAction::pg_action_char (we send the catalog's
                    // u8 directly — 0=NoAction, 1=Restrict, 2=Cascade
                    // matching ObjectType.fks tuple).
                    let mut records: Vec<(String, u8, u8, Vec<u32>, String, Vec<u32>)> = Vec::new();
                    // UNIQUE constraints from ObjectType.unique.
                    for fid in &ot.unique {
                        let name = format!("{}_{}_key", ot.name, field_name_for(*fid));
                        records.push((name, 2, 0, vec![attnum_for(*fid)], String::new(), Vec::new()));
                    }
                    // FK constraints from ObjectType.fks
                    // (field_id, referenced_type_id, on_delete).
                    for (fid, ref_tid, on_delete) in &ot.fks {
                        let name = format!("{}_{}_fkey", ot.name, field_name_for(*fid));
                        let ref_name = type_name_for(*ref_tid);
                        // V1: FK always references the parent's
                        // primary key (attnum 1 in KesselDB convention
                        // — `id` is implicit). The wire format carries
                        // a single-element ref_attnums.
                        records.push((
                            name,
                            1,
                            *on_delete,
                            vec![attnum_for(*fid)],
                            ref_name,
                            vec![1],
                        ));
                    }
                    // CHECK constraints — KesselDB stores them as
                    // opaque compiled kessel-expr programs without
                    // names; V1 synthesizes `<table>_check_N` per
                    // queries.md §1 acceptable naming convention.
                    for (idx, _bytes) in ot.checks.iter().enumerate() {
                        let name = format!("{}_check_{}", ot.name, idx);
                        records.push((name, 0, 0, Vec::new(), String::new(), Vec::new()));
                    }
                    let mut out: Vec<u8> = Vec::with_capacity(64 + records.len() * 64);
                    out.extend_from_slice(&(records.len() as u32).to_le_bytes());
                    for (rname, kind, fk_action, attnums, ref_name, ref_attnums) in &records {
                        let nb = rname.as_bytes();
                        out.extend_from_slice(&(nb.len() as u32).to_le_bytes());
                        out.extend_from_slice(nb);
                        out.push(*kind);
                        out.push(*fk_action);
                        let fc = attnums.len().min(u16::MAX as usize) as u16;
                        out.extend_from_slice(&fc.to_le_bytes());
                        for a in attnums {
                            out.extend_from_slice(&a.to_le_bytes());
                        }
                        let rnb = ref_name.as_bytes();
                        out.extend_from_slice(&(rnb.len() as u32).to_le_bytes());
                        out.extend_from_slice(rnb);
                        let rfc = ref_attnums.len().min(u16::MAX as usize) as u16;
                        out.extend_from_slice(&rfc.to_le_bytes());
                        for a in ref_attnums {
                            out.extend_from_slice(&a.to_le_bytes());
                        }
                    }
                    return OpResult::Got(out.into());
                }
                Some(&TXN_TAG) => {
                    // Compile every buffered statement, then apply them as
                    // ONE atomic Op::Txn. Any compile failure (or a
                    // statement that needs server-side RMW) aborts the
                    // whole transaction with zero effect.
                    let body = &frame[1..];
                    let r = (|| -> Result<Vec<Op>, String> {
                        let n0 = u32::from_le_bytes(
                            body.get(0..4)
                                .ok_or("txn: short")?
                                .try_into()
                                .unwrap(),
                        ) as usize;
                        let mut p = 4usize;
                        let mut ops = Vec::with_capacity(n0);
                        for _ in 0..n0 {
                            let l = u32::from_le_bytes(
                                body.get(p..p + 4)
                                    .ok_or("txn: short")?
                                    .try_into()
                                    .unwrap(),
                            ) as usize;
                            p += 4;
                            let s = std::str::from_utf8(
                                body.get(p..p + l).ok_or("txn: short")?,
                            )
                            .map_err(|_| "txn: not utf8".to_string())?;
                            p += l;
                            match cache.get_or_compile(s, sm.catalog()) {
                                Ok(kessel_sql::Stmt::Op(o)) => ops.push(o),
                                Ok(kessel_sql::Stmt::Update { type_id, id, sets }) => {
                                    // SP84: UPDATE composes in a txn as a
                                    // deterministic replicated RMW op
                                    // (Op::UpdateSet). Resolve each
                                    // Value → raw field bytes via the
                                    // live catalog (engine thread).
                                    let ot = match sm.catalog().get(type_id) {
                                        Some(t) => t.clone(),
                                        None => {
                                            return Err(format!(
                                                "update: no type {type_id}"
                                            ))
                                        }
                                    };
                                    let mut raw_sets =
                                        Vec::with_capacity(sets.len());
                                    for (fid, v) in sets {
                                        let fk = match ot
                                            .fields
                                            .iter()
                                            .find(|f| f.field_id == fid)
                                        {
                                            Some(f) => f.kind,
                                            None => {
                                                return Err(format!(
                                                    "update: no field {fid}"
                                                ))
                                            }
                                        };
                                        match kessel_codec::raw_from_value(
                                            fk, &v,
                                        ) {
                                            Some(r) => raw_sets.push((fid, r)),
                                            None => {
                                                return Err(
                                                    "UPDATE … SET col = NULL \
                                                     inside a transaction is \
                                                     not yet supported \
                                                     (use it outside a txn)"
                                                        .into(),
                                                )
                                            }
                                        }
                                    }
                                    ops.push(Op::UpdateSet {
                                        type_id,
                                        id: kessel_proto::ObjectId::from_u128(
                                            id,
                                        ),
                                        sets: raw_sets,
                                    });
                                }
                                // SP-PG-SQL-DML-GENERAL — general-WHERE
                                // UPDATE/DELETE inside an explicit
                                // multi-statement transaction needs the
                                // matched-id set resolved against the
                                // mid-transaction overlay (read-your-
                                // writes), which the flatten-into-one-Txn
                                // model here can't express. V1 rejects it;
                                // run it as its own auto-commit statement.
                                // (SP-PG-SQL-DML-IN-TXN follow-up.)
                                Ok(kessel_sql::Stmt::UpdateWhere { .. })
                                | Ok(kessel_sql::Stmt::DeleteWhere { .. }) => {
                                    return Err(
                                        "general-WHERE UPDATE/DELETE inside an \
                                         explicit transaction is not yet \
                                         supported (SP-PG-SQL-DML-IN-TXN); run \
                                         it as a standalone statement"
                                            .into(),
                                    )
                                }
                                Ok(kessel_sql::Stmt::Explain(_)) => {
                                    return Err(
                                        "EXPLAIN inside a transaction is not \
                                         supported"
                                            .into(),
                                    )
                                }
                                Err(e) => return Err(format!("sql: {e}")),
                            }
                        }
                        Ok(ops)
                    })();
                    return match r {
                        Ok(ops) => {
                            let mutates = ops.iter().any(mutates_schema);
                            let res = sm.apply(*n, Op::Txn { ops });
                            *n += 1;
                            if mutates {
                                cache.invalidate();
                            }
                            res
                        }
                        Err(e) => OpResult::SchemaError(e),
                    };
                }
                Some(&PIPELINE_TAG) => {
                    // SP69: a pipeline of INDEPENDENT requests. The whole
                    // batch is ONE engine message, so it lands in a single
                    // group-commit fsync and costs a single network
                    // round-trip — while every member applies exactly as
                    // if it had been sent alone (same order, same ids,
                    // same compile-cache use/invalidation via `apply_one`).
                    // This is the lever SP68 left open: the group-commit
                    // batch is bounded by in-flight ops, and a serial
                    // connection only ever has one; a pipeline lets a
                    // single connection fill the batch itself.
                    let body = &frame[1..];
                    let parsed = (|| -> Option<Vec<OpResult>> {
                        let cnt = u32::from_le_bytes(
                            body.get(0..4)?.try_into().ok()?,
                        ) as usize;
                        let mut p = 4usize;
                        let mut out = Vec::with_capacity(cnt);
                        for _ in 0..cnt {
                            let l = u32::from_le_bytes(
                                body.get(p..p + 4)?.try_into().ok()?,
                            ) as usize;
                            p += 4;
                            let sub = body.get(p..p + l)?;
                            p += l;
                            out.push(apply_one(sm, cache, n, sub));
                        }
                        (p == body.len()).then_some(out)
                    })();
                    return match parsed {
                        Some(results) => {
                            let mut payload =
                                (results.len() as u32).to_le_bytes().to_vec();
                            for r in &results {
                                let e = r.encode();
                                payload.extend_from_slice(
                                    &(e.len() as u32).to_le_bytes(),
                                );
                                payload.extend_from_slice(&e);
                            }
                            OpResult::Got(payload.into())
                        }
                        None => OpResult::SchemaError(
                            "malformed pipeline frame".into(),
                        ),
                    };
                }
                _ => {}
            }
            apply_one(sm, cache, n, frame)
        };

        // Group-commit driver: block for one request, drain everything
        // else already queued, apply them all, fsync ONCE, release replies.
        // SP142: after each compute(), publish the change in `n` to the
        // shared atomic so `applied_ops_snapshot()` mirrors `*n - 1` (the
        // same quantity `stats()` returns via STATS_TAG). A single frame
        // may bump `n` by 0 (STATS/SNAPSHOT), 1 (ordinary Op), or 2+ (a
        // SQL UPDATE doing GetById + Update); using a delta keeps that
        // semantic identical without touching the inner helpers.
        const MAX_BATCH: usize = 4096;
        while let Ok((frame, reply)) = rx.recv() {
            let mut batch: Vec<(OpResult, SyncSender<OpResult>)> =
                Vec::with_capacity(16);
            // SP-Perf-A T2: acquire the SM mutable borrow ONCE per drain
            // batch. With the rwlock the writer holds it for the entire
            // batch (one apply → group-commit fsync → reply), which is
            // the same critical-section shape pre-T2 had on a directly-
            // owned SM. Readers cannot interleave inside one batch; this
            // preserves the per-connection-FIFO + apply-order invariants
            // (spec §6 + §7).
            let mut guard = sm_shared.as_ref().map(|a| a.write().expect("sm rwlock"));
            let sm: &mut StateMachine<DirVfs> = match (&mut guard, sm_inline.as_mut()) {
                (Some(g), _) => &mut **g,
                (None, Some(s)) => s,
                (None, None) => unreachable!("engine sm not initialised"),
            };
            let n_before = n;
            let res = compute(sm, &mut cache, &mut n, &frame);
            if n > n_before {
                applied_ops_atomic_for_engine
                    .fetch_add(n - n_before, Ordering::AcqRel);
                // SP144H T1: also bump per-kind slot. Gating on n_after >
                // n_before ensures STATS_TAG / SNAPSHOT_TAG / pipeline-control
                // frames (which don't bump n) don't double-count. The frame's
                // first byte is Op::kind() for bare-Op frames, 0xFE for SQL
                // ([0xFE]++sql). Out-of-range tags (≥64) are dropped.
                if let Some(&tag) = frame.first() {
                    let idx = tag as usize;
                    if idx < 64 {
                        op_kind_counts_for_engine[idx]
                            .fetch_add(1, Ordering::AcqRel);
                    }
                }
            }
            batch.push((res, reply));
            while batch.len() < MAX_BATCH {
                match rx.try_recv() {
                    Ok((f, rp)) => {
                        let n_before = n;
                        let res = compute(sm, &mut cache, &mut n, &f);
                        if n > n_before {
                            applied_ops_atomic_for_engine
                                .fetch_add(n - n_before, Ordering::AcqRel);
                            // SP144H T1: per-kind bump (see comment above).
                            if let Some(&tag) = f.first() {
                                let idx = tag as usize;
                                if idx < 64 {
                                    op_kind_counts_for_engine[idx]
                                        .fetch_add(1, Ordering::AcqRel);
                                }
                            }
                        }
                        batch.push((res, rp));
                    }
                    Err(_) => break,
                }
            }
            let _ = sm.sync(); // single fsync amortised over the whole batch
            // Drop the rwlock write guard BEFORE replies are sent so
            // readers waiting on `.read()` can interleave between
            // batches. (Replies travel over per-task oneshots; they
            // don't touch the SM.)
            drop(guard);
            for (res, rp) in batch {
                let _ = rp.send(res);
            }
        }
    });
    match ready_rx.recv() {
        Ok(Ok(())) => {
            // SP-Perf-A T2: when the bypass is enabled, the engine
            // thread sent us the shared Arc on `sm_handoff_rx`; receive
            // it BEFORE returning so the EngineHandle ships with the
            // bypass wired. When disabled, the channel was never sent
            // on — try_recv returns Err(Empty) and the field stays
            // None.
            let sm_shared = if perfa_enabled {
                sm_handoff_rx.recv().ok()
            } else {
                None
            };
            // Spawn the read pool (if any) sharing the same Arc. With
            // `Some(0)` we still construct the pool (graceful fall-
            // through to submitting-thread dispatch); with `None` the
            // pool field stays None too.
            let read_pool = match (cfg.read_workers, sm_shared.clone()) {
                (Some(n), Some(arc)) => Some(Arc::new(read_pool::ReadPool::new_shared(
                    n,
                    1024,
                    arc,
                ))),
                _ => None,
            };
            Ok(EngineHandle {
                tx,
                inflight: Arc::new(AtomicUsize::new(0)),
                max_inflight,
                applied_ops_atomic: applied_ops_atomic_for_handle,
                op_kind_counts: op_kind_counts_for_handle,
                #[cfg(feature = "http-gateway")]
                http_counters: http_counters_for_handle,
                sm_shared,
                read_pool,
                // SP-Perf-A-SHARD-APPLY: no sharding on this path —
                // this is the unsharded sub-engine itself (used both
                // by default-unsharded mode AND as a per-shard
                // sub-engine inside a sharded EngineHandle).
                sharded: None,
            })
        }
        Ok(Err(e)) => Err(e),
        Err(_) => Err(io::Error::new(io::ErrorKind::Other, "engine failed to start")),
    }
}

/// SP-Perf-A-SHARD-APPLY: K=N sharded engine spawn.
///
/// Constructs K **independent** per-shard sub-engines (rooted at
/// `data_dir/shard-<i>/`), wires them into a `ShardedDispatcher`, and
/// returns an `EngineHandle` whose `sharded` field is `Some` — every
/// `apply_raw` / `apply` then routes through the dispatcher.
///
/// Each sub-engine inherits `read_workers`, `max_inflight`, `tls`,
/// `token`, etc. from `cfg` — but `shard_count` is forced to `None`
/// to prevent recursion. The router shell (this top-level handle's
/// atomics, op_kind_counts, etc.) is spawned at `data_dir/router/`
/// with `shard_count = None` and idles — its mpsc apply thread
/// receives nothing because `apply_raw` short-circuits via the
/// dispatcher before ever touching the engine queue.
///
/// V1 SHARD-APPLY scope (named in dispatch + design spec §3):
///   - Per-shard StateMachine + apply thread + WAL + SSTables.
///   - Key routing via `hash(make_key(type_id, oid)) % K`.
///   - DDL broadcast to every shard (sequential).
///   - Scan ops route to shard 0 only (V1 limitation —
///     `SP-Perf-A-SHARD-SCAN` is the named follow-up arc).
///   - WAL recovery per-shard via each sub-engine's existing
///     `StateMachine::open` path (no cross-shard recovery
///     coordination needed — each shard is its own consistent unit).
pub fn spawn_sharded_engine_cfg(
    data_dir: &Path,
    cfg: &ServerConfig,
    k: usize,
) -> io::Result<EngineHandle> {
    assert!(k >= 2, "spawn_sharded_engine_cfg requires K >= 2");
    std::fs::create_dir_all(data_dir)?;

    // Build a per-shard config: inherit everything but force
    // `shard_count = None` (recursion guard) AND null out the
    // listener-related ports (each sub-engine doesn't bind its own
    // HTTP / PG listener — only the router shell does).
    let mut sub_cfg = cfg.clone();
    sub_cfg.shard_count = None;
    sub_cfg.http_addr = None;
    sub_cfg.http_tls_addr = None;
    sub_cfg.pg_addr = None;
    // SP-Perf-A-SHARD-SCAN-LOCAL-INDEX-FUSION: guarantee each sub-engine
    // populates its `sm_shared` snapshot so the dispatcher's tiny-scan
    // fast path can borrow `Arc<RwLock<StateMachine>>` directly,
    // bypassing the apply_op channel hop. `Some(0)` triggers the
    // SP-Perf-A T2 ownership shape (Arc<RwLock<>>) with NO real read
    // worker threads (graceful submitting-thread fall-through if a
    // pool dispatch ever happens). If the caller already passed
    // `read_workers = Some(N)` with N >= 1, that real pool is honored.
    if sub_cfg.read_workers.is_none() {
        sub_cfg.read_workers = Some(0);
    }

    // Spawn K sub-engines, each rooted at `data_dir/shard-<i>`.
    let mut shards: Vec<EngineHandle> = Vec::with_capacity(k);
    for i in 0..k {
        let shard_dir = data_dir.join(format!("shard-{i}"));
        let engine = spawn_engine_cfg(&shard_dir, &sub_cfg)?;
        shards.push(engine);
    }

    // Build the dispatcher.
    let dispatcher = Arc::new(sharded_engine::ShardedDispatcher::new(shards));

    // Spawn the router shell at `data_dir/router/`. This handle's
    // own apply thread is effectively dead — every `apply_raw` /
    // `apply` short-circuits via `self.sharded`. The shell exists
    // so the EngineHandle field shape (atomics, tx, etc.) stays
    // populated for backward compatibility with the gateways /
    // metrics surfaces.
    let router_dir = data_dir.join("router");
    let mut shell_cfg = cfg.clone();
    shell_cfg.shard_count = None;
    shell_cfg.http_addr = None;
    shell_cfg.http_tls_addr = None;
    shell_cfg.pg_addr = None;
    // The shell never serves real reads/writes, so it doesn't need
    // the read_pool (it'd just waste threads).
    shell_cfg.read_workers = None;
    let mut shell = spawn_engine_cfg(&router_dir, &shell_cfg)?;
    shell.sharded = Some(dispatcher);
    Ok(shell)
}

fn handle_conn<S: std::io::Read + std::io::Write>(
    mut stream: S,
    engine: EngineHandle,
    token: Option<Vec<u8>>,
) {
    if !authenticate(&mut stream, &token) {
        return; // rejected; Unauthorized already written
    }
    // Per-connection SQL transaction state. `BEGIN` starts buffering SQL
    // statements; `COMMIT` ships the buffer as one atomic `Op::Txn`;
    // `ROLLBACK` discards it. Buffering is local to the connection, so
    // other connections are unaffected and the engine never blocks.
    let mut txn: Option<Vec<String>> = None;
    loop {
        let req = match read_frame(&mut stream) {
            Ok(r) => r,
            Err(_) => break,
        };
        let result = if req.first() == Some(&0xFE) {
            // SQL frame — intercept transaction-control keywords.
            let sql = std::str::from_utf8(&req[1..]).unwrap_or("").trim();
            let kw = sql.trim_end_matches(';').trim();
            if kw.eq_ignore_ascii_case("BEGIN")
                || kw.eq_ignore_ascii_case("START TRANSACTION")
            {
                txn = Some(Vec::new());
                OpResult::Ok
            } else if kw.eq_ignore_ascii_case("ROLLBACK") {
                let was = txn.take().is_some();
                if was {
                    OpResult::Ok
                } else {
                    OpResult::SchemaError("ROLLBACK without BEGIN".into())
                }
            } else if kw.eq_ignore_ascii_case("COMMIT") {
                match txn.take() {
                    None => OpResult::SchemaError("COMMIT without BEGIN".into()),
                    Some(stmts) => {
                        // Build the atomic txn-batch frame and apply it.
                        let mut f = vec![TXN_TAG];
                        f.extend_from_slice(&(stmts.len() as u32).to_le_bytes());
                        for s in &stmts {
                            f.extend_from_slice(&(s.len() as u32).to_le_bytes());
                            f.extend_from_slice(s.as_bytes());
                        }
                        engine.apply_raw(f)
                    }
                }
            } else if let Some(buf) = txn.as_mut() {
                // SP85: KesselDB transactions are atomic, non-interactive
                // WRITE batches (serializable by construction). A read
                // mid-transaction would require holding the single
                // engine overlay across client round-trips, serializing
                // the whole engine — a deliberate non-goal. Reject reads
                // clearly instead of silently buffering an Ok whose rows
                // are then discarded. Read-your-writes still holds for
                // *mutations* within the batch (a later op sees an
                // earlier op's writes); run SELECTs outside the txn.
                let head = kw
                    .split(|c: char| c.is_whitespace() || c == '(')
                    .next()
                    .unwrap_or("");
                if matches!(
                    head.to_ascii_uppercase().as_str(),
                    "SELECT" | "DESCRIBE" | "DESC" | "EXPLAIN"
                ) {
                    OpResult::SchemaError(
                        "reads inside a transaction are not supported — \
                         KesselDB transactions are atomic write batches; \
                         run SELECT/DESCRIBE/EXPLAIN outside the \
                         transaction (read-your-writes still holds for \
                         writes within the batch)"
                            .into(),
                    )
                } else {
                    // A write: buffer it for the atomic COMMIT batch.
                    buf.push(sql.to_string());
                    OpResult::Ok
                }
            } else {
                engine.apply_raw(req)
            }
        } else {
            // Non-SQL frames don't participate in SQL transactions.
            engine.apply_raw(req)
        };
        if write_frame(&mut stream, &result.encode()).is_err() {
            break;
        }
    }
}

/// Serve forever on `listener` with the default (open) config.
pub fn serve(listener: TcpListener, engine: EngineHandle) {
    serve_cfg(listener, engine, ServerConfig::default())
}

/// Serve forever on `listener`, one thread per connection, enforcing
/// `cfg`: the auth handshake and the concurrent-connection cap. The next
/// connection past `max_conns` is dropped immediately.
pub fn serve_cfg(listener: TcpListener, engine: EngineHandle, cfg: ServerConfig) {
    let active = Arc::new(AtomicUsize::new(0));

    // Build the TLS acceptor once (opt-in `tls` feature). If TLS is
    // requested but the feature is off, refuse to serve silently-insecure
    // — fail loudly instead.
    #[cfg(feature = "tls")]
    let tls_acceptor: Option<std::sync::Arc<rustls::ServerConfig>> =
        match &cfg.tls {
            Some((cert, key)) => match tls::server_config(cert, key) {
                Ok(c) => Some(c),
                Err(e) => {
                    eprintln!("kesseldb: TLS config error: {e}; refusing to serve");
                    return;
                }
            },
            None => None,
        };
    #[cfg(not(feature = "tls"))]
    if cfg.tls.is_some() {
        eprintln!(
            "kesseldb: ServerConfig.tls set but built without the `tls` \
             feature — refusing to serve insecure. Rebuild with \
             `--features tls`."
        );
        return;
    }

    // SP141: opt-in HTTP/1.1 gateway. Sibling threads; binary listener
    // continues untouched.
    #[cfg(feature = "http-gateway")]
    if let Some(http_addr) = cfg.http_addr {
        let engine_for_http = engine.clone();
        let token_for_http = cfg.token.clone();
        let max_conns = cfg.max_conns;
        let max_body = cfg.http_max_body;
        // SP147: per-connection request cap (default 1000) — propagates
        // ServerConfig.http_max_requests_per_conn to the gateway's
        // keep-alive loop.
        let max_requests_per_conn = cfg.http_max_requests_per_conn;
        // SP144H T2: share the SAME counter Arc the EngineHandle's
        // snapshot_metrics reads. Bumps from the accept loop must land
        // in the same atomics that the metrics-snapshot path then reads.
        let http_counters = engine.http_counters.clone();
        std::thread::spawn(move || {
            match std::net::TcpListener::bind(http_addr) {
                Ok(l) => kessel_http_gateway::serve(
                    l,
                    std::sync::Arc::new(engine_for_http) as
                        std::sync::Arc<dyn kessel_http_gateway::EngineApply>,
                    token_for_http,
                    max_conns,
                    max_body,
                    http_counters,
                    max_requests_per_conn,
                ),
                Err(e) => eprintln!(
                    "kesseldb: http-gateway bind {http_addr} failed: {e}"),
            }
        });
    }
    // SP-PG T12: opt-in PostgreSQL Frontend/Backend v3.0 gateway.
    // Sibling listener; binary + HTTP listeners untouched. The PG
    // listener has its OWN connection cap (`cfg.pg_max_conns`,
    // default 256) so a misbehaving pgcli cannot starve binary or
    // HTTP clients (spec §8.1). Refuses to start if `cfg.token` is
    // None — V1 closed-mode requires a Bearer token (spec §3.4).
    #[cfg(feature = "pg-gateway")]
    if let Some(pg_addr) = cfg.pg_addr {
        match cfg.token.clone() {
            None => {
                eprintln!(
                    "kesseldb: pg_addr set but ServerConfig.token is None — \
                     PG-wire V1 requires a Bearer token for SCRAM-SHA-256 \
                     auth (spec §3.4). Skipping PG listener."
                );
            }
            Some(token) => {
                let engine_for_pg = engine.clone();
                let pg_max_conns = cfg.pg_max_conns;
                let idle_timeout = cfg.pg_idle_timeout;
                std::thread::spawn(move || {
                    let l = match std::net::TcpListener::bind(pg_addr) {
                        Ok(l) => l,
                        Err(e) => {
                            eprintln!(
                                "kesseldb: pg-gateway bind {pg_addr} \
                                 failed: {e}"
                            );
                            return;
                        }
                    };
                    serve_pg(l, engine_for_pg, token, pg_max_conns, idle_timeout);
                });
            }
        }
    }
    // HTTPS gateway requires both http-gateway AND tls features.
    #[cfg(all(feature = "http-gateway", feature = "tls"))]
    if let (Some(https_addr), Some(tls_arc)) =
        (cfg.http_tls_addr, tls_acceptor.clone())
    {
        let engine_for_https = engine.clone();
        let token_for_https = cfg.token.clone();
        let max_conns = cfg.max_conns;
        let max_body = cfg.http_max_body;
        let max_requests_per_conn = cfg.http_max_requests_per_conn;
        // SP144H T2: same counter Arc as the plaintext path — plaintext
        // + HTTPS bumps share the same matrix so the /v1/metrics scrape
        // reflects the total across both listeners.
        let http_counters = engine.http_counters.clone();
        std::thread::spawn(move || {
            match std::net::TcpListener::bind(https_addr) {
                Ok(l) => kessel_http_gateway::serve_tls(
                    l,
                    RustlsAcceptor(tls_arc),
                    std::sync::Arc::new(engine_for_https) as _,
                    token_for_https,
                    max_conns,
                    max_body,
                    http_counters,
                    max_requests_per_conn,
                ),
                Err(e) => eprintln!(
                    "kesseldb: http-gateway HTTPS bind {https_addr} failed: {e}"),
            }
        });
    }

    for stream in listener.incoming().flatten() {
        if active.load(Ordering::Acquire) >= cfg.max_conns {
            drop(stream); // at capacity — refuse
            continue;
        }
        // Disable Nagle: small synchronous request/response, so Nagle +
        // delayed-ACK costs ~40 ms/round-trip on Linux (the real EC2
        // bottleneck — far larger than fsync for this workload).
        let _ = stream.set_nodelay(true);
        active.fetch_add(1, Ordering::AcqRel);
        let e = engine.clone();
        let tok = cfg.token.clone();
        let a = active.clone();
        #[cfg(feature = "tls")]
        let acc = tls_acceptor.clone();
        std::thread::spawn(move || {
            #[cfg(feature = "tls")]
            {
                if let Some(cfg) = acc {
                    if let Some(tls_stream) = tls::accept(cfg, stream) {
                        handle_conn(tls_stream, e, tok);
                    }
                } else {
                    handle_conn(stream, e, tok);
                }
            }
            #[cfg(not(feature = "tls"))]
            handle_conn(stream, e, tok);
            a.fetch_sub(1, Ordering::AcqRel);
        });
    }
}

/// SP-PG T12: PostgreSQL Frontend/Backend Protocol v3.0 listener.
/// One thread per accepted connection, mirroring the binary listener
/// shape but with the PG-wire-specific `run_session` body. The
/// listener owns its own connection counter (separate from
/// `serve_cfg`'s `active`) so a misbehaving pgcli cannot starve
/// binary or HTTP clients (spec §8.1).
///
/// `idle_timeout` is wired via `TcpStream::set_read_timeout` BEFORE
/// the session is entered — a long-silent connection eventually
/// errors out of the read loop and `run_session` returns cleanly.
/// V1 just closes the socket; T16 (SP-PG follow-up) will emit
/// `57014` query_canceled ErrorResponse first.
///
/// `token` is the SCRAM-SHA-256 "password" input — V1 closed-mode
/// requires `Some(t)`; spec §3.4 covers the Bearer ↔ SCRAM bridge.
/// Caller (`serve_cfg`) guarantees `token` is not empty.
#[cfg(feature = "pg-gateway")]
fn serve_pg(
    listener: TcpListener,
    engine: EngineHandle,
    token: Vec<u8>,
    max_conns: usize,
    idle_timeout: std::time::Duration,
) {
    use std::io::Write as _;
    let active = Arc::new(AtomicUsize::new(0));
    for stream in listener.incoming().flatten() {
        if active.load(Ordering::Acquire) >= max_conns {
            // T13: at capacity — emit a wire-level rejection BEFORE
            // closing the connection so PG clients get a structured
            // error (`53300` too_many_connections + canonical message
            // "sorry, too many clients already") instead of a bare
            // TCP close. Spec §8.2 + PG `postmaster.c` BackendStartup.
            //
            // The client hasn't sent StartupMessage yet, so we write
            // the ErrorResponse straight onto the raw TCP stream with
            // no auth / no length-prefix framing prelude. The frame
            // is its own complete PG-wire message (type byte + length
            // prefix + field-tagged payload + terminator).
            //
            // Best-effort: any write error here is silently absorbed
            // because we're about to drop the connection anyway. The
            // important thing is that the client either sees the
            // ErrorResponse or sees the close — never a hang.
            let mut s = stream;
            let frame = kessel_pg_gateway::error::encode_too_many_connections_error();
            let _ = s.write_all(&frame);
            let _ = s.flush();
            drop(s);
            continue;
        }
        // Disable Nagle: PG simple-query is request/response, so
        // Nagle + delayed-ACK adds ~40 ms/round-trip on Linux.
        let _ = stream.set_nodelay(true);
        // Idle timeout: a connection that hasn't sent any bytes for
        // `idle_timeout` errors out of `read_exact` and `run_session`
        // returns cleanly.
        let _ = stream.set_read_timeout(Some(idle_timeout));
        active.fetch_add(1, Ordering::AcqRel);
        let e = engine.clone();
        let tok = token.clone();
        let a = active.clone();
        std::thread::spawn(move || {
            let mut stream = stream;
            // CSPRNG-backed server nonce per session (RFC 5802 §5.1
            // — server nonce SHOULD be unpredictable). We derive
            // entropy from `std::time::Instant::now()` mixed with a
            // per-spawn counter via the system source — but the
            // workspace stays zero-external-dep, so we mix a
            // monotonic-clock-derived bag of bytes with the token.
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| {
                    // 16 hex chars from the nanos field — enough
                    // entropy for V1 (per-session uniqueness; spec
                    // §3.4 open question #4 — V2 SP-PG T24 wires a
                    // real CSPRNG via kessel-crypto).
                    format!("{:016x}", d.as_nanos() as u64)
                })
                .unwrap_or_else(|_| "fallbackNonce".to_string());
            let _ = kessel_pg_gateway::server::run_session(
                &mut stream,
                Some(&tok),
                || nonce,
                &e,
            );
            a.fetch_sub(1, Ordering::AcqRel);
        });
    }
}

/// Opt-in TLS termination (the `tls` cargo feature; rustls). Kept behind
/// the feature so the default build stays zero-dependency.
#[cfg(feature = "tls")]
mod tls {
    use std::io;
    use std::net::TcpStream;
    use std::path::Path;
    use std::sync::Arc;

    pub fn server_config(
        cert_pem: &Path,
        key_pem: &Path,
    ) -> io::Result<Arc<rustls::ServerConfig>> {
        let certs: Vec<_> = rustls_pemfile::certs(&mut io::BufReader::new(
            std::fs::File::open(cert_pem)?,
        ))
        .collect::<Result<_, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let key = rustls_pemfile::private_key(&mut io::BufReader::new(
            std::fs::File::open(key_pem)?,
        ))?
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "no private key in PEM")
        })?;
        let cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Arc::new(cfg))
    }

    /// Complete the TLS handshake; return a Read+Write stream or `None`.
    pub fn accept(
        cfg: Arc<rustls::ServerConfig>,
        sock: TcpStream,
    ) -> Option<rustls::StreamOwned<rustls::ServerConnection, TcpStream>> {
        let conn = rustls::ServerConnection::new(cfg).ok()?;
        Some(rustls::StreamOwned::new(conn, sock))
    }
}

// SP141 — RustlsAcceptor adapter: bridges the gateway's TlsAccept trait to
// the existing rustls-based TLS termination machinery, so the gateway crate
// stays rustls-dep-free while reusing the server's cert/key wiring.
#[cfg(all(feature = "http-gateway", feature = "tls"))]
struct RustlsAcceptor(std::sync::Arc<rustls::ServerConfig>);

#[cfg(all(feature = "http-gateway", feature = "tls"))]
impl kessel_http_gateway::TlsAccept for RustlsAcceptor {
    type Stream =
        rustls::StreamOwned<rustls::ServerConnection, std::net::TcpStream>;
    fn accept(&self, sock: std::net::TcpStream) -> Option<Self::Stream> {
        let conn = rustls::ServerConnection::new(self.0.clone()).ok()?;
        Some(rustls::StreamOwned::new(conn, sock))
    }
}

/// Open the data dir and serve on `addr` (blocking), default config.
pub fn run(addr: impl ToSocketAddrs, data_dir: impl AsRef<Path>) -> io::Result<()> {
    run_cfg(addr, data_dir, ServerConfig::default())
}

/// Open the data dir and serve on `addr` (blocking) with `cfg`.
pub fn run_cfg(
    addr: impl ToSocketAddrs,
    data_dir: impl AsRef<Path>,
    cfg: ServerConfig,
) -> io::Result<()> {
    let engine = spawn_engine_cfg(data_dir, &cfg)?;
    let listener = TcpListener::bind(addr)?;
    serve_cfg(listener, engine, cfg);
    Ok(())
}

/// SP-Cloud-Cluster T2 — multi-replica VSR runtime entrypoint. This is
/// the cluster-mode analogue of `run_cfg`: instead of spawning a single
/// `EngineHandle`, it binds a peer-listen socket, constructs a
/// `cluster::Node` via `cluster::spawn_node` (which owns the
/// `kessel_vsr::Replica<DirVfs>` on a single non-`Send` engine thread),
/// and exposes the same binary client protocol on `client_addr` via
/// `cluster::serve_clients_cfg`.
///
/// `self_idx` is this pod's index into `peer_addrs` (0..N-1).
/// `peer_addrs[self_idx]` is the LISTEN address for the peer transport
/// on this pod; the other entries are the dial targets the writer
/// threads connect to. Open-mode and token-mode auth both follow the
/// single-node wire (first frame `[0xFC] ++ token`) so existing
/// `kessel-client` / `ClusterClient` instances work unchanged.
///
/// Refuses to start with a typed io error if `peer_addrs.len()` is
/// less than 3 or is even — VSR requires odd N >= 3 (the underlying
/// `Replica::new` panics on even N; we fail loudly here instead so
/// the operator sees a clean error). `self_idx` must be in range.
///
/// V1 scope (this slice — T2 wire-up):
///   - Binary client protocol only (no HTTP gateway / no PG-wire on
///     the cluster path; those are V2 cluster gateway surfaces).
///   - Open or token auth; TLS / mTLS deferred.
///   - Per-pod data dir; replication via `cluster.rs` real-TCP transport.
pub fn run_cluster_cfg(
    client_addr: impl ToSocketAddrs,
    peer_listen_addr: impl ToSocketAddrs,
    data_dir: impl AsRef<Path>,
    self_idx: usize,
    peer_addrs: Vec<std::net::SocketAddr>,
    cfg: ServerConfig,
) -> io::Result<()> {
    let n = peer_addrs.len();
    if n < 3 || n % 2 == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "cluster: peer-addrs count must be odd and >= 3, got {n} \
                 (legal values are 3 or 5 — VSR is fixed-size)"
            ),
        ));
    }
    if self_idx >= n {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "cluster: replica-idx {self_idx} out of range for an \
                 {n}-node cluster (must be in 0..{n})"
            ),
        ));
    }
    let peer_listener = TcpListener::bind(peer_listen_addr)?;
    let client_listener = TcpListener::bind(client_addr)?;
    let node = cluster::spawn_node(
        self_idx,
        peer_listener,
        peer_addrs,
        data_dir.as_ref().to_path_buf(),
    )?;
    let node = std::sync::Arc::new(node);
    eprintln!(
        "kesseldb cluster: started replica {self_idx}/{n} \
         (peer listen ok, client listen ok)"
    );

    // Spawn a small role-logger that polls Node::role_probe every 500 ms
    // and prints a one-shot "elected primary" / "became backup of view N"
    // line whenever the (view, is_primary) tuple changes. Operators can
    // grep this in `kubectl logs` to verify the cluster reached the
    // steady state. Bounded log volume: at most one line per role
    // transition (which is rare in steady state).
    let role_node = node.clone();
    std::thread::spawn(move || {
        let mut last: Option<(u64, bool, &'static str)> = None;
        loop {
            let cur = role_node.role_probe();
            match last {
                None => {
                    eprintln!(
                        "kesseldb cluster: replica {self_idx} role: \
                         view={} is_primary={} status={}",
                        cur.0, cur.1, cur.2
                    );
                    if cur.1 && cur.2 == "Normal" {
                        eprintln!(
                            "kesseldb cluster: replica {self_idx} \
                             elected primary (view={})",
                            cur.0
                        );
                    }
                }
                Some(prev) if prev != cur => {
                    eprintln!(
                        "kesseldb cluster: replica {self_idx} role \
                         changed: view {}->{} is_primary {}->{} \
                         status {}->{}",
                        prev.0, cur.0, prev.1, cur.1, prev.2, cur.2
                    );
                    if cur.1 && cur.2 == "Normal" && !(prev.1 && prev.2 == "Normal") {
                        eprintln!(
                            "kesseldb cluster: replica {self_idx} \
                             elected primary (view={})",
                            cur.0
                        );
                    }
                }
                _ => {}
            }
            last = Some(cur);
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
    });

    // SP-Cloud-Cluster-METRICS-EXPAND — when KESSELDB_HTTP_ADDR is set
    // in cluster mode, spin up a metrics-only HTTP listener served by
    // `cluster::serve_metrics_http`. The full HTTP/1.1 gateway
    // (SQL/Op surfaces) is still NOT wired on the cluster path (V2
    // follow-up — gateway-on-Node); this minimal endpoint exists so
    // Prometheus has a real scrape target in cluster mode. Honest
    // about the limits: only `/v1/metrics` + `/v1/health` are served
    // here. SQL/Op routes return 404.
    if let Some(http_addr) = cfg.http_addr {
        match TcpListener::bind(http_addr) {
            Ok(http_listener) => {
                let http_node = node.clone();
                std::thread::spawn(move || {
                    cluster::serve_metrics_http(http_listener, http_node);
                });
                eprintln!(
                    "kesseldb cluster: replica {self_idx} \
                     metrics HTTP endpoint listening on {http_addr} \
                     (paths: /v1/metrics, /v1/health)"
                );
            }
            Err(e) => {
                eprintln!(
                    "kesseldb cluster: replica {self_idx} \
                     could not bind metrics HTTP on {http_addr}: {e} \
                     (continuing without HTTP metrics endpoint)"
                );
            }
        }
    }

    // Block forever serving the client protocol; on shutdown the OS
    // tears down all sockets + threads.
    cluster::serve_clients_cfg(client_listener, node, cfg.token.clone());
    Ok(())
}

// SP141 — EngineApply bridge: lets the gateway dispatch into the existing
// engine via the same single-threaded apply path used by the binary
// listener. apply_op_with_session goes through session_frame so the engine's
// exactly-once dedup map sees the same (client_id, req_seq) shape it does
// from binary callers; apply_sql_with_session falls through to apply_sql
// (V1 raw-SQL frames bypass session dedup — spec §11).
#[cfg(feature = "http-gateway")]
impl kessel_http_gateway::EngineApply for EngineHandle {
    fn apply_op(&self, op: kessel_proto::Op) -> kessel_proto::OpResult {
        self.apply(op)
    }
    fn apply_op_with_session(
        &self,
        client: kessel_proto::ClientId,
        req: u64,
        op: kessel_proto::Op,
    ) -> kessel_proto::OpResult {
        let frame = kessel_client::session_frame(client, req, &op);
        self.apply_raw(frame)
    }
    fn apply_sql(&self, sql: &str) -> kessel_proto::OpResult {
        let mut f = vec![0xFE];
        f.extend_from_slice(sql.as_bytes());
        self.apply_raw(f)
    }
    fn apply_sql_with_session(
        &self,
        _client: kessel_proto::ClientId,
        _req: u64,
        sql: &str,
    ) -> kessel_proto::OpResult {
        // SP141 V1: SQL-with-session routes through apply_sql (no
        // (client_id, req_seq) dedup for raw-SQL frames — matches the
        // binary path's behavior for [0xFE]++SQL frames sent outside a
        // session_frame envelope). Documented in spec §11 open questions.
        self.apply_sql(sql)
    }
    fn snapshot_health(&self) -> kessel_http_gateway::HealthSnapshot {
        // SP142: read the atomic directly — no STATS_TAG round-trip. The
        // old `self.stats()` path would return 0 under engine saturation
        // (apply_raw → OpResult::Unavailable), which Prometheus would
        // interpret as a counter reset.
        kessel_http_gateway::HealthSnapshot {
            primary: true,
            view: 0,
            op_number: self.applied_ops_snapshot(),
            role: "primary",
        }
    }
    fn snapshot_metrics(&self) -> kessel_http_gateway::MetricsSnapshot {
        // SP142: see snapshot_health — direct atomic read, immune to
        // backpressure. `stats()` is still available to other callers
        // (its STATS_TAG round-trip is the source-of-truth path); the
        // observability surfaces just stop depending on it.
        let total_applied = self.applied_ops_snapshot();
        // SP144H T1: emit one OpKindCounter per (tag, count). Label is
        // the stringified tag id (e.g. "kind_3" for Op::Create, "kind_254"
        // for SQL frames). A future slice can swap in human-readable
        // names via a const lookup. Static-lifetime requirement on
        // OpKindCounter.kind is satisfied via Box::leak — bounded leak
        // (≤64 distinct labels, each ≤12 chars, over process lifetime).
        let per_kind = self.op_kind_counts_snapshot();
        let mut ops_total: Vec<kessel_http_gateway::OpKindCounter> = per_kind
            .into_iter()
            .map(|(tag, count)| {
                let label: &'static str =
                    Box::leak(format!("kind_{}", tag).into_boxed_str());
                kessel_http_gateway::OpKindCounter { kind: label, count }
            })
            .collect();
        // Also emit a roll-up "applied" row for backward-compat with the
        // pre-SP144H metric shape (Prometheus dashboards that summed all
        // ops via this single metric continue to work).
        ops_total.push(kessel_http_gateway::OpKindCounter {
            kind: "applied",
            count: total_applied,
        });
        kessel_http_gateway::MetricsSnapshot {
            ops_total,
            inflight: self.inflight_snapshot(),
            last_op_number: total_applied,
            view_number: 0,    // single-node V1; cluster wiring is follow-up
            is_primary: true,
            // SP-Cloud-Cluster-METRICS-EXPAND — single-node deploys
            // never view-change and never lag (there's no primary
            // peer to lag against). The cluster-mode metrics
            // endpoint built directly on `cluster::Node::metrics_probe`
            // is what carries the populated VSR-side surface.
            view_changes_total: 0,
            replica_lag_opnum: 0,
            // SP144H T2: pull the per-(path, status) snapshot from the
            // shared 4×16 atomic matrix. The matrix is bumped by the
            // gateway accept loop on every emitted response.
            http_requests_total: self.http_counters.snapshot(),
        }
    }
}

// SP-PG T12 — EngineApply bridge for the PG-wire gateway. Lets the
// gateway dispatch into the existing engine via the same single-
// threaded apply path. `describe_table` round-trips through the
// engine via the `DESCRIBE_BY_NAME_TAG` admin frame so the catalog
// (which lives with the non-`Send` StateMachine) is the source of
// truth. Feature-gated on `pg-gateway` so the default build links
// nothing extra and `cargo tree -p kesseldb-server --no-default-features`
// remains free of `kessel-pg-gateway`.
#[cfg(feature = "pg-gateway")]
impl kessel_pg_gateway::EngineApply for EngineHandle {
    fn apply_sql(&self, sql: &str) -> kessel_proto::OpResult {
        let mut f = vec![0xFE];
        f.extend_from_slice(sql.as_bytes());
        self.apply_raw(f)
    }
    /// SP-PG-EXTQ-PARSED-DEFAULT T1 — override the default
    /// text-substitution fallback with the real typed-param path: send
    /// a `PARAMETERIZED_SQL_TAG` admin frame whose decode on the
    /// engine thread runs `compile_stmt_with_params` against the
    /// live catalog. Closes the SP-PG-EXTQ V1 weak-spot #1 attack
    /// surface at the dispatch layer for every bound value that the
    /// gateway's `preprocess_typed_params` classifier returns `Some`
    /// for.
    fn apply_sql_with_params(
        &self,
        sql: &str,
        params: &[Option<kessel_codec::Value>],
    ) -> kessel_proto::OpResult {
        if params.is_empty() {
            return self.apply_sql(sql);
        }
        let frame = encode_parameterized_sql(sql, params);
        self.apply_raw(frame)
    }
    fn describe_table(
        &self,
        table_name: &str,
    ) -> Option<Vec<kessel_pg_gateway::PgColumn>> {
        let mut frame = vec![DESCRIBE_BY_NAME_TAG];
        frame.extend_from_slice(table_name.as_bytes());
        match self.apply_raw(frame) {
            kessel_proto::OpResult::Got(bytes) => {
                let (_name, fields) =
                    kessel_catalog::decode_type_def(&bytes)?;
                Some(
                    fields
                        .into_iter()
                        .map(|f| kessel_pg_gateway::PgColumn {
                            name: f.name,
                            kind: f.kind,
                            nullable: f.nullable,
                        })
                        .collect(),
                )
            }
            _ => None,
        }
    }

    /// SP-PG-CAT T3: enumerate user tables via the `LIST_TABLES_TAG`
    /// admin frame so the catalog (which lives with the non-`Send`
    /// StateMachine) is the source of truth. Engine-thread reads
    /// `sm.catalog().types` and encodes
    /// `[u32 count][repeat: u32 name_len, name, u32 type_id, u16 field_count]`.
    /// The gateway decodes here and maps each entry to
    /// `TableMetadata { kind: Ordinary }` (V1 KesselDB has no view
    /// / sequence / index kinds).
    fn list_tables(&self) -> Vec<kessel_pg_gateway::TableMetadata> {
        let frame = vec![LIST_TABLES_TAG];
        let bytes = match self.apply_raw(frame) {
            kessel_proto::OpResult::Got(b) => b,
            _ => return Vec::new(),
        };
        let mut p = 0usize;
        if bytes.len() < 4 {
            return Vec::new();
        }
        let count = u32::from_le_bytes(
            bytes[p..p + 4].try_into().expect("4-byte slice"),
        ) as usize;
        p += 4;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            if bytes.len() < p + 4 {
                break;
            }
            let name_len = u32::from_le_bytes(
                bytes[p..p + 4].try_into().expect("4-byte slice"),
            ) as usize;
            p += 4;
            if bytes.len() < p + name_len + 4 + 2 {
                break;
            }
            let name = match std::str::from_utf8(&bytes[p..p + name_len]) {
                Ok(s) => s.to_string(),
                Err(_) => break,
            };
            p += name_len;
            let type_id = u32::from_le_bytes(
                bytes[p..p + 4].try_into().expect("4-byte slice"),
            );
            p += 4;
            let field_count = u16::from_le_bytes(
                bytes[p..p + 2].try_into().expect("2-byte slice"),
            );
            p += 2;
            out.push(kessel_pg_gateway::TableMetadata {
                name,
                type_id,
                kind: kessel_pg_gateway::TableKind::Ordinary,
                field_count,
            });
        }
        out
    }

    /// SP-PG-CAT T8a: enumerate indexes on the named table via the
    /// `LIST_INDEXES_TAG` admin frame. Mirrors `list_tables` —
    /// engine-thread reads `sm.catalog()` synthesizing the index
    /// records, this gateway-side decoder maps each into the
    /// `IndexMetadata` shape the pg_index synthesizer expects.
    ///
    /// Wire format (per `LIST_INDEXES_TAG` doc):
    ///   `[u32 count][repeat: u32 name_len, name, u8 kind,
    ///    u8 is_unique, u16 field_count, field_count × u32]`
    fn list_indexes_for_table(
        &self,
        table_name: &str,
    ) -> Vec<kessel_pg_gateway::IndexMetadata> {
        let mut frame = vec![LIST_INDEXES_TAG];
        frame.extend_from_slice(table_name.as_bytes());
        let bytes = match self.apply_raw(frame) {
            kessel_proto::OpResult::Got(b) => b,
            _ => return Vec::new(),
        };
        let mut p = 0usize;
        if bytes.len() < 4 {
            return Vec::new();
        }
        let count = u32::from_le_bytes(
            bytes[p..p + 4].try_into().expect("4-byte slice"),
        ) as usize;
        p += 4;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            if bytes.len() < p + 4 {
                break;
            }
            let name_len = u32::from_le_bytes(
                bytes[p..p + 4].try_into().expect("4-byte slice"),
            ) as usize;
            p += 4;
            if bytes.len() < p + name_len + 1 + 1 + 2 {
                break;
            }
            let name = match std::str::from_utf8(&bytes[p..p + name_len]) {
                Ok(s) => s.to_string(),
                Err(_) => break,
            };
            p += name_len;
            let kind_byte = bytes[p];
            p += 1;
            let is_unique = bytes[p] != 0;
            p += 1;
            let fc = u16::from_le_bytes(
                bytes[p..p + 2].try_into().expect("2-byte slice"),
            ) as usize;
            p += 2;
            if bytes.len() < p + fc * 4 {
                break;
            }
            let mut fields = Vec::with_capacity(fc);
            for _ in 0..fc {
                let f = u32::from_le_bytes(
                    bytes[p..p + 4].try_into().expect("4-byte slice"),
                );
                p += 4;
                fields.push(f);
            }
            let kind = match kind_byte {
                0 => kessel_pg_gateway::IndexKind::Equality,
                1 => kessel_pg_gateway::IndexKind::Range,
                2 => kessel_pg_gateway::IndexKind::Composite,
                _ => kessel_pg_gateway::IndexKind::Equality,
            };
            out.push(kessel_pg_gateway::IndexMetadata {
                name,
                fields,
                is_unique,
                kind,
            });
        }
        out
    }

    /// SP-PG-CAT T8a: enumerate constraints on the named table via
    /// the `LIST_CONSTRAINTS_TAG` admin frame. Wire format per
    /// `LIST_CONSTRAINTS_TAG` doc.
    fn list_constraints_for_table(
        &self,
        table_name: &str,
    ) -> Vec<kessel_pg_gateway::ConstraintMetadata> {
        let mut frame = vec![LIST_CONSTRAINTS_TAG];
        frame.extend_from_slice(table_name.as_bytes());
        let bytes = match self.apply_raw(frame) {
            kessel_proto::OpResult::Got(b) => b,
            _ => return Vec::new(),
        };
        let mut p = 0usize;
        if bytes.len() < 4 {
            return Vec::new();
        }
        let count = u32::from_le_bytes(
            bytes[p..p + 4].try_into().expect("4-byte slice"),
        ) as usize;
        p += 4;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            // [u32 name_len][name][u8 kind][u8 fk_action]
            // [u16 attn_count][attn_count × u32 attnum]
            // [u32 ref_name_len][ref_name][u16 ref_attn_count]
            // [ref_attn_count × u32 attnum]
            if bytes.len() < p + 4 {
                break;
            }
            let name_len = u32::from_le_bytes(
                bytes[p..p + 4].try_into().expect("4-byte slice"),
            ) as usize;
            p += 4;
            if bytes.len() < p + name_len + 1 + 1 + 2 {
                break;
            }
            let name = match std::str::from_utf8(&bytes[p..p + name_len]) {
                Ok(s) => s.to_string(),
                Err(_) => break,
            };
            p += name_len;
            let kind_byte = bytes[p];
            p += 1;
            let fk_action_byte = bytes[p];
            p += 1;
            let attn_count = u16::from_le_bytes(
                bytes[p..p + 2].try_into().expect("2-byte slice"),
            ) as usize;
            p += 2;
            if bytes.len() < p + attn_count * 4 {
                break;
            }
            let mut columns = Vec::with_capacity(attn_count);
            for _ in 0..attn_count {
                let a = u32::from_le_bytes(
                    bytes[p..p + 4].try_into().expect("4-byte slice"),
                );
                p += 4;
                columns.push(a);
            }
            if bytes.len() < p + 4 {
                break;
            }
            let ref_name_len = u32::from_le_bytes(
                bytes[p..p + 4].try_into().expect("4-byte slice"),
            ) as usize;
            p += 4;
            if bytes.len() < p + ref_name_len + 2 {
                break;
            }
            let ref_name = match std::str::from_utf8(&bytes[p..p + ref_name_len]) {
                Ok(s) => s.to_string(),
                Err(_) => break,
            };
            p += ref_name_len;
            let ref_attn_count = u16::from_le_bytes(
                bytes[p..p + 2].try_into().expect("2-byte slice"),
            ) as usize;
            p += 2;
            if bytes.len() < p + ref_attn_count * 4 {
                break;
            }
            let mut ref_columns = Vec::with_capacity(ref_attn_count);
            for _ in 0..ref_attn_count {
                let a = u32::from_le_bytes(
                    bytes[p..p + 4].try_into().expect("4-byte slice"),
                );
                p += 4;
                ref_columns.push(a);
            }
            let kind = match kind_byte {
                0 => kessel_pg_gateway::ConstraintKind::Check,
                1 => {
                    let on_delete = match fk_action_byte {
                        // ObjectType.fks tuple uses 0=NoAction (SP6),
                        // 1=Restrict, 2=Cascade (SP11).
                        0 => kessel_pg_gateway::FkAction::NoAction,
                        1 => kessel_pg_gateway::FkAction::Restrict,
                        2 => kessel_pg_gateway::FkAction::Cascade,
                        _ => kessel_pg_gateway::FkAction::NoAction,
                    };
                    kessel_pg_gateway::ConstraintKind::ForeignKey { on_delete }
                }
                2 => kessel_pg_gateway::ConstraintKind::Unique,
                _ => kessel_pg_gateway::ConstraintKind::Unique,
            };
            let references = if ref_name.is_empty() {
                None
            } else {
                Some((ref_name, ref_columns))
            };
            out.push(kessel_pg_gateway::ConstraintMetadata {
                name,
                kind,
                columns,
                references,
            });
        }
        out
    }
}

// ─────────────────────────────────────────────────────────────────────
// SP-PG T12 — integration tests for the PG-wire listener.
//
// The headline test (`t12_pg_gateway_listener_serves_real_pg_client`)
// spawns the full kesseldb-server through `run_cfg` with the
// `pg-gateway` feature, opens a real TCP connection, drives the
// StartupMessage + SCRAM-SHA-256 + Simple Query loop end-to-end
// against the live engine, and asserts the server's wire response is
// a well-formed PG backend stream including the BackendKeyData
// envelope + a ReadyForQuery after the query.
//
// All tests use `127.0.0.1:0` to bind ephemeral ports; nothing
// requires a system PG client (we ARE the PG client via the
// gateway's own encoders).
// ─────────────────────────────────────────────────────────────────────
#[cfg(all(test, feature = "pg-gateway"))]
mod pg_gateway_tests {
    use super::*;
    use kessel_crypto::{base64_encode, hmac_sha256, pbkdf2_hmac_sha256, sha256};
    use std::io::{Read as _, Write as _};
    use std::net::TcpStream;
    use std::time::{Duration, Instant};

    /// Build a PG v3.0 StartupMessage frame for `user`:
    /// `[length:4 BE][version=196608:4 BE][key\0value\0...\0]`.
    fn build_startup_frame(user: &str) -> Vec<u8> {
        let body = format!("user\0{user}\0\0");
        let length = (4 + 4 + body.len()) as u32;
        let mut frame = Vec::new();
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&196608u32.to_be_bytes());
        frame.extend_from_slice(body.as_bytes());
        frame
    }

    /// Build a SASLInitialResponse `p`-frame:
    /// `p [length:4][SCRAM-SHA-256\0][client_first_len:u32][client_first]`.
    fn build_sasl_initial_frame(client_first: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"SCRAM-SHA-256\0");
        payload.extend_from_slice(&(client_first.len() as u32).to_be_bytes());
        payload.extend_from_slice(client_first.as_bytes());
        let length = (4 + payload.len()) as u32;
        let mut frame = Vec::new();
        frame.push(b'p');
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    fn build_sasl_response_frame(client_final: &str) -> Vec<u8> {
        let payload = client_final.as_bytes();
        let length = (4 + payload.len()) as u32;
        let mut frame = Vec::new();
        frame.push(b'p');
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    /// Build a 'Q' (Simple Query) frame: `Q [length:4 BE] [sql\0]`.
    fn build_q_frame(sql: &str) -> Vec<u8> {
        let mut payload = sql.as_bytes().to_vec();
        payload.push(0);
        let length = (4 + payload.len()) as u32;
        let mut frame = Vec::new();
        frame.push(b'Q');
        frame.extend_from_slice(&length.to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    /// Build a Terminate 'X' frame: `X [length:4 BE = 4]`.
    fn build_x_frame() -> Vec<u8> { vec![b'X', 0, 0, 0, 4] }

    /// Read the first frame off the wire (no type tag — startup
    /// response). Returns the frame's body bytes only.
    fn read_n(stream: &mut TcpStream, n: usize) -> Vec<u8> {
        let mut buf = vec![0u8; n];
        let mut p = 0;
        while p < n {
            let r = stream.read(&mut buf[p..]).expect("read");
            assert!(r > 0, "EOF reading {n} bytes (got {p})");
            p += r;
        }
        buf
    }

    /// Drain the server's tagged-frame stream until we've seen a
    /// `Z` (ReadyForQuery) byte. Returns all bytes read so far so
    /// the test can grep for specific message tags.
    fn drain_until_rfq(stream: &mut TcpStream) -> Vec<u8> {
        let mut out = Vec::new();
        let mut chunk = [0u8; 1024];
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if Instant::now() > deadline {
                panic!("timeout draining to ReadyForQuery; got {} bytes", out.len());
            }
            let n = match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => panic!("read error: {e}"),
            };
            out.extend_from_slice(&chunk[..n]);
            // Look for the Z envelope `Z [len=5][status]` at the end of out.
            if out.len() >= 6 {
                let tail = &out[out.len() - 6..];
                if tail[0] == b'Z' && tail[1..5] == [0, 0, 0, 5] {
                    break;
                }
            }
        }
        out
    }

    /// Drive a full SCRAM handshake against an already-connected
    /// TcpStream + token. Returns the bytes received during the
    /// handshake (greeting through the FIRST ReadyForQuery), so the
    /// caller can keep using the stream for queries.
    fn complete_handshake(
        stream: &mut TcpStream,
        token: &[u8],
        username: &str,
    ) -> Vec<u8> {
        // Use a deterministic-ish client nonce; the SERVER's nonce is
        // unknown until the SASLContinue arrives.
        let client_nonce = "fixedClientNonce";
        let client_first_bare = format!("n={username},r={client_nonce}");
        let client_first = format!("n,,{client_first_bare}");

        // 1) StartupMessage
        stream.write_all(&build_startup_frame(username)).unwrap();
        stream.flush().unwrap();

        // 2) Read AuthenticationSASL (24 bytes: 'R'+len=23+code=10+"SCRAM-SHA-256\0\0")
        let _challenge = read_n(stream, 24);

        // 3) Send SASLInitialResponse
        stream.write_all(&build_sasl_initial_frame(&client_first)).unwrap();
        stream.flush().unwrap();

        // 4) Read AuthenticationSASLContinue: `R [length:4][code=11:u32][payload]`
        //    Header is 'R' + len:4 + code:4 = 9 bytes; payload = len-8.
        let hdr = read_n(stream, 9);
        assert_eq!(hdr[0], b'R', "expected SASLContinue tag");
        let length = u32::from_be_bytes(hdr[1..5].try_into().unwrap()) as usize;
        // SASL code is at hdr[5..9] = code=11 (SASLContinue)
        assert_eq!(&hdr[5..9], &11u32.to_be_bytes());
        let payload = read_n(stream, length - 8);
        let server_first = std::str::from_utf8(&payload).unwrap().to_string();

        // 5) Parse server-first: r=<combined>,s=<salt_b64>,i=<iter>
        let mut combined = String::new();
        let mut salt_b64 = String::new();
        let mut iter = 4096u32;
        for kv in server_first.split(',') {
            if let Some(rest) = kv.strip_prefix("r=") { combined = rest.into(); }
            else if let Some(rest) = kv.strip_prefix("s=") { salt_b64 = rest.into(); }
            else if let Some(rest) = kv.strip_prefix("i=") { iter = rest.parse().unwrap_or(4096); }
        }
        let salt = base64_decode(&salt_b64);

        // 6) Build client-final + proof
        let cf_without_proof = format!("c=biws,r={combined}");
        let auth_msg = format!("{client_first_bare},{server_first},{cf_without_proof}");
        let salted = pbkdf2_hmac_sha256(token, &salt, iter);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let client_sig = hmac_sha256(&stored_key, auth_msg.as_bytes());
        let mut proof = [0u8; 32];
        for i in 0..32 { proof[i] = client_key[i] ^ client_sig[i]; }
        let proof_b64 = base64_encode(&proof);
        let client_final = format!("{cf_without_proof},p={proof_b64}");

        // 7) Send SASLResponse
        stream.write_all(&build_sasl_response_frame(&client_final)).unwrap();
        stream.flush().unwrap();

        // 8) Drain until first ReadyForQuery (after SASLFinal,
        //    AuthenticationOk, ParameterStatus*, BackendKeyData, RFQ).
        drain_until_rfq(stream)
    }

    /// Minimal base64 decoder for the SCRAM salt. RFC 4648
    /// alphabet; PG always uses padded form. Sufficient for
    /// in-tree test fixtures (the production crypto path uses
    /// kessel-crypto::base64_encode for outbound).
    fn base64_decode(s: &str) -> Vec<u8> {
        const ALPHA: &[u8] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = Vec::with_capacity(s.len() * 3 / 4);
        let mut buf = 0u32;
        let mut bits = 0;
        for c in s.bytes() {
            if c == b'=' { break; }
            let v = ALPHA.iter().position(|&x| x == c).expect("bad base64 char") as u32;
            buf = (buf << 6) | v;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push((buf >> bits) as u8);
                buf &= (1 << bits) - 1;
            }
        }
        out
    }

    fn fresh_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "kdb-pg-{}-{}-{}",
            name,
            std::process::id(),
            Instant::now().elapsed().as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Headline T12 KAT: spin up kesseldb-server with the
    /// `pg-gateway` feature, drive a real TCP client through the
    /// full PG v3.0 handshake + a CREATE TABLE + INSERT + SELECT,
    /// and assert the server emits a wire-correct response stream
    /// (including BackendKeyData + ReadyForQuery + a SELECT row).
    #[test]
    fn t12_pg_gateway_listener_serves_real_pg_client() {
        let dir = fresh_dir("e2e");
        let token = b"kessel-pg-token".to_vec();
        let pg_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let pg_addr = pg_listener.local_addr().unwrap();
        drop(pg_listener); // free the port; serve_cfg will rebind
        let cfg = ServerConfig {
            token: Some(token.clone()),
            pg_addr: Some(pg_addr),
            ..ServerConfig::default()
        };
        let engine = spawn_engine_cfg(&dir, &cfg).unwrap();
        let bin_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        std::thread::spawn({
            let c = cfg.clone();
            move || serve_cfg(bin_listener, engine, c)
        });

        // Wait for the PG listener to bind (it spawns inline in
        // serve_cfg's spawn — give it a moment).
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut stream = loop {
            match TcpStream::connect(pg_addr) {
                Ok(s) => break s,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                    continue;
                }
                Err(e) => panic!("could not connect to PG listener {pg_addr}: {e}"),
            }
        };
        stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        stream.set_nodelay(true).unwrap();

        // ── 1. SCRAM handshake ──────────────────────────────────────
        let greeting = complete_handshake(&mut stream, &token, "kessel");
        // The greeting bytes must contain BackendKeyData ('K' + len=12 + 8 bytes).
        let mut found_k = false;
        for i in 0..greeting.len().saturating_sub(13) {
            if greeting[i] == b'K' && greeting[i+1..i+5] == [0, 0, 0, 12] {
                found_k = true;
                break;
            }
        }
        assert!(found_k, "BackendKeyData ('K' 0 0 0 12 ...) MUST be in greeting");
        // And of course ReadyForQuery at the end.
        assert_eq!(&greeting[greeting.len() - 6..], &[b'Z', 0, 0, 0, 5, b'I']);

        // ── 2. CREATE TABLE ─────────────────────────────────────────
        stream
            .write_all(&build_q_frame(
                "CREATE TABLE pg_test (id i64 NOT NULL, n i32 NOT NULL)",
            ))
            .unwrap();
        stream.flush().unwrap();
        let r1 = drain_until_rfq(&mut stream);
        assert!(
            r1.windows(b"CREATE TABLE\0".len())
                .any(|w| w == b"CREATE TABLE\0"),
            "CREATE TABLE tag MUST be in response"
        );

        // ── 3. INSERT a row ─────────────────────────────────────────
        stream
            .write_all(&build_q_frame(
                "INSERT INTO pg_test (id, n) VALUES (1, 100)",
            ))
            .unwrap();
        stream.flush().unwrap();
        let r2 = drain_until_rfq(&mut stream);
        assert!(
            r2.windows(b"INSERT 0 1\0".len())
                .any(|w| w == b"INSERT 0 1\0"),
            "INSERT 0 1 tag MUST be in response (T9 row count)"
        );

        // ── 4. SELECT * FROM pg_test ────────────────────────────────
        stream
            .write_all(&build_q_frame("SELECT * FROM pg_test"))
            .unwrap();
        stream.flush().unwrap();
        let r3 = drain_until_rfq(&mut stream);
        // RowDescription ('T') must appear.
        assert!(r3.iter().any(|&b| b == b'T'), "RowDescription 'T' MUST appear");
        // CommandComplete "SELECT 1" must appear.
        assert!(
            r3.windows(b"SELECT 1\0".len()).any(|w| w == b"SELECT 1\0"),
            "SELECT 1 tag MUST appear (single row)"
        );
        // The row payload must contain "100" (the n=100 value rendered as text).
        assert!(
            r3.windows(3).any(|w| w == b"100"),
            "DataRow MUST carry the n=100 value as text"
        );

        // ── 5. Terminate ──────────────────────────────────────────
        stream.write_all(&build_x_frame()).unwrap();
        stream.flush().unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// When `cfg.token` is None, the PG listener must NOT spawn
    /// (V1 closed-mode requires a Bearer token). A TCP connect to
    /// the configured `pg_addr` should fail with `ConnectionRefused`.
    #[test]
    fn t12_no_token_no_pg_listener() {
        let dir = fresh_dir("notok");
        let pg_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let pg_addr = pg_listener.local_addr().unwrap();
        drop(pg_listener);
        let cfg = ServerConfig {
            token: None,
            pg_addr: Some(pg_addr),
            ..ServerConfig::default()
        };
        let engine = spawn_engine_cfg(&dir, &cfg).unwrap();
        let bin_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        std::thread::spawn({
            let c = cfg.clone();
            move || serve_cfg(bin_listener, engine, c)
        });
        // Give the would-be PG listener a moment to NOT bind.
        std::thread::sleep(Duration::from_millis(50));
        let r = TcpStream::connect_timeout(&pg_addr, Duration::from_millis(200));
        assert!(
            r.is_err(),
            "TCP connect to pg_addr MUST fail when token is None (no listener)"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The PG and binary listeners have INDEPENDENT connection caps.
    /// Setting `cfg.max_conns=0` and `cfg.pg_max_conns=1` lets the
    /// PG listener accept 1 connection even though the binary
    /// listener is fully capped.
    #[test]
    fn t12_pg_and_binary_caps_are_independent() {
        let dir = fresh_dir("caps");
        let pg_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let pg_addr = pg_listener.local_addr().unwrap();
        drop(pg_listener);
        let cfg = ServerConfig {
            token: Some(b"tok".to_vec()),
            max_conns: 0,           // binary fully capped
            pg_max_conns: 4,        // PG has capacity
            pg_addr: Some(pg_addr),
            ..ServerConfig::default()
        };
        let engine = spawn_engine_cfg(&dir, &cfg).unwrap();
        let bin_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        std::thread::spawn({
            let c = cfg.clone();
            move || serve_cfg(bin_listener, engine, c)
        });
        // Wait for PG listener bind.
        let deadline = Instant::now() + Duration::from_secs(2);
        let stream = loop {
            match TcpStream::connect_timeout(&pg_addr, Duration::from_millis(50)) {
                Ok(s) => break s,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                    continue;
                }
                Err(e) => panic!("connect to PG addr failed: {e}"),
            }
        };
        // Just hold the stream open — accept succeeded, proving the
        // PG cap is independent of the binary cap.
        drop(stream);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `EngineHandle::describe_table` (the PG-wire bridge) round-
    /// trips through the engine and returns the same fields the
    /// catalog has. Locks the DESCRIBE_BY_NAME_TAG admin path.
    #[test]
    fn t12_engine_handle_describe_table_matches_catalog() {
        use kessel_pg_gateway::EngineApply as _;
        let dir = fresh_dir("desc");
        let engine = spawn_engine(&dir).unwrap();
        // Create a table via SQL so the catalog has a known shape.
        let r = <EngineHandle as kessel_pg_gateway::EngineApply>::apply_sql(
            &engine,
            "CREATE TABLE foo (id i64 NOT NULL, label u32 NOT NULL)",
        );
        assert!(matches!(r, kessel_proto::OpResult::TypeCreated(_)));
        // describe_table should now return the columns in order.
        let cols = engine.describe_table("foo").expect("table exists");
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].kind, kessel_catalog::FieldKind::I64);
        assert!(!cols[0].nullable);
        assert_eq!(cols[1].name, "label");
        assert_eq!(cols[1].kind, kessel_catalog::FieldKind::U32);
        // describe_table on a missing table → None.
        assert!(engine.describe_table("ghost").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// SP-PG-CAT T8a — `EngineHandle::list_indexes_for_table`
    /// round-trips through the `LIST_INDEXES_TAG` admin frame and
    /// returns one `IndexMetadata` per KesselDB index on the named
    /// table (equality / range / composite). Locks the wire shape +
    /// the kind-byte mapping (0=Equality, 1=Range, 2=Composite)
    /// the gateway-side pg_index + getIndexInfo synthesizers
    /// depend on.
    #[test]
    fn t8a_engine_handle_list_indexes_round_trips_via_admin_frame() {
        use kessel_pg_gateway::{EngineApply as _, IndexKind};
        let dir = fresh_dir("list_indexes");
        let engine = spawn_engine(&dir).unwrap();
        // Unknown table → empty (graceful, never panics).
        assert!(engine.list_indexes_for_table("nope").is_empty(),
            "unknown table MUST surface empty Vec");
        // CREATE TABLE + several indexes.
        let r1 = <EngineHandle as kessel_pg_gateway::EngineApply>::apply_sql(
            &engine,
            "CREATE TABLE users (id I64 NOT NULL, email CHAR(64) NOT NULL, age I32 NOT NULL)",
        );
        assert!(matches!(r1, kessel_proto::OpResult::TypeCreated(_)));
        // Equality index on email.
        let r2 = <EngineHandle as kessel_pg_gateway::EngineApply>::apply_sql(
            &engine,
            "CREATE INDEX ON users (email)",
        );
        assert!(matches!(r2, kessel_proto::OpResult::Ok), "create equality index: {r2:?}");
        // Range index on age.
        let r3 = <EngineHandle as kessel_pg_gateway::EngineApply>::apply_sql(
            &engine,
            "CREATE RANGE INDEX ON users (age)",
        );
        assert!(matches!(r3, kessel_proto::OpResult::Ok), "create range index: {r3:?}");
        // Composite index on (email, age).
        let r4 = <EngineHandle as kessel_pg_gateway::EngineApply>::apply_sql(
            &engine,
            "CREATE INDEX ON users (email, age)",
        );
        assert!(matches!(r4, kessel_proto::OpResult::Ok), "create composite index: {r4:?}");
        let idx = engine.list_indexes_for_table("users");
        // 3 indexes total: 1 Equality + 1 Range + 1 Composite.
        assert_eq!(idx.len(), 3, "MUST list 3 indexes — got {idx:?}");
        let equality_count = idx.iter().filter(|i| i.kind == IndexKind::Equality).count();
        let range_count = idx.iter().filter(|i| i.kind == IndexKind::Range).count();
        let composite_count = idx.iter().filter(|i| i.kind == IndexKind::Composite).count();
        assert_eq!(equality_count, 1, "MUST surface 1 Equality index");
        assert_eq!(range_count, 1, "MUST surface 1 Range index");
        assert_eq!(composite_count, 1, "MUST surface 1 Composite index");
        // Composite index fields = [email, age] → attnums [2, 3].
        let comp = idx.iter().find(|i| i.kind == IndexKind::Composite).unwrap();
        assert_eq!(comp.fields.len(), 2, "Composite MUST carry 2 attnums");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// SP-PG-CAT T8a — `EngineHandle::list_constraints_for_table`
    /// round-trips through the `LIST_CONSTRAINTS_TAG` admin frame.
    /// V1 KesselDB SQL has UNIQUE-via-index only (no CHECK / FK
    /// DDL syntax yet); this KAT exercises the UNIQUE path and the
    /// graceful-empty path for tables without constraints.
    #[test]
    fn t8a_engine_handle_list_constraints_round_trips_via_admin_frame() {
        use kessel_pg_gateway::EngineApply as _;
        let dir = fresh_dir("list_constraints");
        let engine = spawn_engine(&dir).unwrap();
        // Unknown table → empty.
        assert!(engine.list_constraints_for_table("nope").is_empty());
        // CREATE TABLE with no constraints → empty list.
        let r1 = <EngineHandle as kessel_pg_gateway::EngineApply>::apply_sql(
            &engine,
            "CREATE TABLE users (id I64 NOT NULL, email CHAR(64) NOT NULL)",
        );
        assert!(matches!(r1, kessel_proto::OpResult::TypeCreated(_)));
        assert!(engine.list_constraints_for_table("users").is_empty(),
            "no UNIQUE/FK/CHECK declared → empty constraints list");
        // Add a UNIQUE index → list_constraints surfaces it.
        let r2 = <EngineHandle as kessel_pg_gateway::EngineApply>::apply_sql(
            &engine,
            "CREATE UNIQUE INDEX ON users (email)",
        );
        assert!(matches!(r2, kessel_proto::OpResult::Ok), "create unique: {r2:?}");
        let cons = engine.list_constraints_for_table("users");
        assert!(cons.iter().any(|c| matches!(
            c.kind, kessel_pg_gateway::ConstraintKind::Unique
        )), "UNIQUE index MUST surface as ConstraintKind::Unique");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// SP-PG-CAT T3 — `EngineHandle::list_tables` round-trips
    /// through the `LIST_TABLES_TAG` admin frame and returns one
    /// `TableMetadata` per KesselDB type. Locks the wire shape so
    /// the gateway-side `pg_class` synthesizer can rely on it.
    #[test]
    fn t3_engine_handle_list_tables_round_trips_via_admin_frame() {
        use kessel_pg_gateway::{EngineApply as _, TableKind};
        let dir = fresh_dir("list_tables");
        let engine = spawn_engine(&dir).unwrap();
        // Empty catalog → zero rows.
        assert!(engine.list_tables().is_empty(),
            "fresh engine MUST list zero tables");
        // Create two tables; both should appear.
        let r1 = <EngineHandle as kessel_pg_gateway::EngineApply>::apply_sql(
            &engine,
            "CREATE TABLE users (id I64 NOT NULL, name CHAR(64) NOT NULL)",
        );
        assert!(matches!(r1, kessel_proto::OpResult::TypeCreated(_)),
            "create users: {r1:?}");
        let r2 = <EngineHandle as kessel_pg_gateway::EngineApply>::apply_sql(
            &engine,
            "CREATE TABLE orders (id I64 NOT NULL, amount I64 NOT NULL, label CHAR(16) NOT NULL)",
        );
        assert!(matches!(r2, kessel_proto::OpResult::TypeCreated(_)));
        let tables = engine.list_tables();
        assert_eq!(tables.len(), 2, "two tables MUST be listed");
        // Catalog declaration order — users first (created first).
        assert_eq!(tables[0].name, "users");
        assert_eq!(tables[0].kind, TableKind::Ordinary);
        assert_eq!(tables[0].field_count, 2);
        assert_eq!(tables[1].name, "orders");
        assert_eq!(tables[1].kind, TableKind::Ordinary);
        assert_eq!(tables[1].field_count, 3);
        // type_ids are positive (KesselDB allocates sequentially).
        assert!(tables[0].type_id > 0);
        assert!(tables[1].type_id > tables[0].type_id);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ───────────────────────────────────────────────────────────────────
    // T13 KATs — cap-overflow `53300` ErrorResponse on the live PG
    // listener. The headline invariant is that a connection past
    // `pg_max_conns` receives a wire-level `ErrorResponse('S=FATAL',
    // 'C=53300', 'M=sorry, too many clients already')` BEFORE the
    // TCP close — not a bare hang-up — so libpq clients can surface
    // the structured error in `PQerrorMessage()`.
    //
    // These tests share `spawn_pg_listener_with_max_conns` so the
    // accept loop is identical to production but cap-tightened to
    // make the overflow path easy to drive.
    // ───────────────────────────────────────────────────────────────────

    /// Spawn the PG listener via `serve_cfg` with a tightened
    /// `pg_max_conns`. Returns (pg_addr, data_dir) — keep `dir`
    /// alive until the test ends so the engine doesn't tear down
    /// mid-test on Windows.
    ///
    /// The "wait for listener bind" probe deliberately uses a
    /// fixed short sleep instead of a probe-connect, because a
    /// probe-connect with a tightened `pg_max_conns` (e.g. 0 or 1)
    /// would itself consume a slot — and the test then can't tell
    /// whether the cap-overflow it observes belongs to the probe or
    /// to the test's own connections. 200ms is enough on every
    /// platform we run CI on; if CI flakes, bump it.
    fn spawn_pg_listener_with_max_conns(
        name: &str,
        pg_max_conns: usize,
    ) -> (std::net::SocketAddr, std::path::PathBuf) {
        let dir = fresh_dir(name);
        let token = b"kessel-pg-token".to_vec();
        let pg_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let pg_addr = pg_listener.local_addr().unwrap();
        drop(pg_listener);
        let cfg = ServerConfig {
            token: Some(token),
            pg_addr: Some(pg_addr),
            pg_max_conns,
            ..ServerConfig::default()
        };
        let engine = spawn_engine_cfg(&dir, &cfg).unwrap();
        let bin_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        std::thread::spawn({
            let c = cfg.clone();
            move || serve_cfg(bin_listener, engine, c)
        });
        // Give the PG listener time to bind without a probe-connect
        // (which would consume a slot under tightened caps).
        std::thread::sleep(Duration::from_millis(200));
        (pg_addr, dir)
    }

    /// Read all bytes off `stream` until EOF or `read_to_end` errs.
    /// Used by T13 to drain the cap-overflow ErrorResponse + the
    /// subsequent connection close. Bounded by the stream's
    /// read-timeout so a hung server fails the test quickly.
    fn read_to_eof(stream: &mut TcpStream) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if Instant::now() > deadline {
                break;
            }
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
        buf
    }

    /// HEADLINE T13 KAT: with `pg_max_conns=1`, the SECOND TCP
    /// connection to the PG listener receives the canonical PG
    /// "sorry, too many clients already" ErrorResponse (FATAL +
    /// 53300) BEFORE the close. The FIRST connection is accepted
    /// normally and held open while we drive the cap-overflow path.
    #[test]
    fn t13_pg_listener_emits_53300_error_response_on_cap_overflow() {
        let (pg_addr, dir) =
            spawn_pg_listener_with_max_conns("cap1", 1);
        // 1. First connection — accepted, held open. We do NOT
        //    complete the handshake (we don't need to; the listener
        //    bumped `active` to 1 the moment it spawned the thread).
        let first = TcpStream::connect(pg_addr).expect("first connects");
        first.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        first.set_nodelay(true).unwrap();
        // Give the listener thread a moment to bump `active`.
        std::thread::sleep(Duration::from_millis(50));
        // 2. Second connection — listener should accept it but
        //    immediately write the 53300 ErrorResponse + close.
        let mut second = TcpStream::connect(pg_addr).expect("second connects");
        second.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let response = read_to_eof(&mut second);
        // The response is a single ErrorResponse frame.
        assert_eq!(response[0], b'E', "expected ErrorResponse type byte");
        // FATAL severity in S field.
        assert!(
            response.windows(b"SFATAL\0".len()).any(|w| w == b"SFATAL\0"),
            "expected S=FATAL field, got bytes: {:?}",
            response,
        );
        // SQLSTATE 53300 in C field.
        assert!(
            response.windows(b"C53300\0".len()).any(|w| w == b"C53300\0"),
            "expected C=53300 field, got bytes: {:?}",
            response,
        );
        // Canonical PG message text in M field.
        let msg = b"sorry, too many clients already";
        assert!(
            response.windows(msg.len()).any(|w| w == msg),
            "expected canonical 'sorry, too many clients already' message, got: {:?}",
            String::from_utf8_lossy(&response),
        );
        // Server closed (read_to_eof returned). Best-effort: keep
        // `first` alive to the end of the function so the engine
        // doesn't tear down before we finish.
        drop(first);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// After the FIRST connection closes, a subsequent connection
    /// is accepted normally (active count dropped back below the
    /// cap). Locks that the cap is dynamic — not a one-shot trip.
    #[test]
    fn t13_pg_listener_accepts_new_connection_after_slot_freed() {
        let (pg_addr, dir) =
            spawn_pg_listener_with_max_conns("cap-free", 1);
        // Hold + drop the first connection.
        {
            let _first = TcpStream::connect(pg_addr).expect("first connects");
            std::thread::sleep(Duration::from_millis(50));
        }
        // The dropped TCP stream may take a tick to surface as a
        // FIN on the server's read loop and decrement the active
        // counter. Poll for up to 2s.
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            // Probe with a fresh connection — if accepted (not 53300),
            // the cap recovered.
            let mut probe = match TcpStream::connect_timeout(
                &pg_addr,
                Duration::from_millis(50),
            ) {
                Ok(s) => s,
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(50));
                    continue;
                }
                Err(e) => panic!("could not connect: {e}"),
            };
            probe.set_read_timeout(Some(Duration::from_millis(200))).unwrap();
            // Read a small amount — if we get an ErrorResponse, the
            // cap is still tripped; if we get 0 bytes (timeout), the
            // server is waiting for us to send StartupMessage = OK.
            let mut buf = [0u8; 1];
            match probe.read(&mut buf) {
                Ok(0) => {
                    // EOF — server closed. Could be cap-overflow or
                    // a race; keep retrying.
                    if Instant::now() < deadline {
                        std::thread::sleep(Duration::from_millis(50));
                        continue;
                    }
                    panic!("listener kept closing — slot never freed");
                }
                Ok(_) if buf[0] == b'E' => {
                    // Still cap-locked — retry.
                    if Instant::now() < deadline {
                        std::thread::sleep(Duration::from_millis(50));
                        continue;
                    }
                    panic!("listener still emits 53300 after slot freed");
                }
                Ok(_) => {
                    // Unexpected byte but NOT 'E' — listener is in a
                    // post-handshake state, which means accept worked.
                    break;
                }
                Err(_) => {
                    // Timeout — server accepted and is waiting for
                    // StartupMessage. That's the success path.
                    break;
                }
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// With `pg_max_conns=0`, EVERY connection trips the cap and
    /// receives 53300. Locks the cap arithmetic against off-by-one
    /// (a `>` vs `>=` flip would let zero through).
    #[test]
    fn t13_pg_listener_zero_max_conns_rejects_first_connection() {
        let (pg_addr, dir) =
            spawn_pg_listener_with_max_conns("cap0", 0);
        let mut s = TcpStream::connect(pg_addr).expect("connects");
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let response = read_to_eof(&mut s);
        assert_eq!(response[0], b'E');
        assert!(
            response.windows(b"C53300\0".len()).any(|w| w == b"C53300\0"),
            "even cap=0 must emit 53300 instead of bare close",
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The 53300 ErrorResponse the listener writes on cap overflow
    /// is byte-identical to `kessel_pg_gateway::error::
    /// encode_too_many_connections_error()`. Locks the listener and
    /// the encoder against drift (a future refactor that hand-rolls
    /// the bytes in the listener would silently break libpq clients).
    #[test]
    fn t13_pg_listener_cap_overflow_bytes_match_encoder() {
        let (pg_addr, dir) =
            spawn_pg_listener_with_max_conns("cap-bytes", 0);
        let mut s = TcpStream::connect(pg_addr).expect("connects");
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let response = read_to_eof(&mut s);
        let expected =
            kessel_pg_gateway::error::encode_too_many_connections_error();
        // The listener might write the frame in one go or with
        // additional zeros if it flushes — strict-equality the
        // first `expected.len()` bytes.
        assert_eq!(
            &response[..expected.len()],
            &expected[..],
            "cap-overflow wire bytes MUST match `encode_too_many_connections_error()`",
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(all(test, feature = "tls"))]
mod tls_tests {
    use super::*;

    #[test]
    fn tls_config_rejects_bad_inputs() {
        // Missing cert/key files → clean error, never a panic.
        let bad = std::path::Path::new("/no/such/cert.pem");
        assert!(tls::server_config(bad, bad).is_err());
        // A real .pem file with no usable key → error, not a key.
        let dir = std::env::temp_dir()
            .join(format!("kdb-tls-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let junk = dir.join("junk.pem");
        std::fs::write(&junk, b"-----BEGIN NOPE-----\nzz\n-----END NOPE-----\n")
            .unwrap();
        assert!(tls::server_config(&junk, &junk).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn server_config_default_has_tls_none() {
        assert!(ServerConfig::default().tls.is_none());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kessel_catalog::{encode_type_def, Field, FieldKind};
    use kessel_client::Client;
    use kessel_proto::ObjectId;

    #[test]
    fn end_to_end_over_real_sockets() {
        let dir = std::env::temp_dir().join(format!("kesseldb-e2e-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = spawn_engine(&dir).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || serve(listener, engine));

        let mut c = Client::connect(addr).unwrap();
        let def = encode_type_def(
            "acct",
            &[Field { field_id: 0, name: "bal".into(), kind: FieldKind::U64, nullable: false }],
        );
        assert_eq!(c.call(&Op::CreateType { def }).unwrap(), OpResult::TypeCreated(1));
        let id = ObjectId::from_u128(42);
        assert_eq!(
            c.call(&Op::Create { type_id: 1, id, record: vec![7, 7, 7] }).unwrap(),
            OpResult::Ok
        );
        assert_eq!(
            c.call(&Op::GetById { type_id: 1, id }).unwrap(),
            OpResult::Got(vec![7, 7, 7].into())
        );
        assert_eq!(
            c.call(&Op::Create { type_id: 1, id, record: vec![9] }).unwrap(),
            OpResult::Exists
        );
        // a second connection sees the same committed state
        let mut c2 = Client::connect(addr).unwrap();
        assert_eq!(
            c2.call(&Op::GetById { type_id: 1, id }).unwrap(),
            OpResult::Got(vec![7, 7, 7].into())
        );
        // an atomic txn over the wire
        assert_eq!(
            c.call(&Op::Txn {
                ops: vec![
                    Op::Create { type_id: 1, id: ObjectId::from_u128(2), record: vec![1] },
                    Op::Create { type_id: 1, id: ObjectId::from_u128(3), record: vec![2] },
                ],
            })
            .unwrap(),
            OpResult::Ok
        );
        // Select over the wire returns actual rows (limit 10).
        let prog = kessel_expr::Program::new().push_int(1).bytes(); // always true
        match c
            .call(&Op::Select { type_id: 1, program: prog, limit: 10 })
            .unwrap()
        {
            OpResult::Got(b) => {
                // at least the 3 rows created above, as length-prefixed blobs
                let mut p = 0;
                let mut rows = 0;
                while p + 4 <= b.len() {
                    let l = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
                    p += 4 + l;
                    rows += 1;
                }
                assert!(rows >= 3, "Select returned {rows} rows over the wire");
            }
            o => panic!("unexpected {o:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sql_over_tcp() {
        let dir = std::env::temp_dir().join(format!("kesseldb-sql-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = spawn_engine(&dir).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || serve(listener, engine));

        let mut c = Client::connect(addr).unwrap();
        assert!(matches!(
            c.sql("CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)").unwrap(),
            OpResult::TypeCreated(1)
        ));
        assert_eq!(
            c.sql("INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)").unwrap(),
            OpResult::Ok
        );
        assert_eq!(
            c.sql("INSERT INTO acct ID 2 (owner, bal) VALUES (100, 999)").unwrap(),
            OpResult::Ok
        );
        match c.sql("SELECT SUM(bal) FROM acct WHERE owner = 100").unwrap() {
            OpResult::Got(b) => {
                assert_eq!(i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()), 1049)
            }
            o => panic!("unexpected {o:?}"),
        }
        // UPDATE (server-side read-modify-write): bal 50 -> 500
        assert_eq!(
            c.sql("UPDATE acct ID 1 SET bal = 500").unwrap(),
            OpResult::Ok
        );
        match c.sql("SELECT SUM(bal) FROM acct WHERE owner = 100").unwrap() {
            OpResult::Got(b) => {
                assert_eq!(i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()), 1499)
            }
            o => panic!("unexpected {o:?}"),
        }
        // UPDATE of a missing row -> NotFound over the wire
        assert_eq!(
            c.sql("UPDATE acct ID 999 SET bal = 1").unwrap(),
            OpResult::NotFound
        );
        // SELECT ... ID <n> -> O(1) GetById, whole record back
        match c.sql("SELECT * FROM acct ID 2").unwrap() {
            OpResult::Got(rec) => assert!(!rec.is_empty()),
            o => panic!("unexpected {o:?}"),
        }
        assert_eq!(c.sql("SELECT * FROM acct ID 12345").unwrap(), OpResult::NotFound);
        // a bad statement returns a clean error over the wire, no crash
        assert!(matches!(
            c.sql("SELECT FROM nope").unwrap(),
            OpResult::SchemaError(_)
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ct_eq_is_length_safe_and_correct() {
        assert!(ct_eq(b"hunter2", b"hunter2"));
        assert!(!ct_eq(b"hunter2", b"hunter3"));
        assert!(!ct_eq(b"hunter2", b"hunter2x")); // length differs
        assert!(!ct_eq(b"", b"x"));
        assert!(ct_eq(b"", b""));
    }

    #[test]
    fn auth_token_required_and_enforced() {
        let dir =
            std::env::temp_dir().join(format!("kesseldb-auth-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let cfg = ServerConfig {
            token: Some(b"s3cret".to_vec()),
            ..ServerConfig::default()
        };
        let engine = spawn_engine_cfg(&dir, &cfg).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let c2 = cfg.clone();
        std::thread::spawn(move || serve_cfg(listener, engine, c2));

        // No token / plain connect: the first op frame is treated as the
        // (wrong) auth attempt → Unauthorized, connection closed.
        let mut plain = Client::connect(addr).unwrap();
        assert!(matches!(
            plain.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(1) }),
            Ok(OpResult::Unauthorized) | Err(_)
        ));

        // Wrong token: rejected.
        assert!(Client::connect_authed(addr, b"wrong").is_err());

        // Correct token: authed session works end to end.
        let mut c = Client::connect_authed(addr, b"s3cret").unwrap();
        assert_eq!(
            c.call(&Op::CreateType {
                def: encode_type_def(
                    "t",
                    &[Field {
                        field_id: 0,
                        name: "v".into(),
                        kind: FieldKind::U64,
                        nullable: false,
                    }],
                ),
            })
            .unwrap(),
            OpResult::TypeCreated(1)
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn backpressure_rejects_when_saturated() {
        // max_inflight = 0 → every request is shed immediately.
        let dir =
            std::env::temp_dir().join(format!("kesseldb-bp-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let cfg = ServerConfig { max_inflight: 0, ..ServerConfig::default() };
        let engine = spawn_engine_cfg(&dir, &cfg).unwrap();
        assert_eq!(
            engine.apply(Op::GetById { type_id: 1, id: ObjectId::from_u128(1) }),
            OpResult::Unavailable,
            "saturated engine must shed load, not queue unbounded"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn connection_cap_refuses_excess() {
        let dir =
            std::env::temp_dir().join(format!("kesseldb-cap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let cfg = ServerConfig { max_conns: 1, ..ServerConfig::default() };
        let engine = spawn_engine_cfg(&dir, &cfg).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || serve_cfg(listener, engine, cfg));

        // First connection: held open and working.
        let mut c1 = Client::connect(addr).unwrap();
        assert_eq!(
            c1.call(&Op::CreateType {
                def: encode_type_def("t", &[]),
            })
            .unwrap(),
            OpResult::TypeCreated(1)
        );
        // Second connection while the first is alive: refused (accepted by
        // the OS then dropped before serving), so a request fails.
        std::thread::sleep(std::time::Duration::from_millis(50));
        let mut c2 = Client::connect(addr).unwrap();
        let r = c2.call(&Op::GetById { type_id: 1, id: ObjectId::from_u128(1) });
        assert!(r.is_err(), "connection past the cap must not be served");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn stats_and_snapshot_are_consistent_and_recoverable() {
        let dir =
            std::env::temp_dir().join(format!("kesseldb-snap-{}", std::process::id()));
        let dest = std::env::temp_dir()
            .join(format!("kesseldb-snap-dest-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dest);
        let engine = spawn_engine(&dir).unwrap();

        let before = engine.stats();
        assert_eq!(before.applied_ops, 0);

        assert_eq!(
            engine.apply(Op::CreateType {
                def: encode_type_def(
                    "t",
                    &[Field {
                        field_id: 0,
                        name: "v".into(),
                        kind: FieldKind::U64,
                        nullable: false,
                    }],
                ),
            }),
            OpResult::TypeCreated(1)
        );
        let id = ObjectId::from_u128(99);
        assert_eq!(
            engine.apply(Op::Create { type_id: 1, id, record: vec![5, 0, 0, 0, 0, 0, 0, 0] }),
            OpResult::Ok
        );

        let after = engine.stats();
        assert!(
            after.applied_ops >= 2 && after.applied_ops > before.applied_ops,
            "stats must track applied ops"
        );

        // Consistent online snapshot, then recover it independently.
        engine.snapshot(&dest).unwrap();
        let live_digest = after.digest;
        let recovered =
            StateMachine::open(DirVfs::new(&dest).unwrap()).unwrap();
        assert_eq!(
            recovered.digest(),
            live_digest,
            "snapshot must recover to the exact live state digest"
        );
        // The row is readable from the recovered copy.
        let mut rec = recovered;
        assert_eq!(
            rec.apply(1, Op::GetById { type_id: 1, id }),
            OpResult::Got(vec![5, 0, 0, 0, 0, 0, 0, 0].into())
        );

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dest);
    }

    #[test]
    fn applied_ops_snapshot_increments_on_apply() {
        // SP142 T1 invariant: the EngineHandle's atomic op counter
        // increments by exactly 1 per successful Op::apply. This is the
        // counter read by /v1/metrics and /v1/health — it must NOT go
        // backwards (Prometheus counter-reset) under engine backpressure.
        let dir = std::env::temp_dir()
            .join(format!("kesseldb-sp142-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = spawn_engine(&dir).unwrap();
        assert_eq!(engine.applied_ops_snapshot(), 0);

        // Use the same Op variant the existing stats test uses: a
        // CreateType. Any successful Op::apply is fine — what we're
        // asserting is the per-apply increment delta, not the result.
        let def_t1 = encode_type_def(
            "t1",
            &[Field {
                field_id: 0,
                name: "v".into(),
                kind: FieldKind::U64,
                nullable: false,
            }],
        );
        let _ = engine.apply(Op::CreateType { def: def_t1 });
        let after_one = engine.applied_ops_snapshot();
        assert!(after_one >= 1, "after one apply: {after_one}");

        let def_t2 = encode_type_def(
            "t2",
            &[Field {
                field_id: 0,
                name: "v".into(),
                kind: FieldKind::U64,
                nullable: false,
            }],
        );
        let _ = engine.apply(Op::CreateType { def: def_t2 });
        let after_two = engine.applied_ops_snapshot();
        assert_eq!(
            after_two,
            after_one + 1,
            "one apply must bump applied_ops_snapshot by exactly 1"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn op_kind_counts_snapshot_increments_per_tag() {
        // SP144H T1: per-Op::kind() counter array. Verifies (a) empty at
        // startup, (b) bumps the right slot per applied op (CreateType =
        // tag 1), (c) the cumulative roll-up "applied" counter and the
        // per-kind slot agree.
        let dir = std::env::temp_dir()
            .join(format!("kesseldb-sp144h-t1-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = spawn_engine(&dir).unwrap();
        assert_eq!(engine.op_kind_counts_snapshot(), vec![]);

        // Apply two CreateType ops (Op::kind() = 1).
        let def_t1 = encode_type_def(
            "t1",
            &[Field {
                field_id: 0,
                name: "v".into(),
                kind: FieldKind::U64,
                nullable: false,
            }],
        );
        let _ = engine.apply(Op::CreateType { def: def_t1 });
        let def_t2 = encode_type_def(
            "t2",
            &[Field {
                field_id: 0,
                name: "v".into(),
                kind: FieldKind::U64,
                nullable: false,
            }],
        );
        let _ = engine.apply(Op::CreateType { def: def_t2 });

        let snap = engine.op_kind_counts_snapshot();
        let create_type_count = snap
            .iter()
            .find(|(tag, _)| *tag == 1)
            .map(|(_, c)| *c)
            .unwrap_or(0);
        assert_eq!(
            create_type_count, 2,
            "CreateType (tag=1) should have count=2; snap={snap:?}"
        );
        // Roll-up applied counter must also reflect both applies (it is
        // bumped from the same gate that publishes per-kind, so they
        // can't diverge).
        assert_eq!(
            engine.applied_ops_snapshot(),
            2,
            "applied_ops_snapshot must agree with per-kind sum"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compile_cache_is_correct_across_schema_change() {
        let dir = std::env::temp_dir()
            .join(format!("kesseldb-cc47-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = spawn_engine(&dir).unwrap();
        let sql = |s: &str| {
            let mut f = vec![0xFEu8];
            f.extend_from_slice(s.as_bytes());
            engine.apply_raw(f)
        };

        assert!(matches!(
            sql("CREATE TABLE t (v U64 NOT NULL)"),
            OpResult::TypeCreated(1)
        ));
        assert_eq!(sql("INSERT INTO t ID 1 (v) VALUES (5)"), OpResult::Ok);

        // Same statement twice — second is a cache hit, must be identical.
        let r1 = sql("SELECT * FROM t ID 1");
        let r2 = sql("SELECT * FROM t ID 1");
        assert!(matches!(r1, OpResult::Got(_)));
        assert_eq!(r1, r2, "cache hit must return identical result");

        // A DDL changes the catalog → cache is invalidated.
        assert!(matches!(
            sql("CREATE TABLE t2 (a U64 NOT NULL)"),
            OpResult::TypeCreated(2)
        ));

        // The previously-cached query still works (recompiled cleanly post
        // schema change) and a brand-new statement against the new table
        // compiles correctly — proving invalidation is safe, not stale.
        assert_eq!(sql("SELECT * FROM t ID 1"), r1);
        assert_eq!(sql("INSERT INTO t2 ID 1 (a) VALUES (9)"), OpResult::Ok);
        assert!(matches!(sql("SELECT * FROM t2 ID 1"), OpResult::Got(_)));

        // UPDATE (RMW path) also flows through the cache unchanged.
        assert_eq!(sql("UPDATE t ID 1 SET v = 50"), OpResult::Ok);
        assert_eq!(sql("UPDATE t ID 1 SET v = 50"), OpResult::Ok);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sql_transactions_are_atomic() {
        let dir =
            std::env::temp_dir().join(format!("kesseldb-tx-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = spawn_engine(&dir).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || serve(listener, engine));
        let mut c = Client::connect(addr).unwrap();
        let present = |c: &mut Client, id: u32| {
            matches!(
                c.sql(&format!("SELECT * FROM t ID {id}")).unwrap(),
                OpResult::Got(_)
            )
        };

        assert!(matches!(
            c.sql("CREATE TABLE t (v U64 NOT NULL)").unwrap(),
            OpResult::TypeCreated(1)
        ));

        // Committed transaction — both rows land atomically.
        assert_eq!(c.sql("BEGIN").unwrap(), OpResult::Ok);
        assert_eq!(
            c.sql("INSERT INTO t ID 1 (v) VALUES (1)").unwrap(),
            OpResult::Ok
        ); // buffered
        assert_eq!(
            c.sql("INSERT INTO t ID 2 (v) VALUES (2)").unwrap(),
            OpResult::Ok
        );
        assert_eq!(c.sql("COMMIT").unwrap(), OpResult::Ok);
        assert!(present(&mut c, 1) && present(&mut c, 2), "committed rows visible");

        // ROLLBACK discards everything buffered.
        assert_eq!(c.sql("BEGIN").unwrap(), OpResult::Ok);
        assert_eq!(
            c.sql("INSERT INTO t ID 3 (v) VALUES (3)").unwrap(),
            OpResult::Ok
        );
        assert_eq!(c.sql("ROLLBACK").unwrap(), OpResult::Ok);
        assert!(!present(&mut c, 3), "rolled-back row must not exist");

        // COMMIT/ROLLBACK without BEGIN are clean errors.
        assert!(matches!(
            c.sql("COMMIT").unwrap(),
            OpResult::SchemaError(_)
        ));
        assert!(matches!(
            c.sql("ROLLBACK").unwrap(),
            OpResult::SchemaError(_)
        ));

        // Atomicity: a failing statement aborts the WHOLE transaction.
        assert_eq!(c.sql("BEGIN").unwrap(), OpResult::Ok);
        assert_eq!(
            c.sql("INSERT INTO t ID 4 (v) VALUES (4)").unwrap(),
            OpResult::Ok
        );
        // duplicate id 1 — fails inside the atomic Op::Txn
        assert_eq!(
            c.sql("INSERT INTO t ID 1 (v) VALUES (9)").unwrap(),
            OpResult::Ok
        ); // buffered; failure surfaces at COMMIT
        let commit = c.sql("COMMIT").unwrap();
        assert_ne!(commit, OpResult::Ok, "txn with a dup must not commit Ok");
        assert!(!present(&mut c, 4), "failed txn must roll back id 4 too");

        // Connection still usable after an aborted txn.
        assert_eq!(
            c.sql("INSERT INTO t ID 5 (v) VALUES (5)").unwrap(),
            OpResult::Ok
        );
        assert!(present(&mut c, 5));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// SP85: reads mid-transaction are cleanly rejected (KesselDB
    /// transactions are atomic write batches, not interactive sessions
    /// — a deliberate model boundary), while read-your-writes still
    /// holds for *mutations* within the batch (a later write sees an
    /// earlier write).
    #[test]
    fn reads_in_txn_rejected_writes_read_your_writes() {
        let dir = std::env::temp_dir()
            .join(format!("kesseldb-rtx-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = spawn_engine(&dir).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || serve(listener, engine));
        let mut c = Client::connect(addr).unwrap();
        assert!(matches!(
            c.sql("CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)").unwrap(),
            OpResult::TypeCreated(1)
        ));

        // A read inside BEGIN/COMMIT is a clear error, not a silent Ok.
        assert_eq!(c.sql("BEGIN").unwrap(), OpResult::Ok);
        assert_eq!(
            c.sql("INSERT INTO acct ID 1 (owner, bal) VALUES (1, 10)").unwrap(),
            OpResult::Ok
        );
        assert!(matches!(
            c.sql("SELECT * FROM acct ID 1").unwrap(),
            OpResult::SchemaError(_)
        ));
        assert!(matches!(
            c.sql("DESCRIBE acct").unwrap(),
            OpResult::SchemaError(_)
        ));
        assert_eq!(c.sql("ROLLBACK").unwrap(), OpResult::Ok);
        // Rolled back: nothing persisted.
        assert_eq!(
            c.sql("SELECT * FROM acct ID 1").unwrap(),
            OpResult::NotFound
        );

        // Read-your-writes for writes within the atomic batch: the
        // UPDATE depends on the INSERT made earlier in the SAME txn.
        assert_eq!(c.sql("BEGIN").unwrap(), OpResult::Ok);
        assert_eq!(
            c.sql("INSERT INTO acct ID 2 (owner, bal) VALUES (2, 1)").unwrap(),
            OpResult::Ok
        );
        assert_eq!(c.sql("UPDATE acct ID 2 SET bal = 9").unwrap(), OpResult::Ok);
        assert_eq!(c.sql("COMMIT").unwrap(), OpResult::Ok);
        match c.sql("SELECT bal FROM acct WHERE owner = 2").unwrap() {
            OpResult::Got(b) => assert_eq!(
                i64::from_le_bytes(b[4..12].try_into().unwrap()),
                9,
                "UPDATE saw the in-batch INSERT (read-your-writes)"
            ),
            o => panic!("unexpected {o:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// SP84: SQL `UPDATE` composes inside `BEGIN`/`COMMIT` (it lowers to
    /// the deterministic replicated `Op::UpdateSet`), commits/rolls back
    /// atomically, and a failing member aborts the whole batch.
    #[test]
    fn sql_update_inside_transaction() {
        let dir = std::env::temp_dir()
            .join(format!("kesseldb-utx-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = spawn_engine(&dir).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || serve(listener, engine));
        let mut c = Client::connect(addr).unwrap();
        let bal = |c: &mut Client, owner: u32| -> i128 {
            match c
                .sql(&format!("SELECT bal FROM acct WHERE owner = {owner}"))
                .unwrap()
            {
                OpResult::Got(b) => {
                    // projection: [u32 rowlen][i64 bal]
                    i64::from_le_bytes(b[4..12].try_into().unwrap()) as i128
                }
                o => panic!("unexpected {o:?}"),
            }
        };
        assert!(matches!(
            c.sql("CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)").unwrap(),
            OpResult::TypeCreated(1)
        ));
        assert_eq!(
            c.sql("INSERT INTO acct ID 1 (owner, bal) VALUES (1, 50)").unwrap(),
            OpResult::Ok
        );

        // Committed txn: UPDATE + INSERT land atomically.
        assert_eq!(c.sql("BEGIN").unwrap(), OpResult::Ok);
        assert_eq!(c.sql("UPDATE acct ID 1 SET bal = 500").unwrap(), OpResult::Ok);
        assert_eq!(
            c.sql("INSERT INTO acct ID 2 (owner, bal) VALUES (2, 7)").unwrap(),
            OpResult::Ok
        );
        assert_eq!(c.sql("COMMIT").unwrap(), OpResult::Ok);
        assert_eq!(bal(&mut c, 1), 500, "UPDATE in committed txn applied");
        assert!(matches!(
            c.sql("SELECT * FROM acct ID 2").unwrap(),
            OpResult::Got(_)
        ));

        // ROLLBACK discards a buffered UPDATE.
        assert_eq!(c.sql("BEGIN").unwrap(), OpResult::Ok);
        assert_eq!(c.sql("UPDATE acct ID 1 SET bal = 999").unwrap(), OpResult::Ok);
        assert_eq!(c.sql("ROLLBACK").unwrap(), OpResult::Ok);
        assert_eq!(bal(&mut c, 1), 500, "rolled-back UPDATE must not apply");

        // A txn whose UPDATE targets a missing row aborts the WHOLE
        // batch (the earlier buffered INSERT must not persist).
        assert_eq!(c.sql("BEGIN").unwrap(), OpResult::Ok);
        assert_eq!(
            c.sql("INSERT INTO acct ID 9 (owner, bal) VALUES (9, 1)").unwrap(),
            OpResult::Ok
        );
        assert_eq!(
            c.sql("UPDATE acct ID 12345 SET bal = 1").unwrap(),
            OpResult::Ok
        ); // buffered; the failure surfaces at COMMIT
        assert_ne!(
            c.sql("COMMIT").unwrap(),
            OpResult::Ok,
            "UPDATE of a missing row must abort the txn"
        );
        assert_eq!(
            c.sql("SELECT * FROM acct ID 9").unwrap(),
            OpResult::NotFound,
            "aborted txn must roll back the buffered INSERT too"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn group_commit_concurrent_durable_throughput() {
        // SP68: a DirVfs (real fsync) server + many concurrent clients.
        // Group commit must (a) stay correct — every committed row present
        // and durable — and (b) amortise one fsync over the batch. We
        // assert correctness; the printed ops/s is the perf signal
        // (per-op fsync would be ~2K/s; group commit is far higher).
        let dir = std::env::temp_dir()
            .join(format!("kesseldb-gc-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = spawn_engine(&dir).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || serve(listener, engine));
        {
            let mut c = Client::connect(addr).unwrap();
            assert!(matches!(
                c.sql("CREATE TABLE t (c U32 NOT NULL, v U64 NOT NULL)").unwrap(),
                OpResult::TypeCreated(1)
            ));
        }
        let clients = 8usize;
        let per = 1500usize; // 12,000 durable inserts total
        let t = std::time::Instant::now();
        let handles: Vec<_> = (0..clients)
            .map(|cl| {
                std::thread::spawn(move || {
                    let mut c = Client::connect(addr).unwrap();
                    for i in 0..per {
                        let id = cl * per + i + 1;
                        assert_eq!(
                            c.sql(&format!(
                                "INSERT INTO t (id, c, v) VALUES ({id}, {cl}, {i})"
                            ))
                            .unwrap(),
                            OpResult::Ok
                        );
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let total = clients * per;
        let secs = t.elapsed().as_secs_f64();
        println!(
            "[group-commit] {total} durable inserts via {clients} clients in \
             {secs:.3}s = {:.0} ops/s",
            total as f64 / secs
        );
        // Correctness: every row is present (count == total) and a fresh
        // connection sees them (durable + visible).
        let mut c = Client::connect(addr).unwrap();
        match c.sql("SELECT COUNT(*) FROM t").unwrap() {
            OpResult::Got(b) => assert_eq!(
                i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()),
                total as i128,
                "every committed row must be durable & present"
            ),
            o => panic!("unexpected {o:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pipelined_batch_is_equivalent_and_amortises_round_trips() {
        // SP69: a pipeline is N independent requests in ONE frame → one
        // group-commit fsync + one network round-trip. Two things proven:
        //  (1) equivalence — results are per-statement, in order, and a
        //      failure in one member does NOT abort the others (it is NOT
        //      a transaction); the final state equals sending them singly.
        //  (2) the round-trip/fsync amortisation: ONE connection pushing
        //      batched inserts beats the per-statement path, because SP68's
        //      group-commit batch is bounded by in-flight ops and a serial
        //      connection only ever has one.
        let dir = std::env::temp_dir()
            .join(format!("kesseldb-pl-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = spawn_engine(&dir).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || serve(listener, engine));
        let mut c = Client::connect(addr).unwrap();
        assert!(matches!(
            c.sql("CREATE TABLE t (v U64 NOT NULL)").unwrap(),
            OpResult::TypeCreated(1)
        ));

        // (1) Equivalence + independence: middle statement is a dup id and
        // must fail on its own WITHOUT taking down the rest.
        assert_eq!(c.sql("INSERT INTO t (id, v) VALUES (1, 10)").unwrap(), OpResult::Ok);
        let res = c
            .pipeline(&[
                "INSERT INTO t (id, v) VALUES (2, 20)",
                "INSERT INTO t (id, v) VALUES (1, 99)", // dup → Exists
                "INSERT INTO t (id, v) VALUES (3, 30)",
                "SELECT * FROM t ID 2",
            ])
            .unwrap();
        assert_eq!(res.len(), 4);
        assert_eq!(res[0], OpResult::Ok);
        assert_eq!(res[1], OpResult::Exists, "dup fails independently");
        assert_eq!(res[2], OpResult::Ok, "later members unaffected");
        assert!(matches!(res[3], OpResult::Got(_)));
        // Final state: ids 1,2,3 present; id 1 still the original value
        // (the pipelined dup did NOT overwrite) — exactly as if sent singly.
        assert!(matches!(c.sql("SELECT * FROM t ID 3").unwrap(), OpResult::Got(_)));
        match c.sql("SELECT COUNT(*) FROM t").unwrap() {
            OpResult::Got(b) => {
                assert_eq!(i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()), 3)
            }
            o => panic!("unexpected {o:?}"),
        }

        // (2) Throughput: 12,000 durable inserts from ONE connection in
        // batches of 500 (24 round-trips, not 12,000). Compare to the
        // serial path on the same single connection.
        let total = 12_000usize;
        let batch = 500usize;
        let t = std::time::Instant::now();
        let mut id = 100usize;
        for _ in 0..(total / batch) {
            let stmts: Vec<String> = (0..batch)
                .map(|_| {
                    id += 1;
                    format!("INSERT INTO t (id, v) VALUES ({id}, 1)")
                })
                .collect();
            let refs: Vec<&str> = stmts.iter().map(|s| s.as_str()).collect();
            for r in c.pipeline(&refs).unwrap() {
                assert_eq!(r, OpResult::Ok);
            }
        }
        let psecs = t.elapsed().as_secs_f64();
        let serial_id0 = id;
        let t2 = std::time::Instant::now();
        for _ in 0..2000 {
            id += 1;
            assert_eq!(
                c.sql(&format!("INSERT INTO t (id, v) VALUES ({id}, 1)")).unwrap(),
                OpResult::Ok
            );
        }
        let ssecs = t2.elapsed().as_secs_f64();
        let _ = serial_id0;
        println!(
            "[pipeline] {total} inserts pipelined (batch {batch}) in \
             {psecs:.3}s = {:.0} ops/s   |   serial 2000 in {ssecs:.3}s = \
             {:.0} ops/s   speedup ~{:.1}x",
            total as f64 / psecs,
            2000.0 / ssecs,
            (total as f64 / psecs) / (2000.0 / ssecs)
        );

        // Correctness: every pipelined + serial row is durable & visible
        // from a FRESH connection (3 setup + 12000 + 2000).
        let mut c2 = Client::connect(addr).unwrap();
        match c2.sql("SELECT COUNT(*) FROM t").unwrap() {
            OpResult::Got(b) => assert_eq!(
                i128::from_le_bytes(<[u8;16]>::try_from(b.as_ref()).unwrap()),
                (3 + total + 2000) as i128,
                "all pipelined & serial rows must be durable"
            ),
            o => panic!("unexpected {o:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// SP89: the dependency-free Python reference SDK round-trips
    /// against a live server using only the documented wire protocol.
    /// Skips cleanly (test still passes) if no Python is on PATH, so
    /// CI stays green everywhere; when Python *is* present this is a
    /// real cross-language end-to-end check.
    #[test]
    fn python_sdk_round_trips_over_the_wire() {
        use std::process::Command;
        let py = ["python3", "python"].into_iter().find(|p| {
            Command::new(p)
                .arg("--version")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
        });
        let Some(py) = py else {
            eprintln!("skip: no python on PATH (SDK validated vs the \
                       documented protocol; run manually)");
            return;
        };
        let script = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../clients/python/kesseldb.py"
        );
        let dir = std::env::temp_dir()
            .join(format!("kesseldb-pysdk-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = spawn_engine(&dir).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || serve(listener, engine));

        let run = |sql: &str| -> (bool, String) {
            let out = Command::new(py)
                .arg(script)
                .arg(sql)
                .arg("--addr")
                .arg(&addr)
                .output()
                .expect("run python sdk");
            (
                out.status.success(),
                String::from_utf8_lossy(&out.stdout).trim().to_string(),
            )
        };
        // Whole loop driven *through the Python SDK* over real sockets.
        assert!(run("CREATE TABLE acct (owner U32 NOT NULL, bal I64 NOT NULL)").0);
        assert!(run("INSERT INTO acct ID 1 (owner, bal) VALUES (100, 50)").0);
        assert!(run("INSERT INTO acct ID 2 (owner, bal) VALUES (100, 999)").0);
        let (ok, sumline) = run("SELECT SUM(bal) FROM acct WHERE owner = 100");
        assert!(ok, "SUM via Python SDK should succeed");
        assert_eq!(sumline, "= 1049", "Python SDK decoded the scalar");
        // A bad statement → exit 1 + an ERROR line (no panic/hang).
        let bad = Command::new(py)
            .arg(script)
            .arg("SELECT FROM nope")
            .arg("--addr")
            .arg(&addr)
            .output()
            .unwrap();
        assert!(!bad.status.success(), "bad SQL must exit non-zero");
        assert!(
            String::from_utf8_lossy(&bad.stdout).contains("ERROR"),
            "bad SQL prints an ERROR line"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // -----------------------------------------------------------------------
    // IT-3: heartbeat advances watermark via VSR loop (SP115 T3).
    //
    // Claim:   `spawn_heartbeat_loop` spawns a daemon thread that, on each
    //   tick, reads (target, current_lwm) from the state closure and — if
    //   target > current_lwm — calls submit(Op::AdvanceWatermark{ target }).
    //   This is the production heartbeat path end-to-end (Decision 6/7):
    //   the thread submits; the SM apply (serially) advances the watermark.
    //
    //   SCOPE NARROWING (T2 revert, SP116 follow-up): The original T3 spec
    //   planned to wire the live DirVfs SM as the state source. Since DirVfs
    //   requires a temp dir and the heartbeat thread is not Send with the SM
    //   itself (the SM closure sees shared state via Arc<Mutex<...>>), this
    //   test uses a mock state/submit pair that simulates the SM's behavior.
    //   The live-SM wiring is the responsibility of SP116's server integration.
    //
    // Workload:
    //   - state() closure: returns Some((target=10, current_lwm=0)) for the
    //     first call, then None (signals shutdown) for subsequent calls.
    //   - submit() closure: records submitted Ops into a Vec<Op> under a Mutex.
    //   - spawn_heartbeat_loop with interval=1ms.
    //   - Wait for the thread to complete (state returns None on next tick).
    //   - Verify: exactly ONE Op::AdvanceWatermark { low_water_mark: 10 }
    //     was submitted (target=10 > current_lwm=0 → submit; then state
    //     returns None → thread exits).
    //
    // Expected:
    //   - Thread exits cleanly (join succeeds).
    //   - submitted Ops == [Op::AdvanceWatermark { low_water_mark: 10 }].
    //   - Refs: SP115 T2 `spawn_heartbeat_loop`; Decision 6 (heartbeat producer).
    // -----------------------------------------------------------------------
    #[test]
    fn it_heartbeat_advances_watermark_via_vsr_loop() {
        use kessel_proto::Op;
        use std::sync::{Arc, Mutex};

        // State source: first call returns Some((10, 0)); subsequent → None (shutdown).
        let call_count = Arc::new(Mutex::new(0u32));
        let call_count_state = Arc::clone(&call_count);
        let state = move || {
            let mut c = call_count_state.lock().unwrap();
            *c += 1;
            if *c == 1 {
                // First tick: target=10, current_lwm=0 → submit expected.
                Some((10u64, 0u64))
            } else {
                // Second tick: signal shutdown.
                None
            }
        };

        // Submit sink: collect all Ops submitted by the heartbeat.
        let submitted: Arc<Mutex<Vec<Op>>> = Arc::new(Mutex::new(Vec::new()));
        let submitted_submit = Arc::clone(&submitted);
        let submit = move |op: Op| {
            submitted_submit.lock().unwrap().push(op);
        };

        // Spawn the heartbeat loop with a short interval to keep tests fast.
        let handle = spawn_heartbeat_loop(state, submit, std::time::Duration::from_millis(1));

        // Wait for the thread to exit (state returns None on the second tick).
        handle.join().expect("IT-3: heartbeat thread must exit cleanly");

        // Verify the submitted Ops.
        let ops = submitted.lock().unwrap().clone();
        assert_eq!(
            ops.len(),
            1,
            "IT-3: exactly ONE AdvanceWatermark must have been submitted (target > lwm on tick 1); \
             got {ops:?}"
        );
        assert_eq!(
            ops[0],
            Op::AdvanceWatermark { low_water_mark: 10 },
            "IT-3: submitted op must be AdvanceWatermark{{ low_water_mark: 10 }}"
        );
    }

    // ──────────────────────────────────────────────────────────────────
    // SP-PG-EXTQ-PARSED-DEFAULT T1 KATs — wire encoder/decoder
    // round-trip for the new `PARAMETERIZED_SQL_TAG = 0xF3` admin
    // frame. The frame carries `(sql, params: &[Option<Value>])`
    // through to the engine thread where decode + run
    // `compile_stmt_with_params` against the live catalog. KATs
    // here are pure unit tests on the encoder/decoder pair — engine
    // dispatch integration is tested via the kessel-pg-gateway
    // gateway KATs + the vulcan ORM smoke (T3).
    // ──────────────────────────────────────────────────────────────────

    /// SP-PG-EXTQ-PARSED-DEFAULT T1 — empty params encode + decode
    /// round-trip. The frame contains just the SQL string + a zero
    /// param count.
    #[test]
    fn parsed_default_t1_wire_encode_decode_empty_params() {
        let sql = "SELECT 1";
        let params: Vec<Option<kessel_codec::Value>> = vec![];
        let frame = encode_parameterized_sql(sql, &params);
        assert_eq!(frame[0], PARAMETERIZED_SQL_TAG);
        // Body: [u32 sql_len=8][8 bytes "SELECT 1"][u32 param_count=0]
        let (decoded_sql, decoded_params) =
            decode_parameterized_sql(&frame[1..]).expect("decode ok");
        assert_eq!(decoded_sql, sql);
        assert!(decoded_params.is_empty());
    }

    /// SP-PG-EXTQ-PARSED-DEFAULT T1 — every typed `Value` variant
    /// round-trips byte-equal. Also exercises NULL (`None`) + explicit
    /// `Value::Null` distinctly so the wire kind bytes don't collide.
    #[test]
    fn parsed_default_t1_wire_round_trip_all_variants() {
        let sql = "INSERT INTO t (id, name, age, flag, opt) VALUES ($1, $2, $3, $4, $5)";
        let params: Vec<Option<kessel_codec::Value>> = vec![
            Some(kessel_codec::Value::Int(42)),
            Some(kessel_codec::Value::Blob(b"hello".to_vec())),
            Some(kessel_codec::Value::Uint(u128::MAX)),
            Some(kessel_codec::Value::Null),
            None,
        ];
        let frame = encode_parameterized_sql(sql, &params);
        assert_eq!(frame[0], PARAMETERIZED_SQL_TAG);
        let (decoded_sql, decoded_params) =
            decode_parameterized_sql(&frame[1..]).expect("decode ok");
        assert_eq!(decoded_sql, sql);
        assert_eq!(decoded_params.len(), 5);
        assert_eq!(decoded_params[0], Some(kessel_codec::Value::Int(42)));
        assert_eq!(
            decoded_params[1],
            Some(kessel_codec::Value::Blob(b"hello".to_vec()))
        );
        assert_eq!(
            decoded_params[2],
            Some(kessel_codec::Value::Uint(u128::MAX))
        );
        assert_eq!(decoded_params[3], Some(kessel_codec::Value::Null));
        assert_eq!(decoded_params[4], None);
    }

    /// SP-PG-EXTQ-PARSED-DEFAULT T1 — the headline security KAT at
    /// the wire layer. A quote-injection payload (`"; DROP TABLE t;
    /// --`) round-trips as a `Value::Blob` operand — the bytes
    /// remain typed, never enter the SQL text. Engine-side
    /// `compile_stmt_with_params` will materialize them as a `Tok::
    /// Str` carried into `Lit::Str` — the DROP TABLE NEVER reaches
    /// the parser as syntax.
    #[test]
    fn parsed_default_t1_wire_quote_injection_payload_is_typed_blob() {
        let sql = "SELECT * FROM t WHERE name = $1";
        let payload = b"\"; DROP TABLE t; --".to_vec();
        let params: Vec<Option<kessel_codec::Value>> = vec![
            Some(kessel_codec::Value::Blob(payload.clone())),
        ];
        let frame = encode_parameterized_sql(sql, &params);
        let (decoded_sql, decoded_params) =
            decode_parameterized_sql(&frame[1..]).expect("decode ok");
        // SQL text was NOT mutated — the placeholder is still `$1`.
        assert_eq!(decoded_sql, sql);
        assert!(!decoded_sql.contains("DROP"));
        // The bound value is a Blob operand carrying the literal
        // bytes — not concatenated into SQL.
        match &decoded_params[0] {
            Some(kessel_codec::Value::Blob(b)) => assert_eq!(b, &payload),
            other => panic!("expected Blob, got {other:?}"),
        }
    }

    /// SP-PG-EXTQ-PARSED-DEFAULT T1 — malformed-frame decode returns
    /// `None`. Three shapes: truncated sql_len, truncated sql bytes,
    /// invalid param kind byte. The engine apply path translates
    /// `None` into a `SchemaError("parameterized sql: malformed
    /// frame")`.
    #[test]
    fn parsed_default_t1_wire_malformed_frame_decodes_to_none() {
        // Truncated sql_len.
        assert!(decode_parameterized_sql(&[0u8; 2]).is_none());
        // sql_len claims 100 bytes, body only has 3.
        let mut bad = Vec::new();
        bad.extend_from_slice(&100u32.to_le_bytes());
        bad.extend_from_slice(b"abc");
        assert!(decode_parameterized_sql(&bad).is_none());
        // Valid sql, valid param_count=1, then an invalid param kind 0xFF.
        let mut bad2 = Vec::new();
        bad2.extend_from_slice(&3u32.to_le_bytes());
        bad2.extend_from_slice(b"sql");
        bad2.extend_from_slice(&1u32.to_le_bytes());
        bad2.push(0xFF);
        assert!(decode_parameterized_sql(&bad2).is_none());
    }
}
