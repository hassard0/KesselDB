//! kesseldb-server: a runnable single-node TCP server.
//!
//! The deterministic core (`kessel-sm`) lives on ONE owning thread and never
//! moves; connection threads talk to it over a channel. So apply is serial
//! (matching the single-threaded-core design) and the engine never needs to
//! be `Send`. The server is just the real-I/O edge; the engine stays pure.
//! VSR-over-sockets (multi-node networking) is still deferred and documented.

#![forbid(unsafe_code)]

pub mod cluster;
pub mod router;

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
use std::sync::Arc;

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
    let op = if frame.first() == Some(&0xFE) {
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
            Ok(kessel_sql::Stmt::Explain(plan)) => {
                return OpResult::Got(plan.into_bytes());
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
    pub fn apply_raw(&self, frame: Vec<u8>) -> OpResult {
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
    pub fn apply(&self, op: Op) -> OpResult {
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
pub fn spawn_engine_cfg(
    data_dir: impl AsRef<Path>,
    cfg: &ServerConfig,
) -> io::Result<EngineHandle> {
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
    std::thread::spawn(move || {
        let mut sm = match DirVfs::new(&dir).and_then(StateMachine::open) {
            Ok(sm) => {
                let _ = ready_tx.send(Ok(()));
                sm
            }
            Err(e) => {
                let _ = ready_tx.send(Err(e));
                return;
            }
        };
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
        sm.set_autosync(false);
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
                    return OpResult::Got(st.encode());
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
                            OpResult::Got(payload)
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
            let n_before = n;
            let res = compute(&mut sm, &mut cache, &mut n, &frame);
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
                        let res = compute(&mut sm, &mut cache, &mut n, &f);
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
            for (res, rp) in batch {
                let _ = rp.send(res);
            }
        }
    });
    match ready_rx.recv() {
        Ok(Ok(())) => Ok(EngineHandle {
            tx,
            inflight: Arc::new(AtomicUsize::new(0)),
            max_inflight,
            applied_ops_atomic: applied_ops_atomic_for_handle,
            op_kind_counts: op_kind_counts_for_handle,
            #[cfg(feature = "http-gateway")]
            http_counters: http_counters_for_handle,
        }),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(io::Error::new(io::ErrorKind::Other, "engine failed to start")),
    }
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
            // SP144H T2: pull the per-(path, status) snapshot from the
            // shared 4×16 atomic matrix. The matrix is bumped by the
            // gateway accept loop on every emitted response.
            http_requests_total: self.http_counters.snapshot(),
        }
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
            OpResult::Got(vec![7, 7, 7])
        );
        assert_eq!(
            c.call(&Op::Create { type_id: 1, id, record: vec![9] }).unwrap(),
            OpResult::Exists
        );
        // a second connection sees the same committed state
        let mut c2 = Client::connect(addr).unwrap();
        assert_eq!(
            c2.call(&Op::GetById { type_id: 1, id }).unwrap(),
            OpResult::Got(vec![7, 7, 7])
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
                assert_eq!(i128::from_le_bytes(b.try_into().unwrap()), 1049)
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
                assert_eq!(i128::from_le_bytes(b.try_into().unwrap()), 1499)
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
            OpResult::Got(vec![5, 0, 0, 0, 0, 0, 0, 0])
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
                i128::from_le_bytes(b.try_into().unwrap()),
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
                assert_eq!(i128::from_le_bytes(b.try_into().unwrap()), 3)
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
                i128::from_le_bytes(b.try_into().unwrap()),
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
}
