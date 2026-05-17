//! kesseldb-server: a runnable single-node TCP server.
//!
//! The deterministic core (`kessel-sm`) lives on ONE owning thread and never
//! moves; connection threads talk to it over a channel. So apply is serial
//! (matching the single-threaded-core design) and the engine never needs to
//! be `Send`. The server is just the real-I/O edge; the engine stays pure.
//! VSR-over-sockets (multi-node networking) is still deferred and documented.

#![forbid(unsafe_code)]

pub mod cluster;

use kessel_io::DirVfs;
use kessel_proto::wire::{read_frame, write_frame};
use kessel_proto::{Op, OpResult};
use kessel_sm::StateMachine;
use std::io;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender};
use std::sync::Arc;

/// First-frame auth handshake tag: `[0xFC] ++ token`.
pub const AUTH_TAG: u8 = 0xFC;

/// Server policy: optional shared-secret token, plus quota / backpressure
/// caps. Defaults are open + generous so existing embeddings are unchanged.
#[derive(Clone)]
pub struct ServerConfig {
    /// `None` = open (no handshake expected). `Some(t)` = the first frame on
    /// every connection must be `[0xFC] ++ t` (constant-time compared).
    pub token: Option<Vec<u8>>,
    /// Max concurrent client connections; the next is refused immediately.
    pub max_conns: usize,
    /// Max requests in flight to the engine; over this, callers get
    /// `OpResult::Unavailable` (backpressure) instead of unbounded queueing.
    pub max_inflight: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig { token: None, max_conns: 1024, max_inflight: 4096 }
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
fn authenticate(stream: &mut TcpStream, token: &Option<Vec<u8>>) -> bool {
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

/// One request to the engine thread: an op and a one-shot reply channel.
type EngineMsg = (Vec<u8>, SyncSender<OpResult>);

/// Handle used by connection threads to submit ops to the single engine.
#[derive(Clone)]
pub struct EngineHandle {
    tx: Sender<EngineMsg>,
    inflight: Arc<AtomicUsize>,
    max_inflight: usize,
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
        while let Ok((frame, reply)) = rx.recv() {
            let op = if frame.first() == Some(&0xFE) {
                let sql = match std::str::from_utf8(&frame[1..]) {
                    Ok(s) => s,
                    Err(_) => {
                        let _ = reply.send(OpResult::SchemaError("sql: not utf8".into()));
                        continue;
                    }
                };
                match kessel_sql::compile_stmt(sql, sm.catalog()) {
                    Ok(kessel_sql::Stmt::Op(o)) => Some(o),
                    Ok(kessel_sql::Stmt::Update { type_id, id, sets }) => {
                        // Server-side read-modify-write for SQL UPDATE.
                        let oid = kessel_proto::ObjectId::from_u128(id);
                        let cur = sm.apply(n, Op::GetById { type_id, id: oid });
                        n += 1;
                        let rec = match cur {
                            OpResult::Got(r) => r,
                            other => {
                                let _ = reply.send(other); // NotFound etc.
                                continue;
                            }
                        };
                        let ot = match sm.catalog().get(type_id) {
                            Some(t) => t.clone(),
                            None => {
                                let _ = reply
                                    .send(OpResult::SchemaError("update: no type".into()));
                                continue;
                            }
                        };
                        let mut vals = match kessel_codec::decode(&ot, &rec) {
                            Ok(v) => v,
                            Err(e) => {
                                let _ = reply.send(OpResult::SchemaError(format!(
                                    "update decode: {e:?}"
                                )));
                                continue;
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
                                let _ = reply.send(OpResult::SchemaError(format!(
                                    "update encode: {e:?}"
                                )));
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = reply.send(OpResult::SchemaError(format!("sql: {e}")));
                        continue;
                    }
                }
            } else {
                Op::decode(&frame)
            };
            let r = match op {
                Some(o) => {
                    let r = sm.apply(n, o);
                    n += 1;
                    r
                }
                None => OpResult::SchemaError("malformed request frame".into()),
            };
            let _ = reply.send(r);
        }
    });
    match ready_rx.recv() {
        Ok(Ok(())) => Ok(EngineHandle {
            tx,
            inflight: Arc::new(AtomicUsize::new(0)),
            max_inflight,
        }),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(io::Error::new(io::ErrorKind::Other, "engine failed to start")),
    }
}

fn handle_conn(mut stream: TcpStream, engine: EngineHandle, token: Option<Vec<u8>>) {
    if !authenticate(&mut stream, &token) {
        return; // rejected; Unauthorized already written
    }
    loop {
        let req = match read_frame(&mut stream) {
            Ok(r) => r,
            Err(_) => break,
        };
        let result = engine.apply_raw(req);
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
    for stream in listener.incoming().flatten() {
        if active.load(Ordering::Acquire) >= cfg.max_conns {
            drop(stream); // at capacity — refuse
            continue;
        }
        active.fetch_add(1, Ordering::AcqRel);
        let e = engine.clone();
        let tok = cfg.token.clone();
        let a = active.clone();
        std::thread::spawn(move || {
            handle_conn(stream, e, tok);
            a.fetch_sub(1, Ordering::AcqRel);
        });
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
}
